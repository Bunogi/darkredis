use crate::{Command, CommandList, Error, Result, Value};
use futures::lock::Mutex;

#[cfg(feature = "runtime_agnostic")]
use async_std::{
    io,
    net::{TcpStream, ToSocketAddrs},
};
#[cfg(feature = "runtime_agnostic")]
use futures::{AsyncReadExt, AsyncWriteExt};

#[cfg(feature = "runtime_tokio")]
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
#[cfg(feature = "runtime_tokio")]
use tokio_net::ToSocketAddrs;

use std::{sync::Arc, time};

pub mod stream;
pub use stream::{Message, MessageStream, PMessage, PMessageStream};

#[cfg(test)]
mod test;

async fn read_until(r: &mut TcpStream, byte: u8) -> io::Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut single = [0; 1];
    loop {
        r.read(&mut single).await?;
        buffer.push(single[0]);
        if single[0] == byte {
            return Ok(buffer);
        }
    }
}

///A connection to Redis. Copying is cheap as the inner type is a simple, futures-aware, `Arc<Mutex>`, and will
///not create a new connection. Use a [`ConnectionPool`](crate::ConnectionPool) if you want to use pooled conections.
///Every convenience function can work with any kind of data as long as it can be converted into bytes.
#[derive(Clone)]
pub struct Connection {
    pub(crate) stream: Arc<Mutex<TcpStream>>,
}

impl Connection {
    ///Connect to a Redis instance running at `address`. If you wish to name this connection, run the [`CLIENT SETNAME`](https://redis.io/commands/client-setname) command.
    pub async fn connect<A>(address: A, password: Option<&str>) -> Result<Self>
    where
        A: ToSocketAddrs,
    {
        let stream = Arc::new(Mutex::new(
            TcpStream::connect(address)
                .await
                .map_err(Error::ConnectionFailed)?,
        ));
        let mut out = Self { stream };

        if let Some(pass) = password {
            out.run_command(Command::new("AUTH").arg(&pass)).await?;
        }

        Ok(out)
    }

    async fn parse_simple_value(buf: &[u8]) -> Result<Value> {
        match buf[0] {
            b'+' => {
                if buf == b"+OK\r\n" {
                    Ok(Value::Ok)
                } else {
                    Ok(Value::String(buf[1..].into()))
                }
            }
            b'-' => Err(Error::RedisError(
                String::from_utf8_lossy(&buf[1..]).to_string(),
            )),
            b':' => {
                let string = String::from_utf8_lossy(&buf[1..]);
                let num = string.trim().parse::<isize>().unwrap();
                Ok(Value::Integer(num))
            }
            _ => Err(Error::UnexpectedResponse(
                String::from_utf8_lossy(buf).to_string(),
            )),
        }
    }

    async fn parse_string(start: &[u8], stream: &mut TcpStream) -> Result<Value> {
        if start == b"$-1\r\n" {
            Ok(Value::Nil)
        } else {
            let num = String::from_utf8_lossy(&start[1..])
                .trim()
                .parse::<usize>()
                .unwrap();
            let mut buf = vec![0u8; num + 2]; // add two to catch the final \r\n from Redis
            stream.read_exact(&mut buf).await?;

            buf.pop(); //Discard the last \r\n
            buf.pop();
            Ok(Value::String(buf))
        }
    }

    //Assumes that there will never be nested arrays in a redis response.
    async fn parse_array(start: &[u8], mut stream: &mut TcpStream) -> Result<Value> {
        let num = String::from_utf8_lossy(&start[1..])
            .trim()
            .parse::<usize>()
            .unwrap();

        let mut values = Vec::with_capacity(num);

        for _ in 0..num {
            let buf = read_until(&mut stream, b'\n').await?;
            match buf[0] {
                b'+' | b'-' | b':' => values.push(Self::parse_simple_value(&buf).await?),
                b'$' => values.push(Self::parse_string(&buf, &mut stream).await?),
                _ => {
                    return Err(Error::UnexpectedResponse(
                        String::from_utf8_lossy(&buf).to_string(),
                    ))
                }
            }
        }

        Ok(Value::Array(values))
    }

    //Read a value from the connection.
    pub(crate) async fn read_value(mut stream: &mut TcpStream) -> Result<Value> {
        let buf = read_until(&mut stream, b'\n').await?;
        match buf[0] {
            b'+' | b'-' | b':' => Self::parse_simple_value(&buf).await,
            b'$' => Self::parse_string(&buf, &mut stream).await,
            b'*' => Self::parse_array(&buf, &mut stream).await,
            _ => Err(Error::UnexpectedResponse(
                String::from_utf8_lossy(&buf).to_string(),
            )),
        }
    }

    ///Run a series of commands on this connection.
    pub async fn run_commands(&mut self, command: CommandList<'_>) -> Result<Vec<Value>> {
        let mut stream = self.stream.lock().await;
        let number_of_commands = command.command_count();
        let serialized: Vec<u8> = command.serialize();
        stream.write_all(&serialized).await?;

        let mut results = Vec::with_capacity(number_of_commands);
        for _ in 0..number_of_commands {
            results.push(Self::read_value(&mut stream).await?);
        }

        Ok(results)
    }

    ///Run a single command on this connection.
    pub async fn run_command(&mut self, command: Command<'_>) -> Result<Value> {
        let mut stream = self.stream.lock().await;
        let serialized: Vec<u8> = command.serialize();
        stream.write_all(&serialized).await?;

        Ok(Self::read_value(&mut stream).await?)
    }

    ///Consume `self`, and subscribe to `channels`, returning a stream of [`Message`s](stream::Message). As of now, there's no way to get the connection back, nor change the subscribed topics.
    pub async fn subscribe<K>(mut self, channels: &[K]) -> Result<stream::MessageStream>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("SUBSCRIBE").args(channels);

        //TODO: Find out if we care about the values given here
        let _ = self.run_command(command).await?;
        {
            let mut stream = self.stream.lock().await;
            for _ in 0..channels.len() - 1 {
                let response = Self::read_value(&mut stream).await?;
                assert_eq!(
                    response.unwrap_array()[0],
                    Value::String("subscribe".into())
                );
            }
        }

        Ok(stream::MessageStream::new(self))
    }

    ///Exactly like [`subscribe`](Self::subscribe), but subscribe to patterns instead.
    pub async fn psubscribe<K>(mut self, patterns: &[K]) -> Result<stream::PMessageStream>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("PSUBSCRIBE").args(patterns);

        //TODO: Find out if we care about the values given here
        let _ = self.run_command(command).await?;
        {
            let mut stream = self.stream.lock().await;
            for _ in 0..patterns.len() - 1 {
                let response = Self::read_value(&mut stream).await?;
                assert_eq!(
                    response.unwrap_array()[0],
                    Value::String("psubscribe".into())
                );
            }
        }

        Ok(stream::PMessageStream::new(self))
    }

    ///Publsh `message` to `channel`. Returns how many clients received the message.
    pub async fn publish<C, M>(&mut self, channel: C, message: M) -> Result<isize>
    where
        C: AsRef<[u8]>,
        M: AsRef<[u8]>,
    {
        let command = Command::new("PUBLISH").arg(&channel).arg(&message);
        self.run_command(command).await.map(|i| i.unwrap_integer())
    }

    ///Sets `key` to `value`.
    pub async fn set<K, D>(&mut self, key: K, value: D) -> Result<()>
    where
        K: AsRef<[u8]>,
        D: AsRef<[u8]>,
    {
        let command = Command::new("SET").arg(&key).arg(&value);

        self.run_command(command).await.map(|_| ())
    }

    ///Set `key` to `value`, and set `key` to expire in `expiry`'s timeframe.
    pub async fn set_with_expiry<K, D>(
        &mut self,
        key: K,
        data: D,
        expiry: time::Duration,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        D: AsRef<[u8]>,
    {
        let expiry = expiry.as_secs().to_string();
        let command = Command::new("SET")
            .arg(&key)
            .arg(&data)
            .arg(b"EX")
            .arg(&expiry);

        self.run_command(command).await.map(|_| ())
    }

    ///Delete `key`.
    pub async fn del<K>(&mut self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("DEL").arg(&key);
        self.run_command(command).await.map(|_| ())
    }

    ///Get the value of `key`.
    pub async fn get<K>(&mut self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("GET").arg(&key);

        Ok(self.run_command(command).await?.optional_string())
    }

    ///Push a value to `list` from the left.
    ///# Return value
    ///The number of elements in `list`
    pub async fn lpush<K, V>(&mut self, list: K, value: V) -> Result<isize>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let command = Command::new("LPUSH").arg(&list).arg(&value);

        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Like [lpush](Self::lpush), but push multiple values through a slice.
    pub async fn lpush_slice<K, V>(&mut self, key: K, data: &[V]) -> Result<isize>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let command = Command::new("LPUSH").arg(&key).args(data);

        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Push a value to `list` from the right.
    ///# Return value
    ///The number of elements in `list`
    pub async fn rpush<K, V>(&mut self, list: K, value: V) -> Result<isize>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let command = Command::new("RPUSH").arg(&list).arg(&value);

        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Like [rpush](Self::rpush) but push multiple values through a slice.
    pub async fn rpush_slice<K, V>(&mut self, key: K, values: &[V]) -> Result<isize>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let command = Command::new("RPUSH").arg(&key).args(values);

        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Pop a value from a list from the left side.
    ///# Return value
    ///The value popped from `list`
    pub async fn lpop<K>(&mut self, list: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("LPOP").arg(&list);

        Ok(self.run_command(command).await?.optional_string())
    }

    ///Pop a value from a list from the right side.
    ///# Return value
    ///The value popped from `list`
    pub async fn rpop<K>(&mut self, list: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("RPOP").arg(&list);

        Ok(self.run_command(command).await?.optional_string())
    }

    ///Get a series of elements from `list`, from index `from` to `to`. If they are negative, take the
    ///index from the right side of the list.
    pub async fn lrange<K>(&mut self, list: K, from: isize, to: isize) -> Result<Vec<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let from = from.to_string();
        let to = to.to_string();
        let command = Command::new("LRANGE").arg(&list).arg(&from).arg(&to);

        Ok(self
            .run_command(command)
            .await?
            .unwrap_array()
            .into_iter()
            .map(|s| s.unwrap_string())
            .collect())
    }

    ///Get the number of elements in `list`, or `None` if the list doesn't exist.
    pub async fn llen<K>(&mut self, list: K) -> Result<Option<isize>>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("LLEN").arg(&list);
        Ok(self.run_command(command).await?.optional_integer())
    }

    ///Set the value of the element at `index` in `list` to `value`.
    pub async fn lset<K, V>(&mut self, list: K, index: usize, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let index = index.to_string();
        let command = Command::new("LSET").arg(&list).arg(&index).arg(&value);

        self.run_command(command).await?;
        Ok(())
    }

    ///Trim `list` from `start` to `stop`.
    pub async fn ltrim<K>(&mut self, list: K, start: usize, stop: usize) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let start = start.to_string();
        let stop = stop.to_string();
        let command = Command::new("LTRIM").arg(&list).arg(&start).arg(&stop);
        self.run_command(command).await?;

        Ok(())
    }

    ///Increment `key` by one.
    ///# Return value
    ///The new value of `key`.
    pub async fn incr<K>(&mut self, key: K) -> Result<isize>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("INCR").arg(&key);
        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Increment `key` by `val`.
    ///# Return value
    ///The new value of `key`
    pub async fn incrby<K>(&mut self, key: K, val: isize) -> Result<isize>
    where
        K: AsRef<[u8]>,
    {
        let val = val.to_string();
        let command = Command::new("INCRBY").arg(&key).arg(&val);
        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Increment `key` by a floating point value `val`.
    ///# Return value
    ///The new value of `key`
    pub async fn incrbyfloat<K>(&mut self, key: K, val: f64) -> Result<f64>
    where
        K: AsRef<[u8]>,
    {
        let val = val.to_string();
        let command = Command::new("INCRBYFLOAT").arg(&key).arg(&val);
        let result = self.run_command(command).await?.unwrap_string();
        Ok(String::from_utf8_lossy(&result).parse::<f64>().unwrap())
    }

    ///Decrement `key` by a floating point value `val`.
    ///# Return value
    ///The new value of `key`
    pub async fn decr<K>(&mut self, key: K) -> Result<isize>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("DECR").arg(&key);
        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Decrement `key` by `val`.
    ///# Return value
    ///The new value of `key`
    pub async fn decrby<K>(&mut self, key: K, val: isize) -> Result<isize>
    where
        K: AsRef<[u8]>,
    {
        let val = val.to_string();
        let command = Command::new("DECRBY").arg(&key).arg(&val);
        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Append a string `val` to `key`.
    ///# Return value
    ///The new size of `key`
    pub async fn append<K, V>(&mut self, key: K, val: V) -> Result<isize>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let command = Command::new("APPEND").arg(&key).arg(&val);
        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Get the string value for every `key`, or `None`` if it doesn't exist
    pub async fn mget<K>(&mut self, keys: &[K]) -> Result<Vec<Option<Vec<u8>>>>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("MGET").args(&keys);
        let result = self.run_command(command).await?.unwrap_array();
        let output: Vec<Option<Vec<u8>>> =
            result.into_iter().map(|r| r.optional_string()).collect();
        Ok(output)
    }

    ///Set multiple keys at once. If the number of keys and values are not equal, set all keys that have values and vice versa.
    pub async fn mset<K, V>(&mut self, keys: &[K], values: &[V]) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let args: Vec<&[u8]> = keys
            .iter()
            .zip(values.iter())
            .flat_map(|(key, value)| vec![key.as_ref(), value.as_ref()].into_iter())
            .collect();
        let command = Command::new("MSET").args(&args);
        self.run_command(command).await?;
        Ok(())
    }

    ///Returns true if a key has been previously set.
    pub async fn exists<K>(&mut self, key: K) -> Result<bool>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("EXISTS").arg(&key);
        Ok(self.run_command(command).await? == Value::Integer(1))
    }
}
