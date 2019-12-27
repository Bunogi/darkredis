use crate::{Command, CommandList, Error, Result, Value};
use futures::lock::Mutex;

#[cfg(feature = "runtime_async_std")]
use async_std::{
    io,
    net::{TcpStream, ToSocketAddrs},
};
#[cfg(feature = "runtime_async_std")]
use futures::{AsyncReadExt, AsyncWriteExt};

#[cfg(feature = "runtime_tokio")]
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, ToSocketAddrs},
};

use std::{sync::Arc, time};

pub mod stream;
pub use stream::{Message, MessageStream, PMessage, PMessageStream, ResponseStream};

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
///Alternatively, there's the `deadpool-darkredis` crate.
///Every convenience function can work with any kind of data as long as it can be converted into bytes.
///Check the [redis command reference](https://redis.io/commands) for in-depth explanations of each command.
#[derive(Clone)]
pub struct Connection {
    pub(crate) stream: Arc<Mutex<TcpStream>>,
}

impl Connection {
    ///Connect to a Redis instance running at `address`. If you wish to name this connection, run the [`CLIENT SETNAME`](https://redis.io/commands/client-setname) command.
    pub async fn connect<A>(address: A) -> Result<Self>
    where
        A: ToSocketAddrs,
    {
        let stream = Arc::new(Mutex::new(
            TcpStream::connect(address)
                .await
                .map_err(Error::ConnectionFailed)?,
        ));

        Ok(Self { stream })
    }

    ///Connect to a Redis instance running at `address`, and authenticate using `password`.
    pub async fn connect_and_auth<A, P>(address: A, password: P) -> Result<Self>
    where
        A: ToSocketAddrs,
        P: AsRef<[u8]>,
    {
        let mut out = Self::connect(address).await?;
        out.run_command(Command::new("AUTH").arg(&password)).await?;

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
        let num_parsed = String::from_utf8_lossy(&start[1..])
            .trim()
            .parse::<i32>()
            .unwrap();

        // result can be negative (blpop/brpop return '-1' on timeout)
        if num_parsed < 0 {
            return Ok(Value::Nil);
        }

        let num = num_parsed as usize;
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

    ///Run a single command on this connection.
    pub async fn run_command(&mut self, command: Command<'_>) -> Result<Value> {
        let mut stream = self.stream.lock().await;
        let mut buffer = Vec::new();
        command.serialize(&mut buffer);
        stream.write_all(&buffer).await?;

        Ok(Self::read_value(&mut stream).await?)
    }

    ///Run a series of commands on this connection, returning a stream of the results.
    pub async fn run_commands(&mut self, command: CommandList<'_>) -> Result<ResponseStream> {
        let mut lock = self.stream.lock().await;
        let command_count = command.command_count();
        let buffer = command.serialize();
        lock.write_all(&buffer).await?;

        Ok(ResponseStream::new(command_count, self.stream.clone()))
    }

    ///Send a `PING` to the server, returning Ok(()) on success.
    pub async fn ping(&mut self) -> Result<()> {
        self.run_command(Command::new("PING")).await.map(|_| ())
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

    ///Exactly like [`subscribe`](Connection::subscribe), but subscribe to patterns instead.
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

    ///Publish `message` to `channel`. Returns how many clients received the message.
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
    ///# Deprecated
    ///`std::time::Duration` consists of nanoseconds and seconds, which don't map well to either `SET EX` nor `SET PX`.
    ///This function only used the seconds part of the Duration, which may be confusing.
    #[deprecated(
        since = "0.5.0",
        note = "This function will be removed in 0.6.0. Please use set_and_expire_seconds or set_and_expire_ms instead."
    )]
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

    ///Set the key `key` to `data`, and set it to expire after `seconds` seconds.
    pub async fn set_and_expire_seconds<K, D>(
        &mut self,
        key: K,
        data: D,
        seconds: u32,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        D: AsRef<[u8]>,
    {
        let seconds = seconds.to_string();
        let command = Command::new("SET")
            .arg(&key)
            .arg(&data)
            .arg(b"EX")
            .arg(&seconds);

        self.run_command(command).await.map(|_| ())
    }

    ///Set the key `key` to `data`, and set it to expire after `milliseconds` ms.
    pub async fn set_and_expire_ms<K, D>(
        &mut self,
        key: K,
        data: D,
        milliseconds: u32,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        D: AsRef<[u8]>,
    {
        let milliseconds = milliseconds.to_string();
        let command = Command::new("SET")
            .arg(&key)
            .arg(&data)
            .arg(b"PX")
            .arg(&milliseconds);

        self.run_command(command).await.map(|_| ())
    }

    ///Set `key` to expire `seconds` seconds from now.
    pub async fn expire_seconds<K>(&mut self, key: K, seconds: u32) -> Result<isize>
    where
        K: AsRef<[u8]>,
    {
        let seconds = seconds.to_string();
        let command = Command::new("EXPIRE").arg(&key).arg(&seconds);

        self.run_command(command).await.map(|i| i.unwrap_integer())
    }

    ///Set `key` to expire `milliseconds` ms from now.
    pub async fn expire_ms<K>(&mut self, key: K, seconds: u32) -> Result<isize>
    where
        K: AsRef<[u8]>,
    {
        let seconds = seconds.to_string();
        let command = Command::new("PEXPIRE").arg(&key).arg(&seconds);

        self.run_command(command).await.map(|i| i.unwrap_integer())
    }

    ///Set `key` to expire at unix timestamp `timestamp`, measured in seconds.
    pub async fn expire_at_seconds<K>(&mut self, key: K, timestamp: u64) -> Result<isize>
    where
        K: AsRef<[u8]>,
    {
        let timestamp = timestamp.to_string();
        let command = Command::new("EXPIREAT").arg(&key).arg(&timestamp);

        self.run_command(command).await.map(|i| i.unwrap_integer())
    }

    ///Set `key` to expire at unix timestamp `timestamp`, measured in milliseconds.
    pub async fn expire_at_ms<K>(&mut self, key: K, timestamp: u64) -> Result<isize>
    where
        K: AsRef<[u8]>,
    {
        let timestamp = timestamp.to_string();
        let command = Command::new("PEXPIREAT").arg(&key).arg(&timestamp);

        self.run_command(command).await.map(|i| i.unwrap_integer())
    }

    ///Delete `key`.
    ///# Return value
    ///The number of deleted keys
    pub async fn del<K>(&mut self, key: K) -> Result<isize>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("DEL").arg(&key);
        self.run_command(command).await.map(|i| i.unwrap_integer())
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

    ///Like [`lpush`](Connection::lpush), but push multiple values.
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

    ///Like [`rpush`](Connection::rpush), but push multiple values through a slice.
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

    ///Pop a value from one of the lists from the left side.
    ///Block timeout seconds when there are no values to pop (timeout=0 means infinite)
    ///# Return value
    ///* `Ok(Some((list,value)))`: name of the list and corresponding value
    ///* `Ok(None)`: timeout (no values)
    ///* `Err(err)`: there was an error
    pub async fn blpop<K>(&mut self, lists: &[K], timeout: u32) -> Result<Option<Vec<Vec<u8>>>>
    where
        K: AsRef<[u8]>,
    {
        self.blpop_brpop(lists, timeout, "BLPOP").await
    }

    ///Pop a value from one of the lists from the right side.
    ///Block timeout seconds when there are no values to pop (timeout=0 means infinite)
    ///# Return value
    ///* `Ok(Some((list,value)))`: name of the list and corresponding value
    ///* `Ok(None)`: timeout (no values)
    ///* `Err(err)`: there was an error
    pub async fn brpop<K>(&mut self, lists: &[K], timeout: u32) -> Result<Option<Vec<Vec<u8>>>>
    where
        K: AsRef<[u8]>,
    {
        self.blpop_brpop(lists, timeout, "BRPOP").await
    }

    ///blpop and brpop common code
    async fn blpop_brpop<K>(
        &mut self,
        lists: &[K],
        timeout: u32,
        redis_cmd: &str,
    ) -> Result<Option<Vec<Vec<u8>>>>
    where
        K: AsRef<[u8]>,
    {
        let timeout = timeout.to_string();
        let command = Command::new(redis_cmd).args(&lists).arg(&timeout);
        match self.run_command(command).await? {
            Value::Array(values) => {
                let vlen = values.len();
                if vlen == 2 {
                    return Ok(Some(
                        values.into_iter().map(|s| s.unwrap_string()).collect(),
                    ));
                }
                Err(Error::UnexpectedResponse(format!(
                    "{}: wrong number of elements received: {}",
                    redis_cmd, vlen
                )))
            }
            Value::Nil => Ok(None),
            other => Err(Error::UnexpectedResponse(format!(
                "{}: {:?}",
                redis_cmd, other
            ))),
        }
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
