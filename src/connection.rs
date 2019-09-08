use crate::{Command, CommandList, Error, Result, Value};
use async_std::net::TcpStream;
use futures::{lock::Mutex, prelude::*};
use std::io;
use std::net;
use std::sync::Arc;
use std::time;

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
    stream: Arc<Mutex<TcpStream>>,
}

impl Connection {
    ///Connect to a Redis instance running at `address`. If you wish to name this connection, run the [`CLIENT SETNAME`](https://redis.io/commands/client-setname) command.
    pub async fn connect<A>(address: A, password: Option<&str>) -> Result<Self>
    where
        A: net::ToSocketAddrs,
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

    //Read one value from the stream using the parse_* utility functions
    async fn read_value(mut stream: &mut TcpStream) -> Result<Value> {
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

    ///Run a series of commands on this connection
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

    ///Run a single command on this connection
    pub async fn run_command(&mut self, command: Command<'_>) -> Result<Value> {
        let mut stream = self.stream.lock().await;
        let serialized: Vec<u8> = command.serialize();
        stream.write_all(&serialized).await?;

        Ok(Self::read_value(&mut stream).await?)
    }

    ///Convenience function for the Redis command SET
    pub async fn set<K, D>(&mut self, key: K, data: D) -> Result<()>
    where
        K: AsRef<[u8]>,
        D: AsRef<[u8]>,
    {
        let command = Command::new("SET").arg(&key).arg(&data);

        self.run_command(command).await.map(|_| ())
    }

    ///Convenience function for the Redis command SET, with an expiry time.
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

    ///Convenience function for the Redis command DEL
    pub async fn del<K>(&mut self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("DEL").arg(&key);
        self.run_command(command).await.map(|_| ())
    }

    ///Convenience function for the Redis command GET
    pub async fn get<D>(&mut self, key: D) -> Result<Option<Vec<u8>>>
    where
        D: AsRef<[u8]>,
    {
        let command = Command::new("GET").arg(&key);

        Ok(self.run_command(command).await?.optional_string())
    }

    ///Convenience function for the Redis command LPUSH
    pub async fn lpush<K, D>(&mut self, key: K, data: D) -> Result<isize>
    where
        K: AsRef<[u8]>,
        D: AsRef<[u8]>,
    {
        let command = Command::new("LPUSH").arg(&key).arg(&data);

        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Like lpush, but push multiple values through a slice
    pub async fn lpush_slice<K, D>(&mut self, key: K, data: &[D]) -> Result<isize>
    where
        K: AsRef<[u8]>,
        D: AsRef<[u8]>,
    {
        let command = Command::new("LPUSH").arg(&key).args(data);

        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Convenience function for the Redis command RPUSH
    pub async fn rpush<K, D>(&mut self, key: K, data: D) -> Result<isize>
    where
        K: AsRef<[u8]>,
        D: AsRef<[u8]>,
    {
        let command = Command::new("RPUSH").arg(&key).arg(&data);

        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Like rpush, but push multiple values through a slice
    pub async fn rpush_slice<K, D>(&mut self, key: K, data: &[D]) -> Result<isize>
    where
        K: AsRef<[u8]>,
        D: AsRef<[u8]>,
    {
        let command = Command::new("RPUSH").arg(&key).args(data);

        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Convenience function for the Redis command LPOP
    pub async fn lpop<K>(&mut self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("LPOP").arg(&key);

        Ok(self.run_command(command).await?.optional_string())
    }

    ///Convenience function for the Redis command RPOP
    pub async fn rpop<K>(&mut self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("RPOP").arg(&key);

        Ok(self.run_command(command).await?.optional_string())
    }

    ///Convenience function for the Redis command LRANGE
    pub async fn lrange<K>(&mut self, key: K, from: isize, to: isize) -> Result<Vec<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let from = from.to_string();
        let to = to.to_string();
        let command = Command::new("LRANGE").arg(&key).arg(&from).arg(&to);

        Ok(self
            .run_command(command)
            .await?
            .unwrap_array()
            .into_iter()
            .map(|s| s.unwrap_string())
            .collect())
    }

    ///Convenience function for the Redis command LLEN
    pub async fn llen<K>(&mut self, key: K) -> Result<Option<isize>>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("LLEN").arg(&key);
        Ok(self.run_command(command).await?.optional_integer())
    }

    ///Convenience function for LSET.
    pub async fn lset<K, D>(&mut self, key: K, index: usize, value: D) -> Result<()>
    where
        K: AsRef<[u8]>,
        D: AsRef<[u8]>,
    {
        let index = index.to_string();
        let command = Command::new("LSET").arg(&key).arg(&index).arg(&value);

        self.run_command(command).await?;
        Ok(())
    }

    ///Convenience function for LTRIM
    pub async fn ltrim<K>(&mut self, key: K, start: usize, stop: usize) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let start = start.to_string();
        let stop = stop.to_string();
        let command = Command::new("LTRIM").arg(&key).arg(&start).arg(&stop);
        self.run_command(command).await?;

        Ok(())
    }

    ///Convenience function for INCR
    pub async fn incr<K>(&mut self, key: K) -> Result<isize>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("INCR").arg(&key);
        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Convenience function for INCRBY
    pub async fn incrby<K>(&mut self, key: K, val: isize) -> Result<isize>
    where
        K: AsRef<[u8]>,
    {
        let val = val.to_string();
        let command = Command::new("INCRBY").arg(&key).arg(&val);
        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Convenience function for INCRBYFLOAT
    pub async fn incrbyfloat<K>(&mut self, key: K, val: f64) -> Result<f64>
    where
        K: AsRef<[u8]>,
    {
        let val = val.to_string();
        let command = Command::new("INCRBYFLOAT").arg(&key).arg(&val);
        let result = self.run_command(command).await?.unwrap_string();
        Ok(String::from_utf8_lossy(&result).parse::<f64>().unwrap())
    }

    ///Convenience function for DECR
    pub async fn decr<K>(&mut self, key: K) -> Result<isize>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("DECR").arg(&key);
        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Convenience function for DECRBY
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
    where K:AsRef<[u8]>
    {
        let command = Command::new("EXISTS").arg(&key);
        Ok(self.run_command(command).await? == Value::Integer(1))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{redis_test, test::*, Command};
    #[runtime::test]
    async fn parse_nil() {
        redis_test!(
            redis,
            {
                let command = Command::new("GET").arg(&null_key);

                assert_eq!(redis.run_command(command).await.unwrap(), Value::Nil);
            },
            null_key
        );
    }
    #[runtime::test]
    async fn parse_ok() {
        redis_test!(
            redis,
            {
                let command = Command::new("SET").arg(&some_key).arg(b"");

                assert_eq!(redis.run_command(command).await.unwrap(), Value::Ok);
            },
            some_key
        );
    }
    #[runtime::test]
    async fn pipelined_commands() {
        redis_test!(
            redis,
            {
                let command = CommandList::new("SET")
                    .arg(&simple_key)
                    .arg(b"")
                    .command("LPUSH")
                    .arg(&list_key)
                    .arg(b"")
                    .command("LPUSH")
                    .arg(&list_key)
                    .arg(b"");

                assert_eq!(
                    redis.run_commands(command).await.unwrap(),
                    vec![Value::Ok, Value::Integer(1), Value::Integer(2)]
                );
            },
            simple_key,
            list_key
        );
    }

    #[runtime::test]
    async fn get_set() {
        redis_test!(
            redis,
            {
                redis.set(&key, "foo").await.unwrap();
                assert_eq!(redis.get(&key).await.unwrap(), Some("foo".into()));
            },
            key
        );
    }

    #[runtime::test]
    async fn list_convenience() {
        redis_test!(
            redis,
            {
                redis.rpush_slice(&list_key, &["1", "2"]).await.unwrap();
                redis.lpush(&list_key, "0").await.unwrap();

                let expected: Vec<Vec<u8>> = vec![b"0", b"1", b"2"]
                    .into_iter()
                    .map(|s| s.to_vec())
                    .collect();
                assert_eq!(redis.lrange(&list_key, 0, 3).await.unwrap(), expected);
                assert_eq!(redis.lpop(&list_key).await.unwrap(), Some(b"0".to_vec()));
                assert_eq!(redis.rpop(&list_key).await.unwrap(), Some(b"2".to_vec()));
                assert_eq!(redis.llen(&list_key).await.unwrap(), Some(1));

                let long_list: Vec<String> =
                    std::iter::repeat("value".to_string()).take(10).collect();
                redis.lpush_slice(&list_key, &long_list).await.unwrap();
                redis.ltrim(&list_key, 0, 4).await.unwrap();
                redis.lset(&list_key, 0, b"hello").await.unwrap();
                assert_eq!(redis.llen(&list_key).await.unwrap(), Some(5));
                assert_eq!(redis.lrange(&list_key, 0, 0).await.unwrap(), vec![b"hello"]);
            },
            list_key
        );
    }

    #[runtime::test]
    async fn incr_decr() {
        redis_test!(
            redis,
            {
                assert_eq!(redis.incr(&int_key).await.unwrap(), 1);
                assert_eq!(redis.incrby(&int_key, 41).await.unwrap(), 42);
                assert_eq!(redis.decr(&int_key).await.unwrap(), 41);
                assert_eq!(redis.decrby(&int_key, 20).await.unwrap(), 21);
                assert_eq!(redis.get(&int_key).await.unwrap(), Some(b"21".to_vec()));

                assert_eq!(redis.incrbyfloat(&float_key, 8.0).await.unwrap(), 8.0);
                assert_eq!(redis.incrbyfloat(&float_key, -4.0).await.unwrap(), 4.0);
            },
            int_key,
            float_key
        );
    }

    #[runtime::test]
    async fn append() {
        redis_test!(
            redis,
            {
                assert_eq!(redis.append(&key, b"Hello, ").await.unwrap(), 7);
                assert_eq!(redis.append(&key, b"world!").await.unwrap(), 13);
                assert_eq!(
                    redis.get(&key).await.unwrap(),
                    Some(b"Hello, world!".to_vec())
                );
            },
            key
        );
    }

    #[runtime::test]
    async fn mget_mset() {
        redis_test!(
            redis,
            {
                //Verify that it works with trait objects
                let keys: Vec<&(dyn AsRef<[u8]> + Sync)> = vec![&"key1", &b"key2"];
                let values: Vec<&(dyn AsRef<[u8]> + Sync)> = vec![&"value1", &b"value2"];
                redis.mset(&keys, &values).await.unwrap();

                let simple_keys = vec!["key1", "key2", "key3"];
                let simple_values = vec!["value1", "value2"];
                redis
                    .mset(simple_keys.as_slice(), simple_values.as_slice())
                    .await
                    .unwrap();

                assert_eq!(redis.mget(&simple_keys).await.unwrap(), vec![Some(b"value1".to_vec()), Some(b"value2".to_vec()), None]);

                //clean up
                redis.del("key1").await.unwrap();
                redis.del("key2").await.unwrap();
                redis.del("key3").await.unwrap();
            },
            key
        );
    }

    #[runtime::test]
    async fn exists() {
        redis_test!(
            redis,
            {
                assert_eq!(redis.exists(&key).await.unwrap(), false);
                redis.set(&key, "foo").await.unwrap();
                assert_eq!(redis.exists(&key).await.unwrap(), true);
            },
            key
        );
    }
}
