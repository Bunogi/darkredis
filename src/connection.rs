use crate::{Command, CommandList, Error, Result, Value};
use futures::{lock::Mutex, prelude::*};
use runtime::net::TcpStream;
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
///not create a new connection. Use a [`Pool`](crate::ConnectionPool) if you want to use pooled conections.
#[derive(Clone)]
pub struct Connection {
    stream: Arc<Mutex<TcpStream>>,
}

impl Connection {
    ///Connect to a Redis instance running at `address`.
    pub async fn connect<A>(address: A) -> Result<Self>
    where
        A: net::ToSocketAddrs,
    {
        let stream = Arc::new(Mutex::new(
            TcpStream::connect(address)
                .await
                .map_err(Error::ConnectionFailed)?,
        ));
        Ok(Self { stream })
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
            b'-' => {
                //TODO: find a way to do this without copying
                Err(Error::RedisError(
                    String::from_utf8_lossy(&buf[1..]).to_string(),
                ))
            }
            b':' => {
                //TODO: find a way to do this without copying
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
            let mut buf = vec![0u8; num + 2]; // add two to catch the final \r\n from redis
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
    pub async fn run_commands(&mut self, command: CommandList) -> Result<Vec<Value>> {
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
    pub async fn run_command(&mut self, command: Command) -> Result<Value> {
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
        let command = Command::new("SET").arg(key.as_ref()).arg(data.as_ref());

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
        let command = Command::new("SET")
            .arg(key.as_ref())
            .arg(data.as_ref())
            .arg(b"EX")
            .arg(expiry.as_secs().to_string().as_bytes());

        self.run_command(command).await.map(|_| ())
    }

    ///Convenience function for the Redis command DEL
    pub async fn del<K>(&mut self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("DEL").arg(key.as_ref());
        self.run_command(command).await.map(|_| ())
    }

    ///Convenience function for the Redis command GET
    pub async fn get<D>(&mut self, key: D) -> Result<Option<Vec<u8>>>
    where
        D: AsRef<[u8]>,
    {
        let command = Command::new("GET").arg(key.as_ref());

        Ok(self.run_command(command).await?.optional_string())
    }

    ///Convenience function for the Redis command LPUSH
    pub async fn lpush<K, D>(&mut self, key: K, data: D) -> Result<isize>
    where
        K: AsRef<[u8]>,
        D: AsRef<[u8]>,
    {
        let command = Command::new("LPUSH").arg(key.as_ref()).arg(data.as_ref());

        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Convenience function for the Redis command RPUSH
    pub async fn rpush<K, D>(&mut self, key: K, data: D) -> Result<isize>
    where
        K: AsRef<[u8]>,
        D: AsRef<[u8]>,
    {
        let command = Command::new("RPUSH").arg(key.as_ref()).arg(data.as_ref());

        Ok(self.run_command(command).await?.unwrap_integer())
    }

    ///Convenience function for the Redis command LPOP
    pub async fn lpop<K, D>(&mut self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("LPOP").arg(key.as_ref());

        Ok(self.run_command(command).await?.optional_string())
    }

    ///Convenience function for the Redis command RPOP
    pub async fn rpop<K, D>(&mut self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("RPOP").arg(key.as_ref());

        Ok(self.run_command(command).await?.optional_string())
    }

    ///Convenience function for the Redis command LRANGE
    pub async fn lrange<K>(
        &mut self,
        key: K,
        from: isize,
        to: isize,
    ) -> Result<Option<Vec<Vec<u8>>>>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("LRANGE")
            .arg(key.as_ref())
            .arg(&from.to_string())
            .arg(&to.to_string());

        match self.run_command(command).await? {
            Value::Array(a) => Ok(Some(a.into_iter().map(|e| e.unwrap_string()).collect())),
            Value::Nil => Ok(None),
            _ => unreachable!(),
        }
    }

    ///Convenience function for the Redis command LLEN
    pub async fn llen<K>(&mut self, key: K) -> Result<Option<isize>>
    where
        K: AsRef<[u8]>,
    {
        let command = Command::new("LLEN").arg(key.as_ref());
        Ok(self.run_command(command).await?.optional_integer())
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
}
