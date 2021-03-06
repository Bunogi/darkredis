use crate::{Command, Connection, Result};
use futures::stream::StreamExt;
use std::sync::Arc;

#[cfg(feature = "runtime_async_std")]
use async_std::sync::{Mutex, MutexGuard};
#[cfg(feature = "runtime_tokio")]
use tokio::sync::{Mutex, MutexGuard};

///A connection pool. Clones are cheap and is the expected way to send the pool around your application.
#[derive(Clone, Debug)]
pub struct ConnectionPool {
    connections: Vec<Arc<Mutex<Connection>>>,
    address: Arc<String>,
    password: Option<Arc<String>>,
    name: Arc<String>,
}

impl ConnectionPool {
    ///Create a new connection pool for `address`, with `connection_count` connections. All connections
    ///are created in this function, and depending on the amount of connections desired, can therefore
    ///take some time to complete. By default, connections will be created with the name `darkredis-n`,
    ///where n represents the connection number.
    ///# Panics
    ///Will panic if the number of connections is equal to zero.
    pub async fn create(
        address: String,
        password: Option<&str>,
        connection_count: usize,
    ) -> Result<Self> {
        Self::create_with_name("darkredis", address, password, connection_count).await
    }

    ///Create a connection pool, but name each connection by `name`. Useful if you are running multiple services on a single Redis instance.
    ///# Panics
    ///Will panic if the number of connections is equal to zero.
    pub async fn create_with_name(
        name: &str,
        address: String,
        password: Option<&str>,
        connection_count: usize,
    ) -> Result<Self> {
        assert!(connection_count > 0);
        let connections = Vec::new();
        let mut out = Self {
            connections,
            name: Arc::new(name.to_string()),
            password: password.map(|s| Arc::new(s.to_string())),
            address: Arc::new(address),
        };

        for i in 0..connection_count {
            let mut conn = if let Some(p) = password {
                Connection::connect_and_auth(out.address.as_ref(), p).await?
            } else {
                Connection::connect(out.address.as_ref()).await?
            };
            let client_name = format!("{}-{}", name, i + 1);
            conn.run_command(Command::new("CLIENT").arg(b"SETNAME").arg(&client_name))
                .await?;
            out.connections.push(Arc::new(Mutex::new(conn)));
        }

        Ok(out)
    }

    ///Get an available connection from the pool, or wait for one to become available if none are
    ///available.
    pub async fn get(&self) -> MutexGuard<'_, Connection> {
        for conn in self.connections.iter() {
            #[cfg(feature = "runtime_tokio")]
            {
                if let Ok(lock) = conn.try_lock() {
                    return lock;
                }
            }
            #[cfg(feature = "runtime_async_std")]
            {
                if let Some(lock) = conn.try_lock() {
                    return lock;
                }
            }
        }

        //No free connections found, get the first available one
        let mut lockers: futures::stream::FuturesUnordered<_> =
            self.connections.iter().map(|l| l.lock()).collect();
        lockers.next().await.unwrap()
    }

    ///Create a new, owned connection using the settings of the current pool. Useful for subscribers or blocking operations that may not yield a value for a long time.
    pub async fn spawn<'a, N>(&'a self, name: N) -> Result<Connection>
    where
        N: Into<Option<&'a str>>,
    {
        let mut out = if let Some(p) = &self.password {
            Connection::connect_and_auth(self.address.as_ref(), p.as_bytes()).await?
        } else {
            Connection::connect(self.address.as_ref()).await?
        };
        let name = name.into().unwrap_or("spawned_connection");
        let name = format!("{}-{}", self.name, name);
        let command = Command::new("CLIENT").arg(&"SETNAME").arg(&name);
        out.run_command(command).await?;
        Ok(out)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Value;
    #[cfg_attr(feature = "runtime_tokio", tokio::test)]
    #[cfg_attr(feature = "runtime_async_std", async_std::test)]
    async fn pooling() {
        let connections = 4; //Arbitrary number, must be bigger than 1
        let pool = ConnectionPool::create(crate::test::TEST_ADDRESS.into(), None, connections)
            .await
            .unwrap();
        let mut locks = Vec::with_capacity(connections);
        for i in 0..connections - 1 {
            let mut conn = pool.get().await;
            let command = Command::new("CLIENT").arg(b"GETNAME");
            //If we keep getting the next connection in the queue, the connection pooling is functional
            assert_eq!(
                conn.run_command(command).await.unwrap(),
                Value::String(format!("darkredis-{}", i + 1).into_bytes())
            );
            locks.push(conn);
        }
    }

    #[cfg_attr(feature = "runtime_tokio", tokio::test)]
    #[cfg_attr(feature = "runtime_async_std", async_std::test)]
    async fn spawning() {
        let pool = ConnectionPool::create(crate::test::TEST_ADDRESS.into(), None, 1)
            .await
            .unwrap();

        let mut conn = pool.spawn(Some("named")).await.unwrap();
        let command = Command::new("CLIENT").arg(b"GETNAME");
        assert_eq!(
            conn.run_command(command).await.unwrap(),
            Value::String("darkredis-named".to_string().into_bytes())
        );
    }

    #[cfg_attr(feature = "runtime_tokio", tokio::test)]
    #[cfg_attr(feature = "runtime_async_std", async_std::test)]
    async fn timeout() {
        let pool = ConnectionPool::create(crate::test::TEST_ADDRESS.into(), None, 1)
            .await
            .unwrap();

        let mut _conn = pool.get().await;

        #[cfg(feature = "runtime_tokio")]
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), pool.get())
                .await
                .is_err()
        );
        #[cfg(feature = "runtime_async_std")]
        assert!(
            async_std::future::timeout(std::time::Duration::from_millis(100), pool.get())
                .await
                .is_err()
        );
    }
}
