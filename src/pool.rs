use crate::{Command, Connection, Result};
use futures::lock::{Mutex, MutexGuard};
use std::sync::Arc;

///A connection pool. Clones are cheap and is the expected way to send the pool around te application.
#[derive(Clone)]
pub struct ConnectionPool {
    connections: Vec<Arc<Mutex<Connection>>>,
    address: Arc<String>,
}

impl ConnectionPool {
    ///Create a new connection pool for `address`, with `connection_count` connections. All connections
    ///are created in this function, and depending on the amount of connections desired, can therefore
    ///take some time to complete.
    pub async fn create(address: String, connection_count: usize) -> Result<Self> {
        let connections = Vec::new();
        let mut out = Self {
            connections,
            address: Arc::new(address),
        };

        for i in 0..connection_count {
            let mut conn = Connection::connect(out.address.as_ref()).await?;
            conn.run_command(
                Command::new("CLIENT")
                    .arg(b"SETNAME")
                    .arg(&format!("redis-async-{}", i + 1).into_bytes()),
            )
            .await?;
            out.connections.push(Arc::new(Mutex::new(conn)));
        }

        Ok(out)
    }

    ///Get an available connection from the pool, or wait for one to become available if none are
    ///available.
    pub async fn get(&self) -> MutexGuard<Connection> {
        for conn in self.connections.iter() {
            if let Some(lock) = conn.try_lock() {
                return lock;
            }
        }

        //No free connections found, get the first available one
        let lockers = self.connections.iter().map(|l| l.lock());
        futures::future::select_all(lockers).await.0
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Value;
    #[runtime::test]
    async fn pooling() {
        let connections = 4; //Arbitrary number, must be bigger than 1
        let pool = ConnectionPool::create(crate::test::TEST_ADDRESS.into(), connections)
            .await
            .unwrap();
        let mut locks = Vec::with_capacity(connections);
        for i in 0..connections - 1 {
            let mut conn = pool.get().await;
            let command = Command::new("CLIENT").arg(b"GETNAME");
            //If we keep getting the next connection in the queue, the connection pooling is functional
            assert_eq!(
                conn.run_command(command).await.unwrap(),
                Value::String(format!("redis-async-{}", i + 1).into_bytes())
            );
            locks.push(conn);
        }
    }
}
