#![warn(missing_docs)]

//! Adapter crate for using Darkredis with Deadpool.

use async_trait::async_trait;
use darkredis::{Command, Connection, Error, ToSocketAddrs};

///The connection pool type for Darkredis. See the Deadpool documentation for more information.
pub type Pool = deadpool::Pool<Connection, Error>;

///The struct which manages the state necesarry for creating and re-using connections.
pub struct Manager<A: ToSocketAddrs + Send + Sync> {
    name: Option<String>,
    password: Option<Vec<u8>>,
    address: A,
}

impl<A: ToSocketAddrs + Send + Sync> Manager<A> {
    ///Create a new `Manager` which connects to Redis at `address`. When a connection is created, it
    ///will automatically set the Redis client name to `name`.
    pub fn with_name<'a, P>(name: &str, address: A, password: Option<P>) -> Self
    where
        P: AsRef<[u8]>,
    {
        let password = password.map(|v| v.as_ref().to_vec());
        Self {
            name: Some(name.to_string()),
            password,
            address,
        }
    }

    ///Create a new `Manager` which connects to redis at `address`. This one will not set the Redis
    ///client name.
    pub fn new<P>(address: A, password: Option<P>) -> Self
    where
        P: AsRef<[u8]>,
    {
        let password = password.map(|v| v.as_ref().to_vec());
        Self {
            name: None,
            password,
            address,
        }
    }
}

#[async_trait]
impl<A: ToSocketAddrs + Send + Sync> deadpool::Manager<Connection, Error> for Manager<A> {
    async fn create(&self) -> Result<Connection, Error> {
        let mut conn = Connection::connect(&self.address, self.password.as_ref()).await?;
        if let Some(ref name) = self.name {
            conn.run_command(Command::new("CLIENT").arg(b"SETNAME").arg(&name))
                .await?;
        }
        Ok(conn)
    }

    async fn recycle(&self, conn: &mut Connection) -> deadpool::RecycleResult<Error> {
        match conn.ping().await {
            Ok(()) => Ok(()),
            Err(e) => Err(deadpool::RecycleError::Backend(e)),
        }
    }
}
