///The address to connect to when testing
pub const TEST_ADDRESS: &str = "127.0.0.1:6379";

lazy_static::lazy_static! {
    pub static ref CONNECTION_POOL: crate::ConnectionPool = futures::executor::block_on(
        crate::ConnectionPool::create(TEST_ADDRESS.into(), num_cpus::get())
    ).unwrap();
}

#[macro_export]
///Clean up any keys given as identifiers.
macro_rules! cleanup_keys {
    ($conn:ident, $first_key:expr, $( $key:expr ),* ) => {
        let command = Command::new("DEL").arg(&$first_key)
            $(
                .arg(&$key)
            )*;

        (*$conn).run_command(command).await.unwrap();
    };
    ($conn:ident, $key:expr) => {
        let command = Command::new("DEL").arg(&$key);

        (*$conn).run_command(command).await.unwrap();
    }
}

#[macro_export]
///Creates a guaranteed unique key for use in tests, in order to prevent any collisions.
macro_rules! create_key {
    ($name:ident) => {
        format!(
            "redis-async.test.{}.{}.{}",
            file!(),
            line!(),
            stringify!($name)
        )
        .into_bytes()
    };
}

#[macro_export]
///Define a test which mutates the test Redis connection, clearing state before and after running to ensure consistent results. This also prevents leaving a mess in the testing Redis instance.
macro_rules! redis_test {
    ($redis:ident, $block:tt, $( $key:ident ),+) => {
        use crate::create_key;
        use crate::cleanup_keys;
        let mut $redis = CONNECTION_POOL.get().await;
        $(
            let $key: Vec<u8> = create_key!($key);
        )*
        cleanup_keys!($redis, $($key),*);
        $block
        cleanup_keys!($redis, $($key),*);
    }
}

#[macro_export]
///Defines a redis test but in a way that is appropriate for a doc test. Does NOT do housekeeping and therefore does not clean up after itself.
macro_rules! redis_doc_test {
    ($redis:ident, $block:tt) => {
        let mut $redis = CONNECTION_POOL.get().await;
        $block
    };
}
