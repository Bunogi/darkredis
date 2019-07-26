#![feature(async_await)]

use darkredis::ConnectionPool;

#[runtime::main]
async fn main() -> darkredis::Result<()> {
    let pool = ConnectionPool::create("127.0.0.1:6379".into(), num_cpus::get()).await?;
    let mut conn = pool.get().await;

    //And away!
    conn.set("secret_entrance", b"behind the bookshelf").await?;
    let secret_entrance = conn.get("secret_entrance").await?;
    assert_eq!(secret_entrance, Some("behind the bookshelf".into()));

    //Keep our secrets
    conn.del("secret_entrance").await?;

    Ok(())
}
