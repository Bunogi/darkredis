use darkredis::ConnectionPool;

//In your own code, you'd use simply #[tokio::main] or #[async_std::main]
#[cfg_attr(feature = "runtime_tokio", tokio::main)]
#[cfg_attr(feature = "runtime_async_std", async_std::main)]
async fn main() -> darkredis::Result<()> {
    let pool = ConnectionPool::create("127.0.0.1:6379".into(), None, num_cpus::get()).await?;
    let mut conn = pool.get().await;

    //And away!
    conn.set("secret_entrance", b"behind the bookshelf").await?;
    let secret_entrance = conn.get("secret_entrance").await?;
    assert_eq!(secret_entrance, Some("behind the bookshelf".into()));

    //Keep our secrets
    conn.del("secret_entrance").await?;

    Ok(())
}
