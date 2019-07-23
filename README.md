# redis-async : A Redis client written based on `std::future` and async await
`redis-async` is a Redis client for Rust written using the new `std::future` and `async` await. Currently nightly only, the library tries to be ergonomic and easy to use.

## Why?
There are other Redis clients out there for Rust, but none of them allow you to easily write `await` in your code. `redis-rs` is a good client for sure, but it's async module leaves is based on `futures 0.1`. That's good, but porting it to `std::future` seemed like quite a task. Instead, I ripped my custom-written Redis client from an async project of mine. The result of this is `redis-async`, and I hope it will be useful to you, even if only to experiment with async and await.

# Getting started
- Add `redis-async` and `runtime` to your `Cargo.toml`.
- Use the runtime crate to bootstrap an async context.
- Create a `Pool` and use it to grab connections from a pool

```rust
#![feature(async_await)]

use redis_async::Pool;

#[runtime::main]
async fn main() -> redis_async::Result<()> {
    let pool = Pool::create("127.0.0.1:6379".into(), num_cpus::get()).await?;
    let mut conn = pool.get().await;

    //And away!
    conn.set("secret_entrance", b"behind the bookshelf").await?;
    let secret_entrance = conn.get("secret_entrance").await?;
    assert_eq!(secret_entrance, Some("behind the bookshelf".into()));

    //Keep our secrets
    conn.del("secret_entrance").await?;

    Ok(())
}
```
