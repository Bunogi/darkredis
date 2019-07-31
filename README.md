# darkredis : A Redis client based on `std::future` and async await
[![Documentation](https://docs.rs/darkredis/badge.svg)](https://docs.rs/darkredis) [![Build Status](https://travis-ci.org/Bunogi/darkredis.svg?branch=master)](https://travis-ci.org/Bunogi/darkredis) [![Crates.io Status](https://img.shields.io/crates/v/darkredis.svg)](https://crates.io/crates/darkredis)


`darkredis` is a Redis client for Rust written using the new `std::future` and `async` await. Currently nightly only, the library tries to be ergonomic and easy to use.

Currently not all Redis commands have convenience functions, and there may be ergonomic improvements to make.

## Why?
There are other Redis clients out there for Rust, but none of them allow you to easily write `await` in your code. `redis-rs` is a good client for sure, but it's async module leaves is based on `futures 0.1`. Therefore, I ripped my custom-written Redis client from an async project of mine. The result of this is `darkredis`, and I hope it will be useful to you, even if only to experiment with async and await.

# Getting started
- Add `darkredis` and `runtime` to your `Cargo.toml`.
- Use the runtime crate to bootstrap an async context.
- Create a `ConnectionPool` and grab a connection!

```rust
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
```

# Testing
If you're hacking on `darkredis` and want to run the tests, make sure you have a Redis instance running on your local machine on port 6379. The tests clean up any keys set by themselves, unless the test fails. Please submit an issue if it does not.
