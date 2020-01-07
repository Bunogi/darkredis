#![cfg_attr(not(feature = "bench"), warn(missing_docs))]
#![warn(missing_debug_implementations)]
#![deny(unsafe_code)]

//! Asyncronous redis client built using futures and async await, with optional connection pooling.
//! ```
//! use darkredis::*;
//!
//! # #[tokio::main]
//! # async fn main() {
//! // Create a connection pool with 4 connections
//! let pool = ConnectionPool::create("127.0.0.1:6379".into(), None, 4).await.unwrap();
//! let mut connection = pool.get().await; // Grab a connection from the pool
//!
//! connection.set("some-key", "Hello, world!").await.unwrap();
//! assert_eq!(connection.get("some-key").await.unwrap(), Some("Hello, world!".into()));
//! # connection.del("some-key").await.unwrap();
//! # }
//! ```

#[cfg(all(feature = "runtime_tokio", feature = "runtime_async_std"))]
compile_error!("The `runtime_tokio` and `runtime_async_std` features are mutually exclusive!");

#[cfg(not(any(feature = "runtime_tokio", feature = "runtime_async_std")))]
compile_error!("Expected one of the features `runtime_tokio` and `runtime_async_std`");

#[macro_use]
extern crate quick_error;

mod command;
mod connection;
mod connectionpool;
mod error;

///Export the ToSocketAddrs trait to be used for deadpool-darkredis. You probably won't need this unless you're implementing an adapter crate for a different connection pool.
#[cfg(feature = "runtime_async_std")]
pub use async_std::net::ToSocketAddrs;
#[cfg(feature = "runtime_tokio")]
pub use tokio::net::ToSocketAddrs;

#[cfg(feature = "bench")]
pub mod test;

#[cfg(all(not(feature = "bench"), test))]
mod test;

pub use command::{Command, CommandList};
pub use connection::{
    builder::MSetBuilder, Connection, Message, MessageStream, PMessage, PMessageStream,
    ResponseStream,
};
pub use connectionpool::ConnectionPool;
pub use error::Error;

///Result type used in the whole crate.
pub type Result<T> = std::result::Result<T, Error>;

///Enum depicting the various possible responses one can get from Redis.
#[derive(Debug, PartialEq)]
pub enum Value {
    ///A Redis `OK` response.
    Ok,
    ///Nil Response.
    Nil,
    ///Array response.
    Array(Vec<Value>),
    ///Integer response.
    Integer(isize),
    ///String response. This cannot be a `String` type, because Redis strings need not be valid UTF-8, unlike Rust.
    String(Vec<u8>),
}

impl Value {
    ///Returns the inner `isize` of a [`Value::Integer`](enum.Value.html#Integer.v).
    ///# Panics
    ///Panics if `self` is not a [`Value::Integer`](enum.Value.html#Integer.v)
    #[inline]
    pub fn unwrap_integer(self) -> isize {
        if let Value::Integer(i) = self {
            i
        } else {
            panic!("expected integer value, got {:?}", self)
        }
    }

    ///Returns the inner `Vec<Value>` of a `Value::Array`.
    ///# Panics
    ///Panics if `self` is not a [`Value::Array`](enum.Value.html#Array.v)
    #[inline]
    pub fn unwrap_array(self) -> Vec<Value> {
        if let Value::Array(a) = self {
            a
        } else {
            panic!("expected array value, got {:?}", self)
        }
    }

    ///Returns the inner `Vec<u8>` of a [`Value::String`](enum.Value.html#String.v).
    ///# Panics
    ///Panics if `self` is not a [`Value::String`](enum.Value.html#String.v)
    #[inline]
    pub fn unwrap_string(self) -> Vec<u8> {
        if let Value::String(s) = self {
            s
        } else {
            panic!("expected string value, got {:?}", self)
        }
    }

    ///Returns `true` if `self` is nonzero.
    ///# Panics
    ///Panics if `self is not a [`Value::Integer`](enum.Value.html#Integer.v)
    #[inline]
    pub fn unwrap_bool(self) -> bool {
        self.unwrap_integer() != 0
    }

    ///Returns `self` as a vector of Redis strings.
    ///# Panics
    ///Panics if `self` is not a [`Value::Array`](enum.Value.html#Array.v) or not all the elements are strings.
    #[inline]
    pub fn unwrap_string_array(self) -> Vec<Vec<u8>> {
        self.unwrap_array()
            .into_iter()
            .map(|v| v.unwrap_string())
            .collect()
    }

    ///Like `unwrap_string`, but returns an `Option` instead of panicking.
    #[inline]
    pub fn optional_string(self) -> Option<Vec<u8>> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    ///Like `unwrap_array`, but returns an `Option` instead of panicking.
    #[inline]
    pub fn optional_array(self) -> Option<Vec<Value>> {
        match self {
            Value::Array(a) => Some(a),
            _ => None,
        }
    }

    ///Like `unwrap_integer`, but returns an `Option` instead of panicking.
    #[inline]
    pub fn optional_integer(self) -> Option<isize> {
        match self {
            Value::Integer(i) => Some(i),
            _ => None,
        }
    }

    ///Like `unwrap_bool`, but returns an `Option` instead of panicking.
    #[inline]
    pub fn optional_bool(self) -> Option<bool> {
        self.optional_integer().map(|i| i != 0)
    }
}
