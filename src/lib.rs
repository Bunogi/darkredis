#![cfg_attr(not(feature = "bench"), deny(missing_docs))]

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

#[cfg(all(feature = "runtime_tokio", feature = "runtime_agnostic"))]
compile_error!("The `runtime_tokio` and `runtime_agnostic` features are mutually exclusive!");

#[cfg(not(any(feature = "runtime_tokio", feature = "runtime_agnostic")))]
compile_error!("Expected one of the features `runtime_tokio` and `runtime_agnostic`");

#[macro_use]
extern crate quick_error;

mod command;
mod connection;
mod connectionpool;
mod error;

#[cfg(feature = "bench")]
pub mod test;

#[cfg(all(not(feature = "bench"), test))]
mod test;

pub use command::{Command, CommandList};
pub use connection::{
    Connection, Message, MessageStream, PMessage, PMessageStream, ResponseStream,
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
    ///Returns the inner `isize` of a [`Value::Integer`](crate::Value::Integer).
    ///# Panics
    ///Panics if `self` is not a [`Value::Integer`](crate::Value::Integer)
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
    ///Panics if `self` is not a [`Value::Array`](crate::Value::Array)
    #[inline]
    pub fn unwrap_array(self) -> Vec<Value> {
        if let Value::Array(a) = self {
            a
        } else {
            panic!("expected array value, got {:?}", self)
        }
    }

    ///Returns the inner `Vec<u8>` of a [`Value::String`](crate::Value::String).
    ///# Panics
    ///Panics if `self` is not a [`Value::String`](crate::Value::String)
    #[inline]
    pub fn unwrap_string(self) -> Vec<u8> {
        if let Value::String(s) = self {
            s
        } else {
            panic!("expected string value, got {:?}", self)
        }
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
}
