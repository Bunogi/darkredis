# deadpool-darkredis : A `deadpool` adapter crate for `darkredis`
[![Documentation](https://docs.rs/deadpool-darkredis/badge.svg)](https://docs.rs/deadpool-darkredis) [![Build Status](https://travis-ci.org/Bunogi/deadpool-darkredis.svg?branch=master)](https://travis-ci.org/Bunogi/darkredis) [![Crates.io Status](https://img.shields.io/crates/v/deadpool-darkredis.svg)](https://crates.io/crates/deadpool-darkredis) [![Join the chat at https://gitter.im/dotnet/coreclr](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/darkredis/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

This crate contains the code necesarry to use [`darkredis`](crates.io/crates/darkredis) with [`deadpool`](https://crates.io/crates/deadpool).

# NOTE
Trying to use `deadpool-darkredis` with the `runtime_agnostic` feature will result in a compile error, due to it's version of `ToSocketAddrs` missing a `Send` implementation.
