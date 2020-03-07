quick_error! {
    ///The `darkredis` error type.
    #[derive(Debug)]
    pub enum Error {
        ///An io error occured trying to write or read from a TCP socket.
        Io(err: std::io::Error) {
            from()
        }
        ///Failed to connect to Redis.
        ConnectionFailed(err: std::io::Error) {}
        ///Received an invalid or unexpected response. If you receive this error, it is probably a bug in darkredis.
        UnexpectedResponse(got: String) {
            display("Unexpected Redis response \"{}\"", got)
        }
        ///Command returned an error from Redis.
        RedisError(err: String) {
            display("Redis replied with error: {}", err)
        }
        ///A command taking a slice was given an empty slice.
        EmptySlice {
            display("A command expecting a slice of values received an empty slice")
        }
    }
}
