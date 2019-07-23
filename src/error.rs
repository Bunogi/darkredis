quick_error! {
    ///General error.
    #[derive(Debug)]
    pub enum Error {
        ///An io error occured.
        Io(err: std::io::Error) {
            from()
        }
        ///Failed to connect to Redis.
        ConnectionFailed(err: std::io::Error) {}
        ///Received an invalid or unexpected response. If you receive this error, it is probably a bug in redis-async.
        UnexpectedResponse(got: String) {
            display("Unexpected Redis response \"{}\"", got)
        }
        ///Command returned an error from Redis.
        RedisError(err: String) {
            display("Redis replied with error: {}", err)
        }
    }
}

// quick_error! {
//     #[derive(Debug)]
//     pub enum ConversionError {
//         WrongType(expected: String, received: String) {
//             display("Expected type '{}' but found '{}", expected, received)
//         }
//     }
// }
