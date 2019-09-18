use super::{Connection, Result, Value};
use futures::{task::Context, Future, FutureExt, Poll, Stream};
use std::pin::Pin;

///A message received from a channel.
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct Message {
    ///The channel the message was received on
    pub channel: Vec<u8>,
    ///The actual message data
    pub message: Vec<u8>,
}

///A message received from a channel (Pattern version)
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct PMessage {
    ///The channel the message was received on
    pub channel: Vec<u8>,
    ///The actual message data
    pub message: Vec<u8>,
    ///The pattern matched by `channel`.
    pub pattern: Vec<u8>,
}

struct ValueStream {
    conn: Connection,
    poll_future: Pin<Box<dyn Future<Output = Result<Value>>>>,
}

impl ValueStream {
    pub(crate) fn new(conn: Connection) -> Self {
        //FIXME: Find a way to avoid cloning?
        let poll_future = Self::create_poll_future(conn.clone());
        Self { conn, poll_future }
    }

    fn create_poll_future(conn: Connection) -> Pin<Box<dyn Future<Output = Result<Value>>>> {
        async move {
            let mut lock = conn.stream.lock().await;
            Connection::read_value(&mut lock).await
        }
            .boxed()
    }
}

impl Stream for ValueStream {
    type Item = Value;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.poll_future.as_mut().poll(cx) {
            Poll::Ready(p) => {
                self.poll_future = Self::create_poll_future(self.conn.clone());

                match p {
                    Ok(p) => Poll::Ready(Some(p)),
                    Err(_) => Poll::Ready(None),
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

///A stream of [`Message`s](Message). The stream will end if an error is encountered, if the logging feature is enabled. Requires a logger compatible with the [`log`](https://crates.io/crates/log) crate.
pub struct MessageStream {
    inner: Pin<Box<ValueStream>>,
}

impl MessageStream {
    pub(crate) fn new(connection: Connection) -> Self {
        Self {
            inner: Pin::new(Box::new(ValueStream::new(connection))),
        }
    }
}

impl Stream for MessageStream {
    type Item = Result<Message>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(v)) => {
                let mut result = v.unwrap_array().into_iter();
                assert_eq!(result.next().unwrap(), Value::String("message".into()));
                let channel = result.next().unwrap().unwrap_string();
                let message = result.next().unwrap().unwrap_string();
                let output = Message { channel, message };
                Poll::Ready(Some(Ok(output)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

///A stream of [`PMessage`s](PMessage). See [`MessageStream`](MessageStream) for more info.
pub struct PMessageStream {
    inner: Pin<Box<ValueStream>>,
}

impl PMessageStream {
    pub(crate) fn new(connection: Connection) -> Self {
        Self {
            inner: Pin::new(Box::new(ValueStream::new(connection))),
        }
    }
}

impl Stream for PMessageStream {
    type Item = PMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(v)) => {
                let mut result = v.unwrap_array().into_iter();
                assert_eq!(result.next().unwrap(), Value::String("pmessage".into()));
                let pattern = result.next().unwrap().unwrap_string();
                let channel = result.next().unwrap().unwrap_string();
                let message = result.next().unwrap().unwrap_string();
                let output = PMessage {
                    channel,
                    message,
                    pattern,
                };
                Poll::Ready(Some(output))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
