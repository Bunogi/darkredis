use super::{Connection, Result, Value};
use futures::{
    lock::Mutex,
    task::{Context, Poll},
    Future, FutureExt, Stream,
};
use std::{pin::Pin, sync::Arc};

#[cfg(feature = "runtime_async_std")]
use async_std::net::TcpStream;
#[cfg(feature = "runtime_tokio")]
use tokio::net::TcpStream;

///A message received from a channel.
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub struct Message {
    ///The channel the message was received on
    pub channel: Vec<u8>,
    ///The actual message data
    pub message: Vec<u8>,
}

///A message received from a channel (Pattern version)
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub struct PMessage {
    ///The channel the message was received on
    pub channel: Vec<u8>,
    ///The actual message data
    pub message: Vec<u8>,
    ///The pattern matched by `channel`.
    pub pattern: Vec<u8>,
}

#[allow(missing_debug_implementations)]
struct ValueStream {
    conn: Connection,
    poll_future: Pin<Box<dyn Future<Output = Result<Value>> + Send>>,
}

impl ValueStream {
    pub(crate) fn new(conn: Connection) -> Self {
        //FIXME: Find a way to avoid cloning?
        let poll_future = Self::create_poll_future(conn.clone());
        Self { conn, poll_future }
    }

    fn create_poll_future(conn: Connection) -> Pin<Box<dyn Future<Output = Result<Value>> + Send>> {
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

///A stream of [`Message`s](struct.Message.html).
#[must_use = "No messages will be received if left unused"]
#[allow(missing_debug_implementations)]
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
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(v)) => {
                let mut result = v.unwrap_array().into_iter();
                assert_eq!(result.next().unwrap(), Value::String("message".into()));
                let channel = result.next().unwrap().unwrap_string();
                let message = result.next().unwrap().unwrap_string();
                let output = Message { channel, message };
                Poll::Ready(Some(output))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

///A stream of [`PMessage`s](struct.PMessage.html). See [`MessageStream`](struct.MessageStream.html) for more info.
#[must_use = "No messages will be received if left unused"]
#[allow(missing_debug_implementations)]
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

type ResponseFuture = Pin<Box<dyn Future<Output = Result<Value>> + Send>>;
///A stream of responses from a pipelined command.
#[must_use]
#[allow(missing_debug_implementations)]
pub struct ResponseStream {
    expected: usize,
    received: usize,
    stream: Arc<Mutex<TcpStream>>,
    poll_future: ResponseFuture,
}

impl ResponseStream {
    pub(crate) fn new(reply_count: usize, stream: Arc<Mutex<TcpStream>>) -> Self {
        let poll_future = Self::create_future(stream.clone());
        Self {
            poll_future,
            expected: reply_count,
            received: 0,
            stream,
        }
    }

    fn create_future(stream: Arc<Mutex<TcpStream>>) -> ResponseFuture {
        async move {
            let mut stream = stream.lock().await;
            Connection::read_value(&mut stream).await
        }
        .boxed()
    }
}

impl Stream for ResponseStream {
    type Item = Result<Value>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        //Are there any values left to get?
        if self.received == self.expected {
            return Poll::Ready(None);
        }

        match self.poll_future.as_mut().poll(cx) {
            Poll::Ready(p) => {
                self.received += 1;
                self.poll_future = Self::create_future(self.stream.clone());

                Poll::Ready(Some(p))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
