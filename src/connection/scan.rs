use super::Connection;
use crate::Command;
use futures::{
    task::{Context, Poll},
    FutureExt, Stream,
};
use std::{collections::VecDeque, future::Future, iter::Iterator, pin::Pin};

#[derive(Debug)]
///Builder to build a SCAN or SSCAN command. Borrows all it's data.
pub struct ScanBuilder<'a> {
    connection: &'a mut Connection,
    pattern: Option<&'a [u8]>,
    key: Option<&'a [u8]>,
    count: Option<isize>,
    command: &'static str,
}

impl<'a> ScanBuilder<'a> {
    pub(crate) fn new(
        command: &'static str,
        key: Option<&'a [u8]>,
        connection: &'a mut Connection,
    ) -> Self {
        Self {
            connection,
            key,
            command,
            pattern: None,
            count: None,
        }
    }

    ///Match keys using `pattern`.
    pub fn pattern<P>(mut self, pattern: &'a P) -> Self
    where
        P: AsRef<[u8]>,
    {
        self.pattern = Some(pattern.as_ref());
        self
    }

    ///Return a maximum of `count` keys per query to Redis. This does not limit the number of returnd
    ///keys in the stream, see the Redis documentation on [`SCAN`](https://redis.io/commands/scan)
    ///for more information.
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count as isize);
        self
    }

    ///Consume `self` and create a stream of scanned values.
    pub fn run(self) -> ScanStream<'a> {
        ScanStream::new(
            self.command,
            self.key,
            self.pattern,
            self.count,
            self.connection,
        )
    }
}

//Future used in ScanStream
type ScanStreamFuture<'a> = Pin<Box<dyn Future<Output = (Vec<u8>, Vec<Vec<u8>>)> + Send + 'a>>;

///A Stream of results from running SCAN or SSCAN. The same value might appear multiple times. Polling
///until the stream is empty will return all matched elements.
#[must_use]
#[allow(missing_debug_implementations)]
pub struct ScanStream<'a> {
    command: &'static str,
    key: Option<&'a [u8]>,
    pattern: Option<&'a [u8]>,
    count: Option<isize>,
    connection: Connection,
    poll_future: ScanStreamFuture<'a>,
    last_cursor: Vec<u8>,
    receive_buffer: VecDeque<Vec<u8>>,
}

impl<'a> ScanStream<'a> {
    pub(crate) fn new(
        command: &'static str,
        key: Option<&'a [u8]>,
        pattern: Option<&'a [u8]>,
        count: Option<isize>,
        connection: &'a mut Connection,
    ) -> Self {
        let connection = connection.clone();
        let poll_future = Self::create_poll_future(
            command,
            key,
            b"0".to_vec(), // Start at a zero-cursor to get everything
            pattern,
            count,
            connection.clone(),
        );

        let receive_buffer = VecDeque::new();

        Self {
            command,
            connection,
            count,
            key,
            last_cursor: b"1".to_vec(), // Just has to not be zero
            pattern,
            poll_future,
            receive_buffer,
        }
    }

    fn create_poll_future(
        command: &'static str,
        key: Option<&'a [u8]>,
        cursor: Vec<u8>,
        pattern: Option<&'a [u8]>,
        count: Option<isize>,
        mut conn: Connection,
    ) -> ScanStreamFuture<'a> {
        async move {
            //Build the command
            let mut command = Command::new(command);
            //Will be Some if we're working on any SCAN family command except... SCAN
            if let Some(ref key) = key {
                command.append_arg(key);
            }

            //Required argument: Cursor
            command.append_arg(&cursor);

            //Optional arguments
            if let Some(ref pattern) = pattern {
                command.append_arg(b"MATCH");
                command.append_arg(pattern);
            }
            let count = count.clone().map(|s| s.to_string()); // to fix an ownership problem
            if let Some(ref count) = count {
                command.append_arg(b"COUNT");
                command.append_arg(count);
            }

            //Get the results as an iterator
            //When using the stream, we cannot really return an error without making an awkward API.
            //Therefore, it is okay to unwrap here as the scan commands will, at worst, return an empty list.
            let mut result = conn
                .run_command(command)
                .await
                .expect("Running SCAN command")
                .unwrap_array()
                .into_iter();

            //Assume that the response is valid since we did not receive an error.
            let cursor = result.next().expect("getting cursor field").unwrap_string();
            let values = result
                .next()
                .expect("getting value array")
                .unwrap_array()
                .into_iter()
                .map(|s| s.unwrap_string())
                .collect();

            (cursor, values)
        }
        .boxed()
    }
}

impl<'a> Stream for ScanStream<'a> {
    type Item = Vec<u8>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Some(v) = self.receive_buffer.pop_front() {
            Poll::Ready(Some(v))
        } else if self.last_cursor == b"0" {
            //Redis returns a 0 cursor when done.
            Poll::Ready(None)
        } else {
            match self.poll_future.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready((cursor, fields)) => {
                    self.poll_future = Self::create_poll_future(
                        self.command,
                        self.key,
                        cursor.clone(),
                        self.pattern,
                        self.count.clone(),
                        self.connection.clone(),
                    );

                    for f in fields {
                        self.receive_buffer.push_back(f);
                    }
                    //Note what the last cursor is so we can exit when the receive buffer is empty.
                    self.last_cursor = cursor;
                    //If the buffer is empty now, that means that no keys were found.
                    Poll::Ready(self.receive_buffer.pop_front())
                }
            }
        }
    }
}

///Builder to build a HSCAN command. Borrows all it's data.
#[derive(Debug)]
pub struct HScanBuilder<'a> {
    connection: &'a mut Connection,
    pattern: Option<&'a [u8]>,
    key: &'a [u8],
    count: Option<isize>,
}

impl<'a> HScanBuilder<'a> {
    pub(crate) fn new(key: &'a [u8], connection: &'a mut Connection) -> Self {
        Self {
            connection,
            key,
            pattern: None,
            count: None,
        }
    }

    ///Match keys using `pattern`.
    pub fn pattern<P>(mut self, pattern: &'a P) -> Self
    where
        P: AsRef<[u8]>,
    {
        self.pattern = Some(pattern.as_ref());
        self
    }

    ///Return a maximum of `count` keys per query to Redis. This does not limit the number of returned
    ///keys in the stream, see the Redis documentation on [`SCAN`](https://redis.io/commands/scan)
    ///for more information.
    pub fn count(mut self, count: isize) -> Self {
        self.count = Some(count);
        self
    }

    ///Consume self and return a HScanStream.
    pub fn run(self) -> HScanStream<'a> {
        HScanStream::new(self.key, self.pattern, self.count, self.connection)
    }
}

///A Stream of results from running HSCAN. The same key might appear multiple times, and polling
///until the stream is empty will return all matching fields and values in the hash set.
///# Return Value
///The field and its associated value in the form (field, value) as a tuple.
#[must_use]
#[allow(missing_debug_implementations)]
pub struct HScanStream<'a> {
    inner: Pin<Box<ScanStream<'a>>>,
    current_field: Option<Vec<u8>>,
}

impl<'a> HScanStream<'a> {
    pub(crate) fn new(
        key: &'a [u8],
        pattern: Option<&'a [u8]>,
        count: Option<isize>,
        connection: &'a mut Connection,
    ) -> Self {
        let inner = Pin::new(Box::new(ScanStream::new(
            "HSCAN",
            Some(key),
            pattern,
            count,
            connection,
        )));
        Self {
            inner,
            current_field: None,
        }
    }
}

impl<'a> Stream for HScanStream<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(v)) => {
                if self.current_field.is_some() {
                    let field = self.current_field.clone().unwrap();
                    self.current_field = None;
                    Poll::Ready(Some((field, v)))
                } else {
                    self.current_field = Some(v);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}
