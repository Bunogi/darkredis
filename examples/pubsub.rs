//An example of how to use the `subscribe` function.

use darkredis::ConnectionPool;
use futures::StreamExt;
use std::time::Duration;

#[tokio::main]
async fn main() {
    //Creating a connection pool allows us to easily `spawn` a new connection to use as our listener.
    let pool = ConnectionPool::create("127.0.0.1:6379".into(), None, 1)
        .await
        .unwrap();

    let channels = vec!["some-channel", "some-other-channel"];
    //Create a listener and name it "mylistener" so we can identify it using `CLIENT LIST`.
    let listener = pool.spawn("mylistener").await.unwrap();
    let messagestream = listener.subscribe(&channels).await.unwrap();

    //Publish some messages
    tokio::spawn(async move {
        let mut publisher = pool.get().await;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            publisher.publish("some-channel", "hello!").await.unwrap();
            publisher
                .publish("some-other-channel", "hello again!")
                .await
                .unwrap();
            interval.tick().await; //Don't spam with too many messages
        }
    });

    //Use the stream to receive the messages. For a real application you might want to spawn a task, in order
    //to always be listening for updates, so you don't miss any.
    messagestream
        .for_each(|e| {
            async move {
                println!(
                    "Received a message on channel '{}': {}",
                    String::from_utf8_lossy(&e.channel),
                    String::from_utf8_lossy(&e.message)
                );
            }
        })
        .await;

    //From here you could, for example, spawn a task which only listens for messages and send them along
    //in your program using channels or other methods.
}
