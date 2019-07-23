#![feature(async_await)]

use futures::stream::{self, StreamExt};
use redis_async::Connection;

async fn add_comment(mut connection: Connection, post: usize, text: &str) {
    let key = format!("posts.{}.comments", post);
    connection.rpush(&key, text).await.unwrap();
}

async fn add_post(mut connection: Connection, postid: usize, text: &str) {
    let key = "posts";
    connection
        .rpush(&key, format!("{}\n{}", postid, text))
        .await
        .unwrap();
}

async fn show_posts(mut connection: Connection) {
    let posts = connection.lrange("posts", 0, 10).await.unwrap().unwrap();

    for post in posts {
        let post = String::from_utf8_lossy(&post);
        let id = (&post[0..1]).parse::<usize>().unwrap();
        println!("Post #{}: {}", id, &post[2..]);
        let comments = connection
            .lrange(format!("posts.{}.comments", id), 0, 10)
            .await
            .unwrap()
            .unwrap();

        for (number, comment) in comments.iter().enumerate() {
            println!("Comment #{}: {}", number, String::from_utf8_lossy(comment));
        }

        println!();
    }
}

#[runtime::main]
async fn main() {
    let connection = Connection::connect("127.0.0.1:6379").await.unwrap();

    //Write some posts
    let first_post = "My first ever blog post!";
    add_post(connection.clone(), 1, first_post).await;
    stream::iter(vec!["Cool!", "Nice!", "I'm excited!"])
        .for_each(|comment| add_comment(connection.clone(), 1, comment))
        .await;

    let second_post = "Blogging is hard work!";
    add_post(connection.clone(), 2, second_post).await;

    let last_post = "My last ever blog post...";
    add_post(connection.clone(), 3, last_post).await;
    stream::iter(vec!["Sad to see you go, you were good.", "It's not fair!"])
        .for_each(|comment| add_comment(connection.clone(), 3, comment))
        .await;

    show_posts(connection).await;
}
