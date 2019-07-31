#![feature(async_await)]

use darkredis::Connection;

async fn add_comments(mut connection: Connection, post: usize, comments: Vec<&str>) {
    let key = format!("posts.{}.comments", post);
    connection.rpush_slice(&key, &comments).await.unwrap();
}

async fn add_post(mut connection: Connection, postid: usize, text: &str) {
    let key = "posts";
    let post = format!("{}\n{}", postid, text);
    connection.rpush(&key, post).await.unwrap();
}

async fn show_posts(mut connection: Connection) {
    let posts = connection.lrange("posts", 0, 10).await.unwrap().unwrap();

    for post in posts {
        let post = String::from_utf8_lossy(&post);
        let id = (&post[0..1]).parse::<usize>().unwrap();
        let comment_key = format!("posts.{}.comments", id);
        println!("Post #{}: {}", id, &post[2..]);
        let comments = connection
            .lrange(&comment_key, 0, 10)
            .await
            .unwrap()
            .unwrap();

        for (number, comment) in comments.iter().enumerate() {
            println!("Comment #{}: {}", number, String::from_utf8_lossy(comment));
        }

        connection.del(comment_key).await.unwrap();

        println!();
    }

    connection.del("posts").await.unwrap();
}

#[runtime::main]
async fn main() {
    let connection = Connection::connect("127.0.0.1:6379", None).await.unwrap();

    //Write some posts
    let first_post = "My first ever blog post!";
    let first_comments = vec!["Cool!", "Nice!", "I'm excited!"];
    add_post(connection.clone(), 1, first_post).await;
    add_comments(connection.clone(), 1, first_comments).await;

    let second_post = "Blogging is hard work!";
    add_post(connection.clone(), 2, second_post).await;

    let last_post = "My last ever blog post...";
    let last_comments = vec!["Sad to see you go, you were good.", "It's not fair!"];
    add_post(connection.clone(), 3, last_post).await;
    add_comments(connection.clone(), 3, last_comments).await;

    show_posts(connection).await;
}
