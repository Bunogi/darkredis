use darkredis::{Connection, MSetBuilder};
use futures::StreamExt;

//In your own code, you'd use simply #[tokio::main] or #[async_std::main]
#[cfg_attr(feature = "runtime_tokio", tokio::main)]
#[cfg_attr(feature = "runtime_async_std", async_std::main)]
async fn main() {
    let mut connection = Connection::connect("127.0.0.1:6379").await.unwrap();

    let key = "usernames";

    //The simplest scan is SSCAN:

    //Add some users
    connection.sadd(&key, "john123").await.unwrap();
    connection.sadd(&key, "jane123").await.unwrap();
    connection.sadd(&key, "bob").await.unwrap();
    connection.sadd(&key, "bill").await.unwrap();
    connection.sadd(&key, "james123").await.unwrap();

    //Let's say you want to search for users ending in 123.
    let users = connection
        .sscan(&key)
        .pattern(b"*123")
        .run()
        .collect::<Vec<Vec<u8>>>()
        .await;

    println!("Got {} results!", users.len());
    for (i, u) in users.into_iter().enumerate() {
        println!("Result {}: {}", i, String::from_utf8_lossy(&u));
    }

    //SCAN works the same:
    let sets = connection.scan().run().collect::<Vec<Vec<u8>>>().await;

    println!("There are {} keys in the database!", sets.len());

    //HSCAN is a little different, it returns the name of the field as well as the value of the field.

    let hash_key = "hash";
    let builder = MSetBuilder::new()
        .set(b"field1", b"foo")
        .set(b"field2", b"bar");
    connection.hset_many(&hash_key, builder).await.unwrap();

    let fields = connection
        .hscan(&hash_key)
        .run()
        .map(|(f, v)| (String::from_utf8(f).unwrap(), String::from_utf8(v).unwrap()))
        .collect::<Vec<(String, String)>>()
        .await;
    for (field, value) in fields {
        println!("Field {} is {}", field, value);
    }
}
