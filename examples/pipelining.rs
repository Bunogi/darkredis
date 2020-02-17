use darkredis::{CommandList, Connection, ResponseStream, Result, Value};
use futures::StreamExt;

//In your own code, you'd use simply #[tokio::main] or #[async_std::main]
#[cfg_attr(feature = "runtime_tokio", tokio::main)]
#[cfg_attr(feature = "runtime_async_std", async_std::main)]
async fn main() -> Result<()> {
    let mut connection = Connection::connect("127.0.0.1:6379").await?;

    //Create a list of commands to be executed. In addition to the builder-style seen here, you could
    //also prefix the functions with `append`. This would mutate the CommandList object without
    //moving it.
    let key = "some-key";
    let commands = CommandList::new("SET")
        .arg(&key)
        .arg(b"some-value")
        .command("GET")
        .arg(&key)
        .command("DEL")
        .arg(&key);

    let mut stream: ResponseStream = connection.run_commands(commands).await?;

    //Set successfully
    assert_eq!(stream.next().await.unwrap().unwrap(), Value::Ok);
    //Result of GET
    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        Value::String(b"some-value".to_vec())
    );
    //Successfully deleted 1 key
    assert_eq!(stream.next().await.unwrap().unwrap(), Value::Integer(1));

    Ok(())
}
