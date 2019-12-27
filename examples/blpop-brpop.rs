use darkredis::Connection;
use std::time::Duration;

async fn blpop(mut conn: Connection) -> darkredis::Result<()> {
    let mut timeouts = 0;
    loop {
        println!("blpop: waiting on 'list_a' and 'list_b' for 1 sec...");
        match conn.blpop(&["list_a", "list_b"], 1).await? {
            Some(res) => {
                let list = String::from_utf8_lossy(&res[0]);
                let value = String::from_utf8_lossy(&res[1]);
                println!("blpop: {} -> {}", list, value);
                if value == "quit" {
                    break;
                }
            }
            None => {
                timeouts += 1;
                println!("blpop: timeout {}", timeouts);
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> darkredis::Result<()> {
    let mut conn = Connection::connect("127.0.0.1:6379").await?;
    tokio::spawn(async move {
        let mut conn = Connection::connect("127.0.0.1:6379").await.unwrap();
        let step = Duration::from_millis(1_500);
        let msgs = vec![
            ("list_a", "msg1"),
            ("list_b", "msg2"),
            ("list_a", "msg3"),
            ("list_b", "msg4"),
            ("list_a", "quit"),
        ];
        for (list, val) in msgs.iter() {
            println!("rpush: {} -> {}", val, list);
            conn.rpush(list, val).await.unwrap();
            tokio::time::delay_for(step).await;
        }
    });
    blpop(conn.clone()).await?;
    conn.del("list_a").await?;
    conn.del("list_b").await?;
    Ok(())
}
