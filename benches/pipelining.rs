use criterion::{black_box, criterion_group, criterion_main, Criterion};
use darkredis::{test::*, CommandList, Connection, Result, Value};
use futures::StreamExt;

macro_rules! create_commands {
    ($key:ident) => {{
        let mut out = CommandList::new("SET").arg(&$key).arg(b"0");

        for _ in 0..500 {
            out.append_command("INCR");
            out.append_arg(&$key);
        }

        //leave a clean state
        out.command("DEL").arg(&$key)
    }};
}

async fn connect() -> Connection {
    Connection::connect(TEST_ADDRESS, None).await.unwrap()
}

async fn pipelined_stream() {
    let list_key = "darkredis.bench.pipelined_stream";
    let mut conn = connect().await;

    let commands = create_commands!(list_key);
    let stream = conn.run_commands_stream(commands).await.unwrap();
    let _: Vec<Result<Value>> = black_box(stream.collect().await);
}

async fn pipelined_vec() {
    let list_key = "darkredis.bench.pipelined_vec";
    let mut conn = connect().await;

    let commands = create_commands!(list_key);
    let _: Result<Vec<Value>> = black_box(conn.run_commands(commands).await);
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("Pipelined Stream", |b| {
        b.iter(|| rt.block_on(pipelined_stream()))
    });
    c.bench_function("Pipelined Vector", |b| {
        b.iter(|| rt.block_on(pipelined_vec()))
    });
}

criterion_group!(pipelining, criterion_benchmark);
criterion_main!(pipelining);
