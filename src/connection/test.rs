use super::*;
use crate::{redis_test, test::*, Command, CommandList, PMessage, Value};
use futures::StreamExt;

#[tokio::test]
async fn parse_nil() {
    redis_test!(
        redis,
        {
            let command = Command::new("GET").arg(&null_key);

            assert_eq!(redis.run_command(command).await.unwrap(), Value::Nil);
        },
        null_key
    );
}
#[tokio::test]
async fn parse_ok() {
    redis_test!(
        redis,
        {
            let command = Command::new("SET").arg(&some_key).arg(b"");

            assert_eq!(redis.run_command(command).await.unwrap(), Value::Ok);
        },
        some_key
    );
}
#[tokio::test]
async fn pipelined_commands() {
    redis_test!(
        redis,
        {
            let command = CommandList::new("SET")
                .arg(&simple_key)
                .arg(b"")
                .command("LPUSH")
                .arg(&list_key)
                .arg(b"")
                .command("LPUSH")
                .arg(&list_key)
                .arg(b"");

            assert_eq!(
                redis.run_commands(command).await.unwrap(),
                vec![Value::Ok, Value::Integer(1), Value::Integer(2)]
            );
        },
        simple_key,
        list_key
    );
}

#[tokio::test]
async fn pubsub() {
    redis_test!(
        publisher,
        {
            let receiver = Connection::connect(TEST_ADDRESS, None).await.unwrap();
            let channels = vec![&channel0, &channel1, &channel2];
            let mut stream = receiver.subscribe(&channels).await.unwrap();
            let publish_future = async {
                let commands = CommandList::new("PUBLISH")
                    .arg(&channel0)
                    .arg(&"foo")
                    .command("PUBLISH")
                    .arg(&channel1)
                    .arg(&"bar")
                    .command(&"PUBLISH")
                    .arg(&channel2)
                    .arg(&"foobar");

                publisher.run_commands(commands).await.unwrap();
            };

            let c0 = channel0.clone();
            let c1 = channel1.clone();
            let c2 = channel2.clone();
            let receiver_future = async move {
                let expected = vec![
                    Message {
                        channel: c0,
                        message: "foo".into(),
                    },
                    Message {
                        channel: c1,
                        message: "bar".into(),
                    },
                    Message {
                        channel: c2,
                        message: "foobar".into(),
                    },
                ];
                for i in 0..3 {
                    let result = stream.next().await.unwrap().unwrap();
                    assert_eq!(result, expected[i]);
                }
            };

            futures::future::join(publish_future, receiver_future).await;
        },
        channel0,
        channel1,
        channel2
    );
}

#[tokio::test]
async fn pubsub_pattern() {
    redis_test!(
        publisher,
        {
            let receiver = Connection::connect(TEST_ADDRESS, None).await.unwrap();
            let mut pattern = base_channel.clone();
            pattern.push(b'*');
            let patterns = vec![pattern.clone()];
            let mut stream = receiver.psubscribe(&patterns).await.unwrap();

            let mut publish_channel = base_channel.clone();
            publish_channel.push(b'a');
            let receive_channel = publish_channel.clone();

            let publish_future = async {
                let command = Command::new("PUBLISH").arg(&publish_channel).arg(&"foo");

                publisher.run_command(command).await.unwrap();
            };

            let receiver_future = async move {
                let expected = PMessage {
                    message: b"foo".to_vec(),
                    channel: receive_channel,
                    pattern,
                };
                let result = stream.next().await.unwrap();
                assert_eq!(result, expected);
            };

            futures::future::join(publish_future, receiver_future).await;
        },
        base_channel
    );
}

#[tokio::test]
async fn get_set() {
    redis_test!(
        redis,
        {
            redis.set(&key, "foo").await.unwrap();
            assert_eq!(redis.get(&key).await.unwrap(), Some("foo".into()));
        },
        key
    );
}

#[tokio::test]
async fn list_convenience() {
    redis_test!(
        redis,
        {
            redis.rpush_slice(&list_key, &["1", "2"]).await.unwrap();
            redis.lpush(&list_key, "0").await.unwrap();

            let expected: Vec<Vec<u8>> = vec![b"0", b"1", b"2"]
                .into_iter()
                .map(|s| s.to_vec())
                .collect();
            assert_eq!(redis.lrange(&list_key, 0, 3).await.unwrap(), expected);
            assert_eq!(redis.lpop(&list_key).await.unwrap(), Some(b"0".to_vec()));
            assert_eq!(redis.rpop(&list_key).await.unwrap(), Some(b"2".to_vec()));
            assert_eq!(redis.llen(&list_key).await.unwrap(), Some(1));

            let long_list: Vec<String> = std::iter::repeat("value".to_string()).take(10).collect();
            redis.lpush_slice(&list_key, &long_list).await.unwrap();
            redis.ltrim(&list_key, 0, 4).await.unwrap();
            redis.lset(&list_key, 0, b"hello").await.unwrap();
            assert_eq!(redis.llen(&list_key).await.unwrap(), Some(5));
            assert_eq!(redis.lrange(&list_key, 0, 0).await.unwrap(), vec![b"hello"]);
        },
        list_key
    );
}

#[tokio::test]
async fn incr_decr() {
    redis_test!(
        redis,
        {
            assert_eq!(redis.incr(&int_key).await.unwrap(), 1);
            assert_eq!(redis.incrby(&int_key, 41).await.unwrap(), 42);
            assert_eq!(redis.decr(&int_key).await.unwrap(), 41);
            assert_eq!(redis.decrby(&int_key, 20).await.unwrap(), 21);
            assert_eq!(redis.get(&int_key).await.unwrap(), Some(b"21".to_vec()));

            assert_eq!(redis.incrbyfloat(&float_key, 8.0).await.unwrap(), 8.0);
            assert_eq!(redis.incrbyfloat(&float_key, -4.0).await.unwrap(), 4.0);
        },
        int_key,
        float_key
    );
}

#[tokio::test]
async fn append() {
    redis_test!(
        redis,
        {
            assert_eq!(redis.append(&key, b"Hello, ").await.unwrap(), 7);
            assert_eq!(redis.append(&key, b"world!").await.unwrap(), 13);
            assert_eq!(
                redis.get(&key).await.unwrap(),
                Some(b"Hello, world!".to_vec())
            );
        },
        key
    );
}

#[tokio::test]
async fn mget_mset() {
    redis_test!(
        redis,
        {
            //Verify that it works with trait objects
            let keys: Vec<&(dyn AsRef<[u8]> + Sync)> = vec![&"key1", &b"key2"];
            let values: Vec<&(dyn AsRef<[u8]> + Sync)> = vec![&"value1", &b"value2"];
            redis.mset(&keys, &values).await.unwrap();

            let simple_keys = vec!["key1", "key2", "key3"];
            let simple_values = vec!["value1", "value2"];
            redis
                .mset(simple_keys.as_slice(), simple_values.as_slice())
                .await
                .unwrap();

            assert_eq!(
                redis.mget(&simple_keys).await.unwrap(),
                vec![Some(b"value1".to_vec()), Some(b"value2".to_vec()), None]
            );

            //clean up
            redis.del("key1").await.unwrap();
            redis.del("key2").await.unwrap();
            redis.del("key3").await.unwrap();
        },
        key
    );
}

#[tokio::test]
async fn exists() {
    redis_test!(
        redis,
        {
            assert_eq!(redis.exists(&key).await.unwrap(), false);
            redis.set(&key, "foo").await.unwrap();
            assert_eq!(redis.exists(&key).await.unwrap(), true);
        },
        key
    );
}
