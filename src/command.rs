///A struct for defining commands manually, which also allows for pipelining of several commands.
///# Example
/// ```
///#![feature(async_await)]
///use redis_async::{Command, Connection};
///# use redis_async::*;
///# #[runtime::main]
///# async fn main() {
///    # let mut connection = Connection::connect("127.0.0.1:6379").await.unwrap();
///    # connection.del("pipelined-list").await.unwrap();
///
///    let command = Command::new("LPUSH").arg(b"pipelined-list").arg(b"bar")
///        .command("LTRIM").arg(b"pipelined-list").arg(b"0").arg(b"100");
///    let results = connection.run_command(command).await.unwrap();
///
///    assert_eq!(results, vec![Value::Integer(1), Value::Ok]);
///    # connection.del("pipelined-list").await.unwrap();
///# }
/// ```
pub struct Command {
    commands: Vec<Vec<Vec<u8>>>,
    command_count: usize,
}

impl Command {
    ///Create a new command from `cmd`.
    pub fn new(cmd: &str) -> Self {
        let commands = vec![vec![cmd.to_string().into_bytes()]];
        Self {
            commands,
            command_count: 1,
        }
    }

    ///Consumes the command and appends an argument to it. Note that this will NOT create a new
    ///command for pipelining. That's what `Command::command` is for.
    pub fn arg(mut self, bytes: &[u8]) -> Self {
        self.commands.last_mut().unwrap().push(bytes.to_vec());
        self
    }

    ///Add a command to be executed in a pipeline. Calls to `Command::arg` will add arguments from
    ///now on.
    pub fn command(mut self, cmd: &str) -> Self {
        self.commands.push(vec![cmd.to_string().into_bytes()]);
        self.command_count += 1;
        self
    }

    ///Count the number of commands currently in the pipeline
    pub fn command_count(&self) -> usize {
        self.command_count
    }

    //Convert to redis protocol encoding
    pub(crate) fn serialize(self) -> Vec<u8> {
        let mut out = Vec::new();
        for command in self.commands {
            let mut this_command = format!("*{}\r\n", command.len()).into_bytes();
            for arg in command {
                let mut serialized = format!("${}\r\n", arg.len()).into_bytes();
                for byte in arg {
                    serialized.push(byte);
                }
                serialized.push(b'\r');
                serialized.push(b'\n');

                this_command.append(&mut serialized);
            }
            out.append(&mut this_command);
        }

        out
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn serialize_singular() {
        //The conversion makes it easier to understand failures
        let command = Command::new("GET").arg(b"some-key").serialize();
        assert_eq!(
            String::from_utf8_lossy(&command),
            "*2\r\n$3\r\nGET\r\n$8\r\nsome-key\r\n"
        )
    }

    #[test]
    fn serialize_multiple() {
        let command = Command::new("GET")
            .arg(b"some-key")
            .command("LLEN")
            .arg(b"some-other-key")
            .serialize();
        assert_eq!(
            String::from_utf8_lossy(&command),
            "*2\r\n$3\r\nGET\r\n$8\r\nsome-key\r\n*2\r\n$4\r\nLLEN\r\n$14\r\nsome-other-key\r\n"
        );
    }
}
