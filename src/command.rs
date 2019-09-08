///A struct for defining commands manually, which allows for pipelining of several commands. If you need
///to only run one command, use [`Command`](crate::Command), which has almost the same API.
///# Example
/// ```
///use darkredis::{CommandList, Connection};
///# use darkredis::*;
///# #[runtime::main]
///# async fn main() {
///# let mut connection = Connection::connect("127.0.0.1:6379", None).await.unwrap();
///# connection.del("pipelined-list").await.unwrap();
///
/// let command = CommandList::new("LPUSH").arg(b"pipelined-list").arg(b"bar")
///     .command("LTRIM").arg(b"pipelined-list").arg(b"0").arg(b"100");
/// let results = connection.run_commands(command).await.unwrap();
///
/// assert_eq!(results, vec![Value::Integer(1), Value::Ok]);
///# connection.del("pipelined-list").await.unwrap();
///# }
/// ```
pub struct CommandList<'a> {
    commands: Vec<Command<'a>>,
}

impl<'a> CommandList<'a> {
    ///Create a new command from `cmd`.
    pub fn new(cmd: &'a str) -> Self {
        let commands = vec![Command::new(cmd)];
        Self { commands }
    }

    ///Consumes the command and appends an argument to it. Note that this will NOT create a new
    ///command for pipelining. That's what [`Commandlist::command`](crate::CommandList::command) is for.
    pub fn arg<D>(mut self, data: &'a D) -> Self
    where
        D: AsRef<[u8]>,
    {
        self.commands.last_mut().unwrap().args.push(data.as_ref());
        self
    }

    ///Add multiple arguments from a slice.
    pub fn args<D>(mut self, arguments: &'a [D]) -> Self
    where
        D: AsRef<[u8]>,
    {
        let last_command = self.commands.last_mut().unwrap();
        for arg in arguments {
            last_command.args.push(arg.as_ref());
        }
        self
    }

    ///Add a command to be executed in a pipeline. Calls to `Command::arg` will add arguments from
    ///now on.
    pub fn command(mut self, cmd: &'a str) -> Self {
        self.commands.push(Command::new(cmd));
        self
    }

    ///Count the number of commands currently in the pipeline
    pub fn command_count(&self) -> usize {
        self.commands.len()
    }

    //Convert to redis protocol encoding
    pub(crate) fn serialize(self) -> Vec<u8> {
        let mut out = Vec::new();
        for command in self.commands {
            let mut serialized = command.serialize();
            out.append(&mut serialized);
        }

        out
    }
}

///A struct for defining commands manually. If you want pipelining, use [`CommandList`](crate::CommandList).
///# Example
/// ```
///use darkredis::{Command, Connection};
///# use darkredis::*;
///# #[runtime::main]
///# async fn main() {
///# let mut connection = Connection::connect("127.0.0.1:6379", None).await.unwrap();
///# connection.del("singular-key").await.unwrap();
///
///let command = Command::new("SET").arg(b"singular-key").arg(b"some-value");
///let result = connection.run_command(command).await.unwrap();
///
///assert_eq!(result, Value::Ok);
///# connection.del("singular-key").await.unwrap();
///# }
/// ```
pub struct Command<'a> {
    command: &'a str,
    args: Vec<&'a [u8]>,
}

impl<'a> Command<'a> {
    ///Create a new a.
    pub fn new(cmd: &'a str) -> Self {
        Self {
            command: cmd,
            args: Vec::new(),
        }
    }

    ///Append an argument to this command.
    pub fn arg<D>(mut self, data: &'a D) -> Self
    where
        D: AsRef<[u8]>,
    {
        self.args.push(data.as_ref());
        self
    }

    ///Add multiple arguments to a command in slice form.
    pub fn args<D>(mut self, arguments: &'a [D]) -> Self
    where
        D: AsRef<[u8]>,
    {
        for arg in arguments {
            self.args.push(arg.as_ref());
        }

        self
    }

    pub(crate) fn serialize(self) -> Vec<u8> {
        let mut out = format!("*{}\r\n", self.args.len() + 1).into_bytes();
        let mut serialized_command =
            format!("${}\r\n{}\r\n", self.command.len(), self.command).into_bytes();
        out.append(&mut serialized_command);

        for arg in self.args {
            let mut serialized = format!("${}\r\n", arg.len()).into_bytes();
            for byte in arg {
                serialized.push(*byte);
            }
            serialized.push(b'\r');
            serialized.push(b'\n');

            out.append(&mut serialized);
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
        let command = CommandList::new("GET")
            .arg(b"some-key")
            .command("LLEN")
            .arg(b"some-other-key")
            .serialize();
        assert_eq!(
            String::from_utf8_lossy(&command),
            "*2\r\n$3\r\nGET\r\n$8\r\nsome-key\r\n*2\r\n$4\r\nLLEN\r\n$14\r\nsome-other-key\r\n"
        );
    }

    #[test]
    fn multiple_args() {
        let arguments = vec!["a", "b", "c"];
        let command = Command::new("LPUSH")
            .arg(b"some-key")
            .args(&arguments)
            .serialize();
        assert_eq!(
            String::from_utf8_lossy(&command),
            "*5\r\n$5\r\nLPUSH\r\n$8\r\nsome-key\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
        );
    }
}
