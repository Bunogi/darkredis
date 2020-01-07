use crate::connection::builder::MSetBuilder;
use std::io::Write;

///A struct for defining commands manually, which allows for pipelining of several commands. If you need
///to only run one command, use [`Command`](struct.Command.html), which has almost the same API.
///# Example
/// ```
///use darkredis::{CommandList, Connection};
///use futures::TryStreamExt; //for `try_collect`
///# use darkredis::*;
///# #[tokio::main]
///# async fn main() {
///# let mut connection = Connection::connect("127.0.0.1:6379").await.unwrap();
///# connection.del("pipelined-list").await.unwrap();
///
/// let command = CommandList::new("LPUSH").arg(b"pipelined-list").arg(b"bar")
///     .command("LTRIM").arg(b"pipelined-list").arg(b"0").arg(b"100");
/// let results = connection.run_commands(command).await.unwrap();
///
/// assert_eq!(results.try_collect::<Vec<Value>>().await.unwrap(), vec![Value::Integer(1), Value::Ok]);
///# connection.del("pipelined-list").await.unwrap();
///# }
/// ```
#[derive(Clone, Debug)]
pub struct CommandList<'a> {
    commands: Vec<Command<'a>>,
}

impl<'a> CommandList<'a> {
    ///Create a new command list starting with `cmd`.
    pub fn new(cmd: &'a str) -> Self {
        let commands = vec![Command::new(cmd)];
        Self { commands }
    }

    ///Consumes the command and appends an argument to it, builder style. Note that this will NOT create a new
    ///command for pipelining. That's what [`Commandlist::command`](struct.CommandList.html#method.command) is for.
    ///# See also
    ///[`append_arg`](struct.CommandList.html#method.append_arg)
    pub fn arg<D>(mut self, data: &'a D) -> Self
    where
        D: AsRef<[u8]>,
    {
        self.append_arg(data);
        self
    }

    ///Add multiple arguments from a slice, builder-style.
    ///# See also
    ///[`append_args`](struct.CommandList.html#method.append_args)
    pub fn args<D>(mut self, arguments: &'a [D]) -> Self
    where
        D: AsRef<[u8]>,
    {
        self.append_args(arguments);
        self
    }

    ///Add a command to be executed in a pipeline, builder-style. Calls to `Command::arg` will add arguments from
    ///now on.
    ///# See also
    ///[`append_command`](struct.CommandList.html#method.append_command)
    pub fn command(mut self, cmd: &'a str) -> Self {
        self.commands.push(Command::new(cmd));
        self
    }

    ///Append arguments from a slice.
    ///# See also
    ///[`args`](struct.CommandList.html#method.args)
    pub fn append_args<D>(&mut self, arguments: &'a [D])
    where
        D: AsRef<[u8]>,
    {
        let last_command = self.commands.last_mut().unwrap();
        for arg in arguments {
            last_command.args.push(arg.as_ref());
        }
    }

    ///Mutate `self` by adding an additional argument.
    ///# See also
    ///[`arg`](struct.CommandList.html#method.arg)
    pub fn append_arg<D>(&mut self, data: &'a D)
    where
        D: AsRef<[u8]>,
    {
        self.commands.last_mut().unwrap().args.push(data.as_ref());
    }

    ///Append a new command to `self`.
    ///# See also
    ///[`command`](struct.CommandList.html#method.command)
    pub fn append_command(&mut self, cmd: &'a str) {
        self.commands.push(Command::new(cmd))
    }

    ///Count the number of commands currently in the pipeline
    pub fn command_count(&self) -> usize {
        self.commands.len()
    }

    //Convert to redis protocol encoding
    pub(crate) fn serialize(self, buffer: &mut Vec<u8>) {
        for command in self.commands {
            command.serialize(buffer);
        }
    }
}

#[cfg(feature = "bench")]
impl<'a> CommandList<'a> {
    ///Workaround for benchmarking
    #[inline(always)]
    pub fn serialize_bench(self) -> Vec<u8> {
        self.serialize()
    }
}

///A struct for defining commands manually. If you want to run multiple commands in a pipeline, use [`CommandList`](struct.CommandList.html).
///# Example
/// ```
///use darkredis::{Command, Connection};
///# use darkredis::*;
///# #[tokio::main]
///# async fn main() {
///# let mut connection = Connection::connect("127.0.0.1:6379").await.unwrap();
///# connection.del("singular-key").await.unwrap();
///
///let command = Command::new("SET").arg(b"singular-key").arg(b"some-value");
///let result = connection.run_command(command).await.unwrap();
///
///assert_eq!(result, Value::Ok);
///# connection.del("singular-key").await.unwrap();
///# }
/// ```
#[derive(Clone, Debug)]
pub struct Command<'a> {
    command: &'a str,
    args: Vec<&'a [u8]>,
}

impl<'a> Command<'a> {
    ///Create a new Command.
    pub fn new(cmd: &'a str) -> Self {
        Self {
            command: cmd,
            args: Vec::new(),
        }
    }

    ///Append an argument to this command, builder-style.
    ///# See also
    ///[`append_arg`](struct.Command.html#method.append_arg)
    pub fn arg<D>(mut self, data: &'a D) -> Self
    where
        D: AsRef<[u8]>,
    {
        self.append_arg(data);
        self
    }

    ///Add multiple arguments to a command in slice form.
    ///# See also
    ///[`append_args`](struct.Command.html#method.append_args)
    pub fn args<D>(mut self, arguments: &'a [D]) -> Self
    where
        D: AsRef<[u8]>,
    {
        self.append_args(arguments);

        self
    }

    ///Append an argument to `self`.
    ///# See also
    ///[`arg`](struct.Command.html#method.append_arg)
    pub fn append_arg<D>(&mut self, data: &'a D)
    where
        D: AsRef<[u8]>,
    {
        self.args.push(data.as_ref());
    }

    ///Append multiple arguments to `self`.
    ///# See also
    ///[`args`](struct.Command.html#method.args)
    pub fn append_args<D>(&mut self, arguments: &'a [D])
    where
        D: AsRef<[u8]>,
    {
        for arg in arguments {
            self.args.push(arg.as_ref());
        }
    }

    pub(crate) fn serialize(self, buffer: &mut Vec<u8>) {
        //Write array and command header
        write!(
            buffer,
            "*{}\r\n${}\r\n{}\r\n",
            self.args.len() + 1,
            self.command.len(),
            self.command
        )
        .unwrap();

        for arg in self.args {
            //Serialize as byte string
            write!(buffer, "${}\r\n", arg.len()).unwrap();
            for byte in arg {
                buffer.push(*byte);
            }
            buffer.push(b'\r');
            buffer.push(b'\n');
        }
    }

    pub(crate) fn append_msetbuilder(&mut self, builder: &'a MSetBuilder<'a>) {
        for item in builder.build() {
            self.args.push(*item);
        }
    }
}

#[cfg(any(feature = "bench", test))]
impl<'a> Command<'a> {
    #[inline(always)]
    ///Wrapper/Workaround for benchmarking and testing `serialize`
    pub fn serialize_bench(self) -> Vec<u8> {
        let mut out = Vec::new();
        self.serialize(&mut out);
        out
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn serialize_singular() {
        //The conversion makes it easier to understand failures
        let command = Command::new("GET").arg(b"some-key").serialize_bench();
        assert_eq!(
            String::from_utf8_lossy(&command),
            "*2\r\n$3\r\nGET\r\n$8\r\nsome-key\r\n"
        )
    }

    #[test]
    fn serialize_multiple() {
        let mut buf = Vec::new();
        CommandList::new("GET")
            .arg(b"some-key")
            .command("LLEN")
            .arg(b"some-other-key")
            .serialize(&mut buf);
        assert_eq!(
            String::from_utf8_lossy(&buf),
            "*2\r\n$3\r\nGET\r\n$8\r\nsome-key\r\n*2\r\n$4\r\nLLEN\r\n$14\r\nsome-other-key\r\n"
        );
    }

    #[test]
    fn multiple_args() {
        let arguments = vec!["a", "b", "c"];
        let command = Command::new("LPUSH")
            .arg(b"some-key")
            .args(&arguments)
            .serialize_bench();
        assert_eq!(
            String::from_utf8_lossy(&command),
            "*5\r\n$5\r\nLPUSH\r\n$8\r\nsome-key\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
        );
    }
}
