use criterion::{black_box, criterion_group, criterion_main, Criterion};
use darkredis::{Command, CommandList};

fn serialize_arrays() {
    let elements_per_array = 1000;
    let element = black_box(vec![0u8; 100]);

    let mut command = Command::new("GET");
    let elements = black_box(vec![element; elements_per_array]);
    command.append_args(&elements);

    black_box(command.serialize_bench());
}

fn serialize_commandlist() {
    let array_count = 100;
    let elements_per_array = 100;
    let element = black_box(vec![0u8; 100]);

    let mut command = CommandList::new("GET");
    //Populate first GET command
    for _ in 0..elements_per_array {
        command.append_arg(&element);
    }

    //Populate the rest
    for _ in 1..array_count {
        command.append_command("GET");
        for _ in 0..elements_per_array {
            command.append_arg(&element);
        }
    }

    black_box(command.serialize_bench());
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("Serialize many arrays", |b| b.iter(|| serialize_arrays()));
    c.bench_function("Serialize large commandlist", |b| {
        b.iter(|| serialize_commandlist())
    });
}

criterion_group!(command, criterion_benchmark);
criterion_main!(command);
