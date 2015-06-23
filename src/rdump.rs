// Dump main.

use std::env;
use std::path::Path;

use cas::pool::FilePool;

extern crate cas;

fn main() {
    let args: Vec<_> = env::args().collect();

    // TODO: Find argument processing library.
    if args.len() < 2 {
        panic!("Expecting command");
    }

    if args.len() == 3 && args[1] == "create" {
        create(&args[2]).unwrap();
    } else {
        panic!("Unknown command");
    }
}

fn create(path: &str) -> cas::Result<()> {
    FilePool::create(&Path::new(path))
}
