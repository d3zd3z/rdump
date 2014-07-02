// Main dump program.

extern crate libpool = "pool";

use std::os;
use libpool::pool;

fn main() {
    let args = os::args();
    let args = args.tail();
    match args {
        [] => fail!("Expecting command"),
        _ => ()
    };

    match (args[0].as_slice(), args.tail()) {
        ("create", [ref path]) => create(path.as_slice()),
        ("list", [ref path]) => list(path.as_slice()),
        (ref cmd, e) => fail!("Unknown args: {} {}", cmd, e)
    };
}

fn create(path: &str) {
    match pool::create(&Path::new(path)) {
        Ok(()) => (),
        Err(e) => fail!("Unable to create pool: {}", e)
    };
}

fn list(path: &str) {
    let p = pool::open(Path::new(path)).unwrap();

    for id in p.backups().unwrap().iter() {
        println!("{}", id.to_hex());
    }
}
