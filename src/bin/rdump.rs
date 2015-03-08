// Main dump program.

#![feature(collections)]
#![feature(plugin)]
#![plugin(fourcc)]

// Apparently, this doesn't get inhereted.
#[macro_use]
extern crate libpool;
// #[plugin(libpool)]

use std::env;
// use libpool::pool;
// use libpool::chunk::Chunk;
// use libpool::pdump::HexDump;

fn main() {
    println!("Hello world");
    println!("Kind: {:?}", kind!("Foob"));
    let args: Vec<_> = env::args().collect();
    let args = args.tail();
    match args {
        [] => panic!("Expecting command"),
        _ => ()
    };

    match (&args[0][..], &args[1..]) {
        ("create", [ref path]) => create(&path[..]),
        ("list", [ref path]) => list(&path[..]),
        (ref cmd, e) => panic!("Unknown args: {} {:?}", cmd, e)
    };
}

fn create(_path: &str) {
    // match pool::create(&Path::new(path)) {
    //     Ok(()) => (),
    //     Err(e) => panic!("Unable to create pool: {}", e)
    // };
}

fn list(_path: &str) {
    // let p = pool::open(Path::new(path)).unwrap();

    // println!("Pool has {} chunks", p.len());
    // for id in p.backups().unwrap().iter() {
    //     let ch = p.find(id).unwrap();
    //     println!("kind: {}", ch.kind());
    //     println!("{}", ch.oid().to_hex());
    //     ch.with_data(|d| d.dump());
    // }
}
