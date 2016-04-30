// Show a tree.

extern crate cas;

use cas::{Kind, Oid};
use cas::pdump::HexDump;
use cas::pool::{AdumpPool, ChunkSource};
use std::env;

fn main() {
    let mut argsi = env::args();

    match argsi.next() {
        None => panic!("No program name given"),
        Some(_) => (),
    }

    let path = match argsi.next() {
        Some(path) => path,
        None => panic!("Expecting a single argument, of the pool name"),
    };

    match argsi.next() {
        Some(_) => panic!("Unexpected extra argument"),
        None => (),
    }

    let pool = AdumpPool::open(&path).unwrap();

    let walk = Walk { source: &pool };

    match pool.backups().unwrap().first() {
        None => println!("No backups"),
        Some(oid) => walk.show_backup(oid),
    }
}

struct Walk<'a> {
    source: &'a ChunkSource,
}

impl<'a> Walk<'a> {
    fn show_backup(self, id: &Oid) {
        println!("back: {:?}", id);
        let ch = self.source.find(id).unwrap();
        assert_eq!(ch.kind(), Kind::new("back").unwrap());
        (&ch.data()[..]).dump();
    }
}
