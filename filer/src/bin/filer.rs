// Show a tree.

extern crate cas;
extern crate byteorder;

use byteorder::{BigEndian, ReadBytesExt};
use cas::{Kind, Oid};
use cas::Result;
use cas::pdump::HexDump;
use cas::pool::{AdumpPool, ChunkSource};
use std::collections::BTreeMap;
use std::env;
use std::io::Read;

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

        let mut buf = &ch.data()[..];

        let kind = buf.read_string1().unwrap();

        let mut dict = BTreeMap::new();
        while buf.len() > 0 {
            let key = buf.read_string1().unwrap();
            let value = buf.read_string2().unwrap();
            dict.insert(key, value);
        }
        println!("kind: {:?}, dict: {:#?}", kind, dict);
    }
}

trait Decode: Read {
    fn read_string1(&mut self) -> Result<String> {
        let len = try!(self.read_u8());
        let mut buf = vec![0u8; len as usize];
        try!(self.read_exact(&mut buf));
        Ok(try!(String::from_utf8(buf)))
    }

    fn read_string2(&mut self) -> Result<String> {
        let len = try!(self.read_u16::<BigEndian>());
        let mut buf = vec![0u8; len as usize];
        try!(self.read_exact(&mut buf));
        Ok(try!(String::from_utf8(buf)))
    }
}

impl<T: Read> Decode for T {}
