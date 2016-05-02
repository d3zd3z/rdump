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
    fn show_backup(&self, id: &Oid) {
        println!("back: {:?}", id);
        let ch = self.source.find(id).unwrap();
        assert_eq!(ch.kind(), Kind::new("back").unwrap());
        (&ch.data()[..]).dump();

        let mut buf = &ch.data()[..];
        let props = buf.read_props().unwrap();
        println!("props: {:#?}", props);

        // Get the backup hash.
        let hash = props.data.get("hash").unwrap();
        let oid = Oid::from_hex(hash).unwrap();
        println!("root: {:?}", oid);
        self.show_node(&oid);
    }

    fn show_node(&self, id: &Oid) {
        let ch = self.source.find(id).unwrap();
        println!("kind: {:?}", ch.kind());
        (&ch.data()[..]).dump();
        let props = (&ch.data()[..]).read_props().unwrap();
        println!("props: {:#?}", props);
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

    fn read_props(&mut self) -> Result<Props> {
        let kind = try!(self.read_string1());
        let mut dict = BTreeMap::new();
        loop {
            let key = match self.read_string1() {
                Ok(key) => key,
                Err(ref err) if err.is_unexpected_eof() => break,
                Err(e) => return Err(e),
            };
            let value = try!(self.read_string2());
            dict.insert(key, value);
        }
        Ok(Props {
            kind: kind,
            data: dict,
        })
    }
}

impl<T: Read> Decode for T {}

#[derive(Debug)]
struct Props {
    kind: String,
    data: BTreeMap<String, String>,
}
