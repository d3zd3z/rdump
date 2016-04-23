// Filedata storage.

#![allow(dead_code)]

use Result;
use indirect;
use std::io;
use std::io::ErrorKind;
use std::iter;
use cas;
use cas::pool::ChunkSink;
use cas::{Chunk, Kind, Oid};

pub struct DataWrite<'a> {
    sink: &'a ChunkSink,
    limit: usize,
}

impl<'a> DataWrite<'a> {
    pub fn new(sink: &ChunkSink) -> DataWrite {
        DataWrite::new_limit(sink, 256 * 1024)
    }

    pub fn new_limit(sink: &ChunkSink, limit: usize) -> DataWrite {
        DataWrite {
            sink: sink,
            limit: limit,
        }
    }

    // Attempt to write all of the contents of `source` to the pool,
    // returning the hash of the data or an error.
    pub fn write<'b>(&mut self, source: &'b mut io::Read) -> cas::Result<Oid> {
        let mut ind = indirect::Write::new(self.sink, self.limit, "IND".to_string());
        loop {
            let buf = try!(self.fill(source));
            if buf.len() == 0 {
                break;
            }

            let ch = Chunk::new_plain(Kind::new("blob").unwrap(), buf);
            try!(self.sink.inner().add(&ch, self.sink));
            try!(ind.add(ch.oid()));
            // println!("write {} bytes", ch.data_len());
        }

        ind.finish()
    }

    // Return a buffer filled with data.  Note that this will potentially
    // discard data on error.
    fn fill(&mut self, source: &mut io::Read) -> Result<Vec<u8>> {
        let mut buf: Vec<u8> = iter::repeat(0).take(self.limit).collect();
        let mut len = 0;

        loop {
            if len == buf.len() {
                break;
            }

            match source.read(&mut buf[len..]) {
                Ok(0) => break,
                Ok(n) => len += n,
                Err(ref e) if e.kind() == ErrorKind::Interrupted => {},
                Err(e) => return Err(From::from(e)),
            }
        }

        buf.truncate(len);
        Ok(buf)
    }
}
