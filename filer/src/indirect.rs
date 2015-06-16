// Indirect block management

#![allow(dead_code)]

use Result;
use cas::pool::ChunkSink;
use cas::chunk;
use cas::Kind;
use cas::Oid;

// Items that are larger than a single chunk are written in multiple chunks
// and then use indirect chunks to store all of these.  The indirect chunks
// work somewhat like a Merkle tree (which because of the hash-addressed
// storage can also be used to find the data).

pub struct Write<'a> {
    // Maximum size (in bytes) to write to each indirection block.
    limit: usize,

    // Maximum number of Oids that fit within `limit` bytes.
    oid_limit: usize,

    // Three character string prefix for the indirect block type.  The
    // lowest-level of indirection chunks will be prefix + "0", the next up
    // "1", and so on.
    prefix: String,

    // The buffers for each level.  The highest index will be level zero.
    buffers: Vec<Vec<u8>>,

    // The indirection level of the first element of `buffers`.
    level: usize,

    // The sink for the data.
    sink: &'a ChunkSink,
}

impl<'a> Write<'a> {
    pub fn new<'b>(sink: &'b ChunkSink, limit: usize, prefix: String) -> Write<'b> {
        if prefix.as_bytes().len() != 3 {
            panic!("prefix must be 3 bytes");
        }

        Write {
            limit: limit,
            oid_limit: limit / Oid::size(),
            prefix: prefix,
            buffers: Vec::new(),
            level: 0,
            sink: sink,
        }
    }

    pub fn add(&mut self, oid: &Oid) -> Result<()> {
        self.add_level(oid, 0)
    }

    // Push on the back end of the stack.
    fn add_level(&mut self, oid: &Oid, level: usize) -> Result<()> {
        trace!("add: {} (level={})", oid.to_hex(), level);
        if self.buffers.is_empty() {
            // If we're out of nodes, create and push one.
            self.push_buffer();
        } else if self.buf().len() + Oid::size() > self.limit {
            trace!("Past limit");
            let top = try!(self.collapse());
            try!(self.add_level(&top, level + 1));

            self.push_buffer();
        }

        self.buf_mut().extend(oid.bytes.iter().map(|&x| x));
        /*
        unsafe {
            use std::ptr;
            use std::mem;

            let mut b = self.buf_mut();
            let mut blen = b.len();
            b.set_len(blen + Oid::size());
            let dest = mem::transmute(&mut b[blen]);
            ptr::copy(oid as *const Oid, dest, 1);
        }
        */
        Ok(())
    }

    // Add a new empty buffer.
    fn push_buffer(&mut self) {
        self.buffers.push(Vec::with_capacity(Oid::size() * self.oid_limit));
        let len = self.buffers.len();
        if len > self.level {
            self.level = len;
        }
        trace!("Push: {} buffers, level: {}", len, self.level);
    }

    // Collapse the current lowest level down to a summary hash.
    // Will panic if there are not currently any buffers.
    fn collapse(&mut self) -> Result<Oid> {
        let buf = self.buffers.pop().unwrap();
        assert!(buf.len() > 0);
        if buf.len() == Oid::size() {
            trace!("collapse: single");
            Ok(Oid::from_raw(&buf))
        } else {
            let blevel = self.buffers.len();
            trace!("Collapse: {}, {}, {}", self.prefix, blevel, self.level);
            let kind = Kind::new(&format!("{}{}", self.prefix, self.level - blevel - 1)).unwrap();
            // let kind = Kind::new(&format!("{}0", self.prefix)).unwrap();
            let ch = chunk::new_plain(kind, buf);
            try!(self.sink.add(&*ch));

            // TODO: Implement a move out of the oid?
            trace!("collapsed: {}", ch.oid().to_hex());
            Ok(ch.oid().clone())
        }
    }

    // Finalize everything.
    pub fn finish(&mut self) -> Result<Oid> {
        trace!("Running finish: {} levels, l={}", self.buffers.len(), self.level);
        if self.buffers.is_empty() {
            // TODO: Make this more general.
            let ch = chunk::new_plain(Kind::new("NULL").unwrap(), vec![]);
            try!(self.sink.add(&*ch));
            Ok(ch.oid().clone())
        } else {
            loop {
                trace!("Collapse loop: {}", self.buffers.len());
                for buf in self.buffers.iter() {
                    trace!("  buf: {} long", buf.len());
                }
                let top = try!(self.collapse());
                if self.buffers.is_empty() {
                    return Ok(top);
                }
                let level = self.level - self.buffers.len();
                try!(self.add_level(&top, level));
            }
        }
    }

    // Get the last buffer.
    fn buf(&self) -> &Vec<u8> {
        &self.buffers[self.buffers.len()-1]
    }

    fn buf_mut(&mut self) -> &mut Vec<u8> {
        let last = self.buffers.len() - 1;
        &mut self.buffers[last]
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use Result;
    use cas::chunk::new_plain;
    use cas::chunk::Data;
    use cas::Chunk;
    use cas::Kind;
    use cas::Oid;
    use cas::pool::RamPool;
    use cas::pool::ChunkSource;

    #[test]
    fn indirect() {
        // Tacky.
        ::env_logger::init().unwrap();

        let pool = RamPool::new();
        let wr = pool.get_writer().unwrap();
        let mut ind = Write::new(&*wr, Oid::size() * 10, "IND".to_string());

        let limit = 2101;
        for i in 0 .. limit {
            let ch = integer_payload(i);
            wr.add(&*ch).unwrap();
            ind.add(ch.oid()).unwrap();
        }
        let top = ind.finish().unwrap();
        println!("finish: {}", top.to_hex());

        // Walk the result, and see if it is meaningful.
        let mut walker = Walker {
            pool: &pool,
            seq: 0,
        };
        walker.walk(&top).unwrap();
        assert_eq!(walker.seq, limit);
    }

    struct Walker<'a> {
        pool: &'a ChunkSource,
        seq: usize,
    }

    impl<'a> Walker<'a> {
        fn walk(&mut self, id: &Oid) -> Result<()> {
            let ch = try!(self.pool.find(id));
            let k = ch.kind().to_string();
            if &k[0..3] == "IND" {
                trace!("indirect ({}): {} bytes", k, ch.data_len());
                for id in oid_iter(&*ch) {
                    try!(self.walk(&id));
                }
            } else if k == "blob" {
                trace!("Data: {}", id.to_hex());
                let expect = integer_payload(self.seq);
                self.seq += 1;
                assert_eq!(id, expect.oid());
            } else {
                panic!("Unknown chunk kind");
            }
            Ok(())
        }
    }

    fn oid_iter<'a>(chunk: &'a Chunk) -> OidIter<'a> {
        OidIter {
            data: chunk.data(),
            offset: 0,
        }
    }

    struct OidIter<'a> {
        data: Data<'a>,
        offset: usize,
    }

    impl<'a> Iterator for OidIter<'a> {
        type Item = Oid;
        fn next(&mut self) -> Option<Oid> {
            if self.offset == self.data.len() {
                None
            } else {
                let result = Oid::from_raw(&self.data[self.offset..self.offset + Oid::size()]);
                self.offset += Oid::size();
                Some(result)
            }
        }
    }

    // Generate a chunk containing integer data.
    fn integer_payload(index: usize) -> Box<Chunk> {
        new_plain(Kind::new("blob").unwrap(), format!("{}", index).into_bytes())
    }
}
