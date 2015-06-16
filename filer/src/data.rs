// Filedata storage.

#![allow(dead_code)]

use Result;
use indirect;
use std::io;
use std::io::ErrorKind;
use std::iter;
use cas::pool::ChunkSink;
use cas::chunk;
use cas::{Kind, Oid};

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
    pub fn write<'b>(&mut self, source: &'b mut io::Read) -> Result<Oid> {
        let mut ind = indirect::Write::new(self.sink, self.limit, "IND".to_string());
        loop {
            let buf = try!(self.fill(source));
            if buf.len() == 0 {
                break;
            }

            let ch = chunk::new_plain(Kind::new("blob").unwrap(), buf);
            try!(self.sink.add(&*ch));
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

#[cfg(test)]
mod test {
    use super::*;

    use std::io;

    use cas::pool;
    use cas::pool::FilePool;
    use tempdir::TempDir;

    // A simple reader that reads up to a given length, simply fulfilling
    // all of the read requests.
    struct FakeRead {
        offset: usize,
        limit: usize,
    }

    impl FakeRead {
        fn new(limit: usize) -> FakeRead {
            FakeRead {
                offset: 0,
                limit: limit,
            }
        }
    }

    impl io::Read for FakeRead {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let mut size = buf.len();
            if self.offset + size > self.limit {
                size = self.limit - self.offset;
            }

            for i in 0 .. size {
                buf[i] = 0;
            }

            let mut tmp = self.offset;
            let mut pos = 0;
            while tmp > 0 && pos < size {
                buf[pos] = (tmp & 0xFF) as u8;
                pos += 1;
                tmp >>= 8;
            }

            self.offset += size;
            Ok(size)
        }
    }

    #[test]
    fn data() {
        let tmp = TempDir::new("data").unwrap();
        let path = tmp.path().join("pool");
        FilePool::create(&path).unwrap();
        let pool = pool::open(&path).unwrap();
        {
            let pw = pool.get_writer().unwrap();
            {
                let mut rd = FakeRead::new(1 * 1024 * 1024 + 137);
                let mut wr = DataWrite::new_limit(&*pw, 1024);
                wr.write(&mut rd).unwrap();
            }
            pw.flush().unwrap();
        }
    }
}
