// Test data.

use std::io;

use cas::Oid;
use cas::pool::RamPool;
use cas::pool::ChunkSource;
use filer::data::DataWrite;

use rand::isaac::IsaacRng;
use rand::Rng;

extern crate cas;
extern crate filer;
extern crate rand;
#[macro_use] extern crate log;

#[test]
fn indirection() {
    let limit = 1 * 1024 * 1024 + 136;

    let mut pool = RamPool::new();
    let top;
    {
        let pw = pool.get_writer().unwrap();
        {
            let mut rd = FakeRead::new(limit);
            let mut wr = DataWrite::new_limit(&*pw, 256 * 1024);
            top = wr.write(&mut rd).unwrap();
        }
        pw.flush().unwrap();
    }

    // Read it back and make sure it is ok.
    {
        let mut w = Walker::new(&pool, limit);
        w.walk(&top).unwrap();
    }
}

struct Walker<'a> {
    reader: FakeRead,
    pool: &'a ChunkSource,
}

impl<'a> Walker<'a> {
    fn new(pool: &ChunkSource, limit: usize) -> Walker {
        Walker {
            reader: FakeRead::new(limit),
            pool: pool,
        }
    }

    fn walk(&mut self, oid: &Oid) -> cas::Result<()> {
        use filer::decode::decode;
        use filer::decode::Node;
        use std::io::prelude::*;

        let ch = try!(self.pool.find(oid));
        trace!("Chunk: {}", ch.oid().to_hex());
        match try!(decode(ch)) {
            Node::Blob(data) => {
                let mut temp = vec![0u8; data.len()];
                assert_eq!(try!(self.reader.read(&mut temp)), temp.len());
                assert_eq!(&data, &temp);
            },
            Node::Indirect { level, children } => {
                trace!("Indirect: {} {}", level, children.len());
                for child in children.iter() {
                    try!(self.walk(child));
                }
            },
        }
        Ok(())
    }
}

// The IsaacRng fills based on 32-bit values.  Because of this, calls to
// fill_bytes() don't work right if the fill amount is not a multiple of 4.
// Test that it does work right with unusual 4-byte values.
#[test]
fn fill_bytes() {
    // use cas::pdump::HexDump;

    let mut arng = IsaacRng::new_unseeded();
    let mut brng = arng.clone();

    let mut b1 = vec![0u8; 256];
    arng.fill_bytes(&mut b1);
    // println!("{}", b1.len());
    // b1.dump();

    let mut b2 = vec![0u8; 256];
    brng.fill_bytes(&mut b2[0..8]);
    brng.fill_bytes(&mut b2[8..32]);
    brng.fill_bytes(&mut b2[32..128]);
    brng.fill_bytes(&mut b2[128..248]);
    brng.fill_bytes(&mut b2[248..256]);
    // println!("Second");
    // b2.dump();
    assert_eq!(&b1, &b2);
}

// A fake reader that always provides data, up to a given length.
struct FakeRead {
    offset: usize,
    limit: usize,
    rng: IsaacRng,
}

impl FakeRead {
    fn new(limit: usize) -> FakeRead {
        FakeRead {
            offset: 0,
            limit: limit,
            rng: IsaacRng::new_unseeded(),
        }
    }
}

impl io::Read for FakeRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut size = buf.len();
        if self.offset + size > self.limit {
            size = self.limit - self.offset;
        }

        // The IsaacRng fills based on 32-bit values, which is discards
        // across calls.  As such, this only is possible with 32-bit
        // aligned buffers.  This should be OK, as long as the test data
        // above respects this.  Check alignment to 8-bytes, since that
        // seems to not be guaranteed on 64-bit.
        assert!(size & 7 == 0);

        self.rng.fill_bytes(buf);

        self.offset += size;
        Ok(size)
    }
}
