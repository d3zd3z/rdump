//! The chunkstream files.

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use Chunk;
use Error;
use Kind;
use Oid;
use Result;
use std::io::{Read, Write};

// Each chunk contains a header
//  offset  length  field
//       0      16  chunk-magic
//      16       4  compressed length, amount stored in file.
//      20       4  uncompress length, or -1 for not compressed
//      24       4  kind
//      28      20  sha1 of type + uncompressed-data
//      48     clen data
//            0-15  padding
//
// The numbers are always represented in little endian, and the whole
// chunk is padded to a multiple of 16 bytes.

pub trait ChunkWrite {
    fn write_chunk(&mut self, chunk: &Chunk) -> Result<()>;
}

impl<T: Write> ChunkWrite for T {
    fn write_chunk(&mut self, chunk: &Chunk) -> Result<()> {
        let (clen, ulen, payload) = match chunk.zdata() {
            Some(zdata) => (zdata.len() as u32, chunk.data_len(), zdata),
            None => (chunk.data_len(), 0xFFFF_FFFF, chunk.data()),
        };

        let mut header = Vec::with_capacity(48);
        try!(header.write_all(b"adump-pool-v1.1\n"));
        try!(header.write_u32::<LittleEndian>(clen));
        try!(header.write_u32::<LittleEndian>(ulen));
        try!(header.write_all(&chunk.kind().bytes()));
        try!(header.write_all(&chunk.oid().0));

        try!(self.write_all(&header));
        try!(self.write_all(&payload));

        let pad_len = 15 & ((-(clen as i32)) as u32);
        if pad_len > 0 {
            let pad = vec![0; pad_len as usize];
            try!(self.write_all(&pad));
        }
        Ok(())
    }
}

pub trait ChunkRead {
    // Read a chunk from the stream.
    fn read_chunk(&mut self) -> Result<Chunk>;
}

impl<T: Read> ChunkRead for T {
    fn read_chunk(&mut self) -> Result<Chunk> {
        let mut header = vec![0u8; 48];
        try!(self.read_exact(&mut header));

        let mut header = &header[..];

        let mut magic = vec![0u8; 16];
        try!(header.read_exact(&mut magic));
        if magic != b"adump-pool-v1.1\n" {
            return Err(Error::CorruptChunk("Invalid magic".to_owned()));
        }
        let clen = try!(header.read_u32::<LittleEndian>());
        let ulen = try!(header.read_u32::<LittleEndian>());

        let mut kind = vec![0u8; 4];
        try!(header.read_exact(&mut kind));
        let kind = try!(String::from_utf8(kind));
        let kind = try!(Kind::new(&kind));

        let mut oid = vec![0u8; 20];
        try!(header.read_exact(&mut oid));
        let oid = Oid::from_raw(&oid);

        let mut payload = vec![0u8; clen as usize];
        if clen > 0 {
            try!(self.read_exact(&mut payload));
        }

        let pad_len = 15 & ((-(clen as i32)) as u32);
        if pad_len > 0 {
            let mut pad = vec![0; pad_len as usize];
            try!(self.read_exact(&mut pad));
        }

        if ulen == 0xFFFF_FFFF {
            Ok(Chunk::new_plain(kind, payload))
        } else {
            Ok(Chunk::new_compressed(kind, oid, payload, ulen))
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use super::*;
    use tempdir::TempDir;
    use testutil;

    #[test]
    fn test_write() {
        let tmp = TempDir::new("testfile").unwrap();
        let name = tmp.path().join("sample.data");

        {
            let mut fd = File::create(&name).unwrap();

            for size in testutil::boundary_sizes() {
                let ch = testutil::make_random_chunk(size, size);
                fd.write_chunk(&ch).unwrap();
            }
        }

        {
            let mut fd = File::open(&name).unwrap();

            for size in testutil::boundary_sizes() {
                let ch1 = testutil::make_random_chunk(size, size);
                let ch2 = fd.read_chunk().unwrap();
                assert_eq!(ch1.oid(), ch2.oid());
                assert_eq!(ch1.kind(), ch2.kind());
                assert_eq!(ch1.data_len(), ch2.data_len());
                assert_eq!(&ch1.data()[..], &ch2.data()[..]);
            }
        }
    }
}
