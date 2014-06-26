// Backup chunks.

use std::cell::RefCell;
use kind::Kind;
use oid::Oid;
use flate::{deflate_bytes_zlib, inflate_bytes_zlib};

#[cfg(test)]
use pdump::HexDump;

// Note that because the Chunks may compress and decompress lazily,
// the references can't be directly returned.

pub trait Chunk {
    // Return the kind associated with this chunk.
    fn kind(&self) -> Kind;

    // Return the Oid describing this chunk.
    fn oid<'a>(&'a self) -> &'a Oid;

    // Return a slice of the data for this chunk.
    // TODO: I'd actually like to be able to make this generic, but
    // rustc gives "error: cannot call a generic method through an
    // object"
    // fn with_data<U>(&self, f: |v: &[u8]| -> U) -> U;
    fn with_data(&self, f: |v: &[u8]|);

    // Sometimes, only the data length is needed, and can be computed
    // without decompressing the data.
    fn data_len(&self) -> uint;

    // There may be compressed data.
    fn with_zdata(&self, f: |v: Option<&[u8]>|);

    #[cfg(test)]
    fn dump(&self) {
        println!("Chunk: '{}' ({} bytes)", self.kind().textual(), self.data_len());
        self.with_data(|v| v.dump());
        self.with_zdata(|v| match v {
            None => println!("Uncompressible"),
            Some(v) => {
                println!("zdata:");
                v.dump();
            }
        });
    }
}

// Construct a plain chunk by taking the given data.
pub fn new_plain(kind: Kind, data: Vec<u8>) -> Box<Chunk> {
    box PlainChunk::new(kind, data) as Box<Chunk>
}

// If we know the oid, construct the chunk without computing it.
pub fn new_plain_with_oid(kind: Kind, oid: Oid, data: Vec<u8>) -> Box<Chunk> {
    box PlainChunk::new_with_oid(kind, oid, data) as Box<Chunk>
}

// Construct a chunk from compressed data.
pub fn new_compressed(kind: Kind, oid: Oid, zdata: Vec<u8>, data_len: uint) -> Box<Chunk> {
    box CompressedChunk::new(kind, oid, zdata, data_len) as Box<Chunk>
}

// There are different implementations of chunks, depending on where
// the data came from.  First, are Chunks derived from plain
// uncompressed data.
struct PlainChunk {
    kind: Kind,
    oid: Oid,
    data: Vec<u8>,

    // The compressed data is None for untried, Some(None) for
    // non-compressible data, and Some(Some(payload)) for data that
    // can be compressed.  This is wrapped in a RefCell to be able to
    // update this robustly.
    zdata: RefCell<Option<Option<Vec<u8>>>>,
}

impl PlainChunk {
    // Construct a new Chunk by copying the payload.
    fn new(kind: Kind, data: Vec<u8>) -> PlainChunk {
        let oid = Oid::from_data(kind, data.as_slice());
        PlainChunk {
            kind: kind,
            oid: oid,
            data: data,
            zdata: RefCell::new(None)
        }
    }

    // Construct a new Chunk, in the case where we know the OID.
    fn new_with_oid(kind: Kind, oid: Oid, data: Vec<u8>) -> PlainChunk {
        PlainChunk {
            kind: kind,
            oid: oid,
            data: data,
            zdata: RefCell::new(None)
        }
    }
}

impl Chunk for PlainChunk {
    fn kind(&self) -> Kind {
        self.kind
    }

    fn oid<'a>(&'a self) -> &'a Oid {
        &self.oid
    }

    fn with_data(&self, f: |v: &[u8]|) {
        f(self.data.as_slice())
    }

    fn data_len(&self) -> uint {
        self.data.len()
    }

    fn with_zdata(&self, f: |v: Option<&[u8]>|) {
        match *self.zdata.borrow() {
            Some(ref p) => {
                match p {
                    &None => f(None),
                    &Some(ref v) => f(Some(v.as_slice()))
                };
                return
            },
            None => ()
        }

        // Compression hasn't yet been tried.  Compress the data and
        // repeat.
        *self.zdata.borrow_mut() = Some({
            match deflate_bytes_zlib(self.data.as_slice()) {
                None => {
                    warn!("zlib wasn't able to compress");
                    None
                },
                Some(buf) => {
                    if buf.len() < self.data.len() {
                        Some(Vec::from_slice(buf.as_slice()))
                    } else {
                        None
                    }
                }
            }
        });

        match *self.zdata.borrow() {
            Some(ref p) => match p {
                &None => f(None),
                &Some(ref v) => f(Some(v.as_slice()))
            },
            None => unreachable!()
        }
    }
}

struct CompressedChunk {
    kind: Kind,
    oid: Oid,
    data: RefCell<Option<Vec<u8>>>,
    data_len: uint,
    zdata: Vec<u8>,
}

impl CompressedChunk {
    // Construct a new Chunk by copying the given compressed payload.
    fn new(kind: Kind, oid: Oid, zdata: Vec<u8>, data_len: uint) -> CompressedChunk {
        CompressedChunk {
            kind: kind,
            oid: oid,
            data: RefCell::new(None),
            data_len: data_len,
            zdata: zdata
        }
    }
}

impl Chunk for CompressedChunk {
    fn kind(&self) -> Kind {
        self.kind
    }

    fn oid<'a>(&'a self) -> &'a Oid {
        &self.oid
    }

    fn with_data(&self, f: |v: &[u8]|) {
        match *self.data.borrow() {
            Some(ref v) => return f(v.as_slice()),
            None => ()
        };

        // Need to decompress.
        *self.data.borrow_mut() = Some({
            match inflate_bytes_zlib(self.zdata.as_slice()) {
                None => fail!("zlib unable to inflate"),
                Some(buf) => Vec::from_slice(buf.as_slice())
            }
        });

        match *self.data.borrow() {
            Some(ref v) => f(v.as_slice()),
            None => unreachable!()
        };
    }

    fn data_len(&self) -> uint {
        self.data_len
    }

    fn with_zdata(&self, f: |v: Option<&[u8]>|) {
        f(Some(self.zdata.as_slice()))
    }
}

#[cfg(test)]
mod test {
    // use super::*; // Do we want this?
    use super::{new_compressed, new_plain};
    use testutil::{boundary_sizes, make_random_string};
    use flate::inflate_bytes_zlib;

    fn single_chunk(index: uint) {
        let p1 = make_random_string(index, index);
        let c1 = new_plain(kind!("blob"), Vec::from_slice(p1.as_bytes()));
        assert!(c1.kind() == kind!("blob"));
        c1.with_data(|p| assert!(p == p1.as_bytes()));

        c1.with_zdata(|p| match p {
            None => (),  // Fine if not compressible.
            Some(comp) => {
                match inflate_bytes_zlib(comp) {
                    None => fail!("Unable to decompress data"),
                    Some(raw) => assert!(raw.as_slice() == p1.as_bytes())
                };

                let c2 = new_compressed(c1.kind(), c1.oid().clone(), Vec::from_slice(comp), c1.data_len());
                assert!(c1.kind() == c2.kind());
                assert!(c1.oid() == c2.oid());
                c1.with_data(|v1| {
                    c2.with_data(|v2| assert!(v1 == v2))
                });
            }
        });
        // c1.dump();
    }

    #[test]
    fn basic() {
        for &size in boundary_sizes().iter() {
            single_chunk(size);
        }
    }
}
