// Backup chunks.

use std::ops::Deref;
use std::cell::{Ref, RefCell};

use kind::Kind;
use oid::Oid;
use zlib;

pub trait Chunk {
    /// Return the kind associated with this chunk.
    fn kind(&self) -> Kind;

    /// Return the Oid describing this chunk.
    fn oid<'a>(&'a self) -> &'a Oid;

    /// Get the uncompressed data of this chunk.
    fn data<'a>(&'a self) -> Data<'a>;

    /// Get the potentially compressed data of this chunk.
    fn zdata<'a>(&'a self) -> Option<Data<'a>>;

    /// Return the length of the data.
    ///
    /// Sometimes, only the length is needed, and can be determined without
    /// decompressing the data.
    fn data_len(&self) -> u32;

    /// Move the underlying uncompressed data out of the chunk.
    fn into_bytes(self: Box<Self>) -> Vec<u8>;
}

// Data from chunks may be coming out of either a direct vector, or a
// vector inside of a box.  This wraps the return result when borrowing
// data so that it can be up to the implementation to return the proper
// type.
pub enum Data<'a> {
    Ptr(&'a [u8]),
    Cell(Ref<'a, Compressed>),
    VecCell(Ref<'a, Option<Vec<u8>>>),
}

impl<'b> Deref for Data<'b> {
    type Target = [u8];

    #[inline]
    fn deref<'a>(&'a self) -> &'a [u8] {
        match *self {
            Data::Ptr(v) => v,
            Data::Cell(ref v) => {
                match **v {
                    Compressed::Compressed(ref p) => &p[..],
                    _ => unreachable!(),
                }
            },
            Data::VecCell(ref v) => {
                match **v {
                    Some(ref p) => &p[..],
                    _ => unreachable!(),
                }
            }
        }
    }
}

struct PlainChunk {
    kind: Kind,
    oid: Oid,
    data_: Vec<u8>,

    zdata_: RefCell<Compressed>,
}

pub enum Compressed {
    Untried,
    Uncompressible,
    Compressed(Vec<u8>),
}

pub fn new_plain(kind: Kind, data: Vec<u8>) -> Box<Chunk + 'static> {
    Box::new(PlainChunk::new(kind, data))
}

// Construct a chunk from compressed data.
pub fn new_compressed(kind: Kind, oid: Oid, zdata: Vec<u8>, data_len: u32) -> Box<Chunk + 'static> {
    Box::new(CompressedChunk::new(kind, oid, zdata, data_len))
}

impl PlainChunk {
    // Construct a Chunk by moving the payload.
    pub fn new(kind: Kind, data: Vec<u8>) -> PlainChunk {
        let oid = Oid::from_data(kind, &data[..]);
        PlainChunk {
            kind: kind,
            oid: oid,
            data_: data,
            zdata_: RefCell::new(Compressed::Untried),
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

    fn data<'a>(&'a self) -> Data<'a> {
        Data::Ptr(&self.data_[..])
    }

    fn zdata<'a>(&'a self) -> Option<Data<'a>> {
        {
            let cell = self.zdata_.borrow();
            match *cell {
                Compressed::Uncompressible => return None,
                Compressed::Compressed(_) => return Some(Data::Cell(cell)),
                _ => (),
            }
        }

        *self.zdata_.borrow_mut() = {
            match zlib::deflate(&self.data_[..]) {
                None => Compressed::Uncompressible,
                Some(buf) => Compressed::Compressed(buf),
            }
        };

        self.zdata()
    }

    fn data_len(&self) -> u32 {
        self.data_.len() as u32
    }

    fn into_bytes(self: Box<Self>) -> Vec<u8> {
        self.data_
    }
}

struct CompressedChunk {
    kind: Kind,
    oid: Oid,
    data: RefCell<Option<Vec<u8>>>,
    data_len: u32,
    zdata: Vec<u8>,
}

impl CompressedChunk {
    // Construct a new Chunk by copying the given compressed payload.
    fn new(kind: Kind, oid: Oid, zdata: Vec<u8>, data_len: u32) -> CompressedChunk {
        CompressedChunk {
            kind: kind,
            oid: oid,
            data: RefCell::new(None),
            data_len: data_len,
            zdata: zdata
        }
    }

    // Ensure that the data has been uncompressed.
    fn force_data(&self) {
        let mut cell = self.data.borrow_mut();
        match *cell {
            Some(_) => (),
            None => {
                *cell = match zlib::inflate(&self.zdata[..], self.data_len() as usize) {
                    None => panic!("zlib unable to inflate"),
                    Some(buf) => Some(buf),
                };
            }
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

    fn data<'a>(&'a self) -> Data<'a> {
        self.force_data();
        let cell = self.data.borrow();
        match *cell {
            Some(_) => return Data::VecCell(cell),
            _ => unreachable!(),
        }
    }

    fn data_len(&self) -> u32 {
        self.data_len
    }

    fn zdata<'a>(&'a self) -> Option<Data<'a>> {
        Some(Data::Ptr(&self.zdata[..]))
    }

    fn into_bytes(self: Box<Self>) -> Vec<u8> {
        self.force_data();
        match self.data.into_inner() {
            None => unreachable!(),
            Some(data) => data,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use testutil::{boundary_sizes, make_random_string};
    use kind::Kind;
    use zlib;

    fn single_chunk(index: u32) {
        let p1 = make_random_string(index, index);
        let c1 = new_plain(Kind::new("blob").unwrap(), p1.clone().into_bytes());
        assert_eq!(c1.kind(), Kind::new("blob").unwrap());
        assert_eq!(&c1.data()[..], p1.as_bytes());

        match c1.zdata() {
            None => (), // Fine if not compressible..
            Some(ref comp) => {
                match zlib::inflate(&comp[..], p1.len()) {
                    None => panic!("Unable to decompress data"),
                    Some(raw) => assert_eq!(&raw[..], p1.as_bytes()),
                };

                // Make a new chunk out of the compressed data.
                let c2 = new_compressed(c1.kind(), c1.oid().clone(), comp[..].to_vec(), c1.data_len());
                assert_eq!(c1.kind(), c2.kind());
                assert_eq!(c1.oid(), c2.oid());

                assert_eq!(&c1.data()[..], &c2.data()[..]);

                // Ensure we can pull the uncompressed data out.
                let d2 = c2.into_bytes();
                assert_eq!(&c1.data()[..], &d2[..]);
            }
        };
    }

    #[test]
    fn basic() {
        for size in boundary_sizes() {
            single_chunk(size);
        }
    }
}
