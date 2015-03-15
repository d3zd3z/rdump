// Backup chunks.

use kind::Kind;
use oid::Oid;
use std::cell::RefCell;
use std::cell::Ref as CellRef;

use flate::{deflate_bytes_zlib, inflate_bytes_zlib};

#[cfg(test)]
use pdump::HexDump;

// Note that because the Chunks may compress and decompress lazily,
// the references can't be directly returned.

pub trait Chunk {
    /// Return the kind associated with this chunk.
    fn kind(&self) -> Kind;

    /// Return the Oid describing this chunk.
    fn oid<'a>(&'a self) -> &'a Oid;

    /// Get the uncompressed data of this chunk.
    fn data<'a>(&'a self) -> Data<'a>;

    /// Get the compressed data of this chunk.
    fn zdata<'a>(&'a self) -> Option<Data<'a>>;

    /// Return the length of the data.
    ///
    /// Sometimes, only the data length is needed, and can be determined
    /// without decompressing the data.
    fn data_len(&self) -> u32;

    #[cfg(test)]
    fn dump(&self) {
        println!("Chunk: '{}' ({} bytes)", self.kind().textual(), self.data_len());
        self.data().as_slice().dump();
        match self.zdata() {
            None => println!("Uncompressible"),
            Some(ref v) => {
                println!("zdata:");
                v.as_slice().dump();
            }
        }
    }
}

// Data from chunks may be coming out of either a direct vector, or a vector
// inside of a box.  This wraps the return result when borrowing data so that
// it can be up to the implementation to return the proper type.
pub enum Data<'a> {
    Ptr(&'a [u8]),
    Cell(CellRef<'a, Compressed>),
    VecCell(CellRef<'a, Option<Vec<u8>>>),
}

// For now, just implement AsSlice and anything else can be determined by the
// slice.
impl<'b> AsSlice<u8> for Data<'b> {
    fn as_slice<'a>(&'a self) -> &'a [u8] {
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
            },
        }
    }
}

// Construct a plain chunk by taking the given data.
pub fn new_plain(kind: Kind, data: Vec<u8>) -> Box<Chunk + 'static> {
    Box::new(PlainChunk::new(kind, data))
}

/*
// If we know the oid, construct the chunk without computing it.
pub fn new_plain_with_oid(kind: Kind, oid: Oid, data: Vec<u8>) -> Box<Chunk> {
    box PlainChunk::new_with_oid(kind, oid, data) as Box<Chunk>
}
*/

// Construct a chunk from compressed data.
pub fn new_compressed(kind: Kind, oid: Oid, zdata: Vec<u8>, data_len: u32) -> Box<Chunk + 'static> {
    Box::new(CompressedChunk::new(kind, oid, zdata, data_len))
}

// There are different implementations of chunks, depending on where
// the data came from.  First, are Chunks derived from plain
// uncompressed data.
struct PlainChunk {
    kind: Kind,
    oid: Oid,
    data_: Vec<u8>,

    // The compressed data is None for untried, Some(None) for
    // non-compressible data, and Some(Some(payload)) for data that
    // can be compressed.  This is wrapped in a RefCell to be able to
    // update this robustly.
    zdata_: RefCell<Compressed>,
}

// There are pros and cons of having this data in a single enum, rather than
// having an additional Cell that holds the state.  This case makes access a
// little more complicated, but simplifies use.
pub enum Compressed {
    Untried,
    Uncompressible,
    Compressed(Vec<u8>),
}

impl PlainChunk {
    // Construct a new Chunk by copying the payload.
    fn new(kind: Kind, data: Vec<u8>) -> PlainChunk {
        let oid = Oid::from_data(kind, data.as_slice());
        PlainChunk {
            kind: kind,
            oid: oid,
            data_: data,
            zdata_: RefCell::new(Compressed::Untried)
        }
    }

/*
    // Construct a new Chunk, in the case where we know the OID.
    fn new_with_oid(kind: Kind, oid: Oid, data: Vec<u8>) -> PlainChunk {
        PlainChunk {
            kind: kind,
            oid: oid,
            data: data,
            zdata: RefCell::new(None)
        }
    }
*/
}

impl Chunk for PlainChunk {
    fn kind(&self) -> Kind {
        self.kind
    }

    fn oid<'a>(&'a self) -> &'a Oid {
        &self.oid
    }

    fn data<'a>(&'a self) -> Data<'a> {
        Data::Ptr(self.data_.as_slice())
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
            match deflate_bytes_zlib(self.data_.as_slice()) {
                None => {
                    warn!("zlib wasn't able to compress");
                    Compressed::Uncompressible
                },
                Some(buf) => {
                    if buf.len() < self.data_.len() {
                        Compressed::Compressed(buf.as_slice().to_vec())
                    } else {
                        Compressed::Uncompressible
                    }
                }
            }
        };

        self.zdata()
    }

    fn data_len(&self) -> u32 {
        self.data_.len() as u32
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
}

impl Chunk for CompressedChunk {
    fn kind(&self) -> Kind {
        self.kind
    }

    fn oid<'a>(&'a self) -> &'a Oid {
        &self.oid
    }

    fn data<'a>(&'a self) -> Data<'a> {
        {
            let cell = self.data.borrow();
            match *cell {
                Some(_) => return Data::VecCell(cell),
                _ => ()
            }
        }

        *self.data.borrow_mut() = Some({
            match inflate_bytes_zlib(self.zdata.as_slice()) {
                None => panic!("zlib unable to inflate"),
                Some(buf) => buf.as_slice().to_vec(),
            }
        });

        self.data()
    }

    fn data_len(&self) -> u32 {
        self.data_len
    }

    fn zdata<'a>(&'a self) -> Option<Data<'a>> {
        Some(Data::Ptr(self.zdata.as_slice()))
    }
}

#[cfg(test)]
mod test {
    use super::{new_plain, new_compressed};
    use testutil::{boundary_sizes, make_random_string};
    use flate::inflate_bytes_zlib;

    fn single_chunk(index: u32) {
        let p1 = make_random_string(index, index);
        let c1 = new_plain(kind!("blob"), p1.clone().into_bytes());
        assert!(c1.kind() == kind!("blob"));
        assert!(c1.data().as_slice() == p1.as_bytes());

        match c1.zdata() {
            None => (), // Fine if not compressible.
            Some(ref comp) => {
                match inflate_bytes_zlib(comp.as_slice()) {
                    None => panic!("Unable to decompress data"),
                    Some(raw) => assert!(raw.as_slice() == p1.as_bytes()),
                };

                // Make a new chunk out of the compressed data.
                let c2 = new_compressed(c1.kind(), c1.oid().clone(), comp.as_slice().to_vec(), c1.data_len());
                assert!(c1.kind() == c2.kind());
                assert!(c1.oid() == c2.oid());

                assert!(c1.data().as_slice() == c2.data().as_slice());
            },
        }

        // c1.dump();
    }

    #[test]
    fn basic() {
        for &size in boundary_sizes().iter() {
            single_chunk(size);
        }
    }
}
