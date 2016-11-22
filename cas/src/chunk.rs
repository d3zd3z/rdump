// Backup chunks.

use std::ops::Deref;
use std::cell::{Ref, RefCell};

use kind::Kind;
use oid::Oid;
use zlib;

// A `Chunk` is a single unit of backup.  It has a 'kind' which is a
// 4-byte identifier, and 0 or more bytes of data.  It is identified
// by the SHA-1 hash of the kind followed by the data.  This structure
// is the in-memory representation of a Chunk, including some
// complexity to allow the origin to be compressed or uncompressed,
// and the other information to be computed lazily as needed.
pub struct Chunk {
    kind: Kind,
    oid: Oid,
    data_len: u32,

    // These are both optional, since one can be derived from the
    // other.  It is non-sensical to have neither present.
    data: RefCell<Option<Vec<u8>>>,
    zdata: RefCell<Compressed>,
}

impl Chunk {
    /// Construct a new chunk out of some uncompressed data.
    pub fn new_plain(kind: Kind, data: Vec<u8>) -> Chunk {
        let oid = Oid::from_data(kind, &data[..]);
        let dlen = data.len();
        assert!(dlen <= 0x7ffffff);
        Chunk {
            kind: kind,
            oid: oid,
            data: RefCell::new(Some(data)),
            data_len: dlen as u32,
            zdata: RefCell::new(Compressed::Untried),
        }
    }

    /// Construct a new chunk out of the compressed representation of a
    /// chunk.  The `data_len` must match the size of the 'zdata' when
    /// it is decompressed, and the `oid` must match the SHA1 hash, per
    /// the style of chunks described above.
    pub fn new_compressed(kind: Kind, oid: Oid, zdata: Vec<u8>, data_len: u32) -> Chunk {
        Chunk {
            kind: kind,
            oid: oid,
            data: RefCell::new(None),
            data_len: data_len,
            zdata: RefCell::new(Compressed::Compressed(zdata)),
        }
    }

    /// Return the kind asociated with this chunk.
    pub fn kind(&self) -> Kind {
        self.kind
    }

    /// Return the Oid identifying this chunk.
    pub fn oid<'a>(&'a self) -> &'a Oid {
        &self.oid
    }

    /// Return the length of the data.
    pub fn data_len(&self) -> u32 {
        self.data_len
    }

    /// Return a view of the compressed data within this chunk, if that
    /// results in a smaller block of data.
    pub fn zdata<'a>(&'a self) -> Option<Data<'a>> {
        // If we already have knowledge of the compression result, just
        // return it.
        {
            let cell = self.zdata.borrow();
            match *cell {
                Compressed::Uncompressible => return None,
                Compressed::Compressed(_) => return Some(Data::Cell(cell)),
                _ => (),
            }
        }

        // If we get here, it means we haven't attempted compression.
        let data = self.data.borrow();
        let data = match *data {
            Some(ref payload) => payload,
            None => panic!("Constructed a chunk with no data"),
        };

        *self.zdata.borrow_mut() = {
            match zlib::deflate(&data[..]) {
                None => Compressed::Uncompressible,
                Some(buf) => Compressed::Compressed(buf),
            }
        };

        // And recurse to get the result.
        self.zdata()
    }

    /// Return a reference to the data.
    pub fn data<'a>(&'a self) -> Data<'a> {
        self.force_data();
        let cell = self.data.borrow();
        match *cell {
            // TODO: Ref::map() might make this easier some day.
            Some(_) => return Data::VecCell(cell),
            _ => unreachable!(),
        }
    }

    /// Move the uncompressed data out of the chunk.
    pub fn into_bytes(self) -> Vec<u8> {
        self.force_data();
        match self.data.into_inner() {
            None => unreachable!(),
            Some(data) => data,
        }
    }

    // Ensure that the data has been uncompressed.
    fn force_data(&self) {
        let mut cell = self.data.borrow_mut();
        match *cell {
            Some(_) => (),
            None => {
                let zdata = self.zdata.borrow();
                let zdata = match *zdata {
                    Compressed::Compressed(ref buf) => buf,
                    _ => panic!("Improperly constructed chunk"),
                };

                *cell = match zlib::inflate(&zdata[..], self.data_len() as usize) {
                    None => panic!("zlib unable to inflate"),
                    Some(buf) => Some(buf),
                };
            }
        }
    }
}

pub enum Compressed {
    Untried,
    Uncompressible,
    Compressed(Vec<u8>),
}

// Data from chunks may be coming out of either a direct vector, or a
// vector inside of a box.  This wraps the return result when borrowing
// data so that it can be up to the implementation to return the proper
// type.
pub enum Data<'a> {
    Cell(Ref<'a, Compressed>),
    VecCell(Ref<'a, Option<Vec<u8>>>),
}

// TODO: Implement index for this (if this helps).
impl<'b> Deref for Data<'b> {
    type Target = [u8];

    #[inline]
    fn deref<'a>(&'a self) -> &'a [u8] {
        match *self {
            Data::Cell(ref v) => {
                match **v {
                    Compressed::Compressed(ref p) => &p[..],
                    _ => unreachable!(),
                }
            }
            Data::VecCell(ref v) => {
                match **v {
                    Some(ref p) => &p[..],
                    _ => unreachable!(),
                }
            }
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
        let c1 = Chunk::new_plain(Kind::new("blob").unwrap(), p1.clone().into_bytes());
        assert_eq!(c1.kind(), Kind::new("blob").unwrap());
        assert_eq!(&c1.data()[..], p1.as_bytes());

        match c1.zdata() {
            None => (), // Fine if not compressible.
            Some(ref comp) => {
                match zlib::inflate(&comp[..], p1.len()) {
                    None => panic!("Unable to decompress data"),
                    Some(raw) => assert_eq!(&raw[..], p1.as_bytes()),
                }

                // Make a new chunk out of the compressed data.
                let c2 = Chunk::new_compressed(c1.kind(),
                                               c1.oid().clone(),
                                               comp[..].to_vec(),
                                               c1.data_len());
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
