// Adump file format.

// use byteorder::{
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use Error;
use Kind;
use Oid;
use Result;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

pub trait Index {
    fn contains_key(&self, key: &Oid) -> bool;
    fn get(&self, key: &Oid) -> Option<IndexInfo>;
}

pub trait IndexUpdate {
    // Like a map insert, but panics if the key is already present.
    fn insert(&mut self, key: Oid, kind: Kind, offset: u32);
}

// The RAM Index
#[cfg(all(feature = "nightly", test))]
mod bench {
    use crypto::aessafe::AesSafe128Encryptor;
    use crypto::symmetriccipher::BlockEncryptor;
    use rand::chacha::ChaChaRng;
    use rand::Rng;
    use test;
    use Kind;
    use Oid;
    use std::mem;

    #[bench]
    fn bench_oid(b: &mut test::Bencher) {
        b.iter(|| {
            Oid::from_data(Kind::new("blob").unwrap(), b"12345");
        })
    }

    #[bench]
    fn bench_aes(b: &mut test::Bencher) {
        let enc = AesSafe128Encryptor::new(b"1234567890123456");
        b.iter(|| {
            let input = vec![0u8; 16];
            let mut output = vec![0u8; 16];
            enc.encrypt_block(&input, &mut output);
        })
    }

    #[bench]
    fn bench_chacha(b: &mut test::Bencher) {
        let mut rng = ChaChaRng::new_unseeded();
        b.iter(|| {
            let mut oid: Oid = unsafe { mem::uninitialized() };
            let mut raw = oid.as_mut_bytes();
            // rng.set_counter(0, 5*20);
            rng.fill_bytes(raw);
        })
    }
}

// In memory index.
pub struct RamIndex(pub BTreeMap<Oid, IndexInfo>);

#[derive(Debug, Clone)]
pub struct IndexInfo {
    offset: u32,
    kind: Kind,
}

impl RamIndex {
    pub fn new() -> RamIndex {
        RamIndex(BTreeMap::new())
    }

    pub fn insert(&mut self, id: Oid, offset: u32, kind: Kind) {
        self.0.insert(id, IndexInfo { offset: offset, kind: kind });
    }

    // This doesn't really belong here, since this will generally come from
    // a combination of two of these.
    pub fn save<P: AsRef<Path>>(&self, path: P, size: u32) -> Result<()> {
        let tmp_name = try!(tmpify(path.as_ref()));
        println!("tmp: {:?} -> {:?}", tmp_name, path.as_ref());
        {
            let ofd = try!(File::create(&tmp_name));
            let mut ofd = BufWriter::new(ofd);

            try!(ofd.write_all(b"ldumpidx"));
            try!(ofd.write_u32::<LittleEndian>(4));
            try!(ofd.write_u32::<LittleEndian>(size));

            // Write the top-level index.
            let top = self.compute_top();
            for elt in top {
                try!(ofd.write_u32::<LittleEndian>(elt));
            }

            // Write out the hashes themselves.
            for h in self.0.keys() {
                try!(ofd.write_all(&h.0));
            }

            // Write out the offset table.
            for ri in self.0.values() {
                try!(ofd.write_u32::<LittleEndian>(ri.offset));
            }

            // Compute the kind map.
            let mut kinds = vec![];
            let mut kind_map = BTreeMap::new();
            for ri in self.0.values() {
                if !kind_map.contains_key(&ri.kind) {
                    kind_map.insert(ri.kind, kinds.len());
                    kinds.push(ri.kind);
                }
            }

            // Write out the kind map itself.
            try!(ofd.write_u32::<LittleEndian>(kinds.len() as u32));
            for &k in &kinds {
                try!(ofd.write_u32::<LittleEndian>(k.0));
            }

            // Then write out the values.
            let mut buf = Vec::with_capacity(self.0.len());
            for ri in self.0.values() {
                buf.push(kind_map[&ri.kind] as u8);
            }
            try!(ofd.write_all(&buf));
        }

        // It worked, so do the atomic rename/overwrite.  'std' tries to do
        // this sane behavior on Windows as well.
        try!(fs::rename(tmp_name, path.as_ref()));

        Ok(())
    }

    // Compute the "top" index.  Each index gives the first hash with a
    // first byte greater than that particular index offset.
    fn compute_top(&self) -> Vec<u32> {
        let mut top = Vec::with_capacity(256);

        let mut iter = self.0.keys().enumerate().peekable();
        for first in 0 .. 256 {
            // Scan until we hit a value that is too large.
            loop {
                match iter.peek() {
                    None => break,
                    Some(&(_, key)) => {
                        if key[0] as usize > first {
                            break;
                        }
                        iter.next();
                    },
                }
            }
            let index = match iter.peek() {
                None => self.0.len(),
                Some(&(n, _)) => n,
            };
            top.push(index as u32);
        }

        top
    }
}

impl Index for RamIndex {
    fn contains_key(&self, key: &Oid) -> bool {
        self.0.contains_key(key)
    }

    fn get(&self, key: &Oid) -> Option<IndexInfo> {
        self.0.get(key).cloned()
    }
}

impl IndexUpdate for RamIndex {
    fn insert(&mut self, key: Oid, kind: Kind, offset: u32) {
        match self.0.insert(key, IndexInfo {
            kind: kind,
            offset: offset,
        }) {
            None => (),
            Some(_) => panic!("Duplicate key inserted into index"),
        }
    }
}

// Given a filename, generate another with a ".tmp" suffix, if possible.
fn tmpify(path: &Path) -> Result<PathBuf> {
    let base = path
        .file_name()
        .ok_or_else(|| Error::PathError(format!("path does not have a filename {:?}", path)));
    let base = try!(base);

    let base = base.to_str()
        .ok_or_else(|| Error::PathError(format!("path isn't valid UTF-8 {:?}", path)));
    let base = try!(base);

    let tmp = format!("{}.tmp", base);
    Ok(path.with_file_name(&tmp))
}

// Represents the in-memory format for a single index file.  There is a
// tradeoff here between load time (reading and decoding the file, or using
// accessors to decode the file as it is read).  There really isn't a way
// to compare these other than two try both approaches, and benchmark the
// results.
//
// This IndexFile uses the byteorder crate to read and decode the data.
#[allow(dead_code)]
pub struct IndexFile {
    top: Vec<u32>,
    offsets: Vec<u32>,
    oids: Vec<Oid>,
    kind_names: Vec<Kind>,
    kinds: Vec<u8>,
}

impl IndexFile {
    /// Try loading the given named index file, returning it if it is
    /// valid.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<IndexFile> {
        let f = try!(File::open(path));
        let mut rd = BufReader::new(f);

        let mut magic = vec![0u8; 8];
        try!(rd.read_exact(&mut magic));
        if magic != b"ldumpidx" {
            return Err(Error::InvalidIndex("bad magic".to_owned()));
        }

        let version = try!(rd.read_u32::<LittleEndian>());
        if version != 4 {
        }

        let _file_size = try!(rd.read_u32::<LittleEndian>());
        // TODO: Process this
        // The file_size is the number of bytes in the pool file.  If this
        // differs, it indicates that this index doesn't match the file,
        // and should be regenerated.

        let mut top = Vec::with_capacity(256);
        for _ in 0 .. 256 {
            top.push(try!(rd.read_u32::<LittleEndian>()));
        }

        let size = *top.last().unwrap() as usize;

        let mut oid_buf = vec![0u8; 20];
        let mut oids = Vec::with_capacity(size);
        for _ in 0 .. size {
            try!(rd.read_exact(&mut oid_buf));
            oids.push(Oid::from_raw(&oid_buf));
        }

        let mut offsets = Vec::with_capacity(size);
        for _ in 0 .. size {
            offsets.push(try!(rd.read_u32::<LittleEndian>()));
        }

        let kind_count = try!(rd.read_u32::<LittleEndian>()) as usize;
        let mut kind_names = Vec::with_capacity(size);
        for _ in 0 .. kind_count {
            let mut kind_buf = vec![0u8; 4];
            try!(rd.read_exact(&mut kind_buf));
            let text = try!(String::from_utf8(kind_buf));
            kind_names.push(try!(Kind::new(&text)));
        }

        let mut kinds = vec![0u8; kind_count];
        try!(rd.read_exact(&mut kinds));

        Ok(IndexFile {
            top: top,
            offsets: offsets,
            oids: oids,
            kind_names: kind_names,
            kinds: kinds,
        })
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use {Kind, Oid};
    use super::*;

    struct Tracker {
        nodes: BTreeMap<u32, Kind>,
        // size: u32,
        kinds: Vec<Kind>,
    }

    impl Tracker {
        fn new() -> Tracker {
            let mut kinds = vec![];
            kinds.push(Kind::new("blob").unwrap());
            kinds.push(Kind::new("idx0").unwrap());
            kinds.push(Kind::new("idx1").unwrap());
            kinds.push(Kind::new("data").unwrap());
            kinds.push(Kind::new("dir ").unwrap());

            Tracker {
                nodes: BTreeMap::new(),
                // size: 0,
                kinds: kinds,
            }
        }

        fn add<U: IndexUpdate>(&mut self, index: &mut U, num: u32) {
            if self.nodes.contains_key(&num) {
                panic!("Test error, duplicate: {}", num);
            }
            let kind = self.kinds[num as usize % self.kinds.len()];
            index.insert(Oid::from_u32(num), kind, num);
            self.nodes.insert(num, kind);
        }

        fn check<I: Index>(&self, index: &I) {
            // Ensure we can find each node.
            for (&num, &kind) in &self.nodes {
                let oid = Oid::from_u32(num);

                assert!(index.contains_key(&oid));
                match index.get(&oid) {
                    None => panic!("Couldn't find key"),
                    Some(info) => {
                        assert_eq!(info.offset, num);
                        assert_eq!(info.kind, kind);
                    },
                }

                let oid2 = oid.inc();
                assert!(!index.contains_key(&oid2));
                match index.get(&oid2) {
                    None => (),
                    Some(_) => panic!("Key should not be present"),
                }

                let oid3 = oid.dec();
                assert!(!index.contains_key(&oid3));
                match index.get(&oid3) {
                    None => (),
                    Some(_) => panic!("Key should not be present"),
                }
            }
        }
    }

    #[test]
    fn test_index() {
        let mut track = Tracker::new();
        let mut ri = RamIndex::new();

        for ofs in 0 .. 10000 {
            track.add(&mut ri, ofs);
        }

        track.check(&ri);

        ri.save("haha.idx", 12345).unwrap();

        let r2 = IndexFile::load("haha.idx" /*, 12345*/).unwrap();
    }
}
