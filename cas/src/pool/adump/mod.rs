// Adump file format.

// use byteorder::{
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use Error;
use Kind;
use Oid;
use Result;
use std::collections::BTreeMap;
use std::collections::btree_map;
use std::fs::{self, File};
use std::iter::Chain;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

pub trait Index {
    fn contains_key(&self, key: &Oid) -> bool;
    fn get(&self, key: &Oid) -> Option<IndexInfo>;
}

pub trait IndexUpdate {
    // Like a map insert, but panics if the key is already present.
    fn insert(&mut self, key: Oid, offset: u32, kind: Kind);
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
    fn insert(&mut self, key: Oid, offset: u32, kind: Kind) {
        match self.0.insert(key, IndexInfo {
            kind: kind,
            offset: offset,
        }) {
            None => (),
            Some(_) => panic!("Duplicate key inserted into index"),
        }
    }
}

pub struct IterItem<'a> {
    oid: &'a Oid,
    kind: Kind,
    offset: u32,
}

impl<'a> IntoIterator for &'a RamIndex {
    type Item = IterItem<'a>;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        Iter(self.0.iter())
    }
}

pub struct Iter<'a>(btree_map::Iter<'a, Oid, IndexInfo>);

impl<'a> Iterator for Iter<'a> {
    type Item = IterItem<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(oid, info)| IterItem {
            oid: oid,
            kind: info.kind,
            offset: info.offset
        })
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
    pub fn load<P: AsRef<Path>>(path: P, size: u32) -> Result<IndexFile> {
        let f = try!(File::open(path));
        let mut rd = BufReader::new(f);

        let mut magic = vec![0u8; 8];
        try!(rd.read_exact(&mut magic));
        if magic != b"ldumpidx" {
            return Err(Error::InvalidIndex("bad magic".to_owned()));
        }

        let version = try!(rd.read_u32::<LittleEndian>());
        if version != 4 {
            return Err(Error::InvalidIndex("Version mismatch".to_owned()));
        }

        let file_size = try!(rd.read_u32::<LittleEndian>());
        if file_size != size {
            return Err(Error::InvalidIndex("Index size mismatch".to_owned()));
        }
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

        let mut kinds = vec![0u8; size];
        try!(rd.read_exact(&mut kinds));

        Ok(IndexFile {
            top: top,
            offsets: offsets,
            oids: oids,
            kind_names: kind_names,
            kinds: kinds,
        })
    }

    /// Construct an empty index, that contains no values.
    pub fn empty() -> IndexFile {
        IndexFile {
            top: vec![0; 256],
            offsets: vec![],
            oids: vec![],
            kind_names: vec![],
            kinds: vec![],
        }
    }

    /// Save an index from something that can be iterated over.
    pub fn save<'a, P: AsRef<Path>, I>(path: P, size: u32, index: I) -> Result<()>
        where I: IntoIterator<Item=IterItem<'a>>
    {
        let mut nodes: Vec<IterItem<'a>> = index.into_iter().collect();
        nodes.sort_by_key(|n| n.oid);
        let nodes = nodes;

        let tmp_name = try!(tmpify(path.as_ref()));
        println!("tmp: {:?} -> {:?}", tmp_name, path.as_ref());
        {
            let ofd = try!(File::create(&tmp_name));
            let mut ofd = BufWriter::new(ofd);

            try!(ofd.write_all(b"ldumpidx"));
            try!(ofd.write_u32::<LittleEndian>(4));
            try!(ofd.write_u32::<LittleEndian>(size));

            // Write the top-level index.
            let top = compute_top(&nodes);
            for elt in top {
                try!(ofd.write_u32::<LittleEndian>(elt));
            }

            // Write out the hashes themselves.
            for n in &nodes {
                try!(ofd.write_all(&n.oid.0));
            }

            // Write out the offset table.
            for n in &nodes {
                try!(ofd.write_u32::<LittleEndian>(n.offset));
            }

            // Compute the kind map.
            let mut kinds = vec![];
            let mut kind_map = BTreeMap::new();
            for n in &nodes {
                if !kind_map.contains_key(&n.kind) {
                    kind_map.insert(n.kind, kinds.len());
                    kinds.push(n.kind);
                }
            }

            // Write out the kind map itself.
            try!(ofd.write_u32::<LittleEndian>(kinds.len() as u32));
            for &k in &kinds {
                try!(ofd.write_u32::<LittleEndian>(k.0));
            }

            // Then write out the values.
            let mut buf = Vec::with_capacity(nodes.len());
            for n in &nodes {
                buf.push(kind_map[&n.kind] as u8);
            }
            try!(ofd.write_all(&buf));
        }

        // It worked, so do the atomic rename/overwrite.  'std' tries to do
        // this sane behavior on Windows as well.
        try!(fs::rename(tmp_name, path.as_ref()));

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Scan this index for a given hash.
    fn find(&self, key: &Oid) -> Option<usize> {
        let first_byte = key.0[0] as usize;

        let low = if first_byte > 0 {
            self.top[first_byte - 1] as usize
        } else {
            0
        };
        let high = self.top[first_byte] as usize;
        match self.oids[low..high].binary_search(key) {
            Ok(index) => Some(index + low),
            Err(_) => None,
        }
    }

    pub fn iter(&self) -> FIter {
        self.into_iter()
    }
}

impl Index for IndexFile {
    fn contains_key(&self, key: &Oid) -> bool {
        self.find(key).is_some()
    }

    fn get(&self, key: &Oid) -> Option<IndexInfo> {
        self.find(key).map(|num| {
            IndexInfo {
                offset: self.offsets[num],
                kind: self.kind_names[self.kinds[num] as usize],
            }
        })
    }
}

impl<'a> IntoIterator for &'a IndexFile {
    type Item = IterItem<'a>;
    type IntoIter = FIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        FIter {
            parent: self,
            pos: 0,
        }
    }
}

pub struct FIter<'a> {
    parent: &'a IndexFile,
    pos: usize,
}

impl<'a> Iterator for FIter<'a> {
    type Item = IterItem<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.parent.len() {
            None
        } else {
            let pos = self.pos;
            self.pos = pos + 1;

            Some(IterItem {
                oid: &self.parent.oids[pos],
                kind: self.parent.kind_names[self.parent.kinds[pos] as usize],
                offset: self.parent.offsets[pos],
            })
        }
    }
}

fn compute_top<'a>(nodes: &[IterItem<'a>]) -> Vec<u32> {
    let mut top = Vec::with_capacity(256);

    let mut iter = nodes.iter().enumerate().peekable();
    for first in 0 .. 256 {
        // Scan until we hit a value that is too large.
        loop {
            match iter.peek() {
                None => break,
                Some(&(_, key)) => {
                    if key.oid[0] as usize > first {
                        break;
                    }
                    iter.next();
                },
            }
        }
        let index = match iter.peek() {
            None => nodes.len(),
            Some(&(n, _)) => n,
        };
        top.push(index as u32);
    }
    top
}

/// An IndexPair combines a possibly loaded index with a ram index allowing
/// for update.  The whole pair can then be written to a new index file,
/// and loaded later.
pub struct IndexPair {
    file: IndexFile,
    ram: RamIndex,
}

impl IndexPair {
    pub fn load<P: AsRef<Path>>(path: P, size: u32) -> Result<IndexPair> {
        Ok(IndexPair {
            file: try!(IndexFile::load(path, size)),
            ram: RamIndex::new(),
        })
    }

    pub fn empty() -> IndexPair {
        IndexPair {
            file: IndexFile::empty(),
            ram: RamIndex::new(),
        }
    }
}

impl Index for IndexPair {
    fn contains_key(&self, key: &Oid) -> bool {
        self.ram.contains_key(key) ||
            self.file.contains_key(key)
    }

    fn get(&self, key: &Oid) -> Option<IndexInfo> {
        self.ram.get(key)
            .or_else(|| self.file.get(key))
    }
}

impl IndexUpdate for IndexPair {
    fn insert(&mut self, key: Oid, offset: u32, kind: Kind) {
        self.ram.insert(key, offset, kind);
    }
}

impl<'a> IntoIterator for &'a IndexPair {
    type Item = IterItem<'a>;
    type IntoIter = Chain<FIter<'a>, Iter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.file.iter().chain(&self.ram)
    }
}

#[cfg(test)]
mod test {
    use Error;
    use std::collections::BTreeMap;
    use {Kind, Oid};
    use super::*;
    use tempdir::TempDir;

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
            index.insert(Oid::from_u32(num), num, kind);
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
        let tmp = TempDir::new("testindex").unwrap();

        let mut track = Tracker::new();
        let mut r1 = IndexPair::empty();

        static COUNT: u32 = 10000;

        for ofs in 0 .. COUNT {
            track.add(&mut r1, ofs);
        }

        track.check(&r1);

        let name1 = tmp.path().join("r1.idx");
        IndexFile::save(&name1, COUNT, &r1).unwrap();

        match IndexPair::load(&name1, COUNT-1) {
            Err(Error::InvalidIndex(_)) => (),
            Err(e) => panic!("Unexpected error: {:?}", e),
            Ok(_) => panic!("Shouldn't be able to load index with size incorrect"),
        }

        match IndexPair::load(&tmp.path().join("r1.bad"), COUNT) {
            Err(_) => (),
            Ok(_) => panic!("Shouldn't be able to load non-existant index"),
        }

        let mut r2 = IndexPair::load(&name1, COUNT).unwrap();
        track.check(&r2);

        // Add some more.
        for ofs in COUNT .. 2*COUNT {
            track.add(&mut r2, ofs);
        }
        track.check(&r2);

        let name2 = tmp.path().join("r2.idx");
        IndexFile::save(&name2, 2*COUNT, &r2).unwrap();

        let r3 = IndexPair::load(&name2, 2*COUNT).unwrap();
        track.check(&r3);

        // Print out the path, which will prevent it from being removed.
        // println!("Path: {:?}", tmp.into_path());
    }

    #[test]
    fn test_empty() {
        let fi = IndexFile::empty();
        assert!(!fi.contains_key(&Oid::from_u32(1)));
    }
}
