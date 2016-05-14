//! The hash index.
//!
//! Each backup file contains one or more chunks, which are identified by
//! their sha1 hash.  To allow us to find chunks, we manage an index of
//! every chunk within the file.  This index is written to a separate file,
//! and loaded into memory.  The file being written can also have additions
//! made to its index in memory that are later written out.
//!
//! The types exposed here are the `Index` and `IndexUpdate` trait which
//! are types that can be searched and updated respectively.  The
//! `PairIndex` combines an index loaded from a file with an index in ram.

use Kind;
use Oid;

pub trait Index {
    fn contains_key(&self, key: &Oid) -> bool;
    fn get(&self, key: &Oid) -> Option<IndexInfo>;
}

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub offset: u32,
    pub kind: Kind,
}

pub trait IndexUpdate {
    // Like a map insert, but panics if the key is already present.
    fn insert(&mut self, key: Oid, offset: u32, kind: Kind);
}

/// All of the indices can be iterated, producing an IterItem.
#[derive(Debug)]
pub struct IterItem<'a> {
    pub oid: &'a Oid,
    pub kind: Kind,
    pub offset: u32,
}

mod ram_index;
pub use self::ram_index::RamIndex;

mod file_index;
pub use self::file_index::FileIndex;

mod pair_index;
pub use self::pair_index::PairIndex;

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
                    }
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
        let mut r1 = PairIndex::empty();

        static COUNT: u32 = 10000;

        for ofs in 0..COUNT {
            track.add(&mut r1, ofs);
        }

        track.check(&r1);

        let name1 = tmp.path().join("r1.idx");
        FileIndex::save(&name1, COUNT, &r1).unwrap();

        match PairIndex::load(&name1, COUNT - 1) {
            Err(Error::InvalidIndex(_)) => (),
            Err(e) => panic!("Unexpected error: {:?}", e),
            Ok(_) => panic!("Shouldn't be able to load index with size incorrect"),
        }

        match PairIndex::load(&tmp.path().join("r1.bad"), COUNT) {
            Err(_) => (),
            Ok(_) => panic!("Shouldn't be able to load non-existant index"),
        }

        let mut r2 = PairIndex::load(&name1, COUNT).unwrap();
        track.check(&r2);

        // Add some more.
        for ofs in COUNT..2 * COUNT {
            track.add(&mut r2, ofs);
        }
        track.check(&r2);

        let name2 = tmp.path().join("r2.idx");
        FileIndex::save(&name2, 2 * COUNT, &r2).unwrap();

        let r3 = PairIndex::load(&name2, 2 * COUNT).unwrap();
        track.check(&r3);

        // Print out the path, which will prevent it from being removed.
        // println!("Path: {:?}", tmp.into_path());
    }

    #[test]
    fn test_empty() {
        let fi = FileIndex::empty();
        assert!(!fi.contains_key(&Oid::from_u32(1)));
    }
}
