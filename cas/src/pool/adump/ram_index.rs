///! A `RamIndex` is a purely memory-based index mapping hashes to IndexInfo.

use Kind;
use Oid;
use std::collections::{btree_map, BTreeMap};
use super::{Index, IndexUpdate, IndexInfo, IterItem};

pub struct RamIndex(pub BTreeMap<Oid, IndexInfo>);

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

