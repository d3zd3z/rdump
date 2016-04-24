///! A PairIndex combines a FileIndex with a RamIndex to allow in-memory
///updates to file data, that can then be written out.

use Kind;
use Oid;
use Result;
use std::iter::Chain;
use std::path::Path;
use super::{Index, IndexUpdate, IndexInfo, IterItem};
use super::{ram_index, RamIndex, file_index, FileIndex};

/// A PairIndex combines a possibly loaded index with a ram index allowing
/// for update.  The whole pair can then be written to a new index file,
/// and loaded later.
pub struct PairIndex {
    file: FileIndex,
    ram: RamIndex,
}

impl PairIndex {
    pub fn load<P: AsRef<Path>>(path: P, size: u32) -> Result<PairIndex> {
        Ok(PairIndex {
            file: try!(FileIndex::load(path, size)),
            ram: RamIndex::new(),
        })
    }

    pub fn save<P: AsRef<Path>>(&self, path: P, size: u32) -> Result<()> {
        FileIndex::save(path, size, self)
    }

    pub fn empty() -> PairIndex {
        PairIndex {
            file: FileIndex::empty(),
            ram: RamIndex::new(),
        }
    }

    pub fn is_dirty(&self) -> bool {
        !self.ram.is_empty()
    }
}

impl Index for PairIndex {
    fn contains_key(&self, key: &Oid) -> bool {
        self.ram.contains_key(key) ||
            self.file.contains_key(key)
    }

    fn get(&self, key: &Oid) -> Option<IndexInfo> {
        self.ram.get(key)
            .or_else(|| self.file.get(key))
    }
}

impl IndexUpdate for PairIndex {
    fn insert(&mut self, key: Oid, offset: u32, kind: Kind) {
        self.ram.insert(key, offset, kind);
    }
}

impl<'a> IntoIterator for &'a PairIndex {
    type Item = IterItem<'a>;
    type IntoIter = Chain<file_index::Iter<'a>, ram_index::Iter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.file.iter().chain(&self.ram)
    }
}
