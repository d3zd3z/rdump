// Adump file format.

use Chunk;
use Error;
use Kind;
use Oid;
use Result;
use std::cell::RefCell;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Read, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

use self::chunkio::ChunkRead;
use super::{ChunkSink, ChunkSource};

// TODO: These probably don't need to be exported.
pub
use self::index::{Index, FileIndex, RamIndex, PairIndex};

mod index;
pub mod chunkio;
mod pfile;

pub struct AdumpPool {
    base: PathBuf,
    uuid: Uuid,
    newfile: bool,
    limit: u32,

    cfiles: Vec<ChunkFile>,
}

impl AdumpPool {
    pub fn new_builder<P: AsRef<Path>>(dir: P) -> PoolBuilder<P> {
        PoolBuilder {
            dir: dir,
            newfile: false,
            limit: 640 * 1024 * 1024,
        }
    }

    pub fn open<P: AsRef<Path>>(dir: P) -> Result<AdumpPool> {
        let base = dir.as_ref().to_owned();
        let meta = base.join("metadata");

        let props = {
            let fd = try!(File::open(&meta.join("props.txt")));
            try!(pfile::parse(fd))
        };
        let uuid = try!(props.get("uuid").ok_or_else(|| Error::PropertyError("No uuid property".to_owned())));
        let uuid = try!(Uuid::parse_str(&uuid));
        let newfile = try!(props.get("newfile").ok_or_else(|| Error::PropertyError("No newfile property".to_owned())));
        let newfile = try!(newfile.parse::<bool>());
        let limit = try!(props.get("limit").ok_or_else(|| Error::PropertyError("No limit property".to_owned())));
        let limit = try!(limit.parse::<u32>());

        let cfiles = try!(scan_backups(&base));

        Ok(AdumpPool {
            base: base,
            uuid: uuid,
            newfile: newfile,
            limit: limit,
            cfiles: cfiles,
        })
    }
}

impl ChunkSource for AdumpPool {
    fn find(&self, key: &Oid) -> Result<Chunk> {
        for cf in &self.cfiles {
            match try!(cf.find(key)) {
                None => (),
                Some(chunk) => return Ok(chunk),
            }
        }
        Err(Error::MissingChunk)
    }

    fn contains_key(&self, key: &Oid) -> Result<bool> {
        for cf in &self.cfiles {
            if cf.contains_key(key) {
                return Ok(true)
            }
        }
        Ok(false)
    }

    fn uuid<'a>(&'a self) -> &'a Uuid {
        &self.uuid
    }

    fn backups(&self) -> Result<Vec<Oid>> {
        let back = Kind::new("back").unwrap();
        let mut result = vec![];

        // Scan actual files for these.
        for cfile in &self.cfiles {
            for ent in &cfile.index {
                if ent.kind == back {
                    println!("ent: {:?}", ent);
                    result.push(ent.oid.clone());
                }
            }
        }

        Ok(result)
    }

    fn get_writer<'a>(&'a self) -> Result<Box<ChunkSink + 'a>> {
        unimplemented!();
    }
}

/// A builder to set parameters before creating a pool.
pub struct PoolBuilder<P: AsRef<Path>> {
    dir: P,
    newfile: bool,
    limit: u32,
}

impl<P: AsRef<Path>> PoolBuilder<P> {
    /// Change the default value of the `newfile` flag on the pool.  If
    /// set to try, files will not be appended to, but each time the pool
    /// is opened for writing, a new file will be created.  This will
    /// create more smaller files, but can, in some situations, make
    /// synchronization easier.
    pub fn set_newfile(mut self, newfile: bool) -> Self {
        self.newfile = newfile;
        self
    }

    /// Change the default value of the `limit` flag on the pool.  No
    /// individual pool file will grow larger than this value.  Note that
    /// this is a u32, but it is best to not allow the value to exceed a
    /// positive i32 to avoid compatibility issues with legacy programs
    /// that may read this format.
    pub fn set_limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Actually create the pool.  The given path must name either an empty
    /// directory, or a path where one can be created.
    pub fn create(self) -> Result<()> {
        // The given directory must represent either an empty directory, or
        // a path that a new directory can be created at.
        let base = self.dir.as_ref();
        try!(ensure_dir(base));
        let meta = base.join("metadata");
        let seen = base.join("seen");

        try!(fs::create_dir(&meta));
        try!(fs::create_dir(&seen));

        {
            let mut fd = try!(File::create(meta.join("props.txt")));
            try!(writeln!(&mut fd, "uuid={}", Uuid::new_v4().hyphenated()));
            try!(writeln!(&mut fd, "newfile={}", self.newfile));
            try!(writeln!(&mut fd, "limit={}", self.limit));
        }

        try!(File::create(meta.join("backups.txt")));

        Ok(())
    }
}

// Ensure that we have an empty directory for the pool.  It can either be
// an existing empty directory (or a symlink to one), or a path where a
// directory can be created.  If the directory doesn't exist, this will
// create it.
fn ensure_dir(base: &Path) -> Result<()> {
    if base.is_dir() {
        // An existing directory is allowed, if it is completely empty.
        for ent in try!(base.read_dir()) {
            let _ = try!(ent);
            return Err(Error::PathError(format!("Directory is not empty: {:?}", base)));
        }
    } else {
        // If not a directory, see if we can create one.
        try!(fs::create_dir(base));
    }
    Ok(())
}

// Scan the directory for backup files.
fn scan_backups(base: &Path) -> Result<Vec<ChunkFile>> {
    let mut bpaths = vec![];

    // We'll consider every file in the pool directory that ends in '.data'
    // to be a pool file.
    for ent in try!(base.read_dir()) {
        let ent = try!(ent);
        let name = ent.path();
        if match name.extension().and_then(|x| x.to_str()) {
            Some(ext) if ext == "data" => true,
            _ => false
        } {
            bpaths.push(name);
        }
    }
    bpaths.sort();

    // Open all of the files.
    bpaths.into_iter().map(|x| ChunkFile::open(x)).collect()
}

struct ChunkFile {
    name: PathBuf,
    index: PairIndex,
    state: RefCell<State>,
}

enum State {
    Closed,
    Reading(BufReader<File>),
}

impl ChunkFile {
    fn open(p: PathBuf) -> Result<ChunkFile> {
        let m = try!(p.metadata());
        if !m.is_file() {
            return Err(Error::CorruptPool(format!("file {:?} is not a regular file", p)));
        }
        let size = m.len();
        if size > i32::max_value() as u64 {
            return Err(Error::CorruptPool(format!("file {:?} is larger than 2^31", p)));
        }
        let index_name = p.with_extension("idx");
        let index = match PairIndex::load(&index_name, size as u32) {
            Ok(x) => x,
            Err(e @ Error::InvalidIndex(_)) => return Err(e),
            Err(e) => return Err(Error::InvalidIndex(format!("Index error in {:?}, {:?}", p, e))),
        };
        Ok(ChunkFile {
            name: p,
            index: index,
            state: RefCell::new(State::Closed),
        })
    }

    fn contains_key(&self, key: &Oid) -> bool {
        self.index.contains_key(key)
    }

    // Read a chunk from this file, if that is possible.
    fn find(&self, key: &Oid) -> Result<Option<Chunk>> {
        match self.index.get(key) {
            None => Ok(None),
            Some(info) => {
                self.with_reader(|fd| {
                    try!(fd.seek(SeekFrom::Start(info.offset as u64)));
                    let ch = try!(fd.read_chunk());
                    Ok(Some(ch))
                })
            }
        }
    }

    // Use a reader, opening if necessary.
    fn with_reader<F, Z>(&self, f: F) -> Result<Z> where F: FnOnce(&mut BufReader<File>) -> Result<Z> {
        let mut field = self.state.borrow_mut();

        // Open the descriptor if necessary.
        match *field {
            State::Reading(_) => (),
            State::Closed => {
                let fd = try!(File::open(&self.name));
                *field = State::Reading(BufReader::new(fd));
            }
        }

        let mut fd = match *field {
            State::Reading(ref mut fd) => fd,
            _ => panic!("Unexpected state"),
        };

        f(fd)
    }
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;
    use super::*;

    #[test]
    fn test_pool() {
        let tmp = TempDir::new("adump").unwrap();
        let name = tmp.path().join("blort");
        AdumpPool::new_builder(&name).create().unwrap();
    }
}
