// Adump file format.

use Chunk;
use Error;
use Kind;
use Oid;
use regex::Regex;
use Result;
use std::cell::RefCell;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::mem;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use self::chunkio::{ChunkRead, ChunkWrite};
use super::ChunkSource;

use self::index::{Index, IndexUpdate, PairIndex};

mod index;
pub mod chunkio;
mod pfile;

pub struct AdumpPool {
    base: PathBuf,
    uuid: Uuid,
    newfile: bool,
    limit: u32,

    // Have we ever written to this pool in this session?
    dirty: bool,

    cfiles: RefCell<Vec<ChunkFile>>,

    next_file: u32,
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

        let (cfiles, next_file) = try!(scan_backups(&base));

        Ok(AdumpPool {
            base: base,
            uuid: uuid,
            newfile: newfile,
            limit: limit,
            dirty: false,
            cfiles: RefCell::new(cfiles),
            next_file: next_file,
        })
    }

    /// Does a write of size 'size' need a new pool file?
    fn needs_new_file(&self, size: u32) -> bool {
        // If we're configured in newfile mode, always write the new file.
        if self.newfile && !self.dirty {
            return true
        }

        match self.cfiles.borrow().last() {
            None => true,
            Some(ref cf) => cf.size + size > self.limit,
        }
    }
}

impl ChunkSource for AdumpPool {
    fn find(&self, key: &Oid) -> Result<Chunk> {
        let mut cfiles = self.cfiles.borrow_mut();
        for cf in cfiles.iter_mut() {
            match try!(cf.find(key)) {
                None => (),
                Some(chunk) => return Ok(chunk),
            }
        }
        Err(Error::MissingChunk)
    }

    fn contains_key(&self, key: &Oid) -> Result<bool> {
        let mut cfiles = self.cfiles.borrow_mut();
        for cf in cfiles.iter_mut() {
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
        let cfiles = self.cfiles.borrow();
        for cfile in cfiles.iter() {
            for ent in &cfile.index {
                if ent.kind == back {
                    println!("ent: {:?}", ent);
                    result.push(ent.oid.clone());
                }
            }
        }

        Ok(result)
    }

    fn begin_writing(&mut self) -> Result<()> {
        unimplemented!();
    }

    fn add(&mut self, chunk: &Chunk) -> Result<()> {
        if self.needs_new_file(write_size(chunk)) {
            let name = self.base.with_file_name(&format!("pool-data-{:04}.data", self.next_file));
            self.next_file += 1;

            println!("Needs new file: {:?}", name);
            self.cfiles.borrow_mut().push(try!(ChunkFile::create(name)));
        }

        let mut cfiles = self.cfiles.borrow_mut();
        let cfile = cfiles.last_mut().expect("should've created a poolfile");

        cfile.add(chunk)
    }

    fn flush(&mut self) -> Result<()> {
        unimplemented!();
    }
}

fn write_size(chunk: &Chunk) -> u32 {
    let payload = match chunk.zdata() {
        Some(p) => p,
        None => chunk.data(),
    };
    48 + ((payload.len() + 15) & !15) as u32
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
fn scan_backups(base: &Path) -> Result<(Vec<ChunkFile>, u32)> {
    let reg = Regex::new(r"^pool-data-(\d\d\d\d).data").unwrap();

    let mut bpaths = vec![];
    let mut next_file = 0;

    // We'll consider every file in the pool directory that ends in '.data'
    // to be a pool file.
    for ent in try!(base.read_dir()) {
        let ent = try!(ent);
        let name = ent.path();
        if match name.extension().and_then(|x| x.to_str()) {
            Some(ext) if ext == "data" => true,
            _ => false
        } {
            match name.file_name().and_then(|x| x.to_str())
                .and_then(|x| reg.captures(x))
            {
                Some(cap) => {
                    let num = cap.at(1).unwrap().parse::<u32>().unwrap() + 1;
                    if num > next_file {
                        next_file = num;
                    }
                },
                None => (),
            }
            bpaths.push(name);
        }
    }
    bpaths.sort();

    // Open all of the files.
    Ok((try!(bpaths.into_iter().map(|x| ChunkFile::open(x)).collect()), next_file))
}

struct ChunkFile {
    name: PathBuf,
    index: PairIndex,

    // The BufReader or BufWriter holding the descriptor (or nothing, if it
    // isn't opened at all.
    buf: ReadWriter,
    // True if the underlying file descriptor is opened for writing.
    writable: bool,
    // The known size of the file.  Should always be updated after writes.
    size: u32,
}

enum ReadWriter {
    None,
    Read(BufReader<File>),
    Write(BufWriter<File>),
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
            buf: ReadWriter::None,
            writable: false,
            size: size as u32,
        })
    }

    fn create(p: PathBuf) -> Result<ChunkFile> {
        if p.is_file() {
            panic!("Pool file shouldn't be present for creation");
        }

        let fd = try!(OpenOptions::new().read(true).append(true).create(true).open(&p));
        Ok(ChunkFile {
            name: p,
            index: PairIndex::empty(),
            buf: ReadWriter::Write(BufWriter::new(fd)),
            writable: true,
            size: 0,
        })
    }

    fn contains_key(&self, key: &Oid) -> bool {
        self.index.contains_key(key)
    }

    // Read a chunk from this file, if that is possible.
    fn find(&mut self, key: &Oid) -> Result<Option<Chunk>> {
        match self.index.get(key) {
            None => Ok(None),
            Some(info) => {
                let fd = try!(self.read());
                try!(fd.seek(SeekFrom::Start(info.offset as u64)));
                let ch = try!(fd.read_chunk());
                Ok(Some(ch))
            },
        }
    }

    // Add a chunk to this file.
    fn add(&mut self, chunk: &Chunk) -> Result<()> {
        let pos;
        let size;
        {
            let fd = try!(self.write());
            pos = try!(fd.seek(SeekFrom::End(0))) as u32;
            try!(fd.write_chunk(chunk));
            size = try!(fd.seek(SeekFrom::Current(0))) as u32;
        }

        self.index.insert(chunk.oid().to_owned(), pos, chunk.kind());
        self.size = size;
        Ok(())
    }

    // Configure the state for reading, and borrow the reader.
    fn read(&mut self) -> Result<&mut BufReader<File>> {
        match self.buf {
            ReadWriter::None => {
                let file = try!(File::open(&self.name));
                self.buf = ReadWriter::Read(BufReader::new(file));
                return self.read();
            },
            ReadWriter::Read(ref mut rd) => return Ok(rd),
            ReadWriter::Write(_) => (),
        }

        // Writable files will always be opened for reading as well.
        // Consuming the buffer flushes it, so we can wrap it in a read
        // buffer.
        let wr = mem::replace(&mut self.buf, ReadWriter::None);
        let fd = if let ReadWriter::Write(buf) = wr {
            match buf.into_inner() {
                Ok(fd) => fd,
                Err(e) => {
                    // In case of error, just leave things closed, and
                    // return the error.  We've likely lost data, at this
                    // point, so let the caller handle things.
                    // Unfortunately, the error can't be recovered, only by
                    // reference, so just return a general error as a
                    // string.
                    return Err(Error::CorruptPool(format!("error flushing buffer: {:?}", e.error())));
                }
            }
        } else {
            panic!("Unexpected path");
        };
        self.buf = ReadWriter::Read(BufReader::new(fd));
        self.read()
    }

    // Configure the state for writing, and borrow the writer.
    fn write(&mut self) -> Result<&mut BufWriter<File>> {
        match self.buf {
            ReadWriter::Write(ref mut wr) => return Ok(wr),
            _ => (),
        }
        unimplemented!();
    }
}

#[cfg(test)]
mod test {
    use tempdir::TempDir;
    use testutil;
    use super::*;
    use pool::ChunkSource;

    #[test]
    fn test_pool() {
        let tmp = TempDir::new("adump").unwrap();
        let name = tmp.path().join("blort");
        AdumpPool::new_builder(&name).create().unwrap();

        let mut pool = AdumpPool::open(&name).unwrap();
        assert_eq!(pool.backups().unwrap().len(), 0);

        let ch = testutil::make_random_chunk(64, 64);
        pool.add(&ch).unwrap();

        // println!("Path: {:?}", tmp.into_path());
    }
}
