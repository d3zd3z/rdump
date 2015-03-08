// The traits that define a backup pool.

// use oid::Oid;
// use chunk::Chunk;

// use std::io::{fs, IoResult};
// use uuid::Uuid;

// pub use self::file::create;
// use self::file::FilePool;

mod sql;
// mod file;

/*
/// A source of chunks.  This is similar to a `Map`, except that the
/// values aren't kept in memory, so we have to return real items
/// rather than references to them.
pub trait ChunkSource: Collection {
    /// Return a new chunk with the given key.
    fn find(&self, key: &Oid) -> IoResult<Box<Chunk>>;

    /// It is also useful to find things, possibly not using all of
    /// the information about the chunk.
    /// TODO

    /// Return the Uuid associated with this pool.
    fn uuid<'a>(&'a self) -> &'a Uuid;

    /// Return the set of backups stored in this pool.
    fn backups(&self) -> IoResult<Vec<Oid>>;
}

/// A sync for chunks.
pub trait ChunkSync: ChunkSource {
    fn add(&mut self, chunk: &Chunk) -> IoResult<()>;

    fn flush(&mut self) -> IoResult<()>;
}

/// Attempt to open a pool, returning if it possible.
pub fn open(path: Path) -> IoResult<Box<ChunkSync>> {
   try!(fs::stat(&path.join("data.db")));

   match FilePool::open(path) {
       Ok(p) => Ok(box p as Box<ChunkSync>),
       Err(e) => Err(e)
   }
}

#[cfg(test)]
mod test {
    use testutil::TempDir;
    use super::{create, open};

    #[test]
    fn simple() {
        let tmp = TempDir::new();
        let pname = tmp.join("mypool");
        create(&pname).unwrap();

        let pool = open(pname).unwrap();

        // Make sure the uuid is valid.
        assert!(pool.uuid().get_version() == Some(::uuid::Version4Random));
        // println!("uuid: {}", pool.uuid().to_hyphenated_str());
    }
}
*/
