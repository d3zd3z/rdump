// A pool is a place that chunks can be stored.

use Result;
use Error;
use oid::Oid;
use chunk::Chunk;
use uuid::Uuid;

use std::path::Path;
use std::fs;

pub use pool::file::FilePool;
pub use pool::adump::AdumpPool;
pub use self::ram::RamPool;

mod sql;
mod file;
mod ram;
mod wrapper;
pub mod adump;

/// A source of chunks.  This is similar to a `Map`, except that the values
/// aren't kept in memory, so we have to return real items rather than
/// references to them.
pub trait ChunkSource {
    /// Return a new chunk with the given key.
    fn find(&self, key: &Oid) -> Result<Chunk>;

    /// Is this key present in the store.
    fn contains_key(&self, key: &Oid) -> Result<bool>;

    // It is also use to find things, possibly not having to read the
    // entire chunk.

    /// Return the Uuid associated with this pool.
    fn uuid<'a>(&'a self) -> &'a Uuid;

    /// Return the set of backups stored in this pool.
    fn backups(&self) -> Result<Vec<Oid>>;

    /// Begin allowing writing on this particular ChunkSource.
    fn begin_writing(&mut self) -> Result<()>;

    /// Add a new chunk to this pool.
    fn add(&mut self, chunk: &Chunk) -> Result<()>;

    /// Consume the writer, closing the transaction.
    fn flush(&mut self) -> Result<()>;
}

/// Attempt to open a pool for reading, auto-determining the type.
pub fn open<P: AsRef<Path>>(path: P) -> Result<Box<ChunkSource>> {
    let meta = fs::metadata(path.as_ref().join("data.db"))?;

    if !meta.is_file() {
        return Err(Error::NotAPool);
    }

    match FilePool::open(path) {
        Ok(p) => Ok(Box::new(p)),
        Err(e) => Err(e),
    }
}
