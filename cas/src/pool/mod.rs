// A pool is a place that chunks can be stored.

use Result;
use oid::Oid;
use chunk::Chunk;
use uuid::Uuid;

mod sql;
mod file;

/// A source of chunks.  This is similar to a `Map`, except that the values
/// aren't kept in memory, so we have to return real items rather than
/// references to them.
pub trait ChunkSource {
    /// Return a new chunk with the given key.
    fn find(&self, key: &Oid) -> Result<Box<Chunk>>;

    // It is also use to find things, possibly not having to read the
    // entire chunk.

    /// Return the Uuid associated with this pool.
    fn uuid<'a>(&'a self) -> &'a Uuid;

    /// Return the set of backups stored in this pool.
    fn backups(&self) -> Result<Vec<Oid>>;

    /// Get a writer for this source (if possible).
    fn get_writer<'a>(&'a mut self) -> Result<Box<ChunkSink + 'a>>;
}

/// A sink for chunks.
pub trait ChunkSink: ChunkSource {
    fn add(&mut self, chunk: &Chunk) -> Result<()>;

    /// Flush, and consume this sink.
    fn flush(self: Box<Self>) -> Result<()>;
}
