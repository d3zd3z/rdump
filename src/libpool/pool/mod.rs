// The traits that define a backup pool.

use oid::Oid;
use chunk::Chunk;

use std::io::IoResult;
use uuid::Uuid;

mod sql;

/// A source of chunks.  This is similar to a `Map`, except that the
/// values aren't kept in memory, so we have to return real items
/// rather than references to them.
trait ChunkSource: Collection {
    /// Return a new chunk with the given key.
    fn find(&mut self, key: &Oid) -> IoResult<Box<Chunk>>;

    /// It is also useful to find things, possibly not using all of
    /// the information about the chunk.
    /// TODO

    /// Return the Uuid associated with this pool.
    fn uuid<'a>(&'a self) -> &'a Uuid;
}

/// A sync for chunks.
trait ChunkSync: ChunkSource {
    fn add(&mut self, chunk: &Chunk) -> IoResult<()>;

    fn flush(&mut self) -> IoResult<()>;
}