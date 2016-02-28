// RAM pools.

use std::cell::RefCell;
use std::collections::HashMap;
use uuid::Uuid;

use chunk;
use Chunk;
use Kind;
use Oid;
use Result;
use Error;
use pool::{ChunkSink, ChunkSource};

// TODO: Should Chunks implement clone, so we could just store them
// directly?

pub struct RamPool {
    uuid: Uuid,
    chunks: RefCell<HashMap<Oid, Stashed>>,
}

pub struct Stashed {
    kind: Kind,
    data: Vec<u8>,
}

impl Stashed {
    fn to_chunk(&self) -> Box<Chunk> {
        chunk::new_plain(self.kind, self.data.clone())
    }
}

impl RamPool {
    pub fn new() -> RamPool {
        RamPool {
            uuid: Uuid::new_v4(),
            chunks: RefCell::new(HashMap::new()),
        }
    }
}

impl ChunkSource for RamPool {
    fn find(&self, key: &Oid) -> Result<Box<Chunk>> {
        self.chunks.borrow().get(key).map(|x| x.to_chunk()).ok_or(Error::MissingChunk)
    }

    fn contains_key(&self, key: &Oid) -> Result<bool> {
        Ok(self.chunks.borrow().contains_key(key))
    }

    fn uuid<'a>(&'a self) -> &'a Uuid {
        &self.uuid
    }

    fn backups(&self) -> Result<Vec<Oid>> {
        unimplemented!();
    }

    fn get_writer<'a>(&'a self) -> Result<Box<ChunkSink + 'a>> {
        Ok(Box::new(self))
    }
}

impl<'a> ChunkSink for &'a RamPool {
    fn add(&self, chunk: &Chunk) -> Result<()> {
        let id = chunk.oid().clone();
        let payload = Stashed {
            kind: chunk.kind(),
            data: chunk.data().to_vec(),
        };
        self.chunks.borrow_mut().entry(id)
            .or_insert(payload);
        Ok(())
    }

    fn flush(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
