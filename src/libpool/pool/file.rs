// File (sqlite3) based pools.

// For development.
#![allow(dead_code)]

use oid::Oid;
use chunk;
use chunk::Chunk;
use kind::Kind;
use std::fs;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use pool::sql;
use pool::{ChunkSource, ChunkSink};
use error;
use rusqlite::{SqliteConnection, SqliteTransaction};
use uuid::Uuid;
// use error::Result;

pub struct FilePool {
    db: SqliteConnection,
    uuid: Uuid,
    path: PathBuf,
}

pub struct FilePoolWriter<'a> {
    parent: &'a FilePool,
    tx: SqliteTransaction<'a>,
}

impl FilePool {
    pub fn create(path: &Path) -> error::Result<()> {
        try!(fs::create_dir(path));
        try!(fs::create_dir(&path.join("blobs")));
        let db = try!(SqliteConnection::open(&path.join("data.db")));
        try!(POOL_SCHEMA.set(&db));
        try!(POOL_SCHEMA.check(&db));

        let tx = try!(db.transaction());
        try!(db.execute("INSERT INTO props (key, value) values ('uuid', ?)",
            &[&Uuid::new_v4().to_hyphenated_string()]));
        try!(tx.commit());
        Ok(())
    }

    pub fn open(path: &Path) -> error::Result<FilePool> {
        let db = try!(SqliteConnection::open(&path.join("data.db")));

        // Retrieve the uuid.
        // TODO: Need something more robust than their query_one.
        let uuid: String = db.query_row("SELECT value FROM props WHERE key = 'uuid'", &[],
            |row| { row.get(0) });

        let uuid = try!(Uuid::parse_str(&uuid));

        Ok(FilePool {
            db: db,
            uuid: uuid,
            path: path.to_path_buf(),
        })
    }

    // Get writable access to the pool.
    pub fn get_writer<'a>(&'a mut self) -> error::Result<FilePoolWriter<'a>> {
        let tx = try!(self.db.transaction());
        Ok(FilePoolWriter {
            parent: self,
            tx: tx,
        })
    }

    // Generate paths to the directory and filename for storing a fs blob.
    fn get_paths<'a>(&'a self, oid: &Oid) -> (PathBuf, PathBuf) {
        let oid_text = oid.to_hex();
        let dir_text = &oid_text[0..2];
        let name_text = &oid_text[2..];

        let blobs = self.path.join("blobs");
        let dir = blobs.join(dir_text);
        let name = dir.join(name_text);

        (dir, name)
    }

    fn read_payload<'a>(&'a self, oid: &Oid) -> error::Result<Vec<u8>> {
        let (_, fname) = self.get_paths(oid);
        // println!("read_payload: {:?}", fname);
        let mut fd = try!(fs::File::open(&fname));
        let mut result = Vec::new();
        try!(fd.read_to_end(&mut result));
        Ok(result)
    }
}

#[cfg(test)]
fn dump(key: &Oid, zsize: i32, payload: &[u8]) {
    use pdump::HexDump;

    println!("Read from: {}, expect {} bytes, got {}", key.to_hex(), zsize, payload.len());
    payload.dump();
    panic!("Incorrect payload size");
}

#[cfg(not(test))]
fn dump(_key: &Oid, _zsize: i32, _payload: &[u8]) {
}

impl ChunkSource for FilePool {
    fn find(&self, key: &Oid) -> error::Result<Box<Chunk>> {
        // Ideally, we could just query the data for NULL, but this
        // doesn't seem to be exposed properly.  Instead, retrieve it as a
        // separate column.
        let mut stmt = try!(self.db.prepare(
            "SELECT kind, size, zsize, data, data IS NULL FROM blobs WHERE oid = ?"));
        let mut rows = try!(stmt.query(&[&&key.bytes[..]]));
        match rows.next() {
            None => Err(error::Error::MissingChunk),
            Some(row) => {
                let row = try!(row);
                let kind: String = row.get(0);
                let kind = Kind::from_str(&kind).unwrap();
                let size: i32 = row.get(1);
                let zsize: i32 = row.get(2);
                let null_data: i32 = row.get(4);
                let payload: Vec<u8> = if null_data != 0 {
                    try!(self.read_payload(key))
                } else {
                    row.get(3)
                };

                if payload.len() != zsize as usize {
                    dump(key, zsize, &payload);
                }

                let chunk = if size == zsize {
                    // TODO: Use new_plain_with_oid().
                    // println!("size: {}, zsize: {} ({:?}:{})", size, zsize, kind, key.to_hex());
                    chunk::new_plain(kind, payload)
                } else {
                    // println!("size: {}, zsize: {} ({:?}:{})", size, zsize, kind, key.to_hex());
                    chunk::new_compressed(kind, key.clone(), payload, size as u32)
                };

                assert_eq!(key, chunk.oid());

                Ok(chunk)
            }
        }
    }

    fn uuid<'a>(&'a self) -> &'a Uuid {
        &self.uuid
    }

    fn backups(&self) -> error::Result<Vec<Oid>> {
        let mut stmt = try!(self.db.prepare(
                "SELECT oid FROM blobs WHERE kind = 'back'"));
        let mut result = Vec::new();
        for row in try!(stmt.query(&[])) {
            let row = try!(row);
            let oid: Vec<u8> = row.get(0);
            result.push(Oid::from_raw(&oid));
        }
        Ok(result)
    }
}

impl<'a> ChunkSink for FilePoolWriter<'a> {
    fn add(&mut self, chunk: &Chunk) -> error::Result<()> {
        // /println!("size: {}", chunk.data_len());
        let payload = match chunk.zdata() {
            None => chunk.data(),
            Some (zdata) => zdata,
        };
        let payload = payload.as_slice();

        if payload.len() < 100000 {
            try!(self.parent.db.execute(
                    "INSERT INTO blobs (oid, kind, size, zsize, data)
                     VALUES (?, ?, ?, ?, ?)",
                     &[&&chunk.oid().bytes[..],
                       &chunk.kind().textual(),
                       &(chunk.data_len() as i32),
                       &(payload.len() as i32),
                       &payload]));
            Ok(())
        } else {
            let (dir, name) = self.parent.get_paths(chunk.oid());

            // Just try writing the fd first.
            let mut fd = match fs::File::create(&name) {
                Ok(fd) => fd,
                _ => {
                    // Try creating the directory and retry it.
                    try!(fs::create_dir(&dir));
                    // println!("Create: {:?}", name);
                    try!(fs::File::create(&name))
                },
            };

            try!(fd.write_all(payload));

            try!(self.parent.db.execute(
                    "INSERT INTO blobs (oid, kind, size, zsize)
                     VALUES (?, ?, ?, ?)",
                    &[&&chunk.oid().bytes[..],
                      &chunk.kind().textual(),
                      &(chunk.data_len() as i32),
                      &(payload.len() as i32)]));

            Ok(())
        }
    }

    // Flushes and closes the writer.
    fn flush(self) -> error::Result<()> {
        try!(self.tx.commit());
        Ok(())
    }
}

impl<'a> ChunkSource for FilePoolWriter<'a> {
    fn find(&self, key: &Oid) -> error::Result<Box<Chunk>> {
        self.parent.find(key)
    }

    fn uuid<'b>(&'b self) -> &'b Uuid {
        self.parent.uuid()
    }

    fn backups(&self) -> error::Result<Vec<Oid>> {
        self.parent.backups()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use pool::{ChunkSource, ChunkSink};
    use testutil::{make_random_chunk, make_kinded_random_chunk, make_uncompressible_chunk, boundary_sizes};
    use std::collections::HashMap;
    use tempdir::TempDir;

    #[test]
    fn simple_create() {
        let tmp = TempDir::new("filepool").unwrap();
        let path = tmp.path().join("pool");

        FilePool::create(&path).unwrap();
        let mut pool = FilePool::open(&path).unwrap();
        let mut all = HashMap::new();

        {
            let mut pw = pool.get_writer().unwrap();

            for i in boundary_sizes() {
                let ch = make_random_chunk(i, i);
                pw.add(&*ch).unwrap();
                let oi = all.insert(ch.oid().clone(), ch);
                match oi {
                    None => (),
                    Some(_) => panic!("Duplicate chunk in test"),
                }
            }

            // Repeat this with uncompressible data.
            for i in boundary_sizes() {
                if i < 16 {
                    continue;
                }
                let ch = make_uncompressible_chunk(i, i);
                pw.add(&*ch).unwrap();
                let oi = all.insert(ch.oid().clone(), ch);
                match oi {
                    None => (),
                    Some(_) => panic!("Duplicate chunk in test"),
                }
            }

            pw.flush().unwrap();
        }

        // Verify all of them.
        for (key, c1) in all.iter() {
            let c2 = pool.find(key).unwrap();
            assert_eq!(c1.kind(), c2.kind());
            assert_eq!(c1.oid(), c2.oid());
            assert_eq!(c1.data().as_slice(), c2.data().as_slice());
        }
    }

    #[test]
    fn backups() {
        use std::collections::HashSet;

        let tmp = TempDir::new("filepool").unwrap();
        let path = tmp.path().join("pool");

        FilePool::create(&path).unwrap();
        let mut pool = FilePool::open(&path).unwrap();
        let mut oids = HashSet::new();

        {
            let mut pw = pool.get_writer().unwrap();

            for i in 0 .. 1000 {
                let ch = make_kinded_random_chunk(kind!("back"), 64, i);
                pw.add(&*ch).unwrap();
                oids.insert(ch.oid().clone());
            }
            pw.flush().unwrap();
        }

        for id in pool.backups().unwrap() {
            let present = oids.remove(&id);
            assert!(present);
        }

        assert_eq!(oids.len(), 0);
    }
}

#[derive(PartialEq, Eq, Clone)]
enum PoolInabilities {
    NoFilesystems,
    NoCTimeCache,
}

static POOL_SCHEMA: sql::Schema<'static, PoolInabilities> =
    sql::Schema {
        version: "1:2014-03-18",
        schema: &[
            r#"PRAGMA PAGE_SIZE=8192"#,
            r#"CREATE TABLE blobs (
                id INTEGER PRIMARY KEY,
                oid BLOB UNIQUE NOT NULL,
                kind TEXT,
                size INTEGER,
                zsize INTEGER,
                data BLOB)"#,
            r#"CREATE INDEX blobs_oid ON blobs(oid)"#,
            r#"CREATE INDEX blobs_backs ON blobs(kind) where kind = 'back'"#,
            r#"CREATE TABLE props (
                key text PRIMARY KEY,
                value TEXT)"#,
            r#"CREATE TABLE filesystems (
                fsid INTEGER PRIMARY KEY,
                uuid TEXT UNIQUE)"#,
            r#"CREATE TABLE ctime_dirs (
                pkey INTEGER PRIMARY KEY,
                fsid INTEGER REFERENCES filesystem (fsid) NOT NULL,
                pino INTEGER NOT NULL,
                UNIQUE (fsid, pino))"#,
            r#"CREATE TABLE ctime_cache (
                pkey INTEGER REFERENCES ctime_dirs (pkey) NOT NULL,
                ino INTEGER NOT NULL,
                expire INTEGER NOT NULL,
                ctime INTEGER NOT NULL,
                oid BLOB NOT NULL)"#,
            r#"CREATE INDEX ctime_cache_pkey ON ctime_cache(pkey)"#,
            ],
        compats: &[
            sql::SchemaCompat {
                version: "1:2014-03-13",
                inabilities: &[ PoolInabilities::NoFilesystems, PoolInabilities::NoCTimeCache ]
            } ],
    };

