// File (sqlite3) based pools.

// For development.
#![allow(dead_code)]

use std::io::prelude::*;
use std::fs;
use std::path::{Path, PathBuf};
use rusqlite::{SqliteConnection, SqliteTransaction};
use uuid::Uuid;

use oid::Oid;
use chunk::Chunk;
use kind::Kind;
use pool::sql;
use pool::wrapper::XactConnection;
use pool::ChunkSource;
use Result;
use Error;

pub struct FilePool {
    db: XactConnection,
    uuid: Uuid,
    path: PathBuf,
}

impl FilePool {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<()> {
        let path = path.as_ref();
        fs::create_dir(path)?;
        fs::create_dir(&path.join("blobs"))?;
        let db = SqliteConnection::open(&path.join("data.db"))?;
        POOL_SCHEMA.set(&db)?;
        POOL_SCHEMA.check(&db)?;

        let tx = db.transaction()?;
        db.execute("INSERT INTO props (key, value) values ('uuid', ?)",
                     &[&Uuid::new_v4().hyphenated().to_string()])?;
        tx.commit()?;
        Ok(())
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<FilePool> {
        let path = path.as_ref();
        let db = SqliteConnection::open(&path.join("data.db"))?;
        let db = XactConnection::new(db);

        let _inabilities = POOL_SCHEMA.check(&db)?;

        // Retrieve the uuid.
        // TODO: Need something more robust than their query_one.
        // We should be able to handle no uuid, and probably just create
        // one.
        let uuid: String = db.query_row("SELECT value FROM props WHERE key = 'uuid'",
                       &[],
                       |row| row.get(0))?;

        let uuid = Uuid::parse_str(&uuid)?;

        Ok(FilePool {
            db: db,
            uuid: uuid,
            path: path.to_path_buf(),
        })
    }

    // Generate the paths to the directory and filename for storing a fs
    // blob.
    fn get_paths(&self, oid: &Oid) -> (PathBuf, PathBuf) {
        let oid_text = oid.to_hex();
        let dir_text = &oid_text[0..2];
        let name_text = &oid_text[2..];

        let blobs = self.path.join("blobs");
        let dir = blobs.join(dir_text);
        let name = dir.join(name_text);

        (dir, name)
    }

    fn read_payload(&self, oid: &Oid) -> Result<Vec<u8>> {
        let (_, fname) = self.get_paths(oid);
        let mut fd = fs::File::open(&fname)?;
        let mut result = Vec::new();
        fd.read_to_end(&mut result)?;
        Ok(result)
    }
}

impl ChunkSource for FilePool {
    fn find(&self, key: &Oid) -> Result<Chunk> {
        // Ideally, we could just query the data for NULL, but this doesn't
        // seem to be exposed properly.  Instead, retrieve it as a separate
        // column.
        let mut stmt = self.db
            .prepare("SELECT kind, size, zsize, data, data IS NULL FROM blobs WHERE oid = ?")?;
        let mut rows = stmt.query(&[&&key.0[..]])?;
        match rows.next() {
            None => Err(Error::MissingChunk),
            Some(row) => {
                let row = row?;
                let kind: String = row.get(0);
                let kind = Kind::new(&kind).unwrap();
                let size: i32 = row.get(1);
                let zsize: i32 = row.get(2);
                let null_data: i32 = row.get(4);
                let payload: Vec<u8> = if null_data != 0 {
                    self.read_payload(key)?
                } else {
                    row.get(3)
                };

                let chunk = if size == zsize {
                    // TODO: Use new_plain_with_oid()
                    Chunk::new_plain(kind, payload)
                } else {
                    Chunk::new_compressed(kind, key.clone(), payload, size as u32)
                };

                assert_eq!(key, chunk.oid());

                Ok(chunk)
            }
        }
    }

    fn contains_key(&self, key: &Oid) -> Result<bool> {
        let count: i32 = self.db
            .query_row("SELECT COUNT(*) FROM blobs WHERE oid = ?",
                       &[&&key.0[..]],
                       |row| row.get(0))?;
        Ok(count > 0)
    }

    fn uuid<'a>(&'a self) -> &'a Uuid {
        &self.uuid
    }

    fn backups(&self) -> Result<Vec<Oid>> {
        let mut stmt = self.db
            .prepare("SELECT oid FROM blobs WHERE kind = 'back'")?;
        let mut result = Vec::new();
        for row in stmt.query(&[])? {
            let row = row?;
            let oid: Vec<u8> = row.get(0);
            result.push(Oid::from_raw(&oid));
        }

        Ok(result)
    }

    fn begin_writing(&mut self) -> Result<()> {
        self.db.begin()?;
        Ok(())
    }

    fn add(&mut self, chunk: &Chunk) -> Result<()> {
        let payload = match chunk.zdata() {
            None => chunk.data(),
            Some(zdata) => zdata,
        };

        if payload.len() < 100000 {
            self.db
                .execute("INSERT INTO blobs (oid, kind, size, zsize, data)
                    \
                          VALUES (?, ?, ?, ?, ?)",
                         &[&&chunk.oid().0[..],
                           &chunk.kind().to_string(),
                           &(chunk.data_len() as i32),
                           &(payload.len() as i32),
                           &&payload[..]])?;
        } else {
            let (dir, name) = self.get_paths(chunk.oid());

            // Just try writing the fd first.
            let mut fd = match fs::File::create(&name) {
                Ok(fd) => fd,
                _ => {
                    // Try creating the directory, and retrying.
                    fs::create_dir(&dir)?;
                    fs::File::create(&name)?
                }
            };

            fd.write_all(&payload[..])?;

            self.db
                .execute("INSERT INTO blobs (oid, kind, size, zsize)
                     VALUES \
                          (?, ?, ?, ?)",
                         &[&&chunk.oid().0[..],
                           &chunk.kind().to_string(),
                           &(chunk.data_len() as i32),
                           &(payload.len() as i32)])?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.db.commit()?;
        Ok(())
    }
}

pub struct FilePoolWriter<'a> {
    tx: SqliteTransaction<'a>,
}

#[cfg(test)]
mod test {
    use super::*;
    use pool::ChunkSource;
    use kind::Kind;
    // use std::path::Path;
    use std::collections::HashMap;
    use tempdir::TempDir;
    use testutil::{make_random_chunk, make_uncompressible_chunk, make_kinded_random_chunk,
                   boundary_sizes};

    #[test]
    fn simple_create() {
        let tmp = TempDir::new("filepool").unwrap();
        let path = tmp.path().join("pool");
        // let path = Path::new("blort");

        FilePool::create(&path).unwrap();
        let mut pool = FilePool::open(&path).unwrap();
        let mut all = HashMap::new();

        {
            pool.begin_writing().unwrap();

            for i in boundary_sizes() {
                let ch = make_random_chunk(i, i);
                pool.add(&ch).unwrap();
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
                pool.add(&ch).unwrap();
                let oi = all.insert(ch.oid().clone(), ch);
                match oi {
                    None => (),
                    Some(_) => panic!("Duplicate chunk in test"),
                }
            }

            pool.flush().unwrap();
        }

        // Verify all of them.
        for (key, c1) in all.iter() {
            let c2 = pool.find(key).unwrap();
            assert_eq!(c1.kind(), c2.kind());
            assert_eq!(c1.oid(), c2.oid());
            assert_eq!(&c1.data()[..], &c2.data()[..]);
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
            pool.begin_writing().unwrap();

            for i in 0..1000 {
                let ch = make_kinded_random_chunk(Kind::new("back").unwrap(), 64, i);
                pool.add(&ch).unwrap();
                oids.insert(ch.oid().clone());
            }
            pool.flush().unwrap();
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

static POOL_SCHEMA: sql::Schema<'static, PoolInabilities> = sql::Schema {
    version: "1:2014-03-18",
    schema: &[r#"PRAGMA PAGE_SIZE=8192"#,
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
              r#"CREATE INDEX ctime_cache_pkey ON ctime_cache(pkey)"#],
    compats: &[sql::SchemaCompat {
                   version: "1:2014-03-13",
                   inabilities: &[PoolInabilities::NoFilesystems, PoolInabilities::NoCTimeCache],
               }],
};
