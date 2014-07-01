// File (sqlite3) based pools.

use std::io;
use std::io::{fs, IoResult};
use super::sql;
use super::sql::{Schema, SchemaCompat};
use super::{ChunkSync, ChunkSource};
use chunk::{Chunk};

use oid::Oid;
use uuid::Uuid;

// Like try!(), but remaps the SQL error to an IoResult.
macro_rules! sql_try( ($e:expr) => ( try!($e.map_err(sql::to_ioerror))))

pub fn create(path: &Path) -> IoResult<()> {
    try!(fs::mkdir(path, io::UserRWX));
    try!(fs::mkdir(&path.join("blobs"), io::UserRWX));
    let db = sql_try!(sql::open(path.join("data.db").as_str().unwrap()));
    sql_try!(pool_schema.set(&db));

    sql_try!(sql::sql_simple(&db,
                             "INSERT INTO props (key, value) VALUES (?, ?)",
                             &[sql::Text("uuid".to_string()),
                             sql::Text(Uuid::new_v4().to_hyphenated_str())]));
    Ok(())
}

pub struct FilePool {
    db: sql::Database,
    path: Path,
    uuid: Uuid,
}

impl FilePool {
    pub fn open(path: Path) -> IoResult<FilePool> {
        let db = sql_try!(sql::open(path.join("data.db").as_str().unwrap()));
        let _features = sql_try!(pool_schema.check(&db));

        // Retrieve the uuid.
        // TODO: Obviously, we need a better way of decoding these.
        let uuid = match sql_try!(sql::sql_one(&db, "SELECT value FROM props WHERE key = 'uuid'", &[])) {
            None => fail!("No uuid present"),
            // TODO: Vector patterns would be nice.
            Some(elts) => {
                match elts.as_slice() {
                    [sql::Text(ref text)] => match Uuid::parse_string(text.as_slice()) {
                        Ok(u) => u,
                        Err(e) => fail!("Invalid uuid: {}", e)
                    },
                    _ => fail!("Invalid column result for uuid")
                }
            }
        };

        Ok(FilePool {
            db: db,
            path: path,
            uuid: uuid
        })
    }
}

impl Collection for FilePool {
    fn len(&self) -> uint {
        fail!("TODO");
    }
}

impl ChunkSource for FilePool {
    fn find(&mut self, key: &Oid) -> IoResult<Box<Chunk>> {
        fail!("TODO");
    }

    fn uuid<'a>(&'a self) -> &'a Uuid {
        &self.uuid
    }
}

impl ChunkSync for FilePool {
    fn add(&mut self, chunk: &Chunk) -> IoResult<()> {
        fail!("TODO");
    }

    fn flush(&mut self) -> IoResult<()> {
        fail!("TODO");
    }
}

enum PoolInabilities {
    NoFilesystems,
    NoCTimeCache,
}

static pool_schema: Schema<PoolInabilities> =
    Schema {
        version: "1:2014-03-18",
        schema: &[
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
            SchemaCompat {
                version: "1:2014-03-13",
                inabilities: &[ NoFilesystems, NoCTimeCache ]
            } ],
    };

#[cfg(test)]
mod test {
    use super::create;
    use testutil::TempDir;

    #[test]
    fn simple_create() {
        let tmp = TempDir::new();
        create(&tmp.join("blort")).unwrap();
    }
}
