// File (sqlite3) based pools.

use std::io;
use std::io::{fs, IoResult};
use super::sql;
use super::sql::{Schema, SchemaCompat};
use uuid::Uuid;

pub fn create(path: &Path) -> IoResult<()> {
    try!(fs::mkdir(path, io::UserRWX));
    try!(fs::mkdir(&path.join("blobs"), io::UserRWX));
    let db = try!(sql::open(path.join("data.db").as_str().unwrap())
                 .map_err(sql::to_ioerror));
    try!(pool_schema.set(&db).map_err(sql::to_ioerror));

    try!(sql::sql_simple(&db,
                         "INSERT INTO props (key, value) VALUES (?, ?)",
                         &[sql::Text("uuid".to_string()),
                         sql::Text(Uuid::new_v4().to_hyphenated_str())])
         .map_err(sql::to_ioerror));
    Ok(())
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
