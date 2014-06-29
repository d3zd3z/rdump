// Main dump program.

#![feature(macro_rules)]

extern crate pool;
extern crate sqlite3;

use sqlite3::database::Database;
use sqlite3::SqliteResult;
use sqlite3::BindArg;

fn main() {
    let db = sqlite3::open("test.db").unwrap();

    pool_schema.set(&db).unwrap();
}

impl<C> Schema<C> {
    fn set(&self, db: &Database) -> SqliteResult<()> {
        for line in self.schema.iter() {
            try!(db.exec(*line));
        }
        try!(db.exec("CREATE TABLE schema_version (version TEXT)"));
        try!(sql_simple(db, "INSERT INTO schema_version VALUES (?)",
            &[sqlite3::Text(self.version.to_string())]));
        Ok(())
    }
}

// A utility to execute an SQL statement, with parameters, but no
// results.
fn sql_simple(db: &Database, sql: &str, values: &[BindArg]) -> SqliteResult<()> {
    let cur = try!(db.prepare(sql, &None));
    try!(sql_check(cur.bind_params(values)));
    match cur.step() {
        sqlite3::SQLITE_DONE => Ok(()),
        e => Err(e)
    }
}

// Some SQL routines return just a plain ResultCode, and not a
// SqliteResult.  Wrap this, such that SQLITE_OK returns Ok(()), and
// any other code returns it in an Err.
fn sql_check(code: sqlite3::ResultCode) -> SqliteResult<()> {
    match code {
        sqlite3::SQLITE_OK => Ok(()),
        e => Err(e)
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

/// A description of a database schema.  A given schema has a specific
/// version.  It is also possible for there to be older versions that
/// are supported in a degraded mode.
pub struct Schema<C> {
    /// A specific version string for the version described in
    /// `schema` below.
    version: &'static str,
    /// The SQL commands that will initialize the database to this
    /// schema.
    schema: &'static [&'static str],
    /// Possible compatible versions.
    compats: &'static [SchemaCompat<C>],
}

/// Each version of the compatible database will have zero or more
/// inabilities to that database.  These are of type `C` which is
/// specific to the given database.
pub struct SchemaCompat<C> {
    /// The version of this compat.
    version: &'static str,
    /// The inabilities we have when this version is seen.
    inabilities: &'static [C],
}
