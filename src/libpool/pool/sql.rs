// SQL Support.

use std::io;
pub use sqlite3::{
    open,
    Database,
    ResultCode, SqliteResult,
    BindArg, Integer, Text, Float64, Blob, Null,
    SQLITE_OK, SQLITE_DONE, SQLITE_ROW,

    SQLITE_INTEGER, SQLITE_FLOAT, SQLITE_TEXT, SQLITE_BLOB,
    SQLITE_NULL,
};

// First, some utilities to make sqlite3 a little easier to use.

/// Some SQL routines return just a plain ResultCode, and not an
/// SqliteResult.  Wrap this, such that SQLITE_OK returns Ok((()), and
/// any other code returns it in an Err.
pub fn sql_check(code: ResultCode) -> SqliteResult<()> {
    match code {
        SQLITE_OK => Ok(()),
        e => Err(e)
    }
}

/// Execute an SQL statement, with parameters, that expects no
/// results.
pub fn sql_simple(db: &Database, sql: &str, values: &[BindArg]) -> SqliteResult<()> {
    let cur = try!(db.prepare(sql, &None));
    try!(sql_check(cur.bind_params(values)));
    match cur.step() {
        SQLITE_DONE => Ok(()),
        e => Err(e)
    }
}

/// Execute an SQL query, with parameters, that expects a single
/// result row.
pub fn sql_one(db: &Database, sql: &str, values: &[BindArg]) -> SqliteResult<Option<Vec<BindArg>>> {
    let cur = try!(db.prepare(sql, &None));
    try!(sql_check(cur.bind_params(values)));
    let mut result = Vec::new();
    match cur.step() {
        SQLITE_DONE => return Ok(None),
        SQLITE_ROW => {
            for i in range(0, cur.get_column_count()) {
                let res = match cur.get_column_type(i) {
                    SQLITE_INTEGER => Integer(cur.get_int(i)),
                    SQLITE_FLOAT   => Float64(cur.get_f64(i)),
                    SQLITE_TEXT    => Text(cur.get_text(i).unwrap().to_string()),
                    SQLITE_BLOB    => Blob(cur.get_blob(i).unwrap().to_vec()),
                    SQLITE_NULL    => Null
                };
                result.push(res);
            }
        },
        e => return Err(e)
    };

    // Make sure it is the one and only row.
    match cur.step() {
        SQLITE_DONE => (),
        e => return Err(e)
    };

    Ok(Some(result))
}

/// Convert an ResultCode into an IOError
pub fn to_ioerror(rc: ResultCode) -> io::IoError {
    io::IoError {
        kind: io::OtherIoError,
        desc: "SQLite3 error",
        detail: Some(format!("SQLite: {}", rc).to_string())
    }
}

/// Schema support.

/// A description of a database schema.  A given schema has a specific
/// version.  It is also possible for there to be older versions that
/// are supported in a degraded mode.
pub struct Schema<C> {
    /// A specific version string for the version described in
    /// `schema` below.
    pub version: &'static str,
    /// The SQL commands that will initialize the database to this
    /// schema.
    pub schema: &'static [&'static str],
    /// Possible compatible versions.
    pub compats: &'static [SchemaCompat<C>],
}

impl<C> Schema<C> {
    /// Given an empty database, create the given schema in it.
    pub fn set(&self, db: &Database) -> SqliteResult<()> {
        for &line in self.schema.iter() {
            try!(db.exec(line));
        }
        try!(db.exec("CREATE TABLE schema_version (version TEXT)"));
        try!(sql_simple(db, "INSERT INTO schema_version VALUES (?)",
            &[Text(self.version.to_string())]));
        Ok(())
    }

    /// Check if this schema matches, and if there are any inabilities
    /// to be reported.
    pub fn check<'a>(&'a self, db: &Database) -> SqliteResult<&'a [C]> {
        let cur = try!(db.prepare("SELECT version FROM schema_version", &None));
        let version: String;
        match cur.step() {
            SQLITE_ROW => version = cur.get_text(0).unwrap().to_string(),
            e => return Err(e)
        }

        // Make sure there aren't any other rows returned.
        match cur.step() {
            SQLITE_DONE => (),
            SQLITE_ROW => fail!("Multiple versions in database"),
            e => return Err(e)
        }

        if version.as_slice() == self.version {
            return Ok(&[])
        }

        for compat in self.compats.iter() {
            if version.as_slice() == compat.version {
                return Ok(compat.inabilities);
            }
        }

        // This isn't really an Sqlite failure, so just fail here.
        fail!("No compatible database schema found");
    }
}

/// A sequence of operations can be wrapped in a transaction.
/// Currently, transactions cannot be nested.  If a transaction
/// executes a `commit` before being dropped, then the operations will
/// be committed, otherwise they will be rolled back.  Although the
/// database doesn't sequence it, operations performed after the
/// commit will not be part of the transaction.
///
/// TODO: Are savepoints useful?
#[cfg(test)]
pub struct Transaction<'a> {
    db: &'a Database,
    committed: bool
}

#[cfg(test)]
impl<'a> Transaction<'a> {
    pub fn new(db: &'a Database) -> SqliteResult<Transaction<'a>> {
        try!(sql_simple(db, "BEGIN TRANSACTION", &[]));
        Ok(Transaction {
            db: db,
            committed: false
        })
    }

    pub fn commit(&mut self) -> SqliteResult<()> {
        assert!(!self.committed);
        try!(sql_simple(self.db, "COMMIT", &[]));
        self.committed = true;
        Ok(())
    }

    // Sometimes, it's handy to just wrap a function in a transaction.
    // This calls 'f', and commits, if 'f' returns an "Ok" result.
    pub fn with_xact<U>(db: &'a Database, f: || -> SqliteResult<U>) -> SqliteResult<U> {
        let mut xact = try!(Transaction::new(db));
        match f() {
            Ok(r) => {
                try!(xact.commit());
                Ok(r)
            },
            e => e
        }
    }
}

#[cfg(test)]
#[unsafe_destructor]
impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if !self.committed {
            match sql_simple(self.db, "ROLLBACK", &[]) {
                Ok(_) => (),
                Err(e) => fail!("Error rolling back transaction: {}", e)
            }
        }
    }
}

/// Each version of the compatible database will have zero or more
/// inabilities to that database.  These are of type `C` which is
/// specific to the given database.
pub struct SchemaCompat<C> {
    /// The version of this compat.
    pub version: &'static str,
    /// The inabilities we have when this version is seen.
    pub inabilities: &'static [C],
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use super::{Schema, SchemaCompat, Transaction, SqliteResult};
    use super::{SQLITE_DONE, SQLITE_ROW};
    use super::{Integer};
    use super::{sql_one};
    use testutil::TempDir;

    #[deriving(PartialEq)]
    enum Modes {
        NoBar
    }

    static schema1: Schema<Modes> =
        Schema {
            version: "1",
            schema: &[
                r"CREATE TABLE foo(id INTEGER PRIMARY KEY)",
            ],
            compats: &[]
        };

    static schema2: Schema<Modes> =
        Schema {
            version: "2",
            schema: &[
                r"CREATE TABLE foo(id INTEGER PRIMARY KEY, bar TEXT)",
            ],
            compats: &[
                SchemaCompat {
                    version: "1",
                    inabilities: &[ NoBar ]
                } ],
        };

    #[test]
    fn test_set() {
        let tmp = TempDir::new();
        let db = ::sqlite3::open(tmp.join("test1.db").as_str().unwrap()).unwrap();
        schema1.set(&db).unwrap();
        schema1.check(&db).unwrap();
    }

    #[test]
    fn test_compat() {
        let tmp = TempDir::new();
        let db = ::sqlite3::open(tmp.join("test2.db").as_str().unwrap()).unwrap();
        schema1.set(&db).unwrap();
        assert!(schema1.check(&db).unwrap() == &[]);
        assert!(schema2.check(&db).unwrap() == &[NoBar]);
    }

    // Try adding the number to the database.
    fn add_number(db: &super::Database, num: int) -> SqliteResult<()> {
        super::sql_simple(db, "INSERT INTO foo VALUES (?)", &[super::Integer(num)])
    }

    fn check_numbers(db: &super::Database) -> SqliteResult<HashSet<int>> {
        let cur = try!(db.prepare("SELECT id FROM foo", &None));
        let mut result = HashSet::new();
        loop {
            match cur.step() {
                SQLITE_DONE => break,
                SQLITE_ROW => result.insert(cur.get_int(0)),
                e => return Err(e)
            };
        }
        Ok(result)
    }

    fn add_abort(db: &super::Database, num: int) -> SqliteResult<()> {
        let _xact = try!(Transaction::new(db));
        try!(super::sql_simple(db, "INSERT INTO foo VALUES (?)", &[super::Integer(num)]));
        // Don't commit.
        Ok(())
    }

    #[test]
    fn transaction_test() {
        let tmp = TempDir::new();
        let db = ::sqlite3::open(tmp.join("xact.db").as_str().unwrap()).unwrap();
        Transaction::with_xact(&db, || schema1.set(&db)).unwrap();
        Transaction::with_xact(&db, || add_number(&db, 10)).unwrap();
        let good1 = [10i].iter().map(|&x| x).collect();
        assert!(Transaction::with_xact(&db, || check_numbers(&db)).unwrap() == good1);

        add_abort(&db, 11).unwrap();
        assert!(Transaction::with_xact(&db, || check_numbers(&db)).unwrap() == good1);
    }

    #[test]
    fn one_test() {
        let tmp = TempDir::new();
        let db = ::sqlite3::open(tmp.join("xact.db").as_str().unwrap()).unwrap();
        Transaction::with_xact(&db, || schema1.set(&db)).unwrap();

        assert!(sql_one(&db, "SELECT id FROM foo where id = 42", &[]) == Ok(None));
        Transaction::with_xact(&db, || add_number(&db, 10)).unwrap();
        assert!(sql_one(&db, "SELECT id FROM foo where id = 42", &[]) == Ok(None));
        assert!(sql_one(&db, "SELECT id FROM foo where id = 10", &[]) == Ok(Some(vec![Integer(10)])));
    }
}
