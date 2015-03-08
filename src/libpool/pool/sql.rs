// SQL Support.

// TODO: For development, be sure to remove.
#![allow(dead_code)]

// use std::io;
use sqlite3::{
//     open,
    // Cursor,
    Database,
    ResultCode, SqliteResult,
    BindArg,
//     ColumnType,
//     BindArg, Integer, Text, Float64, Blob, Null,
//     SQLITE_OK, SQLITE_DONE, SQLITE_ROW,

//     SQLITE_INTEGER, SQLITE_FLOAT, SQLITE_TEXT, SQLITE_BLOB,
//     SQLITE_NULL,
};

use sqlite3::BindArg::*;
use sqlite3::ColumnType::*;
use sqlite3::ResultCode::*;

use std::cell::{Cell, RefCell};

// A single connection to an sqlite database.
pub struct Connection {
    db: RefCell<Database>,
    in_xact: Cell<bool>,
}

impl Connection {
    pub fn new(p: &Path) -> SqliteResult<Connection> {
        Ok(Connection {
            db: RefCell::new(try!(::sqlite3::open(p.as_str().unwrap()))),
            in_xact: Cell::new(false),
        })
    }

    // TODO: Better binding possibility than using the BindArg code.  Put some
    // though into that.
    pub fn execute(&self, sql: &str, values: &[BindArg]) -> SqliteResult<()> {
        let db = self.db.borrow_mut();
        let mut cur = try!(db.prepare(sql, &None));
        try!(cur.bind_params(values).ok());
        match cur.step() {
            SQLITE_DONE => Ok(()),
            e => Err(e),
        }
    }

    // 'db' doesn't live long enough to make it to the end.
    /*
    pub fn prepare<'con>(&'con self, sql: &str, values: &[BindArg]) -> SqliteResult<Prepared<'con>> {
        let db = self.db.borrow_mut();
        let mut cur = try!(db.prepare(sql, &None));
        try!(cur.bind_params(values).ok());
        Ok(Prepared { cur: cur })
    }
    */

    // Transaction control.
    pub fn begin(&self) -> SqliteResult<()> {
        assert!(!self.in_xact.get());
        self.in_xact.set(true);
        self.execute("BEGIN TRANSACTION", &[])
    }

    // Transaction control.
    pub fn commit(&self) -> SqliteResult<()> {
        assert!(self.in_xact.get());
        let result = self.execute("COMMIT", &[]);
        self.in_xact.set(false);
        result
    }

    // Transaction control.
    pub fn rollback(&self) -> SqliteResult<()> {
        assert!(self.in_xact.get());
        let result = self.execute("ROLLBACK", &[]);
        self.in_xact.set(false);
        result
    }
}

// A prepared statement with its own life.
/*
pub struct Prepared<'con> {
    cur: Cursor<'con>,
}
*/

// First, some utilities to make sqlite3 a little easier to use.

/// Some SQL routines return just a plain ResultCode, and not an SqliteResult.
/// Augment that with a method that can wrap this in a result code.
pub trait ToSqliteResult<T> {
    fn ok(self) -> SqliteResult<T>;
}

impl ToSqliteResult<()> for ResultCode {
    fn ok(self) -> SqliteResult<()> {
        match self {
            ResultCode::SQLITE_OK => Ok(()),
            e => Err(e)
        }
    }
}

/// Execute an SQL statement, with parameters, that expects no
/// results.
pub fn simple(db: &Database, sql: &str, values: &[BindArg]) -> SqliteResult<()> {
    let mut cur = try!(db.prepare(sql, &None));
    try!(cur.bind_params(values).ok());
    match cur.step() {
        ResultCode::SQLITE_DONE => Ok(()),
        e => Err(e),
    }
}

/// Execute an SQL query, with parameters, that expects a single
/// result row.
pub fn one(db: &Database, sql: &str, values: &[BindArg]) -> SqliteResult<Option<Vec<BindArg>>> {
    let mut cur = try!(db.prepare(sql, &None));
    try!(cur.bind_params(values).ok());
    let mut result = Vec::new();
    match cur.step() {
        ResultCode::SQLITE_DONE => return Ok(None),
        ResultCode::SQLITE_ROW => {
            for i in (0 .. cur.get_column_count()) {
                let res = match cur.get_column_type(i) {
                    SQLITE_INTEGER => Integer(cur.get_int(i)),
                    SQLITE_FLOAT   => Float64(cur.get_f64(i)),
                    SQLITE_TEXT    => Text(cur.get_text(i).unwrap().to_string()),
                    SQLITE_BLOB    => Blob(cur.get_blob(i).unwrap().to_vec()),
                    SQLITE_NULL    => Null,
                };
                result.push(res);
            }
        },
        e => return Err(e),
    };

    // Make sure a single row, and that we fininsh the transaction.
    match cur.step() {
        ResultCode::SQLITE_DONE => (),
        e => return Err(e),
    };

    Ok(Some(result))
}

/// Schema support.

// TODO: Can this be done with other than a static lifetime?
/// A description of a database schema.  A given schema has a specific
/// version.  It is also possible for there to be older versions that
/// are supported in a degraded mode.
pub struct Schema<'a, C: Clone + 'a> {
    /// A specific version string for the version described in
    /// `schema` below.
    pub version: &'a str,
    /// The SQL commands that will initialize the database to this
    /// schema.
    pub schema: &'a [&'a str],
    /// Possible compatible versions.
    pub compats: &'a [SchemaCompat<'a, C>],
}

/// Each version of the compatible database will have zero or more
/// inabilities to that database.  These are of type `C` which is
/// specific to the given database.
pub struct SchemaCompat<'a, C: Clone + 'a> {
    /// The version of this compat.
    pub version: &'a str,
    /// The inabilities we have when this version is seen.
    pub inabilities: &'a [C],
}

impl<'a, C> Schema<'a, C> where C: 'a + Clone {
    /// Given an empty database, create the given schema in it.
    pub fn set(&self, db: &Connection) -> SqliteResult<()> {
        try!(db.begin());
        for &line in self.schema.iter() {
            try!(db.execute(line, &[]));
        }
        try!(db.execute("CREATE TABLE schema_version (version TEXT)", &[]));
        try!(db.execute("INSERT INTO schema_version VALUES (?)",
            &[Text(self.version.to_string())]));
        Ok(())
    }

    /// Check if this schema matches, and if there are any inabilities
    /// to be reported.
    pub fn check(&self, _db: &Connection) -> SqliteResult<Vec<C>> {
        panic!("TODO");
        /*
        let mut cur = try!(db.prepare("SELECT version FROM schema_version", &None));
        let version: String;
        match cur.step() {
            SQLITE_ROW => version = cur.get_text(0).unwrap().to_string(),
            e => return Err(e)
        }

        // Make sure there aren't any other rows returned.
        match cur.step() {
            SQLITE_DONE => (),
            SQLITE_ROW => panic!("Multiple versions in database"),
            e => return Err(e)
        }

        if version.as_slice() == self.version {
            return Ok(vec![])
        }

        for compat in self.compats.iter() {
            if version.as_slice() == compat.version {
                return Ok(compat.inabilities.to_vec());
            }
        }

        // This isn't really an Sqlite failure, so just fail here.
        panic!("No compatible database schema found");
        */
    }
}

/* Is seems challenging to do any of this safely.
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
    db: &'a mut Database,
    committed: bool
}

#[cfg(test)]
impl<'a> Transaction<'a> {
    pub fn new(db: &'a mut Database) -> SqliteResult<Transaction<'a>> {
        try!(simple(db, "BEGIN TRANSACTION", &[]));
        Ok(Transaction {
            db: db,
            committed: false
        })
    }

    pub fn commit(&mut self) -> SqliteResult<()> {
        assert!(!self.committed);
        try!(simple(self.db, "COMMIT", &[]));
        self.committed = true;
        Ok(())
    }

    // Sometimes, it's handy to just wrap a function in a transaction.
    // This calls 'f', and commits, if 'f' returns an "Ok" result.
    pub fn with_xact<U, F>(db: &'a mut Database, f: F) -> SqliteResult<U>
        where F: FnOnce(&'a mut Database) -> SqliteResult<U>
    {
        let mut xact = try!(Transaction::new(db));
        match f(db) {
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
// https://github.com/rust-lang/rust/pull/21022 and friends to implement safely
// checking these.  As it stands now, this is probably not actually safe, hence
// enabling it only for tests.
impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if !self.committed {
            match simple(self.db, "ROLLBACK", &[]) {
                Ok(_) => (),
                Err(e) => panic!("Error rolling back transaction: {:?}", e)
            }
        }
    }
}
*/

#[cfg(test)]
mod test {
    use super::*;
    use std::io::TempDir;
    // use std::collections::HashSet;
    /*
    use super::{Schema, SchemaCompat, Transaction, SqliteResult};
    use super::{SQLITE_DONE, SQLITE_ROW};
    use super::{Integer};
    use super::{sql_one};
    use testutil::TempDir;
    */

    #[derive(PartialOrd, Ord, PartialEq, Eq, Clone)]
    enum Modes {
        NoBar
    }

    static SCHEMA1: Schema<'static, Modes> =
        Schema {
            version: "1",
            schema: &[
                r"CREATE TABLE foo(id INTEGER PRIMARY KEY)",
            ],
            compats: &[]
        };

    static SCHEMA2: Schema<'static, Modes> =
        Schema {
            version: "2",
            schema: &[
                r"CREATE TABLE foo(id INTEGER PRIMARY KEY, bar TEXT)",
            ],
            compats: &[
                SchemaCompat {
                    version: "1",
                    inabilities: &[ Modes::NoBar ]
                } ],
        };

    #[test]
    fn test_set() {
        let tmp = TempDir::new("sql").unwrap();
        let con = Connection::new(&tmp.path().join("test1.db")).unwrap();
        SCHEMA1.set(&con).unwrap();
        SCHEMA1.check(&con).unwrap();
        /*
        let mut db = ::sqlite3::open(tmp.path().join("test1.db").as_str().unwrap()).unwrap();
        SCHEMA1.set(&mut db).unwrap();
        SCHEMA1.check(&db).unwrap();
        */
    }

    /*
    #[test]
    fn test_compat() {
        let tmp = TempDir::new("sql").unwrap();
        let mut db = ::sqlite3::open(tmp.path().join("test2.db").as_str().unwrap()).unwrap();
        SCHEMA1.set(&mut db).unwrap();

        static EMPTY: &'static [Modes] = &[];
        assert!(SCHEMA1.check(&db).unwrap().as_slice() == EMPTY);

        static NOBAR: &'static [Modes] = &[Modes::NoBar];
        assert!(SCHEMA2.check(&db).unwrap() == NOBAR);
    }

    // Try adding the number to the database.
    fn add_number(db: &Database, num: int) -> SqliteResult<()> {
        super::simple(db, "INSERT INTO foo VALUES (?)", &[Integer(num)])
    }

    fn check_numbers(db: &Database) -> SqliteResult<HashSet<int>> {
        let mut cur = try!(db.prepare("SELECT id FROM foo", &None));
        let mut result = HashSet::new();
        loop {
            match cur.step() {
                ResultCode::SQLITE_DONE => break,
                ResultCode::SQLITE_ROW => result.insert(cur.get_int(0)),
                e => return Err(e)
            };
        }
        Ok(result)
    }

    fn add_abort(db: &Database, num: int) -> SqliteResult<()> {
        let _xact = try!(Transaction::new(db));
        try!(simple(db, "INSERT INTO foo VALUES (?)", &[Integer(num)]));
        // Don't commit.
        Ok(())
    }

    #[test]
    fn transaction_test() {
        let tmp = TempDir::new("sql").unwrap();
        let mut db = ::sqlite3::open(tmp.path().join("test2.db").as_str().unwrap()).unwrap();
        Transaction::with_xact(&mut db, |db| SCHEMA1.set(db)).unwrap();
        Transaction::with_xact(&mut db, |db| add_number(db, 10)).unwrap();
        let good1 = [10i].iter().map(|&x| x).collect();
        assert!(Transaction::with_xact(&mut db, |db| check_numbers(db)).unwrap() == good1);

        add_abort(&db, 11).unwrap();
        assert!(Transaction::with_xact(&mut db, |db| check_numbers(db)).unwrap() == good1);
    }
    */

    /*
    #[test]
    fn one_test() {
        let tmp = TempDir::new();
        let db = ::sqlite3::open(tmp.join("xact.db").as_str().unwrap()).unwrap();
        Transaction::with_xact(&db, || SCHEMA1.set(&db)).unwrap();

        assert!(sql_one(&db, "SELECT id FROM foo where id = 42", &[]) == Ok(None));
        Transaction::with_xact(&db, || add_number(&db, 10)).unwrap();
        assert!(sql_one(&db, "SELECT id FROM foo where id = 42", &[]) == Ok(None));
        assert!(sql_one(&db, "SELECT id FROM foo where id = 10", &[]) == Ok(Some(vec![Integer(10)])));
    }
    */
}
