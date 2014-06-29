// SQL Support.

pub use sqlite3::{
    Database,
    ResultCode, SqliteResult,
    BindArg, Text,
    SQLITE_OK, SQLITE_DONE, SQLITE_ROW
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

/// Schema support.

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
            SQLITE_ROW => version = cur.get_text(0),
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

/// Each version of the compatible database will have zero or more
/// inabilities to that database.  These are of type `C` which is
/// specific to the given database.
pub struct SchemaCompat<C> {
    /// The version of this compat.
    version: &'static str,
    /// The inabilities we have when this version is seen.
    inabilities: &'static [C],
}

#[cfg(test)]
mod test {
    use super::{Schema, SchemaCompat};
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
}
