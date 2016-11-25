// SQL utilities and such.

// TODO: Remove
#![allow(dead_code)]

use rusqlite::{SqliteConnection, SqliteResult};

/// A description of a database schema.  A given schema has a specific
/// version.  It is also possible for there to be older versions that are
/// supported in a degraaded mode.
pub struct Schema<'a, C: Clone + 'a> {
    /// A specific version string for the version described in `schema`
    /// below.
    pub version: &'a str,
    /// The SQL commands that will initialize the database to this schema.
    pub schema: &'a [&'a str],
    /// Possible compatible versions.
    pub compats: &'a [SchemaCompat<'a, C>],
}

/// Each compatible schema will have zero or more inabilities to that
/// database.  These are of type `C`.
pub struct SchemaCompat<'a, C: Clone + 'a> {
    /// The version of this compat.
    pub version: &'a str,
    /// The inabilities we have when this version is seen.
    pub inabilities: &'a [C],
}

impl<'a, C> Schema<'a, C>
    where C: 'a + Clone
{
    /// Given an empty database, create the given schema in it.
    pub fn set(&self, db: &mut SqliteConnection) -> SqliteResult<()> {
        let tx = db.transaction()?;
        for line in self.schema {
            tx.execute(line, &[])?;
        }

        tx.execute("CREATE TABLE schema_version (version TEXT)", &[])?;
        tx.execute("INSERT INTO schema_version VALUES (?)", &[&self.version])?;

        tx.commit()?;
        Ok(())
    }

    /// Check if this schema matches, and if there are any inabilities to
    /// be reported.
    pub fn check(&self, db: &SqliteConnection) -> SqliteResult<Option<Vec<C>>> {
        let mut stmt = db.prepare("SELECT version FROM schema_version")?;
        let mut rows = stmt.query(&[])?;
        let version: String = match rows.next() {
            None => return Ok(None),
            Some(row) => {
                let row = row?;
                row.get(0)
            }
        };

        // Make sure this is the last row.
        match rows.next() {
            None => (),
            Some(_) => panic!("Multiple version in database"),
        }

        if version == self.version {
            return Ok(Some(vec![]));
        }

        // Check any of the compats.
        for compat in self.compats {
            if version == compat.version {
                return Ok(Some(compat.inabilities.to_vec()));
            }
        }

        // Nothing matches, for now just panic.
        // TODO: Improve this.
        panic!("No compatible schema version");
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rusqlite::SqliteConnection;
    use tempdir::TempDir;

    #[derive(PartialOrd, Ord, PartialEq, Eq, Clone)]
    enum Modes {
        NoBar,
    }

    static SCHEMA1: Schema<'static, Modes> = Schema {
        version: "1",
        schema: &[r"CREATE TABLE foo(id INTEGER PRIMARY KEY)"],
        compats: &[],
    };

    #[test]
    fn test_set() {
        let tmp = TempDir::new("sqlpool").unwrap();
        let path = tmp.path();
        let mut conn = SqliteConnection::open(&path.join("blort.db")).unwrap();
        SCHEMA1.set(&mut conn).unwrap();
        SCHEMA1.check(&conn).unwrap();
    }
}
