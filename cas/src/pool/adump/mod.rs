// Adump file format.

use Error;
use Result;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use uuid::Uuid;

// TODO: These probably don't need to be exported.
pub use self::index::{FileIndex, RamIndex, PairIndex};

mod index;
pub mod file;

pub struct AdumpPool;

impl AdumpPool {
    pub fn new_builder<P: AsRef<Path>>(dir: P) -> PoolBuilder<P> {
        PoolBuilder {
            dir: dir,
            newfile: false,
            limit: 640 * 1024 * 1024,
        }
    }
}

/// A builder to set parameters before creating a pool.
pub struct PoolBuilder<P: AsRef<Path>> {
    dir: P,
    newfile: bool,
    limit: u32,
}

impl<P: AsRef<Path>> PoolBuilder<P> {
    /// Change the default value of the `newfile` flag on the pool.  If
    /// set to try, files will not be appended to, but each time the pool
    /// is opened for writing, a new file will be created.  This will
    /// create more smaller files, but can, in some situations, make
    /// synchronization easier.
    pub fn set_newfile(mut self, newfile: bool) -> Self {
        self.newfile = newfile;
        self
    }

    /// Change the default value of the `limit` flag on the pool.  No
    /// individual pool file will grow larger than this value.  Note that
    /// this is a u32, but it is best to not allow the value to exceed a
    /// positive i32 to avoid compatibility issues with legacy programs
    /// that may read this format.
    pub fn set_limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Actually create the pool.  The given path must name either an empty
    /// directory, or a path where one can be created.
    pub fn create(self) -> Result<()> {
        // The given directory must represent either an empty directory, or
        // a path that a new directory can be created at.
        let base = self.dir.as_ref();
        try!(ensure_dir(base));
        let meta = base.join("metadata");
        let seen = base.join("seen");

        try!(fs::create_dir(&meta));
        try!(fs::create_dir(&seen));

        {
            let mut fd = try!(File::create(meta.join("props.txt")));
            try!(writeln!(&mut fd, "uuid={}", Uuid::new_v4().hyphenated()));
            try!(writeln!(&mut fd, "newfile={}", self.newfile));
            try!(writeln!(&mut fd, "limit={}", self.limit));
        }

        try!(File::create(meta.join("backups.txt")));

        Ok(())
    }
}

// Ensure that we have an empty directory for the pool.  It can either be
// an existing empty directory (or a symlink to one), or a path where a
// directory can be created.  If the directory doesn't exist, this will
// create it.
fn ensure_dir(base: &Path) -> Result<()> {
    if base.is_dir() {
        // An existing directory is allowed, if it is completely empty.
        for ent in try!(base.read_dir()) {
            let _ = try!(ent);
            return Err(Error::PathError(format!("Directory is not empty: {:?}", base)));
        }
    } else {
        // If not a directory, see if we can create one.
        try!(fs::create_dir(base));
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_pool() {
        AdumpPool::new_builder("blort").create().unwrap();
    }
}
