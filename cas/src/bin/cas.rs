// Cas main

extern crate cas;

#[macro_use]
extern crate timeit;

use cas::Kind;
use cas::pool::ChunkSource;
use cas::pool::{AdumpPool, FilePool, RamPool};
use std::error;
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;
use std::result;

pub type Result<T> = result::Result<T, Box<error::Error + Send + Sync>>;

fn main() {
    static BASE: &'static str = "/mnt/linaro/optee-qemu/linux";

    // First, dump everything to a ram pool.  This should fill the cache
    // with the read data, so that the subsequent operations 

    let sec = timeit_loops!(1, {
        let mut pool = RamPool::new();
        walk_tree(&mut pool, BASE).unwrap();
    });
    println!("RamPool:: {}", sec);

    cleanup("pool1");
    let sec = timeit_loops!(1, {
        FilePool::create("pool1").unwrap();
        let mut pool = FilePool::open("pool1").unwrap();
        pool.begin_writing().unwrap();
        walk_tree(&mut pool, BASE).unwrap();
        pool.flush().unwrap();
    });
    println!("FilePool: {}", sec);

    cleanup("pool2");
    let sec = timeit_loops!(1, {
        AdumpPool::new_builder("pool2").create().unwrap();
        let mut pool = AdumpPool::open("pool2").unwrap();
        walk_tree(&mut pool, BASE).unwrap();
        pool.flush().unwrap();
    });
    println!("AdumpPool: {}", sec);
}

fn walk_tree<P: AsRef<Path>>(pool: &mut ChunkSource, tree: P) -> Result<()> {
    let mut walk = Walker::new(pool);
    try!(walk.walk(tree.as_ref()));
    println!("Total:\n{:?}", walk.info);
    Ok(())
}

fn cleanup<P: AsRef<Path>>(path: P) {
    let path = path.as_ref();
    if path.exists() {
        fs::remove_dir_all(path).unwrap();
    }
}

struct Walker<'a> {
    pool: &'a mut ChunkSource,

    info: WalkInfo,
}

#[derive(Debug)]
struct WalkInfo {
    files: u64,
    dirs: u64,
    chunks: u64,
    bytes: u64,
    dup_chunks: u64,
    dup_bytes: u64,
}

impl<'a> Walker<'a> {

    fn new(pool: &mut ChunkSource) -> Walker {
        Walker {
            pool: pool,
            info: WalkInfo {
                files: 0,
                dirs: 0,
                chunks: 0,
                bytes: 0,
                dup_chunks: 0,
                dup_bytes: 0,
            },
        }
    }

    // Walk a filesystem at a given path, chop up all of the data and write it to the store.  We
    // don't keep any of this data, and the whole point here is to measure performance of the
    // pools.
    fn walk(&mut self, name: &Path) -> Result<()> {
        self.pool.begin_writing()?;
        try!(self.iwalk(name));
        self.pool.flush()?;
        Ok(())
    }

    fn iwalk(&mut self, name: &Path) -> Result<()> {
        // println!("d {:?}", name);

        let mut dirs = vec![];
        let mut files = vec![];

        for entry in try!(fs::read_dir(name)) {
            let entry = try!(entry);
            let path = entry.path();
            let meta = try!(fs::symlink_metadata(&path));
            if meta.is_dir() {
                dirs.push(path);
            } else if meta.is_file() {
                files.push(path);
            } // Skip other node types.
        }

        dirs.sort();
        files.sort();

        // Walk deeply first.
        for dir in &dirs {
            try!(self.iwalk(dir));
        }

        // The process  the files at this level.
        for file in &files {
            try!(self.encode_file(file));
        }

        self.info.dirs += 1;

        Ok(())
    }

    fn encode_file(&mut self, name: &Path) -> Result<()> {
        // print!("- {:?}", name);
        let mut f = try!(File::open(name));

        loop {
            let mut buffer = vec![0u8; 256 * 1024];

            let count = try!(f.read(&mut buffer));
            if count == 0 {
                break;
            }

            buffer.truncate(count);
            let ch = cas::Chunk::new_plain(Kind::new("blob").unwrap(), buffer);

            self.info.chunks += 1;
            // self.info.bytes += count as u64;

            /*
            let payload = match ch.zdata() {
                None => ch.data(),
                Some(zdata) => zdata,
            };
            self.info.bytes += payload.len() as u64;
            */
            if try!(self.pool.contains_key(ch.oid())) {
                self.info.dup_chunks += 1;
                self.info.dup_bytes += count as u64;
            } else {
                try!(self.pool.add(&ch));
                self.info.chunks += 1;
                self.info.bytes += count as u64;
            }
            // print!(".");
        }

        self.info.files += 1;

        // println!("");
        Ok(())
    }
}
