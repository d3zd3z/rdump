// Cas main

extern crate cas;

use cas::Kind;
use cas::pool::ChunkSource;
use std::error;
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;
use std::result;

pub type Result<T> = result::Result<T, Box<error::Error + Send + Sync>>;

fn main() {
    cas::pool::FilePool::create(&Path::new("/wd/test-pool/foo")).unwrap();
    let mut pool = cas::pool::open(&Path::new("/wd/test-pool/foo")).unwrap();
    let mut walk = Walker::new(&mut *pool);
    // walk.walk(&Path::new("/mnt/linaro/optee-qemu/.zfs/snapshot/tip-2016-02-10/linux")).unwrap();
    walk.walk(&Path::new("/mnt/linaro/optee-qemu/linux")).unwrap();
    // walk.walk(&Path::new("/mnt/linaro/.zfs/snapshot/tip-2016-02-10")).unwrap();
    println!("Total:\n{:#?}", walk.info);
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
