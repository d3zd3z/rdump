// Index building.

extern crate cas;

use cas::{Kind, Oid};
use cas::pool::adump::{FileIndex, RamIndex /*, RamInfo */};

fn main () {
    /*
    let count = 1000;
    println!("Reading {} index files", count);
    let mut all_idx = vec![];
    let mut size = 0;
    for _ in 0 .. count {
        let idx = IndexFile::load("tpool/pool-data-0000.idx").unwrap();
        size += idx.len();
        all_idx.push(idx);
    }
    println!("Done: {} hashes", size);
    */

    let mut kinds = vec![];
    kinds.push(Kind::new("blob").unwrap());
    kinds.push(Kind::new("idx0").unwrap());
    kinds.push(Kind::new("idx1").unwrap());
    kinds.push(Kind::new("data").unwrap());
    kinds.push(Kind::new("dir ").unwrap());

    let mut idx = RamIndex::new();
    for ofs in 0 .. 10000 {
        idx.insert(Oid::from_u32(ofs), ofs,
            kinds[ofs as usize % kinds.len()]);
    }

    FileIndex::save("testfile.idx", 0x12345678, &idx).unwrap();
}
