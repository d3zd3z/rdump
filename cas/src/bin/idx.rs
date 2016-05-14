// Index building.

extern crate cas;

// use cas::{Kind, Oid};
use cas::pdump::HexDump;
use cas::pool::ChunkSource;
use cas::pool::adump::AdumpPool;

fn main() {
    let pool = AdumpPool::open("/a64/tpool").unwrap();
    println!("uuid: {:?}", pool.uuid());

    for back in pool.backups().unwrap() {
        println!("{:?}", back);

        // Ensure this key is present.
        assert!(pool.contains_key(&back).unwrap());

        let ch = pool.find(&back).unwrap();
        (&ch.data()[..]).dump();
    }
}
