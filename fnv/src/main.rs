extern crate fnv;
extern crate openssl;

use openssl::crypto::hash::{hash, Type};
use fnv::FnvHasher;
use std::hash::Hasher;

mod bloom;
use bloom::{Bloom, BloomItem};

fn main() {
    if false {
        for i in 0 .. 10 {
            let mut h: FnvHasher = Default::default();
            h.write_usize(i);
            println!("{:5} {:016x}", i, h.finish());
        }
    }

    if true {
        let mut bl = Bloom::new(26, 1);

        static SIZE: usize = 8000000;
        let mut duplicates = 0;
        for i in 0 .. SIZE {
            let h = Fnv256::of_usize(i);
            if bl.maybe_contains(&h) {
                duplicates += 1;
            }
            bl.add(&h);
        }
        println!("{} duplicates during insert", duplicates);

        // Verify that the filter actually works.
        for i in 0 .. SIZE {
            let h = Fnv256::of_usize(i);
            if !bl.maybe_contains(&h) {
                h.dump();
                panic!("Bloom filter failed");
            }
        }

        // Now count the next step, and see how many false positives.
        let mut count = 0;
        for i in SIZE .. 2*SIZE {
            let h = Fnv256::of_usize(i);
            if bl.maybe_contains(&h) {
                count += 1;
            }
        }
        println!("{}/{} false positives ({:.5}%)", count, SIZE,
                 count as f64 / SIZE as f64 * 100.0);
        bl.debug();
    }

    if false {
        let mut bl = Bloom::new(27, 5);

        static SIZE: usize = 8000000;
        let mut duplicates = 0;
        for i in 0 .. SIZE {
            let h = sha1_usize(i);
            if bl.maybe_contains(&h) {
                duplicates += 1;
            }
            bl.add(&h);
        }
        println!("{} duplicates during insert", duplicates);

        // Verify that the filter actually works.
        for i in 0 .. SIZE {
            let h = sha1_usize(i);
            if !bl.maybe_contains(&h) {
                // h.dump();
                panic!("Bloom filter failed");
            }
        }

        // Now count the next step, and see how many false positives.
        let mut count = 0;
        for i in SIZE .. 2*SIZE {
            let h = sha1_usize(i);
            if bl.maybe_contains(&h) {
                count += 1;
            }
        }
        println!("{}/{} false positives ({:.5}%)", count, SIZE,
                 count as f64 / SIZE as f64 * 100.0);
        bl.debug();
    }
}

// Compute the sha1 of a usize.
struct Sha1Result(Vec<u8>);
fn sha1_usize(item: usize) -> Sha1Result {
    let mut block = vec![0u8; 8];
    let mut tmp = item;
    for i in 0 .. 8 {
        block[i] = tmp as u8;
        tmp >>= 8;
    }
    Sha1Result(hash(Type::SHA1, &block))
}

impl BloomItem for Sha1Result {
    fn get_key(&self, index: usize) -> u32 {
        let mut result = 0;
        for offset in 4*index .. 4*(index+1) {
            result = (result << 8) | self.0[offset] as u32;
        }
        result
    }
}

// Manual computation of an FNV256 number.
#[derive(Clone)]
struct Fnv256([u32; 8]);

impl Default for Fnv256 {
    fn default() -> Fnv256 {
        // The default value is a little endian representation of the FNV
        // offset-bias value.
        Fnv256([
               0xcaee0535,
               0x1023b4c8,
               0x47b6bbb3,
               0xc8b15368,
               0xc4e576cc,
               0x2d98c384,
               0xaac55036,
               0xdd268dbc,
        ])
    }
}

impl Fnv256 {

    // Generate an fnv hash, with sufficient dispersion based on a single usize.
    pub fn of_usize(value: usize) -> Fnv256 {
        let mut h: Fnv256 = Default::default();
        let mut tmp;

        // Since fnv is linear, hashing small integers will result in very
        // few hash collisions.  We can mitigate this a little, but running
        // the hash some extra times (although that takes away much of the
        // benefit of using it).
        for _ in 0 .. 5 {
            tmp = value;
            for _ in 0 .. 8 {
                h.add_byte(tmp as u8);
                tmp >>= 8;
            }
        }

        /*
        // Mix up the results, since the values take some rounds to proprage to the middle of the
        // hash value.
        for _ in 0 .. 16 {
            h.add_byte(0);
        }
        */
        h
    }

    fn add_byte(&mut self, byte: u8) {
        // Fnv-1a do the xor first.
        self.0[0] ^= byte as u32;

        let old = self.clone();

        for i in 0..8 {
            self.0[i] = 0;
        }

        // The factor is 2^168 + 2^8 + 0x63.
        multiply(&mut self.0, &old.0, 0x163);

        // 2^168 is 2^8 * 2^(5*32)
        multiply(&mut self.0[5..], &old.0, 0x100);
    }

    #[allow(dead_code)]
    fn dump(&self) {
        print!("0x");
        for i in 0..8 {
            print!(" {:08X}", self.0[7-i]);
        }
        println!("");
    }
}

// dest += src * num, with overflow discarded.
fn multiply(dest: &mut [u32], src: &[u32], num: u32) {
    let mut carry = 0;
    let mut add_carry = 0;

    for pos in 0..dest.len() {
        let tmp = (src[pos] as u64 * num as u64) + carry;
        carry = tmp >> 32;
        let tmp = tmp as u32 as u64;

        let add_tmp = dest[pos] as u64 + (tmp as u64) + add_carry;
        add_carry = add_tmp >> 32;
        dest[pos] = add_tmp as u32;
    }
}

impl BloomItem for Fnv256 {
    fn get_key(&self, index: usize) -> u32 {
        self.0[index]
    }
}
