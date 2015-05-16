// Test utilities.

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::fmt::Write;
use std::num::Wrapping;
use chunk;
use kind::Kind;

// A short list of words to help generate reasonably compressible
// data.
static WORD_LIST: &'static [&'static str] = &[
  "the", "be", "to", "of", "and", "a", "in", "that", "have", "I",
  "it", "for", "not", "on", "with", "he", "as", "you", "do", "at",
  "this", "but", "his", "by", "from", "they", "we", "say", "her",
  "she", "or", "an", "will", "my", "one", "all", "would", "there",
  "their", "what", "so", "up", "out", "if", "about", "who", "get",
  "which", "go", "me", "when", "make", "can", "like", "time", "no",
  "just", "him", "know", "take", "person", "into", "year", "your",
  "good", "some", "could", "them", "see", "other", "than", "then",
  "now", "look", "only", "come", "its", "over", "think", "also"
];

// Construct a random string of a given size and index.
pub fn make_random_string(size: u32, index: u32) -> String {
    // Allow 5 characters to allow room for a full word to be
    // appended, beyond the desired length.
    let mut result = String::with_capacity(size as usize + 6);
    let _ = write!(&mut result, "{}-{}", index, size);

    let mut gen = SimpleRandom::new(index);

    while result.len() < size as usize {
        result.push(' ');
        result.push_str(WORD_LIST[gen.next(WORD_LIST.len() as u32) as usize]);
    }

    result.truncate(size as usize);
    result
}

// Make a random chunk.
pub fn make_random_chunk(size: u32, index: u32) -> Box<chunk::Chunk> {
    chunk::new_plain(Kind::new("blob").unwrap(), make_random_string(size, index).into_bytes())
}

pub fn make_kinded_random_chunk(kind: Kind, size: u32, index: u32) -> Box<chunk::Chunk> {
    chunk::new_plain(kind, make_random_string(size, index).into_bytes())
}

pub fn make_uncompressible_chunk(size: u32, index: u32) -> Box<chunk::Chunk> {
    use rand::{Rng, SeedableRng, XorShiftRng};
    use std::iter::repeat;

    let mut buf: Vec<u8> = repeat(0u8).take(size as usize).collect();

    let mut gen: XorShiftRng = SeedableRng::from_seed([index, 0, 0, 0]);

    gen.fill_bytes(&mut buf);
    /* {
        use pdump::HexDump;
        println!("Buf of {:x} bytes", size);
        buf.dump();
    } */
    chunk::new_plain(Kind::new("unco").unwrap(), buf)
}

// Generate a useful series of sizes, build around powers of two and
// values 1 greater or less than them.
pub fn boundary_sizes() -> Vec<u32> {
    let mut nums: BTreeSet<u32> = BTreeSet::new();

    for i in 0 .. 19 {
        let bit = 1 << i;
        if bit > 0 {
            nums.insert(bit - 1);
        }
        nums.insert(bit);
        nums.insert(bit + 1);
    }

    nums.iter().map(|&x| x).collect()
}

// Simple random number generator.
struct SimpleRandom {
    state: u32
}

impl SimpleRandom {
    fn new(index: u32) -> SimpleRandom {
        SimpleRandom { state: index }
    }

    fn next(&mut self, limit: u32) -> u32 {
        let t1 = Wrapping(self.state) * Wrapping(1103515245);
        let t2 = t1 + Wrapping(12345);
        self.state = t2.0 & 0x7fffffff;
        self.state % limit
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use super::make_random_string;
    use super::boundary_sizes;
    // use test::Bencher;

    macro_rules! check( ($e:expr) => {
        match $e {
            Ok(t) => t,
            Err(e) => panic!("{} failed with: {}", stringify!($e), e),
        }
    } );

    #[test]
    fn random_strings() {
        fn check(size: u32, index: u32) -> String {
            let text = make_random_string(size, index);
            assert!(text.len() == size as usize);
            text
        }
        let mut texts: HashSet<String> = HashSet::new();
        for &i in boundary_sizes().iter() {
            let text = check(i, i);
            assert!(texts.insert(text));

        }
    }

    /*
    #[bench]
    fn large_strings(b: &mut Bencher) {
        b.iter(|| make_random_string(256 * 1024, 256));
    }

    #[bench]
    fn small_strings(b: &mut Bencher) {
        b.iter(|| make_random_string(32, 32));
    }
    */

    #[test]
    fn test_boundaries() {
        let sizes_vec = boundary_sizes();
        let sizes = &sizes_vec[..];

        // Make sure they are unique and incrementing.
        let mut prior = sizes[0];
        for &sz in sizes[1..].iter() {
            assert!(sz > prior);
            prior = sz;
        }
    }

    #[test]
    fn test_tmpdir() {
        use std::{fs, path};
        use tempdir::TempDir;

        let path: path::PathBuf = {
            let tmp = TempDir::new("testutil").unwrap();
            let path = tmp.path().to_path_buf();
            check!(fs::create_dir(&path.join("subdir")));
            assert!(check!(fs::metadata(&path.join("subdir"))).is_dir());
            // println!("Tmp: '{}'", path.display());
            path
        };

        // Make sure it goes away when the TempDir goes out of scope.
        match fs::metadata(&path) {
            Ok(_) => panic!("Directory should have been removed"),
            Err(_) => ()
        };
    }
}
