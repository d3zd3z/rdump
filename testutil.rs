// Test utilities.

use collections::treemap::TreeSet;

// A short list of words to help generate reasonably compressible
// data.
static word_list: &'static [&'static str] = &[
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
pub fn make_random_string(size: uint, index: uint) -> String {
    // Allow 5 characters to allow room for a full word to be
    // appended, beyond the desired length.
    let mut result = String::with_capacity(size + 6);
    result.push_str(format!("{:u}-{:u}", index, size).as_slice());

    let mut gen = SimpleRandom::new(index);

    while result.len() < size {
        result.push_char(' ');
        result.push_str(word_list[gen.next(word_list.len())]);
    }

    result.truncate(size);
    result
}

// Generate a useful series of sizes, build around powers of two and
// values 1 greater or less than them.
pub fn boundary_sizes() -> Vec<uint> {
    let mut nums: TreeSet<uint> = TreeSet::new();

    for i in range(0u, 19) {
        let bit = 1u << i;
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
    fn new(index: uint) -> SimpleRandom {
        SimpleRandom { state: index as u32 }
    }

    fn next(&mut self, limit: uint) -> uint {
        self.state = ((self.state * 1103515245) + 12345) & 0x7fffffff;
        self.state as uint % limit
    }
}

#[cfg(test)]
mod test {
    use std::collections::hashmap::HashSet;
    use super::make_random_string;
    use super::boundary_sizes;
    use test::Bencher;

    #[test]
    fn random_strings() {
        fn check(size: uint, index: uint) -> String {
            let text = make_random_string(size, index);
            assert!(text.len() == size);
            text
        }
        let mut texts: HashSet<String> = HashSet::new();
        for &i in boundary_sizes().iter() {
            let text = check(i, i);
            assert!(texts.insert(text));

        }
    }

    #[bench]
    fn large_strings(b: &mut Bencher) {
        b.iter(|| make_random_string(256 * 1024, 256));
    }

    #[bench]
    fn small_strings(b: &mut Bencher) {
        b.iter(|| make_random_string(32, 32));
    }

    #[test]
    fn test_boundaries() {
        let sizes_vec = boundary_sizes();
        let sizes = sizes_vec.as_slice();

        // Make sure they are unique and incrementing.
        let mut prior = sizes[0];
        for &sz in sizes.tail().iter() {
            assert!(sz > prior);
            prior = sz;
        }
    }
}
