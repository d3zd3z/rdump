// A simple bloom filter.
//
// The filter has two parameters, one is the size, which generally should
// be a power of two.  The other is the number of keys to store for each
// element.

/// Something that can be added to a bloom filter.  It needs to have some
/// number of keys available.  For simplicity, we'll just return distinct
/// u32s, even though that is not the best way of getting the key data.
/// For the test, since we have 256-bits, it gives us up to 8 keys.
pub trait BloomItem {
    // Get the specific key (0-based).
    fn get_key(&self, index: usize) -> u32;
}

pub struct Bloom {
    mask: usize,
    nk: usize,
    data: Vec<u32>,
}

impl Bloom {
    /// Construct a new bloom filter, with `bit_size` bits.  There will be
    /// room for 2**bit_size items.
    pub fn new(bit_size: usize, nk: usize) -> Bloom {
        assert!(bit_size > 5);
        assert!(bit_size <= 32);
        let mask = (1 << bit_size) - 1;
        let data = vec![0u32; 1 << (bit_size - 5)];
        Bloom {
            mask: mask,
            data: data,
            nk: nk,
        }
    }

    /// Add the item to the blook filter.
    pub fn add(&mut self, item: &BloomItem) {
        for i in 0 .. self.nk {
            let num = item.get_key(i) as usize & self.mask;
            self.data[num >> 5] |= 1 << (num & 31);
        }
    }

    /// Check if something is present in the bloom filter.  'false' is a
    /// definitive answer, but 'true' can have false positives depending on
    /// the parameters of the filter.
    pub fn maybe_contains(&self, item: &BloomItem) -> bool {
        for i in 0 .. self.nk {
            let num = item.get_key(i) as usize & self.mask;
            if (self.data[num >> 5] & (1 << (num & 31))) == 0 {
                return false;
            }
        }
        true
    }

    /// Dump out some values to get an idea of how much is set.
    pub fn debug(&self) {
        // Get the population density.
        let mut count = 0;
        for &elt in &self.data {
            for bit in 0 .. 32 {
                if elt & (1 << bit) != 0 {
                    count += 1;
                }
            }
        }
        let total = self.data.len() * 32;
        println!("{} set of {} ({:.5}%)", count, total, count as f64 / total as f64 * 100.0);

        for i in 0 .. 32 {
            if i > 0 && i % 8 == 0 {
                println!("");
            }
            print!(" {:08x}", self.data[i]);
        }
        println!("");
    }
}
