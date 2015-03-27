// Backup nodes.

use std::collections::HashMap;
use error::Result;

/// A node has a kind, which is usually a deeper 'kind' value than the
/// particular chunk type that represents it.
/// There are also properties which are keys that map to sequences of
/// bytes.
pub struct Node {
    pub kind: String,
    pub props: HashMap<String, Vec<u8>>,
}

impl Node {
    /// Decode an encoded Node, returning the new node.
    pub fn new(data: &[u8]) -> Result<Node> {
        let mut dec = Decoder::new(data);

        let kind = try!(dec.get_string(1));
        println!("Kind: '{}'", kind);

        let mut props = HashMap::new();
        while !dec.done() {
            let key = try!(dec.get_string(1));
            let value = dec.get_bytes(2);
            println!("  key: '{}'", key);
            println!("  value: {:?}", value);
            props.insert(key, value);
        }

        Ok(Node {
            kind: kind,
            props: props,
        })
    }
}

// The decoder itself.
struct Decoder<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new<'b>(data: &'b [u8]) -> Decoder<'b> {
        Decoder {
            data: data,
            offset: 0,
        }
    }

    #[inline]
    fn get_byte(&mut self) -> u8 {
        let result = self.data[self.offset];
        self.offset += 1;
        result
    }

    fn get_n(&mut self, len_bytes: u32) -> usize {
        let mut result = 0;
        for _ in 0 .. len_bytes {
            result <<= 8;
            result |= self.get_byte() as usize;
        }
        result
    }

    fn get_bytes(&mut self, len_bytes: u32) -> Vec<u8> {
        let len = self.get_n(len_bytes);
        let mut result = Vec::with_capacity(len);

        for _ in 0 .. len {
            result.push(self.get_byte());
        }
        result
    }

    fn get_string(&mut self, len_bytes: u32) -> Result<String> {
        let buf = self.get_bytes(len_bytes);
        Ok(try!(String::from_utf8(buf)))
    }

    fn done(&self) -> bool {
        self.offset >= self.data.len()
    }
}
