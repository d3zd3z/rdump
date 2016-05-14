// Backup decoder.

#![allow(dead_code)]

use cas;
use cas::Chunk;
use cas::Oid;

pub enum Node {
    Blob(Vec<u8>),
    Indirect {
        level: usize,
        children: Vec<Oid>,
    },
}

pub fn decode(chunk: Chunk) -> cas::Result<Node> {
    let kind = chunk.kind().to_string();

    if &kind[0..3] == "IND" {
        let data = chunk.into_bytes();
        let size = data.len() / Oid::size();
        let mut children = Vec::with_capacity(size);
        for i in 0..size {
            let a = i * Oid::size();
            let b = a + Oid::size();
            children.push(Oid::from_raw(&data[a..b]));
        }
        return Ok(Node::Indirect {
            level: (kind.as_bytes()[3] as usize) - ('0' as usize),
            children: children,
        });
    } else if kind == "blob" {
        return Ok(Node::Blob(chunk.into_bytes()));
    } else {
        panic!("Unknown chunk type");
    }
}
