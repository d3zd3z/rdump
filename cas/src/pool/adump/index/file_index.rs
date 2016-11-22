//! A FileIndex is a file-based mapping of hashes to IndexInfo.

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use Error;
use Kind;
use Oid;
use Result;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use super::{Index, /* IndexUpdate, */ IndexInfo, IterItem};

// Represents the in-memory format for a single index file.  There is a
// tradeoff here between load time (reading and decoding the file, or using
// accessors to decode the file as it is read).  There really isn't a way
// to compare these other than two try both approaches, and benchmark the
// results.
//
// This FileIndex uses the byteorder crate to read and decode the data.
#[allow(dead_code)]
pub struct FileIndex {
    top: Vec<u32>,
    offsets: Vec<u32>,
    oids: Vec<Oid>,
    kind_names: Vec<Kind>,
    kinds: Vec<u8>,
}

impl FileIndex {
    /// Try loading the given named index file, returning it if it is
    /// valid.
    pub fn load<P: AsRef<Path>>(path: P, size: u32) -> Result<FileIndex> {
        let f = File::open(path)?;
        let mut rd = BufReader::new(f);

        let mut magic = vec![0u8; 8];
        rd.read_exact(&mut magic)?;
        if magic != b"ldumpidx" {
            return Err(Error::InvalidIndex("bad magic".to_owned()));
        }

        let version = rd.read_u32::<LittleEndian>()?;
        if version != 4 {
            return Err(Error::InvalidIndex("Version mismatch".to_owned()));
        }

        let file_size = rd.read_u32::<LittleEndian>()?;
        if file_size != size {
            return Err(Error::InvalidIndex("Index size mismatch".to_owned()));
        }
        // TODO: Process this
        // The file_size is the number of bytes in the pool file.  If this
        // differs, it indicates that this index doesn't match the file,
        // and should be regenerated.

        let mut top = Vec::with_capacity(256);
        for _ in 0..256 {
            top.push(rd.read_u32::<LittleEndian>()?);
        }

        let size = *top.last().unwrap() as usize;

        let mut oid_buf = vec![0u8; 20];
        let mut oids = Vec::with_capacity(size);
        for _ in 0..size {
            rd.read_exact(&mut oid_buf)?;
            oids.push(Oid::from_raw(&oid_buf));
        }

        let mut offsets = Vec::with_capacity(size);
        for _ in 0..size {
            offsets.push(rd.read_u32::<LittleEndian>()?);
        }

        let kind_count = rd.read_u32::<LittleEndian>()? as usize;
        let mut kind_names = Vec::with_capacity(size);
        for _ in 0..kind_count {
            let mut kind_buf = vec![0u8; 4];
            rd.read_exact(&mut kind_buf)?;
            let text = String::from_utf8(kind_buf)?;
            kind_names.push(Kind::new(&text)?);
        }

        let mut kinds = vec![0u8; size];
        rd.read_exact(&mut kinds)?;

        Ok(FileIndex {
            top: top,
            offsets: offsets,
            oids: oids,
            kind_names: kind_names,
            kinds: kinds,
        })
    }

    /// Construct an empty index, that contains no values.
    pub fn empty() -> FileIndex {
        FileIndex {
            top: vec![0; 256],
            offsets: vec![],
            oids: vec![],
            kind_names: vec![],
            kinds: vec![],
        }
    }

    /// Save an index from something that can be iterated over.
    pub fn save<'a, P: AsRef<Path>, I>(path: P, size: u32, index: I) -> Result<()>
        where I: IntoIterator<Item = IterItem<'a>>
    {
        let mut nodes: Vec<IterItem<'a>> = index.into_iter().collect();
        nodes.sort_by_key(|n| n.oid);
        let nodes = nodes;

        let tmp_name = tmpify(path.as_ref())?;
        println!("tmp: {:?} -> {:?}", tmp_name, path.as_ref());
        {
            let ofd = File::create(&tmp_name)?;
            let mut ofd = BufWriter::new(ofd);

            ofd.write_all(b"ldumpidx")?;
            ofd.write_u32::<LittleEndian>(4)?;
            ofd.write_u32::<LittleEndian>(size)?;

            // Write the top-level index.
            let top = compute_top(&nodes);
            for elt in top {
                ofd.write_u32::<LittleEndian>(elt)?;
            }

            // Write out the hashes themselves.
            for n in &nodes {
                ofd.write_all(&n.oid.0)?;
            }

            // Write out the offset table.
            for n in &nodes {
                ofd.write_u32::<LittleEndian>(n.offset)?;
            }

            // Compute the kind map.
            let mut kinds = vec![];
            let mut kind_map = BTreeMap::new();
            for n in &nodes {
                if !kind_map.contains_key(&n.kind) {
                    kind_map.insert(n.kind, kinds.len());
                    kinds.push(n.kind);
                }
            }

            // Write out the kind map itself.
            ofd.write_u32::<LittleEndian>(kinds.len() as u32)?;
            for &k in &kinds {
                ofd.write_u32::<LittleEndian>(k.0)?;
            }

            // Then write out the values.
            let mut buf = Vec::with_capacity(nodes.len());
            for n in &nodes {
                buf.push(kind_map[&n.kind] as u8);
            }
            ofd.write_all(&buf)?;
        }

        // It worked, so do the atomic rename/overwrite.  'std' tries to do
        // this sane behavior on Windows as well.
        fs::rename(tmp_name, path.as_ref())?;

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Scan this index for a given hash.
    fn find(&self, key: &Oid) -> Option<usize> {
        let first_byte = key.0[0] as usize;

        let low = if first_byte > 0 {
            self.top[first_byte - 1] as usize
        } else {
            0
        };
        let high = self.top[first_byte] as usize;
        match self.oids[low..high].binary_search(key) {
            Ok(index) => Some(index + low),
            Err(_) => None,
        }
    }

    pub fn iter(&self) -> Iter {
        self.into_iter()
    }
}

impl Index for FileIndex {
    fn contains_key(&self, key: &Oid) -> bool {
        self.find(key).is_some()
    }

    fn get(&self, key: &Oid) -> Option<IndexInfo> {
        self.find(key).map(|num| {
            IndexInfo {
                offset: self.offsets[num],
                kind: self.kind_names[self.kinds[num] as usize],
            }
        })
    }
}

impl<'a> IntoIterator for &'a FileIndex {
    type Item = IterItem<'a>;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            parent: self,
            pos: 0,
        }
    }
}

pub struct Iter<'a> {
    parent: &'a FileIndex,
    pos: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = IterItem<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.parent.len() {
            None
        } else {
            let pos = self.pos;
            self.pos = pos + 1;

            Some(IterItem {
                oid: &self.parent.oids[pos],
                kind: self.parent.kind_names[self.parent.kinds[pos] as usize],
                offset: self.parent.offsets[pos],
            })
        }
    }
}

fn compute_top<'a>(nodes: &[IterItem<'a>]) -> Vec<u32> {
    let mut top = Vec::with_capacity(256);

    let mut iter = nodes.iter().enumerate().peekable();
    for first in 0..256 {
        // Scan until we hit a value that is too large.
        loop {
            match iter.peek() {
                None => break,
                Some(&(_, key)) => {
                    if key.oid[0] as usize > first {
                        break;
                    }
                    iter.next();
                }
            }
        }
        let index = match iter.peek() {
            None => nodes.len(),
            Some(&(n, _)) => n,
        };
        top.push(index as u32);
    }
    top
}

// Given a filename, generate another with a ".tmp" suffix, if possible.
fn tmpify(path: &Path) -> Result<PathBuf> {
    let base = path.file_name()
        .ok_or_else(|| Error::PathError(format!("path does not have a filename {:?}", path)));
    let base = base?;

    let base = base.to_str()
        .ok_or_else(|| Error::PathError(format!("path isn't valid UTF-8 {:?}", path)));
    let base = base?;

    let tmp = format!("{}.tmp", base);
    Ok(path.with_file_name(&tmp))
}
