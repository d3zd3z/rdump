// Property file parser.
//
// This is a simplistic property file parser.  It is only expected to parse
// the properties output by this program, not the full (and someone
// inconsistently defined) property format defined the
// java.util.Properties, which is where this originally came from.

use Error;
use Result;
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read};

/// Try to parse the input as properties, building the hashmap.  This is
/// fairly strict, no spaces are allowed around the '=', there are no
/// continuation lines.  The only comment format recognized is '#'.
pub fn parse<R: Read>(input: R) -> Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    for line in BufReader::new(input).lines() {
        let line = try!(line);
        if line.len() == 0 || line.starts_with("#") {
            continue;
        }

        let fields: Vec<_> = line.splitn(2, '=').collect();
        if fields.len() != 2 {
            return Err(Error::PropertyError(format!("Line has no '=': {:?}", line)));
        }

        result.insert(fields[0].to_owned(), fields[1].to_owned());
    }
    Ok(result)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn simple_parse() {
        let buf = b"# This is a comment\n\
                    uuid=c39b7bde-b83a-47b2-b597-6546f08c9183\n\
                    newfile=false\n\
                    limit=671088640\n";
        let mut buf = &buf[..];
        let map = parse(&mut buf).unwrap();
        assert_eq!(map["limit"], "671088640");
        assert_eq!(map["uuid"], "c39b7bde-b83a-47b2-b597-6546f08c9183");
        assert_eq!(map["newfile"], "false");
        println!("map: {:?}", map);
    }
}
