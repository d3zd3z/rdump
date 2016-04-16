// Adump file format.

// TODO: These probably don't need to be exported.
pub use self::index::{FileIndex, RamIndex, PairIndex};

mod index;
pub mod file;
