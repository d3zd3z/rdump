// Filer library.

extern crate cas;

#[cfg(test)]
extern crate uuid;

#[cfg(test)]
extern crate tempdir;

#[macro_use]
extern crate log;
extern crate env_logger;

// For now, reuse the Error/Result types from cas.
type Result<T> = cas::Result<T>;

// #[cfg(test)]
// mod itrack;

mod indirect;
pub mod data;
pub mod decode;
