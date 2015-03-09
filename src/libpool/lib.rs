// Tmp to build the test.

// #![crate_name = "libpool"]
// #![crate_type = "rlib"]
// #![crate_type = "dylib"]

#![plugin(fourcc)]
#![feature(plugin)]

// Needed for raw manipulation in kind.  Maybe shouldn't use?
#![feature(core)]

// Collections aren't considered stable yet.
#![feature(collections)]

// Needed for libc
#![feature(libc)]

#![cfg_attr(test, feature(test))]

// Needed during alpha/beta transition of rustc.
// #![feature(int_uint)]
// #![allow(unstable)]

// Will be removed from the compiler.  The destructor in pool::sql will
// hopefully be considered safe before this is removed.
// #![feature(unsafe_destructor)]

// Needed until https://github.com/rust-lang/rust/issues/13853 and/or
// https://github.com/rust-lang/rust/issues/14889 are fixed.
// #![feature(unsafe_destructor)]

/// Rust dump

extern crate core;
extern crate libc;
// // extern crate collections;
// extern crate flate;
// extern crate uuid;
// extern crate sqlite3;
extern crate "rustc-serialize" as rustc_serialize;

/*
// #[macro_use] // #[no_link]
#[plugin]
extern crate fourcc;

#[macro_use]
extern crate log;
*/

#[cfg(test)]
extern crate test;

/*
#[cfg(test)]
mod testutil;

#[cfg(test)]
pub mod pdump;
*/

pub mod kind;
pub mod oid;
/*
pub mod chunk;
pub mod pool;
*/
