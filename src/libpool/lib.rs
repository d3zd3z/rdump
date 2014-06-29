// Tmp to build the test.

#![crate_id = "pool#0.1.0"]
#![crate_type = "rlib"]
#![crate_type = "dylib"]
#![license = "MIT"]

// Suppress error about compile time plugins.
#![feature(phase)]
#![feature(macro_rules)]

/// Rust dump

extern crate core;
extern crate libc;
extern crate collections;
extern crate flate;
extern crate uuid;

#[phase(plugin)]
extern crate fourcc;

#[phase(plugin, link)]
extern crate log;

#[cfg(test)]
extern crate test;

#[cfg(test)]
mod testutil;

#[cfg(test)]
mod pdump;

pub mod kind;
pub mod oid;
pub mod chunk;
pub mod pool;
