// Copyright 2015 David Brown
// MIT License

extern crate byteorder;

pub use error::Error;

use std::result;

pub type Result<T> = result::Result<T, Error>;

pub mod error;
pub mod kind;
