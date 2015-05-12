// Content Addressible Store errors

use std::error;
use std::fmt;
use std::io;
use byteorder;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    ByteOrder(byteorder::Error),
    NonAsciiKind,
    BadKindLength,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<byteorder::Error> for Error {
    // Not completely sure if we should be unwrapping this or not.
    fn from(err: byteorder::Error) -> Error {
        match err {
            byteorder::Error::Io(err) => Error::Io(err),
            err => Error::ByteOrder(err),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref err) => err.fmt(f),
            Error::ByteOrder(ref err) => err.fmt(f),
            Error::NonAsciiKind => write!(f, "Non ascii Kind"),
            Error::BadKindLength => write!(f, "Invalid Kind length (!= 4)"),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Io(ref err) => err.description(),
            Error::ByteOrder(ref err) => err.description(),
            Error::NonAsciiKind => "Non ascii Kind",
            Error::BadKindLength => "Invalid Kind length (!= 4)",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::NonAsciiKind => None,
            Error::BadKindLength => None,
            Error::Io(ref err) => err.cause(),
            Error::ByteOrder(ref err) => err.cause(),
        }
    }
}
