// Content Addressible Store errors

use std::error;
use std::fmt;
use std::io::{self, ErrorKind};
use std::num::ParseIntError;
use std::str::ParseBoolError;
use std::string::FromUtf8Error;
use rusqlite;
use uuid;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Sql(rusqlite::SqliteError),
    Uuid(uuid::ParseError),
    InvalidIndex(String),
    PathError(String),
    CorruptChunk(String),
    CorruptPool(String),
    PropertyError(String),
    Utf8Error(FromUtf8Error),
    ParseBoolError(ParseBoolError),
    ParseIntError(ParseIntError),
    NonAsciiKind,
    BadKindLength,
    MissingChunk,
    NotAPool,
}

impl Error {
    pub fn is_unexpected_eof(&self) -> bool {
        match *self {
            Error::Io(ref err) if err.kind() == ErrorKind::UnexpectedEof => true,
            _ => false,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<rusqlite::SqliteError> for Error {
    fn from(err: rusqlite::SqliteError) -> Error {
        Error::Sql(err)
    }
}

impl From<uuid::ParseError> for Error {
    fn from(err: uuid::ParseError) -> Error {
        Error::Uuid(err)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Error {
        Error::Utf8Error(err)
    }
}

impl From<ParseBoolError> for Error {
    fn from(err: ParseBoolError) -> Error {
        Error::ParseBoolError(err)
    }
}

impl From<ParseIntError> for Error {
    fn from(err: ParseIntError) -> Error {
        Error::ParseIntError(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref err) => err.fmt(f),
            Error::Sql(ref err) => err.fmt(f),
            Error::Uuid(ref err) => err.fmt(f),
            Error::Utf8Error(ref err) => err.fmt(f),
            Error::ParseBoolError(ref err) => err.fmt(f),
            Error::ParseIntError(ref err) => err.fmt(f),
            Error::NonAsciiKind => write!(f, "Non ascii Kind"),
            Error::BadKindLength => write!(f, "Invalid Kind length (!= 4)"),
            Error::MissingChunk => write!(f, "Missing chunk"),
            Error::NotAPool => write!(f, "Not a storage pool"),
            Error::InvalidIndex(ref msg) => write!(f, "Invalid index file: {:?}", msg),
            Error::PathError(ref msg) => write!(f, "Path error: {:?}", msg),
            Error::CorruptChunk(ref msg) => write!(f, "Corrupt chunk: {:?}", msg),
            Error::CorruptPool(ref msg) => write!(f, "Corrupt pool: {:?}", msg),
            Error::PropertyError(ref msg) => write!(f, "Property parse error: {:?}", msg),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Io(ref err) => err.description(),
            Error::Sql(ref err) => err.description(),
            Error::Uuid(_) => "UUID parse error",
            Error::Utf8Error(_) => "UTF-8 decode error",
            Error::ParseBoolError(_) => "Error parsing bool",
            Error::ParseIntError(_) => "Error parsing number",
            Error::NonAsciiKind => "Non ascii Kind",
            Error::BadKindLength => "Invalid Kind length (!= 4)",
            Error::MissingChunk => "Missing Chunk",
            Error::NotAPool => "Not a storage pool",
            Error::InvalidIndex(_) => "Invalid index file",
            Error::PathError(_) => "Invalid Path name",
            Error::CorruptChunk(_) => "Corrupt chunk",
            Error::CorruptPool(_) => "Corrupt pool",
            Error::PropertyError(_) => "Property parse error",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::NonAsciiKind => None,
            Error::BadKindLength => None,
            Error::MissingChunk => None,
            Error::NotAPool => None,
            Error::InvalidIndex(_) => None,
            Error::PathError(_) => None,
            Error::CorruptChunk(_) => None,
            Error::CorruptPool(_) => None,
            Error::PropertyError(_) => None,
            Error::Io(ref err) => err.cause(),
            Error::Sql(ref err) => err.cause(),
            Error::Uuid(_) => None,
            Error::Utf8Error(ref err) => err.cause(),
            Error::ParseBoolError(ref err) => err.cause(),
            Error::ParseIntError(ref err) => err.cause(),
        }
    }
}
