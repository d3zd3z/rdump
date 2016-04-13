// Content Addressible Store errors

use std::error;
use std::fmt;
use std::io;
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
    Utf8Error(FromUtf8Error),
    NonAsciiKind,
    BadKindLength,
    MissingChunk,
    NotAPool,
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

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref err) => err.fmt(f),
            Error::Sql(ref err) => err.fmt(f),
            Error::Uuid(ref err) => err.fmt(f),
            Error::Utf8Error(ref err) => err.fmt(f),
            Error::NonAsciiKind => write!(f, "Non ascii Kind"),
            Error::BadKindLength => write!(f, "Invalid Kind length (!= 4)"),
            Error::MissingChunk => write!(f, "Missing chunk"),
            Error::NotAPool => write!(f, "Not a storage pool"),
            Error::InvalidIndex(ref msg) => write!(f, "Invalid index file: {:?}", msg),
            Error::PathError(ref msg) => write!(f, "Path error: {:?}", msg),
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
            Error::NonAsciiKind => "Non ascii Kind",
            Error::BadKindLength => "Invalid Kind length (!= 4)",
            Error::MissingChunk => "Missing Chunk",
            Error::NotAPool => "Not a storage pool",
            Error::InvalidIndex(_) => "Invalid index file",
            Error::PathError(_) => "Invalid Path name",
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
            Error::Io(ref err) => err.cause(),
            Error::Sql(ref err) => err.cause(),
            Error::Uuid(_) => None,
            Error::Utf8Error(ref err) => err.cause(),
        }
    }
}
