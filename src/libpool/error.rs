// General error type.

use std::error;
use std::fmt;
use std::io;
use std::result;
use rusqlite;
use uuid::ParseError;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Sql(rusqlite::SqliteError),
    Uuid(ParseError),
    MissingChunk,
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Io(ref err) => (err as &error::Error).description(),
            Error::Sql(ref err) => (err as &error::Error).description(),

            // uuid::ParseError doesn't properly implement Error, so fake
            // it.  It means we won't get much of a description.
            Error::Uuid(_) => "UUID parse error",

            Error::MissingChunk => "Missing chunk",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref ioe) => {
                try!(write!(fmt, "Io("));
                try!(ioe.fmt(fmt));
                write!(fmt, ")")
            },
            Error::Sql(ref err) => {
                try!(write!(fmt, "Sql("));
                try!(err.fmt(fmt));
                write!(fmt, ")")
            },
            Error::Uuid(ref err) => {
                try!(write!(fmt, "Uuid("));
                try!(err.fmt(fmt));
                write!(fmt, ")")
            },
            Error::MissingChunk => write!(fmt, "MissingChunk"),
        }
    }
}

impl error::FromError<io::Error> for Error {
    fn from_error(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl error::FromError<rusqlite::SqliteError> for Error {
    fn from_error(err: rusqlite::SqliteError) -> Error {
        Error::Sql(err)
    }
}

impl error::FromError<ParseError> for Error {
    fn from_error(err: ParseError) -> Error {
        Error::Uuid(err)
    }
}
