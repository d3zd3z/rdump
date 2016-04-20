//! Transaction Wrapper for rusqlite::Connection

use rusqlite::{Connection, Transaction, Result};
use std::mem;
use std::ops::{Deref, DerefMut};

/// Wrap a rusqlite::Connection an maintain a transaction within it.
pub struct XactConnection {
    conn: Connection,
    // The possibly open transaction.  We lie about the lifetime, since it can't be tied to
    // the connection, because that is not safe.  We make it safe by: 1. Having a Drop
    // implementation that drops the `xact` before the `conn`, and 2. Making sure that the only
    // access to 'xact' requires a mutable borrow of the entire `XactConnection`.  This should
    // also provide no way to modify or replace the connection itself, if a transaction is
    // open.
    xact: Option<Box<Transaction<'static>>>,
}

impl XactConnection {
    pub fn new(conn: Connection) -> XactConnection {
        XactConnection {
            conn: conn,
            xact: None,
        }
    }

    /// Get the transaction or panics if this is called outside of a transaction.
    /// The lifetime appears to be static, but it is really bound by self.  This is safe
    /// because of the mutable borrow of self.
    #[allow(dead_code)]
    pub fn xact(&mut self) -> &mut Transaction<'static> {
        match self.xact {
            None => panic!("Attempt to retrieve transaction outside of one"),
            Some(ref mut xact) => xact,
        }
    }

    /// Begin a new transaction.  Will panic if one is already opened (no nesting).
    pub fn begin(&mut self) -> Result<()> {
        if self.xact.is_some() {
            panic!("Attempt to nest transactions");
        }

        let xact = Box::new(self.conn.transaction()?);
        self.xact = unsafe { Some(mem::transmute(xact)) };
        Ok(())
    }

    /// Commit the transaction.
    pub fn commit(&mut self) -> Result<()> {
        let xact = mem::replace(&mut self.xact, None);
        match xact {
            None => panic!("No transaction started"),
            Some(xact) => xact.commit(),
        }
    }

    /// Rollback the transaction.
    #[allow(dead_code)]
    pub fn rollback(&mut self) -> Result<()> {
        let xact = mem::replace(&mut self.xact, None);
        match xact {
            None => panic!("No transaction started"),
            Some(xact) => xact.rollback(),
        }
    }

    // Get the connection out of the wrapper, dropping any transaction (whatever behavior that
    // is configured for).
    // TODO: This doesn't work, and probably requires something unsafe in order to work.
    // pub fn into_inner(self) -> Connection {
    //     self.xact = None;
    //     self.conn
    // }
}

impl Deref for XactConnection {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        &self.conn
    }
}

impl DerefMut for XactConnection {
    fn deref_mut(&mut self) -> &mut Connection {
        &mut self.conn
    }
}

/// Make sure the transaction is dropped before the connection is dropped.
impl Drop for XactConnection {
    fn drop(&mut self) {
        let _ = mem::replace(&mut self.xact, None);
    }
}
