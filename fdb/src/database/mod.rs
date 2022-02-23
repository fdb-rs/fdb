//! Provides [`FdbDatabase`] type for working with FDB Database.
//!
//! Clients operating on a [`FdbDatabase`] should, in most cases use the
//! [`run`] method. This implements a proper retry loop around the
//! work that needs to be done and, assure
//! that [`commit`] has returned successfully before returning.
//!
//! [`run`]: FdbDatabase::run
//! [`commit`]: crate::transaction::Transaction::commit

mod fdb_database;

#[doc(hidden)]
pub mod open_database;

pub use crate::option::DatabaseOption;

pub use fdb_database::FdbDatabase;
