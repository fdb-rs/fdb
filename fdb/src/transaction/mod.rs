//! Provides types and traits for working with FDB Transactions and
//! Snapshots.

mod fdb_transaction;
mod read_transaction;

// We do this in order to preserve consistency with Java and Go
// bindings.
#[allow(clippy::module_inception)]
mod transaction;

pub use crate::option::MutationType;
pub use crate::option::TransactionOption;

pub use fdb_transaction::{
    CommittedVersion, FdbReadTransaction, FdbTransaction, TransactionVersionstamp,
};

pub use read_transaction::ReadTransaction;
pub use transaction::Transaction;
