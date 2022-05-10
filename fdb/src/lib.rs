#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]

//! FoundationDB Client API for Tokio
//!
//! Guide level documentation is on our [website]. You will find API
//! level documentation here.
//!
//! [website]: https://fdb-rs.github.io/docs/getting-started/introduction/
//!
//! A minimal example to get started
//!
//! ```no_run
//! #[tokio::main]
//! async fn main() {
//!   unsafe {
//!       fdb::select_api_version(fdb::FDB_API_VERSION as _);
//!       fdb::start_network();
//!   }
//!   let db = fdb::open_database("").expect("Open database");
//!
//!   // Use db
//!
//!   drop(db);
//!   unsafe {
//!       fdb::stop_network();
//!   }
//! }

mod fdb;
mod key_value;
mod option;

#[cfg(feature = "fdb-7_1")]
mod mapped_key_value;

#[cfg(feature = "fdb-7_1")]
mod mapped_range;

pub mod database;
pub mod error;
pub mod future;
pub mod range;
pub mod subspace;
pub mod transaction;
pub mod tuple;

#[cfg(feature = "fdb-7_1")]
pub mod tenant;

/// Maximum API version supported by the client
pub use fdb_sys::FDB_API_VERSION;

pub use crate::fdb::{select_api_version, set_network_option, start_network, stop_network};

pub use crate::key_value::{Key, KeySelector, KeyValue, Value};

pub use crate::database::open_database::open_database;

pub use crate::option::NetworkOption;

#[cfg(feature = "fdb-7_1")]
pub use crate::mapped_key_value::{MappedKeyValue, Mapper};

#[cfg(feature = "fdb-7_1")]
pub use crate::tenant::tenant_inner::Tenant;
