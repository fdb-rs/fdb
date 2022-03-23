#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]

//! FoundationDB Client API for Tokio

mod fdb;
mod key_value;
mod option;

pub mod database;
pub mod error;
pub mod future;
pub mod range;
pub mod subspace;
pub mod transaction;
pub mod tuple;

/// Maximum API version supported by the client
pub use fdb_sys::FDB_API_VERSION;

pub use crate::fdb::{select_api_version, set_network_option, start_network, stop_network};

pub use crate::key_value::{Key, KeySelector, KeyValue, Value};

pub use crate::database::open_database::open_database;

pub use crate::option::NetworkOption;
