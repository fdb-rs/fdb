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
//! A minimal example to get started.
//!
//! ```no_run
//! use tokio::runtime::Runtime;
//!
//! use std::env;
//! use std::error::Error
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let fdb_cluster_file = env::var("FDB_CLUSTER_FILE").expect("FDB_CLUSTER_FILE not defined!");
//!
//!     unsafe {
//!         fdb::select_api_version(710);
//!         fdb::start_network();
//!     }
//!
//!     let fdb_database = fdb::open_database(fdb_cluster_file)?;
//!
//!     let rt = Runtime::new()?;
//!
//!     let cloned_fdb_database = fdb_database.clone();
//!
//!     rt.block_on(async {
//!         let fdb_database = cloned_fdb_database;
//!
//!         // your main async app here
//!
//!         Result::<(), Box<dyn Error>>::Ok(())
//!     })?;
//!
//!     drop(fdb_database);
//!
//!     unsafe {
//!         fdb::stop_network();
//!     }
//!
//!     Ok(())
//! }
//! ```

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
