//! Starting point for accessing FoundationDB
use parking_lot::{Once, OnceState};

use std::thread::{self, JoinHandle};

use crate::error::{check, FdbResult};
use crate::option::NetworkOption;

static SELECT_API_VERSION_INIT: Once = Once::new();

/// Select the version of the client API.
///
/// # Panic
///
/// This will panic if the version is not supported by the
/// implementation of the API.
///
/// As only one version can be selected for the lifetime of the
/// process, calling this function more than once will also result in
/// a panic.
///
/// # Safety
///
/// This API is part of FDB client setup.
///
/// # Warning
///
/// When using the multi-version client API, setting an API version
/// that is not supported by a particular client library will prevent
/// that client from being used to connect to the cluster.
///
/// In particular, you should not advance the API version of your
/// application after upgrading your client **until** the cluster has
/// also been upgraded.
pub unsafe fn select_api_version(version: i32) {
    if SELECT_API_VERSION_INIT.state() == OnceState::New {
        SELECT_API_VERSION_INIT.call_once(|| {
            // run initialization here
            check(fdb_sys::fdb_select_api_version_impl(
                version,
                // `bindgen` defaults `FDB_API_VERSION` to `u32`
                fdb_sys::FDB_API_VERSION as i32,
            ))
            .unwrap_or_else(|_| {
                panic!("Unable to call select_api_version for version {}", version)
            });
        });
    } else {
        panic!("select_api_version(...) was previously called!");
    }
}

/// Set global options for the [FDB API].
///
/// # Safety
///
/// This API is part of FDB client setup.
///
/// [FDB API]: crate
pub unsafe fn set_network_option(option: NetworkOption) -> FdbResult<()> {
    option.apply()
}

static mut FDB_NETWORK_THREAD: Option<JoinHandle<FdbResult<()>>> = None;

// `FDB_NETWORK_STARTED` is set to `true` in `main` thread, and
// `FDB_NETWORK_STOPPED` is set to `true`, in `fdb-network-thread`.
static mut FDB_NETWORK_STARTED: bool = false;
static mut FDB_NETWORK_STOPPED: bool = false;

/// Initializes FDB network.
///
/// # Safety
///
/// This API is part of FDB client setup.
pub unsafe fn start_network() {
    if FDB_NETWORK_STOPPED {
        panic!("Network has been stopped and cannot be started");
    }

    if FDB_NETWORK_STARTED {
        return;
    }

    check(fdb_sys::fdb_setup_network()).unwrap_or_else(|e| {
        panic!("fdb_sys::fdb_setup_network() failed with error {:?}", e);
    });

    FDB_NETWORK_STARTED = true;

    FDB_NETWORK_THREAD = Some(
        thread::Builder::new()
            .name("fdb-network-thread".into())
            .spawn(|| {
                let res = check(fdb_sys::fdb_run_network());
                FDB_NETWORK_STOPPED = true;
                res
            })
            .unwrap_or_else(|e| {
                panic!("unable to create fdb-network-thread: error = {}", e);
            }),
    );
}

/// Stops the FDB networking engine.
///
/// # Safety
///
/// This API is part of FDB client setup.
pub unsafe fn stop_network() {
    if !FDB_NETWORK_STARTED {
        panic!("Trying to stop the network, before network has been started");
    };

    check(fdb_sys::fdb_stop_network()).unwrap_or_else(|e| {
        panic!("fdb_sys::fdb_stop_network() failed with error {:?}", e);
    });

    FDB_NETWORK_THREAD
        .take()
        .unwrap()
        .join()
        .unwrap_or_else(|e| {
            panic!("failed to join on fdb-network-thread: error {:?}", e);
        })
        .unwrap_or_else(|e| {
            panic!("fdb_sys::fdb_run_network() failed with error {:?}", e);
        });
}
