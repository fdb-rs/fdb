use std::ffi::CString;
use std::path::Path;
use std::ptr::{self, NonNull};
use std::sync::Arc;

use crate::database::FdbDatabase;
use crate::error::{check, FdbError, FdbResult, DATABASE_OPEN};

/// Returns [`FdbDatabase`] handle to the FDB cluster identified by
/// the provided cluster file.
///
/// If no cluster file is passed, FDB automatically [determines a
/// cluster file] with which to connect to a cluster.
///
/// A single client can use this function multiple times to connect to
/// different clusters simultaneously, with each invocation requiring
/// its own cluster file.
///
/// # Note
///
/// The caller *must* ensure that [`FdbDatabase`] stays alive during
/// the lifetime of any transactions or futures that [`FdbDatabase`]
/// creates.
///
/// [determines a cluster file]: https://apple.github.io/foundationdb/administration.html#specifying-a-cluster-file
pub fn open_database<P>(cluster_file_path: P) -> FdbResult<FdbDatabase>
where
    P: AsRef<Path>,
{
    let path = CString::new(
        cluster_file_path
            .as_ref()
            .to_str()
            .ok_or_else(|| FdbError::new(DATABASE_OPEN))?,
    )
    .map_err(|_| FdbError::new(DATABASE_OPEN))?;

    // `path_ptr` is valid till we do `drop(path)`.
    let path_ptr = path.as_ptr();

    let mut v = ptr::null_mut();

    let err = unsafe { fdb_sys::fdb_create_database(path_ptr, &mut v) };

    drop(path);

    // At this stage, we either have an error or a valid v.
    check(err)?;

    Ok(FdbDatabase::new(Some(Arc::new(NonNull::new(v).expect(
        "fdb_create_database returned null, but did not return an error",
    )))))
}
