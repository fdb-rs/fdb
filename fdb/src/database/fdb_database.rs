use bytes::{BufMut, Bytes, BytesMut};

use tokio_stream::StreamExt;

use std::future::Future;
use std::ptr::{self, NonNull};
use std::sync::Arc;

use crate::database::DatabaseOption;
use crate::error::{check, FdbError, FdbResult};
use crate::range::{Range, RangeOptions};
use crate::transaction::{
    FdbReadTransaction, FdbTransaction, ReadTransaction, Transaction, TransactionOption,
};
use crate::{Key, KeySelector};

/// A mutable, lexicographically ordered mapping from binary keys to
/// binary values.
///
/// [`FdbTransaction`]s are used to manipulate data within a single
/// [`FdbDatabase`] - multiple concurrent [`FdbTransaction`]s on a
/// [`FdbDatabase`] enforce **ACID** properties.
///
/// The simplest correct programs using FDB will make use of the
/// [`run`] and [`read`] methods. [`run`] will call [`commit`] after
/// the user code has been executed.
///
/// A handle to FDB database. All reads and writes to the database are
/// transactional.
///
/// A [`FdbDatabase`] can be created using [`open_database`] function.
///
/// [`commit`]: FdbTransaction::commit
/// [`read`]: FdbDatabase::read
/// [`run`]: FdbDatabase::run
/// [`open_database`]: crate::open_database
//
// *NOTE*: If you make changes to this type, make sure you update
//         tests for `DummyFdbDatabase`, `DropTestDummyFdbDatabase`
//         accordingly.
#[derive(Clone, Debug)]
pub struct FdbDatabase {
    c_ptr: Option<Arc<NonNull<fdb_sys::FDBDatabase>>>,
}

impl FdbDatabase {
    // In Java following method is on `Interface Database`.

    /// Creates a [`FdbTransaction`] that operates on this
    /// [`FdbDatabase`].
    pub fn create_transaction(&self) -> FdbResult<FdbTransaction> {
        let mut ptr: *mut fdb_sys::FDB_transaction = ptr::null_mut();
        // Safety: It is safe to unwrap here because if we have given
        // out an `FdbDatabase` then `c_ptr` *must* be
        // `Some<Arc<...>>`.
        check(unsafe {
            fdb_sys::fdb_database_create_transaction(
                (*(self.c_ptr.as_ref().unwrap())).as_ptr(),
                &mut ptr,
            )
        })
        .map(|_| {
            FdbTransaction::new(Some(Arc::new(NonNull::new(ptr).expect(
                "fdb_database_create_transaction returned null, but did not return an error",
            ))))
        })
    }

    /// Returns an array of [`Key`]s `k` such that `begin <= k < end`
    /// and `k` is located at the start of contiguous range stored on
    /// a single server.
    ///
    /// If `limit` is non-zero, only the first `limit` number of keys
    /// will be returned. In large databases, the number of boundary
    /// keys may be large. In these cases, a non-zero `limit` should
    /// be used, along with multiple calls to [`get_boundary_keys`].
    ///
    /// If `read_version` is non-zero, the boundary keys as of
    /// `read_version` will be returned.
    ///
    /// This method is not transactional.
    ///
    /// [`get_boundary_keys`]: FdbDatabase::get_boundary_keys
    pub async fn get_boundary_keys(
        &self,
        begin: impl Into<Key>,
        end: impl Into<Key>,
        limit: i32,
        read_version: i64,
    ) -> FdbResult<Vec<Key>> {
        let tr = self.create_transaction()?;

        if read_version != 0 {
            unsafe {
                tr.set_read_version(read_version);
            }
        }

        tr.set_option(TransactionOption::ReadSystemKeys)?;
        tr.set_option(TransactionOption::LockAware)?;

        let range = Range::new(
            {
                let mut b = BytesMut::new();
                b.put(&b"\xFF/keyServers/"[..]);
                b.put(Into::<Bytes>::into(begin.into()));
                Into::<Bytes>::into(b)
            },
            {
                let mut b = BytesMut::new();
                b.put(&b"\xFF/keyServers/"[..]);
                b.put(Into::<Bytes>::into(end.into()));
                Into::<Bytes>::into(b)
            },
        );

        let mut res = Vec::new();

        let mut range_stream = tr.snapshot().get_range(
            KeySelector::first_greater_or_equal(range.begin().clone()),
            KeySelector::first_greater_or_equal(range.end().clone()),
            {
                let mut ro = RangeOptions::default();
                ro.set_limit(limit);
                ro
            },
        );

        while let Some(x) = range_stream.next().await {
            let kv = x?;
            res.push({
                // `13` because that is the length of
                // `"\xFF/keyServers/"`.
                Into::<Key>::into(Into::<Bytes>::into(kv.into_key()).slice(13..))
            });
        }

        Ok(res)
    }

    // In Java following method is on `Interface TransactionContext`.

    /// Runs a closure in the context that takes a [`FdbTransaction`].
    ///
    /// # Note
    ///
    /// The closure `FnMut: FnMut(FdbTransaction) -> Fut` will run
    /// multiple times (retry) when certain errors are
    /// encountered. Therefore the closure should be prepared to be
    /// called more than once. This consideration means that the
    /// closure should use caution when modifying state.
    pub async fn run<T, F, Fut>(&self, mut f: F) -> FdbResult<T>
    where
        F: FnMut(FdbTransaction) -> Fut,
        Fut: Future<Output = FdbResult<T>>,
    {
        let t = self.create_transaction()?;

        loop {
            let ret_val = f(t.clone()).await;

            // Closure returned an error
            if let Err(e) = ret_val {
                if FdbError::layer_error(e.code()) {
                    // Check if it is a layer error. If so, just
                    // return it.
                    return Err(e);
                } else if let Err(e1) = unsafe { t.on_error(e) }.await {
                    // Check if `on_error` returned an error. This
                    // means we have a non-retryable error.
                    return Err(e1);
                } else {
                    continue;
                }
            }

            // No error from closure. Attempt to commit the
            // transaction.
            if let Err(e) = unsafe { t.commit() }.await {
                // Commit returned an error
                if let Err(e1) = unsafe { t.on_error(e) }.await {
                    // Check if `on_error` returned an error. This
                    // means we have a non-retryable error.
                    return Err(e1);
                } else {
                    continue;
                }
            }

            // Commit successful, return `Ok(T)`
            return ret_val;
        }
    }

    // In Java following method is on `Interface
    // ReadTransactionContext`.

    /// Runs a closure in the context that takes a
    /// [`FdbReadTransaction`].
    ///
    /// # Note
    ///
    /// The closure `F: FnMut(FdbReadTransaction) -> Fut` will run
    /// multiple times (retry) when certain errors are
    /// encountered. Therefore the closure should be prepared to be
    /// called more than once. This consideration means that the
    /// closure should use caution when modifying state.
    //
    // It is okay to for `F` to have the signature
    // `FnMut(FdbReadTransaction) -> Fut` because we are not allowing
    // any mutations to occur. We are only concerned about retrying in
    // case of retryable errors.
    pub async fn read<T, F, Fut>(&self, mut f: F) -> FdbResult<T>
    where
        F: FnMut(FdbReadTransaction) -> Fut,
        Fut: Future<Output = FdbResult<T>>,
    {
        let t = self.create_transaction()?.snapshot();
        loop {
            let ret_val = f(t.clone()).await;

            // Closure returned an error
            if let Err(e) = ret_val {
                if FdbError::layer_error(e.code()) {
                    // Check if it is a layer error. If so, just
                    // return it.
                    return Err(e);
                } else if let Err(e1) = unsafe { t.on_error(e) }.await {
                    // Check if `on_error` returned an error. This
                    // means we have a non-retryable error.
                    return Err(e1);
                } else {
                    continue;
                }
            }

            // We don't need to commit read transaction, return
            // `Ok(T)`
            return ret_val;
        }
    }

    /// Set options on a [`FdbDatabase`].
    pub fn set_option(&self, option: DatabaseOption) -> FdbResult<()> {
        // Safety: It is safe to unwrap here because if we have given
        // out an `FdbDatabase` then `c_ptr` *must* be
        // `Some<Arc<...>>`.
        unsafe { option.apply((self.c_ptr.as_ref().unwrap()).as_ptr()) }
    }

    pub(crate) fn new(c_ptr: Option<Arc<NonNull<fdb_sys::FDBDatabase>>>) -> FdbDatabase {
        FdbDatabase { c_ptr }
    }
}

impl Drop for FdbDatabase {
    fn drop(&mut self) {
        if let Some(a) = self.c_ptr.take() {
            match Arc::try_unwrap(a) {
                Ok(a) => unsafe {
                    fdb_sys::fdb_database_destroy(a.as_ptr());
                },
                Err(at) => {
                    drop(at);
                }
            };
        }
    }
}

// # Safety
//
// After `FdbDatabase` is created, `NonNull<fdb_sys::FDBDatabase>` is
// accessed read-only, till it is finally dropped.
//
// Due to the use of `Arc`, copies are carefully managed, and
// `Drop::drop` calls `fdb_sys::fdb_database_destroy`, when the last
// copy of the `Arc` pointer is dropped.
//
// Other than `Drop::drop` (where we already ensure exclusive access),
// we don't have any mutable state inside `FdbDatabase` that needs to
// be protected with exclusive access. This allows us to add the
// `Send` trait.
//
// `FdbDatabase` is read-only, *without* interior mutability, it is
// safe to add `Sync` trait.
//
// The main reason for adding `Send` and `Sync` traits is so that
// values of `FdbDatabase` can be moved to other threads.
unsafe impl Send for FdbDatabase {}
unsafe impl Sync for FdbDatabase {}

#[cfg(test)]
mod tests {
    use impls::impls;

    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use super::FdbDatabase;

    #[test]
    fn impls() {
        #[rustfmt::skip]
	assert!(impls!(
	    FdbDatabase:
	        Send &
		Sync &
		Clone &
		!Copy));
    }

    #[allow(dead_code)]
    #[derive(Clone, Debug)]
    struct DummyFdbDatabase {
        c_ptr: Option<Arc<NonNull<fdb_sys::FDBDatabase>>>,
    }

    unsafe impl Send for DummyFdbDatabase {}
    unsafe impl Sync for DummyFdbDatabase {}

    #[test]
    fn trait_bounds() {
        fn trait_bounds_for_fdb_database<T>(_t: T)
        where
            T: Send + Sync + 'static,
        {
        }
        let d = DummyFdbDatabase {
            c_ptr: Some(Arc::new(NonNull::dangling())),
        };
        trait_bounds_for_fdb_database(d);
    }

    static mut DROP_TEST_DUMMY_FDB_DATABASE_HAS_DROPPED: AtomicBool = AtomicBool::new(false);

    #[derive(Clone, Debug)]
    struct DropTestDummyFdbDatabase {
        c_ptr: Option<Arc<NonNull<fdb_sys::FDBDatabase>>>,
    }

    unsafe impl Send for DropTestDummyFdbDatabase {}
    unsafe impl Sync for DropTestDummyFdbDatabase {}

    impl Drop for DropTestDummyFdbDatabase {
        fn drop(&mut self) {
            if let Some(a) = self.c_ptr.take() {
                match Arc::try_unwrap(a) {
                    Ok(_) => {
                        unsafe {
                            DROP_TEST_DUMMY_FDB_DATABASE_HAS_DROPPED.store(true, Ordering::SeqCst);
                        };
                    }
                    Err(at) => {
                        drop(at);
                    }
                };
            }
        }
    }

    #[tokio::test]
    async fn multiple_drop() {
        let d0 = DropTestDummyFdbDatabase {
            c_ptr: Some(Arc::new(NonNull::dangling())),
        };

        // Initially this is false.
        assert!(!unsafe { DROP_TEST_DUMMY_FDB_DATABASE_HAS_DROPPED.load(Ordering::SeqCst) });

        let d1 = d0.clone();

        assert_eq!(Arc::strong_count(d1.c_ptr.as_ref().unwrap()), 2);

        tokio::spawn(async move {
            let _ = d1;
        })
        .await
        .unwrap();

        assert_eq!(Arc::strong_count(d0.c_ptr.as_ref().unwrap()), 1);

        let d2 = d0.clone();
        let d3 = d2.clone();

        tokio::spawn(async move {
            let _ = d2;
            let _ = d3;
        })
        .await
        .unwrap();

        assert_eq!(Arc::strong_count(d0.c_ptr.as_ref().unwrap()), 1);

        drop(d0);

        assert!(unsafe { DROP_TEST_DUMMY_FDB_DATABASE_HAS_DROPPED.load(Ordering::SeqCst) });
    }
}
