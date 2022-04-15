use std::future::Future;
use std::ptr::{self, NonNull};
use std::sync::Arc;

use crate::error::{check, FdbError, FdbResult};
use crate::transaction::{FdbReadTransaction, FdbTransaction, ReadTransaction, Transaction};
use crate::Tenant;

/// [`FdbTenant`] provides APIs for transactionally interacting with
/// [`Tenant`]s.
///
/// The simplest correct programs using tentants will make use of the
/// [`run`] and [`read`] methods. [`run`] will call [`commit`] after
/// the user code has been executed.
///
/// A handle to FDB tentant. All reads and writes to the tenant are
/// transactional.
///
/// A [`FdbTenant`] can be created using [`open_tenant`] method.
///
/// [`commit`]: FdbTransaction::commit
/// [`read`]: FdbTenant::read
/// [`run`]: FdbTenant::run
/// [`open_tenant`]: crate::database::FdbDatabase::open_tenant
//
// *NOTE*: If you make changes to this type, make sure you update
//         tests for `DummyFdbTenant`, `DropTestDummyTenant`
//         accordingly.
#[derive(Clone, Debug)]
pub struct FdbTenant {
    c_ptr: Option<Arc<NonNull<fdb_sys::FDBTenant>>>,
    name: Tenant,
}

impl FdbTenant {
    // In Java following method is on `Interface Tenant`

    /// Creates a [`FdbTransaction`] that operates on this
    /// [`FdbTenant`].
    pub fn create_transaction(&self) -> FdbResult<FdbTransaction> {
        let mut ptr: *mut fdb_sys::FDB_transaction = ptr::null_mut();
        // Safety: It is safe to unwrap here because if we have given
        // out an `FdbTenant` then `c_ptr` *must* be `Some<Arc<...>>`.
        check(unsafe {
            fdb_sys::fdb_tenant_create_transaction(
                (*(self.c_ptr.as_ref().unwrap())).as_ptr(),
                &mut ptr,
            )
        })
        .map(|_| {
            FdbTransaction::new(Some(Arc::new(NonNull::new(ptr).expect(
                "fdb_tenant_create_transaction returned null, but did not return an error",
            ))))
        })
    }

    /// Returns the name of this [`Tenant`].
    pub fn get_name(&self) -> Tenant {
        self.name.clone()
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

    pub(crate) fn new(c_ptr: Option<Arc<NonNull<fdb_sys::FDBTenant>>>, name: Tenant) -> FdbTenant {
        FdbTenant { c_ptr, name }
    }
}

impl Drop for FdbTenant {
    fn drop(&mut self) {
        if let Some(a) = self.c_ptr.take() {
            match Arc::try_unwrap(a) {
                Ok(a) => unsafe {
                    fdb_sys::fdb_tenant_destroy(a.as_ptr());
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
// After `FdbTenant` is created, `NonNull<fdb_sys::FDBTenant>` is
// accessed read-only, till it is finally dropped.
//
// Due to the use of `Arc`, copies are carefully managed, and
// `Drop::drop` calls `fdb_sys::fdb_tenant_destroy`, when the last
// copy of the `Arc` pointer is dropped.
//
// Other than `Drop::drop` (where we already ensure exclusive access),
// we don't have any mutable state inside `FdbTenant` that needs to be
// protected with exclusive access. This allows us to add the `Send`
// trait.
//
// `FdbTenant` is read-only, *without* interior mutability, it is safe
// to add `Sync` trait.
//
// The main reason for adding `Send` and `Sync` traits is so that
// values of `FdbTenant` can be moved to other threads.
unsafe impl Send for FdbTenant {}
unsafe impl Sync for FdbTenant {}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use impls::impls;

    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use crate::Tenant;

    use super::FdbTenant;

    #[test]
    fn impls() {
        #[rustfmt::skip]
	assert!(impls!(
	    FdbTenant:
	        Send &
		Sync &
		Clone &
		!Copy));
    }

    #[allow(dead_code)]
    #[derive(Clone, Debug)]
    struct DummyFdbTenant {
        c_ptr: Option<Arc<NonNull<fdb_sys::FDBTenant>>>,
        name: Tenant,
    }

    unsafe impl Send for DummyFdbTenant {}
    unsafe impl Sync for DummyFdbTenant {}

    #[test]
    fn trait_bounds() {
        fn trait_bounds_for_fdb_tenant<T>(_t: T)
        where
            T: Send + Sync + 'static,
        {
        }
        let d = DummyFdbTenant {
            c_ptr: Some(Arc::new(NonNull::dangling())),
            name: Bytes::new().into(),
        };
        trait_bounds_for_fdb_tenant(d);
    }

    static mut DROP_TEST_DUMMY_FDB_TENANT_HAS_DROPPED: AtomicBool = AtomicBool::new(false);

    // We don't use `name` in the tests. We add it here as it is in
    // `FdbTenant` type.
    #[allow(dead_code)]
    #[derive(Clone, Debug)]
    struct DropTestDummyFdbTenant {
        c_ptr: Option<Arc<NonNull<fdb_sys::FDBTenant>>>,
        name: Tenant,
    }

    unsafe impl Send for DropTestDummyFdbTenant {}
    unsafe impl Sync for DropTestDummyFdbTenant {}

    impl Drop for DropTestDummyFdbTenant {
        fn drop(&mut self) {
            if let Some(a) = self.c_ptr.take() {
                match Arc::try_unwrap(a) {
                    Ok(_) => {
                        unsafe {
                            DROP_TEST_DUMMY_FDB_TENANT_HAS_DROPPED.store(true, Ordering::SeqCst);
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
        let d0 = DropTestDummyFdbTenant {
            c_ptr: Some(Arc::new(NonNull::dangling())),
            name: Bytes::new().into(),
        };

        // Initially this is false.
        assert!(!unsafe { DROP_TEST_DUMMY_FDB_TENANT_HAS_DROPPED.load(Ordering::SeqCst) });

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

        assert!(unsafe { DROP_TEST_DUMMY_FDB_TENANT_HAS_DROPPED.load(Ordering::SeqCst) });
    }
}
