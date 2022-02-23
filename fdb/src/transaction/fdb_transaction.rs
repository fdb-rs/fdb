use bytes::Bytes;

use std::convert::TryInto;
use std::future::Future;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::error::{check, FdbError, FdbResult};
use crate::future::{
    FdbFuture, FdbFutureCStringArray, FdbFutureI64, FdbFutureKey, FdbFutureMaybeValue,
    FdbFutureUnit, FdbStreamKeyValue,
};
use crate::option::ConflictRangeType;
use crate::range::{Range, RangeOptions};
use crate::transaction::{MutationType, ReadTransaction, Transaction, TransactionOption};
use crate::tuple::key_util;
use crate::{Key, KeySelector, Value};

/// Committed version of the [`Transaction`].
///
/// [`get_committed_version`] provides a value of this type. This
/// value can be returned from [`run`] method closure. After the
/// transaction successfully commits, you can use [`into`] method to
/// get the committed version.
///
/// [`get_committed_version`]: Transaction::get_committed_version
/// [`run`]: crate::database::FdbDatabase::run
/// [`into`]: Into::into
//
// Here we are maintaining a copy of `FdbTransaction`, in order to
// prevent the `run` method from advertently destroying
// `fdb_sys::FDBTransaction`.
#[derive(Debug)]
pub struct CommittedVersion {
    fdb_transaction: FdbTransaction,
}

impl CommittedVersion {
    fn new(fdb_transaction: FdbTransaction) -> CommittedVersion {
        CommittedVersion { fdb_transaction }
    }
}

impl From<CommittedVersion> for FdbResult<i64> {
    fn from(t: CommittedVersion) -> FdbResult<i64> {
        let mut out_version = 0;

        check(unsafe {
            fdb_sys::fdb_transaction_get_committed_version(
                t.fdb_transaction.get_c_api_ptr(),
                &mut out_version,
            )
        })
        .map(|_| out_version)
    }
}

/// [`fdb_c`] client level versionstamp.
///
/// [`get_versionstamp`] provides a value of this type. This value can
/// be returned from the [`run`] method closure. After the transaction
/// successfully commits, you can use the [`get`] method to get the
/// versionstamp.
///
/// [`fdb_c`]: https://apple.github.io/foundationdb/data-modeling.html#versionstamps
/// [`run`]: crate::database::FdbDatabase::run
/// [`get_versionstamp`]: Transaction::get_versionstamp
/// [`get`]: TransactionVersionstamp::get
//
// Here we are maintaining a copy of `FdbTransaction`, in order to
// prevent the `run` method from advertently destroying
// `fdb_sys::FDBTransaction`
#[derive(Debug)]
#[allow(dead_code)]
pub struct TransactionVersionstamp {
    fdb_transaction: FdbTransaction,
    future: FdbFutureKey,
}

impl TransactionVersionstamp {
    /// Get [`fdb_c`] client level versionstamp.
    ///
    /// [`fdb_c`]: https://apple.github.io/foundationdb/data-modeling.html#versionstamps
    pub async fn get(self) -> FdbResult<Bytes> {
        self.future.await.map(|k| k.into())
    }

    /// Gets the inner [`FdbFutureKey`] while dropping the inner
    /// [`FdbTransaction`].
    ///
    /// # Safety
    ///
    /// You should not use this API. It exists to support binding
    /// tester.
    pub unsafe fn get_inner_future(self) -> FdbFutureKey {
        self.future
    }

    fn new(fdb_transaction: FdbTransaction, future: FdbFutureKey) -> TransactionVersionstamp {
        TransactionVersionstamp {
            fdb_transaction,
            future,
        }
    }
}

/// A handle to a FDB transaction.
///
/// [`create_transaction`] method on [`FdbDatabase`] can be used to
/// create a [`FdbTransaction`].
///
/// [`create_transaction`]: crate::database::FdbDatabase::create_transaction
/// [`FdbDatabase`]: crate::database::FdbDatabase
//
// Unlike `FDBTransaction.java`, we do not store a copy of
// `FdbDatabase` here. It is the responsibility of the caller to
// ensure that `FdbDatabase` is alive during the lifetime of the
// `FdbTransaction`.
//
// The design of `FdbTransaction` is very similar to the design of
// `FdbDatabase`, where in the `Drop` trait we ensure that when we
// have the last `Arc` to `fdb_sys::FDBTransaction`, then we
// `fdb_sys::fdb_transaction_destroy`.
#[derive(Clone, Debug)]
pub struct FdbTransaction {
    c_ptr: Option<Arc<NonNull<fdb_sys::FDBTransaction>>>,
}

impl FdbTransaction {
    /// Return a special-purpose, read-only view of the database
    /// ([`FdbReadTransaction`]). Reads done using [`snapshot`] are
    /// known as *snapshot reads*. Snapshot reads selectively relax
    /// FDB's isolation property, reducing transaction conflicts but
    /// making reasoning about concurrency harder.
    ///
    /// For more information about how to use snapshot reads
    /// correctly, see [snapshot reads].
    ///
    /// [`snapshot`]: FdbTransaction::snapshot
    /// [snapshot reads]: https://apple.github.io/foundationdb/developer-guide.html#snapshot-reads
    pub fn snapshot(&self) -> FdbReadTransaction {
        let c_ptr = self.c_ptr.clone();

        FdbReadTransaction::new(FdbTransaction { c_ptr })
    }

    /// Runs a closure in the context of this [`FdbTransaction`].
    ///
    /// # Safety
    ///
    /// You should not use this API. It exists to support binding
    /// tester.
    pub async unsafe fn run<T, F, Fut>(&self, mut f: F) -> FdbResult<T>
    where
        F: FnMut(FdbTransaction) -> Fut,
        Fut: Future<Output = FdbResult<T>>,
    {
        // We don't do `commit()` here because the semantics of `run`
        // is not very clear. If binding tester's `execute_mutation`
        // fails with a retryable transaction on `tr:
        // &FdbTransaction`, we'll possibly have to consider adding
        // `on_error` and `commit` check here.
        f(self.clone()).await
    }

    /// Runs a closure in the context of [`FdbReadTransaction`],
    /// derived from [`FdbTransaction`].
    ///
    /// # Safety
    ///
    /// You should not use this API. It exists to support binding
    /// tester.
    //
    // Note:
    //
    // In the following case we must use `F: FnMut(FdbTransaction) ->
    // Fut` because we need to ensure that any reads done within the
    // closure should be non-snapshot reads. Non-snapshot reads happen
    // on `FdbTransaction`. It is the responsibility of the caller to
    // ensure that they don't do any mutations in the closure.
    pub async unsafe fn read<T, F, Fut>(&self, mut f: F) -> FdbResult<T>
    where
        F: FnMut(FdbTransaction) -> Fut,
        Fut: Future<Output = FdbResult<T>>,
    {
        f(self.clone()).await
    }

    pub(crate) fn new(c_ptr: Option<Arc<NonNull<fdb_sys::FDBTransaction>>>) -> FdbTransaction {
        FdbTransaction { c_ptr }
    }

    pub(crate) fn get_c_api_ptr(&self) -> *mut fdb_sys::FDB_transaction {
        // Safety: It is safe to unwrap here because if we have a
        // `self: &FdbTransaction`, then `c_ptr` *must* be
        // `Some<Arc<NonNull<...>>>`. *Only* time that `c_ptr` is
        // `None` is after `Drop::drop` has been called.
        (self.c_ptr.as_ref().unwrap()).as_ptr()
    }
}

impl Drop for FdbTransaction {
    fn drop(&mut self) {
        if let Some(a) = self.c_ptr.take() {
            match Arc::try_unwrap(a) {
                Ok(a) => unsafe {
                    fdb_sys::fdb_transaction_destroy(a.as_ptr());
                },
                Err(at) => {
                    drop(at);
                }
            };
        }
    }
}

// `snapshot` is `false` below because any reads that we do on
// `FdbTransaction` is a `non-snapshot` read.
impl ReadTransaction for FdbTransaction {
    unsafe fn on_error(&self, e: FdbError) -> FdbFutureUnit {
        FdbFuture::new(fdb_sys::fdb_transaction_on_error(
            self.get_c_api_ptr(),
            e.code(),
        ))
    }

    fn get(&self, key: impl Into<Key>) -> FdbFutureMaybeValue {
        internal::read_transaction::get(self.get_c_api_ptr(), key, false)
    }

    fn get_addresses_for_key(&self, key: impl Into<Key>) -> FdbFutureCStringArray {
        internal::read_transaction::get_addresses_for_key(self.get_c_api_ptr(), key)
    }

    fn get_estimated_range_size_bytes(&self, range: Range) -> FdbFutureI64 {
        let (begin, end) = range.deconstruct();

        internal::read_transaction::get_estimated_range_size_bytes(self.get_c_api_ptr(), begin, end)
    }

    fn get_key(&self, selector: KeySelector) -> FdbFutureKey {
        internal::read_transaction::get_key(self.get_c_api_ptr(), selector, false)
    }

    fn get_range(
        &self,
        begin: KeySelector,
        end: KeySelector,
        options: RangeOptions,
    ) -> FdbStreamKeyValue {
        FdbStreamKeyValue::new(self.clone(), begin, end, options, false)
    }

    unsafe fn get_read_version(&self) -> FdbFutureI64 {
        internal::read_transaction::get_read_version(self.get_c_api_ptr())
    }

    fn set_option(&self, option: TransactionOption) -> FdbResult<()> {
        internal::read_transaction::set_option(self.get_c_api_ptr(), option)
    }

    unsafe fn set_read_version(&self, version: i64) {
        internal::read_transaction::set_read_version(self.get_c_api_ptr(), version)
    }
}

impl Transaction for FdbTransaction {
    fn add_read_conflict_key(&self, key: impl Into<Key>) -> FdbResult<()> {
        let begin_key = key.into();
        // Add a 0x00 to `end_key`. `begin_key` is inclusive and
        // `end_key` is exclusive. By appending `0x00` to `end_key` we
        // can make the conflict range contain only
        // `begin_key`. `key_util::key_after` method does this for us.
        let end_key = key_util::key_after(begin_key.clone());

        internal::transaction::add_conflict_range(
            self.get_c_api_ptr(),
            begin_key,
            end_key,
            ConflictRangeType::Read,
        )
    }

    fn add_read_conflict_range(&self, range: Range) -> FdbResult<()> {
        let (begin, end) = range.deconstruct();

        internal::transaction::add_conflict_range(
            self.get_c_api_ptr(),
            begin,
            end,
            ConflictRangeType::Read,
        )
    }

    fn add_write_conflict_key(&self, key: impl Into<Key>) -> FdbResult<()> {
        let begin_key = key.into();
        // Add a 0x00 to `end_key`. `begin_key` is inclusive and
        // `end_key` is exclusive. By appending `0x00` to `end_key` we
        // can make the conflict range contain only
        // `begin_key`. `key_util::key_after` method does this for us.
        let end_key = key_util::key_after(begin_key.clone());

        internal::transaction::add_conflict_range(
            self.get_c_api_ptr(),
            begin_key,
            end_key,
            ConflictRangeType::Write,
        )
    }

    fn add_write_conflict_range(&self, range: Range) -> FdbResult<()> {
        let (begin, end) = range.deconstruct();

        internal::transaction::add_conflict_range(
            self.get_c_api_ptr(),
            begin,
            end,
            ConflictRangeType::Write,
        )
    }

    unsafe fn cancel(&self) {
        fdb_sys::fdb_transaction_cancel(self.get_c_api_ptr());
    }

    fn clear(&self, key: impl Into<Key>) {
        let k = Bytes::from(key.into());
        let key_name = k.as_ref().as_ptr();
        let key_name_length = k.as_ref().len().try_into().unwrap();

        unsafe { fdb_sys::fdb_transaction_clear(self.get_c_api_ptr(), key_name, key_name_length) }
    }

    fn clear_range(&self, range: Range) {
        let (begin_key, end_key) = range.deconstruct();

        let bk = Bytes::from(begin_key);
        let begin_key_name = bk.as_ref().as_ptr();
        let begin_key_name_length = bk.as_ref().len().try_into().unwrap();

        let ek = Bytes::from(end_key);
        let end_key_name = ek.as_ref().as_ptr();
        let end_key_name_length = ek.as_ref().len().try_into().unwrap();

        unsafe {
            fdb_sys::fdb_transaction_clear_range(
                self.get_c_api_ptr(),
                begin_key_name,
                begin_key_name_length,
                end_key_name,
                end_key_name_length,
            )
        }
    }

    unsafe fn commit(&self) -> FdbFutureUnit {
        FdbFuture::new(fdb_sys::fdb_transaction_commit(self.get_c_api_ptr()))
    }

    fn get_approximate_size(&self) -> FdbFutureI64 {
        FdbFuture::new(unsafe {
            fdb_sys::fdb_transaction_get_approximate_size(self.get_c_api_ptr())
        })
    }

    unsafe fn get_committed_version(&self) -> CommittedVersion {
        CommittedVersion::new(self.clone())
    }

    unsafe fn get_versionstamp(&self) -> TransactionVersionstamp {
        let fdb_transaction = self.clone();

        let future = FdbFuture::new(fdb_sys::fdb_transaction_get_versionstamp(
            self.get_c_api_ptr(),
        ));

        TransactionVersionstamp::new(fdb_transaction, future)
    }

    unsafe fn mutate(&self, optype: MutationType, key: impl Into<Key>, param: Bytes) {
        let k = Bytes::from(key.into());
        let key_name = k.as_ref().as_ptr();
        let key_name_length = k.as_ref().len().try_into().unwrap();

        let p = param;
        let param = p.as_ref().as_ptr();
        let param_length = p.as_ref().len().try_into().unwrap();

        fdb_sys::fdb_transaction_atomic_op(
            self.get_c_api_ptr(),
            key_name,
            key_name_length,
            param,
            param_length,
            optype.code(),
        );
    }

    unsafe fn reset(&self) {
        fdb_sys::fdb_transaction_reset(self.get_c_api_ptr());
    }

    fn set(&self, key: impl Into<Key>, value: impl Into<Value>) {
        let k = Bytes::from(key.into());
        let key_name = k.as_ref().as_ptr();
        let key_name_length = k.as_ref().len().try_into().unwrap();

        // `value` is being overridden to get naming consistent with C
        // API parameters
        let v = Bytes::from(value.into());
        let value = v.as_ref().as_ptr();
        let value_length = v.as_ref().len().try_into().unwrap();

        unsafe {
            fdb_sys::fdb_transaction_set(
                self.get_c_api_ptr(),
                key_name,
                key_name_length,
                value,
                value_length,
            )
        }
    }

    fn watch(&self, key: impl Into<Key>) -> FdbFutureUnit {
        let k = Bytes::from(key.into());
        let key_name = k.as_ref().as_ptr();
        let key_name_length = k.as_ref().len().try_into().unwrap();

        FdbFuture::new(unsafe {
            fdb_sys::fdb_transaction_watch(self.get_c_api_ptr(), key_name, key_name_length)
        })
    }
}

// # Safety
//
// After `FdbTransaction` is created,
// `NonNull<fdb_sys::FDBTransaction>` is accessed read-only, till it
// is finally dropped.
//
// Due to the use of `Arc`, copies are carefully managed, and
// `Drop::drop` calls `fdb_sys::fdb_transaction_destroy`, when the
// last copy of the `Arc` pointer is dropped.
//
// Other than `Drop::drop` (where we already ensure exclusive access),
// we don't have any mutable state inside `FdbDatabase` that needs to
// be protected with exclusive access. This allows us to add the
// `Send` trait.
//
// `FdbTransaction` is read-only, *without* interior mutability, it is
// safe to add `Sync` trait.
//
// The main reason for adding `Send` trait is so that values of
// `FdbTransaction` can be moved to other threads.
//
// The main reason for adding `Sync` trait is because in binding
// tester, we are putting a `FdbTransaction` behind a
// `Arc<DashMap<...>` and this requires `Sync`.
unsafe impl Send for FdbTransaction {}
unsafe impl Sync for FdbTransaction {}

/// A handle to a FDB snapshot, suitable for performing snapshot
/// reads.
///
/// Snapshot reads offer more relaxed isolation level than FDB's
/// default serializable isolation, reducing transaction conflicts but
/// making it harder to reason about concurrency.
///
/// For more information about how to use snapshot reads correctly,
/// see [snapshot reads].
///
/// [`snapshot`] method on [`FdbTransaction`] can be used to create a
/// [`FdbReadTransaction`].
///
/// [`snapshot`]: FdbTransaction::snapshot
/// [snapshot reads]: https://apple.github.io/foundationdb/developer-guide.html#snapshot-reads
//
// `FdbReadTransaction` internally has a owned `FdbTransaction`. When
// the last `FdbTransaction` gets dropped, then `<FdbTransaction as
// Drop>::drop` will call `fdb_transaction_destroy()`. Therefore we
// don't have to do anything special when dropping
// `FdbReadTransaction`.
#[derive(Clone, Debug)]
pub struct FdbReadTransaction {
    inner: FdbTransaction,
}

impl FdbReadTransaction {
    /// Runs a closure in the context of this [`FdbReadTransaction`].
    ///
    /// # Safety
    ///
    /// You should not use this API. It exists to support binding
    /// tester.
    pub async unsafe fn read<T, F, Fut>(&self, mut f: F) -> FdbResult<T>
    where
        F: FnMut(FdbReadTransaction) -> Fut,
        Fut: Future<Output = FdbResult<T>>,
    {
        f(self.clone()).await
    }

    fn new(inner: FdbTransaction) -> FdbReadTransaction {
        FdbReadTransaction { inner }
    }
}

// `snapshot` is `true` below because any reads that we do on
// `FdbReadTransaction` is a `snapshot` read.
impl ReadTransaction for FdbReadTransaction {
    unsafe fn on_error(&self, e: FdbError) -> FdbFutureUnit {
        self.inner.on_error(e)
    }

    fn get(&self, key: impl Into<Key>) -> FdbFutureMaybeValue {
        internal::read_transaction::get(self.inner.get_c_api_ptr(), key, true)
    }

    fn get_addresses_for_key(&self, key: impl Into<Key>) -> FdbFutureCStringArray {
        internal::read_transaction::get_addresses_for_key(self.inner.get_c_api_ptr(), key)
    }

    fn get_estimated_range_size_bytes(&self, range: Range) -> FdbFutureI64 {
        self.inner.get_estimated_range_size_bytes(range)
    }

    fn get_key(&self, selector: KeySelector) -> FdbFutureKey {
        internal::read_transaction::get_key(self.inner.get_c_api_ptr(), selector, true)
    }

    fn get_range(
        &self,
        begin: KeySelector,
        end: KeySelector,
        options: RangeOptions,
    ) -> FdbStreamKeyValue {
        FdbStreamKeyValue::new(self.inner.clone(), begin, end, options, true)
    }

    unsafe fn get_read_version(&self) -> FdbFutureI64 {
        self.inner.get_read_version()
    }

    fn set_option(&self, option: TransactionOption) -> FdbResult<()> {
        self.inner.set_option(option)
    }

    unsafe fn set_read_version(&self, version: i64) {
        self.inner.set_read_version(version)
    }
}

pub(super) mod internal {
    pub(super) mod transaction {
        use bytes::Bytes;

        use std::convert::TryInto;

        use crate::error::{check, FdbResult};
        use crate::option::ConflictRangeType;
        use crate::Key;

        pub(crate) fn add_conflict_range(
            transaction: *mut fdb_sys::FDBTransaction,
            begin_key: Key,
            end_key: Key,
            ty: ConflictRangeType,
        ) -> FdbResult<()> {
            let bk = Bytes::from(begin_key);
            let begin_key_name = bk.as_ref().as_ptr();
            let begin_key_name_length = bk.as_ref().len().try_into().unwrap();

            let ek = Bytes::from(end_key);
            let end_key_name = ek.as_ref().as_ptr();
            let end_key_name_length = ek.as_ref().len().try_into().unwrap();

            check(unsafe {
                fdb_sys::fdb_transaction_add_conflict_range(
                    transaction,
                    begin_key_name,
                    begin_key_name_length,
                    end_key_name,
                    end_key_name_length,
                    ty.code(),
                )
            })
        }
    }

    pub(super) mod read_transaction {
        use bytes::Bytes;

        use std::convert::TryInto;

        use crate::error::FdbResult;
        use crate::future::{
            FdbFuture, FdbFutureCStringArray, FdbFutureI64, FdbFutureKey, FdbFutureMaybeValue,
        };
        use crate::transaction::TransactionOption;
        use crate::{Key, KeySelector};

        pub(crate) fn get(
            transaction: *mut fdb_sys::FDBTransaction,
            key: impl Into<Key>,
            snapshot: bool,
        ) -> FdbFutureMaybeValue {
            let k = Bytes::from(key.into());
            let key_name = k.as_ref().as_ptr();
            let key_name_length = k.as_ref().len().try_into().unwrap();
            let s = if snapshot { 1 } else { 0 };

            FdbFuture::new(unsafe {
                fdb_sys::fdb_transaction_get(transaction, key_name, key_name_length, s)
            })
        }

        pub(crate) fn get_addresses_for_key(
            transaction: *mut fdb_sys::FDBTransaction,
            key: impl Into<Key>,
        ) -> FdbFutureCStringArray {
            let k = Bytes::from(key.into());
            let key_name = k.as_ref().as_ptr();
            let key_name_length = k.as_ref().len().try_into().unwrap();

            FdbFuture::new(unsafe {
                fdb_sys::fdb_transaction_get_addresses_for_key(
                    transaction,
                    key_name,
                    key_name_length,
                )
            })
        }

        pub(crate) fn get_estimated_range_size_bytes(
            transaction: *mut fdb_sys::FDBTransaction,
            begin_key: Key,
            end_key: Key,
        ) -> FdbFutureI64 {
            let bk = Bytes::from(begin_key);
            let begin_key_name = bk.as_ref().as_ptr();
            let begin_key_name_length = bk.as_ref().len().try_into().unwrap();

            let ek = Bytes::from(end_key);
            let end_key_name = ek.as_ref().as_ptr();
            let end_key_name_length = ek.as_ref().len().try_into().unwrap();

            FdbFuture::new(unsafe {
                fdb_sys::fdb_transaction_get_estimated_range_size_bytes(
                    transaction,
                    begin_key_name,
                    begin_key_name_length,
                    end_key_name,
                    end_key_name_length,
                )
            })
        }

        pub(crate) fn get_key(
            transaction: *mut fdb_sys::FDBTransaction,
            selector: KeySelector,
            snapshot: bool,
        ) -> FdbFutureKey {
            let k = Bytes::from(selector.get_key().clone());
            let key_name = k.as_ref().as_ptr();
            let key_name_length = k.as_ref().len().try_into().unwrap();
            let or_equal = if selector.or_equal() { 1 } else { 0 };
            let offset = selector.get_offset();

            let s = if snapshot { 1 } else { 0 };

            FdbFuture::new(unsafe {
                fdb_sys::fdb_transaction_get_key(
                    transaction,
                    key_name,
                    key_name_length,
                    or_equal,
                    offset,
                    s,
                )
            })
        }

        pub(crate) fn get_read_version(transaction: *mut fdb_sys::FDBTransaction) -> FdbFutureI64 {
            FdbFuture::new(unsafe { fdb_sys::fdb_transaction_get_read_version(transaction) })
        }

        pub(crate) fn set_option(
            transaction: *mut fdb_sys::FDBTransaction,
            option: TransactionOption,
        ) -> FdbResult<()> {
            unsafe { option.apply(transaction) }
        }

        pub(crate) fn set_read_version(transaction: *mut fdb_sys::FDBTransaction, version: i64) {
            unsafe { fdb_sys::fdb_transaction_set_read_version(transaction, version) }
        }
    }
}

#[cfg(test)]
mod tests {
    use impls::impls;

    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use super::{FdbReadTransaction, FdbTransaction};

    #[test]
    fn impls() {
        #[rustfmt::skip]
	assert!(impls!(
	    FdbTransaction:
	        Send &
		!Copy));

        #[rustfmt::skip]
	assert!(impls!(
	    FdbReadTransaction:
	        Send &
		!Copy));
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct DummyFdbTransaction {
        c_ptr: Option<Arc<NonNull<fdb_sys::FDBTransaction>>>,
    }

    unsafe impl Send for DummyFdbTransaction {}

    #[test]
    fn trait_bounds() {
        fn trait_bounds_for_fdb_transaction<T>(_t: T)
        where
            T: Send + 'static,
        {
        }

        let d = DummyFdbTransaction {
            c_ptr: Some(Arc::new(NonNull::dangling())),
        };
        trait_bounds_for_fdb_transaction(d);
    }

    static mut DROP_TEST_DUMMY_FDB_TRANSACTION_HAS_DROPPED: AtomicBool = AtomicBool::new(false);

    #[derive(Debug)]
    struct DropTestDummyFdbTransaction {
        c_ptr: Option<Arc<NonNull<fdb_sys::FDBTransaction>>>,
    }

    unsafe impl Send for DropTestDummyFdbTransaction {}
    unsafe impl Sync for DropTestDummyFdbTransaction {}

    impl Drop for DropTestDummyFdbTransaction {
        fn drop(&mut self) {
            if let Some(a) = self.c_ptr.take() {
                match Arc::try_unwrap(a) {
                    Ok(_) => {
                        unsafe {
                            DROP_TEST_DUMMY_FDB_TRANSACTION_HAS_DROPPED
                                .store(true, Ordering::SeqCst);
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
        let d0 = DropTestDummyFdbTransaction {
            c_ptr: Some(Arc::new(NonNull::dangling())),
        };

        // Initially this is false.
        assert!(!unsafe { DROP_TEST_DUMMY_FDB_TRANSACTION_HAS_DROPPED.load(Ordering::SeqCst) });

        let d1 = DropTestDummyFdbTransaction {
            c_ptr: d0.c_ptr.clone(),
        };

        assert_eq!(Arc::strong_count(d1.c_ptr.as_ref().unwrap()), 2);

        tokio::spawn(async move {
            let _ = d1;
        })
        .await
        .unwrap();

        assert_eq!(Arc::strong_count(d0.c_ptr.as_ref().unwrap()), 1);

        let d2 = DropTestDummyFdbTransaction {
            c_ptr: d0.c_ptr.clone(),
        };
        let d3 = DropTestDummyFdbTransaction {
            c_ptr: d2.c_ptr.clone(),
        };

        tokio::spawn(async move {
            let _ = d2;
            let _ = d3;
        })
        .await
        .unwrap();

        assert_eq!(Arc::strong_count(d0.c_ptr.as_ref().unwrap()), 1);

        drop(d0);

        assert!(unsafe { DROP_TEST_DUMMY_FDB_TRANSACTION_HAS_DROPPED.load(Ordering::SeqCst) });
    }
}
