use crate::error::{FdbError, FdbResult};
use crate::future::{
    FdbFutureCStringArray, FdbFutureI64, FdbFutureKey, FdbFutureMaybeValue, FdbFutureUnit,
    FdbStreamKeyValue,
};
use crate::range::{Range, RangeOptions};
use crate::transaction::TransactionOption;
use crate::{Key, KeySelector};

#[cfg(feature = "fdb-7_1")]
use crate::future::{FdbFutureKeyArray, FdbStreamMappedKeyValue};

#[cfg(feature = "fdb-7_1")]
use crate::Mapper;

/// A read-only subset of a FDB [`Transaction`].
///
/// [`Transaction`]: crate::transaction::Transaction
//
// NOTE: Unlike Java API, `ReadTransaction` does not extend (i.e., is
//       a subtrait of) `ReadTransactionContext` (There is no
//       `ReadTransactionContext` in our case). This is to maintain
//       consistency with `Transaction`.
//
//       Also there is no `snapshot()` method on `ReadTransaction`
//       trait. Instead `snapshot()` is just a method on
//       `FdbTransaction` type. `onError` method is on
//       `ReadTransaction` trait, as its used to implement the retry
//       loop in `FdbDatabase::read`. We also don't have methods
//       `addReadConflictKeyIfNotSnapshot` and
//       `addReadConflictRangeIfNotSnapshot`.
pub trait ReadTransaction {
    /// Determines whether an error returned by a [`Transaction`]
    /// or [`ReadTransaction`] method is retryable. Waiting on the returned future will
    /// return the same error when fatal, or return `()` for retryable
    /// errors.
    ///
    /// Typical code will not use this method directly. It is used by
    /// [`run`] and [`read`] methods when they need to implement
    /// correct retry loop.
    ///
    /// Equivalent to:
    ///
    /// ```ignore
    /// async unsafe fn on_error(&self, e: FdbError) -> FdbResult<()>
    /// ```
    ///
    /// # Safety
    ///
    /// See [C API] for more details.
    ///
    /// [`Transaction`]: crate::transaction::Transaction
    /// [`run`]: crate::database::FdbDatabase::run
    /// [`read`]: crate::database::FdbDatabase::read
    /// [C API]: https://apple.github.io/foundationdb/api-c.html#c.fdb_transaction_on_error
    unsafe fn on_error(&self, e: FdbError) -> FdbFutureUnit;

    /// Gets a value from the database.    
    ///
    /// Equivalent to:
    ///
    /// ```ignore
    /// async fn get(&self, key: impl Into<Key>) -> FdbResult<Option<Value>>
    /// ```
    fn get(&self, key: impl Into<Key>) -> FdbFutureMaybeValue;

    /// Get a list of public network addresses as [`CString`], one for
    /// each of the storage servers responsible for storing [`Key`]
    /// and its associated value.
    ///
    /// Equivalent to:
    ///
    /// ```ignore
    /// async fn get_addresses_for_key(&self, key: impl Into<Key>) -> FdbResult<Vec<CString>>;
    /// ```
    ///
    /// [`CString`]: std::ffi::CString
    fn get_addresses_for_key(&self, key: impl Into<Key>) -> FdbFutureCStringArray;

    /// Gets an estimate for the number of bytes stored in the given
    /// range.
    ///
    /// Equivalent to:
    ///
    /// ```ignore
    /// async fn get_estimated_range_size_bytes(&self, range: Range) -> FdbResult<i64>
    /// ```
    ///
    /// # Note
    ///
    /// The estimated size is calculated based on the sampling done by
    /// FDB server. The sampling algorithm roughly works this way: The
    /// sampling algorithm works roughly in this way: the lager the
    /// key-value pair is, the more likely it would be sampled and the
    /// more accurate its sampled size would be. And due to that
    /// reason, it is recommended to use this API to query against
    /// large ranges for accuracy considerations. For a rough
    /// reference, if the returned size is larger than 3MB, one can
    /// consider the size to be accurate.
    fn get_estimated_range_size_bytes(&self, range: Range) -> FdbFutureI64;

    /// Returns the key referenced by the specificed [`KeySelector`].
    ///
    /// Equivalent to:
    ///
    /// ```ignore
    /// async fn get_key(&self, selector: KeySelector) -> FdbResult<Key>
    /// ```
    fn get_key(&self, selector: KeySelector) -> FdbFutureKey;

    #[cfg(feature = "fdb-7_1")]
    /// WARNING: This feature is considered experimental at this time.
    ///
    /// Gets an ordered range of mapped keys and values from the
    /// database.
    ///
    /// The returned [`FdbStreamMappedKeyValue`] implements [`Stream`]
    /// trait that yields a [`MappedKeyValue`] item.
    ///
    /// [`Stream`]: futures::Stream
    /// [`MappedKeyValue`]: crate::MappedKeyValue
    fn get_mapped_range(
        &self,
        begin: KeySelector,
        end: KeySelector,
        mapper: impl Into<Mapper>,
        options: RangeOptions,
    ) -> FdbStreamMappedKeyValue;

    /// Gets an ordered range of keys and values from the database.
    ///
    /// The returned [`FdbStreamKeyValue`] implements [`Stream`] trait
    /// that yields a [`KeyValue`] item.
    ///
    /// [`Stream`]: futures::Stream
    /// [`KeyValue`]: crate::KeyValue
    fn get_range(
        &self,
        begin: KeySelector,
        end: KeySelector,
        options: RangeOptions,
    ) -> FdbStreamKeyValue;

    #[cfg(feature = "fdb-7_1")]
    /// Gets a list of keys that can split the given range into
    /// (roughly) equally sized chunks based on `chunk_size`.
    ///
    /// Equivalent to:
    ///
    /// ```ignore
    /// async fn get_range_split_points(
    ///     &self,
    ///     begin: impl Into<Key>,
    ///     end: impl Into<Key>,
    ///     chunk_size: i64,
    /// ) -> FdbResult<Vec<Key>>
    /// ```
    fn get_range_split_points(
        &self,
        begin: impl Into<Key>,
        end: impl Into<Key>,
        chunk_size: i64,
    ) -> FdbFutureKeyArray;

    /// Gets the version at which the reads for this [`Transaction`]
    /// or [`ReadTransaction`] will access the database.
    ///
    /// Equivalent to:
    ///
    /// ```ignore
    /// async unsafe fn get_read_version(&self) -> FdbResult<i64>
    /// ```
    ///
    /// # Safety
    ///
    /// The [`FdbFuture`] resolves to an [`i64`] instead of a [`u64`]
    /// because of [internal representation]. Even though it is an
    /// [`i64`], the future will always return a positive
    /// number. Negative GRV numbers are used internally within FDB.
    ///
    /// You only rely on GRV only for read-only transactions. For
    /// read-write transactions you should use commit version.
    ///
    /// [`Transaction`]: crate::transaction::Transaction
    /// [`FdbFuture`]: crate::future::FdbFuture
    /// [internal representation]: https://github.com/apple/foundationdb/blob/6.3.22/fdbclient/FDBTypes.h#L32
    unsafe fn get_read_version(&self) -> FdbFutureI64;

    /// Set options on a [`Transaction`] or [`ReadTransaction`]
    ///
    /// [`Transaction`]: crate::transaction::Transaction
    fn set_option(&self, option: TransactionOption) -> FdbResult<()>;

    /// Directly sets the version of the database at which to execute
    /// reads.
    ///
    /// # Safety
    ///
    /// See [C API] for more details.
    ///
    /// [C API]: https://apple.github.io/foundationdb/api-c.html#c.fdb_transaction_set_read_version
    unsafe fn set_read_version(&self, version: i64);
}
