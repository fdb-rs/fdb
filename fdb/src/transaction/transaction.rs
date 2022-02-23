use bytes::Bytes;

use crate::error::FdbResult;
use crate::future::{FdbFutureI64, FdbFutureUnit};
use crate::range::Range;
use crate::transaction::{
    CommittedVersion, MutationType, ReadTransaction, TransactionVersionstamp,
};
use crate::{Key, Value};

/// A [`Transaction`] represents a FDB database transaction.
///
/// All operations on FDB take place, explicity or implicity, through
/// a [`Transaction`].
///
/// In FDB, a transaction is a mutable snapshot of a database. All
/// read and write operations on a transaction see and modify an
/// otherwise-unchanging version of the database and only change the
/// underlying database if and when the transaction is committed. Read
/// operations do see the effects of previous write operations on the
/// same transactions. Committing a transaction usually succeeds in
/// the absence of [conflicts].
///
/// Transactions group operations into a unit with the properties of
/// atomicity, isolation, and durability. Transactions also provide
/// the ability to maintain an application's invariants or integrity
/// constraints, supporting the property of consistency. Together
/// these properties are known as [ACID].
///
/// Transactions are also causally consistent: once a transaction has
/// been successfully committed, all subsequently created transactions
/// will see the modifications made by it. The most convenient way for
/// a developer to manage the lifecycle and retrying of a transaction
/// is to use [`run`] method on [`FdbDatabase`]. Otherwise, the client
/// must have retry logic for fatal failures, failures to commit, and
/// other transient errors.
///
/// Keys and values in FDB are byte arrays. To encode other data
/// types, see the [tuple layer] documentation.
///
/// **Note**: All keys with first byte `0xff` are reserved for
/// internal use.
///
/// [conflicts]: https://apple.github.io/foundationdb/developer-guide.html#developer-guide-transaction-conflicts
/// [ACID]: https://apple.github.io/foundationdb/developer-guide.html#acid
/// [`FdbDatabase`]: crate::database::FdbDatabase
/// [tuple layer]: crate::tuple
/// [`run`]: crate::database::FdbDatabase::run
//
// NOTE: Unlike Java API, `Transaction` does not extend (i.e., is a
//       subtrait of) `TransactionContext` (There is no
//       `TransactionContext` in our case). Also `onError` method is
//       on `ReadTransaction` as we need it to implement the retry
//       loop in `FdbDatabase::read`. There is no `getDatabase`
//       method, as we don't implement `Database` interface/trait.
pub trait Transaction: ReadTransaction {
    /// Adds a key to the transaction's read conflict ranges as if you
    /// had read the key.
    fn add_read_conflict_key(&self, key: impl Into<Key>) -> FdbResult<()>;

    /// Adds a range of keys to the transaction's read conflict ranges
    /// as if you had read the range.
    fn add_read_conflict_range(&self, range: Range) -> FdbResult<()>;

    /// Adds a key to the transaction's write conflict ranges as if
    /// you had written the key.
    fn add_write_conflict_key(&self, key: impl Into<Key>) -> FdbResult<()>;

    /// Adds a range of keys to the transaction's write conflict
    /// ranges as if you had cleared the range.
    fn add_write_conflict_range(&self, range: Range) -> FdbResult<()>;

    /// Cancels the [`Transaction`].
    ///
    /// # Safety
    ///
    /// See [C API] for more details.
    ///
    /// [C API]: https://apple.github.io/foundationdb/api-c.html#c.fdb_transaction_cancel
    unsafe fn cancel(&self);

    /// Clears a given key from the database.
    fn clear(&self, key: impl Into<Key>);

    /// Clears a range of keys from the database.
    fn clear_range(&self, range: Range);

    /// Commit this [`Transaction`].
    ///
    /// Equivalent to:
    ///
    /// ```ignore
    /// async unsafe fn commit(&self) -> FdbResult<()>
    /// ```
    /// # Safety
    ///
    /// See [C API] for more details.
    ///
    /// [C API]: https://apple.github.io/foundationdb/api-c.html#c.fdb_transaction_commit
    unsafe fn commit(&self) -> FdbFutureUnit;

    /// Returns a future that will contain the approximated size of
    /// the commit, which is the summation of mutations, read conflict
    /// ranges, and write conflict ranges.
    ///
    /// ```ignore
    /// async fn get_approximate_size(&self) -> FdbResult<i64>
    /// ```
    fn get_approximate_size(&self) -> FdbFutureI64;

    /// Gets the version number at which a successful commit modified
    /// the database.
    ///
    /// # Safety
    ///
    /// See [C API] for more details.
    ///
    /// [C API]: https://apple.github.io/foundationdb/api-c.html#c.fdb_transaction_get_committed_version
    unsafe fn get_committed_version(&self) -> CommittedVersion;

    /// Returns [`TransactionVersionstamp`] from which you can get the
    /// versionstamp which was used by any versionstamp operations in
    /// this transaction.
    ///
    /// # Safety
    ///
    /// See [C API] for more details.
    ///
    /// [C API]: https://apple.github.io/foundationdb/api-c.html#c.fdb_transaction_get_versionstamp
    unsafe fn get_versionstamp(&self) -> TransactionVersionstamp;

    /// An atomic operation is a single database command that carries
    /// out several logical steps: reading the value of a key,
    /// performing a transformation on that value, and writing the
    /// result.
    ///
    /// # Safety
    ///
    /// See the warning for [`MutationType::AppendIfFits`] variant.
    unsafe fn mutate(&self, optype: MutationType, key: impl Into<Key>, param: Bytes);

    /// Reset the [`Transaction`].
    ///
    /// # Safety
    ///
    /// See [C API] for more details.
    ///
    /// [C API]: https://apple.github.io/foundationdb/api-c.html#c.fdb_transaction_cancel
    unsafe fn reset(&self);

    /// Sets the value for a given key.
    fn set(&self, key: impl Into<Key>, value: impl Into<Value>);

    /// Creates a watch that will become ready when it reports a
    /// change to the value of the specified key.
    ///
    /// A watch's behavior is relative to the transaction that created
    /// it.
    fn watch(&self, key: impl Into<Key>) -> FdbFutureUnit;
}
