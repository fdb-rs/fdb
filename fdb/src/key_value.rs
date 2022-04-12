//! Key, value and selector types used to access FoundationDB
use bytes::Bytes;

/// [`Key`] represents a FDB key, a lexicographically-ordered sequence
/// of bytes.
///
/// [`Key`] can be converted from and into [`Bytes`].
#[derive(Clone, Debug, PartialEq)]
pub struct Key(Bytes);

impl From<Bytes> for Key {
    fn from(b: Bytes) -> Key {
        Key(b)
    }
}

impl From<Key> for Bytes {
    fn from(k: Key) -> Bytes {
        k.0
    }
}

/// [`Value`] represents a value of an FDB [`Key`] and is a sequence
/// of bytes.
///
/// [`Value`] can be converted from and into [`Bytes`].
#[derive(Clone, Debug, PartialEq)]
pub struct Value(Bytes);

impl From<Bytes> for Value {
    fn from(b: Bytes) -> Value {
        Value(b)
    }
}

impl From<Value> for Bytes {
    fn from(v: Value) -> Bytes {
        v.0
    }
}

/// A key/value pair.
///
/// Range read operations on FDB return [`KeyValue`]s.
#[derive(Clone, Debug)]
pub struct KeyValue {
    key: Key,
    value: Value,
}

impl KeyValue {
    /// Gets a reference to [`Key`] from [`KeyValue`].
    pub fn get_key_ref(&self) -> &Key {
        &self.key
    }

    /// Gets a reference to [`Value`] from [`KeyValue`].
    pub fn get_value_ref(&self) -> &Value {
        &self.value
    }

    /// Extract [`Key`] from [`KeyValue`].
    pub fn into_key(self) -> Key {
        self.key
    }

    /// Extract [`Value`] from [`KeyValue`].
    pub fn into_value(self) -> Value {
        self.value
    }

    /// Extract [`Key`] and [`Value`] from [`KeyValue`].
    pub fn into_parts(self) -> (Key, Value) {
        (self.key, self.value)
    }

    pub(crate) fn new(key: Key, value: Value) -> KeyValue {
        KeyValue { key, value }
    }
}

/// [`KeySelector`] identifies a particular key in the database.
///
/// FDB's lexicographically ordered data model permits finding keys
/// based on their order (for example, finding the first key in the
/// database greater than a given key). Key selectors represent a
/// description of a key in the database that could be resolved to an
/// actual key by transaction's [`get_key`] or used directly as the
/// beginning or end of a range in transaction's [`get_range`].
///
/// For more about how key selectors work in practive, see the [`key
/// selector`] documentation. Note that the way key selectors are
/// resolved is somewhat non-intuitive, so users who wish to use a key
/// selector other than the default ones described below should
/// probably consult that documentation before proceeding.
///
/// Generally one of the following methods should be used to construct
/// a [`KeySelector`].
/// - [`last_less_than`]
/// - [`last_less_or_equal`]
/// - [`first_greater_than`]
/// - [`first_greater_or_equal`]
///
/// This is an *immutable* type. The `add(i32)` call does not modify
/// internal state, but returns a new value.
///
/// [`get_key`]: crate::transaction::ReadTransaction::get_key
/// [`get_range`]: crate::transaction::ReadTransaction::get_range
/// [`key selector`]: https://apple.github.io/foundationdb/developer-guide.html#key-selectors
/// [`last_less_than`]: KeySelector::last_less_than
/// [`last_less_or_equal`]: KeySelector::last_less_or_equal
/// [`first_greater_than`]: KeySelector::first_greater_than
/// [`first_greater_or_equal`]: KeySelector::first_greater_or_equal
#[derive(Clone, Debug)]
pub struct KeySelector {
    key: Key,
    or_equal: bool,
    offset: i32,
}

impl KeySelector {
    /// Create a new [`KeySelector`] from the given parameters.
    pub fn new(key: impl Into<Key>, or_equal: bool, offset: i32) -> KeySelector {
        KeySelector {
            key: key.into(),
            or_equal,
            offset,
        }
    }

    /// Returns a new [`KeySelector`] offset by a given number of keys
    /// from this one.
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, offset: i32) -> KeySelector {
        KeySelector::new(self.key, self.or_equal, self.offset + offset)
    }

    /// Creates a [`KeySelector`] that picks the first key greater
    /// than or equal to the parameter.
    pub fn first_greater_or_equal(key: impl Into<Key>) -> KeySelector {
        KeySelector::new(key, false, 1)
    }

    /// Creates a [`KeySelector`] that picks the first key greater
    /// than or equal to the parameter.
    pub fn first_greater_than(key: impl Into<Key>) -> KeySelector {
        KeySelector::new(key, true, 1)
    }

    /// Returns a reference to the key that serves as the anchor for
    /// this [`KeySelector`].
    pub fn get_key(&self) -> &Key {
        &self.key
    }

    /// Returns the key offset parameter for this [`KeySelector`].
    pub fn get_offset(&self) -> i32 {
        self.offset
    }

    /// Creates a [`KeySelector`] that picks the last key less than or
    /// equal to the parameter.
    pub fn last_less_or_equal(key: impl Into<Key>) -> KeySelector {
        KeySelector::new(key, true, 0)
    }

    /// Creates a [`KeySelector`] that picks the last key less than the parameter.
    pub fn last_less_than(key: impl Into<Key>) -> KeySelector {
        KeySelector::new(key, false, 0)
    }

    pub(crate) fn or_equal(&self) -> bool {
        self.or_equal
    }
}
