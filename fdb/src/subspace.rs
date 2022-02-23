//! Provides a convenient way to define namespaces for different
//! categories of data.
//!
//! The namespace is specified by a prefix tuple which is prepended to
//! all tuples packed by the subspace. When unpacking a key with the
//! subspace, the prefix tuple will be removed from the result. As a
//! best practice, API clients should use atleast one subspace for
//! application data.
//!
//! See [general subspace documentation] for information about how
//! subspaces work and interact with other parts of the built-in
//! keyspace management features.
//!
//! [general subspace documentation]: https://apple.github.io/foundationdb/developer-guide.html#developer-guide-sub-keyspaces

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{
    FdbError, FdbResult, SUBSPACE_PACK_WITH_VERSIONSTAMP_PREFIX_INCOMPLETE,
    SUBSPACE_UNPACK_KEY_MISMATCH,
};
use crate::range::Range;
use crate::tuple::Tuple;

/// Subspace provides a convenient way to use [`Tuple`] to define
/// namespaces for different categories of data.
#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct Subspace {
    // Used to track if the raw_prefix contains an incomplete
    // versionstamp. This is important when trying to pack the
    // subspace that contains a versionstamp.
    raw_prefix_has_incomplete_versionstamp: bool,
    raw_prefix: Bytes,
}

impl Subspace {
    /// Create a new [`Subspace`] with prefix [`Bytes`] and an empty
    /// prefix [`Tuple`].
    pub fn new(prefix_bytes: Bytes) -> Subspace {
        Subspace {
            raw_prefix_has_incomplete_versionstamp: false,
            raw_prefix: prefix_bytes,
        }
    }

    /// Gets a new subspace which is equivalent of this subspace with
    /// its prefix [`Tuple`] extended by the specified [`Tuple`].
    pub fn subspace(&self, tuple: &Tuple) -> Subspace {
        let raw_prefix_has_incomplete_versionstamp =
            self.raw_prefix_has_incomplete_versionstamp || tuple.has_incomplete_versionstamp();
        let mut raw_prefix = BytesMut::new();
        raw_prefix.put(self.raw_prefix.clone());
        raw_prefix.put(tuple.pack());

        Subspace {
            raw_prefix_has_incomplete_versionstamp,
            raw_prefix: raw_prefix.into(),
        }
    }

    /// Tests whether the specified key starts with this
    /// [`Subspace`]'s prefix, indicating that the [`Subspace`].
    /// logically contains key.
    pub fn contains(&self, key: &Bytes) -> bool {
        // Check to make sure `key` is atleast as long as
        // `raw_prefix`. Otherwise the slice operator will panic.
        if key.len() < self.raw_prefix.len() {
            false
        } else {
            self.raw_prefix[..] == key[..self.raw_prefix.len()]
        }
    }

    /// Get the key encoding prefix used for this [`Subspace`].
    pub fn pack(&self) -> Bytes {
        self.raw_prefix.clone()
    }

    /// Get the key encoding of the specified [`Tuple`] in this
    /// [`Subspace`] for use with [`SetVersionstampedKey`].
    ///
    /// # Panic
    ///
    /// The index where incomplete versionstamp is located is a 32-bit
    /// little-endian integer. If the generated index overflows
    /// [`u32`], then this function panics.
    ///
    /// [`SetVersionstampedKey`]: crate::transaction::MutationType::SetVersionstampedKey
    pub fn pack_with_versionstamp(&self, tuple: &Tuple) -> FdbResult<Bytes> {
        if self.raw_prefix_has_incomplete_versionstamp {
            Err(FdbError::new(
                SUBSPACE_PACK_WITH_VERSIONSTAMP_PREFIX_INCOMPLETE,
            ))
        } else {
            tuple.pack_with_versionstamp(self.raw_prefix.clone())
        }
    }

    /// Gets a [`Range`] representing all keys in the [`Subspace`]
    /// strictly starting with the specified [`Tuple`].
    ///
    /// # Panic
    ///
    /// Panics if the tuple or subspace contains an incomplete
    /// [`Versionstamp`].
    ///
    /// [`Versionstamp`]: crate::tuple::Versionstamp
    pub fn range(&self, tuple: &Tuple) -> Range {
        if self.raw_prefix_has_incomplete_versionstamp {
            panic!(
                "Cannot create Range value as subspace prefix contains an incomplete versionstamp"
            );
        }
        tuple.range(self.raw_prefix.clone())
    }

    /// Gets the [`Tuple`] encoded by the given key, with this
    /// [`Subspace`]'s prefix removed.
    pub fn unpack(&self, key: &Bytes) -> FdbResult<Tuple> {
        if !self.contains(key) {
            Err(FdbError::new(SUBSPACE_UNPACK_KEY_MISMATCH))
        } else {
            Tuple::from_bytes(key.slice(self.raw_prefix.len()..))
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};

    use crate::error::{
        FdbError, SUBSPACE_PACK_WITH_VERSIONSTAMP_PREFIX_INCOMPLETE, SUBSPACE_UNPACK_KEY_MISMATCH,
        TUPLE_PACK_WITH_VERSIONSTAMP_MULTIPLE_FOUND,
    };
    use crate::range::Range;
    use crate::tuple::{Tuple, Versionstamp};

    use super::Subspace;

    #[test]
    fn new() {
        let s = Subspace::new(Bytes::from_static(&b"prefix"[..]));
        assert!(
            !s.raw_prefix_has_incomplete_versionstamp
                && s.raw_prefix == Bytes::from_static(&b"prefix"[..])
        );
    }

    #[test]
    fn subspace() {
        let mut t = Tuple::new();
        t.add_string("hello".to_string());

        let s = Subspace::new(Bytes::new()).subspace(&t);

        assert!(!s.raw_prefix_has_incomplete_versionstamp && s.raw_prefix == t.pack());

        let mut t = Tuple::new();
        t.add_versionstamp(Versionstamp::incomplete(0));

        let s = Subspace::new(Bytes::new()).subspace(&t);

        assert!(s.raw_prefix_has_incomplete_versionstamp && s.raw_prefix == t.pack());
    }

    #[test]
    fn contains() {
        let s = Subspace::new(Bytes::from_static(&b"prefix"[..]));

        let mut t = Tuple::new();

        // length mismatch
        assert!(!s.contains(
            &Subspace::new(Bytes::from_static(&b"p"[..]))
                .subspace(&t)
                .pack()
        ));

        t.add_string("hello".to_string());

        assert!(!s.contains(
            &Subspace::new(Bytes::from_static(&b"wrong_prefix"[..]))
                .subspace(&t)
                .pack()
        ));

        // While this returns `true`, doing something like this will
        // cause `unpack()` to fail.
        assert!(s.contains(
            &Subspace::new(Bytes::from_static(&b"prefix_plus_garbage"[..]))
                .subspace(&t)
                .pack()
        ));

        assert!(s.contains(
            &Subspace::new(Bytes::from_static(&b"prefix"[..]))
                .subspace(&t)
                .pack()
        ));
    }

    #[test]
    fn pack() {
        let mut t = Tuple::new();
        t.add_string("hello".to_string());

        let s = Subspace::new(Bytes::from_static(&b"prefix"[..]));

        assert_eq!(s.subspace(&t).pack(), {
            let mut b = BytesMut::new();

            b.put(&b"prefix"[..]);
            b.put(t.pack());
            Into::<Bytes>::into(b)
        });
    }

    #[test]
    fn pack_with_versionstamp() {
        let mut t = Tuple::new();
        t.add_versionstamp(Versionstamp::incomplete(0));

        let s = Subspace::new(Bytes::new()).subspace(&t);

        assert_eq!(
            s.pack_with_versionstamp(&{
                let mut t1 = Tuple::new();
                t1.add_string("hello".to_string());
                t1
            }),
            Err(FdbError::new(
                SUBSPACE_PACK_WITH_VERSIONSTAMP_PREFIX_INCOMPLETE
            ))
        );

        let mut t = Tuple::new();
        t.add_string("foo".to_string());
        t.add_versionstamp(Versionstamp::incomplete(0));

        let s = Subspace::new(Bytes::from_static(&b"prefix"[..]));

        assert_eq!(
            s.pack_with_versionstamp(&t),
            t.pack_with_versionstamp(Bytes::from_static(&b"prefix"[..]))
        );

        let mut t = Tuple::new();
        t.add_null();
        t.add_versionstamp(Versionstamp::incomplete(0));
        t.add_tuple({
            let mut t1 = Tuple::new();
            t1.add_string("foo".to_string());
            t1.add_versionstamp(Versionstamp::incomplete(1));
            t1
        });

        assert_eq!(
            s.pack_with_versionstamp(&t),
            Err(FdbError::new(TUPLE_PACK_WITH_VERSIONSTAMP_MULTIPLE_FOUND))
        );
    }

    #[test]
    fn range() {
        let s = Subspace::new(Bytes::new()).subspace(&{
            let mut t = Tuple::new();
            t.add_versionstamp(Versionstamp::incomplete(0));
            t
        });

        assert!(std::panic::catch_unwind(|| {
            s.range(&{
                let mut t = Tuple::new();
                t.add_string("should_panic".to_string());
                t
            });
        })
        .is_err());

        let s = Subspace::new(Bytes::from_static(&b"prefix"[..]));

        assert_eq!(
            s.range(&{
                let mut t = Tuple::new();
                t.add_bytes(Bytes::from_static(&b"foo"[..]));
                t
            }),
            Range::new(
                Bytes::from_static(&b"prefix\x01foo\x00\x00"[..]),
                Bytes::from_static(&b"prefix\x01foo\x00\xFF"[..])
            )
        );
    }

    #[test]
    fn unpack() {
        let s = Subspace::new(Bytes::from_static(&b"prefix"[..]));

        let key = Subspace::new(Bytes::from_static(&b"wrong_prefix"[..]))
            .subspace(&{
                let mut t = Tuple::new();
                t.add_string("hello".to_string());
                t
            })
            .pack();

        assert_eq!(
            s.unpack(&key),
            Err(FdbError::new(SUBSPACE_UNPACK_KEY_MISMATCH))
        );

        let key = Subspace::new(Bytes::from_static(&b"prefix"[..]))
            .subspace(&{
                let mut t = Tuple::new();
                t.add_string("hello".to_string());
                t
            })
            .pack();

        assert_eq!(
            s.unpack(&key),
            Ok({
                let mut t = Tuple::new();
                t.add_string("hello".to_string());
                t
            })
        );
    }
}
