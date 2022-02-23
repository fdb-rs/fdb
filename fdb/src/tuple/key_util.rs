//! Utility functions for operating on [`Key`].
//!
//! Although built for FDB tuple layer, some functions may be useful
//! otherwise.
// In Java, this is `ByteArrayUtil` class.

use bytes::{BufMut, Bytes, BytesMut};

use std::convert::TryFrom;

use crate::error::{FdbError, FdbResult, TUPLE_KEY_UTIL_STRINC_ERROR};
use crate::Key;

/// Computes the key that would sort immediately after `key`.
///
/// # Panic
///
/// `key` must not be empty.
pub fn key_after(key: impl Into<Key>) -> Key {
    let mut res = BytesMut::new();
    res.put(Bytes::from(key.into()));
    res.put_u8(0x00);
    Bytes::from(res).into()
}

/// Checks if `key` starts with `prefix`.
pub fn starts_with(key: impl Into<Key>, prefix: impl Into<Key>) -> bool {
    let key = Bytes::from(key.into());
    let prefix = Bytes::from(prefix.into());

    // Check to make sure `key` is atleast as long as
    // `prefix`. Otherwise the slice operator will panic.
    if key.len() < prefix.len() {
        false
    } else {
        prefix[..] == key[..prefix.len()]
    }
}

/// Computes the first key that would sort outside the range prefixed
/// by `prefix`.
///
/// The `prefix` must not be empty or contain only `0xFF` bytes. That
/// is `prefix` must contain at least one byte not equal to `0xFF`.
///
/// This resulting [`Key`] serves as the exclusive upper-bound for
/// all keys prefixed by the argument `prefix`. In other words, it is
/// the first key for which the argument `prefix` is not a prefix.
pub fn strinc(prefix: impl Into<Key>) -> FdbResult<Key> {
    rstrip_xff(Bytes::from(prefix.into())).map(|x| {
        let mut res = BytesMut::new();

        // Ok to subtract because in the worst case the range will
        // be 0..0
        let non_ff_byte_index = x.len() - 1;

        res.put(&x[0..non_ff_byte_index]);
        res.put_u8(x[non_ff_byte_index] + 1);

        Bytes::from(res).into()
    })
}

// Custom function to strip `\xFF` from the right of a byte string.
fn rstrip_xff(input: Bytes) -> FdbResult<Bytes> {
    if input.is_empty() {
        Err(FdbError::new(TUPLE_KEY_UTIL_STRINC_ERROR))
    } else {
        // Safety: FDB key size cannot exceed 10,000 bytes in
        // size. So, this is well within isize::max.
        let mut i = isize::try_from(input.len() - 1).unwrap();

        while i >= 0 {
            // Safety: Safe to unwrap as we are converting from a
            // usize and also checking that `i >= 0` above.
            if input[usize::try_from(i).unwrap()] != 0xFF {
                break;
            }
            i -= 1;
        }

        if i < 0 {
            Err(FdbError::new(TUPLE_KEY_UTIL_STRINC_ERROR))
        } else {
            // Safety: We are checking above to make sure `i` is not
            // negative.
            Ok(input.slice(0..usize::try_from(i + 1).unwrap()))
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::error::{FdbError, TUPLE_KEY_UTIL_STRINC_ERROR};
    use crate::Key;

    use super::{key_after, rstrip_xff, starts_with, strinc};

    #[test]
    fn test_key_after() {
        assert_eq!(
            key_after(Bytes::new()),
            Key::from(Bytes::from_static(&b"\x00"[..]))
        );
        assert_eq!(
            key_after(Bytes::from_static(&b"hello_world"[..])),
            Key::from(Bytes::from_static(&b"hello_world\x00"[..])),
        );
    }

    #[test]
    fn test_starts_with() {
        // length mismatch
        assert!(!starts_with(
            Bytes::from_static(&b"p"[..]),
            Bytes::from_static(&b"prefix"[..])
        ));

        assert!(!starts_with(
            Bytes::from_static(&b"wrong_prefix"[..]),
            Bytes::from_static(&b"prefix"[..])
        ));

        assert!(starts_with(
            Bytes::from_static(&b"prefix_plus_something_else"[..]),
            Bytes::from_static(&b"prefix"[..])
        ));
    }

    #[test]
    fn test_rstrip_xff() {
        assert_eq!(
            rstrip_xff(Bytes::new()),
            Err(FdbError::new(TUPLE_KEY_UTIL_STRINC_ERROR))
        );

        assert_eq!(
            rstrip_xff(Bytes::from_static(&b"\xFF"[..])),
            Err(FdbError::new(TUPLE_KEY_UTIL_STRINC_ERROR))
        );

        assert_eq!(
            rstrip_xff(Bytes::from_static(&b"\xFF\xFF"[..])),
            Err(FdbError::new(TUPLE_KEY_UTIL_STRINC_ERROR))
        );
        assert_eq!(
            rstrip_xff(Bytes::from_static(&b"\x00"[..])),
            Ok(Bytes::from_static(&b"\x00"[..]))
        );
        assert_eq!(
            rstrip_xff(Bytes::from_static(&b"\xFE"[..])),
            Ok(Bytes::from_static(&b"\xFE"[..]))
        );
        assert_eq!(
            rstrip_xff(Bytes::from_static(&b"a\xFF"[..])),
            Ok(Bytes::from_static(&b"a"[..]))
        );
        assert_eq!(
            rstrip_xff(Bytes::from_static(&b"hello1"[..])),
            Ok(Bytes::from_static(&b"hello1"[..]))
        );
        assert_eq!(
            rstrip_xff(Bytes::from_static(&b"hello1\xFF"[..])),
            Ok(Bytes::from_static(&b"hello1"[..]))
        );
        assert_eq!(
            rstrip_xff(Bytes::from_static(&b"hello1\xFF\xFF"[..])),
            Ok(Bytes::from_static(&b"hello1"[..]))
        );
    }

    #[test]
    fn test_strinc() {
        assert_eq!(
            strinc(Bytes::new()),
            Err(FdbError::new(TUPLE_KEY_UTIL_STRINC_ERROR))
        );
        assert_eq!(
            strinc(Bytes::from_static(&b"\xFF"[..])),
            Err(FdbError::new(TUPLE_KEY_UTIL_STRINC_ERROR))
        );
        assert_eq!(
            strinc(Bytes::from_static(&b"\xFF\xFF"[..])),
            Err(FdbError::new(TUPLE_KEY_UTIL_STRINC_ERROR))
        );
        assert_eq!(
            strinc(Bytes::from_static(&b"\x00"[..])),
            Ok(Key::from(Bytes::from_static(&b"\x01"[..])))
        );
        assert_eq!(
            strinc(Bytes::from_static(&b"\xFE"[..])),
            Ok(Key::from(Bytes::from_static(&b"\xFF"[..])))
        );
        assert_eq!(
            strinc(Bytes::from_static(&b"a\xFF"[..])),
            Ok(Key::from(Bytes::from_static(&b"b"[..])))
        );
        assert_eq!(
            strinc(Bytes::from_static(&b"hello1"[..])),
            Ok(Key::from(Bytes::from_static(&b"hello2"[..])))
        );
        assert_eq!(
            strinc(Bytes::from_static(&b"hello1\xFF"[..])),
            Ok(Key::from(Bytes::from_static(&b"hello2"[..])))
        );
        assert_eq!(
            strinc(Bytes::from_static(&b"hello1\xFF\xFF"[..])),
            Ok(Key::from(Bytes::from_static(&b"hello2"[..])))
        );
    }
}
