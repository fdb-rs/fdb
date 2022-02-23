use bytes::{Buf, BufMut};
use bytes::{Bytes, BytesMut};

// As mentioned here [1], depending on the context, there are two
// concepts of versionstamp.
//
// At the `fdb_c` client level, the "versionstamp" is 10 bytes,
// consisting of the transaction's commit version (8 bytes) and
// transaction batch order (2 bytes).
//
// In the context of the Tuple layer, the "versionstamp" is 12
// bytes. The user can manually add 2 additional bytes to provide
// application level ordering.
//
// `VERSIONSTAMP_TR_VERSION_LEN` below is the `fdb_c` client level
// versionstamp. When `VERSIONSTAMP_TR_VERSION` is all `\xFF`, it
// means that versionstamp is "incomplete".
//
// [1]: https://apple.github.io/foundationdb/data-modeling.html#versionstamps
const VERSIONSTAMP_TR_VERSION_LEN: usize = 10;
const VERSIONSTAMP_USER_VERSION_LEN: usize = 2;

/// Used to represent values written by versionstamp operations with a
/// [`Tuple`].
///
/// [`Versionstamp`] contains twelve bytes. The first ten bytes are
/// the "transaction" version, and they are usually assigned by the
/// database in such a way that all transactions receive a different
/// version that is consistent with a serialization order of the
/// transactions within the database (One can use the
/// [`get_versionstamp`] method to retrieve this version from a
/// [`Transaction`]). This also implies that the transaction version
/// of newly committed transactions will be monotonically increasing
/// over time. The final two bytes are the "user" version and should
/// be set by the client. This allows the user to use this type to
/// impose a total order of items across multiple transactions in the
/// database in a consistent and conflict-free way.
///
/// All [`Versionstamp`]s can exist in one of two states: "incomplete"
/// and "complete". An "incomplete" [`Versionstamp`] is a [`Versionstamp`]
/// that has not been initialized with a meaningful transaction
/// version. For example, this might be used with a [`Versionstamp`] that
/// one wants to fill in with the current transaction's version
/// information. A "complete" [`Versionstamp`], in contradistinction, is
/// one that *has* been assigned a meaningful transaction version. This
/// is usually the case if one is reading back a Versionstamp from the
/// database.
///
/// Example usage might be to do something like the following:
///
/// ```ignore
/// let tr_version = fdb_database
///     .run(|tr| async move {
///         let t = {
///             let mut tup = Tuple::new();
///             tup.add_string(String::from("prefix"));
///             tup.add_versionstamp(Versionstamp::incomplete(0));
///             tup
///         };
///
///         unsafe {
///             tr.mutate(
///                 MutationType::SetVersionstampedKey,
///                 t.pack_with_versionstamp(Bytes::new())?,
///                 Bytes::new(),
///             );
///         }
///
///         Ok(unsafe { tr.get_versionstamp() })
///     })
///     .await?
///     .get()
///     .await?;
///
/// let vs = fdb_database
///     .run(|tr| async move {
///         let subspace = Subspace::new(Bytes::new()).subspace(&{
///             let mut tup = Tuple::new();
///             tup.add_string("prefix".to_string());
///             tup
///         });
///
///         let subspace_range = subspace.range(&Tuple::new());
///
///         let key = tr
///             .get_range(
///                 KeySelector::first_greater_or_equal(subspace_range.begin().clone()),
///                 KeySelector::first_greater_or_equal(subspace_range.end().clone()),
///                 RangeOptions::default(),
///             )
///             .take(1)
///             .next()
///             .await
///             .unwrap()?
///             .get_key()
///             .clone();
///
///         Ok(subspace
///             .unpack(&key.into())?
///             .get_versionstamp_ref(0)?
///             .clone())
///     })
///     .await?;
///
/// assert_eq!(vs, Versionstamp::complete(tr_version, 0));
/// ```
///
/// Here, an incomplete [`Versionstamp`] is packed and written to the
/// database with [`SetVersionstampedKey`] mutation type.
///
/// After committing, we then attempt to read back the same key that
/// we just wrote. Then we verify the invariant that the deserialized
/// [`Versionstamp`] is the same as a complete [`Versionstamp`] value
/// created from the first transaction's version information.
///
/// [`Tuple`]: crate::tuple::Tuple
/// [`get_versionstamp`]: crate::transaction::Transaction::get_versionstamp
/// [`Transaction`]: crate::transaction::Transaction
/// [`SetVersionstampedKey`]: crate::transaction::MutationType::SetVersionstampedKey
#[derive(Clone, Ord, Eq, PartialOrd, PartialEq, Debug)]
pub struct Versionstamp {
    complete: bool,
    tr_version: Bytes,
    user_version: u16,
}

impl Versionstamp {
    /// Creates a complete [`Versionstamp`] instance with the given
    /// transaction and user versions.
    ///
    /// # Panic
    ///
    /// Panics if the length of the transaction version is incorrect.
    pub fn complete(tr_version: Bytes, user_version: u16) -> Versionstamp {
        if tr_version.len() != VERSIONSTAMP_TR_VERSION_LEN {
            panic!("Global version has invalid length {}", tr_version.len());
        }

        let complete = true;

        Versionstamp {
            complete,
            tr_version,
            user_version,
        }
    }

    /// Creates a value of [`Versionstamp`] type based on the given
    /// byte array.
    ///
    /// # Panic
    ///
    /// Panics if the length of the byte array is incorrect.
    pub fn from_bytes(version_bytes: Bytes) -> Versionstamp {
        if version_bytes.len() != VERSIONSTAMP_TR_VERSION_LEN + VERSIONSTAMP_USER_VERSION_LEN {
            panic!(
                "Versionstamp bytes must have length {}",
                VERSIONSTAMP_TR_VERSION_LEN + VERSIONSTAMP_USER_VERSION_LEN
            );
        }

        // If we find any of the bytes to be not `0xFF`, then it means
        // that versionstamp is in complete state.
        let mut complete = false;
        version_bytes[0..VERSIONSTAMP_TR_VERSION_LEN]
            .iter()
            .for_each(|x| {
                if *x != 0xFF {
                    complete = true;
                }
            });

        let tr_version = version_bytes.slice(0..VERSIONSTAMP_TR_VERSION_LEN);
        let user_version = version_bytes.slice(VERSIONSTAMP_TR_VERSION_LEN..).get_u16();

        Versionstamp {
            complete,
            tr_version,
            user_version,
        }
    }

    /// Creates an incomplete [`Versionstamp`] instance with the given
    /// user version.
    pub fn incomplete(user_version: u16) -> Versionstamp {
        let complete = false;
        let tr_version = Bytes::from_static(&b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..]);

        Versionstamp {
            complete,
            tr_version,
            user_version,
        }
    }

    /// Retrieve a byte representation of this [`Versionstamp`].
    pub fn get_bytes(&self) -> Bytes {
        let mut buf =
            BytesMut::with_capacity(VERSIONSTAMP_TR_VERSION_LEN + VERSIONSTAMP_USER_VERSION_LEN);
        buf.put(self.tr_version.clone());

        buf.put_u16(self.user_version);
        buf.into()
    }

    /// Retrieve the portion of this [`Versionstamp`] that is set by
    /// the database.
    pub fn get_transaction_version(&self) -> Bytes {
        self.tr_version.clone()
    }

    /// Retrieve the portion of this [`Versionstamp`] that is set by
    /// the user.
    pub fn get_user_version(&self) -> u16 {
        self.user_version
    }

    /// Whether this [`Versionstamp`]'s transaction version is
    /// meaningful.
    pub fn is_complete(&self) -> bool {
        self.complete
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::Versionstamp;

    #[test]
    fn complete() {
        assert!(std::panic::catch_unwind(|| {
            Versionstamp::complete(Bytes::from_static(&b"invalid"[..]), 0)
        })
        .is_err());

        assert_eq!(
            Versionstamp::complete(
                Bytes::from_static(&b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"[..]),
                0
            ),
            Versionstamp {
                complete: true,
                tr_version: Bytes::from_static(&b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"[..]),
                user_version: 0
            }
        );
    }

    #[test]
    fn from_bytes() {
        assert!(std::panic::catch_unwind(|| {
            Versionstamp::from_bytes(Bytes::from_static(&b"invalid"[..]))
        })
        .is_err());

        assert_eq!(
            Versionstamp::from_bytes(Bytes::from_static(
                &b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x02\x91"[..]
            )),
            Versionstamp::incomplete(657)
        );

        assert_eq!(
            Versionstamp::from_bytes(Bytes::from_static(
                &b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x02\x91"[..]
            )),
            Versionstamp::complete(
                Bytes::from_static(&b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A"[..]),
                657
            )
        );
    }

    #[test]
    fn incomplete() {
        assert_eq!(
            Versionstamp::incomplete(657),
            Versionstamp {
                complete: false,
                tr_version: Bytes::from_static(&b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..]),
                user_version: 657
            }
        );
    }

    #[test]
    fn get_bytes() {
        assert_eq!(
            (Versionstamp::complete(
                Bytes::from_static(&b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A"[..]),
                657
            ))
            .get_bytes(),
            Bytes::from_static(&b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x02\x91"[..])
        );
        assert_eq!(
            (Versionstamp::incomplete(657)).get_bytes(),
            Bytes::from_static(&b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x02\x91"[..])
        );
    }

    #[test]
    fn get_transaction_version() {
        assert_eq!(
            (Versionstamp::complete(
                Bytes::from_static(&b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A"[..]),
                657
            ))
            .get_transaction_version(),
            Bytes::from_static(&b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A"[..])
        );

        assert_eq!(
            (Versionstamp::incomplete(657)).get_transaction_version(),
            Bytes::from_static(&b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..])
        );
    }

    #[test]
    fn get_user_version() {
        assert_eq!(
            (Versionstamp::complete(
                Bytes::from_static(&b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A"[..]),
                657
            ))
            .get_user_version(),
            657
        );

        assert_eq!((Versionstamp::incomplete(657)).get_user_version(), 657);
    }

    #[test]
    fn is_complete() {
        assert!((Versionstamp::complete(
            Bytes::from_static(&b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A"[..]),
            657
        ))
        .is_complete());

        assert!(!(Versionstamp::incomplete(657)).is_complete());
    }
}
