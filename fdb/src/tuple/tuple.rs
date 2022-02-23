use bytes::{BufMut, Bytes, BytesMut};
use num_bigint::BigInt;
use num_traits::sign::Signed;
use uuid::Uuid;

use std::cmp::Ordering;
use std::convert::TryFrom;
use std::convert::TryInto;

use crate::error::{FdbError, FdbResult, TUPLE_GET, TUPLE_PACK_WITH_VERSIONSTAMP_NOT_FOUND};
use crate::range::Range;
use crate::tuple::{
    element::{self, TupleValue},
    Versionstamp,
};

/// Represents a set of elements that make up a sortable, typed key.
///
/// [`Tuple`] is comparable with other [`Tuple`]s and will sort in
/// Rust in the same order in which they would sort in FDB. [`Tuple`]s
/// sort first by the first element, then by the second, etc., This
/// make tuple layer ideal for building a variety of higher-level data
/// models.
///
/// For general guidance on tuple usage, see [this] link.
///
/// [`Tuple`] can contain [`null`], [`Bytes`], [`String`], another
/// [`Tuple`], [`BigInt`], [`i64`], [`i32`], [`i16`], [`i8`], [`f32`],
/// [`f64`], [`bool`], [`Uuid`], [`Versionstamp`] values.
///
/// [layer]: https://github.com/apple/foundationdb/blob/6.3.0/design/tuple.md
/// [this]: https://apple.github.io/foundationdb/data-modeling.html#tuples
/// [`null`]: https://github.com/apple/foundationdb/blob/release-6.3/design/tuple.md#null-value
///
// NOTE: Unlike the Java API, we do not implement `Iterator` trait, as
//       that would mean we will have to expose `TupleValue` type to
//       the client. Instead we provide `size()` method and let the
//       client call appropriate `get_<type>(...)` methods.
#[derive(Debug, Clone)]
pub struct Tuple {
    elements: Vec<TupleValue>,
    // This is `true` *only* when `Tuple` contains a `Versionstamp`
    // *and* it that `Versionstamp` is incomplete.
    //
    // This is `false` (default) when `Tuple` does not contain a
    // `Versionstamp` or if it contains a `Versionstamp` that is
    // complete.
    //
    // Basically adding complete `Versionstamp` won't change this
    // value to `true`. It is only when incomplete `Versionstamp` is
    // added, that this value changes to `true`.
    has_incomplete_versionstamp: bool,
}

impl Tuple {
    /// Create a new empty [`Tuple`].
    pub fn new() -> Tuple {
        Tuple {
            elements: Vec::new(),
            has_incomplete_versionstamp: false,
        }
    }

    /// Construct a new [`Tuple`] with elements decoded from a supplied [`Bytes`].
    pub fn from_bytes(b: impl Into<Bytes>) -> FdbResult<Tuple> {
        element::from_bytes(b.into())
    }

    /// Append FDB Tuple [`null`] value to [`Tuple`].
    ///
    /// [`null`]: https://github.com/apple/foundationdb/blob/release-6.3/design/tuple.md#null-value
    pub fn add_null(&mut self) {
        self.elements.push(TupleValue::NullValue);
    }

    /// Append [`Bytes`] value to the [`Tuple`].
    pub fn add_bytes(&mut self, b: Bytes) {
        self.elements.push(TupleValue::ByteString(b));
    }

    /// Append [`String`] value to the [`Tuple`].
    pub fn add_string(&mut self, s: String) {
        self.elements.push(TupleValue::UnicodeString(s));
    }

    /// Append [`Tuple`] value to the [`Tuple`]
    pub fn add_tuple(&mut self, t: Tuple) {
        self.has_incomplete_versionstamp =
            self.has_incomplete_versionstamp || t.has_incomplete_versionstamp();
        self.elements.push(TupleValue::NestedTuple(t));
    }

    /// Append [`BigInt`] value to the [`Tuple`]
    ///
    /// # Panic
    ///
    /// Panics if the [`Bytes`] encoded length of the [`BigInt`] is
    /// greater than 255.
    pub fn add_bigint(&mut self, i: BigInt) {
        let _ = i64::try_from(i.clone())
            .map(|x| self.add_i64(x))
            .map_err(|_| {
                if i.is_negative() {
		    // We are making the negative number positive for
		    // storing it in
		    // `TupleValue::NegativeArbitraryPrecisionInteger`.
                    if ((BigInt::parse_bytes(b"-18446744073709551615", 10).unwrap())
                        ..=(BigInt::parse_bytes(b"-9223372036854775809", 10).unwrap()))
                        .contains(&i)
                    {
                        self.elements.push(TupleValue::NegInt8(
                            // Safe to unwrap here because we are
                            // checking the range above.
                            u64::try_from(i * -1).unwrap(),
                        ));
                    } else {
                        let b: BigInt = i * -1;
                        let (_, bigint_vec_u8) = b.to_bytes_be();

                        if Bytes::from(bigint_vec_u8).len() > 255 {
			    panic!("Byte encoded length of BigInt *must* be less than or equal to 255.");
                        }
                        self.elements
                            .push(TupleValue::NegativeArbitraryPrecisionInteger(b));
                    }
		}
		else if ((BigInt::parse_bytes(b"9223372036854775808", 10).unwrap())
                        ..=(BigInt::parse_bytes(b"18446744073709551615", 10).unwrap()))
                        .contains(&i)
                {
                    self.elements.push(TupleValue::PosInt8(
                        // Safe to unwrap here because we are
                        // checking the range above.
                        u64::try_from(i).unwrap(),
                    ));
                } else {
                    let b: BigInt = i;
                    let (_, bigint_vec_u8) = b.to_bytes_be();

                    if Bytes::from(bigint_vec_u8).len() > 255 {
			panic!("Byte encoded length of BigInt *must* be less than or equal to 255.");
                    }
                    self.elements
                        .push(TupleValue::PositiveArbitraryPrecisionInteger(b));
                }
            });
    }

    /// Append [`i64`] value to the [`Tuple`]
    pub fn add_i64(&mut self, i: i64) {
        let _ = i32::try_from(i).map(|x| self.add_i32(x)).map_err(|_| {
            if i.is_negative() {
                match i {
                    i64::MIN..=-72057594037927936 => {
                        self.elements.push(TupleValue::NegInt8(i.unsigned_abs()))
                    }
                    -72057594037927935..=-281474976710656 => {
                        self.elements.push(TupleValue::NegInt7(i.unsigned_abs()))
                    }
                    -281474976710655..=-1099511627776 => {
                        self.elements.push(TupleValue::NegInt6(i.unsigned_abs()))
                    }
                    -1099511627775..=-4294967296 => {
                        self.elements.push(TupleValue::NegInt5(i.unsigned_abs()))
                    }
                    _ => self.elements.push(TupleValue::NegInt4(
                        // Safe to unwrap here because we are checking
                        // the range in `try_from` and
                        // `i64::MIN..=-72057594037927936`,
                        // `-72057594037927935..=-281474976710656`,
                        // `-281474976710655..=-1099511627776`,
                        // `-1099511627775..=-4294967296`.
                        i.unsigned_abs().try_into().unwrap(),
                    )),
                }
            } else {
                match i {
                    2147483648..=4294967295 => self.elements.push(TupleValue::PosInt4(
                        // Safe to unwrap here because we are checking
                        // the range in `try_from` and
                        // `4294967296..=1099511627775`,
                        // `1099511627776..=281474976710655`,
                        // `281474976710656..=72057594037927935`,
                        // `>72057594037927935` using `_` below.
                        i.unsigned_abs().try_into().unwrap(),
                    )),
                    4294967296..=1099511627775 => {
                        self.elements.push(TupleValue::PosInt5(i.unsigned_abs()))
                    }
                    1099511627776..=281474976710655 => {
                        self.elements.push(TupleValue::PosInt6(i.unsigned_abs()))
                    }
                    281474976710656..=72057594037927935 => {
                        self.elements.push(TupleValue::PosInt7(i.unsigned_abs()))
                    }
                    _ => self.elements.push(TupleValue::PosInt8(i.unsigned_abs())),
                }
            }
        });
    }

    /// Append [`i32`] value to the [`Tuple`]
    pub fn add_i32(&mut self, i: i32) {
        let _ = i16::try_from(i).map(|x| self.add_i16(x)).map_err(|_| {
            if i.is_negative() {
                match i {
                    i32::MIN..=-16777216 => {
                        self.elements.push(TupleValue::NegInt4(i.unsigned_abs()))
                    }
                    -16777215..=-65536 => self.elements.push(TupleValue::NegInt3(i.unsigned_abs())),
                    _ => self.elements.push(TupleValue::NegInt2(
                        // Safe to unwrap here because we are checking
                        // the range in `try_from` and
                        // `i32::MIN..=-16777216`,
                        // `-16777215..=-65536`
                        i.unsigned_abs().try_into().unwrap(),
                    )),
                }
            } else {
                match i {
                    32768..=65535 => self.elements.push(TupleValue::PosInt2(
                        // Safe to unwrap here because we are checking
                        // the range in `try_from` and
                        // `65536..=16777215`, `>16777215` using `_`
                        // below.
                        i.unsigned_abs().try_into().unwrap(),
                    )),
                    65536..=16777215 => self.elements.push(TupleValue::PosInt3(i.unsigned_abs())),
                    _ => self.elements.push(TupleValue::PosInt4(i.unsigned_abs())),
                }
            }
        });
    }

    /// Append [`i16`] value to the [`Tuple`].
    pub fn add_i16(&mut self, i: i16) {
        let _ = i8::try_from(i).map(|x| self.add_i8(x)).map_err(|_| {
            if i.is_negative() {
                match i {
                    i16::MIN..=-256 => self.elements.push(TupleValue::NegInt2(i.unsigned_abs())),
                    _ => self.elements.push(TupleValue::NegInt1(
                        // Safe to unwrap here because we are
                        // checking the range in `try_from` and
                        // `i16::MIN..=-256`.
                        i.unsigned_abs().try_into().unwrap(),
                    )),
                }
            } else {
                match i {
                    128..=255 => self.elements.push(TupleValue::PosInt1(
                        // Safe to unwrap here because we are checking
                        // the range in `try_from` and `>255` using
                        // `_` below.
                        i.unsigned_abs().try_into().unwrap(),
                    )),
                    _ => self.elements.push(TupleValue::PosInt2(i.unsigned_abs())),
                }
            }
        });
    }

    /// Append [`i8`] value to the [`Tuple`].
    pub fn add_i8(&mut self, i: i8) {
        match i {
            i8::MIN..=-1 => self.elements.push(TupleValue::NegInt1(i.unsigned_abs())),
            0 => self.elements.push(TupleValue::IntZero),
            1..=i8::MAX => self.elements.push(TupleValue::PosInt1(i.unsigned_abs())),
        }
    }

    /// Append [`f32`] value to the [`Tuple`].
    ///
    /// # Note
    ///
    /// The [`f32`] value is encoded using type code [`0x20`], without any conversion.
    ///
    /// [`0x20`]: https://github.com/apple/foundationdb/blob/release-6.3/design/tuple.md#ieee-binary-floating-point
    pub fn add_f32(&mut self, f: f32) {
        self.elements
            .push(TupleValue::IeeeBinaryFloatingPointFloat(f));
    }

    /// Append [`f64`] value to the [`Tuple`].
    ///
    /// # Note
    ///
    /// The [`f64`] value is encoded using type code [`0x21`], without any conversion.
    ///
    /// [`0x21`]: https://github.com/apple/foundationdb/blob/release-6.3/design/tuple.md#ieee-binary-floating-point
    pub fn add_f64(&mut self, f: f64) {
        self.elements
            .push(TupleValue::IeeeBinaryFloatingPointDouble(f));
    }

    /// Append [`bool`] value to the [`Tuple`].
    pub fn add_bool(&mut self, b: bool) {
        if b {
            self.elements.push(TupleValue::TrueValue);
        } else {
            self.elements.push(TupleValue::FalseValue);
        }
    }

    /// Append [`Uuid`] value to the [`Tuple`].
    pub fn add_uuid(&mut self, u: Uuid) {
        self.elements.push(TupleValue::Rfc4122Uuid(u));
    }

    /// Append [`Versionstamp`] value to the [`Tuple`]    
    pub fn add_versionstamp(&mut self, v: Versionstamp) {
        self.has_incomplete_versionstamp = self.has_incomplete_versionstamp || (!v.is_complete());
        self.elements.push(TupleValue::Versionstamp96Bit(v));
    }

    /// Append elements of [`Tuple`] `t` to [`Tuple`] `Self`
    pub fn append(&mut self, mut t: Tuple) {
        self.has_incomplete_versionstamp =
            self.has_incomplete_versionstamp || t.has_incomplete_versionstamp();

        self.elements.append(&mut t.elements);
    }

    /// Determines if there is a [`Versionstamp`] included in this
    /// [`Tuple`] that has not had its transaction version set.
    pub fn has_incomplete_versionstamp(&self) -> bool {
        self.has_incomplete_versionstamp
    }

    /// Gets an indexed item as FDB Tuple [`null`] value.
    ///
    /// [`null`]: https://github.com/apple/foundationdb/blob/release-6.3/design/tuple.md#null-value
    pub fn get_null(&self, index: usize) -> FdbResult<()> {
        self.elements
            .get(index)
            .and_then(|x| match x {
                &TupleValue::NullValue => Some(()),
                _ => None,
            })
            .ok_or_else(Tuple::tuple_get_error)
    }

    /// Gets an indexed item as [`Bytes`] ref.
    pub fn get_bytes_ref(&self, index: usize) -> FdbResult<&Bytes> {
        self.elements
            .get(index)
            .and_then(|x| match x {
                &TupleValue::ByteString(ref b) => Some(b),
                _ => None,
            })
            .ok_or_else(Tuple::tuple_get_error)
    }

    /// Gets an indexed item as [`String`] ref.
    pub fn get_string_ref(&self, index: usize) -> FdbResult<&String> {
        self.elements
            .get(index)
            .and_then(|x| match x {
                &TupleValue::UnicodeString(ref s) => Some(s),
                _ => None,
            })
            .ok_or_else(Tuple::tuple_get_error)
    }

    /// Gets an indexed item as [`Tuple`] ref.
    pub fn get_tuple_ref(&self, index: usize) -> FdbResult<&Tuple> {
        self.elements
            .get(index)
            .and_then(|x| match x {
                &TupleValue::NestedTuple(ref t) => Some(t),
                _ => None,
            })
            .ok_or_else(Tuple::tuple_get_error)
    }

    /// Gets an indexed item as [`BigInt`].
    pub fn get_bigint(&self, index: usize) -> FdbResult<BigInt> {
        self.get_i64(index).map(|x| x.into()).or_else(|_| {
            self.elements
                .get(index)
                .and_then(|x| match *x {
                    TupleValue::NegativeArbitraryPrecisionInteger(ref i) => Some(i.clone() * -1),
                    TupleValue::NegInt8(ref i)
                        if (9223372036854775809..=18446744073709551615).contains(i) =>
                    {
                        Some(Into::<BigInt>::into(*i) * -1)
                    }
                    TupleValue::PosInt8(ref i)
                        if (9223372036854775808..=18446744073709551615).contains(i) =>
                    {
                        Some((*i).into())
                    }
                    TupleValue::PositiveArbitraryPrecisionInteger(ref i) => Some(i.clone()),
                    _ => None,
                })
                .ok_or_else(Tuple::tuple_get_error)
        })
    }

    /// Gets an indexed item as [`i64`].
    pub fn get_i64(&self, index: usize) -> FdbResult<i64> {
        self.get_i32(index).map(|x| x.into()).or_else(|_| {
            self.elements
                .get(index)
                .and_then(|x| match *x {
                    TupleValue::NegInt8(ref i)
                        if (72057594037927936..=9223372036854775808).contains(i) =>
                    {
                        Some(
                            // Safe to unwrap here because we are
                            // checking for the range.
                            i64::try_from(-Into::<i128>::into(*i)).unwrap(),
                        )
                    }
                    TupleValue::NegInt7(ref i)
                        if (281474976710656..=72057594037927935).contains(i) =>
                    {
                        Some(
                            // Safe to unwrap here because we are
                            // checking for the range.
                            i64::try_from(-Into::<i128>::into(*i)).unwrap(),
                        )
                    }
                    TupleValue::NegInt6(ref i) if (1099511627776..=281474976710655).contains(i) => {
                        Some(
                            // Safe to unwrap here because we are
                            // checking for the range.
                            i64::try_from(-Into::<i128>::into(*i)).unwrap(),
                        )
                    }
                    TupleValue::NegInt5(ref i) if (4294967296..=1099511627775).contains(i) => {
                        Some(
                            // Safe to unwrap here because we are
                            // checking for the range.
                            i64::try_from(-Into::<i128>::into(*i)).unwrap(),
                        )
                    }

                    TupleValue::NegInt4(ref i) if (2147483649..=4294967295).contains(i) => {
                        Some(-Into::<i64>::into(*i))
                    }
                    TupleValue::PosInt4(ref i) if (2147483648..=4294967295).contains(i) => {
                        Some((*i).into())
                    }
                    TupleValue::PosInt5(ref i) if (4294967296..=1099511627775).contains(i) => {
                        Some(
                            // Safe to unwrap here because we are
                            // checking for the range.
                            i64::try_from(*i).unwrap(),
                        )
                    }
                    TupleValue::PosInt6(ref i) if (1099511627776..=281474976710655).contains(i) => {
                        Some(
                            // Safe to unwrap here because we are
                            // checking for the range.
                            i64::try_from(*i).unwrap(),
                        )
                    }
                    TupleValue::PosInt7(ref i)
                        if (281474976710656..=72057594037927935).contains(i) =>
                    {
                        Some(
                            // Safe to unwrap here because we are
                            // checking for the range.
                            i64::try_from(*i).unwrap(),
                        )
                    }
                    TupleValue::PosInt8(ref i)
                        if (72057594037927936..=9223372036854775807).contains(i) =>
                    {
                        Some(
                            // Safe to unwrap here because we are
                            // checking for the range.
                            i64::try_from(*i).unwrap(),
                        )
                    }
                    _ => None,
                })
                .ok_or_else(Tuple::tuple_get_error)
        })
    }

    /// Gets an indexed item as [`i32`].
    pub fn get_i32(&self, index: usize) -> FdbResult<i32> {
        self.get_i16(index).map(|x| x.into()).or_else(|_| {
            self.elements
                .get(index)
                .and_then(|x| match *x {
                    TupleValue::NegInt4(ref i) if (16777216..=2147483648).contains(i) => Some(
                        // Safe to unwrap here because we are
                        // checking for the range.
                        i32::try_from(-Into::<i64>::into(*i)).unwrap(),
                    ),
                    TupleValue::NegInt3(ref i) if (65536..=16777215).contains(i) => Some(
                        // Safe to unwrap here because we are
                        // checking for the range.
                        i32::try_from(-Into::<i64>::into(*i)).unwrap(),
                    ),
                    TupleValue::NegInt2(ref i) if (32769..=65535).contains(i) => {
                        Some(-Into::<i32>::into(*i))
                    }
                    TupleValue::PosInt2(ref i) if (32768..=65535).contains(i) => Some((*i).into()),
                    TupleValue::PosInt3(ref i) if (65536..=16777215).contains(i) => Some(
                        // Safe to unwrap here because we are checking
                        // for the range.
                        i32::try_from(*i).unwrap(),
                    ),
                    TupleValue::PosInt4(ref i) if (16777216..=2147483647).contains(i) => Some(
                        // Safe to unwrap here because we are checking
                        // for the range.
                        i32::try_from(*i).unwrap(),
                    ),
                    _ => None,
                })
                .ok_or_else(Tuple::tuple_get_error)
        })
    }

    /// Gets an indexed item as [`i16`].
    pub fn get_i16(&self, index: usize) -> FdbResult<i16> {
        self.get_i8(index).map(|x| x.into()).or_else(|_| {
            self.elements
                .get(index)
                .and_then(|x| match *x {
                    TupleValue::NegInt2(ref i) if (256..=32768).contains(i) => Some(
                        // Safe to unwrap here because we are
                        // checking for the range.
                        i16::try_from(-Into::<i32>::into(*i)).unwrap(),
                    ),
                    TupleValue::NegInt1(ref i) if (129..=255).contains(i) => {
                        Some(-Into::<i16>::into(*i))
                    }
                    TupleValue::PosInt1(ref i) if (128..=255).contains(i) => Some((*i).into()),
                    TupleValue::PosInt2(ref i) if (256..=32767).contains(i) => Some(
                        // Safe to unwrap here because we are checking
                        // for the range.
                        i16::try_from(*i).unwrap(),
                    ),
                    _ => None,
                })
                .ok_or_else(Tuple::tuple_get_error)
        })
    }

    /// Gets an indexed item as [`i8`].
    pub fn get_i8(&self, index: usize) -> FdbResult<i8> {
        self.elements
            .get(index)
            .and_then(|x| match *x {
                TupleValue::NegInt1(i) if i <= 128 => {
                    Some(
                        // Safe to unwrap here because we are checking
                        // for the range.
                        i8::try_from(-Into::<i16>::into(i)).unwrap(),
                    )
                }
                TupleValue::IntZero => Some(0),
                TupleValue::PosInt1(i) if i <= 127 => Some(
                    // Safe to unwrap here because we are checking for
                    // the range.
                    i8::try_from(i).unwrap(),
                ),
                _ => None,
            })
            .ok_or_else(Tuple::tuple_get_error)
    }

    /// Gets an indexed item as [`f32`].
    pub fn get_f32(&self, index: usize) -> FdbResult<f32> {
        self.elements
            .get(index)
            .and_then(|x| match x {
                &TupleValue::IeeeBinaryFloatingPointFloat(f) => Some(f),
                _ => None,
            })
            .ok_or_else(Tuple::tuple_get_error)
    }

    /// Gets an indexed item as [`f64`].
    pub fn get_f64(&self, index: usize) -> FdbResult<f64> {
        self.elements
            .get(index)
            .and_then(|x| match x {
                &TupleValue::IeeeBinaryFloatingPointDouble(f) => Some(f),
                _ => None,
            })
            .ok_or_else(Tuple::tuple_get_error)
    }

    /// Gets an indexed item as [`bool`].
    pub fn get_bool(&self, index: usize) -> FdbResult<bool> {
        self.elements
            .get(index)
            .and_then(|x| match *x {
                TupleValue::FalseValue => Some(false),
                TupleValue::TrueValue => Some(true),
                _ => None,
            })
            .ok_or_else(Tuple::tuple_get_error)
    }

    /// Gets an indexed item as [`Uuid`] ref.
    pub fn get_uuid_ref(&self, index: usize) -> FdbResult<&Uuid> {
        self.elements
            .get(index)
            .and_then(|x| match x {
                &TupleValue::Rfc4122Uuid(ref u) => Some(u),
                _ => None,
            })
            .ok_or_else(Tuple::tuple_get_error)
    }

    /// Gets an indexed item as [`Versionstamp`] ref.
    pub fn get_versionstamp_ref(&self, index: usize) -> FdbResult<&Versionstamp> {
        self.elements
            .get(index)
            .and_then(|x| match x {
                &TupleValue::Versionstamp96Bit(ref v) => Some(v),
                _ => None,
            })
            .ok_or_else(Tuple::tuple_get_error)
    }

    /// Determine if this [`Tuple`] contains no elements.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Gets the number of elements in this [`Tuple`].
    pub fn size(&self) -> usize {
        self.elements.len()
    }

    /// Get an encoded representation of this [`Tuple`].
    pub fn pack(&self) -> Bytes {
        element::to_bytes(self.clone())
    }

    /// Get an encoded representation of this [`Tuple`] for use with
    /// [`SetVersionstampedKey`].
    ///
    /// # Panic
    ///
    /// The index where incomplete versionstamp is located is a 32-bit
    /// little-endian integer. If the generated index overflows
    /// [`u32`], then this function panics.
    ///
    /// [`SetVersionstampedKey`]: crate::transaction::MutationType::SetVersionstampedKey
    pub fn pack_with_versionstamp(&self, prefix: Bytes) -> FdbResult<Bytes> {
        if self.has_incomplete_versionstamp() {
            element::find_incomplete_versionstamp(self.clone()).map(|x| {
                let index = TryInto::<u32>::try_into(x + prefix.len()).unwrap();

                let mut res = BytesMut::new();

                res.put(prefix);
                res.put(self.pack());
                res.put_u32_le(index);

                res.into()
            })
        } else {
            Err(FdbError::new(TUPLE_PACK_WITH_VERSIONSTAMP_NOT_FOUND))
        }
    }

    /// Returns a range representing all keys that encode [`Tuple`]s
    /// strictly starting with this [`Tuple`].
    ///
    /// # Panic
    ///
    /// Panics if the tuple contains an incomplete [`Versionstamp`].
    pub fn range(&self, prefix: Bytes) -> Range {
        if self.has_incomplete_versionstamp() {
            panic!("Cannot create Range value as tuple contains an incomplete versionstamp");
        }

        let begin = {
            let mut x = BytesMut::new();
            x.put(prefix.clone());
            x.put(self.pack());
            x.put_u8(0x00);
            Into::<Bytes>::into(x)
        };

        let end = {
            let mut x = BytesMut::new();
            x.put(prefix);
            x.put(self.pack());
            x.put_u8(0xFF);
            Into::<Bytes>::into(x)
        };

        Range::new(begin, end)
    }

    pub(crate) fn from_elements(elements: Vec<TupleValue>) -> Tuple {
        let has_incomplete_versionstamp = (&elements).iter().fold(false, |acc, x| match *x {
            TupleValue::NestedTuple(ref t) => acc || t.has_incomplete_versionstamp(),
            TupleValue::Versionstamp96Bit(ref vs) => acc || (!vs.is_complete()),
            _ => acc,
        });

        Tuple {
            elements,
            has_incomplete_versionstamp,
        }
    }

    pub(crate) fn into_elements(self) -> Vec<TupleValue> {
        self.elements
    }

    fn tuple_get_error() -> FdbError {
        FdbError::new(TUPLE_GET)
    }
}

impl Default for Tuple {
    fn default() -> Tuple {
        Tuple::new()
    }
}

impl PartialEq for Tuple {
    fn eq(&self, other: &Self) -> bool {
        self.pack().eq(&other.pack())
    }
}

impl Eq for Tuple {}

impl PartialOrd for Tuple {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.pack().partial_cmp(&other.pack())
    }
}

impl Ord for Tuple {
    fn cmp(&self, other: &Self) -> Ordering {
        self.pack().cmp(&other.pack())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use impls::impls;
    use num_bigint::BigInt;
    use uuid::Uuid;

    use crate::error::{
        FdbError, TUPLE_FROM_BYTES, TUPLE_PACK_WITH_VERSIONSTAMP_MULTIPLE_FOUND,
        TUPLE_PACK_WITH_VERSIONSTAMP_NOT_FOUND,
    };
    use crate::range::Range;
    use crate::tuple::{element::TupleValue, Versionstamp};

    use super::Tuple;

    #[test]
    fn impls() {
        #[rustfmt::skip]
	assert!(impls!(
	    Tuple:
	        PartialEq<Tuple> &
                Eq &
                PartialOrd<Tuple> &
                Ord
	));
    }

    #[test]
    fn from_bytes() {
        // For additonal tests, see the tests for `parsers::tuple`
        // (`test_tuple`) in `element.rs`.
        assert_eq!(
            Tuple::from_bytes(Bytes::from_static(&b"\x00moredata"[..])),
            Err(FdbError::new(TUPLE_FROM_BYTES)),
        );
        assert_eq!(
            Tuple::from_bytes(Bytes::from_static(&b"no_tuple"[..])),
            Err(FdbError::new(TUPLE_FROM_BYTES)),
        );
        assert_eq!(
            Tuple::from_bytes(Bytes::from_static(&b"\x02hello\x00"[..])),
            Ok({
                let mut t = Tuple::new();
                t.add_string("hello".to_string());
                t
            })
        );
    }

    #[test]
    fn add_null() {
        let mut t = Tuple::new();

        t.add_null();

        assert_eq!(t.elements, vec![TupleValue::NullValue]);
    }

    #[test]
    fn add_bytes() {
        let mut t = Tuple::new();

        t.add_bytes(Bytes::from_static(&b"hello_world"[..]));

        assert_eq!(
            t.elements,
            vec![TupleValue::ByteString(Bytes::from_static(
                &b"hello_world"[..]
            ))]
        );
    }

    #[test]
    fn add_string() {
        let mut t = Tuple::new();

        t.add_string("hello world".to_string());

        assert_eq!(
            t.elements,
            vec![TupleValue::UnicodeString("hello world".to_string())]
        );
    }

    #[test]
    fn add_tuple() {
        let mut t = Tuple::new();

        t.add_bigint(BigInt::parse_bytes(b"0", 10).unwrap());
        t.add_tuple({
            let mut t1 = Tuple::new();
            t1.add_versionstamp(Versionstamp::incomplete(0));
            t1
        });

        assert!(t.has_incomplete_versionstamp());

        assert_eq!(
            t.elements,
            vec![
                TupleValue::IntZero,
                TupleValue::NestedTuple(Tuple::from_elements(vec![TupleValue::Versionstamp96Bit(
                    Versionstamp::incomplete(0)
                )])),
            ]
        );
    }

    #[test]
    fn add_bigint() {
        let mut t = Tuple::new();

        t.add_bigint(BigInt::parse_bytes(b"-18446744073709551616", 10).unwrap());
        t.add_bigint(BigInt::parse_bytes(b"-18446744073709551615", 10).unwrap());
        t.add_bigint(BigInt::parse_bytes(b"-9223372036854775809", 10).unwrap());
        t.add_bigint(BigInt::parse_bytes(b"-9223372036854775808", 10).unwrap()); // i64::MIN
        t.add_bigint(BigInt::parse_bytes(b"9223372036854775807", 10).unwrap()); // i64::MAX
        t.add_bigint(BigInt::parse_bytes(b"9223372036854775808", 10).unwrap());
        t.add_bigint(BigInt::parse_bytes(b"18446744073709551615", 10).unwrap());
        t.add_bigint(BigInt::parse_bytes(b"18446744073709551616", 10).unwrap());

        assert_eq!(
            t.elements,
            vec![
                TupleValue::NegativeArbitraryPrecisionInteger(
                    BigInt::parse_bytes(b"18446744073709551616", 10).unwrap()
                ),
                TupleValue::NegInt8(18446744073709551615),
                TupleValue::NegInt8(9223372036854775809),
                TupleValue::NegInt8(9223372036854775808),
                TupleValue::PosInt8(9223372036854775807),
                TupleValue::PosInt8(9223372036854775808),
                TupleValue::PosInt8(18446744073709551615),
                TupleValue::PositiveArbitraryPrecisionInteger(
                    BigInt::parse_bytes(b"18446744073709551616", 10).unwrap()
                )
            ]
        );
    }

    #[test]
    fn add_i64() {
        let mut t = Tuple::new();

        t.add_i64(i64::MIN);
        t.add_i64(-72057594037927936);
        t.add_i64(-72057594037927935);
        t.add_i64(-281474976710656);
        t.add_i64(-281474976710655);
        t.add_i64(-1099511627776);
        t.add_i64(-1099511627775);
        t.add_i64(-4294967296);
        t.add_i64(-4294967295);
        t.add_i64(-2147483649);
        t.add_i64(-2147483648); // i32::MIN
        t.add_i64(2147483647); // i32::MAX
        t.add_i64(2147483648);
        t.add_i64(4294967295);
        t.add_i64(4294967296);
        t.add_i64(1099511627775);
        t.add_i64(1099511627776);
        t.add_i64(281474976710655);
        t.add_i64(281474976710656);
        t.add_i64(72057594037927935);
        t.add_i64(72057594037927936);
        t.add_i64(i64::MAX);

        assert_eq!(
            t.elements,
            vec![
                TupleValue::NegInt8(9223372036854775808),
                TupleValue::NegInt8(72057594037927936),
                TupleValue::NegInt7(72057594037927935),
                TupleValue::NegInt7(281474976710656),
                TupleValue::NegInt6(281474976710655),
                TupleValue::NegInt6(1099511627776),
                TupleValue::NegInt5(1099511627775),
                TupleValue::NegInt5(4294967296),
                TupleValue::NegInt4(4294967295),
                TupleValue::NegInt4(2147483649),
                TupleValue::NegInt4(2147483648),
                TupleValue::PosInt4(2147483647),
                TupleValue::PosInt4(2147483648),
                TupleValue::PosInt4(4294967295),
                TupleValue::PosInt5(4294967296),
                TupleValue::PosInt5(1099511627775),
                TupleValue::PosInt6(1099511627776),
                TupleValue::PosInt6(281474976710655),
                TupleValue::PosInt7(281474976710656),
                TupleValue::PosInt7(72057594037927935),
                TupleValue::PosInt8(72057594037927936),
                TupleValue::PosInt8(9223372036854775807),
            ]
        );
    }

    #[test]
    fn add_i32() {
        let mut t = Tuple::new();

        t.add_i32(i32::MIN);
        t.add_i32(-16777216);
        t.add_i32(-16777215);
        t.add_i32(-65536);
        t.add_i32(-65535);
        t.add_i32(-32769);
        t.add_i32(-32768); // i16::MIN
        t.add_i32(32767); // i16::MAX
        t.add_i32(32768);
        t.add_i32(65535);
        t.add_i32(65536);
        t.add_i32(16777215);
        t.add_i32(16777216);
        t.add_i32(i32::MAX);

        assert_eq!(
            t.elements,
            vec![
                TupleValue::NegInt4(2147483648),
                TupleValue::NegInt4(16777216),
                TupleValue::NegInt3(16777215),
                TupleValue::NegInt3(65536),
                TupleValue::NegInt2(65535),
                TupleValue::NegInt2(32769),
                TupleValue::NegInt2(32768),
                TupleValue::PosInt2(32767),
                TupleValue::PosInt2(32768),
                TupleValue::PosInt2(65535),
                TupleValue::PosInt3(65536),
                TupleValue::PosInt3(16777215),
                TupleValue::PosInt4(16777216),
                TupleValue::PosInt4(2147483647),
            ]
        );
    }

    #[test]
    fn add_i16() {
        let mut t = Tuple::new();

        t.add_i16(i16::MIN);
        t.add_i16(-256);
        t.add_i16(-255);
        t.add_i16(-129);
        t.add_i16(-128); // i8::MIN
        t.add_i16(127); // i8::MAX
        t.add_i16(128);
        t.add_i16(255);
        t.add_i16(256);
        t.add_i16(i16::MAX);

        assert_eq!(
            t.elements,
            vec![
                TupleValue::NegInt2(32768),
                TupleValue::NegInt2(256),
                TupleValue::NegInt1(255),
                TupleValue::NegInt1(129),
                TupleValue::NegInt1(128),
                TupleValue::PosInt1(127),
                TupleValue::PosInt1(128),
                TupleValue::PosInt1(255),
                TupleValue::PosInt2(256),
                TupleValue::PosInt2(32767),
            ]
        );
    }

    #[test]
    fn add_i8() {
        let mut t = Tuple::new();

        t.add_i8(i8::MIN);
        t.add_i8(0);
        t.add_i8(i8::MAX);

        assert_eq!(
            t.elements,
            vec![
                TupleValue::NegInt1(128),
                TupleValue::IntZero,
                TupleValue::PosInt1(127),
            ]
        );
    }

    // `3.14` is copied from Java binding tests
    #[allow(clippy::approx_constant)]
    #[test]
    fn add_f32() {
        let mut t = Tuple::new();

        t.add_f32(3.14f32);

        assert_eq!(
            t.elements,
            vec![TupleValue::IeeeBinaryFloatingPointFloat(3.14f32)]
        );
    }

    // `3.14` is copied from Java binding tests
    #[allow(clippy::approx_constant)]
    #[test]
    fn add_f64() {
        let mut t = Tuple::new();

        t.add_f64(-3.14f64);

        assert_eq!(
            t.elements,
            vec![TupleValue::IeeeBinaryFloatingPointDouble(-3.14f64)]
        );
    }

    #[test]
    fn add_bool() {
        let mut t = Tuple::new();

        t.add_bool(true);
        assert_eq!(t.elements, vec![TupleValue::TrueValue]);

        t.add_bool(false);
        assert_eq!(
            t.elements,
            vec![TupleValue::TrueValue, TupleValue::FalseValue]
        );
    }

    #[test]
    fn add_uuid() {
        let mut t = Tuple::new();

        t.add_uuid(Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap());

        assert_eq!(
            t.elements,
            vec![TupleValue::Rfc4122Uuid(
                Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap()
            )]
        );
    }

    #[test]
    fn add_versionstamp() {
        let mut t = Tuple::new();

        t.add_versionstamp(Versionstamp::complete(
            Bytes::from_static(&b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A"[..]),
            657,
        ));

        assert!(!t.has_incomplete_versionstamp());

        t.add_versionstamp(Versionstamp::incomplete(0));

        assert!(t.has_incomplete_versionstamp());

        assert_eq!(
            t.elements,
            vec![
                TupleValue::Versionstamp96Bit(Versionstamp::complete(
                    Bytes::from_static(&b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A"[..]),
                    657,
                )),
                TupleValue::Versionstamp96Bit(Versionstamp::incomplete(0))
            ]
        );
    }

    #[test]
    fn append() {
        let mut t = Tuple::new();

        t.append({
            let mut t1 = Tuple::new();
            t1.add_null();
            t1
        });

        t.append({
            let mut t1 = Tuple::new();
            t1.add_versionstamp(Versionstamp::incomplete(0));
            t1
        });

        assert!(t.has_incomplete_versionstamp());

        assert_eq!(
            t.elements,
            vec![
                TupleValue::NullValue,
                TupleValue::Versionstamp96Bit(Versionstamp::incomplete(0))
            ]
        );
    }

    #[test]
    fn has_incomplete_versionstamp() {
        let mut t = Tuple::new();

        assert!(!t.has_incomplete_versionstamp());

        t.add_versionstamp(Versionstamp::incomplete(0));

        assert!(t.has_incomplete_versionstamp());
    }

    #[test]
    fn get_null() {
        let t = Tuple::new();

        assert_eq!(t.get_null(0).unwrap_err(), Tuple::tuple_get_error());

        let mut t = Tuple::new();
        t.add_bool(true);
        t.add_null();

        assert_eq!(t.get_null(0).unwrap_err(), Tuple::tuple_get_error());
        assert_eq!(t.get_null(1), Ok(()));
    }

    #[test]
    fn get_bytes_ref() {
        let t = Tuple::new();

        assert_eq!(t.get_bytes_ref(0).unwrap_err(), Tuple::tuple_get_error());

        let mut t = Tuple::new();
        t.add_null();
        t.add_bytes(Bytes::from_static(&b"hello_world"[..]));

        assert_eq!(t.get_bytes_ref(0).unwrap_err(), Tuple::tuple_get_error());
        assert_eq!(
            t.get_bytes_ref(1).unwrap(),
            &Bytes::from_static(&b"hello_world"[..])
        );
    }

    #[test]
    fn get_string_ref() {
        let t = Tuple::new();

        assert_eq!(t.get_string_ref(0).unwrap_err(), Tuple::tuple_get_error());

        let mut t = Tuple::new();
        t.add_null();
        t.add_string("hello world".to_string());

        assert_eq!(t.get_string_ref(0).unwrap_err(), Tuple::tuple_get_error());
        assert_eq!(t.get_string_ref(1).unwrap(), &"hello world".to_string());
    }

    #[test]
    fn get_tuple_ref() {
        let t = Tuple::new();

        assert_eq!(t.get_tuple_ref(0).unwrap_err(), Tuple::tuple_get_error());

        let mut t = Tuple::new();
        t.add_null();
        t.add_tuple({
            let mut t1 = Tuple::new();
            t1.add_versionstamp(Versionstamp::incomplete(0));
            t1
        });

        assert_eq!(t.get_tuple_ref(0).unwrap_err(), Tuple::tuple_get_error());
        assert_eq!(t.get_tuple_ref(1).unwrap(), &{
            let mut t1 = Tuple::new();
            t1.add_versionstamp(Versionstamp::incomplete(0));
            t1
        });
    }

    #[test]
    fn get_bigint() {
        let t = Tuple::new();

        assert_eq!(t.get_bigint(0).unwrap_err(), Tuple::tuple_get_error());

        let mut t = Tuple::new();
        t.add_null();
        t.add_bigint(BigInt::parse_bytes(b"-18446744073709551616", 10).unwrap());
        t.add_bigint(BigInt::parse_bytes(b"-18446744073709551615", 10).unwrap());
        t.add_bigint(BigInt::parse_bytes(b"-9223372036854775809", 10).unwrap());
        t.add_bigint(BigInt::parse_bytes(b"-9223372036854775808", 10).unwrap()); // i64::MIN
        t.add_bigint(BigInt::parse_bytes(b"9223372036854775807", 10).unwrap()); // i64::MAX
        t.add_bigint(BigInt::parse_bytes(b"9223372036854775808", 10).unwrap());
        t.add_bigint(BigInt::parse_bytes(b"18446744073709551615", 10).unwrap());
        t.add_bigint(BigInt::parse_bytes(b"18446744073709551616", 10).unwrap());

        assert_eq!(t.get_bigint(0).unwrap_err(), Tuple::tuple_get_error());
        assert_eq!(
            t.get_bigint(1).unwrap(),
            BigInt::parse_bytes(b"-18446744073709551616", 10).unwrap()
        );
        assert_eq!(
            t.get_bigint(2).unwrap(),
            BigInt::parse_bytes(b"-18446744073709551615", 10).unwrap()
        );
        assert_eq!(
            t.get_bigint(3).unwrap(),
            BigInt::parse_bytes(b"-9223372036854775809", 10).unwrap()
        );
        assert_eq!(
            t.get_bigint(4).unwrap(),
            BigInt::parse_bytes(b"-9223372036854775808", 10).unwrap()
        );
        assert_eq!(
            t.get_bigint(5).unwrap(),
            BigInt::parse_bytes(b"9223372036854775807", 10).unwrap()
        );
        assert_eq!(
            t.get_bigint(6).unwrap(),
            BigInt::parse_bytes(b"9223372036854775808", 10).unwrap()
        );
        assert_eq!(
            t.get_bigint(7).unwrap(),
            BigInt::parse_bytes(b"18446744073709551615", 10).unwrap()
        );
        assert_eq!(
            t.get_bigint(8).unwrap(),
            BigInt::parse_bytes(b"18446744073709551616", 10).unwrap()
        );
    }

    #[test]
    fn get_i64() {
        let t = Tuple::new();

        assert_eq!(t.get_i64(0).unwrap_err(), Tuple::tuple_get_error());

        let mut t = Tuple::new();
        t.add_null();
        t.add_bigint(BigInt::parse_bytes(b"-9223372036854775809", 10).unwrap());
        t.add_i64(i64::MIN);
        t.add_i64(-72057594037927936);
        t.add_i64(-72057594037927935);
        t.add_i64(-281474976710656);
        t.add_i64(-281474976710655);
        t.add_i64(-1099511627776);
        t.add_i64(-1099511627775);
        t.add_i64(-4294967296);
        t.add_i64(-4294967295);
        t.add_i64(-2147483649);
        t.add_i64(-2147483648); // i32::MIN
        t.add_i64(2147483647); // i32::MAX
        t.add_i64(2147483648);
        t.add_i64(4294967295);
        t.add_i64(4294967296);
        t.add_i64(1099511627775);
        t.add_i64(1099511627776);
        t.add_i64(281474976710655);
        t.add_i64(281474976710656);
        t.add_i64(72057594037927935);
        t.add_i64(72057594037927936);
        t.add_i64(i64::MAX);
        t.add_bigint(BigInt::parse_bytes(b"9223372036854775808", 10).unwrap());

        assert_eq!(t.get_i64(0).unwrap_err(), Tuple::tuple_get_error());
        assert_eq!(t.get_i64(1).unwrap_err(), Tuple::tuple_get_error());
        assert_eq!(t.get_i64(2).unwrap(), i64::MIN);
        assert_eq!(t.get_i64(3).unwrap(), -72057594037927936);
        assert_eq!(t.get_i64(4).unwrap(), -72057594037927935);
        assert_eq!(t.get_i64(5).unwrap(), -281474976710656);
        assert_eq!(t.get_i64(6).unwrap(), -281474976710655);
        assert_eq!(t.get_i64(7).unwrap(), -1099511627776);
        assert_eq!(t.get_i64(8).unwrap(), -1099511627775);
        assert_eq!(t.get_i64(9).unwrap(), -4294967296);
        assert_eq!(t.get_i64(10).unwrap(), -4294967295);
        assert_eq!(t.get_i64(11).unwrap(), -2147483649);
        assert_eq!(t.get_i64(12).unwrap(), -2147483648);
        assert_eq!(t.get_i64(13).unwrap(), 2147483647);
        assert_eq!(t.get_i64(14).unwrap(), 2147483648);
        assert_eq!(t.get_i64(15).unwrap(), 4294967295);
        assert_eq!(t.get_i64(16).unwrap(), 4294967296);
        assert_eq!(t.get_i64(17).unwrap(), 1099511627775);
        assert_eq!(t.get_i64(18).unwrap(), 1099511627776);
        assert_eq!(t.get_i64(19).unwrap(), 281474976710655);
        assert_eq!(t.get_i64(20).unwrap(), 281474976710656);
        assert_eq!(t.get_i64(21).unwrap(), 72057594037927935);
        assert_eq!(t.get_i64(22).unwrap(), 72057594037927936);
        assert_eq!(t.get_i64(23).unwrap(), i64::MAX);
        assert_eq!(t.get_i64(24).unwrap_err(), Tuple::tuple_get_error());
    }

    #[test]
    fn get_i32() {
        let t = Tuple::new();

        assert_eq!(t.get_i32(0).unwrap_err(), Tuple::tuple_get_error());

        let mut t = Tuple::new();
        t.add_null();
        t.add_i64(-2147483649);
        t.add_i32(i32::MIN);
        t.add_i32(-16777216);
        t.add_i32(-16777215);
        t.add_i32(-65536);
        t.add_i32(-65535);
        t.add_i32(-32769);
        t.add_i32(-32768); // i16::MIN
        t.add_i32(32767); // i16::MAX
        t.add_i32(32768);
        t.add_i32(65535);
        t.add_i32(65536);
        t.add_i32(16777215);
        t.add_i32(16777216);
        t.add_i32(i32::MAX);
        t.add_i64(2147483648);

        assert_eq!(t.get_i32(0).unwrap_err(), Tuple::tuple_get_error());
        assert_eq!(t.get_i32(1).unwrap_err(), Tuple::tuple_get_error());
        assert_eq!(t.get_i32(2).unwrap(), i32::MIN);
        assert_eq!(t.get_i32(3).unwrap(), -16777216);
        assert_eq!(t.get_i32(4).unwrap(), -16777215);
        assert_eq!(t.get_i32(5).unwrap(), -65536);
        assert_eq!(t.get_i32(6).unwrap(), -65535);
        assert_eq!(t.get_i32(7).unwrap(), -32769);
        assert_eq!(t.get_i32(8).unwrap(), -32768);
        assert_eq!(t.get_i32(9).unwrap(), 32767);
        assert_eq!(t.get_i32(10).unwrap(), 32768);
        assert_eq!(t.get_i32(11).unwrap(), 65535);
        assert_eq!(t.get_i32(12).unwrap(), 65536);
        assert_eq!(t.get_i32(13).unwrap(), 16777215);
        assert_eq!(t.get_i32(14).unwrap(), 16777216);
        assert_eq!(t.get_i32(15).unwrap(), i32::MAX);
        assert_eq!(t.get_i32(16).unwrap_err(), Tuple::tuple_get_error());
    }

    #[test]
    fn get_i16() {
        let t = Tuple::new();

        assert_eq!(t.get_i16(0).unwrap_err(), Tuple::tuple_get_error());

        let mut t = Tuple::new();
        t.add_null();
        t.add_i32(-32769);
        t.add_i16(i16::MIN);
        t.add_i16(-256);
        t.add_i16(-255);
        t.add_i16(-129);
        t.add_i16(-128); // i8::MIN
        t.add_i16(127); // i8::MAX
        t.add_i16(128);
        t.add_i16(255);
        t.add_i16(256);
        t.add_i16(i16::MAX);
        t.add_i32(32768);

        assert_eq!(t.get_i16(0).unwrap_err(), Tuple::tuple_get_error());
        assert_eq!(t.get_i16(1).unwrap_err(), Tuple::tuple_get_error());
        assert_eq!(t.get_i16(2).unwrap(), i16::MIN);
        assert_eq!(t.get_i16(3).unwrap(), -256);
        assert_eq!(t.get_i16(4).unwrap(), -255);
        assert_eq!(t.get_i16(5).unwrap(), -129);
        assert_eq!(t.get_i16(6).unwrap(), -128);
        assert_eq!(t.get_i16(7).unwrap(), 127);
        assert_eq!(t.get_i16(8).unwrap(), 128);
        assert_eq!(t.get_i16(9).unwrap(), 255);
        assert_eq!(t.get_i16(10).unwrap(), 256);
        assert_eq!(t.get_i16(11).unwrap(), i16::MAX);
        assert_eq!(t.get_i16(12).unwrap_err(), Tuple::tuple_get_error());
    }

    #[test]
    fn get_i8() {
        let t = Tuple::new();

        assert_eq!(t.get_i8(0).unwrap_err(), Tuple::tuple_get_error());

        let mut t = Tuple::new();
        t.add_null();
        t.add_i16(-129);
        t.add_i8(i8::MIN);
        t.add_i8(0);
        t.add_i8(i8::MAX);
        t.add_i16(128);

        assert_eq!(t.get_i8(0).unwrap_err(), Tuple::tuple_get_error());
        assert_eq!(t.get_i8(1).unwrap_err(), Tuple::tuple_get_error());
        assert_eq!(t.get_i8(2).unwrap(), i8::MIN);
        assert_eq!(t.get_i8(3).unwrap(), 0);
        assert_eq!(t.get_i8(4).unwrap(), i8::MAX);
        assert_eq!(t.get_i8(5).unwrap_err(), Tuple::tuple_get_error());
    }

    // `3.14` is copied from Java binding tests
    #[allow(clippy::approx_constant)]
    #[test]
    fn get_f32() {
        let t = Tuple::new();

        assert_eq!(t.get_f32(0).unwrap_err(), Tuple::tuple_get_error());

        let mut t = Tuple::new();
        t.add_null();
        t.add_f32(3.14f32);

        assert_eq!(t.get_f32(0).unwrap_err(), Tuple::tuple_get_error());
        assert_eq!(t.get_f32(1).unwrap(), 3.14f32);
    }

    // `3.14` is copied from Java binding tests
    #[allow(clippy::approx_constant)]
    #[test]
    fn get_f64() {
        let t = Tuple::new();

        assert_eq!(t.get_f64(0).unwrap_err(), Tuple::tuple_get_error());

        let mut t = Tuple::new();
        t.add_null();
        t.add_f64(3.14f64);

        assert_eq!(t.get_f64(0).unwrap_err(), Tuple::tuple_get_error());
        assert_eq!(t.get_f64(1).unwrap(), 3.14f64);
    }

    #[test]
    fn get_bool() {
        let t = Tuple::new();

        assert_eq!(t.get_bool(0).unwrap_err(), Tuple::tuple_get_error());

        let mut t = Tuple::new();
        t.add_null();
        t.add_bool(true);
        t.add_bool(false);

        assert_eq!(t.get_bool(0).unwrap_err(), Tuple::tuple_get_error());
        assert_eq!(t.get_bool(1), Ok(true));
        assert_eq!(t.get_bool(2), Ok(false));
    }

    #[test]
    fn get_uuid_ref() {
        let t = Tuple::new();

        assert_eq!(t.get_uuid_ref(0).unwrap_err(), Tuple::tuple_get_error());

        let mut t = Tuple::new();
        t.add_null();
        t.add_uuid(Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap());

        assert_eq!(t.get_uuid_ref(0).unwrap_err(), Tuple::tuple_get_error());
        assert_eq!(
            t.get_uuid_ref(1).unwrap(),
            &Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap()
        );
    }

    #[test]
    fn get_versionstamp_ref() {
        let t = Tuple::new();

        assert_eq!(
            t.get_versionstamp_ref(0).unwrap_err(),
            Tuple::tuple_get_error()
        );

        let mut t = Tuple::new();
        t.add_null();
        t.add_versionstamp(Versionstamp::complete(
            Bytes::from_static(&b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A"[..]),
            657,
        ));

        assert_eq!(
            t.get_versionstamp_ref(0).unwrap_err(),
            Tuple::tuple_get_error()
        );
        assert_eq!(
            t.get_versionstamp_ref(1).unwrap(),
            &Versionstamp::complete(
                Bytes::from_static(&b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A"[..]),
                657,
            )
        );
    }

    #[test]
    fn is_empty() {
        let mut t = Tuple::new();

        assert!(t.is_empty());

        t.add_null();

        assert!(!t.is_empty());
    }

    #[test]
    fn size() {
        let mut t = Tuple::new();

        assert_eq!(t.size(), 0);

        t.add_null();

        assert_eq!(t.size(), 1);
    }

    // `3.14` is copied from Java binding tests
    #[allow(clippy::approx_constant)]
    #[test]
    fn pack() {
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_i64(0);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x14"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bigint(BigInt::parse_bytes(b"0", 10).unwrap());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x14"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_i64(1);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x15\x01"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bigint(BigInt::parse_bytes(b"1", 10).unwrap());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x15\x01"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_i64(-1);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x13\xFE"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bigint(BigInt::parse_bytes(b"-1", 10).unwrap());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x13\xFE"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_i64(255);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x15\xFF"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bigint(BigInt::parse_bytes(b"255", 10).unwrap());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x15\xFF"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_i64(-255);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x13\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bigint(BigInt::parse_bytes(b"-255", 10).unwrap());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x13\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_i64(256);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x16\x01\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bigint(BigInt::parse_bytes(b"256", 10).unwrap());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x16\x01\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_i32(65536);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x17\x01\x00\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_i32(-65536);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x11\xFE\xFF\xFF"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_i64(i64::MAX);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x1C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..]),
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bigint(BigInt::parse_bytes(b"9223372036854775807", 10).unwrap());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x1C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bigint(BigInt::parse_bytes(b"9223372036854775808", 10).unwrap());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x1C\x80\x00\x00\x00\x00\x00\x00\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bigint(BigInt::parse_bytes(b"18446744073709551615", 10).unwrap());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x1C\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bigint(BigInt::parse_bytes(b"18446744073709551616", 10).unwrap());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x1D\x09\x01\x00\x00\x00\x00\x00\x00\x00\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_i64(-4294967295);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x10\x00\x00\x00\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bigint(BigInt::parse_bytes(b"-4294967295", 10).unwrap());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x10\x00\x00\x00\x00"[..]),
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_i64(i64::MIN + 2);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x0C\x80\x00\x00\x00\x00\x00\x00\x01"[..]),
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_i64(i64::MIN + 1);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x0C\x80\x00\x00\x00\x00\x00\x00\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bigint(
                    // i64::MIN + 1
                    BigInt::parse_bytes(b"-9223372036854775808", 10).unwrap() + 1,
                );
                t
            }
            .pack(),
            Bytes::from_static(&b"\x0C\x80\x00\x00\x00\x00\x00\x00\x00"[..]),
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_i64(i64::MIN);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x0C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bigint(
                    // i64::MIN
                    BigInt::parse_bytes(b"-9223372036854775808", 10).unwrap(),
                );
                t
            }
            .pack(),
            Bytes::from_static(&b"\x0C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bigint(
                    // i64::MIN - 1
                    BigInt::parse_bytes(b"-9223372036854775808", 10).unwrap() - 1,
                );
                t
            }
            .pack(),
            Bytes::from_static(&b"\x0C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFE"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bigint(BigInt::parse_bytes(b"-18446744073709551615", 10).unwrap());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x0C\x00\x00\x00\x00\x00\x00\x00\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_f32(3.14f32);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x20\xC0\x48\xF5\xC3"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_f32(-3.14f32);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x20\x3F\xB7\x0A\x3C"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_f64(3.14f64);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x21\xC0\x09\x1E\xB8\x51\xEB\x85\x1F"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_f64(-3.14f64);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x21\x3F\xF6\xE1\x47\xAE\x14\x7A\xE0"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_f32(0.0f32);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x20\x80\x00\x00\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_f32(-0.0f32);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x20\x7F\xFF\xFF\xFF"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_f64(0.0f64);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x21\x80\x00\x00\x00\x00\x00\x00\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_f64(-0.0f64);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x21\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_f32(f32::INFINITY);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x20\xFF\x80\x00\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_f32(f32::NEG_INFINITY);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x20\x00\x7F\xFF\xFF"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_f64(f64::INFINITY);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x21\xFF\xF0\x00\x00\x00\x00\x00\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_f64(f64::NEG_INFINITY);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x21\x00\x0F\xFF\xFF\xFF\xFF\xFF\xFF"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bytes(Bytes::new());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x01\x00"[..]),
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bytes(Bytes::from_static(&b"\x01\x02\x03"[..]));
                t
            }
            .pack(),
            Bytes::from_static(&b"\x01\x01\x02\x03\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bytes(Bytes::from_static(&b"\x00\x00\x00\x04"[..]));
                t
            }
            .pack(),
            Bytes::from_static(&b"\x01\x00\xFF\x00\xFF\x00\xFF\x04\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_string("".to_string());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x02\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_string("hello".to_string());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x02hello\x00"[..]),
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_string("中文".to_string());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x02\xE4\xB8\xAD\xE6\x96\x87\x00"[..]),
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_string("μάθημα".to_string());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x02\xCE\xBC\xCE\xAC\xCE\xB8\xCE\xB7\xCE\xBC\xCE\xB1\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_string("\u{10ffff}".to_string());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x02\xF4\x8F\xBF\xBF\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_tuple({
                    let mut t1 = Tuple::new();
                    t1.add_null();
                    t1
                });
                t
            }
            .pack(),
            Bytes::from_static(&b"\x05\x00\xFF\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_tuple({
                    let mut t1 = Tuple::new();
                    t1.add_null();
                    t1.add_string("hello".to_string());
                    t1
                });
                t
            }
            .pack(),
            Bytes::from_static(&b"\x05\x00\xFF\x02hello\x00\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_tuple({
                    let mut t1 = Tuple::new();
                    t1.add_null();
                    t1.add_string("hell\x00".to_string());
                    t1
                });
                t
            }
            .pack(),
            Bytes::from_static(&b"\x05\x00\xFF\x02hell\x00\xFF\x00\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_tuple({
                    let mut t1 = Tuple::new();
                    t1.add_null();
                    t1
                });
                t.add_string("hello".to_string());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x05\x00\xFF\x00\x02hello\x00"[..]),
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_tuple({
                    let mut t1 = Tuple::new();
                    t1.add_null();
                    t1
                });
                t.add_string("hello".to_string());
                t.add_bytes(Bytes::from_static(&b"\x01\x00"[..]));
                t.add_bytes(Bytes::new());
                t
            }
            .pack(),
            Bytes::from_static(&b"\x05\x00\xFF\x00\x02hello\x00\x01\x01\x00\xFF\x00\x01\x00"[..]),
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_uuid(Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap());
                t
            }
            .pack(),
            Bytes::from_static(
                &b"\x30\xFF\xFF\xFF\xFF\xBA\x5E\xBA\x11\x00\x00\x00\x00\x5C\xA1\xAB\x1E"[..]
            )
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bool(false);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x26"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bool(true);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x27"[..]),
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_i8(3);
                t
            }
            .pack(),
            Bytes::from_static(&b"\x15\x03"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_versionstamp(Versionstamp::complete(
                    Bytes::from_static(&b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"[..]),
                    0,
                ));
                t
            }
            .pack(),
            Bytes::from_static(&b"\x33\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03\x00\x00"[..])
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_versionstamp(Versionstamp::complete(
                    Bytes::from_static(&b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A"[..]),
                    657,
                ));
                t
            }
            .pack(),
            Bytes::from_static(&b"\x33\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x02\x91"[..])
        );
    }

    #[test]
    fn pack_with_versionstamp() {
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_string("foo".to_string());
                t.add_versionstamp(Versionstamp::incomplete(0));
                t
            }
            .pack_with_versionstamp(Bytes::new()),
            Ok(Bytes::from_static(
                &b"\x02foo\x00\x33\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x00\x00\x06\x00\x00\x00"
                    [..]
            ))
        );
        assert_eq!(
            Tuple::new().pack_with_versionstamp(Bytes::new()),
            Err(FdbError::new(TUPLE_PACK_WITH_VERSIONSTAMP_NOT_FOUND))
        );
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_null();
                t.add_versionstamp(Versionstamp::incomplete(0));
                t.add_tuple({
                    let mut t1 = Tuple::new();
                    t1.add_string("foo".to_string());
                    t1.add_versionstamp(Versionstamp::incomplete(1));
                    t1
                });
                t
            }
            .pack_with_versionstamp(Bytes::new()),
            Err(FdbError::new(TUPLE_PACK_WITH_VERSIONSTAMP_MULTIPLE_FOUND))
        );
    }

    #[test]
    fn range() {
        assert!(std::panic::catch_unwind(|| {
            {
                let mut t = Tuple::new();
                t.add_versionstamp(Versionstamp::incomplete(0));
                t
            }
            .range(Bytes::new());
        })
        .is_err());
        assert_eq!(
            {
                let mut t = Tuple::new();
                t.add_bytes(Bytes::from_static(&b"bar"[..]));
                t
            }
            .range(Bytes::from_static(&b"foo"[..])),
            Range::new(
                Bytes::from_static(&b"foo\x01bar\x00\x00"[..]),
                Bytes::from_static(&b"foo\x01bar\x00\xFF"[..])
            )
        );
    }

    #[test]
    fn from_elements() {
        let mut t1 = Tuple::new();
        t1.add_null();

        let t = Tuple::from_elements(t1.elements);

        assert!(!t.has_incomplete_versionstamp());
        assert_eq!(t.elements, vec![TupleValue::NullValue]);

        let mut t1 = Tuple::new();
        t1.add_null();

        let mut t2 = Tuple::new();
        t2.add_versionstamp(Versionstamp::incomplete(0));
        t1.add_tuple(t2);

        let t = Tuple::from_elements(t1.elements);

        assert!(t.has_incomplete_versionstamp());
        assert_eq!(
            t.elements,
            vec![
                TupleValue::NullValue,
                // We don't want to use `Tuple::from_elements` here.
                TupleValue::NestedTuple({
                    let mut x = Tuple::new();
                    x.add_versionstamp(Versionstamp::incomplete(0));
                    x
                }),
            ]
        );

        let mut t1 = Tuple::new();
        t1.add_null();
        t1.add_versionstamp(Versionstamp::incomplete(0));

        let t = Tuple::from_elements(t1.elements);

        assert!(t.has_incomplete_versionstamp());
        assert_eq!(
            t.elements,
            vec![
                TupleValue::NullValue,
                // We don't want to use `Tuple::from_elements` here.
                TupleValue::Versionstamp96Bit(Versionstamp::incomplete(0)),
            ]
        );
    }
}
