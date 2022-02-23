use bytes::{BufMut, Bytes, BytesMut};
use num_bigint::BigInt;
use uuid::Uuid;

use crate::error::{FdbError, FdbResult, TUPLE_FROM_BYTES};
use crate::tuple::{Tuple, Versionstamp};

// The specifications for FDB Tuple layer typecodes is here.
// https://github.com/apple/foundationdb/blob/master/design/tuple.md
// Key and values are encoded as tuples.
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum TupleValue {
    NullValue,                                 // 0x00
    ByteString(Bytes),                         // 0x01
    UnicodeString(String),                     // 0x02
    NestedTuple(Tuple),                        // 0x05
    NegativeArbitraryPrecisionInteger(BigInt), // 0x0b
    NegInt8(u64),                              // 0x0c
    NegInt7(u64),                              // 0x0d
    NegInt6(u64),                              // 0x0e
    NegInt5(u64),                              // 0x0f
    NegInt4(u32),                              // 0x10
    NegInt3(u32),                              // 0x11
    NegInt2(u16),                              // 0x12
    NegInt1(u8),                               // 0x13
    IntZero,                                   // 0x14
    PosInt1(u8),                               // 0x15
    PosInt2(u16),                              // 0x16
    PosInt3(u32),                              // 0x17
    PosInt4(u32),                              // 0x18
    PosInt5(u64),                              // 0x19
    PosInt6(u64),                              // 0x1a
    PosInt7(u64),                              // 0x1b
    PosInt8(u64),                              // 0x1c
    PositiveArbitraryPrecisionInteger(BigInt), // 0x1d
    IeeeBinaryFloatingPointFloat(f32),         // 0x20
    IeeeBinaryFloatingPointDouble(f64),        // 0x21
    FalseValue,                                // 0x26
    TrueValue,                                 // 0x27
    Rfc4122Uuid(Uuid),                         // 0x30
    Versionstamp96Bit(Versionstamp),           // 0x33
}

pub(crate) fn from_bytes(b: Bytes) -> FdbResult<Tuple> {
    parser::tuple(b.as_ref())
        .map(|(_, t)| t)
        .map_err(|_| FdbError::new(TUPLE_FROM_BYTES))
}

pub(crate) fn to_bytes(t: Tuple) -> Bytes {
    let mut res = BytesMut::new();

    t.into_elements().into_iter().for_each(|x| {
        res.put(match x {
            TupleValue::NullValue => serializer::null_value(),
            TupleValue::ByteString(b) => serializer::byte_string(b),
            TupleValue::UnicodeString(s) => serializer::unicode_string(s),
            TupleValue::NestedTuple(t) => serializer::nested_tuple(t),
            TupleValue::NegativeArbitraryPrecisionInteger(b) => {
                serializer::negative_arbitrary_precision_integer(b)
            }
            TupleValue::NegInt8(u) => serializer::neg_int_8(u),
            TupleValue::NegInt7(u) => serializer::neg_int_7(u),
            TupleValue::NegInt6(u) => serializer::neg_int_6(u),
            TupleValue::NegInt5(u) => serializer::neg_int_5(u),
            TupleValue::NegInt4(u) => serializer::neg_int_4(u),
            TupleValue::NegInt3(u) => serializer::neg_int_3(u),
            TupleValue::NegInt2(u) => serializer::neg_int_2(u),
            TupleValue::NegInt1(u) => serializer::neg_int_1(u),
            TupleValue::IntZero => serializer::int_zero(),
            TupleValue::PosInt1(u) => serializer::pos_int_1(u),
            TupleValue::PosInt2(u) => serializer::pos_int_2(u),
            TupleValue::PosInt3(u) => serializer::pos_int_3(u),
            TupleValue::PosInt4(u) => serializer::pos_int_4(u),
            TupleValue::PosInt5(u) => serializer::pos_int_5(u),
            TupleValue::PosInt6(u) => serializer::pos_int_6(u),
            TupleValue::PosInt7(u) => serializer::pos_int_7(u),
            TupleValue::PosInt8(u) => serializer::pos_int_8(u),
            TupleValue::PositiveArbitraryPrecisionInteger(b) => {
                serializer::positive_arbitrary_precision_integer(b)
            }
            TupleValue::IeeeBinaryFloatingPointFloat(f) => {
                serializer::ieee_binary_floating_point_float(f)
            }
            TupleValue::IeeeBinaryFloatingPointDouble(f) => {
                serializer::ieee_binary_floating_point_double(f)
            }
            TupleValue::FalseValue => serializer::false_value(),
            TupleValue::TrueValue => serializer::true_value(),
            TupleValue::Rfc4122Uuid(u) => serializer::rfc_4122_uuid(u),
            TupleValue::Versionstamp96Bit(v) => serializer::versionstamp_96_bit(v),
        })
    });

    res.into()
}

pub(crate) fn find_incomplete_versionstamp(t: Tuple) -> FdbResult<usize> {
    versionstamp::query_for_incomplete_versionstamp(t)
}

// Internal submodule that provides methods to other submodules.
pub(self) mod utils {
    use bytes::{BufMut, Bytes, BytesMut};
    use nom::{bytes as nom_bytes, IResult};

    pub(crate) fn neg_u8_slice_into_vec(i: &[u8]) -> Vec<u8> {
        let mut res = Vec::new();
        i.iter().for_each(|x| {
            res.push(!(*x));
        });
        res
    }

    // Both `byte_string` and `unicode_string` uses the same packing
    // format.
    pub(crate) fn extract_unpacked_bytes(mut i: &[u8]) -> IResult<&[u8], Bytes> {
        let mut res_output = BytesMut::new();
        let res_input;

        loop {
            let (i1, o1) = nom_bytes::complete::take_until(&b"\x00"[..])(i)?;
            res_output.put(o1);

            // At this time i1 is either b"\x00", or b"\x00\xFF", or b"\x00....".
            if i1.len() >= 2 {
                if i1[1] == b'\xFF' {
                    res_output.put(&b"\x00"[..]);
                    // Update i1, so we can iterate through the loop.
                    i = &i1[2..];
                } else {
                    // exit the loop
                    res_input = &i1[1..];
                    return Ok((res_input, res_output.into()));
                }
            } else {
                // When i1.len() == 1, it means that we have b"\x00".
                res_input = &i1[1..];
                return Ok((res_input, res_output.into()));
            }
        }
    }
}

pub(self) mod versionstamp {
    use crate::error::{
        FdbError, FdbResult, TUPLE_PACK_WITH_VERSIONSTAMP_MULTIPLE_FOUND,
        TUPLE_PACK_WITH_VERSIONSTAMP_NOT_FOUND,
    };
    use crate::tuple::Tuple;

    use super::{serializer, TupleValue};

    // As we recurse through the `Tuple`, we'll go from `NotFound(0)`
    // to `Found(x)`, in case we find an incomplete version
    // stamp. Once we are `Found(x)` state, we ignore any further
    // `NotFound(x)`. However, if we find another `Found(x)`, then we
    // switch to `MultipleFound`, which is our error state.
    //
    // The variant names make sense here.
    #[allow(clippy::enum_variant_names)]
    #[derive(Debug, PartialEq)]
    enum QueryForIncompleteVersionstamp {
        NotFound(usize),
        Found(usize),
        MultipleFound,
    }

    pub(crate) fn query_for_incomplete_versionstamp(t: Tuple) -> FdbResult<usize> {
        let query_for_incomplete_versionstamp = t
            .into_elements()
            .into_iter()
            .map(|tv| match tv {
                TupleValue::NullValue => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::null_value().len())
                }
                TupleValue::ByteString(b) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::byte_string(b).len())
                }
                TupleValue::UnicodeString(s) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::unicode_string(s).len())
                }
                TupleValue::NestedTuple(t) => query_for_incomplete_versionstamp_nested_tuple(t),
                TupleValue::NegativeArbitraryPrecisionInteger(b) => {
                    QueryForIncompleteVersionstamp::NotFound(
                        serializer::negative_arbitrary_precision_integer(b).len(),
                    )
                }
                TupleValue::NegInt8(u) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::neg_int_8(u).len())
                }
                TupleValue::NegInt7(u) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::neg_int_7(u).len())
                }
                TupleValue::NegInt6(u) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::neg_int_6(u).len())
                }
                TupleValue::NegInt5(u) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::neg_int_5(u).len())
                }
                TupleValue::NegInt4(u) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::neg_int_4(u).len())
                }
                TupleValue::NegInt3(u) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::neg_int_3(u).len())
                }
                TupleValue::NegInt2(u) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::neg_int_2(u).len())
                }
                TupleValue::NegInt1(u) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::neg_int_1(u).len())
                }
                TupleValue::IntZero => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::int_zero().len())
                }
                TupleValue::PosInt1(u) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::pos_int_1(u).len())
                }
                TupleValue::PosInt2(u) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::pos_int_2(u).len())
                }
                TupleValue::PosInt3(u) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::pos_int_3(u).len())
                }
                TupleValue::PosInt4(u) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::pos_int_4(u).len())
                }
                TupleValue::PosInt5(u) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::pos_int_5(u).len())
                }
                TupleValue::PosInt6(u) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::pos_int_6(u).len())
                }
                TupleValue::PosInt7(u) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::pos_int_7(u).len())
                }
                TupleValue::PosInt8(u) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::pos_int_8(u).len())
                }
                TupleValue::PositiveArbitraryPrecisionInteger(b) => {
                    QueryForIncompleteVersionstamp::NotFound(
                        serializer::positive_arbitrary_precision_integer(b).len(),
                    )
                }
                TupleValue::IeeeBinaryFloatingPointFloat(f) => {
                    QueryForIncompleteVersionstamp::NotFound(
                        serializer::ieee_binary_floating_point_float(f).len(),
                    )
                }
                TupleValue::IeeeBinaryFloatingPointDouble(f) => {
                    QueryForIncompleteVersionstamp::NotFound(
                        serializer::ieee_binary_floating_point_double(f).len(),
                    )
                }
                TupleValue::FalseValue => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::false_value().len())
                }
                TupleValue::TrueValue => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::true_value().len())
                }
                TupleValue::Rfc4122Uuid(u) => {
                    QueryForIncompleteVersionstamp::NotFound(serializer::rfc_4122_uuid(u).len())
                }
                TupleValue::Versionstamp96Bit(v) => {
                    if v.is_complete() {
                        QueryForIncompleteVersionstamp::NotFound(
                            serializer::versionstamp_96_bit(v).len(),
                        )
                    } else {
                        // We return `Found(1)` here, which will then get
                        // added up to determine the actual index.
                        QueryForIncompleteVersionstamp::Found(1)
                    }
                }
            })
            .fold(
                QueryForIncompleteVersionstamp::NotFound(0),
                |acc, x| match acc {
                    QueryForIncompleteVersionstamp::NotFound(u) => match x {
                        QueryForIncompleteVersionstamp::NotFound(v) => {
                            QueryForIncompleteVersionstamp::NotFound(u + v)
                        }
                        QueryForIncompleteVersionstamp::Found(v) => {
                            QueryForIncompleteVersionstamp::Found(u + v)
                        }
                        QueryForIncompleteVersionstamp::MultipleFound => {
                            QueryForIncompleteVersionstamp::MultipleFound
                        }
                    },
                    QueryForIncompleteVersionstamp::Found(u) => match x {
                        QueryForIncompleteVersionstamp::NotFound(_) => {
                            QueryForIncompleteVersionstamp::Found(u)
                        }
                        QueryForIncompleteVersionstamp::Found(_) => {
                            QueryForIncompleteVersionstamp::MultipleFound
                        }
                        QueryForIncompleteVersionstamp::MultipleFound => {
                            QueryForIncompleteVersionstamp::MultipleFound
                        }
                    },
                    QueryForIncompleteVersionstamp::MultipleFound => {
                        QueryForIncompleteVersionstamp::MultipleFound
                    }
                },
            );

        match query_for_incomplete_versionstamp {
            QueryForIncompleteVersionstamp::NotFound(_) => {
                Err(FdbError::new(TUPLE_PACK_WITH_VERSIONSTAMP_NOT_FOUND))
            }
            QueryForIncompleteVersionstamp::Found(u) => Ok(u),
            QueryForIncompleteVersionstamp::MultipleFound => {
                Err(FdbError::new(TUPLE_PACK_WITH_VERSIONSTAMP_MULTIPLE_FOUND))
            }
        }
    }

    fn query_for_incomplete_versionstamp_nested_tuple(t: Tuple) -> QueryForIncompleteVersionstamp {
        t.into_elements()
            .into_iter()
            .map(|tv| {
                match tv {
                    TupleValue::NullValue => QueryForIncompleteVersionstamp::NotFound(
                        serializer::nested_tuple_null_value().len(),
                    ),
                    TupleValue::ByteString(b) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::byte_string(b).len())
                    }
                    TupleValue::UnicodeString(s) => QueryForIncompleteVersionstamp::NotFound(
                        serializer::unicode_string(s).len(),
                    ),
                    TupleValue::NestedTuple(t) => query_for_incomplete_versionstamp_nested_tuple(t),
                    TupleValue::NegativeArbitraryPrecisionInteger(b) => {
                        QueryForIncompleteVersionstamp::NotFound(
                            serializer::negative_arbitrary_precision_integer(b).len(),
                        )
                    }
                    TupleValue::NegInt8(u) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::neg_int_8(u).len())
                    }
                    TupleValue::NegInt7(u) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::neg_int_7(u).len())
                    }
                    TupleValue::NegInt6(u) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::neg_int_6(u).len())
                    }
                    TupleValue::NegInt5(u) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::neg_int_5(u).len())
                    }
                    TupleValue::NegInt4(u) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::neg_int_4(u).len())
                    }
                    TupleValue::NegInt3(u) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::neg_int_3(u).len())
                    }
                    TupleValue::NegInt2(u) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::neg_int_2(u).len())
                    }
                    TupleValue::NegInt1(u) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::neg_int_1(u).len())
                    }
                    TupleValue::IntZero => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::int_zero().len())
                    }
                    TupleValue::PosInt1(u) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::pos_int_1(u).len())
                    }
                    TupleValue::PosInt2(u) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::pos_int_2(u).len())
                    }
                    TupleValue::PosInt3(u) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::pos_int_3(u).len())
                    }
                    TupleValue::PosInt4(u) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::pos_int_4(u).len())
                    }
                    TupleValue::PosInt5(u) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::pos_int_5(u).len())
                    }
                    TupleValue::PosInt6(u) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::pos_int_6(u).len())
                    }
                    TupleValue::PosInt7(u) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::pos_int_7(u).len())
                    }
                    TupleValue::PosInt8(u) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::pos_int_8(u).len())
                    }
                    TupleValue::PositiveArbitraryPrecisionInteger(b) => {
                        QueryForIncompleteVersionstamp::NotFound(
                            serializer::positive_arbitrary_precision_integer(b).len(),
                        )
                    }
                    TupleValue::IeeeBinaryFloatingPointFloat(f) => {
                        QueryForIncompleteVersionstamp::NotFound(
                            serializer::ieee_binary_floating_point_float(f).len(),
                        )
                    }
                    TupleValue::IeeeBinaryFloatingPointDouble(f) => {
                        QueryForIncompleteVersionstamp::NotFound(
                            serializer::ieee_binary_floating_point_double(f).len(),
                        )
                    }
                    TupleValue::FalseValue => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::false_value().len())
                    }
                    TupleValue::TrueValue => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::true_value().len())
                    }
                    TupleValue::Rfc4122Uuid(u) => {
                        QueryForIncompleteVersionstamp::NotFound(serializer::rfc_4122_uuid(u).len())
                    }
                    TupleValue::Versionstamp96Bit(v) => {
                        if v.is_complete() {
                            QueryForIncompleteVersionstamp::NotFound(
                                serializer::versionstamp_96_bit(v).len(),
                            )
                        } else {
                            // We return `Found(1)` here, which will then get
                            // added up to determine the actual index.
                            QueryForIncompleteVersionstamp::Found(1)
                        }
                    }
                }
            })
            .fold(
                QueryForIncompleteVersionstamp::NotFound(0),
                |acc, x| match acc {
                    QueryForIncompleteVersionstamp::NotFound(u) => match x {
                        QueryForIncompleteVersionstamp::NotFound(v) => {
                            QueryForIncompleteVersionstamp::NotFound(u + v)
                        }
                        QueryForIncompleteVersionstamp::Found(v) => {
                            QueryForIncompleteVersionstamp::Found(u + v)
                        }
                        QueryForIncompleteVersionstamp::MultipleFound => {
                            QueryForIncompleteVersionstamp::MultipleFound
                        }
                    },
                    QueryForIncompleteVersionstamp::Found(u) => match x {
                        QueryForIncompleteVersionstamp::NotFound(_) => {
                            QueryForIncompleteVersionstamp::Found(u)
                        }
                        QueryForIncompleteVersionstamp::Found(_) => {
                            QueryForIncompleteVersionstamp::MultipleFound
                        }
                        QueryForIncompleteVersionstamp::MultipleFound => {
                            QueryForIncompleteVersionstamp::MultipleFound
                        }
                    },
                    QueryForIncompleteVersionstamp::MultipleFound => {
                        QueryForIncompleteVersionstamp::MultipleFound
                    }
                },
            )
    }

    #[cfg(test)]
    mod tests {
        use crate::error::{
            FdbError, TUPLE_PACK_WITH_VERSIONSTAMP_MULTIPLE_FOUND,
            TUPLE_PACK_WITH_VERSIONSTAMP_NOT_FOUND,
        };
        use crate::tuple::{Tuple, Versionstamp};

        use super::{
            query_for_incomplete_versionstamp, query_for_incomplete_versionstamp_nested_tuple,
            QueryForIncompleteVersionstamp,
        };

        #[test]
        fn test_query_for_incomplete_versionstamp() {
            assert_eq!(
                query_for_incomplete_versionstamp({
                    let mut t = Tuple::new();
                    t.add_null();
                    t.add_versionstamp(Versionstamp::incomplete(0));
                    t.add_string("foo".to_string());
                    t
                }),
                Ok(2)
            );
            assert_eq!(
                query_for_incomplete_versionstamp({
                    let mut t = Tuple::new();
                    t.add_null();
                    t.add_string("foo".to_string());
                    t
                }),
                Err(FdbError::new(TUPLE_PACK_WITH_VERSIONSTAMP_NOT_FOUND))
            );
            assert_eq!(
                query_for_incomplete_versionstamp({
                    let mut t = Tuple::new();
                    t.add_null();
                    t.add_tuple({
                        let mut t1 = Tuple::new();
                        t1.add_versionstamp(Versionstamp::incomplete(1));
                        t1
                    });
                    t.add_string("foo".to_string());
                    t.add_tuple({
                        let mut t2 = Tuple::new();
                        t2.add_versionstamp(Versionstamp::incomplete(1));
                        t2
                    });
                    t.add_string("bar".to_string());
                    t
                }),
                Err(FdbError::new(TUPLE_PACK_WITH_VERSIONSTAMP_MULTIPLE_FOUND))
            );
        }

        #[test]
        fn test_query_for_incomplete_versionstamp_nested_tuple() {
            assert_eq!(
                query_for_incomplete_versionstamp_nested_tuple({
                    let mut t = Tuple::new();
                    t.add_null();
                    t.add_versionstamp(Versionstamp::incomplete(0));
                    t.add_string("foo".to_string());
                    t
                }),
                QueryForIncompleteVersionstamp::Found(3)
            );
            assert_eq!(
                query_for_incomplete_versionstamp_nested_tuple({
                    let mut t = Tuple::new();
                    t.add_null();
                    t.add_string("foo".to_string());
                    t
                }),
                QueryForIncompleteVersionstamp::NotFound(7)
            );
            assert_eq!(
                query_for_incomplete_versionstamp_nested_tuple({
                    let mut t = Tuple::new();
                    t.add_null();
                    t.add_tuple({
                        let mut t1 = Tuple::new();
                        t1.add_versionstamp(Versionstamp::incomplete(1));
                        t1
                    });
                    t.add_string("foo".to_string());
                    t.add_tuple({
                        let mut t2 = Tuple::new();
                        t2.add_versionstamp(Versionstamp::incomplete(1));
                        t2
                    });
                    t.add_string("bar".to_string());
                    t
                }),
                QueryForIncompleteVersionstamp::MultipleFound
            );
        }
    }
}

pub(self) mod serializer {
    use bytes::Bytes;
    use bytes::{BufMut, BytesMut};
    use num_bigint::BigInt;
    use uuid::Uuid;

    use std::convert::TryFrom;

    use crate::tuple::{Tuple, Versionstamp};

    use super::{utils::neg_u8_slice_into_vec, TupleValue};

    pub(crate) fn null_value() -> Bytes {
        Bytes::from_static(&b"\x00"[..])
    }

    pub(crate) fn nested_tuple_null_value() -> Bytes {
        Bytes::from_static(&b"\x00\xFF"[..])
    }

    pub(crate) fn byte_string(b: Bytes) -> Bytes {
        let mut res = BytesMut::new();

        res.put_u8(b'\x01');

        b.into_iter().for_each(|x| {
            if x == b'\x00' {
                res.put(&b"\x00\xFF"[..]);
            } else {
                res.put_u8(x);
            }
        });

        res.put_u8(b'\x00');

        res.into()
    }

    pub(crate) fn unicode_string(s: String) -> Bytes {
        let mut res = BytesMut::new();

        res.put_u8(b'\x02');

        s.into_bytes().into_iter().for_each(|x| {
            if x == b'\x00' {
                res.put(&b"\x00\xFF"[..]);
            } else {
                res.put_u8(x);
            }
        });

        res.put_u8(b'\x00');

        res.into()
    }

    pub(crate) fn nested_tuple(t: Tuple) -> Bytes {
        let mut res = BytesMut::new();

        res.put_u8(b'\x05');

        t.into_elements().into_iter().for_each(|x| {
            res.put(match x {
                TupleValue::NullValue => nested_tuple_null_value(),
                TupleValue::ByteString(b) => byte_string(b),
                TupleValue::UnicodeString(s) => unicode_string(s),
                TupleValue::NestedTuple(t) => nested_tuple(t),
                TupleValue::NegativeArbitraryPrecisionInteger(b) => {
                    negative_arbitrary_precision_integer(b)
                }
                TupleValue::NegInt8(u) => neg_int_8(u),
                TupleValue::NegInt7(u) => neg_int_7(u),
                TupleValue::NegInt6(u) => neg_int_6(u),
                TupleValue::NegInt5(u) => neg_int_5(u),
                TupleValue::NegInt4(u) => neg_int_4(u),
                TupleValue::NegInt3(u) => neg_int_3(u),
                TupleValue::NegInt2(u) => neg_int_2(u),
                TupleValue::NegInt1(u) => neg_int_1(u),
                TupleValue::IntZero => int_zero(),
                TupleValue::PosInt1(u) => pos_int_1(u),
                TupleValue::PosInt2(u) => pos_int_2(u),
                TupleValue::PosInt3(u) => pos_int_3(u),
                TupleValue::PosInt4(u) => pos_int_4(u),
                TupleValue::PosInt5(u) => pos_int_5(u),
                TupleValue::PosInt6(u) => pos_int_6(u),
                TupleValue::PosInt7(u) => pos_int_7(u),
                TupleValue::PosInt8(u) => pos_int_8(u),
                TupleValue::PositiveArbitraryPrecisionInteger(b) => {
                    positive_arbitrary_precision_integer(b)
                }
                TupleValue::IeeeBinaryFloatingPointFloat(f) => ieee_binary_floating_point_float(f),
                TupleValue::IeeeBinaryFloatingPointDouble(f) => {
                    ieee_binary_floating_point_double(f)
                }
                TupleValue::FalseValue => false_value(),
                TupleValue::TrueValue => true_value(),
                TupleValue::Rfc4122Uuid(u) => rfc_4122_uuid(u),
                TupleValue::Versionstamp96Bit(v) => versionstamp_96_bit(v),
            })
        });

        res.put_u8(b'\x00');

        res.into()
    }

    pub(crate) fn negative_arbitrary_precision_integer(b: BigInt) -> Bytes {
        let b: BigInt = b * -1;
        let (_, val) = b.to_bytes_be();

        let val = Bytes::from(neg_u8_slice_into_vec(val.as_ref()));

        // Maximum length of val should be 255. We do not allow
        // encoding of [`BigInt`] greator than 255 using
        // `add_bigint`.
        let len = (u8::try_from(val.len()).unwrap()) ^ 0xFFu8;

        let mut res = BytesMut::new();

        res.put_u8(b'\x0B');

        res.put_u8(len);
        res.put(val);

        res.into()
    }

    pub(crate) fn neg_int_8(u: u64) -> Bytes {
        let u = u.to_be_bytes();
        let val = Bytes::from(neg_u8_slice_into_vec(&u));

        let mut res = BytesMut::new();

        res.put_u8(b'\x0C');
        res.put(val);

        res.into()
    }

    pub(crate) fn neg_int_7(u: u64) -> Bytes {
        let u = u.to_be_bytes();
        let val = Bytes::from(neg_u8_slice_into_vec(&u[1..]));

        let mut res = BytesMut::new();

        res.put_u8(b'\x0D');
        res.put(val);

        res.into()
    }

    pub(crate) fn neg_int_6(u: u64) -> Bytes {
        let u = u.to_be_bytes();
        let val = Bytes::from(neg_u8_slice_into_vec(&u[2..]));

        let mut res = BytesMut::new();

        res.put_u8(b'\x0E');
        res.put(val);

        res.into()
    }

    pub(crate) fn neg_int_5(u: u64) -> Bytes {
        let u = u.to_be_bytes();
        let val = Bytes::from(neg_u8_slice_into_vec(&u[3..]));

        let mut res = BytesMut::new();

        res.put_u8(b'\x0F');
        res.put(val);

        res.into()
    }

    pub(crate) fn neg_int_4(u: u32) -> Bytes {
        let u = u.to_be_bytes();
        let val = Bytes::from(neg_u8_slice_into_vec(&u));

        let mut res = BytesMut::new();

        res.put_u8(b'\x10');
        res.put(val);

        res.into()
    }

    pub(crate) fn neg_int_3(u: u32) -> Bytes {
        let u = u.to_be_bytes();
        let val = Bytes::from(neg_u8_slice_into_vec(&u[1..]));

        let mut res = BytesMut::new();

        res.put_u8(b'\x11');
        res.put(val);

        res.into()
    }

    pub(crate) fn neg_int_2(u: u16) -> Bytes {
        let u = u.to_be_bytes();
        let val = Bytes::from(neg_u8_slice_into_vec(&u));

        let mut res = BytesMut::new();

        res.put_u8(b'\x12');
        res.put(val);

        res.into()
    }

    pub(crate) fn neg_int_1(u: u8) -> Bytes {
        let u = u.to_be_bytes();
        let val = Bytes::from(neg_u8_slice_into_vec(&u));

        let mut res = BytesMut::new();

        res.put_u8(b'\x13');
        res.put(val);

        res.into()
    }

    pub(crate) fn int_zero() -> Bytes {
        Bytes::from_static(&b"\x14"[..])
    }

    pub(crate) fn pos_int_1(u: u8) -> Bytes {
        let val = Bytes::from(u.to_be_bytes().to_vec());

        let mut res = BytesMut::new();

        res.put_u8(b'\x15');
        res.put(val);

        res.into()
    }

    pub(crate) fn pos_int_2(u: u16) -> Bytes {
        let val = Bytes::from(u.to_be_bytes().to_vec());

        let mut res = BytesMut::new();

        res.put_u8(b'\x16');
        res.put(val);

        res.into()
    }

    pub(crate) fn pos_int_3(u: u32) -> Bytes {
        let val = Bytes::from((&u.to_be_bytes()[1..]).to_vec());

        let mut res = BytesMut::new();

        res.put_u8(b'\x17');
        res.put(val);

        res.into()
    }

    pub(crate) fn pos_int_4(u: u32) -> Bytes {
        let val = Bytes::from(u.to_be_bytes().to_vec());

        let mut res = BytesMut::new();

        res.put_u8(b'\x18');
        res.put(val);

        res.into()
    }

    pub(crate) fn pos_int_5(u: u64) -> Bytes {
        let val = Bytes::from((&u.to_be_bytes()[3..]).to_vec());

        let mut res = BytesMut::new();

        res.put_u8(b'\x19');
        res.put(val);

        res.into()
    }

    pub(crate) fn pos_int_6(u: u64) -> Bytes {
        let val = Bytes::from((&u.to_be_bytes()[2..]).to_vec());

        let mut res = BytesMut::new();

        res.put_u8(b'\x1A');
        res.put(val);

        res.into()
    }

    pub(crate) fn pos_int_7(u: u64) -> Bytes {
        let val = Bytes::from((&u.to_be_bytes()[1..]).to_vec());

        let mut res = BytesMut::new();

        res.put_u8(b'\x1B');
        res.put(val);

        res.into()
    }

    pub(crate) fn pos_int_8(u: u64) -> Bytes {
        let val = Bytes::from(u.to_be_bytes().to_vec());

        let mut res = BytesMut::new();

        res.put_u8(b'\x1C');
        res.put(val);

        res.into()
    }

    pub(crate) fn positive_arbitrary_precision_integer(b: BigInt) -> Bytes {
        let (_, val) = b.to_bytes_be();

        let val = Bytes::from(val);

        // Maximum length of val should be 255. We do not allow
        // encoding of [`BigInt`] greator than 255 using
        // `add_bigint`.
        let len = u8::try_from(val.len()).unwrap();

        let mut res = BytesMut::new();

        res.put_u8(b'\x1D');

        res.put_u8(len);
        res.put(val);

        res.into()
    }

    pub(crate) fn ieee_binary_floating_point_float(f: f32) -> Bytes {
        let x = f.to_be_bytes();

        let mut res = Vec::new();

        res.put_u8(b'\x20');

        if f.is_sign_negative() {
            // Negative number. Flip all the bytes.
            x.iter().for_each(|y| res.push(*y ^ 0xFF));
        } else {
            // Positive number. Flip just the sign bit.
            res.push(x[0] ^ 0x80);
            x[1..].iter().for_each(|y| res.push(*y));
        }

        Bytes::from(res)
    }

    pub(crate) fn ieee_binary_floating_point_double(f: f64) -> Bytes {
        let x = f.to_be_bytes();

        let mut res = Vec::new();

        res.put_u8(b'\x21');

        if f.is_sign_negative() {
            // Negative number. Flip all the bytes.
            x.iter().for_each(|y| res.push(*y ^ 0xFF));
        } else {
            // Positive number. Flip just the sign bit.
            res.push(x[0] ^ 0x80);
            x[1..].iter().for_each(|y| res.push(*y));
        }

        Bytes::from(res)
    }

    pub(crate) fn false_value() -> Bytes {
        Bytes::from_static(&b"\x26"[..])
    }

    pub(crate) fn true_value() -> Bytes {
        Bytes::from_static(&b"\x27"[..])
    }

    pub(crate) fn rfc_4122_uuid(u: Uuid) -> Bytes {
        let mut res = BytesMut::new();

        res.put_u8(b'\x30');

        res.put(&u.as_bytes()[..]);

        res.into()
    }

    pub(crate) fn versionstamp_96_bit(v: Versionstamp) -> Bytes {
        let mut res = BytesMut::new();

        res.put_u8(b'\x33');

        res.put(v.get_bytes());

        res.into()
    }

    #[cfg(test)]
    mod tests {
        use bytes::Bytes;
        use num_bigint::BigInt;
        use uuid::Uuid;

        use crate::tuple::{Tuple, Versionstamp};

        use super::{
            byte_string, false_value, ieee_binary_floating_point_double,
            ieee_binary_floating_point_float, int_zero, neg_int_1, neg_int_2, neg_int_3, neg_int_4,
            neg_int_5, neg_int_6, neg_int_7, neg_int_8, negative_arbitrary_precision_integer,
            nested_tuple, nested_tuple_null_value, null_value, pos_int_1, pos_int_2, pos_int_3,
            pos_int_4, pos_int_5, pos_int_6, pos_int_7, pos_int_8,
            positive_arbitrary_precision_integer, rfc_4122_uuid, true_value, unicode_string,
            versionstamp_96_bit, TupleValue,
        };

        #[test]
        fn test_null_value() {
            assert_eq!(null_value(), Bytes::from_static(&b"\x00"[..]));
        }

        #[test]
        fn test_nested_tuple_null_value() {
            assert_eq!(
                nested_tuple_null_value(),
                Bytes::from_static(&b"\x00\xFF"[..])
            );
        }

        #[test]
        fn test_byte_string() {
            assert_eq!(
                byte_string(Bytes::from_static(&b"foo\x00bar"[..])),
                Bytes::from_static(&b"\x01foo\x00\xFFbar\x00"[..]),
            );
            assert_eq!(
                byte_string(Bytes::new()),
                Bytes::from_static(&b"\x01\x00"[..]),
            );
            assert_eq!(
                byte_string(Bytes::from_static(&b"\x01\x02\x03"[..])),
                Bytes::from_static(&b"\x01\x01\x02\x03\x00"[..]),
            );
            assert_eq!(
                byte_string(Bytes::from_static(&b"\x00\x00\x00\x04"[..])),
                Bytes::from_static(&b"\x01\x00\xFF\x00\xFF\x00\xFF\x04\x00"[..]),
            );
        }

        #[test]
        fn test_unicode_string() {
            assert_eq!(
                unicode_string("F\u{00d4}O\u{0000}bar".to_string()),
                Bytes::from_static(&b"\x02F\xC3\x94O\x00\xffbar\x00"[..]),
            );
            assert_eq!(
                unicode_string("".to_string()),
                Bytes::from_static(&b"\x02\x00"[..]),
            );
            assert_eq!(
                unicode_string("hello".to_string()),
                Bytes::from_static(&b"\x02hello\x00"[..]),
            );
            assert_eq!(
                unicode_string("中文".to_string()),
                Bytes::from_static(&b"\x02\xE4\xB8\xAD\xE6\x96\x87\x00"[..]),
            );
            assert_eq!(
                unicode_string("μάθημα".to_string()),
                Bytes::from_static(
                    &b"\x02\xCE\xBC\xCE\xAC\xCE\xB8\xCE\xB7\xCE\xBC\xCE\xB1\x00"[..]
                ),
            );
        }

        #[test]
        fn test_nested_tuple() {
            assert_eq!(
                nested_tuple(Tuple::from_elements(vec![TupleValue::NullValue])),
                Bytes::from_static(&b"\x05\x00\xFF\x00"[..])
            );
            assert_eq!(
                nested_tuple(Tuple::from_elements(vec![
                    TupleValue::NullValue,
                    TupleValue::UnicodeString("hello".to_string()),
                ])),
                Bytes::from_static(&b"\x05\x00\xFF\x02hello\x00\x00"[..])
            );
            assert_eq!(
                nested_tuple(Tuple::from_elements(vec![
                    TupleValue::NullValue,
                    TupleValue::UnicodeString("hell\u{0}".to_string()),
                ])),
                Bytes::from_static(&b"\x05\x00\xFF\x02hell\x00\xFF\x00\x00"[..])
            );
        }

        #[test]
        fn test_negative_arbitrary_precision_integer() {
            assert_eq!(
                negative_arbitrary_precision_integer(
                    BigInt::parse_bytes(b"18446744073709551616", 10).unwrap()
                ),
                Bytes::from_static(&b"\x0B\xF6\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..]),
            );
        }

        #[test]
        fn test_neg_int_8() {
            assert_eq!(
                neg_int_8(18446744073709551615),
                Bytes::from_static(&b"\x0C\x00\x00\x00\x00\x00\x00\x00\x00"[..]),
            );
            assert_eq!(
                neg_int_8(72057594037927936),
                Bytes::from_static(&b"\x0C\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..]),
            );
            assert_eq!(
                neg_int_8(9223372036854775809),
                Bytes::from_static(&b"\x0C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFE"[..]),
            );
            assert_eq!(
                neg_int_8(9223372036854775808),
                Bytes::from_static(&b"\x0C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..]),
            );
        }

        #[test]
        fn test_neg_int_7() {
            assert_eq!(
                neg_int_7(72057594037927935),
                Bytes::from_static(&b"\x0D\x00\x00\x00\x00\x00\x00\x00"[..]),
            );
            assert_eq!(
                neg_int_7(281474976710656),
                Bytes::from_static(&b"\x0D\xFE\xFF\xFF\xFF\xFF\xFF\xFF"[..]),
            );
        }

        #[test]
        fn test_neg_int_6() {
            assert_eq!(
                neg_int_6(281474976710655),
                Bytes::from_static(&b"\x0E\x00\x00\x00\x00\x00\x00"[..]),
            );
            assert_eq!(
                neg_int_6(1099511627776),
                Bytes::from_static(&b"\x0E\xFE\xFF\xFF\xFF\xFF\xFF"[..]),
            );
        }

        #[test]
        fn test_neg_int_5() {
            assert_eq!(
                neg_int_5(1099511627775),
                Bytes::from_static(&b"\x0F\x00\x00\x00\x00\x00"[..]),
            );
            assert_eq!(
                neg_int_5(4294967296),
                Bytes::from_static(&b"\x0F\xFE\xFF\xFF\xFF\xFF"[..]),
            );
        }

        #[test]
        fn test_neg_int_4() {
            assert_eq!(
                neg_int_4(4294967295),
                Bytes::from_static(&b"\x10\x00\x00\x00\x00"[..]),
            );
            assert_eq!(
                neg_int_4(16777216),
                Bytes::from_static(&b"\x10\xFE\xFF\xFF\xFF"[..]),
            );
            assert_eq!(
                neg_int_4(2147483649),
                Bytes::from_static(&b"\x10\x7F\xFF\xFF\xFE"[..]),
            );
            assert_eq!(
                neg_int_4(2147483648),
                Bytes::from_static(&b"\x10\x7F\xFF\xFF\xFF"[..]),
            );
        }

        #[test]
        fn test_neg_int_3() {
            assert_eq!(
                neg_int_3(16777215),
                Bytes::from_static(&b"\x11\x00\x00\x00"[..]),
            );
            assert_eq!(
                neg_int_3(65536),
                Bytes::from_static(&b"\x11\xFE\xFF\xFF"[..]),
            );
        }

        #[test]
        fn test_neg_int_2() {
            assert_eq!(neg_int_2(65535), Bytes::from_static(&b"\x12\x00\x00"[..]));
            assert_eq!(neg_int_2(256), Bytes::from_static(&b"\x12\xFE\xFF"[..]));
            assert_eq!(neg_int_2(32769), Bytes::from_static(&b"\x12\x7F\xFE"[..]));
            assert_eq!(neg_int_2(32768), Bytes::from_static(&b"\x12\x7F\xFF"[..]));
        }

        #[test]
        fn test_neg_int_1() {
            assert_eq!(neg_int_1(255), Bytes::from_static(&b"\x13\x00"[..]));
            assert_eq!(neg_int_1(1), Bytes::from_static(&b"\x13\xFE"[..]));
            assert_eq!(neg_int_1(129), Bytes::from_static(&b"\x13\x7E"[..]));
            assert_eq!(neg_int_1(128), Bytes::from_static(&b"\x13\x7F"[..]));
        }

        #[test]
        fn test_int_zero() {
            assert_eq!(int_zero(), Bytes::from_static(&b"\x14"[..]));
        }

        #[test]
        fn test_pos_int_1() {
            assert_eq!(pos_int_1(1), Bytes::from_static(&b"\x15\x01"[..]));
            assert_eq!(pos_int_1(255), Bytes::from_static(&b"\x15\xFF"[..]));
            assert_eq!(pos_int_1(127), Bytes::from_static(&b"\x15\x7F"[..]));
            assert_eq!(pos_int_1(128), Bytes::from_static(&b"\x15\x80"[..]));
        }

        #[test]
        fn test_pos_int_2() {
            assert_eq!(pos_int_2(256), Bytes::from_static(&b"\x16\x01\x00"[..]));
            assert_eq!(pos_int_2(65535), Bytes::from_static(&b"\x16\xFF\xFF"[..]));
            assert_eq!(pos_int_2(32767), Bytes::from_static(&b"\x16\x7F\xFF"[..]));
            assert_eq!(pos_int_2(32768), Bytes::from_static(&b"\x16\x80\x00"[..]));
        }

        #[test]
        fn test_pos_int_3() {
            assert_eq!(
                pos_int_3(65536),
                Bytes::from_static(&b"\x17\x01\x00\x00"[..])
            );
            assert_eq!(
                pos_int_3(16777215),
                Bytes::from_static(&b"\x17\xFF\xFF\xFF"[..])
            );
        }

        #[test]
        fn test_pos_int_4() {
            assert_eq!(
                pos_int_4(16777216),
                Bytes::from_static(&b"\x18\x01\x00\x00\x00"[..])
            );
            assert_eq!(
                pos_int_4(4294967295),
                Bytes::from_static(&b"\x18\xFF\xFF\xFF\xFF"[..])
            );
            assert_eq!(
                pos_int_4(2147483647),
                Bytes::from_static(&b"\x18\x7F\xFF\xFF\xFF"[..])
            );
            assert_eq!(
                pos_int_4(2147483648),
                Bytes::from_static(&b"\x18\x80\x00\x00\x00"[..])
            );
        }

        #[test]
        fn test_pos_int_5() {
            assert_eq!(
                pos_int_5(4294967296),
                Bytes::from_static(&b"\x19\x01\x00\x00\x00\x00"[..])
            );
            assert_eq!(
                pos_int_5(1099511627775),
                Bytes::from_static(&b"\x19\xFF\xFF\xFF\xFF\xFF"[..])
            );
        }

        #[test]
        fn test_pos_int_6() {
            assert_eq!(
                pos_int_6(1099511627776),
                Bytes::from_static(&b"\x1A\x01\x00\x00\x00\x00\x00"[..])
            );
            assert_eq!(
                pos_int_6(281474976710655),
                Bytes::from_static(&b"\x1A\xFF\xFF\xFF\xFF\xFF\xFF"[..])
            );
        }

        #[test]
        fn test_pos_int_7() {
            assert_eq!(
                pos_int_7(281474976710656),
                Bytes::from_static(&b"\x1B\x01\x00\x00\x00\x00\x00\x00"[..])
            );
            assert_eq!(
                pos_int_7(72057594037927935),
                Bytes::from_static(&b"\x1B\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..])
            );
        }

        #[test]
        fn test_pos_int_8() {
            assert_eq!(
                pos_int_8(72057594037927936),
                Bytes::from_static(&b"\x1C\x01\x00\x00\x00\x00\x00\x00\x00"[..])
            );
            assert_eq!(
                pos_int_8(18446744073709551615),
                Bytes::from_static(&b"\x1C\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..])
            );
            assert_eq!(
                pos_int_8(9223372036854775807),
                Bytes::from_static(&b"\x1C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..])
            );
            assert_eq!(
                pos_int_8(9223372036854775808),
                Bytes::from_static(&b"\x1C\x80\x00\x00\x00\x00\x00\x00\x00"[..])
            );
        }

        #[test]
        fn test_positive_arbitrary_precision_integer() {
            assert_eq!(
                positive_arbitrary_precision_integer(
                    BigInt::parse_bytes(b"18446744073709551616", 10).unwrap()
                ),
                Bytes::from_static(&b"\x1D\x09\x01\x00\x00\x00\x00\x00\x00\x00\x00"[..])
            );
        }

        // `3.14` is copied from Java binding tests
        #[allow(clippy::approx_constant)]
        #[test]
        fn test_ieee_binary_floating_point_float() {
            assert_eq!(
                ieee_binary_floating_point_float(3.14f32),
                Bytes::from_static(&b"\x20\xC0\x48\xF5\xC3"[..])
            );
            assert_eq!(
                ieee_binary_floating_point_float(-3.14f32),
                Bytes::from_static(&b"\x20\x3F\xB7\x0A\x3C"[..])
            );
            assert_eq!(
                ieee_binary_floating_point_float(0.0f32),
                Bytes::from_static(&b"\x20\x80\x00\x00\x00"[..])
            );
            assert_eq!(
                ieee_binary_floating_point_float(-0.0f32),
                Bytes::from_static(&b"\x20\x7F\xFF\xFF\xFF"[..])
            );
            assert_eq!(
                ieee_binary_floating_point_float(f32::INFINITY),
                Bytes::from_static(&b"\x20\xFF\x80\x00\x00"[..])
            );
            assert_eq!(
                ieee_binary_floating_point_float(f32::NEG_INFINITY),
                Bytes::from_static(&b"\x20\x00\x7F\xFF\xFF"[..])
            );
        }

        // `3.14` is copied from Java binding tests
        #[allow(clippy::approx_constant)]
        #[test]
        fn test_ieee_binary_floating_point_double() {
            assert_eq!(
                ieee_binary_floating_point_double(3.14f64),
                Bytes::from_static(&b"\x21\xC0\x09\x1E\xB8\x51\xEB\x85\x1F"[..])
            );
            assert_eq!(
                ieee_binary_floating_point_double(-3.14f64),
                Bytes::from_static(&b"\x21\x3F\xF6\xE1\x47\xAE\x14\x7A\xE0"[..])
            );
            assert_eq!(
                ieee_binary_floating_point_double(0.0f64),
                Bytes::from_static(&b"\x21\x80\x00\x00\x00\x00\x00\x00\x00"[..])
            );
            assert_eq!(
                ieee_binary_floating_point_double(-0.0f64),
                Bytes::from_static(&b"\x21\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..])
            );
            assert_eq!(
                ieee_binary_floating_point_double(f64::INFINITY),
                Bytes::from_static(&b"\x21\xFF\xF0\x00\x00\x00\x00\x00\x00"[..])
            );
            assert_eq!(
                ieee_binary_floating_point_double(f64::NEG_INFINITY),
                Bytes::from_static(&b"\x21\x00\x0F\xFF\xFF\xFF\xFF\xFF\xFF"[..])
            );
        }

        #[test]
        fn test_false_value() {
            assert_eq!(false_value(), Bytes::from_static(&b"\x26"[..]));
        }

        #[test]
        fn test_true_value() {
            assert_eq!(true_value(), Bytes::from_static(&b"\x27"[..]));
        }

        #[test]
        fn test_rfc_4122_uuid() {
            assert_eq!(
                rfc_4122_uuid(Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap()),
                Bytes::from_static(
                    &b"\x30\xFF\xFF\xFF\xFF\xBA\x5E\xBA\x11\x00\x00\x00\x00\x5C\xA1\xAB\x1E"[..]
                )
            );
        }

        #[test]
        fn test_versionstamp_96_bit() {
            assert_eq!(
                versionstamp_96_bit(Versionstamp::complete(
                    Bytes::from_static(&b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"[..]),
                    0
                )),
                Bytes::from_static(&b"\x33\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03\x00\x00"[..])
            );
            assert_eq!(
                versionstamp_96_bit(Versionstamp::complete(
                    Bytes::from_static(&b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A"[..]),
                    657
                )),
                Bytes::from_static(&b"\x33\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x02\x91"[..])
            );
        }
    }
}

pub(self) mod parser {
    use bytes::{Buf, Bytes};
    use nom::error::{Error, ErrorKind};
    use nom::{bytes as nom_bytes, combinator, multi, number, sequence, IResult};
    use num_bigint::{BigInt, Sign};
    use uuid::Uuid;

    use crate::tuple::{Tuple, Versionstamp};

    use super::{
        utils::{extract_unpacked_bytes, neg_u8_slice_into_vec},
        TupleValue,
    };

    pub(crate) fn tuple(mut i: &[u8]) -> IResult<&[u8], Tuple> {
        let mut res = Vec::new();

        loop {
            match i.len() {
                0 => return Ok((i, Tuple::from_elements(res))),
                _ => {
                    let (i1, tv) = match i[0] {
                        b'\x00' => null_value(i),
                        b'\x01' => byte_string(i),
                        b'\x02' => unicode_string(i),
                        b'\x05' => nested_tuple(i),
                        b'\x0B' => negative_arbitrary_precision_integer(i),
                        b'\x0C' => neg_int_8(i),
                        b'\x0D' => neg_int_7(i),
                        b'\x0E' => neg_int_6(i),
                        b'\x0F' => neg_int_5(i),
                        b'\x10' => neg_int_4(i),
                        b'\x11' => neg_int_3(i),
                        b'\x12' => neg_int_2(i),
                        b'\x13' => neg_int_1(i),
                        b'\x14' => int_zero(i),
                        b'\x15' => pos_int_1(i),
                        b'\x16' => pos_int_2(i),
                        b'\x17' => pos_int_3(i),
                        b'\x18' => pos_int_4(i),
                        b'\x19' => pos_int_5(i),
                        b'\x1A' => pos_int_6(i),
                        b'\x1B' => pos_int_7(i),
                        b'\x1C' => pos_int_8(i),
                        b'\x1D' => positive_arbitrary_precision_integer(i),
                        b'\x20' => ieee_binary_floating_point_float(i),
                        b'\x21' => ieee_binary_floating_point_double(i),
                        b'\x26' => false_value(i),
                        b'\x27' => true_value(i),
                        b'\x30' => rfc_4122_uuid(i),
                        b'\x33' => versionstamp_96_bit(i),
                        _ => Err(nom::Err::Error(nom::error::Error::new(
                            i,
                            nom::error::ErrorKind::Fail,
                        ))),
                    }?;

                    // continue looping
                    res.push(tv);
                    i = i1;
                }
            }
        }
    }

    fn null_value(i: &[u8]) -> IResult<&[u8], TupleValue> {
        combinator::map(nom_bytes::complete::tag(&b"\x00"[..]), |_| {
            TupleValue::NullValue
        })(i)
    }

    // Null values inside a nested tuple are represented differently.
    fn nested_tuple_null_value(i: &[u8]) -> IResult<&[u8], TupleValue> {
        combinator::map(nom_bytes::complete::tag(&b"\x00\xFF"[..]), |_| {
            TupleValue::NullValue
        })(i)
    }

    fn byte_string(i: &[u8]) -> IResult<&[u8], TupleValue> {
        let (i1, _) = nom_bytes::complete::tag(&b"\x01"[..])(i)?;

        let (res_input, res_output) = extract_unpacked_bytes(i1)?;

        Ok((res_input, TupleValue::ByteString(res_output)))
    }

    fn unicode_string(i: &[u8]) -> IResult<&[u8], TupleValue> {
        let (i1, _) = nom_bytes::complete::tag(&b"\x02"[..])(i)?;

        let (res_input, res_bytes) = extract_unpacked_bytes(i1)?;

        let res_string = String::from_utf8((&res_bytes[..]).to_vec())
            .map_err(|_| nom::Err::Error(Error::new(res_input, ErrorKind::Fail)))?;

        Ok((res_input, TupleValue::UnicodeString(res_string)))
    }

    fn nested_tuple(i: &[u8]) -> IResult<&[u8], TupleValue> {
        #[derive(Debug)]
        enum NestedTuple<'a> {
            Value((&'a [u8], TupleValue)),
            End(&'a [u8]),
            NoParserFound(&'a [u8]),
        }

        let mut res = Vec::new();
        let (mut i1, _) = nom_bytes::complete::tag(&b"\x05"[..])(i)?;

        loop {
            match i1.len() {
                0 => {
                    // We have a premature ending. Return an error.
                    return Err(nom::Err::Error(nom::error::Error::new(
                        i1,
                        nom::error::ErrorKind::Eof,
                    )));
                }
                1 if i1[0] == b'\x00' => {
                    return Ok((
                        // Consume the last remaining '\x00', and return the existing vec.
                        &i1[1..],
                        TupleValue::NestedTuple(Tuple::from_elements(res)),
                    ));
                }
                _ => {
                    let val = if i1[0..=1] == b"\x00\xFF"[..] {
                        nested_tuple_null_value(i1).map(NestedTuple::Value)
                    } else {
                        match i1[0] {
                            b'\x00' => Ok(NestedTuple::End(&i1[1..])),
                            b'\x01' => byte_string(i1).map(NestedTuple::Value),
                            b'\x02' => unicode_string(i1).map(NestedTuple::Value),
                            b'\x05' => nested_tuple(i1).map(NestedTuple::Value),
                            b'\x0B' => {
                                negative_arbitrary_precision_integer(i1).map(NestedTuple::Value)
                            }
                            b'\x0C' => neg_int_8(i1).map(NestedTuple::Value),
                            b'\x0D' => neg_int_7(i1).map(NestedTuple::Value),
                            b'\x0E' => neg_int_6(i1).map(NestedTuple::Value),
                            b'\x0F' => neg_int_5(i1).map(NestedTuple::Value),
                            b'\x10' => neg_int_4(i1).map(NestedTuple::Value),
                            b'\x11' => neg_int_3(i1).map(NestedTuple::Value),
                            b'\x12' => neg_int_2(i1).map(NestedTuple::Value),
                            b'\x13' => neg_int_1(i1).map(NestedTuple::Value),
                            b'\x14' => int_zero(i1).map(NestedTuple::Value),
                            b'\x15' => pos_int_1(i1).map(NestedTuple::Value),
                            b'\x16' => pos_int_2(i1).map(NestedTuple::Value),
                            b'\x17' => pos_int_3(i1).map(NestedTuple::Value),
                            b'\x18' => pos_int_4(i1).map(NestedTuple::Value),
                            b'\x19' => pos_int_5(i1).map(NestedTuple::Value),
                            b'\x1A' => pos_int_6(i1).map(NestedTuple::Value),
                            b'\x1B' => pos_int_7(i1).map(NestedTuple::Value),
                            b'\x1C' => pos_int_8(i1).map(NestedTuple::Value),
                            b'\x1D' => {
                                positive_arbitrary_precision_integer(i1).map(NestedTuple::Value)
                            }

                            b'\x20' => ieee_binary_floating_point_float(i1).map(NestedTuple::Value),
                            b'\x21' => {
                                ieee_binary_floating_point_double(i1).map(NestedTuple::Value)
                            }
                            b'\x26' => false_value(i1).map(NestedTuple::Value),
                            b'\x27' => true_value(i1).map(NestedTuple::Value),
                            b'\x30' => rfc_4122_uuid(i1).map(NestedTuple::Value),
                            b'\x33' => versionstamp_96_bit(i1).map(NestedTuple::Value),
                            _ => Ok(NestedTuple::NoParserFound(i1)),
                        }
                    }?;

                    match val {
                        NestedTuple::Value((i2, tv)) => {
                            // continue looping
                            res.push(tv);
                            i1 = i2;
                        }
                        NestedTuple::End(i2) => {
                            return Ok((i2, TupleValue::NestedTuple(Tuple::from_elements(res))));
                        }
                        NestedTuple::NoParserFound(i2) => {
                            return Err(nom::Err::Error(nom::error::Error::new(
                                i2,
                                nom::error::ErrorKind::Fail,
                            )));
                        }
                    }
                }
            }
        }
    }

    fn negative_arbitrary_precision_integer(i: &[u8]) -> IResult<&[u8], TupleValue> {
        let (i1, _) = nom_bytes::complete::tag(&b"\x0B"[..])(i)?;
        let (i2, o2) = nom_bytes::complete::take(1u8)(i1)?;
        let len = o2[0] ^ 0xFFu8;

        // NOTE: Within
        // `TupleValue::NegativeArbitraryPrecisionInteger`, we
        // maintain the negative `BigInt` in the positive (unsigned)
        // form.
        combinator::map(nom_bytes::complete::take(len), |x: &[u8]| {
            TupleValue::NegativeArbitraryPrecisionInteger(BigInt::from_bytes_be(
                Sign::Plus,
                &neg_u8_slice_into_vec(x)[..],
            ))
        })(i2)
    }

    fn neg_int_8(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x0C"[..]),
            combinator::map(nom_bytes::complete::take(8u8), |x: &[u8]| {
                TupleValue::NegInt8((&neg_u8_slice_into_vec(x)[..]).get_u64())
            }),
        )(i)
    }

    fn neg_int_7(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x0D"[..]),
            combinator::map(nom_bytes::complete::take(7u8), |x: &[u8]| {
                let mut val = vec![0xFFu8];
                val.extend_from_slice(x);
                TupleValue::NegInt7((&neg_u8_slice_into_vec(&val[..])[..]).get_u64())
            }),
        )(i)
    }

    fn neg_int_6(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x0E"[..]),
            combinator::map(nom_bytes::complete::take(6u8), |x: &[u8]| {
                let mut val = vec![0xFFu8, 0xFFu8];
                val.extend_from_slice(x);
                TupleValue::NegInt6((&neg_u8_slice_into_vec(&val[..])[..]).get_u64())
            }),
        )(i)
    }

    fn neg_int_5(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x0F"[..]),
            combinator::map(nom_bytes::complete::take(5u8), |x: &[u8]| {
                let mut val = vec![0xFFu8, 0xFFu8, 0xFFu8];
                val.extend_from_slice(x);
                TupleValue::NegInt5((&neg_u8_slice_into_vec(&val[..])[..]).get_u64())
            }),
        )(i)
    }

    fn neg_int_4(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x10"[..]),
            combinator::map(nom_bytes::complete::take(4u8), |x: &[u8]| {
                TupleValue::NegInt4((&neg_u8_slice_into_vec(x)[..]).get_u32())
            }),
        )(i)
    }

    fn neg_int_3(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x11"[..]),
            combinator::map(nom_bytes::complete::take(3u8), |x: &[u8]| {
                let mut val = vec![0xFFu8];
                val.extend_from_slice(x);
                TupleValue::NegInt3((&neg_u8_slice_into_vec(&val[..])[..]).get_u32())
            }),
        )(i)
    }

    fn neg_int_2(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x12"[..]),
            combinator::map(nom_bytes::complete::take(2u8), |x: &[u8]| {
                TupleValue::NegInt2((&neg_u8_slice_into_vec(x)[..]).get_u16())
            }),
        )(i)
    }

    fn neg_int_1(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x13"[..]),
            combinator::map(nom_bytes::complete::take(1u8), |x: &[u8]| {
                TupleValue::NegInt1((&neg_u8_slice_into_vec(x)[..]).get_u8())
            }),
        )(i)
    }

    fn int_zero(i: &[u8]) -> IResult<&[u8], TupleValue> {
        combinator::map(nom_bytes::complete::tag(&b"\x14"[..]), |_| {
            TupleValue::IntZero
        })(i)
    }

    fn pos_int_1(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x15"[..]),
            combinator::map(nom_bytes::complete::take(1u8), |mut x: &[u8]| {
                TupleValue::PosInt1(x.get_u8())
            }),
        )(i)
    }

    fn pos_int_2(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x16"[..]),
            combinator::map(nom_bytes::complete::take(2u8), |mut x: &[u8]| {
                TupleValue::PosInt2(x.get_u16())
            }),
        )(i)
    }

    fn pos_int_3(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x17"[..]),
            combinator::map(nom_bytes::complete::take(3u8), |x: &[u8]| {
                let mut val = vec![0u8];
                val.extend_from_slice(x);
                TupleValue::PosInt3((&val[..]).get_u32())
            }),
        )(i)
    }

    fn pos_int_4(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x18"[..]),
            combinator::map(nom_bytes::complete::take(4u8), |mut x: &[u8]| {
                TupleValue::PosInt4(x.get_u32())
            }),
        )(i)
    }

    fn pos_int_5(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x19"[..]),
            combinator::map(nom_bytes::complete::take(5u8), |x: &[u8]| {
                let mut val = vec![0u8, 0u8, 0u8];
                val.extend_from_slice(x);
                TupleValue::PosInt5((&val[..]).get_u64())
            }),
        )(i)
    }

    fn pos_int_6(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x1A"[..]),
            combinator::map(nom_bytes::complete::take(6u8), |x: &[u8]| {
                let mut val = vec![0u8, 0u8];
                val.extend_from_slice(x);
                TupleValue::PosInt6((&val[..]).get_u64())
            }),
        )(i)
    }

    fn pos_int_7(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x1B"[..]),
            combinator::map(nom_bytes::complete::take(7u8), |x: &[u8]| {
                let mut val = vec![0u8];
                val.extend_from_slice(x);
                TupleValue::PosInt7((&val[..]).get_u64())
            }),
        )(i)
    }

    fn pos_int_8(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x1C"[..]),
            combinator::map(nom_bytes::complete::take(8u8), |mut x: &[u8]| {
                TupleValue::PosInt8(x.get_u64())
            }),
        )(i)
    }

    fn positive_arbitrary_precision_integer(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x1D"[..]),
            combinator::map(multi::length_data(number::complete::be_u8), |x| {
                TupleValue::PositiveArbitraryPrecisionInteger(BigInt::from_bytes_be(Sign::Plus, x))
            }),
        )(i)
    }

    fn ieee_binary_floating_point_float(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x20"[..]),
            combinator::map(nom_bytes::complete::take(4u8), |x: &[u8]| {
                if x[0] & 0x80 == 0x00 {
                    // Negative number. Flip all the bytes.
                    let mut res = Vec::new();
                    x.iter().for_each(|y| res.push(*y ^ 0xFF));
                    TupleValue::IeeeBinaryFloatingPointFloat((&res[..]).get_f32())
                } else {
                    // Positive number. Flip just the sign bit.
                    let mut res = vec![(x[0] ^ 0x80)];
                    x[1..].iter().for_each(|y| res.push(*y));
                    TupleValue::IeeeBinaryFloatingPointFloat((&res[..]).get_f32())
                }
            }),
        )(i)
    }

    fn ieee_binary_floating_point_double(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x21"[..]),
            combinator::map(nom_bytes::complete::take(8u8), |x: &[u8]| {
                if x[0] & 0x80 == 0x00 {
                    // Negative number. Flip all the bytes.
                    let mut res = Vec::new();
                    x.iter().for_each(|y| res.push(*y ^ 0xFF));
                    TupleValue::IeeeBinaryFloatingPointDouble((&res[..]).get_f64())
                } else {
                    // Positive number. Flip just the sign bit.
                    let mut res = vec![(x[0] ^ 0x80)];
                    x[1..].iter().for_each(|y| res.push(*y));
                    TupleValue::IeeeBinaryFloatingPointDouble((&res[..]).get_f64())
                }
            }),
        )(i)
    }

    fn false_value(i: &[u8]) -> IResult<&[u8], TupleValue> {
        combinator::map(nom_bytes::complete::tag(&b"\x26"[..]), |_| {
            TupleValue::FalseValue
        })(i)
    }

    fn true_value(i: &[u8]) -> IResult<&[u8], TupleValue> {
        combinator::map(nom_bytes::complete::tag(&b"\x27"[..]), |_| {
            TupleValue::TrueValue
        })(i)
    }

    fn rfc_4122_uuid(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x30"[..]),
            combinator::map(nom_bytes::complete::take(16u8), |x: &[u8]| {
                // It is safe to unwrap, because we are taking 16 bytes.
                TupleValue::Rfc4122Uuid(Uuid::from_slice(x).unwrap())
            }),
        )(i)
    }

    fn versionstamp_96_bit(i: &[u8]) -> IResult<&[u8], TupleValue> {
        sequence::preceded(
            nom_bytes::complete::tag(&b"\x33"[..]),
            combinator::map(nom_bytes::complete::take(12u8), |x: &[u8]| {
                // `from_bytes` won't panic because we are taking 12 bytes.
                TupleValue::Versionstamp96Bit(Versionstamp::from_bytes(Bytes::from(x.to_vec())))
            }),
        )(i)
    }

    #[cfg(test)]
    mod tests {
        use super::{
            byte_string, false_value, ieee_binary_floating_point_double,
            ieee_binary_floating_point_float, int_zero, neg_int_1, neg_int_2, neg_int_3, neg_int_4,
            neg_int_5, neg_int_6, neg_int_7, neg_int_8, negative_arbitrary_precision_integer,
            nested_tuple, nested_tuple_null_value, null_value, pos_int_1, pos_int_2, pos_int_3,
            pos_int_4, pos_int_5, pos_int_6, pos_int_7, pos_int_8,
            positive_arbitrary_precision_integer, rfc_4122_uuid, true_value, tuple, unicode_string,
            versionstamp_96_bit, TupleValue,
        };
        use crate::tuple::{Tuple, Versionstamp};
        use bytes::Bytes;
        use nom::error::{Error, ErrorKind};
        use num_bigint::BigInt;
        use std::num::NonZeroUsize;
        use uuid::Uuid;

        // For this test, we build the `Tuple` using its public APIs
        // (instead of `Tuple::from_elements`).
        //
        // This test is adapted from
        // https://github.com/apple/foundationdb/blob/a167bf344e87946376f6e02243e37c831c7f7299/bindings/java/src/test/com/apple/foundationdb/test/TupleTest.java#L92-L166
        //
        // Following does not work in Rust.
        //
        // `\x20\xFF\xFF\xFF\xFF`
        // `\x20\x00\x00\x00\x00`
        // `\x21\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF`
        // `\x21\x00\x00\x00\x00\x00\x00\x00\x00`
        //
        // `\x02\xF0\x9F\x94\xA5\x00`
        // `\x02\xF0\x9F\xA5\xAF\x00`
        // `\x02\xF0\x9F\xA6\xA5\x00`
        //
        // `3.14` is copied from Java binding tests
        #[allow(clippy::approx_constant)]
        #[test]
        fn test_tuple() {
            assert_eq!(
                tuple(&b"\x00moredata"[..]),
                Err(nom::Err::Error(nom::error::Error::new(
                    &b"moredata"[..],
                    nom::error::ErrorKind::Fail
                )))
            );
            assert_eq!(
                tuple(&b"no_tuple"[..]),
                Err(nom::Err::Error(nom::error::Error::new(
                    &b"no_tuple"[..],
                    nom::error::ErrorKind::Fail
                )))
            );
            assert_eq!(tuple(&b""[..]), Ok((&b""[..], Tuple::new())));
            assert_eq!(
                tuple(&b"\x14"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_i64(0);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x14"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bigint(BigInt::parse_bytes(b"0", 10).unwrap());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x15\x01"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_i64(1);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x15\x01"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bigint(BigInt::parse_bytes(b"1", 10).unwrap());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x13\xFE"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_i64(-1);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x13\xFE"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bigint(BigInt::parse_bytes(b"-1", 10).unwrap());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x15\xFF"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_i64(255);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x15\xFF"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bigint(BigInt::parse_bytes(b"255", 10).unwrap());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x13\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_i64(-255);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x13\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bigint(BigInt::parse_bytes(b"-255", 10).unwrap());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x16\x01\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_i64(256);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x16\x01\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bigint(BigInt::parse_bytes(b"256", 10).unwrap());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x17\x01\x00\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_i32(65536);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x11\xFE\xFF\xFF"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_i32(-65536);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x1C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_i64(i64::MAX);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x1C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bigint(BigInt::parse_bytes(b"9223372036854775807", 10).unwrap());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x1C\x80\x00\x00\x00\x00\x00\x00\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bigint(BigInt::parse_bytes(b"9223372036854775808", 10).unwrap());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x1C\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bigint(BigInt::parse_bytes(b"18446744073709551615", 10).unwrap());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x1D\x09\x01\x00\x00\x00\x00\x00\x00\x00\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bigint(BigInt::parse_bytes(b"18446744073709551616", 10).unwrap());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x10\x00\x00\x00\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_i64(-4294967295);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x10\x00\x00\x00\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bigint(BigInt::parse_bytes(b"-4294967295", 10).unwrap());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x0C\x80\x00\x00\x00\x00\x00\x00\x01"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_i64(i64::MIN + 2);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x0C\x80\x00\x00\x00\x00\x00\x00\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_i64(i64::MIN + 1);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x0C\x80\x00\x00\x00\x00\x00\x00\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bigint(
                        // i64::MIN + 1
                        BigInt::parse_bytes(b"-9223372036854775808", 10).unwrap() + 1,
                    );
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x0C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_i64(i64::MIN);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x0C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bigint(
                        // i64::MIN
                        BigInt::parse_bytes(b"-9223372036854775808", 10).unwrap(),
                    );
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x0C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFE"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bigint(
                        // i64::MIN - 1
                        BigInt::parse_bytes(b"-9223372036854775808", 10).unwrap() - 1,
                    );
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x0C\x00\x00\x00\x00\x00\x00\x00\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bigint(BigInt::parse_bytes(b"-18446744073709551615", 10).unwrap());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x20\xC0\x48\xF5\xC3"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_f32(3.14f32);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x20\x3F\xB7\x0A\x3C"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_f32(-3.14f32);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x21\xC0\x09\x1E\xB8\x51\xEB\x85\x1F"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_f64(3.14f64);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x21\x3F\xF6\xE1\x47\xAE\x14\x7A\xE0"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_f64(-3.14f64);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x20\x80\x00\x00\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_f32(0.0f32);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x20\x7F\xFF\xFF\xFF"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_f32(-0.0f32);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x21\x80\x00\x00\x00\x00\x00\x00\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_f64(0.0f64);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x21\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_f64(-0.0f64);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x20\xFF\x80\x00\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_f32(f32::INFINITY);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x20\x00\x7F\xFF\xFF"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_f32(f32::NEG_INFINITY);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x21\xFF\xF0\x00\x00\x00\x00\x00\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_f64(f64::INFINITY);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x21\x00\x0F\xFF\xFF\xFF\xFF\xFF\xFF"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_f64(f64::NEG_INFINITY);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x01\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bytes(Bytes::new());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x01\x01\x02\x03\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bytes(Bytes::from_static(&b"\x01\x02\x03"[..]));
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x01\x00\xFF\x00\xFF\x00\xFF\x04\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bytes(Bytes::from_static(&b"\x00\x00\x00\x04"[..]));
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x02\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_string("".to_string());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x02hello\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_string("hello".to_string());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x02\xE4\xB8\xAD\xE6\x96\x87\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_string("中文".to_string());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x02\xCE\xBC\xCE\xAC\xCE\xB8\xCE\xB7\xCE\xBC\xCE\xB1\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_string("μάθημα".to_string());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x02\xF4\x8F\xBF\xBF\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_string("\u{10ffff}".to_string());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x05\x00\xFF\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_tuple({
                        let mut t1 = Tuple::new();
                        t1.add_null();
                        t1
                    });
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x05\x00\xFF\x02hello\x00\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_tuple({
                        let mut t1 = Tuple::new();
                        t1.add_null();
                        t1.add_string("hello".to_string());
                        t1
                    });
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x05\x00\xFF\x02hell\x00\xFF\x00\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_tuple({
                        let mut t1 = Tuple::new();
                        t1.add_null();
                        t1.add_string("hell\x00".to_string());
                        t1
                    });
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x05\x00\xFF\x00\x02hello\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_tuple({
                        let mut t1 = Tuple::new();
                        t1.add_null();
                        t1
                    });
                    t.add_string("hello".to_string());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x05\x00\xFF\x00\x02hello\x00\x01\x01\x00\xFF\x00\x01\x00"[..]),
                Ok((&b""[..], {
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
                }))
            );
            assert_eq!(
                tuple(&b"\x30\xFF\xFF\xFF\xFF\xBA\x5E\xBA\x11\x00\x00\x00\x00\x5C\xA1\xAB\x1E"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_uuid(Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap());
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x26"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bool(false);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x27"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_bool(true);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x15\x03"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_i8(3);
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x33\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03\x00\x00"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_versionstamp(Versionstamp::complete(
                        Bytes::from_static(&b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"[..]),
                        0,
                    ));
                    t
                }))
            );
            assert_eq!(
                tuple(&b"\x33\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x02\x91"[..]),
                Ok((&b""[..], {
                    let mut t = Tuple::new();
                    t.add_versionstamp(Versionstamp::complete(
                        Bytes::from_static(&b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A"[..]),
                        657,
                    ));
                    t
                }))
            );
        }

        #[test]
        fn test_null_value() {
            assert_eq!(
                null_value(&b"\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NullValue))
            );
            assert_eq!(
                null_value(&b"no_null_value"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_null_value"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::null_value(null_value(&b"\x00moredata"[..]).unwrap().1),
                Ok(())
            );
        }

        #[test]
        fn test_nested_tuple_null_value() {
            assert_eq!(
                nested_tuple_null_value(&b"\x00\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NullValue))
            );
            assert_eq!(
                null_value(&b"no_nested_tuple_null_value"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_nested_tuple_null_value"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::nested_tuple_null_value(
                    nested_tuple_null_value(&b"\x00\xFFmoredata"[..]).unwrap().1
                ),
                Ok(())
            );
        }

        #[test]
        fn test_byte_string() {
            assert_eq!(
                byte_string(&b"\x01\x00"[..]),
                Ok((&b""[..], TupleValue::ByteString(Bytes::new())))
            );
            assert_eq!(
                byte_string(&b"\x01\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::ByteString(Bytes::new())))
            );
            assert_eq!(
                byte_string(&b"\x01\x01\x02\x03\x00"[..]),
                Ok((
                    &b""[..],
                    TupleValue::ByteString(Bytes::from_static(&b"\x01\x02\x03"[..]))
                ))
            );
            assert_eq!(
                byte_string(&b"\x01\x01\x02\x03\x00moredata"[..]),
                Ok((
                    &b"moredata"[..],
                    TupleValue::ByteString(Bytes::from_static(&b"\x01\x02\x03"[..]))
                ))
            );
            assert_eq!(
                byte_string(&b"\x01\x00\xFF\x00\xFF\x00\xFF\x04\x00"[..]),
                Ok((
                    &b""[..],
                    TupleValue::ByteString(Bytes::from_static(&b"\x00\x00\x00\x04"[..]))
                ))
            );
            assert_eq!(
                byte_string(&b"\x01\x00\xFF\x00\xFF\x00\xFF\x04\x00moredata"[..]),
                Ok((
                    &b"moredata"[..],
                    TupleValue::ByteString(Bytes::from_static(&b"\x00\x00\x00\x04"[..]))
                ))
            );
            assert_eq!(
                byte_string(&b"no_byte_string"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_byte_string"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::byte_string(
                    byte_string(&b"\x01\x01\x02\x03\x00moredata"[..]).unwrap().1
                )
                .unwrap(),
                Bytes::from_static(&b"\x01\x02\x03"[..])
            );
        }

        #[test]
        fn test_unicode_string() {
            assert_eq!(
                unicode_string(&b"\x02\x00"[..]),
                Ok((&b""[..], TupleValue::UnicodeString("".to_string())))
            );
            assert_eq!(
                unicode_string(&b"\x02\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::UnicodeString("".to_string())))
            );
            assert_eq!(
                unicode_string(&b"\x02hello\x00"[..]),
                Ok((&b""[..], TupleValue::UnicodeString("hello".to_string())))
            );
            assert_eq!(
                unicode_string(&b"\x02hello\x00moredata"[..]),
                Ok((
                    &b"moredata"[..],
                    TupleValue::UnicodeString("hello".to_string())
                ))
            );
            assert_eq!(
                unicode_string(&b"\x02\xE4\xB8\xAD\xE6\x96\x87\x00"[..]),
                Ok((&b""[..], TupleValue::UnicodeString("中文".to_string())))
            );
            assert_eq!(
                unicode_string(&b"\x02\xE4\xB8\xAD\xE6\x96\x87\x00moredata"[..]),
                Ok((
                    &b"moredata"[..],
                    TupleValue::UnicodeString("中文".to_string())
                ))
            );
            assert_eq!(
                unicode_string(&b"\x02\xCE\xBC\xCE\xAC\xCE\xB8\xCE\xB7\xCE\xBC\xCE\xB1\x00"[..]),
                Ok((&b""[..], TupleValue::UnicodeString("μάθημα".to_string())))
            );
            assert_eq!(
                unicode_string(
                    &b"\x02\xCE\xBC\xCE\xAC\xCE\xB8\xCE\xB7\xCE\xBC\xCE\xB1\x00moredata"[..]
                ),
                Ok((
                    &b"moredata"[..],
                    TupleValue::UnicodeString("μάθημα".to_string())
                ))
            );
            assert_eq!(
                byte_string(&b"no_unicode_string"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_unicode_string"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::unicode_string(
                    unicode_string(&b"\x02hello\x00moredata"[..]).unwrap().1
                )
                .unwrap(),
                "hello".to_string()
            );
        }

        #[test]
        fn test_nested_tuple() {
            assert_eq!(
                nested_tuple(&b"\x05\x00\xFF\x00"[..]),
                Ok((
                    &b""[..],
                    TupleValue::NestedTuple(Tuple::from_elements(vec![TupleValue::NullValue]))
                ))
            );
            assert_eq!(
                nested_tuple(&b"\x05\x00\xFF\x00moredata"[..]),
                Ok((
                    &b"moredata"[..],
                    TupleValue::NestedTuple(Tuple::from_elements(vec![TupleValue::NullValue]))
                ))
            );
            assert_eq!(
                nested_tuple(&b"\x05\x00\xFF\x02hello\x00\x00"[..]),
                Ok((
                    &b""[..],
                    TupleValue::NestedTuple(Tuple::from_elements(vec![
                        TupleValue::NullValue,
                        TupleValue::UnicodeString("hello".to_string()),
                    ]))
                ))
            );
            assert_eq!(
                nested_tuple(&b"\x05\x00\xFF\x02hello\x00\x00moredata"[..]),
                Ok((
                    &b"moredata"[..],
                    TupleValue::NestedTuple(Tuple::from_elements(vec![
                        TupleValue::NullValue,
                        TupleValue::UnicodeString("hello".to_string()),
                    ]))
                ))
            );
            assert_eq!(
                nested_tuple(&b"\x05\x00\xFF\x02hell\x00\xFF\x00\x00"[..]),
                Ok((
                    &b""[..],
                    TupleValue::NestedTuple(Tuple::from_elements(vec![
                        TupleValue::NullValue,
                        TupleValue::UnicodeString("hell\u{0}".to_string()),
                    ]))
                ))
            );
            assert_eq!(
                nested_tuple(&b"\x05\x00\xFF\x02hell\x00\xFF\x00\x00moredata"[..]),
                Ok((
                    &b"moredata"[..],
                    TupleValue::NestedTuple(Tuple::from_elements(vec![
                        TupleValue::NullValue,
                        TupleValue::UnicodeString("hell\u{0}".to_string()),
                    ]))
                ))
            );
            assert_eq!(
                nested_tuple(&b"\x05\x00\xFF\x34hello\x00\x00"[..]),
                Err(nom::Err::Error(nom::error::Error::new(
                    &b"\x34hello\x00\x00"[..],
                    nom::error::ErrorKind::Fail
                )))
            );
            assert_eq!(
                nested_tuple(&b"\x05"[..]),
                Err(nom::Err::Error(nom::error::Error::new(
                    &b""[..],
                    nom::error::ErrorKind::Eof
                )))
            );
            assert_eq!(
                nested_tuple(&b"no_nested_tuple"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_nested_tuple"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::nested_tuple(
                    nested_tuple(&b"\x05\x00\xFF\x02hell\x00\xFF\x00\x00moredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                Tuple::from_elements(vec![
                    TupleValue::NullValue,
                    TupleValue::UnicodeString("hell\u{0}".to_string())
                ])
            );
        }

        #[test]
        fn test_negative_arbitrary_precision_integer() {
            assert_eq!(
                negative_arbitrary_precision_integer(
                    &b"\x0B\xF6\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..]
                ),
                Ok((
                    &b"moredata"[..],
                    TupleValue::NegativeArbitraryPrecisionInteger(
                        BigInt::parse_bytes(b"18446744073709551616", 10).unwrap()
                    )
                ))
            );
            assert_eq!(
                positive_arbitrary_precision_integer(
                    &b"no_negative_arbitrary_precision_integer"[..]
                ),
                Err(nom::Err::Error(Error::new(
                    &b"no_negative_arbitrary_precision_integer"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                negative_arbitrary_precision_integer(
                    &b"\x0B\xF6\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..]
                ),
                Err(nom::Err::Error(nom::error::Error::new(
                    &b"\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF"[..],
                    nom::error::ErrorKind::Eof
                )))
            );
            assert_eq!(
                tuple_extractor::negative_arbitrary_precision_integer(
                    negative_arbitrary_precision_integer(
                        &b"\x0B\xF6\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..]
                    )
                    .unwrap()
                    .1
                )
                .unwrap(),
                BigInt::parse_bytes(b"-18446744073709551616", 10).unwrap()
            );
        }

        #[test]
        fn test_neg_int_8() {
            assert_eq!(
                neg_int_8(&b"\x0C\x00\x00\x00\x00\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt8(18446744073709551615)))
            );
            assert_eq!(
                neg_int_8(&b"\x0C\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt8(72057594037927936)))
            );
            assert_eq!(
                neg_int_8(&b"no_neg_int_8"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_neg_int_8"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::neg_int_8_bigint(
                    neg_int_8(&b"\x0C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFEmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                BigInt::parse_bytes(b"-9223372036854775809", 10).unwrap()
            );
            assert_eq!(
                tuple_extractor::neg_int_8_i64(
                    neg_int_8(&b"\x0C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                -9223372036854775808i64
            );
        }

        #[test]
        fn test_neg_int_7() {
            assert_eq!(
                neg_int_7(&b"\x0D\x00\x00\x00\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt7(72057594037927935)))
            );
            assert_eq!(
                neg_int_7(&b"\x0D\xFE\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt7(281474976710656)))
            );
            assert_eq!(
                neg_int_7(&b"no_neg_int_7"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_neg_int_7"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::neg_int_7_i64(
                    neg_int_7(&b"\x0D\x00\x00\x00\x00\x00\x00\x00moredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                -72057594037927935i64
            );
            assert_eq!(
                tuple_extractor::neg_int_7_i64(
                    neg_int_7(&b"\x0D\xFE\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                -281474976710656i64
            );
        }

        #[test]
        fn test_neg_int_6() {
            assert_eq!(
                neg_int_6(&b"\x0E\x00\x00\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt6(281474976710655)))
            );
            assert_eq!(
                neg_int_6(&b"\x0E\xFE\xFF\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt6(1099511627776)))
            );
            assert_eq!(
                neg_int_6(&b"no_neg_int_6"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_neg_int_6"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::neg_int_6_i64(
                    neg_int_6(&b"\x0E\x00\x00\x00\x00\x00\x00moredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                -281474976710655i64
            );
            assert_eq!(
                tuple_extractor::neg_int_6_i64(
                    neg_int_6(&b"\x0E\xFE\xFF\xFF\xFF\xFF\xFFmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                -1099511627776i64
            );
        }

        #[test]
        fn test_neg_int_5() {
            assert_eq!(
                neg_int_5(&b"\x0F\x00\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt5(1099511627775)))
            );
            assert_eq!(
                neg_int_5(&b"\x0F\xFE\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt5(4294967296)))
            );
            assert_eq!(
                neg_int_5(&b"no_neg_int_5"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_neg_int_5"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::neg_int_5_i64(
                    neg_int_5(&b"\x0F\x00\x00\x00\x00\x00moredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                -1099511627775i64
            );
            assert_eq!(
                tuple_extractor::neg_int_5_i64(
                    neg_int_5(&b"\x0F\xFE\xFF\xFF\xFF\xFFmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                -4294967296i64
            );
        }

        #[test]
        fn test_neg_int_4() {
            assert_eq!(
                neg_int_4(&b"\x10\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt4(4294967295)))
            );
            assert_eq!(
                neg_int_4(&b"\x10\xFE\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt4(16777216)))
            );
            assert_eq!(
                neg_int_4(&b"no_neg_int_4"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_neg_int_4"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::neg_int_4_i64(
                    neg_int_4(&b"\x10\x7F\xFF\xFF\xFEmoredata"[..]).unwrap().1
                )
                .unwrap(),
                -2147483649i64
            );
            assert_eq!(
                tuple_extractor::neg_int_4_i32(
                    neg_int_4(&b"\x10\x7F\xFF\xFF\xFFmoredata"[..]).unwrap().1
                )
                .unwrap(),
                -2147483648i32
            );
        }

        #[test]
        fn test_neg_int_3() {
            assert_eq!(
                neg_int_3(&b"\x11\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt3(16777215)))
            );
            assert_eq!(
                neg_int_3(&b"\x11\xFE\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt3(65536)))
            );
            assert_eq!(
                neg_int_3(&b"no_neg_int_3"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_neg_int_3"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::neg_int_3_i32(
                    neg_int_3(&b"\x11\x00\x00\x00moredata"[..]).unwrap().1
                )
                .unwrap(),
                -16777215i32
            );
            assert_eq!(
                tuple_extractor::neg_int_3_i32(
                    neg_int_3(&b"\x11\xFE\xFF\xFFmoredata"[..]).unwrap().1
                )
                .unwrap(),
                -65536i32
            );
        }

        #[test]
        fn test_neg_int_2() {
            assert_eq!(
                neg_int_2(&b"\x12\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt2(65535)))
            );
            assert_eq!(
                neg_int_2(&b"\x12\xFE\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt2(256)))
            );
            assert_eq!(
                neg_int_2(&b"no_neg_int_2"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_neg_int_2"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::neg_int_2_i32(neg_int_2(&b"\x12\x7F\xFEmoredata"[..]).unwrap().1)
                    .unwrap(),
                -32769i32
            );
            assert_eq!(
                tuple_extractor::neg_int_2_i16(neg_int_2(&b"\x12\x7F\xFFmoredata"[..]).unwrap().1)
                    .unwrap(),
                -32768i16
            );
        }

        #[test]
        fn test_neg_int_1() {
            assert_eq!(
                neg_int_1(&b"\x13\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt1(255)))
            );
            assert_eq!(
                neg_int_1(&b"\x13\xFEmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::NegInt1(1)))
            );
            assert_eq!(
                neg_int_1(&b"no_neg_int_1"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_neg_int_1"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::neg_int_1_i16(neg_int_1(&b"\x13\x7Emoredata"[..]).unwrap().1)
                    .unwrap(),
                -129i16
            );
            assert_eq!(
                tuple_extractor::neg_int_1_i8(neg_int_1(&b"\x13\x7Fmoredata"[..]).unwrap().1)
                    .unwrap(),
                -128i8
            );
        }

        #[test]
        fn test_int_zero() {
            assert_eq!(
                int_zero(&b"\x14moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::IntZero))
            );
            assert_eq!(
                int_zero(&b"no_int_zero"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_int_zero"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::int_zero(int_zero(&b"\x14moredata"[..]).unwrap().1).unwrap(),
                0
            );
        }

        #[test]
        fn test_pos_int_1() {
            assert_eq!(
                pos_int_1(&b"\x15\x01moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt1(1)))
            );
            assert_eq!(
                pos_int_1(&b"\x15\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt1(255)))
            );
            assert_eq!(
                pos_int_1(&b"no_pos_int_1"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_pos_int_1"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::pos_int_1_i8(pos_int_1(&b"\x15\x7Fmoredata"[..]).unwrap().1)
                    .unwrap(),
                127i8
            );
            assert_eq!(
                tuple_extractor::pos_int_1_i16(pos_int_1(&b"\x15\x80moredata"[..]).unwrap().1)
                    .unwrap(),
                128i16
            );
        }

        #[test]
        fn test_pos_int_2() {
            assert_eq!(
                pos_int_2(&b"\x16\x01\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt2(256)))
            );
            assert_eq!(
                pos_int_2(&b"\x16\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt2(65535)))
            );
            assert_eq!(
                pos_int_2(&b"no_pos_int_2"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_pos_int_2"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::pos_int_2_i16(pos_int_2(&b"\x16\x7F\xFFmoredata"[..]).unwrap().1)
                    .unwrap(),
                32767i16
            );
            assert_eq!(
                tuple_extractor::pos_int_2_i32(pos_int_2(&b"\x16\x80\x00moredata"[..]).unwrap().1)
                    .unwrap(),
                32768i32
            );
        }

        #[test]
        fn test_pos_int_3() {
            assert_eq!(
                pos_int_3(&b"\x17\x01\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt3(65536)))
            );
            assert_eq!(
                pos_int_3(&b"\x17\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt3(16777215)))
            );
            assert_eq!(
                pos_int_3(&b"no_pos_int_3"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_pos_int_3"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::pos_int_3_i32(
                    pos_int_3(&b"\x17\x01\x00\x00moredata"[..]).unwrap().1
                )
                .unwrap(),
                65536i32
            );
            assert_eq!(
                tuple_extractor::pos_int_3_i32(
                    pos_int_3(&b"\x17\xFF\xFF\xFFmoredata"[..]).unwrap().1
                )
                .unwrap(),
                16777215i32,
            );
        }

        #[test]
        fn test_pos_int_4() {
            assert_eq!(
                pos_int_4(&b"\x18\x01\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt4(16777216)))
            );
            assert_eq!(
                pos_int_4(&b"\x18\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt4(4294967295)))
            );
            assert_eq!(
                pos_int_4(&b"no_pos_int_4"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_pos_int_4"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::pos_int_4_i32(
                    pos_int_4(&b"\x18\x7F\xFF\xFF\xFFmoredata"[..]).unwrap().1
                )
                .unwrap(),
                2147483647i32
            );
            assert_eq!(
                tuple_extractor::pos_int_4_i64(
                    pos_int_4(&b"\x18\x80\x00\x00\x00moredata"[..]).unwrap().1
                )
                .unwrap(),
                2147483648i64
            );
        }

        #[test]
        fn test_pos_int_5() {
            assert_eq!(
                pos_int_5(&b"\x19\x01\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt5(4294967296)))
            );
            assert_eq!(
                pos_int_5(&b"\x19\xFF\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt5(1099511627775)))
            );
            assert_eq!(
                pos_int_5(&b"no_pos_int_5"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_pos_int_5"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::pos_int_5_i64(
                    pos_int_5(&b"\x19\x01\x00\x00\x00\x00moredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                4294967296i64
            );
            assert_eq!(
                tuple_extractor::pos_int_5_i64(
                    pos_int_5(&b"\x19\xFF\xFF\xFF\xFF\xFFmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                1099511627775i64
            );
        }

        #[test]
        fn test_pos_int_6() {
            assert_eq!(
                pos_int_6(&b"\x1A\x01\x00\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt6(1099511627776)))
            );
            assert_eq!(
                pos_int_6(&b"\x1A\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt6(281474976710655)))
            );
            assert_eq!(
                pos_int_6(&b"no_pos_int_6"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_pos_int_6"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::pos_int_6_i64(
                    pos_int_6(&b"\x1A\x01\x00\x00\x00\x00\x00moredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                1099511627776i64
            );
            assert_eq!(
                tuple_extractor::pos_int_6_i64(
                    pos_int_6(&b"\x1A\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                281474976710655i64
            );
        }

        #[test]
        fn test_pos_int_7() {
            assert_eq!(
                pos_int_7(&b"\x1B\x01\x00\x00\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt7(281474976710656)))
            );
            assert_eq!(
                pos_int_7(&b"\x1B\xFF\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt7(72057594037927935)))
            );
            assert_eq!(
                pos_int_7(&b"no_pos_int_7"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_pos_int_7"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::pos_int_7_i64(
                    pos_int_7(&b"\x1B\x01\x00\x00\x00\x00\x00\x00moredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                281474976710656i64
            );
            assert_eq!(
                tuple_extractor::pos_int_7_i64(
                    pos_int_7(&b"\x1B\xFF\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                72057594037927935i64
            );
        }

        #[test]
        fn test_pos_int_8() {
            assert_eq!(
                pos_int_8(&b"\x1C\x01\x00\x00\x00\x00\x00\x00\x00moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt8(72057594037927936)))
            );
            assert_eq!(
                pos_int_8(&b"\x1C\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..]),
                Ok((&b"moredata"[..], TupleValue::PosInt8(18446744073709551615)))
            );
            assert_eq!(
                pos_int_8(&b"no_pos_int_8"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_pos_int_8"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::pos_int_8_i64(
                    pos_int_8(&b"\x1C\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                9223372036854775807i64
            );
            assert_eq!(
                tuple_extractor::pos_int_8_bigint(
                    pos_int_8(&b"\x1C\x80\x00\x00\x00\x00\x00\x00\x00moredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                BigInt::parse_bytes(b"9223372036854775808", 10).unwrap()
            );
        }

        #[test]
        fn test_positive_arbitrary_precision_integer() {
            assert_eq!(
                positive_arbitrary_precision_integer(
                    &b"\x1D\x09\x01\x00\x00\x00\x00\x00\x00\x00\x00moredata"[..]
                ),
                Ok((
                    &b"moredata"[..],
                    TupleValue::PositiveArbitraryPrecisionInteger(
                        BigInt::parse_bytes(b"18446744073709551616", 10).unwrap()
                    )
                ))
            );
            assert_eq!(
                positive_arbitrary_precision_integer(
                    &b"no_positive_arbitrary_precision_integer"[..]
                ),
                Err(nom::Err::Error(Error::new(
                    &b"no_positive_arbitrary_precision_integer"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                positive_arbitrary_precision_integer(
                    &b"\x1D\x09\x01\x00\x00\x00\x00\x00\x00\x00"[..]
                ),
                Err(nom::Err::Incomplete(nom::Needed::Size(
                    NonZeroUsize::new(1).unwrap()
                )))
            );
            assert_eq!(
                tuple_extractor::positive_arbitrary_precision_integer(
                    positive_arbitrary_precision_integer(
                        &b"\x1D\x09\x01\x00\x00\x00\x00\x00\x00\x00\x00moredata"[..]
                    )
                    .unwrap()
                    .1
                )
                .unwrap(),
                BigInt::parse_bytes(b"18446744073709551616", 10).unwrap()
            );
        }

        // `3.14` is copied from Java binding tests
        #[allow(clippy::approx_constant)]
        #[test]
        fn test_ieee_binary_floating_point_float() {
            assert_eq!(
                ieee_binary_floating_point_float(&b"\x20\xC0\x48\xF5\xC3moredata"[..]),
                Ok((
                    &b"moredata"[..],
                    TupleValue::IeeeBinaryFloatingPointFloat(3.14f32)
                ))
            );
            assert_eq!(
                ieee_binary_floating_point_float(&b"\x20\x3F\xB7\x0A\x3Cmoredata"[..]),
                Ok((
                    &b"moredata"[..],
                    TupleValue::IeeeBinaryFloatingPointFloat(-3.14f32)
                ))
            );
            assert_eq!(
                ieee_binary_floating_point_float(&b"\x20\x80\x00\x00\x00moredata"[..]),
                Ok((
                    &b"moredata"[..],
                    TupleValue::IeeeBinaryFloatingPointFloat(0.0f32)
                ))
            );
            assert_eq!(
                ieee_binary_floating_point_float(&b"\x20\x7F\xFF\xFF\xFFmoredata"[..]),
                Ok((
                    &b"moredata"[..],
                    TupleValue::IeeeBinaryFloatingPointFloat(-0.0f32)
                ))
            );
            assert_eq!(
                ieee_binary_floating_point_float(&b"\x20\xFF\x80\x00\x00moredata"[..]),
                Ok((
                    &b"moredata"[..],
                    TupleValue::IeeeBinaryFloatingPointFloat(f32::INFINITY)
                ))
            );
            assert_eq!(
                ieee_binary_floating_point_float(&b"\x20\x00\x7F\xFF\xFFmoredata"[..]),
                Ok((
                    &b"moredata"[..],
                    TupleValue::IeeeBinaryFloatingPointFloat(f32::NEG_INFINITY)
                ))
            );

            // b"\x20\xFF\xFF\xFF\xFF" and b"\x20\x00\x00\x00\x00
            // results in f32::NAN, but they cannot be compared.

            assert_eq!(
                ieee_binary_floating_point_float(&b"no_ieee_binary_floating_point_float"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_ieee_binary_floating_point_float"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                ieee_binary_floating_point_float(&b"\x20\xC0\x48\xF5"[..]),
                Err(nom::Err::Error(nom::error::Error::new(
                    &b"\xC0\x48\xF5"[..],
                    nom::error::ErrorKind::Eof
                )))
            );

            assert_eq!(
                tuple_extractor::ieee_binary_floating_point_float(
                    ieee_binary_floating_point_float(&b"\x20\xC0\x48\xF5\xC3moredata"[..])
                        .unwrap()
                        .1
                )
                .unwrap(),
                3.14f32
            );
        }

        // `3.14` is copied from Java binding tests
        #[allow(clippy::approx_constant)]
        #[test]
        fn test_ieee_binary_floating_point_double() {
            assert_eq!(
                ieee_binary_floating_point_double(
                    &b"\x21\xC0\x09\x1E\xB8\x51\xEB\x85\x1Fmoredata"[..]
                ),
                Ok((
                    &b"moredata"[..],
                    TupleValue::IeeeBinaryFloatingPointDouble(3.14f64)
                ))
            );
            assert_eq!(
                ieee_binary_floating_point_double(
                    &b"\x21\x3F\xF6\xE1\x47\xAE\x14\x7A\xE0moredata"[..]
                ),
                Ok((
                    &b"moredata"[..],
                    TupleValue::IeeeBinaryFloatingPointDouble(-3.14f64)
                ))
            );
            assert_eq!(
                ieee_binary_floating_point_double(
                    &b"\x21\x80\x00\x00\x00\x00\x00\x00\x00moredata"[..]
                ),
                Ok((
                    &b"moredata"[..],
                    TupleValue::IeeeBinaryFloatingPointDouble(0.0f64)
                ))
            );
            assert_eq!(
                ieee_binary_floating_point_double(
                    &b"\x21\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..]
                ),
                Ok((
                    &b"moredata"[..],
                    TupleValue::IeeeBinaryFloatingPointDouble(-0.0f64)
                ))
            );
            assert_eq!(
                ieee_binary_floating_point_double(
                    &b"\x21\xFF\xF0\x00\x00\x00\x00\x00\x00moredata"[..]
                ),
                Ok((
                    &b"moredata"[..],
                    TupleValue::IeeeBinaryFloatingPointDouble(f64::INFINITY)
                ))
            );
            assert_eq!(
                ieee_binary_floating_point_double(
                    &b"\x21\x00\x0F\xFF\xFF\xFF\xFF\xFF\xFFmoredata"[..]
                ),
                Ok((
                    &b"moredata"[..],
                    TupleValue::IeeeBinaryFloatingPointDouble(f64::NEG_INFINITY)
                ))
            );

            // b"\x21\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF" and
            // b"\x21\x00\x00\x00\x00\x00\x00\x00\x00 results in
            // f64::NAN, but they cannot be compared.

            assert_eq!(
                ieee_binary_floating_point_double(&b"no_ieee_binary_floating_point_double"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_ieee_binary_floating_point_double"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                ieee_binary_floating_point_double(&b"\x21\xC0\x09\x1E\xB8\x51\xEB\x85"[..]),
                Err(nom::Err::Error(nom::error::Error::new(
                    &b"\xC0\x09\x1E\xB8\x51\xEB\x85"[..],
                    nom::error::ErrorKind::Eof
                )))
            );

            assert_eq!(
                tuple_extractor::ieee_binary_floating_point_double(
                    ieee_binary_floating_point_double(
                        &b"\x21\xC0\x09\x1E\xB8\x51\xEB\x85\x1Fmoredata"[..]
                    )
                    .unwrap()
                    .1
                )
                .unwrap(),
                3.14f64
            );
        }

        #[test]
        fn test_false_value() {
            assert_eq!(
                false_value(&b"\x26moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::FalseValue))
            );
            assert_eq!(
                false_value(&b"no_false_value"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_false_value"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::false_value(false_value(&b"\x26moredata"[..]).unwrap().1),
                Ok(false)
            );
        }

        #[test]
        fn test_true_value() {
            assert_eq!(
                true_value(&b"\x27moredata"[..]),
                Ok((&b"moredata"[..], TupleValue::TrueValue))
            );
            assert_eq!(
                true_value(&b"no_true_value"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_true_value"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                tuple_extractor::true_value(true_value(&b"\x27moredata"[..]).unwrap().1),
                Ok(true)
            );
        }

        #[test]
        fn test_rfc_4122_uuid() {
            assert_eq!(
        	     rfc_4122_uuid(
        		 &b"\x30\xFF\xFF\xFF\xFF\xBA\x5E\xBA\x11\x00\x00\x00\x00\x5C\xA1\xAB\x1Emoredata"[..]
        	     ),
        	Ok((&b"moredata"[..], TupleValue::Rfc4122Uuid(Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap())))
            );
            assert_eq!(
                rfc_4122_uuid(&b"no_rfc_4122_uuid"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_rfc_4122_uuid"[..],
                    ErrorKind::Tag
                )))
            );
            assert_eq!(
                rfc_4122_uuid(
                    &b"\x30\xFF\xFF\xFF\xFF\xBA\x5E\xBA\x11\x00\x00\x00\x00\x5C\xA1\xAB"[..]
                ),
                Err(nom::Err::Error(nom::error::Error::new(
                    &b"\xFF\xFF\xFF\xFF\xBA\x5E\xBA\x11\x00\x00\x00\x00\x5C\xA1\xAB"[..],
                    nom::error::ErrorKind::Eof
                )))
            );

            assert_eq!(
        	tuple_extractor::rfc_4122_uuid(
        	    rfc_4122_uuid(
        		&b"\x30\xFF\xFF\xFF\xFF\xBA\x5E\xBA\x11\x00\x00\x00\x00\x5C\xA1\xAB\x1Emoredata"[..]).unwrap().1
        	).unwrap(),
        	Uuid::parse_str("ffffffff-ba5e-ba11-0000-00005ca1ab1e").unwrap()
            );
        }

        #[test]
        fn test_versionstamp_96_bit() {
            assert_eq!(
                versionstamp_96_bit(
                    &b"\x33\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03\x00\x00moredata"[..]
                ),
                Ok((
                    &b"moredata"[..],
                    TupleValue::Versionstamp96Bit(Versionstamp::complete(
                        Bytes::from_static(&b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"[..]),
                        0
                    ))
                ))
            );
            assert_eq!(
                versionstamp_96_bit(
                    &b"\x33\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x02\x91moredata"[..]
                ),
                Ok((
                    &b"moredata"[..],
                    TupleValue::Versionstamp96Bit(Versionstamp::complete(
                        Bytes::from_static(&b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A"[..]),
                        657
                    ))
                ))
            );
            assert_eq!(
                versionstamp_96_bit(
                    &b"\x33\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x02\x91moredata"[..]
                ),
                Ok((
                    &b"moredata"[..],
                    TupleValue::Versionstamp96Bit(Versionstamp::incomplete(657))
                ))
            );
            assert_eq!(
                versionstamp_96_bit(&b"no_versionstamp_96_bit"[..]),
                Err(nom::Err::Error(Error::new(
                    &b"no_versionstamp_96_bit"[..],
                    ErrorKind::Tag
                )))
            );

            assert_eq!(
                tuple_extractor::versionstamp_96_bit(
                    versionstamp_96_bit(
                        &b"\x33\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03\x00\x00moredata"[..]
                    )
                    .unwrap()
                    .1
                )
                .unwrap(),
                Versionstamp::complete(
                    Bytes::from_static(&b"\xAA\xBB\xCC\xDD\xEE\xFF\x00\x01\x02\x03"[..]),
                    0
                )
            );
        }

        // `test_size_limits_pos_int` and `test_size_limits_neg_int`
        // below were written in order to determine the boundaries for
        // various integer sizes. Leaving it here, in case we need to
        // come back and debug this part of code sometime in the
        // future.

        // #[test]
        // fn test_size_limits_pos_int() {
        //     let size_limits_pos: Vec<i128> = vec![
        //         // (1 << (0 * 8)) - 1,
        //         // (1 << (1 * 8)) - 1,
        //         // (1 << (2 * 8)) - 1,
        //         // (1 << (3 * 8)) - 1,
        //         // (1 << (4 * 8)) - 1,
        //         // (1 << (5 * 8)) - 1,
        //         // (1 << (6 * 8)) - 1,
        //         // (1 << (7 * 8)) - 1,
        //         // (1 << (8 * 8)) - 1,
        //     ];
        //     println!("size_limits_pos_int: {:?}", size_limits_pos);
        // }

        // #[test]
        // fn test_size_limits_neg_int() {
        //     let size_limits_neg: Vec<i128> = vec![
        //         // ((1 << (0 * 8)) * -1) + 1,
        //         // ((1 << (1 * 8)) * -1) + 1,
        //         // ((1 << (2 * 8)) * -1) + 1,
        //         // ((1 << (3 * 8)) * -1) + 1,
        //         // ((1 << (4 * 8)) * -1) + 1,
        //         // ((1 << (5 * 8)) * -1) + 1,
        //         // ((1 << (6 * 8)) * -1) + 1,
        //         // ((1 << (7 * 8)) * -1) + 1,
        //         // ((1 << (8 * 8)) * -1) + 1,
        //     ];
        //     println!("size_limits_neg_int: {:?}", size_limits_neg);
        // }

        // We usually don't factor out code in tests, as we want
        // maximal context relating to the test to be available in one
        // place. This is an exception as it removes some boilerplate
        // code and makes the tests easier to read.
        pub(self) mod tuple_extractor {
            use super::TupleValue;
            use crate::error::{FdbError, FdbResult};
            use crate::tuple::{Tuple, Versionstamp};
            use bytes::Bytes;
            use num_bigint::BigInt;
            use std::convert::TryInto;
            use uuid::Uuid;

            // This is just a dummy value
            const TUPLE_EXTRACTOR: i32 = 0xFFFF;

            fn tuple_extractor_error() -> FdbError {
                FdbError::new(TUPLE_EXTRACTOR)
            }

            pub(crate) fn null_value(tv: TupleValue) -> FdbResult<()> {
                if let TupleValue::NullValue = tv {
                    Ok(())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn nested_tuple_null_value(tv: TupleValue) -> FdbResult<()> {
                if let TupleValue::NullValue = tv {
                    Ok(())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn byte_string(tv: TupleValue) -> FdbResult<Bytes> {
                if let TupleValue::ByteString(i) = tv {
                    Ok(i)
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn unicode_string(tv: TupleValue) -> FdbResult<String> {
                if let TupleValue::UnicodeString(i) = tv {
                    Ok(i)
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn nested_tuple(tv: TupleValue) -> FdbResult<Tuple> {
                if let TupleValue::NestedTuple(i) = tv {
                    Ok(i)
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn negative_arbitrary_precision_integer(
                tv: TupleValue,
            ) -> FdbResult<BigInt> {
                if let TupleValue::NegativeArbitraryPrecisionInteger(i) = tv {
                    Ok(i * -1)
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn neg_int_8_bigint(tv: TupleValue) -> FdbResult<BigInt> {
                if let TupleValue::NegInt8(i) = tv {
                    Ok(Into::<BigInt>::into(i) * -1)
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn neg_int_8_i64(tv: TupleValue) -> FdbResult<i64> {
                if let TupleValue::NegInt8(i) = tv {
                    (-Into::<i128>::into(i))
                        .try_into()
                        .map_err(|_| tuple_extractor_error())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn neg_int_7_i64(tv: TupleValue) -> FdbResult<i64> {
                if let TupleValue::NegInt7(i) = tv {
                    // Even though NegInt5's range
                    // -72057594037927935..=-281474976710656 is well
                    // within i64::MIN (-9223372036854775808), this
                    // information is not known to `i: u64`. So we need to
                    // use `try_into()`.
                    i.try_into()
                        .map(|x: i64| -x)
                        .map_err(|_| tuple_extractor_error())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn neg_int_6_i64(tv: TupleValue) -> FdbResult<i64> {
                if let TupleValue::NegInt6(i) = tv {
                    // Even though NegInt5's range
                    // -281474976710655..=-1099511627776 is well within
                    // i64::MIN (-9223372036854775808), this information
                    // is not known to `i: u64`. So we need to use
                    // `try_into()`.
                    i.try_into()
                        .map(|x: i64| -x)
                        .map_err(|_| tuple_extractor_error())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn neg_int_5_i64(tv: TupleValue) -> FdbResult<i64> {
                if let TupleValue::NegInt5(i) = tv {
                    // Even though NegInt5's range
                    // -1099511627775..=-4294967296 is well within
                    // i64::MIN (-9223372036854775808), this information
                    // is not known to `i: u64`. So we need to use
                    // `try_into()`.
                    i.try_into()
                        .map(|x: i64| -x)
                        .map_err(|_| tuple_extractor_error())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn neg_int_4_i64(tv: TupleValue) -> FdbResult<i64> {
                if let TupleValue::NegInt4(i) = tv {
                    Ok(-Into::<i64>::into(i))
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn neg_int_4_i32(tv: TupleValue) -> FdbResult<i32> {
                if let TupleValue::NegInt4(i) = tv {
                    (-Into::<i64>::into(i))
                        .try_into()
                        .map_err(|_| tuple_extractor_error())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn neg_int_3_i32(tv: TupleValue) -> FdbResult<i32> {
                if let TupleValue::NegInt3(i) = tv {
                    // Even though NegInt3's range -16777215..=-65536 is
                    // well within i32::MIN (-2147483648), this
                    // information is not known to `i: u32`. So, we need
                    // to use `try_into()`.
                    i.try_into()
                        .map(|x: i32| -x)
                        .map_err(|_| tuple_extractor_error())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn neg_int_2_i32(tv: TupleValue) -> FdbResult<i32> {
                if let TupleValue::NegInt2(i) = tv {
                    Ok(-Into::<i32>::into(i))
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn neg_int_2_i16(tv: TupleValue) -> FdbResult<i16> {
                if let TupleValue::NegInt2(i) = tv {
                    (-Into::<i32>::into(i))
                        .try_into()
                        .map_err(|_| tuple_extractor_error())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn neg_int_1_i16(tv: TupleValue) -> FdbResult<i16> {
                if let TupleValue::NegInt1(i) = tv {
                    Ok(-Into::<i16>::into(i))
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn neg_int_1_i8(tv: TupleValue) -> FdbResult<i8> {
                if let TupleValue::NegInt1(i) = tv {
                    (-Into::<i16>::into(i))
                        .try_into()
                        .map_err(|_| tuple_extractor_error())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn int_zero(tv: TupleValue) -> FdbResult<i8> {
                if let TupleValue::IntZero = tv {
                    Ok(0)
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn pos_int_1_i8(tv: TupleValue) -> FdbResult<i8> {
                if let TupleValue::PosInt1(i) = tv {
                    i.try_into().map_err(|_| tuple_extractor_error())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn pos_int_1_i16(tv: TupleValue) -> FdbResult<i16> {
                if let TupleValue::PosInt1(i) = tv {
                    Ok(i.into())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn pos_int_2_i16(tv: TupleValue) -> FdbResult<i16> {
                if let TupleValue::PosInt2(i) = tv {
                    i.try_into().map_err(|_| tuple_extractor_error())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn pos_int_2_i32(tv: TupleValue) -> FdbResult<i32> {
                if let TupleValue::PosInt2(i) = tv {
                    Ok(i.into())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn pos_int_3_i32(tv: TupleValue) -> FdbResult<i32> {
                if let TupleValue::PosInt3(i) = tv {
                    // Even though PosInt3's range 65536..=16777215 is
                    // within i32::MAX (2147483647), this information is
                    // not known to `i: u32`. So we need to use
                    // `try_into()`.
                    i.try_into().map_err(|_| tuple_extractor_error())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn pos_int_4_i32(tv: TupleValue) -> FdbResult<i32> {
                if let TupleValue::PosInt4(i) = tv {
                    i.try_into().map_err(|_| tuple_extractor_error())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn pos_int_4_i64(tv: TupleValue) -> FdbResult<i64> {
                if let TupleValue::PosInt4(i) = tv {
                    Ok(i.into())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn pos_int_5_i64(tv: TupleValue) -> FdbResult<i64> {
                if let TupleValue::PosInt5(i) = tv {
                    // Even though PosInt5's range
                    // 4294967296..=1099511627775 is within i64::MAX
                    // (9223372036854775807), this information is not
                    // known to `i: u64`. So we need to use `try_into()`.
                    i.try_into().map_err(|_| tuple_extractor_error())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn pos_int_6_i64(tv: TupleValue) -> FdbResult<i64> {
                if let TupleValue::PosInt6(i) = tv {
                    // Even though PosInt6's range
                    // 1099511627776..=281474976710655 is within i64::MAX
                    // (9223372036854775807), this information is not
                    // known to `i: u64`. So we need to use `try_into()`.
                    i.try_into().map_err(|_| tuple_extractor_error())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn pos_int_7_i64(tv: TupleValue) -> FdbResult<i64> {
                if let TupleValue::PosInt7(i) = tv {
                    // Even though PosInt6's range
                    // 281474976710656..=72057594037927935 is within
                    // i64::MAX (9223372036854775807), this information is
                    // not known to `i: u64`. So we need to use
                    // `try_into()`.
                    i.try_into().map_err(|_| tuple_extractor_error())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn pos_int_8_i64(tv: TupleValue) -> FdbResult<i64> {
                if let TupleValue::PosInt8(i) = tv {
                    i.try_into().map_err(|_| tuple_extractor_error())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn pos_int_8_bigint(tv: TupleValue) -> FdbResult<BigInt> {
                if let TupleValue::PosInt8(i) = tv {
                    Ok(i.into())
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn positive_arbitrary_precision_integer(
                tv: TupleValue,
            ) -> FdbResult<BigInt> {
                if let TupleValue::PositiveArbitraryPrecisionInteger(i) = tv {
                    Ok(i)
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn ieee_binary_floating_point_float(tv: TupleValue) -> FdbResult<f32> {
                if let TupleValue::IeeeBinaryFloatingPointFloat(i) = tv {
                    Ok(i)
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn ieee_binary_floating_point_double(tv: TupleValue) -> FdbResult<f64> {
                if let TupleValue::IeeeBinaryFloatingPointDouble(i) = tv {
                    Ok(i)
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn false_value(tv: TupleValue) -> FdbResult<bool> {
                if let TupleValue::FalseValue = tv {
                    Ok(false)
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn true_value(tv: TupleValue) -> FdbResult<bool> {
                if let TupleValue::TrueValue = tv {
                    Ok(true)
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn rfc_4122_uuid(tv: TupleValue) -> FdbResult<Uuid> {
                if let TupleValue::Rfc4122Uuid(i) = tv {
                    Ok(i)
                } else {
                    Err(tuple_extractor_error())
                }
            }

            pub(crate) fn versionstamp_96_bit(tv: TupleValue) -> FdbResult<Versionstamp> {
                if let TupleValue::Versionstamp96Bit(i) = tv {
                    Ok(i)
                } else {
                    Err(tuple_extractor_error())
                }
            }
        }
    }
}
