//! Mapper and mapped key-value types

use crate::range::Range;
use crate::tuple::Tuple;
use crate::KeyValue;

/// [`Mapper`] represents the behaviour of a mapped range read.
///
/// [`Mapper`] can be converted from and into [`Tuple`].
#[derive(Clone, Debug, PartialEq)]
pub struct Mapper(Tuple);

impl From<Tuple> for Mapper {
    fn from(t: Tuple) -> Mapper {
        Mapper(t)
    }
}

impl From<Mapper> for Tuple {
    fn from(m: Mapper) -> Tuple {
        m.0
    }
}

/// A mapped key/value pair.
///
/// Mapped range read operations on FDB return [`MappedKeyValue`].
#[derive(Clone, Debug)]
pub struct MappedKeyValue {
    key_value: KeyValue,
    range: Range,
    range_result: Vec<KeyValue>,
}

impl MappedKeyValue {
    /// Gets a reference to [`KeyValue`] from [`MappedKeyValue`].
    pub fn get_key_value_ref(&self) -> &KeyValue {
        &self.key_value
    }

    /// Gets a reference to [`Range`] from [`MappedKeyValue`].
    pub fn get_range_ref(&self) -> &Range {
        &self.range
    }

    /// Gets a reference to [`Vec<KeyValue>`] from [`MappedKeyValue`].
    pub fn get_range_result_ref(&self) -> &Vec<KeyValue> {
        &self.range_result
    }

    /// Extract [`KeyValue`] from [`MappedKeyValue`].
    pub fn into_key_value(self) -> KeyValue {
        self.key_value
    }

    /// Extract [`Range`] from [`MappedKeyValue`].
    pub fn into_range(self) -> Range {
        self.range
    }

    /// Extract [`Vec<KeyValue>`] from [`MappedKeyValue`].
    pub fn into_range_result(self) -> Vec<KeyValue> {
        self.range_result
    }

    /// Extract [`KeyValue`], [`Range`] and [`Vec<KeyValue>`] from
    /// [`MappedKeyValue`].
    pub fn into_parts(self) -> (KeyValue, Range, Vec<KeyValue>) {
        (self.key_value, self.range, self.range_result)
    }

    pub(crate) fn new(
        key_value: KeyValue,
        range: Range,
        range_result: Vec<KeyValue>,
    ) -> MappedKeyValue {
        MappedKeyValue {
            key_value,
            range,
            range_result,
        }
    }
}
