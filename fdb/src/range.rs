//! Provides types for working with FDB range.

use bytes::Bytes;

use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::error::{FdbError, FdbResult};
use crate::future::{FdbFuture, FdbFutureKeyValueArray, FdbStreamKeyValue};
use crate::transaction::{FdbTransaction, ReadTransaction};
use crate::tuple::key_util;
use crate::{Key, KeySelector, KeyValue};

pub use crate::option::StreamingMode;

/// [`Range`] describes an exact range of keyspace, specified by a
/// begin and end key.
///
/// As with all FDB APIs, begin is inclusive, and end exclusive.
#[derive(Clone, Debug, PartialEq)]
pub struct Range {
    begin: Key,
    end: Key,
}

impl Range {
    /// Construct a new [`Range`] with an inclusive begin key an
    /// exclusive end key.
    pub fn new(begin: impl Into<Key>, end: impl Into<Key>) -> Range {
        Range {
            begin: begin.into(),
            end: end.into(),
        }
    }

    /// Return a [`Range`] that describes all possible keys that are
    /// prefixed with the specified key.
    ///
    /// # Panic
    ///
    /// Panics if the supplied [`Key`] is empty or contains only
    /// `0xFF` bytes.
    pub fn starts_with(prefix_key: impl Into<Key>) -> Range {
        let prefix_key = prefix_key.into();
        Range::new(
            prefix_key.clone(),
            key_util::strinc(prefix_key).unwrap_or_else(|err| {
                panic!("Error occurred during `bytes_util::strinc`: {:?}", err)
            }),
        )
    }

    /// Return the beginning of the range.
    pub fn begin(&self) -> &Key {
        &self.begin
    }

    /// Return the end of the range.
    pub fn end(&self) -> &Key {
        &self.end
    }

    /// Gets an ordered range of keys and values from the database.
    ///
    /// The returned [`FdbStreamKeyValue`] implements [`Stream`] trait
    /// that yields a [`KeyValue`] item.
    ///
    /// [`Stream`]: futures::Stream
    pub fn into_stream<T>(self, rt: &T, options: RangeOptions) -> FdbStreamKeyValue
    where
        T: ReadTransaction,
    {
        let (begin_key, end_key) = self.deconstruct();

        let begin_key_selector = KeySelector::first_greater_or_equal(begin_key);
        let end_key_selector = KeySelector::first_greater_or_equal(end_key);

        rt.get_range(begin_key_selector, end_key_selector, options)
    }

    pub(crate) fn deconstruct(self) -> (Key, Key) {
        let Range { begin, end } = self;
        (begin, end)
    }
}

/// [`RangeOptions`] specify how a database range operation is carried out.
///
/// There are three parameters for which accessors methods are provided.
///
/// 1. Limit restricts the number of key-value pairs returned as part
///    of a range read. A value of zero indicates no limit.
///
/// 2. Mode sets the [streaming mode] of the range read, allowing
///    database to balance latency and bandwidth for this read.
///
/// 3. Reverse indicates that the read should be performed
///    lexicographic order (when false) or reverse lexicographic (when
///    true).
///
///    When reverse is true and limit is non-zero, last limit
///    key-value pairs in the range are returned. Ranges in reverse is
///    supported natively by the database should have minimal extra
///    cost.
///
/// To create a value of [`RangeOptions`] type, use
/// [`Default::default`] method. The default value represents - no
/// limit, [iterator streaming mode] and lexicographic order.
///
/// [streaming mode]: StreamingMode
/// [iterator streaming mode]: StreamingMode::Iterator
#[derive(Clone, Debug)]
pub struct RangeOptions {
    limit: i32,
    mode: StreamingMode,
    reverse: bool,
}

impl RangeOptions {
    /// Set limit
    pub fn set_limit(&mut self, limit: i32) {
        self.limit = limit;
    }

    /// Get limit
    pub fn get_limit(&self) -> i32 {
        self.limit
    }

    /// Set streaming mode
    pub fn set_mode(&mut self, mode: StreamingMode) {
        self.mode = mode;
    }

    /// Get streaming mode
    pub fn get_mode(&self) -> StreamingMode {
        self.mode
    }

    /// Set the read order (lexicographic or non-lexicographic)
    pub fn set_reverse(&mut self, reverse: bool) {
        self.reverse = reverse;
    }

    /// Get the read order (lexicographic or non-lexicographic)
    pub fn get_reverse(&self) -> bool {
        self.reverse
    }

    pub(crate) fn new(limit: i32, mode: StreamingMode, reverse: bool) -> RangeOptions {
        RangeOptions {
            limit,
            mode,
            reverse,
        }
    }
}

impl Default for RangeOptions {
    fn default() -> RangeOptions {
        RangeOptions {
            limit: 0,
            mode: StreamingMode::Iterator,
            reverse: false,
        }
    }
}

// Java API refers to this type `RangeResult` and Go API has something
// simliar with `futureKeyValueArray` and `[]KeyValue`. Go API
// `RangeResult` is similar to Java API `RangeQuery`. Be careful and
// don't confuse Java API `RangeResult` with Go API `RangeResult`.
#[derive(Debug)]
pub(crate) struct KeyValueArray {
    kvs: Vec<KeyValue>,
    index: i32,
    count: i32,
    more: bool,
}

impl KeyValueArray {
    pub(crate) fn new(kvs: Vec<KeyValue>, count: i32, more: bool) -> KeyValueArray {
        let index = 0;
        KeyValueArray {
            kvs,
            index,
            count,
            more,
        }
    }
}

#[derive(Debug)]
enum RangeResultStateMachineState {
    Fetching,
    KeyValueArrayAvailable,
    Error,
    Done,
}

#[derive(Debug)]
enum RangeResultStateMachineData {
    Fetching {
        fdb_future_key_value_array: FdbFutureKeyValueArray,
    },
    KeyValueArrayAvailable {
        kvs: Vec<KeyValue>,
        index: i32,
        count: i32,
        more: bool,
    },
    Error {
        fdb_error: FdbError,
    },
    Done,
}

// The variant names match with the sismic events.
#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
enum RangeResultStateMachineEvent {
    FetchOk {
        kvs: Vec<KeyValue>,
        index: i32,
        count: i32,
        more: bool,
    },
    FetchNextBatch {
        fdb_future_key_value_array: FdbFutureKeyValueArray,
    },
    FetchError {
        fdb_error: FdbError,
    },
    FetchDone,
}

// An state machine that returns the key-value pairs in the database
// satisfying the range specified in a range read.
//
// See `sismic/range_result_state_machine.yaml` for the design of the
// state machine.
#[derive(Debug)]
pub(crate) struct RangeResultStateMachine {
    transaction: FdbTransaction,
    snapshot: bool,
    mode: StreamingMode,
    reverse: bool,

    // This is *only* used in case of `StreamingMode::Iterator`. In
    // other cases, we set it to `None`.
    iteration: Option<i32>,

    // When `limit` is `None`, it means that the C API is allowed to
    // choose how many key values it can return. If `limit` is
    // `Some(x)` then that is the *maximum* allowed KVs, but it can
    // return less. Therefore in subsequent calls to `get_range`, we
    // reduce the limit.
    //
    // *Note* When `StreamingMode::Exact` is used, `limit` *must* be
    // specified. However, we don't check for this as binding tester
    // checks for `2210` errors.
    limit: Option<i32>,
    begin_sel: KeySelector,
    end_sel: KeySelector,

    range_result_state_machine_state: RangeResultStateMachineState,
    range_result_state_machine_data: RangeResultStateMachineData,
}

impl RangeResultStateMachine {
    // We need to have these parameters in order to construct a value
    // of `RangeResultStateMachine` type. This is an internal API and
    // the meaning of the parameters documented above.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        transaction: FdbTransaction,
        begin_sel: KeySelector,
        end_sel: KeySelector,
        mode: StreamingMode,
        iteration: Option<i32>,
        reverse: bool,
        limit: Option<i32>,
        snapshot: bool,
        fdb_future_key_value_array: FdbFutureKeyValueArray,
    ) -> RangeResultStateMachine {
        RangeResultStateMachine {
            transaction,
            snapshot,
            mode,
            reverse,
            iteration,
            limit,
            begin_sel,
            end_sel,
            range_result_state_machine_state: RangeResultStateMachineState::Fetching,
            range_result_state_machine_data: RangeResultStateMachineData::Fetching {
                fdb_future_key_value_array,
            },
        }
    }

    pub(crate) fn poll_next(
        mut self: Pin<&mut RangeResultStateMachine>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<FdbResult<KeyValue>>> {
        loop {
            match self.range_result_state_machine_state {
                RangeResultStateMachineState::Fetching => {
                    if let RangeResultStateMachineData::Fetching {
                        ref mut fdb_future_key_value_array,
                    } = self.range_result_state_machine_data
                    {
                        match Pin::new(fdb_future_key_value_array).poll(cx) {
                            Poll::Ready(res) => match res {
                                Ok(key_value_array) => {
                                    let KeyValueArray {
                                        kvs,
                                        index,
                                        count,
                                        more,
                                    } = key_value_array;
                                    if count == 0 {
                                        // In case count is zero, we are done.
                                        self.step_once_with_event(
                                            RangeResultStateMachineEvent::FetchDone,
                                        );
                                    } else {
                                        self.step_once_with_event(
                                            RangeResultStateMachineEvent::FetchOk {
                                                kvs,
                                                index,
                                                count,
                                                more,
                                            },
                                        );
                                    }
                                }
                                Err(fdb_error) => {
                                    self.step_once_with_event(
                                        RangeResultStateMachineEvent::FetchError { fdb_error },
                                    );
                                }
                            },
                            Poll::Pending => return Poll::Pending,
                        }
                    } else {
                        panic!("invalid range_result_state_machine_data");
                    }
                }
                RangeResultStateMachineState::KeyValueArrayAvailable => {
                    if let RangeResultStateMachineData::KeyValueArrayAvailable {
                        ref kvs,
                        ref mut index,
                        count,
                        more,
                    } = self.range_result_state_machine_data
                    {
                        // Unlike in Python, where the `index ==
                        // count` check is done when returning the
                        // last element, in our case the last element
                        // gets returned and in the next call to
                        // `poll_next`, we do our check.
                        if *index == count {
                            // Should we get more?
                            if more {
                                if let Some(0) = self.limit {
                                    self.step_once_with_event(
                                        RangeResultStateMachineEvent::FetchDone,
                                    );
                                } else {
                                    // `limit` is either `None` or
                                    // non-zero.

                                    // iteration, limit, begin_sel and
                                    // end_sel have already been updated
                                    // in the transition action.
                                    let options = match self.limit {
                                        Some(limit) => RangeOptions {
                                            limit,
                                            mode: self.mode,
                                            reverse: self.reverse,
                                        },
                                        None => RangeOptions {
                                            limit: 0,
                                            mode: self.mode,
                                            reverse: self.reverse,
                                        },
                                    };

                                    let fdb_future_key_value_array = fdb_transaction_get_range(
                                        self.transaction.get_c_api_ptr(),
                                        self.begin_sel.clone(),
                                        self.end_sel.clone(),
                                        options,
                                        self.iteration.unwrap_or(0),
                                        self.snapshot,
                                    );

                                    self.step_once_with_event(
                                        RangeResultStateMachineEvent::FetchNextBatch {
                                            fdb_future_key_value_array,
                                        },
                                    );
                                }
                            } else {
                                self.step_once_with_event(RangeResultStateMachineEvent::FetchDone);
                            }
                        } else {
                            // We need to remove elements from the
                            // beginning. If we used `Vec::remove`
                            // that would keep shifting elements to
                            // the left. Instead of modifying `kvs`,
                            // we just clone the element that we need.
                            //
                            // Safety: `index` starts with `0` (set in
                            //          `KeyValueArray::new`) and is
                            //          incremented till it reaches
                            //          `count`.
                            let result = kvs[TryInto::<usize>::try_into(*index).unwrap()].clone();
                            *index += 1;

                            return Poll::Ready(Some(Ok(result)));
                        }
                    } else {
                        panic!("invalid range_result_state_machine_data");
                    }
                }
                RangeResultStateMachineState::Error => {
                    if let RangeResultStateMachineData::Error { fdb_error } =
                        self.range_result_state_machine_data
                    {
                        return Poll::Ready(Some(Err(fdb_error)));
                    } else {
                        panic!("invalid range_result_state_machine_data");
                    }
                }
                RangeResultStateMachineState::Done => return Poll::Ready(None),
            }
        }
    }

    fn step_once_with_event(&mut self, event: RangeResultStateMachineEvent) {
        self.range_result_state_machine_state = match self.range_result_state_machine_state {
            RangeResultStateMachineState::Fetching => match event {
                RangeResultStateMachineEvent::FetchOk {
                    kvs,
                    index,
                    count,
                    more,
                } => {
                    // tansition action

                    // Once we are done with `kvs` we'll we need to
                    // fetch the next batch if `more` is `true`. Do
                    // the required setup for creating the next
                    // `FdbFutureKeyValueArray` in case it is
                    // needed. This would be used by `FetchNextBatch`
                    // event.

                    if more {
                        // This assumes that we have mode to be
                        // `StreamingMode::Iterator`.
                        if let Some(iteration) = self.iteration.as_mut() {
                            *iteration += 1;
                        }

                        if let Some(limit) = self.limit.as_mut() {
                            *limit -= count;
                        }

                        // Safety: We only generate the `FetchOk` event
                        // when count > 0, otherwise we go to `FetchDone`.
                        let last_index = TryInto::<usize>::try_into(count - 1).unwrap();

                        if self.reverse {
                            self.end_sel = KeySelector::first_greater_or_equal(
                                kvs[last_index].get_key().clone(),
                            );
                        } else {
                            self.begin_sel =
                                KeySelector::first_greater_than(kvs[last_index].get_key().clone());
                        }
                    }

                    self.range_result_state_machine_data =
                        RangeResultStateMachineData::KeyValueArrayAvailable {
                            kvs,
                            index,
                            count,
                            more,
                        };
                    RangeResultStateMachineState::KeyValueArrayAvailable
                }
                RangeResultStateMachineEvent::FetchDone => {
                    self.range_result_state_machine_data = RangeResultStateMachineData::Done;
                    RangeResultStateMachineState::Done
                }
                RangeResultStateMachineEvent::FetchError { fdb_error } => {
                    self.range_result_state_machine_data =
                        RangeResultStateMachineData::Error { fdb_error };
                    RangeResultStateMachineState::Error
                }
                _ => panic!("Invalid event!"),
            },
            RangeResultStateMachineState::KeyValueArrayAvailable => match event {
                RangeResultStateMachineEvent::FetchNextBatch {
                    fdb_future_key_value_array,
                } => {
                    self.range_result_state_machine_data = RangeResultStateMachineData::Fetching {
                        fdb_future_key_value_array,
                    };
                    RangeResultStateMachineState::Fetching
                }
                RangeResultStateMachineEvent::FetchDone => {
                    self.range_result_state_machine_data = RangeResultStateMachineData::Done;
                    RangeResultStateMachineState::Done
                }
                _ => panic!("Invalid event!"),
            },
            RangeResultStateMachineState::Error | RangeResultStateMachineState::Done => {
                panic!("Invalid event!");
            }
        }
    }
}

pub(crate) fn fdb_transaction_get_range(
    transaction: *mut fdb_sys::FDBTransaction,
    begin_key: KeySelector,
    end_key: KeySelector,
    options: RangeOptions,
    iteration: i32,
    snapshot: bool,
) -> FdbFutureKeyValueArray {
    let bk = Bytes::from(begin_key.get_key().clone());
    let begin_key_name = bk.as_ref().as_ptr();
    let begin_key_name_length = bk.as_ref().len().try_into().unwrap();
    let begin_or_equal = if begin_key.or_equal() { 1 } else { 0 };
    let begin_offset = begin_key.get_offset();

    let ek = Bytes::from(end_key.get_key().clone());
    let end_key_name = ek.as_ref().as_ptr();
    let end_key_name_length = ek.as_ref().len().try_into().unwrap();
    let end_or_equal = if end_key.or_equal() { 1 } else { 0 };
    let end_offset = end_key.get_offset();

    // This is similar to Java, where calls to `tr.getRange_internal`
    // sets the `target_bytes` to `0`.
    let target_bytes = 0;

    let limit = options.get_limit();
    let mode = options.get_mode().code();
    let reverse = if options.get_reverse() { 1 } else { 0 };

    let s = if snapshot { 1 } else { 0 };

    FdbFuture::new(unsafe {
        fdb_sys::fdb_transaction_get_range(
            transaction,
            begin_key_name,
            begin_key_name_length,
            begin_or_equal,
            begin_offset,
            end_key_name,
            end_key_name_length,
            end_or_equal,
            end_offset,
            limit,
            target_bytes,
            mode,
            iteration,
            s,
            reverse,
        )
    })
}

#[cfg(test)]
mod tests {
    use impls::impls;

    use super::RangeOptions;

    #[test]
    fn impls() {
        #[rustfmt::skip]
        assert!(impls!(
	    RangeOptions:
	    Default));
    }
}
