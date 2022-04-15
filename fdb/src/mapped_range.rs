//! Provides types for working with FDB mapped range.
use bytes::Bytes;

use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::error::{FdbError, FdbResult};
use crate::future::{FdbFuture, FdbFutureMappedKeyValueArray};
use crate::range::{RangeOptions, StreamingMode};
use crate::transaction::FdbTransaction;
use crate::tuple::Tuple;
use crate::{KeySelector, MappedKeyValue, Mapper};

// Java API refers to this type `MappedRangeResult`. It is also very
// similar to `KeyValueArray`. We could potentially in future make
// this type and `KeyValueArray` generic over `T`, where `T` can be
// `MappedKeyValue` or `KeyValue`.
#[derive(Debug)]
pub(crate) struct MappedKeyValueArray {
    mkvs: Vec<MappedKeyValue>,
    index: i32,
    count: i32,
    more: bool,
}

impl MappedKeyValueArray {
    pub(crate) fn new(mkvs: Vec<MappedKeyValue>, count: i32, more: bool) -> MappedKeyValueArray {
        let index = 0;
        MappedKeyValueArray {
            mkvs,
            index,
            count,
            more,
        }
    }
}

#[derive(Debug)]
enum MappedRangeResultStateMachineState {
    Fetching,
    MappedKeyValueArrayAvailable,
    Error,
    Done,
}

#[derive(Debug)]
enum MappedRangeResultStateMachineData {
    Fetching {
        fdb_future_mapped_key_value_array: FdbFutureMappedKeyValueArray,
    },
    MappedKeyValueArrayAvailable {
        mkvs: Vec<MappedKeyValue>,
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
enum MappedRangeResultStateMachineEvent {
    FetchOk {
        mkvs: Vec<MappedKeyValue>,
        index: i32,
        count: i32,
        more: bool,
    },
    FetchNextBatch {
        fdb_future_mapped_key_value_array: FdbFutureMappedKeyValueArray,
    },
    FetchError {
        fdb_error: FdbError,
    },
    FetchDone,
}

// A state machine that returns the mapped key-value pairs from the
// database satisfying the range specified in a range read.
//
// See `sismic/mapped_range_result_state_machine.yaml` for the design
// of the state machine.
#[derive(Debug)]
pub(crate) struct MappedRangeResultStateMachine {
    transaction: FdbTransaction,
    snapshot: bool,
    mode: StreamingMode,
    reverse: bool,
    mapper: Mapper,

    // This is *only* used in case of `StreamingMode::Iterator`. In
    // other cases, we set it to `None`.
    iteration: Option<i32>,

    // When `limit` is `None`, it means that the C API is allowed to
    // choose how many key values it can return. If `limit` is
    // `Some(x)` then that is the *maximum* allowed mapped KVs, but it
    // can return less. Therefore in subsequent calls to
    // `get_mapped_range`, we reduce the limit.
    //
    // *Note* When `StreamingMode::Exact` is used, `limit` *must* be
    // specified. However, we don't check for this as binding tester
    // checks for `2210` errors.
    limit: Option<i32>,
    begin_sel: KeySelector,
    end_sel: KeySelector,

    mapped_range_result_state_machine_state: MappedRangeResultStateMachineState,
    mapped_range_result_state_machine_data: MappedRangeResultStateMachineData,
}

impl MappedRangeResultStateMachine {
    // We need to have these parameters in order to construct a value
    // of `MappedRangeResultStateMachine` type. This is an internal
    // API and the meaning of the parameters documented above.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        transaction: FdbTransaction,
        begin_sel: KeySelector,
        end_sel: KeySelector,
        mapper: Mapper,
        mode: StreamingMode,
        iteration: Option<i32>,
        reverse: bool,
        limit: Option<i32>,
        snapshot: bool,
        fdb_future_mapped_key_value_array: FdbFutureMappedKeyValueArray,
    ) -> MappedRangeResultStateMachine {
        MappedRangeResultStateMachine {
            transaction,
            snapshot,
            mode,
            reverse,
            mapper,
            iteration,
            limit,
            begin_sel,
            end_sel,
            mapped_range_result_state_machine_state: MappedRangeResultStateMachineState::Fetching,
            mapped_range_result_state_machine_data: MappedRangeResultStateMachineData::Fetching {
                fdb_future_mapped_key_value_array,
            },
        }
    }

    pub(crate) fn poll_next(
        mut self: Pin<&mut MappedRangeResultStateMachine>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<FdbResult<MappedKeyValue>>> {
        loop {
            match self.mapped_range_result_state_machine_state {
                MappedRangeResultStateMachineState::Fetching => {
                    if let MappedRangeResultStateMachineData::Fetching {
                        ref mut fdb_future_mapped_key_value_array,
                    } = self.mapped_range_result_state_machine_data
                    {
                        match Pin::new(fdb_future_mapped_key_value_array).poll(cx) {
                            Poll::Ready(res) => match res {
                                Ok(mapped_key_value_array) => {
                                    let MappedKeyValueArray {
                                        mkvs,
                                        index,
                                        count,
                                        more,
                                    } = mapped_key_value_array;
                                    if count == 0 {
                                        // In case count is zero, we are done.
                                        self.step_once_with_event(
                                            MappedRangeResultStateMachineEvent::FetchDone,
                                        );
                                    } else {
                                        self.step_once_with_event(
                                            MappedRangeResultStateMachineEvent::FetchOk {
                                                mkvs,
                                                index,
                                                count,
                                                more,
                                            },
                                        );
                                    }
                                }
                                Err(fdb_error) => {
                                    self.step_once_with_event(
                                        MappedRangeResultStateMachineEvent::FetchError {
                                            fdb_error,
                                        },
                                    );
                                }
                            },
                            Poll::Pending => return Poll::Pending,
                        }
                    } else {
                        panic!("invalid mapped_range_result_state_machine_data");
                    }
                }
                MappedRangeResultStateMachineState::MappedKeyValueArrayAvailable => {
                    if let MappedRangeResultStateMachineData::MappedKeyValueArrayAvailable {
                        ref mkvs,
                        ref mut index,
                        count,
                        more,
                    } = self.mapped_range_result_state_machine_data
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
                                        MappedRangeResultStateMachineEvent::FetchDone,
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

                                    let fdb_future_mapped_key_value_array =
                                        fdb_transaction_get_mapped_range(
                                            self.transaction.get_c_api_ptr(),
                                            self.begin_sel.clone(),
                                            self.end_sel.clone(),
                                            self.mapper.clone(),
                                            options,
                                            self.iteration.unwrap_or(0),
                                            self.snapshot,
                                        );

                                    self.step_once_with_event(
                                        MappedRangeResultStateMachineEvent::FetchNextBatch {
                                            fdb_future_mapped_key_value_array,
                                        },
                                    );
                                }
                            } else {
                                self.step_once_with_event(
                                    MappedRangeResultStateMachineEvent::FetchDone,
                                );
                            }
                        } else {
                            // We need to remove elements from the
                            // beginning. If we used `Vec::remove`
                            // that would keep shifting elements to
                            // the left. Instead of modifying `mkvs`,
                            // we just clone the element that we need.
                            //
                            // Safety: `index` starts with `0` (set in
                            //          `MappedKeyValueArray::new`)
                            //          and is incremented till it
                            //          reaches `count`.
                            let result = mkvs[TryInto::<usize>::try_into(*index).unwrap()].clone();
                            *index += 1;

                            return Poll::Ready(Some(Ok(result)));
                        }
                    } else {
                        panic!("invalid mapped_range_result_state_machine_data");
                    }
                }
                MappedRangeResultStateMachineState::Error => {
                    if let MappedRangeResultStateMachineData::Error { fdb_error } =
                        self.mapped_range_result_state_machine_data
                    {
                        return Poll::Ready(Some(Err(fdb_error)));
                    } else {
                        panic!("invalid mapped_range_result_state_machine_data");
                    }
                }
                MappedRangeResultStateMachineState::Done => return Poll::Ready(None),
            }
        }
    }

    fn step_once_with_event(&mut self, event: MappedRangeResultStateMachineEvent) {
        self.mapped_range_result_state_machine_state =
            match self.mapped_range_result_state_machine_state {
                MappedRangeResultStateMachineState::Fetching => match event {
                    MappedRangeResultStateMachineEvent::FetchOk {
                        mkvs,
                        index,
                        count,
                        more,
                    } => {
                        // tansition action

                        // Once we are done with `mkvs` we'll we need
                        // to fetch the next batch if `more` is
                        // `true`. Do the required setup for creating
                        // the next `FdbFutureMappedKeyValueArray` in
                        // case it is needed. This would be used by
                        // `FetchNextBatch` event.

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
                                    mkvs[last_index].get_key_value_ref().get_key_ref().clone(),
                                );
                            } else {
                                self.begin_sel = KeySelector::first_greater_than(
                                    mkvs[last_index].get_key_value_ref().get_key_ref().clone(),
                                );
                            }
                        }

                        self.mapped_range_result_state_machine_data =
                            MappedRangeResultStateMachineData::MappedKeyValueArrayAvailable {
                                mkvs,
                                index,
                                count,
                                more,
                            };
                        MappedRangeResultStateMachineState::MappedKeyValueArrayAvailable
                    }
                    MappedRangeResultStateMachineEvent::FetchDone => {
                        self.mapped_range_result_state_machine_data =
                            MappedRangeResultStateMachineData::Done;
                        MappedRangeResultStateMachineState::Done
                    }
                    MappedRangeResultStateMachineEvent::FetchError { fdb_error } => {
                        self.mapped_range_result_state_machine_data =
                            MappedRangeResultStateMachineData::Error { fdb_error };
                        MappedRangeResultStateMachineState::Error
                    }
                    _ => panic!("Invalid event!"),
                },
                MappedRangeResultStateMachineState::MappedKeyValueArrayAvailable => match event {
                    MappedRangeResultStateMachineEvent::FetchNextBatch {
                        fdb_future_mapped_key_value_array,
                    } => {
                        self.mapped_range_result_state_machine_data =
                            MappedRangeResultStateMachineData::Fetching {
                                fdb_future_mapped_key_value_array,
                            };
                        MappedRangeResultStateMachineState::Fetching
                    }
                    MappedRangeResultStateMachineEvent::FetchDone => {
                        self.mapped_range_result_state_machine_data =
                            MappedRangeResultStateMachineData::Done;
                        MappedRangeResultStateMachineState::Done
                    }
                    _ => panic!("Invalid event!"),
                },
                MappedRangeResultStateMachineState::Error
                | MappedRangeResultStateMachineState::Done => {
                    panic!("Invalid event!");
                }
            }
    }
}

pub(crate) fn fdb_transaction_get_mapped_range(
    transaction: *mut fdb_sys::FDBTransaction,
    begin_key: KeySelector,
    end_key: KeySelector,
    mapper: Mapper,
    options: RangeOptions,
    iteration: i32,
    snapshot: bool,
) -> FdbFutureMappedKeyValueArray {
    let (key, begin_or_equal, begin_offset) = begin_key.deconstruct();
    let bk = Bytes::from(key);
    let begin_key_name = bk.as_ref().as_ptr();
    let begin_key_name_length = bk.as_ref().len().try_into().unwrap();
    let begin_or_equal = if begin_or_equal { 1 } else { 0 };

    let (key, end_or_equal, end_offset) = end_key.deconstruct();
    let ek = Bytes::from(key);
    let end_key_name = ek.as_ref().as_ptr();
    let end_key_name_length = ek.as_ref().len().try_into().unwrap();
    let end_or_equal = if end_or_equal { 1 } else { 0 };

    let mapper = Tuple::from(mapper).pack();
    let mapper_name = mapper.as_ref().as_ptr();
    let mapper_name_length = mapper.as_ref().len().try_into().unwrap();

    // This is similar to Java, where calls to
    // `tr.getMappedRange_internal` sets the `target_bytes` to `0`.
    let target_bytes = 0;

    let limit = options.get_limit();
    let mode = options.get_mode().code();
    let reverse = if options.get_reverse() { 1 } else { 0 };

    let s = if snapshot { 1 } else { 0 };

    FdbFuture::new(unsafe {
        fdb_sys::fdb_transaction_get_mapped_range(
            transaction,
            begin_key_name,
            begin_key_name_length,
            begin_or_equal,
            begin_offset,
            end_key_name,
            end_key_name_length,
            end_or_equal,
            end_offset,
            mapper_name,
            mapper_name_length,
            limit,
            target_bytes,
            mode,
            iteration,
            s,
            reverse,
        )
    })
}
