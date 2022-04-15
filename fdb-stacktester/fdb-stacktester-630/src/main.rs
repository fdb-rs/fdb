use bytes::{Buf, BufMut, Bytes, BytesMut};

use dashmap::DashMap;

use fdb::database::{DatabaseOption, FdbDatabase};
use fdb::error::{
    FdbError, FdbResult, TUPLE_PACK_WITH_VERSIONSTAMP_MULTIPLE_FOUND,
    TUPLE_PACK_WITH_VERSIONSTAMP_NOT_FOUND,
};
use fdb::future::{FdbFutureKey, FdbFutureUnit};
use fdb::range::{Range, RangeOptions, StreamingMode};
use fdb::subspace::Subspace;
use fdb::transaction::{
    FdbReadTransaction, FdbTransaction, MutationType, ReadTransaction, Transaction,
    TransactionOption,
};
use fdb::tuple::{key_util, Tuple, Versionstamp};
use fdb::{KeySelector, KeyValue};

// This code is automatically generated, so we can ignore the
// warnings.
use fdb_sys::{
    FDBStreamingMode_FDB_STREAMING_MODE_EXACT, FDBStreamingMode_FDB_STREAMING_MODE_ITERATOR,
    FDBStreamingMode_FDB_STREAMING_MODE_LARGE, FDBStreamingMode_FDB_STREAMING_MODE_MEDIUM,
    FDBStreamingMode_FDB_STREAMING_MODE_SERIAL, FDBStreamingMode_FDB_STREAMING_MODE_SMALL,
    FDBStreamingMode_FDB_STREAMING_MODE_WANT_ALL,
};

use itertools::Itertools;

use num_bigint::BigInt;

use tokio::runtime::Runtime;
use tokio::sync::mpsc::{self, Sender, UnboundedSender};
use tokio::time::{sleep, Duration};

use tokio_stream::StreamExt;

use uuid::Uuid;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::env;
use std::error::Error;
use std::future::Future;
use std::ops::Range as OpsRange;
use std::sync::Arc;

const VERBOSE: bool = false;
const VERBOSE_INST_RANGE: Option<OpsRange<usize>> = None;
const VERBOSE_INST_ONLY: bool = false;

// `TRANSACTION_NAME` and thread `PREFIX` are maintained seperately in
// the stack machine. In the `StackMachine` type `TRANSACTION_NAME`
// maps to `tr_name` field and thread `PREFIX` maps to `prefix` field.
//
// `PREFIX` is passed either via command line or using the
// `START_THREAD` operation.
//
// While it is not mentioned explicity, from [this] Go binding code we
// can infer that when a value of `StackMachine` type is created,
// `tr_name` would be a copy of the `prefix` value.
//
// `NEW_TRANSACTION` and `USE_TRANSACTION` operations operate on
// `tr_name`.
//
// `TrMap` maps `tr_name` with a `*mut FdbTransaction`.
//
// In Go, `trMap` is of type `map[string]fdb.Transaction{}`. We are
// using `Bytes` for our key as that is what the spec says.
//
// [this]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/go/src/_stacktester/stacktester.go#L88
type TrMap = Arc<DashMap<Bytes, FdbTransaction>>;

// Semantically stack entries can either contain `FdbFuture` or they
// may not contain `FdbFuture`. We use types `NonFutureStackEntry` and
// `StackEntry` to represent these.

// These are items that can go on the stack.
//
// We don't construct variants `FdbFutureI64(FdbFutureI64)`,
// `FdbFutureCStringArray(FdbFutureCStringArray)`.
#[derive(Debug)]
enum StackEntryItem {
    FdbFutureKey(FdbFutureKey),
    FdbFutureUnit(FdbFutureUnit),
    BigInt(BigInt),
    Bool(bool),
    Bytes(Bytes),
    Float(f32),
    Double(f64),
    Null,
    String(String),
    Tuple(Tuple),
    Uuid(Uuid),
    Versionstamp(Versionstamp),
}

impl StackEntryItem {
    fn into_non_future_stack_entry_item(self) -> Option<NonFutureStackEntryItem> {
        match self {
            StackEntryItem::BigInt(b) => Some(NonFutureStackEntryItem::BigInt(b)),
            StackEntryItem::Bool(b) => Some(NonFutureStackEntryItem::Bool(b)),
            StackEntryItem::Bytes(b) => Some(NonFutureStackEntryItem::Bytes(b)),
            StackEntryItem::Float(f) => Some(NonFutureStackEntryItem::Float(f)),
            StackEntryItem::Double(f) => Some(NonFutureStackEntryItem::Double(f)),
            StackEntryItem::Null => Some(NonFutureStackEntryItem::Null),
            StackEntryItem::String(s) => Some(NonFutureStackEntryItem::String(s)),
            StackEntryItem::Tuple(t) => Some(NonFutureStackEntryItem::Tuple(t)),
            StackEntryItem::Uuid(u) => Some(NonFutureStackEntryItem::Uuid(u)),
            StackEntryItem::Versionstamp(vs) => Some(NonFutureStackEntryItem::Versionstamp(vs)),
            _ => None,
        }
    }

    fn checked_clone(&self) -> Option<StackEntryItem> {
        match self {
            StackEntryItem::BigInt(bi) => Some(StackEntryItem::BigInt(bi.clone())),
            StackEntryItem::Bool(b) => Some(StackEntryItem::Bool(*b)),
            StackEntryItem::Bytes(b) => Some(StackEntryItem::Bytes(b.clone())),
            StackEntryItem::Float(f) => Some(StackEntryItem::Float(*f)),
            StackEntryItem::Double(d) => Some(StackEntryItem::Double(*d)),
            StackEntryItem::Null => Some(StackEntryItem::Null),
            StackEntryItem::String(s) => Some(StackEntryItem::String(s.clone())),
            StackEntryItem::Tuple(t) => Some(StackEntryItem::Tuple(t.clone())),
            StackEntryItem::Uuid(u) => Some(StackEntryItem::Uuid(*u)),
            StackEntryItem::Versionstamp(v) => Some(StackEntryItem::Versionstamp(v.clone())),
            _ => None,
        }
    }
}

// This is a subset of `StackEntryItem` that does not contain any
// `FdbFuture`.
#[derive(Debug, Clone)]
enum NonFutureStackEntryItem {
    BigInt(BigInt),
    Bool(bool),
    Bytes(Bytes),
    Float(f32),
    Double(f64),
    Null,
    String(String),
    Tuple(Tuple),
    Uuid(Uuid),
    Versionstamp(Versionstamp),
}

impl NonFutureStackEntryItem {
    fn into_stack_entry_item(self) -> StackEntryItem {
        match self {
            NonFutureStackEntryItem::BigInt(b) => StackEntryItem::BigInt(b),
            NonFutureStackEntryItem::Bool(b) => StackEntryItem::Bool(b),
            NonFutureStackEntryItem::Bytes(b) => StackEntryItem::Bytes(b),
            NonFutureStackEntryItem::Float(f) => StackEntryItem::Float(f),
            NonFutureStackEntryItem::Double(f) => StackEntryItem::Double(f),
            NonFutureStackEntryItem::Null => StackEntryItem::Null,
            NonFutureStackEntryItem::String(s) => StackEntryItem::String(s),
            NonFutureStackEntryItem::Tuple(t) => StackEntryItem::Tuple(t),
            NonFutureStackEntryItem::Uuid(u) => StackEntryItem::Uuid(u),
            NonFutureStackEntryItem::Versionstamp(vs) => StackEntryItem::Versionstamp(vs),
        }
    }
}

#[derive(Debug, Clone)]
struct NonFutureStackEntry {
    item: NonFutureStackEntryItem,
    inst_number: usize,
}

#[derive(Debug)]
struct StackEntry {
    item: StackEntryItem,
    inst_number: usize,
}

impl StackEntry {
    fn into_non_future_stack_entry(self) -> Option<NonFutureStackEntry> {
        let StackEntry { item, inst_number } = self;

        item.into_non_future_stack_entry_item()
            .map(|item| NonFutureStackEntry { item, inst_number })
    }
}

#[derive(Debug)]
enum StartThreadTaskMessage {
    Exec {
        prefix: Bytes,
        db: FdbDatabase,
        tr_map: TrMap,
        task_finished: Sender<()>,
        start_thread_task_send: UnboundedSender<StartThreadTaskMessage>,
    },
}

// We need to have `db` and `task_finished` because for `START_THREAD`
// operation we'll need to create a new Tokio task which will requires
// us to clone `db` and `task_finished`. If the `StackMachine` does
// not create a new Tokio task, `task_finished` will get dropped when
// `StackMachine` is dropped.
#[derive(Debug)]
struct StackMachine {
    tr_map: TrMap,

    prefix: Bytes,
    tr_name: Bytes,
    stack: Vec<StackEntry>,
    verbose: bool,
    db: FdbDatabase,
    last_version: i64,

    task_finished: Sender<()>,
    start_thread_task_send: UnboundedSender<StartThreadTaskMessage>,
}

impl StackMachine {
    fn new(
        tr_map: TrMap,
        prefix: Bytes,
        db: FdbDatabase,
        task_finished: Sender<()>,
        start_thread_task_send: UnboundedSender<StartThreadTaskMessage>,
        verbose: bool,
    ) -> StackMachine {
        let stack = Vec::new();
        let tr_name = prefix.clone();
        let last_version = 0;

        StackMachine {
            tr_map,
            prefix,
            tr_name,
            stack,
            verbose,
            db,
            last_version,
            task_finished,
            start_thread_task_send,
        }
    }

    async fn exec(
        prefix: Bytes,
        db: FdbDatabase,
        tr_map: TrMap,
        task_finished: Sender<()>,
        start_thread_task_send: UnboundedSender<StartThreadTaskMessage>,
    ) {
        let prefix_clone = prefix.clone();
        let prefix_clone_ref = &prefix_clone;

        let kvs = db
            .read(|tr| async move {
                let mut res = Vec::new();

                let ops_tup: (Bytes,) = (prefix_clone_ref.clone(),);

                let mut range_stream = {
                    let mut tup = Tuple::new();

                    tup.add_bytes(ops_tup.0);

                    tup
                }
                .range(Bytes::new())
                .into_stream(&tr, RangeOptions::default());

                while let Some(x) = range_stream.next().await {
                    let kv = x?;
                    res.push(kv);
                }

                Ok(res)
            })
            .await
            .unwrap_or_else(|err| panic!("Error occurred during `read`: {:?}", err));

        let mut sm = StackMachine::new(
            tr_map,
            prefix,
            db,
            task_finished,
            start_thread_task_send,
            VERBOSE,
        );

        for (inst_number, kv) in kvs.into_iter().enumerate() {
            let inst = Tuple::from_bytes(kv.into_value()).unwrap_or_else(|err| {
                panic!("Error occurred during `Tuple::from_bytes`: {:?}", err)
            });
            sm.process_inst(inst_number, inst).await;
        }
    }

    // From the spec [1]
    //
    // An operation may have a second element which provides
    // additional data, which may be of any tuple type.
    //
    // [1]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/spec/bindingApiTester.md#overview
    fn get_additional_inst_data(inst: &Tuple) -> StackEntryItem {
        inst.get_bigint(1)
            .map(StackEntryItem::BigInt)
            .or_else(|_| inst.get_bool(1).map(StackEntryItem::Bool))
            .or_else(|_| {
                inst.get_bytes_ref(1)
                    .map(|b| StackEntryItem::Bytes(b.clone()))
            })
            .or_else(|_| inst.get_f32(1).map(StackEntryItem::Float))
            .or_else(|_| inst.get_f64(1).map(StackEntryItem::Double))
            .or_else(|_| inst.get_null(1).map(|_| StackEntryItem::Null))
            .or_else(|_| {
                inst.get_string_ref(1)
                    .map(|s| StackEntryItem::String(s.clone()))
            })
            .or_else(|_| {
                inst.get_tuple_ref(1)
                    .map(|t| StackEntryItem::Tuple(t.clone()))
            })
            .or_else(|_| inst.get_uuid_ref(1).map(|u| StackEntryItem::Uuid(*u)))
            .or_else(|_| {
                inst.get_versionstamp_ref(1)
                    .map(|v| StackEntryItem::Versionstamp(v.clone()))
            })
            .unwrap_or_else(|err| {
                panic!(
                    "Error occurred during `instruction_additonal_data`: {:?}",
                    err
                )
            })
    }

    // This is used for `ATOMIC_OP(_DATABASE)` operation. The `OPTYPE`
    // is pushed on the stack as a string. The list of `OPTYPE`s that
    // is pushed onto the stack maintained here [1] in `atomic_ops`
    // variable. If new `OPTYPE`s are added in future, then we'll need
    // to update this method.
    //
    // [1]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/tests/api.py#L177
    fn from_mutation_type_string(s: String) -> MutationType {
        match s.as_str() {
            "BIT_AND" => MutationType::BitAnd,
            "BIT_OR" => MutationType::BitOr,
            "MAX" => MutationType::Max,
            "MIN" => MutationType::Min,
            "BYTE_MIN" => MutationType::ByteMin,
            "BYTE_MAX" => MutationType::ByteMax,
            "ADD" => MutationType::Add,
            "BIT_XOR" => MutationType::BitXor,
            "APPEND_IF_FITS" => MutationType::AppendIfFits,
            "SET_VERSIONSTAMPED_KEY" => MutationType::SetVersionstampedKey,
            "SET_VERSIONSTAMPED_VALUE" => MutationType::SetVersionstampedValue,
            "COMPARE_AND_CLEAR" => MutationType::CompareAndClear,
            _ => panic!("Invalid mutation type string provided: {:?}", s),
        }
    }

    fn bigint_to_bool(bi: BigInt) -> bool {
        // non-zero is `true`.
        bi != BigInt::from(0)
    }

    // There is a crate only method `.code()` on `StreamingMode` that
    // gives converts a `StreamingMode` into C API level
    // constant. This method does the opposite. It takes the C API
    // level constant and returns a value of `StreamingMode` type.
    //
    // If additional streaming modes are added in the future, this API
    // will need to be revised.
    //
    // It will panic if the conversion fails.
    #[allow(non_upper_case_globals)]
    fn from_streaming_mode_code(code: i32) -> StreamingMode {
        match code {
            FDBStreamingMode_FDB_STREAMING_MODE_WANT_ALL => StreamingMode::WantAll,
            FDBStreamingMode_FDB_STREAMING_MODE_ITERATOR => StreamingMode::Iterator,
            FDBStreamingMode_FDB_STREAMING_MODE_EXACT => StreamingMode::Exact,
            FDBStreamingMode_FDB_STREAMING_MODE_SMALL => StreamingMode::Small,
            FDBStreamingMode_FDB_STREAMING_MODE_MEDIUM => StreamingMode::Medium,
            FDBStreamingMode_FDB_STREAMING_MODE_LARGE => StreamingMode::Large,
            FDBStreamingMode_FDB_STREAMING_MODE_SERIAL => StreamingMode::Serial,
            _ => panic!("Invalid streaming mode code provided {:?}", code),
        }
    }

    async fn process_inst(&mut self, inst_number: usize, inst: Tuple) {
        let mut op = inst
            .get_string_ref(0)
            .unwrap_or_else(|err| panic!("Error occurred during `inst.get_string_ref`: {:?}", err))
            .clone();

        let verbose_inst_range = VERBOSE_INST_RANGE
            .map(|x| x.contains(&inst_number))
            .unwrap_or(false);

        if self.verbose || verbose_inst_range {
            println!("Stack from [");
            self.dump_stack();
            println!(" ] ({})", self.stack.len());
            println!("{}. Instruction is {} ({:?})", inst_number, op, self.prefix);
        } else if VERBOSE_INST_ONLY {
            println!("{}. Instruction is {} ({:?})", inst_number, op, self.prefix);
        }

        if [
            "NEW_TRANSACTION",
            "PUSH",
            "DUP",
            "EMPTY_STACK",
            "SWAP",
            "POP",
            "SUB",
            "CONCAT",
            "LOG_STACK",
            "START_THREAD",
            "UNIT_TESTS",
        ]
        .contains(&op.as_str())
        {
            // For these instructions we can potentially have an
            // invalid `TrMap` entry (which can call
            // `current_transaction` to fail) *or* we don't need to
            // use `current_transaction` as it only operates on the
            // stack.
            match op.as_str() {
                "NEW_TRANSACTION" => {
                    // `NEW_TRANSACTION` op is special. We assume that
                    // we have a valid transaction below in
                    // `self.current_transaction`. However, we won't
                    // have a valid transaction till `NEW_TRANSACTION`
                    // instruction create it.

                    self.new_transaction();
                }
                // Data Operations [1]
                //
                // [1]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/spec/bindingApiTester.md#data-operations
                "PUSH" => self.store(inst_number, StackMachine::get_additional_inst_data(&inst)),
                "DUP" => {
                    let entry = &self.stack[self.stack.len() - 1];
                    let entry_inst_number = entry.inst_number;
                    let entry_item = entry
                        .item
                        .checked_clone()
                        .expect("DUP instruction called on a stack item containing a future");
                    self.store(entry_inst_number, entry_item);
                }
                "EMPTY_STACK" => self.stack.clear(),
                "SWAP" => {
                    // We assume that the `INDEX` is stored using the
                    // variant `StackEntryItem::BigInt(..)`, which we then
                    // convert to a `usize`.
                    let index = if let StackEntryItem::BigInt(b) = self.stack.pop().unwrap().item {
                        usize::try_from(b).unwrap_or_else(|err| {
                            panic!("Error occurred during `try_from`: {:?}", err)
                        })
                    } else {
                        panic!("Expected StackEntryItem::BigInt variant, which was not found!");
                    };

                    let depth_0 = self.stack.len() - 1;
                    let depth_index = depth_0 - index;

                    self.stack.swap(depth_0, depth_index);
                }
                "POP" => {
                    self.stack.pop();
                }
                "SUB" => {
                    let a = if let StackEntryItem::BigInt(bi) = self.stack.pop().unwrap().item {
                        bi
                    } else {
                        panic!("Expected StackEntryItem::BigInt variant, which was not found!");
                    };
                    let b = if let StackEntryItem::BigInt(bi) = self.stack.pop().unwrap().item {
                        bi
                    } else {
                        panic!("Expected StackEntryItem::BigInt variant, which was not found!");
                    };
                    self.store(inst_number, StackEntryItem::BigInt(a - b));
                }
                "CONCAT" => {
                    let mut outer_a = self.stack.pop().unwrap().item;
                    let mut outer_b = self.stack.pop().unwrap().item;

                    // The block below is hack. It looks like
                    // `scripted` tests pushes a `FdbFutureKey`.
                    // . So, if either `outer_a` or `outer_b` is a
                    // future, then `.await` on it and covert it into
                    // `StackEntryItem::Bytes`.
                    //
                    // This is a hacky compromise.
                    {
                        if let StackEntryItem::FdbFutureKey(fdb_future_key) = outer_a {
                            outer_a = fdb_future_key
                                .await
                                .map(|x| StackEntryItem::Bytes(x.into()))
                                .unwrap_or_else(|err| {
                                    panic!("Error occurred during `.await`: {:?}", err)
                                });
                        }

                        if let StackEntryItem::FdbFutureKey(fdb_future_key) = outer_b {
                            outer_b = fdb_future_key
                                .await
                                .map(|x| StackEntryItem::Bytes(x.into()))
                                .unwrap_or_else(|err| {
                                    panic!("Error occurred during `.await`: {:?}", err)
                                });
                        }
                    }
                    match (outer_a, outer_b) {
			(StackEntryItem::Bytes(a), StackEntryItem::Bytes(b)) => {
			    let mut res = BytesMut::new();
			    res.put(a);
			    res.put(b);
			    self.store(inst_number, StackEntryItem::Bytes(res.into()));
			},
			(StackEntryItem::String(a), StackEntryItem::String(b)) => {
			    let mut res = String::new();
			    res.push_str(&a);
			    res.push_str(&b);
			    self.store(inst_number, StackEntryItem::String(res));
			},
			_ => panic!("Expected StackEntryItem::Bytes or StackEntryItem::String variants, which was not found!"),
		    }
                }
                "LOG_STACK" => {
                    // Here we assume that when running in `--compare` mode
                    // the stack *does not* have any `FdbFuture`
                    // variants, so we can successfully log the entire
                    // stack, *without* having to wait on any
                    // `FdbFuture`.
                    //
                    // In `--concurrency` mode, the stack can have a
                    // future in it, which we just ignore. This means,
                    // `--compare` mode and `--concurrency` modes are
                    // mutually exclusive.
                    //
                    // In both the cases `fdb-stacktester` won't panic
                    // here. Error has be detected by
                    // `bindingtester.py`.
                    //
                    // `log_prefix` is `PREFIX`.
                    let log_prefix =
                        if let StackEntryItem::Bytes(b) = self.stack.pop().unwrap().item {
                            b
                        } else {
                            panic!("Expected StackEntryItem::Bytes variant, which was not found!");
                        };

                    let mut entries = HashMap::new();

                    while !self.stack.is_empty() {
                        let k = self.stack.len() - 1;

                        self.stack
                            .pop()
                            .unwrap()
                            .into_non_future_stack_entry()
                            .map(|i| entries.insert(k, i));

                        // Do the transaction on 100 entries.
                        if entries.len() == 100 {
                            self.log_stack(entries.clone(), log_prefix.clone()).await;
                            entries.clear();
                        }
                    }

                    // Log remaining entires.
                    if !entries.is_empty() {
                        self.log_stack(entries, log_prefix).await;
                    }
                }
                // Thread Operations
                //
                // `WAIT_EMPTY` uses `current_transaction`.
                "START_THREAD" => {
                    let prefix = if let StackEntryItem::Bytes(b) = self.stack.pop().unwrap().item {
                        b
                    } else {
                        panic!("StackEntryItem::Bytes was expected, but not found");
                    };

                    let db = self.db.clone();
                    let tr_map = self.tr_map.clone();
                    let task_finished = self.task_finished.clone();
                    let start_thread_task_send = self.start_thread_task_send.clone();

                    // We need to do this round about message passing
                    // dance because trying to call
                    // `tokio::spawn(StackMachine::exec(...)` will
                    // result in a async recursion.
                    self.start_thread_task_send
                        .send(StartThreadTaskMessage::Exec {
                            prefix,
                            db,
                            tr_map,
                            task_finished,
                            start_thread_task_send,
                        })
                        .unwrap_or_else(|err| {
                            panic!(
                                "Error occurred during `start_thread_task_send.send`: {:?}",
                                err
                            )
                        });
                }
                // Miscellaneous
                "UNIT_TESTS" => {
                    self.test_db_options();

                    // We don't have `select_api_version` tests like
                    // Go and Java because in our case, trying to call
                    // `fdb::select_api_version` more than once will
                    // cause a panic. We have integration tests for
                    // `fdb::select_api_version`.

                    self.test_tr_options().await;

                    self.test_watches().await;
                    self.test_locality().await;
                }
                _ => panic!("Unhandled operation {}", op),
            }
        } else {
            // In the Go bindings, there is no `is_snapshot`.
            let mut is_snapshot = false;
            let mut is_database = false;

            let tr = self.current_transaction();
            let tr_snap = tr.snapshot();

            if op.ends_with("_SNAPSHOT") {
                op.drain((op.len() - 9)..);
                is_snapshot = true;
            } else if op.ends_with("_DATABASE") {
                op.drain((op.len() - 9)..);
                is_database = true;
            }

            match op.as_str() {
                // FoundationDB Operations [1]
                //
                // While there is no classification of FoundationDB
                // operations in the spec, we have a classification in the
                // `api.py` test generator [2].
                //
                // Following order is followed.
                // - resets
                // - tuples
                // - versions, snapshot_versions
                // - reads, snapshot_reads, database_reads
                // - mutations, database_mutations
                // - read_conflicts
                // - write_conflicts
                // - txn_sizes
                // - storage_metrics
                //
                // Then we do Thread Operations [3] and Miscellaneous [4]
                //
                // `NEW_TRANSACTION` is taken care of at the beginning
                // of this function.
                //
                // [1]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/spec/bindingApiTester.md#foundationdb-operations
                // [2]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/tests/api.py#L143-L174
                // [3]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/spec/bindingApiTester.md#thread-operations
                // [4]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/spec/bindingApiTester.md#miscellaneous
                "USE_TRANSACTION" => {
                    let tr_name = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    };
                    self.switch_transaction(tr_name);
                }
                "COMMIT" => {
                    let item = StackEntryItem::FdbFutureUnit(unsafe {
                        self.current_transaction().commit()
                    });
                    self.store(inst_number, item);
                }
                "WAIT_FUTURE" => {
                    let NonFutureStackEntry {
                        // inst_number is passed in as one of the
                        // parameters to this function. We don't want to
                        // use that.
                        inst_number: i,
                        item,
                    } = self.pop().await;

		    self.store(i, item.into_stack_entry_item());
                }
                // resets
                "ON_ERROR" => {
                    let fdb_error = FdbError::new(
                        i32::try_from(
                            if let NonFutureStackEntryItem::BigInt(b) = self.pop().await.item {
                                b
                            } else {
                                panic!(
                                    "NonFutureStackEntryItem::BigInt was expected, but not found"
                                );
                            },
                        )
                        .unwrap_or_else(|err| {
                            panic!(
                                "Expected i32 inside BigInt, but conversion failed {:?}",
                                err
                            )
                        }),
                    );
                    // We are just pushing the future here, and
                    // letting the `WAIT_FUTURE` instruction take care
                    // of `join`-ing and indicating
                    // `RESULT_NOT_PRESENT` or `ERROR`.
                    self.store(
                        inst_number,
                        StackEntryItem::FdbFutureUnit(unsafe {
                            self.current_transaction().on_error(fdb_error)
                        }),
                    );
                }
                // In Java bindings, this is `self.new_transaction()`,
                // but Go bindings uses `reset()` on the transaction.
                "RESET" => unsafe { self.current_transaction().reset() },
                "CANCEL" => unsafe {
                    self.current_transaction().cancel();
                },
                "GET_VERSIONSTAMP" => {
                    let item = StackEntryItem::FdbFutureKey(unsafe {
                        self.current_transaction()
                            .get_versionstamp()
                            .get_inner_future()
                    });

                    self.store(inst_number, item);
                }
                // Take care of `GET_READ_VERSION`, `SET`, `ATOMIC_OP` here as it
                // is one of the first APIs that is needed for binding
                // tester to work.
                "GET_READ_VERSION" => {
                    let fn_mut_closure_t = |t: FdbTransaction| async move {
                        unsafe { t.get_read_version() }.await
                    };

                    let fn_mut_closure_rt = |rt: FdbReadTransaction| async move {
                        unsafe { rt.get_read_version() }.await
                    };

		    match if is_database {
			self.execute_read_db(fn_mut_closure_rt).await
		    } else if is_snapshot {
			unsafe { self.execute_read_snap(fn_mut_closure_rt, &tr_snap) }.await
		    } else {
			unsafe { self.execute_read_tr(fn_mut_closure_t, &tr) }.await
		    } {
                        Ok(last_version) => {
                            self.last_version = last_version;

                            self.store(
                                inst_number,
                                StackEntryItem::Bytes(Bytes::from_static(b"GOT_READ_VERSION")),
                            );
                        }
                        Err(err) => self.push_err(inst_number, err),
                    }
                }
                "SET" => {
                    let key = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    };

                    let value = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    };

                    let key_ref = &key;
                    let value_ref = &value;

		    let fn_mut_closure_t = |t: FdbTransaction| async move {
                        t.set(key_ref.clone(), value_ref.clone());
                        Ok(())
                    };

                    self.execute_mutation(
			fn_mut_closure_t,
                        &tr,
                        is_database,
                        inst_number,
                    )
                    .await;
                }
                "ATOMIC_OP" => {
                    // `OPTYPE` is a string, while `KEY` and `VALUE` are bytes.
                    let op_name = if let NonFutureStackEntryItem::String(s) = self.pop().await.item
                    {
                        s
                    } else {
                        panic!("NonFutureStackEntryItem::String was expected, but not found");
                    };

                    let mutation_type = StackMachine::from_mutation_type_string(op_name);

                    let key = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    };

                    let param = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    };

                    let key_ref = &key;
                    let param_ref = &param;

		    let fn_mut_closure_t = |t: FdbTransaction| async move {
                        unsafe {
                            t.mutate(mutation_type, key_ref.clone(), param_ref.clone());
                        }
                        Ok(())
                    };

                    self.execute_mutation(
			fn_mut_closure_t,
                        &tr,
                        is_database,
                        inst_number,
                    )
                    .await;
                }
                // tuples
                //
                // NOTE: Even though `SUB` [1] is mentioned in
                // `tuples` in `api.py`, we deal with it as part of
                // data operations.
                //
                // [1]: https://github.com/apple/foundationdb/blob/6.3.22/bindings/bindingtester/tests/api.py#L153
                "TUPLE_PACK" => {
                    let count = usize::try_from(
                        if let NonFutureStackEntryItem::BigInt(bi) = self.pop().await.item {
                            bi
                        } else {
                            panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
                        },
                    )
                    .unwrap_or_else(|err| {
                        panic!("Error occurred during `usize::try_from`: {:?}", err);
                    });

                    let mut res = Tuple::new();

                    for _ in 0..count {
                        // `add_bigint` code internally uses
                        // `add_i64`, `add_i32`, `add_i16, `add_i8`.
                        match self.pop().await.item {
                            NonFutureStackEntryItem::BigInt(bi) => res.add_bigint(bi),
                            NonFutureStackEntryItem::Bool(b) => res.add_bool(b),
                            NonFutureStackEntryItem::Bytes(b) => res.add_bytes(b),
                            NonFutureStackEntryItem::Float(f) => res.add_f32(f),
                            NonFutureStackEntryItem::Double(d) => res.add_f64(d),
                            NonFutureStackEntryItem::Null => res.add_null(),
                            NonFutureStackEntryItem::String(s) => res.add_string(s),
                            NonFutureStackEntryItem::Tuple(t) => res.add_tuple(t),
                            NonFutureStackEntryItem::Uuid(u) => res.add_uuid(u),
                            NonFutureStackEntryItem::Versionstamp(v) => res.add_versionstamp(v),
                        }
                    }

                    self.store(inst_number, StackEntryItem::Bytes(res.pack()));
                }
                "TUPLE_PACK_WITH_VERSIONSTAMP" => {
                    let prefix = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    };

                    let count = usize::try_from(
                        if let NonFutureStackEntryItem::BigInt(bi) = self.pop().await.item {
                            bi
                        } else {
                            panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
                        },
                    )
                    .unwrap_or_else(|err| {
                        panic!("Error occurred during `usize::try_from`: {:?}", err)
                    });

                    let mut res = Tuple::new();

                    for _ in 0..count {
                        // `add_bigint` code internally uses
                        // `add_i64`, `add_i32`, `add_i16, `add_i8`.
                        match self.pop().await.item {
                            NonFutureStackEntryItem::BigInt(bi) => res.add_bigint(bi),
                            NonFutureStackEntryItem::Bool(b) => res.add_bool(b),
                            NonFutureStackEntryItem::Bytes(b) => res.add_bytes(b),
                            NonFutureStackEntryItem::Float(f) => res.add_f32(f),
                            NonFutureStackEntryItem::Double(d) => res.add_f64(d),
                            NonFutureStackEntryItem::Null => res.add_null(),
                            NonFutureStackEntryItem::String(s) => res.add_string(s),
                            NonFutureStackEntryItem::Tuple(t) => res.add_tuple(t),
                            NonFutureStackEntryItem::Uuid(u) => res.add_uuid(u),
                            NonFutureStackEntryItem::Versionstamp(v) => res.add_versionstamp(v),
                        }
                    }

                    match res.pack_with_versionstamp(prefix) {
                        Ok(packed) => {
                            self.store(
                                inst_number,
                                StackEntryItem::Bytes(Bytes::from_static(b"OK")),
                            );

                            self.store(inst_number, StackEntryItem::Bytes(packed));
                        }
                        Err(err) => match err.code() {
                            TUPLE_PACK_WITH_VERSIONSTAMP_NOT_FOUND => self.store(
                                inst_number,
                                StackEntryItem::Bytes(Bytes::from_static(b"ERROR: NONE")),
                            ),
                            TUPLE_PACK_WITH_VERSIONSTAMP_MULTIPLE_FOUND => self.store(
                                inst_number,
                                StackEntryItem::Bytes(Bytes::from_static(b"ERROR: MULTIPLE")),
                            ),
                            _ => panic!("Received invalid FdbError code: {:?}", err.code()),
                        },
                    }
                }
                "TUPLE_UNPACK" => {
                    let packed_tuple = Tuple::from_bytes(
                        if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
                            b
                        } else {
                            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                        },
                    )
                    .unwrap_or_else(|err| {
                        panic!("Error occurred during `Types::from_bytes`: {:?}", err);
                    });

                    for ti in 0..packed_tuple.size() {
                        let mut res = Tuple::new();

                        let _ = packed_tuple
                            .get_bigint(ti)
                            .map(|bi| res.add_bigint(bi))
                            .or_else(|_| packed_tuple.get_bool(ti).map(|b| res.add_bool(b)))
                            .or_else(|_| {
                                packed_tuple
                                    .get_bytes_ref(ti)
                                    .map(|b| res.add_bytes(b.clone()))
                            })
                            .or_else(|_| packed_tuple.get_f32(ti).map(|f| res.add_f32(f)))
                            .or_else(|_| packed_tuple.get_f64(ti).map(|d| res.add_f64(d)))
                            .or_else(|_| packed_tuple.get_null(ti).map(|_| res.add_null()))
                            .or_else(|_| {
                                packed_tuple
                                    .get_string_ref(ti)
                                    .map(|s| res.add_string(s.clone()))
                            })
                            .or_else(|_| {
                                packed_tuple
                                    .get_tuple_ref(ti)
                                    .map(|tup_ref| res.add_tuple(tup_ref.clone()))
                            })
                            .or_else(|_| {
                                packed_tuple
                                    .get_uuid_ref(ti)
                                    .map(|u| res.add_uuid(*u))
                            })
                            .or_else(|_| {
                                packed_tuple
                                    .get_versionstamp_ref(ti)
                                    .map(|vs| res.add_versionstamp(vs.clone()))
                            })
                            .unwrap_or_else(|_| {
                                panic!("Unable to unpack packed_tuple: {:?}", packed_tuple);
                            });

                        self.store(inst_number, StackEntryItem::Bytes(res.pack()));
                    }
                }
                "TUPLE_RANGE" => {
                    let count = usize::try_from(
                        if let NonFutureStackEntryItem::BigInt(bi) = self.pop().await.item {
                            bi
                        } else {
                            panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
                        },
                    )
                    .unwrap_or_else(|err| {
                        panic!("Error occurred during `usize::try_from`: {:?}", err);
                    });

                    let mut res_tup = Tuple::new();

                    for _ in 0..count {
                        // `add_bigint` code internally uses
                        // `add_i64`, `add_i32`, `add_i16, `add_i8`.
                        match self.pop().await.item {
                            NonFutureStackEntryItem::BigInt(bi) => res_tup.add_bigint(bi),
                            NonFutureStackEntryItem::Bool(b) => res_tup.add_bool(b),
                            NonFutureStackEntryItem::Bytes(b) => res_tup.add_bytes(b),
                            NonFutureStackEntryItem::Float(f) => res_tup.add_f32(f),
                            NonFutureStackEntryItem::Double(d) => res_tup.add_f64(d),
                            NonFutureStackEntryItem::Null => res_tup.add_null(),
                            NonFutureStackEntryItem::String(s) => res_tup.add_string(s),
                            NonFutureStackEntryItem::Tuple(t) => res_tup.add_tuple(t),
                            NonFutureStackEntryItem::Uuid(u) => res_tup.add_uuid(u),
                            NonFutureStackEntryItem::Versionstamp(v) => res_tup.add_versionstamp(v),
                        }
                    }

                    let (res_range_begin, res_range_end) = res_tup.range(Bytes::new()).into_parts();

                    self.store(
                        inst_number,
                        StackEntryItem::Bytes(res_range_begin.into()),
                    );
                    self.store(
                        inst_number,
                        StackEntryItem::Bytes(res_range_end.into()),
                    );
                }
                "TUPLE_SORT" => {
                    let count = usize::try_from(
                        if let NonFutureStackEntryItem::BigInt(bi) = self.pop().await.item {
                            bi
                        } else {
                            panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
                        },
                    )
                    .unwrap_or_else(|err| {
                        panic!("Error occurred during `usize::try_from`: {:?}", err);
                    });

                    let mut unsorted_tuples = Vec::new();

                    for _ in 0..count {
                        unsorted_tuples.push(
                            Tuple::from_bytes(
                                if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
                                    b
                                } else {
                                    panic!(
                                    "NonFutureStackEntryItem::Bytes was expected, but not found"
                                );
                                },
                            )
                            .unwrap_or_else(|err| {
                                panic!("Error occurred during `Tuple::from_bytes`: {:?}", err)
                            }),
                        );
                    }

                    unsorted_tuples
                        .into_iter()
                        .sorted()
                        .for_each(|tup| self.store(inst_number, StackEntryItem::Bytes(tup.pack())));
                }
                "ENCODE_FLOAT" => {
                    let val_bytes = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item
                    {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    };

                    let val = (&val_bytes[..]).get_f32();

                    self.store(inst_number, StackEntryItem::Float(val));
                }
                "ENCODE_DOUBLE" => {
                    let val_bytes = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item
                    {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    };

                    let val = (&val_bytes[..]).get_f64();

                    self.store(inst_number, StackEntryItem::Double(val));
                }
                "DECODE_FLOAT" => {
                    let val = if let NonFutureStackEntryItem::Float(f) = self.pop().await.item {
                        f
                    } else {
                        panic!("NonFutureStackEntryItem::Float was expected, but not found");
                    };

                    let val_bytes = {
                        let mut b = BytesMut::new();
                        b.put(&val.to_be_bytes()[..]);
                        b.into()
                    };

                    self.store(inst_number, StackEntryItem::Bytes(val_bytes));
                }
                "DECODE_DOUBLE" => {
                    let val = if let NonFutureStackEntryItem::Double(d) = self.pop().await.item {
                        d
                    } else {
                        panic!("NonFutureStackEntryItem::Double was expected, but not found");
                    };

                    let val_bytes = {
                        let mut b = BytesMut::new();
                        b.put(&val.to_be_bytes()[..]);
                        b.into()
                    };

                    self.store(inst_number, StackEntryItem::Bytes(val_bytes));
                }
                // versions, snapshot_versions
                //
                // `GET_READ_VERSION` and `GET_READ_VERSION_SNAPSHOT`
                // is take care of above.
                "SET_READ_VERSION" => unsafe {
		    self.current_transaction()
                        .set_read_version(self.last_version)
                },
                "GET_COMMITTED_VERSION" => {
                    self.last_version = Into::<FdbResult<i64>>::into(unsafe {
                        self.current_transaction().get_committed_version()
                    })
                    .unwrap_or_else(|err| {
                        panic!("Error occurred during `get_committed_version`: {:?}", err)
                    });
                    self.store(
                        inst_number,
                        StackEntryItem::Bytes(Bytes::from_static(b"GOT_COMMITTED_VERSION")),
                    );
                }
                // reads, snapshot_reads, database_reads
                "GET" => {
                    let key = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    };

                    let key_ref = &key;

                    let fn_mut_closure_t = |t: FdbTransaction| async move {
                        t.get(key_ref.clone()).await
                    };

                    let fn_mut_closure_rt = |rt: FdbReadTransaction| async move {
                        rt.get(key_ref.clone()).await
                    };

		    // Don't push future onto the stack (similar to
		    // how "GET" works in python).
		    match if is_database {
			self.execute_read_db(fn_mut_closure_rt).await
		    } else if is_snapshot {
			unsafe { self.execute_read_snap(fn_mut_closure_rt, &tr_snap) }.await
		    } else {
			unsafe { self.execute_read_tr(fn_mut_closure_t, &tr) }.await
		    } {
                        Ok(value) => {
                            let item =
                                StackEntryItem::Bytes(value.map(|v| v.into()).unwrap_or_else(
                                    || Bytes::from_static(b"RESULT_NOT_PRESENT"),
                                ));
                            self.store(inst_number, item);
                            }
                        Err(err) => self.push_err(inst_number, err),
		    }
                }
                "GET_KEY" => {
		    // key, or_equal, offset gets popped here.
                    let sel = self.pop_selector().await;

                    let prefix = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    };

                    let sel_ref = &sel;

                    let fn_mut_closure_t = |t: FdbTransaction| async move {
                        t.get_key(sel_ref.clone()).await
                    };

                    let fn_mut_closure_rt = |rt: FdbReadTransaction| async move {
                        rt.get_key(sel_ref.clone()).await
                    };

		    // Like python, we deal with "GET_KEY" in a
		    // synchronous way.
		    match if is_database {
			self.execute_read_db(fn_mut_closure_rt).await
		    } else if is_snapshot {
			unsafe { self.execute_read_snap(fn_mut_closure_rt, &tr_snap) }.await
		    } else {
			unsafe { self.execute_read_tr(fn_mut_closure_t, &tr) }.await
		    } {
                        Ok(key) => {
                            if key_util::starts_with(key.clone(), prefix.clone()) {
                                self.store(inst_number, StackEntryItem::Bytes(key.into()));
                            } else if Bytes::from(key).cmp(&prefix) == Ordering::Less {
                                self.store(inst_number, StackEntryItem::Bytes(prefix));
                            } else {
                                self.store(
                                    inst_number,
                                    StackEntryItem::Bytes(
                                        key_util::strinc(prefix)
                                            .unwrap_or_else(|err| {
                                                panic!(
                                                "Error occurred during `key_util::strinc`: {:?}",
                                                err
                                            )
                                            })
                                            .into(),
                                    ),
                                );
                            }
                        }
                        Err(err) => self.push_err(inst_number, err),
                    }
                }
		"GET_RANGE" => {
		    // begin, end gets popped here.
		    let key_range = self.pop_key_range().await;

		    // limit, reverse, mode gets popped here.
		    let range_options = self.pop_range_options().await;

		    let key_range_ref = &key_range;
                    let range_options_ref = &range_options;

                    let fn_mut_closure_t = |t: FdbTransaction| async move {
                        let mut range_stream = key_range_ref.clone().into_stream(&t, range_options_ref.clone());

                        let mut res = Vec::new();

                        while let Some(x) = range_stream.next().await {
                            let kv = x?;
                            res.push(kv);
                        }

                        Result::<Vec<KeyValue>, FdbError>::Ok(res)
                    };

                    let fn_mut_closure_rt = |rt: FdbReadTransaction| async move {
                        let mut range_stream = key_range_ref.clone().into_stream(&rt,
                            range_options_ref.clone()
                        );

                        let mut res = Vec::new();

                        while let Some(x) = range_stream.next().await {
                            let kv = x?;
                            res.push(kv);
                        }

                        Result::<Vec<KeyValue>, FdbError>::Ok(res)
                    };

		    match if is_database {
			self.execute_read_db(fn_mut_closure_rt).await
		    } else if is_snapshot {
			unsafe { self.execute_read_snap(fn_mut_closure_rt, &tr_snap) }.await
		    } else {
			unsafe { self.execute_read_tr(fn_mut_closure_t, &tr) }.await
		    } {
			Ok(kvs) => self.push_range(inst_number, kvs, None),
			Err(err) => self.push_err(inst_number, err),
		    }
		}
		"GET_RANGE_STARTS_WITH" => {
		    let prefix = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
			b
		    } else {
			panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
		    };

		    // limit, reverse, mode gets popped here.
		    let range_options = self.pop_range_options().await;

		    let key_range = Range::starts_with(prefix);

                    let range_options_ref = &range_options;
		    let key_range_ref = &key_range;

                    let fn_mut_closure_t = |t: FdbTransaction| async move {
                        let mut range_stream = key_range_ref.clone().into_stream(&t, range_options_ref.clone());

                        let mut res = Vec::new();

                        while let Some(x) = range_stream.next().await {
                            let kv = x?;
                            res.push(kv);
                        }

                        Result::<Vec<KeyValue>, FdbError>::Ok(res)
                    };

                    let fn_mut_closure_rt = |rt: FdbReadTransaction| async move {
                        let mut range_stream = key_range_ref.clone().into_stream(&rt, range_options_ref.clone());

                        let mut res = Vec::new();

                        while let Some(x) = range_stream.next().await {
                            let kv = x?;
                            res.push(kv);
                        }

                        Result::<Vec<KeyValue>, FdbError>::Ok(res)
                    };

		    match if is_database {
			self.execute_read_db(fn_mut_closure_rt).await
		    } else if is_snapshot {
			unsafe { self.execute_read_snap(fn_mut_closure_rt, &tr_snap) }.await
		    } else {
			unsafe { self.execute_read_tr(fn_mut_closure_t, &tr) }.await
		    } {
			Ok(kvs) => self.push_range(inst_number, kvs, None),
			Err(err) => self.push_err(inst_number, err),
		    }
		}
		"GET_RANGE_SELECTOR" => {
		    // begin_key, begin_or_equal, begin_offset popped here.
		    let begin_key_selector = self.pop_selector().await;

		    // end_key, end_or_equal, end_offset popped here.
		    let end_key_selector = self.pop_selector().await;

		    // limit, reverse, mode gets popped here.
		    let range_options = self.pop_range_options().await;

                    let prefix = Some(
                        if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
                                b
                        } else {
                            panic!(
                                "NonFutureStackEntryItem::Bytes was expected, but not found"
                            );
                        },
                    );

                    let begin_key_selector_ref = &begin_key_selector;
                    let end_key_selector_ref = &end_key_selector;
                    let range_options_ref = &range_options;

                    let fn_mut_closure_t = |t: FdbTransaction| async move {
                        let mut range_stream = t.get_range(
                            begin_key_selector_ref.clone(),
                            end_key_selector_ref.clone(),
                            range_options_ref.clone(),
                        );

                        let mut res = Vec::new();

                        while let Some(x) = range_stream.next().await {
                            let kv = x?;
                            res.push(kv);
                        }

                        Result::<Vec<KeyValue>, FdbError>::Ok(res)
                    };

                    let fn_mut_closure_rt = |rt: FdbReadTransaction| async move {
                        let mut range_stream = rt.get_range(
                            begin_key_selector_ref.clone(),
                            end_key_selector_ref.clone(),
                            range_options_ref.clone(),
                        );

                        let mut res = Vec::new();

                        while let Some(x) = range_stream.next().await {
                            let kv = x?;
                            res.push(kv);
                        }

                        Result::<Vec<KeyValue>, FdbError>::Ok(res)
                    };

		    match if is_database {
			self.execute_read_db(fn_mut_closure_rt).await
		    } else if is_snapshot {
			unsafe { self.execute_read_snap(fn_mut_closure_rt, &tr_snap) }.await
		    } else {
			unsafe { self.execute_read_tr(fn_mut_closure_t, &tr) }.await
		    } {
			Ok(kvs) => self.push_range(inst_number, kvs, prefix),
			Err(err) => self.push_err(inst_number, err),
		    }
		}
                // mutations, database_mutations
                //
                // `SET` and `ATOMIC_OP` is taken care of above. Even
                // though mutations has `VERSIONSTAMP`, there is no
                // instruction with that name.
                "CLEAR" => {
                    let key = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    };

                    let key_ref = &key;

		    let fn_mut_closure_t = |t: FdbTransaction| async move {
			t.clear(key_ref.clone());
                        Ok(())
                    };

                    self.execute_mutation(
			fn_mut_closure_t,
                        &tr,
                        is_database,
                        inst_number,
                    )
                    .await;
                }
		"CLEAR_RANGE" => {
		    // begin, end gets popped here.
		    let key_range = self.pop_key_range().await;

		    let key_range_ref = &key_range;

		    let fn_mut_closure_t = |t: FdbTransaction| async move {
			t.clear_range(key_range_ref.clone());
                        Ok(())
                    };

                    self.execute_mutation(
			fn_mut_closure_t,
                        &tr,
                        is_database,
                        inst_number,
                    )
                    .await;
		}
		"CLEAR_RANGE_STARTS_WITH" => {
		    let prefix = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
			b
		    } else {
			panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
		    };

		    let key_range = Range::starts_with(prefix);

		    let key_range_ref = &key_range;

		    let fn_mut_closure_t = |t: FdbTransaction| async move {
			t.clear_range(key_range_ref.clone());
                        Ok(())
                    };

                    self.execute_mutation(
			fn_mut_closure_t,
                        &tr,
                        is_database,
                        inst_number,
                    )
                    .await;
		}
                // read_conflicts
                "READ_CONFLICT_RANGE" => {
                    let key_range = self.pop_key_range().await;

                    match self
                        .current_transaction()
                        .add_read_conflict_range(key_range)
                    {
                        Ok(_) => self.store(
                            inst_number,
                            StackEntryItem::Bytes(Bytes::from_static(b"SET_CONFLICT_RANGE")),
                        ),
                        Err(err) => self.push_err(inst_number, err),
                    }
                }
                "READ_CONFLICT_KEY" => {
                    let key = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    };

                    match self.current_transaction().add_read_conflict_key(key) {
                        Ok(_) => self.store(
                            inst_number,
                            StackEntryItem::Bytes(Bytes::from_static(b"SET_CONFLICT_KEY")),
                        ),
                        Err(err) => self.push_err(inst_number, err),
                    }
                }
                // write_conflicts
                "WRITE_CONFLICT_RANGE" => {
                    let key_range = self.pop_key_range().await;

                    match self
                        .current_transaction()
                        .add_write_conflict_range(key_range)
                    {
                        Ok(_) => self.store(
                            inst_number,
                            StackEntryItem::Bytes(Bytes::from_static(b"SET_CONFLICT_RANGE")),
                        ),
                        Err(err) => self.push_err(inst_number, err),
                    }
                }
                "WRITE_CONFLICT_KEY" => {
                    let key = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
                        b
                    } else {
                        panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
                    };

                    match self.current_transaction().add_write_conflict_key(key) {
                        Ok(_) => self.store(
                            inst_number,
                            StackEntryItem::Bytes(Bytes::from_static(b"SET_CONFLICT_KEY")),
                        ),
                        Err(err) => self.push_err(inst_number, err),
                    }
                }
		"DISABLE_WRITE_CONFLICT" => self.current_transaction().set_option(TransactionOption::NextWriteNoWriteConflictRange)
		    .unwrap_or_else(|err| panic!("Error occurred during `set_option(TransactionOption::NextWriteNoWriteConflictRange)`: {:?}", err)),
		// txn_sizes
		"GET_APPROXIMATE_SIZE" => match self
                        .current_transaction()
                        .get_approximate_size().await
                    {
                        Ok(_) => self.store(
                            inst_number,
                            StackEntryItem::Bytes(Bytes::from_static(b"GOT_APPROXIMATE_SIZE"))
                        ),
                        Err(err) => self.push_err(inst_number, err),
                    }
		// storage_metrics
		"GET_ESTIMATED_RANGE_SIZE" => {
		    // begin, end gets popped here.
		    let key_range = self.pop_key_range().await;

		    let key_range_ref = &key_range;

                    let fn_mut_closure_t = |t: FdbTransaction| async move {
                        t.get_estimated_range_size_bytes(key_range_ref.clone()).await
                    };

                    let fn_mut_closure_rt = |rt: FdbReadTransaction| async move {
                        rt.get_estimated_range_size_bytes(key_range_ref.clone()).await
                    };

		    match if is_database {
			self.execute_read_db(fn_mut_closure_rt).await
		    } else if is_snapshot {
			unsafe { self.execute_read_snap(fn_mut_closure_rt, &tr_snap) }.await
		    } else {
			unsafe { self.execute_read_tr(fn_mut_closure_t, &tr) }.await
		    } {
                        Ok(_) => self.store(
                            inst_number,
                            StackEntryItem::Bytes(Bytes::from_static(b"GOT_ESTIMATED_RANGE_SIZE"))
                        ),
                        Err(err) => self.push_err(inst_number, err),
		    }
		}
		// Thread Operations
		"WAIT_EMPTY" => {
		    let prefix_range = self.pop_prefix_range().await;
		    let prefix_range_ref = &prefix_range;

		    match self.db.run(|tr| async move {
			let mut range_stream = prefix_range_ref.clone().into_stream(&tr, RangeOptions::default());

			let mut res = Vec::new();

			while let Some(x) = range_stream.next().await {
			    let kv = x?;
			    res.push(kv);
			}

			if !res.is_empty() {
			    Err(FdbError::new(1020))
			} else {
			    Ok(())
			}
		    }).await {
			Ok(_) => self.store(
                            inst_number,
                            StackEntryItem::Bytes(Bytes::from_static(b"WAITED_FOR_EMPTY")),
                        ),
                        Err(err) => self.push_err(inst_number, err),
		    }
		}
                _ => panic!("Unhandled operation {}", op),
            }
        }

        if self.verbose || verbose_inst_range {
            println!("        to [");
            self.dump_stack();
            println!(" ] ({})\n", self.stack.len());
        }
    }

    async fn test_locality(&self) {
        self.db
            .run(|tr| async move {
                tr.set_option(TransactionOption::Timeout(60 * 1000))?;
                tr.set_option(TransactionOption::ReadSystemKeys)?;

                let boundary_keys = self
                    .db
                    .get_boundary_keys(
                        Bytes::from_static(b""),
                        Bytes::from_static(b"\xFF\xFF"),
                        0,
                        0,
                    )
                    .await?;

                for i in 0..boundary_keys.len() - 1 {
                    let start = boundary_keys[i].clone();
                    let end = tr
                        .get_key(KeySelector::last_less_than(boundary_keys[i + 1].clone()))
                        .await?;

                    let start_addresses = tr.get_addresses_for_key(start).await?;
                    let end_addresses = tr.get_addresses_for_key(end).await?;

                    for a in start_addresses.iter() {
                        if !end_addresses.contains(a) {
                            panic!("Locality not internally consistent.");
                        }
                    }
                }

                Ok(())
            })
            .await
            .unwrap_or_else(|err| panic!("Unit test failed {:?}", err));
    }

    async fn test_watches(&self) {
        loop {
            self.db
                .run(|tr| async move {
                    tr.set(Bytes::from_static(b"w0"), Bytes::from_static(b"0"));
                    tr.set(Bytes::from_static(b"w2"), Bytes::from_static(b"2"));
                    tr.set(Bytes::from_static(b"w3"), Bytes::from_static(b"3"));
                    Ok(())
                })
                .await
                .unwrap_or_else(|err| panic!("Unit test failed {:?}", err));

            // create watches for keys `w0` (`w0_fut`), `w1`
            // (`w1_fut`) (which does not exist), `w2` (`w2_fut`) and
            // `w3` (`w3_fut`).

            let mut watches = self
                .db
                .run(|tr| async move {
                    let watches = vec![
                        tr.watch(Bytes::from_static(b"w0")),
                        tr.watch(Bytes::from_static(b"w1")),
                        tr.watch(Bytes::from_static(b"w2")),
                        tr.watch(Bytes::from_static(b"w3")),
                    ];

                    tr.set(Bytes::from_static(b"w0"), Bytes::from_static(b"0"));
                    tr.clear(Bytes::from_static(b"w1"));

                    Ok(watches)
                })
                .await
                .unwrap_or_else(|err| panic!("Unit test failed {:?}", err));

            // `w0_fut` with `(version, "w0", "0")`, `w1_fut` with
            // `(version, "w1", None)`, `w2_fut` with `(version, "w2",
            // "2")` and `w3_fut` with `(version, "w3", "3")` is set
            // globally.
            //
            // even though keys `w0` and `w1` was mutated in the
            // earlier transaction, the watch won't fire because no
            // change happened to its value.

            sleep(Duration::from_secs(5)).await;

            let cw = self.check_watches(&mut watches, false).await;

            if !cw {
                continue;
            }

            // make sure we still have all the watches `w0`, `w1`,
            // `w2`, `w3` with us.
            assert!(watches.len() == 4);

            self.db
                .run(|tr| async move {
                    // w0_fut: (version, "w0", "0") -> (new_version, "w0", "a")
                    tr.set(Bytes::from_static(b"w0"), Bytes::from_static(b"a"));

                    // w1_fut: (version, "w1", None) -> (new_version, "w1", "b")
                    tr.set(Bytes::from_static(b"w1"), Bytes::from_static(b"b"));

                    // w2_fut: (version, "w2", "2") -> (new_version, "w2", None)
                    tr.clear(Bytes::from_static(b"w2"));

                    // w3_fut: (version, "w3", "3") -> (new_version,
                    // "w3", BitXor "3" "\xFF\xFF")
                    unsafe {
                        tr.mutate(
                            MutationType::BitXor,
                            Bytes::from_static(b"w3"),
                            Bytes::from_static(b"\xFF\xFF"),
                        );
                    }

                    Ok(())
                })
                .await
                .unwrap_or_else(|err| panic!("Unit test failed {:?}", err));

            let cw = self.check_watches(&mut watches, true).await;

            // make sure we all the watches have been resolved.
            // assert!(watches.len() == 0);

            if cw {
                return;
            }
        }
    }

    async fn check_watches(&self, watches: &mut Vec<FdbFutureUnit>, expected: bool) -> bool {
        for watch_fut in watches {
            // watch_fut: &mut FdbFuture<()>;
            if unsafe { watch_fut.is_ready() } || expected {
                match watch_fut.await {
                    Ok(_) => {
                        if !expected {
                            panic!("Watch triggered too early");
                        }
                    }
                    Err(_) => return false,
                }
            }
        }

        true
    }

    async fn test_tr_options(&self) {
        self.db
            .run(|tr| async move {
                tr.set_option(TransactionOption::PrioritySystemImmediate)?;
                tr.set_option(TransactionOption::PriorityBatch)?;
                tr.set_option(TransactionOption::CausalReadRisky)?;
                tr.set_option(TransactionOption::CausalWriteRisky)?;
                tr.set_option(TransactionOption::ReadYourWritesDisable)?;
                tr.set_option(TransactionOption::ReadSystemKeys)?;
                tr.set_option(TransactionOption::AccessSystemKeys)?;
                tr.set_option(TransactionOption::TransactionLoggingMaxFieldLength(1000))?;
                tr.set_option(TransactionOption::Timeout(60 * 1000))?;
                tr.set_option(TransactionOption::RetryLimit(50))?;
                tr.set_option(TransactionOption::MaxRetryDelay(100))?;
                tr.set_option(TransactionOption::UsedDuringCommitProtectionDisable)?;
                tr.set_option(TransactionOption::DebugTransactionIdentifier(
                    "my_transaction".to_string(),
                ))?;
                tr.set_option(TransactionOption::LogTransaction)?;
                tr.set_option(TransactionOption::ReadLockAware)?;
                tr.set_option(TransactionOption::LockAware)?;
                tr.set_option(TransactionOption::IncludePortInAddress)?;

                tr.get(Bytes::from_static(b"\xFF")).await?;

                Ok(())
            })
            .await
            .unwrap_or_else(|err| panic!("Unit test failed {:?}", err));
    }

    fn test_db_options(&self) {
        self.db
            .set_option(DatabaseOption::LocationCacheSize(100001))
            .and_then(|_| self.db.set_option(DatabaseOption::MaxWatches(10001)))
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::DatacenterId("dc_id".to_string()))
            })
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::MachineId("machine_id".to_string()))
            })
            .and_then(|_| self.db.set_option(DatabaseOption::SnapshotRywEnable))
            .and_then(|_| self.db.set_option(DatabaseOption::SnapshotRywDisable))
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::TransactionLoggingMaxFieldLength(1000))
            })
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::TransactionTimeout(100000))
            })
            .and_then(|_| self.db.set_option(DatabaseOption::TransactionTimeout(0)))
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::TransactionMaxRetryDelay(100))
            })
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::TransactionSizeLimit(100000))
            })
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::TransactionRetryLimit(10))
            })
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::TransactionRetryLimit(-1))
            })
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::TransactionCausalReadRisky)
            })
            .and_then(|_| {
                self.db
                    .set_option(DatabaseOption::TransactionIncludePortInAddress)
            })
            .unwrap_or_else(|err| panic!("Unit test failed {:?}", err));
    }

    async fn pop_range_options(&mut self) -> RangeOptions {
        let limit = i32::try_from(
            if let NonFutureStackEntryItem::BigInt(bi) = self.pop().await.item {
                bi
            } else {
                panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
            },
        )
        .unwrap_or_else(|err| panic!("Error occurred during `i32::try_from`: {:?}", err));

        // Even though `reverse` is a bool, in the stack it is stored as a number.
        let reverse = StackMachine::bigint_to_bool(
            if let NonFutureStackEntryItem::BigInt(bi) = self.pop().await.item {
                bi
            } else {
                panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
            },
        );

        let mode = StackMachine::from_streaming_mode_code(
            i32::try_from(
                if let NonFutureStackEntryItem::BigInt(bi) = self.pop().await.item {
                    bi
                } else {
                    panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
                },
            )
            .unwrap_or_else(|err| panic!("Error occurred during `i32::try_from`: {:?}", err)),
        );

        let mut res = RangeOptions::default();

        res.set_limit(limit);
        res.set_reverse(reverse);
        res.set_mode(mode);

        res
    }

    async fn pop_key_range(&mut self) -> Range {
        let begin_key = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
            b
        } else {
            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
        };

        let end_key = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
            b
        } else {
            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
        };

        Range::new(begin_key, end_key)
    }

    async fn pop_prefix_range(&mut self) -> Range {
        let prefix_key = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
            b
        } else {
            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
        };

        Range::starts_with(prefix_key)
    }

    async fn pop_selector(&mut self) -> KeySelector {
        let key = if let NonFutureStackEntryItem::Bytes(b) = self.pop().await.item {
            b
        } else {
            panic!("NonFutureStackEntryItem::Bytes was expected, but not found");
        };

        // Even though `or_equal` is a bool, in the stack it is stored as a number.
        let or_equal = StackMachine::bigint_to_bool(
            if let NonFutureStackEntryItem::BigInt(bi) = self.pop().await.item {
                bi
            } else {
                panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
            },
        );

        let offset = i32::try_from(
            if let NonFutureStackEntryItem::BigInt(bi) = self.pop().await.item {
                bi
            } else {
                panic!("NonFutureStackEntryItem::BigInt was expected, but not found");
            },
        )
        .unwrap_or_else(|err| panic!("Error occurred during `i32::try_from`: {:?}", err));

        KeySelector::new(key, or_equal, offset)
    }

    // Methods `execute_read_db`, `execute_read_snap`,
    // `execute_read_tr` simulates reads on `obj` in python binding
    // tester `tester.py`'s `run` method.

    async fn execute_read_db<T, F, Fut>(&mut self, f: F) -> FdbResult<T>
    where
        F: FnMut(FdbReadTransaction) -> Fut,
        Fut: Future<Output = FdbResult<T>>,
    {
        self.db.read(f).await
    }

    async unsafe fn execute_read_snap<T, F, Fut>(
        &mut self,
        f: F,
        tr_snap: &FdbReadTransaction,
    ) -> FdbResult<T>
    where
        F: FnMut(FdbReadTransaction) -> Fut,
        Fut: Future<Output = FdbResult<T>>,
    {
        tr_snap.read(f).await
    }

    async unsafe fn execute_read_tr<T, F, Fut>(&mut self, f: F, tr: &FdbTransaction) -> FdbResult<T>
    where
        F: FnMut(FdbTransaction) -> Fut,
        Fut: Future<Output = FdbResult<T>>,
    {
        tr.read(f).await
    }

    async fn execute_mutation<T, F, Fut>(
        &mut self,
        f: F,
        tr: &FdbTransaction,
        is_database: bool,
        inst_number: usize,
    ) where
        F: FnMut(FdbTransaction) -> Fut,
        Fut: Future<Output = FdbResult<T>>,
    {
        if is_database {
            match self.db.run(f).await {
                Ok(_) => {
                    // We do this to simulate "_DATABASE may
                    // optionally push a future onto the stack".
                    self.store(
                        inst_number,
                        StackEntryItem::Bytes(Bytes::from_static(b"RESULT_NOT_PRESENT")),
                    );
                }
                Err(err) => self.push_err(inst_number, err),
            }
        } else if let Err(err) = unsafe { tr.run(f).await } {
            self.push_err(inst_number, err);
        }
    }

    // Similar to python `push_range`.
    fn push_range(&mut self, inst_number: usize, kvs: Vec<KeyValue>, prefix_filter: Option<Bytes>) {
        let mut tup = Tuple::new();

        for kv in kvs {
            let (key, value) = kv.into_parts();
            match prefix_filter {
                Some(ref p) => {
                    if key_util::starts_with(key.clone(), p.clone()) {
                        tup.add_bytes(key.into());
                        tup.add_bytes(value.into());
                    }
                }
                None => {
                    tup.add_bytes(key.into());
                    tup.add_bytes(value.into());
                }
            }
        }

        self.store(inst_number, StackEntryItem::Bytes(tup.pack()));
    }

    // In Go bindings this is handled by `defer`.
    fn push_err(&mut self, inst_number: usize, err: FdbError) {
        let item = StackEntryItem::Bytes({
            let mut tup = Tuple::new();
            tup.add_bytes(Bytes::from_static(b"ERROR"));
            tup.add_bytes(Bytes::from(format!("{}", err.code())));
            tup.pack()
        });

        self.store(inst_number, item);
    }

    async fn pop(&mut self) -> NonFutureStackEntry {
        let StackEntry { item, inst_number } = self.stack.pop().unwrap();

        match item {
            StackEntryItem::FdbFutureKey(fdb_future_key) => fdb_future_key
                .await
                .map(|x| {
                    let item = NonFutureStackEntryItem::Bytes(x.into());
                    NonFutureStackEntry { item, inst_number }
                })
                .unwrap_or_else(|err| {
                    let item = NonFutureStackEntryItem::Bytes({
                        let mut tup = Tuple::new();
                        tup.add_bytes(Bytes::from_static(b"ERROR"));
                        tup.add_bytes(Bytes::from(format!("{}", err.code())));
                        tup.pack()
                    });
                    NonFutureStackEntry { item, inst_number }
                }),
            StackEntryItem::FdbFutureUnit(fdb_future_unit) => fdb_future_unit
                .await
                .map(|_| {
                    let item =
                        NonFutureStackEntryItem::Bytes(Bytes::from_static(b"RESULT_NOT_PRESENT"));
                    NonFutureStackEntry { item, inst_number }
                })
                .unwrap_or_else(|err| {
                    let item = NonFutureStackEntryItem::Bytes({
                        let mut tup = Tuple::new();
                        tup.add_bytes(Bytes::from_static(b"ERROR"));
                        tup.add_bytes(Bytes::from(format!("{}", err.code())));
                        tup.pack()
                    });
                    NonFutureStackEntry { item, inst_number }
                }),
            StackEntryItem::BigInt(b) => {
                let item = NonFutureStackEntryItem::BigInt(b);
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::Bool(b) => {
                let item = NonFutureStackEntryItem::Bool(b);
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::Bytes(b) => {
                let item = NonFutureStackEntryItem::Bytes(b);
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::Float(f) => {
                let item = NonFutureStackEntryItem::Float(f);
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::Double(f) => {
                let item = NonFutureStackEntryItem::Double(f);
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::Null => {
                let item = NonFutureStackEntryItem::Null;
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::String(s) => {
                let item = NonFutureStackEntryItem::String(s);
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::Tuple(t) => {
                let item = NonFutureStackEntryItem::Tuple(t);
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::Uuid(u) => {
                let item = NonFutureStackEntryItem::Uuid(u);
                NonFutureStackEntry { item, inst_number }
            }
            StackEntryItem::Versionstamp(v) => {
                let item = NonFutureStackEntryItem::Versionstamp(v);
                NonFutureStackEntry { item, inst_number }
            }
        }
    }

    // // For transaction tracing
    // fn new_transaction_with_tracing(&mut self, inst_number: usize) {
    //     let new_fdb_transaction = self
    //         .db
    //         .create_transaction()
    //         .unwrap_or_else(|err| panic!("Error occurred during `create_transaction`: {:?}", err));

    //     new_fdb_transaction
    //         .set_option(TransactionOption::DebugTransactionIdentifier(
    //             inst_number.to_string(),
    //         ))
    //         .and_then(|_| {
    //             new_fdb_transaction
    //                 .set_option(TransactionOption::TransactionLoggingMaxFieldLength(-1))
    //         })
    //         .and_then(|_| new_fdb_transaction.set_option(TransactionOption::LogTransaction))
    //         .unwrap_or_else(|err| panic!("Error occurred during `set_option`: {:?}", err));

    //     if let Some(old_fdb_transaction) = self
    //         .tr_map
    //         .insert(self.tr_name.clone(), new_fdb_transaction)
    //     {
    //         // Get rid of any old `FdbTransaction` that we might
    //         // have. (We don't have to do this, but we are doing
    //         // it anyway).
    //         //
    //         // In Go, the garbage collector will take care of
    //         // this.
    //         drop(old_fdb_transaction);
    //     }
    // }

    fn new_transaction(&mut self) {
        let new_fdb_transaction = self
            .db
            .create_transaction()
            .unwrap_or_else(|err| panic!("Error occurred during `create_transaction`: {:?}", err));

        if let Some(old_fdb_transaction) = self
            .tr_map
            .insert(self.tr_name.clone(), new_fdb_transaction)
        {
            // Get rid of any old `FdbTransaction` that we might
            // have. (We don't have to do this, but we are doing
            // it anyway).
            //
            // In Go, the garbage collector will take care of
            // this.
            drop(old_fdb_transaction);
        }
    }

    fn current_transaction(&self) -> FdbTransaction {
        // Here we assume that we have a valid `*mut FdbTransaction`
        // that was previously created using `new_transaction` or
        // `switch_transaction`.
        (self.tr_map.get(&self.tr_name).unwrap()).value().clone()
    }

    fn switch_transaction(&mut self, tr_name: Bytes) {
        self.tr_map.entry(tr_name.clone()).or_insert_with(|| {
            self.db.create_transaction().unwrap_or_else(|err| {
                panic!("Error occurred during `create_transaction`: {:?}", err)
            })
        });

        // If the previous `tr_name` has a valid `FdbTransaction` in
        // `TrMap` and is not dropped by any other thread, it will
        // eventually be garbage collected in the main thread.
        self.tr_name = tr_name;
    }

    async fn log_stack(&self, entries: HashMap<usize, NonFutureStackEntry>, log_prefix: Bytes) {
        let entries_ref = &entries;
        let log_prefix_ref = &log_prefix;
        self.db
            .run(|tr| async move {
                for (stack_index, stack_entry) in entries_ref.clone().drain() {
                    let packed_key = {
                        let mut tup = Tuple::new();

                        // We can't use `tup.add_i64` because `usize`
                        // will overflow it. Instead use `BigInt` and
                        // let the Tuple layer take care of properly
                        // encoding it.
                        tup.add_bigint(stack_index.into());
                        tup.add_bigint(stack_entry.inst_number.into());

                        Subspace::new(log_prefix_ref.clone()).subspace(&tup).pack()
                    };

                    let packed_value = {
                        let mut tup = Tuple::new();

                        match stack_entry.item {
                            NonFutureStackEntryItem::BigInt(b) => tup.add_bigint(b),
                            NonFutureStackEntryItem::Bool(b) => tup.add_bool(b),
                            NonFutureStackEntryItem::Bytes(b) => tup.add_bytes(b),
                            NonFutureStackEntryItem::Float(f) => tup.add_f32(f),
                            NonFutureStackEntryItem::Double(f) => tup.add_f64(f),
                            NonFutureStackEntryItem::Null => tup.add_null(),
                            NonFutureStackEntryItem::String(s) => tup.add_string(s),
                            NonFutureStackEntryItem::Tuple(tu) => tup.add_tuple(tu),
                            NonFutureStackEntryItem::Uuid(u) => tup.add_uuid(u),
                            NonFutureStackEntryItem::Versionstamp(vs) => tup.add_versionstamp(vs),
                        }

                        let mut packed_tup = tup.pack();

                        let max_value_length = 40000;

                        if packed_tup.len() >= max_value_length {
                            packed_tup = packed_tup.slice(0..max_value_length);
                        }

                        packed_tup
                    };

                    tr.set(packed_key, packed_value);
                }

                Ok(())
            })
            .await
            .unwrap_or_else(|err| panic!("Error occurred during `.await`: {:?}", err));
    }

    fn store(&mut self, inst_number: usize, item: StackEntryItem) {
        self.stack.push(StackEntry { item, inst_number });
    }

    fn dump_stack(&self) {
        let stack_len = self.stack.len();

        if stack_len == 0 {
            return;
        }

        let mut i = stack_len - 1;
        loop {
            print!(" {}. {:?}", self.stack[i].inst_number, self.stack[i].item);
            if i == 0 {
                return;
            } else {
                println!(",");
            }
            i -= 1;
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<String>>();

    let prefix = Bytes::from(args[1].clone());
    let api_version = args[2].parse::<i32>()?;
    let cluster_file_path = if args.len() > 3 { args[3].as_str() } else { "" };

    unsafe {
        fdb::select_api_version(api_version);

        // // Enable tracing
        // fdb::set_network_option(fdb::NetworkOption::TraceEnable("trace-rs".to_string()))?;
        // fdb::set_network_option(fdb::NetworkOption::TraceFormat("json".to_string()))?;

        fdb::start_network();
    }

    let fdb_database = fdb::open_database(cluster_file_path)?;

    let rt = Runtime::new()?;

    let tr_map = Arc::new(DashMap::<Bytes, FdbTransaction>::new());

    let fdb_database_clone = fdb_database.clone();
    let tr_map_clone = tr_map.clone();

    rt.block_on(async move {
        let (task_finished, mut task_finished_recv) = mpsc::channel::<()>(1);

        let (start_thread_task_send, mut start_thread_task_recv) =
            mpsc::unbounded_channel::<StartThreadTaskMessage>();

        // We need to do this round about message passing dance
        // because trying to call
        // `tokio::spawn(StackMachine::exec(...)` for `START_THREAD`
        // instruction will result in a async recursion.
        let start_thread_task_join_handle = tokio::spawn(async move {
            while let Some(start_thread_task_message) = start_thread_task_recv.recv().await {
                let StartThreadTaskMessage::Exec {
                    prefix,
                    db,
                    tr_map,
                    task_finished,
                    start_thread_task_send,
                } = start_thread_task_message;

                tokio::spawn(StackMachine::exec(
                    prefix,
                    db,
                    tr_map,
                    task_finished,
                    start_thread_task_send,
                ));
            }
        });

        tokio::spawn(StackMachine::exec(
            prefix,
            fdb_database_clone,
            tr_map_clone,
            task_finished,
            start_thread_task_send,
        ));

        let _ = task_finished_recv.recv().await;

        // Ensure that start thread task successfully exits. This
        // happens when there are no more active threads.
        let _ = start_thread_task_join_handle.await?;

        Result::<(), Box<dyn Error>>::Ok(())
    })?;

    // Get rid of any old `FdbTransaction` that we might have. In Go,
    // the garbage collector will take care of this. Even though doing
    // `drop(tr_map)` would have sufficed here, we want to verify that
    // by the time we get here, there is only *one* remaining strong
    // count on `tr_map`.
    //
    // Safety: By this time, we should have only one strong reference
    // to `tr_map`. So, the following `Arc::try_unwrap` should not
    // fail.
    Arc::try_unwrap(tr_map)
        .unwrap_or_else(|err| panic!("Error occurred during `Arc::try_unwrap()`: {:?}", err))
        .into_iter()
        .for_each(|(_, v)| {
            drop(v);
        });

    drop(fdb_database);

    unsafe {
        fdb::stop_network();
    }

    Ok(())
}
