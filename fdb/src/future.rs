//! Provides [`FdbFuture`], [`FdbStreamKeyValue`] types and
//! [`FdbFutureGet`] trait for working with FDB Future.

use bytes::Bytes;

use futures::task::AtomicWaker;
use futures::Stream;

use std::convert::TryInto;
use std::ffi;
use std::ffi::{CStr, CString};
use std::future::Future;
use std::marker::PhantomData;
use std::marker::Unpin;
use std::pin::Pin;
use std::ptr::{self, NonNull};
use std::slice;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::error::{check, FdbResult};
use crate::range::{
    fdb_transaction_get_range, KeyValueArray, RangeOptions, RangeResultStateMachine, StreamingMode,
};
use crate::transaction::FdbTransaction;
use crate::{Key, KeySelector, KeyValue, Value};

/// A [`FdbFuture`] represents a value (or error) to be available at
/// some other time.
///
/// Asynchronous FDB APIs return an [`FdbFuture`].
//
// When a value of `FdbFuture<T>` is created, both `c_ptr` and `waker`
// will be `Some(...)`.
//
// `c_ptr` will be set to `None`, in `Drop::drop`, just before Rust
// destroys the value.
//
// `callback_set` is used to track if callback has been set. When
// `FdbFuture` value is created, this is set to `false`. Once we set
// the callback (by calling `fdb_future_set_callback`) in `poll`, we
// set this to `true`, and it remains `true` for the remaining
// lifetime of `FdbFuture`. Even though there can be multiple calls to
// `poll` and `Waker` can get updated, callback is set *only* once.
//
// `waker` will be set of `None` just before the `poll` returns
// `Poll::Ready(...)`. This is used to check if the future gets polled
// after it has returned `Poll::Ready(...)`.
//
// When `poll` returns `Poll::Pending`, it sets up a
// callback. FoundationDB C API allows us to specify
// `callback_parameter`. The `callback_parameter` is a raw
// `Arc<AtomicWaker>`. `AtomicWaker` internally stores a `Waker`. This
// `Waker` is used to wake the executor when the future becomes ready.
//
// In FoundationDB callback will be executed with "at most once"
// semantics. This means that callback will be executed either when
// FDB future resolves *or* when the FDB future is destroyed (which we
// do in `Drop::drop`).
//
// If Rust `FdbFuture<T>` gets dropped before FDB future is resolved,
// we will get a callback that does a wake on the `Waker` that is
// stored in `AtomicWaker`. This is okay, because `Waker` is an
// `Arc<Task>` like value, and hence it will keep the task alive, even
// though the `.wake()` might not call `poll`, as `FdbFuture<T>`
// would be long gone by then.
#[derive(Debug)]
pub struct FdbFuture<T> {
    c_ptr: Option<NonNull<fdb_sys::FDBFuture>>,
    callback_set: bool,
    waker: Option<Arc<AtomicWaker>>,
    _marker: PhantomData<T>,
}

impl<T> FdbFuture<T> {
    /// Returns [`true`] if the FDB future is ready, [`false`]
    /// otherwise, without blocking. A FDB future is ready either when
    /// it has received a value or has been set to an error state.
    ///
    /// # Safety
    ///
    /// You should not use this API. It exists to support binding
    /// tester.
    pub unsafe fn is_ready(&self) -> bool {
        // Safety: Only time `c_ptr` will be `None` is after
        // `Drop::drop` has been called. Otherwise, it will have a
        // `Some(...)` value. So, it is safe to unwrap here.
        let fut_c_ptr = self.c_ptr.as_ref().unwrap().as_ptr();

        // non-zero is `true`.
        fdb_sys::fdb_future_is_ready(fut_c_ptr) != 0
    }

    pub(crate) fn new(c_ptr: *mut fdb_sys::FDBFuture) -> FdbFuture<T> {
        FdbFuture {
            c_ptr: Some(NonNull::new(c_ptr).expect("c_ptr cannot be null")),
            callback_set: false,
            waker: Some(Arc::new(AtomicWaker::new())),
            _marker: PhantomData,
        }
    }
}

// # Safety
//
// `FdbFuture` does not implement `Copy` or `Clone` traits. Also
// inside `FdbFuture` we don't do anything that would prevent it from
// being sent to a different thread. All pointers are behind an `Arc`.
//
// The main reason for adding `Send` and `Sync` traits is so that it
// can be used inside `tokio::spawn`.
unsafe impl<T> Send for FdbFuture<T> {}
unsafe impl<T> Sync for FdbFuture<T> {}

// `FDBCallback` is used in C API documentation, so name it that way.
#[allow(non_snake_case)]
extern "C" fn FDBCallback(_f: *mut fdb_sys::FDBFuture, callback_parameter: *mut ffi::c_void) {
    let arc_atomic_waker = unsafe { Arc::from_raw(callback_parameter as *const AtomicWaker) };
    arc_atomic_waker.wake();
}

impl<T> Drop for FdbFuture<T> {
    fn drop(&mut self) {
        if let Some(c_ptr) = self.c_ptr.take() {
            // `fdb_future_destroy` cancels the FDB future, so we
            // don't need to call `fdb_future_cancel`. In addition, if
            // the callback has not yet been called, it will be
            // called.
            unsafe {
                fdb_sys::fdb_future_destroy(c_ptr.as_ptr());
            }
        }
    }
}

impl<T> Future for FdbFuture<T>
where
    T: FdbFutureGet + Unpin,
{
    type Output = FdbResult<T>;

    fn poll(self: Pin<&mut FdbFuture<T>>, cx: &mut Context<'_>) -> Poll<FdbResult<T>> {
        if self.waker.is_none() {
            panic!("Poll called after Poll::Ready(...) was returned!");
        }
        // Safety: Only time `c_ptr` will be `None` is after
        // `Drop::drop` has been called. Otherwise, it will have a
        // `Some(...)` value. So, it is safe to unwrap here.
        let fut_c_ptr = self.c_ptr.as_ref().unwrap().as_ptr();

        let fdb_fut_ref = self.get_mut();

        if unsafe { fdb_sys::fdb_future_is_ready(fut_c_ptr) } != 0 {
            // FDB future is ready

            // Set `waker` to `None` to indicate that we are done with
            // the future, and it would be an error if it was polled
            // again.
            fdb_fut_ref.waker = None;

            Poll::Ready(unsafe { FdbFutureGet::get(fut_c_ptr) })
        } else {
            // FDB future is not ready

            // Safety: Waker will be `Some(...)` here as we *only* set
            // it to `None` just before returning `Poll::Ready`.
            let arc_atomic_waker_ref = fdb_fut_ref.waker.as_ref().unwrap();

            arc_atomic_waker_ref.register(cx.waker());

            // As mentioned in the `AtomicWaker` documentation, do
            // another check to confirm that previous waker did not
            // cause a `wake()` in the meantime, resulting in lost
            // notification.
            if unsafe { fdb_sys::fdb_future_is_ready(fut_c_ptr) } != 0 {
                // FDB future is ready

                // Set `waker` to `None` to indicate that we are done
                // with the future, and it would be an error if it was
                // polled again.
                fdb_fut_ref.waker = None;

                Poll::Ready(unsafe { FdbFutureGet::get(fut_c_ptr) })
            } else if !fdb_fut_ref.callback_set {
                let arc_atomic_waker_copy_ptr = Arc::into_raw(arc_atomic_waker_ref.clone());

                match check(unsafe {
                    fdb_sys::fdb_future_set_callback(
                        fut_c_ptr,
                        Some(FDBCallback),
                        arc_atomic_waker_copy_ptr as *mut ffi::c_void,
                    )
                }) {
                    Ok(_) => {
                        // Setting callback was successful.
                        fdb_fut_ref.callback_set = true;

                        Poll::Pending
                    }
                    Err(err) => {
                        // Setting callback was unsuccessful.

                        // Avoid memory leak as callback won't get
                        // called in case of an error. So we have
                        // to clean up the copy that we created.
                        drop(unsafe { Arc::from_raw(arc_atomic_waker_copy_ptr) });

                        Poll::Ready(Err(err))
                    }
                }
            } else {
                // Callback was previously set, return by just
                // updating the `Waker`.
                Poll::Pending
            }
        }
    }
}

/// Extracts value that are owned by [`FdbFuture`].
///
/// # Note
///
/// You will not directly use this trait. It is used by
/// [`Future::poll`] method on [`FdbFuture`].
pub trait FdbFutureGet {
    /// Extract value that are owned by [`FdbFuture`].
    ///
    /// # Safety
    ///
    /// The caller is responsible for making sure that the pointer
    /// `future` is a valid.
    unsafe fn get(future: *mut fdb_sys::FDBFuture) -> FdbResult<Self>
    where
        Self: Sized;
}

/// Represents the asynchronous result of a function that has no
/// return value.
pub type FdbFutureUnit = FdbFuture<()>;

impl FdbFutureGet for () {
    unsafe fn get(future: *mut fdb_sys::FDBFuture) -> FdbResult<()> {
        check(fdb_sys::fdb_future_get_error(future))
    }
}

/// Represents the asynchronous result of a function that returns a
/// database version.
pub type FdbFutureI64 = FdbFuture<i64>;

impl FdbFutureGet for i64 {
    unsafe fn get(future: *mut fdb_sys::FDBFuture) -> FdbResult<i64> {
        let mut out = 0;
        check(fdb_sys::fdb_future_get_int64(future, &mut out)).map(|_| out)
    }
}

/// Represents the asynchronous result of a function that returns a
/// [`Key`] from a database.
pub type FdbFutureKey = FdbFuture<Key>;

impl FdbFutureGet for Key {
    unsafe fn get(future: *mut fdb_sys::FDBFuture) -> FdbResult<Key> {
        let mut out_key = ptr::null();
        let mut out_key_length = 0;

        check(fdb_sys::fdb_future_get_key(
            future,
            &mut out_key,
            &mut out_key_length,
        ))
        .map(|_| {
            Bytes::copy_from_slice(if out_key_length == 0 {
                &b""[..]
            } else {
                slice::from_raw_parts(out_key, out_key_length.try_into().unwrap())
            })
            .into()
        })
    }
}

/// Represents the asynchronous result of a function that *maybe*
/// returns a key [`Value`] from a database.
pub type FdbFutureMaybeValue = FdbFuture<Option<Value>>;

impl FdbFutureGet for Option<Value> {
    unsafe fn get(future: *mut fdb_sys::FDBFuture) -> FdbResult<Option<Value>> {
        let mut out_present = 0;
        let mut out_value = ptr::null();
        let mut out_value_length = 0;

        check(fdb_sys::fdb_future_get_value(
            future,
            &mut out_present,
            &mut out_value,
            &mut out_value_length,
        ))
        .map(|_| {
            if out_present != 0 {
                Some(
                    Bytes::copy_from_slice(if out_value_length == 0 {
                        &b""[..]
                    } else {
                        slice::from_raw_parts(out_value, out_value_length.try_into().unwrap())
                    })
                    .into(),
                )
            } else {
                None
            }
        })
    }
}

/// Represents the asynchronous result of a function that returns an
/// array of [`CString`].
pub type FdbFutureCStringArray = FdbFuture<Vec<CString>>;

impl FdbFutureGet for Vec<CString> {
    unsafe fn get(future: *mut fdb_sys::FDBFuture) -> FdbResult<Vec<CString>> {
        let mut out_strings = ptr::null_mut();
        let mut out_count = 0;

        check(fdb_sys::fdb_future_get_string_array(
            future,
            &mut out_strings,
            &mut out_count,
        ))
        .map(|_| {
            let mut cstring_list = Vec::with_capacity(out_count.try_into().unwrap());

            (0..out_count).into_iter().for_each(|i| {
                cstring_list.push(CString::from(CStr::from_ptr(
                    *out_strings.offset(i.try_into().unwrap()),
                )));
            });

            cstring_list
        })
    }
}

pub(crate) type FdbFutureKeyValueArray = FdbFuture<KeyValueArray>;

impl FdbFutureGet for KeyValueArray {
    unsafe fn get(future: *mut fdb_sys::FDBFuture) -> FdbResult<KeyValueArray> {
        let mut out_kv = ptr::null();
        let mut out_count = 0;
        let mut out_more = 0;

        check(fdb_sys::fdb_future_get_keyvalue_array(
            future,
            &mut out_kv,
            &mut out_count,
            &mut out_more,
        ))
        .map(|_| {
            let mut kvs = Vec::with_capacity(out_count.try_into().unwrap());

            (0..out_count).into_iter().for_each(|i| {
                let kv = out_kv.offset(i.try_into().unwrap());

                let key = Bytes::copy_from_slice(slice::from_raw_parts(
                    (*kv).key,
                    (*kv).key_length.try_into().unwrap(),
                ))
                .into();

                let value = Bytes::copy_from_slice(slice::from_raw_parts(
                    (*kv).value,
                    (*kv).value_length.try_into().unwrap(),
                ))
                .into();

                kvs.push(KeyValue::new(key, value));
            });

            // non-zero is `true`.
            KeyValueArray::new(kvs, out_count, out_more != 0)
        })
    }
}

/// A stream of [`KeyValue`]s.
#[derive(Debug)]
pub struct FdbStreamKeyValue {
    range_result_state_machine: RangeResultStateMachine,
}

impl FdbStreamKeyValue {
    pub(crate) fn new(
        transaction: FdbTransaction,
        begin: KeySelector,
        end: KeySelector,
        options: RangeOptions,
        snapshot: bool,
    ) -> FdbStreamKeyValue {
        let limit = if options.get_limit() == 0 {
            None
        } else {
            Some(options.get_limit())
        };

        // Binding tester tests for `2210` error. So, if we are
        // provided with `StreamingMode::Exact` and a `limit` of `0`,
        // we can't change it to `StreamingMode::WantAll`
        let mode = options.get_mode();

        let reverse = options.get_reverse();

        // `iteration` is only valid when mode is
        // `StreamingMode::Iterator`. It is ignored in other modes.
        let iteration = if options.get_mode() == StreamingMode::Iterator {
            Some(1)
        } else {
            None
        };

        let fdb_future_key_value_array = fdb_transaction_get_range(
            transaction.get_c_api_ptr(),
            begin.clone(),
            end.clone(),
            RangeOptions::new(limit.unwrap_or(0), mode, reverse),
            iteration.unwrap_or(0),
            snapshot,
        );

        let range_result_state_machine = RangeResultStateMachine::new(
            transaction,
            begin,
            end,
            mode,
            iteration,
            reverse,
            limit,
            snapshot,
            fdb_future_key_value_array,
        );

        FdbStreamKeyValue {
            range_result_state_machine,
        }
    }
}

impl Stream for FdbStreamKeyValue {
    type Item = FdbResult<KeyValue>;

    fn poll_next(
        mut self: Pin<&mut FdbStreamKeyValue>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<FdbResult<KeyValue>>> {
        Pin::new(&mut self.range_result_state_machine).poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use futures::task::AtomicWaker;
    use futures::Stream;

    use impls::impls;

    use std::future::Future;
    use std::marker::PhantomData;
    use std::ptr::NonNull;
    use std::sync::Arc;

    use super::{
        FdbFutureCStringArray, FdbFutureI64, FdbFutureKey, FdbFutureKeyValueArray,
        FdbFutureMaybeValue, FdbFutureUnit, FdbStreamKeyValue,
    };

    #[test]
    fn impls() {
        #[rustfmt::skip]
	assert!(impls!(
	    FdbFutureUnit:
	        Send &
	        Future &
		!Clone &
		!Copy));

        #[rustfmt::skip]
	assert!(impls!(
	    FdbFutureI64:
	        Send &
	        Future &
		!Clone &
		!Copy));

        #[rustfmt::skip]
	assert!(impls!(
	    FdbFutureKey:
	        Send &
	        Future &
		!Clone &
		!Copy));

        #[rustfmt::skip]
	assert!(impls!(
	    FdbFutureMaybeValue:
	        Send &
	        Future &
		!Clone &
		!Copy));

        #[rustfmt::skip]
	assert!(impls!(
	    FdbFutureCStringArray:
	        Send &
	        Future &
		!Clone &
		!Copy));

        #[rustfmt::skip]
	assert!(impls!(
	    FdbFutureKeyValueArray:
	        Send &
	        Future &
		!Clone &
		!Copy));

        #[rustfmt::skip]
	assert!(impls!(
	    FdbStreamKeyValue:
	        Send &
	        Stream &
		!Clone &
		!Copy));
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    struct DummyFdbFuture<T> {
        c_ptr: Option<NonNull<fdb_sys::FDBFuture>>,
        callback_set: bool,
        waker: Option<Arc<AtomicWaker>>,
        _marker: PhantomData<T>,
    }

    unsafe impl<T> Send for DummyFdbFuture<T> {}

    #[test]
    fn trait_bounds() {
        fn trait_bounds_for_fdb_transaction<T>(_t: T)
        where
            T: Send + 'static,
        {
        }

        let d = DummyFdbFuture::<()> {
            c_ptr: Some(NonNull::dangling()),
            callback_set: false,
            waker: Some(Arc::new(AtomicWaker::new())),
            _marker: PhantomData,
        };

        trait_bounds_for_fdb_transaction(d);
    }
}
