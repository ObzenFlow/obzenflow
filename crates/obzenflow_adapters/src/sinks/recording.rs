// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Sinks that keep or count every input they receive, for tests.
//!
//! Replay makes no call to a sink, so a replayed run records nothing here.
//! Read a replay's sink outcomes from its journal instead.

use obzenflow_core::event::schema::{EmptySet, WithMember};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{
    port, system, Command, DeclaredEffectSet, EffectError, Port, SinkRole,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use serde::Serialize;
use serde_json::Value;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::Notify;

/// The request a recording sink sends for each input, its payload.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RecordedInput {
    pub payload: Value,
}

/// The port a recording sink sends through. Keeping an input twice changes
/// what a test reads, so the method is `at_least_once`.
#[port]
pub trait RecordingPort {
    #[effect(type = "obzenflow.testing.record", at_least_once, records = reply)]
    fn record(&self, input: &RecordedInput) -> Result<(), EffectError>;
}

struct Recorder {
    rows: Arc<Mutex<Vec<Value>>>,
    arrived: Arc<Notify>,
}

#[system("recording")]
impl RecordingPort for Recorder {
    fn record(&self, input: &RecordedInput) -> Result<(), EffectError> {
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(input.payload.clone());
        self.arrived.notify_waiters();
        Ok(())
    }
}

/// What a recording sink has received, in the order its calls ran.
pub struct Recorded<T> {
    rows: Arc<Mutex<Vec<Value>>>,
    arrived: Arc<Notify>,
    _input: PhantomData<fn() -> T>,
}

impl<T> Clone for Recorded<T> {
    fn clone(&self) -> Self {
        Self {
            rows: self.rows.clone(),
            arrived: self.arrived.clone(),
            _input: PhantomData,
        }
    }
}

impl<T: TypedPayload> Recorded<T> {
    pub fn items(&self) -> Vec<T> {
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|payload| {
                serde_json::from_value(payload.clone()).expect("a recorded input decodes")
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Wait until the sink has received at least `count` inputs.
    pub async fn wait_for(&self, count: usize) {
        loop {
            let arrived = self.arrived.notified();
            if self.len() >= count {
                return;
            }
            arrived.await;
        }
    }
}

/// A sink that keeps each input for the test to read through its [`Recorded`].
pub struct RecordingSink<T> {
    port: Port<dyn RecordingPort>,
    _input: PhantomData<fn() -> T>,
}

impl<T: TypedPayload + Send + Sync + 'static> RecordingSink<T> {
    /// A sink whose port is named after `T`. A flow with two recording sinks
    /// of the same input names the second with [`Self::named`].
    pub fn new() -> (Self, Recorded<T>) {
        let type_name = std::any::type_name::<T>();
        Self::named(type_name.rsplit("::").next().unwrap_or(type_name))
    }

    pub fn named(name: &str) -> (Self, Recorded<T>) {
        let rows = Arc::new(Mutex::new(Vec::new()));
        let arrived = Arc::new(Notify::new());
        let port = Port::to(Recorder {
            rows: rows.clone(),
            arrived: arrived.clone(),
        })
        .instance(name);
        (
            Self {
                port,
                _input: PhantomData,
            },
            Recorded {
                rows,
                arrived,
                _input: PhantomData,
            },
        )
    }
}

impl<T> Clone for RecordingSink<T> {
    fn clone(&self) -> Self {
        Self {
            port: self.port.clone(),
            _input: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for RecordingSink<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordingSink")
            .field("input", &std::any::type_name::<T>())
            .finish()
    }
}

impl<T> Command for RecordingSink<T> {
    type Ports = (Port<dyn RecordingPort>, ());
    type AllowedEffects = DeclaredEffectSet<WithMember<RecordedInput, EmptySet>>;

    fn ports(&self) -> Self::Ports {
        (self.port.clone(), ())
    }
}

impl<T: TypedPayload + Send + Sync + 'static> SinkRole<T> for RecordingSink<T> {
    type Request = RecordedInput;

    fn prepare(&self, input: &T) -> Result<RecordedInput, HandlerError> {
        serde_json::to_value(input)
            .map(|payload| RecordedInput { payload })
            .map_err(|error| HandlerError::Validation(error.to_string()))
    }
}

/// The request a counting sink sends for each input, which holds nothing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Counted;

/// The port a counting sink sends through. Counting an input twice changes
/// what a test reads, so the method is `at_least_once`.
#[port]
pub trait CountingPort {
    #[effect(type = "obzenflow.testing.count", at_least_once, records = reply)]
    fn count(&self, counted: &Counted) -> Result<(), EffectError>;
}

/// A test's atomic counter, which a counting sink adds one to for each call.
pub trait CallCounter: Send + Sync + 'static {
    fn add_one(&self);
}

impl CallCounter for AtomicUsize {
    fn add_one(&self) {
        self.fetch_add(1, Ordering::SeqCst);
    }
}

impl CallCounter for AtomicU64 {
    fn add_one(&self) {
        self.fetch_add(1, Ordering::SeqCst);
    }
}

struct Counter(Arc<dyn CallCounter>);

#[system("counting")]
impl CountingPort for Counter {
    fn count(&self, _: &Counted) -> Result<(), EffectError> {
        self.0.add_one();
        Ok(())
    }
}

/// A sink that adds one to a test's counter for each call it makes. Replay
/// makes no call, so a replayed run counts nothing.
pub struct CountingSink<T> {
    port: Port<dyn CountingPort>,
    _input: PhantomData<fn() -> T>,
}

impl<T> CountingSink<T> {
    /// A sink whose port is named after `T`. A flow with two counting sinks of
    /// the same input names the second with [`Self::named`].
    pub fn new(counter: Arc<impl CallCounter>) -> Self {
        let type_name = std::any::type_name::<T>();
        Self::named(type_name.rsplit("::").next().unwrap_or(type_name), counter)
    }

    pub fn named(name: &str, counter: Arc<impl CallCounter>) -> Self {
        Self {
            port: Port::to(Counter(counter)).instance(name),
            _input: PhantomData,
        }
    }
}

impl<T> Clone for CountingSink<T> {
    fn clone(&self) -> Self {
        Self {
            port: self.port.clone(),
            _input: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for CountingSink<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CountingSink")
            .field("input", &std::any::type_name::<T>())
            .finish()
    }
}

impl<T> Command for CountingSink<T> {
    type Ports = (Port<dyn CountingPort>, ());
    type AllowedEffects = DeclaredEffectSet<WithMember<Counted, EmptySet>>;

    fn ports(&self) -> Self::Ports {
        (self.port.clone(), ())
    }
}

impl<T: TypedPayload + Send + Sync + 'static> SinkRole<T> for CountingSink<T> {
    type Request = Counted;

    fn prepare(&self, _input: &T) -> Result<Counted, HandlerError> {
        Ok(Counted)
    }
}
