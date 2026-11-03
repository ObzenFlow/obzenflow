// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::stages::sink::{InlineSink, SinkDescription, SinkWriteFailure};
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Count physical deliveries, including deliveries admitted during replay.
pub struct CountDeliveries<T> {
    counter: Arc<AtomicUsize>,
    input: PhantomData<fn() -> T>,
}
impl<T> CountDeliveries<T> {
    pub fn new(counter: Arc<AtomicUsize>) -> Self {
        Self {
            counter,
            input: PhantomData,
        }
    }
}
impl<T> Clone for CountDeliveries<T> {
    fn clone(&self) -> Self {
        Self::new(self.counter.clone())
    }
}
#[async_trait::async_trait]
impl<T: TypedPayload + Send + Sync + 'static> InlineSink for CountDeliveries<T> {
    type Input = T;
    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Custom("delivery_counter".into()))
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }
    async fn write(&mut self, _input: T) -> Result<(), SinkWriteFailure> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl<T> fmt::Debug for CountDeliveries<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CountDeliveries")
    }
}
