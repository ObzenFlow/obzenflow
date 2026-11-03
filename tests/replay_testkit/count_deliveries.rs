// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::stages::sink::SinkWriteFailure;

/// Count physical deliveries, including deliveries admitted during replay.
pub struct CountDeliveries<T> {
    counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    input: std::marker::PhantomData<fn() -> T>,
}
impl<T> CountDeliveries<T> {
    pub fn new(counter: std::sync::Arc<std::sync::atomic::AtomicUsize>) -> Self {
        Self {
            counter,
            input: std::marker::PhantomData,
        }
    }
}
impl<T> Clone for CountDeliveries<T> {
    fn clone(&self) -> Self {
        Self::new(self.counter.clone())
    }
}
#[async_trait::async_trait]
impl<T: obzenflow_core::TypedPayload + Send + Sync + 'static>
    obzenflow_runtime::stages::sink::InlineSink for CountDeliveries<T>
{
    type Input = T;
    fn describe(&self) -> obzenflow_runtime::stages::sink::SinkDescription {
        obzenflow_runtime::stages::sink::SinkDescription::method(
            obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Custom(
                "delivery_counter".into(),
            ),
        )
        .with_redelivery_safety(obzenflow_runtime::effects::SinkRedeliverySafety::SafeToRepeat)
    }
    async fn write(&mut self, _input: T) -> Result<(), SinkWriteFailure> {
        self.counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

impl<T> std::fmt::Debug for CountDeliveries<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CountDeliveries")
    }
}
