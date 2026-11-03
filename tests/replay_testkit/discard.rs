// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::stages::sink::SinkWriteFailure;

/// A named no-op destination for replay fixtures that only inspect journals.
pub struct Discard<T>(std::marker::PhantomData<fn() -> T>);

impl<T> Default for Discard<T> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}
impl<T> Clone for Discard<T> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl<T: obzenflow_core::TypedPayload + Send + Sync + 'static>
    obzenflow_runtime::stages::sink::InlineSink for Discard<T>
{
    type Input = T;

    fn describe(&self) -> obzenflow_runtime::stages::sink::SinkDescription {
        obzenflow_runtime::stages::sink::SinkDescription::method(
            obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
        )
        .with_redelivery_safety(obzenflow_runtime::effects::SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, _input: T) -> Result<(), SinkWriteFailure> {
        Ok(())
    }
}

impl<T> std::fmt::Debug for Discard<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Discard")
    }
}
