// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::stages::sink::{InlineSink, SinkDescription, SinkWriteFailure};
use std::fmt;
use std::marker::PhantomData;

/// A named no-op destination for replay fixtures that only inspect journals.
pub struct Discard<T>(PhantomData<fn() -> T>);

impl<T> Default for Discard<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}
impl<T> Clone for Discard<T> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl<T: TypedPayload + Send + Sync + 'static> InlineSink for Discard<T> {
    type Input = T;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Noop)
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, _input: T) -> Result<(), SinkWriteFailure> {
        Ok(())
    }
}

impl<T> fmt::Debug for Discard<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Discard")
    }
}
