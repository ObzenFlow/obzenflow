// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::TypedPayload;
use obzenflow_runtime::events::{source_channel, SourceReceiver};
use obzenflow_runtime::stages::common::handlers::source::{Continuous, SourceRead};
use obzenflow_runtime::stages::common::handlers::TypedInfiniteSourceHandler;
use serde::de::DeserializeOwned;
use std::sync::Arc;

pub struct PreloadedInput<T> {
    receiver: Arc<SourceReceiver<T>>,
}

pub fn preloaded_input<T>(values: impl IntoIterator<Item = T>) -> PreloadedInput<T> {
    let values: Vec<T> = values.into_iter().collect();
    let (sender, receiver) = source_channel(values.len().max(1));
    for value in values {
        assert!(sender.try_send(value).is_ok(), "a preloaded value fits");
    }
    std::mem::forget(sender);
    PreloadedInput {
        receiver: Arc::new(receiver),
    }
}

impl<T> TypedInfiniteSourceHandler for PreloadedInput<T>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
{
    type Output = T;

    fn read(&self) -> SourceRead<T, Continuous> {
        SourceRead::channel(self.receiver.clone())
    }
}
