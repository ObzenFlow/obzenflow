// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::TypedPayload;
use obzenflow_runtime::events::{source_channel, Events, IngressDecoder};
use obzenflow_runtime::stages::common::handlers::source::{Continuous, SourceRead};
use obzenflow_runtime::stages::common::handlers::TypedInfiniteSourceHandler;
use obzenflow_runtime::systems::Port;
use std::marker::PhantomData;

pub struct PreloadedInput<T> {
    port: Port<dyn Events>,
    output: PhantomData<fn() -> T>,
}

pub struct ChannelIngress<T>(PhantomData<fn() -> T>);

impl<T> ChannelIngress<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Clone for ChannelIngress<T> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<T: TypedPayload + Send + Sync + 'static> IngressDecoder for ChannelIngress<T> {
    type Output = T;
}

pub fn preloaded_input<T>(values: impl IntoIterator<Item = T>) -> PreloadedInput<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    let values: Vec<T> = values.into_iter().collect();
    let (sender, receiver) = source_channel(values.len().max(1));
    for value in values {
        assert!(sender.try_send(value).is_ok(), "a preloaded value fits");
    }
    std::mem::forget(sender);
    PreloadedInput {
        port: Port::to(receiver),
        output: PhantomData,
    }
}

impl<T> TypedInfiniteSourceHandler for PreloadedInput<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    type Output = T;

    fn read(&self) -> SourceRead<T, Continuous> {
        SourceRead::events(self.port.clone(), ChannelIngress::new())
    }
}
