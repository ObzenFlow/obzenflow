// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed sink helper facades.

use obzenflow_adapters::sinks::{
    ConsoleSink, DebugFormatter, Formatter, JsonFormatter, JsonPrettyFormatter, TableFormatter,
};
use obzenflow_core::TypedPayload;
use serde::de::DeserializeOwned;

pub fn console<T, F>(formatter: F) -> ConsoleSink<T, F>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: Formatter<T>,
{
    ConsoleSink::<T, _>::new(formatter)
}

pub fn json<T>() -> ConsoleSink<T, JsonFormatter>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    T: serde::Serialize,
{
    ConsoleSink::<T, _>::json()
}

pub fn json_pretty<T>() -> ConsoleSink<T, JsonPrettyFormatter>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    T: serde::Serialize,
{
    ConsoleSink::<T, _>::json_pretty()
}

pub fn debug<T>() -> ConsoleSink<T, DebugFormatter>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    T: std::fmt::Debug,
{
    ConsoleSink::<T, _>::debug()
}

pub fn table<T, E>(columns: &[&str], extractor: E) -> ConsoleSink<T, TableFormatter<T, E>>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    E: Fn(&T) -> Vec<String> + Send + Sync + Clone,
{
    ConsoleSink::<T, _>::table(columns, extractor)
}

