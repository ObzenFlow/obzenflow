// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter sinks

pub mod console;
pub mod csv;
#[cfg(feature = "postgres")]
pub mod postgres;

pub use console::{
    ConsoleSink, DebugFormatter, Formatter, JsonFormatter, JsonPrettyFormatter, OutputDestination,
    SnapshotTableFormatter, TableFormatter,
};

pub use csv::{CsvProjection, CsvSink, CsvSinkBuilder};

use obzenflow_core::TypedPayload;
use serde::de::DeserializeOwned;

/// Construct a console sink with a custom formatter.
pub fn console<T, F>(formatter: F) -> ConsoleSink<T, F>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: Formatter<T>,
{
    ConsoleSink::<T>::new(formatter)
}

/// Construct a PostgreSQL sink from its validated, I/O-free recipe.
#[cfg(feature = "postgres")]
pub fn postgres<B>(config: postgres::PostgresSinkConfig<B>) -> postgres::PostgresSink<B>
where
    B: postgres::PostgresBind,
{
    config.into_sink()
}

/// Construct a console sink using compact JSON formatting.
pub fn json<T>() -> ConsoleSink<T, JsonFormatter>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    T: serde::Serialize,
{
    ConsoleSink::<T>::json()
}

/// Construct a console sink using pretty JSON formatting.
pub fn json_pretty<T>() -> ConsoleSink<T, JsonPrettyFormatter>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    T: serde::Serialize,
{
    ConsoleSink::<T>::json_pretty()
}

/// Construct a console sink using `Debug` formatting.
pub fn debug<T>() -> ConsoleSink<T, DebugFormatter>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    T: std::fmt::Debug,
{
    ConsoleSink::<T>::debug()
}

/// Construct a buffered table-formatted console sink.
pub fn table<T, E>(columns: &[&str], extractor: E) -> ConsoleSink<T, TableFormatter<T, E>>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    E: Fn(&T) -> Vec<String> + Send + Sync + Clone,
{
    ConsoleSink::<T>::table(columns, extractor)
}

#[cfg(test)]
mod helper_tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Event {
        value: u32,
    }

    impl TypedPayload for Event {
        const EVENT_TYPE: &'static str = "sink.helper.event";
    }

    #[test]
    fn owner_helpers_construct_every_supported_console_sink() {
        let custom = console::<Event, _>(|event: &Event| event.value.to_string());
        let compact = json::<Event>();
        let pretty = json_pretty::<Event>();
        let debug = debug::<Event>();
        let table = table::<Event, _>(&["value"], |event| vec![event.value.to_string()]);

        for description in [
            format!("{custom:?}"),
            format!("{compact:?}"),
            format!("{pretty:?}"),
            format!("{debug:?}"),
            format!("{table:?}"),
        ] {
            assert!(description.contains("ConsoleSink"));
        }
    }
}
