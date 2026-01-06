//! Common sink implementations (FLOWIP-083 series).

pub use obzenflow_adapters::sinks::{
    ConsoleSink, CsvSink, CsvSinkBuilder, DebugFormatter, Formatter, JsonFormatter,
    JsonPrettyFormatter, OutputDestination, TableFormatter,
};
