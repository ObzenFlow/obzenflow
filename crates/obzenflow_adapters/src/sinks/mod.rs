//! Adapter sinks (FLOWIP-083 series).

pub mod console;
pub mod csv;

pub use console::{
    ConsoleSink, DebugFormatter, Formatter, JsonFormatter, JsonPrettyFormatter,
    OutputDestination, TableFormatter,
};

pub use csv::{CsvSink, CsvSinkBuilder};
