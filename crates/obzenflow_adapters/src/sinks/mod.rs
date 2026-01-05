//! Adapter sinks (FLOWIP-083 series).

pub mod console;

pub use console::{
    ConsoleSink, DebugFormatter, Formatter, JsonFormatter, JsonPrettyFormatter,
    OutputDestination, TableFormatter,
};
