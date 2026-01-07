//! Adapter sources (FLOWIP-084 series).

pub mod csv;
pub mod http;

pub use csv::{CsvRow, CsvSource, CsvSourceBuilder};
pub use http::{HttpSource, HttpSourceConfig};
