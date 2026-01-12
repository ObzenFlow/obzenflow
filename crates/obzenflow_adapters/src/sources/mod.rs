//! Adapter sources (FLOWIP-084 series).

pub mod csv;
pub mod http;
pub mod http_pull;

pub use csv::{CsvRow, CsvSource, CsvSourceBuilder};
pub use http::{HttpSource, HttpSourceConfig};

pub use http_pull::{
    simple_poll, CursorlessPullDecoder, DecodeError, DecodeResult, FnPullDecoder, HttpPollConfig,
    HttpPollSource, HttpPullConfig, HttpPullSource, HttpResponse, PullDecoder,
};
