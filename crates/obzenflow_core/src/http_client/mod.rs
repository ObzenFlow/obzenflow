//! Core outbound HTTP client abstractions.
//!
//! This module exists to support onion architecture: inner layers define traits and types,
//! outer layers provide concrete implementations (e.g. reqwest in `obzenflow_infra`).

pub mod client;
pub mod error;
pub mod mock;
pub mod response;
pub mod types;

pub use client::HttpClient;
pub use error::HttpClientError;
pub use mock::MockHttpClient;
pub use response::HttpResponse;
pub use types::RequestSpec;

// Convenience re-exports for user code and adapters.
pub use crate::web::HttpMethod;
pub use bytes::Bytes;
pub use http::HeaderMap;
pub use url::Url;

