//! HTTP client implementations for outbound requests.
//!
//! The core `HttpClient` trait lives in `obzenflow_core::http_client`.
//! Concrete implementations live here (in infra) behind feature flags.

use obzenflow_core::http_client::HttpClient;
use std::sync::Arc;

#[cfg(feature = "reqwest-client")]
mod reqwest_client;

#[cfg(feature = "reqwest-client")]
pub use reqwest_client::ReqwestHttpClient;

pub fn default_http_client() -> Result<Arc<dyn HttpClient>, HttpClientFactoryError> {
    #[cfg(feature = "reqwest-client")]
    {
        Ok(Arc::new(ReqwestHttpClient::new()))
    }

    #[cfg(not(feature = "reqwest-client"))]
    {
        Err(HttpClientFactoryError::FeatureNotEnabled(
            "reqwest-client".to_string(),
        ))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HttpClientFactoryError {
    #[error("Feature not enabled: {0}")]
    FeatureNotEnabled(String),
}
