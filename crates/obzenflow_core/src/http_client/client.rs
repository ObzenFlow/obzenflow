use crate::http_client::{HttpClientError, HttpResponse, RequestSpec};
use async_trait::async_trait;

#[async_trait]
pub trait HttpClient: Send + Sync + 'static {
    async fn execute(&self, request: RequestSpec) -> Result<HttpResponse, HttpClientError>;
}
