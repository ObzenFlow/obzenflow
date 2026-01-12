use async_trait::async_trait;
use obzenflow_core::http_client::{HttpClient, HttpClientError, HttpResponse, RequestSpec};
use obzenflow_core::web::HttpMethod;

#[derive(Debug, Clone)]
pub struct ReqwestHttpClient {
    client: reqwest::Client,
}

impl ReqwestHttpClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    pub fn with_client(client: reqwest::Client) -> Self {
        Self { client }
    }
}

fn map_method(method: HttpMethod) -> reqwest::Method {
    match method {
        HttpMethod::Get => reqwest::Method::GET,
        HttpMethod::Post => reqwest::Method::POST,
        HttpMethod::Put => reqwest::Method::PUT,
        HttpMethod::Delete => reqwest::Method::DELETE,
        HttpMethod::Patch => reqwest::Method::PATCH,
        HttpMethod::Head => reqwest::Method::HEAD,
        HttpMethod::Options => reqwest::Method::OPTIONS,
    }
}

fn map_reqwest_error(err: reqwest::Error) -> HttpClientError {
    if err.is_timeout() {
        return HttpClientError::Timeout(err.to_string());
    }

    if err.is_connect() {
        return HttpClientError::Connection(err.to_string());
    }

    HttpClientError::Transport(err.to_string())
}

#[async_trait]
impl HttpClient for ReqwestHttpClient {
    async fn execute(&self, request: RequestSpec) -> Result<HttpResponse, HttpClientError> {
        let mut builder = self
            .client
            .request(map_method(request.method), request.url)
            .headers(request.headers);

        if let Some(body) = request.body {
            builder = builder.body(body);
        }

        let response = builder.send().await.map_err(map_reqwest_error)?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response.bytes().await.map_err(map_reqwest_error)?;

        Ok(HttpResponse::new(status, headers, body))
    }
}

