// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use futures::stream::once;
use obzenflow_core::http_client::{HeaderValue, HttpClient, HttpMethod, HttpRequest, Url};
use rig_rs::http_client::{
    BoxedStream, Bytes, Error, HttpClientExt, LazyBody, Method, MultipartForm, Request, Response,
    StatusCode, StreamingResponse,
};
use rig_rs::wasm_compat::WasmCompatSend;
use std::future::{ready, Future};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
enum TransportError {
    #[error("the chat transport cannot send method {0}")]
    Method(Method),
    #[error("the chat transport cannot parse its request URL: {0}")]
    Url(url::ParseError),
    #[error("the chat transport sends no multipart request")]
    Multipart,
}

#[derive(Clone)]
pub(super) struct ChatTransport {
    client: Arc<dyn HttpClient>,
    request_timeout: Duration,
}

impl ChatTransport {
    pub(super) fn through(client: Arc<dyn HttpClient>, request_timeout: Duration) -> Self {
        Self {
            client,
            request_timeout,
        }
    }

    pub(super) fn request_timeout(&self) -> Duration {
        self.request_timeout
    }
}

fn http_method(method: &Method) -> Option<HttpMethod> {
    match *method {
        Method::GET => Some(HttpMethod::Get),
        Method::POST => Some(HttpMethod::Post),
        Method::PUT => Some(HttpMethod::Put),
        Method::DELETE => Some(HttpMethod::Delete),
        Method::PATCH => Some(HttpMethod::Patch),
        Method::HEAD => Some(HttpMethod::Head),
        Method::OPTIONS => Some(HttpMethod::Options),
        _ => None,
    }
}

fn http_request<T: Into<Bytes>>(
    request: Request<T>,
    request_timeout: Duration,
) -> Result<HttpRequest, TransportError> {
    let (parts, body) = request.into_parts();
    let method =
        http_method(&parts.method).ok_or_else(|| TransportError::Method(parts.method.clone()))?;
    let url = Url::parse(&parts.uri.to_string()).map_err(TransportError::Url)?;
    let body: Bytes = body.into();
    let content_type = parts
        .headers
        .get("content-type")
        .cloned()
        .unwrap_or_else(|| HeaderValue::from_static("application/octet-stream"));
    let mut request = HttpRequest::new(method, url).with_timeout(request_timeout);
    request.headers = parts.headers;
    if body.is_empty() {
        return Ok(request);
    }
    Ok(request.with_body(content_type, body))
}

fn no_response<E>(source: E) -> Box<Error>
where
    E: std::error::Error + Send + Sync + 'static,
{
    Box::new(Error::instance(source))
}

async fn send_whole(
    client: Arc<dyn HttpClient>,
    request: Result<HttpRequest, TransportError>,
) -> Result<Response<Bytes>, Box<Error>> {
    let request = request.map_err(no_response)?;
    let reply = client.send(request).await.map_err(no_response)?;
    let status = StatusCode::from_u16(reply.status)
        .map_err(|source| Box::new(Error::Protocol(source.into())))?;
    if status != StatusCode::OK {
        return Err(Box::new(Error::non_success_with_details(
            status,
            reply.headers,
            String::from_utf8_lossy(&reply.body).into_owned(),
        )));
    }
    let mut response = Response::new(reply.body);
    *response.status_mut() = status;
    *response.headers_mut() = reply.headers;
    Ok(response)
}

impl HttpClientExt for ChatTransport {
    fn send<T, U>(
        &self,
        req: Request<T>,
    ) -> impl Future<Output = Result<Response<LazyBody<U>>, Error>> + WasmCompatSend + 'static
    where
        T: Into<Bytes>,
        T: WasmCompatSend,
        U: From<Bytes>,
        U: WasmCompatSend + 'static,
    {
        let reply = send_whole(
            Arc::clone(&self.client),
            http_request(req, self.request_timeout),
        );
        async move {
            let response = reply.await.map_err(|failure| *failure)?;
            Ok(response.map(|body| -> LazyBody<U> { Box::pin(ready(Ok(U::from(body)))) }))
        }
    }

    fn send_multipart<U>(
        &self,
        _req: Request<MultipartForm>,
    ) -> impl Future<Output = Result<Response<LazyBody<U>>, Error>> + WasmCompatSend + 'static
    where
        U: From<Bytes>,
        U: WasmCompatSend + 'static,
    {
        ready(Err(Error::instance(TransportError::Multipart)))
    }

    fn send_streaming<T>(
        &self,
        req: Request<T>,
    ) -> impl Future<Output = Result<StreamingResponse, Error>> + WasmCompatSend
    where
        T: Into<Bytes> + WasmCompatSend,
    {
        let reply = send_whole(
            Arc::clone(&self.client),
            http_request(req, self.request_timeout),
        );
        async move {
            let response = reply.await.map_err(|failure| *failure)?;
            Ok(response.map(|body| -> BoxedStream { Box::pin(once(ready(Ok(body)))) }))
        }
    }
}
