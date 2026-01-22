use bytes::Bytes;
use http::HeaderMap;

/// Framework-owned response representation.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl HttpResponse {
    pub fn new(status: u16, headers: HeaderMap, body: impl Into<Bytes>) -> Self {
        Self {
            status,
            headers,
            body: body.into(),
        }
    }

    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.body)
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}
