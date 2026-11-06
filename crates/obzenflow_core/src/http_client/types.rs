// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::web::HttpMethod;
use bytes::Bytes;
use http::header::{HeaderName, HeaderValue, AUTHORIZATION};
use http::HeaderMap;
use serde::Serialize;
use std::time::Duration;
use url::Url;

/// The body of an [`HttpRequest`].
#[derive(Debug, Clone)]
pub enum Body {
    Empty,
    /// Encoded bytes; sending writes `content_type` as the request's content type.
    Encoded {
        content_type: HeaderValue,
        bytes: Bytes,
    },
    /// A body or header value that could not be encoded. Sending answers
    /// `HttpClientError::Unencodable` without sending, and later helpers keep it.
    Unencodable(String),
}

/// The HTTP request a system builds in its vendor's shape and hands to the client.
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub url: Url,
    pub headers: HeaderMap,
    pub body: Body,
    /// The deadline for the whole exchange; `None` takes the client's default.
    pub timeout: Option<Duration>,
}

impl HttpRequest {
    pub fn new(method: HttpMethod, url: Url) -> Self {
        Self {
            method,
            url,
            headers: HeaderMap::new(),
            body: Body::Empty,
            timeout: None,
        }
    }

    pub fn get(url: Url) -> Self {
        Self::new(HttpMethod::Get, url)
    }

    pub fn post(url: Url) -> Self {
        Self::new(HttpMethod::Post, url)
    }

    pub fn with_header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.insert(name, value);
        self
    }

    pub fn with_query(mut self, key: &str, value: &str) -> Self {
        self.url.query_pairs_mut().append_pair(key, value);
        self
    }

    /// Bytes already encoded as `content_type`.
    pub fn with_body(self, content_type: HeaderValue, body: impl Into<Bytes>) -> Self {
        self.with_encoded(Body::Encoded {
            content_type,
            bytes: body.into(),
        })
    }

    /// A body encoded as `application/json`.
    pub fn with_json<T: Serialize + ?Sized>(self, value: &T) -> Self {
        let body = match serde_json::to_vec(value) {
            Ok(bytes) => Body::Encoded {
                content_type: HeaderValue::from_static("application/json"),
                bytes: bytes.into(),
            },
            Err(error) => Body::Unencodable(error.to_string()),
        };
        self.with_encoded(body)
    }

    /// A body encoded as `application/x-www-form-urlencoded`.
    pub fn with_form<T: Serialize + ?Sized>(self, value: &T) -> Self {
        let body = match serde_urlencoded::to_string(value) {
            Ok(form) => Body::Encoded {
                content_type: HeaderValue::from_static("application/x-www-form-urlencoded"),
                bytes: form.into(),
            },
            Err(error) => Body::Unencodable(error.to_string()),
        };
        self.with_encoded(body)
    }

    /// An `Authorization: Bearer` header, marked sensitive so `Debug` never prints it.
    pub fn with_bearer_token(self, token: &str) -> Self {
        match HeaderValue::try_from(format!("Bearer {token}")) {
            Ok(mut value) => {
                value.set_sensitive(true);
                self.with_header(AUTHORIZATION, value)
            }
            Err(error) => self.with_encoded(Body::Unencodable(format!("{AUTHORIZATION}: {error}"))),
        }
    }

    /// An `Idempotency-Key` header.
    pub fn with_idempotency_key(self, key: &str) -> Self {
        let name = HeaderName::from_static("idempotency-key");
        match HeaderValue::try_from(key) {
            Ok(value) => self.with_header(name, value),
            Err(error) => self.with_encoded(Body::Unencodable(format!("{name}: {error}"))),
        }
    }

    /// A deadline for this request's whole exchange, over the client's default.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    fn with_encoded(mut self, body: Body) -> Self {
        if !matches!(self.body, Body::Unencodable(_)) {
            self.body = body;
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    fn request() -> HttpRequest {
        HttpRequest::post("https://api.example.test/v1/charges".parse().unwrap())
    }

    fn encoded(request: &HttpRequest) -> (&str, &[u8]) {
        match &request.body {
            Body::Encoded {
                content_type,
                bytes,
            } => (content_type.to_str().unwrap(), bytes),
            other => panic!("expected an encoded body, got {other:?}"),
        }
    }

    #[derive(Serialize)]
    struct Confirm {
        amount: u64,
        confirm: bool,
        #[serde(rename = "metadata[invoice_id]")]
        invoice_id: &'static str,
    }

    #[test]
    fn form_encodes_renamed_keys() {
        let request = request().with_form(&Confirm {
            amount: 9900,
            confirm: true,
            invoice_id: "INV 1",
        });
        assert_eq!(
            encoded(&request),
            (
                "application/x-www-form-urlencoded",
                b"amount=9900&confirm=true&metadata%5Binvoice_id%5D=INV+1".as_slice()
            )
        );
    }

    #[test]
    fn json_sets_content_type() {
        let request = request().with_json(&serde_json::json!({"model": "m"}));
        assert_eq!(
            encoded(&request),
            ("application/json", br#"{"model":"m"}"#.as_slice())
        );
    }

    #[test]
    fn bearer_token_debug_hides_token() {
        let request = request().with_bearer_token("sk_test_secret");
        assert_eq!(
            request.headers.get(AUTHORIZATION).unwrap().as_bytes(),
            b"Bearer sk_test_secret"
        );
        assert!(!format!("{request:?}").contains("sk_test_secret"));
    }

    #[test]
    fn idempotency_key_sets_header() {
        let request = request().with_idempotency_key("invoice:INV-1:1");
        assert_eq!(
            request.headers.get("Idempotency-Key").unwrap(),
            "invoice:INV-1:1"
        );
    }

    #[test]
    fn refused_header_value_stays_unencodable() {
        for request in [
            request()
                .with_idempotency_key("key\n")
                .with_form(&[("a", "b")]),
            request()
                .with_form(&[("a", "b")])
                .with_idempotency_key("key\n"),
        ] {
            assert!(matches!(request.body, Body::Unencodable(_)), "{request:?}");
            assert!(!format!("{request:?}").contains("key\n"));
        }
    }
}
