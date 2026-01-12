//! HTTP Pull Decoder Examples (FLOWIP-084e).
//!
//! Run with:
//! `cargo run -p obzenflow_adapters --example http_pull_decoders`

fn main() {
    example::run();
}

mod example {
    use obzenflow_adapters::sources::{
        simple_poll, CursorlessPullDecoder, DecodeError, DecodeResult, HttpResponse, PullDecoder,
    };
    use obzenflow_core::http_client::{HeaderMap, RequestSpec, Url};

    #[derive(Debug, Clone)]
    struct OffsetPaginationDecoder {
        base_url: Url,
        limit: usize,
    }

    impl PullDecoder for OffsetPaginationDecoder {
        type Cursor = usize;
        type Item = serde_json::Value;

        fn event_type(&self) -> String {
            "example.http_pull.offset.v1".to_string()
        }

        fn request_spec(&self, cursor: Option<&Self::Cursor>) -> RequestSpec {
            let offset = cursor.copied().unwrap_or(0);

            let mut url = self.base_url.clone();
            url.query_pairs_mut()
                .append_pair("offset", &offset.to_string())
                .append_pair("limit", &self.limit.to_string());

            RequestSpec::get(url)
        }

        fn decode_success(
            &self,
            _cursor: Option<&Self::Cursor>,
            response: &HttpResponse,
        ) -> Result<DecodeResult<Self::Cursor, Self::Item>, DecodeError> {
            let value: serde_json::Value = response
                .json()
                .map_err(|e| DecodeError::Parse(e.to_string()))?;

            let items = value
                .get("items")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            let next_cursor = value
                .get("next_offset")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize);

            Ok(DecodeResult { items, next_cursor })
        }
    }

    #[derive(Debug, Clone)]
    struct CursorPaginationDecoder {
        base_url: Url,
    }

    #[derive(Debug, Clone)]
    struct CursorlessJsonArrayDecoder {
        url: Url,
    }

    impl CursorlessPullDecoder for CursorlessJsonArrayDecoder {
        type Item = serde_json::Value;

        fn event_type(&self) -> String {
            "example.http_pull.cursorless.v1".to_string()
        }

        fn request_spec(&self) -> RequestSpec {
            RequestSpec::get(self.url.clone())
        }

        fn decode_success(&self, response: &HttpResponse) -> Result<Vec<Self::Item>, DecodeError> {
            response
                .json()
                .map_err(|e| DecodeError::Parse(e.to_string()))
        }
    }

    impl PullDecoder for CursorPaginationDecoder {
        type Cursor = String;
        type Item = serde_json::Value;

        fn event_type(&self) -> String {
            "example.http_pull.cursor.v1".to_string()
        }

        fn request_spec(&self, cursor: Option<&Self::Cursor>) -> RequestSpec {
            let mut url = self.base_url.clone();
            if let Some(cursor) = cursor {
                url.query_pairs_mut().append_pair("after", cursor);
            }
            RequestSpec::get(url)
        }

        fn decode_success(
            &self,
            _cursor: Option<&Self::Cursor>,
            response: &HttpResponse,
        ) -> Result<DecodeResult<Self::Cursor, Self::Item>, DecodeError> {
            let value: serde_json::Value = response
                .json()
                .map_err(|e| DecodeError::Parse(e.to_string()))?;

            let items = value
                .get("items")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            let next_cursor = value
                .get("next_cursor")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string());

            Ok(DecodeResult { items, next_cursor })
        }
    }

    pub(super) fn run() {
        // Offset pagination (e.g., ?offset=N&limit=M)
        let offset_decoder = OffsetPaginationDecoder {
            base_url: "http://example.invalid/items"
                .parse()
                .expect("offset base_url"),
            limit: 2,
        };

        let request = offset_decoder.request_spec(None);
        println!("offset page1 request: {}", request.url);

        let page1 = HttpResponse::new(
            200,
            HeaderMap::new(),
            r#"{"items":[{"id":1},{"id":2}],"next_offset":2}"#,
        );
        let decoded1 = offset_decoder.decode(None, &page1).expect("decode page1");
        assert_eq!(decoded1.items.len(), 2);
        assert_eq!(decoded1.next_cursor, Some(2));

        let page2 = HttpResponse::new(
            200,
            HeaderMap::new(),
            r#"{"items":[{"id":3}],"next_offset":null}"#,
        );
        let decoded2 = offset_decoder
            .decode(Some(&decoded1.next_cursor.unwrap()), &page2)
            .expect("decode page2");
        assert_eq!(decoded2.items.len(), 1);
        assert_eq!(decoded2.next_cursor, None);

        let mut rate_limit_headers = HeaderMap::new();
        rate_limit_headers.insert("Retry-After", "60".parse().expect("Retry-After header"));
        let rate_limited = HttpResponse::new(429, rate_limit_headers, "");
        assert!(matches!(
            offset_decoder.decode(None, &rate_limited),
            Err(DecodeError::RateLimited { .. })
        ));

        // Cursor pagination (e.g., ?after=cursor)
        let cursor_decoder = CursorPaginationDecoder {
            base_url: "http://example.invalid/items"
                .parse()
                .expect("cursor base_url"),
        };

        let request = cursor_decoder.request_spec(None);
        println!("cursor page1 request: {}", request.url);

        let page1 = HttpResponse::new(
            200,
            HeaderMap::new(),
            r#"{"items":[{"id":"a"}],"next_cursor":"cursor-1"}"#,
        );
        let decoded1 = cursor_decoder.decode(None, &page1).expect("decode page1");
        assert_eq!(decoded1.items.len(), 1);
        assert_eq!(decoded1.next_cursor.as_deref(), Some("cursor-1"));

        let request = cursor_decoder.request_spec(decoded1.next_cursor.as_ref());
        println!("cursor page2 request: {}", request.url);

        let page2 = HttpResponse::new(
            200,
            HeaderMap::new(),
            r#"{"items":[{"id":"b"}],"next_cursor":null}"#,
        );
        let decoded2 = cursor_decoder
            .decode(decoded1.next_cursor.as_ref(), &page2)
            .expect("decode page2");
        assert_eq!(decoded2.items.len(), 1);
        assert_eq!(decoded2.next_cursor, None);

        // Cursorless helper trait: no cursor type in user code
        let cursorless = CursorlessJsonArrayDecoder {
            url: "http://example.invalid/items"
                .parse()
                .expect("cursorless url"),
        };
        let request = CursorlessPullDecoder::request_spec(&cursorless);
        println!("cursorless request: {}", request.url);

        let page = HttpResponse::new(200, HeaderMap::new(), r#"[{"id":1},{"id":2}]"#);
        let decoded =
            CursorlessPullDecoder::decode_success(&cursorless, &page).expect("cursorless decode");
        assert_eq!(decoded.len(), 2);

        // simple_poll helper: closure-based, cursorless-by-default
        let poll = simple_poll(
            "example.http_pull.simple_poll.v1",
            "http://example.invalid/items"
                .parse()
                .expect("simple_poll url"),
            |response: &HttpResponse| {
                response
                    .json::<Vec<serde_json::Value>>()
                    .map_err(|e| DecodeError::Parse(e.to_string()))
            },
        );

        let request = poll.request_spec(None);
        println!("simple_poll request: {}", request.url);

        let page = HttpResponse::new(200, HeaderMap::new(), r#"[{"id":1}]"#);
        let decoded = poll.decode(None, &page).expect("simple_poll decode");
        assert_eq!(decoded.items.len(), 1);
        assert_eq!(decoded.next_cursor, None);

        // Default status policy: rate limiting maps to DecodeError::RateLimited automatically.
        let mut headers = HeaderMap::new();
        headers.insert("Retry-After", "5".parse().expect("Retry-After header"));
        let rate_limited = HttpResponse::new(429, headers, "");
        assert!(matches!(
            poll.decode(None, &rate_limited),
            Err(DecodeError::RateLimited { .. })
        ));
    }
}
