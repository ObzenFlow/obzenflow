// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Unit tests for HnStoryDecoder (FLOWIP-084f).
//!
//! These tests validate the decoder logic with fixtures — no network required.

use obzenflow::sources::{
    DecodeError, DecodeResult, HeaderMap, HttpResponse, PullDecoder, RequestSpec, Url,
};
use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

// ============================================================================
// Duplicated types from the example (tests should be self-contained)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HnStory {
    id: u64,

    #[serde(default, rename = "type")]
    item_type: Option<String>,

    #[serde(default)]
    by: Option<String>,

    #[serde(default)]
    time: Option<u64>,

    #[serde(default)]
    title: Option<String>,

    #[serde(default)]
    url: Option<String>,

    #[serde(default)]
    score: Option<u32>,

    #[serde(default)]
    descendants: Option<u32>,
}

impl TypedPayload for HnStory {
    const EVENT_TYPE: &'static str = "hn.story";
}

#[derive(Clone, Debug)]
enum HnFetchState {
    FetchItems { pending: VecDeque<u64> },
}

#[derive(Clone, Debug)]
struct HnStoryDecoder {
    base_url: Url,
    max_stories: usize,
}

impl HnStoryDecoder {
    fn new(max_stories: usize) -> Self {
        Self {
            base_url: Url::parse("https://hacker-news.firebaseio.com/")
                .expect("HN base URL should parse"),
            max_stories,
        }
    }
}

impl PullDecoder for HnStoryDecoder {
    type Cursor = HnFetchState;
    type Item = HnStory;

    fn event_type(&self) -> String {
        HnStory::versioned_event_type()
    }

    fn request_spec(&self, cursor: Option<&Self::Cursor>) -> RequestSpec {
        match cursor {
            None => {
                let url = self
                    .base_url
                    .join("v0/topstories.json")
                    .expect("topstories url should join");
                RequestSpec::get(url)
            }
            Some(HnFetchState::FetchItems { pending }) => {
                let id = pending.front().expect("pending not empty");
                let url = self
                    .base_url
                    .join(&format!("v0/item/{id}.json"))
                    .expect("item url should join");
                RequestSpec::get(url)
            }
        }
    }

    fn decode_success(
        &self,
        cursor: Option<&Self::Cursor>,
        response: &HttpResponse,
    ) -> Result<DecodeResult<Self::Cursor, Self::Item>, DecodeError> {
        match cursor {
            None => {
                let ids: Vec<u64> = response
                    .json()
                    .map_err(|e| DecodeError::Parse(e.to_string()))?;

                let pending = ids
                    .into_iter()
                    .take(self.max_stories)
                    .collect::<VecDeque<_>>();

                if pending.is_empty() {
                    return Ok(DecodeResult {
                        items: Vec::new(),
                        next_cursor: None,
                    });
                }

                Ok(DecodeResult {
                    items: Vec::new(),
                    next_cursor: Some(HnFetchState::FetchItems { pending }),
                })
            }
            Some(HnFetchState::FetchItems { pending }) => {
                let mut pending = pending.clone();
                let _ = pending.pop_front();

                // Deleted items return `null` — treat as skip
                let story: Option<HnStory> = response
                    .json()
                    .map_err(|e| DecodeError::Parse(e.to_string()))?;

                let next_cursor = if pending.is_empty() {
                    None
                } else {
                    Some(HnFetchState::FetchItems { pending })
                };

                Ok(DecodeResult {
                    items: story.into_iter().collect(),
                    next_cursor,
                })
            }
        }
    }
}

// ============================================================================
// Test Helpers
// ============================================================================

fn http_ok(body: &str) -> HttpResponse {
    HttpResponse::new(200, HeaderMap::new(), body.as_bytes().to_vec())
}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn hn_decoder_parses_topstories_and_seeds_cursor() {
    let decoder = HnStoryDecoder::new(3);
    let resp = http_ok("[101,102,103,104]");

    let out = decoder.decode_success(None, &resp).expect("decode ok");
    assert!(out.items.is_empty());

    let cursor = out.next_cursor.expect("cursor expected");
    let HnFetchState::FetchItems { pending } = cursor;

    let pending = pending.into_iter().collect::<Vec<_>>();
    assert_eq!(pending, vec![101, 102, 103]);
}

#[test]
fn hn_decoder_max_stories_zero_terminates_immediately() {
    let decoder = HnStoryDecoder::new(0);
    let resp = http_ok("[101,102,103]");

    let out = decoder.decode_success(None, &resp).expect("decode ok");
    assert!(out.items.is_empty());
    assert!(out.next_cursor.is_none());
}

#[test]
fn hn_decoder_parses_item_and_advances_cursor() {
    let decoder = HnStoryDecoder::new(2);
    let cursor = HnFetchState::FetchItems {
        pending: VecDeque::from([42_u64, 43_u64]),
    };

    let item = r#"{
      "id": 42,
      "type": "story",
      "by": "alice",
      "time": 123,
      "title": "Hello",
      "url": "https://example.com/",
      "score": 5,
      "descendants": 1
    }"#;
    let resp = http_ok(item);

    let out = decoder
        .decode_success(Some(&cursor), &resp)
        .expect("decode ok");
    assert_eq!(out.items.len(), 1);
    assert_eq!(out.items[0].id, 42);

    let next = out.next_cursor.expect("cursor expected");
    let HnFetchState::FetchItems { pending } = next;
    let pending = pending.into_iter().collect::<Vec<_>>();
    assert_eq!(pending, vec![43]);
}

#[test]
fn hn_decoder_skips_null_items() {
    let decoder = HnStoryDecoder::new(2);
    let cursor = HnFetchState::FetchItems {
        pending: VecDeque::from([42_u64, 43_u64]),
    };

    let resp = http_ok("null");
    let out = decoder
        .decode_success(Some(&cursor), &resp)
        .expect("decode ok");

    assert!(out.items.is_empty());

    let next = out.next_cursor.expect("cursor expected");
    let HnFetchState::FetchItems { pending } = next;
    let pending = pending.into_iter().collect::<Vec<_>>();
    assert_eq!(pending, vec![43]);
}

#[test]
fn hn_decoder_builds_expected_urls() {
    let decoder = HnStoryDecoder::new(1);
    let req = decoder.request_spec(None);
    assert_eq!(
        req.url.as_str(),
        "https://hacker-news.firebaseio.com/v0/topstories.json"
    );

    let cursor = HnFetchState::FetchItems {
        pending: VecDeque::from([123_u64]),
    };
    let req = decoder.request_spec(Some(&cursor));
    assert_eq!(
        req.url.as_str(),
        "https://hacker-news.firebaseio.com/v0/item/123.json"
    );
}
