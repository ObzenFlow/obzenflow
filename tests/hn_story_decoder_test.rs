// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Unit tests for HnStoryDecoder (FLOWIP-084f).
//!
//! These tests validate the decoder logic with fixtures — no network required.

use obzenflow::sources::{HeaderMap, HttpResponse, ListDetailDecoder, PullDecoder, Url};
use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};

// ============================================================================
// Duplicated types from the example (tests should be self-contained)
// ============================================================================

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
struct HnStoryId(u64);

impl std::fmt::Display for HnStoryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HnStory {
    id: HnStoryId,

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

fn hn_story_decoder(max_stories: usize) -> ListDetailDecoder<HnStoryId, HnStory> {
    let base_url =
        Url::parse("https://hacker-news.firebaseio.com/").expect("HN base URL should parse");
    ListDetailDecoder::builder(HnStory::versioned_event_type())
        .base_url(base_url)
        .list_path("v0/topstories.json")
        .parse_list(|response| Ok(response.json()?))
        .detail_path(|id| format!("v0/item/{id}.json"))
        .parse_item(|response| Ok(response.json()?))
        .max_list_items(max_stories)
        .build()
        .expect("HN test decoder builder should be fully configured")
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
    let decoder = hn_story_decoder(3);
    let resp = http_ok("[101,102,103,104]");

    let out = decoder.decode_success(None, &resp).expect("decode ok");
    assert!(out.items.is_empty());

    let cursor = out.next_cursor.expect("cursor expected");

    let req = decoder.request_spec(Some(&cursor));
    assert_eq!(
        req.url.as_str(),
        "https://hacker-news.firebaseio.com/v0/item/101.json"
    );

    let item = http_ok(r#"{"id": 101, "type": "story"}"#);
    let out = decoder
        .decode_success(Some(&cursor), &item)
        .expect("decode ok");
    let cursor = out.next_cursor.expect("cursor expected");
    assert_eq!(out.items.len(), 1);
    assert_eq!(out.items[0].id, HnStoryId(101));

    let req = decoder.request_spec(Some(&cursor));
    assert_eq!(
        req.url.as_str(),
        "https://hacker-news.firebaseio.com/v0/item/102.json"
    );

    let item = http_ok(r#"{"id": 102, "type": "story"}"#);
    let out = decoder
        .decode_success(Some(&cursor), &item)
        .expect("decode ok");
    let cursor = out.next_cursor.expect("cursor expected");
    assert_eq!(out.items.len(), 1);
    assert_eq!(out.items[0].id, HnStoryId(102));

    let req = decoder.request_spec(Some(&cursor));
    assert_eq!(
        req.url.as_str(),
        "https://hacker-news.firebaseio.com/v0/item/103.json"
    );

    let item = http_ok(r#"{"id": 103, "type": "story"}"#);
    let out = decoder
        .decode_success(Some(&cursor), &item)
        .expect("decode ok");
    assert_eq!(out.items.len(), 1);
    assert_eq!(out.items[0].id, HnStoryId(103));
    assert!(out.next_cursor.is_none());
}

#[test]
fn hn_decoder_max_stories_zero_terminates_immediately() {
    let decoder = hn_story_decoder(0);
    let resp = http_ok("[101,102,103]");

    let out = decoder.decode_success(None, &resp).expect("decode ok");
    assert!(out.items.is_empty());
    assert!(out.next_cursor.is_none());
}

#[test]
fn hn_decoder_parses_item_and_advances_cursor() {
    let decoder = hn_story_decoder(2);
    let resp = http_ok("[42,43]");
    let out = decoder.decode_success(None, &resp).expect("decode ok");
    let cursor = out.next_cursor.expect("cursor expected");

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
    assert_eq!(out.items[0].id, HnStoryId(42));

    let next = out.next_cursor.expect("cursor expected");
    let req = decoder.request_spec(Some(&next));
    assert_eq!(
        req.url.as_str(),
        "https://hacker-news.firebaseio.com/v0/item/43.json"
    );
}

#[test]
fn hn_decoder_skips_null_items() {
    let decoder = hn_story_decoder(2);
    let resp = http_ok("[42,43]");
    let out = decoder.decode_success(None, &resp).expect("decode ok");
    let cursor = out.next_cursor.expect("cursor expected");

    let resp = http_ok("null");
    let out = decoder
        .decode_success(Some(&cursor), &resp)
        .expect("decode ok");

    assert!(out.items.is_empty());

    let next = out.next_cursor.expect("cursor expected");
    let req = decoder.request_spec(Some(&next));
    assert_eq!(
        req.url.as_str(),
        "https://hacker-news.firebaseio.com/v0/item/43.json"
    );
}

#[test]
fn hn_decoder_builds_expected_urls() {
    let decoder = hn_story_decoder(1);
    let req = decoder.request_spec(None);
    assert_eq!(
        req.url.as_str(),
        "https://hacker-news.firebaseio.com/v0/topstories.json"
    );

    let resp = http_ok("[123]");
    let out = decoder.decode_success(None, &resp).expect("decode ok");
    let cursor = out.next_cursor.expect("cursor expected");
    let req = decoder.request_spec(Some(&cursor));
    assert_eq!(
        req.url.as_str(),
        "https://hacker-news.firebaseio.com/v0/item/123.json"
    );
}
