// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{HnStory, HnStoryId};
use obzenflow::sources::{ListDetailDecoder, Url};
use obzenflow_core::TypedPayload;

/// Build the decoder for the HN Firebase API:
/// `topstories.json` → many `item/{id}.json`.
pub fn hn_story_decoder(
    base_url: Url,
    max_stories: usize,
) -> ListDetailDecoder<HnStoryId, HnStory> {
    ListDetailDecoder::builder(HnStory::versioned_event_type())
        .base_url(base_url)
        .list_path("v0/topstories.json")
        .parse_list(|response| Ok(response.json()?))
        .detail_path(|id| format!("v0/item/{id}.json"))
        .parse_item(|response| Ok(response.json()?))
        .max_list_items(max_stories)
        .on_skip(|id| tracing::info!(id = %id, "HN item deleted, skipping"))
        .build()
        .expect("HN decoder builder should be fully configured")
}
