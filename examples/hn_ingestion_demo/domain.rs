use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};

/// Raw HN item from the Firebase API (we only care about `type=story` in the sink).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnStory {
    pub id: u64,

    #[serde(default, rename = "type")]
    pub item_type: Option<String>,

    #[serde(default)]
    pub by: Option<String>,

    #[serde(default)]
    pub time: Option<u64>,

    #[serde(default)]
    pub title: Option<String>,

    #[serde(default)]
    pub url: Option<String>,

    #[serde(default)]
    pub score: Option<u32>,

    #[serde(default)]
    pub descendants: Option<u32>,
}

impl TypedPayload for HnStory {
    const EVENT_TYPE: &'static str = "hn.story";
}

/// Formatted story for display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormattedStory {
    pub id: u64,
    pub title: String,
    pub url: String,
    pub author: String,
    pub points: u32,
    pub comments: u32,
}

impl TypedPayload for FormattedStory {
    const EVENT_TYPE: &'static str = "hn.story.formatted";
}
