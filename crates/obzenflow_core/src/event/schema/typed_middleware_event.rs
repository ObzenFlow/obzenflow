// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed middleware lifecycle events.

use crate::event::chain_event::{ChainEvent, ChainEventContent, ChainEventFactory};
use crate::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload, UserMiddlewareEvent,
};
use crate::event::types::WriterId;
use serde::{de::DeserializeOwned, Serialize};

/// Trait for strongly-typed user middleware events.
///
/// These events are stored as `ObservabilityPayload::Middleware(MiddlewareLifecycle::User(..))`.
/// `ChainEvent::event_type()` remains stable (`lifecycle.middleware.user`) regardless of user
/// event types; users match on `Self::EVENT_TYPE` inside the payload.
pub trait TypedMiddlewareEvent: Serialize + DeserializeOwned + Sized {
    const EVENT_TYPE: &'static str;

    fn from_event(event: &ChainEvent) -> Option<Self> {
        match &event.content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::User(UserMiddlewareEvent {
                    event_type,
                    payload,
                }),
            )) if event_type == Self::EVENT_TYPE => serde_json::from_value(payload.clone()).ok(),
            _ => None,
        }
    }

    fn to_event(self, writer_id: WriterId) -> ChainEvent {
        ChainEventFactory::observability_event(
            writer_id,
            ObservabilityPayload::Middleware(MiddlewareLifecycle::User(UserMiddlewareEvent {
                event_type: Self::EVENT_TYPE.to_string(),
                payload: serde_json::to_value(self).expect("serialisation should not fail"),
            })),
        )
    }
}
