// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! AI roles, effects and the generated members of `ai!` stages.

mod collect;
mod effect;
mod grouping;
mod handlers;
mod providers;
mod role;
mod unavailable;

pub use effect::{
    AiBinding, AiBindingError, AiEffect, AiModelSource, AiProviderPort, AiRequestError,
};
pub use providers::AiModelProviders;
pub use role::{AiReply, AiRequest, AiRole};

#[doc(hidden)]
pub use collect::{AiCollector, AiCollectorState};
#[doc(hidden)]
pub use grouping::{AiGrouping, AiGroupingState, AiKeyFn, AiKeying};
#[doc(hidden)]
pub use handlers::{
    AiInferHandler, AiMapHandler, AiMapPartialFacts, AiPartialMapHandler, AiReduceFacts,
    AiReduceHandler,
};
