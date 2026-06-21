// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![doc = include_str!("../README.md")]

pub mod ai;
pub mod build_info;
pub mod contracts;
pub mod error;
pub mod event;
pub mod http_client;
pub mod id;
pub mod ingress;
pub mod journal;
pub mod metrics;
pub mod observer_middleware;
pub mod time;
pub mod web;

// Re-export key types for convenience
pub use error::Result;
pub use event::chain_event::ChainEvent;
pub use event::context::runtime_context;
pub use event::context::MiddlewareExecutionScope;
pub use event::event_envelope::EventEnvelope;
pub use event::EventId;
pub use event::{EventType, JournalWriterId, WriterId};
pub use journal::journal_error::JournalError;
pub use journal::journal_owner::JournalOwner;
pub use journal::Journal;
pub use observer_middleware::{
    EffectObserver, EffectObserverContext, EffectObserverOutcome, HandlerMiddlewareObserver,
    HandlerObserverContext, IngressObserver, IngressObserverContext, IngressObserverOutcome,
    JoinCanonicalMergeMetadata, JoinDeliverySnapshot, JoinMiddlewareObserver, JoinObserverContext,
    JoinSide, JoinSignalKind, JoinSignalSnapshot, ObserverCommitError, ObserverCommitResult,
    ObserverDeterminism, ObserverReport, OutputCommitObserver, OutputCommitObserverContext,
    SinkDeliveryObserver, SinkDeliveryObserverContext, SinkDeliveryObserverOutcome,
    SourcePollObserver, SourcePollObserverContext, SourcePollObserverOutcome,
    StageLifecycleObserver, StageLifecycleObserverContext, StageLifecyclePhase,
    StageObserverBundle, StatefulMiddlewareObserver, StatefulObserverContext,
};

// The derive generates paths through `::obzenflow_core`, so make that name
// resolve inside this crate too (the serde-style self-alias trick).
extern crate self as obzenflow_core;

// Re-export schema types (FLOWIP-082a; EffectOutcomeFacts FLOWIP-120m)
pub use event::schema::{
    EffectOutcomeFacts, MiddlewareContextKey, TypedFact, TypedFactSet, TypedFactSetError,
    TypedFactType, TypedMiddlewareEvent, TypedPayload,
};

// Re-export typed IDs
pub use id::{CycleDepth, FlowId, JournalId, SccId, StageId, StageKey, SystemId};

// Re-export Ulid for convenience since it's used in many IDs
pub use ulid::Ulid;

// Re-export chrono so macro expansions can reference timestamps without
// requiring downstream crates to depend on chrono directly (FLOWIP-095a).
pub use chrono;

// Re-export core contract types (FLOWIP-090c)
pub use contracts::{
    Contract, ContractContext, ContractEvidence, ContractReadContext, ContractResult,
    ContractState, ContractViolation, ContractWriteContext, DeliveryContract, DivergenceContract,
    DivergenceThresholds, HashMismatch, SourceContract, TransportContract, ViolationCause,
};
