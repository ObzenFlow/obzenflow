// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Replay-safe external operations: the requests systems answer, the ports a
//! command works through, and the systems the flow builder chooses for them.

pub use obzenflow_core::{BindingEvidenceError, SystemBindingEvidence};
pub use obzenflow_runtime::effects::{
    command, perform, port, system, Command, Keyed, Perform, PerformRequest, Port, StagePort,
    SystemName,
};
pub use obzenflow_runtime::effects::{
    BindingAuthorityFault, BindingMismatchKind, EffectAttemptOrdinal, EffectCursor,
    EffectDescriptorHash, EffectInputPosition, EffectLabel, EffectOrdinal, EffectStageKey,
    EffectType, RecordedFlowId,
};
pub use obzenflow_runtime::effects::{
    EffectBinding, EffectCommitHandle, EffectContext, EffectError, EffectFailureCause,
    EffectFailureCode, EffectFailureDetail, EffectFailureKind, EffectFailureSource,
    EffectPortResolutionError, EffectSet, Effects, IdempotencyKey, RetryDisposition,
    StageCompletion,
};

#[doc(hidden)]
pub use obzenflow_runtime::effects::{
    __async_trait, assert_port_answer, AtLeastOnce, Call, CallRequest, CallSafety,
    DeclaredEffectSet, EffectList, FieldsEffects, Idempotent, KeyRule, NarrowedPort, PortAnswer,
    PortEffects, PortField, PortImpl, PortServesRequest, RequestList, RequiresKey, ServesPort,
    Transactional, WholePort,
};

/// Building blocks for simulated outside systems: a call log that answers a
/// repeated idempotency key with its first decided response and records every
/// call, across process kills.
pub mod simulated {
    pub use obzenflow_infra::simulated::{CallLog, CallLogError};
}

/// The client a system speaks HTTP through, and the framework's one rule for
/// a call that failed: [`unsent`](http::unsent) for a send that got no
/// response, [`unreadable`](http::unreadable) for a body that would not parse,
/// and [`unclaimed`](http::unclaimed) for a status the system did not claim.
/// [`default_http_client`](http::default_http_client) needs the `http` feature.
pub mod http {
    pub use obzenflow_adapters::effects::http::{unclaimed, unreadable, unsent};
    pub use obzenflow_core::http_client::{
        Body, Bytes, HeaderMap, HeaderName, HeaderValue, HttpClient, HttpClientError, HttpMethod,
        HttpRequest, HttpResponse, MockHttpClient, Url,
    };
    pub use obzenflow_infra::http_client::{default_http_client, HttpClientFactoryError};
}
