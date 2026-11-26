// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Events are the input that a sender pushes to a source, for example an HTTP POST
//! or a value on a source channel. A push source is a `subscribe(decoder)` line
//! through the port of an event system. The source records each event before its fact.

mod arrival;
mod channel;
mod ingress;
mod subscribe;

pub(crate) use arrival::ArrivalKind;
pub use arrival::{Arrival, Events};
pub use channel::{source_channel, SourceClosed, SourcePermit, SourceReceiver, SourceSender};
#[cfg(test)]
pub(crate) use ingress::one_fact;
pub(crate) use ingress::LineDecoder;
pub use ingress::{
    prove_journal_form, IngressDecodeError, IngressDecoder, IngressReceiptSender, IngressRecord,
};
pub use obzenflow_core::ingress::{
    BatchSubmission, EventSubmission, IngressContext, IngressRequestId, IngressRequestIdentity,
    InvalidIngressRequestId,
};
pub use subscribe::{subscribe, PendingSubscription, Subscription};
