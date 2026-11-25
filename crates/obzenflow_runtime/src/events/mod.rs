// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Events are the input that a sender pushes to a source, for example an HTTP POST
//! or a value on a source channel. The source records each admitted member before
//! the fact that it produces.

mod channel;
mod ingress;

pub use channel::{source_channel, SourceClosed, SourcePermit, SourceReceiver, SourceSender};
pub use ingress::{
    prove_journal_form, HostedSubmission, IngressDecodeError, IngressDecoder, IngressReceiptSender,
    IngressRecord,
};
pub use obzenflow_core::ingress::{
    BatchSubmission, EventSubmission, IngressContext, IngressRequestId, IngressRequestIdentity,
    InvalidIngressRequestId,
};
