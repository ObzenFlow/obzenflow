// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Events: the input that a sender starts, such as an HTTP POST or a value on a
//! source channel. An [`IngressDecoder`] turns each submitted body into the
//! source's output, and [`source_channel`] gives in-process code a sender. The
//! source records each admitted member before the fact that it produces.
//!
//! ```rust
//! use obzenflow::events::{IngressDecodeError, IngressDecoder, IngressRecord};
//! use obzenflow::schema::TypedPayload;
//! use serde::{Deserialize, Serialize};
//! use serde_json::json;
//!
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct LedgerEntry {
//!     account_id: String,
//!     amount_cents: u64,
//! }
//!
//! impl TypedPayload for LedgerEntry {
//!     const EVENT_TYPE: &'static str = "bank.ledger_entry";
//! }
//!
//! #[derive(Clone)]
//! struct LedgerIngress;
//!
//! impl IngressDecoder for LedgerIngress {
//!     type Output = LedgerEntry;
//!
//!     fn decode(&self, record: IngressRecord<'_>) -> Result<LedgerEntry, IngressDecodeError> {
//!         let entry: LedgerEntry = record.deserialize()?;
//!         if entry.amount_cents == 0 {
//!             return Err(IngressDecodeError::invalid_value("amount_cents"));
//!         }
//!         Ok(entry)
//!     }
//! }
//!
//! let zero = json!({"account_id": "acct-1", "amount_cents": 0});
//! let refused = LedgerIngress.decode(IngressRecord::new(&zero)).unwrap_err();
//! assert_eq!(refused.to_string(), "invalid value in field amount_cents");
//! ```

pub use obzenflow_runtime::events::{
    source_channel, BatchSubmission, EventSubmission, IngressDecodeError, IngressDecoder,
    IngressRecord, IngressRequestId, IngressRequestIdentity, InvalidIngressRequestId, SourceClosed,
    SourcePermit, SourceReceiver, SourceSender,
};
