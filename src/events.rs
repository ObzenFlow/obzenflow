// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Events are the input that a sender pushes to a source, for example an HTTP POST or a
//! value on a source channel. A push source is the line
//! `subscribe(decoder).through(port)`. The system behind the event port [`Events`]
//! holds the events. The source takes each event and records it before the fact that it
//! produces. The [`IngressDecoder`] of the line maps each event to the output of the
//! source.
//!
//! [`source_channel`] gives in-process code a sender and a receiver.
//! `Port::to(receiver)` builds the event port of the receiver.
//!
//! ```rust
//! use obzenflow::events::{source_channel, subscribe};
//! use obzenflow::events::{IngressDecodeError, IngressDecoder, IngressRecord};
//! use obzenflow::schema::TypedPayload;
//! use obzenflow::systems::Port;
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
//!
//! let (sender, receiver) = source_channel::<LedgerEntry>(16);
//! let ledger = subscribe(LedgerIngress).through(Port::to(receiver));
//! ```

pub use obzenflow_runtime::events::{
    source_channel, subscribe, Arrival, BatchSubmission, EventSubmission, Events,
    IngressDecodeError, IngressDecoder, IngressRecord, IngressRequestId, IngressRequestIdentity,
    InvalidIngressRequestId, PendingSubscription, SourceClosed, SourcePermit, SourceReceiver,
    SourceSender, Subscription,
};
