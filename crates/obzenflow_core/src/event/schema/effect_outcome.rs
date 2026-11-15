// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Effect outcomes.
//!
//! A port method answers each call with exactly one outcome: one fact, or an
//! `outcome![...]` list of the facts it may answer with. The answer lowers to
//! flat named `Data` facts for the journal and is rebuilt from the recorded
//! fact group on replay. It is never persisted as a wrapper event and never
//! the stage output contract.

use super::member_set::FactList;
use super::outcome_list::{OutcomeList, OutcomeOr};
use super::stage_fact_set::StageFactSet;
use super::typed_fact_set::TypedFactSet;
use super::typed_payload::TypedPayload;

/// The bound of a port method's answer: one fact, or an `outcome![...]`
/// list.
///
/// ```
/// use obzenflow_core::event::schema::{EffectOutcomeFacts, TypedFactSet};
/// use obzenflow_core::{outcome, TypedPayload};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct PaymentAuthorized { pub authorization_id: String }
/// impl TypedPayload for PaymentAuthorized {
///     const EVENT_TYPE: &'static str = "payment.authorized";
/// }
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct PaymentDeclined { pub reason: String }
/// impl TypedPayload for PaymentDeclined {
///     const EVENT_TYPE: &'static str = "payment.declined";
/// }
///
/// fn answers<A: EffectOutcomeFacts>(answer: A) -> usize {
///     answer.into_facts().unwrap().len()
/// }
///
/// let declined: outcome![PaymentAuthorized | PaymentDeclined] =
///     outcome(PaymentDeclined { reason: "insufficient funds".into() });
/// assert_eq!(answers(declined), 1);
/// ```
///
/// `StageOutputs<T>` and `stage_fact_set![...]` are refused:
///
/// ```compile_fail
/// # use obzenflow_core::{EffectOutcomeFacts, StageOutputs, TypedPayload};
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Authorized;
/// # impl TypedPayload for Authorized { const EVENT_TYPE: &'static str = "doc.authorized"; }
/// fn port_answer<A: EffectOutcomeFacts>() {}
/// port_answer::<StageOutputs<Authorized>>();
/// ```
///
/// ```compile_fail
/// # use obzenflow_core::{stage_fact_set, EffectOutcomeFacts, TypedPayload};
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Authorized;
/// # impl TypedPayload for Authorized { const EVENT_TYPE: &'static str = "doc.authorized"; }
/// # #[derive(serde::Serialize, serde::Deserialize)] struct Declined;
/// # impl TypedPayload for Declined { const EVENT_TYPE: &'static str = "doc.declined"; }
/// fn port_answer<A: EffectOutcomeFacts>() {}
/// port_answer::<stage_fact_set![Authorized, Declined]>();
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be a port's answer",
    label = "a port answers with one fact or `outcome![...]`",
    note = "a port method answers each call with exactly one outcome, listed as \
            `outcome![A | (B, C)]`"
)]
pub trait EffectOutcomeFacts: TypedFactSet + StageFactSet {}

impl<T: TypedPayload + Send + Sync + 'static> EffectOutcomeFacts for T {}

impl<Head, Rest> EffectOutcomeFacts for OutcomeOr<Head, Rest>
where
    Self: OutcomeList,
    <Self as OutcomeList>::Members: FactList,
{
}
