// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::command::{Command, PortEffects};
use super::request::CallRequest;
use crate::effects::{DeclaredEffectSet, Effects, StageCompletion};
use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::EffectfulTransformHandler;
use crate::systems::Port;
use async_trait::async_trait;
use obzenflow_core::event::schema::{EffectOutcome, EmptySet, FactsOf, WithMember};
use obzenflow_core::TypedPayload;
use std::marker::PhantomData;

/// `perform` starts a command that performs one request for each input and emits the outcome
/// facts of that request. [`PerformRequest::through`] gives the command its port.
///
/// ```
/// # use obzenflow_core::event::schema::TypedPayload;
/// # use obzenflow_runtime::commands::perform;
/// # use obzenflow_runtime::effects::EffectError;
/// # use obzenflow_runtime::systems::Port;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Invoice;
/// # impl TypedPayload for Invoice { const EVENT_TYPE: &'static str = "docs.perform.invoice"; }
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Paid;
/// # impl TypedPayload for Paid { const EVENT_TYPE: &'static str = "docs.perform.paid"; }
/// # #[derive(Clone, Debug, Serialize)]
/// # struct ChargeRequest;
/// # impl ChargeRequest {
/// #     fn for_invoice(_: &Invoice) -> Self { Self }
/// # }
/// # #[obzenflow_runtime::effects::port]
/// # trait PaymentGatewayPort: Send + Sync {
/// #     #[effect(type = "docs.perform.charge", exactly_once)]
/// #     fn charge(&self, request: &ChargeRequest) -> Result<Paid, EffectError>;
/// # }
/// # struct Scripted;
/// # impl PaymentGatewayPort for Scripted {
/// #     fn charge(&self, _: &ChargeRequest) -> Result<Paid, EffectError> { Ok(Paid) }
/// # }
/// let charge = perform(ChargeRequest::for_invoice).through(Port::to(Scripted));
/// ```
///
/// A function that builds no request does not compile:
///
/// ```compile_fail,E0277
/// # use obzenflow_runtime::commands::perform;
/// # #[derive(Clone, Debug)]
/// # struct Invoice;
/// fn same_invoice(invoice: &Invoice) -> Invoice {
///     invoice.clone()
/// }
///
/// let charge = perform(same_invoice);
/// ```
pub fn perform<F, In, R>(request: F) -> PerformRequest<F, In, R>
where
    F: for<'input> Fn(&'input In) -> R,
    R: PerformedRequest,
{
    PerformRequest {
        request,
        performed: PhantomData,
    }
}

/// `PerformRequest` is the value that `perform` returns. It is not a [`Command`] until
/// [`PerformRequest::through`] gives it a port.
pub struct PerformRequest<F, In, R> {
    request: F,
    performed: PhantomData<fn(In) -> R>,
}

impl<F, In, R: CallRequest> PerformRequest<F, In, R> {
    /// `through` gives the request its port and returns the command. Only a port that
    /// `#[obzenflow::port]` marks answers a request:
    ///
    /// ```
    /// # use obzenflow_runtime::commands::{perform, CallRequest};
    /// # use obzenflow_runtime::effects::{DomainFacts, EffectContext, EffectError, Idempotent};
    /// # use obzenflow_runtime::systems::Port;
    /// # #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    /// # struct Ticket;
    /// # impl obzenflow_core::TypedPayload for Ticket {
    /// #     const EVENT_TYPE: &'static str = "doc.ticket";
    /// # }
    /// #[obzenflow_runtime::effects::port]
    /// trait TicketDesk {}
    /// struct Desk;
    /// impl TicketDesk for Desk {}
    ///
    /// # #[derive(Debug, Clone, serde::Serialize)]
    /// # struct Read;
    /// # #[obzenflow_runtime::effects::__async_trait]
    /// # impl CallRequest for Read {
    /// #     const EFFECT_TYPE: &'static str = "doc.read";
    /// #     type Safety = Idempotent;
    /// #     type Port = dyn TicketDesk;
    /// #     type Outcome = Ticket;
    /// #     type Records = DomainFacts;
    /// #     async fn call(&self, _: &dyn TicketDesk, _: &mut EffectContext) -> Result<Ticket, EffectError> {
    /// #         Ok(Ticket)
    /// #     }
    /// # }
    /// let read = perform(|_: &Ticket| Read).through(Port::to(Desk));
    /// ```
    ///
    /// A `perform` line that builds the request of a `#[query]` method does not compile:
    ///
    /// ```compile_fail,E0277
    /// # use obzenflow_runtime::commands::perform;
    /// # use obzenflow_runtime::queries::{FiniteRead, QueryContext, QueryError};
    /// # use obzenflow_runtime::systems::Port;
    /// # #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    /// # struct Ticket;
    /// # impl obzenflow_core::TypedPayload for Ticket {
    /// #     const EVENT_TYPE: &'static str = "doc.ticket";
    /// # }
    /// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    /// struct NextTicket;
    ///
    /// #[obzenflow_runtime::effects::port]
    /// #[async_trait::async_trait]
    /// trait TicketFeed: Send + Sync {
    ///     #[query(type = "doc.tickets.read")]
    ///     async fn read(&self, from: &NextTicket, ctx: &QueryContext)
    ///         -> Result<FiniteRead<Ticket, NextTicket>, QueryError>;
    /// }
    ///
    /// struct Feed;
    ///
    /// #[async_trait::async_trait]
    /// impl TicketFeed for Feed {
    ///     async fn read(&self, _: &NextTicket, _: &QueryContext)
    ///         -> Result<FiniteRead<Ticket, NextTicket>, QueryError> {
    ///         Ok(FiniteRead::EndOfInput)
    ///     }
    /// }
    ///
    /// let read = perform(|_: &Ticket| NextTicket).through(Port::to(Feed));
    /// ```
    pub fn through(self, port: Port<R::Port>) -> Perform<F, In, R>
    where
        R::Port: PortEffects,
    {
        Perform {
            request: self.request,
            port,
            performed: PhantomData,
        }
    }
}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`perform` takes a function that builds the request of an `#[effect]` method from the input of the stage. This function returns `{Self}`, and no port method takes `{Self}`",
    label = "this function does not build a request"
)]
pub trait PerformedRequest: CallRequest {}

#[diagnostic::do_not_recommend]
impl<R: CallRequest> PerformedRequest for R {}

/// `Perform` is the command that `perform(f).through(port)` returns. It performs one
/// request for each input and emits the outcome facts of that request.
pub struct Perform<F, In, R: CallRequest> {
    request: F,
    port: Port<R::Port>,
    performed: PhantomData<fn(In) -> R>,
}

impl<F: Clone, In, R: CallRequest> Clone for Perform<F, In, R> {
    fn clone(&self) -> Self {
        Self {
            request: self.request.clone(),
            port: self.port.clone(),
            performed: PhantomData,
        }
    }
}

impl<F, In, R: CallRequest> std::fmt::Debug for Perform<F, In, R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Perform")
            .field("request", &std::any::type_name::<R>())
            .field("port", &self.port)
            .finish()
    }
}

impl<F, In, R: CallRequest> Command for Perform<F, In, R> {
    type Ports = (Port<R::Port>, ());
    type AllowedEffects = DeclaredEffectSet<WithMember<R, EmptySet>>;

    fn ports(&self) -> Self::Ports {
        (self.port.clone(), ())
    }
}

#[async_trait]
impl<F, In, R> EffectfulTransformHandler for Perform<F, In, R>
where
    F: for<'input> Fn(&'input In) -> R + Send + Sync,
    In: TypedPayload + Send + Sync + 'static,
    R: CallRequest,
    R::Outcome: EffectOutcome,
{
    type Input = In;
    type Output = FactsOf<R::Outcome>;

    async fn process(
        &self,
        input: In,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        fx.call_as_output((self.request)(&input)).await?;
        Ok(fx.complete()?)
    }
}

impl<F, In, R> crate::effects::SinkRole<In> for Perform<F, In, R>
where
    F: for<'input> Fn(&'input In) -> R + Send + Sync + 'static,
    In: 'static,
    R: crate::effects::SinkRequest,
{
    type Request = R;

    fn prepare(&self, input: In) -> Result<R, HandlerError> {
        Ok((self.request)(&input))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::effectful_transform_effects;
    use crate::effects::{EffectError, EffectSafety, EffectSet};
    use obzenflow_core::Outcome;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Invoice {
        amount: u64,
    }

    impl TypedPayload for Invoice {
        const EVENT_TYPE: &'static str = "perform_test.invoice";
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Paid {
        amount: u64,
    }

    impl TypedPayload for Paid {
        const EVENT_TYPE: &'static str = "perform_test.paid";
    }

    #[derive(Clone, Debug, Serialize)]
    struct ChargeRequest {
        amount: u64,
    }

    impl ChargeRequest {
        fn for_invoice(invoice: &Invoice) -> Self {
            Self {
                amount: invoice.amount,
            }
        }
    }

    #[derive(Clone, Debug, Serialize)]
    struct RefundRequest;

    #[derive(Clone, Debug, Serialize)]
    struct ResendRequest;

    #[crate::effects::port]
    trait GatewayPort: Send + Sync {
        #[effect(type = "perform_test.charge", exactly_once)]
        fn charge(&self, request: &ChargeRequest) -> Result<Paid, EffectError>;

        #[effect(type = "perform_test.refund", exactly_once)]
        fn refund(&self, request: &RefundRequest) -> Result<Paid, EffectError>;

        #[effect(type = "perform_test.resend", at_least_once)]
        fn resend(&self, request: &ResendRequest) -> Result<Paid, EffectError>;
    }

    struct Scripted;

    impl GatewayPort for Scripted {
        fn charge(&self, request: &ChargeRequest) -> Result<Paid, EffectError> {
            Ok(Paid {
                amount: request.amount,
            })
        }

        fn refund(&self, _: &RefundRequest) -> Result<Paid, EffectError> {
            Ok(Paid { amount: 0 })
        }

        fn resend(&self, _: &ResendRequest) -> Result<Paid, EffectError> {
            Ok(Paid { amount: 0 })
        }
    }

    fn assert_handler<H>(_: &H)
    where
        H: EffectfulTransformHandler<Input = Invoice> + Clone + std::fmt::Debug + 'static,
    {
    }

    #[test]
    fn input_output_inferred_from_request_fn() {
        let charge = perform(ChargeRequest::for_invoice).through(Port::to(Scripted));
        assert_handler(&charge);
        assert_eq!(
            <<Perform<fn(&Invoice) -> ChargeRequest, Invoice, ChargeRequest> as EffectfulTransformHandler>::Output as Outcome>::member_fact_types(),
            <Paid as Outcome>::member_fact_types(),
        );
    }

    #[test]
    fn closure_builds_request() {
        let floor = 10;
        let charge = perform(move |invoice: &Invoice| ChargeRequest {
            amount: invoice.amount.max(floor),
        })
        .through(Port::to(Scripted));
        assert_handler(&charge);
    }

    #[test]
    fn allowed_effects_limited_to_request() {
        let charge = perform(ChargeRequest::for_invoice).through(Port::to(Scripted));
        let declarations = effectful_transform_effects(&charge).expect("one declaration");
        let effects: Vec<_> = declarations
            .iter()
            .map(|declaration| declaration.effect_type())
            .collect();
        assert_eq!(effects, ["perform_test.charge"]);
        assert_eq!(
            <<Perform<fn(&Invoice) -> ChargeRequest, Invoice, ChargeRequest> as Command>::AllowedEffects as EffectSet>::effect_types(),
            ["perform_test.charge"],
        );
        assert_eq!(declarations[0].safety(), EffectSafety::Idempotent);
    }

    #[test]
    fn perform_at_least_once_preserves_safety() {
        let resend = perform(|_: &Invoice| ResendRequest).through(Port::to(Scripted));
        let declarations = effectful_transform_effects(&resend).expect("one declaration");
        assert_eq!(
            declarations[0].safety(),
            EffectSafety::NonIdempotentAtLeastOnce
        );
    }

    #[test]
    fn debug_format_redacts_port() {
        let charge = perform(ChargeRequest::for_invoice).through(Port::to(Scripted));
        let debug = format!("{charge:?}");
        assert!(debug.contains("ChargeRequest"), "{debug}");
        assert!(!debug.contains("Scripted"), "{debug}");
    }
}
