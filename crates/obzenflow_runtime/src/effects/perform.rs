// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::command::{Command, Port, PortField};
use super::{CallRequest, DeclaredEffectSet, Effects, StageCompletion};
use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::EffectfulTransformHandler;
use async_trait::async_trait;
use obzenflow_core::event::schema::{EmptySet, WithMember};
use obzenflow_core::TypedPayload;
use std::marker::PhantomData;

/// The command for a stage that performs one request per input. `f` builds the
/// request from the stage's input, [`PerformRequest::through`] supplies the port
/// the request names, and the request's outcome facts are the stage's output.
///
/// ```
/// # use obzenflow_core::event::schema::TypedPayload;
/// # use obzenflow_runtime::effects::{perform, EffectError, Port};
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
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait PaymentGatewayPort: Send + Sync {
/// #     #[effect(type = "docs.perform.charge", exactly_once)]
/// #     fn charge(&self, request: &ChargeRequest) -> Result<Paid, EffectError>;
/// # }
/// # struct Scripted;
/// # #[obzenflow_runtime::effects::system("scripted", effects = obzenflow_runtime::effects)]
/// # impl PaymentGatewayPort for Scripted {
/// #     fn charge(&self, _: &ChargeRequest) -> Result<Paid, EffectError> { Ok(Paid) }
/// # }
/// let charge = perform(ChargeRequest::for_invoice).through(Port::to(Scripted));
/// ```
///
/// A function that builds no request does not compile:
///
/// ```compile_fail,E0277
/// # use obzenflow_runtime::effects::perform;
/// # #[derive(Clone, Debug)]
/// # struct Invoice;
/// fn same_invoice(invoice: &Invoice) -> Invoice {
///     invoice.clone()
/// }
///
/// let charge = perform(same_invoice);
/// ```
///
/// A command that does more than this, emits a fact of its own or holds state is
/// a struct under `#[obzenflow::command]`.
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

/// What `perform` builds and what `through` turns into a command. It is not a
/// command until it has its port.
pub struct PerformRequest<F, In, R> {
    request: F,
    performed: PhantomData<fn(In) -> R>,
}

impl<F, In, R: CallRequest> PerformRequest<F, In, R> {
    /// Supplies the port the request names, which makes this a command.
    pub fn through(self, port: Port<R::Port>) -> Perform<F, In, R> {
        Perform {
            request: self.request,
            port,
            performed: PhantomData,
        }
    }
}

/// A request a function given to `perform` builds.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`perform` takes a function from the stage's input to a request, one a port's `#[effect]` method takes; this function returns `{Self}`, which no port method takes",
    label = "this function does not build a request"
)]
pub trait PerformedRequest: CallRequest {}

#[diagnostic::do_not_recommend]
impl<R: CallRequest> PerformedRequest for R {}

/// The command `perform(f).through(port)` builds: one input, one request, and
/// the request's outcome facts as the stage's output.
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

    const PORT_FIELDS: &'static [PortField] = &[PortField::Performed];

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
{
    type Input = In;
    type Output = R::Outcome;

    async fn process(
        &self,
        input: In,
        fx: &mut Effects<R::Outcome, Self::AllowedEffects>,
    ) -> Result<StageCompletion<R::Outcome>, HandlerError> {
        fx.call_as_output((self.request)(&input)).await?;
        Ok(fx.complete()?)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        effectful_transform_effects, EffectAcknowledgement, EffectError, EffectSafety, EffectSet,
    };
    use super::*;
    use obzenflow_core::StageFactSet;
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

    #[crate::effects::port(effects = crate::effects)]
    trait GatewayPort: Send + Sync {
        #[effect(type = "perform_test.charge", exactly_once)]
        fn charge(&self, request: &ChargeRequest) -> Result<Paid, EffectError>;

        #[effect(type = "perform_test.refund", exactly_once)]
        fn refund(&self, request: &RefundRequest) -> Result<Paid, EffectError>;

        #[effect(type = "perform_test.resend", at_least_once)]
        fn resend(&self, request: &ResendRequest) -> Result<Paid, EffectError>;
    }

    struct Scripted;

    #[crate::effects::system("scripted", effects = crate::effects)]
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
            <<Perform<fn(&Invoice) -> ChargeRequest, Invoice, ChargeRequest> as EffectfulTransformHandler>::Output as StageFactSet>::member_fact_types(),
            <Paid as StageFactSet>::member_fact_types(),
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
        let declarations = effectful_transform_effects(&charge, &[]).expect("one declaration");
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
    fn at_least_once_requires_acknowledgement() {
        let resend = perform(|_: &Invoice| ResendRequest).through(Port::to(Scripted));
        let declarations = effectful_transform_effects(&resend, &[]).expect("one declaration");
        let refusal = declarations[0]
            .validate_safety_syntax()
            .expect_err("an at-least-once request needs its acknowledgement");
        assert_eq!(
            refusal,
            "performs at-least-once effect 'perform_test.resend' without acknowledging it; add `.at_least_once::<ResendRequest>()` after the handler"
        );

        let acknowledged = effectful_transform_effects(
            &resend,
            &[EffectAcknowledgement::at_least_once::<ResendRequest>(
                "ResendRequest",
            )],
        )
        .expect("one declaration");
        assert_eq!(acknowledged[0].validate_safety_syntax(), Ok(()));
    }

    #[test]
    fn debug_format_redacts_port() {
        let charge = perform(ChargeRequest::for_invoice).through(Port::to(Scripted));
        let debug = format!("{charge:?}");
        assert!(debug.contains("ChargeRequest"), "{debug}");
        assert!(debug.contains("\"scripted\""), "{debug}");
        assert!(debug.contains("<not disclosed>"), "{debug}");
    }
}
