// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Local order validation for the resilience tutorial.
//!
//! Validation is deterministic business classification, performed once per
//! order by a single stage with a multi-type output contract:
//!
//! ```text
//! CustomerOrderPlaced -> { ValidatedOrder, InvalidOrder, OrderCancelled }
//! ```
//! `ValidationOutcome` is the handler-side carrier proven leaf-equal to that
//! flat fact set; it is not a topology payload.
//!
//! A valid order becomes `ValidatedOrder` and proceeds to payment
//! authorization. An invalid order records two facts: `InvalidOrder` (the
//! validation outcome, the provenance for anyone asking "why") and the derived
//! lifecycle consequence `OrderCancelled` (the order's fate, which the
//! cancelled-orders subscriber consumes regardless of where cancellation
//! originated).
//!
//! The stage performs no external I/O, so it returns a typed, deterministic
//! classifier outcome rather than using the effectful surface.

use super::domain::{
    CustomerOrderPlaced, InvalidOrder, InvalidOrderReason, OrderCancellationReason, OrderCancelled,
    PaymentMethodState, ValidatedOrder,
};
use obzenflow_core::StageOutputFacts;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::TypedTransformHandler;

/// In-memory carrier for the flat facts authored by local validation.
///
/// The carrier itself is never journalled. Its selected variant is lowered to
/// one `ValidatedOrder`, or to `InvalidOrder` followed by `OrderCancelled`.
#[derive(Debug, Clone, StageOutputFacts)]
pub enum ValidationOutcome {
    Validated(ValidatedOrder),
    Invalid {
        invalid: InvalidOrder,
        cancelled: OrderCancelled,
    },
}

/// The validation stage handler. Classifies each order exactly once.
#[derive(Debug, Clone)]
pub struct ValidateOrder;

impl TypedTransformHandler for ValidateOrder {
    type Input = CustomerOrderPlaced;
    type Output = ValidationOutcome;

    fn process(&self, order: CustomerOrderPlaced) -> Result<Self::Output, HandlerError> {
        match invalid_reason_for(&order) {
            None => Ok(ValidationOutcome::Validated(validated_order(order))),
            Some(reason) => {
                // Fact first, consequence second: the validation outcome is the
                // provenance, the cancellation is the derived lifecycle fact.
                let invalid = invalid_order(&order, reason.clone());
                let cancelled = cancellation_for_invalid(&order, reason);
                Ok(ValidationOutcome::Invalid { invalid, cancelled })
            }
        }
    }
}

fn validated_order(order: CustomerOrderPlaced) -> ValidatedOrder {
    ValidatedOrder {
        order_id: order.order_id,
        customer_id: order.customer_id,
        channel: order.channel,
        amount_cents: order.amount_cents,
        payment_method_state: order.payment_method_state,
        phase: order.phase,
    }
}

fn invalid_order(order: &CustomerOrderPlaced, reason: InvalidOrderReason) -> InvalidOrder {
    InvalidOrder {
        order_id: order.order_id.clone(),
        customer_id: order.customer_id.clone(),
        amount_cents: order.amount_cents,
        phase: order.phase.clone(),
        reason,
    }
}

fn cancellation_for_invalid(
    order: &CustomerOrderPlaced,
    reason: InvalidOrderReason,
) -> OrderCancelled {
    OrderCancelled {
        order_id: order.order_id.clone(),
        customer_id: order.customer_id.clone(),
        amount_cents: order.amount_cents,
        phase: order.phase.clone(),
        reason: OrderCancellationReason::LocalValidationFailed { reason },
    }
}

fn invalid_reason_for(order: &CustomerOrderPlaced) -> Option<InvalidOrderReason> {
    if matches!(
        order.payment_method_state,
        PaymentMethodState::InvalidNumber
    ) {
        Some(InvalidOrderReason::InvalidPaymentMethod)
    } else if order.amount_cents == 0 {
        Some(InvalidOrderReason::ZeroAmount)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::super::domain::{OrderChannel, TrafficPhase};
    use super::*;

    fn order(payment_method_state: PaymentMethodState, amount_cents: u64) -> CustomerOrderPlaced {
        CustomerOrderPlaced {
            order_id: "order-1".to_string(),
            customer_id: "customer-1".to_string(),
            channel: OrderChannel::Web,
            amount_cents,
            payment_method_state,
            phase: TrafficPhase::Warmup,
        }
    }

    #[test]
    fn valid_order_has_no_invalid_reason() {
        assert_eq!(
            invalid_reason_for(&order(PaymentMethodState::Valid, 1000)),
            None
        );
        // Gateway-declined states are locally valid: the gateway decides those.
        assert_eq!(
            invalid_reason_for(&order(PaymentMethodState::InsufficientFunds, 1000)),
            None
        );
        assert_eq!(
            invalid_reason_for(&order(PaymentMethodState::AddressMismatch, 1000)),
            None
        );
    }

    #[test]
    fn invalid_number_and_zero_amount_classify_invalid() {
        assert_eq!(
            invalid_reason_for(&order(PaymentMethodState::InvalidNumber, 1000)),
            Some(InvalidOrderReason::InvalidPaymentMethod)
        );
        assert_eq!(
            invalid_reason_for(&order(PaymentMethodState::Valid, 0)),
            Some(InvalidOrderReason::ZeroAmount)
        );
    }

    #[test]
    fn invalid_order_derives_cancellation_with_local_validation_reason() {
        let placed = order(PaymentMethodState::InvalidNumber, 1000);
        let reason = invalid_reason_for(&placed).expect("order is invalid");
        let cancelled = cancellation_for_invalid(&placed, reason.clone());

        assert_eq!(cancelled.order_id, placed.order_id);
        assert_eq!(
            cancelled.reason,
            OrderCancellationReason::LocalValidationFailed { reason }
        );
    }
}
