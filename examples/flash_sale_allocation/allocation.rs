// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The serial allocation Process Manager.

use super::domain::{
    AllocationInput, CancelIgnored, HoldId, OrderId, ReservationFailed, Sku, SoldOut,
    StockReleased, StockReserved,
};
use super::warehouse::{ReleaseStock, ReserveStock, StockRelease, StockReservation, WarehousePort};
use async_trait::async_trait;
use obzenflow::effects::{EffectError, EffectHandler, Effects, StageCompletion, System};
use obzenflow::error::HandlerError;
use obzenflow::schema::StageOutputFacts;
use obzenflow::stages::stateful::EffectfulStatefulHandler;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq)]
struct HeldStock {
    sku: Sku,
    hold_id: HoldId,
}

/// The authoritative allocation projection, rebuilt only from committed facts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AllocationState {
    allocated_by_sku: HashMap<Sku, u32>,
    holds_by_order: HashMap<OrderId, HeldStock>,
}

impl AllocationState {
    pub(crate) fn allocated(&self, sku: &Sku) -> u32 {
        self.allocated_by_sku.get(sku).copied().unwrap_or(0)
    }

    pub(crate) fn hold(&self, order_id: &OrderId) -> Option<(&Sku, &HoldId)> {
        self.holds_by_order
            .get(order_id)
            .map(|held| (&held.sku, &held.hold_id))
    }
}

/// Compiler-only per-fact witness required by the typed stateful handler.
///
/// It is deliberately not a `TypedPayload`: the enum is never journalled or
/// routed. Each unary variant contains the first-class domain fact that is.
#[derive(Clone, Debug, StageOutputFacts)]
#[stage_output(schema = obzenflow::schema)]
pub enum AllocationOutput {
    Reserved(StockReserved),
    Released(StockReleased),
    ReservationFailed(ReservationFailed),
    SoldOut(SoldOut),
    CancelIgnored(CancelIgnored),
}

type AllocationEffects = obzenflow::effects::effect_set![ReserveStock, ReleaseStock];

#[derive(Clone, Debug, EffectHandler)]
pub struct Allocator {
    #[system]
    warehouse: System<dyn WarehousePort>,
    capacity_per_sku: u32,
    reservation_pace: Duration,
}

impl Allocator {
    pub fn new(warehouse: System<dyn WarehousePort>, capacity_per_sku: u32) -> Self {
        assert!(capacity_per_sku > 0, "allocation capacity must be non-zero");
        Self {
            warehouse,
            capacity_per_sku,
            reservation_pace: Duration::ZERO,
        }
    }

    pub fn with_reservation_pace(mut self, reservation_pace: Duration) -> Self {
        self.reservation_pace = reservation_pace;
        self
    }
}

/// Match only the stable structured cause shared by a live boundary rejection
/// and its strict-replay `RecordedFailure` form.
pub(crate) fn is_reserve_policy_rejection(error: &EffectError) -> bool {
    error.failure_cause().is_some_and(|cause| {
        cause.source.as_str() == "circuit_breaker"
            && matches!(cause.code.as_str(), "circuit_open" | "probe_in_progress")
    })
}

#[async_trait]
impl EffectfulStatefulHandler for Allocator {
    type State = AllocationState;
    type Input = AllocationInput;
    type Output = AllocationOutput;
    type AllowedEffects = AllocationEffects;

    fn initial_state(&self) -> Self::State {
        AllocationState::default()
    }

    async fn decide(
        &mut self,
        state: &Self::State,
        input: &Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        match input {
            AllocationInput::OrderPlaced(placed) => {
                if !fx.is_replaying() {
                    tokio::time::sleep(self.reservation_pace).await;
                }
                if state.allocated(&placed.sku) >= self.capacity_per_sku {
                    fx.emit(SoldOut {
                        order_id: placed.order_id.clone(),
                        sku: placed.sku.clone(),
                    })
                    .await?;
                    return Ok(fx.complete()?);
                }

                let reservation = StockReservation {
                    order_id: placed.order_id.clone(),
                    sku: placed.sku.clone(),
                };
                match fx.call(&self.warehouse, reservation).await {
                    Ok(_) => Ok(fx.complete()?),
                    Err(error) if is_reserve_policy_rejection(&error) => {
                        fx.emit(ReservationFailed {
                            order_id: placed.order_id.clone(),
                            sku: placed.sku.clone(),
                        })
                        .await?;
                        Ok(fx.complete()?)
                    }
                    Err(error) => Err(error.into()),
                }
            }
            AllocationInput::OrderCancelled(cancelled) => {
                let Some((sku, hold_id)) = state
                    .hold(&cancelled.order_id)
                    .map(|(sku, hold_id)| (sku.clone(), hold_id.clone()))
                else {
                    fx.emit(CancelIgnored {
                        order_id: cancelled.order_id.clone(),
                    })
                    .await?;
                    return Ok(fx.complete()?);
                };

                let release = StockRelease {
                    order_id: cancelled.order_id.clone(),
                    sku,
                    hold_id,
                };
                fx.call(&self.warehouse, release).await?;
                Ok(fx.complete()?)
            }
        }
    }

    fn apply(&mut self, state: &mut Self::State, fact: Self::Output) -> Result<(), HandlerError> {
        match fact {
            AllocationOutput::Reserved(reserved) => {
                *state
                    .allocated_by_sku
                    .entry(reserved.sku.clone())
                    .or_default() += 1;
                state.holds_by_order.insert(
                    reserved.order_id,
                    HeldStock {
                        sku: reserved.sku,
                        hold_id: reserved.hold_id,
                    },
                );
            }
            AllocationOutput::Released(released) => {
                let held = state
                    .holds_by_order
                    .remove(&released.order_id)
                    .ok_or_else(|| {
                        HandlerError::ContractViolation(
                            "StockReleased had no folded hold".to_string(),
                        )
                    })?;
                if held.sku != released.sku || held.hold_id != released.hold_id {
                    return Err(HandlerError::ContractViolation(
                        "StockReleased did not match the folded hold".to_string(),
                    ));
                }
                let allocated = state
                    .allocated_by_sku
                    .get_mut(&released.sku)
                    .ok_or_else(|| {
                        HandlerError::ContractViolation(
                            "StockReleased had no folded allocation count".to_string(),
                        )
                    })?;
                *allocated = allocated.checked_sub(1).ok_or_else(|| {
                    HandlerError::ContractViolation(
                        "StockReleased would underflow allocation count".to_string(),
                    )
                })?;
            }
            AllocationOutput::ReservationFailed(_)
            | AllocationOutput::SoldOut(_)
            | AllocationOutput::CancelIgnored(_) => {}
        }
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "flash-sale-allocation-v1"
    }
}

#[cfg(test)]
mod tests {
    use super::super::warehouse::{warehouse_system, WarehouseConfig, WarehouseStats};
    use super::*;
    use obzenflow::effects::{
        EffectFailureCause, EffectFailureCode, EffectFailureSource, RetryDisposition,
    };
    use std::sync::Arc;

    fn order(value: &str) -> OrderId {
        OrderId::from(value)
    }

    fn sku() -> Sku {
        Sku::from("flash-sku")
    }

    #[test]
    fn only_the_structured_reserve_policy_rejection_is_translated() {
        let live = EffectError::BoundaryRejected {
            rejected_by: EffectFailureSource::new("circuit_breaker"),
            code: EffectFailureCode::new("circuit_open"),
            message: "open".to_string(),
            retry: RetryDisposition::Retryable,
        };
        let replay = EffectError::RecordedFailure {
            error_type: "boundary_rejected".into(),
            error_message: "open".to_string(),
            retry: RetryDisposition::Retryable,
            cause: Some(EffectFailureCause {
                source: EffectFailureSource::new("circuit_breaker"),
                code: EffectFailureCode::new("circuit_open"),
            }),
            detail: None,
        };
        let dependency = EffectError::DependencyFailed {
            failure_source: EffectFailureSource::new("warehouse"),
            code: EffectFailureCode::new("unavailable"),
            message: "down".to_string(),
            retry: RetryDisposition::Retryable,
        };
        let unrelated_recording = EffectError::RecordedFailure {
            error_type: "timeout".into(),
            error_message: "slow".to_string(),
            retry: RetryDisposition::Retryable,
            cause: None,
            detail: None,
        };

        assert!(is_reserve_policy_rejection(&live));
        assert!(is_reserve_policy_rejection(&replay));
        assert!(!is_reserve_policy_rejection(&dependency));
        assert!(!is_reserve_policy_rejection(&unrelated_recording));
        assert!(!is_reserve_policy_rejection(
            &EffectError::EffectProvenanceMismatch("wrong binding evidence".to_string())
        ));
        assert!(!is_reserve_policy_rejection(&EffectError::Journal(
            "append failed".to_string()
        )));
        assert!(!is_reserve_policy_rejection(&EffectError::Validation(
            "bad request".to_string()
        )));
        assert!(!is_reserve_policy_rejection(&EffectError::Domain(
            "reservation refused".to_string()
        )));
    }

    #[test]
    fn failed_reservation_is_a_noop_for_capacity_and_holds() {
        let warehouse = warehouse_system(
            &WarehouseConfig::default(),
            Arc::new(WarehouseStats::default()),
        )
        .expect("the demo warehouse is a valid system");
        let mut allocator = Allocator::new(warehouse, 1);
        let mut state = AllocationState::default();
        allocator
            .apply(
                &mut state,
                AllocationOutput::Reserved(StockReserved {
                    order_id: order("order-1"),
                    sku: sku(),
                    hold_id: HoldId("hold-1".to_string()),
                }),
            )
            .unwrap();
        let before = state.clone();

        allocator
            .apply(
                &mut state,
                AllocationOutput::ReservationFailed(ReservationFailed {
                    order_id: order("order-2"),
                    sku: sku(),
                }),
            )
            .unwrap();

        assert_eq!(state, before);
        assert_eq!(state.allocated(&sku()), 1);
        assert!(state.hold(&order("order-1")).is_some());
        assert!(state.hold(&order("order-2")).is_none());
    }
}
