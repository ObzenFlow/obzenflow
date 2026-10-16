// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{ReservationConfirmed, StockReserved};
use obzenflow::error::HandlerError;
use obzenflow::stages::stateful::{StatefulEmission, TypedStatefulHandler};

#[derive(Clone, Debug, Default)]
pub struct DeskState {
    issued: u64,
    pending: Option<ReservationConfirmed>,
}

#[derive(Clone, Debug)]
pub struct ReservationDesk;

impl TypedStatefulHandler for ReservationDesk {
    type State = DeskState;
    type Input = StockReserved;
    type Output = ReservationConfirmed;

    fn accumulate(&self, state: &mut Self::State, reserved: StockReserved) {
        state.issued += 1;
        state.pending = Some(ReservationConfirmed {
            order_id: reserved.order_id,
            sku: reserved.sku,
            confirmation: state.issued,
        });
    }

    fn initial_state(&self) -> Self::State {
        DeskState::default()
    }

    fn should_emit(&self, state: &Self::State) -> bool {
        state.pending.is_some()
    }

    fn emit(
        &self,
        state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        let mut next_state = state.clone();
        let outputs = next_state.pending.take().into_iter().collect();
        Ok(StatefulEmission::ResetEpoch {
            next_state,
            outputs,
        })
    }
}
