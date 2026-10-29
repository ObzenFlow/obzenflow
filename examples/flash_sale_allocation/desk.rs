// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{ReservationConfirmed, StockReserved};
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

    fn emit(&self, state: &Self::State) -> StatefulEmission<Self::Output> {
        StatefulEmission::ResetEpoch {
            outputs: state.pending.iter().cloned().collect(),
        }
    }

    fn post_emit(&self, state: &mut Self::State) {
        state.pending = None;
    }
}
