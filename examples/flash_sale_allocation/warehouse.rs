// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The warehouse port, its simulated system, and the requests it answers.

use super::domain::{HoldId, OrderId, Sku, StockReleased, StockReserved};
use async_trait::async_trait;
use obzenflow::effects::{EffectError, System, SystemError};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct WarehouseConfig {
    pub warehouse_id: String,
    pub reserve_latency: Duration,
    pub reserve_unavailable: bool,
}

impl WarehouseConfig {
    pub fn new(warehouse_id: String, reserve_latency: Duration, reserve_unavailable: bool) -> Self {
        Self {
            warehouse_id,
            reserve_latency,
            reserve_unavailable,
        }
    }
}

impl Default for WarehouseConfig {
    fn default() -> Self {
        Self::new(
            "warehouse-demo".to_string(),
            Duration::from_millis(20),
            false,
        )
    }
}

/// The physical integration port. Capacity is intentionally not implemented
/// here: the allocator is the domain authority and sends only authorised work.
#[obzenflow::port]
#[async_trait]
pub trait WarehousePort: Send + Sync {
    #[effect(type = "allocation.reserve_stock", exactly_once)]
    async fn reserve(&self, request: &StockReservation) -> Result<StockReserved, EffectError>;

    #[effect(type = "allocation.release_stock", exactly_once)]
    async fn release(&self, request: &StockRelease) -> Result<StockReleased, EffectError>;
}

#[derive(Debug)]
struct SimulatedWarehouse {
    config: WarehouseConfig,
}

#[async_trait]
impl WarehousePort for SimulatedWarehouse {
    async fn reserve(&self, request: &StockReservation) -> Result<StockReserved, EffectError> {
        tokio::time::sleep(self.config.reserve_latency).await;
        if self.config.reserve_unavailable {
            return Err(EffectError::unavailable(
                "warehouse",
                "unavailable",
                "warehouse unavailable",
            ));
        }
        Ok(request.reserved(HoldId(format!(
            "{}:{}:{}",
            self.config.warehouse_id, request.sku.0, request.order_id.0
        ))))
    }

    async fn release(&self, request: &StockRelease) -> Result<StockReleased, EffectError> {
        Ok(request.released())
    }
}

pub fn warehouse_system(
    config: &WarehouseConfig,
) -> Result<System<dyn WarehousePort>, SystemError> {
    System::eager(
        &config.warehouse_id,
        Arc::new(SimulatedWarehouse {
            config: config.clone(),
        }),
    )
}

#[derive(Clone, Debug, Serialize)]
pub struct StockReservation {
    pub order_id: OrderId,
    pub sku: Sku,
}

impl StockReservation {
    pub fn reserved(&self, hold_id: HoldId) -> StockReserved {
        StockReserved {
            order_id: self.order_id.clone(),
            sku: self.sku.clone(),
            hold_id,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StockRelease {
    pub order_id: OrderId,
    pub sku: Sku,
    pub hold_id: HoldId,
}

impl StockRelease {
    pub fn released(&self) -> StockReleased {
        StockReleased {
            order_id: self.order_id.clone(),
            sku: self.sku.clone(),
            hold_id: self.hold_id.clone(),
        }
    }
}
