// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The warehouse port, its simulated system, and the requests it answers.

use super::domain::{HoldId, OrderId, Sku, StockReleased, StockReserved};
use async_trait::async_trait;
use obzenflow::effects::{EffectError, Port, SystemName};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum WarehouseId {
    #[serde(rename = "warehouse-demo")]
    Demo,
    #[serde(rename = "warehouse-other")]
    Other,
}

#[derive(Debug, Clone)]
pub struct WarehouseConfig {
    pub warehouse_id: WarehouseId,
    pub reserve_latency: Duration,
    pub reserve_unavailable: bool,
}

impl WarehouseConfig {
    pub fn new(
        warehouse_id: WarehouseId,
        reserve_latency: Duration,
        reserve_unavailable: bool,
    ) -> Self {
        Self {
            warehouse_id,
            reserve_latency,
            reserve_unavailable,
        }
    }
}

impl Default for WarehouseConfig {
    fn default() -> Self {
        Self::new(WarehouseId::Demo, Duration::from_millis(20), false)
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
struct WarehouseFloor {
    reserve_latency: Duration,
    reserve_unavailable: bool,
}

impl WarehouseFloor {
    async fn reserve(
        &self,
        warehouse: &str,
        request: &StockReservation,
    ) -> Result<StockReserved, EffectError> {
        tokio::time::sleep(self.reserve_latency).await;
        if self.reserve_unavailable {
            return Err(EffectError::unavailable(
                "warehouse",
                "unavailable",
                "warehouse unavailable",
            ));
        }
        Ok(request.reserved(HoldId(format!(
            "{warehouse}:{}:{}",
            request.sku.0, request.order_id.0
        ))))
    }
}

#[derive(Debug)]
struct SimulatedWarehouse(WarehouseFloor);

#[obzenflow::system("warehouse-demo")]
#[async_trait]
impl WarehousePort for SimulatedWarehouse {
    async fn reserve(&self, request: &StockReservation) -> Result<StockReserved, EffectError> {
        self.0.reserve(Self::NAME, request).await
    }

    async fn release(&self, request: &StockRelease) -> Result<StockReleased, EffectError> {
        Ok(request.released())
    }
}

#[derive(Debug)]
struct OtherWarehouse(WarehouseFloor);

#[obzenflow::system("warehouse-other")]
#[async_trait]
impl WarehousePort for OtherWarehouse {
    async fn reserve(&self, request: &StockReservation) -> Result<StockReserved, EffectError> {
        self.0.reserve(Self::NAME, request).await
    }

    async fn release(&self, request: &StockRelease) -> Result<StockReleased, EffectError> {
        Ok(request.released())
    }
}

pub fn warehouse_port(config: &WarehouseConfig) -> Port<dyn WarehousePort> {
    let floor = WarehouseFloor {
        reserve_latency: config.reserve_latency,
        reserve_unavailable: config.reserve_unavailable,
    };
    match config.warehouse_id {
        WarehouseId::Demo => Port::to(SimulatedWarehouse(floor)),
        WarehouseId::Other => Port::to(OtherWarehouse(floor)),
    }
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
