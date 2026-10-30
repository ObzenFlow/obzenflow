// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The warehouse port, its simulated system, and the requests it answers.

use super::domain::{HoldId, OrderId, Sku, StockReleased, StockReserved};
use async_trait::async_trait;
#[cfg(test)]
use obzenflow::effects::EffectPortResolutionError;
use obzenflow::effects::{EffectError, System, SystemError};
use serde::Serialize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct WarehouseConfig {
    pub warehouse_id: String,
    pub reserve_latency: Duration,
    pub reserve_unavailable: bool,
    #[cfg(test)]
    pub reserve_test_fault: Option<WarehouseTestFault>,
}

impl WarehouseConfig {
    pub fn new(warehouse_id: String, reserve_latency: Duration, reserve_unavailable: bool) -> Self {
        Self {
            warehouse_id,
            reserve_latency,
            reserve_unavailable,
            #[cfg(test)]
            reserve_test_fault: None,
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

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WarehouseTestFault {
    BindingResolution,
    Provenance,
    Journal,
    Validation,
    Domain,
}

#[derive(Debug, Default)]
pub struct WarehouseStats {
    reserve_calls: AtomicUsize,
    release_calls: AtomicUsize,
}

impl WarehouseStats {
    #[allow(dead_code)] // exercised by the checked-in acceptance suite
    pub fn reserve_calls(&self) -> usize {
        self.reserve_calls.load(Ordering::SeqCst)
    }

    #[allow(dead_code)] // exercised by the checked-in acceptance suite
    pub fn release_calls(&self) -> usize {
        self.release_calls.load(Ordering::SeqCst)
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
    stats: Arc<WarehouseStats>,
}

#[async_trait]
impl WarehousePort for SimulatedWarehouse {
    async fn reserve(&self, request: &StockReservation) -> Result<StockReserved, EffectError> {
        self.stats.reserve_calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.config.reserve_latency).await;
        if self.config.reserve_unavailable {
            return Err(EffectError::unavailable(
                "warehouse",
                "unavailable",
                "warehouse unavailable",
            ));
        }
        #[cfg(test)]
        match self.config.reserve_test_fault {
            Some(WarehouseTestFault::Validation) => {
                return Err(EffectError::Validation(
                    "warehouse rejected invalid reservation input".to_string(),
                ));
            }
            Some(WarehouseTestFault::Domain) => {
                return Err(EffectError::Domain(
                    "warehouse refused the reservation".to_string(),
                ));
            }
            Some(WarehouseTestFault::Provenance) => {
                return Err(EffectError::EffectProvenanceMismatch(
                    "injected warehouse provenance failure".to_string(),
                ));
            }
            Some(WarehouseTestFault::Journal) => {
                return Err(EffectError::Journal(
                    "injected warehouse journal failure".to_string(),
                ));
            }
            Some(WarehouseTestFault::BindingResolution) | None => {}
        }
        Ok(request.reserved(HoldId(format!(
            "{}:{}:{}",
            self.config.warehouse_id, request.sku.0, request.order_id.0
        ))))
    }

    async fn release(&self, request: &StockRelease) -> Result<StockReleased, EffectError> {
        self.stats.release_calls.fetch_add(1, Ordering::SeqCst);
        Ok(request.released())
    }
}

pub fn warehouse_system(
    config: &WarehouseConfig,
    stats: Arc<WarehouseStats>,
) -> Result<System<dyn WarehousePort>, SystemError> {
    #[cfg(test)]
    if config.reserve_test_fault == Some(WarehouseTestFault::BindingResolution) {
        return System::deferred(
            &config.warehouse_id,
            || -> Result<Arc<SimulatedWarehouse>, EffectPortResolutionError> {
                Err(EffectPortResolutionError::CredentialUnavailable)
            },
        );
    }
    System::eager(
        &config.warehouse_id,
        Arc::new(SimulatedWarehouse {
            config: config.clone(),
            stats,
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
