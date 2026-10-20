// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The warehouse port, its simulated system, and the requests it answers.

use super::domain::{HoldId, OrderId, Sku, StockReleased, StockReserved};
use async_trait::async_trait;
#[cfg(test)]
use obzenflow::effects::EffectPortResolutionError;
use obzenflow::effects::{
    Call, CallRequest, EffectContext, EffectError, EffectSafety, System, SystemError,
};
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

#[derive(Debug, Clone, thiserror::Error)]
pub enum WarehousePortError {
    #[error("warehouse unavailable")]
    Unavailable,
    #[cfg(test)]
    #[error("warehouse rejected invalid reservation input")]
    Validation,
    #[cfg(test)]
    #[error("warehouse refused the reservation")]
    Domain,
    #[cfg(test)]
    #[error("injected warehouse provenance failure")]
    Provenance,
    #[cfg(test)]
    #[error("injected warehouse journal failure")]
    Journal,
}

/// The physical integration port. Capacity is intentionally not implemented
/// here: the allocator is the domain authority and sends only authorised work.
#[async_trait]
pub trait WarehousePort: Send + Sync {
    async fn reserve(&self, order_id: &OrderId, sku: &Sku) -> Result<HoldId, WarehousePortError>;

    async fn release(
        &self,
        order_id: &OrderId,
        sku: &Sku,
        hold_id: &HoldId,
    ) -> Result<(), WarehousePortError>;
}

#[derive(Debug)]
struct SimulatedWarehouse {
    config: WarehouseConfig,
    stats: Arc<WarehouseStats>,
}

#[async_trait]
impl WarehousePort for SimulatedWarehouse {
    async fn reserve(&self, order_id: &OrderId, sku: &Sku) -> Result<HoldId, WarehousePortError> {
        self.stats.reserve_calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.config.reserve_latency).await;
        if self.config.reserve_unavailable {
            return Err(WarehousePortError::Unavailable);
        }
        #[cfg(test)]
        match self.config.reserve_test_fault {
            Some(WarehouseTestFault::Validation) => {
                return Err(WarehousePortError::Validation);
            }
            Some(WarehouseTestFault::Domain) => return Err(WarehousePortError::Domain),
            Some(WarehouseTestFault::Provenance) => {
                return Err(WarehousePortError::Provenance);
            }
            Some(WarehouseTestFault::Journal) => return Err(WarehousePortError::Journal),
            Some(WarehouseTestFault::BindingResolution) | None => {}
        }
        Ok(HoldId(format!(
            "{}:{}:{}",
            self.config.warehouse_id, sku.0, order_id.0
        )))
    }

    async fn release(
        &self,
        _order_id: &OrderId,
        _sku: &Sku,
        _hold_id: &HoldId,
    ) -> Result<(), WarehousePortError> {
        self.stats.release_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

pub fn warehouse_system(
    config: &WarehouseConfig,
    stats: Arc<WarehouseStats>,
) -> Result<System<dyn WarehousePort>, SystemError> {
    #[cfg(test)]
    if config.reserve_test_fault == Some(WarehouseTestFault::BindingResolution) {
        return System::deferred(&config.warehouse_id, || {
            Err(EffectPortResolutionError::CredentialUnavailable)
        });
    }
    let warehouse: Arc<dyn WarehousePort> = Arc::new(SimulatedWarehouse {
        config: config.clone(),
        stats,
    });
    System::eager(&config.warehouse_id, warehouse)
}

#[derive(Clone, Debug, Serialize)]
pub struct StockReservation {
    pub order_id: OrderId,
    pub sku: Sku,
}

pub type ReserveStock = Call<StockReservation>;

#[async_trait]
impl CallRequest for StockReservation {
    const EFFECT_TYPE: &'static str = "allocation.reserve_stock";
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type Port = dyn WarehousePort;
    type Outcome = StockReserved;

    async fn call(
        &self,
        warehouse: &dyn WarehousePort,
        _: &mut EffectContext,
    ) -> Result<StockReserved, EffectError> {
        let hold_id = warehouse
            .reserve(&self.order_id, &self.sku)
            .await
            .map_err(warehouse_error)?;
        Ok(StockReserved {
            order_id: self.order_id.clone(),
            sku: self.sku.clone(),
            hold_id,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StockRelease {
    pub order_id: OrderId,
    pub sku: Sku,
    pub hold_id: HoldId,
}

pub type ReleaseStock = Call<StockRelease>;

#[async_trait]
impl CallRequest for StockRelease {
    const EFFECT_TYPE: &'static str = "allocation.release_stock";
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type Port = dyn WarehousePort;
    type Outcome = StockReleased;

    async fn call(
        &self,
        warehouse: &dyn WarehousePort,
        _: &mut EffectContext,
    ) -> Result<StockReleased, EffectError> {
        warehouse
            .release(&self.order_id, &self.sku, &self.hold_id)
            .await
            .map_err(warehouse_error)?;
        Ok(StockReleased {
            order_id: self.order_id.clone(),
            sku: self.sku.clone(),
            hold_id: self.hold_id.clone(),
        })
    }
}

fn warehouse_error(error: WarehousePortError) -> EffectError {
    match error {
        WarehousePortError::Unavailable => {
            EffectError::unavailable("warehouse", "unavailable", error)
        }
        #[cfg(test)]
        WarehousePortError::Validation => EffectError::Validation(error.to_string()),
        #[cfg(test)]
        WarehousePortError::Domain => EffectError::Domain(error.to_string()),
        #[cfg(test)]
        WarehousePortError::Provenance => EffectError::EffectProvenanceMismatch(error.to_string()),
        #[cfg(test)]
        WarehousePortError::Journal => EffectError::Journal(error.to_string()),
    }
}
