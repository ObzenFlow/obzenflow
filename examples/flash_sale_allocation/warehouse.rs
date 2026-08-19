// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed warehouse bindings, effects, and package-local domain operations.

use super::domain::{HoldId, OrderId, Sku, StockReleased, StockReserved};
use async_trait::async_trait;
use obzenflow_core::event::{EffectFailureCode, EffectFailureSource, RetryDisposition};
use obzenflow_core::{BoundedBindingEvidence, StageFactSet};
use obzenflow_runtime::effects::{
    AllowedEffectsAllowEffect, Effect, EffectBinding, EffectBindingEvidence, EffectBindingUse,
    EffectContext, EffectError, EffectOutcomeFitsOutput, EffectPortSlot, EffectPortSlotSet,
    EffectRegistrationBuilder, EffectSafety, EffectSet, Effects, LogicalEffectBindingName, Named,
    NamedEffect,
};
#[cfg(test)]
use obzenflow_runtime::effects::{EffectPortResolutionError, EffectPortResolver};
use serde_json::json;
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

impl Default for WarehouseConfig {
    fn default() -> Self {
        Self {
            warehouse_id: "warehouse-demo".to_string(),
            reserve_latency: Duration::from_millis(20),
            reserve_unavailable: false,
            #[cfg(test)]
            reserve_test_fault: None,
        }
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

#[derive(Clone, PartialEq, Eq)]
pub struct WarehouseBindingEvidence {
    canonical: BoundedBindingEvidence,
}

impl WarehouseBindingEvidence {
    fn new(warehouse_id: &str) -> Self {
        let canonical = BoundedBindingEvidence::try_new(
            json!({ "warehouse_id": warehouse_id })
                .to_string()
                .into_bytes(),
        )
        .expect("warehouse binding evidence is bounded");
        Self { canonical }
    }
}

impl EffectBindingEvidence for WarehouseBindingEvidence {
    const SCHEMA_VERSION: u32 = 1;

    fn canonical_bytes(&self) -> BoundedBindingEvidence {
        self.canonical.clone()
    }
}

pub const WAREHOUSE: EffectPortSlot<dyn WarehousePort> = EffectPortSlot::new("warehouse");

#[derive(Clone, Debug)]
pub struct ReserveStock {
    order_id: OrderId,
    sku: Sku,
    binding: EffectBindingUse<Self>,
}

impl ReserveStock {
    fn new(order_id: OrderId, sku: Sku, binding: EffectBindingUse<Self>) -> Self {
        Self {
            order_id,
            sku,
            binding,
        }
    }
}

#[async_trait]
impl Effect for ReserveStock {
    const EFFECT_TYPE: &'static str = "allocation.reserve_stock";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type BindingMode = Named<WarehouseBindingEvidence>;
    type Outcome = StockReserved;
    type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

    fn label(&self) -> &str {
        "reserve_stock"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "order_id": self.order_id, "sku": self.sku })
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        let warehouse = ctx.port(WAREHOUSE)?;
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

impl NamedEffect for ReserveStock {
    type BindingEvidence = WarehouseBindingEvidence;

    fn binding_use(&self) -> &EffectBindingUse<Self> {
        &self.binding
    }

    fn required_slots() -> EffectPortSlotSet {
        EffectPortSlotSet::single(WAREHOUSE)
    }
}

#[derive(Clone, Debug)]
pub struct ReleaseStock {
    order_id: OrderId,
    sku: Sku,
    hold_id: HoldId,
    binding: EffectBindingUse<Self>,
}

impl ReleaseStock {
    fn new(order_id: OrderId, sku: Sku, hold_id: HoldId, binding: EffectBindingUse<Self>) -> Self {
        Self {
            order_id,
            sku,
            hold_id,
            binding,
        }
    }
}

#[async_trait]
impl Effect for ReleaseStock {
    const EFFECT_TYPE: &'static str = "allocation.release_stock";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type BindingMode = Named<WarehouseBindingEvidence>;
    type Outcome = StockReleased;
    type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

    fn label(&self) -> &str {
        "release_stock"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({
            "order_id": self.order_id,
            "sku": self.sku,
            "hold_id": self.hold_id,
        })
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        let warehouse = ctx.port(WAREHOUSE)?;
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

impl NamedEffect for ReleaseStock {
    type BindingEvidence = WarehouseBindingEvidence;

    fn binding_use(&self) -> &EffectBindingUse<Self> {
        &self.binding
    }

    fn required_slots() -> EffectPortSlotSet {
        EffectPortSlotSet::single(WAREHOUSE)
    }
}

fn warehouse_error(error: WarehousePortError) -> EffectError {
    match error {
        WarehousePortError::Unavailable => EffectError::DependencyFailed {
            failure_source: EffectFailureSource::new("warehouse"),
            code: EffectFailureCode::new("unavailable"),
            message: error.to_string(),
            retry: RetryDisposition::Retryable,
        },
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

#[derive(Debug, thiserror::Error)]
pub enum WarehouseBindingsBuildError {
    #[error("reserve warehouse binding is invalid: {0}")]
    Reserve(#[source] obzenflow_runtime::effects::EffectBindingBuildError),
    #[error("release warehouse binding is invalid: {0}")]
    Release(#[source] obzenflow_runtime::effects::EffectBindingBuildError),
}

/// Composition-root bundle. Each field is the exact proof required by its
/// lexical `via` declaration; this is not a multi-effect binding abstraction.
pub struct WarehouseEffectBindings {
    pub reserve: EffectBinding<ReserveStock>,
    pub release: EffectBinding<ReleaseStock>,
}

impl WarehouseEffectBindings {
    #[allow(dead_code)] // documented composition-root convenience constructor
    pub fn from_config(config: &WarehouseConfig) -> Result<Self, WarehouseBindingsBuildError> {
        Self::with_stats(config, Arc::new(WarehouseStats::default()))
    }

    pub fn with_stats(
        config: &WarehouseConfig,
        stats: Arc<WarehouseStats>,
    ) -> Result<Self, WarehouseBindingsBuildError> {
        let warehouse: Arc<dyn WarehousePort> = Arc::new(SimulatedWarehouse {
            config: config.clone(),
            stats,
        });
        let evidence = WarehouseBindingEvidence::new(&config.warehouse_id);

        let reserve_builder = EffectRegistrationBuilder::<ReserveStock>::new(
            LogicalEffectBindingName::new("reserve_warehouse")
                .expect("static binding name is valid"),
            evidence.clone(),
        );
        #[cfg(test)]
        let reserve = if config.reserve_test_fault == Some(WarehouseTestFault::BindingResolution) {
            let resolver: EffectPortResolver<dyn WarehousePort> =
                Arc::new(|| Err(EffectPortResolutionError::CredentialUnavailable));
            reserve_builder
                .bind_deferred(WAREHOUSE, resolver)
                .map_err(WarehouseBindingsBuildError::Reserve)?
                .finish()
                .map_err(WarehouseBindingsBuildError::Reserve)?
        } else {
            reserve_builder
                .bind_eager(WAREHOUSE, warehouse.clone())
                .map_err(WarehouseBindingsBuildError::Reserve)?
                .finish()
                .map_err(WarehouseBindingsBuildError::Reserve)?
        };
        #[cfg(not(test))]
        let reserve = reserve_builder
            .bind_eager(WAREHOUSE, warehouse.clone())
            .map_err(WarehouseBindingsBuildError::Reserve)?
            .finish()
            .map_err(WarehouseBindingsBuildError::Reserve)?;

        let release = EffectRegistrationBuilder::<ReleaseStock>::new(
            LogicalEffectBindingName::new("release_warehouse")
                .expect("static binding name is valid"),
            evidence,
        )
        .bind_eager(WAREHOUSE, warehouse)
        .map_err(WarehouseBindingsBuildError::Release)?
        .finish()
        .map_err(WarehouseBindingsBuildError::Release)?;

        Ok(Self { reserve, release })
    }
}

/// Domain-named operations keep command construction and generic `perform`
/// plumbing inside the package that owns the integration contract.
#[allow(async_fn_in_trait)]
pub trait WarehouseEffects<Output, AllowedEffects>
where
    Output: StageFactSet,
    AllowedEffects: EffectSet,
{
    async fn reserve_stock<EffectAt, OutcomeProof>(
        &mut self,
        order_id: OrderId,
        sku: Sku,
    ) -> Result<StockReserved, EffectError>
    where
        ReserveStock: AllowedEffectsAllowEffect<AllowedEffects, EffectAt>
            + EffectOutcomeFitsOutput<Output, OutcomeProof>;

    async fn release_stock<EffectAt, OutcomeProof>(
        &mut self,
        order_id: OrderId,
        sku: Sku,
        hold_id: HoldId,
    ) -> Result<StockReleased, EffectError>
    where
        ReleaseStock: AllowedEffectsAllowEffect<AllowedEffects, EffectAt>
            + EffectOutcomeFitsOutput<Output, OutcomeProof>;
}

impl<Output, AllowedEffects> WarehouseEffects<Output, AllowedEffects>
    for Effects<Output, AllowedEffects>
where
    Output: StageFactSet,
    AllowedEffects: EffectSet,
{
    async fn reserve_stock<EffectAt, OutcomeProof>(
        &mut self,
        order_id: OrderId,
        sku: Sku,
    ) -> Result<StockReserved, EffectError>
    where
        ReserveStock: AllowedEffectsAllowEffect<AllowedEffects, EffectAt>
            + EffectOutcomeFitsOutput<Output, OutcomeProof>,
    {
        let binding = self.project_named_effect::<ReserveStock, EffectAt>()?;
        self.perform::<ReserveStock, EffectAt, OutcomeProof>(ReserveStock::new(
            order_id, sku, binding,
        ))
        .await
    }

    async fn release_stock<EffectAt, OutcomeProof>(
        &mut self,
        order_id: OrderId,
        sku: Sku,
        hold_id: HoldId,
    ) -> Result<StockReleased, EffectError>
    where
        ReleaseStock: AllowedEffectsAllowEffect<AllowedEffects, EffectAt>
            + EffectOutcomeFitsOutput<Output, OutcomeProof>,
    {
        let binding = self.project_named_effect::<ReleaseStock, EffectAt>()?;
        self.perform::<ReleaseStock, EffectAt, OutcomeProof>(ReleaseStock::new(
            order_id, sku, hold_id, binding,
        ))
        .await
    }
}
