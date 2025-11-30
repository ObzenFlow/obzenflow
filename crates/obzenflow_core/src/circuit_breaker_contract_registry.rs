//! Registry for circuit breaker contract modes per stage.
//!
//! This is a small, process-local helper that lets middleware (in the
//! adapters crate) annotate stages with a `CircuitBreakerContractMode`
//! without introducing dependencies from core contract types back up
//! into middleware or runtime code.

use crate::id::StageId;
use std::collections::HashMap;
use std::sync::RwLock;

use once_cell::sync::OnceCell;

/// How contract policies should interpret circuit breaker activity for a stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitBreakerContractMode {
    /// Current behaviour; any SeqDivergence remains a violation.
    Strict,
    /// Allow breaker-driven truncation + fallback to be treated as pass when
    /// policies have sufficient evidence that this was intentional.
    BreakerAware,
}

/// Aggregated contract-related hints for a breaker-protected stage.
#[derive(Debug, Clone, Copy)]
pub struct CircuitBreakerContractInfo {
    pub mode: CircuitBreakerContractMode,
    /// Whether this breaker has ever transitioned Closed → Open since registration.
    pub has_opened_since_registration: bool,
    /// Whether a fallback is configured for this stage (raw or typed).
    pub has_fallback_configured: bool,
}

fn registry() -> &'static RwLock<HashMap<StageId, CircuitBreakerContractInfo>> {
    static REGISTRY: OnceCell<RwLock<HashMap<StageId, CircuitBreakerContractInfo>>> =
        OnceCell::new();
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Register or update the breaker contract info for a stage.
pub fn register_stage_mode(
    stage_id: StageId,
    mode: CircuitBreakerContractMode,
    has_fallback_configured: bool,
) {
    let mut reg = registry()
        .write()
        .expect("circuit_breaker_contract_registry: poisoned write lock");
    let entry = reg.entry(stage_id).or_insert(CircuitBreakerContractInfo {
        mode,
        has_opened_since_registration: false,
        has_fallback_configured,
    });
    // Update mode and fallback flag if re-registering.
    entry.mode = mode;
    entry.has_fallback_configured = has_fallback_configured;
}

/// Mark that the breaker for this stage has transitioned Closed → Open at least once.
pub fn mark_stage_opened(stage_id: StageId) {
    let mut reg = registry()
        .write()
        .expect("circuit_breaker_contract_registry: poisoned write lock");
    if let Some(info) = reg.get_mut(&stage_id) {
        info.has_opened_since_registration = true;
    }
}

/// Get the full contract info for a stage, if registered.
pub fn get_stage_info(stage_id: &StageId) -> Option<CircuitBreakerContractInfo> {
    let reg = registry()
        .read()
        .expect("circuit_breaker_contract_registry: poisoned read lock");
    reg.get(stage_id).cloned()
}

/// Get the breaker contract mode for a stage, if registered.
pub fn get_stage_mode(stage_id: &StageId) -> Option<CircuitBreakerContractMode> {
    get_stage_info(stage_id).map(|info| info.mode)
}
