//! Global registry for circuit breaker states by stage
//!
//! This module provides a simple, process-local registry that allows
//! middleware (in the adapters crate) and runtime strategies (in the
//! runtime_services crate) to share the current circuit breaker state
//! for a given stage without introducing cyclic crate dependencies.
//!
//! It is intentionally minimal: a StageId-keyed map of Arc<AtomicU8>,
//! where the u8 value encodes the breaker state (as defined by the
//! circuit breaker middleware).

use crate::id::StageId;
use std::collections::HashMap;
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, RwLock};

use once_cell::sync::OnceCell;

/// Global registry instance
fn registry() -> &'static RwLock<HashMap<StageId, Arc<AtomicU8>>> {
    static REGISTRY: OnceCell<RwLock<HashMap<StageId, Arc<AtomicU8>>>> = OnceCell::new();
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Register or update the breaker state handle for a stage
pub fn register_stage_state(stage_id: StageId, state: Arc<AtomicU8>) {
    let mut reg = registry()
        .write()
        .expect("circuit_breaker_registry: poisoned write lock");
    reg.insert(stage_id, state);
}

/// Get a cloned handle to the breaker state for a stage, if registered
pub fn get_stage_state(stage_id: &StageId) -> Option<Arc<AtomicU8>> {
    let reg = registry()
        .read()
        .expect("circuit_breaker_registry: poisoned read lock");
    reg.get(stage_id).cloned()
}

