// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{publish_acknowledgements, EdgeState};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

#[derive(Debug)]
pub(super) struct BackpressureAckGateState {
    key: (String, String),
    counts: Mutex<Counts>,
    edge: Mutex<Option<Weak<EdgeState>>>,
    changed: tokio::sync::Notify,
}

#[derive(Debug)]
struct Counts {
    pass_remaining: u64,
    withheld: u64,
    open: bool,
}

/// Test-only control for withholding downstream acknowledgements on one
/// named physical edge.
#[derive(Debug)]
pub struct BackpressureAckGate {
    state: Arc<BackpressureAckGateState>,
}

impl BackpressureAckGate {
    pub fn install(
        upstream_stage: impl Into<String>,
        downstream_stage: impl Into<String>,
        pass_through_acks: u64,
    ) -> Result<Self, String> {
        let key = (upstream_stage.into(), downstream_stage.into());
        let state = Arc::new(BackpressureAckGateState {
            key: key.clone(),
            counts: Mutex::new(Counts {
                pass_remaining: pass_through_acks,
                withheld: 0,
                open: false,
            }),
            edge: Mutex::new(None),
            changed: tokio::sync::Notify::new(),
        });
        let mut gates = gates()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if gates.get(&key).and_then(Weak::upgrade).is_some() {
            return Err(format!(
                "a backpressure acknowledgement gate is already installed for '{} -> {}'",
                key.0, key.1
            ));
        }
        gates.insert(key, Arc::downgrade(&state));
        Ok(Self { state })
    }

    pub fn withheld(&self) -> u64 {
        self.state
            .counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .withheld
    }

    pub async fn wait_for_withheld(&self, minimum: u64) {
        loop {
            let changed = self.state.changed.notified();
            if self.withheld() >= minimum {
                return;
            }
            changed.await;
        }
    }

    pub fn release(&self, acknowledgements: u64) -> Result<(), String> {
        {
            let mut counts = self
                .state
                .counts
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if acknowledgements > counts.withheld {
                return Err(format!(
                    "cannot release {acknowledgements} acknowledgements; only {} are withheld",
                    counts.withheld
                ));
            }
            counts.withheld -= acknowledgements;
        }
        self.state.publish(acknowledgements);
        self.state.changed.notify_waiters();
        Ok(())
    }

    /// Release all held acknowledgements and pass future acknowledgements
    /// through. The gate remains registered until dropped.
    pub fn open(&self) {
        let released = {
            let mut counts = self
                .state
                .counts
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            counts.open = true;
            let released = counts.withheld;
            counts.withheld = 0;
            released
        };
        self.state.publish(released);
        self.state.changed.notify_waiters();
    }
}

impl Drop for BackpressureAckGate {
    fn drop(&mut self) {
        self.open();
        let mut gates = gates()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if gates
            .get(&self.state.key)
            .and_then(Weak::upgrade)
            .is_some_and(|registered| Arc::ptr_eq(&registered, &self.state))
        {
            gates.remove(&self.state.key);
        }
    }
}

impl BackpressureAckGateState {
    pub(super) fn attach(&self, edge: &Arc<EdgeState>) {
        *self
            .edge
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::downgrade(edge));
    }

    pub(super) fn partition(&self, acknowledgements: u64) -> u64 {
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if counts.open {
            return acknowledgements;
        }
        let immediate = acknowledgements.min(counts.pass_remaining);
        counts.pass_remaining -= immediate;
        counts.withheld = counts.withheld.saturating_add(acknowledgements - immediate);
        drop(counts);
        self.changed.notify_waiters();
        immediate
    }

    fn publish(&self, acknowledgements: u64) {
        if acknowledgements == 0 {
            return;
        }
        let edge = self
            .edge
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .and_then(Weak::upgrade);
        if let Some(edge) = edge {
            publish_acknowledgements(&edge, acknowledgements);
        }
    }
}

pub(super) fn gate_for(upstream: &str, downstream: &str) -> Option<Arc<BackpressureAckGateState>> {
    gates()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&(upstream.to_string(), downstream.to_string()))
        .and_then(Weak::upgrade)
}

fn gates() -> &'static Mutex<HashMap<(String, String), Weak<BackpressureAckGateState>>> {
    static GATES: OnceLock<Mutex<HashMap<(String, String), Weak<BackpressureAckGateState>>>> =
        OnceLock::new();
    GATES.get_or_init(|| Mutex::new(HashMap::new()))
}
