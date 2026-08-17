// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::BoundedBindingEvidence;
use obzenflow_runtime::effects::{
    Effect, EffectBinding, EffectBindingEvidence, EffectBindingUse, EffectContext, EffectError,
    EffectPortRegistry, EffectPortResolutionError, EffectPortResolver, EffectPortSlot,
    EffectPortSlotSet, EffectRegistration, EffectRegistrationBuilder, EffectSafety,
    LogicalEffectBindingName, Named, NamedEffect, RecordedReply,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

trait LocalClient: Send + Sync {
    fn answer(&self) -> u64;
}

struct FakeClient;

impl LocalClient for FakeClient {
    fn answer(&self) -> u64 {
        42
    }
}

const CLIENT: EffectPortSlot<dyn LocalClient> = EffectPortSlot::new("client");

#[derive(Clone, Debug, PartialEq, Eq)]
struct LocalEvidence;

impl EffectBindingEvidence for LocalEvidence {
    const SCHEMA_VERSION: u32 = 1;

    fn canonical_bytes(&self) -> BoundedBindingEvidence {
        BoundedBindingEvidence::try_new(b"local-provider-v1".to_vec()).unwrap()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LocalReply(u64);

#[derive(Clone, Debug)]
struct LocalEffect {
    binding: EffectBindingUse<Self>,
}

#[async_trait]
impl Effect for LocalEffect {
    const EFFECT_TYPE: &'static str = "trybuild.local.effect";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type BindingMode = Named<LocalEvidence>;
    type Outcome = LocalReply;
    type OutcomeSemantics = RecordedReply;

    fn label(&self) -> &str {
        "local"
    }

    fn canonical_input(&self) -> serde_json::Value {
        serde_json::Value::Null
    }

    async fn execute(&self, context: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        Ok(LocalReply(context.port(CLIENT)?.answer()))
    }
}

impl NamedEffect for LocalEffect {
    type BindingEvidence = LocalEvidence;

    fn binding_use(&self) -> &EffectBindingUse<Self> {
        &self.binding
    }

    fn required_slots() -> EffectPortSlotSet {
        EffectPortSlotSet::single(CLIENT)
    }
}

fn application_local_provider() -> (EffectBinding<LocalEffect>, EffectRegistration<LocalEffect>) {
    EffectRegistrationBuilder::<LocalEffect>::new(
        LogicalEffectBindingName::new("local").unwrap(),
        LocalEvidence,
    )
    .bind_eager(CLIENT, Arc::new(FakeClient) as Arc<dyn LocalClient>)
    .unwrap()
    .finish()
    .unwrap()
}

fn outward_facade() -> (EffectBinding<LocalEffect>, EffectRegistration<LocalEffect>) {
    let resolver: EffectPortResolver<dyn LocalClient> = Arc::new(|| {
        Ok::<Arc<dyn LocalClient>, EffectPortResolutionError>(Arc::new(FakeClient))
    });
    EffectRegistrationBuilder::<LocalEffect>::new(
        LogicalEffectBindingName::new("facade").unwrap(),
        LocalEvidence,
    )
    .bind_deferred(CLIENT, resolver)
    .unwrap()
    .finish()
    .unwrap()
}

fn test_fixture() -> (EffectBinding<LocalEffect>, EffectRegistration<LocalEffect>) {
    EffectRegistrationBuilder::<LocalEffect>::new(
        LogicalEffectBindingName::new("fixture").unwrap(),
        LocalEvidence,
    )
    .bind_eager(CLIENT, Arc::new(FakeClient) as Arc<dyn LocalClient>)
    .unwrap()
    .finish()
    .unwrap()
}

fn main() {
    for (_binding, registration) in [
        application_local_provider(),
        outward_facade(),
        test_fixture(),
    ] {
        let mut registry = EffectPortRegistry::new();
        registry.install(registration).unwrap();
    }
}
