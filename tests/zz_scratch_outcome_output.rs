use async_trait::async_trait;
use obzenflow::effects::{
    Call, CallRequest, EffectContext, EffectError, EffectHandler, Effects, Idempotent,
    StageCompletion, System,
};
use obzenflow::error::HandlerError;
use obzenflow::schema::{EffectOutcomeFacts, TypedPayload};
use obzenflow::stages::transforms::EffectfulTransformHandler;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Requested {
    id: u64,
}
impl TypedPayload for Requested {
    const EVENT_TYPE: &'static str = "scratch.requested";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Loaded {
    id: u64,
}
impl TypedPayload for Loaded {
    const EVENT_TYPE: &'static str = "scratch.loaded";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NotFound {
    id: u64,
}
impl TypedPayload for NotFound {
    const EVENT_TYPE: &'static str = "scratch.not_found";
}

#[obzenflow::port]
#[async_trait]
pub trait TelemetryPort: Send + Sync {
    async fn load(&self, request: &EvidenceRequest) -> Result<EvidenceOutcome, EffectError>;
}

#[derive(Clone, Debug, Serialize)]
pub struct EvidenceRequest {
    id: u64,
}

pub type LoadEvidence = Call<EvidenceRequest>;

#[derive(Debug, Clone, EffectOutcomeFacts)]
#[effect_outcome(schema = obzenflow::schema)]
pub enum EvidenceOutcome {
    Loaded(Loaded),
    NotFound(NotFound),
}

#[async_trait]
impl CallRequest for EvidenceRequest {
    const EFFECT_TYPE: &'static str = "scratch.load_evidence";
    type Safety = Idempotent;
    type Port = dyn TelemetryPort;
    type Outcome = EvidenceOutcome;

    async fn call(
        &self,
        telemetry: &dyn TelemetryPort,
        _: &mut EffectContext,
    ) -> Result<EvidenceOutcome, EffectError> {
        telemetry.load(self).await
    }
}

#[derive(Debug, Clone, EffectHandler)]
pub struct EvidenceLoader {
    #[system]
    telemetry: System<dyn TelemetryPort>,
}

#[async_trait]
impl EffectfulTransformHandler for EvidenceLoader {
    type Input = Requested;
    type Output = EvidenceOutcome;
    type AllowedEffects = obzenflow::effects::effect_set![LoadEvidence];

    async fn process(
        &self,
        request: Requested,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        fx.call(EvidenceRequest { id: request.id }).await?;
        Ok(fx.complete()?)
    }
}
