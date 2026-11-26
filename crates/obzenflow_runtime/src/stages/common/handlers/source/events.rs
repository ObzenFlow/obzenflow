// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::hosted::{receipt_key, HostedAdmission, HostedFact, HostedMember, ReceiptKey, Staged};
use super::read::{Admission, ReadEnd, Reads, SourceOutput};
use super::{SourceContinuation, SourceError};
use crate::commands::StageEffectsError;
use crate::effects::{EffectDeclaration, EffectError, EffectsCore};
use crate::events::{
    prove_journal_form, ArrivalKind, Events, IngressDecoder, IngressReceiptSender, IngressRecord,
    LineDecoder,
};
use crate::systems::Port;
use async_trait::async_trait;
use obzenflow_core::event::payloads::execution_payload::ReattachGuarantee;
use obzenflow_core::event::SourceDiagnosticReason;
use obzenflow_core::ingress::HostedIngressBindingSlot;
use obzenflow_core::EventId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RecordedEvent {
    Body(serde_json::Value),
    Admitted(HostedMember),
}

enum Pending<Output> {
    Value(Output),
    Rejected(SourceError),
    Admitted(Box<Staged>),
}

struct Hosted {
    slot: HostedIngressBindingSlot,
    admission: HostedAdmission,
    receipts: Mutex<HashMap<ReceiptKey, IngressReceiptSender>>,
}

pub(super) struct EventReads<D: IngressDecoder> {
    port: Port<dyn Events>,
    system: OnceLock<Arc<dyn Events>>,
    decoder: D,
    hosted: Option<Hosted>,
    pending: VecDeque<Pending<D::Output>>,
}

impl<D: IngressDecoder> EventReads<D> {
    pub(super) fn new(port: Port<dyn Events>, decoder: D) -> Self {
        let mut hosted = None;
        let system = match port.eager_system() {
            Some(eager) => {
                if let Some(slot) = eager.hosted_ingress() {
                    slot.install_decoder(Arc::new(LineDecoder(decoder.clone())));
                    hosted = Some(Hosted {
                        admission: HostedAdmission::new(slot.clone()),
                        slot,
                        receipts: Mutex::new(HashMap::new()),
                    });
                }
                OnceLock::from(eager)
            }
            None => OnceLock::new(),
        };
        Self {
            port,
            system,
            decoder,
            hosted,
            pending: VecDeque::new(),
        }
    }

    pub(super) fn continuation(&self) -> SourceContinuation {
        match self.hosted {
            Some(_) => HostedAdmission::continuation(),
            None => SourceContinuation::Reattach {
                guarantee: ReattachGuarantee::AtMostOnce,
            },
        }
    }

    fn system(&self) -> Result<Arc<dyn Events>, SourceError> {
        if let Some(system) = self.system.get() {
            return Ok(Arc::clone(system));
        }
        let resolved = self
            .port
            .resolve_system()
            .map_err(|_| SourceError::Other(SourceDiagnosticReason::InputUnavailable.into()))?;
        Ok(Arc::clone(self.system.get_or_init(|| resolved)))
    }
}

#[async_trait]
impl<D: IngressDecoder> Reads<D::Output> for EventReads<D> {
    fn declaration(&self) -> Result<Option<EffectDeclaration>, StageEffectsError> {
        Ok(None)
    }

    fn is_continuous(&self) -> bool {
        true
    }

    fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    async fn ready(&self) -> Result<(), SourceError> {
        if let Some(hosted) = &self.hosted {
            if hosted.admission.pending.is_empty() {
                hosted.slot.set_restoring(false);
            }
        }
        self.system()?.ready().await
    }

    fn next_output(&mut self) -> Option<Result<SourceOutput<D::Output>, SourceError>> {
        Some(match self.pending.pop_front()? {
            Pending::Value(value) => Ok(SourceOutput::Value(value)),
            Pending::Rejected(error) => Err(error),
            Pending::Admitted(staged) => Ok(SourceOutput::Admitted(staged)),
        })
    }

    async fn read(
        &mut self,
        effects: &mut EffectsCore,
        _deadline: Option<Duration>,
    ) -> Result<ReadEnd, EffectError> {
        let live = !effects.is_replaying();
        let system = if live {
            Some(
                self.system()
                    .map_err(|error| EffectError::Execution(error.to_string()))?,
            )
        } else {
            None
        };
        let receipts = self.hosted.as_ref().map(|hosted| &hosted.receipts);
        let recorded = effects
            .record_event(|| async move {
                let arrival = system.and_then(|system| system.take()).ok_or_else(|| {
                    EffectError::Execution("the event system held no event".into())
                })?;
                let recorded = match arrival.kind {
                    ArrivalKind::Body(body) => RecordedEvent::Body(body),
                    ArrivalKind::Admitted(admitted) => {
                        let receipts = receipts.ok_or_else(|| {
                            EffectError::Serialization(
                                "an event system that admits no event delivered an admitted event"
                                    .into(),
                            )
                        })?;
                        let admitted = *admitted;
                        receipts
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .insert(receipt_key(&admitted.context), admitted.receipt);
                        RecordedEvent::Admitted(HostedMember {
                            fact: HostedFact::of(admitted.fact)
                                .map_err(EffectError::Serialization)?,
                            context: admitted.context,
                        })
                    }
                    ArrivalKind::Unrepresentable(error) => {
                        return Err(EffectError::Serialization(error))
                    }
                };
                serde_json::to_value(recorded)
                    .map_err(|error| EffectError::Serialization(error.to_string()))
            })
            .await?;
        let recorded: RecordedEvent = serde_json::from_value(recorded).map_err(|error| {
            EffectError::Serialization(format!("a recorded event cannot be decoded: {error}"))
        })?;
        match recorded {
            RecordedEvent::Body(body) => {
                let decoded = self
                    .decoder
                    .decode(IngressRecord::new(&body))
                    .and_then(prove_journal_form);
                self.pending.push_back(match decoded {
                    Ok(value) => Pending::Value(value),
                    Err(error) => {
                        Pending::Rejected(SourceError::Validation(error.into_diagnostic()))
                    }
                });
            }
            RecordedEvent::Admitted(member) => {
                let Some(Hosted {
                    admission,
                    receipts,
                    ..
                }) = self.hosted.as_mut()
                else {
                    return Err(EffectError::Serialization(
                        "the recording holds an admitted event for a system that admits none"
                            .into(),
                    ));
                };
                if live {
                    if let Some(foreign) = admission.foreign_member(std::slice::from_ref(&member)) {
                        return Err(EffectError::Serialization(foreign));
                    }
                }
                let staged = admission.decide(vec![member], |context| {
                    live.then(|| {
                        receipts
                            .get_mut()
                            .unwrap_or_else(PoisonError::into_inner)
                            .remove(&receipt_key(context))
                    })
                    .flatten()
                });
                self.pending.extend(
                    staged
                        .into_iter()
                        .map(|staged| Pending::Admitted(Box::new(staged))),
                );
            }
        }
        Ok(ReadEnd::Batch)
    }

    fn hosted_ingress_slot(&self) -> Option<HostedIngressBindingSlot> {
        self.hosted.as_ref().map(|hosted| hosted.slot.clone())
    }

    fn holds_receipts(&self) -> bool {
        self.hosted
            .as_ref()
            .is_some_and(|hosted| !hosted.admission.pending.is_empty())
    }

    fn install_context(&mut self, resuming: bool) {
        if let Some(hosted) = &self.hosted {
            hosted.slot.set_restoring(resuming);
        }
    }

    fn crossed_to_live(&self) {
        if let Some(hosted) = &self.hosted {
            hosted.slot.set_restoring(false);
        }
    }

    fn admitted(&mut self, admission: Admission, event: EventId) {
        if let Some(hosted) = self.hosted.as_mut() {
            hosted.admission.admitted(
                admission.identity,
                admission.accepted_at_ns,
                admission.receipts,
                event,
            );
        }
    }

    fn settle_committed(&mut self, committed: &[EventId]) {
        if let Some(hosted) = self.hosted.as_mut() {
            hosted.admission.settle(committed);
        }
    }
}
