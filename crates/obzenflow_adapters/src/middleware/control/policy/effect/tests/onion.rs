// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Onion order and structural finalization for neutral effect policies.

use super::support::*;

struct RecordingPolicy {
    observed: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl EffectPolicy for RecordingPolicy {
    fn label(&self) -> &'static str {
        "test.recording"
    }

    async fn admit(&self, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
        PolicyAdmission::Admit
    }

    fn observe(&self, attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {
        let value = match attempt {
            EffectAttemptOutcome::Executed(Ok(_)) => "executed_ok".to_string(),
            EffectAttemptOutcome::Executed(Err(_)) => "executed_err".to_string(),
            EffectAttemptOutcome::RejectedBy(cause) => {
                format!("rejected_by:{}", cause.source)
            }
        };
        self.observed.lock().unwrap().push(value);
    }
}

struct RejectingPolicy;

#[async_trait]
impl EffectPolicy for RejectingPolicy {
    fn label(&self) -> &'static str {
        "test.rejecting"
    }

    async fn admit(&self, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
        PolicyAdmission::Reject(Box::new(MiddlewareAbortCause {
            source: EffectFailureSource::new("test.rejecting"),
            code: EffectFailureCode::new("rejected"),
            message: "test rejection".to_string(),
            retry: RetryDisposition::NotRetryable,
            event: None,
        }))
    }

    fn observe(&self, _attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {}
}

#[tokio::test]
async fn admitted_policies_observe_rejection_by_later_policy() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let chain = Arc::new(vec![
        EffectPolicyAttachment::neutral(Arc::new(RecordingPolicy {
            observed: observed.clone(),
        })),
        EffectPolicyAttachment::neutral(Arc::new(RejectingPolicy)),
    ]);
    let mut chains = HashMap::new();
    chains.insert("effect.a", chain);
    let boundary = PerEffectPolicyBoundary::new(chains);

    let report = boundary
        .around_repeatable_effect(&identity_for("effect.a"), &data_event(), ok_execute())
        .await;

    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Aborted(ref reason)
            if reason.cause.source == "test.rejecting"
    ));
    assert_eq!(
        observed.lock().unwrap().as_slice(),
        ["rejected_by:test.rejecting"]
    );
}
