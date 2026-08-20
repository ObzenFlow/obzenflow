// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Packaged downstream-author witness for the public writer conformance kit.

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::sink::{
    SinkConnector, SinkDescription, SinkOperationResult, SinkTerminalOutcome, SinkWriteContext,
    SinkWriteReport, SinkWriteResult, SinkWriter, SinkWriterInitContext, SinkWriterLifecycleReport,
};
use obzenflow_runtime::testing::sink::{
    run_writer_conformance, SinkBuildCase, SinkConformanceProfile, SinkDiagnosticSample,
    SinkDiagnosticSurface, SinkExternalCall, SinkExternalCallKind, SinkExternalCallSnapshot,
    SinkFault, SinkFixtureError, SinkFixtureInputs, SinkSettlementMode,
    SinkWriterConformanceFixture, SINK_CONFORMANCE_PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Serialize, Deserialize)]
struct PublicInput(u64);

impl TypedPayload for PublicInput {
    const EVENT_TYPE: &'static str = "flowip_122a.public_author.input";
}

#[derive(Default)]
struct Shared {
    destination: AtomicUsize,
    next_writer: AtomicU64,
    next_sequence: AtomicU64,
    calls: Mutex<Vec<SinkExternalCall>>,
}

impl Shared {
    fn record(&self, writer: u64, kind: SinkExternalCallKind) {
        let sequence = self.next_sequence.fetch_add(1, Ordering::SeqCst);
        self.calls
            .lock()
            .expect("call log")
            .push(SinkExternalCall::new(writer, sequence, kind));
    }
}

struct PublicConnector(Arc<Shared>);
struct PublicWriter {
    shared: Arc<Shared>,
    writer: u64,
}

impl Drop for PublicWriter {
    fn drop(&mut self) {
        self.shared.record(self.writer, SinkExternalCallKind::Drop);
    }
}

#[async_trait]
impl SinkConnector for PublicConnector {
    type Input = PublicInput;
    type Writer = PublicWriter;

    fn describe(&self) -> SinkDescription {
        SinkDescription::destination("public-author", DeliveryMethod::Noop)
    }

    async fn open(&self, _context: SinkWriterInitContext) -> SinkOperationResult<Self::Writer> {
        let writer = self.0.next_writer.fetch_add(1, Ordering::SeqCst);
        self.0.record(writer, SinkExternalCallKind::Open);
        Ok(PublicWriter {
            shared: Arc::clone(&self.0),
            writer,
        })
    }
}

#[async_trait]
impl SinkWriter for PublicWriter {
    type Input = PublicInput;

    async fn write(&mut self, _input: Self::Input, _context: SinkWriteContext) -> SinkWriteResult {
        self.shared.record(self.writer, SinkExternalCallKind::Write);
        self.shared.destination.fetch_add(1, Ordering::SeqCst);
        Ok(SinkWriteReport::terminal(
            SinkTerminalOutcome::success(None).with_items(1),
        ))
    }

    async fn flush(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
        self.shared.record(self.writer, SinkExternalCallKind::Flush);
        Ok(SinkWriterLifecycleReport::default())
    }

    async fn drain(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
        self.shared.record(self.writer, SinkExternalCallKind::Drain);
        Ok(SinkWriterLifecycleReport::default())
    }
}

struct PublicFixture(Arc<Shared>);

#[async_trait]
impl SinkWriterConformanceFixture for PublicFixture {
    type Connector = PublicConnector;
    type DestinationSnapshot = usize;

    fn profile(&self) -> SinkConformanceProfile {
        SinkConformanceProfile::new(
            SINK_CONFORMANCE_PROTOCOL_VERSION,
            SinkSettlementMode::Terminal,
        )
    }

    fn build_cases(&self) -> Vec<SinkBuildCase<Self::Connector>> {
        let shared = Arc::clone(&self.0);
        vec![
            SinkBuildCase::valid("public-valid", move || {
                Ok(PublicConnector(Arc::clone(&shared)))
            }),
            SinkBuildCase::invalid("public-invalid", || {
                Err(SinkFixtureError::new("missing public destination"))
            }),
        ]
    }

    fn fresh_inputs(&mut self) -> Result<SinkFixtureInputs<PublicInput>, SinkFixtureError> {
        Ok(SinkFixtureInputs::new((0..8).map(PublicInput)))
    }

    async fn reset_destination(&mut self) -> Result<(), SinkFixtureError> {
        self.0.destination.store(0, Ordering::SeqCst);
        Ok(())
    }

    async fn arm_fault(&mut self, _fault: SinkFault) -> Result<(), SinkFixtureError> {
        Err(SinkFixtureError::new("public fixture declares no faults"))
    }

    async fn destination_snapshot(&self) -> Result<usize, SinkFixtureError> {
        Ok(self.0.destination.load(Ordering::SeqCst))
    }

    fn external_calls(&self) -> Result<SinkExternalCallSnapshot, SinkFixtureError> {
        Ok(SinkExternalCallSnapshot::new(
            self.0.calls.lock().expect("call log").clone(),
        ))
    }

    fn diagnostic_samples(&self) -> Result<Vec<SinkDiagnosticSample>, SinkFixtureError> {
        Ok(vec![SinkDiagnosticSample::new(
            SinkDiagnosticSurface::Debug,
            "public connector redacted",
        )])
    }
}

#[tokio::test]
async fn public_author_can_pass_without_private_conformance_construction() {
    let mut fixture = PublicFixture(Arc::new(Shared::default()));
    let report = run_writer_conformance(&mut fixture)
        .await
        .expect("public-author connector conforms");
    assert_eq!(report.protocol_version(), SINK_CONFORMANCE_PROTOCOL_VERSION);
}
