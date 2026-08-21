// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![cfg(feature = "test-support")]

use async_trait::async_trait;
use obzenflow_adapters::sinks::csv::testing::CsvTestProbe;
use obzenflow_adapters::sinks::CsvSink;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::sink::SinkWriteFailureDisposition;
use obzenflow_runtime::testing::sink::{
    run_writer_conformance, SinkBuildCase, SinkConformanceProfile, SinkDiagnosticSample,
    SinkDiagnosticSurface, SinkExternalCallSnapshot, SinkFault, SinkFaultCase, SinkFixtureError,
    SinkFixtureInputs, SinkSettlementMode, SinkWriterConformanceFixture,
    SINK_CONFORMANCE_PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tempfile::TempDir;

#[derive(Debug, Serialize, Deserialize)]
struct Row {
    id: u64,
    value: String,
}

impl TypedPayload for Row {
    const EVENT_TYPE: &'static str = "sink.conformance.csv.row";
}

struct CsvFixture {
    _temp: TempDir,
    path: PathBuf,
    probe: CsvTestProbe,
}

#[async_trait]
impl SinkWriterConformanceFixture for CsvFixture {
    type Connector = CsvSink<Row>;
    type DestinationSnapshot = Vec<u8>;

    fn profile(&self) -> SinkConformanceProfile {
        SinkConformanceProfile::new(
            SINK_CONFORMANCE_PROTOCOL_VERSION,
            SinkSettlementMode::Buffered { batch_size: 2 },
        )
        .with_fault(SinkFaultCase::operation(SinkFault::Open))
        .with_fault(SinkFaultCase::write(
            SinkFault::Encode,
            SinkWriteFailureDisposition::CurrentOnly,
        ))
        .with_fault(SinkFaultCase::write(
            SinkFault::MidBatchMutation,
            SinkWriteFailureDisposition::Poisoned,
        ))
        .with_fault(SinkFaultCase::write(
            SinkFault::PreCommit,
            SinkWriteFailureDisposition::Poisoned,
        ))
        .with_fault(SinkFaultCase::operation(SinkFault::Flush))
        .with_fault(SinkFaultCase::operation(SinkFault::Drain))
    }

    fn build_cases(&self) -> Vec<SinkBuildCase<Self::Connector>> {
        let valid_path = self.path.clone();
        let invalid_path = self.path.clone();
        let probe = self.probe.clone();
        vec![
            SinkBuildCase::valid("csv-valid", move || {
                CsvSink::<Row>::builder()
                    .path(&valid_path)
                    .columns(["id", "value"])
                    .buffer_size(2)
                    .auto_flush(false)
                    .test_probe(probe.clone())
                    .build()
                    .map_err(|error| SinkFixtureError::new(error.to_string()))
            }),
            SinkBuildCase::invalid("csv-zero-buffer", move || {
                CsvSink::<Row>::builder()
                    .path(&invalid_path)
                    .buffer_size(0)
                    .build()
                    .map_err(|error| SinkFixtureError::new(error.to_string()))
            }),
        ]
    }

    fn fresh_inputs(&mut self) -> Result<SinkFixtureInputs<Row>, SinkFixtureError> {
        Ok(SinkFixtureInputs::new((0..12).map(|id| Row {
            id,
            value: format!("value-{id}"),
        })))
    }

    async fn reset_destination(&mut self) -> Result<(), SinkFixtureError> {
        self.probe.clear();
        std::fs::write(&self.path, []).map_err(|error| SinkFixtureError::new(error.to_string()))
    }

    async fn arm_fault(&mut self, fault: SinkFault) -> Result<(), SinkFixtureError> {
        self.probe.arm(fault);
        Ok(())
    }

    async fn destination_snapshot(&self) -> Result<Vec<u8>, SinkFixtureError> {
        std::fs::read(&self.path).map_err(|error| SinkFixtureError::new(error.to_string()))
    }

    fn external_calls(&self) -> Result<SinkExternalCallSnapshot, SinkFixtureError> {
        Ok(self.probe.snapshot())
    }

    fn diagnostic_samples(&self) -> Result<Vec<SinkDiagnosticSample>, SinkFixtureError> {
        Ok(vec![SinkDiagnosticSample::new(
            SinkDiagnosticSurface::Debug,
            format!("CsvSink(path={})", self.path.display()),
        )])
    }
}

#[tokio::test]
async fn csv_passes_the_writer_protocol_and_lifecycle_control_suite() {
    let temp = tempfile::tempdir().expect("temporary conformance directory");
    let path = temp.path().join("rows.csv");
    let mut fixture = CsvFixture {
        _temp: temp,
        path,
        probe: CsvTestProbe::default(),
    };
    let report = run_writer_conformance(&mut fixture)
        .await
        .expect("CSV conforms to the buffered writer protocol");
    assert_eq!(report.protocol_version(), SINK_CONFORMANCE_PROTOCOL_VERSION);
}
