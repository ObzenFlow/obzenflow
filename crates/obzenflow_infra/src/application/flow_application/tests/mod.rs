// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::journal::disk_journals;
use async_trait::async_trait;
use futures::future::BoxFuture;
use obzenflow_adapters::sources::ValuesSource;
use obzenflow_dsl::{flow, sink, source};
use obzenflow_runtime::events::{source_channel, Events, IngressDecoder};
use obzenflow_runtime::pipeline::PipelineState;
use obzenflow_runtime::queries::{FiniteRead, QueryError};
use obzenflow_runtime::stages::common::handlers::source::{Continuous, SourceRead};
use obzenflow_runtime::stages::common::handlers::{
    TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};
use obzenflow_runtime::systems::Port;
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::oneshot;

#[cfg(all(feature = "tokio-console", tokio_unstable))]
mod console_telemetry;
mod lifecycle;
#[cfg(feature = "prometheus")]
mod prometheus;
mod startup;

#[cfg(unix)]
#[tokio::test]
async fn verify_quoted_paths_preserve_arguments() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    use std::process::Command;

    let dir = tempfile::tempdir().unwrap();
    let baseline = dir.path().join("Pat's Payment Runs/base");
    let candidate = dir
        .path()
        .join(OsStr::from_bytes(b"Pat's Payment Runs/replay-\xff"));
    let error =
        FlowApplication::run_post_replay_verification(baseline.clone(), Some(candidate.clone()))
            .await
            .unwrap_err();
    let ApplicationError::Verification {
        exit_code: 3,
        summary,
    } = error
    else {
        panic!("missing baseline must refuse verification: {error}");
    };
    let (_, operands) = summary.split_once("then run obzenflow verify ").unwrap();
    let output = Command::new("sh")
        .args(["-c", &format!(r"printf '%s\0' {operands}")])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let args: Vec<_> = output
        .stdout
        .strip_suffix(&[0])
        .unwrap()
        .split(|byte| *byte == 0)
        .collect();
    assert_eq!(
        args,
        [
            b"--baseline".as_slice(),
            baseline.as_os_str().as_bytes(),
            b"--candidate",
            candidate.as_os_str().as_bytes()
        ]
    );
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct IdlePayload;

impl TypedPayload for IdlePayload {
    const EVENT_TYPE: &'static str = "flow_application.idle";
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct NextInput;

#[obzenflow_runtime::effects::port]
#[async_trait]
trait TestInput {
    #[query(type = "flow_application.input.read")]
    async fn read(
        &self,
        from: &NextInput,
        ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<IdlePayload, NextInput>, obzenflow_runtime::queries::QueryError>;
}

type Answer = Result<FiniteRead<IdlePayload, NextInput>, QueryError>;
type Answering = Arc<dyn Fn() -> BoxFuture<'static, Answer> + Send + Sync>;

struct ScriptedInput {
    answer: Answering,
}

#[async_trait]
impl TestInput for ScriptedInput {
    async fn read(
        &self,
        _from: &NextInput,
        _ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Answer {
        (self.answer)().await
    }
}

struct FiniteInput(Port<dyn TestInput>);

impl FiniteInput {
    fn new(ends: bool, reads: Arc<AtomicUsize>) -> Self {
        Self::answering(move || {
            reads.fetch_add(1, Ordering::SeqCst);
            if ends {
                Box::pin(async { Ok(FiniteRead::EndOfInput) })
            } else {
                Box::pin(std::future::pending())
            }
        })
    }

    fn waiting() -> Self {
        Self::new(false, Arc::default())
    }

    fn answering(answer: impl Fn() -> BoxFuture<'static, Answer> + Send + Sync + 'static) -> Self {
        Self(Port::to(ScriptedInput {
            answer: Arc::new(answer),
        }))
    }
}

impl TypedFiniteSourceHandler for FiniteInput {
    type Output = IdlePayload;

    fn read(&self) -> SourceRead<IdlePayload> {
        SourceRead::finite(self.0.clone(), NextInput, Ok)
    }
}

struct IdleInfiniteSource {
    port: Port<dyn Events>,
}

#[derive(Clone)]
struct IdleIngress;

impl IngressDecoder for IdleIngress {
    type Output = IdlePayload;
}

impl IdleInfiniteSource {
    fn new() -> Self {
        let (sender, receiver) = source_channel::<IdlePayload>(1);
        std::mem::forget(sender);
        Self {
            port: Port::to(receiver),
        }
    }
}

impl TypedInfiniteSourceHandler for IdleInfiniteSource {
    type Output = IdlePayload;

    fn read(&self) -> SourceRead<IdlePayload, Continuous> {
        SourceRead::events(self.port.clone(), IdleIngress)
    }
}

fn one_shot() -> ValuesSource<[IdlePayload; 1]> {
    ValuesSource::new([IdlePayload])
}

fn noop_sink() -> obzenflow_adapters::sinks::DiscardSink<IdlePayload> {
    obzenflow_adapters::sinks::DiscardSink::new()
}
