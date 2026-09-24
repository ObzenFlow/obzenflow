// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::journal::read::open_disk_run;
use crate::web::run_control::*;
use async_trait::async_trait;
use obzenflow_core::web::{
    EndpointError, HttpEndpoint, HttpMethod, ManagedResponse, Request, Response,
};
use obzenflow_runtime::pipeline::FlowHandle;
use std::path::PathBuf;

pub(crate) struct RunDiscoveryEndpoint {
    target: RunControlTarget,
    path: Option<Result<PathBuf, String>>,
}

impl RunDiscoveryEndpoint {
    pub(crate) fn new(handle: &FlowHandle, target: RunControlTarget) -> Self {
        Self {
            target,
            path: handle.run_substrate().locator().map(|locator| {
                std::path::absolute(locator.path()).map_err(|error| error.to_string())
            }),
        }
    }
}

#[async_trait]
impl HttpEndpoint for RunDiscoveryEndpoint {
    fn path(&self) -> &str {
        RUN_DISCOVERY_PATH
    }
    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Get]
    }
    async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
        let archive = match &self.path {
            None => RunArchive::Unavailable {
                reason: ArchiveUnavailableReason::Ephemeral,
            },
            Some(path) => {
                let admission = async {
                    let path = path.as_ref().map_err(Clone::clone)?;
                    let snapshot = open_disk_run(path).await.map_err(|e| e.to_string())?;
                    if snapshot.identity().pipeline_writer_id != self.target.pipeline_writer_id {
                        return Err("archive pipeline writer does not match host".to_string());
                    }
                    Ok(RunArchive::LocalDisk {
                        flow_id: snapshot.identity().flow_id.to_string(),
                        path: NativeRunPath::encode(path)?,
                    })
                }
                .await;
                match admission {
                    Ok(archive) => archive,
                    Err(error) => {
                        tracing::warn!(runtime_instance_id = %self.target.runtime_instance_id, pipeline_writer_id = %self.target.pipeline_writer_id, %error, "run_archive_admission_failed");
                        RunArchive::Unavailable {
                            reason: ArchiveUnavailableReason::Unreadable,
                        }
                    }
                }
            }
        };
        Response::ok()
            .with_header("Cache-Control".into(), "no-store".into())
            .with_json(&CurrentRunDiscovery {
                protocol_version: RUN_CONTROL_PROTOCOL_VERSION,
                target: self.target.clone(),
                archive,
            })
            .map(Into::into)
            .map_err(|error| EndpointError::with_source("Serialising run discovery", error))
    }
}
