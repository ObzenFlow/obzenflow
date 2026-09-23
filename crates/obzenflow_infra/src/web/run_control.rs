// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Versioned host discovery and conditional control. These are outer protocol
//! values, never runtime commands, reader ports or persisted execution truth.

use super::endpoints::flow_control::{FlowControlRequest, FlowControlResponse};
use super::RuntimeInstanceId;
use base64::Engine;
use obzenflow_core::event::WriterId;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const RUN_CONTROL_PROTOCOL_VERSION: u16 = 1;
pub const RUN_DISCOVERY_PATH: &str = "/api/flow/run";
pub const FLOW_CONTROL_PATH: &str = "/api/flow/control";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunControlTarget {
    pub runtime_instance_id: RuntimeInstanceId,
    pub pipeline_writer_id: WriterId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetedFlowControlRequest {
    pub protocol_version: u16,
    pub target: RunControlTarget,
    pub control: FlowControlRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetedFlowControlResponse {
    pub protocol_version: u16,
    pub target: RunControlTarget,
    pub result: FlowControlResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CurrentRunDiscovery {
    pub protocol_version: u16,
    pub target: RunControlTarget,
    pub archive: RunArchive,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunArchive {
    LocalDisk {
        flow_id: String,
        path: NativeRunPath,
    },
    Unavailable {
        reason: ArchiveUnavailableReason,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveUnavailableReason {
    Ephemeral,
    Unreadable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRunPath {
    pub encoding: NativePathEncoding,
    pub value: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativePathEncoding {
    UnixBytesBase64,
    WindowsUtf16leBase64,
}

impl NativeRunPath {
    pub fn encode(path: &Path) -> Result<Self, String> {
        if !path.is_absolute() {
            return Err("run path must be absolute".into());
        }
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            let bytes = path.as_os_str().as_bytes();
            if bytes.contains(&0) {
                return Err("run path contains NUL".into());
            }
            Ok(Self {
                encoding: NativePathEncoding::UnixBytesBase64,
                value: base64::engine::general_purpose::STANDARD.encode(bytes),
            })
        }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            let units: Vec<_> = path.as_os_str().encode_wide().collect();
            if units.contains(&0) {
                return Err("run path contains NUL".into());
            }
            let bytes: Vec<_> = units.into_iter().flat_map(u16::to_le_bytes).collect();
            Ok(Self {
                encoding: NativePathEncoding::WindowsUtf16leBase64,
                value: base64::engine::general_purpose::STANDARD.encode(bytes),
            })
        }
        #[cfg(not(any(unix, windows)))]
        Err("unsupported native path platform".into())
    }

    pub fn decode(&self) -> Result<PathBuf, String> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&self.value)
            .map_err(|e| format!("invalid native path encoding: {e}"))?;
        let path = match self.encoding {
            #[cfg(unix)]
            NativePathEncoding::UnixBytesBase64 => {
                use std::os::unix::ffi::OsStringExt;
                if bytes.contains(&0) {
                    return Err("run path contains NUL".into());
                }
                PathBuf::from(std::ffi::OsString::from_vec(bytes))
            }
            #[cfg(windows)]
            NativePathEncoding::WindowsUtf16leBase64 => {
                use std::os::windows::ffi::OsStringExt;
                if bytes.len() % 2 != 0 {
                    return Err("invalid UTF-16 path length".into());
                }
                let units: Vec<_> = bytes
                    .chunks_exact(2)
                    .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
                    .collect();
                if units.contains(&0) {
                    return Err("run path contains NUL".into());
                }
                PathBuf::from(std::ffi::OsString::from_wide(&units))
            }
            _ => return Err("server path encoding is not native to this client".into()),
        };
        if !path.is_absolute() {
            return Err("server run path is not absolute".into());
        }
        Ok(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_paths_round_trip_and_reject_relative_or_malformed_paths() {
        let path = std::env::current_dir().unwrap().join("a directory");
        assert_eq!(
            NativeRunPath::encode(&path).unwrap().decode().unwrap(),
            path
        );
        assert!(NativeRunPath::encode(Path::new("relative")).is_err());
        let mut encoded = NativeRunPath::encode(&path).unwrap();
        encoded.value = "%%%".into();
        assert!(encoded.decode().is_err());
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            let path = PathBuf::from(std::ffi::OsString::from_vec(b"/tmp/run-\xff".to_vec()));
            for bytes in [b"relative".as_slice(), b"/tmp/with\0nul"] {
                assert!(NativeRunPath {
                    encoding: NativePathEncoding::UnixBytesBase64,
                    value: base64::engine::general_purpose::STANDARD.encode(bytes)
                }
                .decode()
                .is_err());
            }
            assert!(NativeRunPath {
                encoding: NativePathEncoding::WindowsUtf16leBase64,
                value: "LwA=".into()
            }
            .decode()
            .is_err());
            assert_eq!(
                NativeRunPath::encode(&path).unwrap().decode().unwrap(),
                path
            );
        }
    }
}
