// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    config::{COMPOSE_FILE, IMAGE, LOG_FILE, POSTGRES_DATABASE, POSTGRES_PASSWORD, POSTGRES_USER},
    state::SessionState,
};
use crate::{error, Result};
use std::{
    fs::File,
    io::Write,
    path::Path,
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Compose {
    program: String,
    prefix: Vec<String>,
}

#[derive(Clone, Debug)]
pub(super) struct ServiceEvidence {
    pub(super) container_id: String,
    pub(super) health: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadinessDisposition {
    Ready,
    Retry,
    Failed,
}

trait DockerProbe {
    fn succeeds(&mut self, program: &str, args: &[&str]) -> bool;
    fn stdout(&mut self, program: &str, args: &[&str]) -> Option<String>;
}

struct SystemDockerProbe;

impl DockerProbe for SystemDockerProbe {
    fn succeeds(&mut self, program: &str, args: &[&str]) -> bool {
        command_succeeds(program, args)
    }

    fn stdout(&mut self, program: &str, args: &[&str]) -> Option<String> {
        command_stdout(program, args)
    }
}

impl Compose {
    pub(super) fn preflight() -> Result<Self> {
        Self::preflight_with(&mut SystemDockerProbe)
    }

    fn preflight_with(probe: &mut impl DockerProbe) -> Result<Self> {
        let compose = if probe.succeeds("docker", &["compose", "version"]) {
            Self {
                program: "docker".to_string(),
                prefix: vec!["compose".to_string()],
            }
        } else if probe.succeeds("docker-compose", &["version"]) {
            Self {
                program: "docker-compose".to_string(),
                prefix: Vec::new(),
            }
        } else {
            return Err(error(
                "Docker Compose is unavailable; install either `docker compose` or `docker-compose`",
            ));
        };

        let context = probe
            .stdout("docker", &["context", "show"])
            .and_then(|value| safe_docker_context(&value))
            .unwrap_or_else(|| "unknown".to_string());
        if !probe.succeeds("docker", &["info"]) {
            if context == "colima" {
                return Err(error(
                    "Docker context 'colima' is selected, but its daemon is unavailable. Start it with: colima start",
                ));
            }
            return Err(error(format!(
                "Docker context '{context}' is selected, but its daemon is unavailable"
            )));
        }
        Ok(compose)
    }

    pub(super) fn command(
        &self,
        root: &Path,
        directory: &Path,
        state: &SessionState,
        args: &[&str],
    ) -> Command {
        let mut command = Command::new(&self.program);
        command
            .current_dir(root)
            .args(&self.prefix)
            .arg("-f")
            .arg(root.join(COMPOSE_FILE))
            .arg("-p")
            .arg(&state.project)
            .args(args)
            .env("OBZENFLOW_POSTGRES_PASSWORD", POSTGRES_PASSWORD)
            .env("OBZENFLOW_POSTGRES_TLS_DIR", directory.join("tls"));
        command
    }

    pub(super) fn output(
        &self,
        root: &Path,
        directory: &Path,
        state: &SessionState,
        args: &[&str],
    ) -> Result<Output> {
        Ok(self
            .command(root, directory, state, args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()?)
    }

    pub(super) fn start(
        &self,
        root: &Path,
        directory: &Path,
        state: &mut SessionState,
    ) -> Result<()> {
        let output = self.output(root, directory, state, &["up", "-d"])?;
        if !output.status.success() {
            let _ = self.capture_logs(root, directory, state, &directory.join(LOG_FILE));
            return Err(error(format!(
                "PostgreSQL failed to start. project={} image={} port=dynamic; captured logs: {}",
                state.project,
                IMAGE,
                directory.join(LOG_FILE).display()
            )));
        }
        self.wait_ready(root, directory, state)?;
        state.port = self.published_port(root, directory, state)?;
        Ok(())
    }

    pub(super) fn stop(
        &self,
        root: &Path,
        directory: &Path,
        state: &SessionState,
        volumes: bool,
    ) -> Result<()> {
        let mut args = vec!["down", "--remove-orphans"];
        if volumes {
            args.push("--volumes");
        }
        let output = self.output(root, directory, state, &args)?;
        if output.status.success() {
            Ok(())
        } else {
            Err(error(format!(
                "failed to stop exact PostgreSQL project '{}'",
                state.project
            )))
        }
    }

    pub(super) fn health(
        &self,
        root: &Path,
        directory: &Path,
        state: &SessionState,
    ) -> Result<String> {
        let Some(id) = self.container_id(root, directory, state)? else {
            return Ok("stopped".to_string());
        };
        let output = Command::new("docker")
            .args(["inspect", "--format", "{{.State.Health.Status}}", &id])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()?;
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        } else {
            Ok("unavailable".to_string())
        }
    }

    pub(super) fn container_id(
        &self,
        root: &Path,
        directory: &Path,
        state: &SessionState,
    ) -> Result<Option<String>> {
        let output = self.output(root, directory, state, &["ps", "-q", "postgres"])?;
        if !output.status.success() {
            return Ok(None);
        }
        let id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok((!id.is_empty()).then_some(id))
    }

    pub(super) fn service_evidence(
        &self,
        root: &Path,
        directory: &Path,
        state: &SessionState,
    ) -> Result<ServiceEvidence> {
        let container_id = self
            .container_id(root, directory, state)?
            .ok_or_else(|| error("PostgreSQL test service has no container identity"))?;
        let health = self.health(root, directory, state)?;
        if health != "healthy" {
            return Err(error("PostgreSQL test service is not healthy"));
        }
        Ok(ServiceEvidence {
            container_id,
            health,
        })
    }

    pub(super) fn published_port(
        &self,
        root: &Path,
        directory: &Path,
        state: &SessionState,
    ) -> Result<u16> {
        let output = self.output(root, directory, state, &["port", "postgres", "5432"])?;
        if !output.status.success() {
            return Err(error(format!(
                "failed to discover Docker-assigned PostgreSQL port for project '{}'",
                state.project
            )));
        }
        parse_published_port(&String::from_utf8_lossy(&output.stdout))
    }

    pub(super) fn verify_container_authority(
        &self,
        container_id: &str,
        project: &str,
    ) -> Result<()> {
        let output = Command::new("docker")
            .args([
                "inspect",
                "--format",
                "{{ index .Config.Labels \"com.docker.compose.project\" }}\t{{ index .Config.Labels \"com.docker.compose.service\" }}",
                container_id,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()?;
        if !output.status.success() {
            return Err(error(
                "refusing PostgreSQL cleanup because container authority could not be verified",
            ));
        }
        validate_container_labels(&String::from_utf8_lossy(&output.stdout), project)
    }

    pub(super) fn logs(
        &self,
        root: &Path,
        directory: &Path,
        state: &SessionState,
    ) -> Result<Output> {
        self.output(root, directory, state, &["logs", "--no-color", "postgres"])
    }

    pub(super) fn capture_logs(
        &self,
        root: &Path,
        directory: &Path,
        state: &SessionState,
        destination: &Path,
    ) -> Result<()> {
        let output = self.logs(root, directory, state)?;
        let mut file = File::create(destination)?;
        file.write_all(&output.stdout)?;
        file.write_all(&output.stderr)?;
        Ok(())
    }

    fn wait_ready(&self, root: &Path, directory: &Path, state: &SessionState) -> Result<()> {
        let deadline = Instant::now() + HEALTH_TIMEOUT;
        let mut sql_preflight_attempts = 0_u32;
        loop {
            let health = self.health(root, directory, state)?;
            let preflight = if health == "healthy" {
                sql_preflight_attempts += 1;
                Some(self.server_preflight(root, directory, state)?)
            } else {
                None
            };
            match readiness_disposition(
                &health,
                preflight.as_ref().map(|output| output.status.success()),
                Instant::now() >= deadline,
            ) {
                ReadinessDisposition::Ready => {
                    return validate_server_preflight(
                        &preflight.expect("ready state has SQL evidence").stdout,
                    )
                }
                ReadinessDisposition::Retry => thread::sleep(Duration::from_millis(250)),
                ReadinessDisposition::Failed => {
                    let _ = self.capture_logs(root, directory, state, &directory.join(LOG_FILE));
                    return Err(error(format!(
                        "PostgreSQL did not become SQL-ready within {}s. project={} image={} port=dynamic last_health={} sql_preflight_attempts={}; captured logs: {}",
                        HEALTH_TIMEOUT.as_secs(),
                        state.project,
                        IMAGE,
                        health,
                        sql_preflight_attempts,
                        directory.join(LOG_FILE).display()
                    )));
                }
            }
        }
    }

    fn server_preflight(
        &self,
        root: &Path,
        directory: &Path,
        state: &SessionState,
    ) -> Result<Output> {
        self.output(
            root,
            directory,
            state,
            &[
                "exec",
                "-T",
                "postgres",
                "psql",
                "-U",
                POSTGRES_USER,
                "-d",
                POSTGRES_DATABASE,
                "-Atqc",
                "SHOW server_version_num; SHOW fsync;",
            ],
        )
    }
}

fn readiness_disposition(
    health: &str,
    sql_preflight_succeeded: Option<bool>,
    deadline_reached: bool,
) -> ReadinessDisposition {
    if health == "healthy" && sql_preflight_succeeded == Some(true) {
        ReadinessDisposition::Ready
    } else if health == "unhealthy" || deadline_reached {
        ReadinessDisposition::Failed
    } else {
        ReadinessDisposition::Retry
    }
}

fn validate_server_preflight(stdout: &[u8]) -> Result<()> {
    let lines = String::from_utf8_lossy(stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let version = lines
        .first()
        .and_then(|line| line.parse::<u32>().ok())
        .ok_or_else(|| error("PostgreSQL readiness returned an invalid server version"))?;
    if !(170_000..180_000).contains(&version) {
        return Err(error("PostgreSQL test service must run major version 17"));
    }
    if lines.get(1).map(String::as_str) != Some("on") {
        return Err(error("PostgreSQL test service must keep fsync enabled"));
    }
    Ok(())
}

fn parse_published_port(output: &str) -> Result<u16> {
    let mappings = output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    let [mapping] = mappings.as_slice() else {
        return Err(error(
            "Docker must publish exactly one loopback PostgreSQL endpoint",
        ));
    };
    let (host, port) = mapping
        .rsplit_once(':')
        .ok_or_else(|| error("Docker returned an invalid PostgreSQL port mapping"))?;
    if host.trim_matches(['[', ']']) != "127.0.0.1" {
        return Err(error(
            "Docker published PostgreSQL on a non-loopback host address",
        ));
    }
    let port = port
        .parse::<u16>()
        .map_err(|_| error("Docker returned an invalid PostgreSQL host port"))?;
    if port == 0 {
        return Err(error("Docker returned an unassigned PostgreSQL host port"));
    }
    Ok(port)
}

fn validate_container_labels(labels: &str, project: &str) -> Result<()> {
    let Some((actual_project, service)) = labels.trim().split_once('\t') else {
        return Err(error(
            "refusing PostgreSQL cleanup because container labels are incomplete",
        ));
    };
    if actual_project == project && service == "postgres" {
        Ok(())
    } else {
        Err(error(
            "refusing PostgreSQL cleanup because container labels do not match the session authority",
        ))
    }
}

fn safe_docker_context(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')))
    .then(|| value.to_string())
}

fn command_succeeds(program: &str, args: &[&str]) -> bool {
    Command::new(program)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn command_stdout(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct FakeDockerProbe {
        modern_compose: bool,
        legacy_compose: bool,
        context: Option<String>,
        daemon: bool,
        calls: Vec<String>,
    }

    impl DockerProbe for FakeDockerProbe {
        fn succeeds(&mut self, program: &str, args: &[&str]) -> bool {
            self.calls.push(format!("{program} {}", args.join(" ")));
            match (program, args) {
                ("docker", ["compose", "version"]) => self.modern_compose,
                ("docker-compose", ["version"]) => self.legacy_compose,
                ("docker", ["info"]) => self.daemon,
                _ => false,
            }
        }

        fn stdout(&mut self, program: &str, args: &[&str]) -> Option<String> {
            self.calls.push(format!("{program} {}", args.join(" ")));
            match (program, args) {
                ("docker", ["context", "show"]) => self.context.clone(),
                _ => None,
            }
        }
    }

    #[test]
    fn docker_preflight_is_bounded_context_aware_and_redacted() {
        let mut modern = FakeDockerProbe {
            modern_compose: true,
            context: Some("desktop-linux".to_string()),
            daemon: true,
            ..FakeDockerProbe::default()
        };
        assert_eq!(
            Compose::preflight_with(&mut modern).expect("select modern Compose"),
            Compose {
                program: "docker".to_string(),
                prefix: vec!["compose".to_string()],
            }
        );
        assert!(!modern
            .calls
            .iter()
            .any(|call| call.starts_with("docker-compose ")));

        let mut legacy = FakeDockerProbe {
            legacy_compose: true,
            context: Some("default".to_string()),
            daemon: true,
            ..FakeDockerProbe::default()
        };
        assert!(Compose::preflight_with(&mut legacy).is_ok());

        let malicious = format!("postgres://obzenflow:{POSTGRES_PASSWORD}@localhost/db");
        let mut unavailable = FakeDockerProbe {
            modern_compose: true,
            context: Some(malicious.clone()),
            ..FakeDockerProbe::default()
        };
        let diagnostic = Compose::preflight_with(&mut unavailable)
            .expect_err("unavailable daemon is actionable")
            .to_string();
        assert!(diagnostic.contains("'unknown'"));
        assert!(!diagnostic.contains(POSTGRES_PASSWORD));
        assert!(!diagnostic.contains("postgres://"));
    }

    #[test]
    fn readiness_and_port_parsers_fail_closed() {
        assert_eq!(
            readiness_disposition("starting", None, false),
            ReadinessDisposition::Retry
        );
        assert_eq!(
            readiness_disposition("healthy", Some(true), false),
            ReadinessDisposition::Ready
        );
        assert_eq!(
            readiness_disposition("unhealthy", None, false),
            ReadinessDisposition::Failed
        );
        assert_eq!(
            parse_published_port("127.0.0.1:49152\n").expect("loopback port"),
            49152
        );
        assert!(parse_published_port("0.0.0.0:49152\n").is_err());
        assert!(parse_published_port("127.0.0.1:0\n").is_err());
        assert!(parse_published_port("127.0.0.1:49152\n127.0.0.1:49153\n").is_err());
    }

    #[test]
    fn container_labels_are_exact_cleanup_authority() {
        validate_container_labels("owned-project\tpostgres\n", "owned-project")
            .expect("exact labels own cleanup");
        assert!(validate_container_labels("other-project\tpostgres\n", "owned-project").is_err());
        assert!(validate_container_labels("owned-project\tdatabase\n", "owned-project").is_err());
    }
}
