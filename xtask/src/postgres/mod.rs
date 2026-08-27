// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod acceptance;
mod compose;
mod config;
mod credentials;
mod environment;
mod fixtures;
mod state;
mod tls;

use self::{
    compose::{Compose, ServiceEvidence},
    config::{
        verified_tls_url, DEVELOPMENT_PAYMENT_SCHEMA, IMAGE, POSTGRES_BIND_ADDRESS,
        POSTGRES_CLIENT_HOST, POSTGRES_DATABASE, POSTGRES_USER, STATE_FILE,
    },
    state::{DevelopmentIdentity, SessionState},
};
use super::{error, Result};
use std::{
    net::{Ipv4Addr, SocketAddrV4, TcpListener},
    path::Path,
    process::Command,
    thread,
    time::{Duration, Instant},
};

const RETAINED_PORT_RELEASE_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) fn run(args: &[String]) -> Result<()> {
    let Some((command, flags)) = args.split_first() else {
        print_help();
        return Ok(());
    };
    match command.as_str() {
        "up" => up(flags),
        "status" => status(flags),
        "connection" => connection(flags),
        "run" => run_child(flags),
        "test" => test(flags),
        "logs" => logs(flags),
        "down" => down(flags),
        "cleanup" => cleanup(flags),
        "help" | "-h" | "--help" => {
            print_help();
            Ok(())
        }
        other => Err(error(format!("unknown postgres command: {other}"))),
    }
}

fn up(flags: &[String]) -> Result<()> {
    if !accept_no_flags("postgres up", flags)? {
        return Ok(());
    }
    let root = super::workspace_root()?;
    let identity = prepare_development_identity(&root)?;
    let compose = Compose::preflight()?;
    let state_path = identity.directory.join(STATE_FILE);

    let (session, service) = if state_path.is_file() {
        restart_or_verify(&root, &compose, &identity)?
    } else {
        first_up(&root, &compose, &identity).map_err(|failure| {
            error(format!(
                "{failure}; PostgreSQL development setup remains provisional; reset exactly this project with `cargo xtask postgres down --volumes` before retrying"
            ))
        })?
    };

    fixtures::provision_development(
        &root,
        &compose,
        &identity.directory,
        &session,
        DEVELOPMENT_PAYMENT_SCHEMA,
    )?;
    print_status(&compose, &identity, &session, "healthy", Some(&service));
    println!("run a command with: cargo xtask postgres run -- <command> [args...]");
    println!("show the password-free client profile with: cargo xtask postgres connection");
    Ok(())
}

fn first_up(
    root: &Path,
    compose: &Compose,
    identity: &DevelopmentIdentity,
) -> Result<(SessionState, ServiceEvidence)> {
    state::create_session_directory(&identity.directory)?;
    let mut session = state::new_development(identity);
    let state_path = identity.directory.join(STATE_FILE);
    state::write(&state_path, &session)?;
    credentials::create_raw(&identity.directory)?;
    tls::create_development(&identity.directory)?;
    compose.start(root, &identity.directory, &mut session)?;
    let service = compose.service_evidence(root, &identity.directory, &session)?;
    state::record_or_verify_volume(&mut session, &service.volume)?;
    credentials::create_development_pgpass(&identity.directory, session.port)?;
    credentials::validate_development(&identity.directory, session.port)?;
    state::write(&state_path, &session)?;
    Ok((session, service))
}

fn restart_or_verify(
    root: &Path,
    compose: &Compose,
    identity: &DevelopmentIdentity,
) -> Result<(SessionState, ServiceEvidence)> {
    let state_path = identity.directory.join(STATE_FILE);
    let mut session = state::read(&state_path)?;
    state::require_development_authority(&session, identity)?;
    state::require_ready(&session)?;
    credentials::validate_development(&identity.directory, session.port)?;
    tls::verify_development(&identity.directory)?;

    let health = compose.health(root, &identity.directory, &session)?;
    match health.as_str() {
        "healthy" => {
            let actual_port = compose.published_port(root, &identity.directory, &session)?;
            verify_running_port(&session, actual_port)?;
        }
        "stopped" => {
            ensure_retained_port_available(session.port)?;
            let retained_port = session.port;
            compose.start(root, &identity.directory, &mut session)?;
            if session.port != retained_port {
                return Err(error(format!(
                    "PostgreSQL published endpoint changed unexpectedly: expected port {retained_port}, found {}; no replacement endpoint was adopted",
                    session.port
                )));
            }
        }
        other => {
            return Err(error(format!(
                "PostgreSQL project '{}' is {other}; inspect with `cargo xtask postgres logs`",
                session.project
            )))
        }
    }
    let service = compose.service_evidence(root, &identity.directory, &session)?;
    state::record_or_verify_volume(&mut session, &service.volume)?;
    credentials::validate_development(&identity.directory, session.port)?;
    state::write(&state_path, &session)?;
    Ok((session, service))
}

fn status(flags: &[String]) -> Result<()> {
    if !accept_no_flags("postgres status", flags)? {
        return Ok(());
    }
    let root = super::workspace_root()?;
    let identity = prepare_development_identity(&root)?;
    let state_path = identity.directory.join(STATE_FILE);
    let compose = Compose::preflight()?;
    if !state_path.is_file() {
        println!("no PostgreSQL development session state found");
        return report_existing_test_sessions(&root, &compose);
    }
    let mut session = state::read(&state_path)?;
    state::require_development_authority(&session, &identity)?;
    let ready = state::require_ready(&session).is_ok();
    if ready {
        credentials::validate_development(&identity.directory, session.port)?;
        tls::verify_development(&identity.directory)?;
    }
    let health = compose.health(&root, &identity.directory, &session)?;
    let service = if health == "healthy" {
        let actual_port = compose.published_port(&root, &identity.directory, &session)?;
        if ready {
            verify_running_port(&session, actual_port)?;
        }
        let service = compose.service_evidence(&root, &identity.directory, &session)?;
        if ready {
            state::record_or_verify_volume(&mut session, &service.volume)?;
            state::write(&state_path, &session)?;
        }
        Some(service)
    } else {
        None
    };
    print_status(&compose, &identity, &session, &health, service.as_ref());
    if !ready {
        println!(
            "  recovery:        setup is provisional; run `cargo xtask postgres down --volumes`"
        );
    }
    report_existing_test_sessions(&root, &compose)
}

fn connection(flags: &[String]) -> Result<()> {
    if !accept_no_flags("postgres connection", flags)? {
        return Ok(());
    }
    let root = super::workspace_root()?;
    let compose = Compose::preflight()?;
    let (identity, mut session) = development_session(&root, true)?;
    let service = verify_running_session(&root, &compose, &identity, &mut session)?;
    print_status(&compose, &identity, &session, "healthy", Some(&service));
    let pgpass = credentials::pgpass_path(&identity.directory);
    let url = verified_tls_url(session.port, &identity.directory.join("tls/ca.crt"))?;
    println!("copyable password-free psql command:");
    println!("env -u PGPASSWORD \\");
    println!(
        "{} \\",
        shell_quote(&format!("PGPASSFILE={}", pgpass.display()))
    );
    println!("psql {}", shell_quote(&url));
    Ok(())
}

fn run_child(flags: &[String]) -> Result<()> {
    let Some((delimiter, command)) = flags.split_first() else {
        return Err(error("postgres run requires `-- <command> [args...]`"));
    };
    if delimiter != "--" || command.is_empty() {
        return Err(error("postgres run requires `-- <command> [args...]`"));
    }

    let root = super::workspace_root()?;
    let compose = Compose::preflight()?;
    let (identity, mut session) = development_session(&root, true)?;
    let service = verify_running_session(&root, &compose, &identity, &mut session)?;

    let mut child = Command::new(&command[0]);
    child.current_dir(&root).args(&command[1..]);
    let schema = environment::configure_development(&mut child, &identity.directory, &session)?;
    println!(
        "using PostgreSQL development database: project={} volume={} endpoint={}:{} database={} schema={schema:?}",
        session.project,
        service.volume,
        POSTGRES_CLIENT_HOST,
        session.port,
        POSTGRES_DATABASE,
    );
    let child_status = child.status()?;
    if child_status.success() {
        Ok(())
    } else {
        Err(error(format!(
            "PostgreSQL child command failed with status {child_status}"
        )))
    }
}

fn test(flags: &[String]) -> Result<()> {
    if !accept_no_flags("postgres test", flags)? {
        return Ok(());
    }
    let root = super::workspace_root()?;
    let compose = Compose::preflight()?;
    report_existing_test_sessions(&root, &compose)?;
    acceptance::run(root, compose)
}

fn logs(flags: &[String]) -> Result<()> {
    if !accept_no_flags("postgres logs", flags)? {
        return Ok(());
    }
    let root = super::workspace_root()?;
    let compose = Compose::preflight()?;
    let (identity, session) = development_session(&root, false)?;
    let output = compose.logs(&root, &identity.directory, &session)?;
    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    if output.status.success() {
        Ok(())
    } else {
        Err(error("failed to read PostgreSQL container logs"))
    }
}

fn down(flags: &[String]) -> Result<()> {
    let volumes = match flags {
        [] => false,
        [flag] if flag == "--volumes" => true,
        [flag] if is_help(flag) => {
            print_help();
            return Ok(());
        }
        _ => return Err(error("postgres down accepts only `--volumes`")),
    };
    let root = super::workspace_root()?;
    let identity = prepare_development_identity(&root)?;
    let state_path = identity.directory.join(STATE_FILE);
    if !state_path.is_file() {
        println!("no PostgreSQL development session state found");
        return Ok(());
    }
    let compose = Compose::preflight()?;
    let session = state::read(&state_path)?;
    state::require_development_authority(&session, &identity)?;
    let expected_volume = state::expected_volume(&session.project);
    if let Some(container_id) = compose.container_id(&root, &identity.directory, &session)? {
        compose.verify_container_authority(&container_id, &session.project)?;
        let actual_volume = compose.container_volume(&container_id)?;
        if actual_volume != expected_volume {
            return Err(error(
                "refusing cleanup because the PostgreSQL volume changed",
            ));
        }
        compose.verify_volume_authority(&actual_volume, &session.project)?;
    }
    if volumes {
        state::require_owned_directory(&root, &identity.directory)?;
        compose.verify_volume_authority_if_present(&expected_volume, &session.project)?;
        println!(
            "removing project '{}' and named volume '{}'; development data and credentials will not be recoverable",
            session.project, expected_volume
        );
    }
    compose.stop(&root, &identity.directory, &session, volumes)?;
    if volumes {
        if compose.volume_exists(&expected_volume)? {
            return Err(error(format!(
                "Docker retained PostgreSQL volume {expected_volume}; development state was not removed"
            )));
        }
        state::remove_owned_directory(&root, &identity.directory)?;
    } else {
        println!(
            "PostgreSQL project stopped; its retained port, named volume, credentials, and rows are preserved"
        );
    }
    Ok(())
}

fn cleanup(flags: &[String]) -> Result<()> {
    let [run_id] = flags else {
        return Err(error("postgres cleanup requires one reported test run id"));
    };
    let root = super::workspace_root()?;
    let identity = state::test_identity(&root, run_id)?;
    state::require_owned_directory(&root, &identity.directory)?;
    let session = state::read(&identity.directory.join(STATE_FILE))?;
    state::require_test_authority(&session, &identity)?;
    let compose = Compose::preflight()?;
    let expected_volume = state::expected_volume(&session.project);
    if let Some(container_id) = compose.container_id(&root, &identity.directory, &session)? {
        compose.verify_container_authority(&container_id, &identity.project)?;
        let actual_volume = compose.container_volume(&container_id)?;
        if actual_volume != expected_volume {
            return Err(error(
                "refusing cleanup because the disposable PostgreSQL volume changed",
            ));
        }
        compose.verify_volume_authority(&actual_volume, &session.project)?;
    }
    compose.verify_volume_authority_if_present(&expected_volume, &session.project)?;
    println!(
        "removing disposable PostgreSQL test project '{}' and its data volume",
        identity.project
    );
    let _ = compose.capture_logs(
        &root,
        &identity.directory,
        &session,
        &identity.directory.join(config::LOG_FILE),
    );
    compose.stop(&root, &identity.directory, &session, true)?;
    if compose.volume_exists(&expected_volume)? {
        return Err(error(format!(
            "Docker retained disposable PostgreSQL volume {expected_volume}"
        )));
    }
    state::remove_owned_directory(&root, &identity.directory)?;
    println!("PostgreSQL test project '{}' cleaned", identity.project);
    Ok(())
}

fn development_session(
    root: &Path,
    require_ready: bool,
) -> Result<(DevelopmentIdentity, SessionState)> {
    let identity = prepare_development_identity(root)?;
    let state_path = identity.directory.join(STATE_FILE);
    if !state_path.is_file() {
        return Err(error(
            "PostgreSQL development session is unavailable; run `cargo xtask postgres up`",
        ));
    }
    let session = state::read(&state_path)?;
    state::require_development_authority(&session, &identity)?;
    if require_ready {
        state::require_ready(&session)?;
    }
    Ok((identity, session))
}

fn prepare_development_identity(root: &Path) -> Result<DevelopmentIdentity> {
    let identity = state::development_identity(root)?;
    state::reject_legacy_development(root, &identity)?;
    Ok(identity)
}

fn verify_running_session(
    root: &Path,
    compose: &Compose,
    identity: &DevelopmentIdentity,
    session: &mut SessionState,
) -> Result<ServiceEvidence> {
    state::require_ready(session)?;
    credentials::validate_development(&identity.directory, session.port)?;
    tls::verify_development(&identity.directory)?;
    let health = compose.health(root, &identity.directory, session)?;
    if health != "healthy" {
        return Err(error(format!(
            "PostgreSQL project '{}' is not healthy; start it with `cargo xtask postgres up` or inspect `cargo xtask postgres logs`",
            session.project
        )));
    }
    let actual_port = compose.published_port(root, &identity.directory, session)?;
    verify_running_port(session, actual_port)?;
    let service = compose.service_evidence(root, &identity.directory, session)?;
    state::record_or_verify_volume(session, &service.volume)?;
    state::write(&identity.directory.join(STATE_FILE), session)?;
    Ok(service)
}

fn report_existing_test_sessions(root: &Path, compose: &Compose) -> Result<()> {
    for (directory, session) in state::test_sessions(root)? {
        let health = compose.health(root, &directory, &session)?;
        if matches!(health.as_str(), "stopped" | "unavailable") {
            continue;
        }
        println!(
            "PostgreSQL test container remains present: project={} run_id={} health={}; exact state: {}",
            session.project,
            session.run_id,
            health,
            directory.join(STATE_FILE).display()
        );
        println!(
            "if its owning test process is no longer running, remove exactly this disposable project with: cargo xtask postgres cleanup {}",
            session.run_id
        );
    }
    Ok(())
}

fn print_status(
    compose: &Compose,
    identity: &DevelopmentIdentity,
    session: &SessionState,
    health: &str,
    service: Option<&ServiceEvidence>,
) {
    let container = service
        .map(|evidence| evidence.container_id.as_str())
        .unwrap_or("none");
    let volume = service
        .map(|evidence| evidence.volume.as_str())
        .or(session.volume.as_deref())
        .unwrap_or("unrecorded");
    let port = if session.port == 0 {
        "unassigned (provisional first start)".to_string()
    } else {
        format!("{} (retained across normal restarts)", session.port)
    };
    println!("PostgreSQL development service");
    println!("  health:          {health}");
    println!("  docker context:  {}", compose.context());
    println!("  image:           {IMAGE}");
    println!("  project:         {}", session.project);
    println!("  container:       {container}");
    println!("  volume:          {volume}");
    println!("  client host:     {POSTGRES_CLIENT_HOST}");
    println!("  bind address:    {POSTGRES_BIND_ADDRESS}");
    println!("  port:            {port}");
    println!("  database:        {POSTGRES_DATABASE}");
    println!("  user:            {POSTGRES_USER}");
    println!("  schema:          {DEVELOPMENT_PAYMENT_SCHEMA}");
    println!("  TLS mode:        verify-full");
    println!(
        "  CA certificate:  {}",
        identity.directory.join("tls/ca.crt").display()
    );
    println!(
        "  pgpass:          {}",
        credentials::pgpass_path(&identity.directory).display()
    );
    println!(
        "  state:           {}",
        identity.directory.join(STATE_FILE).display()
    );
}

fn verify_running_port(session: &SessionState, actual_port: u16) -> Result<()> {
    if session.port == actual_port {
        Ok(())
    } else {
        Err(error(format!(
            "PostgreSQL published endpoint changed unexpectedly: expected port {}, found {actual_port}; no replacement endpoint was adopted",
            session.port
        )))
    }
}

fn ensure_retained_port_available(port: u16) -> Result<()> {
    let address = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
    let deadline = Instant::now() + RETAINED_PORT_RELEASE_TIMEOUT;
    loop {
        match TcpListener::bind(address) {
            Ok(listener) => {
                drop(listener);
                return Ok(());
            }
            Err(_) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(100));
            }
            Err(_) => {
                return Err(error(format!(
                    "retained PostgreSQL development port {port} is occupied; stop the conflicting service or reset this development session with `cargo xtask postgres down --volumes`"
                )))
            }
        }
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn accept_no_flags(command: &str, flags: &[String]) -> Result<bool> {
    match flags {
        [] => Ok(true),
        [flag] if is_help(flag) => {
            print_help();
            Ok(false)
        }
        _ => Err(error(format!("{command} accepts no options"))),
    }
}

fn is_help(value: &str) -> bool {
    matches!(value, "help" | "-h" | "--help")
}

fn print_help() {
    println!("usage:");
    println!("test suite:");
    println!("  cargo xtask postgres test");
    println!();
    println!("development session:");
    println!("  cargo xtask postgres up");
    println!("  cargo xtask postgres status");
    println!("  cargo xtask postgres connection");
    println!("  cargo xtask postgres run -- <command> [args...]");
    println!("  cargo xtask postgres logs");
    println!("  cargo xtask postgres down [--volumes]");
    println!();
    println!("recovery for an operator-identified abandoned disposable test session:");
    println!("  cargo xtask postgres cleanup <reported-test-run-id>");
}

#[cfg(test)]
mod tests {
    use super::state::SessionMode;
    use super::*;

    #[test]
    fn proof_has_no_hidden_lifecycle_command() {
        let failure = run(&["prove-persistent".to_string()])
            .expect_err("lifecycle proof belongs to an integration test");
        assert_eq!(
            failure.to_string(),
            "unknown postgres command: prove-persistent"
        );
    }

    #[test]
    fn up_and_connection_have_no_credential_or_port_options() {
        assert!(accept_no_flags("postgres up", &[]).unwrap());
        assert!(accept_no_flags("postgres up", &["--port".to_string()]).is_err());
        assert!(accept_no_flags("postgres connection", &["--show-password".to_string()]).is_err());
    }

    #[test]
    fn a_running_endpoint_cannot_drift_silently() {
        let project = "obzenflow-test-endpoint".to_string();
        let session = SessionState {
            volume: Some(state::expected_volume(&project)),
            project,
            run_id: state::unique_run_id(),
            port: 32780,
            mode: SessionMode::Test,
        };
        assert!(verify_running_port(&session, 32780).is_ok());
        assert!(verify_running_port(&session, 32781).is_err());
    }

    #[test]
    fn client_values_are_shell_quoted() {
        assert_eq!(shell_quote("simple"), "'simple'");
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
    }
}
