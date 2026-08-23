use super::{error, Result};
#[cfg(unix)]
use std::sync::atomic::{AtomicI32, Ordering};
use std::{
    collections::BTreeSet,
    env,
    ffi::OsString,
    fs::{self, File},
    hash::{Hash, Hasher},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

const COMPOSE_FILE: &str = "dev/postgres/compose.yml";
const SESSION_ROOT: &str = "target/postgres-sessions";
const DEVELOPMENT_SESSION: &str = "development";
const STATE_FILE: &str = "state.tsv";
const LOG_FILE: &str = "postgres.log";
const IMAGE: &str = "postgres:17";
const POSTGRES_USER: &str = "obzenflow";
const POSTGRES_DATABASE: &str = "obzenflow";
const POSTGRES_PASSWORD: &str = "obzenflow-secret-083c";
const HEALTH_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(unix)]
static RECEIVED_SIGNAL: AtomicI32 = AtomicI32::new(0);

#[cfg(unix)]
extern "C" fn record_signal(signal: libc::c_int) {
    RECEIVED_SIGNAL.store(signal, Ordering::Relaxed);
}

#[cfg(unix)]
struct SignalGuard {
    previous_interrupt: libc::sighandler_t,
    previous_terminate: libc::sighandler_t,
}

#[cfg(unix)]
impl SignalGuard {
    fn install() -> Result<Self> {
        RECEIVED_SIGNAL.store(0, Ordering::Relaxed);
        // SAFETY: the handler performs only a lock-free atomic store. The two
        // previous process-global handlers are restored when this scoped test
        // session ends.
        let previous_interrupt =
            unsafe { libc::signal(libc::SIGINT, record_signal as *const () as _) };
        if previous_interrupt == libc::SIG_ERR {
            return Err(error(
                "failed to install PostgreSQL test SIGINT cleanup handler",
            ));
        }
        // SAFETY: see the SIGINT installation above.
        let previous_terminate =
            unsafe { libc::signal(libc::SIGTERM, record_signal as *const () as _) };
        if previous_terminate == libc::SIG_ERR {
            // SAFETY: restoring the handler returned by `signal`.
            unsafe { libc::signal(libc::SIGINT, previous_interrupt) };
            return Err(error(
                "failed to install PostgreSQL test SIGTERM cleanup handler",
            ));
        }
        Ok(Self {
            previous_interrupt,
            previous_terminate,
        })
    }
}

#[cfg(unix)]
impl Drop for SignalGuard {
    fn drop(&mut self) {
        // SAFETY: restoring the handlers returned by the matching calls to
        // `signal` in `SignalGuard::install`.
        unsafe {
            libc::signal(libc::SIGINT, self.previous_interrupt);
            libc::signal(libc::SIGTERM, self.previous_terminate);
        }
    }
}

#[cfg(not(unix))]
struct SignalGuard;

#[cfg(not(unix))]
impl SignalGuard {
    fn install() -> Result<Self> {
        Ok(Self)
    }
}

fn check_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let signal = RECEIVED_SIGNAL.load(Ordering::Relaxed);
        if signal != 0 {
            return Err(error(format!(
                "PostgreSQL proof interrupted by signal {signal}"
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct ComposeCommand {
    program: String,
    prefix: Vec<String>,
}

#[derive(Clone, Debug)]
struct SessionState {
    project: String,
    run_id: String,
    port: u16,
    mode: SessionMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionMode {
    Development,
    Test,
}

impl SessionMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Development => "development",
            Self::Test => "test",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "development" => Ok(Self::Development),
            "test" => Ok(Self::Test),
            _ => Err(error("invalid PostgreSQL session mode")),
        }
    }
}

pub(super) fn run(args: &[String]) -> Result<()> {
    let Some((command, flags)) = args.split_first() else {
        print_help();
        return Ok(());
    };

    match command.as_str() {
        "up" => up(flags),
        "status" => status(flags),
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
    reject_flags("postgres up", flags)?;
    let root = super::workspace_root()?;
    let compose = preflight_docker()?;
    let directory = root.join(SESSION_ROOT).join(DEVELOPMENT_SESSION);
    let state_path = directory.join(STATE_FILE);

    if state_path.is_file() {
        let mut state = read_state(&state_path)?;
        if container_health(&root, &compose, &directory, &state)? == "healthy" {
            state.port = published_port(&root, &compose, &directory, &state)?;
            write_state(&state_path, &state)?;
            print_status(&state, "healthy");
            return Ok(());
        }
        ensure_tls(&directory)?;
        start_session(&root, &compose, &directory, &mut state)?;
        write_state(&state_path, &state)?;
        print_status(&state, "healthy");
        return Ok(());
    }

    fs::create_dir_all(&directory)?;
    let run_id = unique_run_id();
    let mut state = SessionState {
        project: development_project(&root),
        run_id,
        port: 0,
        mode: SessionMode::Development,
    };
    ensure_tls(&directory)?;
    write_state(&state_path, &state)?;
    start_session(&root, &compose, &directory, &mut state)?;
    write_state(&state_path, &state)?;
    print_status(&state, "healthy");
    println!("run a command with: cargo xtask postgres run -- <command> [args...]");
    Ok(())
}

fn status(flags: &[String]) -> Result<()> {
    reject_flags("postgres status", flags)?;
    let root = super::workspace_root()?;
    let compose = preflight_docker()?;
    let state_path = development_state_path(&root);
    if !state_path.is_file() {
        println!("no PostgreSQL development session state found");
        report_stale_test_sessions(&root, Some(&compose))?;
        return Ok(());
    }
    let mut state = read_state(&state_path)?;
    let directory = state_path
        .parent()
        .ok_or_else(|| error("invalid PostgreSQL state path"))?;
    let health = container_health(&root, &compose, directory, &state)?;
    if health == "healthy" {
        state.port = published_port(&root, &compose, directory, &state)?;
        write_state(&state_path, &state)?;
    }
    print_status(&state, &health);
    report_stale_test_sessions(&root, Some(&compose))
}

fn run_child(flags: &[String]) -> Result<()> {
    let Some((delimiter, command)) = flags.split_first() else {
        return Err(error("postgres run requires `-- <command> [args...]`"));
    };
    if delimiter != "--" || command.is_empty() {
        return Err(error("postgres run requires `-- <command> [args...]`"));
    }

    let root = super::workspace_root()?;
    let compose = preflight_docker()?;
    let state_path = development_state_path(&root);
    let mut state = read_state(&state_path).map_err(|_| {
        error("PostgreSQL development session is unavailable; run `cargo xtask postgres up`")
    })?;
    let directory = state_path
        .parent()
        .ok_or_else(|| error("invalid PostgreSQL state path"))?;
    let health = container_health(&root, &compose, directory, &state)?;
    if health != "healthy" {
        return Err(error(format!(
            "PostgreSQL project '{}' is not healthy; inspect with `cargo xtask postgres logs`",
            state.project
        )));
    }
    state.port = published_port(&root, &compose, directory, &state)?;
    write_state(&state_path, &state)?;

    let mut child = Command::new(&command[0]);
    child
        .current_dir(&root)
        .args(&command[1..])
        .env("OBZENFLOW_POSTGRES_URL", plaintext_url(state.port));
    let status = child.status()?;
    if status.success() {
        Ok(())
    } else {
        Err(error(format!(
            "PostgreSQL child command failed with status {status}"
        )))
    }
}

fn test(flags: &[String]) -> Result<()> {
    reject_flags("postgres test", flags)?;
    let _signal_guard = SignalGuard::install()?;
    let root = super::workspace_root()?;
    let compose = preflight_docker()?;
    report_stale_test_sessions(&root, Some(&compose))?;

    let run_id = unique_run_id();
    let directory = root.join(SESSION_ROOT).join(&run_id);
    fs::create_dir_all(&directory)?;
    let mut state = SessionState {
        project: format!("obzenflow-test-{run_id}"),
        run_id,
        port: 0,
        mode: SessionMode::Test,
    };
    if let Err(error) = ensure_tls(&directory) {
        let _ = fs::remove_dir_all(&directory);
        return Err(error);
    }
    write_state(&directory.join(STATE_FILE), &state)?;

    let proof_result = (|| {
        check_signal()?;
        start_session(&root, &compose, &directory, &mut state)?;
        write_state(&directory.join(STATE_FILE), &state)?;
        check_signal()?;
        prove_concurrent_session_isolation(&root, &compose, &directory, &state)?;
        check_signal()?;
        run_test_inventory(&root, &directory, &state)?;
        check_signal()
    })();
    let _ = capture_logs(&root, &compose, &directory, &state);
    let cleanup_result = stop_session(&root, &compose, &directory, &state, true);
    let key_cleanup_result = remove_ephemeral_keys(&directory);

    match (proof_result, cleanup_result, key_cleanup_result) {
        (Ok(()), Ok(()), Ok(())) => {
            println!(
                "PostgreSQL proof completed; project={} cleaned",
                state.project
            );
            Ok(())
        }
        (Err(proof), cleanup, key_cleanup) => Err(error(format!(
            "{proof}; cleanup={}; key_cleanup={}; captured logs: {}",
            result_label(cleanup),
            result_label(key_cleanup),
            directory.join(LOG_FILE).display()
        ))),
        (Ok(()), Err(cleanup), key_cleanup) => Err(error(format!(
            "PostgreSQL proof passed but cleanup failed: {cleanup}; key_cleanup={}; project={}",
            result_label(key_cleanup),
            state.project
        ))),
        (Ok(()), Ok(()), Err(key_cleanup)) => Err(error(format!(
            "PostgreSQL proof passed but ephemeral key cleanup failed: {key_cleanup}; project={}",
            state.project
        ))),
    }
}

fn logs(flags: &[String]) -> Result<()> {
    reject_flags("postgres logs", flags)?;
    let root = super::workspace_root()?;
    let compose = preflight_docker()?;
    let state_path = development_state_path(&root);
    let state = read_state(&state_path).map_err(|_| {
        error("PostgreSQL development session is unavailable; run `cargo xtask postgres up`")
    })?;
    let directory = state_path
        .parent()
        .ok_or_else(|| error("invalid PostgreSQL state path"))?;
    let output = compose_output(
        &root,
        &compose,
        directory,
        &state,
        &["logs", "--no-color", "postgres"],
    )?;
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
        [flag] if matches!(flag.as_str(), "-h" | "--help" | "help") => {
            print_help();
            return Ok(());
        }
        _ => return Err(error("postgres down accepts only `--volumes`")),
    };
    let root = super::workspace_root()?;
    let state_path = development_state_path(&root);
    if !state_path.is_file() {
        println!("no PostgreSQL development session state found");
        return Ok(());
    }
    let compose = preflight_docker()?;
    let state = read_state(&state_path)?;
    let directory = state_path
        .parent()
        .ok_or_else(|| error("invalid PostgreSQL state path"))?;
    if volumes {
        println!(
            "removing project '{}' and its PostgreSQL data volume; development data will not be recoverable",
            state.project
        );
    }
    stop_session(&root, &compose, directory, &state, volumes)?;
    if volumes {
        fs::remove_dir_all(directory)?;
    } else {
        println!("PostgreSQL project stopped; its named development volume is retained");
    }
    Ok(())
}

fn cleanup(flags: &[String]) -> Result<()> {
    let [run_id] = flags else {
        return Err(error("postgres cleanup requires one reported test run id"));
    };
    if run_id.is_empty()
        || !run_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(error("invalid PostgreSQL test run id"));
    }
    let root = super::workspace_root()?;
    let directory = root.join(SESSION_ROOT).join(run_id);
    let state_path = directory.join(STATE_FILE);
    let state = read_state(&state_path)?;
    if state.mode != SessionMode::Test || state.run_id != *run_id {
        return Err(error("refusing to clean a non-test PostgreSQL session"));
    }
    let compose = preflight_docker()?;
    let _ = capture_logs(&root, &compose, &directory, &state);
    stop_session(&root, &compose, &directory, &state, true)?;
    remove_ephemeral_keys(&directory)?;
    fs::remove_file(state_path)?;
    println!("PostgreSQL test project '{}' cleaned", state.project);
    Ok(())
}

struct RequiredTestTarget {
    label: &'static str,
    cargo_args: &'static [&'static str],
    expected_tests: &'static [&'static str],
    inventory_prefix: Option<&'static str>,
}

const REQUIRED_TEST_TARGETS: &[RequiredTestTarget] = &[
    RequiredTestTarget {
        label: "xtask PostgreSQL lifecycle",
        cargo_args: &["test", "--locked", "-p", "xtask"],
        expected_tests: &[
            "postgres::tests::published_port_parser_accepts_only_loopback_dynamic_mapping",
            "postgres::tests::required_test_inventory_rejects_missing_or_unexpected_tests",
            "postgres::tests::state_round_trips_without_credentials",
        ],
        inventory_prefix: Some("postgres::tests::"),
    },
    RequiredTestTarget {
        label: "DSL order-sensitive sink composition",
        cargo_args: &["test", "--locked", "-p", "obzenflow_dsl", "--lib"],
        expected_tests: &[
            "dsl::tests::typed_stage_contracts_test::tests::order_sensitive_sink_rejects_a_cycle_fed_input",
            "dsl::tests::typed_stage_contracts_test::tests::order_sensitive_sinks_mark_source_and_derived_fan_in_with_the_right_merge_mode",
        ],
        inventory_prefix: Some(
            "dsl::tests::typed_stage_contracts_test::tests::order_sensitive_sink",
        ),
    },
    RequiredTestTarget {
        label: "PostgreSQL adapter unit contract",
        cargo_args: &[
            "test",
            "--locked",
            "-p",
            "obzenflow_adapters",
            "--features",
            "postgres,test-support",
            "--lib",
        ],
        expected_tests: &[
            "sinks::postgres::tests::bindings_retain_only_the_first_encoding_error_for_query_execution",
            "sinks::postgres::tests::build_is_local_and_debug_is_redacted",
            "sinks::postgres::tests::configuration_rejects_unsafe_shapes",
            "sinks::postgres::tests::connector_input_witness_is_the_builder_payload",
            "sinks::postgres::tests::private_assembler_retains_configured_statement_authority",
            "sinks::postgres::tests::real_driver_sqlstates_and_transport_absence_map_without_text_parsing",
            "sinks::postgres::tests::real_writers_own_distinct_one_slot_pools",
            "sinks::postgres::tests::sqlstate_codes_use_the_typed_bounded_carrier",
            "sinks::postgres::tests::target_generation_is_exact_and_does_not_parse_the_body",
            "sinks::postgres::tests::typed_transport_ignores_ambient_pgsslmode",
            "sinks::postgres::tests::typed_transport_is_authoritative_over_urls_and_options",
        ],
        inventory_prefix: Some("sinks::postgres::tests::"),
    },
    RequiredTestTarget {
        label: "PostgreSQL real-driver contract",
        cargo_args: &[
            "test",
            "--locked",
            "-p",
            "obzenflow_adapters",
            "--features",
            "postgres,test-support",
            "--test",
            "postgres_sink_driver_test",
        ],
        expected_tests: &[
            "binding_is_parameterised_and_readiness_remains_point_in_time",
            "buffered_flush_and_drain_rejection_settle_nothing",
            "deferred_origin_failures_poison_with_exact_subject_and_current_failures_remain_reusable",
            "open_is_non_mutating_and_postgres_owns_statement_authority",
            "operation_deadlines_preserve_only_acknowledged_transaction_truth",
            "postgres_tls_uses_native_root_loader_in_an_isolated_process",
            "real_postgres_locks_bound_preparation_rollback_and_quarantine",
            "replacement_authority_query_failures_and_timeouts_close_unverified_sessions",
            "replacement_sessions_reestablish_target_authority_before_begin",
            "server_cancellation_remains_remote_postgres_evidence",
            "typed_transport_proves_plaintext_and_tls_failure_matrix",
        ],
        inventory_prefix: None,
    },
    RequiredTestTarget {
        label: "PostgreSQL writer conformance",
        cargo_args: &[
            "test",
            "--locked",
            "-p",
            "obzenflow_adapters",
            "--features",
            "postgres,test-support",
            "--test",
            "postgres_sink_conformance_test",
        ],
        expected_tests: &["postgres_passes_the_real_writer_protocol_and_fault_matrix"],
        inventory_prefix: None,
    },
    RequiredTestTarget {
        label: "PostgreSQL application and ordering conformance",
        cargo_args: &[
            "test",
            "--locked",
            "-p",
            "obzenflow",
            "--features",
            "postgres,test-support",
            "--test",
            "postgres_sink_application_conformance_test",
        ],
        expected_tests: &[
            "postgres_order_sensitive_cycle_fan_in_is_rejected_before_open",
            "postgres_order_sensitive_derived_fan_in_reports_named_quiet_input",
            "postgres_order_sensitive_source_fan_in_replays_same_word_and_final_row",
            "postgres_passes_live_redelivery_gate_and_archived_failure_projection",
        ],
        inventory_prefix: None,
    },
    RequiredTestTarget {
        label: "PostgreSQL public consumer",
        cargo_args: &[
            "test",
            "--locked",
            "-p",
            "obzenflow",
            "--features",
            "postgres",
            "--test",
            "postgres_public_consumer_test",
        ],
        expected_tests: &["root_feature_exposes_a_sink_macro_compatible_value_binder"],
        inventory_prefix: None,
    },
    RequiredTestTarget {
        label: "PostgreSQL package boundary",
        cargo_args: &[
            "test",
            "--locked",
            "-p",
            "obzenflow",
            "--test",
            "postgres_connector_package_boundary_test",
        ],
        expected_tests: &["postgres_stays_in_the_feature_gated_adapter_boundary"],
        inventory_prefix: None,
    },
];

fn run_test_inventory(root: &Path, directory: &Path, state: &SessionState) -> Result<()> {
    for target in REQUIRED_TEST_TARGETS {
        check_signal()?;
        verify_test_inventory(root, directory, state, target)?;
        run_required_test_target(root, directory, state, target)?;
        check_signal()?;
    }
    run_cross_process_example(root, directory, state)
}

fn configure_postgres_test_environment(
    child: &mut Command,
    directory: &Path,
    state: &SessionState,
) {
    child
        .env("OBZENFLOW_POSTGRES_TEST_URL", plaintext_url(state.port))
        .env("OBZENFLOW_POSTGRES_TEST_RUN_ID", &state.run_id)
        .env(
            "OBZENFLOW_POSTGRES_TEST_TLS_URL",
            tls_url(state.port, "localhost"),
        )
        .env(
            "OBZENFLOW_POSTGRES_TEST_WRONG_HOST_URL",
            tls_url(state.port, "127.0.0.1"),
        )
        .env(
            "OBZENFLOW_POSTGRES_TEST_CA_CERT",
            directory.join("tls/ca.crt"),
        )
        .env(
            "OBZENFLOW_POSTGRES_TEST_UNTRUSTED_CA_CERT",
            directory.join("tls/untrusted-ca.crt"),
        );
}

fn verify_test_inventory(
    root: &Path,
    directory: &Path,
    state: &SessionState,
    target: &RequiredTestTarget,
) -> Result<()> {
    let mut child = Command::new("cargo");
    child
        .current_dir(root)
        .args(target.cargo_args)
        .args(["--", "--list", "--format", "terse"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    configure_postgres_test_environment(&mut child, directory, state);
    let output = child.output()?;
    if !output.status.success() {
        let stderr = redact_proof_output(&output.stderr, state);
        if !stderr.trim().is_empty() {
            eprint!("{stderr}");
        }
        return Err(error(format!(
            "failed to enumerate required {} tests with status {}",
            target.label, output.status
        )));
    }
    let actual = listed_test_names(&String::from_utf8_lossy(&output.stdout));
    require_exact_test_inventory(
        target.label,
        &actual,
        target.expected_tests,
        target.inventory_prefix,
    )
}

fn run_required_test_target(
    root: &Path,
    directory: &Path,
    state: &SessionState,
    target: &RequiredTestTarget,
) -> Result<()> {
    let mut child = Command::new("cargo");
    child.current_dir(root).args(target.cargo_args).arg("--");
    if let Some(prefix) = target.inventory_prefix {
        child.arg(prefix);
    }
    child.arg("--include-ignored");
    configure_postgres_test_environment(&mut child, directory, state);
    let status = child.status()?;
    if status.success() {
        Ok(())
    } else {
        Err(error(format!(
            "required {} tests failed with status {status}",
            target.label
        )))
    }
}

fn listed_test_names(output: &str) -> BTreeSet<String> {
    output
        .lines()
        .filter_map(|line| line.trim().strip_suffix(": test"))
        .map(str::to_owned)
        .collect()
}

fn require_exact_test_inventory(
    label: &str,
    actual: &BTreeSet<String>,
    expected: &[&str],
    prefix: Option<&str>,
) -> Result<()> {
    let scoped_actual = actual
        .iter()
        .filter(|name| prefix.is_none_or(|prefix| name.starts_with(prefix)))
        .cloned()
        .collect::<BTreeSet<_>>();
    let expected = expected
        .iter()
        .map(|name| (*name).to_string())
        .collect::<BTreeSet<_>>();
    if scoped_actual == expected {
        return Ok(());
    }
    let missing = expected
        .difference(&scoped_actual)
        .cloned()
        .collect::<Vec<_>>();
    let unexpected = scoped_actual
        .difference(&expected)
        .cloned()
        .collect::<Vec<_>>();
    Err(error(format!(
        "required {label} test inventory changed; missing={missing:?}; unexpected={unexpected:?}"
    )))
}

fn run_cross_process_example(root: &Path, directory: &Path, state: &SessionState) -> Result<()> {
    check_signal()?;
    let mut build = Command::new("cargo");
    build.current_dir(root).args([
        "build",
        "--locked",
        "-p",
        "obzenflow",
        "--features",
        "postgres",
        "--example",
        "postgres_sink_payments",
    ]);
    let status = build.status()?;
    if !status.success() {
        return Err(error(format!(
            "PostgreSQL cross-process example build failed with status {status}"
        )));
    }

    let binary = example_binary(root);
    let before = executable_identity(&binary)?;
    let journal_root = directory.join("example-journals");
    let schema = format!("obz083c_example_{}", state.run_id);
    run_example_process(root, &binary, &journal_root, &schema, state, &[])?;
    let live_runs = example_run_directories(&journal_root)?;
    let [live_run] = live_runs.as_slice() else {
        return Err(error(format!(
            "live PostgreSQL example must create exactly one archive, found {}",
            live_runs.len()
        )));
    };

    check_signal()?;
    let replay_args = vec![
        OsString::from("--replay-from"),
        live_run.as_os_str().to_os_string(),
        OsString::from("--verify"),
    ];
    run_example_process(root, &binary, &journal_root, &schema, state, &replay_args)?;
    let final_runs = example_run_directories(&journal_root)?;
    if final_runs.len() != 2 || !final_runs.contains(live_run) {
        return Err(error(format!(
            "verified PostgreSQL replay must preserve the live archive and create one replay archive; found {} archives",
            final_runs.len()
        )));
    }
    let after = executable_identity(&binary)?;
    if after != before {
        return Err(error(
            "the PostgreSQL example executable changed between live and replay processes",
        ));
    }
    assert_tree_excludes(
        &journal_root,
        &[POSTGRES_PASSWORD, &plaintext_url(state.port)],
    )?;
    println!(
        "PostgreSQL example passed live and verified replay in separate processes using {}",
        binary.display()
    );
    Ok(())
}

fn example_binary(root: &Path) -> PathBuf {
    let target = env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                root.join(path)
            }
        })
        .unwrap_or_else(|| root.join("target"));
    target
        .join("debug")
        .join("examples")
        .join(format!("postgres_sink_payments{}", env::consts::EXE_SUFFIX))
}

fn executable_identity(path: &Path) -> Result<(u64, std::time::SystemTime)> {
    let metadata = fs::metadata(path).map_err(|_| {
        error(format!(
            "compiled PostgreSQL example was not found at {}",
            path.display()
        ))
    })?;
    Ok((metadata.len(), metadata.modified()?))
}

fn run_example_process(
    root: &Path,
    binary: &Path,
    journal_root: &Path,
    schema: &str,
    state: &SessionState,
    args: &[OsString],
) -> Result<()> {
    let status = Command::new(binary)
        .current_dir(root)
        .args(args)
        .env("OBZENFLOW_POSTGRES_URL", plaintext_url(state.port))
        .env("OBZENFLOW_POSTGRES_SCHEMA", schema)
        .env("OBZENFLOW_JOURNAL_ROOT", journal_root)
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(error(format!(
            "PostgreSQL example process failed with status {status}"
        )))
    }
}

fn example_run_directories(root: &Path) -> Result<Vec<PathBuf>> {
    let flows = root.join("flows");
    if !flows.is_dir() {
        return Ok(Vec::new());
    }
    let mut runs = Vec::new();
    for entry in fs::read_dir(flows)? {
        let path = entry?.path();
        if path.join("run_manifest.json").is_file() {
            runs.push(path);
        }
    }
    runs.sort();
    Ok(runs)
}

fn assert_tree_excludes(root: &Path, forbidden: &[&str]) -> Result<()> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            for entry in fs::read_dir(&path)? {
                pending.push(entry?.path());
            }
            continue;
        }
        let bytes = fs::read(&path)?;
        for value in forbidden {
            if !value.is_empty()
                && bytes
                    .windows(value.len())
                    .any(|window| window == value.as_bytes())
            {
                return Err(error(format!(
                    "PostgreSQL proof archive contains forbidden connection material in {}",
                    path.display()
                )));
            }
        }
    }
    Ok(())
}

fn redact_proof_output(output: &[u8], state: &SessionState) -> String {
    String::from_utf8_lossy(output)
        .replace(&plaintext_url(state.port), "[REDACTED_POSTGRES_URL]")
        .replace(POSTGRES_PASSWORD, "[REDACTED]")
}

fn prove_concurrent_session_isolation(
    root: &Path,
    compose: &ComposeCommand,
    primary_directory: &Path,
    primary: &SessionState,
) -> Result<()> {
    let peer_run_id = unique_run_id();
    let peer_directory = root.join(SESSION_ROOT).join(&peer_run_id);
    fs::create_dir_all(&peer_directory)?;
    let mut peer = SessionState {
        project: format!("obzenflow-test-{peer_run_id}"),
        run_id: peer_run_id,
        port: 0,
        mode: SessionMode::Test,
    };
    if let Err(tls_error) = ensure_tls(&peer_directory) {
        let _ = fs::remove_dir_all(&peer_directory);
        return Err(tls_error);
    }
    write_state(&peer_directory.join(STATE_FILE), &peer)?;

    let proof = (|| {
        start_session(root, compose, &peer_directory, &mut peer)?;
        write_state(&peer_directory.join(STATE_FILE), &peer)?;
        if primary.project == peer.project
            || primary.run_id == peer.run_id
            || primary_directory == peer_directory
            || primary.port == 0
            || peer.port == 0
            || primary.port == peer.port
        {
            return Err(error(
                "concurrent PostgreSQL test sessions did not receive distinct project, run, directory, and port identities",
            ));
        }
        if primary.project == development_project(root)
            || peer.project == development_project(root)
            || primary_directory == root.join(SESSION_ROOT).join(DEVELOPMENT_SESSION)
            || peer_directory == root.join(SESSION_ROOT).join(DEVELOPMENT_SESSION)
        {
            return Err(error(
                "ephemeral PostgreSQL session identity collided with the persistent development session",
            ));
        }
        let primary_schema = format!("obz083c_application_{}", primary.run_id);
        let peer_schema = format!("obz083c_application_{}", peer.run_id);
        if primary_schema == peer_schema {
            return Err(error(
                "concurrent PostgreSQL sessions derived the same application schema",
            ));
        }
        let primary_health = container_health(root, compose, primary_directory, primary)?;
        let peer_health = container_health(root, compose, &peer_directory, &peer)?;
        if primary_health != "healthy" || peer_health != "healthy" {
            return Err(error(format!(
                "concurrent PostgreSQL sessions were not simultaneously healthy; primary={primary_health}, peer={peer_health}"
            )));
        }
        Ok(())
    })();

    if proof.is_err() {
        let _ = capture_logs(root, compose, &peer_directory, &peer);
    }
    let cleanup = stop_session(root, compose, &peer_directory, &peer, true);
    let key_cleanup = remove_ephemeral_keys(&peer_directory);
    match (proof, cleanup, key_cleanup) {
        (Ok(()), Ok(()), Ok(())) => {
            fs::remove_dir_all(&peer_directory)?;
            println!(
                "PostgreSQL concurrent-session proof passed; ports {} and {} were Docker-assigned",
                primary.port, peer.port
            );
            Ok(())
        }
        (Err(proof), cleanup, key_cleanup) => Err(error(format!(
            "{proof}; peer_cleanup={}; peer_key_cleanup={}; peer state: {}",
            result_label(cleanup),
            result_label(key_cleanup),
            peer_directory.join(STATE_FILE).display()
        ))),
        (Ok(()), Err(cleanup), key_cleanup) => Err(error(format!(
            "concurrent PostgreSQL session proof passed but peer cleanup failed: {cleanup}; peer_key_cleanup={}; peer state: {}",
            result_label(key_cleanup),
            peer_directory.join(STATE_FILE).display()
        ))),
        (Ok(()), Ok(()), Err(key_cleanup)) => Err(error(format!(
            "concurrent PostgreSQL session proof passed but peer key cleanup failed: {key_cleanup}; peer state: {}",
            peer_directory.join(STATE_FILE).display()
        ))),
    }
}

fn start_session(
    root: &Path,
    compose: &ComposeCommand,
    directory: &Path,
    state: &mut SessionState,
) -> Result<()> {
    let output = compose_output(root, compose, directory, state, &["up", "-d"])?;
    if !output.status.success() {
        let _ = capture_logs(root, compose, directory, state);
        return Err(error(format!(
            "PostgreSQL failed to start. project={} image={} port=dynamic captured logs: {}",
            state.project,
            IMAGE,
            directory.join(LOG_FILE).display()
        )));
    }
    wait_healthy(root, compose, directory, state)?;
    verify_server(root, compose, directory, state)?;
    state.port = published_port(root, compose, directory, state)?;
    Ok(())
}

fn stop_session(
    root: &Path,
    compose: &ComposeCommand,
    directory: &Path,
    state: &SessionState,
    volumes: bool,
) -> Result<()> {
    let mut args = vec!["down", "--remove-orphans"];
    if volumes {
        args.push("--volumes");
    }
    let output = compose_output(root, compose, directory, state, &args)?;
    if output.status.success() {
        Ok(())
    } else {
        Err(error(format!(
            "failed to stop exact PostgreSQL project '{}'",
            state.project
        )))
    }
}

fn wait_healthy(
    root: &Path,
    compose: &ComposeCommand,
    directory: &Path,
    state: &SessionState,
) -> Result<()> {
    let deadline = Instant::now() + HEALTH_TIMEOUT;
    loop {
        check_signal()?;
        let health = container_health(root, compose, directory, state)?;
        if health == "healthy" {
            return Ok(());
        }
        if health == "unhealthy" || Instant::now() >= deadline {
            let _ = capture_logs(root, compose, directory, state);
            return Err(error(format!(
                "PostgreSQL did not become healthy within 30s. project={} image={} port=dynamic captured logs: {}",
                state.project,
                IMAGE,
                directory.join(LOG_FILE).display()
            )));
        }
        thread::sleep(Duration::from_millis(250));
    }
}

fn verify_server(
    root: &Path,
    compose: &ComposeCommand,
    directory: &Path,
    state: &SessionState,
) -> Result<()> {
    let output = compose_output(
        root,
        compose,
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
    )?;
    if !output.status.success() {
        return Err(error("PostgreSQL readiness preflight failed"));
    }
    let lines = String::from_utf8_lossy(&output.stdout)
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
        return Err(error("PostgreSQL proof service must run major version 17"));
    }
    if lines.get(1).map(String::as_str) != Some("on") {
        return Err(error("PostgreSQL proof service must keep fsync enabled"));
    }
    Ok(())
}

fn container_health(
    root: &Path,
    compose: &ComposeCommand,
    directory: &Path,
    state: &SessionState,
) -> Result<String> {
    let output = compose_output(root, compose, directory, state, &["ps", "-q", "postgres"])?;
    if !output.status.success() {
        return Ok("unavailable".to_string());
    }
    let id = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if id.is_empty() {
        return Ok("stopped".to_string());
    }
    let output = Command::new("docker")
        .args(["inspect", "--format", "{{.State.Health.Status}}", &id])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()?;
    if !output.status.success() {
        return Ok("unavailable".to_string());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn published_port(
    root: &Path,
    compose: &ComposeCommand,
    directory: &Path,
    state: &SessionState,
) -> Result<u16> {
    let output = compose_output(
        root,
        compose,
        directory,
        state,
        &["port", "postgres", "5432"],
    )?;
    if !output.status.success() {
        return Err(error(format!(
            "failed to discover Docker-assigned PostgreSQL port for project '{}'",
            state.project
        )));
    }
    parse_published_port(&String::from_utf8_lossy(&output.stdout))
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

fn capture_logs(
    root: &Path,
    compose: &ComposeCommand,
    directory: &Path,
    state: &SessionState,
) -> Result<()> {
    let output = compose_output(
        root,
        compose,
        directory,
        state,
        &["logs", "--no-color", "postgres"],
    )?;
    let mut file = File::create(directory.join(LOG_FILE))?;
    file.write_all(&output.stdout)?;
    file.write_all(&output.stderr)?;
    Ok(())
}

fn compose_output(
    root: &Path,
    compose: &ComposeCommand,
    directory: &Path,
    state: &SessionState,
    args: &[&str],
) -> Result<Output> {
    let mut command = Command::new(&compose.program);
    command
        .current_dir(root)
        .args(&compose.prefix)
        .arg("-f")
        .arg(root.join(COMPOSE_FILE))
        .arg("-p")
        .arg(&state.project)
        .args(args)
        .env("OBZENFLOW_POSTGRES_PASSWORD", POSTGRES_PASSWORD)
        .env("OBZENFLOW_POSTGRES_TLS_DIR", directory.join("tls"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    Ok(command.output()?)
}

fn preflight_docker() -> Result<ComposeCommand> {
    let compose = if command_succeeds("docker", &["compose", "version"]) {
        ComposeCommand {
            program: "docker".to_string(),
            prefix: vec!["compose".to_string()],
        }
    } else if command_succeeds("docker-compose", &["version"]) {
        ComposeCommand {
            program: "docker-compose".to_string(),
            prefix: Vec::new(),
        }
    } else {
        return Err(error(
            "Docker Compose is unavailable; install either `docker compose` or `docker-compose`",
        ));
    };
    let context =
        command_stdout("docker", &["context", "show"]).unwrap_or_else(|| "unknown".to_string());
    let info = Command::new("docker")
        .arg("info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    if !info.is_ok_and(|status| status.success()) {
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

fn ensure_tls(directory: &Path) -> Result<()> {
    let tls = directory.join("tls");
    fs::create_dir_all(&tls)?;
    let expected = [
        "ca.crt",
        "ca.key",
        "server.crt",
        "server.key",
        "untrusted-ca.crt",
        "untrusted-ca.key",
    ];
    if expected.iter().all(|name| tls.join(name).is_file()) {
        return Ok(());
    }
    if tls.is_dir() {
        fs::remove_dir_all(&tls)?;
        fs::create_dir_all(&tls)?;
    }
    if !command_succeeds("openssl", &["version"]) {
        return Err(error(
            "OpenSSL is required to generate the PostgreSQL proof certificate",
        ));
    }

    let ca_key = tls.join("ca.key");
    let ca_cert = tls.join("ca.crt");
    let server_key = tls.join("server.key");
    let server_csr = tls.join("server.csr");
    let server_cert = tls.join("server.crt");
    let extensions = tls.join("server.ext");
    let untrusted_ca_key = tls.join("untrusted-ca.key");
    let untrusted_ca_cert = tls.join("untrusted-ca.crt");
    fs::write(
        &extensions,
        "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n",
    )?;

    run_redacted(
        "openssl",
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-days",
            "7",
            "-nodes",
            "-subj",
            "/CN=ObzenFlow PostgreSQL Test CA",
            "-keyout",
            path_arg(&ca_key)?,
            "-out",
            path_arg(&ca_cert)?,
        ],
    )?;
    run_redacted(
        "openssl",
        &[
            "req",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-keyout",
            path_arg(&server_key)?,
            "-out",
            path_arg(&server_csr)?,
        ],
    )?;
    run_redacted(
        "openssl",
        &[
            "x509",
            "-req",
            "-in",
            path_arg(&server_csr)?,
            "-CA",
            path_arg(&ca_cert)?,
            "-CAkey",
            path_arg(&ca_key)?,
            "-CAcreateserial",
            "-days",
            "7",
            "-sha256",
            "-extfile",
            path_arg(&extensions)?,
            "-out",
            path_arg(&server_cert)?,
        ],
    )?;
    run_redacted(
        "openssl",
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-days",
            "7",
            "-nodes",
            "-subj",
            "/CN=ObzenFlow Untrusted PostgreSQL Test CA",
            "-keyout",
            path_arg(&untrusted_ca_key)?,
            "-out",
            path_arg(&untrusted_ca_cert)?,
        ],
    )?;
    set_private_permissions(&ca_key)?;
    set_private_permissions(&server_key)?;
    set_private_permissions(&untrusted_ca_key)?;
    Ok(())
}

fn remove_ephemeral_keys(directory: &Path) -> Result<()> {
    let tls = directory.join("tls");
    if tls.is_dir() {
        fs::remove_dir_all(tls)?;
    }
    Ok(())
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

fn run_redacted(program: &str, args: &[&str]) -> Result<()> {
    let status = Command::new(program)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|_| error(format!("failed to launch {program}")))?;
    if status.success() {
        Ok(())
    } else {
        Err(error(format!("{program} failed with status {status}")))
    }
}

fn path_arg(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| error("PostgreSQL session path is not valid Unicode"))
}

fn write_state(path: &Path, state: &SessionState) -> Result<()> {
    let mut file = File::create(path)?;
    writeln!(file, "# obzenflow xtask postgres v1")?;
    writeln!(file, "project\t{}", state.project)?;
    writeln!(file, "run_id\t{}", state.run_id)?;
    writeln!(file, "port\t{}", state.port)?;
    writeln!(file, "mode\t{}", state.mode.as_str())?;
    Ok(())
}

fn read_state(path: &Path) -> Result<SessionState> {
    let contents = fs::read_to_string(path)?;
    let mut project = None;
    let mut run_id = None;
    let mut port = None;
    let mut mode = None;
    for line in contents.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = line
            .split_once('\t')
            .ok_or_else(|| error("invalid PostgreSQL session state"))?;
        match key {
            "project" => project = Some(value.to_string()),
            "run_id" => run_id = Some(value.to_string()),
            "port" => port = Some(value.parse::<u16>()?),
            "mode" => mode = Some(SessionMode::parse(value)?),
            _ => return Err(error("invalid PostgreSQL session state field")),
        }
    }
    Ok(SessionState {
        project: project.ok_or_else(|| error("PostgreSQL session project is missing"))?,
        run_id: run_id.ok_or_else(|| error("PostgreSQL session run id is missing"))?,
        port: port.ok_or_else(|| error("PostgreSQL session port is missing"))?,
        mode: mode.ok_or_else(|| error("PostgreSQL session mode is missing"))?,
    })
}

fn report_stale_test_sessions(root: &Path, compose: Option<&ComposeCommand>) -> Result<()> {
    let session_root = root.join(SESSION_ROOT);
    if !session_root.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(&session_root)? {
        let entry = entry?;
        if entry.file_name() == DEVELOPMENT_SESSION {
            continue;
        }
        let path = entry.path();
        let state_path = path.join(STATE_FILE);
        if !state_path.is_file() {
            continue;
        }
        let state = read_state(&state_path)?;
        if state.mode != SessionMode::Test {
            continue;
        }
        let health = match compose {
            Some(compose) => container_health(root, compose, &path, &state)?,
            None => "unknown".to_string(),
        };
        if !matches!(health.as_str(), "stopped" | "unavailable") {
            println!(
                "stale PostgreSQL test project detected: project={} run_id={} health={}; exact state: {}",
                state.project,
                state.run_id,
                health,
                state_path.display()
            );
            println!(
                "clean it with: cargo xtask postgres cleanup {}",
                state.run_id
            );
        }
    }
    Ok(())
}

fn development_state_path(root: &Path) -> PathBuf {
    root.join(SESSION_ROOT)
        .join(DEVELOPMENT_SESSION)
        .join(STATE_FILE)
}

fn unique_run_id() -> String {
    Uuid::new_v4().simple().to_string()
}

fn development_project(root: &Path) -> String {
    let canonical = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    canonical.hash(&mut hasher);
    format!("obzenflow-postgres-{:x}", hasher.finish())
}

fn plaintext_url(port: u16) -> String {
    format!(
        "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:{port}/{POSTGRES_DATABASE}?sslmode=disable"
    )
}

fn tls_url(port: u16, host: &str) -> String {
    format!(
        "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{host}:{port}/{POSTGRES_DATABASE}?sslmode=verify-full"
    )
}

fn print_status(state: &SessionState, health: &str) {
    println!(
        "project={} image={} health={} host=127.0.0.1 port={}",
        state.project, IMAGE, health, state.port
    );
}

fn reject_flags(command: &str, flags: &[String]) -> Result<()> {
    if flags.is_empty() {
        return Ok(());
    }
    if flags.len() == 1 && matches!(flags[0].as_str(), "-h" | "--help" | "help") {
        print_help();
        return Ok(());
    }
    Err(error(format!("{command} accepts no options")))
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

fn result_label(result: Result<()>) -> String {
    match result {
        Ok(()) => "ok".to_string(),
        Err(error) => error.to_string(),
    }
}

fn print_help() {
    println!("usage:");
    println!("  cargo xtask postgres up");
    println!("  cargo xtask postgres status");
    println!("  cargo xtask postgres run -- <command> [args...]");
    println!("  cargo xtask postgres test");
    println!("  cargo xtask postgres logs");
    println!("  cargo xtask postgres down [--volumes]");
    println!("  cargo xtask postgres cleanup <reported-test-run-id>");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_round_trips_without_credentials() {
        let directory = env::temp_dir().join(format!("obzenflow-postgres-{}", unique_run_id()));
        fs::create_dir_all(&directory).expect("create test directory");
        let path = directory.join(STATE_FILE);
        let state = SessionState {
            project: "obzenflow-test-state".to_string(),
            run_id: "run-state".to_string(),
            port: 15432,
            mode: SessionMode::Test,
        };
        write_state(&path, &state).expect("write state");
        let contents = fs::read_to_string(&path).expect("read state");
        assert!(!contents.contains(POSTGRES_PASSWORD));
        assert!(!contents.contains("postgres://"));
        let restored = read_state(&path).expect("restore state");
        assert_eq!(restored.project, state.project);
        assert_eq!(restored.run_id, state.run_id);
        assert_eq!(restored.port, state.port);
        assert_eq!(restored.mode, state.mode);
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn published_port_parser_accepts_only_loopback_dynamic_mapping() {
        assert_eq!(
            parse_published_port("127.0.0.1:49152\n").expect("loopback mapping"),
            49152
        );
        assert!(parse_published_port("0.0.0.0:49152\n").is_err());
        assert!(parse_published_port("127.0.0.1:0\n").is_err());
        assert!(parse_published_port("127.0.0.1:49152\n127.0.0.1:49153\n").is_err());
    }

    #[test]
    fn required_test_inventory_rejects_missing_or_unexpected_tests() {
        let exact = listed_test_names("postgres::tests::one: test\npostgres::tests::two: test\n");
        require_exact_test_inventory(
            "fixture",
            &exact,
            &["postgres::tests::one", "postgres::tests::two"],
            Some("postgres::tests::"),
        )
        .expect("exact inventory");

        let missing = listed_test_names("postgres::tests::one: test\n");
        assert!(require_exact_test_inventory(
            "fixture",
            &missing,
            &["postgres::tests::one", "postgres::tests::two"],
            Some("postgres::tests::"),
        )
        .is_err());

        let unexpected = listed_test_names(
            "postgres::tests::one: test\npostgres::tests::two: test\npostgres::tests::three: test\n",
        );
        assert!(require_exact_test_inventory(
            "fixture",
            &unexpected,
            &["postgres::tests::one", "postgres::tests::two"],
            Some("postgres::tests::"),
        )
        .is_err());
    }
}
