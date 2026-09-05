// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::web::{RoutePolicy, SurfacePolicy};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

const KEY_A: &str = "OBZENFLOW_TEST_AUTH_A";
const KEY_B: &str = "OBZENFLOW_TEST_AUTH_B";
const EMPTY: &str = "OBZENFLOW_TEST_AUTH_EMPTY";
const MISSING: &str = "OBZENFLOW_TEST_AUTH_MISSING";
const NON_UNICODE: &str = "OBZENFLOW_TEST_AUTH_NON_UNICODE";
const SECRET_A: &str = "auth-a-test-sentinel";
const SECRET_B: &str = "auth-b-test-sentinel";
const NON_UNICODE_SENTINEL: &str = "non-unicode-test-sentinel";

// Configure credentials before the child process starts. This also makes the
// older Warp auth tests safe under the parallel libtest runner, without set_var.
pub(super) fn with_auth_env(test_name: &str) -> bool {
    const CHILD: &str = "OBZENFLOW_AUTH_TEST_CHILD";
    if std::env::var(CHILD).as_deref() == Ok(test_name) {
        return true;
    }
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", test_name, "--nocapture"])
        .env(CHILD, test_name)
        .env(KEY_A, SECRET_A)
        .env(KEY_B, SECRET_B)
        .env(EMPTY, "")
        .env_remove(MISSING)
        .env("OBZENFLOW_TEST_API_KEY_V1", "sekret")
        .env("OBZENFLOW_TEST_HMAC_SECRET_V1", "sekret")
        .env("OBZENFLOW_TEST_CONTROL_PLANE_API_KEY", "Bearer sekret");
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        let mut bytes = NON_UNICODE_SENTINEL.as_bytes().to_vec();
        bytes.push(0xff);
        command.env(NON_UNICODE, std::ffi::OsString::from_vec(bytes));
    }
    let status = command.status().expect("start authentication test process");
    assert!(
        status.success(),
        "authentication regression failed: {test_name}"
    );
    false
}

fn api_key(env_name: &str) -> AuthPolicy {
    AuthPolicy::ApiKey {
        header: "X-Api-Key".to_string(),
        value_env: env_name.to_string(),
    }
}

fn hmac(env_name: &str) -> AuthPolicy {
    AuthPolicy::HmacSha256 {
        secret_env: env_name.to_string(),
        signature_header: "X-Signature".to_string(),
        body_hash: "raw_body".to_string(),
        timestamp_header: None,
        replay_window_secs: None,
    }
}

fn managed(surface: Option<AuthPolicy>, route: Option<AuthPolicy>) -> ManagedRouteInfo {
    ManagedRouteInfo {
        kind: RouteKind::Unary,
        surface_policy: Some(SurfacePolicy {
            auth: surface,
            ..SurfacePolicy::default()
        }),
        route_policy: RoutePolicy {
            auth: route,
            ..RoutePolicy::default()
        },
    }
}

#[test]
fn material_validation_rejects_unusable_values_without_retaining_them() {
    for (value, reason) in [
        (Err(std::env::VarError::NotPresent), "missing"),
        (
            Err(std::env::VarError::NotUnicode(NON_UNICODE_SENTINEL.into())),
            "non-Unicode",
        ),
        (Ok(String::new()), "empty"),
    ] {
        assert!(matches!(validate_secret_material(value), Err(actual) if actual == reason));
    }
    for value in [" ", "\t", SECRET_A] {
        assert!(
            matches!(validate_secret_material(Ok(value.into())), Ok(actual) if actual == value)
        );
    }
}

#[test]
fn protected_surface_policy_matrix() {
    let a = api_key(KEY_A);
    let b = api_key(KEY_B);
    let h = hmac(KEY_A);
    // Each row gives an explicit expected selection; outer None means conflict.
    let cases = [
        (Some(a.clone()), None, Some(Some(a.clone()))),
        (Some(a.clone()), Some(a.clone()), Some(Some(a.clone()))),
        (Some(a.clone()), Some(AuthPolicy::None), None),
        (Some(a.clone()), Some(b.clone()), None),
        (Some(a.clone()), Some(h.clone()), None),
        (Some(h.clone()), None, Some(Some(h.clone()))),
        (Some(h.clone()), Some(h.clone()), Some(Some(h.clone()))),
        (Some(h.clone()), Some(AuthPolicy::None), None),
        (None, Some(a.clone()), Some(Some(a.clone()))),
        (Some(AuthPolicy::None), Some(a.clone()), Some(Some(a))),
        (None, None, Some(None)),
        (None, Some(AuthPolicy::None), Some(Some(AuthPolicy::None))),
        (Some(AuthPolicy::None), None, Some(Some(AuthPolicy::None))),
    ];
    for (surface, route, expected) in cases {
        let result = resolve_managed_auth(&managed(surface, route), "/reports");
        match expected {
            Some(expected) => assert_eq!(result.unwrap(), expected),
            None => assert!(matches!(
                result,
                Err(WebError::EndpointRegistrationFailed { .. })
            )),
        }
    }
    let mut timestamped = hmac(KEY_A);
    if let AuthPolicy::HmacSha256 {
        timestamp_header,
        replay_window_secs,
        ..
    } = &mut timestamped
    {
        *timestamp_header = Some("X-Timestamp".into());
        *replay_window_secs = Some(30);
    }
    assert!(
        resolve_managed_auth(&managed(Some(timestamped), Some(hmac(KEY_A))), "/reports").is_err()
    );
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn unusable_material_fails_closed_and_diagnostics_are_redacted() {
    if !with_auth_env("web::warp::warp_server::auth_tests::unusable_material_fails_closed_and_diagnostics_are_redacted") { return; }
    let capture = Capture::default();
    let writer = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let mut invalid_refs = vec![MISSING, EMPTY];
    if cfg!(unix) {
        invalid_refs.push(NON_UNICODE);
    }
    for env_name in invalid_refs {
        for auth in [api_key(env_name), hmac(env_name)] {
            let error = validate_auth_policy_startup(&auth).unwrap_err();
            let diagnostic = format!("{error} {error:?}");
            assert!(diagnostic.contains(env_name));
            assert!(!diagnostic.contains(NON_UNICODE_SENTINEL));
            let response = enforce_auth_policy(&auth, &HashMap::new(), b"body").unwrap_err();
            assert_eq!(response.status, 500);
            assert!(response.body == b"Internal Server Error");
        }
    }
    let empty_signature =
        ring::hmac::sign(&ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b""), b"body");
    let signature = empty_signature
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let headers = HashMap::from([("X-Signature".into(), signature)]);
    assert_eq!(
        enforce_auth_policy(&hmac(EMPTY), &headers, b"body")
            .unwrap_err()
            .status,
        500
    );
    let logs = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("Authentication policy is misconfigured"));
    for sentinel in [SECRET_A, SECRET_B, NON_UNICODE_SENTINEL] {
        assert!(!logs.contains(sentinel));
    }
}

#[test]
fn malformed_hmac_inputs_return_unauthorized_without_panicking() {
    if !with_auth_env("web::warp::warp_server::auth_tests::malformed_hmac_inputs_return_unauthorized_without_panicking") { return; }
    let mut auth = hmac(KEY_A);
    if let AuthPolicy::HmacSha256 {
        timestamp_header,
        replay_window_secs,
        ..
    } = &mut auth
    {
        *timestamp_header = Some("X-Timestamp".into());
        *replay_window_secs = Some(30);
    }
    for ts in [i64::MIN.to_string(), i64::MAX.to_string(), "invalid".into()] {
        let headers = HashMap::from([
            ("X-Timestamp".into(), ts),
            ("X-Signature".into(), "00".into()),
        ]);
        assert_eq!(
            enforce_auth_policy(&auth, &headers, b"")
                .unwrap_err()
                .status,
            401
        );
    }
    for signature in ["a€", "xx", "0", ""] {
        let headers = HashMap::from([("X-Signature".into(), signature.into())]);
        assert_eq!(
            enforce_auth_policy(&hmac(KEY_A), &headers, b"")
                .unwrap_err()
                .status,
            401
        );
    }
}

struct CountingEndpoint {
    path: &'static str,
    policy: Option<ManagedRouteInfo>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl HttpEndpoint for CountingEndpoint {
    fn path(&self) -> &str {
        self.path
    }
    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Get, HttpMethod::Post]
    }
    fn managed_route(&self) -> Option<ManagedRouteInfo> {
        self.policy.clone()
    }
    async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Response::ok().into())
    }
}

#[tokio::test]
async fn admitted_routes_enforce_their_own_credentials_before_handlers() {
    if !with_auth_env("web::warp::warp_server::auth_tests::admitted_routes_enforce_their_own_credentials_before_handlers") { return; }
    let calls = Arc::new(AtomicUsize::new(0));
    let mut server = WarpServer::new();
    for (path, policy) in [
        ("/reports/new", Some(managed(Some(api_key(KEY_A)), None))),
        ("/reports/health", Some(managed(Some(api_key(KEY_A)), None))),
        ("/partners/b", Some(managed(Some(api_key(KEY_B)), None))),
        ("/mixed/private", Some(managed(None, Some(api_key(KEY_B))))),
        ("/mixed/public", Some(managed(None, Some(AuthPolicy::None)))),
        ("/api/topology", None),
        ("/api/flow/control", None),
        ("/health", None),
        ("/ready", None),
    ] {
        server
            .register_endpoint(Box::new(CountingEndpoint {
                path,
                policy,
                calls: calls.clone(),
            }))
            .unwrap();
    }
    let config = ServerConfig {
        control_plane_auth: Some(api_key(KEY_A)),
        ..ServerConfig::localhost(0)
    };
    let filter = server
        .build_filter(build_host_policy(&config, &server.endpoints, false).unwrap())
        .unwrap();
    for method in ["GET", "POST"] {
        for (path, key, other) in [
            ("/reports/new", SECRET_A, SECRET_B),
            ("/reports/health", SECRET_A, SECRET_B),
            ("/partners/b", SECRET_B, SECRET_A),
            ("/mixed/private", SECRET_B, SECRET_A),
            ("/api/topology", SECRET_A, SECRET_B),
            ("/api/flow/control", SECRET_A, SECRET_B),
        ] {
            for candidate in [None, Some(other)] {
                let before = calls.load(Ordering::SeqCst);
                let request = warp::test::request().method(method).path(path).body("body");
                let request = if let Some(value) = candidate {
                    request.header("x-api-key", value)
                } else {
                    request
                };
                assert_eq!(request.reply(&filter).await.status(), 401);
                assert_eq!(calls.load(Ordering::SeqCst), before);
            }
            let before = calls.load(Ordering::SeqCst);
            assert_eq!(
                warp::test::request()
                    .method(method)
                    .path(path)
                    .body("body")
                    .header("x-api-key", key)
                    .reply(&filter)
                    .await
                    .status(),
                200
            );
            assert_eq!(calls.load(Ordering::SeqCst), before + 1);
        }
    }
    for path in ["/mixed/public", "/health", "/ready"] {
        assert_eq!(
            warp::test::request()
                .path(path)
                .reply(&filter)
                .await
                .status(),
            200
        );
    }
}

#[tokio::test]
async fn sse_rejects_invalid_current_material_before_opening_a_stream() {
    if !with_auth_env("web::warp::warp_server::auth_tests::sse_rejects_invalid_current_material_before_opening_a_stream") { return; }
    let mut server = WarpServer::new();
    server.with_system_journal(Arc::new(crate::journal::MemoryJournal::<SystemEvent>::new()));
    // Exercise the request boundary directly; production admission rejects this policy earlier.
    let filter = server
        .build_filter(HostPolicy {
            max_body_size_bytes: 1024,
            request_timeout: None,
            control_plane_auth: Some(api_key(EMPTY)),
        })
        .unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        warp::test::request()
            .path("/api/flow/events")
            .reply(&filter),
    )
    .await
    .expect("invalid auth must reject before opening SSE");
    assert_eq!(response.status(), 500);
}

#[tokio::test]
async fn invalid_authentication_reaches_the_startup_caller_before_spawn() {
    if !with_auth_env("web::warp::warp_server::auth_tests::invalid_authentication_reaches_the_startup_caller_before_spawn") { return; }
    use crate::web::web_server::{start_web_server_with_config, WebServerResources};
    let cases = [
        (Some(api_key(EMPTY)), None, false),
        (Some(hmac(MISSING)), None, false),
        (None, Some(managed(Some(api_key(EMPTY)), None)), false),
        (None, Some(managed(None, Some(hmac(EMPTY)))), false),
        (
            None,
            Some(managed(Some(api_key(EMPTY)), Some(api_key(KEY_A)))),
            true,
        ),
        (
            None,
            Some(managed(Some(api_key(KEY_A)), Some(AuthPolicy::None))),
            true,
        ),
    ];
    for (control_plane_auth, policy, conflict) in cases {
        let resources = WebServerResources {
            topology: Arc::new(
                obzenflow_topology::TopologyBuilder::new()
                    .build_unchecked()
                    .unwrap(),
            ),
            contract_attachments: None,
            metrics_exporter: None,
            flow_handle: None,
            extra_endpoints: vec![Box::new(CountingEndpoint {
                path: "/reports",
                policy,
                calls: Arc::new(AtomicUsize::new(0)),
            })],
            surface_metrics: None,
            runtime_config: None,
            runtime_instance_id: None,
            shutdown: None,
        };
        // Auth admission returns an error directly, without a background handle.
        let config = ServerConfig {
            control_plane_auth,
            ..ServerConfig::localhost(0)
        };
        let error = start_web_server_with_config(resources, config)
            .await
            .unwrap_err();
        if conflict {
            assert!(matches!(error, WebError::EndpointRegistrationFailed { .. }));
        } else {
            assert!(matches!(error, WebError::StartupFailed { .. }));
        }
    }
}
