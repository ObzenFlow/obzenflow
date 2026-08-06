// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Architecture drift guards for retired handler-shell and replay-cold source bounds.

use std::fs;
use std::path::{Path, PathBuf};
use syn::visit::Visit;

fn rust_sources_under(path: &Path, output: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(path).expect("read source directory") {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            rust_sources_under(&path, output);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            output.push(path);
        }
    }
}

#[test]
fn generic_handler_shell_and_standalone_retry_vocabulary_stay_absent() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources = vec![root.join("src/lib.rs")];
    for crate_entry in fs::read_dir(root.join("crates")).expect("read crates directory") {
        let source_root = crate_entry.expect("read crate entry").path().join("src");
        if source_root.is_dir() {
            rust_sources_under(&source_root, &mut sources);
        }
    }

    let forbidden = [
        "AsyncTransformHandler",
        "AsyncTransformHandlerAdapter",
        "AsyncTransformDescriptor",
        "AsyncTransformBuilder",
        "PlaceholderAsyncTransform",
        "BoundAsyncTransform",
        "AsyncMap",
        "AsyncMapTyped",
        "AsyncTryMapWith",
        "AsyncTryMapWithTyped",
        "async_map",
        "async_try_map_with",
        "async_transform!",
        "__obzenflow_async_transform_untyped",
        "__obzenflow_async_transform_typed",
        "__obzenflow_async_transform_exact_contract",
        "MiddlewareTransform",
        "UnifiedMiddlewareTransform",
        "AsyncMiddlewareTransform",
        "AsyncTransformHandlerExt",
        "AsyncTransformMiddlewareBuilder",
        "MiddlewareStateful",
        "TransformHandlerExt",
        "StatefulHandlerMiddlewareExt",
        "TransformMiddlewareBuilder",
        "StatefulMiddlewareBuilder",
        "MiddlewareAsyncFiniteSource",
        "MiddlewareAsyncInfiniteSource",
        "MiddlewareFiniteSource",
        "MiddlewareInfiniteSource",
        "AsyncFiniteSourceHandlerExt",
        "AsyncInfiniteSourceHandlerExt",
        "FiniteSourceHandlerExt",
        "InfiniteSourceHandlerExt",
        "AsyncFiniteSourceMiddlewareBuilder",
        "AsyncInfiniteSourceMiddlewareBuilder",
        "FiniteSourceMiddlewareBuilder",
        "InfiniteSourceMiddlewareBuilder",
        "MiddlewareJoin",
        "JoinHandlerMiddlewareExt",
        "JoinMiddlewareBuilder",
        "MiddlewareSink",
        "SinkHandlerExt",
        "SinkMiddlewareBuilder",
        "MiddlewareAction",
        "ErrorAction",
        "legacy_shell",
        "create_for_effect",
        "TopologyMiddlewareConfigSlot::Retry",
        "MiddlewareKind",
        "MiddlewareLifecycle::Retry",
        "RetryEvent",
        "lifecycle.middleware.retry",
        "MiddlewarePlanContribution",
        "AiMapReduceChunkContext",
        "AiMapReduceChunkContextKey",
        "UnifiedMiddlewareStateful",
        "CircuitBreakerShouldRetry",
        "CircuitBreakerRetryDelayMs",
        "CircuitBreakerTotalRetryWallMs",
        "ephemeral_events",
        "emit_ephemeral_event",
    ];

    for source_path in sources {
        let source = fs::read_to_string(&source_path).expect("read Rust source");
        for token in forbidden {
            assert!(
                !source.contains(token),
                "retired token {token:?} resurfaced in {}",
                source_path.display()
            );
        }
    }
}

#[test]
fn standalone_retry_discovery_and_topology_production_stay_absent() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cases = [
        (
            "crates/obzenflow_adapters/src/middleware/hints.rs",
            vec![
                "RetryHint",
                "Attempts",
                "BackoffKind",
                "pub retry:",
                "retry: Option",
            ],
        ),
        (
            "crates/obzenflow_adapters/src/middleware/validation/safety.rs",
            vec![
                "Infinite retry on source",
                "test_infinite_retry_on_source_is_error",
                "MockInfiniteRetryFactory",
            ],
        ),
        (
            "crates/obzenflow_dsl/src/middleware_resolution.rs",
            vec![
                "validate_middleware_combination",
                "hints().retry",
                "retry behaviour without a circuit breaker",
            ],
        ),
        (
            "crates/obzenflow_dsl/src/dsl/dsl.rs",
            vec![
                "let mut retry_config",
                "retry_config.map",
                "TopologyMiddlewareConfigSlot::Retry",
            ],
        ),
        (
            "crates/obzenflow_runtime/src/pipeline/handle.rs",
            vec!["pub retry: Option<serde_json::Value>"],
        ),
        (
            "crates/obzenflow_core/src/event/chain_event/factory/middleware.rs",
            vec![
                "pub fn retry_exhausted(",
                "pub fn retry_attempt_failed(",
                "pub fn retry_succeeded_after_retry(",
            ],
        ),
        (
            "crates/obzenflow_core/src/event/chain_event/model.rs",
            vec!["lifecycle.middleware.retry"],
        ),
        (
            "crates/obzenflow_core/src/event/payloads/observability_payload.rs",
            vec!["Retry(RetryEvent)", "enum RetryEvent"],
        ),
    ];

    for (relative, forbidden) in cases {
        let source = fs::read_to_string(root.join(relative)).expect("read guarded source");
        for token in forbidden {
            assert!(
                !source.contains(token),
                "retired standalone-retry token {token:?} resurfaced in {relative}"
            );
        }
    }
}

#[test]
fn retry_contracts_with_live_non_middleware_owners_stay_present() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cases = [
        (
            "crates/obzenflow_adapters/src/middleware/control/resilience.rs",
            vec![
                "pub struct EffectResilienceBuilder",
                "pub fn retry(mut self, retry: Retry) -> Self",
                "BackoffStrategy",
            ],
        ),
        (
            "crates/obzenflow_adapters/src/middleware/control/circuit_breaker/retry.rs",
            vec!["pub struct Retry"],
        ),
        (
            "crates/obzenflow_core/src/event/payloads/effect_payload.rs",
            vec!["RetryDisposition"],
        ),
        (
            "crates/obzenflow_adapters/src/sources/http_pull.rs",
            vec!["HttpRetryConfig"],
        ),
        (
            "crates/obzenflow_core/src/event/payloads/observability_payload.rs",
            vec![
                "RetryScheduled",
                "RetrySucceeded",
                "RetryExhausted",
                "RetryStoppedNonRetryable",
                "RecoveryCompleted",
            ],
        ),
    ];

    for (relative, required) in cases {
        let source = fs::read_to_string(root.join(relative)).expect("read guarded source");
        for token in required {
            assert!(
                source.contains(token),
                "live retry contract {token:?} disappeared from {relative}"
            );
        }
    }
}

#[test]
fn payment_tutorial_is_proof_free_and_uses_configured_retry() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let example_root = root.join("examples/payment_gateway_resilience");

    assert!(
        !example_root.join("proof.rs").exists(),
        "FLOWIP release witnesses must not live in the user-facing example"
    );

    for relative in [
        "console.rs",
        "deliveries.rs",
        "domain.rs",
        "main.rs",
        "flow.rs",
        "gateway.rs",
        "fixtures.rs",
        "support.rs",
        "validation.rs",
        "obzenflow.toml",
        "README.md",
    ] {
        let source = fs::read_to_string(example_root.join(relative))
            .unwrap_or_else(|error| panic!("read payment tutorial {relative}: {error}"));
        for forbidden in [
            "PAYMENT_DEMO_RETRY_PROOF",
            "GatewayRetryProof",
            "RetryProofProfile",
            "assemble_flow_with_resilience",
            "build_gateway_resilience",
            "with_retry_proof",
            "retry_proof",
        ] {
            assert!(
                !source.contains(forbidden),
                "payment tutorial {relative} must not contain release-witness machinery; found {forbidden:?}"
            );
        }
    }

    let flow =
        fs::read_to_string(example_root.join("flow.rs")).expect("read payment tutorial flow");
    for forbidden in [
        "Option<Retry>",
        ".retry(None)",
        ".retry(Some(",
        "MiddlewareFactory",
        "FnOnce(",
    ] {
        assert!(
            !flow.contains(forbidden),
            "payment tutorial flow must use direct configured retry with no proof injection seam; found {forbidden:?}"
        );
    }
    assert_eq!(
        flow.matches("EffectResilience::with_breaker(gateway_breaker)")
            .count(),
        1,
        "the tutorial must have one concrete resilience construction path"
    );
    for required in [
        "let gateway_retry = Retry::fixed(Duration::from_millis(250))",
        ".max_attempts(3)",
        ".attempt_start_window(Duration::from_secs(30))",
        "let gateway_resilience = EffectResilience::with_breaker(gateway_breaker)",
        ".retry(gateway_retry)",
        ".rate_limit_each_attempt(gateway_limiter)",
    ] {
        assert!(
            flow.contains(required),
            "payment tutorial must show the configured retry golden path; missing {required:?}"
        );
    }
    assert_eq!(
        flow.matches(".retry(").count(),
        1,
        "the tutorial must configure retry exactly once on its direct resilience path"
    );

    let main = fs::read_to_string(example_root.join("main.rs"))
        .expect("read payment tutorial entry point");
    assert!(main.contains(".run_blocking(flow::build_flow())"));

    let witness =
        fs::read_to_string(root.join("tests/test_support/payment_gateway_retry_fixture.rs"))
            .expect("read test-only payment resilience witness");
    for required in [
        "Retry::fixed(",
        "ReleasePolicy::BreakerRecovery => resilience.retry(canonical_recovery())",
    ] {
        assert!(
            witness.contains(required),
            "configured test witness must retain positive retry authoring {required:?}"
        );
    }
}

#[test]
fn middleware_context_is_confined_to_the_typed_policy_substrate() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources = vec![root.join("src/lib.rs")];
    for crate_entry in fs::read_dir(root.join("crates")).expect("read crates directory") {
        let source_root = crate_entry.expect("read crate entry").path().join("src");
        if source_root.is_dir() {
            rust_sources_under(&source_root, &mut sources);
        }
    }

    let allowed = [
        "crates/obzenflow_adapters/src/middleware/",
        "crates/obzenflow_core/src/event/schema/middleware_context_key.rs",
        "crates/obzenflow_core/src/event/schema/mod.rs",
        "crates/obzenflow_core/src/lib.rs",
    ];

    for source_path in sources {
        let source = fs::read_to_string(&source_path).expect("read Rust source");
        let production_source = source
            .split("#[cfg(test)]")
            .next()
            .expect("split always yields one segment");
        if !production_source.contains("MiddlewareContext") {
            continue;
        }
        let relative = source_path
            .strip_prefix(&root)
            .expect("source lives under workspace")
            .to_string_lossy();
        assert!(
            allowed.iter().any(|prefix| relative.starts_with(prefix)),
            "MiddlewareContext escaped the adapter-owned typed policy substrate into {relative}"
        );
    }

    let keys =
        fs::read_to_string(root.join("crates/obzenflow_adapters/src/middleware/context_keys.rs"))
            .expect("read typed context keys");
    for retired in [
        "struct CircuitBreakerAttempt;",
        "struct CircuitBreakerShouldRetry;",
        "struct CircuitBreakerRetryDelayMs;",
        "struct CircuitBreakerTotalRetryWallMs;",
    ] {
        assert!(
            !keys.contains(retired),
            "shell-only typed context key {retired:?} resurfaced"
        );
    }
    for key in [
        "EffectCallDurationNanos",
        "CircuitBreakerIsProbe",
        "CircuitBreakerProbeGeneration",
        "CircuitBreakerProbeSlot",
        "CircuitBreakerRetryAfterMs",
    ] {
        assert!(
            keys.contains(&format!("struct {key}")),
            "typed-policy context key {key} disappeared"
        );
    }
}

#[test]
fn resume_live_handoff_clears_idle_state_in_every_source_supervisor() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for relative in [
        "crates/obzenflow_runtime/src/stages/source/finite/supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/finite/async_supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/infinite/supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/infinite/async_supervisor.rs",
    ] {
        let source = fs::read_to_string(root.join(relative)).expect("read source supervisor");
        let handoff_start = source
            .find("SourceReplayExhaustion::ContinueLive =>")
            .unwrap_or_else(|| panic!("{relative} must handle resume-live exhaustion"));
        let handoff = &source[handoff_start..];
        let reset = handoff
            .find("self.idle_backoff.reset();")
            .unwrap_or_else(|| panic!("{relative} must reset resume-live backoff"));
        let clear_delay = handoff
            .find("self.pending_idle_delay = None;")
            .unwrap_or_else(|| panic!("{relative} must clear its pending resume-live delay"));
        let drop_replay = handoff
            .find("self.replay_driver = None;")
            .unwrap_or_else(|| panic!("{relative} must leave replay before the next live poll"));
        let continue_live = handoff
            .find("Ok(EventLoopDirective::Continue)")
            .unwrap_or_else(|| panic!("{relative} must re-enter the live event loop"));
        assert!(
            reset < continue_live && clear_delay < continue_live && drop_replay < continue_live,
            "{relative} must clear stale idle state before its first resumed live poll"
        );
    }
}

#[test]
fn every_source_supervisor_closes_poll_timing_before_error_normalisation() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for relative in [
        "crates/obzenflow_runtime/src/stages/source/finite/supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/finite/async_supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/infinite/supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/infinite/async_supervisor.rs",
    ] {
        let source = fs::read_to_string(root.join(relative)).expect("read source supervisor");
        let poll_start = source
            .rfind("let poll_started_at = tokio::time::Instant::now();")
            .unwrap_or_else(|| panic!("{relative} must use the paused-clock-testable poll timer"));
        let live_poll = &source[poll_start..];
        let duration_capture = live_poll
            .find("poll_started_at.elapsed()")
            .unwrap_or_else(|| panic!("{relative} must capture raw poll duration"));
        let normalisation = live_poll
            .find("normalise_source_poll_error(")
            .unwrap_or_else(|| panic!("{relative} must retain source-error normalisation"));
        assert!(
            duration_capture < normalisation,
            "{relative} must end poll_duration before normalising the raw result"
        );
    }
}

#[test]
fn eof_and_boundary_rejection_cannot_schedule_idle_delay() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for relative in [
        "crates/obzenflow_runtime/src/stages/source/finite/supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/finite/async_supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/infinite/supervisor.rs",
        "crates/obzenflow_runtime/src/stages/source/infinite/async_supervisor.rs",
    ] {
        let source = fs::read_to_string(root.join(relative)).expect("read source supervisor");

        let rejection_start = source
            .find("SourceBoundaryOutcome::Rejected { reason } =>")
            .unwrap_or_else(|| panic!("{relative} must handle boundary rejection"));
        let rejection_end = source[rejection_start..]
            .find("SourceBoundaryOutcome::Polled(poll) =>")
            .map(|offset| rejection_start + offset)
            .unwrap_or_else(|| panic!("{relative} must handle a successful boundary poll"));
        let rejection_arm = &source[rejection_start..rejection_end];
        assert!(
            !rejection_arm.contains("pending_idle_delay = Some"),
            "{relative} must not schedule idle delay after boundary rejection"
        );

        let eof_start = source
            .find("Ok(SourcePollCompletion::Eof) =>")
            .unwrap_or_else(|| panic!("{relative} must handle live EOF"));
        let eof_end = source[eof_start..]
            .find("Err(e) =>")
            .map(|offset| eof_start + offset)
            .unwrap_or_else(|| panic!("{relative} must handle source poll errors after EOF"));
        let eof_arm = &source[eof_start..eof_end];
        assert!(
            !eof_arm.contains("pending_idle_delay = Some"),
            "{relative} must not schedule idle delay after EOF"
        );
    }
}

#[test]
fn effectful_stateful_keeps_only_its_existing_typed_lowerer() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let descriptor =
        fs::read_to_string(root.join("crates/obzenflow_dsl/src/dsl/stage_descriptor.rs"))
            .expect("read stage descriptor");
    let stateful_config =
        fs::read_to_string(root.join("crates/obzenflow_runtime/src/stages/stateful/config.rs"))
            .expect("read stateful config");

    assert!(descriptor.contains("EffectfulStatefulHandlerAdapter(self.handler)"));
    assert!(descriptor.contains("EffectfulStatefulPendingBoundary"));
    assert!(
        !stateful_config.contains("effect_boundary"),
        "FLOWIP-120l must not be pre-implemented as a StatefulConfig carrier"
    );

    let mut stateful_runtime_sources = Vec::new();
    rust_sources_under(
        &root.join("crates/obzenflow_runtime/src/stages/stateful"),
        &mut stateful_runtime_sources,
    );
    for source_path in stateful_runtime_sources {
        let source = fs::read_to_string(&source_path).expect("read stateful runtime source");
        for token in [
            "UnifiedMiddlewareStateful",
            "StatefulHandlerMiddlewareExt",
            "pre_write",
        ] {
            assert!(
                !source.contains(token),
                "retired stateful shell carrier {token:?} resurfaced in {}",
                source_path.display()
            );
        }
    }
}

#[derive(Default)]
struct HttpGuardAliases {
    reqwest_modules: std::collections::HashSet<String>,
    clients: std::collections::HashSet<String>,
    builders: std::collections::HashSet<String>,
    run_mode_names: std::collections::HashSet<String>,
    imported_run_mode: bool,
}

#[derive(Debug)]
struct UseBinding {
    source: Vec<String>,
    alias: String,
}

#[derive(Default)]
struct HttpGuardAliasCollector {
    bindings: Vec<UseBinding>,
    type_aliases: Vec<(String, Vec<String>)>,
}

fn is_test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attribute| {
        if attribute
            .path()
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "test")
        {
            return true;
        }
        attribute.path().is_ident("cfg")
            && attribute
                .parse_args::<syn::Path>()
                .is_ok_and(|path| path.is_ident("test"))
    })
}

fn collect_use_bindings(
    tree: &syn::UseTree,
    prefix: &mut Vec<String>,
    output: &mut Vec<UseBinding>,
) {
    match tree {
        syn::UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            collect_use_bindings(&path.tree, prefix, output);
            prefix.pop();
        }
        syn::UseTree::Name(name) => {
            let mut source = prefix.clone();
            source.push(name.ident.to_string());
            output.push(UseBinding {
                alias: name.ident.to_string(),
                source,
            });
        }
        syn::UseTree::Rename(rename) => {
            let mut source = prefix.clone();
            source.push(rename.ident.to_string());
            output.push(UseBinding {
                alias: rename.rename.to_string(),
                source,
            });
        }
        syn::UseTree::Group(group) => {
            for tree in &group.items {
                collect_use_bindings(tree, prefix, output);
            }
        }
        syn::UseTree::Glob(_) => {
            let mut source = prefix.clone();
            source.push("*".to_string());
            output.push(UseBinding {
                alias: "*".to_string(),
                source,
            });
        }
    }
}

impl<'ast> Visit<'ast> for HttpGuardAliasCollector {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        if !is_test_only(&item.attrs) {
            syn::visit::visit_item_mod(self, item);
        }
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        if !is_test_only(&item.attrs) {
            syn::visit::visit_item_fn(self, item);
        }
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        if !is_test_only(&item.attrs) {
            syn::visit::visit_impl_item_fn(self, item);
        }
    }

    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        if !is_test_only(&item.attrs) {
            collect_use_bindings(&item.tree, &mut Vec::new(), &mut self.bindings);
        }
    }

    fn visit_item_extern_crate(&mut self, item: &'ast syn::ItemExternCrate) {
        if !is_test_only(&item.attrs) && item.ident == "reqwest" {
            self.bindings.push(UseBinding {
                source: vec!["reqwest".to_string()],
                alias: item
                    .rename
                    .as_ref()
                    .map_or_else(|| item.ident.to_string(), |(_, alias)| alias.to_string()),
            });
        }
    }

    fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
        if is_test_only(&item.attrs) {
            return;
        }
        if let syn::Type::Path(path) = item.ty.as_ref() {
            self.type_aliases.push((
                item.ident.to_string(),
                path.path
                    .segments
                    .iter()
                    .map(|segment| segment.ident.to_string())
                    .collect(),
            ));
        }
        syn::visit::visit_item_type(self, item);
    }
}

fn aliases_for(file: &syn::File) -> HttpGuardAliases {
    let mut collector = HttpGuardAliasCollector::default();
    collector.visit_file(file);

    let mut aliases = HttpGuardAliases::default();
    aliases.reqwest_modules.insert("reqwest".to_string());
    aliases.run_mode_names.extend(
        ["ReplayVerb", "RuntimeMode", "source_phase_for"]
            .into_iter()
            .map(str::to_string),
    );

    let run_mode_symbols = ["ReplayVerb", "RuntimeMode", "source_phase_for"];
    for binding in &collector.bindings {
        if binding
            .source
            .last()
            .is_some_and(|name| run_mode_symbols.contains(&name.as_str()))
        {
            aliases.imported_run_mode = true;
            aliases.run_mode_names.insert(binding.alias.clone());
        }
    }

    let mut changed = true;
    while changed {
        changed = false;
        for binding in &collector.bindings {
            let first = binding.source.first().map(String::as_str);
            let last = binding.source.last().map(String::as_str);
            if binding.source.len() == 1 {
                if last.is_some_and(|name| aliases.reqwest_modules.contains(name)) {
                    changed |= aliases.reqwest_modules.insert(binding.alias.clone());
                }
                if last.is_some_and(|name| aliases.clients.contains(name)) {
                    changed |= aliases.clients.insert(binding.alias.clone());
                }
                if last.is_some_and(|name| aliases.builders.contains(name)) {
                    changed |= aliases.builders.insert(binding.alias.clone());
                }
                if last.is_some_and(|name| aliases.run_mode_names.contains(name)) {
                    changed |= aliases.run_mode_names.insert(binding.alias.clone());
                    aliases.imported_run_mode = true;
                }
            } else if first.is_some_and(|name| aliases.reqwest_modules.contains(name)) {
                match last {
                    Some("Client") => changed |= aliases.clients.insert(binding.alias.clone()),
                    Some("ClientBuilder") => {
                        changed |= aliases.builders.insert(binding.alias.clone())
                    }
                    Some("*") => {
                        changed |= aliases.clients.insert("Client".to_string());
                        changed |= aliases.builders.insert("ClientBuilder".to_string());
                    }
                    _ => {}
                }
            }
        }
        for (alias, source) in &collector.type_aliases {
            let first = source.first().map(String::as_str);
            let last = source.last().map(String::as_str);
            if source.len() == 1 {
                if first.is_some_and(|name| aliases.clients.contains(name)) {
                    changed |= aliases.clients.insert(alias.clone());
                }
                if first.is_some_and(|name| aliases.builders.contains(name)) {
                    changed |= aliases.builders.insert(alias.clone());
                }
                if first.is_some_and(|name| aliases.run_mode_names.contains(name)) {
                    changed |= aliases.run_mode_names.insert(alias.clone());
                    aliases.imported_run_mode = true;
                }
            } else if first.is_some_and(|name| aliases.reqwest_modules.contains(name)) {
                match last {
                    Some("Client") => changed |= aliases.clients.insert(alias.clone()),
                    Some("ClientBuilder") => changed |= aliases.builders.insert(alias.clone()),
                    _ => {}
                }
            }
        }
    }
    aliases
}

struct ReqwestConstructorVisitor<'a> {
    aliases: &'a HttpGuardAliases,
    context: Vec<String>,
    constructors: Vec<(String, String)>,
}

impl ReqwestConstructorVisitor<'_> {
    fn constructor_path(&self, path: &syn::Path) -> Option<String> {
        let parts = path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        let operation = parts.last()?.as_str();
        let owner = parts.get(parts.len().checked_sub(2)?)?.as_str();
        let constructor = match operation {
            "new" | "default" => {
                self.aliases.clients.contains(owner)
                    || self.aliases.builders.contains(owner)
                    || ((owner == "Client" || owner == "ClientBuilder")
                        && parts
                            .get(parts.len().saturating_sub(3))
                            .is_some_and(|part| self.aliases.reqwest_modules.contains(part)))
            }
            "builder" => {
                self.aliases.clients.contains(owner)
                    || (owner == "Client"
                        && parts
                            .get(parts.len().saturating_sub(3))
                            .is_some_and(|part| self.aliases.reqwest_modules.contains(part)))
            }
            _ => false,
        };
        constructor.then(|| parts.join("::"))
    }
}

impl<'ast> Visit<'ast> for ReqwestConstructorVisitor<'_> {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        if !is_test_only(&item.attrs) {
            syn::visit::visit_item_mod(self, item);
        }
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        if is_test_only(&item.attrs) {
            return;
        }
        self.context.push(item.sig.ident.to_string());
        syn::visit::visit_item_fn(self, item);
        self.context.pop();
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        if is_test_only(&item.attrs) {
            return;
        }
        self.context.push(item.sig.ident.to_string());
        syn::visit::visit_impl_item_fn(self, item);
        self.context.pop();
    }

    fn visit_expr_path(&mut self, expression: &'ast syn::ExprPath) {
        if let Some(path) = self.constructor_path(&expression.path) {
            self.constructors.push((
                path,
                self.context
                    .last()
                    .cloned()
                    .unwrap_or_else(|| "<module>".to_string()),
            ));
        }
        syn::visit::visit_expr_path(self, expression);
    }
}

struct RunModeVisitor<'a> {
    aliases: &'a HttpGuardAliases,
    references: Vec<String>,
}

#[derive(Default)]
struct ProductionExpectVisitor {
    calls: Vec<String>,
    context: Vec<String>,
}

impl<'ast> Visit<'ast> for ProductionExpectVisitor {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        if !is_test_only(&item.attrs) {
            syn::visit::visit_item_mod(self, item);
        }
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        if is_test_only(&item.attrs) {
            return;
        }
        self.context.push(item.sig.ident.to_string());
        syn::visit::visit_item_fn(self, item);
        self.context.pop();
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        if is_test_only(&item.attrs) {
            return;
        }
        self.context.push(item.sig.ident.to_string());
        syn::visit::visit_impl_item_fn(self, item);
        self.context.pop();
    }

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        if call.method == "expect" {
            self.calls.push(
                self.context
                    .last()
                    .cloned()
                    .unwrap_or_else(|| "<module>".to_string()),
            );
        }
        syn::visit::visit_expr_method_call(self, call);
    }
}

fn production_expect_calls(source: &str) -> Vec<String> {
    let file = syn::parse_file(source).expect("parse guarded reqwest source");
    let mut visitor = ProductionExpectVisitor::default();
    visitor.visit_file(&file);
    visitor.calls
}

impl<'ast> Visit<'ast> for RunModeVisitor<'_> {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        if !is_test_only(&item.attrs) {
            syn::visit::visit_item_mod(self, item);
        }
    }

    fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
        if !is_test_only(&item.attrs) {
            syn::visit::visit_item_fn(self, item);
        }
    }

    fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
        if !is_test_only(&item.attrs) {
            syn::visit::visit_impl_item_fn(self, item);
        }
    }

    fn visit_path(&mut self, path: &'ast syn::Path) {
        for segment in &path.segments {
            let name = segment.ident.to_string();
            if self.aliases.run_mode_names.contains(&name) {
                self.references.push(name);
            }
        }
        syn::visit::visit_path(self, path);
    }

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        let method = call.method.to_string();
        if self.aliases.run_mode_names.contains(&method) {
            self.references.push(method);
        }
        syn::visit::visit_expr_method_call(self, call);
    }
}

fn inspect_http_wiring(source: &str) -> Result<(), String> {
    let file = syn::parse_file(source).map_err(|error| format!("parse Rust source: {error}"))?;
    let aliases = aliases_for(&file);
    let mut constructors = ReqwestConstructorVisitor {
        aliases: &aliases,
        context: Vec::new(),
        constructors: Vec::new(),
    };
    constructors.visit_file(&file);
    let mut run_modes = RunModeVisitor {
        aliases: &aliases,
        references: Vec::new(),
    };
    run_modes.visit_file(&file);

    let mut violations = constructors
        .constructors
        .into_iter()
        .map(|(path, context)| format!("concrete constructor {path} in {context}"))
        .collect::<Vec<_>>();
    if aliases.imported_run_mode || !run_modes.references.is_empty() {
        violations.push(format!(
            "run-mode reference/import: {}",
            run_modes.references.join(", ")
        ));
    }
    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations.join("; "))
    }
}

fn production_http_constructor_sites(root: &Path) -> Vec<(PathBuf, String, String)> {
    let mut sources = Vec::new();
    rust_sources_under(&root.join("src"), &mut sources);
    rust_sources_under(&root.join("examples"), &mut sources);
    for crate_entry in fs::read_dir(root.join("crates")).expect("read crates directory") {
        let source_root = crate_entry.expect("read crate entry").path().join("src");
        if source_root.is_dir() {
            rust_sources_under(&source_root, &mut sources);
        }
    }

    let mut sites = Vec::new();
    for path in sources {
        let relative = path.strip_prefix(root).expect("workspace source");
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if file_name.ends_with("_test.rs") || file_name.ends_with("_tests.rs") {
            continue;
        }
        let source = fs::read_to_string(&path).expect("read production Rust source");
        let file = syn::parse_file(&source).unwrap_or_else(|error| {
            panic!("parse production source {}: {error}", relative.display())
        });
        let aliases = aliases_for(&file);
        let mut visitor = ReqwestConstructorVisitor {
            aliases: &aliases,
            context: Vec::new(),
            constructors: Vec::new(),
        };
        visitor.visit_file(&file);
        for (constructor, context) in visitor.constructors {
            sites.push((relative.to_path_buf(), constructor, context));
        }
    }
    sites
}

#[test]
fn default_http_source_transport_stays_cold_until_execute() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let factory = fs::read_to_string(root.join("crates/obzenflow_infra/src/http_client/mod.rs"))
        .expect("read default HTTP client factory");
    let reqwest_source =
        fs::read_to_string(root.join("crates/obzenflow_infra/src/http_client/reqwest_client.rs"))
            .expect("read reqwest HTTP client");

    let mut guarded = vec![
        root.join("src/sources.rs"),
        root.join("crates/obzenflow_infra/src/http_client/mod.rs"),
        root.join("crates/obzenflow_adapters/src/sources/http_pull.rs"),
        root.join("crates/obzenflow_infra/src/web/studio_registration.rs"),
    ];
    rust_sources_under(&root.join("examples/hn_ai_digest_demo"), &mut guarded);
    for path in guarded {
        let source = fs::read_to_string(&path).expect("read guarded HTTP wiring source");
        inspect_http_wiring(&source).unwrap_or_else(|violation| {
            panic!(
                "{} must stay platform-cold and run-mode-blind: {violation}",
                path.strip_prefix(&root)
                    .expect("workspace source")
                    .display()
            )
        });
    }

    assert!(factory.contains("Arc::new(ReqwestHttpClient::new())"));
    assert!(reqwest_source.contains("state: Arc<OnceLock<ClientInitFuture>>"));
    assert!(reqwest_source.contains("shared_platform_step(\"HTTP client initialization\""));
    assert!(reqwest_source.contains(".spawn_blocking("));
    assert!(!reqwest_source.contains("block_in_place"));
    assert_eq!(
        production_expect_calls(&reqwest_source),
        Vec::<String>::new(),
        "the reqwest production path must not hide initialization failure behind expect"
    );

    let sites = production_http_constructor_sites(&root);
    let mut source_client_sites = Vec::new();
    let mut allowed_ai_sites = 0;
    let native_embedding = Path::new("crates/obzenflow_infra/src/ai/native_embedding_client.rs");
    let ai_preflight = Path::new("crates/obzenflow_infra/src/ai/rig/preflight.rs");
    for site @ (path, constructor, context) in &sites {
        let allowed_ai_site = (path == native_embedding
            && constructor == "reqwest::Client::builder"
            && context == "build_http_client")
            || (path == ai_preflight
                && constructor == "reqwest::Client::new"
                && matches!(
                    context.as_str(),
                    "preflight_ollama" | "preflight_openai_models"
                ));
        if allowed_ai_site {
            allowed_ai_sites += 1;
        } else {
            source_client_sites.push(site);
        }
    }
    assert_eq!(
        allowed_ai_sites, 3,
        "the three explicit AI-owned reqwest constructors must remain accounted for: {sites:?}"
    );
    assert_eq!(
        source_client_sites.len(),
        2,
        "only the two infra-owned default-source constructors may exist beyond the explicit AI owners; found {sites:?}"
    );
    for (path, _constructor, context) in source_client_sites {
        assert_eq!(
            path,
            Path::new("crates/obzenflow_infra/src/http_client/reqwest_client.rs"),
            "concrete reqwest construction escaped its infra owner: {sites:?}"
        );
        assert_eq!(
            context, "build_default_client",
            "concrete reqwest construction escaped the one initializer: {sites:?}"
        );
    }

    let constructor_start = reqwest_source
        .find("pub fn new() -> Self")
        .expect("ReqwestHttpClient::new");
    let execute_start = reqwest_source
        .find("async fn execute(&self")
        .expect("HttpClient::execute implementation");
    let wrappers = &reqwest_source[constructor_start..execute_start];
    assert!(wrappers.contains("pub fn with_client(client: reqwest::Client)"));

    let execute = &reqwest_source[execute_start..];
    let acquire = execute
        .find("let client = self.client().await?")
        .expect("lazy async client acquisition");
    let send = execute.find("builder.send().await").expect("request send");
    assert!(
        acquire < send,
        "execute must acquire the retained single-flight verdict before request send"
    );
}

#[test]
fn cold_http_wiring_guard_rejects_constructor_and_run_mode_aliases() {
    let aliased_constructor = r#"
        use reqwest as transport;
        use transport::Client as ColdLookingClient;
        type TwiceHiddenClient = ColdLookingClient;
        fn hidden_helper() -> TwiceHiddenClient { TwiceHiddenClient::new() }
        fn materialize_source() { let _ = hidden_helper(); }
    "#;
    let aliased_run_mode = r#"
        use obzenflow_runtime::bootstrap::ReplayVerb as RequestedAction;
        fn materialize_source(action: RequestedAction) {
            if matches!(action, RequestedAction::Replay) { build_live_transport(); }
        }
    "#;

    assert!(inspect_http_wiring(aliased_constructor)
        .expect_err("aliased concrete constructor must be rejected")
        .contains("TwiceHiddenClient::new"));
    assert!(inspect_http_wiring(aliased_run_mode)
        .expect_err("aliased run-mode branch input must be rejected")
        .contains("run-mode"));
}

#[test]
fn reqwest_guard_rejects_production_expect_but_ignores_test_expect() {
    let source = r#"
        fn preseeded() { state.set(client).expect("fresh state"); }
        #[cfg(test)]
        mod tests {
            fn fixture() { value.expect("test assertion"); }
        }
    "#;
    assert_eq!(
        production_expect_calls(source),
        vec!["preseeded".to_string()]
    );
}

#[test]
fn cold_http_wiring_guard_allows_explicit_custom_client_injection() {
    let custom_injection = r#"
        fn materialize_source(custom_client: std::sync::Arc<dyn HttpClient>) {
            let _ = HttpPullConfig::builder().client(custom_client);
        }
    "#;

    inspect_http_wiring(custom_injection)
        .expect("an already-constructed custom port is an explicit cold injection");
}
