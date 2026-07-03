// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The immutable resolved snapshot (FLOWIP-010 §7): built once at startup,
//! owned by `FlowApplication`, handed to the flow build and the read
//! surface as `Arc` handles. Rebuild-and-swap is the only mutation model
//! (§7 snapshot lock); nothing here mutates in place.

use super::candidates::{CandidateSet, ConfigValue};
use super::error::ConfigResolveError;
use super::schema::{knob, knob_registry, KnobDefault, KnobSpec, Redaction};
use obzenflow_core::config::{
    ConfigScope, ConfigSource, ConfigValueMeta, ResolvedValueDoc, SecretRef,
};

/// A resolved value with both provenance axes.
#[derive(Debug, Clone, PartialEq)]
pub struct Resolved<T> {
    pub value: T,
    pub meta: ConfigValueMeta,
}

/// The non-persisted runtime patch layer. No write path exists in this
/// slice (FLOWIP-010b owns mutation), so the overlay is truthfully empty;
/// the read surface reports it as such rather than omitting it (gap 12).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RuntimeConfigOverlay {
    entries: Vec<OverlayEntry>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OverlayEntry {
    pub key_path: String,
    pub scope: ConfigScope,
    pub value: ConfigValue,
}

impl RuntimeConfigOverlay {
    pub fn entries(&self) -> &[OverlayEntry] {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// The immutable startup snapshot: the scoped candidate table plus the
/// (empty) overlay. Global- and flow-rung views resolve on demand through
/// the one ladder; per-stage and per-edge resolution happens at flow build
/// (`materialize_flow_config`), where the DSL tier and the topology exist.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResolvedRuntimeConfig {
    candidates: CandidateSet,
    overlay: RuntimeConfigOverlay,
}

impl ResolvedRuntimeConfig {
    pub fn new(candidates: CandidateSet) -> Self {
        Self {
            candidates,
            overlay: RuntimeConfigOverlay::default(),
        }
    }

    /// Built-in defaults only; the explicit test-harness construction
    /// (`FlowBuildContext::for_tests`), never an ambient fallback.
    pub fn builtin_defaults() -> Self {
        Self::default()
    }

    pub fn candidates(&self) -> &CandidateSet {
        &self.candidates
    }

    pub fn overlay(&self) -> &RuntimeConfigOverlay {
        &self.overlay
    }

    /// Resolve one knob's non-structural rungs (flow, then global, then
    /// default). This is the offline and pre-build view: the DSL tier and
    /// stage/edge scopes do not exist here.
    pub fn resolve_view(
        &self,
        key_path: &str,
    ) -> Result<Option<Resolved<ConfigValue>>, ConfigResolveError> {
        let spec = knob(key_path).ok_or_else(|| ConfigResolveError::UnknownKnob {
            key_path: key_path.to_string(),
        })?;
        Ok(self.resolve_view_spec(spec))
    }

    fn resolve_view_spec(&self, spec: &KnobSpec) -> Option<Resolved<ConfigValue>> {
        for scope in [ConfigScope::Flow, ConfigScope::Global] {
            if let Some(slots) = self.candidates.get(spec.key_path, &scope) {
                for (source, slot) in [
                    (ConfigSource::Cli, &slots.cli),
                    (ConfigSource::File, &slots.file),
                    (ConfigSource::Env, &slots.env),
                    (ConfigSource::Dsl, &slots.dsl),
                ] {
                    if let Some(value) = slot {
                        return Some(Resolved {
                            value: value.clone(),
                            meta: ConfigValueMeta {
                                source,
                                scope,
                                key_path: spec.key_path.to_string(),
                            },
                        });
                    }
                }
            }
        }
        match &spec.default {
            KnobDefault::Value(value) => Some(Resolved {
                value: value.clone(),
                meta: ConfigValueMeta {
                    source: ConfigSource::Default,
                    scope: ConfigScope::Global,
                    key_path: spec.key_path.to_string(),
                },
            }),
            KnobDefault::OptionalAbsent | KnobDefault::Required => None,
        }
    }

    /// The global-rung view of every registered knob, in evidence form.
    /// Serves the base/effective endpoints' global sections and the offline
    /// CLI (whose honest coverage this is, per §9).
    pub fn global_view(&self) -> Vec<ResolvedValueDoc> {
        knob_registry()
            .iter()
            .filter_map(|spec| self.resolve_view_spec(spec).map(|r| doc_for(spec, &r)))
            .collect()
    }

    /// Typed `ai.models` section (global-target knobs, so the view rung is
    /// final for them).
    pub fn ai_models(&self) -> AiModelsConfig {
        let text = |key: &str| {
            self.resolve_view(key)
                .expect("registry key")
                .map(|resolved| Resolved {
                    value: resolved
                        .value
                        .as_text()
                        .expect("token/text knob resolves text")
                        .to_string(),
                    meta: resolved.meta,
                })
        };
        AiModelsConfig {
            provider: text("ai.models.provider").expect("provider has a built-in default"),
            model: text("ai.models.model"),
            base_url: text("ai.models.base_url"),
            api_key_env: {
                let resolved =
                    text("ai.models.api_key_env").expect("api_key_env has a built-in default");
                Resolved {
                    value: SecretRef::new(resolved.value),
                    meta: resolved.meta,
                }
            },
        }
    }
}

/// Typed `[ai.models]` view (FLOWIP-010 absorption of `ModelConfig`'s env
/// surface). `api_key_env` is the visible secret REFERENCE (§13).
#[derive(Debug, Clone, PartialEq)]
pub struct AiModelsConfig {
    pub provider: Resolved<String>,
    pub model: Option<Resolved<String>>,
    pub base_url: Option<Resolved<String>>,
    pub api_key_env: Resolved<SecretRef>,
}

/// Evidence/DTO rendering with schema-driven redaction (§13: resolved
/// secret values redact; reference names stay visible).
pub fn doc_for(spec: &KnobSpec, resolved: &Resolved<ConfigValue>) -> ResolvedValueDoc {
    let redacted = matches!(spec.redaction, Redaction::SecretValue);
    ResolvedValueDoc {
        key_path: spec.key_path.to_string(),
        scope: resolved.meta.scope.to_string(),
        source: resolved.meta.source.to_string(),
        value: if redacted {
            serde_json::json!("***redacted***")
        } else {
            resolved.value.to_json()
        },
        redacted,
    }
}

/// One difference between two doc sets, compared per `(key_path, scope)`.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ConfigDiffEntry {
    pub key_path: String,
    pub scope: String,
    pub base: Option<serde_json::Value>,
    pub effective: Option<serde_json::Value>,
}

/// Diff two doc sets (base vs effective). With the overlay empty in this
/// slice the result is truthfully empty; asserted, not assumed (gap 12).
pub fn diff(base: &[ResolvedValueDoc], effective: &[ResolvedValueDoc]) -> Vec<ConfigDiffEntry> {
    use std::collections::BTreeMap;
    let index = |docs: &[ResolvedValueDoc]| -> BTreeMap<(String, String), serde_json::Value> {
        docs.iter()
            .map(|d| ((d.key_path.clone(), d.scope.clone()), d.value.clone()))
            .collect()
    };
    let base_index = index(base);
    let effective_index = index(effective);
    let mut keys: Vec<_> = base_index.keys().chain(effective_index.keys()).collect();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .filter_map(|key| {
            let base_value = base_index.get(key);
            let effective_value = effective_index.get(key);
            if base_value == effective_value {
                None
            } else {
                Some(ConfigDiffEntry {
                    key_path: key.0.clone(),
                    scope: key.1.clone(),
                    base: base_value.cloned(),
                    effective: effective_value.cloned(),
                })
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_config::candidates::ScopedCandidate;
    use crate::runtime_config::schema::{
        EdgeEndpoint, EnvBinding, KnobTarget, KnobType, Mutability,
    };

    #[test]
    fn builtin_defaults_view_reports_default_source() {
        let snapshot = ResolvedRuntimeConfig::builtin_defaults();
        let docs = snapshot.global_view();
        let lineage = docs
            .iter()
            .find(|d| d.key_path == "runtime.max_lineage_depth")
            .unwrap();
        assert_eq!(lineage.value, serde_json::json!(100));
        assert_eq!(lineage.source, "default");
        assert_eq!(lineage.scope, "global");
        // OptionalAbsent knobs produce no doc.
        assert!(!docs
            .iter()
            .any(|d| d.key_path == "runtime.backpressure.window"));
    }

    #[test]
    fn ai_models_typed_view_carries_provenance_and_the_visible_reference() {
        let mut set = CandidateSet::default();
        set.admit(ScopedCandidate {
            key_path: "ai.models.provider".to_string(),
            scope: ConfigScope::Global,
            source: ConfigSource::File,
            value: ConfigValue::Text("openai".to_string()),
        })
        .unwrap();
        let snapshot = ResolvedRuntimeConfig::new(set);
        let ai = snapshot.ai_models();
        assert_eq!(ai.provider.value, "openai");
        assert_eq!(ai.provider.meta.source, ConfigSource::File);
        assert_eq!(ai.api_key_env.value.env_name(), "OPENAI_API_KEY");
        assert_eq!(ai.api_key_env.meta.source, ConfigSource::Default);
    }

    #[test]
    fn secret_value_redaction_machinery_works_via_a_synthetic_spec() {
        let spec = KnobSpec {
            key_path: "ai.endpoints.resolved_api_key",
            file_path: None,
            value_type: KnobType::Text,
            target: KnobTarget::Global,
            default: KnobDefault::OptionalAbsent,
            mutability: Mutability::Restartful,
            redaction: Redaction::SecretValue,
            env: EnvBinding::None,
        };
        let resolved = Resolved {
            value: ConfigValue::Text("hunter2".to_string()),
            meta: ConfigValueMeta {
                source: ConfigSource::File,
                scope: ConfigScope::Global,
                key_path: spec.key_path.to_string(),
            },
        };
        let doc = doc_for(&spec, &resolved);
        assert!(doc.redacted);
        assert_eq!(doc.value, serde_json::json!("***redacted***"));
        let _ = EdgeEndpoint::Upstream; // silence unused import in cfg(test)
    }

    #[test]
    fn overlay_is_truthfully_empty_and_diff_is_empty() {
        let snapshot = ResolvedRuntimeConfig::builtin_defaults();
        assert!(snapshot.overlay().is_empty());
        let docs = snapshot.global_view();
        assert!(diff(&docs, &docs).is_empty());
    }
}
