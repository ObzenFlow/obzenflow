// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use std::future::Future;
use std::pin::Pin;

type ErasedPort = Arc<dyn Any + Send + Sync>;
type ErasedResolveFuture =
    Pin<Box<dyn Future<Output = Result<ErasedPort, EffectPortResolutionError>> + Send>>;
type ErasedResolver = Arc<dyn Fn() -> ErasedResolveFuture + Send + Sync>;

pub type EffectPortResolveFuture<T> =
    Pin<Box<dyn Future<Output = Result<Arc<T>, EffectPortResolutionError>> + Send>>;
pub type EffectPortResolver<T> = Arc<dyn Fn() -> EffectPortResolveFuture<T> + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EffectPortKey {
    type_id: TypeId,
    name: String,
}

impl EffectPortKey {
    pub fn of<T>(name: impl Into<String>) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
    {
        Self {
            type_id: TypeId::of::<T>(),
            name: name.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EffectPortRequirement {
    key: EffectPortKey,
    pub type_name: &'static str,
    pub name: String,
}

impl EffectPortRequirement {
    pub fn of<T>(name: impl Into<String>) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let name = name.into();
        Self {
            key: EffectPortKey::of::<T>(name.clone()),
            type_name: std::any::type_name::<T>(),
            name,
        }
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum EffectPortRegistrationError {
    #[error("effect port '{name}' for type '{type_name}' is already registered")]
    Duplicate {
        type_name: &'static str,
        name: String,
    },
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum EffectPortResolutionError {
    #[error("effect port '{name}' for type '{type_name}' is not registered")]
    Missing {
        type_name: &'static str,
        name: String,
    },
    #[error("effect port '{name}' for type '{type_name}' failed to resolve: {message}")]
    ResolverFailed {
        type_name: &'static str,
        name: String,
        message: String,
    },
}

impl EffectPortResolutionError {
    /// Attach the registered key to an implementation-level resolver error.
    fn at_requirement(self, requirement: &EffectPortRequirement) -> Self {
        match self {
            Self::Missing { .. } | Self::ResolverFailed { .. } => self,
        }
        .with_fallback(requirement)
    }

    fn with_fallback(self, requirement: &EffectPortRequirement) -> Self {
        match self {
            Self::Missing { type_name, name } if name.is_empty() => Self::Missing {
                type_name: if type_name.is_empty() {
                    requirement.type_name
                } else {
                    type_name
                },
                name: requirement.name.clone(),
            },
            Self::ResolverFailed {
                type_name,
                name,
                message,
            } if name.is_empty() => Self::ResolverFailed {
                type_name: if type_name.is_empty() {
                    requirement.type_name
                } else {
                    type_name
                },
                name: requirement.name.clone(),
                message,
            },
            other => other,
        }
    }

    /// Convenience for resolver implementations that have only a bounded,
    /// sanitised diagnostic at the point of failure.
    pub fn failed(message: impl Into<String>) -> Self {
        Self::ResolverFailed {
            type_name: "",
            name: String::new(),
            message: message.into(),
        }
    }
}

#[derive(Clone)]
enum PortRecipe {
    Eager(ErasedPort),
    Deferred(ErasedResolver),
}

#[derive(Clone)]
enum RunPortEntry {
    Eager(ErasedPort),
    Deferred {
        resolver: ErasedResolver,
        verdict: Arc<tokio::sync::OnceCell<Result<ErasedPort, EffectPortResolutionError>>>,
    },
}

/// Flow-authoring registry of eager handles or deferred resolver recipes.
///
/// `StageResourcesBuilder` converts it to a run registry once. Clones within
/// that run share resolution cells; reusing an authoring recipe for another
/// build creates fresh cells.
#[derive(Clone, Default)]
pub struct EffectPortRegistry {
    recipes: Arc<HashMap<EffectPortKey, PortRecipe>>,
    run_entries: Option<Arc<HashMap<EffectPortKey, RunPortEntry>>>,
}

impl std::fmt::Debug for EffectPortRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EffectPortRegistry")
            .field("ports", &self.recipes.len())
            .field("run_registry", &self.run_entries.is_some())
            .finish()
    }
}

impl EffectPortRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert<T>(
        &mut self,
        name: impl Into<String>,
        port: Arc<T>,
    ) -> Result<(), EffectPortRegistrationError>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let name = name.into();
        let key = EffectPortKey::of::<T>(name.clone());
        if self.recipes.contains_key(&key) {
            return Err(EffectPortRegistrationError::Duplicate {
                type_name: std::any::type_name::<T>(),
                name,
            });
        }
        Arc::make_mut(&mut self.recipes).insert(key, PortRecipe::Eager(Arc::new(port)));
        self.run_entries = None;
        Ok(())
    }

    pub fn with_port<T>(
        mut self,
        name: impl Into<String>,
        port: Arc<T>,
    ) -> Result<Self, EffectPortRegistrationError>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.insert(name, port)?;
        Ok(self)
    }

    pub fn with_deferred<T>(
        mut self,
        name: impl Into<String>,
        resolver: EffectPortResolver<T>,
    ) -> Result<Self, EffectPortRegistrationError>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let name = name.into();
        let key = EffectPortKey::of::<T>(name.clone());
        if self.recipes.contains_key(&key) {
            return Err(EffectPortRegistrationError::Duplicate {
                type_name: std::any::type_name::<T>(),
                name,
            });
        }
        let erased: ErasedResolver = Arc::new(move || {
            let future = resolver();
            Box::pin(async move {
                let port = future.await?;
                Ok(Arc::new(port) as ErasedPort)
            })
        });
        Arc::make_mut(&mut self.recipes).insert(key, PortRecipe::Deferred(erased));
        self.run_entries = None;
        Ok(self)
    }

    pub fn get<T>(&self, name: &str) -> Option<Arc<T>>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let key = EffectPortKey {
            type_id: TypeId::of::<T>(),
            name: name.to_string(),
        };
        let erased = match self
            .run_entries
            .as_ref()
            .and_then(|entries| entries.get(&key))
        {
            Some(RunPortEntry::Eager(port)) => Some(port),
            Some(RunPortEntry::Deferred { verdict, .. }) => {
                verdict.get().and_then(|result| result.as_ref().ok())
            }
            None => match self.recipes.get(&key) {
                Some(PortRecipe::Eager(port)) => Some(port),
                _ => None,
            },
        }?;
        erased.downcast_ref::<Arc<T>>().cloned()
    }

    pub fn contains_requirement(&self, requirement: &EffectPortRequirement) -> bool {
        self.recipes.contains_key(&requirement.key)
    }

    pub(crate) fn into_run_registry(mut self) -> Self {
        let entries = self
            .recipes
            .iter()
            .map(|(key, recipe)| {
                let entry = match recipe {
                    PortRecipe::Eager(port) => RunPortEntry::Eager(port.clone()),
                    PortRecipe::Deferred(resolver) => RunPortEntry::Deferred {
                        resolver: resolver.clone(),
                        verdict: Arc::new(tokio::sync::OnceCell::new()),
                    },
                };
                (key.clone(), entry)
            })
            .collect();
        self.run_entries = Some(Arc::new(entries));
        self
    }

    pub(crate) async fn resolve_requirement(
        &self,
        requirement: &EffectPortRequirement,
    ) -> Result<(), EffectPortResolutionError> {
        let Some(entries) = self.run_entries.as_ref() else {
            return match self.recipes.get(&requirement.key) {
                // Direct invocation contexts historically accept eager
                // registries. Deferred recipes require the per-run cell that
                // StageResourcesBuilder installs before materialisation.
                Some(PortRecipe::Eager(_)) => Ok(()),
                Some(PortRecipe::Deferred(_)) => Err(EffectPortResolutionError::ResolverFailed {
                    type_name: requirement.type_name,
                    name: requirement.name.clone(),
                    message: "deferred effect port was not materialised into a run registry"
                        .to_string(),
                }),
                None => Err(EffectPortResolutionError::Missing {
                    type_name: requirement.type_name,
                    name: requirement.name.clone(),
                }),
            };
        };
        let entry =
            entries
                .get(&requirement.key)
                .ok_or_else(|| EffectPortResolutionError::Missing {
                    type_name: requirement.type_name,
                    name: requirement.name.clone(),
                })?;
        match entry {
            RunPortEntry::Eager(_) => Ok(()),
            RunPortEntry::Deferred { resolver, verdict } => {
                let result = verdict
                    .get_or_init(|| async {
                        resolver().await.map_err(|e| e.at_requirement(requirement))
                    })
                    .await;
                result.as_ref().map(|_| ()).map_err(Clone::clone)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn duplicate_registration_is_rejected_without_replacing_the_first_recipe() {
        let mut registry = EffectPortRegistry::new();
        registry
            .insert::<String>("chat", Arc::new("first".to_string()))
            .expect("first registration succeeds");

        let error = registry
            .insert::<String>("chat", Arc::new("second".to_string()))
            .expect_err("duplicate registration must fail");

        assert!(matches!(
            error,
            EffectPortRegistrationError::Duplicate { name, .. } if name == "chat"
        ));
        assert_eq!(
            registry
                .get::<String>("chat")
                .expect("first registration remains installed")
                .as_str(),
            "first"
        );
    }

    #[test]
    fn duplicate_registration_rejects_every_eager_deferred_combination() {
        let deferred = || -> EffectPortResolver<String> {
            Arc::new(|| Box::pin(async { Ok(Arc::new("resolved".to_string())) }))
        };

        let eager_then_deferred = EffectPortRegistry::new()
            .with_port::<String>("chat", Arc::new("eager".to_string()))
            .expect("first eager registration succeeds")
            .with_deferred::<String>("chat", deferred())
            .expect_err("a deferred recipe cannot replace an eager handle");
        assert!(matches!(
            eager_then_deferred,
            EffectPortRegistrationError::Duplicate { name, .. } if name == "chat"
        ));

        let deferred_then_eager = EffectPortRegistry::new()
            .with_deferred::<String>("chat", deferred())
            .expect("first deferred registration succeeds")
            .with_port::<String>("chat", Arc::new("eager".to_string()))
            .expect_err("an eager handle cannot replace a deferred recipe");
        assert!(matches!(
            deferred_then_eager,
            EffectPortRegistrationError::Duplicate { name, .. } if name == "chat"
        ));

        let deferred_then_deferred = EffectPortRegistry::new()
            .with_deferred::<String>("chat", deferred())
            .expect("first deferred registration succeeds")
            .with_deferred::<String>("chat", deferred())
            .expect_err("a deferred recipe cannot replace another deferred recipe");
        assert!(matches!(
            deferred_then_deferred,
            EffectPortRegistrationError::Duplicate { name, .. } if name == "chat"
        ));
    }

    #[tokio::test]
    async fn direct_context_accepts_eager_but_not_unmaterialised_deferred_recipe() {
        let mut eager = EffectPortRegistry::new();
        eager
            .insert::<String>("chat", Arc::new("ready".to_string()))
            .expect("eager registration succeeds");
        let requirement = EffectPortRequirement::of::<String>("chat");
        eager
            .resolve_requirement(&requirement)
            .await
            .expect("direct invocation may use an eager authoring registry");

        let deferred = EffectPortRegistry::new()
            .with_deferred::<String>(
                "chat",
                Arc::new(|| Box::pin(async { Ok(Arc::new("resolved".to_string())) })),
            )
            .expect("deferred registration succeeds");
        assert!(matches!(
            deferred.resolve_requirement(&requirement).await,
            Err(EffectPortResolutionError::ResolverFailed { ref message, .. })
                if message.contains("run registry")
        ));
    }

    #[tokio::test]
    async fn deferred_resolution_is_single_flight_within_one_run() {
        let calls = Arc::new(AtomicUsize::new(0));
        let resolver_calls = calls.clone();
        let resolver: EffectPortResolver<String> = Arc::new(move || {
            let calls = resolver_calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                Ok(Arc::new("resolved".to_string()))
            })
        });
        let registry = EffectPortRegistry::new()
            .with_deferred::<String>("chat", resolver)
            .expect("deferred registration is unique")
            .into_run_registry();
        let requirement = EffectPortRequirement::of::<String>("chat");

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let registry = registry.clone();
            let requirement = requirement.clone();
            tasks.push(tokio::spawn(async move {
                registry.resolve_requirement(&requirement).await
            }));
        }
        for task in tasks {
            task.await
                .expect("resolution task does not panic")
                .expect("resolution succeeds");
        }

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            registry
                .get::<String>("chat")
                .expect("resolved handle is cached")
                .as_str(),
            "resolved"
        );
    }

    #[tokio::test]
    async fn failed_verdict_is_cached_per_run_but_not_across_runs() {
        let calls = Arc::new(AtomicUsize::new(0));
        let resolver_calls = calls.clone();
        let resolver: EffectPortResolver<String> = Arc::new(move || {
            let calls = resolver_calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(EffectPortResolutionError::failed("unavailable"))
            })
        });
        let recipe = EffectPortRegistry::new()
            .with_deferred::<String>("chat", resolver)
            .expect("deferred registration is unique");
        let requirement = EffectPortRequirement::of::<String>("chat");

        let first_run = recipe.clone().into_run_registry();
        for _ in 0..2 {
            assert!(matches!(
                first_run.resolve_requirement(&requirement).await,
                Err(EffectPortResolutionError::ResolverFailed { ref name, .. })
                    if name == "chat"
            ));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let second_run = recipe.into_run_registry();
        assert!(second_run.resolve_requirement(&requirement).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cancelling_the_initialiser_does_not_poison_the_run_cell() {
        let calls = Arc::new(AtomicUsize::new(0));
        let first_started = Arc::new(tokio::sync::Notify::new());
        let resolver_calls = calls.clone();
        let resolver_started = first_started.clone();
        let resolver: EffectPortResolver<String> = Arc::new(move || {
            let invocation = resolver_calls.fetch_add(1, Ordering::SeqCst);
            let started = resolver_started.clone();
            Box::pin(async move {
                if invocation == 0 {
                    started.notify_one();
                    std::future::pending::<Result<Arc<String>, EffectPortResolutionError>>().await
                } else {
                    Ok(Arc::new("recovered".to_string()))
                }
            })
        });
        let registry = EffectPortRegistry::new()
            .with_deferred::<String>("chat", resolver)
            .expect("deferred registration is unique")
            .into_run_registry();
        let requirement = EffectPortRequirement::of::<String>("chat");

        let first_registry = registry.clone();
        let first_requirement = requirement.clone();
        let first =
            tokio::spawn(
                async move { first_registry.resolve_requirement(&first_requirement).await },
            );
        first_started.notified().await;
        first.abort();
        assert!(first
            .await
            .expect_err("initialiser is cancelled")
            .is_cancelled());

        registry
            .resolve_requirement(&requirement)
            .await
            .expect("a later waiter may initialise the empty cell");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            registry
                .get::<String>("chat")
                .expect("second initialisation is cached")
                .as_str(),
            "recovered"
        );
    }
}
