// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

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

#[derive(Clone, Default)]
pub struct EffectPortRegistry {
    ports: Arc<HashMap<EffectPortKey, Arc<dyn Any + Send + Sync>>>,
}

impl std::fmt::Debug for EffectPortRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EffectPortRegistry")
            .field("ports", &self.ports.len())
            .finish()
    }
}

impl EffectPortRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert<T>(&mut self, name: impl Into<String>, port: Arc<T>)
    where
        T: ?Sized + Send + Sync + 'static,
    {
        Arc::make_mut(&mut self.ports).insert(EffectPortKey::of::<T>(name), Arc::new(port));
    }

    pub fn with_port<T>(mut self, name: impl Into<String>, port: Arc<T>) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.insert(name, port);
        self
    }

    pub fn get<T>(&self, name: &str) -> Option<Arc<T>>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.ports
            .get(&EffectPortKey {
                type_id: TypeId::of::<T>(),
                name: name.to_string(),
            })
            .and_then(|port| port.downcast_ref::<Arc<T>>())
            .cloned()
    }

    pub fn contains_requirement(&self, requirement: &EffectPortRequirement) -> bool {
        self.ports.contains_key(&requirement.key)
    }
}
