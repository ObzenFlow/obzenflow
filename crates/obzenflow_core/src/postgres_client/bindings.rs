// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::fmt;

/// The PostgreSQL type a bound value is sent as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresKind {
    Bool,
    Int8,
    Text,
    TextArray,
}

/// One value bound to a statement parameter. A null keeps its type, since
/// PostgreSQL types every parameter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresValue {
    Null(PostgresKind),
    Bool(bool),
    Int8(i64),
    Text(String),
    TextArray(Vec<String>),
}

/// A Rust value a statement parameter can take.
pub trait PostgresParameter {
    const KIND: PostgresKind;

    fn into_value(self) -> PostgresValue;
}

impl PostgresParameter for bool {
    const KIND: PostgresKind = PostgresKind::Bool;

    fn into_value(self) -> PostgresValue {
        PostgresValue::Bool(self)
    }
}

impl PostgresParameter for i64 {
    const KIND: PostgresKind = PostgresKind::Int8;

    fn into_value(self) -> PostgresValue {
        PostgresValue::Int8(self)
    }
}

impl PostgresParameter for String {
    const KIND: PostgresKind = PostgresKind::Text;

    fn into_value(self) -> PostgresValue {
        PostgresValue::Text(self)
    }
}

impl PostgresParameter for &String {
    const KIND: PostgresKind = PostgresKind::Text;

    fn into_value(self) -> PostgresValue {
        PostgresValue::Text(self.clone())
    }
}

impl PostgresParameter for &str {
    const KIND: PostgresKind = PostgresKind::Text;

    fn into_value(self) -> PostgresValue {
        PostgresValue::Text(self.to_owned())
    }
}

impl PostgresParameter for Vec<String> {
    const KIND: PostgresKind = PostgresKind::TextArray;

    fn into_value(self) -> PostgresValue {
        PostgresValue::TextArray(self)
    }
}

impl<T: PostgresParameter> PostgresParameter for Option<T> {
    const KIND: PostgresKind = T::KIND;

    fn into_value(self) -> PostgresValue {
        match self {
            Some(value) => value.into_value(),
            None => PostgresValue::Null(T::KIND),
        }
    }
}

/// The values bound to a statement's parameters, in order.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct PostgresBindings {
    values: Vec<PostgresValue>,
}

impl PostgresBindings {
    /// Bind the next parameter, `$1` first.
    pub fn bind(&mut self, value: impl PostgresParameter) -> &mut Self {
        self.values.push(value.into_value());
        self
    }

    pub fn values(&self) -> &[PostgresValue] {
        &self.values
    }

    pub fn into_values(self) -> Vec<PostgresValue> {
        self.values
    }
}

impl fmt::Debug for PostgresBindings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresBindings")
            .field("parameters", &self.values.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_option_binds_typed_null() {
        let mut bindings = PostgresBindings::default();
        bindings
            .bind(7_i64)
            .bind(None::<&str>)
            .bind(Some("interests"))
            .bind(vec!["1".to_string()]);
        assert_eq!(
            bindings.values(),
            [
                PostgresValue::Int8(7),
                PostgresValue::Null(PostgresKind::Text),
                PostgresValue::Text("interests".into()),
                PostgresValue::TextArray(vec!["1".into()]),
            ]
        );
    }
}
