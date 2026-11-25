// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The list-and-detail port of a pull source. The first read takes the list of
//! item IDs, and each later read takes one item, in list order.

use async_trait::async_trait;
use obzenflow_runtime::queries::{
    FiniteRead, QueryContext, QueryError, SourceDiagnosticReason, SourcePollErrorKind,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `ListDetail` is the port of a supplier that lists the IDs of its items and
/// gives each item by its ID. The framework asks for the list once, then for each
/// item in list order, and records each answer. A system that answers the port asks
/// the request for its [`step`](ListDetailRead::step) and answers it with the
/// request's reply constructors.
///
/// ```
/// # use obzenflow_adapters::sources::{ListDetail, ListDetailRead, ListDetailStep};
/// # use obzenflow_runtime::queries::{query, FiniteRead, QueryContext, QueryError};
/// # use obzenflow_runtime::systems::Port;
/// struct ScriptedItems;
///
/// #[async_trait::async_trait]
/// impl ListDetail for ScriptedItems {
///     async fn read(
///         &self,
///         from: &ListDetailRead,
///         _ctx: &QueryContext,
///     ) -> Result<FiniteRead<serde_json::Value, ListDetailRead>, QueryError> {
///         match from.step() {
///             ListDetailStep::List { .. } => from.list_reply(vec!["1".into(), "2".into()]),
///             ListDetailStep::Detail { id } => {
///                 from.detail_reply(Some(serde_json::json!({ "id": id })))
///             }
///             ListDetailStep::EndOfInput => Ok(FiniteRead::EndOfInput),
///         }
///     }
/// }
///
/// let items = query(ListDetailRead::first(10)).through(Port::to(ScriptedItems));
/// ```
#[obzenflow_runtime::effects::port]
#[async_trait]
pub trait ListDetail: Send + Sync {
    #[query(type = "obzenflow.list_detail.read")]
    async fn read(
        &self,
        from: &ListDetailRead,
        ctx: &QueryContext,
    ) -> Result<FiniteRead<Value, ListDetailRead>, QueryError>;
}

/// `ListDetailRead` is the request of one list-and-detail read. The request of the
/// first read takes the list, and each answer holds the request of the next read.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListDetailRead {
    limit: usize,
    ids: Option<Vec<String>>,
    next: usize,
}

/// `ListDetailStep` is the step of the read that one request asks a system to answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListDetailStep<'a> {
    /// The list of item IDs, of which the source keeps the first `limit`.
    List { limit: usize },
    /// The item with `id`.
    Detail { id: &'a str },
    /// The source read every listed item.
    EndOfInput,
}

impl ListDetailRead {
    /// `first` returns the request of the first read. It takes the list and keeps
    /// its first `limit` IDs.
    pub fn first(limit: usize) -> Self {
        Self {
            limit,
            ids: None,
            next: 0,
        }
    }

    /// `step` returns the step of the read that this request asks for.
    pub fn step(&self) -> ListDetailStep<'_> {
        match &self.ids {
            None => ListDetailStep::List { limit: self.limit },
            Some(ids) => match ids.get(self.next) {
                Some(id) => ListDetailStep::Detail { id },
                None => ListDetailStep::EndOfInput,
            },
        }
    }

    /// `list_reply` answers the list step with the item IDs. It keeps the first `limit`
    /// IDs. At another step it fails the read, so an answer cannot restart the list.
    pub fn list_reply(&self, mut ids: Vec<String>) -> Result<FiniteRead<Value, Self>, QueryError> {
        if self.ids.is_some() {
            return Err(wrong_step());
        }
        ids.truncate(self.limit);
        Ok(FiniteRead::Batch {
            records: Vec::new(),
            next: Self {
                limit: self.limit,
                ids: Some(ids),
                next: 0,
            },
        })
    }

    /// `detail_reply` answers a detail step with the item, or `None` when the supplier no
    /// longer has it, and moves to the next ID. At another step it fails the read, so
    /// an answer cannot pass the end of the list.
    pub fn detail_reply(&self, item: Option<Value>) -> Result<FiniteRead<Value, Self>, QueryError> {
        let Some(ids) = self.ids.as_ref().filter(|ids| self.next < ids.len()) else {
            return Err(wrong_step());
        };
        Ok(FiniteRead::Batch {
            records: item.into_iter().collect(),
            next: Self {
                limit: self.limit,
                ids: Some(ids.clone()),
                next: self.next + 1,
            },
        })
    }
}

fn wrong_step() -> QueryError {
    QueryError::terminal(
        SourcePollErrorKind::Validation,
        SourceDiagnosticReason::UnexpectedShape,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_runtime::queries::QueryRequest;

    struct ScriptedItems;

    #[async_trait]
    impl ListDetail for ScriptedItems {
        async fn read(
            &self,
            from: &ListDetailRead,
            _ctx: &QueryContext,
        ) -> Result<FiniteRead<Value, ListDetailRead>, QueryError> {
            match from.step() {
                ListDetailStep::List { .. } => {
                    from.list_reply(vec!["1".into(), "2".into(), "3".into()])
                }
                ListDetailStep::Detail { id } => {
                    from.detail_reply((id != "2").then(|| serde_json::json!({ "id": id })))
                }
                ListDetailStep::EndOfInput => Ok(FiniteRead::EndOfInput),
            }
        }
    }

    #[tokio::test]
    async fn application_system_answers_list_detail() {
        let ctx = QueryContext::for_test("items", 1);
        let mut read = ListDetailRead::first(3);
        let mut items = Vec::new();
        let mut reads = 0;
        while let FiniteRead::Batch { records, next } =
            read.query(&ScriptedItems, &ctx).await.unwrap()
        {
            reads += 1;
            items.extend(records);
            read = next;
        }
        assert_eq!(reads, 4, "one list read and one read for each item");
        assert_eq!(
            items,
            [
                serde_json::json!({ "id": "1" }),
                serde_json::json!({ "id": "3" })
            ]
        );
    }

    #[tokio::test]
    async fn list_detail_limit_truncates_list() {
        let ctx = QueryContext::for_test("items", 1);
        let FiniteRead::Batch { next, .. } = ListDetailRead::first(1)
            .query(&ScriptedItems, &ctx)
            .await
            .unwrap()
        else {
            panic!("the list read answers with the selection");
        };
        assert_eq!(next.ids.as_deref(), Some(&["1".to_string()][..]));
    }

    #[test]
    fn reply_constructor_wrong_step_fails() {
        let first = ListDetailRead::first(3);
        let error = first
            .detail_reply(None)
            .expect_err("the list step takes no item");
        assert_eq!(
            error.failure.diagnostic.reason(),
            SourceDiagnosticReason::UnexpectedShape
        );
        let FiniteRead::Batch { next, .. } = first.list_reply(vec!["1".into()]).unwrap() else {
            panic!("the list step answers with the selection");
        };
        assert!(next.list_reply(Vec::new()).is_err());
    }
}
