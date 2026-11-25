// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The list-and-detail port of a pull source. The first read takes the list of
//! item IDs, and each later read takes one item, in list order.

use async_trait::async_trait;
use obzenflow_runtime::queries::{FiniteRead, QueryContext, QueryError, QueryRequest};
use obzenflow_runtime::systems::{PortImpl, ServesPort};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// `ListDetail` is the port of a supplier that lists the IDs of its items and
/// gives each item by its ID. The framework asks for the list once, then for each
/// item in list order, and records each answer. A system that answers the port
/// implements both methods.
#[async_trait]
pub trait ListDetail: Send + Sync {
    /// `list` returns the IDs of the items, in the order that the source reads them.
    async fn list(&self, ctx: &QueryContext) -> Result<Vec<String>, QueryError>;

    /// `detail` returns the item with `id`, or `None` when the supplier no longer
    /// has it. The source then reads the next item.
    async fn detail(&self, id: &str, ctx: &QueryContext) -> Result<Option<Value>, QueryError>;
}

impl<T: ListDetail + 'static> ServesPort<dyn ListDetail, T> for PortImpl<T> {
    fn coerce(port: Arc<T>) -> Arc<dyn ListDetail> {
        port
    }
}

/// `ListDetailRead` is the request of one list-and-detail read. The request of the
/// first read takes the list, and each answer holds the request of the next read.
///
/// ```
/// # use obzenflow_adapters::sources::{ListDetail, ListDetailRead};
/// # use obzenflow_runtime::queries::{query, QueryContext, QueryError};
/// # use obzenflow_runtime::systems::Port;
/// #[obzenflow_runtime::effects::system("scripted-items")]
/// struct ScriptedItems;
///
/// #[async_trait::async_trait]
/// impl ListDetail for ScriptedItems {
///     async fn list(&self, _ctx: &QueryContext) -> Result<Vec<String>, QueryError> {
///         Ok(vec!["1".into(), "2".into()])
///     }
///
///     async fn detail(
///         &self,
///         id: &str,
///         _ctx: &QueryContext,
///     ) -> Result<Option<serde_json::Value>, QueryError> {
///         Ok(Some(serde_json::json!({ "id": id })))
///     }
/// }
///
/// let items = query(ListDetailRead::first(10)).through(Port::to(ScriptedItems));
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListDetailRead {
    limit: usize,
    ids: Option<Vec<String>>,
    next: usize,
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
}

#[async_trait]
impl QueryRequest for ListDetailRead {
    const QUERY_TYPE: &'static str = "obzenflow.list_detail.read";
    type Port = dyn ListDetail;
    type Answer = FiniteRead<Value, ListDetailRead>;

    async fn query(
        &self,
        port: &dyn ListDetail,
        ctx: &QueryContext,
    ) -> Result<Self::Answer, QueryError> {
        let Some(ids) = &self.ids else {
            let mut ids = port.list(ctx).await?;
            ids.truncate(self.limit);
            return Ok(FiniteRead::Batch {
                records: Vec::new(),
                next: Self {
                    limit: self.limit,
                    ids: Some(ids),
                    next: 0,
                },
            });
        };
        let Some(id) = ids.get(self.next) else {
            return Ok(FiniteRead::EndOfInput);
        };
        let item = port.detail(id, ctx).await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[obzenflow_runtime::effects::system("scripted-items")]
    struct ScriptedItems;

    #[async_trait]
    impl ListDetail for ScriptedItems {
        async fn list(&self, _ctx: &QueryContext) -> Result<Vec<String>, QueryError> {
            Ok(vec!["1".into(), "2".into(), "3".into()])
        }

        async fn detail(&self, id: &str, _ctx: &QueryContext) -> Result<Option<Value>, QueryError> {
            Ok((id != "2").then(|| serde_json::json!({ "id": id })))
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
}
