// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::sources::http::HttpSource;
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::web::{HttpMethod, Request};
use obzenflow_core::{StageId, WriterId};
use obzenflow_infra::web::endpoints::event_ingestion::{
    create_ingestion_endpoints, IngestionConfig,
};
use obzenflow_runtime::stages::common::handlers::source::AsyncInfiniteSourceHandler;

#[tokio::test]
async fn http_post_feeds_httpsource_chain_events() {
    let (endpoints, rx, state) = create_ingestion_endpoints(IngestionConfig::default());
    state
        .ready
        .store(true, std::sync::atomic::Ordering::Release);

    let events_endpoint = endpoints
        .into_iter()
        .find(|e| e.path().ends_with("/events"))
        .expect("events endpoint");

    let request = Request::new(HttpMethod::Post, events_endpoint.path().to_string()).with_body(
        serde_json::to_vec(&serde_json::json!({
            "event_type": "order.created",
            "data": { "order_id": "1" }
        }))
        .unwrap(),
    );

    let resp = events_endpoint.handle(request).await.unwrap();
    assert_eq!(resp.status, 200);

    let mut source = HttpSource::new(rx);
    source.bind_writer_id(WriterId::from(StageId::new()));
    let out = source.next().await.unwrap();
    assert_eq!(out.len(), 1);

    match &out[0].content {
        ChainEventContent::Data {
            event_type,
            payload,
        } => {
            assert_eq!(event_type, "order.created");
            assert_eq!(payload, &serde_json::json!({ "order_id": "1" }));
        }
        other => panic!("expected data event, got {other:?}"),
    }
}
