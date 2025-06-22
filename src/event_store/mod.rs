//! Event Store infrastructure for FlowState RS
//! 
//! Provides causal consistency through vector clocks,
//! writer isolation for lock-free operation, and
//! multi-worker stage support.

mod vector_clock;
mod stage_semantics;
mod writer;
mod reader;
pub mod constants;
mod subscription;
mod flow_log;
mod writer_id;
mod types;
mod store;

pub use vector_clock::VectorClock;
pub use stage_semantics::*;
pub use writer::EventWriter;
pub use reader::EventReader;
pub use subscription::{EventSubscription, SubscriptionFilter, EventNotification, SubscriptionEvent, EofNotification};
pub use writer_id::WriterId;
pub use types::{EventEnvelope, IsolationMode, EventStoreConfig, RetentionPolicy};
pub use store::{EventStore, EventStoreStatistics};