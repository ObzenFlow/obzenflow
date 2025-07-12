//! Event flow and routing components

pub mod reactive_journal;

// Re-export commonly used types
pub use reactive_journal::{JournalSubscription, SubscriptionFilter};