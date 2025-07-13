//! Core journal abstractions
//!
//! Pure domain types and traits for event journaling.
//! No infrastructure concerns or I/O operations here!

pub mod journal;
pub mod journal_error;
pub mod journal_name;
pub mod journal_owner;
pub mod journal_reader;
pub mod writer_id;
