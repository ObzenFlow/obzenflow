//! Sink stage implementation

pub mod supervisor;
pub mod fsm;
pub mod states;
pub mod events;
pub mod actions;

// Re-export commonly used types
pub use supervisor::SinkSupervisor;
pub use states::SinkState;
pub use events::SinkEvent;
pub use actions::SinkAction;