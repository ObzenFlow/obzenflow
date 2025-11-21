//! Concrete implementations of control event handling strategies

mod composite;
mod jonestown;
mod retry;
mod windowing;

pub use composite::CompositeStrategy;
pub use jonestown::JonestownStrategy;
pub use retry::{BackoffStrategy, RetryStrategy};
pub use windowing::WindowingStrategy;
