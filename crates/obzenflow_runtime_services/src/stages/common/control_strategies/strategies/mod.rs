//! Concrete implementations of control event handling strategies

mod jonestown;
mod retry;
mod windowing;
mod composite;

pub use jonestown::JonestownStrategy;
pub use retry::{RetryStrategy, BackoffStrategy};
pub use windowing::WindowingStrategy;
pub use composite::CompositeStrategy;