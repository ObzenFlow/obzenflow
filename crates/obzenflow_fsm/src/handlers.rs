//! Handler type aliases for FSM callbacks

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use crate::types::Transition;

/// Type alias for async transition handlers
pub type TransitionHandler<S, E, C, A> = Arc<
    dyn Fn(&S, &E, Arc<C>) -> Pin<Box<dyn Future<Output = Result<Transition<S, A>, String>> + Send>>
        + Send
        + Sync,
>;

/// Type alias for async state handlers (entry/exit)
pub type StateHandler<S, C, A> = Arc<
    dyn Fn(&S, Arc<C>) -> Pin<Box<dyn Future<Output = Result<Vec<A>, String>> + Send>>
        + Send
        + Sync,
>;

/// Type alias for timeout handlers
pub type TimeoutHandler<S, C, A> = Arc<
    dyn Fn(&S, Arc<C>) -> Pin<Box<dyn Future<Output = Result<Transition<S, A>, String>> + Send>>
        + Send
        + Sync,
>;