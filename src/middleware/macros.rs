//! Middleware macros

/// Create monitoring middleware
/// 
/// Usage in flow! macro:
/// ```rust
/// flow! {
///     ("source" => MySource::new(), [monitoring!(RED)])
/// }
/// ```
#[macro_export]
macro_rules! monitoring {
    ($taxonomy:ty) => {{
        |name: &str| -> Box<dyn $crate::middleware::Middleware> {
            Box::new($crate::middleware::MonitoringMiddleware::<$taxonomy>::new(name))
        }
    }};
}