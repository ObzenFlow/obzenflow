//! A duration type that stores time in nanoseconds
//!
//! This type ensures consistent time units throughout the system and prevents
//! accidental unit conversion errors that can occur when using raw u64 values.

use serde::{Deserialize, Serialize};

/// A duration of time stored internally as nanoseconds.
/// 
/// This type ensures consistent time units throughout the system and prevents
/// accidental unit conversion errors. All durations in the system should use
/// this type rather than raw numeric values.
/// 
/// # Examples
/// 
/// ```
/// use obzenflow_core::time::MetricsDuration;
/// use std::time::Instant;
///
/// // Create from different units
/// let d1 = MetricsDuration::from_millis(100);
/// let d2 = MetricsDuration::from_secs(1);
///
/// // Measure elapsed time
/// let start = Instant::now();
/// // ... do some work ...
/// let elapsed = MetricsDuration::from_instant_elapsed(start);
///
/// // Convert to different units for display
/// println!("Elapsed: {} ({} ms)", elapsed, elapsed.as_millis());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MetricsDuration {
    nanos: u64,
}

impl MetricsDuration {
    /// Zero duration constant
    pub const ZERO: Self = Self { nanos: 0 };
    
    /// Create a duration from nanoseconds
    #[inline]
    pub fn from_nanos(nanos: u64) -> Self { 
        Self { nanos } 
    }
    
    /// Create a duration from microseconds
    #[inline]
    pub fn from_micros(micros: u64) -> Self { 
        Self { nanos: micros.saturating_mul(1_000) } 
    }
    
    /// Create a duration from milliseconds
    #[inline]
    pub fn from_millis(millis: u64) -> Self { 
        Self { nanos: millis.saturating_mul(1_000_000) } 
    }
    
    /// Create a duration from seconds
    #[inline]
    pub fn from_secs(secs: u64) -> Self { 
        Self { nanos: secs.saturating_mul(1_000_000_000) } 
    }
    
    /// Create a duration from seconds as f64
    #[inline]
    pub fn from_secs_f64(secs: f64) -> Self {
        let nanos = (secs * 1_000_000_000.0) as u64;
        Self { nanos }
    }
    
    /// Create from a standard library Duration
    #[inline]
    pub fn from_std(duration: std::time::Duration) -> Self {
        Self { nanos: duration.as_nanos() as u64 }
    }
    
    /// Create from elapsed time since an Instant
    #[inline]
    pub fn from_instant_elapsed(start: std::time::Instant) -> Self {
        Self::from_std(start.elapsed())
    }
    
    /// Get the duration as nanoseconds
    #[inline]
    pub fn as_nanos(&self) -> u64 { 
        self.nanos 
    }
    
    /// Get the duration as microseconds (truncated)
    #[inline]
    pub fn as_micros(&self) -> u64 { 
        self.nanos / 1_000 
    }
    
    /// Get the duration as milliseconds (truncated)
    #[inline]
    pub fn as_millis(&self) -> u64 { 
        self.nanos / 1_000_000 
    }
    
    /// Get the duration as seconds (truncated)
    #[inline]
    pub fn as_secs(&self) -> u64 {
        self.nanos / 1_000_000_000
    }
    
    /// Get the duration as seconds with fractional part
    #[inline]
    pub fn as_secs_f64(&self) -> f64 { 
        self.nanos as f64 / 1_000_000_000.0 
    }
    
    /// Convert to standard library Duration
    #[inline]
    pub fn to_std(&self) -> std::time::Duration {
        std::time::Duration::from_nanos(self.nanos)
    }
    
    /// Returns true if this duration is zero
    #[inline]
    pub fn is_zero(&self) -> bool {
        self.nanos == 0
    }
    
    /// Checked addition. Returns None if overflow occurs.
    pub fn checked_add(&self, rhs: MetricsDuration) -> Option<MetricsDuration> {
        self.nanos.checked_add(rhs.nanos).map(|nanos| MetricsDuration { nanos })
    }
    
    /// Saturating addition. Saturates at the numeric bounds instead of overflowing.
    pub fn saturating_add(&self, rhs: MetricsDuration) -> MetricsDuration {
        MetricsDuration { nanos: self.nanos.saturating_add(rhs.nanos) }
    }
}

impl Default for MetricsDuration {
    fn default() -> Self {
        Self::ZERO
    }
}

impl std::fmt::Display for MetricsDuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.nanos == 0 {
            write!(f, "0s")
        } else if self.nanos < 1_000 {
            write!(f, "{}ns", self.nanos)
        } else if self.nanos < 1_000_000 {
            write!(f, "{:.1}μs", self.as_nanos() as f64 / 1_000.0)
        } else if self.nanos < 1_000_000_000 {
            write!(f, "{:.1}ms", self.as_nanos() as f64 / 1_000_000.0)
        } else {
            write!(f, "{:.2}s", self.as_secs_f64())
        }
    }
}

impl From<std::time::Duration> for MetricsDuration {
    fn from(d: std::time::Duration) -> Self {
        Self::from_std(d)
    }
}

impl From<MetricsDuration> for std::time::Duration {
    fn from(d: MetricsDuration) -> Self {
        d.to_std()
    }
}

impl std::ops::Add for MetricsDuration {
    type Output = MetricsDuration;
    
    fn add(self, rhs: MetricsDuration) -> MetricsDuration {
        MetricsDuration { nanos: self.nanos + rhs.nanos }
    }
}

impl std::ops::Sub for MetricsDuration {
    type Output = MetricsDuration;
    
    fn sub(self, rhs: MetricsDuration) -> MetricsDuration {
        MetricsDuration { nanos: self.nanos - rhs.nanos }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_duration_conversions() {
        assert_eq!(MetricsDuration::from_nanos(1_000).as_micros(), 1);
        assert_eq!(MetricsDuration::from_micros(1_000).as_millis(), 1);
        assert_eq!(MetricsDuration::from_millis(1_000).as_secs(), 1);
        assert_eq!(MetricsDuration::from_secs(1).as_millis(), 1_000);
    }
    
    #[test]
    fn test_duration_display() {
        assert_eq!(MetricsDuration::from_nanos(0).to_string(), "0s");
        assert_eq!(MetricsDuration::from_nanos(500).to_string(), "500ns");
        assert_eq!(MetricsDuration::from_nanos(1_500).to_string(), "1.5μs");
        assert_eq!(MetricsDuration::from_micros(1_500).to_string(), "1.5ms");
        assert_eq!(MetricsDuration::from_millis(1_500).to_string(), "1.50s");
    }
    
    #[test]
    fn test_duration_arithmetic() {
        let d1 = MetricsDuration::from_millis(100);
        let d2 = MetricsDuration::from_millis(50);
        
        assert_eq!((d1 + d2).as_millis(), 150);
        assert_eq!((d1 - d2).as_millis(), 50);
    }
    
    #[test]
    fn test_duration_ordering() {
        let d1 = MetricsDuration::from_millis(100);
        let d2 = MetricsDuration::from_millis(200);
        
        assert!(d1 < d2);
        assert!(d2 > d1);
        assert_eq!(d1, MetricsDuration::from_micros(100_000));
    }
    
    #[test]
    fn test_std_conversion() {
        let std_dur = std::time::Duration::from_millis(100);
        let our_dur = MetricsDuration::from_std(std_dur);
        
        assert_eq!(our_dur.as_millis(), 100);
        assert_eq!(our_dur.to_std(), std_dur);
    }
}