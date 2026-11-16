//! Idle timeouts for reading HTTP request headers and body frames (#187).

use http_body_util::BodyExt;
use hyper::body::Body;
use std::time::Duration;

/// Default maximum idle time between consecutive request body frames.
pub const DEFAULT_BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(20);

/// Default maximum idle time to finish reading request headers.
pub const DEFAULT_HEADER_IDLE_TIMEOUT: Duration = Duration::from_secs(20);

/// Returned by [`next_frame_with_idle`] when no frame arrives within the idle deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodyIdleTimeout;

/// Awaits the next frame from `body`, returning `Err(BodyIdleTimeout)` if `idle` elapses
/// before a frame (or end-of-stream) arrives.
pub async fn next_frame_with_idle<B>(
    body: &mut B,
    idle: Duration,
) -> Result<Option<Result<hyper::body::Frame<B::Data>, B::Error>>, BodyIdleTimeout>
where
    B: Body + Unpin,
{
    tokio::time::timeout(idle, body.frame())
        .await
        .map_err(|_| BodyIdleTimeout)
}

/// Parses a positive timeout duration from CLI flags such as `"20s"`, `"500ms"`, or `"20"` (seconds).
pub fn parse_timeout_duration(raw: &str) -> Result<Duration, String> {
    let s = raw.trim();
    if s.is_empty() {
        return Err("timeout duration cannot be empty".to_string());
    }
    let dur = if let Some(ms) = s.strip_suffix("ms") {
        let n: u64 = ms.trim().parse().map_err(|_| format!("invalid millisecond duration {:?}", raw))?;
        Duration::from_millis(n)
    } else if let Some(secs) = s.strip_suffix('s') {
        let n: u64 = secs.trim().parse().map_err(|_| format!("invalid second duration {:?}", raw))?;
        Duration::from_secs(n)
    } else {
        let n: u64 = s.parse().map_err(|_| format!("invalid duration {:?}", raw))?;
        Duration::from_secs(n)
    };
    if dur.is_zero() {
        return Err(format!("timeout duration must be greater than zero, got {:?}", raw));
    }
    Ok(dur)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_timeout_accepts_seconds_and_millis() {
        assert_eq!(parse_timeout_duration("20"), Ok(Duration::from_secs(20)));
        assert_eq!(parse_timeout_duration("20s"), Ok(Duration::from_secs(20)));
        assert_eq!(parse_timeout_duration("500ms"), Ok(Duration::from_millis(500)));
        assert!(parse_timeout_duration("0").is_err());
        assert!(parse_timeout_duration("0s").is_err());
        assert!(parse_timeout_duration("").is_err());
        assert!(parse_timeout_duration("abc").is_err());
    }
}
