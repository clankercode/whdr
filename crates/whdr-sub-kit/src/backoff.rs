//! Exponential backoff with jitter for reconnect scheduling.

use std::time::Duration;

/// Policy describing an exponential-backoff-with-jitter schedule.
///
/// Delays follow `initial * multiplier^attempt`, capped at `max`, then
/// multiplied by a random factor in `[1 - jitter, 1 + jitter]`. A fresh
/// [`Backoff`] (see [`BackoffPolicy::start`]) resets to `attempt = 0`; the
/// [`run`](crate::Client::run) loop resets it after every successful
/// connection so a long-lived connection that later drops reconnects fast.
#[derive(Clone, Copy, Debug)]
pub struct BackoffPolicy {
    /// Delay before the first reconnect attempt.
    pub initial: Duration,
    /// Upper bound on the (pre-jitter) delay.
    pub max: Duration,
    /// Growth factor applied per attempt.
    pub multiplier: f64,
    /// Jitter fraction in `[0.0, 1.0)`. `0.2` = ±20%.
    pub jitter: f64,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(500),
            max: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: 0.2,
        }
    }
}

impl BackoffPolicy {
    /// Begin a fresh backoff run at `attempt = 0`.
    pub fn start(self) -> Backoff {
        Backoff {
            policy: self,
            attempt: 0,
        }
    }

    /// The deterministic (pre-jitter) base delay for a given attempt number.
    fn base_delay(&self, attempt: u32) -> Duration {
        let factor = self.multiplier.powi(attempt as i32);
        let millis =
            (self.initial.as_secs_f64() * factor * 1000.0).min(self.max.as_millis() as f64);
        Duration::from_millis(millis.round() as u64)
    }
}

/// Running state for a [`BackoffPolicy`].
#[derive(Clone, Copy, Debug)]
pub struct Backoff {
    policy: BackoffPolicy,
    attempt: u32,
}

impl Backoff {
    /// Reset to the initial delay (call after a successful connection).
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Compute the next delay (with jitter) and advance the attempt counter.
    pub fn next_delay(&mut self) -> Duration {
        let base = self.policy.base_delay(self.attempt);
        self.attempt = self.attempt.saturating_add(1);
        Self::apply_jitter(base, self.policy.jitter, rand::random::<f64>())
    }

    /// Pure jitter application, factored out for testability. `rand01` is a
    /// sample in `[0, 1)`.
    fn apply_jitter(base: Duration, jitter: f64, rand01: f64) -> Duration {
        if jitter <= 0.0 {
            return base;
        }
        // Map rand01 in [0,1) to a factor in [1 - jitter, 1 + jitter).
        let factor = 1.0 - jitter + rand01 * 2.0 * jitter;
        Duration::from_secs_f64(base.as_secs_f64() * factor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_delays_grow_and_cap() {
        let policy = BackoffPolicy {
            initial: Duration::from_millis(500),
            max: Duration::from_secs(8),
            multiplier: 2.0,
            jitter: 0.0,
        };
        let mut b = policy.start();
        // 500ms, 1s, 2s, 4s, 8s (cap), 8s (cap)...
        assert_eq!(b.next_delay(), Duration::from_millis(500));
        assert_eq!(b.next_delay(), Duration::from_millis(1000));
        assert_eq!(b.next_delay(), Duration::from_millis(2000));
        assert_eq!(b.next_delay(), Duration::from_millis(4000));
        assert_eq!(b.next_delay(), Duration::from_millis(8000));
        assert_eq!(b.next_delay(), Duration::from_millis(8000));
    }

    #[test]
    fn reset_returns_to_initial() {
        let mut b = BackoffPolicy {
            jitter: 0.0,
            ..BackoffPolicy::default()
        }
        .start();
        let first = b.next_delay();
        b.next_delay();
        b.next_delay();
        b.reset();
        assert_eq!(b.next_delay(), first, "reset returns to the initial delay");
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let base = Duration::from_secs(1);
        // Extremes of the random range keep the factor in [0.8, 1.2).
        let lo = Backoff::apply_jitter(base, 0.2, 0.0);
        let hi = Backoff::apply_jitter(base, 0.2, 0.9999);
        assert!(lo >= Duration::from_millis(800) && lo <= base);
        assert!(hi >= base && hi < Duration::from_millis(1200));
        // Zero jitter is exact.
        assert_eq!(Backoff::apply_jitter(base, 0.0, 0.5), base);
    }
}
