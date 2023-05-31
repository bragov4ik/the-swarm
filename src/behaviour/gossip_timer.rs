use std::{pin::Pin, time::Duration};

use tokio::time::{sleep, Instant, Sleep};

/// Timer that gradually increases the timeout from [`Self::min_duration`] to [`Self::max_duration`].
///
/// Can be polled through [`Self::as_mut`].
///
/// Next timer can be started with [`Self::start_next`].
///
/// Timeout increase can be reset by [`Self::reset_full`].
pub struct DynamicTimer {
    timer: Pin<Box<Sleep>>,
    last_reset: Instant,
    min_duration: Duration,
    max_duration: Duration,
}

impl DynamicTimer {
    pub fn new(min_duration: Duration, max_duration: Duration) -> Self {
        Self {
            timer: Box::pin(sleep(min_duration)),
            last_reset: Instant::now(),
            min_duration,
            max_duration,
        }
    }

    pub fn as_mut(&mut self) -> Pin<&mut Sleep> {
        self.timer.as_mut()
    }

    fn next_duration(&self) -> Duration {
        let now = Instant::now();
        let passed = now - self.last_reset;
        // sigmoid from ~2 at $passed \in [0; 10]$ to ~12 at passed ~= 40
        let duration = self.min_duration.as_secs_f64()
            + (self.max_duration - self.min_duration).as_secs_f64()
                / (1.0 + f64::exp(-passed.as_secs_f64() / 4.0 + 6.0));
        // `duration` should be within $[self.min_duration, self.max_duration]$ bounds
        // so `unwrap_or` is realistically not called
        Duration::try_from_secs_f64(duration).unwrap_or(self.max_duration)
    }

    pub fn start_next(&mut self) {
        self.timer = Box::pin(sleep(self.next_duration()))
    }

    /// Reset the timeout increase and the timer, if the reset
    /// one will finish sooner
    pub fn reset_full(&mut self) {
        self.last_reset = Instant::now();
        let reset_deadline = Instant::now() + self.next_duration();
        let current_deadline = self.timer.deadline();
        if reset_deadline < current_deadline {
            self.start_next()
        }
    }
}
