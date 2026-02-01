use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct RateLimiter {
    min_interval: Duration,
    last_request: Mutex<Instant>,
}

impl RateLimiter {
    pub fn new(min_interval: Duration) -> Self {
        Self {
            min_interval,
            last_request: Mutex::new(Instant::now() - min_interval),
        }
    }

    pub fn wait(&self) {
        if self.min_interval.is_zero() {
            return;
        }
        let mut last = self.last_request.lock().unwrap();
        let now = Instant::now();
        let next = *last + self.min_interval;
        if next > now {
            std::thread::sleep(next - now);
        }
        *last = Instant::now();
    }
}
