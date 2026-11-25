//! Background task scheduler (#158).
//!
//! Runs periodic maintenance tasks (auto-compaction, incomplete multipart cleanup,
//! lifecycle, scrub) on the blocking pool, at most one tick at a time per task,
//! governed by [`crate::fsm::task_step`].

use crate::fsm::{task_step, TaskEvent, TaskState};
use crate::metrics::Metrics;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Default work budget per background tick.
pub const DEFAULT_TICK_BUDGET: usize = 64;

pub type TickFn = Arc<dyn Fn(usize) -> Result<bool, String> + Send + Sync>;

struct TaskEntry {
    name: String,
    period: Duration,
    budget: usize,
    state: Mutex<TaskState>,
    tick_fn: TickFn,
    done_notify: Arc<Notify>,
}

pub struct Scheduler {
    tasks: Mutex<Vec<Arc<TaskEntry>>>,
    handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    metrics: Arc<Metrics>,
    shutdown: Arc<AtomicBool>,
    stop_notify: Arc<Notify>,
}

impl Scheduler {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            tasks: Mutex::new(Vec::new()),
            handles: Mutex::new(Vec::new()),
            metrics,
            shutdown: Arc::new(AtomicBool::new(false)),
            stop_notify: Arc::new(Notify::new()),
        }
    }

    /// Shared shutdown signal checked by long-running background ticks such as scrub.
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    /// Registers a periodic task and starts its loop if a Tokio runtime is active.
    pub fn register<F>(&self, name: impl Into<String>, period: Duration, tick_fn: F)
    where
        F: Fn(usize) -> Result<bool, String> + Send + Sync + 'static,
    {
        self.register_with_budget(name, period, DEFAULT_TICK_BUDGET, tick_fn);
    }

    pub fn register_with_budget<F>(
        &self,
        name: impl Into<String>,
        period: Duration,
        budget: usize,
        tick_fn: F,
    ) where
        F: Fn(usize) -> Result<bool, String> + Send + Sync + 'static,
    {
        let entry = Arc::new(TaskEntry {
            name: name.into(),
            period: period.max(Duration::from_millis(1)),
            budget: budget.max(1),
            state: Mutex::new(TaskState::Idle),
            tick_fn: Arc::new(tick_fn),
            done_notify: Arc::new(Notify::new()),
        });
        self.tasks.lock().push(Arc::clone(&entry));

        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            let metrics = Arc::clone(&self.metrics);
            let shutdown = Arc::clone(&self.shutdown);
            let stop_notify = Arc::clone(&self.stop_notify);
            let h = rt.spawn(run_task_loop(entry, metrics, shutdown, stop_notify));
            self.handles.lock().push(h);
        }
    }

    /// Signals all tasks to stop and waits up to `timeout` for in-flight ticks to finish.
    /// Returns `true` if every task reached `Stopped` within `timeout`.
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.shutdown.store(true, Ordering::SeqCst);
        self.stop_notify.notify_waiters();

        let entries: Vec<Arc<TaskEntry>> = self.tasks.lock().clone();
        let deadline = Instant::now() + timeout;

        for entry in &entries {
            let mut wait_needed = false;
            {
                let mut st = entry.state.lock();
                if let Ok(next) = task_step(*st, TaskEvent::Stop) {
                    *st = next;
                }
                if *st == TaskState::Stopping {
                    wait_needed = true;
                }
            }
            if wait_needed {
                loop {
                    if *entry.state.lock() == TaskState::Stopped {
                        break;
                    }
                    let rem = deadline.saturating_duration_since(Instant::now());
                    if rem.is_zero() {
                        break;
                    }
                    let _ = tokio::time::timeout(rem, entry.done_notify.notified()).await;
                }
            }
        }

        let handles: Vec<_> = self.handles.lock().drain(..).collect();
        for h in handles {
            let rem = deadline.saturating_duration_since(Instant::now());
            if rem.is_zero() {
                h.abort();
            } else if tokio::time::timeout(rem, h).await.is_err() {
                // Timed out waiting for loop task; Stop already prevents new ticks.
            }
        }

        entries
            .iter()
            .all(|e| *e.state.lock() == TaskState::Stopped)
    }
}

async fn run_task_loop(
    entry: Arc<TaskEntry>,
    metrics: Arc<Metrics>,
    shutdown: Arc<AtomicBool>,
    stop_notify: Arc<Notify>,
) {
    let mut next_delay = entry.period;
    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        tokio::select! {
            _ = tokio::time::sleep(next_delay) => {}
            _ = stop_notify.notified() => {
                break;
            }
        }
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        // Transition Idle -> Running; refused if already Running, Stopping, or Stopped.
        {
            let mut st = entry.state.lock();
            match task_step(*st, TaskEvent::Tick) {
                Ok(next) => *st = next,
                Err(_) => {
                    next_delay = entry.period;
                    continue;
                }
            }
        }

        let entry_run = Arc::clone(&entry);
        let metrics_run = Arc::clone(&metrics);
        let res = tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                (entry_run.tick_fn)(entry_run.budget)
            }));
            let elapsed = start.elapsed().as_secs_f64();
            match outcome {
                Ok(Ok(more)) => {
                    metrics_run.record_background_tick(&entry_run.name, elapsed);
                    more
                }
                Ok(Err(e)) => {
                    eprintln!("background task '{}' failed: {}", entry_run.name, e);
                    false
                }
                Err(_) => {
                    eprintln!("background task '{}' panicked", entry_run.name);
                    false
                }
            }
        })
        .await;

        let more = res.unwrap_or(false);
        {
            let mut st = entry.state.lock();
            if let Ok(next) = task_step(*st, TaskEvent::Done) {
                *st = next;
            }
        }
        entry.done_notify.notify_waiters();

        next_delay = if more {
            entry.period.min(Duration::from_millis(50))
        } else {
            entry.period
        };
    }
}
