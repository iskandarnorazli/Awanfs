//! #158: Background task scheduler, FSM oracle property test, metrics, and multipart cleanup.

use s3_server::clock::{Clock, ManualClock};
use s3_server::fsm::{task_step, TaskEvent, TaskState};
use s3_server::metrics::Metrics;
use s3_server::multipart::MultipartManager;
use s3_server::scheduler::Scheduler;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduler_runs_one_at_a_time_and_stops_cleanly() {
    let metrics = Arc::new(Metrics::default());
    let sched = Scheduler::new(Arc::clone(&metrics));

    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_in_flight = Arc::new(AtomicUsize::new(0));
    let ticks = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));

    let inf = Arc::clone(&in_flight);
    let max_inf = Arc::clone(&max_in_flight);
    let t_cnt = Arc::clone(&ticks);
    let ent = Arc::clone(&entered);
    let rel = Arc::clone(&release);

    sched.register("slow_task", Duration::from_millis(10), move |_budget| {
        let cur = inf.fetch_add(1, Ordering::SeqCst) + 1;
        max_inf.fetch_max(cur, Ordering::SeqCst);
        ent.store(true, Ordering::SeqCst);
        while !rel.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(5));
        }
        t_cnt.fetch_add(1, Ordering::SeqCst);
        inf.fetch_sub(1, Ordering::SeqCst);
        Ok(false)
    });

    // Wait for the first tick to enter, then let several periods pass while it is still running.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while !entered.load(Ordering::SeqCst) {
        assert!(tokio::time::Instant::now() < deadline, "task never started");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(max_in_flight.load(Ordering::SeqCst), 1, "must never run two ticks of one task concurrently");

    release.store(true, Ordering::SeqCst);
    assert!(sched.shutdown(Duration::from_secs(2)).await, "shutdown should complete cleanly");
    let count_at_stop = ticks.load(Ordering::SeqCst);
    assert!(count_at_stop >= 1);

    // No tick may run after shutdown returns.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(ticks.load(Ordering::SeqCst), count_at_stop);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panicking_or_failing_task_is_retried_and_does_not_stop_others() {
    let metrics = Arc::new(Metrics::default());
    let sched = Scheduler::new(Arc::clone(&metrics));

    let panic_attempts = Arc::new(AtomicUsize::new(0));
    let err_attempts = Arc::new(AtomicUsize::new(0));
    let healthy_ticks = Arc::new(AtomicUsize::new(0));

    let pa = Arc::clone(&panic_attempts);
    sched.register("panicky", Duration::from_millis(15), move |_| {
        pa.fetch_add(1, Ordering::SeqCst);
        panic!("boom");
    });

    let ea = Arc::clone(&err_attempts);
    sched.register("errory", Duration::from_millis(15), move |_| {
        ea.fetch_add(1, Ordering::SeqCst);
        Err("transient error".to_string())
    });

    let ht = Arc::clone(&healthy_ticks);
    sched.register("healthy", Duration::from_millis(15), move |_| {
        ht.fetch_add(1, Ordering::SeqCst);
        Ok(false)
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while panic_attempts.load(Ordering::SeqCst) < 2
        || err_attempts.load(Ordering::SeqCst) < 2
        || healthy_ticks.load(Ordering::SeqCst) < 2
    {
        assert!(tokio::time::Instant::now() < deadline, "tasks did not retry/progress");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(sched.shutdown(Duration::from_secs(2)).await);

    let mut out = String::new();
    metrics.render(&mut out);
    assert!(out.contains("awanfs_background_ticks_total{task=\"healthy\"}"));
    assert!(out.contains("awanfs_background_tick_seconds_count{task=\"healthy\"}"));
}

#[test]
fn property_random_interleavings_match_fsm_oracle() {
    let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next_u64 = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };

    for _run in 0..200 {
        let mut oracle = TaskState::Idle;
        let mut active_ticks = 0usize;
        for _step in 0..50 {
            let ev = match next_u64() % 3 {
                0 => TaskEvent::Tick,
                1 => TaskEvent::Done,
                _ => TaskEvent::Stop,
            };
            match task_step(oracle, ev) {
                Ok(next) => {
                    match ev {
                        TaskEvent::Tick => active_ticks += 1,
                        TaskEvent::Done => active_ticks -= 1,
                        TaskEvent::Stop => {}
                    }
                    assert!(active_ticks <= 1, "two ticks ran at once!");
                    oracle = next;
                }
                Err(_) => {
                    assert!(active_ticks <= 1);
                }
            }
        }
    }
}

#[test]
fn incomplete_multipart_cleanup_respects_clock_and_budget() {
    let clock = Arc::new(ManualClock::new(1_700_000_000_000_000_000));
    let dir = std::env::temp_dir().join(format!("awanfs_mp_clean_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mp = MultipartManager::with_staging_dir(dir.clone()).with_clock(clock.clone());

    let u1 = mp.create_upload("bkt", 1, "k1").unwrap();
    let u2 = mp.create_upload("bkt", 1, "k2").unwrap();
    clock.advance(10_000_000_000); // +10s
    let u3 = mp.create_upload("bkt", 1, "k3").unwrap();

    // Stopped clock: max_age 60s -> nothing expires.
    let (removed, more) = mp.cleanup_expired(clock.now_ns() / 1_000_000, 60_000, 10);
    assert_eq!((removed, more), (0, false));
    assert!(mp.get_upload(&u1).is_some());

    // Advance 55s -> u1 and u2 are 65s old, u3 is 55s old. Budget 1 -> removes 1, more=true.
    clock.advance(55_000_000_000);
    let (removed1, more1) = mp.cleanup_expired(clock.now_ns() / 1_000_000, 60_000, 1);
    assert_eq!((removed1, more1), (1, true));

    // Next tick with budget 10 removes the other old upload, leaves u3.
    let (removed2, more2) = mp.cleanup_expired(clock.now_ns() / 1_000_000, 60_000, 10);
    assert_eq!((removed2, more2), (1, false));
    assert!(mp.get_upload(&u1).is_none() || mp.get_upload(&u2).is_none());
    assert!(mp.get_upload(&u3).is_some());

    let _ = std::fs::remove_dir_all(&dir);
}
