//! Background needle scrub (#155, decision 46).
//!
//! Periodically walks every open volume with positioned 1 MiB reads at a bounded MiB/s rate,
//! verifies needle header, key/attributes, and payload CRC32C checksums, transitions each
//! needle through [`crate::fsm::scrub_step`], and exposes the latest pass report for
//! `GET /_awanfs/scrub` and Prometheus metrics.

use crate::fsm::{scrub_step, ScrubEvent, ScrubState};
use crate::volume::{ScrubNeedleResult, VolumeManager};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Default background scrub rate in MiB/s (`--scrub-mib-per-sec`). `0` disables scrub.
pub const DEFAULT_SCRUB_MIB_PER_SEC: u64 = 10;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BadNeedle {
    pub key: String,
    pub volume_id: u32,
    pub offset: u64,
    pub error: String,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ScrubReport {
    pub pass: u64,
    pub checked: u64,
    pub good: u64,
    pub bad: u64,
    pub duration_seconds: f64,
    pub bad_needles: Vec<BadNeedle>,
}

#[derive(Default)]
struct ScrubInner {
    pass: u64,
    bad_in_pass: BTreeMap<(u32, u64), BadNeedle>,
    last_report: ScrubReport,
}

/// Tracks needle integrity across scrub passes and read-path checksum failures.
pub struct ScrubManager {
    inner: Mutex<ScrubInner>,
    corrupt_needles_total: AtomicU64,
    last_pass_duration_bits: AtomicU64,
    rate_mib_per_sec: AtomicU64,
}

impl Default for ScrubManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ScrubManager {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(ScrubInner::default()),
            corrupt_needles_total: AtomicU64::new(0),
            last_pass_duration_bits: AtomicU64::new(0f64.to_bits()),
            rate_mib_per_sec: AtomicU64::new(DEFAULT_SCRUB_MIB_PER_SEC),
        }
    }

    pub fn set_rate_mib_per_sec(&self, mib_per_sec: u64) {
        self.rate_mib_per_sec.store(mib_per_sec, Ordering::Relaxed);
    }

    pub fn rate_mib_per_sec(&self) -> u64 {
        self.rate_mib_per_sec.load(Ordering::Relaxed)
    }

    pub fn corrupt_needles_total(&self) -> u64 {
        self.corrupt_needles_total.load(Ordering::Relaxed)
    }

    pub fn last_pass_duration_seconds(&self) -> f64 {
        f64::from_bits(self.last_pass_duration_bits.load(Ordering::Relaxed))
    }

    pub fn report(&self) -> ScrubReport {
        self.inner.lock().last_report.clone()
    }

    /// Returns true if `(volume_id, offset)` was marked corrupt in the active or latest pass.
    pub fn is_bad(&self, volume_id: u32, offset: u64) -> bool {
        if self.corrupt_needles_total.load(Ordering::Relaxed) == 0 {
            return false;
        }
        let inner = self.inner.lock();
        inner.bad_in_pass.contains_key(&(volume_id, offset))
            || inner
                .last_report
                .bad_needles
                .iter()
                .any(|b| b.volume_id == volume_id && b.offset == offset)
    }

    /// Marks a needle as `Bad` after a read-path checksum or header mismatch (#155).
    pub fn mark_bad(&self, volume_id: u32, offset: u64, key: &str, error: String) {
        let mut inner = self.inner.lock();
        let cur = if inner.bad_in_pass.contains_key(&(volume_id, offset)) {
            ScrubState::Bad
        } else {
            ScrubState::Unchecked
        };
        let next = match scrub_step(cur, ScrubEvent::Corrupted) {
            Ok(s) => s,
            Err(_) => return,
        };
        debug_assert_eq!(next, ScrubState::Bad);
        let entry = BadNeedle {
            key: key.to_string(),
            volume_id,
            offset,
            error,
        };
        let is_new = inner
            .bad_in_pass
            .insert((volume_id, offset), entry.clone())
            .is_none();
        if is_new {
            self.corrupt_needles_total.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(existing) = inner
            .last_report
            .bad_needles
            .iter_mut()
            .find(|b| b.volume_id == volume_id && b.offset == offset)
        {
            *existing = entry;
        } else {
            inner.last_report.bad_needles.push(entry);
            inner.last_report.bad = inner.last_report.bad_needles.len() as u64;
        }
    }

    /// Runs one complete pass across `volumes` at `rate_mib_per_sec` (0 = unlimited for tests).
    pub fn run_pass(
        &self,
        volumes: &[Arc<VolumeManager>],
        rate_mib_per_sec: u64,
        stop: &AtomicBool,
    ) -> ScrubReport {
        let started = Instant::now();
        let pass = {
            let mut inner = self.inner.lock();
            inner.pass += 1;
            let mut reset_ok = true;
            for _ in inner.bad_in_pass.keys() {
                match scrub_step(ScrubState::Bad, ScrubEvent::NewPass) {
                    Ok(ScrubState::Unchecked) => {}
                    _ => {
                        reset_ok = false;
                        break;
                    }
                }
            }
            if reset_ok {
                inner.bad_in_pass.clear();
            }
            inner.pass
        };

        let rate_bytes_per_sec = rate_mib_per_sec.saturating_mul(1024 * 1024);
        let mut checked: u64 = 0;
        let mut good: u64 = 0;
        let mut bad: u64 = 0;
        let mut pass_bad: BTreeMap<(u32, u64), BadNeedle> = BTreeMap::new();

        for vm in volumes {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let vol_id = vm.id();
            if let Err(e) = vm.scrub_volume(rate_bytes_per_sec, stop, |res| {
                let (offset, key, err_opt) = match res {
                    ScrubNeedleResult::Good { offset, key, .. } => (offset, key, None),
                    ScrubNeedleResult::Bad {
                        offset,
                        key,
                        error,
                        ..
                    } => (offset, key, Some(error)),
                };
                let pre_bad = self.inner.lock().bad_in_pass.contains_key(&(vol_id, offset));
                let cur_state = if pre_bad {
                    ScrubState::Bad
                } else {
                    ScrubState::Unchecked
                };
                let ev = if err_opt.is_some() {
                    ScrubEvent::Corrupted
                } else {
                    ScrubEvent::VerifiedOk
                };
                checked += 1;
                match scrub_step(cur_state, ev) {
                    Ok(ScrubState::Good) => {
                        good += 1;
                    }
                    Ok(ScrubState::Bad) => {
                        bad += 1;
                        let err = err_opt.unwrap_or_else(|| "corrupt needle".to_string());
                        if !pre_bad {
                            self.corrupt_needles_total.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(
                                volume_id = vol_id,
                                offset = offset,
                                key = %key,
                                error = %err,
                                "background scrub detected corrupt needle"
                            );
                        }
                        pass_bad.insert(
                            (vol_id, offset),
                            BadNeedle {
                                key,
                                volume_id: vol_id,
                                offset,
                                error: err,
                            },
                        );
                    }
                    Ok(ScrubState::Unchecked) => {}
                    Err(_) => {
                        // Marked bad by a concurrent read during this pass; keep it bad.
                        bad += 1;
                        if let Some(existing) =
                            self.inner.lock().bad_in_pass.get(&(vol_id, offset)).cloned()
                        {
                            pass_bad.insert((vol_id, offset), existing);
                        }
                    }
                }
            }) {
                tracing::error!(volume_id = vol_id, error = ?e, "background scrub failed on volume");
            }
        }

        let duration_seconds = started.elapsed().as_secs_f64();
        self.last_pass_duration_bits
            .store(duration_seconds.to_bits(), Ordering::Relaxed);

        let mut inner = self.inner.lock();
        // Preserve any bad needles marked by concurrent reads during the pass.
        for (k, v) in &inner.bad_in_pass {
            if !pass_bad.contains_key(k) {
                pass_bad.insert(*k, v.clone());
                bad += 1;
            }
        }
        inner.bad_in_pass = pass_bad.clone();
        let report = ScrubReport {
            pass,
            checked,
            good,
            bad,
            duration_seconds,
            bad_needles: pass_bad.into_values().collect(),
        };
        inner.last_report = report.clone();
        report
    }
}
