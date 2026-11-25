//! Reference oracle for deterministic volume crash-recovery simulation (#213, #290).
//!
//! Pure deterministic Rust with no I/O: tracks [`SimOperation`]s and their [`Outcome`]s, and
//! computes the allowed recovered states per key after a crash (Decision 62).

use crate::sim_gen::SimOperation;
use std::collections::{BTreeMap, BTreeSet};

/// Whether a recorded operation's call succeeded or returned an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct KeyState {
    durable: Option<(u32, u64)>,
    after_durable: BTreeSet<Option<(u32, u64)>>,
    last_unsynced_ok: Option<Option<(u32, u64)>>,
    after_last_ok: BTreeSet<Option<(u32, u64)>>,
}

impl KeyState {
    fn settled(value: Option<(u32, u64)>) -> Self {
        Self {
            durable: value,
            after_durable: BTreeSet::new(),
            last_unsynced_ok: None,
            after_last_ok: BTreeSet::new(),
        }
    }

    fn record_write(&mut self, value: Option<(u32, u64)>, outcome: Outcome) {
        self.after_durable.insert(value);
        match outcome {
            Outcome::Ok => {
                self.last_unsynced_ok = Some(value);
                self.after_last_ok.clear();
            }
            Outcome::Failed => {
                self.after_last_ok.insert(value);
            }
        }
    }

    fn sync_ok(&mut self) {
        if let Some(value) = self.last_unsynced_ok.take() {
            self.durable = value;
            self.after_durable = std::mem::take(&mut self.after_last_ok);
        }
    }

    fn allowed(&self) -> BTreeSet<Option<(u32, u64)>> {
        let mut set = self.after_durable.clone();
        set.insert(self.durable);
        set
    }
}

/// Whether a key value is required, permitted, or forbidden after a crash (#213).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Survives {
    Yes,
    Maybe,
    No,
}

/// Pure reference model of which key values may survive a crash and recovery.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SimModel {
    keys: BTreeMap<u8, KeyState>,
}

impl SimModel {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one operation and whether its call succeeded.
    pub fn record(&mut self, op: &SimOperation, outcome: Outcome) {
        match *op {
            SimOperation::Put { key, len, fill } => {
                self.record_key_write(key, Some((len, fill)), outcome);
            }
            SimOperation::Delete { key } => {
                self.record_key_write(key, None, outcome);
            }
            SimOperation::Sync => {
                if outcome == Outcome::Ok {
                    for state in self.keys.values_mut() {
                        state.sync_ok();
                    }
                }
            }
            SimOperation::Fault { .. } | SimOperation::Crash { .. } => {}
        }
    }

    fn record_key_write(&mut self, key: u8, value: Option<(u32, u64)>, outcome: Outcome) {
        self.keys
            .entry(key)
            .or_insert_with(|| KeyState::settled(None))
            .record_write(value, outcome);
    }

    /// After a `Crash`: the states each key may have once recovered. A state is
    /// `None` (absent) or `Some((len, fill))`, the whole value of one write.
    pub fn allowed(&self) -> BTreeMap<u8, BTreeSet<Option<(u32, u64)>>> {
        self.keys
            .iter()
            .map(|(&key, state)| (key, state.allowed()))
            .collect()
    }

    /// Called with what recovery actually found, to continue from it.
    pub fn after_recovery(&mut self, found: &BTreeMap<u8, Option<(u32, u64)>>) {
        let all_keys: BTreeSet<u8> = self.keys.keys().chain(found.keys()).copied().collect();
        let mut next = BTreeMap::new();
        for key in all_keys {
            let value = found.get(&key).copied().flatten();
            next.insert(key, KeyState::settled(value));
        }
        self.keys = next;
    }

    /// Classifies `value` for `key` against the current survival set after a crash.
    pub fn survival(&self, key: u8, value: Option<(u32, u64)>) -> Survives {
        let allowed = self
            .keys
            .get(&key)
            .map(KeyState::allowed)
            .unwrap_or_else(|| BTreeSet::from([None]));
        if !allowed.contains(&value) {
            Survives::No
        } else if allowed.len() == 1 {
            Survives::Yes
        } else {
            Survives::Maybe
        }
    }

    /// Ok, or the first key whose found state is not allowed, with both.
    pub fn check(&self, found: &BTreeMap<u8, Option<(u32, u64)>>) -> Result<(), String> {
        let allowed = self.allowed();
        let all_keys: BTreeSet<u8> = allowed.keys().chain(found.keys()).copied().collect();
        for key in all_keys {
            let found_state = found.get(&key).copied().flatten();
            let allowed_states = allowed
                .get(&key)
                .cloned()
                .unwrap_or_else(|| BTreeSet::from([None]));
            if !allowed_states.contains(&found_state) {
                return Err(format!(
                    "key {key}: found {found_state:?}, allowed {allowed_states:?}"
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::{CrashMode, SimFault, SimOp};
    use crate::sim_gen::{self, SimFileKind};
    use proptest::prelude::*;
    use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};

    fn set(items: impl IntoIterator<Item = Option<(u32, u64)>>) -> BTreeSet<Option<(u32, u64)>> {
        items.into_iter().collect()
    }

    #[test]
    fn synced_put_allows_only_that_value() {
        for mode in [CrashMode::DropUnsynced, CrashMode::SeededPrefix] {
            let mut m = SimModel::new();
            m.record(&SimOperation::Put { key: 1, len: 64, fill: 42 }, Outcome::Ok);
            m.record(&SimOperation::Sync, Outcome::Ok);
            m.record(&SimOperation::Crash { mode }, Outcome::Ok);

            let allowed = m.allowed();
            assert_eq!(allowed.get(&1), Some(&set([Some((64, 42))])), "mode {mode:?}");
            assert!(m.check(&BTreeMap::from([(1, Some((64, 42)))])).is_ok(), "mode {mode:?}");
            assert!(m.check(&BTreeMap::new()).is_err(), "absent key must fail check");
            assert!(m.check(&BTreeMap::from([(1, None)])).is_err(), "None must fail check");
            assert!(m.check(&BTreeMap::from([(1, Some((64, 99)))])).is_err(), "wrong value must fail check");
        }
    }

    #[test]
    fn unsynced_put_after_synced_allows_both_values_and_forbids_absent() {
        let mut m = SimModel::new();
        m.record(&SimOperation::Put { key: 2, len: 100, fill: 1 }, Outcome::Ok);
        m.record(&SimOperation::Sync, Outcome::Ok);
        m.record(&SimOperation::Put { key: 2, len: 200, fill: 2 }, Outcome::Ok);
        m.record(&SimOperation::Crash { mode: CrashMode::SeededPrefix }, Outcome::Ok);

        let allowed = m.allowed();
        assert_eq!(allowed.get(&2), Some(&set([Some((100, 1)), Some((200, 2))])));
        assert!(!allowed[&2].contains(&None));
        assert!(m.check(&BTreeMap::from([(2, Some((100, 1)))])).is_ok());
        assert!(m.check(&BTreeMap::from([(2, Some((200, 2)))])).is_ok());
        assert!(m.check(&BTreeMap::new()).is_err());
        assert!(m.check(&BTreeMap::from([(2, None)])).is_err());
    }

    #[test]
    fn delete_synced_allows_only_absent_and_unsynced_allows_absent_or_previous() {
        let mut synced = SimModel::new();
        synced.record(&SimOperation::Put { key: 3, len: 50, fill: 7 }, Outcome::Ok);
        synced.record(&SimOperation::Sync, Outcome::Ok);
        synced.record(&SimOperation::Delete { key: 3 }, Outcome::Ok);
        synced.record(&SimOperation::Sync, Outcome::Ok);
        synced.record(&SimOperation::Crash { mode: CrashMode::DropUnsynced }, Outcome::Ok);

        assert_eq!(synced.allowed().get(&3), Some(&set([None])));
        assert!(synced.check(&BTreeMap::new()).is_ok());
        assert!(synced.check(&BTreeMap::from([(3, None)])).is_ok());
        assert!(synced.check(&BTreeMap::from([(3, Some((50, 7)))])).is_err());

        let mut unsynced = SimModel::new();
        unsynced.record(&SimOperation::Put { key: 4, len: 50, fill: 8 }, Outcome::Ok);
        unsynced.record(&SimOperation::Sync, Outcome::Ok);
        unsynced.record(&SimOperation::Delete { key: 4 }, Outcome::Ok);
        unsynced.record(&SimOperation::Crash { mode: CrashMode::SeededPrefix }, Outcome::Ok);

        assert_eq!(unsynced.allowed().get(&4), Some(&set([None, Some((50, 8))])));
        assert!(unsynced.check(&BTreeMap::new()).is_ok());
        assert!(unsynced.check(&BTreeMap::from([(4, None)])).is_ok());
        assert!(unsynced.check(&BTreeMap::from([(4, Some((50, 8)))])).is_ok());
    }

    #[test]
    fn failed_put_allows_previous_state_and_new_value_only() {
        let mut from_absent = SimModel::new();
        from_absent.record(&SimOperation::Put { key: 5, len: 32, fill: 9 }, Outcome::Failed);
        from_absent.record(&SimOperation::Crash { mode: CrashMode::SeededPrefix }, Outcome::Ok);
        assert_eq!(from_absent.allowed().get(&5), Some(&set([None, Some((32, 9))])));

        let mut from_synced = SimModel::new();
        from_synced.record(&SimOperation::Put { key: 6, len: 16, fill: 3 }, Outcome::Ok);
        from_synced.record(&SimOperation::Sync, Outcome::Ok);
        from_synced.record(&SimOperation::Put { key: 6, len: 32, fill: 4 }, Outcome::Failed);
        from_synced.record(&SimOperation::Crash { mode: CrashMode::SeededPrefix }, Outcome::Ok);
        assert_eq!(from_synced.allowed().get(&6), Some(&set([Some((16, 3)), Some((32, 4))])));
        assert!(from_synced.check(&BTreeMap::from([(6, None)])).is_err());

        from_synced.record(&SimOperation::Sync, Outcome::Ok);
        from_synced.record(&SimOperation::Crash { mode: CrashMode::DropUnsynced }, Outcome::Ok);
        assert_eq!(
            from_synced.allowed().get(&6),
            Some(&set([Some((16, 3)), Some((32, 4))])),
            "Sync after Failed Put leaves prior durable and failed value allowed",
        );

        from_synced.record(&SimOperation::Put { key: 6, len: 64, fill: 5 }, Outcome::Ok);
        from_synced.record(&SimOperation::Sync, Outcome::Ok);
        from_synced.record(&SimOperation::Crash { mode: CrashMode::DropUnsynced }, Outcome::Ok);
        assert_eq!(
            from_synced.allowed().get(&6),
            Some(&set([Some((64, 5))])),
            "later synced Ok Put supersedes prior durable and failed states",
        );
    }

    #[test]
    fn failed_sync_keeps_prior_writes_as_may() {
        let mut m = SimModel::new();
        m.record(&SimOperation::Put { key: 1, len: 10, fill: 1 }, Outcome::Ok);
        m.record(&SimOperation::Sync, Outcome::Failed);
        m.record(&SimOperation::Crash { mode: CrashMode::SeededPrefix }, Outcome::Ok);
        assert_eq!(m.allowed().get(&1), Some(&set([None, Some((10, 1))])));

        let mut after_durable = SimModel::new();
        after_durable.record(&SimOperation::Put { key: 2, len: 10, fill: 1 }, Outcome::Ok);
        after_durable.record(&SimOperation::Put { key: 3, len: 30, fill: 3 }, Outcome::Ok);
        after_durable.record(&SimOperation::Sync, Outcome::Ok);
        after_durable.record(&SimOperation::Put { key: 2, len: 20, fill: 2 }, Outcome::Ok);
        after_durable.record(&SimOperation::Delete { key: 3 }, Outcome::Ok);
        after_durable.record(&SimOperation::Sync, Outcome::Failed);
        after_durable.record(&SimOperation::Crash { mode: CrashMode::SeededPrefix }, Outcome::Ok);

        assert_eq!(after_durable.allowed().get(&2), Some(&set([Some((10, 1)), Some((20, 2))])));
        assert_eq!(after_durable.allowed().get(&3), Some(&set([None, Some((30, 3))])));
    }

    #[test]
    fn after_recovery_starts_later_crashes_from_found_state() {
        let mut m = SimModel::new();
        m.record(&SimOperation::Put { key: 3, len: 30, fill: 3 }, Outcome::Ok);
        m.record(&SimOperation::Sync, Outcome::Ok);
        m.record(&SimOperation::Put { key: 1, len: 10, fill: 1 }, Outcome::Ok);
        m.record(&SimOperation::Put { key: 2, len: 20, fill: 2 }, Outcome::Ok);
        m.record(&SimOperation::Delete { key: 3 }, Outcome::Ok);
        m.record(&SimOperation::Crash { mode: CrashMode::SeededPrefix }, Outcome::Ok);

        let found = BTreeMap::from([(1, Some((10, 1))), (3, None)]);
        assert!(m.check(&found).is_ok());
        m.after_recovery(&found);

        m.record(&SimOperation::Crash { mode: CrashMode::SeededPrefix }, Outcome::Ok);
        let after_second_crash = m.allowed();
        assert_eq!(after_second_crash.get(&1), Some(&set([Some((10, 1))])));
        assert_eq!(after_second_crash.get(&2), Some(&set([None])));
        assert_eq!(after_second_crash.get(&3), Some(&set([None])));

        m.record(&SimOperation::Put { key: 1, len: 11, fill: 11 }, Outcome::Ok);
        m.record(&SimOperation::Put { key: 2, len: 22, fill: 22 }, Outcome::Ok);
        m.record(&SimOperation::Crash { mode: CrashMode::SeededPrefix }, Outcome::Ok);
        let after_third_crash = m.allowed();
        assert_eq!(after_third_crash.get(&1), Some(&set([Some((10, 1)), Some((11, 11))])));
        assert_eq!(after_third_crash.get(&2), Some(&set([None, Some((22, 22))])));
    }

    #[test]
    fn per_key_order_zero_byte_puts_faults_and_check_errors() {
        let mut m = SimModel::new();
        m.record(&SimOperation::Fault { file: SimFileKind::Data, op: SimOp::Write, fault: SimFault::Eio }, Outcome::Ok);
        m.record(&SimOperation::Put { key: 0, len: 10, fill: 1 }, Outcome::Failed);
        m.record(&SimOperation::Put { key: 0, len: 0, fill: 0 }, Outcome::Ok);
        m.record(&SimOperation::Sync, Outcome::Ok);

        assert_eq!(
            m.allowed().get(&0),
            Some(&set([Some((0, 0))])),
            "synced Ok Put supersedes earlier failed Put and keeps zero-byte value distinct from None",
        );

        m.record(&SimOperation::Put { key: 1, len: 10, fill: 1 }, Outcome::Ok);
        m.record(&SimOperation::Put { key: 1, len: 20, fill: 2 }, Outcome::Failed);
        m.record(&SimOperation::Sync, Outcome::Ok);
        assert_eq!(m.allowed().get(&1), Some(&set([Some((10, 1)), Some((20, 2))])));

        let err_key0 = m.check(&BTreeMap::from([(0, None), (1, Some((99, 99)))])).unwrap_err();
        assert!(err_key0.contains('0'), "first failing key must be 0: {err_key0}");
        assert!(err_key0.contains("None") && err_key0.contains("(0, 0)"), "must show found and allowed: {err_key0}");

        let err_extra = m.check(&BTreeMap::from([
            (0, Some((0, 0))),
            (1, Some((10, 1))),
            (7, Some((1, 2))),
        ])).unwrap_err();
        assert!(err_extra.contains('7'), "unexpected key 7 must be reported: {err_extra}");
        assert!(m.check(&BTreeMap::from([
            (0, Some((0, 0))),
            (1, Some((10, 1))),
            (7, None),
        ])).is_ok(), "untouched key explicitly mapped to None must be accepted");
    }

    #[test]
    fn survival_classifies_yes_maybe_and_no() {
        let mut m = SimModel::new();
        m.record(&SimOperation::Put { key: 1, len: 10, fill: 1 }, Outcome::Ok);
        m.record(&SimOperation::Sync, Outcome::Ok);
        assert_eq!(m.survival(1, Some((10, 1))), Survives::Yes);
        assert_eq!(m.survival(1, Some((10, 2))), Survives::No);
        assert_eq!(m.survival(1, None), Survives::No);

        m.record(&SimOperation::Put { key: 1, len: 20, fill: 2 }, Outcome::Ok);
        assert_eq!(m.survival(1, Some((10, 1))), Survives::Maybe);
        assert_eq!(m.survival(1, Some((20, 2))), Survives::Maybe);
        assert_eq!(m.survival(1, None), Survives::No);
        assert_eq!(m.survival(2, None), Survives::Yes);
        assert_eq!(m.survival(2, Some((1, 1))), Survives::No);
    }

    fn assert_exact_synced_crash(
        model: &mut SimModel,
        crash: &SimOperation,
        expected: &BTreeMap<u8, Option<(u32, u64)>>,
    ) -> Result<(), TestCaseError> {
        model.record(&SimOperation::Sync, Outcome::Ok);
        model.record(crash, Outcome::Ok);
        let allowed = model.allowed();
        prop_assert_eq!(allowed.len(), expected.len());
        for (k, v) in expected {
            prop_assert_eq!(allowed.get(k), Some(&set([*v])));
        }
        prop_assert_eq!(model.check(expected), Ok(()));
        Ok(())
    }

    #[test]
    fn prop_all_ok_all_synced_yields_exact_last_value_per_key() {
        let mut runner = TestRunner::new_with_rng(
            Config { cases: 256, failure_persistence: None, ..Config::default() },
            TestRng::deterministic_rng(RngAlgorithm::ChaCha),
        );
        runner
            .run(&(any::<u64>(), 1usize..=200), |(seed, len)| {
                let ops = sim_gen::generate(seed, len);
                let mut model = SimModel::new();
                let mut expected: BTreeMap<u8, Option<(u32, u64)>> = BTreeMap::new();

                for op in &ops {
                    match *op {
                        SimOperation::Put { key, len, fill } => {
                            expected.insert(key, Some((len, fill)));
                            model.record(op, Outcome::Ok);
                        }
                        SimOperation::Delete { key } => {
                            expected.insert(key, None);
                            model.record(op, Outcome::Ok);
                        }
                        SimOperation::Sync | SimOperation::Fault { .. } => {
                            model.record(op, Outcome::Ok);
                        }
                        SimOperation::Crash { .. } => {
                            assert_exact_synced_crash(&mut model, op, &expected)?;
                            model.after_recovery(&expected);
                        }
                    }
                }

                assert_exact_synced_crash(
                    &mut model,
                    &SimOperation::Crash { mode: CrashMode::DropUnsynced },
                    &expected,
                )
            })
            .unwrap();
    }

    #[test]
    fn prop_mixed_outcomes_match_exhaustive_crash_subsets() {
        let op_strategy = prop_oneof![
            (0u8..3, 0u32..8, 0u64..8).prop_map(|(key, len, fill)| SimOperation::Put { key, len, fill }),
            (0u8..3).prop_map(|key| SimOperation::Delete { key }),
            Just(SimOperation::Sync),
        ];
        let outcome_strategy = prop_oneof![Just(Outcome::Ok), Just(Outcome::Failed)];
        let seq_strategy = proptest::collection::vec((op_strategy, outcome_strategy), 0..=14);

        let mut runner = TestRunner::new_with_rng(
            Config { cases: 256, failure_persistence: None, ..Config::default() },
            TestRng::deterministic_rng(RngAlgorithm::ChaCha),
        );
        runner
            .run(&seq_strategy, |seq| {
                let mut model = SimModel::new();
                for (op, outcome) in &seq {
                    model.record(op, *outcome);
                }
                model.record(&SimOperation::Crash { mode: CrashMode::SeededPrefix }, Outcome::Ok);

                let last_sync_ok = seq
                    .iter()
                    .rposition(|(op, outcome)| matches!(op, SimOperation::Sync) && *outcome == Outcome::Ok);

                let mut by_key: BTreeMap<u8, Vec<(Option<(u32, u64)>, bool)>> = BTreeMap::new();
                for (idx, (op, outcome)) in seq.iter().enumerate() {
                    let forced = *outcome == Outcome::Ok && last_sync_ok.is_some_and(|s| idx < s);
                    match *op {
                        SimOperation::Put { key, len, fill } => {
                            by_key.entry(key).or_default().push((Some((len, fill)), forced));
                        }
                        SimOperation::Delete { key } => {
                            by_key.entry(key).or_default().push((None, forced));
                        }
                        SimOperation::Sync | SimOperation::Fault { .. } | SimOperation::Crash { .. } => {}
                    }
                }

                let actual = model.allowed();
                prop_assert_eq!(actual.len(), by_key.len());
                for (key, writes) in &by_key {
                    let optional_indices: Vec<usize> = writes
                        .iter()
                        .enumerate()
                        .filter_map(|(i, (_, forced))| (!forced).then_some(i))
                        .collect();
                    let mut expected = BTreeSet::new();
                    for mask in 0u32..(1u32 << optional_indices.len()) {
                        let mut state = None;
                        for (i, (val, forced)) in writes.iter().enumerate() {
                            let landed = if *forced {
                                true
                            } else {
                                let opt_pos = optional_indices.iter().position(|&idx| idx == i).unwrap();
                                (mask & (1u32 << opt_pos)) != 0
                            };
                            if landed {
                                state = *val;
                            }
                        }
                        expected.insert(state);
                    }
                    prop_assert_eq!(actual.get(key), Some(&expected));
                }
                Ok(())
            })
            .unwrap();
    }
}
