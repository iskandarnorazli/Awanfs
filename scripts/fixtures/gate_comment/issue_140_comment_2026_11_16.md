Implemented in Batch 3 draft PR https://github.com/iskandarnorazli/Awanfs/pull/216 on branch `batch/batch3-conformance-wiring`.

### Gate verification
- `cargo test -j 1 --locked --no-fail-fast --manifest-path server/Cargo.toml`: exit 0 (90 suites, 395 passed, 0 failed, 3 ignored)
- `cargo check --manifest-path server/fuzz/Cargo.toml`: exit 0
- `python scripts/test_check_state_machines.py && python scripts/check_state_machines.py`: OK (9 tests + check)
- `python scripts/test_check_profile.py && python scripts/check_profile.py`: OK (8 tests + check)
- `python tests/s3tests/test_conformance_map.py && python tests/s3tests/test_ratchet.py && python tests/s3tests/test_triage.py`: OK (8 + 3 + 3 tests)