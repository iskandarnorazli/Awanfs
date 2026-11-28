"""Self-tests for check_profile.py.

    python3 scripts/test_check_profile.py
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_profile as cp  # noqa: E402

WORKFLOW = """\
jobs:
  s3tests:
    steps:
      - name: Build
        run: cargo build --locked --release --bin awanfs --manifest-path server/Cargo.toml
      - name: Start AwanFS
        run: bash tests/s3tests/start_server.sh
"""

START_SH = """\
#!/usr/bin/env bash
AWANFS_ACCESS_KEY=key AWANFS_LOG=off \\
  nohup "$BIN" --port 9000 --auto-compaction off &
"""

PROFILE_MD = """\
## Settings

| Setting | Value in profile | Default | Tests that need it | Safe in production? |
|---|---|---|---|---|
| `--port` | `9000` | `9000` | All | Yes |
| `--auto-compaction` | `off` | `on` | All | No |
| `AWANFS_ACCESS_KEY` | `key` | none | All | No |
| `AWANFS_LOG` | `off` | `on` | All | Optional |

## Skips

| Skip reason | Area | What would un-skip it |
|---|---|---|
| `kms_keyid` | `encryption` | Set `kms_keyid` in `s3tests.conf` |
"""

MAP_MD = """\
- `test_a` (skip): skipped
"""


class CheckProfile(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        for rel, content in [
            (".github/workflows/s3-tests.yml", WORKFLOW),
            ("tests/s3tests/start_server.sh", START_SH),
            ("tests/s3tests/PROFILE.md", PROFILE_MD),
            ("tests/s3tests/CONFORMANCE_MAP.md", MAP_MD),
        ]:
            p = self.root / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(content, encoding="utf-8")

    def test_valid_fixture_passes(self):
        self.assertEqual(cp.problems(self.root), [])

    def test_flag_in_script_without_row_in_profile_fails(self):
        sh = self.root / "tests/s3tests/start_server.sh"
        sh.write_text(START_SH + '  "$BIN" --lifecycle-day-seconds 10\n', encoding="utf-8")
        probs = cp.problems(self.root)
        self.assertTrue(any("--lifecycle-day-seconds" in p for p in probs), probs)

    def test_flag_in_profile_without_script_fails(self):
        prof = self.root / "tests/s3tests/PROFILE.md"
        prof.write_text(
            PROFILE_MD.replace(
                "| `AWANFS_LOG`", "| `--region` | `us-east-1` | `us-east-1` | All | Yes |\n| `AWANFS_LOG`"
            ),
            encoding="utf-8",
        )
        probs = cp.problems(self.root)
        self.assertTrue(any("--region" in p for p in probs), probs)

    def test_inline_s3_server_in_workflow_fails(self):
        wf = self.root / ".github/workflows/s3-tests.yml"
        wf.write_text(WORKFLOW + "      - run: server/target/release/awanfs --port 9000\n", encoding="utf-8")
        probs = cp.problems(self.root)
        self.assertTrue(any("outside start_server.sh" in p for p in probs), probs)

    def test_missing_start_server_in_workflow_fails(self):
        wf = self.root / ".github/workflows/s3-tests.yml"
        wf.write_text("jobs:\n  s3tests:\n    steps:\n      - run: echo hi\n", encoding="utf-8")
        probs = cp.problems(self.root)
        self.assertTrue(any("does not invoke start_server.sh" in p for p in probs), probs)

    def test_undocumented_skip_reason_in_map_fails(self):
        mp = self.root / "tests/s3tests/CONFORMANCE_MAP.md"
        mp.write_text("- `test_x` (skip): needs [s3 cloud] section\n", encoding="utf-8")
        probs = cp.problems(self.root)
        self.assertTrue(any("needs [s3 cloud] section" in p for p in probs), probs)

    def test_documented_skip_reason_in_map_passes(self):
        mp = self.root / "tests/s3tests/CONFORMANCE_MAP.md"
        mp.write_text("- `test_x` (skip): kms_keyid\n", encoding="utf-8")
        self.assertEqual(cp.problems(self.root), [])

    def test_repository_passes(self):
        self.assertEqual(cp.main([]), 0)


if __name__ == "__main__":
    unittest.main()
