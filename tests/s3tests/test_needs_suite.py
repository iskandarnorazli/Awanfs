"""Tests for needs_suite.py (#458): which runs of s3-tests.yml skip the ceph suite."""

import io
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import needs_suite  # noqa: E402
from needs_suite import MAP, MAP_ONLY, NO_SERVER_CODE, decide  # noqa: E402

PR = "pull_request"


class PullRequest(unittest.TestCase):
    def test_map_only_skips(self):
        self.assertEqual(decide(PR, "refs/pull/395/merge", [MAP]), (False, MAP_ONLY))

    def test_map_plus_any_other_file_runs(self):
        for other in ["README.md", "server/src/auth.rs", "tests/s3tests/passing.txt", "scripts/features.py"]:
            with self.subTest(other=other):
                # As before #458: the map is an s3-tests file, so with anything else it runs.
                self.assertEqual(decide(PR, "", [MAP, other]), (True, None))

    def test_a_server_file_runs(self):
        self.assertEqual(decide(PR, "", ["server/src/ops/object.rs"]), (True, None))
        self.assertEqual(decide(PR, "", ["server/Cargo.lock"]), (True, None))

    def test_the_workflow_itself_runs(self):
        self.assertEqual(decide(PR, "", [".github/workflows/s3-tests.yml"]), (True, None))

    def test_no_server_code_skips(self):
        self.assertEqual(decide(PR, "", ["README.md", "docs/x.md"]), (False, NO_SERVER_CODE))


class OtherEvents(unittest.TestCase):
    def test_dispatch_on_the_map_branch(self):
        ref = needs_suite.MAP_BRANCH
        self.assertEqual(decide("workflow_dispatch", ref, [MAP]), (False, MAP_ONLY))
        self.assertEqual(decide("workflow_dispatch", ref, [MAP, "server/src/auth.rs"]), (True, None))
        self.assertEqual(decide("workflow_dispatch", ref, []), (True, None))

    def test_schedule_and_other_dispatches_run(self):
        self.assertEqual(decide("schedule", "refs/heads/main", [MAP]), (True, None))
        self.assertEqual(decide("workflow_dispatch", "refs/heads/main", [MAP]), (True, None))


class Cli(unittest.TestCase):
    def test_writes_the_output_and_the_summary(self):
        with tempfile.TemporaryDirectory() as d:
            out, summ = os.path.join(d, "out"), os.path.join(d, "summary")
            env = {"GITHUB_OUTPUT": out, "GITHUB_STEP_SUMMARY": summ}
            with mock.patch.dict(os.environ, env), mock.patch("sys.stdin", io.StringIO(MAP + "\n")), redirect_stdout(io.StringIO()):
                self.assertEqual(needs_suite.main(["--event", PR]), 0)
            with open(out) as f:
                self.assertEqual(f.read(), "run=false\n")
            with open(summ) as f:
                self.assertEqual(f.read(), MAP_ONLY + "\n")


if __name__ == "__main__":
    unittest.main()
