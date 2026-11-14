"""Self-tests for check_state_machines.py.

    python3 scripts/test_check_state_machines.py
"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_state_machines as c  # noqa: E402

HEAD = "| Machine | States | Status | Function | Tests | Model | Owner |\n|---|---|---|---|---|---|---|\n"
SRC = "pub fn step() {}\n#[cfg(test)]\nmod tests {\n    #[test]\n    fn every_pair() {}\n}\n"


class Check(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        with open(os.path.join(self.root, "m.rs"), "w", encoding="utf-8") as fh:
            fh.write(SRC)

    def run_on(self, row):
        return c.problems(HEAD + row + "\n", self.root)

    def test_a_true_explicit_row_passes(self):
        self.assertEqual(self.run_on("| M | a, b | explicit | `m.rs::step` | `every_pair` | none | #1 |"), [])

    def test_a_missing_function_is_reported(self):
        self.assertEqual(self.run_on("| M | a | explicit | `m.rs::gone` | `every_pair` | | |"), ["M: fn gone is not in m.rs"])

    def test_a_missing_test_is_reported(self):
        self.assertEqual(self.run_on("| M | a | explicit | `m.rs::step` | `no_such_test` | | |"), ["M: test no_such_test is not in m.rs"])

    def test_a_function_that_is_not_a_test_does_not_count_as_one(self):
        self.assertEqual(self.run_on("| M | a | explicit | `m.rs::step` | `step` | | |"), ["M: test step is not in m.rs"])

    def test_explicit_needs_a_function_and_a_test(self):
        got = self.run_on("| M | a | explicit | | | | |")
        self.assertEqual(got, ["M: explicit but no `file::function`", "M: explicit but no tests named"])

    def test_implicit_rows_are_not_checked(self):
        self.assertEqual(self.run_on("| M | a | implicit | `gone.rs` | whatever | | |"), [])

    def test_an_unknown_status_is_reported(self):
        self.assertIn("status 'done'", self.run_on("| M | a | done | | | | |")[0])

    def test_an_empty_table_is_a_problem(self):
        self.assertEqual(c.problems("no table here", self.root), ["no table rows found"])

    def test_the_repository_file_passes(self):
        self.assertEqual(c.main([]), 0)


if __name__ == "__main__":
    unittest.main()
