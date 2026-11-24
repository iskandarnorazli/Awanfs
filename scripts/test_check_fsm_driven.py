"""Self-tests for check_fsm_driven.py.

    python3 scripts/test_check_fsm_driven.py
"""
import contextlib
import io
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_fsm_driven as cfd  # noqa: E402

FSM_RS = """\
pub enum S { A, B }
pub enum E { Go }
pub struct Refusal { pub code: &'static str }

pub fn x_step(s: S, _e: E) -> Result<S, Refusal> {
    Ok(s)
}

pub fn other_step(s: S, _e: E) -> Result<S, Refusal> {
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn discards_in_fsm_tests_are_ignored() {
        let _ = x_step(S::A, E::Go);
    }
}
"""


class CheckFsmDriven(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: shutil.rmtree(self.root, ignore_errors=True))
        self.write("server/src/fsm.rs", FSM_RS)

    def write(self, rel: str, content: str):
        p = self.root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")

    def run_cli(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = cfd.main([str(self.root)])
        return code, buf.getvalue()

    def test_discarded_let_underscore_qualified_is_flagged(self):
        self.write(
            "server/src/handler.rs",
            "fn f(s: S, e: E) {\n    let _ = crate::fsm::x_step(s, e);\n}\n",
        )
        code, out = self.run_cli()
        self.assertEqual(code, 1, out)
        self.assertIn("server/src/handler.rs:2", out)

    def test_discarded_let_underscore_unqualified_is_flagged(self):
        self.write(
            "server/src/handler.rs",
            "use crate::fsm::x_step;\nfn f(s: S, e: E) {\n    let _ = x_step(s, e);\n}\n",
        )
        code, out = self.run_cli()
        self.assertEqual(code, 1, out)
        self.assertIn("server/src/handler.rs:3", out)

    def test_discarded_bare_statement_qualified_is_flagged(self):
        self.write(
            "server/src/ops/handler.rs",
            "fn f(s: S, e: E) {\n    crate::fsm::x_step(s, e);\n}\n",
        )
        code, out = self.run_cli()
        self.assertEqual(code, 1, out)
        self.assertIn("server/src/ops/handler.rs:2", out)

    def test_discarded_bare_statement_unqualified_is_flagged(self):
        self.write(
            "server/src/handler.rs",
            "fn f(s: S, e: E) {\n    x_step(s, e);\n}\n",
        )
        code, out = self.run_cli()
        self.assertEqual(code, 1, out)
        self.assertIn("server/src/handler.rs:2", out)

    def test_discarded_dot_ok_is_flagged(self):
        self.write(
            "server/src/handler.rs",
            (
                "fn f(s: S, e: E) {\n"
                "    crate::fsm::x_step(s, e).ok();\n"
                "    x_step(s, e).map_err(|_| ()).ok();\n"
                "    _ = crate::fsm::x_step(s, e);\n"
                "    #[allow(unused)]\n"
                "    x_step(s, e);\n"
                "}\n"
            ),
        )
        code, out = self.run_cli()
        self.assertEqual(code, 1, out)
        self.assertIn("server/src/handler.rs:2", out)
        self.assertIn("server/src/handler.rs:3", out)
        self.assertIn("server/src/handler.rs:4", out)
        self.assertIn("server/src/handler.rs:6", out)

    def test_multiline_discarded_calls_are_flagged(self):
        self.write(
            "server/src/handler.rs",
            (
                "fn f(s: S, e: E) {\n"
                "    let _ = crate::fsm::x_step(\n"
                "        s,\n"
                "        e,\n"
                "    );\n"
                "    x_step(\n"
                "        s,\n"
                "        e,\n"
                "    )\n"
                "    .ok();\n"
                "}\n"
            ),
        )
        code, out = self.run_cli()
        self.assertEqual(code, 1, out)
        self.assertIn("server/src/handler.rs:2", out)
        self.assertIn("server/src/handler.rs:6", out)

    def test_matched_assigned_try_and_closure_forms_pass(self):
        self.write(
            "server/src/handler.rs",
            (
                "fn f(mut s: S, e: E) -> Result<S, &'static str> {\n"
                "    match crate::fsm::x_step(s, e) {\n"
                "        Ok(next) => s = next,\n"
                "        Err(r) => return Err(r.code),\n"
                "    }\n"
                "    if let Ok(next) = x_step(s, e) {\n"
                "        s = next;\n"
                "    }\n"
                "    if crate::fsm::x_step(s, e).is_ok() {\n"
                "        let next = crate::fsm::x_step(s, e).map_err(|r| r.code)?;\n"
                "        s = next;\n"
                "    }\n"
                "    s = crate::fsm::other_step(\n"
                "        s,\n"
                "        e,\n"
                "    )\n"
                "    .unwrap_or(s);\n"
                "    crate::fsm::x_step(s, e).map_err(|r| r.code)?;\n"
                "    x_step(s, e).map_err(|r| r.code)?;\n"
                "    let step = |st: S, ev: E| x_step(st, ev).unwrap_or(st);\n"
                "    s = step(s, e);\n"
                "    Ok(s)\n"
                "}\n"
                "fn tail_return(s: S, e: E) -> Result<S, Refusal> {\n"
                "    crate::fsm::x_step(s, e)\n"
                "}\n"
            ),
        )
        code, out = self.run_cli()
        self.assertEqual(code, 0, out)

    def test_fsm_rs_and_tests_are_excluded(self):
        self.write(
            "server/src/fsm.rs",
            FSM_RS + "\nfn internal() {\n    let _ = x_step(S::A, E::Go);\n}\n",
        )
        self.write(
            "server/src/handler.rs",
            (
                "#[cfg(test)]\n"
                "mod tests {\n"
                "    fn helper() {\n"
                "        let _ = crate::fsm::x_step(S::A, E::Go);\n"
                "        x_step(S::A, E::Go).ok();\n"
                "    }\n"
                "}\n"
                "#[test]\n"
                "fn standalone() {\n"
                "    x_step(S::A, E::Go);\n"
                "}\n"
                "#[tokio::test(flavor = \"multi_thread\")]\n"
                "async fn async_test() {\n"
                "    let _ = x_step(S::A, E::Go);\n"
                "}\n"
            ),
        )
        self.write(
            "server/tests/integration_test.rs",
            "#[test]\nfn it() {\n    let _ = crate::fsm::x_step(S::A, E::Go);\n}\n",
        )
        code, out = self.run_cli()
        self.assertEqual(code, 0, out)

    def test_production_code_after_cfg_test_is_still_checked(self):
        self.write(
            "server/src/handler.rs",
            (
                "#[cfg(test)]\n"
                "mod tests {\n"
                "    fn ok_in_test() { let _ = x_step(S::A, E::Go); }\n"
                "}\n"
                "fn prod_after_test(s: S, e: E) {\n"
                "    let _ = crate::fsm::x_step(s, e);\n"
                "}\n"
            ),
        )
        code, out = self.run_cli()
        self.assertEqual(code, 1, out)
        self.assertIn("server/src/handler.rs:6", out)

    def test_comments_strings_lifetimes_and_non_fsm_step_do_not_false_positive(self):
        self.write(
            "server/src/handler.rs",
            (
                "fn non_fsm_step() {}\n"
                "struct Obj;\n"
                "impl Obj { fn x_step(&self) {} }\n"
                "fn f<'a>(_s: &'a str, obj: &Obj) {\n"
                "    // let _ = crate::fsm::x_step(s, e);\n"
                "    /* x_step(s, e); /* nested let _ = x_step(s, e); */ */\n"
                "    let _a = \"let _ = crate::fsm::x_step(s, e);\";\n"
                "    let _b = r#\"x_step(s, e).ok();\"#;\n"
                "    let _c = 'x';\n"
                "    'drain: loop {\n"
                "        non_fsm_step();\n"
                "        obj.x_step();\n"
                "        break 'drain;\n"
                "    }\n"
                "}\n"
            ),
        )
        code, out = self.run_cli()
        self.assertEqual(code, 0, out)

    def test_repository_passes(self):
        code, out = self.run_cli_repo()
        self.assertEqual(code, 0, out)

    def run_cli_repo(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = cfd.main([])
        return code, buf.getvalue()


if __name__ == "__main__":
    unittest.main()
