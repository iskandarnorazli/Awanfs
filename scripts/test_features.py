"""Self-tests for features.py.

    python3 scripts/test_features.py
"""
import contextlib
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import features  # noqa: E402


MINIMAL_DRIVE_SH = """#!/usr/bin/env bash
doctor() { :; }
j_cors() { :; }
j_objects() { :; }
"""

MINIMAL_FUZZ_CARGO = """[package]
name = "awanfs-fuzz"

[[bin]]
name = "cors_xml"
path = "fuzz_targets/cors_xml.rs"
"""

MINIMAL_SUITE_PY = """
import pytest

def test_set_cors():
    pass

def test_cors_header_option():
    pass

@pytest.mark.auth_aws2
def test_bad_aws2():
    pass
"""

MINIMAL_JUNIT_XML = """<testsuite>
<testcase classname="s3tests.functional.test_s3" name="test_set_cors"/>
<testcase classname="s3tests.functional.test_s3" name="test_cors_header_option"><failure message="assert 403 == 200"/></testcase>
<testcase classname="s3tests.functional.test_s3" name="test_bad_aws2"><failure message="AuthorizationHeaderMalformed"/></testcase>
</testsuite>"""

OPS_RS = """
pub enum Operation {
    // A comment is not a variant.
    PutBucketCors,
    GetBucketCors,
    DeleteBucketCors,
}
"""

CORS_TEST_RS = """
// covers: PutBucketCors, GetBucketCors
#[tokio::test]
async fn test_set_cors() {}

#[tokio::test]
// covers: DeleteBucketCors
async fn test_delete_cors() {}
"""

VALID_TOML = """
[[feature]]
id = "cors"
name = "Bucket CORS"
status = "done"
issue = "#116"
operations = ["PutBucketCors", "GetBucketCors", "DeleteBucketCors"]
ceph = ["test_s3::test_set_cors", "test_s3::test_cors_header_option"]
server_tests = ["server/tests/cors_test.rs::test_set_cors", "server/tests/cors_test.rs::test_delete_cors"]
drive = ["cors"]
fuzz = ["cors_xml"]

[[feature]]
id = "sigv2"
name = "SigV2 request signing"
status = "out-of-scope"
issue = "out of scope"
decision = "https://github.com/iskandarnorazli/Awanfs/pull/232"
ceph = ["mark:auth_aws2"]

[[feature]]
id = "health"
name = "Health endpoints"
status = "done"
issue = "#44"
user_visible = false
gaps = { fuzz = "not needed: parses no request input" }
server_tests = ["server/tests/health_test.rs::test_live"]
"""


def cors_waits_on(other):
    """VALID_TOML with cors's failing test listed as waiting on `other`."""
    return VALID_TOML.replace(
        'fuzz = ["cors_xml"]\n',
        'fuzz = ["cors_xml"]\n\n[feature.waits_on]\n"test_s3::test_cors_header_option" = "%s"\n' % other,
    )


MAIN_RS = """
fn main() {
    while let Some(arg) = args.next() {
        if arg == "--port" || arg == "-p" {
        } else if arg == "--quiet" {
        }
    }
    let _ = std::env::var("AWANFS_PORT");
}
"""

SETTINGS_TOML = """
[[setting]]
id = "port"
flags = ["--port", "-p"]
env = ["AWANFS_PORT"]
default = "9000"
feature = "server"
what = "listen port"
test = ["server/tests/health_test.rs::test_live"]

[[setting]]
id = "quiet"
flags = ["--quiet"]
env = []
default = "off"
feature = "health"
what = "no startup lines"
untested = "fixture"
internal = "fixture"
"""


class SettingsTest(unittest.TestCase):
    """#494: every flag in main.rs and every environment variable in server/src has a
    [[setting]] entry, with a known feature, a doc and a test."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        (self.root / "server" / "src").mkdir(parents=True)
        (self.root / "server" / "tests").mkdir(parents=True)
        (self.root / "server" / "src" / "main.rs").write_text(MAIN_RS, encoding="utf-8")
        (self.root / "server" / "tests" / "health_test.rs").write_text("#[test]\nfn test_live() {}\n", encoding="utf-8")
        (self.root / "README.md").write_text("Run with `--port 9000` (or `AWANFS_PORT`).\n", encoding="utf-8")
        self.feats = [{"id": "health"}]

    def tearDown(self):
        self._tmp.cleanup()

    def _probs(self, text):
        import tomllib
        return features.check_settings(self.root, tomllib.loads(text).get("setting", []), self.feats)

    def test_a_complete_registry_passes(self):
        self.assertEqual(self._probs(SETTINGS_TOML), [])

    def test_a_flag_with_no_entry_fails(self):
        (self.root / "server" / "src" / "main.rs").write_text(MAIN_RS.replace('"--quiet"', '"--quiet" || arg == "--loud"'), encoding="utf-8")
        self.assertTrue(any("--loud" in p and "no [[setting]]" in p for p in self._probs(SETTINGS_TOML)), self._probs(SETTINGS_TOML))

    def test_an_entry_the_server_no_longer_reads_fails(self):
        text = SETTINGS_TOML.replace('flags = ["--quiet"]', 'flags = ["--quiet", "--gone"]')
        self.assertTrue(any("--gone" in p and "not read" in p for p in self._probs(text)), self._probs(text))

    def test_a_variable_with_no_entry_fails(self):
        (self.root / "server" / "src" / "x.rs").write_text('fn f() { std::env::var_os("AWAN_NEW_KNOB"); }\n', encoding="utf-8")
        self.assertTrue(any("AWAN_NEW_KNOB" in p and "no [[setting]]" in p for p in self._probs(SETTINGS_TOML)))

    def test_an_unknown_feature_fails(self):
        text = SETTINGS_TOML.replace('feature = "health"', 'feature = "nope"')
        self.assertTrue(any("quiet" in p and "nope" in p for p in self._probs(text)), self._probs(text))

    def test_no_readme_mention_and_no_internal_reason_fails(self):
        text = SETTINGS_TOML.replace('internal = "fixture"\n', "")
        self.assertTrue(any("quiet" in p and "README" in p for p in self._probs(text)), self._probs(text))

    def test_a_missing_test_fails(self):
        text = SETTINGS_TOML.replace("health_test.rs::test_live", "health_test.rs::test_gone")
        self.assertTrue(any("port" in p and "test_gone" in p for p in self._probs(text)), self._probs(text))
        text = SETTINGS_TOML.replace('untested = "fixture"\n', "")
        self.assertTrue(any("quiet" in p and "untested" in p for p in self._probs(text)), self._probs(text))

    def test_the_report_has_a_settings_table(self):
        import tomllib
        lines = features.settings_md_lines(tomllib.loads(SETTINGS_TOML)["setting"])
        self.assertIn("## Settings", lines)
        self.assertIn("| port | `--port`, `-p` | `AWANFS_PORT` | 9000 | server | - | README.md | `server/tests/health_test.rs::test_live` |", lines)
        self.assertIn("| quiet | `--quiet` | - | off | health | - | internal: fixture | untested: fixture |", lines)

    def test_the_real_registry_passes(self):
        import tomllib
        with open(features.DEFAULT_ROOT / "features.toml", "rb") as fh:
            data = tomllib.load(fh)
        self.assertEqual(features.check_settings(features.DEFAULT_ROOT, data.get("setting", []), data["feature"]), [])


class FeaturesTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        (self.root / "scripts").mkdir(parents=True)
        (self.root / "server" / "tests").mkdir(parents=True)
        (self.root / "server" / "fuzz" / "fuzz_targets").mkdir(parents=True)
        (self.root / "tests" / "s3tests").mkdir(parents=True)

        (self.root / "scripts" / "verify_drive.sh").write_text(MINIMAL_DRIVE_SH, encoding="utf-8")
        (self.root / "server" / "tests" / "cors_test.rs").write_text(CORS_TEST_RS, encoding="utf-8")
        (self.root / "server" / "tests" / "health_test.rs").write_text("#[test]\nfn test_live() {}\n", encoding="utf-8")
        (self.root / "server" / "src" / "ops").mkdir(parents=True)
        (self.root / "server" / "src" / "ops" / "mod.rs").write_text(OPS_RS, encoding="utf-8")
        (self.root / "server" / "fuzz" / "Cargo.toml").write_text(MINIMAL_FUZZ_CARGO, encoding="utf-8")
        (self.root / "server" / "fuzz" / "fuzz_targets" / "cors_xml.rs").write_text("", encoding="utf-8")
        (self.root / "tests" / "s3tests" / "passing.txt").write_text(
            "test_s3::test_set_cors\ntest_s3::test_cors_header_option\n", encoding="utf-8"
        )

        self.suite = self.root / "test_s3.py"
        self.suite.write_text(MINIMAL_SUITE_PY, encoding="utf-8")
        self.junit = self.root / "junit.xml"
        self.junit.write_text(MINIMAL_JUNIT_XML, encoding="utf-8")
        self.registry = self.root / "features.toml"
        self.registry.write_text(VALID_TOML, encoding="utf-8")

    def tearDown(self):
        self._tmp.cleanup()

    def _run_check(self, with_suite=True):
        argv = ["check", "--root", str(self.root), "--registry", str(self.registry)]
        if with_suite:
            argv += ["--ceph-suite", str(self.suite)]
        with contextlib.redirect_stdout(io.StringIO()):
            return features.main(argv)

    def test_valid_registry_passes(self):
        self.assertEqual(self._run_check(), 0)

    # A feature is done only when its ceph tests pass, or each one that does not names the
    # unfinished feature it waits on (owner, 2026-11-21).
    def _passing(self, *names):
        (self.root / "tests" / "s3tests" / "passing.txt").write_text("".join(n + "\n" for n in names), encoding="utf-8")

    def _probs(self):
        return features.check_registry(root=self.root, registry_path=self.registry, ceph_suites=[self.suite])

    def test_a_done_feature_with_a_failing_ceph_test_fails(self):
        self._passing("test_s3::test_set_cors")
        probs = self._probs()
        self.assertTrue(any("cors" in p and "test_s3::test_cors_header_option" in p and "done" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_a_failing_test_listed_under_waits_on_an_unfinished_feature_passes(self):
        self._passing("test_s3::test_set_cors")
        self.registry.write_text(cors_waits_on("sigv2"), encoding="utf-8")
        self.assertEqual(self._probs(), [])

    def test_waits_on_an_unknown_or_done_feature_fails(self):
        self._passing("test_s3::test_set_cors")
        for other, word in (("nosuch", "unknown"), ("health", "done")):
            self.registry.write_text(cors_waits_on(other), encoding="utf-8")
            probs = self._probs()
            self.assertTrue(any("waits_on" in p and other in p and word in p for p in probs), (other, probs))

    def test_a_waits_on_entry_for_a_passing_test_is_stale_and_fails(self):
        self.registry.write_text(cors_waits_on("sigv2"), encoding="utf-8")
        probs = self._probs()
        self.assertTrue(any("waits_on" in p and "test_cors_header_option" in p and "passes" in p for p in probs), probs)

    def test_a_waits_on_entry_for_another_features_test_fails(self):
        self._passing("test_s3::test_set_cors")
        self.registry.write_text(
            VALID_TOML.replace('server_tests = ["server/tests/health_test.rs::test_live"]',
                               'server_tests = ["server/tests/health_test.rs::test_live"]\n\n[feature.waits_on]\n"test_s3::test_cors_header_option" = "sigv2"'),
            encoding="utf-8",
        )
        probs = self._probs()
        self.assertTrue(any("health" in p and "waits_on" in p and "not one of its tests" in p for p in probs), probs)

    def test_test_in_no_feature_fails(self):
        self.suite.write_text(
            MINIMAL_SUITE_PY + "\ndef test_unassigned_one():\n    pass\n",
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("test_unassigned_one" in p and "unassigned" in p.lower() for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_test_in_two_features_fails(self):
        self.registry.write_text(
            VALID_TOML + '\n[[feature]]\nid = "dup"\nname = "Dup"\nstatus = "partial"\nceph = ["test_s3::test_set_cors"]\n',
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("test_set_cors" in p and "cors" in p and "dup" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_missing_named_test_fails(self):
        self.registry.write_text(
            VALID_TOML.replace("test_cors_header_option", "test_does_not_exist"),
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("test_does_not_exist" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_missing_server_test_file_fails(self):
        self.registry.write_text(
            VALID_TOML.replace("server/tests/cors_test.rs", "server/tests/missing_test.rs"),
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("missing_test.rs" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_missing_drive_journey_fails(self):
        self.registry.write_text(
            VALID_TOML.replace('drive = ["cors"]', 'drive = ["nonexistent_journey"]'),
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("nonexistent_journey" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_missing_fuzz_target_fails(self):
        self.registry.write_text(
            VALID_TOML.replace('fuzz = ["cors_xml"]', 'fuzz = ["no_such_fuzz"]'),
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("no_such_fuzz" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_done_feature_without_server_test_fails(self):
        self.registry.write_text(
            VALID_TOML.replace('server_tests = ["server/tests/cors_test.rs::test_set_cors", "server/tests/cors_test.rs::test_delete_cors"]', "server_tests = []"),
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("cors" in p and "server_tests" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_user_visible_done_feature_without_drive_journey_fails(self):
        self.registry.write_text(
            VALID_TOML.replace('drive = ["cors"]', "drive = []"),
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("cors" in p and "drive" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_out_of_scope_without_decision_link_fails(self):
        self.registry.write_text(
            VALID_TOML.replace('decision = "https://github.com/iskandarnorazli/Awanfs/pull/232"\n', ""),
            encoding="utf-8",
        )
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("sigv2" in p and "decision" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

        # Non-URL decision string is also rejected
        self.registry.write_text(
            VALID_TOML.replace(
                "https://github.com/iskandarnorazli/Awanfs/pull/232",
                "owner said so in chat",
            ),
            encoding="utf-8",
        )
        self.assertEqual(self._run_check(), 1)

    def test_moved_target_must_exist_and_have_decision(self):
        bad_moved = VALID_TOML + '\nmoved = [{ test = "test_s3::test_x", to = "missing-feat", decision = "not-a-url" }]\n'
        # Attach moved to the last feature (health)
        self.registry.write_text(bad_moved, encoding="utf-8")
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
        )
        self.assertTrue(any("missing-feat" in p for p in probs), probs)
        self.assertTrue(any("decision" in p for p in probs), probs)

    def test_mint_suite_unassigned_and_overlap_checked(self):
        mint_log = self.root / "mint.json"
        mint_log.write_text(
            '{"name": "aws-sdk-go", "function": "PutObject", "status": "PASS"}\n'
            '{"name": "aws-sdk-go", "function": "MissingTest", "status": "FAIL"}\n',
            encoding="utf-8",
        )
        reg = VALID_TOML.replace(
            'id = "cors"',
            'id = "cors"\nmint = ["aws-sdk-go::PutObject"]',
        )
        self.registry.write_text(reg, encoding="utf-8")
        probs = features.check_registry(
            root=self.root,
            registry_path=self.registry,
            ceph_suites=[self.suite],
            mint_logs=[mint_log],
        )
        self.assertTrue(any("aws-sdk-go::MissingTest" in p for p in probs), probs)

    def test_mint_log_skips_banner_lines_and_fails_on_malformed_or_empty(self):
        good = self.root / "mint_banner.json"
        good.write_text(
            '{"name": "mc", "function": "test_make_bucket", "status": "PASS"}\n'
            'Dependency validation complete\n'
            '{"name": "mc", "function": "test_rb", "status": "PASS"}\n',
            encoding="utf-8",
        )
        parsed = features.parse_mint_log([good])
        self.assertEqual(
            parsed,
            {"mc::test_make_bucket": ("pass", ""), "mc::test_rb": ("pass", "")},
        )

        malformed = self.root / "mint_bad.json"
        malformed.write_text('{"name": "mc", "function": "test_rb"\n', encoding="utf-8")
        with self.assertRaises(ValueError):
            features.parse_mint_log([malformed])
        probs = features.check_registry(
            root=self.root, registry_path=self.registry, ceph_suites=[self.suite], mint_logs=[malformed]
        )
        self.assertTrue(probs)

        for name, text in (("mint_empty.json", "\n"), ("mint_only_banner.json", "Dependency validation complete\n")):
            empty = self.root / name
            empty.write_text(text, encoding="utf-8")
            with self.assertRaises(ValueError):
                features.parse_mint_log([empty])
            probs = features.check_registry(
                root=self.root, registry_path=self.registry, ceph_suites=[self.suite], mint_logs=[empty]
            )
            self.assertTrue(probs, name)

    def test_mint_log_normalizes_awscli_and_strips_newlines(self):
        mint_log = self.root / "mint_awscli.json"
        mint_log.write_text(
            '{"name": "awscli", "function": "make_bucket\\n", "status": "PASS"}\n'
            '{"name": "awscli", "function": "aws --endpoint-url http://127.0.0.1:9000 s3api create-multipart-upload --bucket awscli-mint-test-bucket-2170 --key awscli-mint-test-bucket-2170-object\\r\\n", "status": "PASS"}\n',
            encoding="utf-8",
        )
        parsed = features.parse_mint_log([mint_log])
        self.assertEqual(
            sorted(parsed),
            [
                "awscli::aws --endpoint-url http://127.0.0.1:9000 s3api create-multipart-upload --bucket awscli-mint-test-bucket --key awscli-mint-test-bucket-object",
                "awscli::make_bucket",
            ],
        )

    def test_parse_passing_file_preserves_brackets_in_mint_keys(self):
        ceph_p = self.root / "ceph_passing.txt"
        ceph_p.write_text(
            "test_copy_enc[sse-c]\n"
            "test_s3::test_other[1]\n"
            "test_headers::test_bad_header[x]\n",
            encoding="utf-8",
        )
        self.assertEqual(
            features.parse_passing_file(ceph_p, default_prefix="test_s3::"),
            {"test_s3::test_copy_enc", "test_s3::test_other", "test_headers::test_bad_header"},
        )

        mint_p = self.root / "mint_passing.txt"
        mint_p.write_text(
            "aws-sdk-php::getBucketLocation ( array $params = [] )\n"
            "minio-js::setObjectLegalHold(bucketName, objectName, setOpts={})\n",
            encoding="utf-8",
        )
        self.assertEqual(
            features.parse_passing_file(mint_p),
            {
                "aws-sdk-php::getBucketLocation ( array $params = [] )",
                "minio-js::setObjectLegalHold(bucketName, objectName, setOpts={})",
            },
        )

    def test_mint_log_empty_function_and_repeated_fail_wins(self):
        mint_log = self.root / "mint_rep.json"
        mint_log.write_text(
            '{"name": "minio-py:test_put_object", "function": "", "status": "PASS"}\n'
            '{"name": "minio-java", "function": "putObject()", "status": "PASS"}\n'
            '{"name": "minio-java", "function": "putObject()", "status": "FAIL", "error": "AuthorizationHeaderMalformed"}\n'
            '{"name": "minio-java", "function": "putObject()", "status": "NA"}\n'
            '{"name": "minio-java", "function": "putObject()", "status": "PASS"}\n',
            encoding="utf-8",
        )
        parsed = features.parse_mint_log([mint_log])
        self.assertEqual(parsed["minio-py:test_put_object"], ("pass", ""))
        self.assertEqual(parsed["minio-java::putObject()"], ("fail", "AuthorizationHeaderMalformed"))

    def test_report_writes_features_md_with_status_counts_and_gaps(self):
        mint_log = self.root / "mint.json"
        mint_log.write_text(
            '{"name": "awscli", "function": "cors_test", "status": "PASS"}\n',
            encoding="utf-8",
        )
        reg = VALID_TOML.replace(
            'id = "cors"',
            'id = "cors"\nmint = ["awscli::cors_test"]',
        )
        self.registry.write_text(reg, encoding="utf-8")
        out_md = self.root / "FEATURES.md"
        rc = features.main([
            "report",
            "--root", str(self.root),
            "--registry", str(self.registry),
            "--ceph-junit", str(self.junit),
            "--mint-log", str(mint_log),
            "--run-id", "99887766",
            "--output", str(out_md),
        ])
        self.assertEqual(rc, 0)
        text = out_md.read_text(encoding="utf-8")
        self.assertIn("99887766", text)
        self.assertIn("| cors | done | #116 | 1/2 | 1/1 |", text)
        self.assertIn("| sigv2 | out-of-scope |", text)
        self.assertIn("## Gaps", text)
        # cors is done and has a failing test (test_cors_header_option)
        self.assertIn("Done features with failing or non-passing listed tests", text)
        self.assertIn("test_s3::test_cors_header_option", text)
        # health is done and has no ceph or fuzz evidence
        self.assertIn("Done features missing evidence in some layer", text)
        self.assertIn("**health**: no ceph, mint, fuzz", text)

    def test_offline_check_catches_unassigned_passing_test(self):
        # Simulate deleting cors's ceph list when only passing.txt is present (no --ceph-suite).
        self.registry.write_text(
            VALID_TOML.replace(
                'ceph = ["test_s3::test_set_cors", "test_s3::test_cors_header_option"]',
                "ceph = []",
            ),
            encoding="utf-8",
        )
        probs = features.check_registry(root=self.root, registry_path=self.registry)
        self.assertTrue(any("test_set_cors" in p and "unassigned" in p.lower() for p in probs), probs)
        self.assertEqual(self._run_check(with_suite=False), 1)

    # A suite test AwanFS cannot run as written (SigV2) is covered by its own port (#433).
    def _ported(self, target, key="test_s3::test_bad_aws2"):
        self.registry.write_text(
            VALID_TOML.replace(
                'ceph = ["mark:auth_aws2"]\n',
                'ceph = ["mark:auth_aws2"]\n\n[feature.ported]\n"%s" = "%s"\n' % (key, target),
            ),
            encoding="utf-8",
        )
        return features.check_registry(root=self.root, registry_path=self.registry, ceph_suites=[self.suite])

    def test_a_port_needs_its_file_and_fn(self):
        probs = self._ported("server/tests/ports_test.rs::test_bad_aws2")
        self.assertTrue(any("ports_test.rs" in p and "no such file" in p for p in probs), probs)
        (self.root / "server" / "tests" / "ports_test.rs").write_text("fn other() {}\n", encoding="utf-8")
        probs = self._ported("server/tests/ports_test.rs::test_bad_aws2")
        self.assertTrue(any("test_bad_aws2" in p and "no such fn" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)
        (self.root / "server" / "tests" / "ports_test.rs").write_text("#[tokio::test]\nasync fn test_bad_aws2() {}\n", encoding="utf-8")
        self.assertEqual(self._ported("server/tests/ports_test.rs::test_bad_aws2"), [])
        self.assertEqual(self._run_check(), 0)

    def test_a_port_to_a_suite_test_needs_it_in_passing_txt(self):
        self.assertEqual(self._ported("test_s3::test_set_cors"), [])
        probs = self._ported("test_s3::test_not_listed")
        self.assertTrue(any("test_not_listed" in p and "passing.txt" in p for p in probs), probs)

    def test_a_port_must_be_for_one_of_the_features_tests(self):
        (self.root / "server" / "tests" / "ports_test.rs").write_text("fn test_set_cors() {}\n", encoding="utf-8")
        probs = self._ported("server/tests/ports_test.rs::test_set_cors", key="test_s3::test_set_cors")
        self.assertTrue(any("sigv2" in p and "test_set_cors" in p and "not one of its tests" in p for p in probs), probs)

    def test_ported_md_and_the_registry_agree(self):
        (self.root / "server" / "tests" / "ports_test.rs").write_text("fn test_bad_aws2() {}\n", encoding="utf-8")
        md = self.root / "tests" / "s3tests" / "PORTED.md"
        md.write_text(
            "| suite test | why ported | AwanFS test |\n|---|---|---|\n"
            "| `test_s3::test_bad_aws2` | SigV2 | `server/tests/ports_test.rs::test_bad_aws2` |\n",
            encoding="utf-8",
        )
        self.assertEqual(self._ported("server/tests/ports_test.rs::test_bad_aws2"), [])
        md.write_text("| suite test | why ported | AwanFS test |\n|---|---|---|\n", encoding="utf-8")
        probs = self._ported("server/tests/ports_test.rs::test_bad_aws2")
        self.assertTrue(any("PORTED.md" in p and "test_bad_aws2" in p for p in probs), probs)
        md.write_text(
            "| suite test | why ported | AwanFS test |\n|---|---|---|\n"
            "| `test_s3::test_bad_aws2` | SigV2 | `server/tests/ports_test.rs::other` |\n",
            encoding="utf-8",
        )
        probs = self._ported("server/tests/ports_test.rs::test_bad_aws2")
        self.assertTrue(any("PORTED.md" in p and "other" in p for p in probs), probs)

    def test_a_ported_test_counts_for_a_done_feature(self):
        (self.root / "server" / "tests" / "ports_test.rs").write_text("fn test_cors_header_option() {}\n", encoding="utf-8")
        self._passing("test_s3::test_set_cors")
        self.registry.write_text(
            VALID_TOML.replace(
                'fuzz = ["cors_xml"]\n',
                'fuzz = ["cors_xml"]\n\n[feature.ported]\n"test_s3::test_cors_header_option" = "server/tests/ports_test.rs::test_cors_header_option"\n',
            ),
            encoding="utf-8",
        )
        self.assertEqual(self._run_check(), 0)

    def test_report_shows_a_ported_test_as_covered_by_its_own_test(self):
        (self.root / "server" / "tests" / "ports_test.rs").write_text("fn test_bad_aws2() {}\n", encoding="utf-8")
        self._ported("server/tests/ports_test.rs::test_bad_aws2")
        out_md = self.root / "FEATURES.md"
        rc = features.main([
            "report", "--root", str(self.root), "--registry", str(self.registry),
            "--ceph-junit", str(self.junit), "--ceph-suite", str(self.suite), "--run-id", "1", "--output", str(out_md),
        ])
        self.assertEqual(rc, 0)
        text = out_md.read_text(encoding="utf-8")
        self.assertIn("| sigv2 | out-of-scope | covered by own tests | 0/1 (+1 own) |", text)
        self.assertIn("- `test_s3::test_bad_aws2`: covered by own test (`server/tests/ports_test.rs::test_bad_aws2`)", text)

    def test_report_without_a_mint_log_does_not_claim_mint_results(self):
        self.registry.write_text(VALID_TOML.replace('id = "cors"', 'id = "cors"\nmint = ["awscli::cors_test"]'), encoding="utf-8")
        out_md = self.root / "FEATURES.md"
        features.main(["report", "--root", str(self.root), "--registry", str(self.registry),
                       "--ceph-junit", str(self.junit), "--run-id", "1", "--output", str(out_md)])
        text = out_md.read_text(encoding="utf-8")
        self.assertNotIn("Mint: 0 tests", text)
        self.assertIn("| cors | done | #116 | 1/2 | — |", text)
        self.assertIn("Mint: not run", text)

    # Coverage guards (#434): every operation has one home and a test that sends it; every
    # server test named is a real fn; a done feature has every layer or a listed gap.
    def _with(self, toml=None, cors_rs=None):
        if toml is not None:
            self.registry.write_text(toml, encoding="utf-8")
        if cors_rs is not None:
            (self.root / "server" / "tests" / "cors_test.rs").write_text(cors_rs, encoding="utf-8")
        return self._probs()

    def test_every_operation_has_exactly_one_home(self):
        probs = self._with(VALID_TOML.replace('"GetBucketCors", "DeleteBucketCors"]', '"GetBucketCors"]'))
        self.assertTrue(any("DeleteBucketCors" in p and "no feature" in p for p in probs), probs)
        probs = self._with(VALID_TOML.replace('user_visible = false\n', 'user_visible = false\noperations = ["DeleteBucketCors"]\n'))
        self.assertTrue(any("DeleteBucketCors" in p and "cors" in p and "health" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_a_server_test_names_a_fn_that_exists(self):
        probs = self._with(VALID_TOML.replace("server/tests/health_test.rs::test_live", "server/tests/health_test.rs"))
        self.assertTrue(any("health" in p and "health_test.rs" in p and "names a file" in p for p in probs), probs)
        probs = self._with(VALID_TOML.replace("health_test.rs::test_live", "health_test.rs::test_gone"))
        self.assertTrue(any("health" in p and "test_gone" in p and "no such fn" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)

    def test_every_operation_is_covered_or_a_listed_gap(self):
        no_delete = CORS_TEST_RS.replace("// covers: DeleteBucketCors\n", "")
        probs = self._with(cors_rs=no_delete)
        self.assertTrue(any("DeleteBucketCors" in p and "no test covers" in p for p in probs), probs)
        self.assertEqual(self._run_check(), 1)
        gap = VALID_TOML.replace('fuzz = ["cors_xml"]\n', 'fuzz = ["cors_xml"]\ngaps = { DeleteBucketCors = "#9" }\n')
        self.assertEqual(self._with(gap, no_delete), [])
        # The gap is stale once a test covers it.
        probs = self._with(gap, CORS_TEST_RS)
        self.assertTrue(any("DeleteBucketCors" in p and "gap" in p and "covered" in p for p in probs), probs)

    def test_a_covers_line_names_a_real_operation_on_a_named_test(self):
        probs = self._with(cors_rs=CORS_TEST_RS + "\n// covers: PutBucketCorz\n#[test]\nfn test_x() {}\n")
        self.assertTrue(any("PutBucketCorz" in p and "not an operation" in p for p in probs), probs)
        unnamed = CORS_TEST_RS.replace("async fn test_delete_cors", "async fn test_other_name")
        probs = self._with(cors_rs=unnamed)
        self.assertTrue(any("DeleteBucketCors" in p and "no test covers" in p for p in probs), probs)

    def test_a_done_feature_has_every_layer_or_a_listed_gap(self):
        probs = self._with(VALID_TOML.replace('fuzz = ["cors_xml"]\n', ""))
        self.assertTrue(any("cors" in p and "fuzz" in p and "gap" in p for p in probs), probs)
        self.assertEqual(self._with(VALID_TOML.replace('fuzz = ["cors_xml"]\n', 'gaps = { fuzz = "#9" }\n')), [])
        probs = self._with(VALID_TOML.replace('fuzz = ["cors_xml"]\n', 'gaps = { fuzz = "later" }\n'))
        self.assertTrue(any("cors" in p and "later" in p for p in probs), probs)
        probs = self._with(VALID_TOML.replace('gaps = { fuzz = "not needed: parses no request input" }\n', ""))
        self.assertTrue(any("health" in p and "fuzz" in p for p in probs), probs)
        # A gap for a layer the feature has is stale.
        probs = self._with(VALID_TOML.replace('fuzz = ["cors_xml"]\n', 'fuzz = ["cors_xml"]\ngaps = { fuzz = "#9" }\n'))
        self.assertTrue(any("cors" in p and "fuzz" in p and "stale" in p for p in probs), probs)

    def test_report_counts_coverage_gaps_per_feature(self):
        reg = VALID_TOML.replace('fuzz = ["cors_xml"]\n', 'gaps = { fuzz = "#9" }\n')
        self.registry.write_text(reg, encoding="utf-8")
        out_md = self.root / "FEATURES.md"
        features.main(["report", "--root", str(self.root), "--registry", str(self.registry),
                       "--ceph-junit", str(self.junit), "--run-id", "1", "--output", str(out_md)])
        text = out_md.read_text(encoding="utf-8")
        self.assertIn("## Coverage gaps", text)
        self.assertIn("| cors | 1 | fuzz: #9 |", text)
        self.assertIn("| health | 0 | fuzz: not needed: parses no request input |", text)

    def test_repo_registry_passes_check(self):
        repo_root = Path(__file__).resolve().parent.parent
        probs = features.check_registry(root=repo_root)
        self.assertEqual(probs, [])


if __name__ == "__main__":
    unittest.main()
