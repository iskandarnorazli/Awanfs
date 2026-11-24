"""Tests for bisect_ratio.py: commit parsing, rotating order, stats, and step verdict."""
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bisect_ratio as br  # noqa: E402
import ratio_check as rc  # noqa: E402


def make_summary(commit, median, minimum, maximum, sha=None, subject="msg", awan_med=350.0, minio_med=200.0):
    return br.BuildSummary(
        commit=commit,
        sha=sha or commit[:7],
        subject=subject,
        ratios=(minimum, median, maximum),
        median=median,
        minimum=minimum,
        maximum=maximum,
        awanfs_median_mib=awan_med,
        minio_median_mib=minio_med,
    )


class CommitParsingTests(unittest.TestCase):
    def test_parses_space_separated_default_commits(self):
        commits = br.parse_commits("ffed2a9 e8290ef d73bbd2^ d73bbd2 528524a 2abdb39")
        self.assertEqual(
            commits,
            ["ffed2a9", "e8290ef", "d73bbd2^", "d73bbd2", "528524a", "2abdb39"],
        )

    def test_parses_commas_newlines_and_sequences(self):
        self.assertEqual(
            br.parse_commits("ffed2a9, e8290ef\n  d73bbd2^\t528524a "),
            ["ffed2a9", "e8290ef", "d73bbd2^", "528524a"],
        )
        self.assertEqual(
            br.parse_commits(["ffed2a9 e8290ef", "d73bbd2^,d73bbd2"]),
            ["ffed2a9", "e8290ef", "d73bbd2^", "d73bbd2"],
        )

    def test_empty_commit_list_raises(self):
        for bad in ("", "   ", ", ,", []):
            with self.assertRaises(ValueError):
                br.parse_commits(bad)


class RotatingOrderTests(unittest.TestCase):
    def test_each_build_is_first_once_in_every_n_rounds(self):
        for n in (2, 3, 6, 7):
            builds = [f"b{i}" for i in range(n)]
            firsts = []
            for r in range(2 * n):
                order = br.rotate_order(builds, r)
                self.assertEqual(len(order), n)
                self.assertEqual(set(order), set(builds))
                firsts.append(order[0])
            self.assertEqual(firsts[:n], builds)
            self.assertEqual(firsts[n:], builds)

    def test_schedule_rounds_gives_every_participant_each_slot_once_per_cycle(self):
        participants = ["ffed2a9", "e8290ef", "d73bbd2^", "d73bbd2", "528524a", "2abdb39", "minio"]
        rounds = br.schedule_rounds(participants, 7)
        self.assertEqual(len(rounds), 7)
        for slot in range(7):
            self.assertEqual(sorted(r[slot] for r in rounds), sorted(participants))


class StatsTests(unittest.TestCase):
    def test_median_min_and_max_over_fixed_numbers(self):
        med, lo, hi = br.summarize_values([1.80, 1.55, 1.72, 1.60, 1.75, 1.68, 1.70])
        self.assertAlmostEqual(med, 1.70)
        self.assertAlmostEqual(lo, 1.55)
        self.assertAlmostEqual(hi, 1.80)

    def test_even_count_median_min_max(self):
        med, lo, hi = br.summarize_values([2.0, 1.0, 1.4, 1.6])
        self.assertAlmostEqual(med, 1.5)
        self.assertAlmostEqual(lo, 1.0)
        self.assertAlmostEqual(hi, 2.0)

    def test_compute_round_ratios_over_fixed_throughput(self):
        awanfs = [340.0, 360.0, 350.0, 330.0, 370.0, 355.0, 345.0]
        minio = [200.0, 200.0, 200.0, 200.0, 200.0, 200.0, 200.0]
        ratios = br.compute_round_ratios(awanfs, minio)
        self.assertEqual(len(ratios), 7)
        med, lo, hi = br.summarize_values(ratios)
        self.assertAlmostEqual(med, 1.75)
        self.assertAlmostEqual(lo, 1.65)
        self.assertAlmostEqual(hi, 1.85)

    def test_zero_minio_throughput_rejected(self):
        with self.assertRaises(ValueError):
            br.compute_round_ratios([300.0], [0.0])


class VerdictRuleTests(unittest.TestCase):
    def test_disjoint_ranges_mean_step_is_real(self):
        a = make_summary("d73bbd2^", 1.74, 1.68, 1.81)
        b = make_summary("d73bbd2", 1.57, 1.52, 1.62)
        is_real, line = br.evaluate_verdict([a, b])
        self.assertTrue(is_real)
        self.assertIn("the step is real and starts at commit d73bbd2", line)
        self.assertIn("1.74", line)
        self.assertIn("1.57", line)
        self.assertIn("1.68", line)
        self.assertIn("1.52", line)

    def test_overlapping_ranges_mean_within_noise(self):
        a = make_summary("d73bbd2^", 1.70, 1.60, 1.80)
        b = make_summary("d73bbd2", 1.62, 1.55, 1.72)
        is_real, line = br.evaluate_verdict([a, b])
        self.assertFalse(is_real)
        self.assertIn("the builds are within noise of each other", line)

    def test_touching_endpoint_counts_as_overlap(self):
        a = make_summary("d73bbd2^", 1.70, 1.62, 1.80)
        b = make_summary("d73bbd2", 1.57, 1.52, 1.62)
        is_real, line = br.evaluate_verdict([a, b])
        self.assertFalse(is_real)
        self.assertIn("the builds are within noise of each other", line)

    def test_six_commits_pinpoint_start_of_step(self):
        summaries = [
            make_summary("ffed2a9", 1.75, 1.70, 1.81),
            make_summary("e8290ef", 1.73, 1.69, 1.79),
            make_summary("d73bbd2^", 1.74, 1.69, 1.80),
            make_summary("d73bbd2", 1.57, 1.52, 1.62),
            make_summary("528524a", 1.58, 1.53, 1.63),
            make_summary("2abdb39", 1.56, 1.51, 1.61),
        ]
        is_real, line = br.evaluate_verdict(summaries)
        self.assertTrue(is_real)
        self.assertIn("the step is real and starts at commit d73bbd2", line)

    def test_six_commits_all_overlapping_are_within_noise(self):
        summaries = [
            make_summary("ffed2a9", 1.72, 1.58, 1.82),
            make_summary("e8290ef", 1.68, 1.57, 1.79),
            make_summary("d73bbd2^", 1.70, 1.59, 1.80),
            make_summary("d73bbd2", 1.62, 1.54, 1.72),
            make_summary("528524a", 1.61, 1.53, 1.70),
            make_summary("2abdb39", 1.60, 1.52, 1.69),
        ]
        is_real, line = br.evaluate_verdict(summaries)
        self.assertFalse(is_real)
        self.assertIn("the builds are within noise of each other", line)

    def test_non_adjacent_disjoint_with_intermediate_overlap_reports_real(self):
        summaries = [
            make_summary("c0", 1.78, 1.72, 1.84),
            make_summary("c1", 1.68, 1.61, 1.75),
            make_summary("c2", 1.57, 1.50, 1.64),
        ]
        is_real, line = br.evaluate_verdict(summaries)
        self.assertTrue(is_real)
        self.assertIn("the step is real and starts at commit c2", line)
        self.assertIn("intermediate builds overlap", line)


class RunnerIntegrationTests(unittest.TestCase):
    def test_workload_matches_get_64k_128_objects_16_workers(self):
        size, conc, mode = rc.WORKLOADS["get_64k"]
        self.assertEqual((size, conc, mode), (64 * 1024, 16, "get"))
        pool_keys = 4 if size >= 1024 * 1024 else 8
        self.assertEqual(conc * pool_keys, 128)

    def test_default_work_dir_prefers_runner_temp(self):
        with mock.patch.dict(os.environ, {"RUNNER_TEMP": "/runner/fast-tmp"}):
            self.assertEqual(br.default_work_dir(), "/runner/fast-tmp")

    def test_build_commits_uses_worktree_under_work_dir_and_cleans_up(self):
        calls = []

        def fake_run(cmd, cwd=None, env=None, capture_output=False, text=False, check=False):
            calls.append((list(cmd), env))
            res = mock.MagicMock()
            res.returncode = 0
            res.stderr = ""
            if cmd[:3] == ["git", "-C", "/repo"] and cmd[3] == "rev-parse":
                ref = cmd[5].removesuffix("^{commit}")
                res.stdout = f"{ref}_fullsha\n"
            elif cmd[:3] == ["git", "-C", "/repo"] and cmd[3] == "log":
                res.stdout = "commit subject\n"
            elif cmd[:2] == ["cargo", "build"]:
                target_dir = env["CARGO_TARGET_DIR"]
                rel_dir = os.path.join(target_dir, "release")
                os.makedirs(rel_dir, exist_ok=True)
                bin_path = os.path.join(rel_dir, "s3-server" + br.EXE)
                with open(bin_path, "wb") as fh:
                    fh.write(b"fake-bin")
            else:
                res.stdout = ""
            return res

        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(br.subprocess, "run", side_effect=fake_run):
                built = br.build_commits(["ffed2a9", "d73bbd2^"], repo_root="/repo", work_dir=tmp)
            self.assertEqual(len(built), 2)
            for b in built:
                self.assertTrue(os.path.isfile(b.bin_path))
                self.assertTrue(os.path.abspath(b.bin_path).startswith(os.path.abspath(tmp)))
            worktree_adds = [c for c, _ in calls if len(c) > 4 and c[3] == "worktree" and c[4] == "add"]
            worktree_removes = [c for c, _ in calls if len(c) > 4 and c[3] == "worktree" and c[4] == "remove"]
            self.assertEqual(len(worktree_adds), 2)
            self.assertEqual(len(worktree_removes), 2)
            for c in worktree_adds:
                self.assertTrue(os.path.abspath(c[6]).startswith(os.path.abspath(tmp)))

    def test_build_failure_names_commit_and_cleans_worktree(self):
        calls = []

        def fake_run(cmd, cwd=None, env=None, capture_output=False, text=False, check=False):
            calls.append(list(cmd))
            res = mock.MagicMock()
            res.returncode = 0
            res.stderr = ""
            if cmd[:3] == ["git", "-C", "/repo"] and cmd[3] == "rev-parse":
                res.stdout = "abcdef1234567\n"
            elif cmd[:3] == ["git", "-C", "/repo"] and cmd[3] == "log":
                res.stdout = "old commit\n"
            elif cmd[:2] == ["cargo", "build"]:
                res.returncode = 1
                res.stderr = "error[E0433]: failed to resolve"
                res.stdout = ""
            else:
                res.stdout = ""
            return res

        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(br.subprocess, "run", side_effect=fake_run):
                with self.assertRaises(RuntimeError) as ctx:
                    br.build_commits(["ffed2a9"], repo_root="/repo", work_dir=tmp)
        self.assertIn("ffed2a9", str(ctx.exception))
        self.assertIn("E0433", str(ctx.exception))
        self.assertEqual(len([c for c in calls if len(c) > 4 and c[3] == "worktree" and c[4] == "remove"]), 1)

    def test_main_writes_output_and_results_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            minio_bin = os.path.join(tmp, "minio")
            mc_bin = os.path.join(tmp, "mc")
            for p in (minio_bin, mc_bin):
                with open(p, "wb") as fh:
                    fh.write(b"x")
            out_md = os.path.join(tmp, "out", "report.md")
            out_json = os.path.join(tmp, "out", "results.json")
            builds = [
                br.BuiltCommit("c1", "1111111", "first", os.path.join(tmp, "b1")),
                br.BuiltCommit("c2", "2222222", "second", os.path.join(tmp, "b2")),
            ]
            summaries = [
                make_summary("c1", 1.75, 1.70, 1.80),
                make_summary("c2", 1.55, 1.50, 1.60),
            ]
            rounds_data = [{"round": 1, "order": ["c1", "c2", "minio"], "minio_mib": 200.0,
                            "builds": {"c1": {"mib_per_sec": 350.0, "ratio": 1.75},
                                       "c2": {"mib_per_sec": 310.0, "ratio": 1.55}}}]
            with mock.patch.object(br, "build_commits", return_value=builds), \
                 mock.patch.object(br, "measure_bisect", return_value=(summaries, rounds_data)), \
                 mock.patch.object(br.rc, "can_pin", return_value=False):
                code = br.main([
                    "--commits", "c1", "c2",
                    "--minio-bin", minio_bin,
                    "--mc-bin", mc_bin,
                    "--rounds", "1",
                    "--work-dir", tmp,
                    "--output", out_md,
                    "--results", out_json,
                    "--run-id", "12345678",
                ])
            self.assertEqual(code, 0)
            with open(out_md, encoding="utf-8") as fh:
                md = fh.read()
            self.assertTrue(md.startswith("Command: `python benchmarks/bisect_ratio.py"))
            self.assertIn("12345678", md)
            self.assertIn("the step is real and starts at commit c2", md.splitlines()[-1])
            self.assertTrue(os.path.isfile(out_json))

    def test_run_bisect_measures_7_rounds_with_rotating_order_and_renders_report(self):
        builds = [
            br.BuiltCommit(commit="c1", sha="1111111", subject="first", bin_path="/bins/c1"),
            br.BuiltCommit(commit="c2", sha="2222222", subject="second", bin_path="/bins/c2"),
        ]
        order_seen = []

        class FakeProc:
            pid = 1234

            def terminate(self):
                pass

            def wait(self, timeout=None):
                pass

            def kill(self):
                pass

        def fake_start_awanfs(bin_path):
            order_seen.append(os.path.basename(bin_path))
            return FakeProc(), 9000, tempfile.mkdtemp(prefix="test_bisect_a_")

        def fake_start_minio(minio_bin, mc_bin):
            order_seen.append("minio")
            return FakeProc(), 9001, tempfile.mkdtemp(prefix="test_bisect_m_")

        def fake_measure_phase(proc, port, size, conc, duration, mode):
            self.assertEqual((size, conc, mode), (64 * 1024, 16, "get"))
            self.assertEqual(duration, 3.0)
            last = order_seen[-1]
            if last == "minio":
                return 200.0, 0.5
            if last == "c1":
                return 350.0, 0.3
            return 310.0, 0.4

        with mock.patch.object(br.rc, "start_awanfs", side_effect=fake_start_awanfs), \
             mock.patch.object(br.rc, "start_minio", side_effect=fake_start_minio), \
             mock.patch.object(br.rc, "_measure_phase", side_effect=fake_measure_phase), \
             mock.patch.object(br.rc, "get_process_peak_rss_mb", return_value=100.0):
            summaries, rounds_data = br.measure_bisect(
                builds, minio_bin="minio", mc_bin="mc", rounds=7, duration=3.0
            )

        self.assertEqual(len(rounds_data), 7)
        # 3 participants (c1, c2, minio): each appears first once in every 3 rounds.
        self.assertEqual([r["order"][0] for r in rounds_data[:3]], ["c1", "c2", "minio"])
        self.assertEqual([r["order"][0] for r in rounds_data[3:6]], ["c1", "c2", "minio"])
        is_real, verdict_line = br.evaluate_verdict(summaries)
        self.assertTrue(is_real)
        report = br.render_report(
            cmd="python benchmarks/bisect_ratio.py --commits c1 c2",
            summaries=summaries,
            rounds_data=rounds_data,
            rounds=7,
            duration=3.0,
            verdict_line=verdict_line,
            run_id="99999999",
        )
        lines = report.strip().splitlines()
        self.assertTrue(lines[0].startswith("Command: "))
        self.assertIn("99999999", report)
        self.assertEqual(lines[-1], verdict_line)


if __name__ == "__main__":
    unittest.main()
