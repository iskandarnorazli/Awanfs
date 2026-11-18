"""Self-tests for core_scaling.py: pinning, scaling, CPU %, unmeasured gaps, charts, and report."""
import math
import os
import sys
import tempfile
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import core_scaling as cs  # noqa: E402


def _make_fake_matplotlib():
    """Minimal recording matplotlib/pyplot stub when matplotlib is not installed."""
    saved_paths = []
    created_figures = []

    class FakeLine:
        def __init__(self, x, y, linestyle="-", marker=None, label="", **kwargs):
            self._x = list(x)
            self._y = list(y)
            self._linestyle = linestyle
            self._marker = marker
            self._label = label

        def get_xdata(self):
            return list(self._x)

        def get_ydata(self):
            return list(self._y)

        def get_linestyle(self):
            return self._linestyle

        def get_marker(self):
            return self._marker

        def get_label(self):
            return self._label

    class FakeAxes:
        def __init__(self):
            self.lines = []
            self._title = ""
            self._xlabel = ""
            self._ylabel = ""
            self._xscale = "linear"
            self._xbase = None
            self._xticks = []
            self._xticklabels = []

        def plot(self, x, y, linestyle="-", marker=None, label="", **kwargs):
            line = FakeLine(x, y, linestyle=linestyle, marker=marker, label=label, **kwargs)
            self.lines.append(line)
            return [line]

        def axhline(self, y=0, linestyle="-", label="", **kwargs):
            line = FakeLine([0, 1], [y, y], linestyle=linestyle, marker=None, label=label, **kwargs)
            self.lines.append(line)
            return line

        def set_title(self, t, **kwargs):
            self._title = t

        def get_title(self):
            return self._title

        def set_xlabel(self, s):
            self._xlabel = s

        def get_xlabel(self):
            return self._xlabel

        def set_ylabel(self, s):
            self._ylabel = s

        def get_ylabel(self):
            return self._ylabel

        def set_xscale(self, scale, base=10):
            self._xscale = scale
            self._xbase = base

        def get_xscale(self):
            return self._xscale

        def set_xticks(self, ticks):
            self._xticks = list(ticks)

        def set_xticklabels(self, labels):
            self._xticklabels = list(labels)

        def grid(self, *args, **kwargs):
            pass

        def get_legend_handles_labels(self):
            return self.lines, [ln.get_label() for ln in self.lines]

    class FakeFigure:
        def __init__(self, axes):
            self.axes = axes
            self._suptitle = ""

        def suptitle(self, t, **kwargs):
            self._suptitle = t

        def legend(self, *args, **kwargs):
            pass

        def tight_layout(self, *args, **kwargs):
            pass

        def savefig(self, path, **kwargs):
            saved_paths.append(path)
            with open(path, "wb") as fh:
                fh.write(b"fake-chart")

    def subplots(nrows=1, ncols=1, **kwargs):
        axes = [FakeAxes() for _ in range(nrows * ncols)]
        fig = FakeFigure(axes)
        created_figures.append(fig)
        return fig, axes if len(axes) > 1 else axes[0]

    def close(fig=None):
        pass

    plt_mod = types.ModuleType("matplotlib.pyplot")
    plt_mod.subplots = subplots
    plt_mod.close = close
    mpl_mod = types.ModuleType("matplotlib")
    mpl_mod.use = lambda *args, **kwargs: None
    mpl_mod.pyplot = plt_mod
    return mpl_mod, plt_mod, created_figures, saved_paths


def _sample_results():
    cores = [1, 2, 4]
    results = {}
    for wkey, _label, _mode, _sz in cs.WORKLOADS:
        results[wkey] = {
            "AwanFS": {
                1: cs.PointResult(ops_per_sec=10000.0, p50_ms=1.5, p99_ms=4.0, cpu_pct=90.0, ops_per_busy_core=11111.1),
                2: cs.PointResult(ops_per_sec=19000.0, p50_ms=1.6, p99_ms=4.2, cpu_pct=180.0, ops_per_busy_core=10555.6),
                4: cs.PointResult(ops_per_sec=35000.0, p50_ms=1.8, p99_ms=5.0, cpu_pct=350.0, ops_per_busy_core=10000.0, shares_cores=True),
            },
            "MinIO": {
                1: cs.PointResult(ops_per_sec=4000.0, p50_ms=3.8, p99_ms=9.0, cpu_pct=95.0, ops_per_busy_core=4210.5),
                2: cs.PointResult(ops_per_sec=7200.0, p50_ms=4.1, p99_ms=10.5, cpu_pct=190.0, ops_per_busy_core=3789.5),
                4: cs.PointResult(ops_per_sec=12000.0, p50_ms=4.5, p99_ms=12.0, cpu_pct=360.0, ops_per_busy_core=3333.3, shares_cores=True),
            },
            "RustFS": {
                1: cs.PointResult(reason="rustfs binary not found"),
                2: cs.PointResult(reason="rustfs binary not found"),
                4: cs.PointResult(reason="rustfs binary not found"),
            },
        }
    return cores, results


class PinningTests(unittest.TestCase):
    def test_linux_pinning_command_uses_taskset(self):
        self.assertEqual(cs.pinning_command(["s3-server", "--port", "9000"], 1, "linux"),
                         ["taskset", "-c", "0", "s3-server", "--port", "9000"])
        self.assertEqual(cs.pinning_command(["s3-server", "--port", "9000"], 4, "linux"),
                         ["taskset", "-c", "0-3", "s3-server", "--port", "9000"])

    def test_windows_pinning_leaves_argv_and_pins_via_psutil(self):
        argv = ["s3-server.exe", "--port", "9000"]
        self.assertEqual(cs.pinning_command(argv, 4, "win32"), argv)

        calls = []

        class FakeProc:
            def __init__(self, pid):
                calls.append(("init", pid))

            def cpu_affinity(self, cpus):
                calls.append(("affinity", list(cpus)))

        fake_psutil = types.SimpleNamespace(Process=FakeProc)
        with mock.patch.dict(sys.modules, {"psutil": fake_psutil}):
            ok = cs.pin_process(1234, [0, 1, 2, 3])
        self.assertTrue(ok)
        self.assertEqual(calls, [("init", 1234), ("affinity", [0, 1, 2, 3])])

    def test_client_cores_use_remaining_cores_or_share(self):
        self.assertEqual(cs.client_cores_for(1, 4), ([1, 2, 3], False))
        self.assertEqual(cs.client_cores_for(2, 4), ([2, 3], False))
        self.assertEqual(cs.client_cores_for(4, 4), ([0, 1, 2, 3], True))


class ScalingAndCpuTests(unittest.TestCase):
    def test_scaling_is_largest_n_over_1_core(self):
        pts = {
            1: cs.PointResult(ops_per_sec=10000.0, p50_ms=1.0, p99_ms=2.0),
            2: cs.PointResult(ops_per_sec=18000.0, p50_ms=1.1, p99_ms=2.2),
            4: cs.PointResult(ops_per_sec=35000.0, p50_ms=1.2, p99_ms=2.5),
        }
        self.assertAlmostEqual(cs.compute_scaling(pts, [1, 2, 4]), 3.5)

    def test_scaling_is_none_when_endpoint_unmeasured(self):
        pts = {
            1: cs.PointResult(reason="failed to start"),
            4: cs.PointResult(ops_per_sec=35000.0, p50_ms=1.2, p99_ms=2.5),
        }
        self.assertIsNone(cs.compute_scaling(pts, [1, 4]))

    def test_cpu_percent_and_ops_per_busy_core(self):
        pct = cs.cpu_percent((1.0, 0.5), (4.0, 2.0), wall_sec=3.0)
        # delta = (4.0 + 2.0) - (1.0 + 0.5) = 4.5 s over 3.0 s = 150.0% (1.5 cores busy)
        self.assertAlmostEqual(pct, 150.0)
        self.assertAlmostEqual(cs.ops_per_busy_core(15000.0, pct), 10000.0)

    def test_process_cpu_times_total_includes_children(self):
        class FakeChild:
            def cpu_times(self):
                return types.SimpleNamespace(user=0.4, system=0.1)

        class FakeProc:
            def __init__(self, pid):
                pass

            def cpu_times(self):
                return types.SimpleNamespace(user=2.0, system=0.5)

            def children(self, recursive=True):
                return [FakeChild()]

        fake_psutil = types.SimpleNamespace(Process=FakeProc, NoSuchProcess=Exception, AccessDenied=Exception)
        with mock.patch.dict(sys.modules, {"psutil": fake_psutil}):
            u, s = cs.process_cpu_times_total(999)
        self.assertAlmostEqual(u, 2.4)
        self.assertAlmostEqual(s, 0.6)

    def test_alternating_order_and_median_aggregation(self):
        self.assertEqual(cs.server_order_for_run(0, ["AwanFS", "MinIO", "RustFS"]), ["AwanFS", "MinIO", "RustFS"])
        self.assertEqual(cs.server_order_for_run(1, ["AwanFS", "MinIO", "RustFS"]), ["RustFS", "MinIO", "AwanFS"])
        agg = cs.aggregate_runs([
            cs.PointResult(ops_per_sec=1000.0, p50_ms=2.0, p99_ms=5.0, cpu_pct=80.0, ops_per_busy_core=1250.0),
            cs.PointResult(ops_per_sec=2000.0, p50_ms=1.0, p99_ms=3.0, cpu_pct=120.0, ops_per_busy_core=1666.7),
        ])
        self.assertAlmostEqual(agg.ops_per_sec, 1500.0)
        self.assertAlmostEqual(agg.p50_ms, 1.5)
        self.assertAlmostEqual(agg.cpu_pct, 100.0)


class FailureAndReportTests(unittest.TestCase):
    def test_failed_server_start_yields_not_measured_with_reason(self):
        def broken_starter():
            raise RuntimeError("exit code 1: address in use")

        res = cs.measure_server_for_cores(
            server_name="RustFS",
            starter=broken_starter,
            n_cores=1,
            cli_cores=[1, 2, 3],
            shares_cores=False,
            duration_sec=0.1,
        )
        for wkey, _label, _mode, _sz in cs.WORKLOADS:
            self.assertFalse(res[wkey].measured)
            self.assertIn("address in use", res[wkey].reason)

    def test_report_starts_with_command_and_shows_unmeasured_and_sharing(self):
        cores, results = _sample_results()
        cmd = "python benchmarks/core_scaling.py --cores 1,2,4 --runs 2 --duration 3"
        md = cs.render_report(
            cmd=cmd,
            cores=cores,
            results=results,
            total_cores=4,
            runs=2,
            duration_sec=3.0,
            max_put_ops={4096: 12000.0, 65536: 8000.0},
            chart_paths=[],
        )
        self.assertTrue(md.startswith(f"Command: `{cmd}`\n"))
        self.assertIn("shares cores", md.lower())
        self.assertIn("| PUT 4 KiB | 4 KiB | RustFS | not measured (rustfs binary not found) |", md)
        self.assertIn("3.50x", md)


class ChartTests(unittest.TestCase):
    def test_chart_writer_four_panels_log2_styles_max_line_and_nan_gap(self):
        cores, results = _sample_results()
        max_put = {4096: 15000.0, 65536: 9000.0}

        try:
            import matplotlib  # noqa: F401
            has_mpl = True
        except ImportError:
            has_mpl = False

        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = os.path.join(tmpdir, "core_scaling.md")
            if has_mpl:
                import matplotlib
                matplotlib.use("Agg")
                import matplotlib.pyplot as plt
                captured_figs = []
                orig_subplots = plt.subplots

                def spy_subplots(*args, **kwargs):
                    fig, axes = orig_subplots(*args, **kwargs)
                    captured_figs.append((fig, list(axes)))
                    return fig, axes

                with mock.patch.object(plt, "subplots", side_effect=spy_subplots):
                    written = cs.write_charts(results, cores, report_path, max_put_ops=max_put)
            else:
                mpl_mod, plt_mod, created_figs, _ = _make_fake_matplotlib()
                with mock.patch.dict(sys.modules, {"matplotlib": mpl_mod, "matplotlib.pyplot": plt_mod}):
                    written = cs.write_charts(results, cores, report_path, max_put_ops=max_put)
                captured_figs = [(f, f.axes) for f in created_figs]

            # 4 workload figures + 1 ops/s per busy core figure = 5 figures -> 10 files (PNG + SVG)
            self.assertEqual(len(captured_figs), 5)
            self.assertEqual(len(written), 10)
            for p in written:
                self.assertTrue(os.path.isfile(p), f"missing chart {p}")
                self.assertGreater(os.path.getsize(p), 0)
                self.assertEqual(os.path.dirname(p), tmpdir)

            # Check PUT 4 KiB (figure 0) and GET 4 KiB (figure 2)
            put4k_fig, put4k_axes = captured_figs[0]
            self.assertEqual(len(put4k_axes), 4)
            expected_ylabels = ["K ops/s", "CPU usage (%)", "P50 latency (ms)", "P99 latency (ms)"]
            for ax, expected_y in zip(put4k_axes, expected_ylabels):
                self.assertEqual(ax.get_xscale(), "log")
                self.assertEqual(ax.get_ylabel(), expected_y)

            # Panel (a) on PUT 4 KiB has 3 server lines + 1 MAX line
            panel_a_labels = [ln.get_label() for ln in put4k_axes[0].lines]
            self.assertIn("AwanFS", panel_a_labels)
            self.assertIn("MinIO", panel_a_labels)
            self.assertIn("RustFS", panel_a_labels)
            self.assertIn("MAX", panel_a_labels)

            # RustFS failed -> all NaN in chart, never 0
            rustfs_line = next(ln for ln in put4k_axes[0].lines if ln.get_label() == "RustFS")
            self.assertTrue(all(math.isnan(float(y)) for y in rustfs_line.get_ydata()))

            # Line styles + markers differ per server, not by color alone
            server_styles = {
                ln.get_label(): (ln.get_linestyle(), ln.get_marker())
                for ln in put4k_axes[0].lines
                if ln.get_label() in cs.SERVERS
            }
            self.assertEqual(len(set(server_styles.values())), 3)

            # GET 4 KiB panel (a) has NO MAX line
            _get4k_fig, get4k_axes = captured_figs[2]
            get_labels = [ln.get_label() for ln in get4k_axes[0].lines]
            self.assertNotIn("MAX", get_labels)

            # 5th figure: ops/s per busy core
            _eff_fig, eff_axes = captured_figs[4]
            self.assertTrue(any("busy core" in ax.get_ylabel().lower() for ax in eff_axes))


if __name__ == "__main__":
    unittest.main()
