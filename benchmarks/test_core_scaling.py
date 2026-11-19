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
            self.images = []
            self.bars = []
            self.texts = []
            self._title = ""
            self._xlabel = ""
            self._ylabel = ""
            self._xscale = "linear"
            self._xbase = None
            self._xticks = []
            self._xticklabels = []
            self._yticks = []
            self._yticklabels = []
            self._ylim = (0.0, 1.0)
            self._ylim_set = False

        def plot(self, x, y, linestyle="-", marker=None, label="", **kwargs):
            line = FakeLine(x, y, linestyle=linestyle, marker=marker, label=label, **kwargs)
            self.lines.append(line)
            return [line]

        def axhline(self, y=0, linestyle="-", label="", **kwargs):
            line = FakeLine([0, 1], [y, y], linestyle=linestyle, marker=None, label=label, **kwargs)
            self.lines.append(line)
            return line

        def bar(self, x, height, width=0.8, label="", **kwargs):
            xs = list(x)
            hs = list(height)
            self.bars.append({"x": xs, "height": hs, "width": width, "label": label, **kwargs})
            if label:
                self.lines.append(FakeLine(xs, hs, label=label))
            return [types.SimpleNamespace(get_height=lambda h=h: h) for h in hs]

        def legend(self, *args, **kwargs):
            pass

        def imshow(self, data, **kwargs):
            rows = [list(r) for r in data]
            img = types.SimpleNamespace(data=rows, kwargs=kwargs)
            self.images.append(img)
            return img

        def text(self, x, y, s, **kwargs):
            self.texts.append((x, y, s))

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

        def get_xticks(self):
            return list(self._xticks)

        def set_xticklabels(self, labels, **kwargs):
            self._xticklabels = list(labels)

        def get_xticklabels(self):
            return [types.SimpleNamespace(get_text=lambda s=s: s) for s in self._xticklabels]

        def set_yticks(self, ticks):
            self._yticks = list(ticks)

        def get_yticks(self):
            return list(self._yticks)

        def set_yticklabels(self, labels, **kwargs):
            self._yticklabels = list(labels)

        def set_ylim(self, bottom=None, top=None):
            if isinstance(bottom, (tuple, list)):
                bottom, top = bottom
            self._ylim = (float(bottom), float(top))
            self._ylim_set = True

        def get_ylim(self):
            return self._ylim

        def grid(self, *args, **kwargs):
            pass

        def get_legend_handles_labels(self):
            return self.lines, [ln.get_label() for ln in self.lines]

    class FakeFigure:
        def __init__(self, axes, grid=None):
            self.axes = axes
            self.grid = grid
            self._suptitle = ""
            self.colorbars = []

        def suptitle(self, t, **kwargs):
            self._suptitle = t

        def legend(self, *args, **kwargs):
            pass

        def colorbar(self, mappable, ax=None, **kwargs):
            cb = types.SimpleNamespace(mappable=mappable, ax=ax, set_label=lambda *a, **kw: None)
            self.colorbars.append(cb)
            return cb

        def tight_layout(self, *args, **kwargs):
            pass

        def savefig(self, path, **kwargs):
            saved_paths.append(path)
            with open(path, "wb") as fh:
                fh.write(b"fake-chart")

    def subplots(nrows=1, ncols=1, **kwargs):
        if nrows > 1 and ncols > 1:
            grid = [[FakeAxes() for _ in range(ncols)] for _ in range(nrows)]
            flat = [ax for row in grid for ax in row]
            fig = FakeFigure(flat, grid=grid)
            created_figures.append(fig)
            return fig, grid
        axes = [FakeAxes() for _ in range(nrows * ncols)]
        fig = FakeFigure(axes)
        created_figures.append(fig)
        return fig, axes if len(axes) > 1 else axes[0]

    class FakeCmap:
        def copy(self):
            return FakeCmap()

        def set_bad(self, *args, **kwargs):
            pass

    def get_cmap(name=None):
        return FakeCmap()

    def close(fig=None):
        pass

    plt_mod = types.ModuleType("matplotlib.pyplot")
    plt_mod.subplots = subplots
    plt_mod.close = close
    plt_mod.get_cmap = get_cmap
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


def _flatten_axes(axes):
    if isinstance(axes, (list, tuple)):
        out = []
        for item in axes:
            out.extend(_flatten_axes(item))
        return out
    try:
        return list(axes.flat)
    except AttributeError:
        return [axes]


def _sample_matrix_results():
    patterns = ["seq-write", "rand-write", "seq-read", "rand-read"]
    sizes = [4096, 1048576]
    cores = [1, 2, 4]
    concs = [1, 4, 16, 64, 256]
    # results[pattern][size][srv][n_cores][conc] -> PointResult
    results = {}
    for p_idx, pat in enumerate(patterns):
        results[pat] = {}
        for s_idx, sz in enumerate(sizes):
            results[pat][sz] = {}
            for srv in cs.SERVERS:
                results[pat][sz][srv] = {}
                for n in cores:
                    results[pat][sz][srv][n] = {}
                    for c in concs:
                        if srv == "RustFS":
                            results[pat][sz][srv][n][c] = cs.PointResult(reason="rustfs binary not found")
                        elif srv == "MinIO" and c == 256:
                            results[pat][sz][srv][n][c] = cs.PointResult(reason="timed out at c=256")
                        else:
                            base = 12000.0 if srv == "AwanFS" else 5000.0
                            if "read" in pat:
                                base *= 1.4
                            if "seq" in pat:
                                base *= 1.15
                            if sz >= 1048576:
                                base /= 8.0
                            ops = base * n * (1.0 + 0.3 * math.log2(c))
                            mib = (ops * sz) / (1024.0 * 1024.0)
                            p50 = max(0.2, 20.0 / n + 0.05 * c)
                            p99 = p50 * 2.5
                            cpu = min(n * 100.0, 45.0 * n + 0.2 * c)
                            results[pat][sz][srv][n][c] = cs.PointResult(
                                ops_per_sec=ops,
                                mib_per_sec=mib,
                                p50_ms=p50,
                                p99_ms=p99,
                                cpu_pct=cpu,
                                ops_per_busy_core=ops / (cpu / 100.0),
                                shares_cores=(n >= 4),
                            )
    return patterns, sizes, cores, concs, results


class PatternAndSizeParsingTests(unittest.TestCase):
    def test_parse_patterns_and_rejects_unknown(self):
        self.assertEqual(
            cs.parse_patterns("seq-write,rand-write,seq-read,rand-read"),
            ["seq-write", "rand-write", "seq-read", "rand-read"],
        )
        with self.assertRaises(ValueError):
            cs.parse_patterns("seq-write,bogus-op")

    def test_parse_sizes_accepts_units_and_raw_bytes(self):
        self.assertEqual(cs.parse_sizes("4KiB,1MiB"), [4096, 1048576])
        self.assertEqual(cs.parse_sizes("4096,1048576"), [4096, 1048576])
        self.assertEqual(cs.parse_size("64MiB"), 64 * 1024 * 1024)

    def test_parse_concurrencies(self):
        self.assertEqual(cs.parse_concurrencies("1,4,16,64,256"), [1, 4, 16, 64, 256])
        with self.assertRaises(ValueError):
            cs.parse_concurrencies("0,4")

    def test_prefill_exceeds_stated_cache_size(self):
        budget_bytes = cs.parse_size(cs.DEFAULT_MEMORY_BUDGET)
        cache_bytes = cs.stated_cache_bytes(budget_bytes)
        prefill_bytes = cs.parse_size(cs.DEFAULT_PREFILL_SIZE)
        self.assertGreater(prefill_bytes, cache_bytes)
        cs.validate_prefill_exceeds_cache(prefill_bytes, cache_bytes)
        with self.assertRaises(ValueError):
            cs.validate_prefill_exceeds_cache(cache_bytes, cache_bytes)
        with self.assertRaises(ValueError):
            cs.validate_prefill_exceeds_cache(cache_bytes - 1024, cache_bytes)


class WorkloadSequenceTests(unittest.TestCase):
    def test_sequential_write_keys_strictly_increasing_and_whole_object(self):
        for sz in (4096, 1048576):
            reqs = cs.generate_request_sequence(
                pattern="seq-write",
                size=sz,
                count=5,
                seed=42,
                prefill_count=100,
                large_obj_size=16 * 1024 * 1024,
            )
            self.assertEqual([r.key for r in reqs], ["seq/000000", "seq/000001", "seq/000002", "seq/000003", "seq/000004"])
            self.assertTrue(all(r.method == "PUT" and r.range_start is None for r in reqs))

    def test_sequential_read_4k_key_order_and_1m_increasing_range_offsets(self):
        reqs_4k = cs.generate_request_sequence(
            pattern="seq-read",
            size=4096,
            count=5,
            seed=42,
            prefill_count=3,
            large_obj_size=16 * 1024 * 1024,
        )
        self.assertEqual(
            [r.key for r in reqs_4k],
            ["prefill_4k/000000", "prefill_4k/000001", "prefill_4k/000002", "prefill_4k/000000", "prefill_4k/000001"],
        )
        self.assertTrue(all(r.method == "GET" and r.range_start is None for r in reqs_4k))

        large_sz = 4 * 1024 * 1024
        reqs_1m = cs.generate_request_sequence(
            pattern="seq-read",
            size=1048576,
            count=6,
            seed=42,
            prefill_count=3,
            large_obj_size=large_sz,
        )
        self.assertEqual(len({r.key for r in reqs_1m}), 1)
        offsets = [(r.range_start, r.range_end) for r in reqs_1m]
        self.assertEqual(
            offsets,
            [
                (0, 1048575),
                (1048576, 2097151),
                (2097152, 3145727),
                (3145728, 4194303),
                (0, 1048575),
                (1048576, 2097151),
            ],
        )
        for s, e in offsets:
            self.assertGreaterEqual(s, 0)
            self.assertLess(e, large_sz)
            self.assertEqual(e - s + 1, 1048576)

    def test_seeded_random_keys_repeat_and_differ_and_ranges_inside_object(self):
        large_sz = 16 * 1024 * 1024
        for pat, sz in [("rand-write", 4096), ("rand-write", 1048576), ("rand-read", 4096), ("rand-read", 1048576)]:
            a = cs.generate_request_sequence(pat, sz, count=20, seed=1234, prefill_count=512, large_obj_size=large_sz)
            b = cs.generate_request_sequence(pat, sz, count=20, seed=1234, prefill_count=512, large_obj_size=large_sz)
            c = cs.generate_request_sequence(pat, sz, count=20, seed=5678, prefill_count=512, large_obj_size=large_sz)
            self.assertEqual(a, b, f"{pat} {sz}: same seed must repeat")
            self.assertNotEqual(a, c, f"{pat} {sz}: different seed must differ")

        rand_1m = cs.generate_request_sequence("rand-read", 1048576, count=50, seed=99, prefill_count=512, large_obj_size=large_sz)
        for r in rand_1m:
            self.assertEqual(r.method, "GET")
            self.assertIsNotNone(r.range_start)
            self.assertIsNotNone(r.range_end)
            self.assertGreaterEqual(r.range_start, 0)
            self.assertLess(r.range_end, large_sz)
            self.assertEqual(r.range_end - r.range_start + 1, 4096)

    def test_deterministic_prefill_bytes_repeat_for_same_seed(self):
        b1 = cs.deterministic_bytes(4096, seed=42)
        b2 = cs.deterministic_bytes(4096, seed=42)
        b3 = cs.deterministic_bytes(4096, seed=43)
        self.assertEqual(len(b1), 4096)
        self.assertEqual(b1, b2)
        self.assertNotEqual(b1, b3)


class MatrixReportTests(unittest.TestCase):
    def test_matrix_report_command_seed_cache_prefill_and_unmeasured(self):
        patterns, sizes, cores, concs, results = _sample_matrix_results()
        cmd = "python benchmarks/core_scaling.py --pattern seq-write,rand-write,seq-read,rand-read --sizes 4KiB,1MiB --concurrencies 1,4,16,64,256 --seed 42"
        md = cs.render_matrix_report(
            cmd=cmd,
            patterns=patterns,
            sizes=sizes,
            cores=cores,
            concurrencies=concs,
            results=results,
            total_cores=4,
            runs=2,
            duration_sec=2.0,
            seed=42,
            memory_budget="64MiB",
            prefill_bytes=80 * 1024 * 1024,
            chart_paths=[],
        )
        self.assertTrue(md.startswith(f"Command: `{cmd}`\n"))
        self.assertIn("seed = `42`", md)
        self.assertIn("64 MiB", md)
        self.assertIn("80 MiB", md)
        self.assertIn("| Server | Pattern | Size | Cores | Concurrency | ops/s | MiB/s | P50 (ms) | P99 (ms) | CPU % |", md)
        self.assertIn("| RustFS | seq-write | 4 KiB | 1 | 1 | not measured (rustfs binary not found) |", md)
        self.assertIn("| MinIO | seq-write | 4 KiB | 1 | 256 | not measured (timed out at c=256) |", md)
        self.assertNotIn("| RustFS | seq-write | 4 KiB | 1 | 1 | 0 ", md)

    def test_aggregate_runs_computes_mib_per_sec_median(self):
        agg = cs.aggregate_runs([
            cs.PointResult(ops_per_sec=1000.0, mib_per_sec=4.0, p50_ms=2.0, p99_ms=5.0, cpu_pct=80.0),
            cs.PointResult(ops_per_sec=2000.0, mib_per_sec=8.0, p50_ms=1.0, p99_ms=3.0, cpu_pct=120.0),
        ])
        self.assertAlmostEqual(agg.mib_per_sec, 6.0)


class MatrixChartTests(unittest.TestCase):
    def test_matrix_charts_panels_axes_shared_ylim_heatmaps_summary_and_gaps(self):
        patterns, sizes, cores, concs, results = _sample_matrix_results()
        try:
            import matplotlib  # noqa: F401
            has_mpl = True
        except ImportError:
            has_mpl = False

        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = os.path.join(tmpdir, "report.md")
            if has_mpl:
                import matplotlib
                matplotlib.use("Agg")
                import matplotlib.pyplot as plt
                captured_figs = []
                orig_subplots = plt.subplots

                def spy_subplots(*args, **kwargs):
                    fig, axes = orig_subplots(*args, **kwargs)
                    captured_figs.append((fig, _flatten_axes(axes)))
                    return fig, axes

                with mock.patch.object(plt, "subplots", side_effect=spy_subplots):
                    written = cs.write_matrix_charts(results, patterns, sizes, cores, concs, report_path)
            else:
                mpl_mod, plt_mod, created_figs, _ = _make_fake_matplotlib()
                with mock.patch.dict(sys.modules, {"matplotlib": mpl_mod, "matplotlib.pyplot": plt_mod}):
                    written = cs.write_matrix_charts(results, patterns, sizes, cores, concs, report_path)
                captured_figs = [(f, list(f.axes)) for f in created_figs]

            # 8 core-scaling figs + 8 concurrency-sweep figs + 3 heatmap figs + 1 summary fig = 20 figures -> 40 files
            self.assertEqual(len(captured_figs), 20)
            self.assertEqual(len(written), 40)
            for p in written:
                self.assertTrue(os.path.isfile(p), f"missing chart {p}")
                self.assertGreater(os.path.getsize(p), 0)

            # 1) Core-scaling figure (first 8): 4 panels, log2 x-axis, distinct server styles, RustFS is NaN
            core_fig, core_axes = captured_figs[0]
            self.assertEqual(len(core_axes), 4)
            self.assertEqual(
                [ax.get_ylabel() for ax in core_axes],
                ["K ops/s", "CPU usage (%)", "P50 latency (ms)", "P99 latency (ms)"],
            )
            for ax in core_axes:
                self.assertEqual(ax.get_xscale(), "log")
            rustfs_line = next(ln for ln in core_axes[0].lines if ln.get_label() == "RustFS")
            self.assertTrue(all(math.isnan(float(y)) for y in rustfs_line.get_ydata()))
            styles = {
                ln.get_label(): (ln.get_linestyle(), ln.get_marker())
                for ln in core_axes[0].lines
                if ln.get_label() in cs.SERVERS
            }
            self.assertEqual(len(set(styles.values())), 3)

            # 2) Concurrency sweep figure (figs 8..15): 2 rows x 3 servers, log2 x, same y scale across servers, c=256 MinIO gap
            conc_fig, conc_axes = captured_figs[8]
            self.assertEqual(len(conc_axes), 6)
            for ax in conc_axes:
                self.assertEqual(ax.get_xscale(), "log")
            ops_ylims = [conc_axes[i].get_ylim() for i in range(3)]
            p99_ylims = [conc_axes[i].get_ylim() for i in range(3, 6)]
            self.assertEqual(len(set(ops_ylims)), 1, f"ops/s ylims differ across servers: {ops_ylims}")
            self.assertEqual(len(set(p99_ylims)), 1, f"P99 ylims differ across servers: {p99_ylims}")
            minio_ops_lines = conc_axes[1].lines
            self.assertEqual(len(minio_ops_lines), len(cores))
            self.assertTrue(math.isnan(float(minio_ops_lines[0].get_ydata()[-1])))

            # 3) Heatmaps per server at 4 KiB (figs 16..18): 4 panels (one per pattern), MinIO c=256 is NaN
            minio_hm_fig, minio_hm_axes = captured_figs[17]
            self.assertEqual(len(minio_hm_axes), 4)
            for ax in minio_hm_axes:
                self.assertEqual(len(ax.images), 1)
                img_data = ax.images[0].get_array() if hasattr(ax.images[0], "get_array") else ax.images[0].data
                self.assertEqual(len(img_data), len(cores))
                self.assertEqual(len(img_data[0]), len(concs))
                self.assertTrue(math.isnan(float(img_data[0][-1])))

            # 4) Summary figure (fig 19): grouped bars per server at 4 cores, c=64, RustFS is NaN (never 0)
            sum_fig, sum_axes = captured_figs[19]
            self.assertEqual(len(sum_axes), len(sizes))
            if not has_mpl:
                rustfs_bar = next(b for b in sum_axes[0].bars if b["label"] == "RustFS")
                self.assertTrue(all(math.isnan(float(h)) for h in rustfs_bar["height"]))


class MatrixExecutionTests(unittest.TestCase):
    def test_failed_matrix_server_start_yields_not_measured_with_reason(self):
        def broken_starter():
            raise RuntimeError("exit code 1: address in use")

        res = cs.measure_server_matrix_for_cores(
            server_name="RustFS",
            starter=broken_starter,
            n_cores=1,
            cli_cores=[1],
            shares_cores=False,
            patterns=["seq-write", "rand-read"],
            sizes=[4096],
            concurrencies=[1, 4],
            duration_sec=0.05,
            seed=42,
            prefill_bytes=16384,
        )
        self.assertEqual(len(res), 4)
        for pt in res.values():
            self.assertFalse(pt.measured)
            self.assertIn("address in use", pt.reason)

    def test_live_http_prefill_and_patterns_against_stub_s3_server(self):
        import threading
        import subprocess

        with tempfile.TemporaryDirectory() as tmpdir:
            cs.rwb.BaselineFileS3Handler.storage_root = tmpdir
            port = cs.rwb.get_free_port()
            httpd = cs.rwb.ThreadedHTTPServer(("127.0.0.1", port), cs.rwb.BaselineFileS3Handler)
            t = threading.Thread(target=httpd.serve_forever, daemon=True)
            t.start()
            os.makedirs(os.path.join(tmpdir, cs.BUCKET), exist_ok=True)
            try:
                prefill_bytes = 2 * cs.MIB
                patterns = ["seq-write", "rand-write", "seq-read", "rand-read"]
                sizes = [4 * cs.KIB, cs.MIB]
                err = cs._prefill_server("127.0.0.1", port, cs.BUCKET, seed=42, prefill_bytes=prefill_bytes,
                                         patterns=patterns, sizes=sizes, timeout_sec=20.0)
                self.assertIsNone(err)
                # Verify deterministic prefill file exists on disk
                large_path = os.path.join(tmpdir, cs.BUCKET, "prefill_large", "seq_obj.bin")
                self.assertTrue(os.path.isfile(large_path))
                self.assertEqual(os.path.getsize(large_path), prefill_bytes)

                for pat in patterns:
                    for sz in sizes:
                        pt = cs._run_pattern_phase_with_cpu(
                            "127.0.0.1", port, cs.BUCKET, pat, sz, concurrency=2, duration_sec=0.08,
                            seed=42, prefill_bytes=prefill_bytes, server_pid=os.getpid(), shares_cores=True,
                        )
                        self.assertTrue(pt.measured, f"{pat} {sz} failed: {pt.reason}")
                        self.assertGreater(pt.ops_per_sec, 0.0)
                        self.assertGreater(pt.mib_per_sec, 0.0)
            finally:
                httpd.shutdown()
                httpd.server_close()


class ThreadCpuSamplingTests(unittest.TestCase):
    def test_get_thread_name_linux_proc_comm(self):
        with tempfile.TemporaryDirectory() as proc_root:
            task_dir = os.path.join(proc_root, "123", "task", "456")
            os.makedirs(task_dir, exist_ok=True)
            with open(os.path.join(task_dir, "comm"), "w", encoding="utf-8") as fh:
                fh.write("tokio-runtime-w\n")
            name = cs.get_thread_name(123, 456, platform_name="linux", proc_root=proc_root)
            self.assertEqual(name, "tokio-runtime-w")
            self.assertEqual(cs.get_thread_name(123, 999, platform_name="linux", proc_root=proc_root), "")

    def test_get_thread_name_psutil_attr_and_windows_description(self):
        ps_named = types.SimpleNamespace(id=11, user_time=0.1, system_time=0.0, name="ps-worker")
        self.assertEqual(cs.get_thread_name(1, 11, platform_name="darwin", psutil_thread=ps_named), "ps-worker")

        if sys.platform == "win32":
            import ctypes
            from ctypes import wintypes
            import threading

            k32 = ctypes.WinDLL("kernel32", use_last_error=True)
            k32.GetCurrentThread.restype = wintypes.HANDLE
            k32.GetCurrentThreadId.restype = wintypes.DWORD
            k32.SetThreadDescription.restype = ctypes.c_long
            k32.SetThreadDescription.argtypes = [wintypes.HANDLE, wintypes.LPCWSTR]

            ready = threading.Event()
            done = threading.Event()
            seen_tid = []

            def worker():
                k32.SetThreadDescription(k32.GetCurrentThread(), "awanfs-test-worker")
                seen_tid.append(int(k32.GetCurrentThreadId()))
                ready.set()
                done.wait(timeout=5.0)

            t = threading.Thread(target=worker, daemon=True)
            t.start()
            self.assertTrue(ready.wait(timeout=5.0))
            try:
                self.assertEqual(
                    cs.get_thread_name(os.getpid(), seen_tid[0], platform_name="win32"),
                    "awanfs-test-worker",
                )
            finally:
                done.set()
                t.join(timeout=5.0)

    def test_collect_thread_samples_handles_deltas_and_mid_phase_exit(self):
        before = {10: (1.0, 0.5, "tokio-worker-0"), 20: (0.2, 0.1, "awanfs-log")}
        last_seen = {
            10: (1.5, 0.7),
            20: (0.2, 0.1),
            30: (0.4, 0.2),
        }
        after = {10: (2.0, 1.0, "tokio-worker-0"), 20: (0.2, 0.1, "awanfs-log")}
        names = {10: "tokio-worker-0", 20: "awanfs-log", 30: "spawn-blocking"}

        samples = cs._collect_thread_samples(before, last_seen, after, names)
        self.assertEqual([s.tid for s in samples], [10, 30, 20])
        self.assertAlmostEqual(samples[0].user_s, 1.0)
        self.assertAlmostEqual(samples[0].system_s, 0.5)
        self.assertAlmostEqual(samples[0].total_s, 1.5)
        self.assertEqual(samples[1].name, "spawn-blocking")
        self.assertAlmostEqual(samples[1].total_s, 0.6)
        self.assertAlmostEqual(samples[2].total_s, 0.0)

    def test_thread_cpu_sampler_polls_psutil_threads(self):
        snapshots = [
            [types.SimpleNamespace(id=100, user_time=0.5, system_time=0.1)],
            [
                types.SimpleNamespace(id=100, user_time=0.9, system_time=0.2),
                types.SimpleNamespace(id=200, user_time=0.3, system_time=0.1),
            ],
            [types.SimpleNamespace(id=100, user_time=1.2, system_time=0.3)],
        ]
        idx = {"n": 0}

        class FakeProc:
            def __init__(self, pid):
                self.pid = pid

            def threads(self):
                i = min(idx["n"], len(snapshots) - 1)
                idx["n"] += 1
                return snapshots[i]

        fake_psutil = types.SimpleNamespace(Process=FakeProc, NoSuchProcess=Exception, AccessDenied=Exception)
        with mock.patch.dict(sys.modules, {"psutil": fake_psutil}), \
             mock.patch.object(cs, "get_thread_name", side_effect=lambda pid, tid, **kw: f"thr-{tid}"):
            sampler = cs.ThreadCpuSampler(999, every=0.01)
            with sampler:
                import time
                time.sleep(0.05)
            samples = sampler.samples()

        by_tid = {s.tid: s for s in samples}
        self.assertIn(100, by_tid)
        self.assertIn(200, by_tid)
        self.assertAlmostEqual(by_tid[100].user_s, 0.7)
        self.assertAlmostEqual(by_tid[100].system_s, 0.2)
        self.assertEqual(by_tid[200].name, "thr-200")
        self.assertAlmostEqual(by_tid[200].total_s, 0.4)

    def test_aggregate_runs_carries_representative_thread_cpu(self):
        t1 = (cs.ThreadCpuSample(tid=101, name="w0", user_s=0.5, system_s=0.1),)
        t2 = (cs.ThreadCpuSample(tid=202, name="w0", user_s=1.0, system_s=0.2),)
        t3 = (cs.ThreadCpuSample(tid=303, name="w0", user_s=0.8, system_s=0.1),)
        agg = cs.aggregate_runs([
            cs.PointResult(ops_per_sec=1000.0, p50_ms=2.0, p99_ms=4.0, cpu_pct=90.0, thread_cpu=t1, wall_sec=2.0),
            cs.PointResult(ops_per_sec=3000.0, p50_ms=1.0, p99_ms=2.0, cpu_pct=110.0, thread_cpu=t2, wall_sec=2.0),
            cs.PointResult(ops_per_sec=2000.0, p50_ms=1.5, p99_ms=3.0, cpu_pct=100.0, thread_cpu=t3, wall_sec=2.1),
        ])
        self.assertAlmostEqual(agg.ops_per_sec, 2000.0)
        self.assertEqual(agg.thread_cpu, t3)
        self.assertAlmostEqual(agg.wall_sec, 2.1)


class DurabilityAndAutoProfileTests(unittest.TestCase):
    def test_start_awanfs_passes_durability_and_validates(self):
        spawned_argvs = []

        class FakeProc:
            pid = 4242

            def poll(self):
                return None

            def terminate(self):
                pass

            def wait(self, timeout=None):
                return 0

        class FakeResp:
            status = 200

            def read(self):
                return b""

        class FakeConn:
            def __init__(self, *args, **kwargs):
                pass

            def request(self, *args, **kwargs):
                pass

            def getresponse(self):
                return FakeResp()

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as tmpdir:
            fake_bin = os.path.join(tmpdir, "s3-server")
            with open(fake_bin, "wb") as fh:
                fh.write(b"bin")
            with mock.patch.object(cs, "_spawn_pinned", side_effect=lambda argv, n, lp: spawned_argvs.append(list(argv)) or FakeProc()), \
                 mock.patch.object(cs, "_wait_port", return_value=None), \
                 mock.patch.object(cs.http.client, "HTTPConnection", FakeConn):
                proc, _port, data_dir, _log = cs._start_awanfs(fake_bin, 4, tmpdir, durability="fast")
                self.assertEqual(proc.pid, 4242)
                cs.shutil.rmtree(data_dir, ignore_errors=True)

            self.assertEqual(len(spawned_argvs), 1)
            idx = spawned_argvs[0].index("--durability")
            self.assertEqual(spawned_argvs[0][idx + 1], "fast")

            with self.assertRaises(ValueError):
                cs._start_awanfs(fake_bin, 4, tmpdir, durability="bogus")

    def test_should_auto_profile_put_requires_16_plus_split_client_cores(self):
        self.assertTrue(cs.should_auto_profile_put(16, [1, 2, 4, 8]))
        self.assertFalse(cs.should_auto_profile_put(4, [1, 2, 4]))
        self.assertFalse(cs.should_auto_profile_put(8, [1, 2, 4, 8]))
        self.assertFalse(cs.should_auto_profile_put(16, [1, 2]))

    def test_fetch_verified_binary_and_ensure_minio_binaries(self):
        import hashlib

        payload = b"fake-minio-binary-bytes"
        good_sha = hashlib.sha256(payload).hexdigest()

        class FakeResponse:
            def __init__(self, data):
                self._data = data
                self._pos = 0

            def read(self, n=-1):
                if n < 0:
                    out = self._data[self._pos:]
                    self._pos = len(self._data)
                    return out
                out = self._data[self._pos:self._pos + n]
                self._pos += len(out)
                return out

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        with tempfile.TemporaryDirectory() as tmpdir:
            dest = os.path.join(tmpdir, "minio.exe")
            with mock.patch("urllib.request.urlopen", return_value=FakeResponse(payload)):
                out = cs.fetch_verified_binary("https://example.invalid/minio.exe", good_sha, dest)
            self.assertEqual(out, dest)
            self.assertTrue(os.path.isfile(dest))
            with open(dest, "rb") as fh:
                self.assertEqual(fh.read(), payload)

            bad_dest = os.path.join(tmpdir, "bad.exe")
            with mock.patch("urllib.request.urlopen", return_value=FakeResponse(payload)):
                with self.assertRaises(RuntimeError):
                    cs.fetch_verified_binary("https://example.invalid/bad.exe", "0" * 64, bad_dest)
            self.assertFalse(os.path.exists(bad_dest))

            # ensure_minio_binaries returns existing binaries without calling fetch_verified_binary
            mc_existing = os.path.join(tmpdir, "mc.exe")
            with open(mc_existing, "wb") as fh:
                fh.write(b"mc")
            with mock.patch.object(cs, "fetch_verified_binary", side_effect=AssertionError("should not download")):
                got_m, got_mc = cs.ensure_minio_binaries(dest, mc_existing, auto_fetch=True, dest_dir=tmpdir)
            self.assertEqual((got_m, got_mc), (dest, mc_existing))


class FastAndThreadReportTests(unittest.TestCase):
    def test_report_renders_fast_row_and_per_thread_cpu_table(self):
        cores, results = _sample_results()
        results["put_4k"]["AwanFS"][4] = cs.PointResult(
            ops_per_sec=3500.0, p50_ms=1.8, p99_ms=5.0, cpu_pct=99.5, ops_per_busy_core=3517.6,
            wall_sec=2.0,
            thread_cpu=(
                cs.ThreadCpuSample(tid=1001, name="tokio-runtime-worker", user_s=0.45, system_s=1.40),
                cs.ThreadCpuSample(tid=1002, name="", user_s=0.05, system_s=0.02),
            ),
        )
        fast_results = {
            1: cs.PointResult(ops_per_sec=12000.0, p50_ms=0.8, p99_ms=2.0, cpu_pct=95.0, ops_per_busy_core=12631.6),
            2: cs.PointResult(ops_per_sec=22000.0, p50_ms=0.9, p99_ms=2.1, cpu_pct=190.0, ops_per_busy_core=11578.9),
            4: cs.PointResult(
                ops_per_sec=38000.0, p50_ms=1.0, p99_ms=2.5, cpu_pct=360.0, ops_per_busy_core=10555.6,
                wall_sec=2.0,
                thread_cpu=(
                    cs.ThreadCpuSample(tid=2001, name="tokio-runtime-worker", user_s=1.20, system_s=0.60),
                ),
            ),
        }
        cmd = "python benchmarks/core_scaling.py --cores 1,2,4 --runs 2 --duration 2"
        md = cs.render_report(
            cmd=cmd,
            cores=cores,
            results=results,
            total_cores=16,
            runs=2,
            duration_sec=2.0,
            max_put_ops={4096: 12000.0, 65536: 8000.0},
            chart_paths=[],
            fast_results=fast_results,
        )
        self.assertIn("| PUT 4 KiB | 4 KiB | AwanFS (--durability fast) |", md)
        self.assertIn("360.0%", md)
        self.assertIn("## Per-Thread CPU Breakdown", md)
        self.assertIn("| group | 4 | 1001 | tokio-runtime-worker | 0.450 | 1.400 | 1.850 |", md)
        self.assertIn("| group | 4 | 1002 | (unnamed) | 0.050 | 0.020 | 0.070 |", md)
        self.assertIn("| fast | 4 | 2001 | tokio-runtime-worker | 1.200 | 0.600 | 1.800 |", md)

    def test_matrix_report_renders_fast_row_and_per_thread_cpu_table(self):
        patterns, sizes, cores, concs, results = _sample_matrix_results()
        results["seq-write"][4096]["AwanFS"][4][64] = cs.PointResult(
            ops_per_sec=3500.0, mib_per_sec=13.67, p50_ms=1.8, p99_ms=5.0, cpu_pct=99.5,
            wall_sec=2.0,
            thread_cpu=(cs.ThreadCpuSample(tid=3001, name="tokio-runtime-worker", user_s=0.50, system_s=1.40),),
        )
        fast_matrix = {
            4: {
                64: cs.PointResult(
                    ops_per_sec=38000.0, mib_per_sec=148.44, p50_ms=1.0, p99_ms=2.5, cpu_pct=360.0,
                    wall_sec=2.0,
                    thread_cpu=(cs.ThreadCpuSample(tid=4001, name="tokio-runtime-worker", user_s=1.10, system_s=0.70),),
                )
            }
        }
        cmd = "python benchmarks/core_scaling.py --matrix --cores 1,2,4"
        md = cs.render_matrix_report(
            cmd=cmd,
            patterns=patterns,
            sizes=sizes,
            cores=cores,
            concurrencies=concs,
            results=results,
            total_cores=16,
            runs=2,
            duration_sec=2.0,
            seed=42,
            memory_budget="32MiB",
            prefill_bytes=48 * 1024 * 1024,
            chart_paths=[],
            fast_results=fast_matrix,
        )
        self.assertIn("| AwanFS (--durability fast) | seq-write | 4 KiB | 4 | 64 |", md)
        self.assertIn("## Per-Thread CPU Breakdown", md)
        self.assertIn("| group | 4 | 3001 | tokio-runtime-worker | 0.500 | 1.400 | 1.900 |", md)
        self.assertIn("| fast | 4 | 4001 | tokio-runtime-worker | 1.100 | 0.700 | 1.800 |", md)

    def test_live_stub_phase_captures_thread_cpu_when_enabled(self):
        import threading

        with tempfile.TemporaryDirectory() as tmpdir:
            cs.rwb.BaselineFileS3Handler.storage_root = tmpdir
            port = cs.rwb.get_free_port()
            httpd = cs.rwb.ThreadedHTTPServer(("127.0.0.1", port), cs.rwb.BaselineFileS3Handler)
            t = threading.Thread(target=httpd.serve_forever, daemon=True)
            t.start()
            os.makedirs(os.path.join(tmpdir, cs.BUCKET), exist_ok=True)
            try:
                pt = cs._run_phase_with_cpu(
                    "127.0.0.1", port, cs.BUCKET, 4 * cs.KIB, 2, 0.1, "put",
                    os.getpid(), shares_cores=True, profile_threads=True,
                )
                self.assertTrue(pt.measured, pt.reason)
                self.assertIsNotNone(pt.wall_sec)
                self.assertGreater(pt.wall_sec, 0.0)
                try:
                    import psutil  # noqa: F401
                    has_psutil = True
                except ImportError:
                    has_psutil = False
                if has_psutil:
                    self.assertIsNotNone(pt.thread_cpu)
                    self.assertGreater(len(pt.thread_cpu), 0)
                    self.assertTrue(all(s.tid > 0 and s.total_s >= 0.0 for s in pt.thread_cpu))
            finally:
                httpd.shutdown()
                httpd.server_close()


if __name__ == "__main__":
    unittest.main()
