"""Self-tests for fetch_rustfs.py (no network).

    python benchmarks/test_fetch_rustfs.py
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_rustfs as f  # noqa: E402

NAMES = [
    "rustfs-linux-aarch64-musl-v1.0.5.zip",
    "rustfs-linux-x86_64-musl-v1.0.5.zip",
    "rustfs-linux-x86_64-gnu-v1.0.5.zip",
    "rustfs-windows-x86_64-v1.0.5.zip",
    "rustfs-macos-aarch64-v1.0.5.zip",
    "rustfs-windows-x86_64-v1.0.5.zip.sha256sum",
]


class Pick(unittest.TestCase):
    def test_windows(self):
        self.assertEqual(f.pick_asset(NAMES, "win32"), "rustfs-windows-x86_64-v1.0.5.zip")
        self.assertEqual(f.binary_name("win32"), "rustfs.exe")

    def test_linux_prefers_the_non_musl_build(self):
        self.assertEqual(f.pick_asset(NAMES, "linux"), "rustfs-linux-x86_64-gnu-v1.0.5.zip")
        self.assertEqual(f.pick_asset([n for n in NAMES if "gnu" not in n], "linux"), "rustfs-linux-x86_64-musl-v1.0.5.zip")
        self.assertEqual(f.binary_name("linux"), "rustfs")

    def test_nothing_for_this_platform(self):
        self.assertIsNone(f.pick_asset(["rustfs-macos-aarch64-v1.0.5.zip"], "win32"))
        self.assertIsNone(f.pick_asset([], "linux"))

    def test_a_checksum_file_is_not_the_asset(self):
        self.assertIsNone(f.pick_asset(["rustfs-windows-x86_64-v1.0.5.zip.sha256sum"], "win32"))


if __name__ == "__main__":
    unittest.main()
