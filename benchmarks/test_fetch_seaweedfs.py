"""Self-tests for fetch_seaweedfs.py (no network).

    python benchmarks/test_fetch_seaweedfs.py
"""
import hashlib
import io
import json
import os
import sys
import tarfile
import tempfile
import unittest
import zipfile
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_seaweedfs as fs  # noqa: E402

NAMES = [
    "darwin_amd64.tar.gz",
    "darwin_arm64.tar.gz",
    "linux_amd64.tar.gz",
    "linux_amd64_full.tar.gz",
    "linux_amd64_large_disk.tar.gz",
    "linux_arm64.tar.gz",
    "weed-volume_linux_amd64.tar.gz",
    "weed-volume_windows_amd64.zip",
    "windows_amd64.zip",
    "windows_amd64.zip.md5",
    "windows_amd64_large_disk.zip",
]


class _FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _make_zip_bytes(filename: str, payload: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(filename, payload)
    return buf.getvalue()


def _make_targz_bytes(filename: str, payload: bytes) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        info = tarfile.TarInfo(name=filename)
        info.size = len(payload)
        tf.addfile(info, io.BytesIO(payload))
    return buf.getvalue()


class FetchSeaweedfsTests(unittest.TestCase):
    def test_pinned_tag_and_checksums_present(self):
        self.assertEqual(fs.DEFAULT_TAG, "4.54")
        for asset in ("linux_amd64.tar.gz", "linux_arm64.tar.gz", "windows_amd64.zip"):
            digest = fs.expected_sha256("4.54", asset)
            self.assertIsNotNone(digest)
            self.assertEqual(len(digest), 64)
            int(digest, 16)

    def test_pick_asset_for_platforms(self):
        self.assertEqual(fs.pick_asset(NAMES, "win32", "AMD64"), "windows_amd64.zip")
        self.assertEqual(fs.binary_name("win32"), "weed.exe")
        self.assertEqual(fs.pick_asset(NAMES, "linux", "x86_64"), "linux_amd64.tar.gz")
        self.assertEqual(fs.pick_asset(NAMES, "linux", "aarch64"), "linux_arm64.tar.gz")
        self.assertEqual(fs.binary_name("linux"), "weed")
        self.assertEqual(fs.pick_asset(NAMES, "darwin", "arm64"), "darwin_arm64.tar.gz")
        self.assertIsNone(fs.pick_asset(["windows_amd64.zip"], "linux", "x86_64"))
        self.assertIsNone(fs.pick_asset(["windows_amd64.zip.md5"], "win32", "AMD64"))

    def test_verify_sha256_passes_and_mismatch_raises(self):
        payload = b"seaweed-archive"
        digest = hashlib.sha256(payload).hexdigest()
        fs.verify_sha256(payload, digest)
        with self.assertRaises(ValueError):
            fs.verify_sha256(payload, "f" * 64)

    def test_main_checksum_mismatch_writes_no_file_and_exits_1(self):
        good_zip = _make_zip_bytes("weed.exe", b"good-weed")
        good_sha = hashlib.sha256(good_zip).hexdigest()
        rel_json = json.dumps({
            "tag_name": "4.54",
            "assets": [
                {
                    "name": "windows_amd64.zip",
                    "browser_download_url": "https://example.invalid/windows_amd64.zip",
                    "digest": f"sha256:{good_sha}",
                }
            ],
        }).encode("utf-8")

        def fake_urlopen(url, timeout=60):
            u = getattr(url, "full_url", url)
            if "api.github.com" in u:
                return _FakeResponse(rel_json)
            return _FakeResponse(b"corrupt-zip")

        with tempfile.TemporaryDirectory() as tmpdir:
            with (
                mock.patch.object(fs, "DEST", tmpdir),
                mock.patch.object(fs.sys, "platform", "win32"),
                mock.patch.object(fs.platform, "machine", return_value="AMD64"),
                mock.patch.dict(fs.PINNED_SHA256, {("4.54", "windows_amd64.zip"): good_sha}),
                mock.patch.object(fs.urllib.request, "urlopen", side_effect=fake_urlopen),
            ):
                rc = fs.main([])
            self.assertEqual(rc, 1)
            self.assertFalse(os.path.exists(os.path.join(tmpdir, "weed.exe")))

    def test_main_extracts_zip_on_windows_and_targz_on_linux(self):
        win_zip = _make_zip_bytes("weed.exe", b"win-weed-bytes")
        win_sha = hashlib.sha256(win_zip).hexdigest()
        win_rel = json.dumps({
            "tag_name": "4.54",
            "assets": [
                {
                    "name": "windows_amd64.zip",
                    "browser_download_url": "https://example.invalid/windows_amd64.zip",
                    "digest": f"sha256:{win_sha}",
                }
            ],
        }).encode("utf-8")

        with tempfile.TemporaryDirectory() as tmpdir:
            with (
                mock.patch.object(fs, "DEST", tmpdir),
                mock.patch.object(fs.sys, "platform", "win32"),
                mock.patch.object(fs.platform, "machine", return_value="AMD64"),
                mock.patch.dict(fs.PINNED_SHA256, {("4.54", "windows_amd64.zip"): win_sha}),
                mock.patch.object(
                    fs.urllib.request,
                    "urlopen",
                    side_effect=lambda u, timeout=60: _FakeResponse(
                        win_rel if "api.github.com" in getattr(u, "full_url", u) else win_zip
                    ),
                ),
            ):
                rc = fs.main([])
            self.assertEqual(rc, 0)
            with open(os.path.join(tmpdir, "weed.exe"), "rb") as fh:
                self.assertEqual(fh.read(), b"win-weed-bytes")

        lin_tgz = _make_targz_bytes("weed", b"linux-weed-bytes")
        lin_sha = hashlib.sha256(lin_tgz).hexdigest()
        lin_rel = json.dumps({
            "tag_name": "4.54",
            "assets": [
                {
                    "name": "linux_amd64.tar.gz",
                    "browser_download_url": "https://example.invalid/linux_amd64.tar.gz",
                    "digest": f"sha256:{lin_sha}",
                }
            ],
        }).encode("utf-8")

        with tempfile.TemporaryDirectory() as tmpdir:
            with (
                mock.patch.object(fs, "DEST", tmpdir),
                mock.patch.object(fs.sys, "platform", "linux"),
                mock.patch.object(fs.platform, "machine", return_value="x86_64"),
                mock.patch.dict(fs.PINNED_SHA256, {("4.54", "linux_amd64.tar.gz"): lin_sha}),
                mock.patch.object(
                    fs.urllib.request,
                    "urlopen",
                    side_effect=lambda u, timeout=60: _FakeResponse(
                        lin_rel if "api.github.com" in getattr(u, "full_url", u) else lin_tgz
                    ),
                ),
            ):
                rc = fs.main([])
            self.assertEqual(rc, 0)
            with open(os.path.join(tmpdir, "weed"), "rb") as fh:
                self.assertEqual(fh.read(), b"linux-weed-bytes")


if __name__ == "__main__":
    unittest.main()
