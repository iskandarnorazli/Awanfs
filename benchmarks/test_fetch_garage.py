"""Self-tests for fetch_garage.py (no network).

    python benchmarks/test_fetch_garage.py
"""
import hashlib
import io
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_garage as fg  # noqa: E402

SAMPLE_RELEASES = [
    {
        "channel": "release",
        "builds": [
            {
                "version": "v2.5.0",
                "builds": [
                    {
                        "version": "v2.5.0",
                        "platform": "aarch64-unknown-linux-musl",
                        "url": "https://garagehq.deuxfleurs.fr/_releases/v2.5.0/aarch64-unknown-linux-musl/garage",
                    },
                    {
                        "version": "v2.5.0",
                        "platform": "x86_64-unknown-linux-musl",
                        "url": "https://garagehq.deuxfleurs.fr/_releases/v2.5.0/x86_64-unknown-linux-musl/garage",
                    },
                ],
            }
        ],
    }
]


class _FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class FetchGarageTests(unittest.TestCase):
    def test_pinned_tag_and_checksums_present(self):
        self.assertEqual(fg.DEFAULT_TAG, "v2.5.0")
        for target in ("x86_64-unknown-linux-musl", "aarch64-unknown-linux-musl"):
            digest = fg.expected_sha256("v2.5.0", target)
            self.assertIsNotNone(digest)
            self.assertEqual(len(digest), 64)
            int(digest, 16)

    def test_pick_target_linux_and_unsupported_platforms(self):
        self.assertEqual(fg.pick_target("linux", "x86_64"), "x86_64-unknown-linux-musl")
        self.assertEqual(fg.pick_target("linux", "amd64"), "x86_64-unknown-linux-musl")
        self.assertEqual(fg.pick_target("linux", "aarch64"), "aarch64-unknown-linux-musl")
        self.assertEqual(fg.pick_target("linux", "arm64"), "aarch64-unknown-linux-musl")
        self.assertIsNone(fg.pick_target("win32", "AMD64"))
        self.assertIsNone(fg.pick_target("darwin", "arm64"))
        self.assertEqual(fg.binary_name("linux"), "garage")

    def test_pick_build_from_releases_index(self):
        b = fg.pick_build(SAMPLE_RELEASES, "2.5.0", "x86_64-unknown-linux-musl")
        self.assertIsNotNone(b)
        self.assertEqual(b["version"], "v2.5.0")
        self.assertIn("x86_64-unknown-linux-musl/garage", b["url"])
        self.assertIsNone(fg.pick_build(SAMPLE_RELEASES, "v9.9.9", "x86_64-unknown-linux-musl"))

    def test_verify_sha256_passes_and_mismatch_raises(self):
        payload = b"garage-binary-bytes"
        digest = hashlib.sha256(payload).hexdigest()
        fg.verify_sha256(payload, digest)
        with self.assertRaises(ValueError):
            fg.verify_sha256(payload, "0" * 64)

    def test_main_unsupported_platform_exits_1_without_network(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with (
                mock.patch.object(fg, "DEST", tmpdir),
                mock.patch.object(fg.sys, "platform", "win32"),
                mock.patch.object(fg.platform, "machine", return_value="AMD64"),
            ):
                rc = fg.main([])
            self.assertEqual(rc, 1)
            self.assertEqual(os.listdir(tmpdir), [])

    def test_main_checksum_mismatch_writes_no_file_and_exits_1(self):
        good_payload = b"valid-garage-binary"
        good_sha = hashlib.sha256(good_payload).hexdigest()
        index_bytes = json.dumps(SAMPLE_RELEASES).encode("utf-8")

        def fake_urlopen(url, timeout=60):
            if url == fg.RELEASES_INDEX_URL:
                return _FakeResponse(index_bytes)
            return _FakeResponse(b"corrupted-garage-binary")

        with tempfile.TemporaryDirectory() as tmpdir:
            with (
                mock.patch.object(fg, "DEST", tmpdir),
                mock.patch.object(fg.sys, "platform", "linux"),
                mock.patch.object(fg.platform, "machine", return_value="x86_64"),
                mock.patch.dict(fg.PINNED_SHA256, {("v2.5.0", "x86_64-unknown-linux-musl"): good_sha}),
                mock.patch.object(fg.urllib.request, "urlopen", side_effect=fake_urlopen),
            ):
                rc = fg.main([])
            self.assertEqual(rc, 1)
            self.assertFalse(os.path.exists(os.path.join(tmpdir, "garage")))

    def test_main_valid_download_verifies_and_writes_binary(self):
        good_payload = b"valid-garage-binary"
        good_sha = hashlib.sha256(good_payload).hexdigest()
        index_bytes = json.dumps(SAMPLE_RELEASES).encode("utf-8")

        def fake_urlopen(url, timeout=60):
            if url == fg.RELEASES_INDEX_URL:
                return _FakeResponse(index_bytes)
            return _FakeResponse(good_payload)

        with tempfile.TemporaryDirectory() as tmpdir:
            with (
                mock.patch.object(fg, "DEST", tmpdir),
                mock.patch.object(fg.sys, "platform", "linux"),
                mock.patch.object(fg.platform, "machine", return_value="x86_64"),
                mock.patch.dict(fg.PINNED_SHA256, {("v2.5.0", "x86_64-unknown-linux-musl"): good_sha}),
                mock.patch.object(fg.urllib.request, "urlopen", side_effect=fake_urlopen),
            ):
                rc = fg.main([])
            self.assertEqual(rc, 0)
            target = os.path.join(tmpdir, "garage")
            self.assertTrue(os.path.isfile(target))
            with open(target, "rb") as fh:
                self.assertEqual(fh.read(), good_payload)


if __name__ == "__main__":
    unittest.main()
