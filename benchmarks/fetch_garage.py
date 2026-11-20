"""Put the pinned Garage release binary in .tmp/garage, on supported Linux platforms.

    python benchmarks/fetch_garage.py [--tag v2.5.0]

Does nothing when the binary is already there. Verifies the downloaded binary's SHA-256 against
the pinned release checksum before placing it on disk. Upstream publishes Linux static musl
binaries only, so non-Linux platforms exit 1 with a clear reason and the benchmark marks Garage
as "not measured".
"""
import argparse
import hashlib
import json
import os
import platform
import sys
import tempfile
import urllib.request
from typing import Dict, Iterable, Optional, Tuple

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEST = os.path.join(REPO_ROOT, ".tmp", "garage")
DEFAULT_TAG = "v2.5.0"
RELEASES_INDEX_URL = "https://garagehq.deuxfleurs.fr/_releases.json"

# Upstream (garagehq.deuxfleurs.fr) publishes raw binaries without .sha256 sidecars, so the
# exact SHA-256 digests of the pinned release builds are recorded here.
PINNED_SHA256: Dict[Tuple[str, str], str] = {
    ("v2.5.0", "x86_64-unknown-linux-musl"): "2ff667286217cd73ebcb1c03af993822365000441dfe31a80cb64c5189a9dea8",
    ("v2.5.0", "aarch64-unknown-linux-musl"): "69b6040cef428b993500c17dd0d0c1240acf2f3f667b9b05ceba4b9660db4e8c",
}


def binary_name(plat: Optional[str] = None) -> str:
    p = sys.platform if plat is None else plat
    return "garage.exe" if p == "win32" else "garage"


def _normalize_tag(tag: str) -> str:
    t = tag.strip()
    return t if t.startswith("v") else f"v{t}"


def pick_target(plat: Optional[str] = None, machine: Optional[str] = None) -> Optional[str]:
    """Map OS/arch to Garage's published Linux musl target triple, or None if unsupported."""
    p = sys.platform if plat is None else plat
    if not p.startswith("linux"):
        return None
    arch = (machine if machine is not None else platform.machine()).lower()
    if arch in ("x86_64", "amd64"):
        return "x86_64-unknown-linux-musl"
    if arch in ("aarch64", "arm64"):
        return "aarch64-unknown-linux-musl"
    if arch in ("i386", "i686", "x86"):
        return "i686-unknown-linux-musl"
    if arch.startswith("armv6"):
        return "armv6l-unknown-linux-musleabihf"
    return None


def expected_sha256(tag: str, target: str) -> Optional[str]:
    return PINNED_SHA256.get((_normalize_tag(tag), target))


def pick_build(releases_index: Iterable[dict], tag: str, target: str) -> Optional[dict]:
    """Find the matching build entry in Garage's _releases.json index."""
    want_tag = _normalize_tag(tag)
    for channel in releases_index:
        for rel in channel.get("builds", []):
            ver = _normalize_tag(str(rel.get("version", "")))
            if ver != want_tag:
                continue
            for item in rel.get("builds", []):
                if item.get("platform") == target:
                    return item
    return None


def verify_sha256(data: bytes, expected_hex: str) -> None:
    actual = hashlib.sha256(data).hexdigest().lower()
    want = expected_hex.strip().lower()
    if actual != want:
        raise ValueError(f"Garage checksum mismatch: expected {want}, got {actual}")


def fetch_releases_index(url: str = RELEASES_INDEX_URL) -> list:
    with urllib.request.urlopen(url, timeout=60) as r:
        return json.load(r)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default=os.environ.get("GARAGE_TAG", DEFAULT_TAG))
    a = ap.parse_args(argv)

    target_bin = os.path.join(DEST, binary_name())
    if os.path.isfile(target_bin):
        print(f"{target_bin} is already there")
        return 0

    target_triple = pick_target()
    if target_triple is None:
        print(
            f"no official Garage binary for {sys.platform}/{platform.machine()}; "
            "upstream releases Linux static binaries only"
        )
        return 1

    want_sha = expected_sha256(a.tag, target_triple)
    if want_sha is None:
        print(f"no pinned SHA-256 checksum for Garage {a.tag} ({target_triple})")
        return 1

    try:
        index = fetch_releases_index()
    except Exception as e:  # noqa: BLE001
        print(f"failed to fetch Garage release index: {e}")
        return 1

    build = pick_build(index, a.tag, target_triple)
    if build is None or not build.get("url"):
        print(f"Garage {a.tag} has no build for {target_triple}")
        return 1

    url = build["url"]
    print(f"downloading {url}")
    try:
        with urllib.request.urlopen(url, timeout=600) as r:
            data = r.read()
        verify_sha256(data, want_sha)
    except Exception as e:  # noqa: BLE001
        print(f"Garage download/verification failed: {e}")
        return 1

    os.makedirs(DEST, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".garage_", dir=DEST)
    try:
        with os.fdopen(fd, "wb") as dst:
            dst.write(data)
        if sys.platform != "win32":
            os.chmod(tmp_path, 0o755)
        os.replace(tmp_path, target_bin)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise
    print(f"wrote {target_bin} (sha256 verified)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
