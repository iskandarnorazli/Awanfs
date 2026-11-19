"""Put the pinned SeaweedFS release binary in .tmp/seaweedfs, on any shell and platform.

    python benchmarks/fetch_seaweedfs.py [--tag 4.54]

Does nothing when the binary is already there. Verifies the downloaded archive's SHA-256
against the pinned checksum before extracting `weed` / `weed.exe`. Exits 1 with a reason when
the release, matching asset, or checksum check fails.
"""
import argparse
import hashlib
import io
import json
import os
import platform
import sys
import tarfile
import tempfile
import urllib.request
import zipfile
from typing import Dict, Iterable, Optional, Tuple

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEST = os.path.join(REPO_ROOT, ".tmp", "seaweedfs")
DEFAULT_TAG = "4.54"

PINNED_SHA256: Dict[Tuple[str, str], str] = {
    ("4.54", "linux_amd64.tar.gz"): "e7424a82eee327809bfa449fda4924c9a0f42f6669a01288eebb1b79674c8196",
    ("4.54", "linux_arm64.tar.gz"): "e4eeae433dc6f5111034b8b1cc0c1a08b9b094b4eea101a36650ddc3a7c0ce58",
    ("4.54", "windows_amd64.zip"): "6d02c72bda207320b69236d34280d827bbedf6cef79d6c37da5b0db2299f28bc",
    ("4.54", "darwin_amd64.tar.gz"): "c6a5129a2eb4d2c4e441c6e61dec7e1f2b822dcca9fd20dbe271c395a7b47d06",
    ("4.54", "darwin_arm64.tar.gz"): "cba792274d5881fb3453291c3af53380c513e80e37aea29a2a25d20333eff325",
}


def binary_name(plat: Optional[str] = None) -> str:
    p = sys.platform if plat is None else plat
    return "weed.exe" if p == "win32" else "weed"


def _normalize_tag(tag: str) -> str:
    return tag.strip().lstrip("v")


def expected_sha256(tag: str, asset_name: str) -> Optional[str]:
    return PINNED_SHA256.get((_normalize_tag(tag), asset_name))


def pick_asset(names: Iterable[str], plat: Optional[str] = None, machine: Optional[str] = None) -> Optional[str]:
    """Choose the standard single-binary SeaweedFS release archive for this OS/arch."""
    p = sys.platform if plat is None else plat
    arch = (machine if machine is not None else platform.machine()).lower()
    if arch in ("x86_64", "amd64"):
        arch_key = "amd64"
    elif arch in ("aarch64", "arm64"):
        arch_key = "arm64"
    else:
        return None

    if p == "win32":
        want = f"windows_{arch_key}.zip"
    elif p == "darwin":
        want = f"darwin_{arch_key}.tar.gz"
    elif p.startswith("linux"):
        want = f"linux_{arch_key}.tar.gz"
    else:
        return None

    available = set(names)
    return want if want in available else None


def verify_sha256(data: bytes, expected_hex: str) -> None:
    actual = hashlib.sha256(data).hexdigest().lower()
    want = expected_hex.strip().lower()
    if actual != want:
        raise ValueError(f"SeaweedFS checksum mismatch: expected {want}, got {actual}")


def _urlopen_github(url: str, timeout: int = 60):
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if token and "api.github.com" in url:
        req = urllib.request.Request(
            url,
            headers={"Authorization": f"Bearer {token}", "User-Agent": "AwanFS-bench"},
        )
        return urllib.request.urlopen(req, timeout=timeout)
    return urllib.request.urlopen(url, timeout=timeout)


def release(tag: str) -> dict:
    last = None
    norm = _normalize_tag(tag)
    for t in (tag.strip(), norm, f"v{norm}"):
        url = f"https://api.github.com/repos/seaweedfs/seaweedfs/releases/tags/{t}"
        try:
            with _urlopen_github(url, timeout=60) as r:
                return json.load(r)
        except Exception as e:  # noqa: BLE001
            last = e
    raise SystemExit(f"SeaweedFS release {tag} not found: {last}")


def _extract_binary(archive_name: str, data: bytes, want_bin: str) -> Optional[bytes]:
    if archive_name.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            inner = [n for n in z.namelist() if os.path.basename(n) == want_bin]
            if not inner:
                return None
            with z.open(inner[0]) as src:
                return src.read()
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
        for m in tf.getmembers():
            if m.isfile() and os.path.basename(m.name) == want_bin:
                fobj = tf.extractfile(m)
                if fobj is not None:
                    return fobj.read()
    return None


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default=os.environ.get("SEAWEEDFS_TAG", DEFAULT_TAG))
    a = ap.parse_args(argv)

    want_bin = binary_name()
    target = os.path.join(DEST, want_bin)
    if os.path.isfile(target):
        print(f"{target} is already there")
        return 0

    try:
        rel = release(a.tag)
    except SystemExit as e:
        print(e)
        return 1

    assets = {x["name"]: x for x in rel.get("assets", [])}
    name = pick_asset(list(assets))
    if name is None:
        print(f"no SeaweedFS {a.tag} asset matches {sys.platform}/{platform.machine()}")
        return 1

    asset = assets[name]
    pinned = expected_sha256(a.tag, name)
    api_digest = asset.get("digest", "")
    api_sha = api_digest.split(":", 1)[1].strip() if api_digest.startswith("sha256:") else ""
    want_sha = pinned or api_sha
    if not want_sha:
        print(f"no SHA-256 checksum available for SeaweedFS {a.tag} {name}")
        return 1
    if pinned and api_sha and pinned.lower() != api_sha.lower():
        print(f"SeaweedFS {a.tag} {name} digest mismatch between pinned {pinned} and API {api_sha}")
        return 1

    print(f"downloading {name}")
    try:
        with urllib.request.urlopen(asset["browser_download_url"], timeout=600) as r:
            data = r.read()
        verify_sha256(data, want_sha)
    except Exception as e:  # noqa: BLE001
        print(f"SeaweedFS download/verification failed: {e}")
        return 1

    extracted = _extract_binary(name, data, want_bin)
    if extracted is None:
        print(f"{name} holds no {want_bin}")
        return 1

    os.makedirs(DEST, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".weed_", dir=DEST)
    try:
        with os.fdopen(fd, "wb") as dst:
            dst.write(extracted)
        if sys.platform != "win32":
            os.chmod(tmp_path, 0o755)
        os.replace(tmp_path, target)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise
    print(f"wrote {target} (sha256 verified)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
