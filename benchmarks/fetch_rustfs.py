"""Put the pinned RustFS release binary in .tmp/rustfs, on any shell and platform.

    python benchmarks/fetch_rustfs.py [--tag 1.0.5]

Does nothing when the binary is already there. Uses only the standard library, so it runs
the same from PowerShell on a Windows laptop and from bash on a hosted runner. Exit 1 with a
reason when the release or a matching asset cannot be found; the full matrix then runs
without RustFS and says so.
"""
import argparse
import io
import json
import os
import re
import sys
import urllib.request
import zipfile

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEST = os.path.join(REPO_ROOT, ".tmp", "rustfs")
DEFAULT_TAG = "1.0.5"


def binary_name(platform: str = sys.platform) -> str:
    return "rustfs.exe" if platform == "win32" else "rustfs"


def pick_asset(names, platform: str = sys.platform):
    """The release asset for this platform: a zip for x86_64 Windows or Linux, preferring a
    non-musl build. None when nothing matches."""
    os_word = "windows" if platform == "win32" else "macos|darwin" if platform == "darwin" else "linux"
    want = re.compile(rf"({os_word}).*(x86_64|amd64).*\.zip$", re.I)
    matches = [n for n in names if want.search(n)]
    plain = [n for n in matches if "musl" not in n.lower()]
    return (plain or matches or [None])[0]


def release(tag: str) -> dict:
    last = None
    for t in (tag, f"v{tag}"):
        try:
            with urllib.request.urlopen(f"https://api.github.com/repos/rustfs/rustfs/releases/tags/{t}", timeout=60) as r:
                return json.load(r)
        except Exception as e:  # noqa: BLE001
            last = e
    raise SystemExit(f"RustFS release {tag} not found: {last}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default=os.environ.get("RUSTFS_TAG", DEFAULT_TAG))
    a = ap.parse_args(argv)
    target = os.path.join(DEST, binary_name())
    if os.path.isfile(target):
        print(f"{target} is already there")
        return 0
    rel = release(a.tag)
    assets = {x["name"]: x["browser_download_url"] for x in rel.get("assets", [])}
    name = pick_asset(list(assets))
    if name is None:
        print(f"no asset of RustFS {a.tag} matches this platform; it has: {sorted(assets)}")
        return 1
    print(f"downloading {name}")
    with urllib.request.urlopen(assets[name], timeout=600) as r:
        data = r.read()
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        inner = [n for n in z.namelist() if os.path.basename(n) == binary_name()]
        if not inner:
            print(f"{name} holds no {binary_name()}: {z.namelist()[:10]}")
            return 1
        os.makedirs(DEST, exist_ok=True)
        with z.open(inner[0]) as src, open(target, "wb") as dst:
            dst.write(src.read())
    if sys.platform != "win32":
        os.chmod(target, 0o755)
    print(f"wrote {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
