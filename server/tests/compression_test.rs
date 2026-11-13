//! #51: opt-in per-bucket compression in 64 KiB blocks (owner decisions 7, 20 and 22). Gate:
//! a compressible 1 MiB object writes fewer disk bytes, random bytes write the same bytes as
//! in a bucket without compression, whole and range GETs (across block boundaries) return the
//! object's bytes, and all of it survives a restart, also one without the config file.

use reqwest::StatusCode;
use s3_server::iam::ReloadingIam;
use s3_server::{start_s3_reactor, S3ServiceState};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const CONFIG: &str = "[[buckets]]\nname = \"logs\"\ncompression = \"zstd\"\n";
const MIB: usize = 1 << 20;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_compress_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_config(dir: &Path) -> PathBuf {
    let p = dir.join("awanfs.toml");
    std::fs::write(&p, CONFIG).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    p
}

fn text(n: usize) -> Vec<u8> {
    let words: &[&[u8]] = &[b"alpha ", b"bravo ", b"charlie ", b"delta ", b"echo ", b"foxtrot\n"];
    let mut out = Vec::with_capacity(n);
    let mut i = 0usize;
    while out.len() < n {
        out.extend_from_slice(words[(i * 7 + i / 3) % words.len()]);
        i += 1;
    }
    out.truncate(n);
    out
}

fn noise(n: usize, mut seed: u64) -> Vec<u8> {
    (0..n)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as u8
        })
        .collect()
}

async fn spawn(dir: &Path, iam: Option<Arc<ReloadingIam>>) -> (String, s3_server::ServerHandle, Arc<S3ServiceState>) {
    let st = Arc::new(S3ServiceState::with_data_dir(dir));
    *st.iam.write() = iam;
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_s3_reactor(addr, Arc::clone(&st)).await.unwrap();
    (format!("http://{}", handle.local_addr), handle, st)
}

/// Disk bytes the PUT wrote.
async fn put(c: &reqwest::Client, st: &S3ServiceState, base: &str, path: &str, body: &[u8]) -> u64 {
    let before = st.work.snapshot();
    let res = c.put(format!("{}/{}", base, path)).body(body.to_vec()).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "PUT {}", path);
    st.work.snapshot().since(&before).disk_write_bytes
}

/// Whole GET, HEAD and a set of ranges, several across 64 KiB block boundaries.
async fn check_reads(c: &reqwest::Client, base: &str, path: &str, want: &[u8]) {
    let url = format!("{}/{}", base, path);
    let res = c.get(&url).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "GET {}", path);
    assert!(res.bytes().await.unwrap() == want, "GET {} returns the object", path);
    let res = c.head(&url).send().await.unwrap();
    assert_eq!(res.headers()["content-length"].to_str().unwrap(), want.len().to_string(), "HEAD {}", path);

    let n = want.len() as u64;
    let b = 64 * 1024u64;
    let mut ranges = vec![(0, 0), (b - 1, b), (b - 5, b + 5), (2 * b - 1, 4 * b), (n - 6, n - 1), (b, 3 * b - 1), (0, n - 1)];
    let mut seed = 0x2545F4914F6CDD1Du64;
    for _ in 0..40 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let s = (seed >> 33) % n;
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ranges.push((s, (s + (seed >> 33) % (3 * b)).min(n - 1)));
    }
    for (s, e) in ranges {
        let res = c.get(&url).header("Range", format!("bytes={}-{}", s, e)).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::PARTIAL_CONTENT, "{} bytes={}-{}", path, s, e);
        assert_eq!(res.headers()["content-range"].to_str().unwrap(), format!("bytes {}-{}/{}", s, e, n));
        assert!(res.bytes().await.unwrap() == want[s as usize..=e as usize], "{} bytes={}-{}", path, s, e);
    }
    let res = c.get(&url).header("Range", "bytes=-100").send().await.unwrap();
    assert!(res.bytes().await.unwrap() == want[want.len() - 100..], "{} suffix range", path);
}

#[tokio::test]
async fn compressible_objects_shrink_on_disk_and_read_back_whole_and_by_range() {
    let dir = temp_dir("gate");
    let iam = ReloadingIam::open(&write_config(&temp_dir("gate_cfg"))).unwrap();
    let c = reqwest::Client::new();
    let (t, r) = (text(MIB), noise(MIB, 42));
    {
        let (base, handle, st) = spawn(&dir, Some(Arc::clone(&iam))).await;
        for b in ["logs", "plain"] {
            assert_eq!(c.put(format!("{}/{}", base, b)).send().await.unwrap().status(), StatusCode::OK);
        }
        let t_logs = put(&c, &st, &base, "logs/t", &t).await;
        let t_plain = put(&c, &st, &base, "plain/t", &t).await;
        let r_logs = put(&c, &st, &base, "logs/r", &r).await;
        let r_plain = put(&c, &st, &base, "plain/r", &r).await;
        assert!(t_plain >= MIB as u64, "uncompressed: {} bytes", t_plain);
        assert!(t_logs * 8 < t_plain, "a compressible 1 MiB object: {} bytes compressed, {} plain", t_logs, t_plain);
        assert_eq!(r_logs, r_plain, "random bytes are stored as they are");

        // A copy into a compressed bucket is compressed too.
        let res = c.put(format!("{}/logs/copy", base)).header("x-amz-copy-source", "/plain/t").send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        for p in ["logs/t", "plain/t", "logs/copy"] {
            check_reads(&c, &base, p, &t).await;
        }
        check_reads(&c, &base, "logs/r", &r).await;
        handle.shutdown();
    }
    // Reads need no config: the needle says it is compressed, and the index row its size.
    {
        let (base, handle, _) = spawn(&dir, None).await;
        for p in ["logs/t", "plain/t", "logs/copy"] {
            check_reads(&c, &base, p, &t).await;
        }
        check_reads(&c, &base, "logs/r", &r).await;
        handle.shutdown();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// Volume 1 is a 32 GiB sparse file: touch only its superblock.
fn superblock_version(path: &Path) -> u32 {
    use std::io::Read;
    let mut b = [0u8; 24];
    std::fs::File::open(path).unwrap().read_exact(&mut b).unwrap();
    u32::from_le_bytes(b[20..24].try_into().unwrap())
}

fn set_superblock_version(path: &Path, v: u32) {
    use std::io::{Seek, SeekFrom, Write};
    let mut f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(20)).unwrap();
    f.write_all(&v.to_le_bytes()).unwrap();
    f.sync_all().unwrap();
}

#[tokio::test]
async fn a_compressed_object_never_lands_in_a_format_1_volume() {
    let dir = temp_dir("v1");
    let c = reqwest::Client::new();
    let t = text(MIB);
    {
        let (base, handle, _) = spawn(&dir, None).await;
        assert_eq!(c.put(format!("{}/logs", base)).send().await.unwrap().status(), StatusCode::OK);
        assert_eq!(c.put(format!("{}/logs/old", base)).body(b"written before".to_vec()).send().await.unwrap().status(), StatusCode::OK);
        handle.shutdown();
    }
    // Make volume 1 a volume written by format 1 AwanFS (the CRC covers bytes 0..16 only).
    let v1 = dir.join("volume_0001.dat");
    assert_eq!(superblock_version(&v1), 2, "new volumes are format 2");
    set_superblock_version(&v1, 1);

    let iam = ReloadingIam::open(&write_config(&temp_dir("v1_cfg"))).unwrap();
    {
        let (base, handle, _) = spawn(&dir, Some(iam)).await;
        let res = c.put(format!("{}/logs/t", base)).body(t.clone()).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(superblock_version(&v1), 1, "the format 1 volume is not rewritten");
        let v2 = dir.join("volume_0002.dat");
        assert!(v2.exists(), "the compressed object went to a new volume");
        assert_eq!(superblock_version(&v2), 2);
        check_reads(&c, &base, "logs/t", &t).await;
        handle.shutdown();
    }
    {
        let (base, handle, _) = spawn(&dir, None).await;
        check_reads(&c, &base, "logs/t", &t).await;
        let res = c.get(format!("{}/logs/old", base)).send().await.unwrap();
        assert_eq!(res.bytes().await.unwrap(), &b"written before"[..]);
        handle.shutdown();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_config_accepts_zstd_and_none_and_refuses_anything_else() {
    use s3_server::iam::Iam;
    let iam = Iam::parse("[[buckets]]\nname = \"a\"\ncompression = \"zstd\"\n[[buckets]]\nname = \"b\"\ncompression = \"none\"\n").unwrap();
    assert!(iam.compresses("a"));
    assert!(!iam.compresses("b"));
    assert!(!iam.compresses("c"));
    let err = Iam::parse("[[buckets]]\nname = \"a\"\ncompression = \"lz4\"\n").unwrap_err();
    assert!(err.contains("lz4"), "{}", err);
    assert!(Iam::parse("[[buckets]]\nname = \"a\"\nlevel = 3\n").is_err(), "unknown keys are refused");
}
