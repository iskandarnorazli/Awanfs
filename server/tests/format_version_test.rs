//! #50: on-disk format version. A volume written before the version field existed (built here
//! byte by byte from the documented layout, FORMAT.md) still opens and serves; a newer format is
//! refused with an error naming both versions; so is a newer multipart manifest.

use s3_server::{decode_extents_manifest, ManifestError, S3ServiceState, AWAN_FLAG_ACTIVE, FORMAT_VERSION, NEEDLE_MAGIC, SUPERBLOCK_MAGIC};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("awanfs_fmt_{}_{}_{}", name, std::process::id(), nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Superblock and one needle exactly as format 1 wrote them before #50 (bytes 20..24 zero).
fn write_legacy_volume(dir: &Path, version_field: u32, key: &str, payload: &[u8]) {
    let mut vol = vec![0u8; 4096];
    vol[0..4].copy_from_slice(&SUPERBLOCK_MAGIC.to_le_bytes());
    vol[4..8].copy_from_slice(&1u32.to_le_bytes());
    vol[8..16].copy_from_slice(&1_700_000_000_000_000_000u64.to_le_bytes());
    let crc = crc32c::crc32c(&vol[0..16]);
    vol[16..20].copy_from_slice(&crc.to_le_bytes());
    vol[20..24].copy_from_slice(&version_field.to_le_bytes());

    let mut h = [0u8; 64];
    h[0..4].copy_from_slice(&NEEDLE_MAGIC.to_le_bytes());
    h[8..12].copy_from_slice(&crc32c::crc32c(payload).to_le_bytes());
    h[12..14].copy_from_slice(&AWAN_FLAG_ACTIVE.to_le_bytes());
    h[14..16].copy_from_slice(&1u16.to_le_bytes());
    h[16..18].copy_from_slice(&(key.len() as u16).to_le_bytes());
    h[20..28].copy_from_slice(&xxhash_rust::xxh3::xxh3_64(key.as_bytes()).to_le_bytes());
    h[28..36].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    h[36..44].copy_from_slice(&1_700_000_000_000_000_001u64.to_le_bytes());
    let header_crc = crc32c::crc32c(&h[8..64]);
    h[4..8].copy_from_slice(&header_crc.to_le_bytes());
    let mut needle = h.to_vec();
    needle.extend_from_slice(key.as_bytes());
    needle.extend_from_slice(payload);
    needle.resize(needle.len().div_ceil(4096) * 4096, 0);
    vol.extend_from_slice(&needle);
    std::fs::write(dir.join("volume_0001.dat"), vol).unwrap();
    std::fs::write(dir.join("buckets.manifest"), "+1:legacy-bkt\n").unwrap();
}

#[test]
fn a_volume_from_before_the_version_field_still_serves_its_objects() {
    let dir = temp_dir("legacy");
    write_legacy_volume(&dir, 0, "old.txt", b"written by format 1 before #50");
    let state = S3ServiceState::try_with_data_dir(&dir).expect("format 1 opens");
    let b_id = state.engine.read().lookup_bucket("legacy-bkt").expect("bucket restored");
    let meta = state.engine.read().get_object(b_id, b"old.txt").expect("object replayed");
    let bytes = state.load_payload(b_id, meta.vol_offset, meta.size as usize).unwrap();
    assert_eq!(&bytes[..], b"written by format 1 before #50");
}

#[test]
fn a_newer_volume_format_is_refused_naming_both_versions() {
    let dir = temp_dir("newer");
    write_legacy_volume(&dir, FORMAT_VERSION + 1, "k", b"v");
    let err = S3ServiceState::try_with_data_dir(&dir).err().expect("refused");
    assert!(err.contains(&(FORMAT_VERSION + 1).to_string()) && err.contains(&FORMAT_VERSION.to_string()), "{}", err);
    assert!(err.contains("newer"), "{}", err);
}

#[test]
fn the_binary_exits_2_on_a_newer_format() {
    let dir = temp_dir("binary");
    write_legacy_volume(&dir, FORMAT_VERSION + 1, "k", b"v");
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_s3-server"))
        .args(["--port", "0", "--allow-anonymous", "--data-dir", dir.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("newer"));
}

#[test]
fn manifests_carry_their_version() {
    let dir = temp_dir("manifest");
    let m = s3_server::encode_extents_manifest(&dir, 0, &[]);
    assert!(m.starts_with(b"AWNMPU1\n"), "version 1 manifests keep their first line");
    assert_eq!(decode_extents_manifest(&dir, &m).map(|(t, e)| (t, e.len())), Ok((0, 0)));
    assert_eq!(
        decode_extents_manifest(&dir, b"AWNMPU2\n0\n").err(),
        Some(ManifestError::NewerVersion { found: 2, supported: 1 })
    );
    assert_eq!(decode_extents_manifest(&dir, b"garbage").err(), Some(ManifestError::Malformed));
}
