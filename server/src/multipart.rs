//! awan-storage/server/src/multipart.rs
//! S3 Multipart Upload State Machine, Extent Table & Validation Engine
//! Reference: awan-storage/ARCHITECTURE.md Section 2.C and awan-storage/API_SPECIFICATION.md Section 2.C

use md5::{Digest, Md5};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::xml::xml_escape;

pub const MIN_PART_SIZE: u64 = 5 * 1024 * 1024; // 5 MiB

static ALLOW_SMALL_PARTS: AtomicBool = AtomicBool::new(false);

pub fn set_allow_small_parts(allow: bool) {
    ALLOW_SMALL_PARTS.store(allow, Ordering::SeqCst);
}

pub fn is_allow_small_parts() -> bool {
    ALLOW_SMALL_PARTS.load(Ordering::Relaxed)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartExtent {
    pub vol_offset: u64,
    pub part_size: u64,
    pub cumulative_end: u64,
    pub ref_count: u32,
    pub raw_md5: [u8; 16],
}

#[derive(Clone, Debug)]
pub struct StagedPart {
    pub part_number: u32,
    pub size: u64,
    pub vol_offset: u64,
    pub etag_hi: u64,
    pub etag_lo: u64,
    pub etag_hex: String,
    pub raw_md5: [u8; 16],
    pub uploaded_at: u64,
}

#[derive(Clone, Debug)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub bucket: String,
    pub bucket_id: u32,
    pub key: String,
    pub initiated_at: u64,
    pub parts: BTreeMap<u32, StagedPart>,
}

pub struct MultipartManager {
    pub uploads: RwLock<BTreeMap<String, MultipartUpload>>,
    /// Completed multipart extents: (bucket_id, key) -> Vec<PartExtent>
    pub completed_extents: RwLock<BTreeMap<(u32, String), Vec<PartExtent>>>,
    pub next_upload_id: RwLock<u64>,
    pub enforce_min_part_size: AtomicBool,
}

impl MultipartManager {
    pub fn new() -> Self {
        Self {
            uploads: RwLock::new(BTreeMap::new()),
            completed_extents: RwLock::new(BTreeMap::new()),
            next_upload_id: RwLock::new(1),
            enforce_min_part_size: AtomicBool::new(false),
        }
    }

    pub fn create_upload(&self, bucket: &str, bucket_id: u32, key: &str) -> String {
        let mut id_gen = self.next_upload_id.write();
        let upload_id = format!("upload-{:016x}", *id_gen);
        *id_gen += 1;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let upload = MultipartUpload {
            upload_id: upload_id.clone(),
            bucket: bucket.to_string(),
            bucket_id,
            key: key.to_string(),
            initiated_at: now_ms,
            parts: BTreeMap::new(),
        };

        self.uploads.write().insert(upload_id.clone(), upload);
        upload_id
    }

    pub fn get_upload(&self, upload_id: &str) -> Option<MultipartUpload> {
        self.uploads.read().get(upload_id).cloned()
    }

    pub fn upload_part(
        &self,
        upload_id: &str,
        part_number: u32,
        vol_offset: u64,
        size: u64,
        raw_md5: [u8; 16],
        etag_hi: u64,
        etag_lo: u64,
        etag_hex: String,
    ) -> Result<String, &'static str> {
        if part_number < 1 || part_number > 10_000 {
            return Err("InvalidArgument");
        }

        let mut uploads = self.uploads.write();
        let upload = uploads.get_mut(upload_id).ok_or("NoSuchUpload")?;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let part = StagedPart {
            part_number,
            size,
            vol_offset,
            etag_hi,
            etag_lo,
            etag_hex: etag_hex.clone(),
            raw_md5,
            uploaded_at: now_ms,
        };

        // Atomically replace prior upload of same partNumber
        upload.parts.insert(part_number, part);
        Ok(etag_hex)
    }

    pub fn abort_upload(&self, upload_id: &str) -> Result<(), &'static str> {
        let mut uploads = self.uploads.write();
        uploads.remove(upload_id).map(|_| ()).ok_or("NoSuchUpload")
    }

    pub fn complete_upload(
        &self,
        upload_id: &str,
        declared_parts: &[(u32, String)], // (part_number, etag)
    ) -> Result<(String, u64, Vec<PartExtent>), &'static str> {
        if declared_parts.is_empty() {
            return Err("MalformedXML");
        }

        let mut uploads = self.uploads.write();
        let upload = uploads.get(upload_id).ok_or("NoSuchUpload")?;

        // 1. Verify strictly ascending part order (InvalidPartOrder)
        for i in 1..declared_parts.len() {
            if declared_parts[i].0 <= declared_parts[i - 1].0 {
                return Err("InvalidPartOrder");
            }
        }

        // 2. Verify all declared parts exist and match recorded ETag (InvalidPart)
        let mut staged_selected = Vec::with_capacity(declared_parts.len());
        for (p_num, declared_etag) in declared_parts {
            let staged = upload.parts.get(p_num).ok_or("InvalidPart")?;
            let clean_declared = declared_etag.trim_matches('"');
            if staged.etag_hex != clean_declared {
                return Err("InvalidPart");
            }
            staged_selected.push(staged.clone());
        }

        // 3. Verify min part size >= 5 MiB for all except last part (EntityTooSmall)
        let enforce_min = self.enforce_min_part_size.load(Ordering::Relaxed) && !is_allow_small_parts();
        if enforce_min {
            for staged in staged_selected.iter().take(staged_selected.len().saturating_sub(1)) {
                if staged.size < MIN_PART_SIZE {
                    return Err("EntityTooSmall");
                }
            }
        }

        // 4. Compute composite ETag: "<md5(concat(raw_md5_1..k))>-<k>"
        let mut composite_hasher = Md5::new();
        let mut extents = Vec::with_capacity(staged_selected.len());
        let mut cumulative_end = 0u64;

        for staged in &staged_selected {
            composite_hasher.update(&staged.raw_md5);
            cumulative_end += staged.size;
            extents.push(PartExtent {
                vol_offset: staged.vol_offset,
                part_size: staged.size,
                cumulative_end,
                ref_count: 1,
                raw_md5: staged.raw_md5,
            });
        }

        let composite_digest: [u8; 16] = composite_hasher.finalize().into();
        let mut composite_hex = String::with_capacity(32);
        for b in composite_digest {
            composite_hex.push_str(&format!("{:02x}", b));
        }
        let composite_etag = format!("{}-{}", composite_hex, staged_selected.len());

        // Successfully validated: now remove upload from in-progress manifest
        let upload = uploads.remove(upload_id).unwrap();

        // Store completed extents for fast zero-copy range queries
        self.completed_extents
            .write()
            .insert((upload.bucket_id, upload.key.clone()), extents.clone());

        Ok((composite_etag, cumulative_end, extents))
    }

    pub fn get_completed_extents(&self, bucket_id: u32, key: &str) -> Option<Vec<PartExtent>> {
        self.completed_extents.read().get(&(bucket_id, key.to_string())).cloned()
    }
}

impl Default for MultipartManager {
    fn default() -> Self {
        Self::new()
    }
}

pub fn initiate_multipart_upload_result_xml(bucket: &str, key: &str, upload_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
          <Bucket>{}</Bucket>\n  \
          <Key>{}</Key>\n  \
          <UploadId>{}</UploadId>\n\
        </InitiateMultipartUploadResult>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id)
    )
}

pub fn complete_multipart_upload_result_xml(
    bucket: &str,
    key: &str,
    etag: &str,
    location: &str,
) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
          <Location>{}</Location>\n  \
          <Bucket>{}</Bucket>\n  \
          <Key>{}</Key>\n  \
          <ETag>\"{}\"</ETag>\n\
        </CompleteMultipartUploadResult>",
        xml_escape(location),
        xml_escape(bucket),
        xml_escape(key),
        etag
    )
}

pub fn list_parts_result_xml(
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: &[StagedPart],
) -> String {
    let mut out = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
          <Bucket>{}</Bucket>\n  \
          <Key>{}</Key>\n  \
          <UploadId>{}</UploadId>\n  \
          <StorageClass>STANDARD</StorageClass>\n  \
          <PartNumberMarker>0</PartNumberMarker>\n  \
          <NextPartNumberMarker>0</NextPartNumberMarker>\n  \
          <MaxParts>1000</MaxParts>\n  \
          <IsTruncated>false</IsTruncated>\n",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id)
    );

    for p in parts {
        out.push_str("  <Part>\n");
        out.push_str(&format!("    <PartNumber>{}</PartNumber>\n", p.part_number));
        out.push_str("    <LastModified>2026-11-10T00:00:00.000Z</LastModified>\n");
        out.push_str(&format!("    <ETag>\"{}\"</ETag>\n", p.etag_hex));
        out.push_str(&format!("    <Size>{}</Size>\n", p.size));
        out.push_str("  </Part>\n");
    }

    out.push_str("</ListPartsResult>");
    out
}

pub fn list_multipart_uploads_result_xml(
    bucket: &str,
    uploads: &[MultipartUpload],
) -> String {
    let mut out = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
          <Bucket>{}</Bucket>\n  \
          <KeyMarker></KeyMarker>\n  \
          <UploadIdMarker></UploadIdMarker>\n  \
          <NextKeyMarker></NextKeyMarker>\n  \
          <NextUploadIdMarker></NextUploadIdMarker>\n  \
          <MaxUploads>1000</MaxUploads>\n  \
          <IsTruncated>false</IsTruncated>\n",
        xml_escape(bucket)
    );

    for u in uploads {
        out.push_str("  <Upload>\n");
        out.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&u.key)));
        out.push_str(&format!("    <UploadId>{}</UploadId>\n", xml_escape(&u.upload_id)));
        out.push_str("    <Initiator><ID>root</ID><DisplayName>root</DisplayName></Initiator>\n");
        out.push_str("    <Owner><ID>root</ID><DisplayName>root</DisplayName></Owner>\n");
        out.push_str("    <StorageClass>STANDARD</StorageClass>\n");
        out.push_str("    <Initiated>2026-11-10T00:00:00.000Z</Initiated>\n");
        out.push_str("  </Upload>\n");
    }

    out.push_str("</ListMultipartUploadsResult>");
    out
}
