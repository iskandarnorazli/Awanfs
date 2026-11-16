use bytes::Bytes;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG};
use hyper::{HeaderMap, Response, StatusCode};
use md5::{Digest, Md5};
use std::collections::HashMap;
use std::sync::Arc;

use crate::ffi_bridge::{AWAN_FLAG_ACTIVE, AWAN_FLAG_MULTIPART};
use crate::iam::Iam;
use crate::multipart::{
    complete_multipart_upload_result_xml, encode_extents_manifest,
    initiate_multipart_upload_result_xml, list_multipart_uploads_result_xml, list_parts_result_xml,
};
use crate::s3_api::{
    bad_request, compute_md5, drain_body, empty_body, engine_error_response, full_body,
    internal_error_response, read_body_admitted, request_timeout_response, ResponseBody, S3ServiceState, CHUNK_FRAMING_SLACK,
    MAX_OBJECT_BODY, MAX_XML_BODY, STREAM_PUT_MIN,
};
use crate::xml::s3_error_xml;

use super::object::finish_version_put;

pub async fn list_multipart_uploads(
    state: Arc<S3ServiceState>,
    bucket: &str,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let uploads_guard = state.multipart.uploads.read();
    let active: Vec<_> = uploads_guard.values().filter(|u| u.bucket == bucket).cloned().collect();
    let xml = list_multipart_uploads_result_xml(bucket, &active);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap())
}

pub async fn create_multipart_upload(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let req_cksum = match crate::checksum::requested_checksum(headers) {
        Ok(Some(c)) => Some(c),
        Ok(None) => match headers.get("x-amz-checksum-algorithm").and_then(|v| v.to_str().ok()) {
            Some(name) => match crate::checksum::ChecksumAlgorithm::from_header_name(name) {
                Some(alg) => Some((alg, None)),
                None => return Ok(bad_request("InvalidRequest", "unsupported checksum algorithm", key)),
            },
            None => None,
        },
        Err(r) => return Ok(bad_request(r.code, r.why, key)),
    };
    let cksum_type = if let Some((alg, _)) = req_cksum {
        let explicit = headers.get("x-amz-checksum-type").and_then(|v| v.to_str().ok()).map(|s| s.trim());
        match explicit {
            Some(t) if t.eq_ignore_ascii_case("FULL_OBJECT") => Some("FULL_OBJECT".to_string()),
            Some(t) if t.eq_ignore_ascii_case("COMPOSITE") => Some("COMPOSITE".to_string()),
            Some(_) => return Ok(bad_request("InvalidRequest", "unsupported checksum type", key)),
            None => Some(if alg == crate::checksum::ChecksumAlgorithm::Crc64Nvme { "FULL_OBJECT" } else { "COMPOSITE" }.to_string()),
        }
    } else {
        None
    };
    let upload_id = match state.multipart.create_upload(bucket, b_id, key) {
        Ok(id) => id,
        Err(e) => return Ok(internal_error_response(key, &format!("could not start the upload: {}", e))),
    };
    if let (Some((alg, _)), Some(ref ct)) = (&req_cksum, &cksum_type) {
        state.multipart.set_upload_checksum(&upload_id, *alg, ct.clone());
    }
    let xml = initiate_multipart_upload_result_xml(bucket, key, &upload_id);
    let mut resp = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/xml");
    if let (Some((alg, _)), Some(ref ct)) = (&req_cksum, &cksum_type) {
        resp = resp
            .header("x-amz-checksum-algorithm", alg.name())
            .header("x-amz-checksum-type", ct.as_str());
    }
    Ok(resp.body(full_body(Bytes::from(xml))).unwrap())
}

pub async fn upload_part(
    state: Arc<S3ServiceState>,
    key: &str,
    path: &str,
    query_map: &HashMap<String, String>,
    headers: &HeaderMap,
    iam: Option<Arc<Iam>>,
    body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let upload_id = &query_map["uploadId"];
    let part_num: u32 = match query_map["partNumber"].parse() {
        Ok(n) => n,
        Err(_) => {
            let xml = s3_error_xml("InvalidArgument", "Invalid partNumber", key, "req-up-part");
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        }
    };

    let is_chunked = crate::auth::is_aws_chunked(headers);
    let signed_headers = headers.clone();
    let req_checksum = match crate::checksum::requested_checksum(&signed_headers) {
        Ok(Some(c)) => Some(c),
        Ok(None) => state.multipart.get_upload(upload_id).and_then(|u| u.checksum_alg.map(|a| (a, None))),
        Err(r) => return Ok(bad_request(r.code, r.why, key)),
    };
    let declared_len = signed_headers
        .get(if is_chunked { "x-amz-decoded-content-length" } else { "content-length" })
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if let Some(len) = declared_len.filter(|&n| n > STREAM_PUT_MIN) {
        if state.multipart.get_upload(upload_id).is_none() {
            let xml = s3_error_xml("NoSuchUpload", "The specified upload does not exist.", key, "req-up-part");
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        }
        let mut writer = match state.multipart.begin_part(upload_id, part_num) {
            Ok(w) => w,
            Err(e) => return Ok(internal_error_response(key, &format!("could not store the part: {}", e))),
        };
        let declared_sha = signed_headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .filter(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()))
            .map(|h| h.to_ascii_lowercase());
        let mut sha = declared_sha.as_ref().map(|_| sha2::Sha256::new());
        let mut md5 = Md5::new();
        let mut cksum_hasher = req_checksum.as_ref().map(|(alg, _)| crate::checksum::Checksum::new(*alg));
        let mut body = body;
        let mut verifier = crate::auth::ChunkVerifier::for_request_with(&state.auth, iam.as_deref(), &signed_headers);
        let cache = &state.sigv4_cache;
        let mut decoder = is_chunked.then(|| {
            crate::aws_chunked::ChunkedDecoder::new(move |sha: &str, sig: Option<&str>| {
                verifier.as_mut().map_or(true, |v| v.check_digest(cache, sha, sig))
            })
        });
        let idle_timeout = state.body_idle_timeout();
        loop {
            let frame = match crate::body_timeout::next_frame_with_idle(&mut body, idle_timeout).await {
                Ok(Some(f)) => f,
                Ok(None) => break,
                Err(crate::body_timeout::BodyIdleTimeout) => {
                    drop(writer);
                    return Ok(request_timeout_response(key));
                }
            };
            if let Ok(data) = frame?.into_data() {
                if let Some(h) = sha.as_mut() {
                    h.update(&data);
                }
                let mut sink = |d: &[u8]| -> Result<(), String> {
                    if writer.written() + d.len() as u64 > len {
                        return Err("more bytes than the declared length".to_string());
                    }
                    md5.update(d);
                    if let Some(h) = cksum_hasher.as_mut() {
                        h.update(d);
                    }
                    writer.write(d).map_err(|e| e.to_string())
                };
                let fed = match decoder.as_mut() {
                    Some(dec) => dec.push(&data, &mut sink),
                    None => sink(&data).map_err(crate::aws_chunked::PushError::Sink),
                };
                match fed {
                    Ok(()) => {}
                    Err(crate::aws_chunked::PushError::Decode(e)) => {
                        drain_body(&mut body, 64 << 20).await;
                        let status = if e == "SignatureDoesNotMatch" {
                            StatusCode::FORBIDDEN
                        } else {
                            StatusCode::BAD_REQUEST
                        };
                        return Ok(Response::builder()
                            .status(status)
                            .header(CONTENT_TYPE, "application/xml")
                            .body(full_body(Bytes::from(s3_error_xml(e, "Error decoding chunked body", key, "req-up-part"))))
                            .unwrap());
                    }
                    Err(crate::aws_chunked::PushError::Sink(e)) if e.starts_with("more bytes") => {
                        return Ok(bad_request("IncompleteBody", "The body is longer than its declared length.", key));
                    }
                    Err(crate::aws_chunked::PushError::Sink(e)) => {
                        return Ok(internal_error_response(key, &format!("could not store the part: {}", e)));
                    }
                }
            }
        }
        if decoder.as_ref().is_some_and(|d| d.finish().is_err()) || writer.written() != len {
            return Ok(bad_request(
                "IncompleteBody",
                "You did not provide the number of bytes specified by the Content-Length HTTP header.",
                key,
            ));
        }
        if let (Some(h), Some(want)) = (sha, declared_sha) {
            let got: String = h.finalize().iter().map(|b| format!("{:02x}", b)).collect();
            if got != want {
                return Ok(bad_request(
                    "XAmzContentSHA256Mismatch",
                    "The provided 'x-amz-content-sha256' header does not match what was computed.",
                    key,
                ));
            }
        }
        let stream_cksum_out = match (req_checksum.as_ref(), cksum_hasher) {
            (Some((alg, expected_opt)), Some(h)) => {
                let got = h.finish_base64();
                if let Some(expected) = expected_opt {
                    if &got != expected {
                        return Ok(bad_request("BadDigest", "The checksum you specified did not match what we received.", key));
                    }
                }
                Some((*alg, got))
            }
            _ => None,
        };
        let raw_md5: [u8; 16] = md5.finalize().into();
        let hi = u64::from_be_bytes(raw_md5[0..8].try_into().unwrap());
        let lo = u64::from_be_bytes(raw_md5[8..16].try_into().unwrap());
        let hex: String = raw_md5.iter().map(|b| format!("{:02x}", b)).collect();
        let staged_path = match writer.finish() {
            Ok(p) => p,
            Err(e) => return Ok(internal_error_response(key, &format!("could not store the part: {}", e))),
        };
        return Ok(
            match state.multipart.upload_part(upload_id, part_num, 0, len, raw_md5, hi, lo, hex.clone(), Some(staged_path)) {
                Ok(_) => {
                    let mut b = Response::builder()
                        .status(StatusCode::OK)
                        .header(ETAG, format!("\"{}\"", hex))
                        .header(CONTENT_LENGTH, "0");
                    if let Some((alg, val)) = stream_cksum_out {
                        state.multipart.set_part_checksum(upload_id, part_num, alg, val.clone());
                        b = b.header(alg.header_name(), val);
                    }
                    b.body(empty_body()).unwrap()
                }
                Err(err) => {
                    let status = if err == "NoSuchUpload" {
                        StatusCode::NOT_FOUND
                    } else {
                        StatusCode::BAD_REQUEST
                    };
                    Response::builder()
                        .status(status)
                        .header(CONTENT_TYPE, "application/xml")
                        .body(full_body(Bytes::from(s3_error_xml(err, "Failed to upload part", key, "req-up-part"))))
                        .unwrap()
                }
            },
        );
    }
    let (raw_bytes, _body_guard) = match read_body_admitted(&state.body_budget, body, MAX_OBJECT_BODY + CHUNK_FRAMING_SLACK, declared_len, state.body_idle_timeout(), path).await? {
        Ok(b) => b,
        Err(resp) => return Ok(resp),
    };
    if !crate::auth::payload_hash_ok(&signed_headers, &raw_bytes) {
        let xml = s3_error_xml(
            "XAmzContentSHA256Mismatch",
            "The provided 'x-amz-content-sha256' header does not match what was computed.",
            key,
            "req-sha256",
        );
        return Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header(CONTENT_TYPE, "application/xml")
            .body(full_body(Bytes::from(xml)))
            .unwrap());
    }
    let decoded_bytes = if is_chunked {
        let mut verifier = crate::auth::ChunkVerifier::for_request_with(&state.auth, iam.as_deref(), &signed_headers);
        let cache = &state.sigv4_cache;
        match crate::aws_chunked::decode_aws_chunked_checked(&raw_bytes, |sha, sig| {
            verifier.as_mut().map_or(true, |v| v.check_digest(cache, sha, sig))
        }) {
            Ok(b) => b,
            Err(e) => {
                let xml = s3_error_xml(e, "Error decoding chunked body", key, "req-up-part");
                return Ok(Response::builder()
                    .status(if e == "SignatureDoesNotMatch" {
                        StatusCode::FORBIDDEN
                    } else {
                        StatusCode::BAD_REQUEST
                    })
                    .header(CONTENT_TYPE, "application/xml")
                    .body(full_body(Bytes::from(xml)))
                    .unwrap());
            }
        }
    } else {
        raw_bytes
    };

    let buf_cksum_out = if let Some((alg, expected_opt)) = req_checksum {
        let mut h = crate::checksum::Checksum::new(alg);
        h.update(&decoded_bytes);
        let got = h.finish_base64();
        if let Some(expected) = expected_opt {
            if got != expected {
                return Ok(bad_request("BadDigest", "The checksum you specified did not match what we received.", key));
            }
        }
        Some((alg, got))
    } else {
        None
    };
    let (hi, lo, hex, raw_md5) = compute_md5(&decoded_bytes);
    let staged_path = match state.multipart.write_staged_part(upload_id, part_num, &decoded_bytes) {
        Ok(p) => Some(p),
        Err(e) => return Ok(internal_error_response(key, &format!("could not store the part: {}", e))),
    };
    let vol_offset = 0;

    match state.multipart.upload_part(
        upload_id,
        part_num,
        vol_offset,
        decoded_bytes.len() as u64,
        raw_md5,
        hi,
        lo,
        hex.clone(),
        staged_path,
    ) {
        Ok(_) => {
            let mut b = Response::builder()
                .status(StatusCode::OK)
                .header(ETAG, format!("\"{}\"", hex))
                .header(CONTENT_LENGTH, "0");
            if let Some((alg, val)) = buf_cksum_out {
                state.multipart.set_part_checksum(upload_id, part_num, alg, val.clone());
                b = b.header(alg.header_name(), val);
            }
            Ok(b.body(empty_body()).unwrap())
        }
        Err(err) => {
            let xml = s3_error_xml(err, "Failed to upload part", key, "req-up-part");
            let status = if err == "NoSuchUpload" {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::BAD_REQUEST
            };
            Ok(Response::builder()
                .status(status)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
    }
}

pub async fn complete_multipart_upload(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    key: &str,
    path: &str,
    query_map: &HashMap<String, String>,
    headers: &HeaderMap,
    body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let upload_id = &query_map["uploadId"];
    let (body_bytes, _body_guard) = match read_body_admitted(&state.body_budget, body, MAX_XML_BODY, None, state.body_idle_timeout(), path).await? {
        Ok(b) => b,
        Err(resp) => return Ok(resp),
    };
    let body_str = String::from_utf8_lossy(&body_bytes);

    let mut declared_parts = Vec::new();
    let mut cursor = body_str.as_ref();
    while let Some(start) = cursor.find("<Part>") {
        let rest = &cursor[start + 6..];
        let part_end = match rest.find("</Part>") {
            Some(e) => e,
            None => break,
        };
        let part_block = &rest[..part_end];
        cursor = &rest[part_end + 7..];

        let p_num = part_block
            .find("<PartNumber>")
            .and_then(|s| {
                let r = &part_block[s + 12..];
                let e = r.find("</PartNumber>")?;
                r[..e].parse::<u32>().ok()
            })
            .unwrap_or(0);

        let etag = part_block
            .find("<ETag>")
            .and_then(|s| {
                let r = &part_block[s + 6..];
                let e = r.find("</ETag>")?;
                Some(r[..e].to_string())
            })
            .unwrap_or_default();

        let part_cksum = if let Some(upload) = state.multipart.get_upload(upload_id) {
            upload.checksum_alg.and_then(|alg| {
                let open = format!("<{}>", alg.xml_name());
                let close = format!("</{}>", alg.xml_name());
                part_block.find(&open).and_then(|s| {
                    let r = &part_block[s + open.len()..];
                    let e = r.find(&close)?;
                    Some(r[..e].trim().to_string())
                })
            })
        } else {
            None
        };
        declared_parts.push((p_num, etag, part_cksum));
    }

    let declared_pairs: Vec<(u32, String)> = declared_parts.iter().map(|(n, e, _)| (*n, e.clone())).collect();
    let upload_meta = state.multipart.get_upload(upload_id);
    match state.multipart.prepare_complete(upload_id, &declared_pairs) {
        Ok((composite_etag, total_size, extents)) => {
            let final_cksum: Option<(crate::checksum::ChecksumAlgorithm, String, String)> = if let Some(ref u) = upload_meta {
                if let Some(alg) = u.checksum_alg {
                    let cksum_type = u.checksum_type.clone().unwrap_or_else(|| "COMPOSITE".to_string());
                    let mut part_raws = Vec::with_capacity(extents.len());
                    for (idx, ext) in extents.iter().enumerate() {
                        let Some((_, ref recorded)) = ext.checksum else {
                            return Ok(bad_request("InvalidRequest", "Missing part checksum for multipart upload.", key));
                        };
                        if let Some(ref decl) = declared_parts[idx].2 {
                            if decl != recorded {
                                return Ok(bad_request("InvalidRequest", "Part checksum in CompleteMultipartUpload does not match uploaded part.", key));
                            }
                        }
                        let Some(raw) = crate::checksum::base64_decode(recorded) else {
                            return Ok(bad_request("InvalidRequest", "Invalid recorded part checksum.", key));
                        };
                        part_raws.push(raw);
                    }
                    let computed = crate::checksum::composite(alg, &part_raws);
                    if let Some(hdr_val) = headers.get(alg.header_name()).and_then(|v| v.to_str().ok()) {
                        if hdr_val.trim() != computed {
                            return Ok(bad_request("BadDigest", "The checksum you specified did not match what we received.", key));
                        }
                    }
                    Some((alg, cksum_type, computed))
                } else {
                    None
                }
            } else {
                None
            };
            let mut attrs = crate::attrs::Attrs::new();
            let part_sizes: Vec<u64> = extents.iter().map(|e| e.part_size).collect();
            attrs.set_part_sizes(&part_sizes);
            if let Some((alg, ref ct, ref val)) = final_cksum {
                attrs.set_checksum(alg, ct, val);
            }
            let attrs_bytes = attrs.encode().unwrap_or_default();
            let attrs_flag = if attrs_bytes.is_empty() { 0 } else { crate::meta_index::FLAG_ATTRS };

            let (hi, lo, _, _) = compute_md5(composite_etag.as_bytes());
            let _key_guard = state.lock_key(b_id, key).await;
            if let Err(code) = state.engine.read().has_room_for(b_id, key.as_bytes()) {
                return Ok(engine_error_response(code, key));
            }
            let manifest = Bytes::from(encode_extents_manifest(&state.multipart.staging_dir, total_size, &extents));
            let stamp = state.version_stamp(b_id, key);
            let created_ns = stamp.map(|s| s.1).unwrap_or_else(|| state.clock.now_ns());
            let mp_flags = AWAN_FLAG_ACTIVE | AWAN_FLAG_MULTIPART | attrs_flag | stamp.map_or(0, |s| s.0);
            let manifest_offset = match state
                .store_payload_with(b_id, key, &attrs_bytes, &manifest, mp_flags, hi, lo, Some(created_ns))
                .await
            {
                Ok(off) => off,
                Err(e) => return Ok(internal_error_response(key, &format!("could not record the multipart object: {:?}", e))),
            };
            state.payload_cache.remove(b_id, manifest_offset);
            if let Err(code) = state.multipart.commit_complete(upload_id, extents.clone()) {
                let xml = s3_error_xml(code, "The upload was aborted or completed while this request ran.", key, "req-complete");
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header(CONTENT_TYPE, "application/xml")
                    .body(full_body(Bytes::from(xml)))
                    .unwrap());
            }
            let mut version_id = None;
            if let Some((_, ts)) = stamp {
                match finish_version_put(&state, b_id, key, ts, mp_flags, manifest_offset, total_size, hi, lo, extents.len() as u16) {
                    Ok(v) => version_id = Some(v),
                    Err(r) => return Ok(r),
                }
            } else {
                let mut engine = state.engine.write();
                if let Err(code) = engine.put_object_at(
                    b_id,
                    key.as_bytes(),
                    manifest_offset,
                    total_size,
                    hi,
                    lo,
                    extents.len() as u16,
                    mp_flags,
                    created_ns / 1_000_000,
                ) {
                    return Ok(engine_error_response(code, key));
                }
            }

            let location = format!("http://127.0.0.1:9000/{}/{}", bucket, key);
            let xml = complete_multipart_upload_result_xml(bucket, key, &composite_etag, &location);
            let mut resp = Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/xml")
                .header(ETAG, format!("\"{}\"", composite_etag));
            if let Some(v) = version_id {
                resp = resp.header("x-amz-version-id", v);
            }
            if let Some((alg, ct, val)) = final_cksum {
                resp = resp.header(alg.header_name(), val).header("x-amz-checksum-type", ct);
            }
            Ok(resp.body(full_body(Bytes::from(xml))).unwrap())
        }
        Err(err) => {
            let xml = s3_error_xml(err, "CompleteMultipartUpload error", key, "req-cmp-part");
            let status = if err == "NoSuchUpload" {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::BAD_REQUEST
            };
            Ok(Response::builder()
                .status(status)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
    }
}

pub async fn abort_multipart_upload(
    state: Arc<S3ServiceState>,
    b_id: u32,
    key: &str,
    query_map: &HashMap<String, String>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let upload_id = &query_map["uploadId"];
    let _key_guard = state.lock_key(b_id, key).await;
    match state.multipart.abort_upload(upload_id) {
        Ok(()) => Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header(CONTENT_LENGTH, "0")
            .body(empty_body())
            .unwrap()),
        Err(err) => {
            let xml = s3_error_xml(err, "NoSuchUpload", key, "req-ab-part");
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
    }
}

pub async fn list_parts(
    state: Arc<S3ServiceState>,
    bucket: &str,
    key: &str,
    query_map: &HashMap<String, String>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let upload_id = &query_map["uploadId"];
    match state.multipart.get_upload(upload_id) {
        Some(u) => {
            let parts: Vec<_> = u.parts.values().cloned().collect();
            let xml = list_parts_result_xml(bucket, key, upload_id, &parts);
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
        None => {
            let xml = s3_error_xml("NoSuchUpload", "The specified upload does not exist", key, "req-lp");
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
    }
}
