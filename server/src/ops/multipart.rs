use bytes::Bytes;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG};
use hyper::{HeaderMap, Response, StatusCode};
use md5::{Digest, Md5};
use std::collections::HashMap;
use std::sync::Arc;

use crate::ffi_bridge::{AWAN_FLAG_ACTIVE, AWAN_FLAG_MULTIPART};
use crate::iam::Iam;
use crate::meta_index::{unpack_flags, unpack_part_count};
use crate::multipart::{
    complete_multipart_upload_result_xml_with_checksum, copy_part_result_xml, encode_extents_manifest,
    initiate_multipart_upload_result_xml, list_multipart_uploads_result_xml, list_parts_result_xml,
    ListPartsPage, ListUploadsPage,
};
use crate::precondition::{
    precondition, ObjectFacts,
    PreconditionOutcome,
};
use crate::s3_api::{
    bad_request, compute_md5, drain_body, empty_body, engine_error_response, full_body,
    internal_error_response, percent_decode_path, read_body_admitted, request_timeout_response,
    s3_url_encode, ResponseBody, S3ServiceState, CHUNK_FRAMING_SLACK, MAX_OBJECT_BODY,
    MAX_XML_BODY, STREAM_PUT_MIN,
};
use crate::xml::s3_error_xml;

use super::object::{
    find_version, finish_version_put, format_object_etag, last_modified_secs_from_ms, last_modified_secs_from_ns, no_such_version,
    precondition_failure_response, version_etag,
};

pub async fn list_multipart_uploads(
    state: Arc<S3ServiceState>,
    bucket: &str,
    query_map: &HashMap<String, String>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let q = |k: &str| query_map.get(k).map(|s| s.as_str()).unwrap_or("");
    if let Some(et) = query_map.get("encoding-type") {
        if !et.is_empty() && et != "url" {
            return Ok(bad_request(
                "InvalidArgument",
                "Invalid Encoding Method specified in Request",
                bucket,
            ));
        }
    }
    let url_encode = q("encoding-type") == "url";
    let max_uploads = match query_map.get("max-uploads") {
        Some(s) => match s.parse::<i32>() {
            Ok(n) if n >= 0 => (n as usize).min(1000),
            _ => {
                return Ok(bad_request(
                    "InvalidArgument",
                    "Argument max-uploads must be an integer between 0 and 2147483647",
                    bucket,
                ));
            }
        },
        None => 1000,
    };

    let prefix = q("prefix");
    let delimiter = q("delimiter");
    let key_marker = q("key-marker");
    let upload_id_marker = q("upload-id-marker");

    let mut active: Vec<_> = {
        let uploads_guard = state.multipart.uploads.read();
        uploads_guard
            .values()
            .filter(|u| u.bucket == bucket && u.key.starts_with(prefix))
            .cloned()
            .collect()
    };
    active.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.upload_id.cmp(&b.upload_id)));

    enum Item<'a> {
        Upload(&'a crate::multipart::MultipartUpload),
        Prefix(String),
    }

    let mut items: Vec<Item<'_>> = Vec::new();
    let mut last_cp: Option<String> = None;
    for u in &active {
        if !delimiter.is_empty() {
            if let Some(pos) = u.key[prefix.len()..].find(delimiter) {
                let cp = u.key[..prefix.len() + pos + delimiter.len()].to_string();
                if !key_marker.is_empty() && cp.as_str() <= key_marker {
                    continue;
                }
                if last_cp.as_deref() == Some(cp.as_str()) {
                    continue;
                }
                last_cp = Some(cp.clone());
                items.push(Item::Prefix(cp));
                continue;
            }
        }
        if !key_marker.is_empty() {
            let cmp = if upload_id_marker.is_empty() {
                u.key.as_str() > key_marker
            } else {
                (u.key.as_str(), u.upload_id.as_str()) > (key_marker, upload_id_marker)
            };
            if !cmp {
                continue;
            }
        }
        items.push(Item::Upload(u));
    }

    let is_truncated = max_uploads > 0 && items.len() > max_uploads;
    let take_n = items.len().min(max_uploads);
    let mut page_uploads: Vec<(String, String, u64)> = Vec::new();
    let mut page_prefixes: Vec<String> = Vec::new();
    let mut next_key_marker = String::new();
    let mut next_upload_id_marker = String::new();

    let enc = |v: &str| if url_encode { s3_url_encode(v) } else { v.to_string() };

    for item in items.into_iter().take(take_n) {
        match item {
            Item::Upload(u) => {
                if is_truncated {
                    next_key_marker = u.key.clone();
                    next_upload_id_marker = u.upload_id.clone();
                }
                page_uploads.push((enc(&u.key), u.upload_id.clone(), u.initiated_at));
            }
            Item::Prefix(cp) => {
                if is_truncated {
                    next_key_marker = cp.clone();
                    next_upload_id_marker.clear();
                }
                page_prefixes.push(enc(&cp));
            }
        }
    }

    let xml = list_multipart_uploads_result_xml(&ListUploadsPage {
        bucket,
        prefix: &enc(prefix),
        delimiter: &enc(delimiter),
        key_marker: &enc(key_marker),
        upload_id_marker,
        next_key_marker: &enc(&next_key_marker),
        next_upload_id_marker: &next_upload_id_marker,
        max_uploads,
        is_truncated,
        encoding_url: url_encode,
        uploads: &page_uploads,
        common_prefixes: &page_prefixes,
    });
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
    let attrs = match super::object::request_attrs_map(headers, key, None) {
        Ok(a) => a,
        Err(resp) => return Ok(resp),
    };
    let tag_set = match crate::ops::tagging::parse_tagging_header_from(headers, key) {
        Ok(t) => t,
        Err(resp) => return Ok(resp),
    };
    let upload_id = match state.multipart.create_upload(bucket, b_id, key) {
        Ok(id) => id,
        Err(e) => return Ok(internal_error_response(key, &format!("could not start the upload: {}", e))),
    };
    if !attrs.is_empty() {
        state.multipart.set_upload_attrs(&upload_id, attrs);
    }
    if let (Some((alg, _)), Some(ref ct)) = (&req_cksum, &cksum_type) {
        state.multipart.set_upload_checksum(&upload_id, *alg, ct.clone());
    }
    if let Some(tags) = tag_set {
        state.multipart.set_upload_tags(&upload_id, tags);
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
    let upload_info = state.multipart.upload_checksum_info(upload_id);
    let req_checksum = match crate::checksum::requested_checksum(&signed_headers) {
        Ok(Some(c)) => Some(c),
        Ok(None) => upload_info.as_ref().and_then(|(alg, _)| alg.map(|a| (a, None))),
        Err(r) => return Ok(bad_request(r.code, r.why, key)),
    };
    let declared_len = signed_headers
        .get(if is_chunked { "x-amz-decoded-content-length" } else { "content-length" })
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if let Some(len) = declared_len.filter(|&n| n > STREAM_PUT_MIN) {
        if upload_info.is_none() {
            let xml = s3_error_xml("NoSuchUpload", "The specified upload does not exist.", key, "req-up-part");
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        }
        enum PartMsg {
            Data(Vec<u8>),
            Finish,
        }
        enum PipelineFailure {
            Timeout,
            Hyper(hyper::Error),
            ChunkDecode(&'static str),
            BadRequest(&'static str, String),
            Internal(String),
        }
        const PART_COALESCE: usize = crate::multipart::PART_CHUNK;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<PartMsg>(2);
        let mut tx = Some(tx);
        let (recycle_tx, mut recycle_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(2);
        let _ = recycle_tx.try_send(Vec::with_capacity(PART_COALESCE));
        let mp_for_task = Arc::clone(&state.multipart);
        let uid_for_task = upload_id.to_string();
        let mut writer_task = Some(tokio::task::spawn_blocking(
            move || -> Result<Option<std::path::PathBuf>, (&'static str, String)> {
                let mut writer = mp_for_task
                    .begin_part(&uid_for_task, part_num)
                    .map_err(|e| ("InternalError", format!("could not store the part: {}", e)))?;
                while let Some(msg) = rx.blocking_recv() {
                    match msg {
                        PartMsg::Data(mut buf) => {
                            if writer.written() + buf.len() as u64 > len {
                                return Err(("IncompleteBody", "more bytes than the declared length".to_string()));
                            }
                            if let Err(e) = writer.write_coalesced(&buf) {
                                return Err(("InternalError", format!("could not store the part: {}", e)));
                            }
                            buf.clear();
                            let _ = recycle_tx.try_send(buf);
                        }
                        PartMsg::Finish => {
                            let p = writer
                                .finish()
                                .map_err(|e| ("InternalError", format!("could not store the part: {}", e)))?;
                            return Ok(Some(p));
                        }
                    }
                }
                Ok(None)
            },
        ));
        let declared_sha = signed_headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .filter(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()))
            .map(|h| h.to_ascii_lowercase());
        let mut sha = declared_sha.as_ref().map(|_| sha2::Sha256::new());
        let mut md5 = Some(Md5::new());
        let mut cksum_hasher = req_checksum.as_ref().map(|(alg, _)| crate::checksum::Checksum::new(*alg));
        let mut body = body;
        let cache = &state.sigv4_cache;
        let mut decoder = is_chunked.then(|| {
            let mut verifier = crate::auth::ChunkVerifier::for_request_with(&state.auth, iam.as_deref(), &signed_headers);
            crate::aws_chunked::ChunkedDecoder::new(move |sha: &str, sig: Option<&str>| {
                verifier.as_mut().map_or(true, |v| v.check_digest(cache, sha, sig))
            })
        });
        let idle_timeout = state.body_idle_timeout();
        let mut cur_buf = Vec::with_capacity(PART_COALESCE);
        let mut total_fed: u64 = 0;
        let mut pipe_state = crate::fsm::PartPipelineState::Filling;
        let mut failure: Option<PipelineFailure> = None;
        let mut staged_path: Option<std::path::PathBuf> = None;
        let mut stream_cksum_out: Option<(crate::checksum::ChecksumAlgorithm, String)> = None;
        loop {
            match pipe_state {
                crate::fsm::PartPipelineState::Filling => {
                    let ev = match crate::body_timeout::next_frame_with_idle(&mut body, idle_timeout).await {
                        Ok(Some(Ok(frame))) => {
                            if let Ok(data) = frame.into_data() {
                                if let Some(h) = sha.as_mut() {
                                    h.update(&data);
                                }
                                let mut ready: Vec<Vec<u8>> = Vec::new();
                                let fed = {
                                    let mut sink = |d: &[u8]| -> Result<(), String> {
                                        if total_fed + d.len() as u64 > len {
                                            return Err("more bytes than the declared length".to_string());
                                        }
                                        total_fed += d.len() as u64;
                                        if let Some(m) = md5.as_mut() {
                                            m.update(d);
                                        }
                                        if let Some(h) = cksum_hasher.as_mut() {
                                            h.update(d);
                                        }
                                        let mut rem = d;
                                        while !rem.is_empty() {
                                            let take = (PART_COALESCE - cur_buf.len()).min(rem.len());
                                            cur_buf.extend_from_slice(&rem[..take]);
                                            rem = &rem[take..];
                                            if cur_buf.len() == PART_COALESCE {
                                                let next_buf = recycle_rx
                                                    .try_recv()
                                                    .unwrap_or_else(|_| Vec::with_capacity(PART_COALESCE));
                                                ready.push(std::mem::replace(&mut cur_buf, next_buf));
                                            }
                                        }
                                        Ok(())
                                    };
                                    match decoder.as_mut() {
                                        Some(dec) => dec.push(&data, &mut sink),
                                        None => sink(&data).map_err(crate::aws_chunked::PushError::Sink),
                                    }
                                };
                                match fed {
                                    Ok(()) => {
                                        let mut send_err_ev = None;
                                        let sender = tx.as_ref().expect("pipeline sender present while filling");
                                        for full_buf in ready {
                                            if sender.send(PartMsg::Data(full_buf)).await.is_err() {
                                                let (ev, fail) = match writer_task.take() {
                                                    Some(t) => match t.await {
                                                        Ok(Err(("IncompleteBody", msg))) => (
                                                            crate::fsm::PartPipelineEvent::LengthMismatch,
                                                            PipelineFailure::BadRequest("IncompleteBody", msg),
                                                        ),
                                                        Ok(Err((_, msg))) => (
                                                            crate::fsm::PartPipelineEvent::WriteError,
                                                            PipelineFailure::Internal(msg),
                                                        ),
                                                        Ok(Ok(_)) => (
                                                            crate::fsm::PartPipelineEvent::WriteError,
                                                            PipelineFailure::Internal("could not store the part: writer closed early".to_string()),
                                                        ),
                                                        Err(e) => (
                                                            crate::fsm::PartPipelineEvent::WriteError,
                                                            PipelineFailure::Internal(format!("part writer panicked: {:?}", e)),
                                                        ),
                                                    },
                                                    None => (
                                                        crate::fsm::PartPipelineEvent::WriteError,
                                                        PipelineFailure::Internal("could not store the part: writer missing".to_string()),
                                                    ),
                                                };
                                                failure = Some(fail);
                                                send_err_ev = Some(ev);
                                                break;
                                            }
                                        }
                                        send_err_ev.unwrap_or(crate::fsm::PartPipelineEvent::Frame)
                                    }
                                    Err(crate::aws_chunked::PushError::Decode(e)) => {
                                        failure = Some(PipelineFailure::ChunkDecode(e));
                                        crate::fsm::PartPipelineEvent::Disconnect
                                    }
                                    Err(crate::aws_chunked::PushError::Sink(e)) if e.starts_with("more bytes") => {
                                        failure = Some(PipelineFailure::BadRequest(
                                            "IncompleteBody",
                                            "The body is longer than its declared length.".to_string(),
                                        ));
                                        crate::fsm::PartPipelineEvent::LengthMismatch
                                    }
                                    Err(crate::aws_chunked::PushError::Sink(e)) => {
                                        failure = Some(PipelineFailure::Internal(format!("could not store the part: {}", e)));
                                        crate::fsm::PartPipelineEvent::WriteError
                                    }
                                }
                            } else {
                                crate::fsm::PartPipelineEvent::Frame
                            }
                        }
                        Ok(Some(Err(e))) => {
                            failure = Some(PipelineFailure::Hyper(e));
                            crate::fsm::PartPipelineEvent::Disconnect
                        }
                        Err(crate::body_timeout::BodyIdleTimeout) => {
                            failure = Some(PipelineFailure::Timeout);
                            crate::fsm::PartPipelineEvent::Disconnect
                        }
                        Ok(None) => crate::fsm::PartPipelineEvent::EndOfBody,
                    };
                    pipe_state = match crate::fsm::part_pipeline_step(pipe_state, ev) {
                        Ok(next) => next,
                        Err(r) => {
                            failure.get_or_insert_with(|| PipelineFailure::Internal(r.why.to_string()));
                            crate::fsm::PartPipelineState::Failed
                        }
                    };
                }
                crate::fsm::PartPipelineState::Draining => {
                    let ev = 'drain: {
                        if decoder.as_ref().is_some_and(|d| d.finish().is_err()) || total_fed != len {
                            failure = Some(PipelineFailure::BadRequest(
                                "IncompleteBody",
                                "You did not provide the number of bytes specified by the Content-Length HTTP header.".to_string(),
                            ));
                            break 'drain crate::fsm::PartPipelineEvent::LengthMismatch;
                        }
                        if let (Some(h), Some(want)) = (sha.take(), declared_sha.as_ref()) {
                            let got: String = h.finalize().iter().map(|b| format!("{:02x}", b)).collect();
                            if &got != want {
                                failure = Some(PipelineFailure::BadRequest(
                                    "XAmzContentSHA256Mismatch",
                                    "The provided 'x-amz-content-sha256' header does not match what was computed.".to_string(),
                                ));
                                break 'drain crate::fsm::PartPipelineEvent::HashError;
                            }
                        }
                        if let (Some((alg, expected_opt)), Some(h)) = (req_checksum.as_ref(), cksum_hasher.take()) {
                            let got = h.finish_base64();
                            if let Some(expected) = expected_opt {
                                if &got != expected {
                                    failure = Some(PipelineFailure::BadRequest(
                                        "BadDigest",
                                        "The checksum you specified did not match what we received.".to_string(),
                                    ));
                                    break 'drain crate::fsm::PartPipelineEvent::HashError;
                                }
                            }
                            stream_cksum_out = Some((*alg, got));
                        }
                        if let Some(sender) = tx.take() {
                            let sent_tail = if !cur_buf.is_empty() {
                                sender.send(PartMsg::Data(std::mem::take(&mut cur_buf))).await.is_ok()
                            } else {
                                true
                            };
                            if sent_tail {
                                let _ = sender.send(PartMsg::Finish).await;
                            }
                        }
                        match writer_task.take().expect("part writer task present").await {
                            Ok(Ok(Some(p))) => {
                                staged_path = Some(p);
                                crate::fsm::PartPipelineEvent::Drained
                            }
                            Ok(Ok(None)) => {
                                failure = Some(PipelineFailure::Internal("could not store the part: writer closed early".to_string()));
                                crate::fsm::PartPipelineEvent::WriteError
                            }
                            Ok(Err(("IncompleteBody", msg))) => {
                                failure = Some(PipelineFailure::BadRequest("IncompleteBody", msg));
                                crate::fsm::PartPipelineEvent::LengthMismatch
                            }
                            Ok(Err((_, msg))) => {
                                failure = Some(PipelineFailure::Internal(msg));
                                crate::fsm::PartPipelineEvent::WriteError
                            }
                            Err(e) => {
                                failure = Some(PipelineFailure::Internal(format!("part writer failed: {:?}", e)));
                                crate::fsm::PartPipelineEvent::WriteError
                            }
                        }
                    };
                    pipe_state = match crate::fsm::part_pipeline_step(pipe_state, ev) {
                        Ok(next) => next,
                        Err(r) => {
                            failure.get_or_insert_with(|| PipelineFailure::Internal(r.why.to_string()));
                            crate::fsm::PartPipelineState::Failed
                        }
                    };
                }
                crate::fsm::PartPipelineState::Failed => {
                    drop(tx.take());
                    if let Some(t) = writer_task.take() {
                        let _ = t.await;
                    }
                    return match failure.take().unwrap_or_else(|| PipelineFailure::Internal("streamed UploadPart failed".to_string())) {
                        PipelineFailure::Timeout => Ok(request_timeout_response(key)),
                        PipelineFailure::Hyper(e) => Err(e),
                        PipelineFailure::ChunkDecode(e) => {
                            drain_body(&mut body, 64 << 20).await;
                            let status = if e == "SignatureDoesNotMatch" {
                                StatusCode::FORBIDDEN
                            } else {
                                StatusCode::BAD_REQUEST
                            };
                            Ok(Response::builder()
                                .status(status)
                                .header(CONTENT_TYPE, "application/xml")
                                .body(full_body(Bytes::from(s3_error_xml(e, "Error decoding chunked body", key, "req-up-part"))))
                                .unwrap())
                        }
                        PipelineFailure::BadRequest(code, msg) => Ok(bad_request(code, &msg, key)),
                        PipelineFailure::Internal(msg) => Ok(internal_error_response(key, &msg)),
                    };
                }
                crate::fsm::PartPipelineState::Done => break,
            }
        }
        let staged_path = staged_path.expect("Done state guarantees staged_path");
        let raw_md5: [u8; 16] = md5.take().expect("md5 hasher present").finalize().into();
        let hi = u64::from_be_bytes(raw_md5[0..8].try_into().unwrap());
        let lo = u64::from_be_bytes(raw_md5[8..16].try_into().unwrap());
        let hex: String = raw_md5.iter().map(|b| format!("{:02x}", b)).collect();
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
    let mp_for_task = Arc::clone(&state.multipart);
    let uid_for_task = upload_id.to_string();
    let write_bytes = decoded_bytes.clone();
    let staged_path = match tokio::task::spawn_blocking(move || {
        mp_for_task.write_staged_part(&uid_for_task, part_num, &write_bytes)
    })
    .await
    {
        Ok(Ok(p)) => Some(p),
        Ok(Err(e)) => return Ok(internal_error_response(key, &format!("could not store the part: {}", e))),
        Err(e) => return Ok(internal_error_response(key, &format!("part write task failed: {}", e))),
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
    let body_str = match std::str::from_utf8(&body_bytes) {
        Ok(s) => s,
        Err(_) => {
            return Ok(bad_request(
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema",
                key,
            ));
        }
    };
    let parsed_parts = match crate::request_xml::parse_complete_multipart(body_str) {
        Ok(p) => p,
        Err(r) => {
            let xml = s3_error_xml(r.code, r.why, key, "req-cmp-part");
            return Ok(Response::builder()
                .status(StatusCode::from_u16(r.status).unwrap_or(StatusCode::BAD_REQUEST))
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        }
    };
    let upload_complete_meta = state.multipart.upload_complete_info(upload_id);
    let upload_info = upload_complete_meta.as_ref().map(|(alg, ct, _)| (*alg, ct.clone()));
    let upload_tags = upload_complete_meta.and_then(|(_, _, tags)| tags);
    let mut part_cksums = Vec::new();
    if let Some((Some(alg), _)) = upload_info {
        let open = format!("<{}>", alg.xml_name());
        let close = format!("</{}>", alg.xml_name());
        let mut cursor = body_str;
        while let Some(start) = cursor.find("<Part>") {
            let rest = &cursor[start + 6..];
            let Some(part_end) = rest.find("</Part>") else { break };
            let part_block = &rest[..part_end];
            cursor = &rest[part_end + 7..];
            let ck = part_block.find(&open).and_then(|s| {
                let r = &part_block[s + open.len()..];
                let e = r.find(&close)?;
                Some(r[..e].trim().to_string())
            });
            part_cksums.push(ck);
        }
    }
    let mut declared_parts: Vec<(u32, String, Option<String>)> = Vec::with_capacity(parsed_parts.len());
    for (i, p) in parsed_parts.into_iter().enumerate() {
        let ck = part_cksums.get(i).cloned().flatten();
        if let Some(last) = declared_parts.last_mut().filter(|(n, _, _)| *n == p.part_number) {
            last.1 = p.etag;
            last.2 = ck;
        } else {
            declared_parts.push((p.part_number, p.etag, ck));
        }
    }

    if let Some(cached) = state.multipart.get_completed_result(upload_id) {
        if cached.bucket == bucket && cached.key == key {
            let xml = complete_multipart_upload_result_xml_with_checksum(
                &cached.bucket,
                &cached.key,
                &cached.composite_etag,
                &cached.location,
                cached.checksum.as_ref().map(|(a, ct, v)| (*a, ct.as_str(), v.as_str())),
            );
            let mut resp = Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/xml")
                .header(ETAG, format!("\"{}\"", cached.composite_etag));
            if let Some(ref v) = cached.version_id {
                resp = resp.header("x-amz-version-id", v.as_str());
            }
            if let Some((alg, ref ct, ref val)) = cached.checksum {
                resp = resp.header(alg.header_name(), val.as_str()).header("x-amz-checksum-type", ct.as_str());
            }
            return Ok(resp.body(full_body(Bytes::from(xml))).unwrap());
        }
    }

    let declared_pairs: Vec<(u32, String)> = declared_parts.iter().map(|(n, e, _)| (*n, e.clone())).collect();
    match state.multipart.prepare_complete(upload_id, &declared_pairs) {
        Ok((composite_etag, total_size, extents)) => {
            let final_cksum: Option<(crate::checksum::ChecksumAlgorithm, String, String)> = if let Some((Some(alg), cksum_type_opt)) = upload_info {
                let cksum_type = cksum_type_opt.unwrap_or_else(|| "COMPOSITE".to_string());
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
                let computed = if cksum_type == "FULL_OBJECT" {
                    let mut full_hasher = crate::checksum::Checksum::new(alg);
                    for ext in &extents {
                        let bytes = match state.part_bytes(b_id, ext) {
                            Ok(b) => b,
                            Err(e) => return Ok(internal_error_response(key, &format!("could not read part for checksum: {:?}", e))),
                        };
                        full_hasher.update(&bytes);
                    }
                    full_hasher.finish_base64()
                } else {
                    crate::checksum::composite(alg, &part_raws)
                };
                if let Some(hdr_val) = headers.get(alg.header_name()).and_then(|v| v.to_str().ok()) {
                    if hdr_val.trim() != computed {
                        return Ok(bad_request("BadDigest", "The checksum you specified did not match what we received.", key));
                    }
                }
                Some((alg, cksum_type, computed))
            } else {
                None
            };
            let mut attrs = upload_meta.as_ref().map(|u| u.attrs.clone()).unwrap_or_default();
            let part_sizes: Vec<u64> = extents.iter().map(|e| e.part_size).collect();
            attrs.set_part_sizes(&part_sizes);
            if let Some((alg, ref ct, ref val)) = final_cksum {
                attrs.set_checksum(alg, ct, val);
            }
            if let Some(ref tag_set) = upload_tags {
                if !tag_set.tags.is_empty() {
                    attrs.set(crate::attrs::TAG_TAGGING, &crate::attrs::encode_tags(tag_set));
                }
            }
            let attrs_bytes = match attrs.encode() {
                Ok(b) => b,
                Err(_) => {
                    return Ok(bad_request(
                        "MetadataTooLarge",
                        "Your metadata headers exceed the maximum allowed metadata size.",
                        key,
                    ));
                }
            };
            let attrs_flag = if attrs_bytes.is_empty() { 0 } else { crate::meta_index::FLAG_ATTRS };

            let hex_prefix = composite_etag.split('-').next().unwrap_or("");
            let hi = u64::from_str_radix(hex_prefix.get(0..16).unwrap_or("0"), 16).unwrap_or(0);
            let lo = u64::from_str_radix(hex_prefix.get(16..32).unwrap_or("0"), 16).unwrap_or(0);
            let _key_guard = state.lock_key(b_id, key).await;
            if let Err(code) = state.engine.read().has_room_for(b_id, key.as_bytes()) {
                return Ok(engine_error_response(code, key));
            }
            let manifest = Bytes::from(encode_extents_manifest(&state.multipart.staging_dir, total_size, &extents));
            let stamp = state.version_stamp(b_id, key);
            let created_ns = stamp.map(|s| s.1).unwrap_or_else(|| state.clock.now_ns());
            let mp_flags = AWAN_FLAG_ACTIVE | AWAN_FLAG_MULTIPART | attrs_flag | stamp.map_or(0, |s| s.0);
            // A manifest whole in the volume is replayed by recovery as the object, and it
            // points at the parts: the upload is committed and indexed as on success, then the
            // request answers 500 (#184, owner decision 4B). A retried Complete gets the
            // recorded result.
            let (manifest_offset, late) = match state
                .store_payload_with(b_id, key, &attrs_bytes, &manifest, mp_flags, hi, lo, Some(created_ns))
                .await
            {
                Ok(off) => (off, None),
                Err(e) => match S3ServiceState::late_record(&e) {
                    Some((off, _)) => (off, Some(e)),
                    None => return Ok(internal_error_response(key, &format!("could not record the multipart object: {:?}", e))),
                },
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
            if let Some((extra, ts)) = stamp {
                match finish_version_put(&state, b_id, key, ts, mp_flags, manifest_offset, total_size, hi, lo, extents.len() as u16) {
                    Ok(v) => {
                        if extra & crate::meta_index::FLAG_NULL_ID == 0 {
                            version_id = Some(v);
                        }
                    }
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
            state.multipart.record_completed_result(
                upload_id,
                crate::multipart::CompletedUploadResult {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    composite_etag: composite_etag.clone(),
                    location: location.clone(),
                    version_id: version_id.clone(),
                    checksum: final_cksum.clone(),
                },
            );
            if let Some(e) = late {
                return Ok(internal_error_response(key, &format!("could not record the multipart object: {:?}", e)));
            }
            let xml = complete_multipart_upload_result_xml_with_checksum(
                bucket,
                key,
                &composite_etag,
                &location,
                final_cksum.as_ref().map(|(a, ct, v)| (*a, ct.as_str(), v.as_str())),
            );
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

fn parse_copy_source_range(raw: &str) -> Result<(u64, u64), ()> {
    let rest = raw.trim().strip_prefix("bytes=").ok_or(())?;
    if rest.contains(',') {
        return Err(());
    }
    let (s_str, e_str) = rest.split_once('-').ok_or(())?;
    let start: u64 = s_str.parse().map_err(|_| ())?;
    let end: u64 = e_str.parse().map_err(|_| ())?;
    if start > end {
        return Err(());
    }
    Ok((start, end))
}

pub async fn upload_part_copy(
    state: Arc<S3ServiceState>,
    key: &str,
    query_map: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let upload_id = &query_map["uploadId"];
    let part_num: u32 = match query_map["partNumber"].parse::<u32>() {
        Ok(n) if (1..=10_000).contains(&n) => n,
        _ => {
            return Ok(bad_request("InvalidArgument", "Invalid partNumber", key));
        }
    };

    if state.multipart.get_upload(upload_id).is_none() {
        let xml = s3_error_xml("NoSuchUpload", "The specified upload does not exist.", key, "req-upc");
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(CONTENT_TYPE, "application/xml")
            .body(full_body(Bytes::from(xml)))
            .unwrap());
    }

    let range_req = match headers.get("x-amz-copy-source-range").and_then(|v| v.to_str().ok()) {
        Some(raw) => match parse_copy_source_range(raw) {
            Ok(r) => Some(r),
            Err(()) => {
                return Ok(bad_request(
                    "InvalidArgument",
                    "The x-amz-copy-source-range value must be expressed as bytes=first-last",
                    key,
                ));
            }
        },
        None => None,
    };

    let copy_source = headers.get("x-amz-copy-source").expect("UploadPartCopy requires x-amz-copy-source");
    let copy_source_raw = copy_source.to_str().unwrap_or("");
    let (copy_path, copy_query) = copy_source_raw.split_once('?').unwrap_or((copy_source_raw, ""));
    let src_vid: Option<String> = copy_query
        .split('&')
        .find_map(|kv| kv.strip_prefix("versionId="))
        .map(percent_decode_path);
    let copy_src_owned = percent_decode_path(copy_path);
    let copy_src_str = copy_src_owned.trim_start_matches('/');
    let (src_bucket, src_key) = match copy_src_str.split_once('/') {
        Some((b, k)) if !b.is_empty() && !k.is_empty() => (b, k),
        _ => return Ok(bad_request("InvalidArgument", "Invalid copy source", copy_src_str)),
    };

    let (src_b_id, src_meta) = {
        let engine = state.engine.read();
        let src_b_id = match engine.lookup_bucket(src_bucket) {
            Ok(id) => id,
            Err(_) => {
                let xml = s3_error_xml("NoSuchBucket", "Source bucket does not exist", src_bucket, "req-upc");
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .header(CONTENT_TYPE, "application/xml")
                    .body(full_body(Bytes::from(xml)))
                    .unwrap());
            }
        };
        let src_meta = if src_vid.is_some() {
            None
        } else {
            match engine.get_object(src_b_id, src_key.as_bytes()) {
                Ok(m) => Some(m),
                Err(_) => {
                    let xml = s3_error_xml("NoSuchKey", "Source object does not exist", src_key, "req-upc");
                    return Ok(Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .header(CONTENT_TYPE, "application/xml")
                        .body(full_body(Bytes::from(xml)))
                        .unwrap());
                }
            }
        };
        (src_b_id, src_meta)
    };

    let (src_size, src_facts, copied_vid, version_opt) = match (&src_vid, &src_meta) {
        (Some(vid), _) => {
            let Some(id) = crate::versions::parse_id(vid) else {
                return Ok(no_such_version(src_key, false));
            };
            let Some(v) = find_version(&state, src_b_id, src_key, Some(id)) else {
                return Ok(no_such_version(src_key, false));
            };
            if v.marker {
                return Ok(bad_request(
                    "InvalidRequest",
                    "The source of a copy request may not specifically refer to a delete marker by version id.",
                    src_key,
                ));
            }
            let facts = ObjectFacts {
                etag: version_etag(&v),
                last_modified_secs: last_modified_secs_from_ns(v.modified),
            };
            let vid_str = v.id_string();
            (v.size, facts, Some(vid_str), Some(v))
        }
        (None, Some(m)) => {
            let flags = unpack_flags(m.bucket_flags);
            let is_mp = flags & AWAN_FLAG_MULTIPART != 0;
            let facts = ObjectFacts {
                etag: format_object_etag(m.etag_hi, m.etag_lo, is_mp, unpack_part_count(m.bucket_flags)),
                last_modified_secs: last_modified_secs_from_ms(m.created_at),
            };
            (m.size, facts, None, None)
        }
        (None, None) => unreachable!(),
    };

    if precondition(headers, Some(&src_facts), true) != PreconditionOutcome::Proceed {
        return Ok(precondition_failure_response(PreconditionOutcome::PreconditionFailed, None, key, false));
    }

    if let Some((_, end)) = range_req {
        if src_size == 0 || end >= src_size {
            let xml = s3_error_xml(
                "InvalidRange",
                "The requested range is not satisfiable.",
                key,
                "req-upc-range",
            );
            return Ok(Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap());
        }
    }

    let bytes = match (version_opt, src_meta) {
        (Some(v), _) => state.read_version_bytes(src_b_id, src_key, &v),
        (None, Some(m)) => {
            let flags = unpack_flags(m.bucket_flags);
            state.read_object_bytes(src_b_id, src_key, m.vol_offset, m.size, flags)
        }
        _ => unreachable!(),
    };
    let bytes = match bytes {
        Ok(b) => b,
        Err(e) => return Ok(internal_error_response(key, &format!("could not read the copy source: {:?}", e))),
    };

    let part_slice: &[u8] = match range_req {
        Some((start, end)) => &bytes[start as usize..=(end as usize).min(bytes.len().saturating_sub(1))],
        None => &bytes[..],
    };

    let (hi, lo, hex, raw_md5) = compute_md5(part_slice);
    let staged_path = match state.multipart.write_staged_part(upload_id, part_num, part_slice) {
        Ok(p) => Some(p),
        Err(e) => return Ok(internal_error_response(key, &format!("could not store the part: {}", e))),
    };

    match state.multipart.upload_part(
        upload_id,
        part_num,
        0,
        part_slice.len() as u64,
        raw_md5,
        hi,
        lo,
        hex.clone(),
        staged_path,
    ) {
        Ok(_) => {
            let lm = crate::versions::iso8601(state.clock.now_ns());
            let xml = copy_part_result_xml(&hex, &lm);
            let mut resp = Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/xml")
                .header(ETAG, format!("\"{}\"", hex));
            if let Some(v) = copied_vid {
                resp = resp.header("x-amz-copy-source-version-id", v);
            }
            Ok(resp.body(full_body(Bytes::from(xml))).unwrap())
        }
        Err(err) => {
            let xml = s3_error_xml(err, "Failed to upload part", key, "req-upc");
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

pub async fn list_parts(
    state: Arc<S3ServiceState>,
    bucket: &str,
    key: &str,
    query_map: &HashMap<String, String>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    if let Some(et) = query_map.get("encoding-type") {
        if !et.is_empty() && et != "url" {
            return Ok(bad_request(
                "InvalidArgument",
                "Invalid Encoding Method specified in Request",
                key,
            ));
        }
    }
    let url_encode = query_map.get("encoding-type").is_some_and(|s| s == "url");
    let marker: u32 = match query_map.get("part-number-marker") {
        Some(s) if !s.is_empty() => match s.parse::<u32>() {
            Ok(n) => n,
            Err(_) => {
                return Ok(bad_request(
                    "InvalidArgument",
                    "Argument part-number-marker must be a valid integer",
                    key,
                ));
            }
        },
        _ => 0,
    };
    let max_parts: usize = match query_map.get("max-parts") {
        Some(s) => match s.parse::<i32>() {
            Ok(n) if n >= 0 => (n as usize).min(1000),
            _ => {
                return Ok(bad_request(
                    "InvalidArgument",
                    "Argument max-parts must be an integer between 0 and 2147483647",
                    key,
                ));
            }
        },
        None => 1000,
    };

    let upload_id = &query_map["uploadId"];
    match state.multipart.get_upload(upload_id) {
        Some(u) if u.bucket == bucket && u.key == key => {
            let filtered: Vec<_> = u
                .parts
                .values()
                .filter(|p| p.part_number > marker)
                .cloned()
                .collect();
            let is_truncated = max_parts > 0 && filtered.len() > max_parts;
            let page_parts: Vec<_> = filtered.into_iter().take(max_parts).collect();
            let next_marker = if is_truncated {
                page_parts.last().map_or(0, |p| p.part_number)
            } else {
                0
            };
            let enc_key = if url_encode { s3_url_encode(key) } else { key.to_string() };
            let xml = list_parts_result_xml(&ListPartsPage {
                bucket,
                key: &enc_key,
                upload_id,
                part_number_marker: marker,
                next_part_number_marker: next_marker,
                max_parts,
                is_truncated,
                encoding_url: url_encode,
                parts: &page_parts,
            });
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
        _ => {
            let xml = s3_error_xml("NoSuchUpload", "The specified upload does not exist", key, "req-lp");
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header(CONTENT_TYPE, "application/xml")
                .body(full_body(Bytes::from(xml)))
                .unwrap())
        }
    }
}
