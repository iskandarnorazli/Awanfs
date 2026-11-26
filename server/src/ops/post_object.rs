//! POST object (#119), the request side: read the form's fields up to the file, check its
//! SigV4 signature and policy, then store the file through `put_object`, so a POST stores
//! exactly what a PUT of the same bytes and headers would.
//!
//! The file's size has to be known before PUT stores it. A file of at most 4 MiB is read
//! into memory first, so its size is exact. A larger one streams: its size is the request's
//! Content-Length minus the bytes before the file and the closing delimiter. The body never
//! hands PUT the file's last bytes before the parser has seen the file end at exactly that
//! size, so a body that does not match can never be stored (#119 invariant: an upload that
//! fails any check leaves no live object).

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use hyper::body::{Body, Frame};
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE, HOST, LOCATION};
use hyper::{HeaderMap, Response, StatusCode};
use parking_lot::Mutex;

use super::Classified;
use crate::form_upload::{extract_boundary, FormEvent, FormLimits, FormParser};
use crate::fsm::Refusal;
use crate::post_object::{self as po, FormFields, Policy};
use crate::s3_api::{empty_body, full_body, request_timeout_response, ResponseBody, S3ServiceState, STREAM_PUT_MIN};
use crate::xml::s3_error_xml;

const MALFORMED: Refusal = Refusal {
    code: "MalformedPOSTRequest",
    status: 400,
    why: "The body of your POST request is not well-formed multipart/form-data.",
};
const MISSING_LENGTH: Refusal = Refusal {
    code: "MissingContentLength",
    status: 411,
    why: "You must provide the Content-Length HTTP header.",
};
const BAD_FIELD_VALUE: Refusal = Refusal {
    code: "InvalidArgument",
    status: 400,
    why: "A form field's value cannot be sent as an object header.",
};

/// What `read_form` read: the fields, the file's name, and the parser stopped at the file.
pub struct PostForm {
    pub fields: FormFields,
    pub filename: Option<String>,
    parser: FormParser,
    /// File bytes already parsed out of the frames read so far.
    pending: Vec<Bytes>,
    /// The parser has seen the closing delimiter.
    done: bool,
    file_offset: u64,
    pub(crate) policy: Option<Policy>,
}

pub(crate) fn refusal_response(r: Refusal, resource: &str) -> Response<ResponseBody> {
    let xml = s3_error_xml(r.code, r.why, resource, "req-post");
    Response::builder()
        .status(StatusCode::from_u16(r.status).unwrap_or(StatusCode::BAD_REQUEST))
        .header(CONTENT_TYPE, "application/xml")
        .body(full_body(Bytes::from(xml)))
        .unwrap()
}

/// Is this POST to a bucket a form upload?
pub fn is_form_upload(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|ct| ct.split(';').next())
        .is_some_and(|t| t.trim().eq_ignore_ascii_case("multipart/form-data"))
}

/// Reads the form up to the start of the file and sets the request's key from it. The
/// fields are bounded by the parser's limits (100 fields of at most 20 KiB).
pub async fn read_form(state: &S3ServiceState, mut c: Classified) -> Result<Classified, Response<ResponseBody>> {
    let path = c.parts.uri.path().to_string();
    let ct = c.parts.headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("");
    let boundary = extract_boundary(ct).map_err(|r| refusal_response(r, &path))?;
    let mut parser = FormParser::new(&boundary, FormLimits::default());
    let mut fields = FormFields::default();
    let mut filename = None;
    let mut pending = Vec::new();
    let mut done = false;
    let mut started = false;
    let idle = state.body_idle_timeout();
    while !started {
        let frame = match crate::body_timeout::next_frame_with_idle(&mut c.body, idle).await {
            Err(_) => return Err(request_timeout_response(&path)),
            Ok(None) => {
                let r = parser.finish().err().unwrap_or(MALFORMED);
                return Err(refusal_response(r, &path));
            }
            Ok(Some(Err(_))) => return Err(refusal_response(MALFORMED, &path)),
            Ok(Some(Ok(f))) => f,
        };
        let Ok(data) = frame.into_data() else { continue };
        let events = parser.push(&data).map_err(|r| refusal_response(r, &path))?;
        for ev in events {
            match ev {
                FormEvent::Field { name, value } => fields.fields.push((name, value)),
                FormEvent::FileStart { filename: f, .. } => {
                    started = true;
                    filename = f;
                }
                FormEvent::FileData(b) => pending.push(b),
                FormEvent::FileEnd => {}
                FormEvent::Done => done = true,
            }
        }
    }
    let key = po::object_key(&fields, filename.as_deref()).map_err(|r| refusal_response(r, &path))?;
    let file_offset = parser.file_data_offset().unwrap_or(0);
    c.key = Some(key);
    c.form = Some(Box::new(PostForm { fields, filename, parser, pending, done, file_offset, policy: None }));
    Ok(c)
}

/// The file part as a request body for `put_object`.
pub enum FormFileBody {
    Buffered(Option<Bytes>),
    Streaming(Box<StreamingFile>),
}

pub struct StreamingFile {
    inner: Option<hyper::body::Incoming>,
    parser: FormParser,
    queue: VecDeque<Bytes>,
    /// File bytes handed to the reader.
    emitted: u64,
    /// File bytes the parser has produced.
    seen: u64,
    /// The size PUT was told.
    limit: u64,
    done: bool,
    failed: Arc<Mutex<Option<Refusal>>>,
}

impl StreamingFile {
    fn fail(&mut self, r: Refusal) {
        *self.failed.lock() = Some(r);
        self.queue.clear();
        self.done = true;
        self.inner = None;
    }

    fn take(&mut self, b: Bytes) {
        self.seen += b.len() as u64;
        if self.seen > self.limit {
            self.fail(MALFORMED);
        } else if !b.is_empty() {
            self.queue.push_back(b);
        }
    }

    fn finish_file(&mut self) {
        if self.seen != self.limit {
            self.fail(MALFORMED);
        } else {
            self.done = true;
            self.inner = None;
        }
    }

    fn feed(&mut self, data: &[u8]) {
        match self.parser.push(data) {
            Err(r) => self.fail(r),
            Ok(events) => {
                for ev in events {
                    if self.failed.lock().is_some() {
                        return;
                    }
                    match ev {
                        FormEvent::FileData(b) => self.take(b),
                        FormEvent::FileEnd => {}
                        FormEvent::Done => self.finish_file(),
                        FormEvent::Field { .. } | FormEvent::FileStart { .. } => self.fail(MALFORMED),
                    }
                }
            }
        }
    }
}

impl Body for FormFileBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, hyper::Error>>> {
        match self.get_mut() {
            FormFileBody::Buffered(b) => Poll::Ready(b.take().map(|d| Ok(Frame::data(d)))),
            FormFileBody::Streaming(s) => loop {
                if let Some(front) = s.queue.front() {
                    let len = front.len() as u64;
                    // The bytes that complete the file wait until the parser has seen it end.
                    let complete = s.done && s.failed.lock().is_none() && s.seen == s.limit;
                    if s.emitted + len < s.limit || complete {
                        let b = s.queue.pop_front().expect("front exists");
                        s.emitted += len;
                        return Poll::Ready(Some(Ok(Frame::data(b))));
                    }
                }
                if s.done {
                    return Poll::Ready(None);
                }
                let Some(inner) = s.inner.as_mut() else {
                    return Poll::Ready(None);
                };
                match Pin::new(inner).poll_frame(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Some(Err(e))) => {
                        s.fail(MALFORMED);
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(Some(Ok(frame))) => {
                        if let Ok(data) = frame.into_data() {
                            s.feed(&data);
                        }
                    }
                    Poll::Ready(None) => {
                        s.inner = None;
                        match s.parser.finish() {
                            Err(r) => s.fail(r),
                            Ok(_) => s.fail(MALFORMED),
                        }
                    }
                }
            },
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            FormFileBody::Buffered(b) => b.is_none(),
            FormFileBody::Streaming(s) => s.done && s.queue.is_empty(),
        }
    }
}

/// Reads the rest of a small file into memory, up to `cap` bytes.
async fn read_small_file(
    state: &S3ServiceState,
    form: &mut PostForm,
    body: &mut hyper::body::Incoming,
    cap: u64,
    path: &str,
) -> Result<Bytes, Response<ResponseBody>> {
    let mut buf = BytesMut::new();
    for b in form.pending.drain(..) {
        buf.extend_from_slice(&b);
    }
    let idle = state.body_idle_timeout();
    while !form.done {
        if buf.len() as u64 > cap {
            return Err(refusal_response(MALFORMED, path));
        }
        let frame = match crate::body_timeout::next_frame_with_idle(body, idle).await {
            Err(_) => return Err(request_timeout_response(path)),
            Ok(None) => {
                let r = form.parser.finish().err().unwrap_or(MALFORMED);
                return Err(refusal_response(r, path));
            }
            Ok(Some(Err(_))) => return Err(refusal_response(MALFORMED, path)),
            Ok(Some(Ok(f))) => f,
        };
        let Ok(data) = frame.into_data() else { continue };
        for ev in form.parser.push(&data).map_err(|r| refusal_response(r, path))? {
            match ev {
                FormEvent::FileData(b) => buf.extend_from_slice(&b),
                FormEvent::FileEnd => {}
                FormEvent::Done => form.done = true,
                FormEvent::Field { .. } | FormEvent::FileStart { .. } => return Err(refusal_response(MALFORMED, path)),
            }
        }
    }
    if buf.len() as u64 > cap {
        return Err(refusal_response(MALFORMED, path));
    }
    Ok(buf.freeze())
}

fn object_headers(form: &FormFields, size: u64, path: &str) -> Result<HeaderMap, Response<ResponseBody>> {
    let mut h = HeaderMap::new();
    h.insert(CONTENT_LENGTH, size.into());
    for (name, value) in po::object_header_fields(form) {
        let n = hyper::header::HeaderName::from_bytes(name.as_bytes()).map_err(|_| refusal_response(BAD_FIELD_VALUE, path))?;
        let v = hyper::header::HeaderValue::from_str(&value).map_err(|_| refusal_response(BAD_FIELD_VALUE, path))?;
        h.insert(n, v);
    }
    if let Some(xml) = form.get("tagging") {
        let tags = po::tagging_header(xml).map_err(|r| refusal_response(r, path))?;
        if !tags.is_empty() {
            let v = hyper::header::HeaderValue::from_str(&tags).map_err(|_| refusal_response(BAD_FIELD_VALUE, path))?;
            h.insert("x-amz-tagging", v);
        }
    }
    Ok(h)
}

#[allow(clippy::too_many_arguments)]
pub async fn execute(
    state: Arc<S3ServiceState>,
    b_id: u32,
    bucket: &str,
    key: &str,
    path: &str,
    req_headers: &HeaderMap,
    writer_account: &str,
    bucket_owner: &str,
    form: Option<Box<PostForm>>,
    mut body: hyper::body::Incoming,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let mut form = form.expect("read_form ran for PostObject");
    let Some(content_length) = req_headers
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
    else {
        return Ok(refusal_response(MISSING_LENGTH, path));
    };
    let Some(predicted) = content_length.checked_sub(form.file_offset + form.parser.closing_len()) else {
        return Ok(refusal_response(MALFORMED, path));
    };
    let failed = Arc::new(Mutex::new(None));
    let (size, file_body, _guard) = if form.done || predicted <= STREAM_PUT_MIN {
        let cap = predicted.max(STREAM_PUT_MIN);
        let guard = match state.body_budget.acquire_async(cap.min(predicted.saturating_add(1024))).await {
            Ok(g) => g,
            Err(_) => {
                return Ok(crate::s3_api::slow_down_response(
                    path,
                    "Please reduce your request rate; in-flight body memory budget exhausted.",
                ))
            }
        };
        let data = match read_small_file(&state, &mut form, &mut body, cap, path).await {
            Ok(d) => d,
            Err(resp) => return Ok(resp),
        };
        (data.len() as u64, FormFileBody::Buffered(Some(data)), Some(guard))
    } else {
        let parser = std::mem::replace(&mut form.parser, FormParser::new("-", FormLimits::default()));
        let mut s = StreamingFile {
            inner: Some(body),
            parser,
            queue: VecDeque::new(),
            emitted: 0,
            seen: 0,
            limit: predicted,
            done: false,
            failed: Arc::clone(&failed),
        };
        for b in std::mem::take(&mut form.pending) {
            s.take(b);
        }
        (predicted, FormFileBody::Streaming(Box::new(s)), None)
    };
    if let Err(r) = po::check_size(form.policy.as_ref(), size) {
        return Ok(refusal_response(r, path));
    }
    let headers = match object_headers(&form.fields, size, path) {
        Ok(h) => h,
        Err(resp) => return Ok(resp),
    };
    let resp = super::object::put_object(
        Arc::clone(&state),
        b_id,
        bucket,
        key,
        path,
        &headers,
        writer_account,
        bucket_owner,
        None,
        file_body,
    )
    .await?;
    if let Some(r) = *failed.lock() {
        return Ok(refusal_response(r, path));
    }
    if resp.status() != StatusCode::OK {
        return Ok(resp);
    }
    let (parts, _) = resp.into_parts();
    let etag = parts.headers.get("etag").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    let mut out = Response::builder();
    for (name, value) in parts.headers.iter() {
        if name != CONTENT_LENGTH && name != CONTENT_TYPE {
            out = out.header(name, value);
        }
    }
    let host = req_headers.get(HOST).and_then(|v| v.to_str().ok()).unwrap_or("");
    let location = format!("http://{}/{}/{}", host, bucket, po::query_encode(key));
    Ok(match po::success(&form.fields, bucket, key, &etag) {
        po::Success::Redirect(url) => {
            // The object is stored; a redirect URL that cannot be a header still gets a 204.
            match hyper::header::HeaderValue::from_str(&url) {
                Ok(v) => out.status(StatusCode::SEE_OTHER).header(LOCATION, v).header(CONTENT_LENGTH, "0").body(empty_body()).unwrap(),
                Err(_) => out.status(StatusCode::NO_CONTENT).body(empty_body()).unwrap(),
            }
        }
        po::Success::Created => out
            .status(StatusCode::CREATED)
            .header(LOCATION, location.as_str())
            .header(CONTENT_TYPE, "application/xml")
            .body(full_body(Bytes::from(po::post_response_xml(&location, bucket, key, &etag))))
            .unwrap(),
        po::Success::Empty(200) => out.status(StatusCode::OK).header(LOCATION, location.as_str()).header(CONTENT_LENGTH, "0").body(empty_body()).unwrap(),
        po::Success::Empty(_) => out.status(StatusCode::NO_CONTENT).header(LOCATION, location.as_str()).body(empty_body()).unwrap(),
    })
}
