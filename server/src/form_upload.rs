//! Streaming `multipart/form-data` parser for S3 POST Object (#193).
//!
//! Parses a multipart body incrementally with bounded buffers:
//! - Non-file field values are buffered up to `limits.max_field_bytes` (default 20 KiB).
//! - At most `limits.max_fields` (default 100) non-file fields are accepted.
//! - Field names are compared case-insensitively; duplicate fields are refused.
//! - The `file` part must be the last part; its bytes are streamed out as `FileData(Bytes)`
//!   without buffering more than the boundary lookahead (`boundary.len() + 3`).

use std::collections::HashSet;

use bytes::Bytes;

use crate::fsm::{form_step, FormInput, FormState, Refusal, FORM_MALFORMED};

/// Default maximum size of a single form field value (20 KiB).
pub const DEFAULT_MAX_FIELD_BYTES: usize = 20 * 1024;
/// Default maximum number of non-file form fields.
pub const DEFAULT_MAX_FIELDS: usize = 100;
/// Default maximum size of a single part's headers (8 KiB).
pub const DEFAULT_MAX_HEADER_BYTES: usize = 8 * 1024;

const MALFORMED_POST_REQUEST: Refusal = FORM_MALFORMED;
const MAX_PRE_DATA_EXCEEDED: Refusal = Refusal {
    code: "MaxPostPreDataLengthExceededError",
    status: 400,
    why: "your POST request fields preceding the upload file were too large",
};

/// Configurable limits for [`FormParser`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FormLimits {
    pub max_field_bytes: usize,
    pub max_fields: usize,
    pub max_header_bytes: usize,
}

impl Default for FormLimits {
    fn default() -> Self {
        Self {
            max_field_bytes: DEFAULT_MAX_FIELD_BYTES,
            max_fields: DEFAULT_MAX_FIELDS,
            max_header_bytes: DEFAULT_MAX_HEADER_BYTES,
        }
    }
}

/// Events emitted by [`FormParser::push`] and [`FormParser::finish`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FormEvent {
    Field {
        name: String,
        value: String,
    },
    FileStart {
        filename: Option<String>,
        content_type: Option<String>,
    },
    FileData(Bytes),
    FileEnd,
    Done,
}

/// Extracts the `boundary` parameter from a `Content-Type: multipart/form-data; ...` header.
/// Supports both quoted (`boundary="abc"`) and unquoted (`boundary=abc`) values.
pub fn extract_boundary(content_type: &str) -> Result<String, Refusal> {
    let mut parts = split_header_params(content_type);
    let media_type = parts.next().unwrap_or("").trim();
    if !media_type.eq_ignore_ascii_case("multipart/form-data") {
        return Err(MALFORMED_POST_REQUEST);
    }
    for param in parts {
        let Some((k, v)) = param.split_once('=') else {
            continue;
        };
        if !k.trim().eq_ignore_ascii_case("boundary") {
            continue;
        }
        let val = parse_param_value(v.trim())?;
        if val.is_empty() || val.contains('\r') || val.contains('\n') {
            return Err(MALFORMED_POST_REQUEST);
        }
        return Ok(val);
    }
    Err(MALFORMED_POST_REQUEST)
}

/// Splits a header value on `;` outside of double-quoted strings.
fn split_header_params(s: &str) -> impl Iterator<Item = &str> {
    let bytes = s.as_bytes();
    let mut start = 0;
    let mut i = 0;
    let mut in_quotes = false;
    let mut escaped = false;
    let mut slices = Vec::new();
    while i < bytes.len() {
        let b = bytes[i];
        if escaped {
            escaped = false;
        } else if b == b'\\' && in_quotes {
            escaped = true;
        } else if b == b'"' {
            in_quotes = !in_quotes;
        } else if b == b';' && !in_quotes {
            slices.push(&s[start..i]);
            start = i + 1;
        }
        i += 1;
    }
    slices.push(&s[start..]);
    slices.into_iter()
}

fn parse_param_value(raw: &str) -> Result<String, Refusal> {
    if let Some(inner) = raw.strip_prefix('"') {
        let Some(inner) = inner.strip_suffix('"') else {
            return Err(MALFORMED_POST_REQUEST);
        };
        let mut out = String::with_capacity(inner.len());
        let mut chars = inner.chars();
        while let Some(c) = chars.next() {
            if c == '\\' {
                out.push(chars.next().ok_or(MALFORMED_POST_REQUEST)?);
            } else {
                out.push(c);
            }
        }
        Ok(out)
    } else {
        if raw.contains('"') || raw.chars().any(char::is_whitespace) {
            return Err(MALFORMED_POST_REQUEST);
        }
        Ok(raw.to_string())
    }
}


/// Streaming `multipart/form-data` parser with bounded internal buffers.
pub struct FormParser {
    boundary_invalid: bool,
    first_delim: Vec<u8>,
    delim: Vec<u8>,
    limits: FormLimits,
    state: FormState,
    /// Still at the very start of the body (the first delimiter has no CRLF before it).
    at_start: bool,
    /// The field whose value is being read.
    field_name: Option<String>,
    seen_fields: HashSet<String>,
    field_count: usize,
    file_seen: bool,
    buf: Vec<u8>,
    /// Bytes fed to `push` so far.
    pushed: u64,
    /// Offset in the body of the file part's first byte, once its headers are read.
    file_data_offset: Option<u64>,
}

impl FormParser {
    pub fn new(boundary: &str, limits: FormLimits) -> Self {
        let boundary_invalid = boundary.is_empty() || boundary.contains('\r') || boundary.contains('\n');
        let mut first_delim = Vec::with_capacity(boundary.len() + 2);
        first_delim.extend_from_slice(b"--");
        first_delim.extend_from_slice(boundary.as_bytes());

        let mut delim = Vec::with_capacity(boundary.len() + 4);
        delim.extend_from_slice(b"\r\n");
        delim.extend_from_slice(&first_delim);

        Self {
            boundary_invalid,
            first_delim,
            delim,
            limits,
            state: FormState::Preamble,
            at_start: true,
            field_name: None,
            seen_fields: HashSet::new(),
            field_count: 0,
            file_seen: false,
            buf: Vec::new(),
            pushed: 0,
            file_data_offset: None,
        }
    }

    /// Number of bytes currently held in the parser's internal buffer.
    ///
    /// Bounded by `max(limits.max_field_bytes, limits.max_header_bytes, boundary.len() + 4)`
    /// (plus up to `boundary.len() + 3` lookahead bytes when a field of exact `max_field_bytes`
    /// sits at a partial boundary). During `FileBody`, at most `boundary.len() + 3` bytes are held.
    pub fn buffered_bytes(&self) -> usize {
        self.buf.len()
    }

    /// Offset in the body of the `file` part's first byte, known once its headers are read
    /// (#119: with the request's Content-Length it gives the file's size before it streams).
    pub fn file_data_offset(&self) -> Option<u64> {
        self.file_data_offset
    }

    /// Bytes of the closing delimiter that follow the file part: `\r\n--<boundary>--\r\n`.
    pub fn closing_len(&self) -> u64 {
        (self.delim.len() + 4) as u64
    }

    /// Feed the next chunk of the request body and return any events produced.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<FormEvent>, Refusal> {
        if self.boundary_invalid {
            return Err(MALFORMED_POST_REQUEST);
        }
        let mut events = Vec::new();
        if self.state == FormState::Epilogue {
            return Ok(events);
        }

        // Process in bounded slices so `self.buf` never grows proportional to huge chunks.
        let slice_cap = self.limits.max_field_bytes.max(self.limits.max_header_bytes).max(self.delim.len()).max(4096);
        for sub in chunk.chunks(slice_cap) {
            self.pushed += sub.len() as u64;
            self.buf.extend_from_slice(sub);
            self.advance(&mut events)?;
            if self.state == FormState::Epilogue {
                self.buf.clear();
                break;
            }
        }
        Ok(events)
    }

    /// Signal end-of-stream and verify the multipart body closed cleanly with a `file` part.
    pub fn finish(&mut self) -> Result<Vec<FormEvent>, Refusal> {
        if self.boundary_invalid {
            return Err(MALFORMED_POST_REQUEST);
        }
        match self.state {
            FormState::Epilogue => Ok(Vec::new()),
            _ => Err(MALFORMED_POST_REQUEST),
        }
    }

    /// Moves the parser on with `crate::fsm::form_step` (`STATE_MACHINES.md`, "POST object form
    /// parser"): this method finds delimiters and part headers in the bytes and tells the
    /// machine what it found; the machine decides the next state or refuses.
    fn step(&mut self, input: FormInput) -> Result<(), Refusal> {
        self.state = form_step(self.state, input, self.file_seen)?;
        Ok(())
    }

    fn advance(&mut self, events: &mut Vec<FormEvent>) -> Result<(), Refusal> {
        loop {
            match self.state {
                FormState::Preamble => {
                    if self.at_start && self.buf.starts_with(&self.first_delim) {
                        self.step(FormInput::Boundary)?;
                        continue;
                    }
                    if self.at_start && self.buf.len() < self.first_delim.len() && self.first_delim.starts_with(&self.buf) {
                        return Ok(());
                    }
                    if let Some(pos) = find_subslice(&self.buf, &self.delim) {
                        self.buf.drain(..pos + 2);
                        self.step(FormInput::Boundary)?;
                        continue;
                    }
                    let keep = self.delim.len().saturating_sub(1);
                    if self.buf.len() > keep {
                        let drop_n = self.buf.len() - keep;
                        self.buf.drain(..drop_n);
                    }
                    self.at_start = false;
                    return Ok(());
                }
                FormState::AfterBoundary => {
                    let need = self.first_delim.len() + 2;
                    if self.buf.len() < need {
                        return Ok(());
                    }
                    let suffix = &self.buf[self.first_delim.len()..need];
                    let input = match suffix {
                        b"\r\n" => FormInput::NextPart,
                        b"--" => FormInput::Close,
                        _ => FormInput::Malformed,
                    };
                    self.buf.drain(..need);
                    self.step(input)?;
                    if self.state == FormState::Epilogue {
                        events.push(FormEvent::Done);
                        return Ok(());
                    }
                }
                FormState::PartHeaders => {
                    let Some(hdr_end) = find_subslice(&self.buf, b"\r\n\r\n") else {
                        if self.buf.len() > self.limits.max_header_bytes {
                            return Err(MALFORMED_POST_REQUEST);
                        }
                        return Ok(());
                    };
                    if hdr_end > self.limits.max_header_bytes {
                        return Err(MALFORMED_POST_REQUEST);
                    }
                    let (name, filename, content_type) = parse_part_headers(&self.buf[..hdr_end])?;
                    self.buf.drain(..hdr_end + 4);

                    if name.eq_ignore_ascii_case("file") {
                        self.step(FormInput::FilePart)?;
                        self.file_seen = true;
                        self.file_data_offset = Some(self.pushed - self.buf.len() as u64);
                        events.push(FormEvent::FileStart { filename, content_type });
                    } else {
                        if self.field_count >= self.limits.max_fields {
                            return Err(MAX_PRE_DATA_EXCEEDED);
                        }
                        let lower = name.to_ascii_lowercase();
                        if !self.seen_fields.insert(lower) {
                            return Err(MALFORMED_POST_REQUEST);
                        }
                        self.step(FormInput::FieldPart)?;
                        self.field_count += 1;
                        self.field_name = Some(name);
                    }
                }
                FormState::FieldBody => {
                    if let Some(pos) = find_subslice(&self.buf, &self.delim) {
                        if pos > self.limits.max_field_bytes {
                            return Err(MAX_PRE_DATA_EXCEEDED);
                        }
                        let value = std::str::from_utf8(&self.buf[..pos])
                            .map_err(|_| MALFORMED_POST_REQUEST)?
                            .to_string();
                        self.buf.drain(..pos + 2);
                        self.step(FormInput::Boundary)?;
                        let name = self.field_name.take().unwrap_or_default();
                        events.push(FormEvent::Field { name, value });
                    } else {
                        let safe = self.buf.len().saturating_sub(self.delim.len() - 1);
                        if safe > self.limits.max_field_bytes {
                            return Err(MAX_PRE_DATA_EXCEEDED);
                        }
                        return Ok(());
                    }
                }
                FormState::FileBody => {
                    if let Some(pos) = find_subslice(&self.buf, &self.delim) {
                        if pos > 0 {
                            events.push(FormEvent::FileData(Bytes::copy_from_slice(&self.buf[..pos])));
                        }
                        events.push(FormEvent::FileEnd);
                        self.buf.drain(..pos + 2);
                        self.step(FormInput::Boundary)?;
                    } else {
                        let safe = self.buf.len().saturating_sub(self.delim.len() - 1);
                        if safe > 0 {
                            events.push(FormEvent::FileData(Bytes::copy_from_slice(&self.buf[..safe])));
                            self.buf.drain(..safe);
                        }
                        return Ok(());
                    }
                }
                FormState::Epilogue => return Ok(()),
            }
        }
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn parse_part_headers(raw: &[u8]) -> Result<(String, Option<String>, Option<String>), Refusal> {
    let text = std::str::from_utf8(raw).map_err(|_| MALFORMED_POST_REQUEST)?;
    let mut name: Option<String> = None;
    let mut filename: Option<String> = None;
    let mut content_type: Option<String> = None;
    let mut saw_disposition = false;

    for line in text.split("\r\n") {
        let Some((hdr_name, hdr_val)) = line.split_once(':') else {
            return Err(MALFORMED_POST_REQUEST);
        };
        let hdr_name = hdr_name.trim();
        let hdr_val = hdr_val.trim();

        if hdr_name.eq_ignore_ascii_case("Content-Disposition") {
            if saw_disposition {
                return Err(MALFORMED_POST_REQUEST);
            }
            saw_disposition = true;
            let mut params = split_header_params(hdr_val);
            let disp_type = params.next().unwrap_or("").trim();
            if !disp_type.eq_ignore_ascii_case("form-data") {
                return Err(MALFORMED_POST_REQUEST);
            }
            for param in params {
                let Some((k, v)) = param.split_once('=') else {
                    continue;
                };
                let k = k.trim();
                let v = v.trim();
                if k.eq_ignore_ascii_case("name") {
                    name = Some(parse_param_value(v)?);
                } else if k.eq_ignore_ascii_case("filename") {
                    filename = Some(parse_param_value(v)?);
                }
            }
        } else if hdr_name.eq_ignore_ascii_case("Content-Type") {
            content_type = Some(hdr_val.to_string());
        }
    }

    let name = name.filter(|n| !n.is_empty()).ok_or(MALFORMED_POST_REQUEST)?;
    Ok((name, filename, content_type))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn build_multipart(boundary: &str, parts: &[(&str, Option<&str>, Option<&str>, &[u8])]) -> Vec<u8> {
        let mut body = Vec::new();
        for (name, filename, content_type, data) in parts {
            body.extend_from_slice(b"--");
            body.extend_from_slice(boundary.as_bytes());
            body.extend_from_slice(b"\r\n");
            body.extend_from_slice(b"Content-Disposition: form-data; name=\"");
            body.extend_from_slice(name.as_bytes());
            body.push(b'"');
            if let Some(fname) = filename {
                body.extend_from_slice(b"; filename=\"");
                body.extend_from_slice(fname.as_bytes());
                body.push(b'"');
            }
            body.extend_from_slice(b"\r\n");
            if let Some(ct) = content_type {
                body.extend_from_slice(b"Content-Type: ");
                body.extend_from_slice(ct.as_bytes());
                body.extend_from_slice(b"\r\n");
            }
            body.extend_from_slice(b"\r\n");
            body.extend_from_slice(data);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(b"--");
        body.extend_from_slice(boundary.as_bytes());
        body.extend_from_slice(b"--\r\n");
        body
    }

    /// #119: the file's size is `Content-Length - file_data_offset - closing_len` for a body
    /// that ends at its closing delimiter, however the body is split into chunks.
    #[test]
    fn file_offset_and_closing_length_give_the_file_size_for_any_chunking() {
        let file = vec![b'x'; 10_000];
        let body = build_multipart(
            "b0undary",
            &[("key", None, None, b"foo.txt"), ("policy", None, None, b"p"), ("file", Some("f.txt"), Some("text/plain"), &file)],
        );
        for chunk in [1usize, 7, 64, 4096, body.len()] {
            let mut parser = FormParser::new("b0undary", FormLimits::default());
            for c in body.chunks(chunk) {
                parser.push(c).unwrap();
            }
            let off = parser.file_data_offset().expect("file part seen");
            assert_eq!(&body[off as usize..off as usize + 3], b"xxx", "chunk {}", chunk);
            assert_eq!(body.len() as u64 - off - parser.closing_len(), file.len() as u64, "chunk {}", chunk);
        }
        let mut no_file = FormParser::new("b0undary", FormLimits::default());
        no_file.push(b"--b0undary\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nk\r\n").unwrap();
        assert_eq!(no_file.file_data_offset(), None);
    }

    /// #119 invariant: the parser's memory is bounded by a constant whatever the body, here a
    /// 1 GiB file part fed in 1 MiB chunks.
    #[test]
    fn a_one_gib_file_part_keeps_the_parser_buffer_bounded() {
        let limits = FormLimits::default();
        let boundary = "b0undary";
        let bound = limits.max_field_bytes.max(limits.max_header_bytes).max(boundary.len() + 4) + boundary.len() + 4;
        let mut parser = FormParser::new(boundary, limits);
        let head = format!("--{b}\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nk\r\n--{b}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"f\"\r\n\r\n", b = boundary);
        parser.push(head.as_bytes()).unwrap();
        let chunk = vec![b'x'; 1 << 20];
        let mut file_bytes = 0u64;
        for _ in 0..1024 {
            for ev in parser.push(&chunk).unwrap() {
                if let FormEvent::FileData(b) = ev {
                    file_bytes += b.len() as u64;
                }
            }
            assert!(parser.buffered_bytes() <= bound, "{} > {}", parser.buffered_bytes(), bound);
        }
        let tail = format!("\r\n--{}--\r\n", boundary);
        for ev in parser.push(tail.as_bytes()).unwrap() {
            if let FormEvent::FileData(b) = ev {
                file_bytes += b.len() as u64;
            }
        }
        parser.finish().unwrap();
        assert_eq!(file_bytes, 1 << 30);
    }

    fn collect_all(
        boundary: &str,
        limits: FormLimits,
        chunks: &[&[u8]],
    ) -> Result<(Vec<(String, String)>, Option<Option<String>>, Option<Option<String>>, Vec<u8>, bool), Refusal> {
        let mut parser = FormParser::new(boundary, limits);
        let bound = limits
            .max_field_bytes
            .max(limits.max_header_bytes)
            .max(boundary.len() + 4)
            + boundary.len()
            + 4;
        let mut fields = Vec::new();
        let mut file_filename = None;
        let mut file_ct = None;
        let mut file_data = Vec::new();
        let mut file_ended = false;
        let mut done = false;

        for chunk in chunks {
            let events = parser.push(chunk)?;
            assert!(
                parser.buffered_bytes() <= bound,
                "buffered_bytes {} exceeded bound {}",
                parser.buffered_bytes(),
                bound
            );
            for ev in events {
                match ev {
                    FormEvent::Field { name, value } => fields.push((name, value)),
                    FormEvent::FileStart { filename, content_type } => {
                        file_filename = Some(filename);
                        file_ct = Some(content_type);
                    }
                    FormEvent::FileData(b) => file_data.extend_from_slice(&b),
                    FormEvent::FileEnd => file_ended = true,
                    FormEvent::Done => done = true,
                }
            }
        }
        for ev in parser.finish()? {
            match ev {
                FormEvent::Field { name, value } => fields.push((name, value)),
                FormEvent::FileStart { filename, content_type } => {
                    file_filename = Some(filename);
                    file_ct = Some(content_type);
                }
                FormEvent::FileData(b) => file_data.extend_from_slice(&b),
                FormEvent::FileEnd => file_ended = true,
                FormEvent::Done => done = true,
            }
        }
        assert!(done, "Done event must be emitted");
        assert!(file_ended, "FileEnd event must be emitted");
        Ok((fields, file_filename, file_ct, file_data, done))
    }

    #[test]
    fn extract_boundary_quoted_and_unquoted() {
        assert_eq!(
            extract_boundary("multipart/form-data; boundary=----WebKitFormBoundary123").unwrap(),
            "----WebKitFormBoundary123"
        );
        assert_eq!(
            extract_boundary("multipart/form-data; boundary=\"----quoted-boundary\"").unwrap(),
            "----quoted-boundary"
        );
        assert_eq!(
            extract_boundary("MULTIPART/FORM-DATA; charset=utf-8; BOUNDARY=\"a;b\"").unwrap(),
            "a;b"
        );
        assert_eq!(
            extract_boundary("application/json").unwrap_err().code,
            "MalformedPOSTRequest"
        );
        assert_eq!(
            extract_boundary("multipart/form-data; boundary=\"\"").unwrap_err().code,
            "MalformedPOSTRequest"
        );
        assert_eq!(
            extract_boundary("multipart/form-data").unwrap_err().code,
            "MalformedPOSTRequest"
        );
    }

    /// Table test derived from `test_s3.py` `test_post_object_*` cases:
    /// - `test_post_object_anonymous_request`
    /// - `test_post_object_upload_larger_than_chunk`
    /// - `test_post_object_set_key_from_filename`
    /// - `test_post_object_ignored_header`
    /// - `test_post_object_escaped_field_values`
    /// - `test_post_object_no_key_specified`
    #[test]
    fn table_test_s3_post_object_bodies() {
        struct Case {
            name: &'static str,
            parts: Vec<(&'static str, Option<&'static str>, Option<&'static str>, Vec<u8>)>,
            expected_fields: Vec<(&'static str, &'static str)>,
            expected_filename: Option<&'static str>,
            expected_file_len: usize,
        }

        let large_body = b"foo".repeat(1024 * 1024); // 3 MiB, like test_post_object_upload_larger_than_chunk
        let cases = vec![
            Case {
                name: "test_post_object_anonymous_request",
                parts: vec![
                    ("key", None, None, b"foo.txt".to_vec()),
                    ("acl", None, None, b"public-read".to_vec()),
                    ("Content-Type", None, None, b"text/plain".to_vec()),
                    ("file", Some("file"), None, b"bar".to_vec()),
                ],
                expected_fields: vec![
                    ("key", "foo.txt"),
                    ("acl", "public-read"),
                    ("Content-Type", "text/plain"),
                ],
                expected_filename: Some("file"),
                expected_file_len: 3,
            },
            Case {
                name: "test_post_object_upload_larger_than_chunk",
                parts: vec![
                    ("key", None, None, b"foo.txt".to_vec()),
                    ("AWSAccessKeyId", None, None, b"AKIAIOSFODNN7EXAMPLE".to_vec()),
                    ("acl", None, None, b"private".to_vec()),
                    ("signature", None, None, b"sig".to_vec()),
                    ("policy", None, None, b"pol".to_vec()),
                    ("Content-Type", None, None, b"text/plain".to_vec()),
                    ("file", Some("file"), None, large_body.clone()),
                ],
                expected_fields: vec![
                    ("key", "foo.txt"),
                    ("AWSAccessKeyId", "AKIAIOSFODNN7EXAMPLE"),
                    ("acl", "private"),
                    ("signature", "sig"),
                    ("policy", "pol"),
                    ("Content-Type", "text/plain"),
                ],
                expected_filename: Some("file"),
                expected_file_len: large_body.len(),
            },
            Case {
                name: "test_post_object_set_key_from_filename",
                parts: vec![
                    ("key", None, None, b"${filename}".to_vec()),
                    ("acl", None, None, b"private".to_vec()),
                    ("Content-Type", None, None, b"text/plain".to_vec()),
                    ("file", Some("foo.txt"), None, b"bar".to_vec()),
                ],
                expected_fields: vec![
                    ("key", "${filename}"),
                    ("acl", "private"),
                    ("Content-Type", "text/plain"),
                ],
                expected_filename: Some("foo.txt"),
                expected_file_len: 3,
            },
            Case {
                name: "test_post_object_ignored_header",
                parts: vec![
                    ("key", None, None, b"foo.txt".to_vec()),
                    ("acl", None, None, b"private".to_vec()),
                    ("Content-Type", None, None, b"text/plain".to_vec()),
                    ("x-ignore-foo", None, None, b"bar".to_vec()),
                    ("file", Some("file"), None, b"bar".to_vec()),
                ],
                expected_fields: vec![
                    ("key", "foo.txt"),
                    ("acl", "private"),
                    ("Content-Type", "text/plain"),
                    ("x-ignore-foo", "bar"),
                ],
                expected_filename: Some("file"),
                expected_file_len: 3,
            },
            Case {
                name: "test_post_object_escaped_field_values",
                parts: vec![
                    ("key", None, None, br"\$foo.txt".to_vec()),
                    ("acl", None, None, b"private".to_vec()),
                    ("Content-Type", None, None, b"text/plain".to_vec()),
                    ("file", Some("file"), None, b"bar".to_vec()),
                ],
                expected_fields: vec![
                    ("key", r"\$foo.txt"),
                    ("acl", "private"),
                    ("Content-Type", "text/plain"),
                ],
                expected_filename: Some("file"),
                expected_file_len: 3,
            },
            Case {
                name: "test_post_object_no_key_specified",
                parts: vec![
                    ("AWSAccessKeyId", None, None, b"AKIAIOSFODNN7EXAMPLE".to_vec()),
                    ("acl", None, None, b"private".to_vec()),
                    ("signature", None, None, b"sig".to_vec()),
                    ("policy", None, None, b"pol".to_vec()),
                    ("Content-Type", None, None, b"text/plain".to_vec()),
                    ("file", Some("file"), None, b"bar".to_vec()),
                ],
                expected_fields: vec![
                    ("AWSAccessKeyId", "AKIAIOSFODNN7EXAMPLE"),
                    ("acl", "private"),
                    ("signature", "sig"),
                    ("policy", "pol"),
                    ("Content-Type", "text/plain"),
                ],
                expected_filename: Some("file"),
                expected_file_len: 3,
            },
        ];

        for case in cases {
            let borrowed_parts: Vec<(&str, Option<&str>, Option<&str>, &[u8])> = case
                .parts
                .iter()
                .map(|(n, f, ct, d)| (*n, *f, *ct, d.as_slice()))
                .collect();
            let boundary = "----awanfsTestBoundary7d9f";
            let body = build_multipart(boundary, &borrowed_parts);
            let chunks: Vec<&[u8]> = body.chunks(4096).collect();
            let (fields, fname, _fct, fdata, _done) =
                collect_all(boundary, FormLimits::default(), &chunks)
                    .unwrap_or_else(|e| panic!("{}: unexpected error {:?}", case.name, e));
            let expected_fields: Vec<(String, String)> = case
                .expected_fields
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            assert_eq!(fields, expected_fields, "{}", case.name);
            assert_eq!(
                fname.unwrap().as_deref(),
                case.expected_filename,
                "{}",
                case.name
            );
            assert_eq!(fdata.len(), case.expected_file_len, "{}", case.name);
        }
    }

    #[test]
    fn duplicate_field_case_insensitive_refused() {
        let boundary = "b1";
        let body = build_multipart(
            boundary,
            &[
                ("Key", None, None, b"a"),
                ("key", None, None, b"b"),
                ("file", Some("f.txt"), None, b"data"),
            ],
        );
        let err = collect_all(boundary, FormLimits::default(), &[&body]).unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "MalformedPOSTRequest");
    }

    #[test]
    fn field_at_exact_limit_accepted_and_over_limit_refused() {
        let boundary = "b1";
        let exact = vec![b'x'; DEFAULT_MAX_FIELD_BYTES];
        let body_ok = build_multipart(
            boundary,
            &[("key", None, None, &exact), ("file", None, None, b"ok")],
        );
        let (fields, _, _, fdata, _) =
            collect_all(boundary, FormLimits::default(), &[&body_ok]).unwrap();
        assert_eq!(fields[0].1.len(), DEFAULT_MAX_FIELD_BYTES);
        assert_eq!(fdata, b"ok");

        let big = vec![b'x'; DEFAULT_MAX_FIELD_BYTES + 1];
        let body_err = build_multipart(
            boundary,
            &[("key", None, None, &big), ("file", None, None, b"ok")],
        );
        let err = collect_all(boundary, FormLimits::default(), &[&body_err]).unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "MaxPostPreDataLengthExceededError");
    }

    #[test]
    fn too_many_fields_refused() {
        let boundary = "b1";
        let mut owned = Vec::new();
        for i in 0..=DEFAULT_MAX_FIELDS {
            owned.push((format!("f{i}"), b"v".to_vec()));
        }
        let mut parts: Vec<(&str, Option<&str>, Option<&str>, &[u8])> = owned
            .iter()
            .map(|(k, v)| (k.as_str(), None, None, v.as_slice()))
            .collect();
        parts.push(("file", None, None, b"data"));
        let body = build_multipart(boundary, &parts);
        let err = collect_all(boundary, FormLimits::default(), &[&body]).unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "MaxPostPreDataLengthExceededError");
    }

    #[test]
    fn no_file_part_refused_with_invalid_argument() {
        let boundary = "b1";
        let body = build_multipart(boundary, &[("key", None, None, b"foo.txt")]);
        let err = collect_all(boundary, FormLimits::default(), &[&body]).unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "InvalidArgument");
    }

    #[test]
    fn file_not_last_part_refused() {
        let boundary = "b1";
        let body = build_multipart(
            boundary,
            &[
                ("file", Some("a.txt"), None, b"hello"),
                ("key", None, None, b"after"),
            ],
        );
        let err = collect_all(boundary, FormLimits::default(), &[&body]).unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "MalformedPOSTRequest");
    }

    #[test]
    fn truncated_body_refused_with_malformed() {
        let boundary = "b1";
        let body = build_multipart(boundary, &[("key", None, None, b"a"), ("file", None, None, b"data")]);
        let truncated = &body[..body.len() - 6];
        let err = collect_all(boundary, FormLimits::default(), &[truncated]).unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.code, "MalformedPOSTRequest");
    }

    #[test]
    fn preamble_ignored_and_empty_file_streamed() {
        let boundary = "b1";
        let mut body = b"preamble text before boundary\r\n".to_vec();
        body.extend_from_slice(&build_multipart(boundary, &[("file", Some("empty.bin"), None, b"")]));
        let chunks: Vec<&[u8]> = body.chunks(3).collect();
        let (_, fname, _, fdata, _) =
            collect_all(boundary, FormLimits::default(), &chunks).unwrap();
        assert_eq!(fname, Some(Some("empty.bin".to_string())));
        assert!(fdata.is_empty());
    }

    proptest! {
        #[test]
        fn prop_random_bodies_and_splits(
            boundary in "[a-zA-Z0-9_-]{1,32}",
            fields in prop::collection::vec(("[a-zA-Z][a-zA-Z0-9_-]{0,12}", "[a-zA-Z0-9 _-]{0,64}"), 0..6),
            file_data in prop::collection::vec(any::<u8>(), 0..512),
            splits in prop::collection::vec(1usize..17, 1..64),
        ) {
            let mut seen = HashSet::new();
            let mut unique_fields = Vec::new();
            for (k, v) in fields {
                let low = k.to_ascii_lowercase();
                if low != "file" && seen.insert(low) {
                    unique_fields.push((k, v));
                }
            }
            let mut parts: Vec<(&str, Option<&str>, Option<&str>, &[u8])> = unique_fields
                .iter()
                .map(|(k, v)| (k.as_str(), None, None, v.as_bytes()))
                .collect();
            parts.push(("file", Some("blob.bin"), Some("application/octet-stream"), &file_data));

            let body = build_multipart(&boundary, &parts);

            let mut chunks = Vec::new();
            let mut off = 0;
            let mut si = 0;
            while off < body.len() {
                let step = splits[si % splits.len()];
                let end = (off + step).min(body.len());
                chunks.push(&body[off..end]);
                off = end;
                si += 1;
            }

            let (got_fields, got_fname, got_ct, got_data, _) =
                collect_all(&boundary, FormLimits::default(), &chunks).unwrap();
            prop_assert_eq!(got_fields, unique_fields);
            prop_assert_eq!(got_fname, Some(Some("blob.bin".to_string())));
            prop_assert_eq!(got_ct, Some(Some("application/octet-stream".to_string())));
            prop_assert_eq!(got_data, file_data);
        }
    }
}
