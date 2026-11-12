//! awan-storage/server/src/xml.rs
//! S3 XML Serialization for Service, Bucket, Object, Batch and Error responses
//! Reference: awan-storage/API_SPECIFICATION.md Section 2 and awan-storage/EDGE_CASES_AND_CONTRACTS.md Section 3

pub fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// `request_id` is used only outside a request; inside one, the request's own ID is written.
pub fn s3_error_xml(code: &str, message: &str, resource: &str, request_id: &str) -> String {
    crate::request_log::note_error_code(code);
    let current = crate::request_log::current_request_id();
    let request_id = current.as_deref().unwrap_or(request_id);
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <Error>\n  \
          <Code>{}</Code>\n  \
          <Message>{}</Message>\n  \
          <Resource>{}</Resource>\n  \
          <RequestId>{}</RequestId>\n\
        </Error>",
        xml_escape(code),
        xml_escape(message),
        xml_escape(resource),
        xml_escape(request_id)
    )
}

pub fn list_buckets_xml(buckets: &[(&str, &str)], request_id: &str) -> String {
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
          <Owner>\n    \
            <ID>s3-storage-root</ID>\n    \
            <DisplayName>root</DisplayName>\n  \
          </Owner>\n  \
          <Buckets>\n"
    );
    for (name, created_at) in buckets {
        out.push_str("    <Bucket>\n");
        out.push_str(&format!("      <Name>{}</Name>\n", xml_escape(name)));
        out.push_str(&format!("      <CreationDate>{}</CreationDate>\n", created_at));
        out.push_str("    </Bucket>\n");
    }
    out.push_str("  </Buckets>\n</ListAllMyBucketsResult>");
    let _ = request_id;
    out
}

pub fn location_constraint_xml(region: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{}</LocationConstraint>",
        xml_escape(region)
    )
}

pub struct S3ObjectEntry<'a> {
    pub key: &'a str,
    pub last_modified: &'a str,
    pub etag: &'a str,
    pub size: u64,
}

pub fn list_objects_v2_xml(
    bucket: &str,
    prefix: &str,
    start_after: &str,
    max_keys: u32,
    is_truncated: bool,
    next_continuation_token: Option<&str>,
    contents: &[S3ObjectEntry],
    common_prefixes: &[&str],
) -> String {
    let mut out = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
          <Name>{}</Name>\n  \
          <Prefix>{}</Prefix>\n  \
          <StartAfter>{}</StartAfter>\n  \
          <MaxKeys>{}</MaxKeys>\n  \
          <KeyCount>{}</KeyCount>\n  \
          <IsTruncated>{}</IsTruncated>\n",
        xml_escape(bucket),
        xml_escape(prefix),
        xml_escape(start_after),
        max_keys,
        contents.len() + common_prefixes.len(),
        is_truncated
    );

    if let Some(token) = next_continuation_token {
        out.push_str(&format!("  <NextContinuationToken>{}</NextContinuationToken>\n", xml_escape(token)));
    }

    for obj in contents {
        out.push_str("  <Contents>\n");
        out.push_str(&format!("    <Key>{}</Key>\n", xml_escape(obj.key)));
        out.push_str(&format!("    <LastModified>{}</LastModified>\n", obj.last_modified));
        out.push_str(&format!("    <ETag>\"{}\"</ETag>\n", obj.etag));
        out.push_str(&format!("    <Size>{}</Size>\n", obj.size));
        out.push_str("    <StorageClass>STANDARD</StorageClass>\n");
        out.push_str("  </Contents>\n");
    }

    for cp in common_prefixes {
        out.push_str("  <CommonPrefixes>\n");
        out.push_str(&format!("    <Prefix>{}</Prefix>\n", xml_escape(cp)));
        out.push_str("  </CommonPrefixes>\n");
    }

    out.push_str("</ListBucketResult>");
    out
}

pub struct DeleteItemResult<'a> {
    pub key: &'a str,
    pub version_id: Option<&'a str>,
}

pub struct DeleteErrorItem<'a> {
    pub key: &'a str,
    pub code: &'a str,
    pub message: &'a str,
}

pub fn delete_objects_result_xml(
    deleted: &[DeleteItemResult],
    errors: &[DeleteErrorItem],
) -> String {
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n"
    );
    for item in deleted {
        out.push_str("  <Deleted>\n");
        out.push_str(&format!("    <Key>{}</Key>\n", xml_escape(item.key)));
        if let Some(v) = item.version_id {
            out.push_str(&format!("    <VersionId>{}</VersionId>\n", xml_escape(v)));
        }
        out.push_str("  </Deleted>\n");
    }
    for err in errors {
        out.push_str("  <Error>\n");
        out.push_str(&format!("    <Key>{}</Key>\n", xml_escape(err.key)));
        out.push_str(&format!("    <Code>{}</Code>\n", xml_escape(err.code)));
        out.push_str(&format!("    <Message>{}</Message>\n", xml_escape(err.message)));
        out.push_str("  </Error>\n");
    }
    out.push_str("</DeleteResult>");
    out
}

pub fn copy_object_result_xml(etag: &str, last_modified: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <CopyObjectResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  \
          <LastModified>{}</LastModified>\n  \
          <ETag>\"{}\"</ETag>\n\
        </CopyObjectResult>",
        last_modified,
        etag
    )
}
