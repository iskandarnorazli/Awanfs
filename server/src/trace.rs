//! OpenTelemetry traces over OTLP/HTTP JSON (#43, owner decision 11).
//!
//! Requests and volume operations are `tracing` spans. With `--otlp-endpoint` (or
//! `OTEL_EXPORTER_OTLP_ENDPOINT`) set, `init` installs a subscriber whose layer turns closed
//! spans into OTLP spans and POSTs them in batches to `<endpoint>/v1/traces` as JSON, which
//! Grafana Tempo, Jaeger and the OpenTelemetry Collector accept. A W3C `traceparent` header on
//! the request continues the caller's trace. Off by default: with no subscriber installed the
//! span macros do nothing.

use crate::request_log::json_escape;
use bytes::Bytes;
use http_body_util::Full;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// One span ready for export.
#[derive(Debug, Clone)]
pub struct ExportSpan {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_id: Option<[u8; 8]>,
    pub name: String,
    pub start_ns: u64,
    pub end_ns: u64,
    pub attributes: Vec<(String, String)>,
    pub server: bool,
}

struct SpanData {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_id: Option<[u8; 8]>,
    start_ns: u64,
    attributes: Vec<(String, String)>,
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

static SEQ: AtomicU64 = AtomicU64::new(1);

/// Distinct ids without a random-number crate: xxh3 of time, a counter and the process id.
fn random_u64() -> u64 {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let mut buf = [0u8; 24];
    buf[..8].copy_from_slice(&now_ns().to_le_bytes());
    buf[8..16].copy_from_slice(&n.to_le_bytes());
    buf[16..20].copy_from_slice(&std::process::id().to_le_bytes());
    xxhash_rust::xxh3::xxh3_64(&buf).max(1)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

/// `00-<trace id>-<parent span id>-<flags>` (W3C Trace Context).
pub fn parse_traceparent(v: &str) -> Option<([u8; 16], [u8; 8])> {
    let parts: Vec<&str> = v.trim().split('-').collect();
    if parts.len() != 4 || parts[1].len() != 32 || parts[2].len() != 16 {
        return None;
    }
    let mut t = [0u8; 16];
    let mut s = [0u8; 8];
    for (i, b) in t.iter_mut().enumerate() {
        *b = u8::from_str_radix(&parts[1][2 * i..2 * i + 2], 16).ok()?;
    }
    for (i, b) in s.iter_mut().enumerate() {
        *b = u8::from_str_radix(&parts[2][2 * i..2 * i + 2], 16).ok()?;
    }
    if t == [0; 16] || s == [0; 8] {
        return None;
    }
    Some((t, s))
}

struct Fields<'a>(&'a mut Vec<(String, String)>);

impl Visit for Fields<'_> {
    fn record_str(&mut self, f: &Field, v: &str) {
        self.0.retain(|(k, _)| k != f.name());
        self.0.push((f.name().to_string(), v.to_string()));
    }
    fn record_u64(&mut self, f: &Field, v: u64) {
        self.record_str(f, &v.to_string());
    }
    fn record_i64(&mut self, f: &Field, v: i64) {
        self.record_str(f, &v.to_string());
    }
    fn record_bool(&mut self, f: &Field, v: bool) {
        self.record_str(f, &v.to_string());
    }
    fn record_debug(&mut self, f: &Field, v: &dyn std::fmt::Debug) {
        self.record_str(f, &format!("{:?}", v));
    }
}

/// The layer that records spans and hands closed ones to the exporter.
pub struct OtlpLayer {
    tx: mpsc::Sender<ExportSpan>,
}

impl<S> Layer<S> for OtlpLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut attributes = Vec::new();
        attrs.record(&mut Fields(&mut attributes));
        let parent = span.parent().and_then(|p| p.extensions().get::<SpanData>().map(|d| (d.trace_id, d.span_id)));
        let (trace_id, parent_id) = match parent {
            Some((t, s)) => (t, Some(s)),
            None => match attributes.iter().find(|(k, _)| k == "traceparent").and_then(|(_, v)| parse_traceparent(v)) {
                Some((t, s)) => (t, Some(s)),
                None => {
                    let mut t = [0u8; 16];
                    t[..8].copy_from_slice(&random_u64().to_be_bytes());
                    t[8..].copy_from_slice(&random_u64().to_be_bytes());
                    (t, None)
                }
            },
        };
        attributes.retain(|(k, v)| k != "traceparent" && !v.is_empty());
        span.extensions_mut().insert(SpanData { trace_id, span_id: random_u64().to_be_bytes(), parent_id, start_ns: now_ns(), attributes });
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            if let Some(d) = span.extensions_mut().get_mut::<SpanData>() {
                values.record(&mut Fields(&mut d.attributes));
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let ext = span.extensions();
        let Some(d) = ext.get::<SpanData>() else { return };
        let _ = self.tx.try_send(ExportSpan {
            trace_id: d.trace_id,
            span_id: d.span_id,
            parent_id: d.parent_id,
            name: span.name().to_string(),
            start_ns: d.start_ns,
            end_ns: now_ns(),
            attributes: d.attributes.clone(),
            server: span.parent().is_none(),
        });
    }
}

/// The OTLP JSON body for a batch of spans (protobuf JSON mapping, ids as hex).
pub fn otlp_json(service: &str, spans: &[ExportSpan]) -> String {
    let mut out = format!(
        "{{\"resourceSpans\":[{{\"resource\":{{\"attributes\":[{{\"key\":\"service.name\",\"value\":{{\"stringValue\":\"{}\"}}}}]}},\"scopeSpans\":[{{\"scope\":{{\"name\":\"awanfs\"}},\"spans\":[",
        json_escape(service)
    );
    for (i, s) in spans.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(
            out,
            "{{\"traceId\":\"{}\",\"spanId\":\"{}\",{}\"name\":\"{}\",\"kind\":{},\"startTimeUnixNano\":\"{}\",\"endTimeUnixNano\":\"{}\",\"attributes\":[",
            hex(&s.trace_id),
            hex(&s.span_id),
            s.parent_id.map(|p| format!("\"parentSpanId\":\"{}\",", hex(&p))).unwrap_or_default(),
            json_escape(&s.name),
            if s.server { 2 } else { 1 },
            s.start_ns,
            s.end_ns
        );
        for (j, (k, v)) in s.attributes.iter().enumerate() {
            if j > 0 {
                out.push(',');
            }
            let _ = write!(out, "{{\"key\":\"{}\",\"value\":{{\"stringValue\":\"{}\"}}}}", json_escape(k), json_escape(v));
        }
        out.push_str("]}");
    }
    out.push_str("]}]}]}");
    out
}

/// Installs the exporting subscriber for the whole process and starts its exporter. Call once,
/// inside a tokio runtime. Spans are sent every second or every 256 spans; a full queue
/// (4096) drops spans rather than slow requests.
pub fn init(endpoint: &str, service: &str) -> Result<(), String> {
    use tracing_subscriber::layer::SubscriberExt;
    let (tx, mut rx) = mpsc::channel::<ExportSpan>(4096);
    let subscriber = tracing_subscriber::registry().with(OtlpLayer { tx });
    tracing::subscriber::set_global_default(subscriber).map_err(|e| e.to_string())?;
    let url: hyper::Uri = format!("{}/v1/traces", endpoint.trim_end_matches('/')).parse().map_err(|e| format!("OTLP endpoint: {}", e))?;
    let service = service.to_string();
    tokio::spawn(async move {
        let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new()).build_http::<Full<Bytes>>();
        let mut batch = Vec::new();
        loop {
            let tick = tokio::time::sleep(std::time::Duration::from_secs(1));
            tokio::pin!(tick);
            loop {
                tokio::select! {
                    s = rx.recv() => match s {
                        Some(s) => { batch.push(s); if batch.len() >= 256 { break; } }
                        None => return,
                    },
                    _ = &mut tick => break,
                }
            }
            if batch.is_empty() {
                continue;
            }
            let body = otlp_json(&service, &batch);
            batch.clear();
            let req = hyper::Request::post(url.clone()).header("content-type", "application/json").body(Full::new(Bytes::from(body)));
            if let Ok(req) = req {
                if let Err(e) = client.request(req).await {
                    eprintln!("OTLP export failed: {}", e);
                }
            }
        }
    });
    Ok(())
}
