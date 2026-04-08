//! Client-side payload size warning checks for WorkflowService calls.

use crate::grpc::WorkflowService;
use futures_util::future::BoxFuture;
use prost::Message;
use temporalio_common::{
    payload_visitor::{
        AsyncPayloadVisitor, MemoObserver, MemoField, MemoVisitable, PayloadField,
        PayloadFieldData, PayloadVisitable,
    },
    protos::temporal::api::workflowservice::v1::*,
};
use tonic::{Request, Response, Status};

/// Severity of a payload-limit violation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LimitSeverity {
    /// Exceeded the hard error limit.
    Error,
    /// Exceeded the soft warning limit.
    Warn,
}

/// A single payload-limit violation.
#[derive(Debug, Clone)]
pub struct LimitExceeded {
    /// Whether this is an error or warning violation.
    pub severity: LimitSeverity,
    /// Payload container name (e.g., `"payloads"`, `"memo"`).
    pub kind: &'static str,
    /// Measured encoded size in bytes.
    pub size: u64,
    /// Configured limit in bytes.
    pub limit: u64,
}

impl LimitExceeded {
    /// Format the TMPRL1103 message appropriate for this violation's severity.
    pub fn message(&self) -> String {
        let level = match self.severity {
            LimitSeverity::Error => "error",
            LimitSeverity::Warn => "warning",
        };
        format!(
            "[TMPRL1103] Attempted to upload {} with size that exceeded the \
             {level} limit.",
            self.kind
        )
    }
}

/// Result of a payload limit check pass.
pub struct PayloadLimitResult {
    /// The first error-threshold violation, if any.
    pub error_exceeded: Option<LimitExceeded>,
    /// The first warning-threshold violation, if any.
    pub warn_exceeded: Option<LimitExceeded>,
}

pub(crate) struct PayloadLimitVisitor {
    payload_error_limit: Option<u64>,
    memo_error_limit: Option<u64>,
    payload_warn_limit: u64,
    memo_warn_limit: u64,
    error_exceeded: Option<LimitExceeded>,
    warn_exceeded: Option<LimitExceeded>,
}

impl PayloadLimitVisitor {
    pub(crate) fn new(
        payload_error_limit: Option<u64>,
        memo_error_limit: Option<u64>,
        payload_warn_limit: u64,
        memo_warn_limit: u64,
    ) -> Self {
        Self {
            payload_error_limit,
            memo_error_limit,
            payload_warn_limit,
            memo_warn_limit,
            error_exceeded: None,
            warn_exceeded: None,
        }
    }

    fn check(&mut self, size: u64, error_limit: Option<u64>, warn_limit: u64, kind: &'static str) {
        if self.error_exceeded.is_some() {
            return;
        }
        if let Some(limit) = error_limit {
            if size > limit {
                self.error_exceeded = Some(LimitExceeded {
                    severity: LimitSeverity::Error,
                    kind,
                    size,
                    limit,
                });
                return;
            }
        }
        if self.warn_exceeded.is_none() && size > warn_limit {
            self.warn_exceeded = Some(LimitExceeded {
                severity: LimitSeverity::Warn,
                kind,
                size,
                limit: warn_limit,
            });
        }
    }
}

impl AsyncPayloadVisitor for PayloadLimitVisitor {
    fn visit<'a>(&'a mut self, field: PayloadField<'a>) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let error_lim = self.payload_error_limit;
            let warn_lim = self.payload_warn_limit;
            match &field.data {
                PayloadFieldData::Payloads(msg) => {
                    let size = msg.encoded_len() as u64;
                    self.check(size, error_lim, warn_lim, "payloads");
                }
                PayloadFieldData::Single(p) => {
                    let size = p.encoded_len() as u64;
                    self.check(size, error_lim, warn_lim, "payloads");
                }
                PayloadFieldData::Repeated(ps) => {
                    for p in ps.iter() {
                        let size = p.encoded_len() as u64;
                        self.check(size, error_lim, warn_lim, "payloads");
                        if self.error_exceeded.is_some() {
                            break;
                        }
                    }
                }
            }
        })
    }
}

impl MemoObserver for PayloadLimitVisitor {
    fn observe_memo<'a>(&'a mut self, field: MemoField<'a>) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let size = field.memo.encoded_len() as u64;
            let error_lim = self.memo_error_limit;
            let warn_lim = self.memo_warn_limit;
            self.check(size, error_lim, warn_lim, "memo");
        })
    }
}

/// Checks payload and memo sizes against warn and error limits.
pub async fn check_payload_limits<M: PayloadVisitable + MemoVisitable + Send>(
    msg: &mut M,
    payload_error_limit: Option<u64>,
    memo_error_limit: Option<u64>,
    payload_warn_limit: u64,
    memo_warn_limit: u64,
) -> PayloadLimitResult {
    let mut v = PayloadLimitVisitor::new(
        payload_error_limit,
        memo_error_limit,
        payload_warn_limit,
        memo_warn_limit,
    );
    msg.visit_memos_mut(&mut v).await;
    msg.visit_payloads_mut(&mut v).await;
    PayloadLimitResult {
        error_exceeded: v.error_exceeded,
        warn_exceeded: v.warn_exceeded,
    }
}

/// A [`WorkflowService`] wrapper that logs warnings for outbound calls with oversized payloads or memos.
#[derive(Clone)]
pub(crate) struct PayloadCheckingWorkflowService {
    pub(crate) inner: Box<dyn WorkflowService>,
    pub(crate) payload_warn: u64,
    pub(crate) memo_warn: u64,
}

/// Generates passthrough [`WorkflowService`] impls for methods with no user payloads.
macro_rules! forward_workflow_methods {
    ($(($method:ident, $req:ty, $resp:ty));* $(;)?) => {
        $(
            fn $method(
                &mut self,
                request: Request<$req>,
            ) -> futures_util::future::BoxFuture<'_, Result<Response<$resp>, Status>> {
                self.inner.$method(request)
            }
        )*
    };
}

/// Generates [`WorkflowService`] impls that check payload sizes before forwarding.
macro_rules! check_and_forward_workflow_methods {
    ($(($method:ident, $req:ty, $resp:ty));* $(;)?) => {
        $(
            fn $method(
                &mut self,
                mut request: Request<$req>,
            ) -> futures_util::future::BoxFuture<'_, Result<Response<$resp>, Status>> {
                let pw = self.payload_warn;
                let mw = self.memo_warn;
                let mut inner = self.inner.clone();
                Box::pin(async move {
                    let result = check_payload_limits(
                        request.get_mut(),
                        None,
                        None,
                        pw,
                        mw,
                    )
                    .await;
                    if let Some(exceeded) = result.warn_exceeded {
                        warn!(
                            payload_size = exceeded.size,
                            payload_size_limit = exceeded.limit,
                            "{}",
                            exceeded.message(),
                        );
                    }
                    inner.$method(request).await
                })
            }
        )*
    };
}

include!(concat!(
    env!("OUT_DIR"),
    "/payload_checking_service_impl.rs"
));

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_common::protos::temporal::api::{
        common::v1::{Memo, Payload, Payloads},
        workflowservice::v1::StartWorkflowExecutionRequest,
    };

    fn make_payload(size: usize) -> Payload {
        Payload {
            data: vec![0u8; size],
            ..Default::default()
        }
    }

    fn make_memo(value_size: usize) -> Memo {
        let mut m = Memo::default();
        m.fields.insert("key".to_string(), make_payload(value_size));
        m
    }

    // --- check_payload_limits ---

    #[tokio::test]
    async fn no_error_limits_no_violation() {
        let mut p = make_payload(1000);
        let r = check_payload_limits(&mut p, None, None, u64::MAX, u64::MAX).await;
        assert!(r.error_exceeded.is_none());
        assert!(r.warn_exceeded.is_none());
    }

    #[tokio::test]
    async fn below_limits_no_violation() {
        let mut p = make_payload(10); // tiny payload
        let r = check_payload_limits(&mut p, Some(1000), Some(1000), 500, 500).await;
        assert!(r.error_exceeded.is_none());
        assert!(r.warn_exceeded.is_none());
    }

    #[tokio::test]
    async fn payload_warn_limit_exceeded() {
        let mut p = make_payload(100);
        // warn = 50 (exceeded), error = 2000 (not exceeded)
        let r = check_payload_limits(&mut p, Some(2000), None, 50, u64::MAX).await;
        assert!(r.error_exceeded.is_none());
        let w = r.warn_exceeded.expect("warn_exceeded should be set");
        assert_eq!(w.kind, "payloads");
        assert_eq!(w.severity, LimitSeverity::Warn);
        assert!(w.size > 50);
        assert_eq!(w.limit, 50);
    }

    #[tokio::test]
    async fn payload_error_limit_exceeded() {
        let mut p = make_payload(100);
        // error = 50 (exceeded)
        let r = check_payload_limits(&mut p, Some(50), None, u64::MAX, u64::MAX).await;
        let e = r.error_exceeded.expect("error_exceeded should be set");
        assert_eq!(e.kind, "payloads");
        assert_eq!(e.severity, LimitSeverity::Error);
        assert!(e.size > 50);
        assert_eq!(e.limit, 50);
    }

    fn make_start_request_with_memo(memo_value_size: usize) -> StartWorkflowExecutionRequest {
        StartWorkflowExecutionRequest {
            memo: Some(make_memo(memo_value_size)),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn memo_warn_limit_exceeded() {
        // Use StartWorkflowExecutionRequest because Memo's own MemoVisitable is a no-op;
        // the observer fires for messages that *contain* a Memo field.
        let mut req = make_start_request_with_memo(100);
        let r = check_payload_limits(&mut req, None, Some(2000), u64::MAX, 50).await;
        assert!(r.error_exceeded.is_none());
        let w = r.warn_exceeded.expect("warn_exceeded should be set");
        assert_eq!(w.kind, "memo");
        assert_eq!(w.severity, LimitSeverity::Warn);
    }

    #[tokio::test]
    async fn memo_error_limit_exceeded() {
        let mut req = make_start_request_with_memo(100);
        let r = check_payload_limits(&mut req, None, Some(50), u64::MAX, u64::MAX).await;
        let e = r.error_exceeded.expect("error_exceeded should be set");
        assert_eq!(e.kind, "memo");
        assert_eq!(e.severity, LimitSeverity::Error);
    }

    #[tokio::test]
    async fn memo_checked_before_payload() {
        // StartWorkflowExecutionRequest has both memo and input (payloads). When both
        // exceed the error limit, memo should be reported because memos are visited first.
        let mut req = StartWorkflowExecutionRequest {
            memo: Some(make_memo(100)),
            input: Some(Payloads { payloads: vec![make_payload(100)] }),
            ..Default::default()
        };
        let r = check_payload_limits(&mut req, Some(50), Some(50), u64::MAX, u64::MAX).await;
        let e = r.error_exceeded.expect("error_exceeded should be set");
        assert_eq!(e.kind, "memo", "memo should be reported first");
    }

    #[tokio::test]
    async fn only_first_warn_captured() {
        // A Payloads message with two large payloads — both exceed the warn limit.
        // Only the first violation should be recorded.
        let mut ps = Payloads {
            payloads: vec![make_payload(100), make_payload(200)],
        };
        // warn = 50; Payloads.encoded_len() is the combined size, so this fires once.
        let r = check_payload_limits(&mut ps, None, None, 50, u64::MAX).await;
        assert!(r.error_exceeded.is_none());
        let w = r.warn_exceeded.expect("warn_exceeded should be set");
        // Only one warn, not overwritten by the second payload.
        assert_eq!(w.limit, 50);
    }

    // --- LimitExceeded::message ---

    #[test]
    fn limit_exceeded_message_error() {
        let e = LimitExceeded {
            severity: LimitSeverity::Error,
            kind: "payloads",
            size: 2048,
            limit: 1024,
        };
        let msg = e.message();
        assert!(msg.contains("TMPRL1103"), "message should contain TMPRL1103: {msg}");
        assert!(msg.contains("error"), "message should contain 'error': {msg}");
    }

    #[test]
    fn limit_exceeded_message_warn() {
        let e = LimitExceeded {
            severity: LimitSeverity::Warn,
            kind: "memo",
            size: 2048,
            limit: 1024,
        };
        let msg = e.message();
        assert!(msg.contains("TMPRL1103"), "message should contain TMPRL1103: {msg}");
        assert!(msg.contains("warning"), "message should contain 'warning': {msg}");
    }

    /// Verify that every entry in `EXCLUDED_FROM_CHECK` in `build.rs` refers to a real
    /// payload-carrying RPC. A stale exclusion would be silently dead — this catches that.
    #[test]
    fn excluded_from_check_entries_are_valid_payload_rpcs() {
        let build_src = include_str!("../build.rs");
        // Extract the slice literal body of EXCLUDED_FROM_CHECK.
        let excluded_block = build_src
            .split("const EXCLUDED_FROM_CHECK")
            .nth(1)
            .and_then(|s| s.split("];").next())
            .expect("EXCLUDED_FROM_CHECK constant not found in build.rs");
        let excluded: Vec<&str> = excluded_block
            .lines()
            .filter_map(|line| {
                let trimmed = line.trim();
                if trimmed.starts_with('"') {
                    trimmed.split('"').nth(1)
                } else {
                    None
                }
            })
            .collect();

        assert!(
            !excluded.is_empty(),
            "Could not parse any entries from EXCLUDED_FROM_CHECK in build.rs — check the format"
        );

        let payload_rpcs = temporalio_common::payload_visitor::WORKFLOW_SERVICE_PAYLOAD_RPC_NAMES;
        for &name in &excluded {
            assert!(
                payload_rpcs.contains(&name),
                "EXCLUDED_FROM_CHECK entry '{name}' in build.rs is not a known payload-carrying \
                 RPC. Either the RPC was renamed/removed, or it never carried payloads. Remove \
                 it from the exclusion list."
            );
        }
    }
}
