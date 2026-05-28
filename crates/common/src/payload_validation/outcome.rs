//! Result types and context for payload validation.
//!
//! [`ValidationContext`] carries the warning/error limits and call metadata into each
//! generated `PayloadFieldValidator` method. [`FieldValidationOutcome`] is what each
//! per-field method returns; the dispatcher accumulates them into a [`ValidationOutcome`]
//! that the proxy layer turns into log entries (warnings) or a `tonic::Status` (errors).

use super::limits::PayloadSizeLimits;

/// Severity of a single validation issue.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Severity {
    /// Non-blocking — logged and passed through.
    Warning,
    /// Blocking — surfaces as `tonic::Status::invalid_argument`, request never leaves the
    /// process. Only emitted when the validation context has error limits set (i.e. worker
    /// mode and error enforcement not disabled).
    Error,
}

/// One detected payload issue.
#[derive(Clone, Debug)]
pub struct ValidationIssue {
    /// Severity of this issue.
    pub severity: Severity,
    /// Proto-qualified path to the offending field (e.g.
    /// `temporal.api.workflowservice.v1.StartWorkflowExecutionRequest.input`).
    pub field_path: &'static str,
    /// Observed encoded size in bytes, if applicable.
    pub observed_bytes: Option<usize>,
    /// Limit that was exceeded, in bytes, if applicable.
    pub limit_bytes: Option<usize>,
    /// Human-readable message.
    pub message: String,
}

/// Outcome of validating a single field — zero or more issues.
#[derive(Clone, Debug, Default)]
pub struct FieldValidationOutcome {
    /// Issues detected on the field.
    pub issues: Vec<ValidationIssue>,
}

impl FieldValidationOutcome {
    /// An outcome with no issues.
    pub const fn ok() -> Self {
        Self { issues: Vec::new() }
    }

    /// Add a single issue to this outcome.
    pub fn with_issue(mut self, issue: ValidationIssue) -> Self {
        self.issues.push(issue);
        self
    }

    /// `true` if any issue is present.
    pub fn has_issues(&self) -> bool {
        !self.issues.is_empty()
    }
}

/// Outcome of validating a whole request — aggregated from per-field results.
#[derive(Clone, Debug, Default)]
pub struct ValidationOutcome {
    /// Issues with severity [`Severity::Warning`].
    pub warnings: Vec<ValidationIssue>,
    /// Issues with severity [`Severity::Error`].
    pub errors: Vec<ValidationIssue>,
}

impl ValidationOutcome {
    /// An empty outcome.
    pub const fn empty() -> Self {
        Self {
            warnings: Vec::new(),
            errors: Vec::new(),
        }
    }

    /// `true` if there are any error-level issues.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// `true` if there are any warning-level issues.
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    /// Merge a per-field outcome into this aggregate.
    pub fn extend(&mut self, field: FieldValidationOutcome) {
        for issue in field.issues {
            match issue.severity {
                Severity::Warning => self.warnings.push(issue),
                Severity::Error => self.errors.push(issue),
            }
        }
    }

    /// Merge another [`ValidationOutcome`] into this one (used when a nested message's
    /// outcome bubbles up).
    pub fn merge(&mut self, other: ValidationOutcome) {
        self.warnings.extend(other.warnings);
        self.errors.extend(other.errors);
    }

    /// If error-level issues exist, build a `tonic::Status` that blocks the call.
    /// Otherwise returns `None`.
    pub fn to_status(&self) -> Option<tonic::Status> {
        if !self.has_errors() {
            return None;
        }
        let mut msg = String::from("payload validation failed:");
        for e in &self.errors {
            msg.push_str("\n  - ");
            msg.push_str(e.field_path);
            msg.push_str(": ");
            msg.push_str(&e.message);
        }
        Some(tonic::Status::invalid_argument(msg))
    }
}

/// Context passed into every generated per-field validator method.
///
/// Borrowed for the duration of the validation call; cheap to construct per request.
#[derive(Copy, Clone, Debug)]
pub struct ValidationContext<'a> {
    /// Warning thresholds — always applied. Source: `ConnectionOptions::payload_warning_limits`.
    pub warning_limits: &'a PayloadSizeLimits,
    /// Error thresholds. `Some` when the call is in worker mode and error enforcement has
    /// not been disabled; `None` otherwise (client mode, or `WorkerConfig::disable_payload_error_validation`).
    /// Source: `DescribeNamespace`'s namespace limits, populated during worker validate.
    pub error_limits: Option<&'a PayloadSizeLimits>,
}

impl<'a> ValidationContext<'a> {
    /// Construct a context with only warning checks enabled (client mode).
    pub const fn warnings_only(warning_limits: &'a PayloadSizeLimits) -> Self {
        Self {
            warning_limits,
            error_limits: None,
        }
    }

    /// Construct a context with both warning and error checks enabled (worker mode).
    pub const fn with_error_limits(
        warning_limits: &'a PayloadSizeLimits,
        error_limits: &'a PayloadSizeLimits,
    ) -> Self {
        Self {
            warning_limits,
            error_limits: Some(error_limits),
        }
    }
}
