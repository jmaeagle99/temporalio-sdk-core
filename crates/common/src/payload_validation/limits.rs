//! Configurable payload-size limits used by the gRPC payload-validation layer.
//!
//! A single [`PayloadSizeLimits`] type is used for both warning thresholds (always applied,
//! sourced from `ConnectionOptions`) and error thresholds (worker-only, sourced from the
//! server's `DescribeNamespace` response). Limits are expressed in bytes; a limit of `0`
//! disables that particular check.

use serde::{Deserialize, Serialize};

/// Per-message-type byte-size limits used by payload validation.
///
/// The same shape is used for both the warning thresholds (configured on the client) and the
/// error thresholds (returned by the server via `DescribeNamespace`).
///
/// A field set to `0` disables the corresponding check.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadSizeLimits {
    /// Maximum allowed encoded size, in bytes, for a single Payload-bearing field on a request
    /// (e.g. `input`, `result`, `details`, `last_completion_result`). Computed as the sum of
    /// each contained Payload's encoded length.
    pub payloads_size: usize,
    /// Maximum allowed encoded size, in bytes, for a Memo on a request — summed across all
    /// values in the memo's `fields` map.
    pub memo_size: usize,
}

impl PayloadSizeLimits {
    /// Construct an explicit `PayloadSizeLimits`.
    pub const fn new(payloads_size: usize, memo_size: usize) -> Self {
        Self {
            payloads_size,
            memo_size,
        }
    }

    /// A `PayloadSizeLimits` with all checks disabled.
    pub const fn disabled() -> Self {
        Self {
            payloads_size: 0,
            memo_size: 0,
        }
    }

    /// Returns `true` if the `payloads_size` check is enabled.
    pub const fn check_payloads_size(&self) -> bool {
        self.payloads_size != 0
    }

    /// Returns `true` if the `memo_size` check is enabled.
    pub const fn check_memo_size(&self) -> bool {
        self.memo_size != 0
    }
}

impl Default for PayloadSizeLimits {
    /// Sensible default warning thresholds: 2 MiB per payloads field, 32 KiB per memo.
    fn default() -> Self {
        Self {
            payloads_size: 2 * 1024 * 1024,
            memo_size: 32 * 1024,
        }
    }
}
