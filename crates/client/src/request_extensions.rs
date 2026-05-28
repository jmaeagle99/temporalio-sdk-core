//! Request extensions for tonic gRPC requests.
//!
//! These types can be inserted into tonic request extensions to modify behavior of retires or other
//! request handling logic.

use crate::RetryOptions;
use std::time::Duration;
use temporalio_common::payload_validation::PayloadSizeLimits;

/// A request extension that, when set, should make the retry behavior consider this call to be a
/// [CallType::TaskLongPoll](crate::CallType::TaskLongPoll)
#[derive(Copy, Clone, Debug)]
pub struct IsWorkerTaskLongPoll;

/// A request extension that, when set, and a call is being processed by a retrying client, allows
/// the caller to request certain matching errors to short-circuit-return immediately and not follow
/// normal retry logic.
#[derive(Copy, Clone, Debug)]
pub struct NoRetryOnMatching {
    /// Return true if the passed-in gRPC error should be immediately returned to the caller
    pub predicate: fn(&tonic::Status) -> bool,
}

/// A request extension that forces overriding the current retry policy of the [crate::Connection].
#[derive(Clone, Debug)]
pub struct RetryConfigForCall(pub RetryOptions);

/// A request extension that signals the payload validator to also enforce error-level
/// (server-defined) limits and short-circuit calls that exceed them.
///
/// Inserted by the worker layer on every outbound call after the worker has fetched the
/// namespace's limits via `DescribeNamespace`. Absent on client-only paths, which keeps
/// the validator in warnings-only mode.
#[derive(Clone, Copy, Debug)]
pub struct WorkerValidationOverride {
    /// Per-namespace size limits returned by the server.
    pub error_limits: PayloadSizeLimits,
}

/// Extension trait for tonic requests to set default timeouts
pub trait RequestExt {
    /// Set a timeout for a request if one is not already specified in the metadata
    fn set_default_timeout(&mut self, duration: Duration);
}

impl<T> RequestExt for tonic::Request<T> {
    fn set_default_timeout(&mut self, duration: Duration) {
        if !self.metadata().contains_key("grpc-timeout") {
            self.set_timeout(duration)
        }
    }
}
