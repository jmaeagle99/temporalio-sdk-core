//! Worker-specific client needs

pub(crate) mod mocks;
use crate::{protosext::legacy_query_failure, worker::WorkerVersioningStrategy};
use parking_lot::Mutex;
use prost_types::Duration as PbDuration;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};
use temporalio_client::{
    Connection, Namespace, NamespacedClient, RetryOptions, SharedReplaceableClient,
    grpc::WorkflowService,
    request_extensions::{IsWorkerTaskLongPoll, NoRetryOnMatching, RetryConfigForCall},
    worker::ClientWorkerSet,
};
use temporalio_client::{LimitExceeded, check_payload_limits};
use temporalio_common::protos::{
    TaskToken,
    coresdk::{workflow_commands::QueryResult, workflow_completion},
    temporal::api::{
        command::v1::Command,
        common::v1::{
            MeteringMetadata, Payloads, WorkerVersionCapabilities, WorkerVersionStamp,
            WorkflowExecution,
        },
        deployment,
        enums::v1::{
            TaskQueueKind, TaskQueueType, VersioningBehavior, WorkerVersioningMode,
            WorkflowTaskFailedCause,
        },
        failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
        nexus::{self, v1::NexusTaskFailure},
        protocol::v1::Message as ProtocolMessage,
        query::v1::WorkflowQueryResult,
        sdk::v1::WorkflowTaskCompletedMetadata,
        taskqueue::v1::{StickyExecutionAttributes, TaskQueue, TaskQueueMetadata},
        worker::v1::{WorkerHeartbeat, WorkerSlotsInfo},
        workflowservice::v1::{get_system_info_response::Capabilities, *},
    },
};
use tonic::IntoRequest;
use uuid::Uuid;

type Result<T, E = tonic::Status> = std::result::Result<T, E>;
type WorkflowTaskCompletionResult =
    std::result::Result<WorkflowTaskCompletionSuccess, WorkflowTaskCompletionError>;

/// Returned on the success path of [`WorkerClient::complete_workflow_task`].
#[derive(Debug, Default)]
pub struct WorkflowTaskCompletionSuccess {
    /// The server's response to the completed workflow task.
    pub response: RespondWorkflowTaskCompletedResponse,
    /// Payload limit warning information.
    pub warn_exceeded: Option<LimitExceeded>,
}

/// Error returned by [`WorkerClient::complete_workflow_task`].
#[derive(Debug)]
pub enum WorkflowTaskCompletionError {
    /// Payload size error limit exceeded; a WFT failure was already sent to the server.
    PayloadsTooLarge(LimitExceeded),
    /// Any other gRPC-level error.
    Rpc(tonic::Status),
}

impl From<tonic::Status> for WorkflowTaskCompletionError {
    fn from(s: tonic::Status) -> Self {
        WorkflowTaskCompletionError::Rpc(s)
    }
}

impl std::fmt::Display for WorkflowTaskCompletionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkflowTaskCompletionError::PayloadsTooLarge(e) => {
                write!(f, "payload size limit exceeded: {}", e.message())
            }
            WorkflowTaskCompletionError::Rpc(s) => write!(f, "gRPC error: {s}"),
        }
    }
}

impl std::error::Error for WorkflowTaskCompletionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WorkflowTaskCompletionError::Rpc(s) => Some(s),
            WorkflowTaskCompletionError::PayloadsTooLarge(_) => None,
        }
    }
}

/// Mirrors [`WorkflowTaskCompletionResult`] for activity task completions.
pub type ActivityTaskCompletionResult =
    std::result::Result<ActivityTaskCompletionSuccess, ActivityTaskCompletionError>;

/// Returned on the success path of [`WorkerClient::complete_activity_task`].
#[derive(Debug, Default)]
pub struct ActivityTaskCompletionSuccess {
    /// Set if the payload size warning threshold was crossed.
    pub warn_exceeded: Option<LimitExceeded>,
}

/// Error returned by [`WorkerClient::complete_activity_task`].
#[derive(Debug)]
pub enum ActivityTaskCompletionError {
    /// Payload size error limit exceeded; a task failure was already sent to the server.
    PayloadsTooLarge(LimitExceeded),
    /// Any other gRPC-level error.
    Rpc(tonic::Status),
}

impl From<tonic::Status> for ActivityTaskCompletionError {
    fn from(s: tonic::Status) -> Self {
        ActivityTaskCompletionError::Rpc(s)
    }
}

impl std::fmt::Display for ActivityTaskCompletionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActivityTaskCompletionError::PayloadsTooLarge(e) => {
                write!(f, "payload size limit exceeded: {}", e.message())
            }
            ActivityTaskCompletionError::Rpc(s) => write!(f, "gRPC error: {s}"),
        }
    }
}

impl std::error::Error for ActivityTaskCompletionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ActivityTaskCompletionError::Rpc(s) => Some(s),
            ActivityTaskCompletionError::PayloadsTooLarge(_) => None,
        }
    }
}

/// Reads an `AtomicU64` limit where `0` means "no limit".
/// Should only be used for payload and memo size **error** limits.
#[inline]
fn load_error_limit(arc: &AtomicU64) -> Option<u64> {
    let v = arc.load(Ordering::Relaxed);
    (v > 0).then_some(v)
}

/// A `Failure` proto for a payload-size-limit violation.
fn payload_size_exceeded_failure(msg: String) -> Failure {
    Failure {
        message: msg,
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: "PayloadSizeLimitExceeded".to_string(),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

pub enum LegacyQueryResult {
    Succeeded(QueryResult),
    Failed(workflow_completion::Failure),
}

/// Contains everything a worker needs to interact with the server
pub(crate) struct WorkerClientBag {
    connection: SharedReplaceableClient<Connection>,
    namespace: String,
    worker_versioning_strategy: WorkerVersioningStrategy,
    worker_instance_key: Uuid,
    worker_heartbeat_map: Arc<Mutex<HashMap<String, ClientHeartbeatData>>>,
    /// Server-provided payload size error limit in bytes. `0` means not yet set.
    payload_size_error_limit: Arc<AtomicU64>,
    /// Server-provided memo size error limit in bytes. `0` means not yet set.
    memo_size_error_limit: Arc<AtomicU64>,
    /// Payload size warning threshold in bytes.
    payload_size_warn_limit: u64,
    /// Memo size warning threshold in bytes.
    memo_size_warn_limit: u64,
}

impl WorkerClientBag {
    pub(crate) fn new(
        connection: SharedReplaceableClient<Connection>,
        namespace: String,
        worker_versioning_strategy: WorkerVersioningStrategy,
        worker_instance_key: Uuid,
        payload_size_warn_limit: u64,
        memo_size_warn_limit: u64,
    ) -> Self {
        Self {
            connection,
            namespace,
            worker_versioning_strategy,
            worker_instance_key,
            worker_heartbeat_map: Arc::new(Mutex::new(HashMap::new())),
            payload_size_error_limit: Arc::new(AtomicU64::new(0)),
            memo_size_error_limit: Arc::new(AtomicU64::new(0)),
            payload_size_warn_limit,
            memo_size_warn_limit,
        }
    }

    fn identity(&self) -> String {
        self.connection.inner_cow().identity().to_owned()
    }

    fn default_capabilities(&self) -> Capabilities {
        self.capabilities().unwrap_or_default()
    }

    fn binary_checksum(&self) -> String {
        if self.default_capabilities().build_id_based_versioning {
            "".to_string()
        } else {
            self.worker_versioning_strategy.build_id().to_owned()
        }
    }

    fn deployment_options(&self) -> Option<deployment::v1::WorkerDeploymentOptions> {
        match &self.worker_versioning_strategy {
            WorkerVersioningStrategy::WorkerDeploymentBased(dopts) => {
                Some(deployment::v1::WorkerDeploymentOptions {
                    deployment_name: dopts.version.deployment_name.clone(),
                    build_id: dopts.version.build_id.clone(),
                    worker_versioning_mode: if dopts.use_worker_versioning {
                        WorkerVersioningMode::Versioned.into()
                    } else {
                        WorkerVersioningMode::Unversioned.into()
                    },
                })
            }
            _ => None,
        }
    }

    fn worker_version_capabilities(&self) -> Option<WorkerVersionCapabilities> {
        if self.default_capabilities().build_id_based_versioning {
            Some(WorkerVersionCapabilities {
                build_id: self.worker_versioning_strategy.build_id().to_owned(),
                use_versioning: self.worker_versioning_strategy.uses_build_id_based(),
                // This will never be used, as it is the v3 version that we never supported in
                // Core SDKs.
                deployment_series_name: "".to_string(),
            })
        } else {
            None
        }
    }

    fn worker_version_stamp(&self) -> Option<WorkerVersionStamp> {
        if self.default_capabilities().build_id_based_versioning {
            Some(WorkerVersionStamp {
                build_id: self.worker_versioning_strategy.build_id().to_owned(),
                use_versioning: self.worker_versioning_strategy.uses_build_id_based(),
            })
        } else {
            None
        }
    }
}

/// This trait contains everything workers need to interact with Temporal, and hence provides a
/// minimal mocking surface.
#[cfg_attr(any(feature = "test-utilities", test), mockall::automock)]
#[async_trait::async_trait]
pub trait WorkerClient: Sync + Send {
    /// Poll workflow tasks
    async fn poll_workflow_task(
        &self,
        poll_options: PollOptions,
        wf_options: PollWorkflowOptions,
    ) -> Result<PollWorkflowTaskQueueResponse>;
    /// Poll activity tasks
    async fn poll_activity_task(
        &self,
        poll_options: PollOptions,
        act_options: PollActivityOptions,
    ) -> Result<PollActivityTaskQueueResponse>;
    /// Poll Nexus tasks
    async fn poll_nexus_task(
        &self,
        poll_options: PollOptions,
        send_heartbeat: bool,
    ) -> Result<PollNexusTaskQueueResponse>;
    /// Complete a workflow task
    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
    ) -> WorkflowTaskCompletionResult;
    /// Complete an activity task
    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> ActivityTaskCompletionResult;
    /// Complete a Nexus task
    async fn complete_nexus_task(
        &self,
        task_token: TaskToken,
        response: nexus::v1::Response,
    ) -> Result<RespondNexusTaskCompletedResponse>;
    /// Record an activity heartbeat
    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse>;
    /// Cancel an activity task
    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse>;
    /// Fail an activity task
    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse>;
    /// Fail a workflow task
    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse>;
    /// Fail a Nexus task
    async fn fail_nexus_task(
        &self,
        task_token: TaskToken,
        error: NexusTaskFailure,
    ) -> Result<RespondNexusTaskFailedResponse>;
    /// Get the workflow execution history
    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse>;
    /// Respond to a legacy query
    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: LegacyQueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse>;
    /// Describe the namespace
    async fn describe_namespace(&self) -> Result<DescribeNamespaceResponse>;
    /// Shutdown the worker
    async fn shutdown_worker(
        &self,
        sticky_task_queue: String,
        task_queue: String,
        task_queue_types: Vec<TaskQueueType>,
        final_heartbeat: Option<WorkerHeartbeat>,
    ) -> Result<ShutdownWorkerResponse>;
    /// Record a worker heartbeat
    async fn record_worker_heartbeat(
        &self,
        namespace: String,
        worker_heartbeat: Vec<WorkerHeartbeat>,
    ) -> Result<RecordWorkerHeartbeatResponse>;

    /// Replace the underlying connection
    fn replace_connection(&self, new_client: Connection);
    /// Return server capabilities
    fn capabilities(&self) -> Option<Capabilities>;
    /// Return workers using this client
    fn workers(&self) -> Arc<ClientWorkerSet>;
    /// Indicates if this is a mock client
    fn is_mock(&self) -> bool;
    /// Returns the shared `Arc<AtomicU64>` stores for the server-provided payload error limits.
    /// These are updated by `Worker::validate()` after the namespace is described.
    fn payload_error_limits(&self) -> (Arc<AtomicU64>, Arc<AtomicU64>);
    /// Return name and version of the SDK
    fn sdk_name_and_version(&self) -> (String, String);
    /// Get the namespace this worker is bound to
    fn namespace(&self) -> String;
    /// Get worker identity
    fn identity(&self) -> String;
    /// Get worker grouping key
    fn worker_grouping_key(&self) -> Uuid;
    /// Get worker instance key (unique per worker instance)
    fn worker_instance_key(&self) -> Uuid;
    /// Sets the client-reliant fields for WorkerHeartbeat. This also updates client-level tracking
    /// of heartbeat fields, like last heartbeat timestamp.
    fn set_heartbeat_client_fields(&self, heartbeat: &mut WorkerHeartbeat);
}

/// Configuration options shared by workflow, activity, and Nexus polling calls
#[derive(Debug, Clone)]
pub struct PollOptions {
    /// The name of the task queue to poll
    pub task_queue: String,
    /// Prevents retrying on specific gRPC statuses
    pub no_retry: Option<NoRetryOnMatching>,
    /// Overrides the default RPC timeout for the poll request
    pub timeout_override: Option<Duration>,
}
/// Additional options specific to workflow task polling
#[derive(Debug, Clone)]
pub struct PollWorkflowOptions {
    /// Optional sticky queue name for session‐based workflow polling
    pub sticky_queue_name: Option<String>,
}
/// Additional options specific to activity task polling
#[derive(Debug, Clone)]
pub struct PollActivityOptions {
    /// Optional rate limit (tasks per second) for activity polling
    pub max_tasks_per_sec: Option<f64>,
}

#[async_trait::async_trait]
impl WorkerClient for WorkerClientBag {
    async fn poll_workflow_task(
        &self,
        poll_options: PollOptions,
        wf_options: PollWorkflowOptions,
    ) -> Result<PollWorkflowTaskQueueResponse> {
        let task_queue = if let Some(sticky) = wf_options.sticky_queue_name {
            TaskQueue {
                name: sticky,
                kind: TaskQueueKind::Sticky.into(),
                normal_name: poll_options.task_queue,
            }
        } else {
            TaskQueue {
                name: poll_options.task_queue,
                kind: TaskQueueKind::Normal.into(),
                normal_name: "".to_string(),
            }
        };
        #[allow(deprecated)] // want to list all fields explicitly
        let mut request = PollWorkflowTaskQueueRequest {
            namespace: self.namespace.clone(),
            task_queue: Some(task_queue),
            identity: self.identity(),
            binary_checksum: self.binary_checksum(),
            worker_version_capabilities: self.worker_version_capabilities(),
            deployment_options: self.deployment_options(),
            worker_instance_key: self.worker_instance_key.to_string(),
        }
        .into_request();
        request.extensions_mut().insert(IsWorkerTaskLongPoll);
        if let Some(nr) = poll_options.no_retry {
            request.extensions_mut().insert(nr);
        }
        if let Some(to) = poll_options.timeout_override {
            request.set_timeout(to);
        }

        Ok(self
            .connection
            .clone()
            .poll_workflow_task_queue(request)
            .await?
            .into_inner())
    }

    async fn poll_activity_task(
        &self,
        poll_options: PollOptions,
        act_options: PollActivityOptions,
    ) -> Result<PollActivityTaskQueueResponse> {
        #[allow(deprecated)] // want to list all fields explicitly
        let mut request = PollActivityTaskQueueRequest {
            namespace: self.namespace.clone(),
            task_queue: Some(TaskQueue {
                name: poll_options.task_queue,
                kind: TaskQueueKind::Normal as i32,
                normal_name: "".to_string(),
            }),
            identity: self.identity(),
            task_queue_metadata: act_options.max_tasks_per_sec.map(|tps| TaskQueueMetadata {
                max_tasks_per_second: Some(tps),
            }),
            worker_version_capabilities: self.worker_version_capabilities(),
            deployment_options: self.deployment_options(),
            worker_instance_key: self.worker_instance_key.to_string(),
        }
        .into_request();
        request.extensions_mut().insert(IsWorkerTaskLongPoll);
        if let Some(nr) = poll_options.no_retry {
            request.extensions_mut().insert(nr);
        }
        if let Some(to) = poll_options.timeout_override {
            request.set_timeout(to);
        }

        Ok(self
            .connection
            .clone()
            .poll_activity_task_queue(request)
            .await?
            .into_inner())
    }

    async fn poll_nexus_task(
        &self,
        poll_options: PollOptions,
        _send_heartbeat: bool,
    ) -> Result<PollNexusTaskQueueResponse> {
        #[allow(deprecated)] // want to list all fields explicitly
        let mut request = PollNexusTaskQueueRequest {
            namespace: self.namespace.clone(),
            task_queue: Some(TaskQueue {
                name: poll_options.task_queue,
                kind: TaskQueueKind::Normal as i32,
                normal_name: "".to_string(),
            }),
            identity: self.identity(),
            worker_version_capabilities: self.worker_version_capabilities(),
            deployment_options: self.deployment_options(),
            worker_heartbeat: Vec::new(),
            worker_instance_key: self.worker_instance_key.to_string(),
        }
        .into_request();
        request.extensions_mut().insert(IsWorkerTaskLongPoll);
        if let Some(nr) = poll_options.no_retry {
            request.extensions_mut().insert(nr);
        }
        if let Some(to) = poll_options.timeout_override {
            request.set_timeout(to);
        }

        Ok(self
            .connection
            .clone()
            .poll_nexus_task_queue(request)
            .await?
            .into_inner())
    }

    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
    ) -> WorkflowTaskCompletionResult {
        #[allow(deprecated)] // want to list all fields explicitly
        let mut request = RespondWorkflowTaskCompletedRequest {
            task_token: request.task_token.into(),
            commands: request.commands,
            messages: request.messages,
            identity: self.identity(),
            sticky_attributes: request.sticky_attributes,
            return_new_workflow_task: request.return_new_workflow_task,
            force_create_new_workflow_task: request.force_create_new_workflow_task,
            worker_version_stamp: self.worker_version_stamp(),
            binary_checksum: self.binary_checksum(),
            query_results: request
                .query_responses
                .into_iter()
                .map(|qr| {
                    let (id, completed_type, query_result, error_message) = qr.into_components();
                    (
                        id,
                        WorkflowQueryResult {
                            result_type: completed_type as i32,
                            answer: query_result,
                            error_message,
                            // TODO: https://github.com/temporalio/sdk-core/issues/867
                            failure: None,
                        },
                    )
                })
                .collect(),
            namespace: self.namespace.clone(),
            sdk_metadata: Some(request.sdk_metadata),
            metering_metadata: Some(request.metering_metadata),
            capabilities: Some(respond_workflow_task_completed_request::Capabilities {
                discard_speculative_workflow_task_with_events: true,
            }),
            // Will never be set, deprecated.
            deployment: None,
            versioning_behavior: request.versioning_behavior.into(),
            deployment_options: self.deployment_options(),
            resource_id: Default::default(),
        };

        // Check payload size limits before sending to server
        let limit_result = check_payload_limits(
            &mut request,
            load_error_limit(&self.payload_size_error_limit),
            load_error_limit(&self.memo_size_error_limit),
            self.payload_size_warn_limit,
            self.memo_size_warn_limit,
        )
        .await;
        if let Some(exceeded) = limit_result.error_exceeded {
            // Send a WFT failure instead of the completion.
            self.connection
                .clone()
                .respond_workflow_task_failed(
                    #[allow(deprecated)] // want to list all fields explicitly
                    RespondWorkflowTaskFailedRequest {
                        task_token: request.task_token.clone(),
                        cause: WorkflowTaskFailedCause::PayloadsTooLarge.into(),
                        failure: Some(payload_size_exceeded_failure(exceeded.message())),
                        identity: self.identity(),
                        binary_checksum: self.binary_checksum(),
                        namespace: self.namespace.clone(),
                        messages: vec![],
                        worker_version: self.worker_version_stamp(),
                        deployment: None,
                        deployment_options: self.deployment_options(),
                        resource_id: Default::default(),
                    }
                    .into_request(),
                )
                .await?;
            return Err(WorkflowTaskCompletionError::PayloadsTooLarge(exceeded));
        }

        let response = self
            .connection
            .clone()
            .respond_workflow_task_completed(request.into_request())
            .await?
            .into_inner();
        Ok(WorkflowTaskCompletionSuccess {
            response,
            warn_exceeded: limit_result.warn_exceeded,
        })
    }

    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> ActivityTaskCompletionResult {
        #[allow(deprecated)] // want to list all fields explicitly
        let mut request = RespondActivityTaskCompletedRequest {
            task_token: task_token.0.clone(),
            result,
            identity: self.identity(),
            namespace: self.namespace.clone(),
            worker_version: self.worker_version_stamp(),
            // Will never be set, deprecated.
            deployment: None,
            deployment_options: self.deployment_options(),
            resource_id: Default::default(),
        };

        // Check payload size limits before sending to server.
        let limit_result = check_payload_limits(
            &mut request,
            load_error_limit(&self.payload_size_error_limit),
            load_error_limit(&self.memo_size_error_limit),
            self.payload_size_warn_limit,
            self.memo_size_warn_limit,
        )
        .await;
        if let Some(exceeded) = limit_result.error_exceeded {
            // Send a task failure instead of the completion.
            self.connection
                .clone()
                .respond_activity_task_failed(
                    #[allow(deprecated)] // want to list all fields explicitly
                    RespondActivityTaskFailedRequest {
                        task_token: task_token.0,
                        failure: Some(payload_size_exceeded_failure(exceeded.message())),
                        identity: self.identity(),
                        namespace: self.namespace.clone(),
                        last_heartbeat_details: None,
                        worker_version: self.worker_version_stamp(),
                        deployment: None,
                        deployment_options: self.deployment_options(),
                        resource_id: Default::default(),
                    }
                    .into_request(),
                )
                .await
                .map_err(ActivityTaskCompletionError::Rpc)?;
            return Err(ActivityTaskCompletionError::PayloadsTooLarge(exceeded));
        }

        self.connection
            .clone()
            .respond_activity_task_completed(request.into_request())
            .await
            .map_err(ActivityTaskCompletionError::Rpc)?;
        Ok(ActivityTaskCompletionSuccess {
            warn_exceeded: limit_result.warn_exceeded,
        })
    }

    async fn complete_nexus_task(
        &self,
        task_token: TaskToken,
        response: nexus::v1::Response,
    ) -> Result<RespondNexusTaskCompletedResponse> {
        Ok(self
            .connection
            .clone()
            .respond_nexus_task_completed(
                RespondNexusTaskCompletedRequest {
                    namespace: self.namespace.clone(),
                    identity: self.identity(),
                    task_token: task_token.0,
                    response: Some(response),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse> {
        Ok(self
            .connection
            .clone()
            .record_activity_task_heartbeat(
                RecordActivityTaskHeartbeatRequest {
                    task_token: task_token.0,
                    details,
                    identity: self.identity(),
                    namespace: self.namespace.clone(),
                    resource_id: Default::default(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse> {
        Ok(self
            .connection
            .clone()
            .respond_activity_task_canceled(
                #[allow(deprecated)] // want to list all fields explicitly
                RespondActivityTaskCanceledRequest {
                    task_token: task_token.0,
                    details,
                    identity: self.identity(),
                    namespace: self.namespace.clone(),
                    worker_version: self.worker_version_stamp(),
                    // Will never be set, deprecated.
                    deployment: None,
                    deployment_options: self.deployment_options(),
                    resource_id: Default::default(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse> {
        Ok(self
            .connection
            .clone()
            .respond_activity_task_failed(
                #[allow(deprecated)] // want to list all fields explicitly
                RespondActivityTaskFailedRequest {
                    task_token: task_token.0,
                    failure,
                    identity: self.identity(),
                    namespace: self.namespace.clone(),
                    // TODO: Implement - https://github.com/temporalio/sdk-core/issues/293
                    last_heartbeat_details: None,
                    worker_version: self.worker_version_stamp(),
                    // Will never be set, deprecated.
                    deployment: None,
                    deployment_options: self.deployment_options(),
                    resource_id: Default::default(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse> {
        #[allow(deprecated)] // want to list all fields explicitly
        let request = RespondWorkflowTaskFailedRequest {
            task_token: task_token.0,
            cause: cause as i32,
            failure,
            identity: self.identity(),
            binary_checksum: self.binary_checksum(),
            namespace: self.namespace.clone(),
            messages: vec![],
            worker_version: self.worker_version_stamp(),
            // Will never be set, deprecated.
            deployment: None,
            deployment_options: self.deployment_options(),
            resource_id: Default::default(),
        };
        Ok(self
            .connection
            .clone()
            .respond_workflow_task_failed(request.into_request())
            .await?
            .into_inner())
    }

    async fn fail_nexus_task(
        &self,
        task_token: TaskToken,
        error: NexusTaskFailure,
    ) -> Result<RespondNexusTaskFailedResponse> {
        let (error, failure) = match error {
            NexusTaskFailure::Legacy(handler_err) => (Some(handler_err), None),
            NexusTaskFailure::Temporal(failure) => (None, Some(failure)),
        };

        Ok(self
            .connection
            .clone()
            .respond_nexus_task_failed(
                #[allow(deprecated)]
                RespondNexusTaskFailedRequest {
                    namespace: self.namespace.clone(),
                    identity: self.identity(),
                    task_token: task_token.0,
                    failure,
                    error,
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse> {
        Ok(self
            .connection
            .clone()
            .get_workflow_execution_history(
                GetWorkflowExecutionHistoryRequest {
                    namespace: self.namespace.clone(),
                    execution: Some(WorkflowExecution {
                        workflow_id,
                        run_id: run_id.unwrap_or_default(),
                    }),
                    next_page_token: page_token,
                    ..Default::default()
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: LegacyQueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse> {
        let mut failure = None;
        let (query_result, cause) = match query_result {
            LegacyQueryResult::Succeeded(s) => (s, WorkflowTaskFailedCause::Unspecified),
            #[allow(deprecated)]
            LegacyQueryResult::Failed(f) => {
                let cause = f.force_cause();
                failure = f.failure.clone();
                (legacy_query_failure(f), cause)
            }
        };
        let (_, completed_type, query_result, error_message) = query_result.into_components();

        Ok(self
            .connection
            .clone()
            .respond_query_task_completed(
                RespondQueryTaskCompletedRequest {
                    task_token: task_token.into(),
                    completed_type: completed_type as i32,
                    query_result,
                    error_message,
                    namespace: self.namespace.clone(),
                    failure,
                    cause: cause.into(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn describe_namespace(&self) -> Result<DescribeNamespaceResponse> {
        Ok(self
            .connection
            .clone()
            .describe_namespace(
                Namespace::Name(self.namespace.clone())
                    .into_describe_namespace_request()
                    .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn shutdown_worker(
        &self,
        sticky_task_queue: String,
        task_queue: String,
        task_queue_types: Vec<TaskQueueType>,
        final_heartbeat: Option<WorkerHeartbeat>,
    ) -> Result<ShutdownWorkerResponse> {
        let mut final_heartbeat = final_heartbeat;
        if let Some(w) = final_heartbeat.as_mut() {
            self.set_heartbeat_client_fields(w);
        }
        let mut request = ShutdownWorkerRequest {
            namespace: self.namespace.clone(),
            identity: self.identity(),
            sticky_task_queue,
            reason: "graceful shutdown".to_string(),
            worker_heartbeat: final_heartbeat,
            worker_instance_key: self.worker_instance_key.to_string(),
            task_queue,
            task_queue_types: task_queue_types.into_iter().map(|t| t as i32).collect(),
        }
        .into_request();
        request
            .extensions_mut()
            .insert(RetryConfigForCall(RetryOptions::no_retries()));

        Ok(
            WorkflowService::shutdown_worker(&mut self.connection.clone(), request)
                .await?
                .into_inner(),
        )
    }

    async fn record_worker_heartbeat(
        &self,
        namespace: String,
        worker_heartbeat: Vec<WorkerHeartbeat>,
    ) -> Result<RecordWorkerHeartbeatResponse> {
        let request = RecordWorkerHeartbeatRequest {
            namespace,
            identity: self.identity(),
            worker_heartbeat,
            resource_id: Default::default(),
        };
        Ok(self
            .connection
            .clone()
            .record_worker_heartbeat(request.into_request())
            .await?
            .into_inner())
    }

    fn replace_connection(&self, new_connection: Connection) {
        self.connection.replace_client(new_connection);
    }

    fn capabilities(&self) -> Option<Capabilities> {
        self.connection.inner_cow().capabilities().cloned()
    }

    fn workers(&self) -> Arc<ClientWorkerSet> {
        self.connection.inner_cow().workers()
    }

    fn is_mock(&self) -> bool {
        false
    }

    fn sdk_name_and_version(&self) -> (String, String) {
        let inner = self.connection.inner_cow();
        (
            inner.client_name().to_owned(),
            inner.client_version().to_owned(),
        )
    }

    fn namespace(&self) -> String {
        self.namespace.clone()
    }

    fn identity(&self) -> String {
        self.identity()
    }

    fn worker_grouping_key(&self) -> Uuid {
        self.connection.inner_cow().worker_grouping_key()
    }

    fn worker_instance_key(&self) -> Uuid {
        self.worker_instance_key
    }

    fn payload_error_limits(&self) -> (Arc<AtomicU64>, Arc<AtomicU64>) {
        (
            self.payload_size_error_limit.clone(),
            self.memo_size_error_limit.clone(),
        )
    }

    fn set_heartbeat_client_fields(&self, heartbeat: &mut WorkerHeartbeat) {
        if let Some(host_info) = heartbeat.host_info.as_mut() {
            host_info.worker_grouping_key = self.worker_grouping_key().to_string();
        }
        heartbeat.worker_identity = WorkerClient::identity(self);
        let sdk_name_and_ver = self.sdk_name_and_version();
        heartbeat.sdk_name = sdk_name_and_ver.0;
        heartbeat.sdk_version = sdk_name_and_ver.1;

        let now = SystemTime::now();
        heartbeat.heartbeat_time = Some(now.into());
        let mut heartbeat_map = self.worker_heartbeat_map.lock();
        let client_heartbeat_data = heartbeat_map
            .entry(heartbeat.worker_instance_key.clone())
            .or_default();
        let elapsed_since_last_heartbeat =
            client_heartbeat_data.last_heartbeat_time.map(|hb_time| {
                let dur = now.duration_since(hb_time).unwrap_or(Duration::ZERO);
                PbDuration {
                    seconds: dur.as_secs() as i64,
                    nanos: dur.subsec_nanos() as i32,
                }
            });
        heartbeat.elapsed_since_last_heartbeat = elapsed_since_last_heartbeat;
        client_heartbeat_data.last_heartbeat_time = Some(now);

        update_slots(
            &mut heartbeat.workflow_task_slots_info,
            &mut client_heartbeat_data.workflow_task_slots_info,
        );
        update_slots(
            &mut heartbeat.activity_task_slots_info,
            &mut client_heartbeat_data.activity_task_slots_info,
        );
        update_slots(
            &mut heartbeat.nexus_task_slots_info,
            &mut client_heartbeat_data.nexus_task_slots_info,
        );
        update_slots(
            &mut heartbeat.local_activity_slots_info,
            &mut client_heartbeat_data.local_activity_slots_info,
        );
    }
}

impl NamespacedClient for WorkerClientBag {
    fn namespace(&self) -> String {
        self.namespace.clone()
    }

    fn identity(&self) -> String {
        self.identity()
    }
}

/// A version of [RespondWorkflowTaskCompletedRequest] that will finish being filled out by the
/// server client
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowTaskCompletion {
    /// The task token that would've been received from polling for a workflow activation
    pub task_token: TaskToken,
    /// A list of new commands to send to the server, such as starting a timer.
    pub commands: Vec<Command>,
    /// A list of protocol messages to send to the server.
    pub messages: Vec<ProtocolMessage>,
    /// If set, indicate that next task should be queued on sticky queue with given attributes.
    pub sticky_attributes: Option<StickyExecutionAttributes>,
    /// Responses to queries in the `queries` field of the workflow task.
    pub query_responses: Vec<QueryResult>,
    /// Indicate that the task completion should return a new WFT if one is available
    pub return_new_workflow_task: bool,
    /// Force a new WFT to be created after this completion
    pub force_create_new_workflow_task: bool,
    /// SDK-specific metadata to send
    pub sdk_metadata: WorkflowTaskCompletedMetadata,
    /// Metering info
    pub metering_metadata: MeteringMetadata,
    /// Versioning behavior of the workflow, if any.
    pub versioning_behavior: VersioningBehavior,
}

#[derive(Clone, Default)]
struct SlotsInfo {
    total_processed_tasks: i32,
    total_failed_tasks: i32,
}

#[derive(Clone, Default)]
struct ClientHeartbeatData {
    last_heartbeat_time: Option<SystemTime>,

    workflow_task_slots_info: SlotsInfo,
    activity_task_slots_info: SlotsInfo,
    nexus_task_slots_info: SlotsInfo,
    local_activity_slots_info: SlotsInfo,
}

fn update_slots(slots_info: &mut Option<WorkerSlotsInfo>, client_heartbeat_data: &mut SlotsInfo) {
    if let Some(wft_slot_info) = slots_info.as_mut() {
        wft_slot_info.last_interval_processed_tasks =
            wft_slot_info.total_processed_tasks - client_heartbeat_data.total_processed_tasks;
        wft_slot_info.last_interval_failure_tasks =
            wft_slot_info.total_failed_tasks - client_heartbeat_data.total_failed_tasks;

        client_heartbeat_data.total_processed_tasks = wft_slot_info.total_processed_tasks;
        client_heartbeat_data.total_failed_tasks = wft_slot_info.total_failed_tasks;
    }
}

#[cfg(test)]
mod tests {
    use super::load_error_limit;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn load_error_limit_zero_means_no_limit() {
        assert_eq!(load_error_limit(&AtomicU64::new(0)), None);
    }

    #[test]
    fn load_error_limit_nonzero_returns_some() {
        assert_eq!(load_error_limit(&AtomicU64::new(1024)), Some(1024));
    }
}
