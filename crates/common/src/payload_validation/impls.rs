//! Hand-written implementation of [`PayloadFieldValidator`] for the default validator.
//!
//! Every method here corresponds to exactly one `(message, payload field)` tuple
//! enumerated by `crates/common/build.rs` from the proto descriptor set. If the proto
//! descriptors gain a new payload-bearing field — or a new payload-bearing message —
//! the generated `PayloadFieldValidator` trait will gain a new required method and
//! this file will fail to compile until it is added. That build-time exhaustiveness
//! is the whole point of the layer.

use super::outcome::{
    FieldValidationOutcome, Severity, ValidationContext, ValidationIssue,
};
use super::{PayloadFieldValidator, limits::PayloadSizeLimits};
use crate::protos::temporal::api::common::v1::{Payload, Payloads};
use prost::Message as _;
use std::collections::HashMap;

/// The default, framework-supplied [`PayloadFieldValidator`].
///
/// Stateless. Each method enforces a sensible default rule for the corresponding
/// `(message, field)` tuple. Project-specific extensions can wrap this with their own
/// `PayloadFieldValidator` that delegates back here for fields they don't customize.
#[derive(Copy, Clone, Debug, Default)]
pub struct DefaultPayloadFieldValidator;

// ---------------------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------------------

fn payload_encoded_size(p: &Payload) -> usize {
    p.encoded_len()
}

fn payloads_encoded_size(payloads: &Payloads) -> usize {
    payloads.payloads.iter().map(payload_encoded_size).sum()
}

fn map_payload_encoded_size(map: &HashMap<String, Payload>) -> usize {
    map.values().map(payload_encoded_size).sum()
}

fn check_size(
    ctx: &ValidationContext<'_>,
    field_path: &'static str,
    observed: usize,
    select_limit: impl Fn(&PayloadSizeLimits) -> usize,
) -> FieldValidationOutcome {
    let mut out = FieldValidationOutcome::ok();
    let warn_limit = select_limit(ctx.warning_limits);
    if warn_limit != 0 && observed > warn_limit {
        out = out.with_issue(ValidationIssue {
            severity: Severity::Warning,
            field_path,
            observed_bytes: Some(observed),
            limit_bytes: Some(warn_limit),
            message: format!(
                "payload size {observed} bytes exceeds warning threshold {warn_limit} bytes"
            ),
        });
    }
    if let Some(err_limits) = ctx.error_limits {
        let err_limit = select_limit(err_limits);
        if err_limit != 0 && observed > err_limit {
            out = out.with_issue(ValidationIssue {
                severity: Severity::Error,
                field_path,
                observed_bytes: Some(observed),
                limit_bytes: Some(err_limit),
                message: format!(
                    "payload size {observed} bytes exceeds server error limit {err_limit} bytes"
                ),
            });
        }
    }
    out
}

/// Apply the standard "payloads" size check (uses `PayloadSizeLimits::payloads_size`).
fn check_payloads(
    ctx: &ValidationContext<'_>,
    field_path: &'static str,
    bytes: usize,
) -> FieldValidationOutcome {
    check_size(ctx, field_path, bytes, |l| l.payloads_size)
}

/// Apply the standard "memo" size check (uses `PayloadSizeLimits::memo_size`).
fn check_memo(
    ctx: &ValidationContext<'_>,
    field_path: &'static str,
    bytes: usize,
) -> FieldValidationOutcome {
    check_size(ctx, field_path, bytes, |l| l.memo_size)
}

// ---------------------------------------------------------------------------------------
// PayloadFieldValidator impl — populated below once the build emits the trait.
// ---------------------------------------------------------------------------------------

impl PayloadFieldValidator for DefaultPayloadFieldValidator {
    fn validate_activity_v1_activity_execution_info_heartbeat_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.activity.v1.ActivityExecutionInfo.heartbeat_details", payloads_encoded_size(value))
    }
    fn validate_batch_v1_batch_operation_signal_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.batch.v1.BatchOperationSignal.input", payloads_encoded_size(value))
    }
    fn validate_batch_v1_batch_operation_termination_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.batch.v1.BatchOperationTermination.details", payloads_encoded_size(value))
    }
    fn validate_cloud_nexus_v1_endpoint_spec_description(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payload,
    ) -> FieldValidationOutcome {
        check_size(ctx, "temporal.api.cloud.nexus.v1.EndpointSpec.description", payload_encoded_size(value), |l| l.payloads_size)
    }
    fn validate_command_v1_cancel_workflow_execution_command_attributes_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.command.v1.CancelWorkflowExecutionCommandAttributes.details", payloads_encoded_size(value))
    }
    fn validate_command_v1_complete_workflow_execution_command_attributes_result(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.command.v1.CompleteWorkflowExecutionCommandAttributes.result", payloads_encoded_size(value))
    }
    fn validate_command_v1_continue_as_new_workflow_execution_command_attributes_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes.input", payloads_encoded_size(value))
    }
    fn validate_command_v1_continue_as_new_workflow_execution_command_attributes_last_completion_result(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes.last_completion_result", payloads_encoded_size(value))
    }
    fn validate_command_v1_schedule_activity_task_command_attributes_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.command.v1.ScheduleActivityTaskCommandAttributes.input", payloads_encoded_size(value))
    }
    fn validate_command_v1_schedule_nexus_operation_command_attributes_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payload,
    ) -> FieldValidationOutcome {
        check_size(ctx, "temporal.api.command.v1.ScheduleNexusOperationCommandAttributes.input", payload_encoded_size(value), |l| l.payloads_size)
    }
    fn validate_command_v1_signal_external_workflow_execution_command_attributes_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes.input", payloads_encoded_size(value))
    }
    fn validate_command_v1_start_child_workflow_execution_command_attributes_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.command.v1.StartChildWorkflowExecutionCommandAttributes.input", payloads_encoded_size(value))
    }
    fn validate_common_v1_header_fields(
        &self,
        ctx: &ValidationContext<'_>,
        value: &::std::collections::HashMap<String, crate::protos::temporal::api::common::v1::Payload>,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.common.v1.Header.fields", map_payload_encoded_size(value))
    }
    fn validate_common_v1_memo_fields(
        &self,
        ctx: &ValidationContext<'_>,
        value: &::std::collections::HashMap<String, crate::protos::temporal::api::common::v1::Payload>,
    ) -> FieldValidationOutcome {
        check_memo(ctx, "temporal.api.common.v1.Memo.fields", map_payload_encoded_size(value))
    }
    fn validate_common_v1_search_attributes_indexed_fields(
        &self,
        ctx: &ValidationContext<'_>,
        value: &::std::collections::HashMap<String, crate::protos::temporal::api::common::v1::Payload>,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.common.v1.SearchAttributes.indexed_fields", map_payload_encoded_size(value))
    }
    fn validate_compute_v1_compute_provider_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payload,
    ) -> FieldValidationOutcome {
        check_size(ctx, "temporal.api.compute.v1.ComputeProvider.details", payload_encoded_size(value), |l| l.payloads_size)
    }
    fn validate_compute_v1_compute_scaler_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payload,
    ) -> FieldValidationOutcome {
        check_size(ctx, "temporal.api.compute.v1.ComputeScaler.details", payload_encoded_size(value), |l| l.payloads_size)
    }
    fn validate_deployment_v1_deployment_info_metadata(
        &self,
        ctx: &ValidationContext<'_>,
        value: &::std::collections::HashMap<String, crate::protos::temporal::api::common::v1::Payload>,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.deployment.v1.DeploymentInfo.metadata", map_payload_encoded_size(value))
    }
    fn validate_deployment_v1_update_deployment_metadata_upsert_entries(
        &self,
        ctx: &ValidationContext<'_>,
        value: &::std::collections::HashMap<String, crate::protos::temporal::api::common::v1::Payload>,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.deployment.v1.UpdateDeploymentMetadata.upsert_entries", map_payload_encoded_size(value))
    }
    fn validate_deployment_v1_version_metadata_entries(
        &self,
        ctx: &ValidationContext<'_>,
        value: &::std::collections::HashMap<String, crate::protos::temporal::api::common::v1::Payload>,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.deployment.v1.VersionMetadata.entries", map_payload_encoded_size(value))
    }
    fn validate_failure_v1_application_failure_info_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.failure.v1.ApplicationFailureInfo.details", payloads_encoded_size(value))
    }
    fn validate_failure_v1_canceled_failure_info_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.failure.v1.CanceledFailureInfo.details", payloads_encoded_size(value))
    }
    fn validate_failure_v1_failure_encoded_attributes(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payload,
    ) -> FieldValidationOutcome {
        check_size(ctx, "temporal.api.failure.v1.Failure.encoded_attributes", payload_encoded_size(value), |l| l.payloads_size)
    }
    fn validate_failure_v1_reset_workflow_failure_info_last_heartbeat_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.failure.v1.ResetWorkflowFailureInfo.last_heartbeat_details", payloads_encoded_size(value))
    }
    fn validate_failure_v1_timeout_failure_info_last_heartbeat_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.failure.v1.TimeoutFailureInfo.last_heartbeat_details", payloads_encoded_size(value))
    }
    fn validate_history_v1_activity_task_canceled_event_attributes_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.history.v1.ActivityTaskCanceledEventAttributes.details", payloads_encoded_size(value))
    }
    fn validate_history_v1_activity_task_completed_event_attributes_result(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.history.v1.ActivityTaskCompletedEventAttributes.result", payloads_encoded_size(value))
    }
    fn validate_history_v1_activity_task_scheduled_event_attributes_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.history.v1.ActivityTaskScheduledEventAttributes.input", payloads_encoded_size(value))
    }
    fn validate_history_v1_child_workflow_execution_canceled_event_attributes_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.history.v1.ChildWorkflowExecutionCanceledEventAttributes.details", payloads_encoded_size(value))
    }
    fn validate_history_v1_child_workflow_execution_completed_event_attributes_result(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.history.v1.ChildWorkflowExecutionCompletedEventAttributes.result", payloads_encoded_size(value))
    }
    fn validate_history_v1_nexus_operation_completed_event_attributes_result(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payload,
    ) -> FieldValidationOutcome {
        check_size(ctx, "temporal.api.history.v1.NexusOperationCompletedEventAttributes.result", payload_encoded_size(value), |l| l.payloads_size)
    }
    fn validate_history_v1_nexus_operation_scheduled_event_attributes_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payload,
    ) -> FieldValidationOutcome {
        check_size(ctx, "temporal.api.history.v1.NexusOperationScheduledEventAttributes.input", payload_encoded_size(value), |l| l.payloads_size)
    }
    fn validate_history_v1_signal_external_workflow_execution_initiated_event_attributes_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.history.v1.SignalExternalWorkflowExecutionInitiatedEventAttributes.input", payloads_encoded_size(value))
    }
    fn validate_history_v1_start_child_workflow_execution_initiated_event_attributes_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.history.v1.StartChildWorkflowExecutionInitiatedEventAttributes.input", payloads_encoded_size(value))
    }
    fn validate_history_v1_workflow_execution_canceled_event_attributes_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.history.v1.WorkflowExecutionCanceledEventAttributes.details", payloads_encoded_size(value))
    }
    fn validate_history_v1_workflow_execution_completed_event_attributes_result(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.history.v1.WorkflowExecutionCompletedEventAttributes.result", payloads_encoded_size(value))
    }
    fn validate_history_v1_workflow_execution_continued_as_new_event_attributes_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.history.v1.WorkflowExecutionContinuedAsNewEventAttributes.input", payloads_encoded_size(value))
    }
    fn validate_history_v1_workflow_execution_continued_as_new_event_attributes_last_completion_result(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.history.v1.WorkflowExecutionContinuedAsNewEventAttributes.last_completion_result", payloads_encoded_size(value))
    }
    fn validate_history_v1_workflow_execution_signaled_event_attributes_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.history.v1.WorkflowExecutionSignaledEventAttributes.input", payloads_encoded_size(value))
    }
    fn validate_history_v1_workflow_execution_started_event_attributes_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.history.v1.WorkflowExecutionStartedEventAttributes.input", payloads_encoded_size(value))
    }
    fn validate_history_v1_workflow_execution_started_event_attributes_last_completion_result(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.history.v1.WorkflowExecutionStartedEventAttributes.last_completion_result", payloads_encoded_size(value))
    }
    fn validate_history_v1_workflow_execution_terminated_event_attributes_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.history.v1.WorkflowExecutionTerminatedEventAttributes.details", payloads_encoded_size(value))
    }
    fn validate_nexus_v1_endpoint_spec_description(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payload,
    ) -> FieldValidationOutcome {
        check_size(ctx, "temporal.api.nexus.v1.EndpointSpec.description", payload_encoded_size(value), |l| l.payloads_size)
    }
    fn validate_nexus_v1_start_operation_request_payload(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payload,
    ) -> FieldValidationOutcome {
        check_size(ctx, "temporal.api.nexus.v1.StartOperationRequest.payload", payload_encoded_size(value), |l| l.payloads_size)
    }
    fn validate_nexus_v1_start_operation_response_sync_payload(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payload,
    ) -> FieldValidationOutcome {
        check_size(ctx, "temporal.api.nexus.v1.StartOperationResponse.Sync.payload", payload_encoded_size(value), |l| l.payloads_size)
    }
    fn validate_query_v1_workflow_query_query_args(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.query.v1.WorkflowQuery.query_args", payloads_encoded_size(value))
    }
    fn validate_query_v1_workflow_query_result_answer(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.query.v1.WorkflowQueryResult.answer", payloads_encoded_size(value))
    }
    fn validate_sdk_v1_user_metadata_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payload,
    ) -> FieldValidationOutcome {
        check_size(ctx, "temporal.api.sdk.v1.UserMetadata.details", payload_encoded_size(value), |l| l.payloads_size)
    }
    fn validate_sdk_v1_user_metadata_summary(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payload,
    ) -> FieldValidationOutcome {
        check_size(ctx, "temporal.api.sdk.v1.UserMetadata.summary", payload_encoded_size(value), |l| l.payloads_size)
    }
    fn validate_update_v1_input_args(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.update.v1.Input.args", payloads_encoded_size(value))
    }
    fn validate_workflow_v1_new_workflow_execution_info_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflow.v1.NewWorkflowExecutionInfo.input", payloads_encoded_size(value))
    }
    fn validate_workflow_v1_pending_activity_info_heartbeat_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflow.v1.PendingActivityInfo.heartbeat_details", payloads_encoded_size(value))
    }
    fn validate_workflow_v1_post_reset_operation_signal_workflow_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflow.v1.PostResetOperation.SignalWorkflow.input", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_count_activity_executions_response_aggregation_group_group_values(
        &self,
        ctx: &ValidationContext<'_>,
        value: &[crate::protos::temporal::api::common::v1::Payload],
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.CountActivityExecutionsResponse.AggregationGroup.group_values", value.iter().map(payload_encoded_size).sum())
    }
    fn validate_workflowservice_v1_count_nexus_operation_executions_response_aggregation_group_group_values(
        &self,
        ctx: &ValidationContext<'_>,
        value: &[crate::protos::temporal::api::common::v1::Payload],
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.CountNexusOperationExecutionsResponse.AggregationGroup.group_values", value.iter().map(payload_encoded_size).sum())
    }
    fn validate_workflowservice_v1_count_schedules_response_aggregation_group_group_values(
        &self,
        ctx: &ValidationContext<'_>,
        value: &[crate::protos::temporal::api::common::v1::Payload],
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.CountSchedulesResponse.AggregationGroup.group_values", value.iter().map(payload_encoded_size).sum())
    }
    fn validate_workflowservice_v1_count_workflow_executions_response_aggregation_group_group_values(
        &self,
        ctx: &ValidationContext<'_>,
        value: &[crate::protos::temporal::api::common::v1::Payload],
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.CountWorkflowExecutionsResponse.AggregationGroup.group_values", value.iter().map(payload_encoded_size).sum())
    }
    fn validate_workflowservice_v1_describe_activity_execution_response_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.DescribeActivityExecutionResponse.input", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_describe_nexus_operation_execution_response_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payload,
    ) -> FieldValidationOutcome {
        check_size(ctx, "temporal.api.workflowservice.v1.DescribeNexusOperationExecutionResponse.input", payload_encoded_size(value), |l| l.payloads_size)
    }
    fn validate_workflowservice_v1_poll_activity_task_queue_response_heartbeat_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.PollActivityTaskQueueResponse.heartbeat_details", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_poll_activity_task_queue_response_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.PollActivityTaskQueueResponse.input", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_query_workflow_response_query_result(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.QueryWorkflowResponse.query_result", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_record_activity_task_heartbeat_by_id_request_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.RecordActivityTaskHeartbeatByIdRequest.details", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_record_activity_task_heartbeat_request_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.RecordActivityTaskHeartbeatRequest.details", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_respond_activity_task_canceled_by_id_request_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.RespondActivityTaskCanceledByIdRequest.details", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_respond_activity_task_canceled_request_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.RespondActivityTaskCanceledRequest.details", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_respond_activity_task_completed_by_id_request_result(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.RespondActivityTaskCompletedByIdRequest.result", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_respond_activity_task_completed_request_result(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.RespondActivityTaskCompletedRequest.result", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_respond_activity_task_failed_by_id_request_last_heartbeat_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.RespondActivityTaskFailedByIdRequest.last_heartbeat_details", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_respond_activity_task_failed_request_last_heartbeat_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.RespondActivityTaskFailedRequest.last_heartbeat_details", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_respond_query_task_completed_request_query_result(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.RespondQueryTaskCompletedRequest.query_result", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_signal_with_start_workflow_execution_request_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest.input", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_signal_with_start_workflow_execution_request_signal_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest.signal_input", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_signal_workflow_execution_request_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.SignalWorkflowExecutionRequest.input", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_start_activity_execution_request_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.StartActivityExecutionRequest.input", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_start_nexus_operation_execution_request_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payload,
    ) -> FieldValidationOutcome {
        check_size(ctx, "temporal.api.workflowservice.v1.StartNexusOperationExecutionRequest.input", payload_encoded_size(value), |l| l.payloads_size)
    }
    fn validate_workflowservice_v1_start_workflow_execution_request_input(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.StartWorkflowExecutionRequest.input", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_start_workflow_execution_request_last_completion_result(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.StartWorkflowExecutionRequest.last_completion_result", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_terminate_workflow_execution_request_details(
        &self,
        ctx: &ValidationContext<'_>,
        value: &crate::protos::temporal::api::common::v1::Payloads,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.TerminateWorkflowExecutionRequest.details", payloads_encoded_size(value))
    }
    fn validate_workflowservice_v1_update_worker_deployment_version_metadata_request_upsert_entries(
        &self,
        ctx: &ValidationContext<'_>,
        value: &::std::collections::HashMap<String, crate::protos::temporal::api::common::v1::Payload>,
    ) -> FieldValidationOutcome {
        check_payloads(ctx, "temporal.api.workflowservice.v1.UpdateWorkerDeploymentVersionMetadataRequest.upsert_entries", map_payload_encoded_size(value))
    }
}