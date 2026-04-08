use prost::Message;
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorSet, MessageOptions,
    field_descriptor_proto::Type,
};
use std::{
    collections::{HashMap, HashSet},
    env,
    io::Write,
    path::PathBuf,
};
use tonic_prost_build::Config;

/// RPC methods that carry payloads but should only have the warn limit applied, never the error
/// limit. These are failure-proto RPCs where the payload is failure details, not user results.
const WARN_ONLY_FROM_ERROR_CHECK: &[&str] = &[
    "RespondWorkflowTaskFailed",
    "RespondActivityTaskFailed",
    "RespondActivityTaskFailedById",
    "RespondNexusTaskFailed",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let protos_dir = manifest_dir.join("../common/protos").canonicalize()?;
    println!("cargo:rerun-if-changed={}", protos_dir.display());

    let out = PathBuf::from(env::var("OUT_DIR")?);
    let descriptor_file = out.join("client_descriptors.bin");

    // Compile the WorkflowService proto to get a descriptor. The generated Rust files
    // land in OUT_DIR but are not included — we only need the descriptor binary.
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .file_descriptor_set_path(&descriptor_file)
        .compile_with_config(
            Config::new(),
            &[protos_dir.join("api_upstream/temporal/api/workflowservice/v1/service.proto")],
            &[
                protos_dir.join("api_upstream"),
                protos_dir.join("api_cloud_upstream"),
                protos_dir.join("local"),
                protos_dir.join("testsrv_upstream"),
                protos_dir.join("grpc"),
                protos_dir.clone(),
            ],
        )?;

    generate_payload_checking_impl(&out, &descriptor_file)
}

fn generate_payload_checking_impl(
    out_dir: &std::path::Path,
    descriptor_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let descriptor_bytes = std::fs::read(descriptor_path)?;
    let descriptor_set = FileDescriptorSet::decode(&descriptor_bytes[..])?;

    // Collect all message types from the descriptor.
    let mut messages: HashMap<String, DescriptorProto> = HashMap::new();
    for file in &descriptor_set.file {
        let package = file.package.as_deref().unwrap_or("");
        for msg in &file.message_type {
            collect_messages(package, msg, &mut messages);
        }
    }

    // Determine which types transitively contain Payload, Payloads, or Memo.
    let mut detector = PayloadDetector {
        messages: &messages,
        payload_containing: HashSet::new(),
        not_containing: HashSet::new(),
        checking: HashSet::new(),
    };
    for name in messages.keys().cloned().collect::<Vec<_>>() {
        detector.check_contains_payload(&name);
    }
    let payload_containing = detector.payload_containing;

    // Categorize each WorkflowService method.
    let mut forward_methods: Vec<String> = Vec::new();
    let mut warn_only_methods: Vec<String> = Vec::new();
    let mut check_methods: Vec<String> = Vec::new();
    for file in &descriptor_set.file {
        for service in &file.service {
            if service.name.as_deref() != Some("WorkflowService") {
                continue;
            }
            for method in &service.method {
                let rpc_name = method.name.as_deref().unwrap_or("");
                let input = method
                    .input_type
                    .as_deref()
                    .unwrap_or("")
                    .trim_start_matches('.');
                let output = method
                    .output_type
                    .as_deref()
                    .unwrap_or("")
                    .trim_start_matches('.');
                let req_type = last_component(input);
                let resp_type = last_component(output);
                let method_name = to_snake_case(rpc_name);
                let entry = format!("    ({method_name}, {req_type}, {resp_type})");
                let warn_only = WARN_ONLY_FROM_ERROR_CHECK.iter().any(|&e| e == rpc_name);
                if warn_only && payload_containing.contains(input) {
                    warn_only_methods.push(entry);
                } else if payload_containing.contains(input) {
                    check_methods.push(entry);
                } else {
                    forward_methods.push(entry);
                }
            }
        }
    }

    // Write the generated impl file.
    let mut output = String::from(
        "// Generated from proto descriptors — DO NOT EDIT\n\
                       impl WorkflowService for PayloadCheckingWorkflowService {\n\
                       forward_workflow_methods!(\n",
    );
    for entry in &forward_methods {
        output.push_str(entry);
        output.push_str(";\n");
    }
    output.push_str(");\n\ncheck_warn_and_forward_methods!(\n");
    for entry in &warn_only_methods {
        output.push_str(entry);
        output.push_str(";\n");
    }
    output.push_str(");\n\ncheck_error_and_forward_methods!(\n");
    for entry in &check_methods {
        output.push_str(entry);
        output.push_str(";\n");
    }
    output.push_str(");\n}\n");

    let out_file = out_dir.join("payload_checking_service_impl.rs");
    let mut file = std::fs::File::create(&out_file)?;
    file.write_all(output.as_bytes())?;
    Ok(())
}

fn collect_messages(
    package: &str,
    msg: &DescriptorProto,
    out: &mut HashMap<String, DescriptorProto>,
) {
    let name = msg.name.as_deref().unwrap_or("");
    let full_name = if package.is_empty() {
        name.to_string()
    } else {
        format!("{}.{}", package, name)
    };
    out.insert(full_name.clone(), msg.clone());
    for nested in &msg.nested_type {
        if !is_map_entry(&nested.options) {
            collect_messages(&full_name, nested, out);
        }
    }
}

struct PayloadDetector<'a> {
    messages: &'a HashMap<String, DescriptorProto>,
    payload_containing: HashSet<String>,
    not_containing: HashSet<String>,
    checking: HashSet<String>,
}

impl PayloadDetector<'_> {
    fn check_contains_payload(&mut self, name: &str) -> bool {
        if self.payload_containing.contains(name) {
            return true;
        }
        if self.not_containing.contains(name) {
            return false;
        }
        if self.checking.contains(name) {
            return false;
        }
        // Base cases — leaf payload types.
        if matches!(
            name,
            "temporal.api.common.v1.Payload"
                | "temporal.api.common.v1.Payloads"
                | "temporal.api.common.v1.Memo"
        ) {
            self.payload_containing.insert(name.to_string());
            return true;
        }
        let msg = match self.messages.get(name) {
            Some(m) => m.clone(),
            None => return false,
        };
        self.checking.insert(name.to_string());
        for field in &msg.field {
            if self.field_contains_payload(&msg, field) {
                self.checking.remove(name);
                self.payload_containing.insert(name.to_string());
                return true;
            }
        }
        self.checking.remove(name);
        self.not_containing.insert(name.to_string());
        false
    }

    fn field_contains_payload(
        &mut self,
        msg: &DescriptorProto,
        field: &FieldDescriptorProto,
    ) -> bool {
        if field.r#type != Some(Type::Message as i32) {
            return false;
        }
        let type_name = field
            .type_name
            .as_deref()
            .unwrap_or("")
            .trim_start_matches('.');
        // Check map fields by inspecting the map entry's value type.
        let map_entry_name = to_map_entry_name(field.name.as_deref().unwrap_or(""));
        if let Some(nested) = msg
            .nested_type
            .iter()
            .find(|n| is_map_entry(&n.options) && n.name.as_deref() == Some(&map_entry_name))
        {
            if let Some(value_field) = nested
                .field
                .iter()
                .find(|f| f.name.as_deref() == Some("value"))
            {
                let value_type = value_field
                    .type_name
                    .as_deref()
                    .unwrap_or("")
                    .trim_start_matches('.');
                return self.check_contains_payload(value_type);
            }
        }
        self.check_contains_payload(type_name)
    }
}

fn last_component(s: &str) -> &str {
    s.rsplit('.').next().unwrap_or(s)
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }
    result
}

fn to_map_entry_name(field_name: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = true;
    for c in field_name.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }
    result.push_str("Entry");
    result
}

fn is_map_entry(options: &Option<MessageOptions>) -> bool {
    options
        .as_ref()
        .is_some_and(|o| o.map_entry.unwrap_or(false))
}
