use prost::Message;
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorSet, MessageOptions,
    field_descriptor_proto::{Label, Type},
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    env,
    fs::File,
    io::{Read, Write},
    path::Path,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=DEP_TEMPORALIO_PROTOS_DESCRIPTOR_PATH");
    let out = std::path::PathBuf::from(env::var("OUT_DIR").unwrap());
    let descriptor_file = std::path::PathBuf::from(
        env::var("DEP_TEMPORALIO_PROTOS_DESCRIPTOR_PATH")
            .map_err(|_| "temporalio-protos did not publish descriptor metadata")?,
    );

    let mut descriptor_bytes = Vec::new();
    File::open(&descriptor_file)?.read_to_end(&mut descriptor_bytes)?;
    let descriptor_set = FileDescriptorSet::decode(&descriptor_bytes[..])?;

    generate_payload_visitor(&out, &descriptor_set)?;
    generate_payload_validator(&out, &descriptor_set)?;
    Ok(())
}

/// Generate PayloadVisitable implementations by parsing proto descriptors.
fn generate_payload_visitor(
    out_dir: &Path,
    descriptor_set: &FileDescriptorSet,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut generator = PayloadVisitorGenerator::new();
    generator.process_descriptors(descriptor_set);

    let output_path = out_dir.join("payload_visitor_impl.rs");
    let mut file = File::create(&output_path)?;
    file.write_all(generator.generate().as_bytes())?;

    Ok(())
}

/// Generate PayloadFieldValidator trait + ValidateRequest impls by parsing proto descriptors.
fn generate_payload_validator(
    out_dir: &Path,
    descriptor_set: &FileDescriptorSet,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut generator = PayloadValidatorGenerator::new();
    generator.process_descriptors(descriptor_set);

    let output_path = out_dir.join("payload_validation_impl.rs");
    let mut file = File::create(&output_path)?;
    file.write_all(generator.generate().as_bytes())?;

    Ok(())
}

/// Stores information about a message field that contains payloads.
#[derive(Debug, Clone)]
struct PayloadFieldInfo {
    /// The proto field name
    name: String,
    /// The fully qualified proto path for the field
    proto_path: String,
    /// What kind of payload field this is
    kind: PayloadFieldKind,
}

#[derive(Debug, Clone)]
enum PayloadFieldKind {
    /// A singular Payload field
    SinglePayload,
    /// A repeated Payload field
    RepeatedPayload,
    /// A Payloads message field
    PayloadsMessage,
    /// A map with Payload values
    MapPayload,
    /// A map with nested message values that contain payloads
    MapNestedMessage,
    /// A nested message that contains payloads
    NestedMessage,
    /// A oneof that may contain payloads
    Oneof {
        /// The name of the oneof field
        oneof_name: String,
        /// Payload-containing variants
        variants: Vec<OneofVariant>,
        /// Total number of variants in the oneof (to know if we need a catch-all)
        total_variants: usize,
    },
}

#[derive(Debug, Clone)]
struct OneofVariant {
    name: String,
}

/// Generator for PayloadVisitable implementations.
struct PayloadVisitorGenerator {
    /// Maps fully qualified message names to their descriptors
    messages: HashMap<String, DescriptorProto>,
    /// Messages that contain Payloads (directly or transitively)
    payload_containing: HashSet<String>,
    /// Types currently being checked (for cycle detection)
    checking: HashSet<String>,
    /// Types that have been checked and don't contain payloads
    not_payload_containing: HashSet<String>,
    /// The payload fields for each message
    message_fields: HashMap<String, Vec<PayloadFieldInfo>>,
}

impl PayloadVisitorGenerator {
    fn new() -> Self {
        Self {
            messages: HashMap::new(),
            payload_containing: HashSet::new(),
            checking: HashSet::new(),
            not_payload_containing: HashSet::new(),
            message_fields: HashMap::new(),
        }
    }

    fn process_descriptors(&mut self, descriptor_set: &FileDescriptorSet) {
        // First pass: collect all message types
        for file in &descriptor_set.file {
            let package = file.package.as_deref().unwrap_or("");
            for msg in &file.message_type {
                self.collect_messages(package, msg);
            }
        }

        // Second pass: find payload-containing types
        let all_names: Vec<String> = self.messages.keys().cloned().collect();
        for name in &all_names {
            self.check_contains_payload(name);
        }

        // Third pass: build field info for payload-containing types
        for name in self.payload_containing.clone() {
            self.build_field_info(&name);
        }
    }

    fn collect_messages(&mut self, package: &str, msg: &DescriptorProto) {
        let name = msg.name.as_deref().unwrap_or("");
        let full_name = if package.is_empty() {
            name.to_string()
        } else {
            format!("{}.{}", package, name)
        };

        self.messages.insert(full_name.clone(), msg.clone());

        // Collect nested types
        for nested in &msg.nested_type {
            // Skip map entry types
            if is_map_entry(&nested.options) {
                continue;
            }
            self.collect_messages(&full_name, nested);
        }
    }

    fn check_contains_payload(&mut self, name: &str) -> bool {
        // Already determined to contain payloads
        if self.payload_containing.contains(name) {
            return true;
        }

        // Already determined to not contain payloads
        if self.not_payload_containing.contains(name) {
            return false;
        }

        // Currently checking this type - break the cycle
        if self.checking.contains(name) {
            return false;
        }

        // Base cases
        if name == "temporal.api.common.v1.Payload" {
            self.payload_containing.insert(name.to_string());
            return true;
        }
        if name == "temporal.api.common.v1.Payloads" {
            self.payload_containing.insert(name.to_string());
            return true;
        }

        let msg = match self.messages.get(name) {
            Some(m) => m.clone(),
            None => return false,
        };

        // Mark as currently checking
        self.checking.insert(name.to_string());

        // Check each field
        for field in &msg.field {
            if self.field_contains_payload(&msg, field) {
                self.checking.remove(name);
                self.payload_containing.insert(name.to_string());
                return true;
            }
        }

        // Done checking - doesn't contain payloads
        self.checking.remove(name);
        self.not_payload_containing.insert(name.to_string());
        false
    }

    fn field_contains_payload(
        &mut self,
        msg: &DescriptorProto,
        field: &FieldDescriptorProto,
    ) -> bool {
        if is_message_type(field) {
            let type_name = field.type_name.as_deref().unwrap_or("");
            let type_name = type_name.trim_start_matches('.');

            // Check if this is a map type
            if let Some(nested) = msg.nested_type.iter().find(|n| {
                is_map_entry(&n.options)
                    && n.name.as_deref()
                        == Some(&Self::to_map_entry_name(
                            field.name.as_deref().unwrap_or(""),
                        ))
            }) {
                // It's a map - check the value type
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

            return self.check_contains_payload(type_name);
        }

        false
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

    fn build_field_info(&mut self, name: &str) {
        if self.message_fields.contains_key(name) {
            return;
        }

        // Skip Payload and Payloads - they are leaf types
        if name == "temporal.api.common.v1.Payload" || name == "temporal.api.common.v1.Payloads" {
            return;
        }

        let msg = match self.messages.get(name) {
            Some(m) => m.clone(),
            None => return,
        };

        let mut fields = Vec::new();

        // Group fields by oneof
        let mut oneof_fields: HashMap<i32, Vec<&FieldDescriptorProto>> = HashMap::new();
        let mut regular_fields: Vec<&FieldDescriptorProto> = Vec::new();

        for field in &msg.field {
            if let Some(oneof_index) = field.oneof_index {
                oneof_fields.entry(oneof_index).or_default().push(field);
            } else {
                regular_fields.push(field);
            }
        }

        // Process regular fields
        for field in regular_fields {
            if let Some(info) = self.build_single_field_info(name, &msg, field) {
                fields.push(info);
            }
        }

        // Process oneofs
        for (oneof_index, oneof_field_list) in oneof_fields {
            let oneof_desc = &msg.oneof_decl[oneof_index as usize];
            let oneof_name = oneof_desc.name.as_deref().unwrap_or("");

            let total_variants = oneof_field_list.len();
            let mut variants = Vec::new();
            for field in oneof_field_list {
                if is_message_type(field) {
                    let type_name = field
                        .type_name
                        .as_deref()
                        .unwrap_or("")
                        .trim_start_matches('.');
                    if self.payload_containing.contains(type_name) {
                        variants.push(OneofVariant {
                            name: field.name.clone().unwrap_or_default(),
                        });
                    }
                }
            }

            if !variants.is_empty() {
                fields.push(PayloadFieldInfo {
                    name: oneof_name.to_string(),
                    proto_path: format!("{}.{}", name, oneof_name),
                    kind: PayloadFieldKind::Oneof {
                        oneof_name: oneof_name.to_string(),
                        variants,
                        total_variants,
                    },
                });
            }
        }

        self.message_fields.insert(name.to_string(), fields);
    }

    fn build_single_field_info(
        &self,
        parent_name: &str,
        parent_msg: &DescriptorProto,
        field: &FieldDescriptorProto,
    ) -> Option<PayloadFieldInfo> {
        let field_name = field.name.as_deref().unwrap_or("");
        let proto_path = format!("{}.{}", parent_name, field_name);

        if !is_message_type(field) {
            return None;
        }

        let type_name = field
            .type_name
            .as_deref()
            .unwrap_or("")
            .trim_start_matches('.');

        // Check if it's a map
        if let Some(nested) = parent_msg.nested_type.iter().find(|n| {
            is_map_entry(&n.options)
                && n.name.as_deref() == Some(&Self::to_map_entry_name(field_name))
        }) {
            let value_field = nested
                .field
                .iter()
                .find(|f| f.name.as_deref() == Some("value"))?;
            let value_type = value_field
                .type_name
                .as_deref()
                .unwrap_or("")
                .trim_start_matches('.');

            if !self.payload_containing.contains(value_type) {
                return None;
            }

            if value_type == "temporal.api.common.v1.Payload" {
                return Some(PayloadFieldInfo {
                    name: field_name.to_string(),
                    proto_path,
                    kind: PayloadFieldKind::MapPayload,
                });
            } else {
                return Some(PayloadFieldInfo {
                    name: field_name.to_string(),
                    proto_path,
                    kind: PayloadFieldKind::MapNestedMessage,
                });
            }
        }

        if !self.payload_containing.contains(type_name) {
            return None;
        }

        let is_repeated = is_repeated(field);

        if type_name == "temporal.api.common.v1.Payload" {
            Some(PayloadFieldInfo {
                name: field_name.to_string(),
                proto_path,
                kind: if is_repeated {
                    PayloadFieldKind::RepeatedPayload
                } else {
                    PayloadFieldKind::SinglePayload
                },
            })
        } else if type_name == "temporal.api.common.v1.Payloads" {
            Some(PayloadFieldInfo {
                name: field_name.to_string(),
                proto_path,
                kind: PayloadFieldKind::PayloadsMessage,
            })
        } else {
            Some(PayloadFieldInfo {
                name: field_name.to_string(),
                proto_path,
                kind: PayloadFieldKind::NestedMessage,
            })
        }
    }

    fn generate(&self) -> String {
        let mut output = String::new();
        output.push_str("// Generated from descriptors.bin - DO NOT EDIT\n\n");

        // Generate impls for each payload-containing type
        for name in self.payload_containing.iter() {
            if name == "temporal.api.common.v1.Payload" || name == "temporal.api.common.v1.Payloads"
            {
                continue;
            }
            if let Some(fields) = self.message_fields.get(name) {
                output.push_str(&self.generate_impl(name, fields));
                output.push('\n');
            }
        }

        output
    }

    fn generate_impl(&self, proto_name: &str, fields: &[PayloadFieldInfo]) -> String {
        let rust_path = self.proto_to_rust_path(proto_name);

        let mut impl_body = String::new();

        for field in fields {
            impl_body.push_str(&self.generate_field_visit(
                &field.name,
                &field.proto_path,
                &field.kind,
            ));
        }

        format!(
            r#"#[allow(deprecated, clippy::single_match, clippy::collapsible_match)]
impl crate::payload_visitor::PayloadVisitable for {rust_path} {{
    fn visit_payloads_mut<'a>(
        &'a mut self,
        visitor: &'a mut (dyn crate::payload_visitor::AsyncPayloadVisitor + Send),
    ) -> futures::future::BoxFuture<'a, ()> {{
        Box::pin(async move {{
{impl_body}        }})
    }}
}}
"#,
            rust_path = rust_path,
            impl_body = impl_body
        )
    }

    fn generate_field_visit(
        &self,
        field_name: &str,
        proto_path: &str,
        kind: &PayloadFieldKind,
    ) -> String {
        let rust_field = Self::to_snake_case(field_name);

        match kind {
            PayloadFieldKind::SinglePayload => {
                format!(
                    r#"        if let Some(payload) = &mut self.{field} {{
            visitor.visit(crate::payload_visitor::PayloadField {{
                path: "{path}",
                data: crate::payload_visitor::PayloadFieldData::Single(payload),
            }}).await;
        }}
"#,
                    field = rust_field,
                    path = proto_path
                )
            }
            PayloadFieldKind::RepeatedPayload => {
                format!(
                    r#"        visitor.visit(crate::payload_visitor::PayloadField {{
            path: "{path}",
            data: crate::payload_visitor::PayloadFieldData::Repeated(&mut self.{field}),
        }}).await;
"#,
                    field = rust_field,
                    path = proto_path
                )
            }
            PayloadFieldKind::PayloadsMessage => {
                format!(
                    r#"        if let Some(payloads) = &mut self.{field} {{
            visitor.visit(crate::payload_visitor::PayloadField {{
                path: "{path}",
                data: crate::payload_visitor::PayloadFieldData::Payloads(payloads),
            }}).await;
        }}
"#,
                    field = rust_field,
                    path = proto_path
                )
            }
            PayloadFieldKind::MapPayload => {
                format!(
                    r#"        for payload in self.{field}.values_mut() {{
            visitor.visit(crate::payload_visitor::PayloadField {{
                path: "{path}",
                data: crate::payload_visitor::PayloadFieldData::Single(payload),
            }}).await;
        }}
"#,
                    field = rust_field,
                    path = proto_path
                )
            }
            PayloadFieldKind::MapNestedMessage => {
                format!(
                    r#"        for item in self.{field}.values_mut() {{
            item.visit_payloads_mut(visitor).await;
        }}
"#,
                    field = rust_field
                )
            }
            PayloadFieldKind::NestedMessage => {
                // Check if the field in the parent is repeated
                let parent_name = proto_path.rsplit_once('.').map(|(p, _)| p).unwrap_or("");
                let is_field_repeated = if let Some(msg) = self.messages.get(parent_name) {
                    msg.field
                        .iter()
                        .any(|f| f.name.as_deref() == Some(field_name) && is_repeated(f))
                } else {
                    false
                };

                if is_field_repeated {
                    format!(
                        r#"        for item in &mut self.{field} {{
            item.visit_payloads_mut(visitor).await;
        }}
"#,
                        field = rust_field
                    )
                } else {
                    format!(
                        r#"        if let Some(msg) = &mut self.{field} {{
            msg.visit_payloads_mut(visitor).await;
        }}
"#,
                        field = rust_field
                    )
                }
            }
            PayloadFieldKind::Oneof {
                oneof_name,
                variants,
                total_variants,
            } => {
                // Compute the parent proto name from the proto_path
                let parent_proto_name = proto_path.rsplit_once('.').map(|(p, _)| p).unwrap_or("");
                // Get the full rust path to the oneof enum
                let enum_path = self.proto_to_rust_oneof_enum_path(parent_proto_name, oneof_name);
                // The field in the struct is snake_case of the oneof field name
                let rust_field = Self::to_snake_case(oneof_name);

                let mut arms = String::new();

                for variant in variants {
                    let variant_name = Self::to_pascal_case(&variant.name);
                    arms.push_str(&format!(
                        "                {enum_path}::{variant}(msg) => msg.visit_payloads_mut(visitor).await,\n",
                        enum_path = enum_path,
                        variant = variant_name
                    ));
                }

                if arms.is_empty() {
                    return String::new();
                }

                // Only add catch-all if not all variants are payload-containing
                let catch_all = if variants.len() < *total_variants {
                    "                _ => {}\n"
                } else {
                    ""
                };

                format!(
                    r#"        if let Some({field}) = &mut self.{field} {{
            match {field} {{
{arms}{catch_all}            }}
        }}
"#,
                    field = rust_field,
                    arms = arms,
                    catch_all = catch_all
                )
            }
        }
    }

    fn proto_to_rust_path(&self, proto_name: &str) -> String {
        let parts: Vec<&str> = proto_name.split('.').collect();
        let mut rust_parts = Vec::new();

        // Handle the package -> module mapping
        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                // Last part is the type name - keep PascalCase
                rust_parts.push((*part).to_string());
            } else {
                // Package parts become snake_case modules
                rust_parts.push(Self::to_snake_case(part));
            }
        }

        // The protos module structure
        let path = rust_parts.join("::");

        // Map to the actual crate paths
        format!("crate::protos::{}", path)
    }

    fn proto_to_rust_oneof_enum_path(&self, parent_proto_name: &str, oneof_name: &str) -> String {
        let parts: Vec<&str> = parent_proto_name.split('.').collect();
        let mut rust_parts = Vec::new();

        // All parts become snake_case modules (struct name becomes a module containing the enum)
        for part in parts.iter() {
            rust_parts.push(Self::to_snake_case(part));
        }

        let module_path = rust_parts.join("::");
        // The enum name is PascalCase of the oneof field name
        let enum_name = Self::to_pascal_case(oneof_name);

        format!("crate::protos::{}::{}", module_path, enum_name)
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

    fn to_pascal_case(s: &str) -> String {
        let mut result = String::new();
        let mut capitalize_next = true;
        for c in s.chars() {
            if c == '_' {
                capitalize_next = true;
            } else if capitalize_next {
                result.push(c.to_ascii_uppercase());
                capitalize_next = false;
            } else {
                result.push(c);
            }
        }
        result
    }
}

fn is_message_type(field: &FieldDescriptorProto) -> bool {
    field.r#type == Some(Type::Message as i32)
}

fn is_repeated(field: &FieldDescriptorProto) -> bool {
    field.label == Some(Label::Repeated as i32)
}

fn is_map_entry(options: &Option<MessageOptions>) -> bool {
    options
        .as_ref()
        .is_some_and(|o| o.map_entry.unwrap_or(false))
}

// =============================================================================================
// Payload validation codegen
// =============================================================================================
//
// Generates two artifacts in $OUT_DIR/payload_validation_impl.rs:
//
// 1. `pub trait PayloadFieldValidator` with one required method per (in-scope message,
//    direct payload field). "In scope" = payload-containing AND in the `temporal.api.*`
//    namespace. The hand-written `DefaultPayloadFieldValidator` in
//    `crates/common/src/payload_validation/impls.rs` must implement every method —
//    adding a new payload field to any proto will break that file's compilation
//    until the new method is added.
//
// 2. `impl ValidateRequest for T` for:
//      * Every gRPC request type listed in any service in the descriptor set (so the
//        `proxy_impl!` macro can call `validate(...)` uniformly on every request body).
//      * Every payload-containing `temporal.api.*` message (so the dispatch can recurse
//        through nested messages).
//    The trait + a no-op default body live in `outcome.rs`-adjacent code; the codegen
//    produces overriding impls.
//
// The `ValidateRequest` trait itself is *not* generated here — it lives in
// `crates/common/src/payload_validation/mod.rs` (declared via this file's emitted
// `pub trait ValidateRequest` at the top of the generated module).

struct PayloadValidatorGenerator {
    /// All messages by fully-qualified proto name.
    messages: HashMap<String, DescriptorProto>,
    /// Payload-containing messages (transitively).
    payload_containing: HashSet<String>,
    checking: HashSet<String>,
    not_payload_containing: HashSet<String>,
    /// Per-message field metadata for payload-bearing messages.
    message_fields: HashMap<String, Vec<PayloadFieldInfo>>,
    /// Every gRPC RPC input type seen in service definitions.
    rpc_request_types: BTreeSet<String>,
}

impl PayloadValidatorGenerator {
    fn new() -> Self {
        Self {
            messages: HashMap::new(),
            payload_containing: HashSet::new(),
            checking: HashSet::new(),
            not_payload_containing: HashSet::new(),
            message_fields: HashMap::new(),
            rpc_request_types: BTreeSet::new(),
        }
    }

    fn process_descriptors(&mut self, descriptor_set: &FileDescriptorSet) {
        // Pass 1: collect messages + service request types.
        for file in &descriptor_set.file {
            let package = file.package.as_deref().unwrap_or("");
            for msg in &file.message_type {
                self.collect_messages(package, msg);
            }
            for service in &file.service {
                for method in &service.method {
                    if let Some(input_type) = method.input_type.as_deref() {
                        self.rpc_request_types
                            .insert(input_type.trim_start_matches('.').to_string());
                    }
                }
            }
        }

        // Pass 2: determine payload-containing messages.
        let all_names: Vec<String> = self.messages.keys().cloned().collect();
        for name in &all_names {
            self.check_contains_payload(name);
        }

        // Pass 3: build field info for payload-containing messages.
        for name in self.payload_containing.clone() {
            self.build_field_info(&name);
        }
    }

    fn collect_messages(&mut self, package: &str, msg: &DescriptorProto) {
        let name = msg.name.as_deref().unwrap_or("");
        let full_name = if package.is_empty() {
            name.to_string()
        } else {
            format!("{}.{}", package, name)
        };
        self.messages.insert(full_name.clone(), msg.clone());
        for nested in &msg.nested_type {
            if is_map_entry(&nested.options) {
                continue;
            }
            self.collect_messages(&full_name, nested);
        }
    }

    fn check_contains_payload(&mut self, name: &str) -> bool {
        if self.payload_containing.contains(name) {
            return true;
        }
        if self.not_payload_containing.contains(name) {
            return false;
        }
        if self.checking.contains(name) {
            return false;
        }
        if name == "temporal.api.common.v1.Payload" || name == "temporal.api.common.v1.Payloads" {
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
        self.not_payload_containing.insert(name.to_string());
        false
    }

    fn field_contains_payload(
        &mut self,
        msg: &DescriptorProto,
        field: &FieldDescriptorProto,
    ) -> bool {
        if !is_message_type(field) {
            return false;
        }
        let type_name = field
            .type_name
            .as_deref()
            .unwrap_or("")
            .trim_start_matches('.');
        if let Some(nested) = msg.nested_type.iter().find(|n| {
            is_map_entry(&n.options)
                && n.name.as_deref()
                    == Some(&to_map_entry_name(field.name.as_deref().unwrap_or("")))
        }) {
            if let Some(value_field) =
                nested.field.iter().find(|f| f.name.as_deref() == Some("value"))
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

    fn build_field_info(&mut self, name: &str) {
        if self.message_fields.contains_key(name) {
            return;
        }
        if name == "temporal.api.common.v1.Payload" || name == "temporal.api.common.v1.Payloads" {
            return;
        }
        let msg = match self.messages.get(name) {
            Some(m) => m.clone(),
            None => return,
        };

        let mut fields: Vec<PayloadFieldInfo> = Vec::new();
        let mut oneof_fields: HashMap<i32, Vec<&FieldDescriptorProto>> = HashMap::new();
        let mut regular_fields: Vec<&FieldDescriptorProto> = Vec::new();
        for field in &msg.field {
            if let Some(oneof_index) = field.oneof_index {
                oneof_fields.entry(oneof_index).or_default().push(field);
            } else {
                regular_fields.push(field);
            }
        }

        for field in regular_fields {
            if let Some(info) = self.build_single_field_info(name, &msg, field) {
                fields.push(info);
            }
        }

        for (oneof_index, oneof_field_list) in oneof_fields {
            let oneof_desc = &msg.oneof_decl[oneof_index as usize];
            let oneof_name = oneof_desc.name.as_deref().unwrap_or("");
            let total_variants = oneof_field_list.len();
            let mut variants = Vec::new();
            for field in oneof_field_list {
                if is_message_type(field) {
                    let type_name = field
                        .type_name
                        .as_deref()
                        .unwrap_or("")
                        .trim_start_matches('.');
                    // Skip oneof variants whose direct type is the leaf Payload/Payloads —
                    // ValidateRequest is not implemented on the leaf types, so we cannot
                    // recurse. If a future need arises, treat these as direct payload
                    // fields keyed by (message, oneof_field, variant).
                    if type_name == "temporal.api.common.v1.Payload"
                        || type_name == "temporal.api.common.v1.Payloads"
                    {
                        continue;
                    }
                    if self.payload_containing.contains(type_name) {
                        variants.push(OneofVariant {
                            name: field.name.clone().unwrap_or_default(),
                        });
                    }
                }
            }
            if !variants.is_empty() {
                fields.push(PayloadFieldInfo {
                    name: oneof_name.to_string(),
                    proto_path: format!("{}.{}", name, oneof_name),
                    kind: PayloadFieldKind::Oneof {
                        oneof_name: oneof_name.to_string(),
                        variants,
                        total_variants,
                    },
                });
            }
        }

        self.message_fields.insert(name.to_string(), fields);
    }

    fn build_single_field_info(
        &self,
        parent_name: &str,
        parent_msg: &DescriptorProto,
        field: &FieldDescriptorProto,
    ) -> Option<PayloadFieldInfo> {
        let field_name = field.name.as_deref().unwrap_or("");
        let proto_path = format!("{}.{}", parent_name, field_name);
        if !is_message_type(field) {
            return None;
        }
        let type_name = field
            .type_name
            .as_deref()
            .unwrap_or("")
            .trim_start_matches('.');
        if let Some(nested) = parent_msg.nested_type.iter().find(|n| {
            is_map_entry(&n.options) && n.name.as_deref() == Some(&to_map_entry_name(field_name))
        }) {
            let value_field = nested
                .field
                .iter()
                .find(|f| f.name.as_deref() == Some("value"))?;
            let value_type = value_field
                .type_name
                .as_deref()
                .unwrap_or("")
                .trim_start_matches('.');
            if !self.payload_containing.contains(value_type) {
                return None;
            }
            if value_type == "temporal.api.common.v1.Payload" {
                return Some(PayloadFieldInfo {
                    name: field_name.to_string(),
                    proto_path,
                    kind: PayloadFieldKind::MapPayload,
                });
            }
            return Some(PayloadFieldInfo {
                name: field_name.to_string(),
                proto_path,
                kind: PayloadFieldKind::MapNestedMessage,
            });
        }
        if !self.payload_containing.contains(type_name) {
            return None;
        }
        let is_rep = is_repeated(field);
        if type_name == "temporal.api.common.v1.Payload" {
            Some(PayloadFieldInfo {
                name: field_name.to_string(),
                proto_path,
                kind: if is_rep {
                    PayloadFieldKind::RepeatedPayload
                } else {
                    PayloadFieldKind::SinglePayload
                },
            })
        } else if type_name == "temporal.api.common.v1.Payloads" {
            Some(PayloadFieldInfo {
                name: field_name.to_string(),
                proto_path,
                kind: PayloadFieldKind::PayloadsMessage,
            })
        } else {
            Some(PayloadFieldInfo {
                name: field_name.to_string(),
                proto_path,
                kind: PayloadFieldKind::NestedMessage,
            })
        }
    }

    /// True if the (fully-qualified) message is in the `temporal.api.*` namespace and not
    /// one of the leaf types (Payload/Payloads).
    fn is_in_scope(name: &str) -> bool {
        name.starts_with("temporal.api.")
            && name != "temporal.api.common.v1.Payload"
            && name != "temporal.api.common.v1.Payloads"
    }

    fn generate(&self) -> String {
        let mut out = String::new();
        out.push_str("// Generated from descriptors.bin - DO NOT EDIT\n\n");

        // Sorted in-scope payload-bearing messages for deterministic output.
        let mut in_scope: Vec<String> = self
            .payload_containing
            .iter()
            .filter(|n| Self::is_in_scope(n))
            .cloned()
            .collect();
        in_scope.sort();

        // Map of method-name -> (proto_path, FieldKind, parent_msg_path) for uniqueness check.
        let mut trait_methods: BTreeMap<String, (String, PayloadFieldKind)> = BTreeMap::new();
        for msg_name in &in_scope {
            let fields = self.message_fields.get(msg_name).cloned().unwrap_or_default();
            for field in &fields {
                if !is_direct_leaf(&field.kind) {
                    continue;
                }
                let method = trait_method_name(msg_name, &field.name);
                if let Some((existing_path, _)) =
                    trait_methods.insert(method.clone(), (field.proto_path.clone(), field.kind.clone()))
                {
                    panic!(
                        "PayloadFieldValidator trait method collision: `{}` is generated by both \
                         `{}` and `{}`. Disambiguate by adjusting trait_method_name().",
                        method, existing_path, field.proto_path,
                    );
                }
            }
        }

        // Emit ValidateRequest trait + PayloadFieldValidator trait.
        out.push_str(&format!(
            r#"/// Implemented for every gRPC request type and every payload-bearing
/// `temporal.api.*` message. Returns the aggregated outcome of running every
/// configured validator over the message's payload-bearing fields.
pub trait ValidateRequest {{
    /// Validate this message against the provided context and field validator.
    fn validate(
        &self,
        ctx: &crate::payload_validation::ValidationContext<'_>,
        v: &dyn PayloadFieldValidator,
    ) -> crate::payload_validation::ValidationOutcome;
}}

/// Auto-generated trait: one required method per `(message, direct-payload-field)` tuple
/// discovered in `temporal.api.*`. The hand-written `DefaultPayloadFieldValidator` in
/// `impls.rs` must implement every method — adding a new payload-bearing field anywhere
/// in `temporal.api.*` will cause that file's compilation to fail until the new method
/// is added, providing exhaustive coverage at build time.
pub trait PayloadFieldValidator: Send + Sync {{
"#
        ));
        for (method, (proto_path, kind)) in &trait_methods {
            let (value_ty, _) = trait_method_value_type(kind);
            out.push_str(&format!(
                "    /// Validate `{path}`.\n    fn {method}(\n        &self,\n        ctx: &crate::payload_validation::ValidationContext<'_>,\n        value: {value_ty},\n    ) -> crate::payload_validation::FieldValidationOutcome;\n",
                method = method,
                path = proto_path,
                value_ty = value_ty,
            ));
        }
        out.push_str("}\n\n");

        // Blanket impl for Box<T> so that fields like `Option<Box<Failure>>` (used for
        // recursive proto types) can recurse into their inner T's ValidateRequest impl.
        out.push_str(
            r#"impl<T: ValidateRequest + ?Sized> ValidateRequest for Box<T> {
    #[inline]
    fn validate(
        &self,
        ctx: &crate::payload_validation::ValidationContext<'_>,
        v: &dyn PayloadFieldValidator,
    ) -> crate::payload_validation::ValidationOutcome {
        (**self).validate(ctx, v)
    }
}

"#,
        );

        // `google.protobuf.Empty` is mapped to `()` by tonic-prost-build; emit an impl
        // for it directly so the macro can call validate() uniformly.
        out.push_str(
            r#"impl ValidateRequest for () {
    #[inline]
    fn validate(
        &self,
        _ctx: &crate::payload_validation::ValidationContext<'_>,
        _v: &dyn PayloadFieldValidator,
    ) -> crate::payload_validation::ValidationOutcome {
        crate::payload_validation::ValidationOutcome::empty()
    }
}

"#,
        );

        // Emit empty `impl ValidateRequest` for the leaf payload types so that nested
        // recursion paths (e.g. map<string, Payloads>) type-check. Direct
        // payload-bearing fields are still checked via `PayloadFieldValidator` trait
        // methods on the parent message; this only suppresses recursion at the leaves.
        out.push_str(
            r#"impl ValidateRequest for crate::protos::temporal::api::common::v1::Payload {
    #[inline]
    fn validate(
        &self,
        _ctx: &crate::payload_validation::ValidationContext<'_>,
        _v: &dyn PayloadFieldValidator,
    ) -> crate::payload_validation::ValidationOutcome {
        crate::payload_validation::ValidationOutcome::empty()
    }
}

impl ValidateRequest for crate::protos::temporal::api::common::v1::Payloads {
    #[inline]
    fn validate(
        &self,
        _ctx: &crate::payload_validation::ValidationContext<'_>,
        _v: &dyn PayloadFieldValidator,
    ) -> crate::payload_validation::ValidationOutcome {
        crate::payload_validation::ValidationOutcome::empty()
    }
}

"#,
        );

        // Emit impl ValidateRequest for every gRPC request type that is *not* in-scope
        // payload-bearing (those get the real impl below). This satisfies the proxy_impl!
        // constraint uniformly without specialization.
        let in_scope_set: BTreeSet<&str> = in_scope.iter().map(String::as_str).collect();
        for req_name in &self.rpc_request_types {
            if in_scope_set.contains(req_name.as_str()) {
                continue;
            }
            // google.protobuf.Empty is mapped to `()` and handled above; skip.
            if req_name.starts_with("google.protobuf.") {
                continue;
            }
            if !self.messages.contains_key(req_name) {
                continue;
            }
            let rust_path = proto_to_rust_path(req_name);
            out.push_str(&format!(
                r#"impl ValidateRequest for {rust} {{
    #[inline]
    fn validate(
        &self,
        _ctx: &crate::payload_validation::ValidationContext<'_>,
        _v: &dyn PayloadFieldValidator,
    ) -> crate::payload_validation::ValidationOutcome {{
        crate::payload_validation::ValidationOutcome::empty()
    }}
}}

"#,
                rust = rust_path
            ));
        }

        // Emit real impl ValidateRequest for every in-scope payload-bearing message.
        for msg_name in &in_scope {
            let fields = self.message_fields.get(msg_name).cloned().unwrap_or_default();
            let rust_path = proto_to_rust_path(msg_name);
            out.push_str(&format!(
                "#[allow(deprecated)]\nimpl ValidateRequest for {rust} {{\n    fn validate(\n        &self,\n        ctx: &crate::payload_validation::ValidationContext<'_>,\n        v: &dyn PayloadFieldValidator,\n    ) -> crate::payload_validation::ValidationOutcome {{\n        let mut outcome = crate::payload_validation::ValidationOutcome::empty();\n",
                rust = rust_path,
            ));
            for field in &fields {
                out.push_str(&self.emit_field_dispatch(msg_name, field));
            }
            out.push_str("        outcome\n    }\n}\n\n");
        }
        out
    }

    fn emit_field_dispatch(&self, parent_proto_name: &str, field: &PayloadFieldInfo) -> String {
        let rust_field = snake(&field.name);
        match &field.kind {
            PayloadFieldKind::SinglePayload => {
                let method = trait_method_name(parent_proto_name, &field.name);
                format!(
                    r#"        if let Some(__pv_payload) = &self.{field} {{
            outcome.extend(v.{method}(ctx, __pv_payload));
        }}
"#,
                    field = rust_field,
                    method = method,
                )
            }
            PayloadFieldKind::RepeatedPayload => {
                let method = trait_method_name(parent_proto_name, &field.name);
                format!(
                    r#"        outcome.extend(v.{method}(ctx, &self.{field}));
"#,
                    field = rust_field,
                    method = method,
                )
            }
            PayloadFieldKind::PayloadsMessage => {
                let method = trait_method_name(parent_proto_name, &field.name);
                format!(
                    r#"        if let Some(__pv_payloads) = &self.{field} {{
            outcome.extend(v.{method}(ctx, __pv_payloads));
        }}
"#,
                    field = rust_field,
                    method = method,
                )
            }
            PayloadFieldKind::MapPayload => {
                let method = trait_method_name(parent_proto_name, &field.name);
                format!(
                    r#"        outcome.extend(v.{method}(ctx, &self.{field}));
"#,
                    field = rust_field,
                    method = method,
                )
            }
            PayloadFieldKind::MapNestedMessage => {
                format!(
                    r#"        for __pv_item in self.{field}.values() {{
            outcome.merge(<_ as ValidateRequest>::validate(__pv_item, ctx, v));
        }}
"#,
                    field = rust_field,
                )
            }
            PayloadFieldKind::NestedMessage => {
                // Need to know if the parent's field is repeated.
                let parent_msg = self.messages.get(parent_proto_name);
                let is_field_repeated = parent_msg
                    .map(|m| {
                        m.field
                            .iter()
                            .any(|f| f.name.as_deref() == Some(&field.name) && is_repeated(f))
                    })
                    .unwrap_or(false);
                if is_field_repeated {
                    format!(
                        r#"        for __pv_item in &self.{field} {{
            outcome.merge(<_ as ValidateRequest>::validate(__pv_item, ctx, v));
        }}
"#,
                        field = rust_field,
                    )
                } else {
                    format!(
                        r#"        if let Some(__pv_msg) = &self.{field} {{
            outcome.merge(<_ as ValidateRequest>::validate(__pv_msg, ctx, v));
        }}
"#,
                        field = rust_field,
                    )
                }
            }
            PayloadFieldKind::Oneof {
                oneof_name,
                variants,
                total_variants,
            } => {
                let parent_path = parent_proto_name;
                let enum_path = proto_to_rust_oneof_enum_path(parent_path, oneof_name);
                let rust_field = snake(oneof_name);
                let mut arms = String::new();
                for variant in variants {
                    let variant_name = pascal(&variant.name);
                    arms.push_str(&format!(
                        "                {enum_path}::{variant}(__pv_inner) => outcome.merge(<_ as ValidateRequest>::validate(__pv_inner, ctx, v)),\n",
                        enum_path = enum_path,
                        variant = variant_name,
                    ));
                }
                if arms.is_empty() {
                    return String::new();
                }
                let catch_all = if variants.len() < *total_variants {
                    "                _ => {}\n"
                } else {
                    ""
                };
                format!(
                    r#"        if let Some(__pv_oneof) = &self.{field} {{
            match __pv_oneof {{
{arms}{catch_all}            }}
        }}
"#,
                    field = rust_field,
                    arms = arms,
                    catch_all = catch_all,
                )
            }
        }
    }
}

/// True if a field kind contributes a required trait method (vs. recursing through a
/// nested message's own ValidateRequest impl).
fn is_direct_leaf(kind: &PayloadFieldKind) -> bool {
    matches!(
        kind,
        PayloadFieldKind::SinglePayload
            | PayloadFieldKind::RepeatedPayload
            | PayloadFieldKind::PayloadsMessage
            | PayloadFieldKind::MapPayload
    )
}

/// Generate the trait method name for a given (message, field) tuple.
///
/// Uses snake_case of the package (with the `temporal_api_` prefix stripped) plus the
/// snake-cased message name and field name, prefixed with `validate_`. Including the
/// package disambiguates messages of the same name across services
/// (e.g. `temporal.api.cloud.nexus.v1.EndpointSpec` vs. `temporal.api.nexus.v1.EndpointSpec`).
///
/// Uniqueness is double-checked at codegen time across all in-scope (message, field)
/// tuples and the build panics on collision.
fn trait_method_name(message_full_name: &str, field_name: &str) -> String {
    // Split off the message name.
    let (package, message) = message_full_name
        .rsplit_once('.')
        .unwrap_or(("", message_full_name));
    // Drop the universal `temporal.api.` prefix to keep names tractable.
    let package_tail = package
        .strip_prefix("temporal.api.")
        .or_else(|| package.strip_prefix("temporal."))
        .unwrap_or(package);
    let package_snake: String = package_tail
        .split('.')
        .map(snake)
        .collect::<Vec<_>>()
        .join("_");
    if package_snake.is_empty() {
        format!("validate_{}_{}", snake(message), snake(field_name))
    } else {
        format!(
            "validate_{}_{}_{}",
            package_snake,
            snake(message),
            snake(field_name)
        )
    }
}

/// Returns (Rust reference type for the value parameter, owned-type description) for a
/// direct-leaf payload field kind.
fn trait_method_value_type(kind: &PayloadFieldKind) -> (&'static str, &'static str) {
    match kind {
        PayloadFieldKind::SinglePayload => (
            "&crate::protos::temporal::api::common::v1::Payload",
            "Payload",
        ),
        PayloadFieldKind::RepeatedPayload => (
            "&[crate::protos::temporal::api::common::v1::Payload]",
            "Vec<Payload>",
        ),
        PayloadFieldKind::PayloadsMessage => (
            "&crate::protos::temporal::api::common::v1::Payloads",
            "Payloads",
        ),
        PayloadFieldKind::MapPayload => (
            "&::std::collections::HashMap<String, crate::protos::temporal::api::common::v1::Payload>",
            "HashMap<String, Payload>",
        ),
        _ => ("", ""),
    }
}

fn proto_to_rust_path(proto_name: &str) -> String {
    // Google well-known types are mapped to prost_types::* by the proto codegen.
    if let Some(name) = proto_name.strip_prefix("google.protobuf.") {
        return format!("::prost_types::{}", name);
    }
    let parts: Vec<&str> = proto_name.split('.').collect();
    let mut rust_parts: Vec<String> = Vec::with_capacity(parts.len());
    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            rust_parts.push((*part).to_string());
        } else {
            rust_parts.push(snake(part));
        }
    }
    format!("crate::protos::{}", rust_parts.join("::"))
}

fn proto_to_rust_oneof_enum_path(parent_proto_name: &str, oneof_name: &str) -> String {
    let parts: Vec<&str> = parent_proto_name.split('.').collect();
    let mut rust_parts: Vec<String> = Vec::with_capacity(parts.len());
    for part in parts.iter() {
        rust_parts.push(snake(part));
    }
    let module_path = rust_parts.join("::");
    format!("crate::protos::{}::{}", module_path, pascal(oneof_name))
}

fn snake(s: &str) -> String {
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

fn pascal(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = true;
    for c in s.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
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
