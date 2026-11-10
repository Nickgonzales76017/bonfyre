const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const fabric = b.addStaticLibrary(.{
        .name = "bonfyre-fabric",
        .target = target,
        .optimize = optimize,
    });
    fabric.linkLibC();
    fabric.addIncludePath(b.path("engine/core/include"));
    fabric.addIncludePath(b.path("lib/libbonfyre/include"));
    fabric.addIncludePath(b.path("cmd/BonfyreLex/src"));
    fabric.addCSourceFiles(.{ .files = &.{ "engine/core/src/fabric.c", "lib/libbonfyre/bf_utf8.c", "engine/core/src/fabric_exec.c", "engine/core/src/lifecycle.c", "engine/core/src/workgraph.c", "engine/core/src/workgraph_schema.c", "engine/core/src/workgraph_events.c", "engine/core/src/workgraph_scheduler.c", "engine/core/src/workgraph_effects.c", "engine/core/src/filesystem_projection.c", "engine/core/src/process_operator.c", "engine/core/src/operator_contract.c", "engine/core/src/admission.c", "engine/core/src/authority.c", "engine/core/src/commitment.c", "engine/core/src/occurrence_origin.c", "engine/core/src/information_policy.c", "engine/core/src/evidence.c", "engine/core/src/commerce.c", "engine/core/src/protocol_contract.c", "engine/core/src/standing_address.c", "engine/core/src/control_provider.c", "engine/core/src/control_admission.c", "engine/core/src/control_attention.c", "engine/core/src/control_capability.c", "engine/core/src/semantic_operator.c", "engine/core/src/semantic_derivation.c", "engine/core/src/agent_contract.c", "engine/core/src/retention.c", "lib/libbonfyre/src/bf_sha256.c", "lib/libbonfyre/src/bf_catalog_generation.c", "lib/libbonfyre/bf_json.c" }, .flags = &.{ "-std=c11", "-D_DEFAULT_SOURCE", "-Wall", "-Wextra", "-Werror" } });
    b.installArtifact(fabric);

    // Replays the control-plane vectors generated from the frozen Python
    // reference. Parity here is what lets the runtime drop that dependency.
    const control_conformance = b.addExecutable(.{ .name = "control_conformance", .target = target, .optimize = optimize });
    control_conformance.linkLibC();
    control_conformance.addIncludePath(b.path("engine/core/include"));
    control_conformance.addCSourceFiles(.{ .files = &.{ "tests/conformance/control/run_native.c", "engine/core/src/control_provider.c", "engine/core/src/control_admission.c", "engine/core/src/control_attention.c", "engine/core/src/control_capability.c" }, .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" } });
    const run_control_conformance = b.addRunArtifact(control_conformance);
    run_control_conformance.addArg("tests/conformance/control/vectors/control.vec");

    const control_conformance_step = b.step("test-control", "Replay control-plane vectors against the native kernel");
    control_conformance_step.dependOn(&run_control_conformance.step);

    const agent_contract_test = b.addExecutable(.{ .name = "agent_contract_test", .target = target, .optimize = optimize });
    agent_contract_test.linkLibC();
    agent_contract_test.addIncludePath(b.path("engine/core/include"));
    agent_contract_test.addCSourceFiles(.{ .files = &.{ "engine/core/tests/agent_contract_test.c", "engine/core/src/agent_contract.c" }, .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" } });
    const run_agent_contract_test = b.addRunArtifact(agent_contract_test);

    const agent_contract_test_step = b.step("test-agent-contract", "Validate AgentSession and ReceiptEnvelope provider fidelity");
    agent_contract_test_step.dependOn(&run_agent_contract_test.step);

    const semantic_operator_test = b.addExecutable(.{ .name = "semantic_operator_test", .target = target, .optimize = optimize });
    semantic_operator_test.linkLibC();
    semantic_operator_test.addIncludePath(b.path("engine/core/include"));
    semantic_operator_test.addIncludePath(b.path("lib/libbonfyre/include"));
    semantic_operator_test.addCSourceFiles(.{ .files = &.{ "engine/core/tests/semantic_operator_test.c", "engine/core/src/semantic_operator.c", "lib/libbonfyre/src/bf_sha256.c" }, .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" } });
    const run_semantic_operator_test = b.addRunArtifact(semantic_operator_test);
    const semantic_operator_test_step = b.step("test-semantic-operator", "Validate source-bound native household semantic lowering");
    semantic_operator_test_step.dependOn(&run_semantic_operator_test.step);

    const probe_test = b.addExecutable(.{ .name = "probe_contract_test", .target = target, .optimize = optimize });
    probe_test.linkLibC();
    probe_test.linkSystemLibrary("sqlite3");
    probe_test.addIncludePath(b.path("engine/core/include"));
    probe_test.addIncludePath(b.path("lib/libbonfyre/include"));
    probe_test.addCSourceFiles(.{ .files = &.{ "engine/core/tests/probe_contract_test.c", "engine/core/src/fabric.c", "engine/core/src/lifecycle.c", "engine/core/src/workgraph.c", "engine/core/src/workgraph_schema.c", "engine/core/src/workgraph_events.c", "engine/core/src/workgraph_scheduler.c", "engine/core/src/workgraph_effects.c", "engine/core/src/filesystem_projection.c", "engine/core/src/process_operator.c", "engine/core/src/operator_contract.c", "engine/core/src/authority.c", "lib/libbonfyre/src/bf_sha256.c", "lib/libbonfyre/src/bf_catalog_generation.c", "lib/libbonfyre/bf_json.c" }, .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" } });
    const run_probe_test = b.addRunArtifact(probe_test);

    inline for ([_][]const u8{ "bonfyre", "bonfyred" }) |name| {
        const exe = b.addExecutable(.{ .name = name, .target = target, .optimize = optimize });
        exe.linkLibC();
        exe.linkSystemLibrary("sqlite3");
        exe.addIncludePath(b.path("engine/core/include"));
        exe.linkLibrary(fabric);
        exe.addCSourceFile(.{ .file = b.path(if (std.mem.eql(u8, name, "bonfyre")) "programs/bonfyre/main.c" else "programs/bonfyred/main.c"), .flags = &.{ "-std=c11" } });
        b.installArtifact(exe);
    }

    const smoke = b.addSystemCommand(&.{ "sh", "tests/fabric_smoke.sh" });
    const frappe = b.addSystemCommand(&.{ "make", "-C", "cmd/BonfyreFrappeCompiler", "test" });
    const unified = b.addSystemCommand(&.{ "sh", "tests/unified_fabric_acceptance.sh" });
    const probe_test_step = b.step("test-probe", "Run the probe_contract_output dispatch test");
    probe_test_step.dependOn(&run_probe_test.step);

    const compensation_test = b.addExecutable(.{ .name = "compensation_evidence_test", .target = target, .optimize = optimize });
    compensation_test.linkLibC();
    compensation_test.linkSystemLibrary("sqlite3");
    compensation_test.addIncludePath(b.path("engine/core/include"));
    compensation_test.addIncludePath(b.path("lib/libbonfyre/include"));
    compensation_test.addCSourceFiles(.{ .files = &.{ "engine/core/tests/compensation_evidence_test.c", "engine/core/src/workgraph.c", "engine/core/src/workgraph_schema.c", "engine/core/src/workgraph_events.c", "engine/core/src/workgraph_scheduler.c", "engine/core/src/workgraph_effects.c", "lib/libbonfyre/src/bf_sha256.c" }, .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" } });
    const run_compensation_test = b.addRunArtifact(compensation_test);

    const compensation_test_step = b.step("test-compensation", "Run the compensation attempt evidence test");
    compensation_test_step.dependOn(&run_compensation_test.step);

    const crash_test = b.addExecutable(.{ .name = "effect_commit_crash_test", .target = target, .optimize = optimize });
    crash_test.linkLibC();
    crash_test.linkSystemLibrary("sqlite3");
    crash_test.addIncludePath(b.path("engine/core/include"));
    crash_test.addIncludePath(b.path("lib/libbonfyre/include"));
    crash_test.addCSourceFiles(.{ .files = &.{ "engine/core/tests/effect_commit_crash_test.c", "engine/core/src/workgraph.c", "engine/core/src/workgraph_schema.c", "engine/core/src/workgraph_events.c", "engine/core/src/workgraph_scheduler.c", "engine/core/src/workgraph_effects.c", "lib/libbonfyre/src/bf_sha256.c" }, .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" } });
    const run_crash_test = b.addRunArtifact(crash_test);

    const crash_test_step = b.step("test-crash", "Run the effect commit crash-consistency test");
    crash_test_step.dependOn(&run_crash_test.step);

    const retention_test = b.addExecutable(.{ .name = "retention_test", .target = target, .optimize = optimize });
    retention_test.linkLibC();
    retention_test.linkSystemLibrary("sqlite3");
    retention_test.addIncludePath(b.path("engine/core/include"));
    retention_test.addCSourceFiles(.{ .files = &.{ "engine/core/tests/retention_test.c", "engine/core/src/retention.c" }, .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" } });
    const run_retention_test = b.addRunArtifact(retention_test);
    const retention_test_step = b.step("test-retention", "Validate native RetentionGC holds, sweep, and tombstones");
    retention_test_step.dependOn(&run_retention_test.step);

    const authority_test = b.addExecutable(.{ .name = "authority_test", .target = target, .optimize = optimize });
    authority_test.linkLibC();
    authority_test.linkSystemLibrary("sqlite3");
    authority_test.addIncludePath(b.path("engine/core/include"));
    authority_test.addCSourceFiles(.{ .files = &.{ "engine/core/tests/authority_test.c", "engine/core/src/authority.c" }, .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" } });
    const run_authority_test = b.addRunArtifact(authority_test);
    const authority_test_step = b.step("test-authority", "Validate native authority grants, bounded delegation, and revocation cascade");
    authority_test_step.dependOn(&run_authority_test.step);

    const commitment_test = b.addExecutable(.{ .name = "commitment_test", .target = target, .optimize = optimize });
    commitment_test.linkLibC();
    commitment_test.linkSystemLibrary("sqlite3");
    commitment_test.addIncludePath(b.path("engine/core/include"));
    commitment_test.addCSourceFiles(.{ .files = &.{ "engine/core/tests/commitment_test.c", "engine/core/src/commitment.c" }, .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" } });
    const run_commitment_test = b.addRunArtifact(commitment_test);
    const commitment_test_step = b.step("test-commitment", "Validate native CommitmentGraph state, delegation, and append-only history");
    commitment_test_step.dependOn(&run_commitment_test.step);

    const origin_test = b.addExecutable(.{ .name = "occurrence_origin_test", .target = target, .optimize = optimize });
    origin_test.linkLibC();
    origin_test.linkSystemLibrary("sqlite3");
    origin_test.addIncludePath(b.path("engine/core/include"));
    origin_test.addCSourceFiles(.{ .files = &.{ "engine/core/tests/occurrence_origin_test.c", "engine/core/src/occurrence_origin.c" }, .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" } });
    const run_origin_test = b.addRunArtifact(origin_test);
    const origin_test_step = b.step("test-occurrence-origin", "Validate native observed, derived, and effect occurrence origins");
    origin_test_step.dependOn(&run_origin_test.step);

    const information_policy_test = b.addExecutable(.{ .name = "information_policy_test", .target = target, .optimize = optimize });
    information_policy_test.linkLibC();
    information_policy_test.addIncludePath(b.path("engine/core/include"));
    information_policy_test.addCSourceFiles(.{ .files = &.{ "engine/core/tests/information_policy_test.c", "engine/core/src/information_policy.c" }, .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" } });
    const run_information_policy_test = b.addRunArtifact(information_policy_test);
    const information_policy_test_step = b.step("test-information-policy", "Validate native default-deny disclosure and aggregation floors");
    information_policy_test_step.dependOn(&run_information_policy_test.step);

    const commerce_test = b.addExecutable(.{ .name = "commerce_test", .target = target, .optimize = optimize });
    commerce_test.linkLibC();
    commerce_test.linkSystemLibrary("sqlite3");
    commerce_test.addIncludePath(b.path("engine/core/include"));
    commerce_test.addCSourceFiles(.{ .files = &.{ "engine/core/tests/commerce_test.c", "engine/core/src/commerce.c" }, .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" } });
    const run_commerce_test = b.addRunArtifact(commerce_test);
    const commerce_test_step = b.step("test-commerce", "Validate native CommerceChain's fifteen non-implying stages");
    commerce_test_step.dependOn(&run_commerce_test.step);

    const protocol_contract_test = b.addExecutable(.{ .name = "protocol_contract_test", .target = target, .optimize = optimize });
    protocol_contract_test.linkLibC();
    protocol_contract_test.linkSystemLibrary("sqlite3");
    protocol_contract_test.addIncludePath(b.path("engine/core/include"));
    protocol_contract_test.addCSourceFiles(.{ .files = &.{ "engine/core/tests/protocol_contract_test.c", "engine/core/src/protocol_contract.c", "engine/core/src/commitment.c", "engine/core/src/authority.c", "engine/core/src/evidence.c", "engine/core/src/information_policy.c" }, .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" } });
    const run_protocol_contract_test = b.addRunArtifact(protocol_contract_test);
    const protocol_contract_test_step = b.step("test-protocol-contract", "Validate native ProtocolContract causality, conformance, and obligation lowering");
    protocol_contract_test_step.dependOn(&run_protocol_contract_test.step);

    const standing_address_test = b.addExecutable(.{ .name = "standing_address_test", .target = target, .optimize = optimize });
    standing_address_test.linkLibC();
    standing_address_test.addIncludePath(b.path("engine/core/include"));
    standing_address_test.addCSourceFiles(.{ .files = &.{ "engine/core/tests/standing_address_test.c", "engine/core/src/standing_address.c", "engine/core/src/information_policy.c" }, .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" } });
    const run_standing_address_test = b.addRunArtifact(standing_address_test);
    const standing_address_test_step = b.step("test-standing-address", "Validate native opportunity ranking and attention/institution separation");
    standing_address_test_step.dependOn(&run_standing_address_test.step);

    const release_lifecycle = b.addSystemCommand(&.{ "sh", "tests/release_lifecycle.sh" });
    const release_lifecycle_step = b.step("test-release", "Run the release lifecycle test");
    release_lifecycle_step.dependOn(&release_lifecycle.step);

    const test_step = b.step("test", "Run the governed fabric smoke test");
    test_step.dependOn(&run_control_conformance.step);
    test_step.dependOn(&run_agent_contract_test.step);
    test_step.dependOn(&run_semantic_operator_test.step);
    test_step.dependOn(&run_probe_test.step);
    test_step.dependOn(&run_compensation_test.step);
    test_step.dependOn(&run_crash_test.step);
    test_step.dependOn(&run_retention_test.step);
    test_step.dependOn(&run_authority_test.step);
    test_step.dependOn(&run_commitment_test.step);
    test_step.dependOn(&run_origin_test.step);
    test_step.dependOn(&run_information_policy_test.step);
    test_step.dependOn(&run_commerce_test.step);
    test_step.dependOn(&run_protocol_contract_test.step);
    test_step.dependOn(&run_standing_address_test.step);
    test_step.dependOn(&smoke.step);
    test_step.dependOn(&frappe.step);
    test_step.dependOn(&release_lifecycle.step);
    test_step.dependOn(&unified.step);
}
