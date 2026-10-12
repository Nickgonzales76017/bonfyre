const std = @import("std");

const strict_c_flags = &.{ "-std=c11", "-D_DEFAULT_SOURCE", "-Wall", "-Wextra", "-Werror" };

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
    fabric.addCSourceFiles(.{ .files = &.{ "engine/core/src/fabric.c", "engine/core/src/fabric_exec.c", "engine/core/src/workgraph.c", "engine/core/src/workgraph_schema.c", "engine/core/src/workgraph_events.c", "engine/core/src/workgraph_scheduler.c", "engine/core/src/workgraph_effects.c", "engine/core/src/filesystem_projection.c", "engine/core/src/process_operator.c", "engine/core/src/operator_contract.c", "lib/libbonfyre/src/bf_sha256.c", "lib/libbonfyre/src/bf_catalog_generation.c" }, .flags = strict_c_flags });
    b.installArtifact(fabric);

    // Replays the control-plane vectors generated from the frozen Python
    // reference. Parity here is what lets the runtime drop that dependency.
    const control_conformance = b.addExecutable(.{ .name = "control_conformance", .target = target, .optimize = optimize });
    control_conformance.linkLibC();
    control_conformance.addIncludePath(b.path("engine/core/include"));
    control_conformance.addCSourceFiles(.{ .files = &.{ "tests/conformance/control/run_native.c", "engine/core/src/control_provider.c", "engine/core/src/control_admission.c", "engine/core/src/control_attention.c", "engine/core/src/control_capability.c" }, .flags = strict_c_flags });
    const run_control_conformance = b.addRunArtifact(control_conformance);
    run_control_conformance.addArg("tests/conformance/control/vectors/control.vec");

    const control_conformance_step = b.step("test-control", "Replay control-plane vectors against the native kernel");
    control_conformance_step.dependOn(&run_control_conformance.step);

    const agent_contract_test = b.addExecutable(.{ .name = "agent_contract_test", .target = target, .optimize = optimize });
    agent_contract_test.linkLibC();
    agent_contract_test.addIncludePath(b.path("engine/core/include"));
    agent_contract_test.addCSourceFiles(.{ .files = &.{ "engine/core/tests/agent_contract_test.c", "engine/core/src/agent_contract.c" }, .flags = strict_c_flags });
    const run_agent_contract_test = b.addRunArtifact(agent_contract_test);

    const agent_contract_test_step = b.step("test-agent-contract", "Validate AgentSession and ReceiptEnvelope provider fidelity");
    agent_contract_test_step.dependOn(&run_agent_contract_test.step);

    const probe_test = b.addExecutable(.{ .name = "probe_contract_test", .target = target, .optimize = optimize });
    probe_test.linkLibC();
    probe_test.linkSystemLibrary("sqlite3");
    probe_test.addIncludePath(b.path("engine/core/include"));
    probe_test.addIncludePath(b.path("lib/libbonfyre/include"));
    probe_test.addCSourceFiles(.{ .files = &.{ "engine/core/tests/probe_contract_test.c", "engine/core/src/fabric.c", "engine/core/src/workgraph.c", "engine/core/src/workgraph_schema.c", "engine/core/src/workgraph_events.c", "engine/core/src/workgraph_scheduler.c", "engine/core/src/workgraph_effects.c", "engine/core/src/filesystem_projection.c", "engine/core/src/process_operator.c", "engine/core/src/operator_contract.c", "lib/libbonfyre/src/bf_sha256.c", "lib/libbonfyre/src/bf_catalog_generation.c" }, .flags = strict_c_flags });
    const run_probe_test = b.addRunArtifact(probe_test);

    inline for ([_][]const u8{ "bonfyre", "bonfyred" }) |name| {
        const exe = b.addExecutable(.{ .name = name, .target = target, .optimize = optimize });
        exe.linkLibC();
        exe.linkSystemLibrary("sqlite3");
        exe.addIncludePath(b.path("engine/core/include"));
        exe.linkLibrary(fabric);
        exe.addCSourceFile(.{ .file = b.path(if (std.mem.eql(u8, name, "bonfyre")) "programs/bonfyre/main.c" else "programs/bonfyred/main.c"), .flags = &.{ "-std=c11", "-D_DEFAULT_SOURCE" } });
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
    compensation_test.addCSourceFiles(.{ .files = &.{ "engine/core/tests/compensation_evidence_test.c", "engine/core/src/workgraph.c", "engine/core/src/workgraph_schema.c", "engine/core/src/workgraph_events.c", "engine/core/src/workgraph_scheduler.c", "engine/core/src/workgraph_effects.c", "lib/libbonfyre/src/bf_sha256.c" }, .flags = strict_c_flags });
    const run_compensation_test = b.addRunArtifact(compensation_test);

    const compensation_test_step = b.step("test-compensation", "Run the compensation attempt evidence test");
    compensation_test_step.dependOn(&run_compensation_test.step);

    const crash_test = b.addExecutable(.{ .name = "effect_commit_crash_test", .target = target, .optimize = optimize });
    crash_test.linkLibC();
    crash_test.linkSystemLibrary("sqlite3");
    crash_test.addIncludePath(b.path("engine/core/include"));
    crash_test.addIncludePath(b.path("lib/libbonfyre/include"));
    crash_test.addCSourceFiles(.{ .files = &.{ "engine/core/tests/effect_commit_crash_test.c", "engine/core/src/workgraph.c", "engine/core/src/workgraph_schema.c", "engine/core/src/workgraph_events.c", "engine/core/src/workgraph_scheduler.c", "engine/core/src/workgraph_effects.c", "lib/libbonfyre/src/bf_sha256.c" }, .flags = strict_c_flags });
    const run_crash_test = b.addRunArtifact(crash_test);

    const crash_test_step = b.step("test-crash", "Run the effect commit crash-consistency test");
    crash_test_step.dependOn(&run_crash_test.step);

    const release_lifecycle = b.addSystemCommand(&.{ "sh", "tests/release_lifecycle.sh" });
    const release_lifecycle_step = b.step("test-release", "Run the release lifecycle test");
    release_lifecycle_step.dependOn(&release_lifecycle.step);

    const test_step = b.step("test", "Run the governed fabric smoke test");
    test_step.dependOn(&run_probe_test.step);
    test_step.dependOn(&run_compensation_test.step);
    test_step.dependOn(&run_crash_test.step);
    test_step.dependOn(&smoke.step);
    test_step.dependOn(&frappe.step);
    test_step.dependOn(&release_lifecycle.step);
    test_step.dependOn(&unified.step);
}
