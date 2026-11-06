const std = @import("std");

const Binding = struct {
    schema: []const u8,
    axis: []const u8,
    owner: []const u8,
    from_owner: []const u8,
    to_owner: []const u8,
    state: []const u8,
    require_binding_validated: bool,
    minimum_programs: ?i64 = null,
    required_hash_fields: [][]const u8,
};

const Witness = struct {
    schema: []const u8,
    require_registered: bool,
    require_scoped: bool,
    require_targeted_invalidation: bool,
    require_atomic: bool,
    minimum_enactments: i64,
    minimum_programs: i64,
};

const Contract = struct {
    schema: []const u8,
    domain_binding: Binding,
    protocol_binding: Binding,
    derivation_witness: Witness,
};

fn cstr(w: anytype, value: []const u8) !void {
    try w.writeByte('"');
    for (value) |c| switch (c) {
        '"' => try w.writeAll("\\\""),
        '\\' => try w.writeAll("\\\\"),
        '\n' => try w.writeAll("\\n"),
        '\r' => try w.writeAll("\\r"),
        '\t' => try w.writeAll("\\t"),
        else => try w.writeByte(c),
    };
    try w.writeByte('"');
}

fn emitBinding(w: anytype, prefix: []const u8, b: Binding) !void {
    try w.print("static const BfBridgeBinding {s} = {{", .{prefix});
    try cstr(w, b.schema);
    try w.writeAll(", ");
    try cstr(w, b.axis);
    try w.writeAll(", ");
    try cstr(w, b.owner);
    try w.writeAll(", ");
    try cstr(w, b.from_owner);
    try w.writeAll(", ");
    try cstr(w, b.to_owner);
    try w.writeAll(", ");
    try cstr(w, b.state);
    try w.print(", {d}, {d}}};\n", .{
        @intFromBool(b.require_binding_validated),
        b.minimum_programs orelse 0,
    });
    try w.print("static const char *const {s}_hash_fields[] = {{\n", .{prefix});
    for (b.required_hash_fields) |field| {
        try w.writeAll("  ");
        try cstr(w, field);
        try w.writeAll(",\n");
    }
    try w.writeAll("};\n");
    try w.print("#define {s}_HASH_FIELD_COUNT (sizeof({s}_hash_fields)/sizeof({s}_hash_fields[0]))\n", .{
        prefix,
        prefix,
        prefix,
    });
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const a = gpa.allocator();
    const args = try std.process.argsAlloc(a);
    defer std.process.argsFree(a, args);
    if (args.len != 3) return error.Usage;

    const input = try std.fs.cwd().readFileAlloc(a, args[1], 1024 * 1024);
    defer a.free(input);
    var parsed = try std.json.parseFromSlice(Contract, a, input, .{ .ignore_unknown_fields = false });
    defer parsed.deinit();

    var out = try std.fs.cwd().createFile(args[2], .{});
    defer out.close();
    const w = out.writer();

    try w.writeAll(
        "/* Generated from readiness-contract.cue. Do not edit. */\n" ++
        "#ifndef BONFYRE_BRIDGE_READINESS_CONTRACT_H\n" ++
        "#define BONFYRE_BRIDGE_READINESS_CONTRACT_H\n" ++
        "#include <stddef.h>\n" ++
        "typedef struct { const char *schema; const char *axis; const char *owner; const char *from_owner; const char *to_owner; const char *state; int require_validated; long long minimum_programs; } BfBridgeBinding;\n" ++
        "typedef struct { const char *schema; int require_registered; int require_scoped; int require_targeted_invalidation; int require_atomic; long long minimum_enactments; long long minimum_programs; } BfBridgeWitness;\n",
    );
    try w.writeAll("static const char *const bf_bridge_readiness_schema = ");
    try cstr(w, parsed.value.schema);
    try w.writeAll(";\n");

    try emitBinding(w, "bf_bridge_domain_binding", parsed.value.domain_binding);
    try emitBinding(w, "bf_bridge_protocol_binding", parsed.value.protocol_binding);

    const witness = parsed.value.derivation_witness;
    try w.writeAll("static const BfBridgeWitness bf_bridge_derivation_witness = {");
    try cstr(w, witness.schema);
    try w.print(", {d}, {d}, {d}, {d}, {d}, {d}}};\n", .{
        @intFromBool(witness.require_registered),
        @intFromBool(witness.require_scoped),
        @intFromBool(witness.require_targeted_invalidation),
        @intFromBool(witness.require_atomic),
        witness.minimum_enactments,
        witness.minimum_programs,
    });
    try w.writeAll("#endif\n");
}
