const std = @import("std");

const State = struct {
    name: []const u8,
    policy_decision: ?[]const u8 = null,
    require_fetch: bool,
    require_transport_success: bool,
    require_http_success: bool,
    require_body: bool,
    require_content_hash: bool,
};

const Contract = struct {
    schema: []const u8,
    receipt_schema: []const u8,
    states: []State,
    cannot_infer: [][]const u8,
};

fn cstr(w: anytype, s: []const u8) !void {
    try w.writeByte('"');
    for (s) |c| switch (c) {
        '"' => try w.writeAll("\\\""),
        '\\' => try w.writeAll("\\\\"),
        '\n' => try w.writeAll("\\n"),
        '\r' => try w.writeAll("\\r"),
        '\t' => try w.writeAll("\\t"),
        else => try w.writeByte(c),
    };
    try w.writeByte('"');
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
        "/* Generated from boundary-contract.cue. Do not edit. */\n" ++
        "#ifndef BONFYRE_OBSERVATION_BOUNDARY_CONTRACT_H\n#define BONFYRE_OBSERVATION_BOUNDARY_CONTRACT_H\n" ++
        "#include <stddef.h>\n" ++
        "typedef struct { const char *name; const char *policy_decision; int require_fetch; int require_transport_success; int require_http_success; int require_body; int require_content_hash; } BfObservationState;\n"
    );
    try w.writeAll("static const char *const bf_observation_boundary_schema = ");
    try cstr(w, parsed.value.schema);
    try w.writeAll(";\nstatic const char *const bf_observation_receipt_schema = ");
    try cstr(w, parsed.value.receipt_schema);
    try w.writeAll(";\nstatic const BfObservationState bf_observation_states[] = {\n");
    for (parsed.value.states) |s| {
        try w.writeAll("  {");
        try cstr(w, s.name); try w.writeAll(", ");
        if (s.policy_decision) |p| try cstr(w, p) else try w.writeAll("\"\"");
        try w.print(", {d}, {d}, {d}, {d}, {d}}},\n", .{
            @intFromBool(s.require_fetch), @intFromBool(s.require_transport_success),
            @intFromBool(s.require_http_success), @intFromBool(s.require_body),
            @intFromBool(s.require_content_hash),
        });
    }
    try w.writeAll("};\n#define BF_OBSERVATION_STATE_COUNT (sizeof(bf_observation_states)/sizeof(bf_observation_states[0]))\n");
    try w.writeAll("static const char *const bf_observation_cannot_infer[] = {\n");
    for (parsed.value.cannot_infer) |v| { try w.writeAll("  "); try cstr(w, v); try w.writeAll(",\n"); }
    try w.writeAll("};\n#define BF_OBSERVATION_CANNOT_INFER_COUNT (sizeof(bf_observation_cannot_infer)/sizeof(bf_observation_cannot_infer[0]))\n#endif\n");
}
