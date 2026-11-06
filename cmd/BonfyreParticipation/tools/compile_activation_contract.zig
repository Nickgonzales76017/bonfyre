const std = @import("std");

const Mode = struct {
    name: []const u8,
    bonus: i32,
    require_role_class: ?[]const u8 = null,
    forbid_role_class: ?[]const u8 = null,
    require_candidate_plane: ?[]const u8 = null,
    join_power_min: ?i32 = null,
    or_source_kind: ?[]const u8 = null,
    or_protocol: ?[]const u8 = null,
    or_role_class: ?[]const u8 = null,
};
const NextRule = struct {
    name: []const u8,
    candidate_plane: []const u8,
    test_axis: []const u8,
    require_unknown: ?bool = null,
    depth_lt: ?i32 = null,
    owner: []const u8,
    action: []const u8,
    why: []const u8,
};
const Next = struct { owner: []const u8, action: []const u8, why: []const u8 };
const Score = struct {
    investigation_join_power_weight: i32,
    investigation_chain_span_cap: i32,
    investigation_candidate_plane_weight: i32,
    investigation_unknown_axis_weight: i32,
    investigation_braid_depth_weight: i32,
    investigation_branch_weight: i32,
    investigation_rejoin_weight: i32,
    investigation_braid_excess_weight: i32,
    activation_join_power_weight: i32,
    activation_chain_span_cap: i32,
    activation_braid_depth_weight: i32,
};
const Contract = struct {
    modes: []Mode,
    default_mode: []const u8,
    next_rules: []NextRule,
    default_next: Next,
    score: Score,
};

fn cstr(w: anytype, s: ?[]const u8) !void {
    try w.writeByte('"');
    if (s) |value| for (value) |c| switch (c) {
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
        "/* Generated from activation-contract.cue. Do not edit. */\n"
        ++ "#ifndef BONFYRE_ACTIVATION_CONTRACT_H\n#define BONFYRE_ACTIVATION_CONTRACT_H\n"
        ++ "#include <stddef.h>\n"
        ++ "typedef struct { const char *name; int bonus; const char *require_role_class; const char *forbid_role_class; "
        ++ "const char *require_candidate_plane; int join_power_min; const char *or_source_kind; const char *or_protocol; "
        ++ "const char *or_role_class; } BfActivationMode;\n"
        ++ "typedef struct { const char *name; const char *candidate_plane; const char *test_axis; int require_unknown; "
        ++ "int depth_lt; const char *owner; const char *action; const char *why; } BfActivationNextRule;\n"
        ++ "typedef struct { int investigation_join_power_weight; int investigation_chain_span_cap; "
        ++ "int investigation_candidate_plane_weight; int investigation_unknown_axis_weight; int investigation_braid_depth_weight; "
        ++ "int investigation_branch_weight; int investigation_rejoin_weight; int investigation_braid_excess_weight; "
        ++ "int activation_join_power_weight; int activation_chain_span_cap; int activation_braid_depth_weight; } BfActivationScore;\n\n"
    );
    try w.writeAll("static const BfActivationMode bf_activation_modes[] = {\n");
    for (parsed.value.modes) |m| {
        try w.writeAll("  {"); try cstr(w, m.name); try w.print(", {d}, ", .{m.bonus});
        try cstr(w, m.require_role_class); try w.writeAll(", "); try cstr(w, m.forbid_role_class); try w.writeAll(", ");
        try cstr(w, m.require_candidate_plane); try w.print(", {d}, ", .{m.join_power_min orelse 0});
        try cstr(w, m.or_source_kind); try w.writeAll(", "); try cstr(w, m.or_protocol); try w.writeAll(", ");
        try cstr(w, m.or_role_class); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_ACTIVATION_MODE_COUNT (sizeof(bf_activation_modes)/sizeof(bf_activation_modes[0]))\n");
    try w.writeAll("static const char *const bf_activation_default_mode = "); try cstr(w, parsed.value.default_mode); try w.writeAll(";\n\n");
    try w.writeAll("static const BfActivationNextRule bf_activation_next_rules[] = {\n");
    for (parsed.value.next_rules) |r| {
        try w.writeAll("  {"); try cstr(w, r.name); try w.writeAll(", "); try cstr(w, r.candidate_plane); try w.writeAll(", ");
        try cstr(w, r.test_axis); try w.print(", {d}, {d}, ", .{@intFromBool(r.require_unknown orelse false), r.depth_lt orelse 0});
        try cstr(w, r.owner); try w.writeAll(", "); try cstr(w, r.action); try w.writeAll(", "); try cstr(w, r.why); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_ACTIVATION_NEXT_RULE_COUNT (sizeof(bf_activation_next_rules)/sizeof(bf_activation_next_rules[0]))\n");
    try w.writeAll("static const char *const bf_activation_default_owner = "); try cstr(w, parsed.value.default_next.owner); try w.writeAll(";\n");
    try w.writeAll("static const char *const bf_activation_default_action = "); try cstr(w, parsed.value.default_next.action); try w.writeAll(";\n");
    try w.writeAll("static const char *const bf_activation_default_why = "); try cstr(w, parsed.value.default_next.why); try w.writeAll(";\n");
    const s = parsed.value.score;
    try w.print("static const BfActivationScore bf_activation_score = {{{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d}}};\n",
        .{s.investigation_join_power_weight, s.investigation_chain_span_cap,
          s.investigation_candidate_plane_weight, s.investigation_unknown_axis_weight,
          s.investigation_braid_depth_weight, s.investigation_branch_weight,
          s.investigation_rejoin_weight, s.investigation_braid_excess_weight,
          s.activation_join_power_weight, s.activation_chain_span_cap, s.activation_braid_depth_weight});
    try w.writeAll("#endif\n");
}
