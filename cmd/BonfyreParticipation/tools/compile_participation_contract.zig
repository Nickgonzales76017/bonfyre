const std = @import("std");

const NamedSet = struct { name: []const u8, members: []const []const u8 };
const Rung = struct { axis: []const u8, state: []const u8, depth: i32, depth_name: []const u8 };
const Contract = struct {
    axes: []const []const u8,
    generic_human: []const []const u8,
    generic_protocol: []const []const u8,
    capability_roles: []const []const u8,
    roundtrip_roles: []const []const u8,
    roundtrip_human: []const []const u8,
    roundtrip_protocols: []const []const u8,
    economic_terms: []const []const u8,
    role_classes: []NamedSet,
    join_families: []NamedSet,
    ladders: []Rung,
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

fn emitStrings(w: anytype, name: []const u8, values: []const []const u8) !void {
    try w.print("static const char *const {s}[] = {{\n", .{name});
    for (values) |v| { try w.writeAll("  "); try cstr(w, v); try w.writeAll(",\n"); }
    try w.writeAll("};\n");
    try w.print("#define {s}_COUNT (sizeof({s})/sizeof({s}[0]))\n\n", .{ name, name, name });
}

fn emitSets(w: anytype, prefix: []const u8, table: []const u8, values: []NamedSet) !void {
    for (values, 0..) |set, i| {
        try w.print("static const char *const {s}_{d}_members[] = {{\n", .{ prefix, i });
        for (set.members) |v| { try w.writeAll("  "); try cstr(w, v); try w.writeAll(",\n"); }
        try w.writeAll("};\n");
    }
    try w.print("static const BfPartSet {s}[] = {{\n", .{table});
    for (values, 0..) |set, i| {
        try w.writeAll("  {"); try cstr(w, set.name);
        try w.print(", {s}_{d}_members, sizeof({s}_{d}_members)/sizeof({s}_{d}_members[0])}},\n",
            .{ prefix, i, prefix, i, prefix, i });
    }
    try w.writeAll("};\n");
    try w.print("#define {s}_COUNT (sizeof({s})/sizeof({s}[0]))\n\n", .{ table, table, table });
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
        "/* Generated from participation-contract.cue. Do not edit. */\n"
        ++ "#ifndef BONFYRE_PARTICIPATION_CONTRACT_H\n#define BONFYRE_PARTICIPATION_CONTRACT_H\n"
        ++ "#include <stddef.h>\n"
        ++ "typedef struct { const char *name; const char *const *members; size_t member_count; } BfPartSet;\n"
        ++ "typedef struct { const char *axis; const char *state; int depth; const char *depth_name; } BfPartRung;\n\n"
    );
    try emitStrings(w, "bf_part_axes", parsed.value.axes);
    try emitStrings(w, "bf_part_generic_human", parsed.value.generic_human);
    try emitStrings(w, "bf_part_generic_protocol", parsed.value.generic_protocol);
    try emitStrings(w, "bf_part_capability_roles", parsed.value.capability_roles);
    try emitStrings(w, "bf_part_roundtrip_roles", parsed.value.roundtrip_roles);
    try emitStrings(w, "bf_part_roundtrip_human", parsed.value.roundtrip_human);
    try emitStrings(w, "bf_part_roundtrip_protocols", parsed.value.roundtrip_protocols);
    try emitStrings(w, "bf_part_economic_terms", parsed.value.economic_terms);
    try emitSets(w, "bf_part_role_class", "bf_part_role_classes", parsed.value.role_classes);
    try emitSets(w, "bf_part_join_family", "bf_part_join_families", parsed.value.join_families);
    try w.writeAll("static const BfPartRung bf_part_ladders[] = {\n");
    for (parsed.value.ladders) |r| {
        try w.writeAll("  {"); try cstr(w, r.axis); try w.writeAll(", "); try cstr(w, r.state);
        try w.print(", {d}, ", .{r.depth}); try cstr(w, r.depth_name); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define bf_part_ladders_COUNT (sizeof(bf_part_ladders)/sizeof(bf_part_ladders[0]))\n#endif\n");
}
