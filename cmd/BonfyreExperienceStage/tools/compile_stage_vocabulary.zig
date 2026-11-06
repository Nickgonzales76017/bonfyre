const std = @import("std");

const Hint = struct { key: []const u8, terms: []const u8 };
const NameRule = struct { contains: []const u8, hints: []const []const u8 };
const Vocabulary = struct {
    affordances: []Hint,
    roles: []Hint,
    presentation_terms: []const u8,
    app_name_rules: []NameRule,
};

fn cString(writer: anytype, value: []const u8) !void {
    try writer.writeByte('"');
    for (value) |c| switch (c) {
        '"' => try writer.writeAll("\\\""),
        '\\' => try writer.writeAll("\\\\"),
        '\n' => try writer.writeAll("\\n"),
        '\r' => try writer.writeAll("\\r"),
        '\t' => try writer.writeAll("\\t"),
        else => try writer.writeByte(c),
    };
    try writer.writeByte('"');
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);
    if (args.len != 3) return error.Usage;

    const input = try std.fs.cwd().readFileAlloc(allocator, args[1], 1024 * 1024);
    defer allocator.free(input);
    var parsed = try std.json.parseFromSlice(Vocabulary, allocator, input, .{ .ignore_unknown_fields = false });
    defer parsed.deinit();

    var out = try std.fs.cwd().createFile(args[2], .{});
    defer out.close();
    const w = out.writer();
    try w.writeAll(
        "/* Generated from stage-vocabulary.cue. Do not edit. */\n"
        ++ "#ifndef BONFYRE_EXPERIENCE_STAGE_VOCABULARY_H\n"
        ++ "#define BONFYRE_EXPERIENCE_STAGE_VOCABULARY_H\n"
        ++ "#include <stddef.h>\n"
        ++ "typedef struct { const char *key; const char *terms; } BfStageHint;\n"
        ++ "typedef struct { const char *contains; const char *const *hints; size_t hint_count; } BfStageNameRule;\n\n",
    );

    try w.writeAll("static const BfStageHint bf_stage_affordance_hints[] = {\n");
    for (parsed.value.affordances) |row| {
        try w.writeAll("  {"); try cString(w, row.key); try w.writeAll(", "); try cString(w, row.terms); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_STAGE_AFFORDANCE_HINT_COUNT (sizeof(bf_stage_affordance_hints)/sizeof(bf_stage_affordance_hints[0]))\n\n");

    try w.writeAll("static const BfStageHint bf_stage_role_hints[] = {\n");
    for (parsed.value.roles) |row| {
        try w.writeAll("  {"); try cString(w, row.key); try w.writeAll(", "); try cString(w, row.terms); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_STAGE_ROLE_HINT_COUNT (sizeof(bf_stage_role_hints)/sizeof(bf_stage_role_hints[0]))\n\n");

    try w.writeAll("static const char *bf_stage_presentation_terms = ");
    try cString(w, parsed.value.presentation_terms);
    try w.writeAll(";\n\n");

    for (parsed.value.app_name_rules, 0..) |rule, i| {
        try w.print("static const char *const bf_stage_name_rule_{d}_hints[] = {{\n", .{i});
        for (rule.hints) |hint| { try w.writeAll("  "); try cString(w, hint); try w.writeAll(",\n"); }
        try w.writeAll("};\n");
    }
    try w.writeAll("static const BfStageNameRule bf_stage_name_rules[] = {\n");
    for (parsed.value.app_name_rules, 0..) |rule, i| {
        try w.writeAll("  {"); try cString(w, rule.contains);
        try w.print(", bf_stage_name_rule_{d}_hints, sizeof(bf_stage_name_rule_{d}_hints)/sizeof(bf_stage_name_rule_{d}_hints[0])}},\n", .{ i, i, i });
    }
    try w.writeAll("};\n#define BF_STAGE_NAME_RULE_COUNT (sizeof(bf_stage_name_rules)/sizeof(bf_stage_name_rules[0]))\n#endif\n");
}
