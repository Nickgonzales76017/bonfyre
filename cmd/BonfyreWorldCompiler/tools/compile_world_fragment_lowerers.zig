const std = @import("std");

const Lowerer = struct {
    input_grammar: []const u8,
    surface_grammar: []const u8,
    renderer_family: []const u8,
    renderer_variant: []const u8,
    extent: []const u8,
};

fn cString(w: anytype, value: []const u8) !void {
    try w.writeByte('"');
    for (value) |c| switch (c) {
        '"' => try w.writeAll("\\\""),
        '\\' => try w.writeAll("\\\\"),
        '\n' => try w.writeAll("\\n"),
        '\r' => try w.writeAll("\\r"),
        else => try w.writeByte(c),
    };
    try w.writeByte('"');
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);
    if (args.len != 3) return error.Usage;

    const body = try std.fs.cwd().readFileAlloc(allocator, args[1], 1024 * 1024);
    defer allocator.free(body);
    var parsed = try std.json.parseFromSlice([]Lowerer, allocator, body, .{
        .ignore_unknown_fields = false,
    });
    defer parsed.deinit();

    var out = try std.fs.cwd().createFile(args[2], .{});
    defer out.close();
    const w = out.writer();
    try w.writeAll(
        "/* Generated from world-fragment-lowering.cue. Do not edit. */\n" ++
        "#ifndef BONFYRE_WORLD_FRAGMENT_LOWERERS_H\n" ++
        "#define BONFYRE_WORLD_FRAGMENT_LOWERERS_H\n" ++
        "#include <stddef.h>\n" ++
        "typedef struct { const char *input_grammar,*surface_grammar,*renderer_family,*renderer_variant,*extent; } BfWorldFragmentLowerer;\n" ++
        "static const BfWorldFragmentLowerer bf_world_fragment_lowerers[] = {\n",
    );
    for (parsed.value) |row| {
        try w.writeAll("  {");
        try cString(w, row.input_grammar); try w.writeByte(',');
        try cString(w, row.surface_grammar); try w.writeByte(',');
        try cString(w, row.renderer_family); try w.writeByte(',');
        try cString(w, row.renderer_variant); try w.writeByte(',');
        try cString(w, row.extent);
        try w.writeAll("},\n");
    }
    try w.writeAll(
        "};\n" ++
        "#define BF_WORLD_FRAGMENT_LOWERER_COUNT (sizeof(bf_world_fragment_lowerers)/sizeof(bf_world_fragment_lowerers[0]))\n" ++
        "#endif\n",
    );
}
