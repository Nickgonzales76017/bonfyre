const std = @import("std");

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
    var parsed = try std.json.parseFromSlice(std.json.Value, a, input, .{});
    defer parsed.deinit();

    var out = try std.fs.cwd().createFile(args[2], .{});
    defer out.close();
    const w = out.writer();
    try w.writeAll(
        "/* Generated from contract.cue. Do not edit. */\n"
        ++ "#ifndef BONFYRE_DISTURBANCE_CONTRACT_H\n"
        ++ "#define BONFYRE_DISTURBANCE_CONTRACT_H\n"
        ++ "static const char *const bf_disturbance_contract_json = "
    );
    try cstr(w, input);
    try w.writeAll(";\n#endif\n");
}
