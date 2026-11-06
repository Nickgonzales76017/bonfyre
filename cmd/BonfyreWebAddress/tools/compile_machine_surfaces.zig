const std = @import("std");

const Surface = struct { name: []const u8, schema: []const u8 };

fn cString(writer: anytype, value: []const u8) !void {
    try writer.writeByte('"');
    for (value) |c| switch (c) {
        '"' => try writer.writeAll("\\\""),
        '\\' => try writer.writeAll("\\\\"),
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
    var parsed = try std.json.parseFromSlice([]Surface, allocator, input, .{ .ignore_unknown_fields = false });
    defer parsed.deinit();
    var out = try std.fs.cwd().createFile(args[2], .{});
    defer out.close();
    const w = out.writer();
    try w.writeAll("/* Generated from schemas/control/native-surface-programs.cue. Do not edit. */\n#ifndef BONFYRE_NATIVE_MACHINE_SURFACES_H\n#define BONFYRE_NATIVE_MACHINE_SURFACES_H\n\ntypedef struct { const char *name; const char *schema; } BfNativeMachineSurface;\nstatic const BfNativeMachineSurface bf_native_machine_surfaces[] = {\n");
    for (parsed.value) |surface| {
        try w.writeAll("  {"); try cString(w, surface.name); try w.writeAll(", "); try cString(w, surface.schema); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_NATIVE_MACHINE_SURFACE_COUNT (sizeof(bf_native_machine_surfaces) / sizeof(bf_native_machine_surfaces[0]))\n#endif\n");
}
