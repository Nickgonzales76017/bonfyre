const std = @import("std");

fn cString(writer: anytype, value: []const u8) !void {
    try writer.writeByte('"');
    for (value) |c| switch (c) {
        '"' => try writer.writeAll("\\\""),
        '\\' => try writer.writeAll("\\\\"),
        '\n' => try writer.writeAll("\\n"),
        '\r' => try writer.writeAll("\\r"),
        '\t' => try writer.writeAll("\\t"),
        else => if (c < 0x20) try writer.print("\\x{x:0>2}", .{c}) else try writer.writeByte(c),
    };
    try writer.writeByte('"');
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);
    if (args.len != 4) return error.Usage;

    const input = try std.fs.cwd().readFileAlloc(allocator, args[1], 1024 * 1024);
    defer allocator.free(input);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, input, .{});
    defer parsed.deinit();

    var out = try std.fs.cwd().createFile(args[2], .{});
    defer out.close();
    const w = out.writer();
    // Multiple generated control models can be embedded into one native owner.
    // A fixed include guard made the first header suppress every later one.
    try w.writeAll("/* Generated from CUE. Do not edit. */\n#pragma once\nstatic const char ");
    try w.writeAll(args[3]);
    try w.writeAll("[] = ");
    try cString(w, input);
    try w.writeAll(";\n");
}
