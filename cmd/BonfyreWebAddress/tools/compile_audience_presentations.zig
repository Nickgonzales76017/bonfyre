const std = @import("std");
const Presentation = struct { scope: []const u8, title: []const u8, description: []const u8 };
fn cString(w: anytype, value: []const u8) !void {
    try w.writeByte('"');
    for (value) |c| switch (c) {
        '"' => try w.writeAll("\\\""), '\\' => try w.writeAll("\\\\"),
        '\n' => try w.writeAll("\\n"), '\r' => try w.writeAll("\\r"),
        else => try w.writeByte(c),
    };
    try w.writeByte('"');
}
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){}; defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const args = try std.process.argsAlloc(allocator); defer std.process.argsFree(allocator, args);
    if (args.len != 3) return error.Usage;
    const body = try std.fs.cwd().readFileAlloc(allocator, args[1], 1024 * 1024); defer allocator.free(body);
    var parsed = try std.json.parseFromSlice([]Presentation, allocator, body, .{ .ignore_unknown_fields = false }); defer parsed.deinit();
    var out = try std.fs.cwd().createFile(args[2], .{}); defer out.close(); const w = out.writer();
    try w.writeAll("/* Generated from audience-presentation.cue. Do not edit. */\n#ifndef BONFYRE_NATIVE_AUDIENCE_PRESENTATIONS_H\n#define BONFYRE_NATIVE_AUDIENCE_PRESENTATIONS_H\ntypedef struct { const char *scope; const char *title; const char *description; } BfNativeAudiencePresentation;\nstatic const BfNativeAudiencePresentation bf_native_audience_presentations[] = {\n");
    for (parsed.value) |row| {
        try w.writeAll("  {"); try cString(w, row.scope); try w.writeByte(','); try cString(w, row.title); try w.writeByte(','); try cString(w, row.description); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_NATIVE_AUDIENCE_PRESENTATION_COUNT (sizeof(bf_native_audience_presentations)/sizeof(bf_native_audience_presentations[0]))\n#endif\n");
}
