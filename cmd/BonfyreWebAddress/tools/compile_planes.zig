const std = @import("std");

// How an institution can be reached, compiled from CUE.
//
// `reach` is the whole point: a plane that answers only to a relationship must
// never be rendered beside a public one as though a reader could simply have
// it. Generating the table is what stops that being a matter of remembering.

const Plane = struct {
    plane: []const u8, label: []const u8, reach: []const u8,
    carries: []const u8, cannot_infer: []const u8,
};

fn cString(w: anytype, v: []const u8) !void {
    try w.writeByte('"');
    for (v) |c| switch (c) {
        '"' => try w.writeAll("\\\""),
        '\\' => try w.writeAll("\\\\"),
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
    const raw = try std.fs.cwd().readFileAlloc(a, args[1], 1024 * 1024);
    defer a.free(raw);
    var parsed = try std.json.parseFromSlice([]Plane, a, raw, .{ .ignore_unknown_fields = false });
    defer parsed.deinit();

    var out = try std.fs.cwd().createFile(args[2], .{});
    defer out.close();
    const w = out.writer();
    try w.writeAll(
        \\/* Generated from schemas/control/institution-planes.cue. Do not edit. */
        \\#ifndef BONFYRE_NATIVE_PLANES_H
        \\#define BONFYRE_NATIVE_PLANES_H
        \\#include <stddef.h>
        \\
        \\/* `granted` means the plane answers only to a relationship the
        \\ * institution itself established. It is never reachable by wanting it,
        \\ * and must not be shown beside a public plane as though it were. */
        \\typedef struct { const char *plane; const char *label; const char *reach;
        \\                 const char *carries; const char *cannot_infer; } BfInstitutionPlane;
        \\
        \\static const BfInstitutionPlane bf_institution_planes[] = {
        \\
    );
    for (parsed.value) |p| {
        try w.writeAll("  {");
        try cString(w, p.plane); try w.writeAll(", ");
        try cString(w, p.label); try w.writeAll(", ");
        try cString(w, p.reach); try w.writeAll(", ");
        try cString(w, p.carries); try w.writeAll(", ");
        try cString(w, p.cannot_infer); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_INSTITUTION_PLANE_COUNT (sizeof(bf_institution_planes) / sizeof(bf_institution_planes[0]))\n#endif\n");
}
