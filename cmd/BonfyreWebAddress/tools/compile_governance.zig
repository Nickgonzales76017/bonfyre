const std = @import("std");

// The governance grammar, compiled from CUE into the header the composer reads.
//
// The point of generating it rather than writing it in C is the same point the
// worlds table makes: a declaration that nothing reads gates nothing. A verb
// whose refusal lives only in a comment is a refusal the page never prints.

const Verb = struct { name: []const u8, label: []const u8, cannot_infer: []const u8 };
const Backing = struct { backing: []const u8, settles: bool, cannot_infer: []const u8 };

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
    if (args.len != 4) return error.Usage;

    const verbs_raw = try std.fs.cwd().readFileAlloc(allocator, args[1], 1024 * 1024);
    defer allocator.free(verbs_raw);
    const backings_raw = try std.fs.cwd().readFileAlloc(allocator, args[2], 1024 * 1024);
    defer allocator.free(backings_raw);

    // CUE is the closed authoring boundary: an unknown field is a second,
    // weaker schema appearing in the generator, so it is an error here.
    var verbs = try std.json.parseFromSlice([]Verb, allocator, verbs_raw, .{ .ignore_unknown_fields = false });
    defer verbs.deinit();
    var backings = try std.json.parseFromSlice([]Backing, allocator, backings_raw, .{ .ignore_unknown_fields = false });
    defer backings.deinit();

    var out = try std.fs.cwd().createFile(args[3], .{});
    defer out.close();
    const w = out.writer();
    try w.writeAll(
        \\/* Generated from schemas/control/governance.cue. Do not edit. */
        \\#ifndef BONFYRE_NATIVE_GOVERNANCE_H
        \\#define BONFYRE_NATIVE_GOVERNANCE_H
        \\#include <stddef.h>
        \\
        \\/* A verb carries its own refusal so no surface can render the word
        \\ * without the sentence that bounds it. */
        \\typedef struct { const char *name; const char *label; const char *cannot_infer; } BfGovernanceVerb;
        \\
        \\/* `settles` is the only thing that may offer settlement, and the
        \\ * institution's declared world still gates it. */
        \\typedef struct { const char *backing; int settles; const char *cannot_infer; } BfGovernanceBacking;
        \\
        \\static const BfGovernanceVerb bf_governance_verbs[] = {
        \\
    );
    for (verbs.value) |v| {
        try w.writeAll("  {");
        try cString(w, v.name);
        try w.writeAll(", ");
        try cString(w, v.label);
        try w.writeAll(", ");
        try cString(w, v.cannot_infer);
        try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_GOVERNANCE_VERB_COUNT (sizeof(bf_governance_verbs) / sizeof(bf_governance_verbs[0]))\n\nstatic const BfGovernanceBacking bf_governance_backings[] = {\n");
    for (backings.value) |b| {
        try w.writeAll("  {");
        try cString(w, b.backing);
        try w.print(", {d}, ", .{@intFromBool(b.settles)});
        try cString(w, b.cannot_infer);
        try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_GOVERNANCE_BACKING_COUNT (sizeof(bf_governance_backings) / sizeof(bf_governance_backings[0]))\n#endif\n");
}
