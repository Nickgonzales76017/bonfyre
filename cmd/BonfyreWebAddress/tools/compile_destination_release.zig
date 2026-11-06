const std = @import("std");

// The destination release lifecycle and change consequences, compiled from CUE.
//
// Generated for the reason the rest of the estate generates its contracts: a
// state machine written in prose is one every caller re-invents slightly
// differently, and the difference that matters here is between a destination
// that is ready and one that has actually delivered.

const State = struct {
    state: []const u8,
    label: []const u8,
    delivered: bool,
    open: bool,
    means: []const u8,
    cannot_infer: []const u8,
};

const Consequence = struct {
    consequence: []const u8,
    label: []const u8,
    invalidates: bool,
    needs_review: bool,
    means: []const u8,
    cannot_infer: []const u8,
};

const Carries = struct {
    destination: []const u8,
    fields: [][]const u8,
    means: []const u8,
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
    if (args.len != 5) return error.Usage;

    const sr = try std.fs.cwd().readFileAlloc(a, args[1], 1024 * 1024);
    defer a.free(sr);
    const cr = try std.fs.cwd().readFileAlloc(a, args[2], 1024 * 1024);
    defer a.free(cr);
    const dr = try std.fs.cwd().readFileAlloc(a, args[3], 1024 * 1024);
    defer a.free(dr);

    // CUE is the closed authoring boundary; an unknown field is a second schema.
    var states = try std.json.parseFromSlice([]State, a, sr, .{ .ignore_unknown_fields = false });
    defer states.deinit();
    var cons = try std.json.parseFromSlice([]Consequence, a, cr, .{ .ignore_unknown_fields = false });
    defer cons.deinit();
    var carries = try std.json.parseFromSlice([]Carries, a, dr, .{ .ignore_unknown_fields = false });
    defer carries.deinit();

    var out = try std.fs.cwd().createFile(args[4], .{});
    defer out.close();
    const w = out.writer();
    try w.writeAll(
        \\/* Generated from schemas/control/destination-release.cue. Do not edit. */
        \\#ifndef BONFYRE_NATIVE_DESTINATION_RELEASE_H
        \\#define BONFYRE_NATIVE_DESTINATION_RELEASE_H
        \\#include <stddef.h>
        \\#include <string.h>
        \\
        \\/* `delivered` is reached only by a receipt naming what was materialized.
        \\ * Readiness, admission and intent all leave it false, which is the whole
        \\ * reason this table is generated rather than remembered. */
        \\typedef struct { const char *state; const char *label; unsigned char delivered, open;
        \\                 const char *means; const char *cannot_infer; } BfDestinationState;
        \\
        \\/* How a source change bears on a destination that already delivered.
        \\ * `invalidates` is what makes one remote change recompile only what it
        \\ * touched instead of everything on a timer. */
        \\typedef struct { const char *consequence; const char *label;
        \\                 unsigned char invalidates, needs_review;
        \\                 const char *means; const char *cannot_infer; } BfChangeConsequence;
        \\
        \\/* Which fields a destination carries, which is what decides whether a
        \\ * change invalidates a delivery. A delivered search fragment holds no
        \\ * amount, so a price that moved does not stale it. */
        \\typedef struct { const char *destination; const char *fields; const char *means; } BfDestinationCarries;
        \\
        \\static const BfDestinationState bf_destination_states[] = {
        \\
    );
    for (states.value) |x| {
        try w.writeAll("  {");
        try cString(w, x.state);
        try w.writeAll(", ");
        try cString(w, x.label);
        try w.print(", {d}, {d}, ", .{ @intFromBool(x.delivered), @intFromBool(x.open) });
        try cString(w, x.means);
        try w.writeAll(", ");
        try cString(w, x.cannot_infer);
        try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_DESTINATION_STATE_COUNT (sizeof(bf_destination_states) / sizeof(bf_destination_states[0]))\n\nstatic const BfChangeConsequence bf_change_consequences[] = {\n");
    for (cons.value) |x| {
        try w.writeAll("  {");
        try cString(w, x.consequence);
        try w.writeAll(", ");
        try cString(w, x.label);
        try w.print(", {d}, {d}, ", .{ @intFromBool(x.invalidates), @intFromBool(x.needs_review) });
        try cString(w, x.means);
        try w.writeAll(", ");
        try cString(w, x.cannot_infer);
        try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_CHANGE_CONSEQUENCE_COUNT (sizeof(bf_change_consequences) / sizeof(bf_change_consequences[0]))\n\nstatic const BfDestinationCarries bf_destination_carries[] = {\n");
    for (carries.value) |x| {
        try w.writeAll("  {");
        try cString(w, x.destination);
        try w.writeAll(", \"|");
        for (x.fields) |f| {
            for (f) |c| try w.writeByte(c);
            try w.writeByte('|');
        }
        try w.writeAll("\", ");
        try cString(w, x.means);
        try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_DESTINATION_CARRIES_COUNT (sizeof(bf_destination_carries) / sizeof(bf_destination_carries[0]))\n");
    try w.writeAll(
        \\
        \\/* The only way to ask whether a state is a delivery. A caller that
        \\ * compares strings itself is a caller that will eventually compare the
        \\ * wrong one. */
        \\static const BfDestinationState *bf_destination_state(const char *name) {
        \\    size_t i;
        \\    if (!name) return NULL;
        \\    for (i = 0; i < BF_DESTINATION_STATE_COUNT; i++) {
        \\        const char *s = bf_destination_states[i].state;
        \\        size_t j = 0;
        \\        while (s[j] && s[j] == name[j]) j++;
        \\        if (!s[j] && !name[j]) return &bf_destination_states[i];
        \\    }
        \\    return NULL;
        \\}
        \\
        \\static int bf_destination_delivered(const char *name) {
        \\    const BfDestinationState *s = bf_destination_state(name);
        \\    return s ? (int)s->delivered : 0;
        \\}
        \\
        \\/* Does this destination carry this field? A change to a field it does not
        \\ * carry cannot invalidate what it delivered. */
        \\static int bf_destination_carries_field(const char *destination, const char *field) {
        \\    size_t i;
        \\    if (!destination || !field || !*field) return 0;
        \\    for (i = 0; i < BF_DESTINATION_CARRIES_COUNT; i++) {
        \\        const char *d = bf_destination_carries[i].destination;
        \\        size_t j = 0;
        \\        while (d[j] && d[j] == destination[j]) j++;
        \\        if (d[j] || destination[j]) continue;
        \\        {
        \\            const char *list = bf_destination_carries[i].fields;
        \\            const char *p = list;
        \\            while ((p = strchr(p, '|')) != NULL) {
        \\                const char *start = ++p;
        \\                const char *end = strchr(start, '|');
        \\                size_t len;
        \\                if (!end) break;
        \\                len = (size_t)(end - start);
        \\                if (len && !strncmp(start, field, len) && !field[len]) return 1;
        \\                p = end;
        \\            }
        \\        }
        \\        return 0;
        \\    }
        \\    return 0;
        \\}
        \\
        \\static const BfChangeConsequence *bf_change_consequence(const char *name) {
        \\    size_t i;
        \\    if (!name) return NULL;
        \\    for (i = 0; i < BF_CHANGE_CONSEQUENCE_COUNT; i++) {
        \\        const char *s = bf_change_consequences[i].consequence;
        \\        size_t j = 0;
        \\        while (s[j] && s[j] == name[j]) j++;
        \\        if (!s[j] && !name[j]) return &bf_change_consequences[i];
        \\    }
        \\    return NULL;
        \\}
        \\#endif
        \\
    );
}
