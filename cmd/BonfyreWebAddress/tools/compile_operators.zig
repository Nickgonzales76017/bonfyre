const std = @import("std");

// Operator archetypes and project facets, compiled from CUE.
//
// Generated for the reason the rest of the estate generates its contracts: a
// declaration nothing reads gates nothing, and a refusal that lives only in a
// comment is one the page never prints.

const Archetype = struct {
    archetype: []const u8, label: []const u8,
    has: []const u8, unlocks: []const u8, cannot_infer: []const u8,
};
const Facet = struct {
    facet: []const u8, label: []const u8, studied: []const u8, cannot_infer: []const u8,
};
// An affinity binds an archetype to a property the compiler reads off a
// DocType's own fields, so the pairing is declared here and evidenced there.
const Affinity = struct {
    archetype: []const u8, needs: []const u8, because: []const u8, cannot_infer: []const u8,
};
// A field name reaches into a household only as a whole segment, and only for
// a name declared here.
const Reach = struct { name: []const u8, why: []const u8 };

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
    if (args.len != 6) return error.Usage;

    const ar = try std.fs.cwd().readFileAlloc(a, args[1], 1024 * 1024);
    defer a.free(ar);
    const fr = try std.fs.cwd().readFileAlloc(a, args[2], 1024 * 1024);
    defer a.free(fr);
    const afr = try std.fs.cwd().readFileAlloc(a, args[3], 1024 * 1024);
    defer a.free(afr);
    const hr = try std.fs.cwd().readFileAlloc(a, args[4], 1024 * 1024);
    defer a.free(hr);
    // CUE is the closed authoring boundary; an unknown field is a second schema.
    var arch = try std.json.parseFromSlice([]Archetype, a, ar, .{ .ignore_unknown_fields = false });
    defer arch.deinit();
    var fac = try std.json.parseFromSlice([]Facet, a, fr, .{ .ignore_unknown_fields = false });
    defer fac.deinit();
    var aff = try std.json.parseFromSlice([]Affinity, a, afr, .{ .ignore_unknown_fields = false });
    defer aff.deinit();
    var reach = try std.json.parseFromSlice([]Reach, a, hr, .{ .ignore_unknown_fields = false });
    defer reach.deinit();

    var out = try std.fs.cwd().createFile(args[5], .{});
    defer out.close();
    const w = out.writer();
    try w.writeAll(
        \\/* Generated from schemas/control/operators.cue. Do not edit. */
        \\#ifndef BONFYRE_NATIVE_OPERATORS_H
        \\#define BONFYRE_NATIVE_OPERATORS_H
        \\#include <stddef.h>
        \\
        \\/* `has` is what an operator already possesses; `unlocks` is what stops
        \\ * being someone else's dependency. Neither claims any named platform
        \\ * agrees, participates, or has been approached. */
        \\typedef struct { const char *archetype; const char *label; const char *has;
        \\                 const char *unlocks; const char *cannot_infer; } BfOperatorArchetype;
        \\
        \\/* `studied` is provenance in the same sense a page region records a
        \\ * reading rhythm: no donor code runs and no donor vocabulary reaches a page. */
        \\typedef struct { const char *facet; const char *label; const char *studied;
        \\                 const char *cannot_infer; } BfProjectFacet;
        \\
        \\/* `needs` is a property measured off a compiled DocType's own fields, so
        \\ * an affinity is a declared pairing evidenced by the compiler rather than
        \\ * a resemblance read out of prose. It is recognition, never permission. */
        \\typedef struct { const char *archetype; const char *needs; const char *because;
        \\                 const char *cannot_infer; } BfRecordAffinity;
        \\
        \\/* A name matches only as a whole underscore-separated segment of a
        \\ * fieldname: no_of_employees names a company's size, and Frappe's own
        \\ * parent names the document a row belongs to. */
        \\typedef struct { const char *name; const char *why; } BfHouseholdReach;
        \\
        \\static const BfOperatorArchetype bf_operator_archetypes[] = {
        \\
    );
    for (arch.value) |x| {
        try w.writeAll("  {");
        try cString(w, x.archetype); try w.writeAll(", ");
        try cString(w, x.label); try w.writeAll(", ");
        try cString(w, x.has); try w.writeAll(", ");
        try cString(w, x.unlocks); try w.writeAll(", ");
        try cString(w, x.cannot_infer); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_OPERATOR_ARCHETYPE_COUNT (sizeof(bf_operator_archetypes) / sizeof(bf_operator_archetypes[0]))\n\nstatic const BfProjectFacet bf_project_facets[] = {\n");
    for (fac.value) |x| {
        try w.writeAll("  {");
        try cString(w, x.facet); try w.writeAll(", ");
        try cString(w, x.label); try w.writeAll(", ");
        try cString(w, x.studied); try w.writeAll(", ");
        try cString(w, x.cannot_infer); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_PROJECT_FACET_COUNT (sizeof(bf_project_facets) / sizeof(bf_project_facets[0]))\n\nstatic const BfRecordAffinity bf_record_affinities[] = {\n");
    for (aff.value) |x| {
        try w.writeAll("  {");
        try cString(w, x.archetype); try w.writeAll(", ");
        try cString(w, x.needs); try w.writeAll(", ");
        try cString(w, x.because); try w.writeAll(", ");
        try cString(w, x.cannot_infer); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_RECORD_AFFINITY_COUNT (sizeof(bf_record_affinities) / sizeof(bf_record_affinities[0]))\n\nstatic const BfHouseholdReach bf_household_reach[] = {\n");
    for (reach.value) |x| {
        try w.writeAll("  {");
        try cString(w, x.name); try w.writeAll(", ");
        try cString(w, x.why); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_HOUSEHOLD_REACH_COUNT (sizeof(bf_household_reach) / sizeof(bf_household_reach[0]))\n#endif\n");
}
