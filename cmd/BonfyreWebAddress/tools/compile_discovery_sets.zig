const std = @import("std");

// Criterion kinds and candidate verdicts, compiled from CUE.
//
// The evaluator reads this table to decide what SQL a criterion becomes. That is
// the whole reason it is generated: a criterion whose column is written in C is
// one that can quietly read a field nobody declared, and a verdict a machine may
// set is a decision nobody made.

const Kind = struct {
    kind: []const u8,
    label: []const u8,
    over: []const u8,
    column: []const u8,
    how: []const u8,
    means: []const u8,
    cannot_infer: []const u8,
};

const Verdict = struct {
    verdict: []const u8,
    label: []const u8,
    machine_may_set: bool,
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
    if (args.len != 4) return error.Usage;

    const kr = try std.fs.cwd().readFileAlloc(a, args[1], 1024 * 1024);
    defer a.free(kr);
    const vr = try std.fs.cwd().readFileAlloc(a, args[2], 1024 * 1024);
    defer a.free(vr);

    // CUE is the closed authoring boundary; an unknown field is a second schema.
    var kinds = try std.json.parseFromSlice([]Kind, a, kr, .{ .ignore_unknown_fields = false });
    defer kinds.deinit();
    var verdicts = try std.json.parseFromSlice([]Verdict, a, vr, .{ .ignore_unknown_fields = false });
    defer verdicts.deinit();

    // A criterion over `root` may not read a channel column, and the reverse.
    // Getting this wrong would make the evaluator build SQL against a table the
    // column does not belong to.
    for (kinds.value) |k| {
        const root_ok = std.mem.eql(u8, k.column, "host") or std.mem.eql(u8, k.column, "name") or
            std.mem.eql(u8, k.column, "channel_count") or std.mem.eql(u8, k.column, "plane");
        const chan_ok = std.mem.eql(u8, k.column, "exact_address") or std.mem.eql(u8, k.column, "protocol") or
            std.mem.eql(u8, k.column, "state") or std.mem.eql(u8, k.column, "truth_level");
        if (std.mem.eql(u8, k.over, "root") and !root_ok) {
            std.debug.print("criterion {s} reads {s} over root, which root does not have\n", .{ k.kind, k.column });
            return error.ColumnNotOnTable;
        }
        if (std.mem.eql(u8, k.over, "channel") and !chan_ok) {
            std.debug.print("criterion {s} reads {s} over channel, which channel does not have\n", .{ k.kind, k.column });
            return error.ColumnNotOnTable;
        }
    }
    var human: usize = 0;
    for (verdicts.value) |v| {
        if (!v.machine_may_set) human += 1;
    }
    if (human == 0) {
        std.debug.print("every verdict may be set by a machine, so nothing stops at a person\n", .{});
        return error.NoHumanLine;
    }

    var out = try std.fs.cwd().createFile(args[3], .{});
    defer out.close();
    const w = out.writer();
    try w.writeAll(
        \\/* Generated from schemas/control/discovery-set.cue. Do not edit. */
        \\#ifndef BONFYRE_NATIVE_DISCOVERY_SETS_H
        \\#define BONFYRE_NATIVE_DISCOVERY_SETS_H
        \\#include <stddef.h>
        \\#include <string.h>
        \\
        \\/* A criterion is bound to the table and column it reads, so the evaluator
        \\ * cannot build SQL against a field nobody declared. */
        \\typedef struct { const char *kind, *label, *over, *column, *how, *means, *cannot_infer; } BfCriterionKind;
        \\
        \\/* `machine_may_set` is the human line: the criteria propose, and moving a
        \\ * candidate to accepted or rejected is somebody's act. */
        \\typedef struct { const char *verdict, *label; unsigned char machine_may_set; const char *means; } BfCandidateVerdict;
        \\
        \\static const BfCriterionKind bf_criterion_kinds[] = {
        \\
    );
    for (kinds.value) |k| {
        try w.writeAll("  {");
        try cString(w, k.kind);
        try w.writeAll(", ");
        try cString(w, k.label);
        try w.writeAll(", ");
        try cString(w, k.over);
        try w.writeAll(", ");
        try cString(w, k.column);
        try w.writeAll(", ");
        try cString(w, k.how);
        try w.writeAll(", ");
        try cString(w, k.means);
        try w.writeAll(", ");
        try cString(w, k.cannot_infer);
        try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_CRITERION_KIND_COUNT (sizeof(bf_criterion_kinds) / sizeof(bf_criterion_kinds[0]))\n\nstatic const BfCandidateVerdict bf_candidate_verdicts[] = {\n");
    for (verdicts.value) |v| {
        try w.writeAll("  {");
        try cString(w, v.verdict);
        try w.writeAll(", ");
        try cString(w, v.label);
        try w.print(", {d}, ", .{@intFromBool(v.machine_may_set)});
        try cString(w, v.means);
        try w.writeAll("},\n");
    }
    try w.writeAll(
        \\};
        \\#define BF_CANDIDATE_VERDICT_COUNT (sizeof(bf_candidate_verdicts) / sizeof(bf_candidate_verdicts[0]))
        \\
        \\static const BfCriterionKind *bf_criterion_kind(const char *name) {
        \\    size_t i;
        \\    if (!name) return NULL;
        \\    for (i = 0; i < BF_CRITERION_KIND_COUNT; i++)
        \\        if (!strcmp(bf_criterion_kinds[i].kind, name)) return &bf_criterion_kinds[i];
        \\    return NULL;
        \\}
        \\
        \\/* May a machine put a candidate in this verdict? A run that could write
        \\ * `accepted` would be the estate accepting on somebody's behalf. */
        \\static int bf_verdict_machine_may_set(const char *name) {
        \\    size_t i;
        \\    if (!name) return 0;
        \\    for (i = 0; i < BF_CANDIDATE_VERDICT_COUNT; i++)
        \\        if (!strcmp(bf_candidate_verdicts[i].verdict, name))
        \\            return (int)bf_candidate_verdicts[i].machine_may_set;
        \\    return 0;
        \\}
        \\#endif
        \\
    );
}
