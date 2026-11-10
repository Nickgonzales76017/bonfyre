const std = @import("std");

// The four previously unowned cells, compiled from CUE.
//
// The one thing this compiler exists to enforce is that no message state claims
// transmission. That property is the whole difference between an outbox this
// estate can honestly keep and one that has quietly become a mail server, and a
// boolean nobody checks is a boolean that eventually flips.

const Cell = struct {
    cell: []const u8,
    label: []const u8,
    record: []const u8,
    does: []const u8,
    refuses: []const u8,
    cannot_infer: []const u8,
};

const Change = struct {
    change: []const u8,
    label: []const u8,
    means: []const u8,
};

const MessageState = struct {
    state: []const u8,
    label: []const u8,
    transmitted: bool,
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

    const cr = try std.fs.cwd().readFileAlloc(a, args[1], 1024 * 1024);
    defer a.free(cr);
    const pr = try std.fs.cwd().readFileAlloc(a, args[2], 1024 * 1024);
    defer a.free(pr);
    const mr = try std.fs.cwd().readFileAlloc(a, args[3], 1024 * 1024);
    defer a.free(mr);

    // CUE is the closed authoring boundary; an unknown field is a second schema.
    var cells = try std.json.parseFromSlice([]Cell, a, cr, .{ .ignore_unknown_fields = false });
    defer cells.deinit();
    var changes = try std.json.parseFromSlice([]Change, a, pr, .{ .ignore_unknown_fields = false });
    defer changes.deinit();
    var states = try std.json.parseFromSlice([]MessageState, a, mr, .{ .ignore_unknown_fields = false });
    defer states.deinit();

    // The refusal that matters: an outbox with no transport must have no state
    // that claims one.
    for (states.value) |st| {
        if (st.transmitted) {
            std.debug.print("message state {s} claims transmission; this estate has no transport\n", .{st.state});
            return error.ClaimsTransmission;
        }
    }
    for (cells.value) |c| {
        if (c.refuses.len == 0) {
            std.debug.print("cell {s} says what it does and not what it refuses\n", .{c.cell});
            return error.NoStatedRefusal;
        }
    }

    var out = try std.fs.cwd().createFile(args[4], .{});
    defer out.close();
    const w = out.writer();
    try w.writeAll(
        \\/* Generated from schemas/control/missing-cells.cue. Do not edit. */
        \\#ifndef BONFYRE_NATIVE_MISSING_CELLS_H
        \\#define BONFYRE_NATIVE_MISSING_CELLS_H
        \\#include <stddef.h>
        \\#include <string.h>
        \\
        \\/* `refuses` is as load-bearing as `does`: each of these is the smallest
        \\ * honest version of a cell, not a clone of the product that named it. */
        \\typedef struct { const char *cell, *label, *record, *does, *refuses, *cannot_infer; } BfCellContract;
        \\
        \\typedef struct { const char *change, *label, *means; } BfPathChange;
        \\
        \\/* `transmitted` is false for every state and the compiler refuses a table
        \\ * where it is not. An outbox with no transport cannot have a state that
        \\ * claims one. */
        \\typedef struct { const char *state, *label; unsigned char transmitted; const char *means; } BfMessageState;
        \\
        \\static const BfCellContract bf_cell_contracts[] = {
        \\
    );
    for (cells.value) |c| {
        try w.writeAll("  {");
        try cString(w, c.cell);
        try w.writeAll(", ");
        try cString(w, c.label);
        try w.writeAll(", ");
        try cString(w, c.record);
        try w.writeAll(", ");
        try cString(w, c.does);
        try w.writeAll(", ");
        try cString(w, c.refuses);
        try w.writeAll(", ");
        try cString(w, c.cannot_infer);
        try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_CELL_CONTRACT_COUNT (sizeof(bf_cell_contracts) / sizeof(bf_cell_contracts[0]))\n\nstatic const BfPathChange bf_path_changes[] = {\n");
    for (changes.value) |c| {
        try w.writeAll("  {");
        try cString(w, c.change);
        try w.writeAll(", ");
        try cString(w, c.label);
        try w.writeAll(", ");
        try cString(w, c.means);
        try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_PATH_CHANGE_COUNT (sizeof(bf_path_changes) / sizeof(bf_path_changes[0]))\n\nstatic const BfMessageState bf_message_states[] = {\n");
    for (states.value) |st| {
        try w.writeAll("  {");
        try cString(w, st.state);
        try w.writeAll(", ");
        try cString(w, st.label);
        try w.print(", {d}, ", .{@intFromBool(st.transmitted)});
        try cString(w, st.means);
        try w.writeAll("},\n");
    }
    try w.writeAll(
        \\};
        \\#define BF_MESSAGE_STATE_COUNT (sizeof(bf_message_states) / sizeof(bf_message_states[0]))
        \\
        \\static const BfCellContract *bf_cell_contract(const char *name) {
        \\    size_t i;
        \\    if (!name) return NULL;
        \\    for (i = 0; i < BF_CELL_CONTRACT_COUNT; i++)
        \\        if (!strcmp(bf_cell_contracts[i].cell, name)) return &bf_cell_contracts[i];
        \\    return NULL;
        \\}
        \\
        \\/* A message state this estate declares, or nothing. A caller inventing a
        \\ * state is a caller inventing a transport. */
        \\static const BfMessageState *bf_message_state(const char *name) {
        \\    size_t i;
        \\    if (!name) return NULL;
        \\    for (i = 0; i < BF_MESSAGE_STATE_COUNT; i++)
        \\        if (!strcmp(bf_message_states[i].state, name)) return &bf_message_states[i];
        \\    return NULL;
        \\}
        \\
        \\/* Did anything leave this estate? Always no, read from the table rather
        \\ * than assumed, so the day somebody adds a transport this stops agreeing
        \\ * with them by accident. */
        \\static int bf_message_transmitted(const char *state) {
        \\    const BfMessageState *s = bf_message_state(state);
        \\    return s ? (int)s->transmitted : 0;
        \\}
        \\#endif
        \\
    );
}
