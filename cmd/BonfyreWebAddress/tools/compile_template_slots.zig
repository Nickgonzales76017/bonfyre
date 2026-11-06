const std = @import("std");

const Slot = struct { template: []const u8, slot: u16, op: []const u8 };

fn cString(writer: anytype, value: []const u8) !void {
    try writer.writeByte('"');
    for (value) |c| switch (c) {
        '"' => try writer.writeAll("\\\""),
        '\\' => try writer.writeAll("\\\\"),
        else => try writer.writeByte(c),
    };
    try writer.writeByte('"');
}

fn opcode(name: []const u8) ![]const u8 {
    if (std.mem.eql(u8, name, "resolved_identity")) return "BF_TEMPLATE_RESOLVED_IDENTITY";
    if (std.mem.eql(u8, name, "authored_navigation")) return "BF_TEMPLATE_AUTHORED_NAVIGATION";
    if (std.mem.eql(u8, name, "matrix_context")) return "BF_TEMPLATE_MATRIX_CONTEXT";
    return error.UnknownTemplateOpcode;
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
    // CUE is the closed authoring boundary.  Quietly dropping a field here
    // creates a second, weaker schema in the generator.
    var parsed = try std.json.parseFromSlice([]Slot, allocator, input, .{ .ignore_unknown_fields = false });
    defer parsed.deinit();
    var out = try std.fs.cwd().createFile(args[2], .{});
    defer out.close();
    const w = out.writer();
    try w.writeAll("/* Generated from schemas/control/native-surface-programs.cue. Do not edit. */\n#ifndef BONFYRE_NATIVE_TEMPLATE_SLOTS_H\n#define BONFYRE_NATIVE_TEMPLATE_SLOTS_H\n\ntypedef enum { BF_TEMPLATE_RESOLVED_IDENTITY, BF_TEMPLATE_AUTHORED_NAVIGATION, BF_TEMPLATE_MATRIX_CONTEXT } BfTemplateOpcode;\ntypedef struct { const char *template_name; unsigned short slot; BfTemplateOpcode opcode; } BfNativeTemplateSlot;\nstatic const BfNativeTemplateSlot bf_native_template_slots[] = {\n");
    for (parsed.value) |slot| {
        try w.writeAll("  {"); try cString(w, slot.template); try w.print(", {d}, {s}}},\n", .{slot.slot, try opcode(slot.op)});
    }
    try w.writeAll("};\n#define BF_NATIVE_TEMPLATE_SLOT_COUNT (sizeof(bf_native_template_slots) / sizeof(bf_native_template_slots[0]))\n#endif\n");
}
