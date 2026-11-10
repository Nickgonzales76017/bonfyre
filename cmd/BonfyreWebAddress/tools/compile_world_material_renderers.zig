const std = @import("std");

const Renderer = struct {
    character: []const u8,
    container_class: []const u8,
    item_variant: []const u8,
};

fn cString(w: anytype, value: []const u8) !void {
    try w.writeByte('"');
    for (value) |c| switch (c) {
        '"' => try w.writeAll("\\\""), '\\' => try w.writeAll("\\\\"),
        '\n' => try w.writeAll("\\n"), '\r' => try w.writeAll("\\r"),
        else => try w.writeByte(c),
    };
    try w.writeByte('"');
}
fn variantCode(value: []const u8) ![]const u8 {
    if (std.mem.eql(u8, value, "flyer")) return "BF_WORLD_MATERIAL_FLYER";
    if (std.mem.eql(u8, value, "pin")) return "BF_WORLD_MATERIAL_PIN";
    if (std.mem.eql(u8, value, "case")) return "BF_WORLD_MATERIAL_CASE";
    if (std.mem.eql(u8, value, "tick")) return "BF_WORLD_MATERIAL_TICK";
    if (std.mem.eql(u8, value, "reading")) return "BF_WORLD_MATERIAL_READING";
    if (std.mem.eql(u8, value, "article")) return "BF_WORLD_MATERIAL_ARTICLE";
    if (std.mem.eql(u8, value, "row")) return "BF_WORLD_MATERIAL_ROW";
    return error.UnknownWorldMaterialVariant;
}
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){}; defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const args = try std.process.argsAlloc(allocator); defer std.process.argsFree(allocator, args);
    if (args.len != 3) return error.Usage;
    const body = try std.fs.cwd().readFileAlloc(allocator, args[1], 1024 * 1024); defer allocator.free(body);
    var parsed = try std.json.parseFromSlice([]Renderer, allocator, body, .{ .ignore_unknown_fields = false }); defer parsed.deinit();
    var out = try std.fs.cwd().createFile(args[2], .{}); defer out.close(); const w = out.writer();
    try w.writeAll("/* Generated from world-material-renderers.cue. Do not edit. */\n#ifndef BONFYRE_NATIVE_WORLD_MATERIAL_RENDERERS_H\n#define BONFYRE_NATIVE_WORLD_MATERIAL_RENDERERS_H\ntypedef enum { BF_WORLD_MATERIAL_FLYER, BF_WORLD_MATERIAL_PIN, BF_WORLD_MATERIAL_CASE, BF_WORLD_MATERIAL_TICK, BF_WORLD_MATERIAL_READING, BF_WORLD_MATERIAL_ARTICLE, BF_WORLD_MATERIAL_ROW } BfWorldMaterialItemVariant;\ntypedef struct { const char *character; const char *container_class; BfWorldMaterialItemVariant item_variant; } BfWorldMaterialRenderer;\nstatic const BfWorldMaterialRenderer bf_world_material_renderers[] = {\n");
    for (parsed.value) |row| {
        try w.writeAll("  {"); try cString(w, row.character); try w.writeByte(','); try cString(w, row.container_class); try w.writeByte(','); try w.writeAll(try variantCode(row.item_variant)); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_WORLD_MATERIAL_RENDERER_COUNT (sizeof(bf_world_material_renderers)/sizeof(bf_world_material_renderers[0]))\n#endif\n");
}
