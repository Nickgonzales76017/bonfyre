const std = @import("std");

const Lowerer = struct {
    application_kind: []const u8,
    layout: []const u8,
    presentation_class: []const u8,
    ops: []const []const u8,
    material_owner: []const u8,
    reader: []const u8,
    query_key: []const u8,
    style_asset: ?[]const u8 = null,
    empty_character: []const u8,
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
fn layoutCode(value: []const u8) !u8 {
    const names = [_][]const u8{ "river", "state", "thread", "board", "audio", "video" };
    for (names, 0..) |name, i| if (std.mem.eql(u8, value, name)) return @intCast(i);
    return error.UnknownSiteApplicationLayout;
}
fn opcode(value: []const u8) ![]const u8 {
    if (std.mem.eql(u8, value, "header")) return "BF_SITE_OP_HEADER";
    if (std.mem.eql(u8, value, "lead")) return "BF_SITE_OP_LEAD";
    if (std.mem.eql(u8, value, "stream")) return "BF_SITE_OP_STREAM";
    if (std.mem.eql(u8, value, "thread")) return "BF_SITE_OP_THREAD";
    if (std.mem.eql(u8, value, "board")) return "BF_SITE_OP_BOARD";
    if (std.mem.eql(u8, value, "media")) return "BF_SITE_OP_MEDIA";
    if (std.mem.eql(u8, value, "transcript")) return "BF_SITE_OP_TRANSCRIPT";
    return error.UnknownSiteApplicationOpcode;
}
fn readerCode(value: []const u8) ![]const u8 {
    if (std.mem.eql(u8, value, "publication")) return "BF_SITE_MATERIAL_READER_PUBLICATION";
    if (std.mem.eql(u8, value, "none")) return "BF_SITE_MATERIAL_READER_NONE";
    if (std.mem.eql(u8, value, "community")) return "BF_SITE_MATERIAL_READER_COMMUNITY";
    if (std.mem.eql(u8, value, "world_fragment")) return "BF_SITE_MATERIAL_READER_WORLD_FRAGMENT";
    if (std.mem.eql(u8, value, "recording")) return "BF_SITE_MATERIAL_READER_RECORDING";
    if (std.mem.eql(u8, value, "media_program")) return "BF_SITE_MATERIAL_READER_MEDIA_PROGRAM";
    return error.UnknownSiteMaterialReader;
}
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){}; defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const args = try std.process.argsAlloc(allocator); defer std.process.argsFree(allocator, args);
    if (args.len != 3) return error.Usage;
    const body = try std.fs.cwd().readFileAlloc(allocator, args[1], 1024 * 1024); defer allocator.free(body);
    var parsed = try std.json.parseFromSlice([]Lowerer, allocator, body, .{ .ignore_unknown_fields = false }); defer parsed.deinit();
    var out = try std.fs.cwd().createFile(args[2], .{}); defer out.close(); const w = out.writer();
    try w.writeAll("/* Generated from site-application-lowerers.cue. Do not edit. */\n#ifndef BONFYRE_NATIVE_SITE_APPLICATION_LOWERERS_H\n#define BONFYRE_NATIVE_SITE_APPLICATION_LOWERERS_H\n#include <stddef.h>\ntypedef enum { BF_SITE_LAYOUT_RIVER, BF_SITE_LAYOUT_STATE, BF_SITE_LAYOUT_THREAD, BF_SITE_LAYOUT_BOARD, BF_SITE_LAYOUT_AUDIO, BF_SITE_LAYOUT_VIDEO } BfSiteApplicationLayout;\ntypedef enum { BF_SITE_OP_HEADER, BF_SITE_OP_LEAD, BF_SITE_OP_STREAM, BF_SITE_OP_THREAD, BF_SITE_OP_BOARD, BF_SITE_OP_MEDIA, BF_SITE_OP_TRANSCRIPT } BfSiteApplicationOpcode;\ntypedef enum { BF_SITE_MATERIAL_READER_PUBLICATION, BF_SITE_MATERIAL_READER_NONE, BF_SITE_MATERIAL_READER_COMMUNITY, BF_SITE_MATERIAL_READER_WORLD_FRAGMENT, BF_SITE_MATERIAL_READER_RECORDING, BF_SITE_MATERIAL_READER_MEDIA_PROGRAM } BfSiteMaterialReader;\ntypedef struct { const char *application_kind,*presentation_class,*material_owner,*query_key,*style_asset,*empty_character; BfSiteMaterialReader reader; BfSiteApplicationLayout layout; const BfSiteApplicationOpcode *ops; size_t op_count; } BfSiteApplicationLowerer;\n");
    for (parsed.value) |row| {
        try w.print("static const BfSiteApplicationOpcode bf_site_application_{s}_ops[] = {{", .{row.application_kind});
        for (row.ops, 0..) |op, index| { if (index != 0) try w.writeByte(','); try w.writeAll(try opcode(op)); }
        try w.writeAll("};\n");
    }
    try w.writeAll("static const BfSiteApplicationLowerer bf_site_application_lowerers[] = {\n");
    for (parsed.value) |row| {
        const layout = try layoutCode(row.layout);
        try w.writeAll("  {"); try cString(w, row.application_kind); try w.writeByte(','); try cString(w, row.presentation_class); try w.writeByte(','); try cString(w, row.material_owner); try w.writeByte(','); try cString(w, row.query_key); try w.writeByte(','); if (row.style_asset) |asset| try cString(w, asset) else try w.writeAll("NULL"); try w.writeByte(','); try cString(w, row.empty_character); try w.writeByte(','); try w.writeAll(try readerCode(row.reader)); try w.print(",{d},bf_site_application_{s}_ops,{d}}},\n", .{layout, row.application_kind, row.ops.len});
    }
    try w.writeAll("};\n#define BF_SITE_APPLICATION_LOWERER_COUNT (sizeof(bf_site_application_lowerers)/sizeof(bf_site_application_lowerers[0]))\n#endif\n");
}
