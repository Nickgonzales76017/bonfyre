const std = @import("std");

// This compiler carries the 15 × 4 presentation algebra into the native
// lowerer.  It deliberately emits no identity, authority, audience or effect
// data: those remain resolved by their real owners at request time.
const Cell = struct {
    grammar: []const u8,
    presentation_class: []const u8,
    question: []const u8,
    experience_character: []const u8,
    visible_sections: []const []const u8,
    preferred_renderers: []const []const u8,
    renderer_family: []const u8,
    renderer_variant: []const u8,
    route: []const u8,
    extent_defaults: []const u8,
    interaction_density: []const u8,
    allowed_affordance_families: []const []const u8,
    external_demand_roles: []const []const u8,
    empty_character: []const u8,
    refusal_character: []const u8,
    navigation_neighbors: []const []const u8,
    requires: []const []const u8,
};

fn cString(writer: anytype, value: []const u8) !void {
    try writer.writeByte('"');
    for (value) |c| switch (c) {
        '"' => try writer.writeAll("\\\""),
        '\\' => try writer.writeAll("\\\\"),
        '\n' => try writer.writeAll("\\n"),
        '\r' => try writer.writeAll("\\r"),
        else => try writer.writeByte(c),
    };
    try writer.writeByte('"');
}

fn valid(value: []const u8) bool {
    if (value.len == 0) return false;
    for (value) |c| if (!((c >= 'a' and c <= 'z') or (c >= '0' and c <= '9') or c == '_' or c == '-')) return false;
    return true;
}

fn emitStringSet(w: anytype, symbol: []const u8, values: []const []const u8) !void {
    try w.print("static const char *const {s}[] = {{", .{symbol});
    for (values, 0..) |value, index| {
        if (index != 0) try w.writeByte(',');
        try cString(w, value);
    }
    try w.writeAll("};\n");
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);
    if (args.len != 3) return error.Usage;
    const body = try std.fs.cwd().readFileAlloc(allocator, args[1], 1024 * 1024);
    defer allocator.free(body);
    var parsed = try std.json.parseFromSlice([]Cell, allocator, body, .{ .ignore_unknown_fields = false });
    defer parsed.deinit();
    if (parsed.value.len != 60) return error.IncompletePresentationMatrix;
    for (parsed.value, 0..) |cell, i| {
        if (!valid(cell.grammar) or !valid(cell.presentation_class) or !valid(cell.renderer_family) or !valid(cell.renderer_variant) or cell.route.len < 2 or cell.route[0] != '/' or !valid(cell.extent_defaults) or !valid(cell.interaction_density)) return error.InvalidPresentationCell;
        for (parsed.value[0..i]) |prior|
            if (std.mem.eql(u8, prior.grammar, cell.grammar) and std.mem.eql(u8, prior.presentation_class, cell.presentation_class)) return error.DuplicatePresentationCell;
    }
    var out = try std.fs.cwd().createFile(args[2], .{});
    defer out.close();
    const w = out.writer();
    try w.writeAll("/* Generated from surface-presentation-cells.cue. Do not edit. */\n#ifndef BONFYRE_NATIVE_PRESENTATION_CELLS_H\n#define BONFYRE_NATIVE_PRESENTATION_CELLS_H\n#include <stddef.h>\ntypedef struct {\n  const char *grammar; const char *presentation_class; const char *question; const char *experience_character;\n  const char *const *visible_sections; const char *const *preferred_renderers; const char *const *affordances;\n  const char *const *demand_roles; const char *const *neighbors; const char *const *requires;\n  size_t visible_section_count,preferred_renderer_count,affordance_count,demand_role_count,neighbor_count,requirement_count;\n  const char *renderer_family; const char *renderer_variant; const char *route; const char *extent; const char *density; const char *empty_character; const char *refusal_character;\n} BfNativePresentationCell;\n");
    for (parsed.value) |cell| {
        var symbol: [128]u8 = undefined;
        const base = try std.fmt.bufPrint(&symbol, "bf_presentation_{s}_{s}", .{ cell.grammar, cell.presentation_class });
        var visible_symbol: [160]u8 = undefined;
        var preferred_symbol: [160]u8 = undefined;
        var affordance_symbol: [160]u8 = undefined;
        var role_symbol: [160]u8 = undefined;
        var neighbor_symbol: [160]u8 = undefined;
        var requirement_symbol: [160]u8 = undefined;
        const visible = try std.fmt.bufPrint(&visible_symbol, "{s}_visible", .{base}); try emitStringSet(w, visible, cell.visible_sections);
        const preferred = try std.fmt.bufPrint(&preferred_symbol, "{s}_preferred", .{base}); try emitStringSet(w, preferred, cell.preferred_renderers);
        const affordances = try std.fmt.bufPrint(&affordance_symbol, "{s}_affordances", .{base}); try emitStringSet(w, affordances, cell.allowed_affordance_families);
        const roles = try std.fmt.bufPrint(&role_symbol, "{s}_roles", .{base}); try emitStringSet(w, roles, cell.external_demand_roles);
        const neighbors = try std.fmt.bufPrint(&neighbor_symbol, "{s}_neighbors", .{base}); try emitStringSet(w, neighbors, cell.navigation_neighbors);
        const requires = try std.fmt.bufPrint(&requirement_symbol, "{s}_requires", .{base}); try emitStringSet(w, requires, cell.requires);
    }
    try w.writeAll("static const BfNativePresentationCell bf_native_presentation_cells[] = {\n");
    for (parsed.value) |cell| {
        var symbol: [128]u8 = undefined;
        const base = try std.fmt.bufPrint(&symbol, "bf_presentation_{s}_{s}", .{ cell.grammar, cell.presentation_class });
        var visible_symbol: [160]u8 = undefined;
        var preferred_symbol: [160]u8 = undefined;
        var affordance_symbol: [160]u8 = undefined;
        var role_symbol: [160]u8 = undefined;
        var neighbor_symbol: [160]u8 = undefined;
        var requirement_symbol: [160]u8 = undefined;
        const visible = try std.fmt.bufPrint(&visible_symbol, "{s}_visible", .{base});
        const preferred = try std.fmt.bufPrint(&preferred_symbol, "{s}_preferred", .{base});
        const affordances = try std.fmt.bufPrint(&affordance_symbol, "{s}_affordances", .{base});
        const roles = try std.fmt.bufPrint(&role_symbol, "{s}_roles", .{base});
        const neighbors = try std.fmt.bufPrint(&neighbor_symbol, "{s}_neighbors", .{base});
        const requires = try std.fmt.bufPrint(&requirement_symbol, "{s}_requires", .{base});
        try w.writeAll("  {");
        try cString(w, cell.grammar); try w.writeByte(','); try cString(w, cell.presentation_class); try w.writeByte(',');
        try cString(w, cell.question); try w.writeByte(','); try cString(w, cell.experience_character); try w.writeByte(',');
        try w.print("{s},{s},{s},{s},{s},{s},{d},{d},{d},{d},{d},{d},", .{ visible, preferred, affordances, roles, neighbors, requires, cell.visible_sections.len, cell.preferred_renderers.len, cell.allowed_affordance_families.len, cell.external_demand_roles.len, cell.navigation_neighbors.len, cell.requires.len });
        try cString(w, cell.renderer_family); try w.writeByte(','); try cString(w, cell.renderer_variant); try w.writeByte(','); try cString(w, cell.route); try w.writeByte(','); try cString(w, cell.extent_defaults); try w.writeByte(','); try cString(w, cell.interaction_density); try w.writeByte(','); try cString(w, cell.empty_character); try w.writeByte(','); try cString(w, cell.refusal_character); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_NATIVE_PRESENTATION_CELL_COUNT (sizeof(bf_native_presentation_cells)/sizeof(bf_native_presentation_cells[0]))\n#endif\n");
}
