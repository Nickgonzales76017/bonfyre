const std = @import("std");

const Op = struct { op: []const u8, text: ?[]const u8 = null, cut: ?[]const u8 = null };
const Slot = struct { slot: u16, ops: []Op };
const Program = struct {
    program: []const u8,
    renderer: []const u8,
    route: []const u8,
    template: []const u8,
    document: []const u8,
    world_route: bool,
    world_navigation: bool,
    slots: []Slot,
};
const RendererPresentation = struct {
    renderer: []const u8,
    territory_nav: []const u8,
    world_nav: []const u8,
    world_link: []const u8,
    subjects: []const u8,
};
const TerritoryPrimary = struct { territory: []const u8, field: []const u8 };
const Input = struct { programs: []Program, renderers: []RendererPresentation, territory_primary: []TerritoryPrimary, renderer_families: []const []const u8, extents: []const []const u8 };

fn opcode(name: []const u8) ![]const u8 {
    const names = .{
        .{ "territory_nav", "BF_SURFACE_TERRITORY_NAV" },
        .{ "world_nav", "BF_SURFACE_WORLD_NAV" },
        .{ "world_link", "BF_SURFACE_WORLD_LINK" },
        .{ "world_fragments", "BF_SURFACE_WORLD_FRAGMENTS" },
        .{ "community_posts", "BF_SURFACE_COMMUNITY_POSTS" },
        .{ "community_title", "BF_SURFACE_COMMUNITY_TITLE" },
        .{ "community_lede", "BF_SURFACE_COMMUNITY_LEDE" },
        .{ "community_standing", "BF_SURFACE_COMMUNITY_STANDING" },
        .{ "community_limit", "BF_SURFACE_COMMUNITY_LIMIT" },
        .{ "offer_status", "BF_SURFACE_OFFER_STATUS" },
        .{ "offer_empty", "BF_SURFACE_OFFER_EMPTY" },
        .{ "night_subjects", "BF_SURFACE_NIGHT_SUBJECTS" },
        .{ "publications", "BF_SURFACE_PUBLICATIONS" },
        .{ "pack_demands", "BF_SURFACE_PACK_DEMANDS" },
        .{ "capability_programs", "BF_SURFACE_CAPABILITY_PROGRAMS" },
        .{ "world_catalogue", "BF_SURFACE_WORLD_CATALOGUE" },
        .{ "subject_apertures", "BF_SURFACE_SUBJECT_APERTURES" },
        .{ "text", "BF_SURFACE_TEXT" },
        .{ "date", "BF_SURFACE_DATE" },
    };
    inline for (names) |entry| if (std.mem.eql(u8, name, entry[0])) return entry[1];
    return error.UnknownOpcode;
}

fn cString(writer: anytype, value: []const u8) !void {
    try writer.writeByte('"');
    for (value) |c| switch (c) {
        '"' => try writer.writeAll("\\\""),
        '\\' => try writer.writeAll("\\\\"),
        '\n' => try writer.writeAll("\\n"),
        '\r' => try writer.writeAll("\\r"),
        '\t' => try writer.writeAll("\\t"),
        else => try writer.writeByte(c),
    };
    try writer.writeByte('"');
}

fn code(value: []const u8, names: []const []const u8) !u8 {
    for (names, 0..) |name, index| if (std.mem.eql(u8, value, name)) return @intCast(index);
    return error.UnknownPresentation;
}

fn rendererFor(renderers: []RendererPresentation, name: []const u8) !RendererPresentation {
    for (renderers) |renderer| if (std.mem.eql(u8, renderer.renderer, name)) return renderer;
    return error.MissingRendererPresentation;
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
    // The compiler input is a closed SurfaceIR.  Unknown fields are an
    // authoring/compiler mismatch, not optional decoration.
    var parsed = try std.json.parseFromSlice(Input, allocator, input, .{ .ignore_unknown_fields = false });
    defer parsed.deinit();
    const required_programs = [_][]const u8{
        "discover", "stage", "channels", "community", "board", "spatial", "listings",
        "inbox", "territory", "observations", "worlds", "packs", "link", "studio", "console",
    };
    if (parsed.value.programs.len != required_programs.len) return error.IncompleteSurfaceAlgebra;
    for (required_programs) |required| {
        var found = false;
        for (parsed.value.programs) |program| {
            if (!std.mem.eql(u8, program.program, required)) continue;
            if (found) return error.DuplicateSurfaceProgram;
            found = true;
        }
        if (!found) return error.MissingSurfaceProgram;
    }
    var out = try std.fs.cwd().createFile(args[2], .{});
    defer out.close();
    const w = out.writer();
    try w.writeAll("/* Generated from schemas/control/native-surface-programs.cue. Do not edit. */\n#ifndef BONFYRE_NATIVE_SURFACE_PROGRAMS_H\n#define BONFYRE_NATIVE_SURFACE_PROGRAMS_H\n\n");
    try w.writeAll("typedef enum { BF_SURFACE_TEXT, BF_SURFACE_DATE, BF_SURFACE_TERRITORY_NAV, BF_SURFACE_WORLD_NAV, BF_SURFACE_WORLD_LINK, BF_SURFACE_WORLD_FRAGMENTS, BF_SURFACE_COMMUNITY_POSTS, BF_SURFACE_COMMUNITY_TITLE, BF_SURFACE_COMMUNITY_LEDE, BF_SURFACE_COMMUNITY_STANDING, BF_SURFACE_COMMUNITY_LIMIT, BF_SURFACE_OFFER_STATUS, BF_SURFACE_OFFER_EMPTY, BF_SURFACE_NIGHT_SUBJECTS, BF_SURFACE_PUBLICATIONS, BF_SURFACE_PACK_DEMANDS, BF_SURFACE_CAPABILITY_PROGRAMS, BF_SURFACE_WORLD_CATALOGUE, BF_SURFACE_SUBJECT_APERTURES } BfSurfaceOpcode;\ntypedef enum { BF_SURFACE_NAV_PLAIN, BF_SURFACE_NAV_STREAM, BF_SURFACE_NAV_COMMUNITY, BF_SURFACE_NAV_BOARD, BF_SURFACE_NAV_MARKET } BfSurfaceNavPresentation;\ntypedef enum { BF_SURFACE_LINK_STREAM, BF_SURFACE_LINK_STAGE, BF_SURFACE_LINK_COMMUNITY, BF_SURFACE_LINK_BOARD, BF_SURFACE_LINK_MARKET } BfSurfaceLinkPresentation;\ntypedef enum { BF_SURFACE_SUBJECT_STREAM, BF_SURFACE_SUBJECT_STAGE } BfSurfaceSubjectPresentation;\ntypedef enum { BF_SURFACE_DOCUMENT_SLOTS, BF_SURFACE_DOCUMENT_NATIVE } BfSurfaceDocumentMode;\ntypedef struct { const char *renderer; BfSurfaceNavPresentation territory_nav,world_nav; BfSurfaceLinkPresentation world_link; BfSurfaceSubjectPresentation subjects; } BfSurfaceRendererPresentation;\ntypedef struct { BfSurfaceOpcode opcode; const char *text,*cut; } BfSurfaceProgramOp;\ntypedef struct { unsigned short first, count; } BfSurfaceProgramSlot;\ntypedef struct { const char *program,*route,*template_name; unsigned char world_route,world_navigation; BfSurfaceDocumentMode document_mode; const BfSurfaceRendererPresentation *presentation; const BfSurfaceProgramOp *ops; const BfSurfaceProgramSlot *slots; unsigned short slot_count; } BfNativeSurfaceProgram;\n\n");
    try w.writeAll("static const BfSurfaceRendererPresentation bf_surface_renderer_presentations[] = {\n");
    for (parsed.value.renderers) |renderer| {
        const territory_nav = try code(renderer.territory_nav, &.{ "plain", "stream" });
        const world_nav = try code(renderer.world_nav, &.{ "plain", "stream", "community", "board", "market" });
        const world_link = try code(renderer.world_link, &.{ "stream", "stage", "community", "board", "market" });
        const subjects = try code(renderer.subjects, &.{ "stream", "stage" });
        try w.writeAll("  {"); try cString(w, renderer.renderer); try w.print(",{d},{d},{d},{d}}},\n", .{ territory_nav, world_nav, world_link, subjects });
    }
    try w.writeAll("};\ntypedef enum { BF_SURFACE_PRIMARY_SUMMARY, BF_SURFACE_PRIMARY_TONIGHT, BF_SURFACE_PRIMARY_OFFER, BF_SURFACE_PRIMARY_OPERATIONS } BfSurfacePrimaryField;\ntypedef struct { const char *territory; BfSurfacePrimaryField field; } BfSurfaceTerritoryPrimary;\nstatic const BfSurfaceTerritoryPrimary bf_surface_territory_primary[] = {\n");
    for (parsed.value.territory_primary) |primary| {
        const field = try code(primary.field, &.{ "summary", "tonight", "offer", "operations" });
        try w.writeAll("  {"); try cString(w, primary.territory); try w.print(",{d}}},\n", .{field});
    }
    try w.writeAll("};\n#define BF_SURFACE_TERRITORY_PRIMARY_COUNT (sizeof(bf_surface_territory_primary)/sizeof(bf_surface_territory_primary[0]))\n\n");
    try w.writeAll("static const char *const bf_surface_renderer_families[] = {");
    for (parsed.value.renderer_families, 0..) |family, index| { if (index != 0) try w.writeByte(','); try cString(w, family); }
    try w.writeAll("};\n#define BF_SURFACE_RENDERER_FAMILY_COUNT (sizeof(bf_surface_renderer_families)/sizeof(bf_surface_renderer_families[0]))\nstatic const char *const bf_surface_extents[] = {");
    for (parsed.value.extents, 0..) |extent, index| { if (index != 0) try w.writeByte(','); try cString(w, extent); }
    try w.writeAll("};\n#define BF_SURFACE_EXTENT_COUNT (sizeof(bf_surface_extents)/sizeof(bf_surface_extents[0]))\n\n");
    for (parsed.value.programs) |program| {
        _ = try rendererFor(parsed.value.renderers, program.renderer);
        try w.print("static const BfSurfaceProgramOp bf_ops_{s}[] = {{\n", .{program.program});
        for (program.slots) |slot| for (slot.ops) |op| {
            try w.writeAll("  {"); try w.writeAll(try opcode(op.op)); try w.writeAll(", ");
            if (op.text) |text| try cString(w, text) else try w.writeAll("NULL");
            try w.writeAll(", "); if (op.cut) |cut| try cString(w, cut) else try w.writeAll("NULL");
            try w.writeAll("},\n");
        };
        try w.writeAll("};\n");
        try w.print("static const BfSurfaceProgramSlot bf_slots_{s}[] = {{\n", .{program.program});
        var first: u16 = 0;
        var expected: u16 = 0;
        for (program.slots) |slot| {
            while (expected < slot.slot) : (expected += 1) try w.writeAll("  {0, 0},\n");
            try w.print("  {{{d}, {d}}},\n", .{ first, slot.ops.len });
            first += @intCast(slot.ops.len); expected += 1;
        }
        try w.writeAll("};\n\n");
    }
    try w.writeAll("static const BfNativeSurfaceProgram bf_native_surface_programs[] = {\n");
    for (parsed.value.programs) |program| {
        var renderer_index: usize = 0;
        const document_mode: u8 = if (std.mem.eql(u8, program.document, "slots")) 0 else if (std.mem.eql(u8, program.document, "native")) 1 else return error.UnknownDocumentMode;
        for (parsed.value.renderers, 0..) |renderer, index| if (std.mem.eql(u8, renderer.renderer, program.renderer)) { renderer_index = index; break; };
        try w.print("  {{\"{s}\",\"{s}\",\"{s}\",{d},{d},{d}, &bf_surface_renderer_presentations[{d}], bf_ops_{s}, bf_slots_{s}, sizeof(bf_slots_{s}) / sizeof(bf_slots_{s}[0])}},\n", .{ program.program, program.route, program.template, @intFromBool(program.world_route), @intFromBool(program.world_navigation), document_mode, renderer_index, program.program, program.program, program.program, program.program });
    }
    try w.writeAll("};\n#define BF_NATIVE_SURFACE_PROGRAM_COUNT (sizeof(bf_native_surface_programs) / sizeof(bf_native_surface_programs[0]))\n#endif\n");
}
