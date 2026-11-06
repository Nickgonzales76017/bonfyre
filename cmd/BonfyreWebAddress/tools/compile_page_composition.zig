const std = @import("std");

const Region = struct {
    role: []const u8,
    label: []const u8,
    material: []const u8,
    topology: []const u8,
    language: []const u8,
    min_items: u32,
    max_items: u32,
};
const HomePhase = struct {
    territory: []const u8,
    from_hour: u32,
    title: []const u8,
    lede: []const u8,
    sections: [][]const u8,
};
const SceneGrammar = struct {
    name: []const u8,
    region: []const u8,
    material: []const u8,
    at_least: u32,
    take: u32,
    weight: u32,
    label: []const u8,
    says: []const u8,
};
const Scheme = struct { scheme: []const u8, material: []const u8 };
const GrammarLabel = struct { grammar: []const u8, label: []const u8 };
const SourceKind = struct { kind: []const u8, material: []const u8 };
const RouteSourceContract = struct {
    route: []const u8,
    territory: []const u8,
    host_suffix: []const u8,
    title_terms: [][]const u8,
};
const Program = struct {
    route: []const u8,
    territory: []const u8,
    title: []const u8,
    lede: []const u8,
    character: []const u8,
    families: [][]const u8,
    source_limit: u32,
    regions: [][]const u8,
    sections: [][]const u8,
    cannot_infer: []const u8,
};
const NavItem = struct { route: []const u8, label: []const u8 };
const PublisherHost = struct { host: []const u8, path: []const u8, publisher: []const u8 };
const PictureOrigin = struct { host: []const u8, path: []const u8, hashed: bool, name: []const u8, page: []const u8, sized: []const u8, sizes: []const u8, everywhere: bool };
const Licence = struct { match: []const u8, words: []const u8, commercial: bool, adapt: bool, credit: bool, share_alike: bool };
const EmbedHost = struct { host: []const u8, path: []const u8, embed: []const u8 };
const WithheldPicture = struct { host: []const u8, path: []const u8, why: []const u8 };
const Composition = struct {
    regions: []Region,
    subject_schemes: []Scheme,
    source_kinds: []SourceKind,
    route_source_contracts: []RouteSourceContract,
    world_regions: [][]const u8,
    publisher_hosts: []PublisherHost,
    picture_origins: []PictureOrigin,
    withheld_pictures: []WithheldPicture,
    licences: []Licence,
    embed_hosts: []EmbedHost,
    programs: []Program,
    home_phases: []HomePhase,
    scene_grammars: []SceneGrammar,
    nav: []NavItem,
    grammar_labels: []GrammarLabel,
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
fn materialCode(value: []const u8) ![]const u8 {
    if (std.mem.eql(u8, value, "publication")) return "BF_PAGE_MATERIAL_PUBLICATION";
    if (std.mem.eql(u8, value, "video")) return "BF_PAGE_MATERIAL_VIDEO";
    if (std.mem.eql(u8, value, "photo")) return "BF_PAGE_MATERIAL_PHOTO";
    if (std.mem.eql(u8, value, "place")) return "BF_PAGE_MATERIAL_PLACE";
    if (std.mem.eql(u8, value, "mix")) return "BF_PAGE_MATERIAL_MIX";
    if (std.mem.eql(u8, value, "audio")) return "BF_PAGE_MATERIAL_AUDIO";
    if (std.mem.eql(u8, value, "post")) return "BF_PAGE_MATERIAL_POST";
    if (std.mem.eql(u8, value, "event")) return "BF_PAGE_MATERIAL_EVENT";
    if (std.mem.eql(u8, value, "passage")) return "BF_PAGE_MATERIAL_PASSAGE";
    if (std.mem.eql(u8, value, "organization")) return "BF_PAGE_MATERIAL_ORGANIZATION";
    if (std.mem.eql(u8, value, "channel")) return "BF_PAGE_MATERIAL_CHANNEL";
    return error.UnknownPageMaterial;
}
fn topologyCode(value: []const u8) ![]const u8 {
    if (std.mem.eql(u8, value, "lead")) return "BF_PAGE_TOPOLOGY_LEAD";
    if (std.mem.eql(u8, value, "shelf")) return "BF_PAGE_TOPOLOGY_SHELF";
    if (std.mem.eql(u8, value, "field")) return "BF_PAGE_TOPOLOGY_FIELD";
    if (std.mem.eql(u8, value, "map")) return "BF_PAGE_TOPOLOGY_MAP";
    if (std.mem.eql(u8, value, "feed")) return "BF_PAGE_TOPOLOGY_FEED";
    if (std.mem.eql(u8, value, "wall")) return "BF_PAGE_TOPOLOGY_WALL";
    if (std.mem.eql(u8, value, "grid")) return "BF_PAGE_TOPOLOGY_GRID";
    if (std.mem.eql(u8, value, "communities")) return "BF_PAGE_TOPOLOGY_COMMUNITIES";
    if (std.mem.eql(u8, value, "table")) return "BF_PAGE_TOPOLOGY_TABLE";
    if (std.mem.eql(u8, value, "results")) return "BF_PAGE_TOPOLOGY_RESULTS";
    if (std.mem.eql(u8, value, "index")) return "BF_PAGE_TOPOLOGY_INDEX";
    if (std.mem.eql(u8, value, "tracks")) return "BF_PAGE_TOPOLOGY_TRACKS";
    if (std.mem.eql(u8, value, "thread")) return "BF_PAGE_TOPOLOGY_THREAD";
    if (std.mem.eql(u8, value, "agenda")) return "BF_PAGE_TOPOLOGY_AGENDA";
    if (std.mem.eql(u8, value, "list")) return "BF_PAGE_TOPOLOGY_LIST";
    return error.UnknownPageTopology;
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
    var parsed = try std.json.parseFromSlice(Composition, allocator, body, .{ .ignore_unknown_fields = false });
    defer parsed.deinit();
    const c = parsed.value;
    var out = try std.fs.cwd().createFile(args[2], .{});
    defer out.close();
    const w = out.writer();
    try w.writeAll("/* Generated from page-composition.cue. Do not edit. */\n#ifndef BONFYRE_NATIVE_PAGE_COMPOSITION_H\n#define BONFYRE_NATIVE_PAGE_COMPOSITION_H\n#include <stddef.h>\n");
    try w.writeAll("typedef enum { BF_PAGE_MATERIAL_PUBLICATION, BF_PAGE_MATERIAL_VIDEO, BF_PAGE_MATERIAL_PHOTO, BF_PAGE_MATERIAL_PLACE, BF_PAGE_MATERIAL_AUDIO, BF_PAGE_MATERIAL_POST, BF_PAGE_MATERIAL_EVENT, BF_PAGE_MATERIAL_MIX, BF_PAGE_MATERIAL_PASSAGE, BF_PAGE_MATERIAL_ORGANIZATION, BF_PAGE_MATERIAL_CHANNEL, BF_PAGE_MATERIAL_COUNT } BfPageMaterial;\n");
    try w.writeAll("typedef enum { BF_PAGE_TOPOLOGY_LEAD, BF_PAGE_TOPOLOGY_SHELF, BF_PAGE_TOPOLOGY_FIELD, BF_PAGE_TOPOLOGY_MAP, BF_PAGE_TOPOLOGY_INDEX, BF_PAGE_TOPOLOGY_TRACKS, BF_PAGE_TOPOLOGY_THREAD, BF_PAGE_TOPOLOGY_LIST, BF_PAGE_TOPOLOGY_FEED, BF_PAGE_TOPOLOGY_WALL, BF_PAGE_TOPOLOGY_GRID, BF_PAGE_TOPOLOGY_COMMUNITIES, BF_PAGE_TOPOLOGY_TABLE, BF_PAGE_TOPOLOGY_RESULTS, BF_PAGE_TOPOLOGY_AGENDA } BfPageTopology;\n");
    try w.writeAll("typedef struct { const char *role; const char *label; BfPageMaterial material; BfPageTopology topology; const char *language; unsigned min_items; unsigned max_items; } BfPageRegion;\n");
    try w.writeAll("typedef struct { const char *scheme; BfPageMaterial material; } BfPageSubjectScheme;\n");
    try w.writeAll("typedef struct { const char *grammar; const char *label; } BfPageGrammarLabel;\n");
    try w.writeAll("typedef struct { const char *kind; BfPageMaterial material; } BfPageSourceKind;\n");
    try w.writeAll("static const BfPageRegion bf_page_regions[] = {\n");
    for (c.regions) |r| {
        if (r.min_items < 1 or r.max_items < r.min_items or r.max_items > 50) return error.InvalidPageRegionBounds;
        try w.writeAll("  {");
        try cString(w, r.role); try w.writeByte(',');
        try cString(w, r.label); try w.writeByte(',');
        try w.writeAll(try materialCode(r.material)); try w.writeByte(',');
        try w.writeAll(try topologyCode(r.topology)); try w.writeByte(',');
        try cString(w, r.language);
        try w.print(",{d},{d}}},\n", .{ r.min_items, r.max_items });
    }
    try w.writeAll("};\n#define BF_PAGE_REGION_COUNT (sizeof(bf_page_regions)/sizeof(bf_page_regions[0]))\n");
    try w.writeAll("static const BfPageSubjectScheme bf_page_subject_schemes[] = {\n");
    for (c.subject_schemes) |s| {
        try w.writeAll("  {"); try cString(w, s.scheme); try w.writeByte(','); try w.writeAll(try materialCode(s.material)); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_PAGE_SUBJECT_SCHEME_COUNT (sizeof(bf_page_subject_schemes)/sizeof(bf_page_subject_schemes[0]))\n");
    try w.writeAll("static const BfPageSourceKind bf_page_source_kinds[] = {\n");
    for (c.source_kinds) |k| {
        try w.writeAll("  {"); try cString(w, k.kind); try w.writeByte(','); try w.writeAll(try materialCode(k.material)); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_PAGE_SOURCE_KIND_COUNT (sizeof(bf_page_source_kinds)/sizeof(bf_page_source_kinds[0]))\n");
    try w.writeAll("typedef struct { const char *route; const char *territory; const char *host_suffix; const char *const *title_terms; size_t title_term_count; } BfPageRouteSourceContract;\n");
    for (c.route_source_contracts, 0..) |contract, index| {
        try w.print("static const char *const bf_page_route_source_contract_{d}_title_terms[] = {{", .{index});
        if (contract.title_terms.len == 0) try w.writeAll("NULL");
        for (contract.title_terms, 0..) |term, k| {
            if (k > 0) try w.writeByte(',');
            try cString(w, term);
        }
        try w.writeAll("};\n");
    }
    try w.writeAll("static const BfPageRouteSourceContract bf_page_route_source_contracts[] = {\n");
    for (c.route_source_contracts, 0..) |contract, index| {
        try w.writeAll("  {"); try cString(w, contract.route); try w.writeByte(',');
        try cString(w, contract.territory); try w.writeByte(','); try cString(w, contract.host_suffix);
        try w.print(",bf_page_route_source_contract_{d}_title_terms,{d}}},\n", .{ index, contract.title_terms.len });
    }
    try w.writeAll("};\n#define BF_PAGE_ROUTE_SOURCE_CONTRACT_COUNT (sizeof(bf_page_route_source_contracts)/sizeof(bf_page_route_source_contracts[0]))\n");
    try w.writeAll("static const BfPageGrammarLabel bf_page_grammar_labels[] = {\n");
    for (c.grammar_labels) |g| {
        try w.writeAll("  {"); try cString(w, g.grammar); try w.writeByte(','); try cString(w, g.label); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_PAGE_GRAMMAR_LABEL_COUNT (sizeof(bf_page_grammar_labels)/sizeof(bf_page_grammar_labels[0]))\n");
    try w.writeAll("typedef struct { const char *host; const char *path; const char *publisher; } BfPagePublisherHost;\nstatic const BfPagePublisherHost bf_page_publisher_hosts[] = {\n");
    for (c.publisher_hosts) |h| { try w.writeAll("  {"); try cString(w, h.host); try w.writeByte(','); try cString(w, h.path); try w.writeByte(','); try cString(w, h.publisher); try w.writeAll("},\n"); }
    try w.writeAll("};\n#define BF_PAGE_PUBLISHER_HOST_COUNT (sizeof(bf_page_publisher_hosts)/sizeof(bf_page_publisher_hosts[0]))\n");
    try w.writeAll("typedef struct { const char *host; const char *path; int hashed; const char *name; const char *page; const char *sized; const char *sizes; int everywhere; } BfPagePictureOrigin;\nstatic const BfPagePictureOrigin bf_page_picture_origins[] = {\n");
    for (c.picture_origins) |o| {
        try w.writeAll("  {"); try cString(w, o.host); try w.writeByte(','); try cString(w, o.path);
        try w.print(",{d},", .{@intFromBool(o.hashed)});
        try cString(w, o.name); try w.writeByte(','); try cString(w, o.page); try w.writeByte(','); try cString(w, o.sized); try w.writeByte(','); try cString(w, o.sizes);
        try w.print(",{d}}},\n", .{@intFromBool(o.everywhere)});
    }
    try w.writeAll("};\n#define BF_PAGE_PICTURE_ORIGIN_COUNT (sizeof(bf_page_picture_origins)/sizeof(bf_page_picture_origins[0]))\n");
    try w.writeAll("typedef struct { const char *host; const char *path; } BfPageWithheldPicture;\nstatic const BfPageWithheldPicture bf_page_withheld_pictures[] = {\n");
    for (c.withheld_pictures) |h| { try w.writeAll("  {"); try cString(w, h.host); try w.writeByte(','); try cString(w, h.path); try w.writeAll("},\n"); }
    try w.writeAll("};\n#define BF_PAGE_WITHHELD_PICTURE_COUNT (sizeof(bf_page_withheld_pictures)/sizeof(bf_page_withheld_pictures[0]))\n");
    try w.writeAll("typedef struct { const char *match; const char *words; int commercial; int adapt; int credit; int share_alike; } BfPageLicence;\nstatic const BfPageLicence bf_page_licences[] = {\n");
    for (c.licences) |l| {
        try w.writeAll("  {"); try cString(w, l.match); try w.writeByte(','); try cString(w, l.words);
        try w.print(",{d},{d},{d},{d}}},\n", .{ @intFromBool(l.commercial), @intFromBool(l.adapt), @intFromBool(l.credit), @intFromBool(l.share_alike) });
    }
    try w.writeAll("};\n#define BF_PAGE_LICENCE_COUNT (sizeof(bf_page_licences)/sizeof(bf_page_licences[0]))\n");
    try w.writeAll("typedef struct { const char *host; const char *path; const char *embed; } BfPageEmbedHost;\nstatic const BfPageEmbedHost bf_page_embed_hosts[] = {\n");
    for (c.embed_hosts) |e| { try w.writeAll("  {"); try cString(w, e.host); try w.writeByte(','); try cString(w, e.path); try w.writeByte(','); try cString(w, e.embed); try w.writeAll("},\n"); }
    try w.writeAll("};\n#define BF_PAGE_EMBED_HOST_COUNT (sizeof(bf_page_embed_hosts)/sizeof(bf_page_embed_hosts[0]))\n");
    try w.writeAll("static const unsigned char bf_page_world_regions[] = {");
    for (c.world_regions, 0..) |role, k| {
        var found: ?usize = null;
        for (c.regions, 0..) |region, r| { if (std.mem.eql(u8, region.role, role)) found = r; }
        if (found == null) return error.UnknownWorldRegion;
        if (k > 0) try w.writeByte(',');
        try w.print("{d}", .{found.?});
    }
    try w.writeAll("};\n#define BF_PAGE_WORLD_REGION_COUNT (sizeof(bf_page_world_regions)/sizeof(bf_page_world_regions[0]))\n");
    try w.writeAll("typedef struct { const char *route; const char *territory; const char *title; const char *lede; const char *character; const char *const *families; size_t family_count; unsigned source_limit; const unsigned char *regions; size_t region_count; const char *const *sections; size_t section_count; const char *cannot_infer; } BfPageProgram;\n");
    for (c.programs, 0..) |prog, index| {
        try w.print("static const char *const bf_page_program_{d}_families[] = {{", .{index});
        for (prog.families, 0..) |family, k| { if (k > 0) try w.writeByte(','); try cString(w, family); }
        try w.print("}};\nstatic const unsigned char bf_page_program_{d}_regions[] = {{", .{index});
        for (prog.regions, 0..) |role, k| {
            var found: ?usize = null;
            for (c.regions, 0..) |region, r| { if (std.mem.eql(u8, region.role, role)) found = r; }
            if (found == null) return error.UnknownProgramRegion;
            if (k > 0) try w.writeByte(',');
            try w.print("{d}", .{found.?});
        }
        try w.writeAll("};\n");
        try w.print("static const char *const bf_page_program_{d}_sections[] = {{", .{index});
        if (prog.sections.len == 0) try w.writeAll("NULL");
        for (prog.sections, 0..) |section, k| { if (k > 0) try w.writeByte(','); try cString(w, section); }
        try w.writeAll("};\n");
    }
    try w.writeAll("static const BfPageProgram bf_page_programs[] = {\n");
    for (c.programs, 0..) |prog, index| {
        if (prog.source_limit < 1 or prog.source_limit > 96) return error.InvalidSourceLimit;
        try w.writeAll("  {"); try cString(w, prog.route); try w.writeByte(','); try cString(w, prog.territory); try w.writeByte(','); try cString(w, prog.title); try w.writeByte(',');
        try cString(w, prog.lede); try w.writeByte(','); try cString(w, prog.character);
        try w.print(",bf_page_program_{d}_families,{d},{d},bf_page_program_{d}_regions,{d},bf_page_program_{d}_sections,{d},", .{ index, prog.families.len, prog.source_limit, index, prog.regions.len, index, prog.sections.len });
        try cString(w, prog.cannot_infer); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_PAGE_PROGRAM_COUNT (sizeof(bf_page_programs)/sizeof(bf_page_programs[0]))\n");
    // A front page's phases: the hour each begins, in this estate's own zone,
    // and the order of the routes below it that the hour makes sense of.
    for (c.home_phases, 0..) |phase, index| {
        try w.print("static const char *const bf_page_home_phase_{d}_sections[] = {{", .{index});
        if (phase.sections.len == 0) try w.writeAll("NULL");
        for (phase.sections, 0..) |section, k| { if (k > 0) try w.writeByte(','); try cString(w, section); }
        try w.writeAll("};\n");
    }
    try w.writeAll("typedef struct { const char *territory; unsigned from_hour; const char *title; const char *lede; const char *const *sections; unsigned section_count; } BfPageHomePhase;\n");
    try w.writeAll("static const BfPageHomePhase bf_page_home_phases[] = {\n");
    for (c.home_phases, 0..) |phase, index| {
        if (phase.from_hour > 23) return error.InvalidHomePhaseHour;
        try w.writeAll("  {"); try cString(w, phase.territory); try w.print(",{d},", .{phase.from_hour});
        try cString(w, phase.title); try w.writeByte(','); try cString(w, phase.lede);
        try w.print(",bf_page_home_phase_{d}_sections,{d}}},\n", .{ index, phase.sections.len });
    }
    try w.writeAll("};\n#define BF_PAGE_HOME_PHASE_COUNT (sizeof(bf_page_home_phases)/sizeof(bf_page_home_phases[0]))\n");
    // The reading rhythms a scene may choose from, each with the shape of
    // material that argues for it.
    try w.writeAll("typedef struct { const char *name; unsigned region; BfPageMaterial material; unsigned at_least; unsigned take; unsigned weight; const char *label; const char *says; } BfPageSceneGrammar;\n");
    try w.writeAll("static const BfPageSceneGrammar bf_page_scene_grammars[] = {\n");
    for (c.scene_grammars) |g| {
        var region: ?usize = null;
        for (c.regions, 0..) |r, i| { if (std.mem.eql(u8, r.role, g.region)) region = i; }
        if (region == null) return error.UnknownSceneRegion;
        if (g.take < 1 or g.at_least < 1) return error.InvalidSceneGrammar;
        try w.writeAll("  {"); try cString(w, g.name);
        try w.print(",{d},", .{region.?});
        try w.writeAll(try materialCode(g.material));
        try w.print(",{d},{d},{d},", .{ g.at_least, g.take, g.weight });
        try cString(w, g.label); try w.writeByte(','); try cString(w, g.says); try w.writeAll("},\n");
    }
    try w.writeAll("};\n#define BF_PAGE_SCENE_GRAMMAR_COUNT (sizeof(bf_page_scene_grammars)/sizeof(bf_page_scene_grammars[0]))\n");
    try w.writeAll("typedef struct { const char *route; const char *label; } BfPageNavItem;\nstatic const BfPageNavItem bf_page_nav[] = {\n");
    for (c.nav) |item| { try w.writeAll("  {"); try cString(w, item.route); try w.writeByte(','); try cString(w, item.label); try w.writeAll("},\n"); }
    try w.writeAll("};\n#define BF_PAGE_NAV_COUNT (sizeof(bf_page_nav)/sizeof(bf_page_nav[0]))\n#endif\n");
}
