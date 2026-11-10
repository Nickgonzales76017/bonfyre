const std = @import("std");

const Field = struct { name: []const u8, audiences: []const []const u8 };
const CarrierField = struct { semantic: []const u8, carrier: []const u8 };
const Projection = struct { carrier: []const u8, standard: []const u8, fields: []CarrierField };
const Profile = struct {
    name: []const u8,
    evidence_requirements: []const []const u8,
    fields: []Field,
    projections: []Projection,
};

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

fn cIdent(writer: anytype, value: []const u8) !void {
    for (value, 0..) |c, i| {
        const alpha = (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or c == '_';
        const digit = c >= '0' and c <= '9';
        if (i == 0 and digit) try writer.writeByte('_');
        try writer.writeByte(if (alpha or digit) c else '_');
    }
}

fn audienceMask(audiences: []const []const u8) !u32 {
    var mask: u32 = 0;
    for (audiences) |audience| {
        const bit: u5 = if (std.mem.eql(u8, audience, "self")) 0
            else if (std.mem.eql(u8, audience, "household")) 1
            else if (std.mem.eql(u8, audience, "group")) 2
            else if (std.mem.eql(u8, audience, "community")) 3
            else if (std.mem.eql(u8, audience, "public")) 4
            else if (std.mem.eql(u8, audience, "market")) 5
            else if (std.mem.eql(u8, audience, "institution")) 6
            else return error.UnknownAudience;
        mask |= @as(u32, 1) << bit;
    }
    return mask;
}

fn containsField(fields: []Field, name: []const u8) bool {
    for (fields) |field| if (std.mem.eql(u8, field.name, name)) return true;
    return false;
}

fn symbol(writer: anytype, profile: []const u8, suffix: []const u8) !void {
    try writer.writeAll("bf_binding_domain_");
    try cIdent(writer, profile);
    try writer.writeByte('_');
    try cIdent(writer, suffix);
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
    var parsed = try std.json.parseFromSlice([]Profile, allocator, input, .{ .ignore_unknown_fields = false });
    defer parsed.deinit();
    var out = try std.fs.cwd().createFile(args[2], .{});
    defer out.close();
    const w = out.writer();

    try w.writeAll("/* Generated from BonfyreExperienceBinding/domain-profile.cue. Do not edit. */\n#ifndef BONFYRE_EXPERIENCE_BINDING_DOMAIN_PROFILES_H\n#define BONFYRE_EXPERIENCE_BINDING_DOMAIN_PROFILES_H\n#include <stddef.h>\n#include <stdint.h>\ntypedef struct { const char *name; uint32_t audience_mask; } BfBindingDomainField;\ntypedef struct { const char *semantic; const char *carrier; } BfBindingCarrierField;\ntypedef struct { const char *carrier; const char *standard; const BfBindingCarrierField *fields; size_t field_count; } BfBindingProjection;\ntypedef struct { const char *name; const char *const *evidence_requirements; size_t evidence_requirement_count; const BfBindingDomainField *fields; size_t field_count; const BfBindingProjection *projections; size_t projection_count; } BfBindingDomainProfile;\n\n");
    for (parsed.value) |profile| {
        try w.writeAll("static const char *const "); try symbol(w, profile.name, "evidence"); try w.writeAll("[] = {\n");
        for (profile.evidence_requirements) |requirement| { try w.writeAll("  "); try cString(w, requirement); try w.writeAll(",\n"); }
        try w.writeAll("};\nstatic const BfBindingDomainField "); try symbol(w, profile.name, "fields"); try w.writeAll("[] = {\n");
        for (profile.fields) |field| { try w.writeAll("  {"); try cString(w, field.name); try w.print(", 0x{x}}},\n", .{try audienceMask(field.audiences)}); }
        try w.writeAll("};\n");
        for (profile.projections) |projection| {
            for (projection.fields) |field| if (!containsField(profile.fields, field.semantic)) return error.UndeclaredSemanticField;
            try w.writeAll("static const BfBindingCarrierField "); try symbol(w, profile.name, projection.carrier); try w.writeAll("[] = {\n");
            for (projection.fields) |field| { try w.writeAll("  {"); try cString(w, field.semantic); try w.writeAll(", "); try cString(w, field.carrier); try w.writeAll("},\n"); }
            try w.writeAll("};\n");
        }
        try w.writeAll("static const BfBindingProjection "); try symbol(w, profile.name, "projections"); try w.writeAll("[] = {\n");
        for (profile.projections) |projection| { try w.writeAll("  {"); try cString(w, projection.carrier); try w.writeAll(", "); try cString(w, projection.standard); try w.writeAll(", "); try symbol(w, profile.name, projection.carrier); try w.writeAll(", sizeof("); try symbol(w, profile.name, projection.carrier); try w.writeAll(") / sizeof("); try symbol(w, profile.name, projection.carrier); try w.writeAll("[0])},\n"); }
        try w.writeAll("};\n\n");
    }
    try w.writeAll("static const BfBindingDomainProfile bf_binding_domain_profiles[] = {\n");
    for (parsed.value) |profile| { try w.writeAll("  {"); try cString(w, profile.name); try w.writeAll(", "); try symbol(w, profile.name, "evidence"); try w.writeAll(", sizeof("); try symbol(w, profile.name, "evidence"); try w.writeAll(") / sizeof("); try symbol(w, profile.name, "evidence"); try w.writeAll("[0]), "); try symbol(w, profile.name, "fields"); try w.writeAll(", sizeof("); try symbol(w, profile.name, "fields"); try w.writeAll(") / sizeof("); try symbol(w, profile.name, "fields"); try w.writeAll("[0]), "); try symbol(w, profile.name, "projections"); try w.writeAll(", sizeof("); try symbol(w, profile.name, "projections"); try w.writeAll(") / sizeof("); try symbol(w, profile.name, "projections"); try w.writeAll("[0])},\n"); }
    try w.writeAll("};\n#define BF_BINDING_DOMAIN_PROFILE_COUNT (sizeof(bf_binding_domain_profiles) / sizeof(bf_binding_domain_profiles[0]))\n#endif\n");
}
