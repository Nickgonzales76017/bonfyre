const std = @import("std");

const Field = struct { name: []const u8, audiences: []const []const u8, withhold_reason: ?[]const u8 = null };
const CarrierField = struct { semantic: []const u8, carrier: []const u8 };
const Projection = struct { carrier: []const u8, standard: []const u8, container: ?[]const u8 = null, fields: []CarrierField };
const Profile = struct { name: []const u8, evidence_requirements: []const []const u8, fields: []Field, projections: []Projection };

fn cString(writer: anytype, value: []const u8) !void {
    try writer.writeByte('"');
    for (value) |c| switch (c) {
        '"' => try writer.writeAll("\\\""),
        '\\' => try writer.writeAll("\\\\"),
        '?' => try writer.writeAll("\\?"), // Preserve bytes even with C trigraphs enabled.
        '\n' => try writer.writeAll("\\n"),
        '\r' => try writer.writeAll("\\r"),
        '\t' => try writer.writeAll("\\t"),
        else => if (c < 32 or c >= 127) {
            // Exactly three octal digits cannot absorb a following hex digit.
            try writer.print("\\{o:0>3}", .{c});
        } else {
            try writer.writeByte(c);
        },
    };
    try writer.writeByte('"');
}

fn audienceMask(audiences: []const []const u8) !u32 {
    var mask: u32 = 0;
    for (audiences) |audience| {
        const bit: u5 = if (std.mem.eql(u8, audience, "self")) 0 else if (std.mem.eql(u8, audience, "household")) 1 else if (std.mem.eql(u8, audience, "group")) 2 else if (std.mem.eql(u8, audience, "community")) 3 else if (std.mem.eql(u8, audience, "public")) 4 else if (std.mem.eql(u8, audience, "market")) 5 else if (std.mem.eql(u8, audience, "institution")) 6 else return error.UnknownAudience;
        mask |= @as(u32, 1) << bit;
    }
    return mask;
}

fn containsField(fields: []Field, name: []const u8) bool {
    for (fields) |field| if (std.mem.eql(u8, field.name, name)) return true;
    return false;
}

fn checkedText(value: []const u8, required: bool) !void {
    if ((required and value.len == 0) or std.mem.indexOfScalar(u8, value, 0) != null)
        return error.InvalidText;
}

// Validate the whole descriptor before opening the output. Names remain exact
// string data. Array positions provide private C symbols, never semantic IDs.
fn validate(profiles: []Profile) !void {
    if (profiles.len == 0) return error.EmptyProfiles;
    for (profiles, 0..) |profile, pi| {
        try checkedText(profile.name, true);
        for (profiles[0..pi]) |previous| {
            if (std.mem.eql(u8, previous.name, profile.name)) return error.DuplicateProfile;
        }
        if (profile.evidence_requirements.len == 0 or profile.fields.len == 0 or profile.projections.len == 0)
            return error.EmptyProfile;
        for (profile.evidence_requirements, 0..) |requirement, ri| try checkedText(requirement, ri == 0);
        for (profile.fields, 0..) |field, fi| {
            try checkedText(field.name, true);
            if (field.audiences.len == 0) return error.EmptyAudience;
            _ = try audienceMask(field.audiences);
            if (field.withhold_reason) |reason| try checkedText(reason, false);
            for (profile.fields[0..fi]) |previous| {
                if (std.mem.eql(u8, previous.name, field.name)) return error.DuplicateField;
            }
        }
        for (profile.projections, 0..) |projection, ci| {
            try checkedText(projection.carrier, true);
            try checkedText(projection.standard, true);
            if (projection.container) |container| try checkedText(container, false);
            if (projection.fields.len == 0) return error.EmptyProjection;
            for (profile.projections[0..ci]) |previous| {
                if (std.mem.eql(u8, previous.carrier, projection.carrier)) return error.DuplicateCarrier;
            }
            for (projection.fields, 0..) |field, fi| {
                try checkedText(field.semantic, true);
                try checkedText(field.carrier, true);
                if (!containsField(profile.fields, field.semantic)) return error.UndeclaredSemanticField;
                for (projection.fields[0..fi]) |previous| {
                    if (std.mem.eql(u8, previous.carrier, field.carrier)) return error.DuplicateCarrierMapping;
                }
            }
        }
    }
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
    try validate(parsed.value);
    var out = try std.fs.cwd().createFile(args[2], .{});
    defer out.close();
    const w = out.writer();
    try w.writeAll("/* Generated from schemas/control/domain-profile.cue. Do not edit. */\n#ifndef BONFYRE_NATIVE_DOMAIN_PROFILES_H\n#define BONFYRE_NATIVE_DOMAIN_PROFILES_H\n\n#include <stddef.h>\n#include <stdint.h>\n\ntypedef struct { const char *semantic; const char *carrier; } BfDomainCarrierField;\ntypedef struct { const char *carrier; const char *standard; const char *container; const BfDomainCarrierField *fields; size_t field_count; } BfDomainCarrierProjection;\ntypedef struct { const char *name; uint32_t audience_mask; const char *withhold_reason; } BfDomainFieldRule;\ntypedef struct { const char *name; const char *evidence_requirement; const BfDomainFieldRule *fields; size_t field_count; const BfDomainCarrierProjection *projections; size_t projection_count; } BfNativeDomainProfile;\n\n");
    for (parsed.value, 0..) |profile, pi| {
        for (profile.projections, 0..) |projection, ci| {
            try w.print("static const BfDomainCarrierField bf_domain_{d}_{d}_fields[] = {{\n", .{ pi, ci });
            for (projection.fields) |field| {
                try w.writeAll("  {"); try cString(w, field.semantic); try w.writeAll(", "); try cString(w, field.carrier); try w.writeAll("},\n");
            }
            try w.writeAll("};\n");
        }
        try w.print("static const BfDomainFieldRule bf_domain_{d}_rules[] = {{\n", .{pi});
        for (profile.fields) |field| {
            try w.writeAll("  {"); try cString(w, field.name); try w.print(", 0x{x}, ", .{try audienceMask(field.audiences)});
            if (field.withhold_reason) |reason| try cString(w, reason) else try w.writeAll("NULL");
            try w.writeAll("},\n");
        }
        try w.writeAll("};\n");
        try w.print("static const BfDomainCarrierProjection bf_domain_{d}_projections[] = {{\n", .{pi});
        for (profile.projections, 0..) |projection, ci| {
            try w.writeAll("  {"); try cString(w, projection.carrier); try w.writeAll(", "); try cString(w, projection.standard); try w.writeAll(", ");
            if (projection.container) |container| try cString(w, container) else try w.writeAll("NULL");
            try w.print(", bf_domain_{d}_{d}_fields, sizeof(bf_domain_{d}_{d}_fields) / sizeof(bf_domain_{d}_{d}_fields[0])}},\n", .{ pi, ci, pi, ci, pi, ci });
        }
        try w.writeAll("};\n\n");
    }
    try w.writeAll("static const BfNativeDomainProfile bf_native_domain_profiles[] = {\n");
    for (parsed.value, 0..) |profile, pi| {
        try w.writeAll("  {"); try cString(w, profile.name); try w.writeAll(", "); try cString(w, profile.evidence_requirements[0]);
        try w.print(", bf_domain_{d}_rules, sizeof(bf_domain_{d}_rules) / sizeof(bf_domain_{d}_rules[0]), bf_domain_{d}_projections, sizeof(bf_domain_{d}_projections) / sizeof(bf_domain_{d}_projections[0])}},\n", .{ pi, pi, pi, pi, pi, pi });
    }
    try w.writeAll("};\n#define BF_NATIVE_DOMAIN_PROFILE_COUNT (sizeof(bf_native_domain_profiles) / sizeof(bf_native_domain_profiles[0]))\n#endif\n");
}
