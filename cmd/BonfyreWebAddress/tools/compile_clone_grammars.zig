const std = @import("std");

// Thirty interaction grammars and the primitives under them, compiled from CUE.
//
// The reason this is generated rather than written is the reason the rest of the
// estate's contracts are: a table of thirty product names in prose is a wish
// list, and a table that compiles is one a page can be held to. What matters
// here is the cross-reference -- which primitive how many grammars share, and
// which grammars depend on a primitive no organ owns -- and that is computed
// from the declarations rather than asserted beside them.

const Primitive = struct {
    primitive: []const u8,
    label: []const u8,
    owner: []const u8,
    unowned_because: []const u8,
    means: []const u8,
    cannot_infer: []const u8,
};

const Grammar = struct {
    grammar: []const u8,
    studied: []const u8,
    explodes: []const u8,
    over: []const u8,
    needs: [][]const u8,
    wave: u8,
    cannot_infer: []const u8,
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

    const pr = try std.fs.cwd().readFileAlloc(a, args[1], 4 * 1024 * 1024);
    defer a.free(pr);
    const gr = try std.fs.cwd().readFileAlloc(a, args[2], 4 * 1024 * 1024);
    defer a.free(gr);

    // CUE is the closed authoring boundary; an unknown field is a second schema.
    var prims = try std.json.parseFromSlice([]Primitive, a, pr, .{ .ignore_unknown_fields = false });
    defer prims.deinit();
    var grams = try std.json.parseFromSlice([]Grammar, a, gr, .{ .ignore_unknown_fields = false });
    defer grams.deinit();

    // A grammar may only need a primitive that was declared. Catching this in
    // the compiler is what stops a page from naming a cell nothing defines.
    for (grams.value) |g| {
        for (g.needs) |n| {
            var found = false;
            for (prims.value) |p| {
                if (std.mem.eql(u8, p.primitive, n)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                std.debug.print("grammar {s} needs undeclared primitive {s}\n", .{ g.grammar, n });
                return error.UndeclaredPrimitive;
            }
        }
        if (g.needs.len == 0) {
            std.debug.print("grammar {s} needs nothing, so it explodes nothing\n", .{g.grammar});
            return error.EmptyGrammar;
        }
    }
    // A primitive with no owner must say what is missing; one with an owner must
    // not, or the page would print a gap beside a binding.
    for (prims.value) |p| {
        if (p.owner.len == 0 and p.unowned_because.len == 0) {
            std.debug.print("primitive {s} is unowned and gives no reason\n", .{p.primitive});
            return error.UnexplainedGap;
        }
        if (p.owner.len != 0 and p.unowned_because.len != 0) {
            std.debug.print("primitive {s} claims an owner and a reason for having none\n", .{p.primitive});
            return error.ContradictoryOwner;
        }
    }

    var out = try std.fs.cwd().createFile(args[3], .{});
    defer out.close();
    const w = out.writer();
    try w.writeAll(
        \\/* Generated from schemas/control/clone-grammar.cue. Do not edit. */
        \\#ifndef BONFYRE_NATIVE_CLONE_GRAMMARS_H
        \\#define BONFYRE_NATIVE_CLONE_GRAMMARS_H
        \\#include <stddef.h>
        \\#include <string.h>
        \\
        \\/* A semantic cell several grammars share. `owner` is the architecture in
        \\ * the atlas that already holds it; an empty owner means no organ does,
        \\ * and `unowned_because` says what is missing. `shared_by` is counted from
        \\ * the grammars rather than asserted here. */
        \\typedef struct { const char *primitive, *label, *owner, *unowned_because,
        \\                 *means, *cannot_infer; unsigned short shared_by; } BfInteractionPrimitive;
        \\
        \\/* Provenance and vocabulary only: naming a grammar is not a claim its
        \\ * authors know this estate exists, that their code runs here, or that this
        \\ * estate reproduces their product. `needs` is a |-delimited list of
        \\ * primitives; `unowned_needs` counts how many of them no organ owns. */
        \\typedef struct { const char *grammar, *studied, *explodes, *over, *needs, *cannot_infer;
        \\                 unsigned char wave; unsigned short need_count, unowned_needs; } BfCloneGrammar;
        \\
        \\static const BfInteractionPrimitive bf_interaction_primitives[] = {
        \\
    );
    for (prims.value) |p| {
        var shared: u16 = 0;
        for (grams.value) |g| {
            for (g.needs) |n| {
                if (std.mem.eql(u8, n, p.primitive)) {
                    shared += 1;
                    break;
                }
            }
        }
        try w.writeAll("  {");
        try cString(w, p.primitive);
        try w.writeAll(", ");
        try cString(w, p.label);
        try w.writeAll(", ");
        try cString(w, p.owner);
        try w.writeAll(", ");
        try cString(w, p.unowned_because);
        try w.writeAll(", ");
        try cString(w, p.means);
        try w.writeAll(", ");
        try cString(w, p.cannot_infer);
        try w.print(", {d}}},\n", .{shared});
    }
    try w.writeAll("};\n#define BF_INTERACTION_PRIMITIVE_COUNT (sizeof(bf_interaction_primitives) / sizeof(bf_interaction_primitives[0]))\n\nstatic const BfCloneGrammar bf_clone_grammars[] = {\n");
    for (grams.value) |g| {
        var unowned: u16 = 0;
        for (g.needs) |n| {
            for (prims.value) |p| {
                if (std.mem.eql(u8, p.primitive, n) and p.owner.len == 0) unowned += 1;
            }
        }
        try w.writeAll("  {");
        try cString(w, g.grammar);
        try w.writeAll(", ");
        try cString(w, g.studied);
        try w.writeAll(", ");
        try cString(w, g.explodes);
        try w.writeAll(", ");
        try cString(w, g.over);
        try w.writeAll(", \"|");
        for (g.needs) |n| {
            for (n) |c| try w.writeByte(c);
            try w.writeByte('|');
        }
        try w.writeAll("\", ");
        try cString(w, g.cannot_infer);
        try w.print(", {d}, {d}, {d}}},\n", .{ g.wave, g.needs.len, unowned });
    }
    try w.writeAll(
        \\};
        \\#define BF_CLONE_GRAMMAR_COUNT (sizeof(bf_clone_grammars) / sizeof(bf_clone_grammars[0]))
        \\
        \\static const BfInteractionPrimitive *bf_interaction_primitive(const char *name) {
        \\    size_t i;
        \\    if (!name) return NULL;
        \\    for (i = 0; i < BF_INTERACTION_PRIMITIVE_COUNT; i++)
        \\        if (!strcmp(bf_interaction_primitives[i].primitive, name))
        \\            return &bf_interaction_primitives[i];
        \\    return NULL;
        \\}
        \\
        \\static const BfCloneGrammar *bf_clone_grammar(const char *name) {
        \\    size_t i;
        \\    if (!name) return NULL;
        \\    for (i = 0; i < BF_CLONE_GRAMMAR_COUNT; i++)
        \\        if (!strcmp(bf_clone_grammars[i].grammar, name))
        \\            return &bf_clone_grammars[i];
        \\    return NULL;
        \\}
        \\
        \\/* Does this grammar need this primitive? Whole segments only, so "media"
        \\ * never matches "media-estate". */
        \\static int bf_grammar_needs(const BfCloneGrammar *g, const char *primitive) {
        \\    const char *p;
        \\    if (!g || !primitive || !*primitive) return 0;
        \\    p = g->needs;
        \\    while ((p = strchr(p, '|')) != NULL) {
        \\        const char *start = ++p;
        \\        const char *end = strchr(start, '|');
        \\        size_t len;
        \\        if (!end) break;
        \\        len = (size_t)(end - start);
        \\        if (len && !strncmp(start, primitive, len) && !primitive[len]) return 1;
        \\        p = end;
        \\    }
        \\    return 0;
        \\}
        \\#endif
        \\
    );
}
