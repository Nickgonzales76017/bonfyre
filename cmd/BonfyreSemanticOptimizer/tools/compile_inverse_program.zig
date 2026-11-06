const std = @import("std");

// This compiler consumes reusable institutional algebra. It deliberately has
// no TargetObligation/program type: targets live in the native owner and are
// supplied at synthesis time.
const Pattern = struct { owner: []const u8, predicate: []const u8, expected_state: []const u8 };
const Cost = struct {
    elapsed_minutes: i64, cash_delta_micros: i64, accrued_cost_micros: i64,
    resource_consumption: i64, rights_conceded: i64, ownership_dilution_bps: i64,
    reversibility_frontier: i64, compensation_cost_micros: i64,
    success_probability_ppm: i64, evidence_strength: i64, proof_distance: i64,
    authority_distance: i64, information_disclosure: i64, provider_dependence: i64,
    failure_domain_concentration: i64, freshness_exposure: i64,
    human_attention_minutes: i64, roundtrip_observability: i64, semantic_loss: i64,
};
const Transformation = struct {
    id: []const u8, requires: []const Pattern = &.{}, establishes: []const Pattern,
    retracts: []const Pattern = &.{}, consumes: []const []const u8 = &.{},
    produces: []const []const u8 = &.{}, creates_commitments: []const []const u8 = &.{},
    discharges_commitments: []const []const u8 = &.{}, grants_rights: []const []const u8 = &.{},
    concedes_rights: []const []const u8 = &.{}, conflicts_with_rights: []const []const u8 = &.{},
    obligations: []const []const u8 = &.{}, protocol_pattern: []const u8,
    resource_pattern: []const u8, effect_kind: []const u8, reversibility: []const u8, cost: Cost,
};
const Equivalence = struct { lhs: []const []const u8, rhs: []const u8 };
const Policy = struct { id: []const u8, prefer: []const []const u8 };
const OwnerState = struct { predicate: []const u8, expected_state: []const u8, native_state: []const u8, owner_query: u16 };
const OwnerAdapter = struct { owner: []const u8, opcode: []const u8, states: []const OwnerState };
const Input = struct { owners: []const OwnerAdapter, transformations: []const Transformation, equivalences: []const Equivalence, preference_policies: []const Policy };

const dims = [_][]const u8{ "elapsed_minutes", "cash_delta_micros", "accrued_cost_micros", "resource_consumption", "rights_conceded", "ownership_dilution_bps", "reversibility_frontier", "compensation_cost_micros", "success_probability_ppm", "evidence_strength", "proof_distance", "authority_distance", "information_disclosure", "provider_dependence", "failure_domain_concentration", "freshness_exposure", "human_attention_minutes", "roundtrip_observability", "semantic_loss" };

fn indexOf(values: []const []const u8, value: []const u8) !usize { for (values, 0..) |v, i| if (std.mem.eql(u8, v, value)) return i; return error.UnknownValue; }
fn uniqueOpcodeIndex(owners: []const OwnerAdapter, opcode: []const u8) !usize { var n: usize=0; for(owners)|owner| { if(std.mem.eql(u8,owner.opcode,opcode)) return n; var earlier=false; for(owners[0..n])|seen|if(std.mem.eql(u8,seen.opcode,opcode)){earlier=true;break;}; if(!earlier)n+=1; } return error.UnknownValue; }
fn stateIndex(owners: []const OwnerAdapter, state: []const u8) !usize {
    var n: usize = 0;
    for (owners) |owner| {
        for (owner.states) |candidate| {
            if (std.mem.eql(u8, candidate.native_state, state)) return n;
            n += 1;
        }
    }
    return error.UnknownValue;
}
fn emitUpperToken(w: anytype, prefix: []const u8, value: []const u8) !void { try w.writeAll(prefix); for(value)|c| try w.writeByte(if(c>='a' and c<='z') c-32 else if((c>='A' and c<='Z')or(c>='0' and c<='9')) c else '_'); }
fn validId(id: []const u8) bool { if (id.len == 0 or id[0] < 'a' or id[0] > 'z') return false; for (id) |c| if (!((c >= 'a' and c <= 'z') or (c >= '0' and c <= '9') or c == '_')) return false; return true; }
fn containsTransform(ts: []const Transformation, id: []const u8) bool { for (ts) |t| if (std.mem.eql(u8, t.id, id)) return true; return false; }
fn csv(w: anytype, xs: []const []const u8) !void { for (xs, 0..) |x, i| { if (i != 0) try w.writeByte(','); try w.writeAll(x); } }
fn binding(owners: []const OwnerAdapter, p: Pattern) !struct { adapter: usize, state: usize, query: u16 } {
    for (owners) |owner| if (std.mem.eql(u8, owner.owner, p.owner)) {
        const a = try uniqueOpcodeIndex(owners, owner.opcode);
        for (owner.states) |s| if (std.mem.eql(u8, s.predicate, p.predicate) and std.mem.eql(u8, s.expected_state, p.expected_state)) {
            const st = try stateIndex(owners, s.native_state);
            return .{ .adapter = a + 1, .state = st + 1, .query = s.owner_query };
        };
        return error.UnmappedOwnerState;
    };
    return error.UnknownOwner;
}
fn emitPatterns(w: anytype, owners: []const OwnerAdapter, ts: []const Transformation, field: []const u8) !void {
    for (ts) |t| {
        const ps: []const Pattern = if (std.mem.eql(u8, field, "requires")) t.requires else if (std.mem.eql(u8, field, "establishes")) t.establishes else t.retracts;
        try w.print("static const BfInversePredicatePattern bf_inverse_{s}_{s}[] = {{\n", .{field, t.id});
        for (ps) |p| { const b = try binding(owners, p); try w.print("{{\"{s}\",\"{s}\",\"{s}\",{d},{d},{d}}},\n", .{p.owner,p.predicate,p.expected_state,b.adapter,b.state,b.query}); }
        if (ps.len == 0) try w.writeAll("{\"\",\"\",\"\",0,0,0},\n");
        try w.writeAll("};\n");
    }
}
fn eclass(ts: []const Transformation, eqs: []const Equivalence, id: []const u8) usize {
    var n: usize = 0; for (ts) |t| { if (std.mem.eql(u8,t.id,id)) break; n += 1; }
    // The grouping is the static extraction shape created from egglog's rewrite
    // algebra. Native selection uses this precompiled result, never a process.
    for (eqs) |eq| {
        var hit = std.mem.eql(u8, eq.rhs, id);
        for (eq.lhs) |x| {
            if (std.mem.eql(u8, x, id)) hit = true;
        }
        if (hit) {
            for (eq.lhs) |x| {
                for (ts, 0..) |t, i| {
                    if (std.mem.eql(u8, t.id, x) and i < n) n = i;
                }
            }
            for (ts, 0..) |t, i| {
                if (std.mem.eql(u8, t.id, eq.rhs) and i < n) n = i;
            }
        }
    }
    return n;
}
fn header(input: Input, path: []const u8) !void {
    var f = try std.fs.cwd().createFile(path, .{}); defer f.close(); const w=f.writer();
    try w.writeAll("/* Generated from YAFF/CUE TransformationAlgebra. Do not edit. */\n#ifndef BF_INVERSE_PROGRAMS_H\n#define BF_INVERSE_PROGRAMS_H\n#include <stddef.h>\n#include <stdint.h>\n");
    try w.writeAll("typedef enum { BF_INVERSE_ADAPTER_NONE=0");
    for (input.owners, 0..) |owner, i| {
        var earlier = false;
        for (input.owners[0..i]) |seen| {
            if (std.mem.eql(u8, seen.opcode, owner.opcode)) { earlier = true; break; }
        }
        if (!earlier) { try w.writeByte(','); try emitUpperToken(w, "BF_INVERSE_ADAPTER_", owner.opcode); }
    }
    try w.writeAll(" } BfInverseAdapter;\ntypedef enum { BF_INVERSE_STATE_NONE=0");
    for (input.owners) |owner| {
        for (owner.states) |state| { try w.writeByte(','); try emitUpperToken(w, "BF_INVERSE_STATE_", state.native_state); }
    }
    try w.writeAll(" } BfInverseNativeState;\n");
    try w.writeAll("typedef enum { BF_COST_ELAPSED,BF_COST_CASH,BF_COST_ACCRUED,BF_COST_RESOURCE,BF_COST_RIGHTS,BF_COST_DILUTION,BF_COST_REVERSIBILITY,BF_COST_COMPENSATION,BF_COST_SUCCESS,BF_COST_EVIDENCE,BF_COST_PROOF_DISTANCE,BF_COST_AUTHORITY_DISTANCE,BF_COST_DISCLOSURE,BF_COST_PROVIDER,BF_COST_FAILURE_DOMAIN,BF_COST_FRESHNESS,BF_COST_ATTENTION,BF_COST_ROUNDTRIP,BF_COST_SEMANTIC_LOSS } BfInverseCostDimension;\ntypedef struct { const char *owner,*predicate,*expected; uint8_t adapter,native_state; uint16_t owner_query; } BfInversePredicatePattern;\ntypedef struct { const char *owner,*predicate,*expected; uint8_t adapter,native_state; uint16_t owner_query; } BfInverseOwnerGrammar;\ntypedef struct { int64_t elapsed_minutes,cash_delta_micros,accrued_cost_micros,resource_consumption,rights_conceded,ownership_dilution_bps,reversibility_frontier,compensation_cost_micros,success_probability_ppm,evidence_strength,proof_distance,authority_distance,information_disclosure,provider_dependence,failure_domain_concentration,freshness_exposure,human_attention_minutes,roundtrip_observability,semantic_loss; } BfInverseSemanticCost;\ntypedef struct { const char *id; const BfInversePredicatePattern *requires,*establishes,*retracts; size_t requires_count,establishes_count,retracts_count; const char *consumes,*produces,*creates_commitments,*discharges_commitments,*grants_rights,*concedes_rights,*conflicts_with_rights,*obligations,*protocol_pattern,*resource_pattern,*effect_kind,*reversibility; BfInverseSemanticCost cost; uint16_t eclass; } BfInverseTransformationDescriptor;\ntypedef struct { const char *id; const BfInverseCostDimension *dimensions; size_t dimension_count; } BfInversePreferencePolicy;\n");
    try w.writeAll("static const BfInverseOwnerGrammar bf_inverse_owner_grammar[] = {\n");
    for (input.owners) |owner| {
        for (owner.states) |s| {
            const b = try binding(input.owners, .{ .owner=owner.owner, .predicate=s.predicate, .expected_state=s.expected_state });
            try w.print("{{\"{s}\",\"{s}\",\"{s}\",{d},{d},{d}}},\n", .{owner.owner,s.predicate,s.expected_state,b.adapter,b.state,b.query});
        }
    }
    try w.writeAll("};\nstatic const size_t bf_inverse_owner_grammar_count=sizeof(bf_inverse_owner_grammar)/sizeof(*bf_inverse_owner_grammar);\n");
    try emitPatterns(w,input.owners,input.transformations,"requires"); try emitPatterns(w,input.owners,input.transformations,"establishes"); try emitPatterns(w,input.owners,input.transformations,"retracts");
    for(input.preference_policies) |p| { try w.print("static const BfInverseCostDimension bf_inverse_policy_{s}[]={{",.{p.id}); for(p.prefer,0..)|d,i| {if(i!=0)try w.writeByte(','); try w.print("{d}",.{try indexOf(&dims,d)});} try w.writeAll("};\n"); }
    try w.writeAll("static const BfInverseTransformationDescriptor bf_inverse_transformations[] = {\n");
    for(input.transformations) |t| { try w.print("{{\"{s}\",bf_inverse_requires_{s},bf_inverse_establishes_{s},bf_inverse_retracts_{s},{d},{d},{d},",.{t.id,t.id,t.id,t.id,t.requires.len,t.establishes.len,t.retracts.len}); for ([_][]const []const u8{t.consumes,t.produces,t.creates_commitments,t.discharges_commitments,t.grants_rights,t.concedes_rights,t.conflicts_with_rights,t.obligations}) |x| {try w.writeByte('"');try csv(w,x);try w.writeAll("\",");} try w.print("\"{s}\",\"{s}\",\"{s}\",\"{s}\",{{{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d},{d}}},{d}}},\n",.{t.protocol_pattern,t.resource_pattern,t.effect_kind,t.reversibility,t.cost.elapsed_minutes,t.cost.cash_delta_micros,t.cost.accrued_cost_micros,t.cost.resource_consumption,t.cost.rights_conceded,t.cost.ownership_dilution_bps,t.cost.reversibility_frontier,t.cost.compensation_cost_micros,t.cost.success_probability_ppm,t.cost.evidence_strength,t.cost.proof_distance,t.cost.authority_distance,t.cost.information_disclosure,t.cost.provider_dependence,t.cost.failure_domain_concentration,t.cost.freshness_exposure,t.cost.human_attention_minutes,t.cost.roundtrip_observability,t.cost.semantic_loss,eclass(input.transformations,input.equivalences,t.id)}); }
    try w.writeAll("};\nstatic const size_t bf_inverse_transformation_count=sizeof(bf_inverse_transformations)/sizeof(*bf_inverse_transformations);\nstatic const BfInversePreferencePolicy bf_inverse_policies[]={"); for(input.preference_policies)|p|try w.print("{{\"{s}\",bf_inverse_policy_{s},sizeof(bf_inverse_policy_{s})/sizeof(*bf_inverse_policy_{s})}},",.{p.id,p.id,p.id,p.id}); try w.writeAll("};\nstatic const size_t bf_inverse_policy_count=sizeof(bf_inverse_policies)/sizeof(*bf_inverse_policies);\n#endif\n");
}
fn egg(input: Input, path: []const u8) !void { var f=try std.fs.cwd().createFile(path,.{});defer f.close();const w=f.writer();try w.writeAll("; Generated closure algebra; saturation is compiled to native eclasses.\n(datatype Closure ");for(input.transformations)|t|try w.print("(T_{s}) ",.{t.id});try w.writeAll("(And Closure Closure))\n");for(input.equivalences)|e| { if(e.lhs.len==1)try w.print("(rewrite (T_{s}) (T_{s}))\n",.{e.lhs[0],e.rhs}) else {if(e.lhs.len!=2)return error.UnsupportedEquivalenceArity;try w.print("(rewrite (And (T_{s}) (T_{s})) (T_{s}))\n",.{e.lhs[0],e.lhs[1],e.rhs});}}try w.writeAll("(run 10)\n"); }
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);
    if (args.len != 4) return error.Usage;
    const text = try std.fs.cwd().readFileAlloc(allocator, args[1], 8 * 1024 * 1024);
    defer allocator.free(text);
    var parsed = try std.json.parseFromSlice(Input, allocator, text, .{ .ignore_unknown_fields = false });
    defer parsed.deinit();
    for (parsed.value.transformations) |t| {
        if (!validId(t.id) or t.establishes.len == 0) return error.InvalidTransformation;
        for (t.establishes) |p| { _ = try binding(parsed.value.owners, p); }
        for (t.requires) |p| { _ = try binding(parsed.value.owners, p); }
        for (t.retracts) |p| { _ = try binding(parsed.value.owners, p); }
    }
    for (parsed.value.equivalences) |e| {
        if (!containsTransform(parsed.value.transformations, e.rhs)) return error.UnknownTransformation;
        for (e.lhs) |x| if (!containsTransform(parsed.value.transformations, x)) return error.UnknownTransformation;
    }
    try egg(parsed.value, args[2]);
    try header(parsed.value, args[3]);
}
