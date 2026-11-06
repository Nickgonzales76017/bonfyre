import { test, expect } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { build, main } from "../tools/compile_experience_map.mjs";
import { parseDelimited, encodeDelimited } from "../tools/tsv_contract.mjs";

test("TSV parser retains quoted tabs, newlines, and escaped quotes", () => {
  const values = [["kind", "name"], ["reference", 'A\tB "quote"\nnext']];
  expect(parseDelimited(encodeDelimited(values))).toEqual(values);
});

test("full estate compiles to exact candidate/reference outputs without selection", async () => {
  const out = mkdtempSync(join(tmpdir(), "bonfyre-experience-map-"));
  const manifest = await build({ write: true, out });
  const external = readFileSync(join(out, "external_bindings.jsonl"), "utf8").trimEnd().split("\n").map(JSON.parse);
  const apps = readFileSync(join(out, "app_bindings.jsonl"), "utf8").trimEnd().split("\n").map(JSON.parse);
  const typed = readFileSync(join(out, "typed_application_nodes.jsonl"), "utf8").trimEnd().split("\n").map(JSON.parse);
  const programs = readFileSync(join(out, "typed_application_programs.jsonl"), "utf8").trimEnd().split("\n").map(JSON.parse);
  const references = readFileSync(join(out, "interaction_evidence.jsonl"), "utf8").trimEnd().split("\n").map(JSON.parse);
  const lattice = readFileSync(join(out, "chain_reference_lattice.jsonl"), "utf8").trimEnd().split("\n").map(JSON.parse);
  expect(external).toHaveLength(manifest.external_occurrences);
  expect(manifest.external_occurrences).toBe(manifest.external_mapped);
  expect(external.every(row => row.protocol_chains.length > 0)).toBe(manifest.coverage.protocol_occurrences);
  expect(apps.filter(row => row.reference_only).every(row => row.cannot_infer.includes("interaction reference -> selected renderer or executable stage"))).toBe(true);
  expect(typed).toHaveLength(manifest.typed_application_estate.application_nodes);
  expect(typed.every(row => row.selection_state === "unselected_declaration" && !row.edges.length)).toBe(true);
  expect(programs).toHaveLength(manifest.human_chain_count);
  expect(programs.every(row => typed.filter(node => node.experience_id === row.experience_id).length >= 6)).toBe(true);
  expect(typed.every(row => row.cannot_infer.includes("action family -> authority or effect"))).toBe(true);
  expect(manifest.typed_application_estate.prose_stage_input_used).toBe(false);
  expect(references.every(row => row.selection_state === "reference_only" || row.selection_state === "unobserved_candidate")).toBe(true);
  expect(references.filter(row => row.provenance === "donor" || row.provenance === "frappe")
    .every(row => row.cannot_infer.includes("reference -> executable application"))).toBe(true);
  expect(references.filter(row => row.provenance === "ghost")
    .every(row => row.selection_state === "unobserved_candidate")).toBe(true);
  expect(lattice).toHaveLength(manifest.human_chain_count);
  expect(lattice.every(row => row.selection_state === "evidence_only" && row.interaction_reference_count > 0)).toBe(true);
  expect(manifest.interaction_reference_lattice.market_reference_is_not_selection_boundary).toBe(true);
  expect(JSON.parse(readFileSync(join(out, "manifest.json"), "utf8"))).toEqual(manifest);
  const release = apps.find(row => row.name === 'training_program_release');
  expect(release.human_chains).toEqual(['learning', 'creator-publish']);
  expect(release.classification).toEqual({basis:'atlas-source-path',human_chains:['learning','creator-publish'],architecture_ids:['training-program-publication']});
  expect(apps.find(row => row.id === 'architecture:training-creator-request').human_chains).toContain('service-case');
  expect(apps.find(row => row.id === 'architecture:training-creator-request').interaction_refs.some(row => row.destination === 'work-graph')).toBe(true);
  expect(apps.find(row => row.id === 'architecture:training-creator-access').human_chains).toContain('identity-access');
  expect(apps.find(row => row.id === 'architecture:training-program-publication').cannot_infer.join(' ')).toContain('paid entitlement');
  expect(manifest.atlas_declaration_digest).toMatch(/^[a-f0-9]{64}$/);
  const graph = JSON.parse(readFileSync(join(out, 'owner_graph.json'), 'utf8'));
  expect(graph.atlas_declaration_digest).toBe(manifest.atlas_declaration_digest);
  expect(graph.owners.length).toBe(manifest.owner_graph.owners);
  expect(graph.interactions.length).toBe(manifest.owner_graph.interactions);
  expect(graph.owners.find(row=>row.id==='work-graph').commands).toContain('BonfyreQueue');
  expect(graph.workflows).toHaveLength(manifest.human_chain_count);
  graph.interactions = [];
  writeFileSync(join(out, 'owner_graph.json'), JSON.stringify(graph));
  await expect(main(['--check','--output',out])).rejects.toThrow('owner graph is stale or damaged');
  rmSync(out, {recursive:true,force:true});
});
