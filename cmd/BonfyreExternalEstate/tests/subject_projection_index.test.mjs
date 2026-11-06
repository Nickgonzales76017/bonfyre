import { test, expect } from "bun:test";
import { candidateCollections, collectionMaterialAcceptance, collectionMaterialAccepts, compareItems, indexCollections } from "../tools/subject_projection_rules.mjs";

test("route item order matches the prior tuple-key law without per-comparison arrays", () => {
  const tupleOrder = (a, b) => {
    const key = item => {
      const at = Number(item.at || 0);
      return [at <= 0 ? 1 : 0, item.material === "event" ? (at > 0 ? at : 2 ** 62) : (at > 0 ? -at : 0),
        String(item.source ?? ""), String(item.key ?? "")];
    };
    const x = key(a), y = key(b);
    for (let i = 0; i < x.length; i++) {
      if (x[i] < y[i]) return -1;
      if (x[i] > y[i]) return 1;
    }
    return 0;
  };
  const items = Array.from({ length: 1_000 }, (_, i) => ({
    at: i % 7 === 0 ? 0 : i % 11 === 0 ? "bad" : (i * 37) % 701,
    material: i % 3 === 0 ? "event" : "publication",
    source: `source-${i % 23}`, key: `key-${i % 17}`,
  }));
  const previous = [...items].sort(tupleOrder).map(item => items.indexOf(item));
  const current = [...items].sort(compareItems).map(item => items.indexOf(item));
  expect(current).toEqual(previous);
  const previousStart = performance.now();
  for (let i = 0; i < 10; i++) [...items].sort(tupleOrder);
  const previousMs = performance.now() - previousStart;
  const currentStart = performance.now();
  for (let i = 0; i < 10; i++) [...items].sort(compareItems);
  const currentMs = performance.now() - currentStart;
  console.log(JSON.stringify({ route_sort_benchmark: { items: items.length, sorts: 10,
    tuple_ms: Math.round(previousMs * 100) / 100, direct_ms: Math.round(currentMs * 100) / 100 } }));
});

test("compiled material acceptance preserves aliases and empty-kind semantics", () => {
  const collections = [{ material_kind: ["PublicationOccurrence", "EventOccurrence"] }];
  const materials = ["publication", "video", "photo", "audio", "event", "post", "place", "mix", "unknown"];
  const accepts = collectionMaterialAcceptance(collections);
  for (const material of materials) expect(accepts(material)).toBe(collectionMaterialAccepts(collections, material));
  expect(accepts("event")).toBe(true);
  expect(accepts("post")).toBe(false);
  expect(collectionMaterialAcceptance([{ material_kind: [] }])("unknown")).toBe(true);
  expect(collectionMaterialAcceptance([])("unknown")).toBe(true);

  const iterations = 20_000;
  const repeatedStart = performance.now();
  let repeated = 0;
  for (let i = 0; i < iterations; i++) repeated += Number(collectionMaterialAccepts(collections, materials[i % materials.length]));
  const repeatedMs = performance.now() - repeatedStart;
  const compiledStart = performance.now();
  let compiled = 0;
  for (let i = 0; i < iterations; i++) compiled += Number(accepts(materials[i % materials.length]));
  const compiledMs = performance.now() - compiledStart;
  expect(compiled).toBe(repeated);
  console.log(JSON.stringify({ material_acceptance_benchmark: {
    item_checks: iterations, repeated_ms: Math.round(repeatedMs * 100) / 100,
    compiled_ms: Math.round(compiledMs * 100) / 100 } }));
});

test("indexed collection selection keeps exact ownership and estate fallback", () => {
  const collections = new Map([
    ["own", { subject: "institution:aurekai:live", domains: ["discover"], grouping: "domain", value: "discover", material_kind: [], sources: ["own-source"] }],
    ["other", { subject: "institution:other", domains: ["discover"], grouping: "domain", value: "discover", material_kind: [], sources: ["other-source"] }],
    ["estate", { subject: "estate:aurekai", domains: [], grouping: "family", value: "news", material_kind: [], sources: ["estate-source"] }],
  ]);
  const index = indexCollections(collections);
  expect(candidateCollections("institution:aurekai:live", "discover", "/discover", index,
    new Map(), ["news"], ["publication"])).toEqual([collections.get("estate"), collections.get("own")]);
  expect(candidateCollections("institution:other", "discover", "/discover", index,
    new Map(), ["news"], ["publication"])).toEqual([collections.get("other")]);
});

test("indexed selection measures the avoided cross-subject scan", () => {
  const collections = new Map();
  for (let i = 0; i < 20_000; i++) {
    collections.set(`other-${i}`, { subject: `institution:other-${i}`, domains: ["discover"],
      grouping: "domain", value: "discover", material_kind: [], sources: [] });
  }
  for (let i = 0; i < 20; i++) {
    collections.set(`target-${i}`, { subject: "institution:target", domains: ["discover"],
      grouping: "domain", value: "discover", material_kind: [], sources: [] });
  }
  const index = indexCollections(collections), sceneRefs = new Map();
  const iterations = 100;
  const scanStart = performance.now();
  let scanRows = 0;
  for (let run = 0; run < iterations; run++) {
    const found = [];
    for (const collection of collections.values())
      if (collection.subject === "institution:target" && collection.domains.includes("discover")) found.push(collection);
    scanRows += found.length;
  }
  const scanMs = performance.now() - scanStart;
  const indexStart = performance.now();
  let indexedRows = 0;
  for (let run = 0; run < iterations; run++)
    indexedRows += candidateCollections("institution:target", "discover", "/discover", index,
      sceneRefs, [], ["publication"]).length;
  const indexMs = performance.now() - indexStart;
  expect(indexedRows).toBe(scanRows);
  expect(index.bySubject.get("institution:target")).toHaveLength(20);
  console.log(JSON.stringify({ collection_selector_benchmark: {
    collections: collections.size, routes: iterations, scan_ms: Math.round(scanMs * 100) / 100,
    indexed_ms: Math.round(indexMs * 100) / 100 } }));
});
