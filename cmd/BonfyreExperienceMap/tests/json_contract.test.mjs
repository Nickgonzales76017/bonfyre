import { test, expect } from "bun:test";
import { canonicalAscii, canonicalJson, prettyCanonical, pythonFlat } from "../tools/json_contract.mjs";

test("canonical JSON matches the retired Python digest grammar", () => {
  const value = { z: ["É", "🚀"], a: { b: true, a: null } };
  expect(canonicalJson(value)).toBe('{"a":{"a":null,"b":true},"z":["É","🚀"]}');
  expect(canonicalAscii(value)).toBe('{"a":{"a":null,"b":true},"z":["\\u00c9","\\ud83d\\ude80"]}');
  expect(pythonFlat(value)).toBe('{"a": {"a": null, "b": true}, "z": ["É", "🚀"]}');
  expect(prettyCanonical(value)).toContain('"a": {');
});
