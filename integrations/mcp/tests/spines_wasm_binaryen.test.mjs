// Binaryen as the optimisation and validation stage for generated Wasm. The
// pinned reducer is optimised to a candidate, and the candidate is held to the
// pinned module's exact behaviour over generated terms, including the ones it
// refuses and the ones that run out of fuel. The pinned artifact is not
// replaced here: this says what the stage would buy and that it is safe.
import { test, expect } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createSpinesWasmCore } from "../spines_wasm_core.mjs";
import { SPINES_HELIX_WASM_BYTES, SPINES_HELIX_WASM_PATH, SPINES_HELIX_WASM_SHA256 } from "../spines_wasm.mjs";

const optimiser = Bun.which("wasm-opt");
const features = ["--enable-bulk-memory", "--enable-sign-ext", "--enable-mutable-globals", "--enable-nontrapping-float-to-int"];

test.skipIf(!optimiser)("a Binaryen-optimised reducer is smaller, valid, and answers every term exactly as the pinned one", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-binaryen-"));
  try {
    const candidatePath = join(dir, "spines_helix.opt.wasm");
    const made = Bun.spawnSync([optimiser, "-Oz", ...features, SPINES_HELIX_WASM_PATH, "-o", candidatePath], { stderr: "pipe" });
    expect(made.exitCode).toBe(0);
    const pinnedBytes = readFileSync(SPINES_HELIX_WASM_PATH), candidateBytes = readFileSync(candidatePath);
    expect(candidateBytes.length).toBeLessThan(pinnedBytes.length * 0.92);
    expect(WebAssembly.validate(candidateBytes)).toBe(true);
    // The surface may only narrow: the same exports, and no import the pinned module did not already ask for.
    const shape = bytes => { const module = new WebAssembly.Module(bytes); return { imports: WebAssembly.Module.imports(module).map(i => `${i.module}.${i.name}:${i.kind}`).sort(), exports: WebAssembly.Module.exports(module).map(e => `${e.name}:${e.kind}`).sort() }; };
    const before = shape(pinnedBytes), after = shape(candidateBytes);
    expect(after.exports).toEqual(before.exports);
    expect(after.imports.every(name => before.imports.includes(name))).toBe(true);
    const dropped = before.imports.filter(name => !after.imports.includes(name));

    const pinned = await createSpinesWasmCore(pinnedBytes, { expectedSha256: SPINES_HELIX_WASM_SHA256, expectedBytes: SPINES_HELIX_WASM_BYTES });
    const candidate = await createSpinesWasmCore(candidateBytes, { expectedSha256: createHash("sha256").update(candidateBytes).digest("hex"), expectedBytes: candidateBytes.length });

    let seed = 20261004;
    const next = n => (seed = (seed * 1103515245 + 12345) & 0x7fffffff) % n;
    const term = depth => depth <= 0 || next(4) === 0 ? String(next(40) - 5) : `${["add", "sub", "mul", "div", "mod", "neg", "nope"][next(7)]}(${term(depth - 1)},${term(depth - 1)})`;
    const terms = ["mul(add(1,2),sub(9,4))", " ", "add(1", "add(1,2))", "div(1,0)", "mul(99999999,99999999)", "add(,)", "1", "sub(0,0)", "x".repeat(4000)];
    for (let i = 0; i < 400; i++) terms.push(term(1 + next(6)));
    let compared = 0, refused = 0;
    for (const one of terms) for (const fuel of [1, 7, 100, 5000]) {
      // Each answer names the module that gave it and how long it took; everything else must be the same.
      const { module_sha256: pinnedModule, duration_ms: _t1, ...a } = await pinned.reduce(one, fuel);
      const { module_sha256: candidateModule, duration_ms: _t2, ...b } = await candidate.reduce(one, fuel);
      expect(b).toEqual(a);
      expect(candidateModule).not.toBe(pinnedModule);
      compared++; if (a.exit_code !== 0) refused++;
    }
    expect(compared).toBe(terms.length * 4);
    expect(refused).toBeGreaterThan(20);          // the comparison covers refusals, not only successes

    const time = async core => { const started = performance.now(); for (let i = 0; i < 3000; i++) await core.reduce("mul(add(1,2),sub(9,4))", 100); return performance.now() - started; };
    await time(pinned); await time(candidate);    // warm
    const [pinnedMs, candidateMs] = [await time(pinned), await time(candidate)];
    console.log(`binaryen -Oz: ${pinnedBytes.length} -> ${candidateBytes.length} bytes; 3000 reductions ${pinnedMs.toFixed(1)} ms pinned, ${candidateMs.toFixed(1)} ms optimised; ${compared} answers compared, ${refused} of them refusals; host imports no longer asked for: ${dropped.join(", ") || "none"}`);
  } finally { rmSync(dir, { recursive: true, force: true }); }
}, 120_000);
