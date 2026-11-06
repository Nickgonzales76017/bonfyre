#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const worker = path.resolve(here, "../runtime/stage-compute-vgpu.mjs");
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "bonfyre-stage-compute-"));
try {
  const places = [];
  for (let i = 0; i < 40; i++) {
    places.push({
      source: "places",
      key: "p" + i,
      kind: "place",
      title: "Place " + i,
      placed: true,
      lon: -96.80 + i * 0.001,
      lat: 32.78,
    });
  }
  const closure = {
    schema: "bonfyre.stage-closure.v1",
    subject: "event:test",
    stageable: true,
    material: { events: [], places },
    compute_plan: [{ job: "spatial-neighborhood", backend_candidate: "vgpu" }],
  };
  const input = path.join(tmp, "closure.json");
  fs.writeFileSync(input, JSON.stringify(closure));

  function run(backend) {
    const result = spawnSync(process.execPath, [worker, "--backend", backend, "--input", input], {
      encoding: "utf8",
      timeout: 30000,
    });
    assert.equal(result.status, 0, result.stderr);
    return JSON.parse(result.stdout);
  }

  const cpu = run("cpu");
  assert.equal(cpu.receipt.actual_backend, "cpu");
  assert.equal(cpu.receipt.points, 40);
  assert.equal(cpu.receipt.pairs, 1600);
  assert.equal(cpu.nearest.length, 40);
  assert.equal(cpu.nearest[0].nearest_key, "p1");
  assert.ok(cpu.nearest[0].meters > 80 && cpu.nearest[0].meters < 120, cpu.nearest[0]);

  const auto = run("auto");
  assert.ok(["cpu", "vgpu-node"].includes(auto.receipt.actual_backend), auto.receipt);
  assert.equal(auto.receipt.input_digest, cpu.receipt.input_digest);
  for (let i = 0; i < cpu.nearest.length; i++) {
    const nearestIndex = Number(auto.nearest[i].nearest_key.slice(1));
    assert.ok(
      (i === 0 && nearestIndex === 1) ||
      (i === 39 && nearestIndex === 38) ||
      (i > 0 && i < 39 && Math.abs(nearestIndex - i) === 1),
      { i, nearest: auto.nearest[i], receipt: auto.receipt });
    assert.ok(Math.abs(auto.nearest[i].meters - cpu.nearest[i].meters) < 1.0,
      { i, auto: auto.nearest[i], cpu: cpu.nearest[i], receipt: auto.receipt });
  }
  assert.match(auto.receipt.cannot_infer, /does not establish place identity/);

  console.log("stage compute:", auto.receipt.actual_backend, "PASS");
} finally {
  fs.rmSync(tmp, { recursive: true, force: true });
}
