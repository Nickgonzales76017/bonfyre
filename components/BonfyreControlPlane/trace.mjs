// Spans over native operations, written in the Trace Event format that
// Perfetto's UI and trace processor load as they are. This is evidence for
// moving a boundary: it shows what was started, how long it ran, and what it
// read, so "this crosses a process" or "this parses the same body twice" is a
// measurement and not an opinion. It instruments; it decides nothing.
import { createHash } from "node:crypto";
import { existsSync, readFileSync, statSync, writeFileSync } from "node:fs";

export class Trace {
  constructor(name = "bonfyre") { this.name = name; this.events = []; this.origin = performance.now(); }

  // A file is identified by its bytes, so two operations reading the same
  // content under different names are still seen to read the same thing.
  static body(path) {
    try { if (!existsSync(path) || !statSync(path).isFile()) return null; const bytes = readFileSync(path); return { path, bytes: bytes.length, sha256: createHash("sha256").update(bytes).digest("hex") }; }
    catch { return null; }
  }

  async span(name, args, work) {
    const started = performance.now();
    const reads = (args.reads ?? []).map(Trace.body).filter(Boolean);
    let failed = "";
    try { return await work(); }
    catch (why) { failed = String(why?.message ?? why).slice(0, 200); throw why; }
    finally {
      const { reads: _, ...rest } = args;
      this.events.push({ name, cat: args.kind ?? "operation", ph: "X", pid: process.pid, tid: 1, ts: Math.round((started - this.origin) * 1000), dur: Math.round((performance.now() - started) * 1000),
        args: { ...rest, reads, ...(failed ? { failed } : {}) } });
    }
  }

  write(path) { writeFileSync(path, JSON.stringify({ displayTimeUnit: "ms", metadata: { source: this.name }, traceEvents: this.events })); return path; }

  // What the trace says, without opening a viewer.
  summary() {
    const byName = {}, bodies = new Map();
    for (const event of this.events) {
      const row = byName[event.name] ??= { count: 0, ms: 0, processes: 0 };
      row.count++; row.ms += event.dur / 1000; if (event.cat === "process") row.processes++;
      for (const read of event.args.reads) {
        const seen = bodies.get(read.sha256) ?? { sha256: read.sha256, bytes: read.bytes, readers: [] };
        seen.readers.push(event.name); bodies.set(read.sha256, seen);
      }
    }
    for (const row of Object.values(byName)) row.ms = Math.round(row.ms * 10) / 10;
    const span = this.events.length ? (Math.max(...this.events.map(e => e.ts + e.dur)) - Math.min(...this.events.map(e => e.ts))) / 1000 : 0;
    return { operations: this.events.length, processes_started: this.events.filter(event => event.cat === "process").length, wall_ms: Math.round(span * 10) / 10, by_operation: byName,
      bodies_read_more_than_once: [...bodies.values()].filter(body => body.readers.length > 1).map(body => ({ ...body, times: body.readers.length, bytes_reread: body.bytes * (body.readers.length - 1) })),
      cannot_infer: ["a body read twice -> a body that should be read once; two readers may need different things from it", "a process started -> a cost worth removing; that depends on how often the path runs"] };
  }
}
