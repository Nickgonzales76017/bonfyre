#!/usr/bin/env bun
// The compiler says a compiled field condition means what the donor's
// JavaScript means. This asks a real browser. Every compiled `eval:` condition
// in an application is evaluated twice over the same generated records: once
// by the native plan, once as the donor evaluates it -- the expression itself,
// run by Chrome with `doc` bound -- and the two answers are compared.
//
// Chrome runs headless in a throwaway profile with its sandbox on, loads one
// local file and nothing from the network.
//
// usage: bun browser_condition_parity.mjs <compiler> <app-dir> [--json FILE]
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const [compiler, app] = process.argv.slice(2);
const jsonOut = process.argv.includes("--json") ? process.argv[process.argv.indexOf("--json") + 1] : "";
const chrome = process.env.BONFYRE_CHROME ?? ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", "/Applications/Chromium.app/Contents/MacOS/Chromium",
  Bun.which("chromium") ?? "", Bun.which("google-chrome") ?? ""].find(path => path && existsSync(path));
if (!compiler || !app || !existsSync(compiler) || !existsSync(app)) { console.log("SKIP: compiler or application not present"); process.exit(0); }
if (!chrome) { console.log("SKIP: no Chrome or Chromium here"); process.exit(0); }

const root = resolve(import.meta.dir, "../../..");
const work = mkdtempSync(join(tmpdir(), "bonfyre-condition-parity-"));
const run = async (argv, stdin) => {
  const proc = Bun.spawn(argv, { stdout: "pipe", stderr: "pipe", stdin: stdin == null ? "ignore" : new Response(stdin) });
  const [out, code] = await Promise.all([new Response(proc.stdout).text(), proc.exited]);
  return { out, code };
};

try {
  await run([compiler, "--root", root, "--emit-condition-plans", join(work, "plans.json"), "-o", join(work, "pack"), app]);
  const plans = JSON.parse(readFileSync(join(work, "plans.json"), "utf8"));
  const byId = new Map(plans.plans.map(plan => [plan.plan_id, plan]));
  const sources = new Map();
  let withheld = 0;
  for (const use of plans.uses) {
    if (use.status !== "compiled" || use.form !== "eval" || sources.has(use.source)) continue;
    const plan = byId.get(use.plan_id);
    // A condition that reads the estate's bindings (which applications are
    // installed, say) needs a client state no generated record supplies;
    // those are counted and left out. One that reads the parent record is
    // given a generated parent, as a child row has in the donor.
    if (!plan || plan.env_reads.length) { withheld++; continue; }
    sources.set(use.source, plan);
  }

  // Records are built from what each condition reads: every field takes, in
  // turn, each value that distinguishes the dialect -- absent, null, empty
  // string, zero, one, their string forms, an empty and a non-empty list --
  // and every literal the condition itself compares against.
  let seed = 20261003;
  const next = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff;
  const cases = [];
  for (const [source, plan] of sources) {
    const literals = [...source.matchAll(/"([^"\\]*)"|'([^'\\]*)'/g)].map(match => match[1] ?? match[2])
      .concat([...source.matchAll(/(?<![\w.])(\d+(?:\.\d+)?)(?![\w.])/g)].map(match => Number(match[1])));
    const pool = [undefined, null, "", 0, 1, "0", "1", [], ["x"], ...new Set(literals)];
    // Each read, of the record or of its parent, takes each value in turn
    // while the others are drawn from the pool.
    const reads = [...plan.reads.map(field => ["doc", field]), ...plan.parent_reads.map(field => ["parent", field])];
    const docs = [{ doc: {}, parent: {} }];
    for (const [where, field] of reads) for (const value of pool) {
      const made = { doc: {}, parent: {} };
      for (const [otherWhere, other] of reads) if (otherWhere !== where || other !== field) { const pick = pool[Math.floor(next() * pool.length)]; if (pick !== undefined) made[otherWhere][other] = pick; }
      if (value !== undefined) made[where][field] = value;
      docs.push(made);
    }
    for (const made of docs.slice(0, 40)) cases.push({ source, doc: made.doc, ...(plan.parent_reads.length ? { parent: made.parent } : {}) });
  }

  // The donor's side, in the browser: the expression after `eval:`, with doc
  // bound. Conditions call helpers the donor's client defines globally
  // (in_list, cint, cstr, has_common); the donor's own source for them is
  // loaded, not a rewrite of it, so the reference stays the donor's.
  const utils = join(root, "integrations/frappe-bench/apps/frappe/frappe/public/js/frappe/utils");
  const helpers = existsSync(join(utils, "datatype.js"))
    ? readFileSync(join(utils, "datatype.js"), "utf8") + "\n"
      + (readFileSync(join(utils, "number_format.js"), "utf8").match(/\nfunction in_list\([\s\S]*?\n}\n/)?.[0] ?? "")
      + (readFileSync(join(utils, "common.js"), "utf8").match(/\nwindow\.lstrip = [\s\S]*?\n};\n/)?.[0] ?? "")
    : "";
  const page = join(work, "donor.html");
  writeFileSync(page, `<!doctype html><meta charset="utf-8"><pre id="out"></pre><script>
${helpers.replace(/<\/script/g, "<\\/script")}
</script><script>
const cases = ${JSON.stringify(cases).replace(/</g, "\\u003c")};
document.getElementById("out").textContent = JSON.stringify(cases.map(one => {
  // The donor's own evaluator, frappe.utils.eval: the code is a statement
  // list whose first expression is the answer, so a trailing ';' is legal.
  // cur_frm is the donor's open form. For a row of a child table its doc is
  // the parent record, which is what the generated parent stands for.
  window.cur_frm = { doc: one.parent ?? one.doc };
  try { return Boolean(new Function("doc", "parent", "let out = " + one.source.slice(5) + "; return out")(one.doc, one.parent)); }
  catch (why) { return "error"; }
}));
</script>`);
  // This Chrome prints the document and then stays up, so it is read until
  // the document closes and stopped, with a bound on how long that may take.
  const dumped = { out: "" };
  {
    const proc = Bun.spawn([chrome, "--headless=new", `--user-data-dir=${join(work, "profile")}`, "--no-first-run", "--no-default-browser-check",
      "--disable-background-networking", "--dump-dom", `file://${page}`], { stdout: "pipe", stderr: "ignore" });
    const limit = setTimeout(() => proc.kill(), 120_000);
    const decoder = new TextDecoder();
    for await (const chunk of proc.stdout) {
      dumped.out += decoder.decode(chunk, { stream: true });
      if (dumped.out.includes("</html>")) break;
    }
    clearTimeout(limit);
    proc.kill();
    await proc.exited;
  }
  const inner = dumped.out.match(/<pre id="out">([\s\S]*?)<\/pre>/);
  if (!inner) { console.log("FAIL: the browser returned no result"); process.exit(1); }
  const donor = JSON.parse(inner[1].replace(/&quot;/g, '"').replace(/&amp;/g, "&"));
  const version = (await run([chrome, "--version"])).out.trim();

  // The native side, a bounded number of evaluations at once.
  const native = new Array(cases.length);
  let at = 0;
  await Promise.all(Array.from({ length: 12 }, async () => {
    for (;;) {
      const i = at++;
      if (i >= cases.length) return;
      const { out } = await run([compiler, "--eval-condition", cases[i].source, "--doc", JSON.stringify(cases[i].doc), ...(cases[i].parent ? ["--parent-doc", JSON.stringify(cases[i].parent)] : [])]);
      try { const answer = JSON.parse(out); native[i] = answer.error ? "error" : answer.result; } catch (_) { native[i] = "no answer"; }
    }
  }));

  const disagreements = [];
  const tally = { agree: 0, both_error: 0, disagree: 0 };
  for (let i = 0; i < cases.length; i++) {
    if (native[i] === donor[i]) { tally.agree++; if (donor[i] === "error") tally.both_error++; }
    else { tally.disagree++; if (disagreements.length < 400) disagreements.push({ ...cases[i], donor: donor[i], native: native[i], plan: sources.get(cases[i].source).plan }); }
  }
  const bySource = new Set(disagreements.map(row => row.source));
  const report = { browser: version, application: app, donor_helpers_loaded: Boolean(helpers), conditions: sources.size, withheld_needing_estate_bindings: withheld, records: cases.length, ...tally,
    conditions_disagreeing: bySource.size, disagreements: disagreements.slice(0, 60),
    cannot_infer: ["agreement on generated records -> agreement on every record", "the browser dialect agreeing -> the server-side Python rules agreeing; those are a different language", "a condition withheld here -> a condition that disagrees"] };
  if (jsonOut) writeFileSync(jsonOut, JSON.stringify(report, null, 1));
  console.log(`${version}: ${sources.size} conditions, ${cases.length} records, ${tally.agree} agree (${tally.both_error} both refuse), ${tally.disagree} disagree across ${bySource.size} conditions; ${withheld} withheld`);
  for (const row of disagreements.slice(0, 12)) console.log(`  ${row.source}  doc=${JSON.stringify(row.doc)}${row.parent ? " parent=" + JSON.stringify(row.parent) : ""}  donor=${row.donor} native=${row.native}`);
  process.exit(tally.disagree ? 1 : 0);
} finally { rmSync(work, { recursive: true, force: true }); }
