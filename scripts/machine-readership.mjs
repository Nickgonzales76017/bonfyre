#!/usr/bin/env bun
// Access-log observations of declared crawler families, not identity proof.
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

let cachedPolicy;
export function readershipPolicy() {
  if (!cachedPolicy) {
    const cwd = fileURLToPath(new URL('../schemas/control/', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', '.', '-e', 'machineReadership'], { cwd });
    if (result.exitCode !== 0) throw new Error(new TextDecoder().decode(result.stderr).trim());
    cachedPolicy = JSON.parse(new TextDecoder().decode(result.stdout));
  }
  return cachedPolicy;
}

const linePattern = /^(\S+).*?"(\w+) (\S+) [^"]*" (\d{3}) (\S+)(?: "[^"]*")? "([^"]*)"/;
const increment = (map, key) => map.set(key, (map.get(key) || 0) + 1);
const ordered = map => [...map].toSorted(([a, x], [b, y]) => y - x || a.localeCompare(b));

export function analyzeReadership(inputs, policy = readershipPolicy()) {
  const families = policy.families.map(family => ({ ...family, regex: new RegExp(family.pattern, 'i') }));
  const byAgent = new Map(), byFamily = new Map(), statusByAgent = new Map();
  const pathsByFamily = new Map(), pathsByAgent = new Map(), familyByAgent = new Map();
  const discoveryMisses = new Map();
  let requestsRead = 0;
  for (const content of inputs) for (const line of content.split(/\r?\n/)) {
    const parsed = linePattern.exec(line);
    if (!parsed) continue;
    requestsRead++;
    const [, , , url, status, , agent] = parsed;
    const family = families.find(item => item.regex.test(agent));
    if (!family) continue;
    const name = family.regex.exec(agent)[0].toLowerCase(), path = url.split('?', 1)[0];
    increment(byAgent, name); increment(byFamily, family.name);
    familyByAgent.set(name, family.name);
    if (!statusByAgent.has(name)) statusByAgent.set(name, new Map());
    increment(statusByAgent.get(name), status);
    if (!pathsByFamily.has(family.name)) pathsByFamily.set(family.name, new Map());
    increment(pathsByFamily.get(family.name), path);
    if (!pathsByAgent.has(name)) pathsByAgent.set(name, new Map());
    increment(pathsByAgent.get(name), path);
    if (status === '404' && policy.discovery_prefixes.some(prefix => path.startsWith(prefix)))
      increment(discoveryMisses, path);
  }
  return { schema: 'bonfyre.machine-readership.v1', requests_read: requestsRead,
    declared_machine_requests: [...byAgent.values()].reduce((sum, n) => sum + n, 0),
    by_family: ordered(byFamily).map(([family, count]) => ({ family, count })),
    by_agent: ordered(byAgent).map(([agent, count]) => ({ agent, family: familyByAgent.get(agent), count,
      top_path: ordered(pathsByAgent.get(agent))[0]?.[0] ?? '',
      statuses: ordered(statusByAgent.get(agent)).map(([status, n]) => ({ status, count: n })) })),
    discovery_misses: ordered(discoveryMisses).map(([path, count]) => ({ path, count })),
    wanted_paths: ordered(byFamily).map(([family]) => ({ family,
      paths: ordered(pathsByFamily.get(family)).map(([path, count]) => ({ path, count })) })),
    cannot_infer: policy.cannot_infer };
}

if (import.meta.main) {
  const paths = process.argv.slice(2).filter(path => path !== '--json');
  const inputs = [];
  for (const path of paths.length ? paths : ['/var/log/nginx/access.log']) {
    try { inputs.push(readFileSync(path, 'utf8')); }
    catch (error) { console.error(`skipping ${path}: ${error.message}`); }
  }
  const result = analyzeReadership(inputs);
  if (process.argv.includes('--json')) console.log(JSON.stringify(result));
  else {
    console.log(`${result.requests_read} requests read; ${result.declared_machine_requests} from a declared machine family`);
    console.log('BY PURPOSE — declared family');
    for (const row of result.by_family) console.log(`  ${String(row.count).padStart(6)}  ${row.family}`);
    console.log('BY DECLARED AGENT');
    for (const row of result.by_agent.slice(0, 12)) console.log(`  ${String(row.count).padStart(6)}  ${row.agent.padEnd(28)} ` +
      row.statuses.slice(0, 3).map(status => `${status.count}x${status.status}`).join(', '));
    if (result.discovery_misses.length) {
      console.log('DISCOVERY REQUESTS ANSWERED 404');
      for (const row of result.discovery_misses.slice(0, 10))
        console.log(`  ${String(row.count).padStart(6)}  ${row.path}`);
    }
    console.log('MOST-WANTED PATHS PER PURPOSE');
    for (const row of result.wanted_paths)
      console.log(`  ${row.family}: ${row.paths.slice(0, 3).map(item => `${item.path} (${item.count})`).join(', ')}`);
    console.log(result.cannot_infer);
  }
}
