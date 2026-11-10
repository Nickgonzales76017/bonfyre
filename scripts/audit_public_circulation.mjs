#!/usr/bin/env bun
// An exported item link is circulation evidence, never readership or endorsement.
import { Database } from 'bun:sqlite';
import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { homedir } from 'node:os';

const args = { estate: join(homedir(), '.bonfyre', 'estate-local'), dist: '', 'program-header': 'cmd/BonfyreWebAddress/generated/native_page_composition.h', json: false };
for (let i = 2; i < process.argv.length; i++) {
  const key = process.argv[i].slice(2);
  if (!process.argv[i].startsWith('--') || !Object.hasOwn(args, key)) throw Error(`unknown option: ${process.argv[i]}`);
  if (key === 'json') args.json = true;
  else args[key] = process.argv[++i];
}
if (!args.dist || !existsSync(args.dist) || !statSync(args.dist).isDirectory() || !existsSync(args['program-header'])) throw Error('--dist and --program-header must exist');
const frontier = join(args.estate, 'frontier.db');
if (!existsSync(frontier)) throw Error('frontier.db is required under --estate');
const db = new Database(frontier, { readonly: true });
const admitted = new Set(db.query("SELECT DISTINCT census_source FROM channel WHERE state IN ('admitted','active') AND COALESCE(census_source,'')<>''").all().map(row => row.census_source));
db.close();
const declared = [];
for (const line of readFileSync(args['program-header'], 'utf8').split(/\r?\n/)) {
  const match = line.match(/^\s*\{"(\/[^\"]*)",/);
  if (match && !declared.includes(match[1])) declared.push(match[1]);
}
if (!declared.includes('/stage')) declared.push('/stage');
function htmlFiles(dir) {
  if (!existsSync(dir) || !statSync(dir).isDirectory()) return [];
  return readdirSync(dir, { withFileTypes: true }).flatMap(entry => {
    const path = join(dir, entry.name);
    return entry.isDirectory() ? htmlFiles(path) : entry.isFile() && entry.name.endsWith('.html') ? [path] : [];
  }).sort();
}
function pagesOf(route) {
  const dist = resolve(args.dist);
  if (route === '/') return existsSync(join(dist, 'index.html')) ? [join(dist, 'index.html')] : [];
  const suffix = route.replace(/^\/+/, '');
  const flat = join(dist, `${suffix}.html`);
  return [...(existsSync(flat) && statSync(flat).isFile() ? [flat] : []), ...htmlFiles(join(dist, suffix))];
}
const routes = {}, all = new Set();
for (const route of declared) {
  const pages = pagesOf(route), linked = new Set();
  for (const page of pages) {
    const html = readFileSync(page, 'utf8');
    for (const match of html.matchAll(/href=["']\/o\/([^/"'?#]+)\//g)) linked.add(match[1]);
  }
  for (const source of linked) all.add(source);
  const count = [...linked].filter(x => admitted.has(x)).length;
  routes[route] = { pages: pages.length, item_links: linked.size, admitted_sources: count,
    admission_coverage_percent: admitted.size ? Math.round(10000 * count / admitted.size) / 100 : 0 };
}
const visible = [...all].filter(x => admitted.has(x)).length;
const missing = [...admitted].filter(x => !all.has(x)).sort();
const result = {
  schema: 'bonfyre-public-circulation.v1', admitted_sources: admitted.size,
  publicly_circulating_admitted_sources: visible,
  admission_coverage_percent: admitted.size ? Math.round(10000 * visible / admitted.size) / 100 : 0,
  linked_but_not_currently_admitted: all.size - visible,
  not_yet_circulating_count: missing.length, not_yet_circulating_sample: missing.slice(0, 20), routes,
  cannot_infer: 'a source linked from an exported public program is not an endorsement, traffic measurement, quality ranking, current availability claim, or a relationship with that publisher',
};
console.log(JSON.stringify(result, null, args.json ? undefined : 2));
