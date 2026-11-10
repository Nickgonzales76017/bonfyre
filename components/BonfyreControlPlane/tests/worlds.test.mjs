import { test, expect } from 'bun:test';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { liveSurfaceEnabled, surface, surfaceHtml } from './live-surface.mjs';

const repo = resolve(import.meta.dir, '../../..');
const surfaces = resolve(repo, 'site/matrix/surfaces');
const reader = readFileSync(resolve(surfaces, 'world.html'), 'utf8');
const kit = readFileSync(resolve(surfaces, '_kit.js'), 'utf8');
const main = [resolve(repo, 'cmd/BonfyreWebAddress/src/main.c'), ...readdirSync(resolve(repo, 'cmd/BonfyreWebAddress/src')).filter(n => /\.(c|h)$/.test(n) && n !== 'main.c').sort().map(n => resolve(repo, 'cmd/BonfyreWebAddress/src', n))].map(p => readFileSync(p, 'utf8')).join('\n');
const spec = JSON.parse(readFileSync(resolve(repo, 'cmd/BonfyreCMS/content-types/world.json'), 'utf8'));
const live = test.skipIf(!liveSurfaceEnabled);

const cached = new Map();
async function recordsAt(path) {
  if (!cached.has(path)) cached.set(path, surface(path).then(({ status, body }) => {
    if (status !== 200 || !Array.isArray(body.records) || body.records.length === 0) {
      throw new Error(`${path}: expected a nonempty published surface, got ${status}`);
    }
    return body.records;
  }));
  return cached.get(path);
}
async function worlds() { return recordsAt('/_surface/worlds'); }
async function census() {
  return new Map((await recordsAt('/_surface/internet-census')).map(row => [row.source, row]));
}
async function templates() { return recordsAt('/_surface/presentation-templates'); }
const names = text => text.split(',').map(s => s.trim()).filter(Boolean);

for (const field of ['world', 'title', 'character', 'lede', 'sources', 'cannot_infer']) {
  live(`every world carries ${field}`, async () => {
    for (const world of await worlds()) expect(world[field]).toBeTruthy();
  });
}

test('world type requires temporary status and refusal', () => {
  for (const field of ['status', 'character', 'cannot_infer']) {
    expect(spec.attributes[field].required).toBe(true);
  }
});

live('world slugs are unique and well formed', async () => {
  const slugs = (await worlds()).map(world => world.world);
  expect(new Set(slugs).size).toBe(slugs.length);
  for (const slug of slugs) expect(slug).toMatch(/^[a-z0-9-]+$/);
});

live('characters share one document and branch within it', async () => {
  const files = new Set(readdirSync(surfaces));
  for (const world of await worlds()) {
    expect(files.has(`${world.character}.html`)).toBe(false);
    expect(files.has(`world-${world.character}.html`)).toBe(false);
    expect(reader).toContain(`w.character === '${world.character}'`);
    expect(reader).toContain(`body.${world.character}{`);
  }
});

live('no character is the silent default', async () => {
  expect((await worlds()).length).toBeGreaterThan(0);
  const chain = reader.slice(reader.indexOf('the characters'), reader.indexOf('if (!page.querySelector'));
  expect(chain).not.toMatch(/\n  else \{/);
});

live('characters have distinct visual grounds', async () => {
  const grounds = [];
  for (const world of await worlds()) {
    const block = reader.match(new RegExp(`body\\.${world.character}\\{([^}]*)`))?.[1];
    expect(block).toBeTruthy();
    const ground = block.match(/--w-ground:\s*(#[0-9a-fA-F]{3,8})/)?.[1];
    expect(ground).toBeTruthy();
    grounds.push(ground.toLowerCase());
  }
  expect(new Set(grounds).size).toBe(grounds.length);
});

live('characters and worlds have no routes of their own', async () => {
  for (const world of await worlds()) {
    expect(main).not.toContain(`"/${world.character}"`);
    expect(main).not.toContain(`"/w/${world.world}"`);
  }
  expect(main).toContain('!strncmp(target, "/w/", 3)');
});

live('world sources resolve to observed census rows with extraction contracts', async () => {
  const rows = await worlds();
  const sources = await census();
  for (const world of rows) {
    const named = names(world.sources);
    expect(named.every(source => sources.has(source))).toBe(true);
    const walked = named.filter(source => sources.get(source).state === 'observed');
    expect(walked.length).toBeGreaterThan(0);
    for (const source of walked) {
      const row = sources.get(source);
      expect(row.twin).toBeTruthy();
      if (!(row.extract || '').startsWith('rss:')) expect(row.title_field).toBeTruthy();
    }
  }
});

test('one census interpreter lives in the kit, not copies in pages', () => {
  expect(kit).toContain('K.readSource = async (row');
  for (const name of readdirSync(surfaces).filter(name => name.endsWith('.html') && !name.startsWith('_'))) {
    const page = readFileSync(resolve(surfaces, name), 'utf8');
    if (!page.includes('K.readSource') && !page.includes('/foreign/body?twin=')) continue;
    const body = page.split('</head>').slice(1).join('</head>');
    expect(body.split('K.readSource = ').length - 1).toBeLessThanOrEqual(1);
    expect(body).not.toContain('async function readSource');
  }
});

live('reader names no source or host beyond font hosts', async () => {
  for (const source of (await census()).keys()) expect(reader).not.toContain(source);
  const hosts = [...reader.matchAll(/https?:\/\/([a-z0-9.-]+)/g)].map(match => match[1]);
  expect(hosts.every(host => ['fonts.googleapis.com', 'fonts.gstatic.com'].includes(host))).toBe(true);
});

for (const field of ['row.extract', 'row.title_field', 'row.image_field', 'row.image_base',
  'row.twin', 'row.link_field', 'row.sub_field']) {
  test(`interpreter reads ${field}`, () => expect(kit).toContain(field));
}

live('world grammars resolve and compose more than one grammar', async () => {
  const block = kit.match(/K\.VIEW_ROUTE\s*=\s*\{(.*?)\}/s)?.[1];
  expect(block).toBeTruthy();
  const known = new Set([...block.matchAll(/(\w+)\s*:/g)].map(match => match[1]));
  for (const world of await worlds()) {
    const grammars = names(world.grammars || '');
    expect(grammars.length).toBeGreaterThanOrEqual(2);
    expect(grammars.every(grammar => known.has(grammar))).toBe(true);
  }
});

test('grammar chips link through the kit', () => expect(reader).toContain('K.VIEW_ROUTE[g'));

live('world limits and ledes speak to strangers', async () => {
  const bannedLimit = ['surfacepattern', 'closure', 'extent', 'projection', 'addressable',
    'foreigntwin', 'renderer'];
  const bannedLede = ['surface', 'pattern', 'census', 'extent', 'closure', 'subject'];
  for (const world of await worlds()) {
    const limit = world.cannot_infer.toLowerCase();
    expect(world.cannot_infer.length).toBeGreaterThan(40);
    for (const word of bannedLimit) expect(limit).not.toContain(word);
    expect(limit).not.toMatch(/\borgan\b/);
    for (const word of bannedLede) expect(world.lede.toLowerCase()).not.toContain(word);
  }
});

live('shelf and worlds share one document', async () => {
  const shelf = await surfaceHtml('/w');
  expect(shelf.status).toBe(200);
  expect(shelf.body.toLowerCase()).toMatch(/<html|<!doctype/);
  for (const world of await worlds()) {
    const page = await surfaceHtml(`/w/${world.world}`);
    expect(page.status).toBe(200);
    expect(page.body).toBe(shelf.body);
  }
});

live('unpublished world is declined without invention', async () => {
  const answer = await surfaceHtml('/w/there-is-no-such-world');
  expect(answer.status).toBe(200);
  expect(answer.body.includes('K.empty(') || answer.body.includes('empty(')).toBe(true);
  expect(reader).toContain('A world by that name');
});

live('shelf shows character-specific covers', async () => {
  expect(reader).toContain("'cover c-' + w.character");
  for (const world of await worlds()) {
    const block = reader.match(new RegExp(`\\.shelf \\.c-${world.character}\\{([^}]*)`))?.[1];
    expect(block).toContain('background:');
  }
});

live('worlds are reachable from ordinary navigation', async () => {
  const nav = await surfaceHtml('/protocols');
  expect(nav.status).toBe(200);
  expect(nav.body).toContain('href="/w"');
});

test('shelf is a list and not itself a world', () => {
  expect(reader).toContain('/_surface/worlds');
  expect(reader).toMatch(/if\s*\(!\s*slug|slug\s*\?|!name/);
});

live('template owns presentation and no state', async () => {
  for (const template of await templates()) {
    const text = JSON.stringify(template).toLowerCase();
    for (const word of ['internet_source', 'world', 'subject', 'sqlite', 'bonfyre-',
      '/cms/', '/studio/', 'surface_pattern']) expect(text).not.toContain(word);
  }
});

for (const field of ['template', 'family', 'reference_source', 'reference_name',
  'reference_url', 'layout_tokens', 'cannot_infer']) {
  live(`every template carries ${field}`, async () => {
    for (const template of await templates()) expect(template[field]).toBeTruthy();
  });
}

live('templates carry provenance and complete reader-applicable tokens', async () => {
  const seen = new Set();
  const roles = ['ground', 'ink', 'display', 'accent', 'muted', 'rule', 'panel'];
  for (const template of await templates()) {
    expect(template.reference_url.startsWith('http')).toBe(true);
    expect(template.reference_source).toBeTruthy();
    const tokens = typeof template.layout_tokens === 'string'
      ? JSON.parse(template.layout_tokens) : template.layout_tokens;
    expect(tokens && typeof tokens === 'object').toBe(true);
    for (const role of roles) expect(tokens).toHaveProperty(role);
    expect(tokens.face_display).toBeTruthy();
    expect(tokens.face_body).toBeTruthy();
    expect(tokens.font_href.startsWith('https://fonts.googleapis.com/')).toBe(true);
    const key = [tokens.ground, tokens.accent, tokens.face_display].join('\0');
    expect(seen.has(key)).toBe(false);
    seen.add(key);
  }
});

live('address-selected template overrides authored template', async () => {
  expect((await worlds()).length).toBeGreaterThan(0);
  expect(kit).toContain("URLSearchParams(location.search).get('template')");
  expect(reader).toContain('asked || w.template');
});

test('character is fallback when template cannot apply', () => {
  const body = kit.slice(kit.indexOf('K.dress = async ('), kit.indexOf('K.askedTemplate'));
  for (const guard of ['if (!name) return null;', 'if (!t) return null;',
    "if (!tok || typeof tok !== 'object') return null;", 'catch (_) { tok = null; }']) {
    expect(body).toContain(guard);
  }
});

test('template only sets declared presentation roles', () => {
  expect(kit).toContain('K.ROLES = [');
  expect(kit).toContain('for (const k of K.ROLES)');
});

test('one dresser serves every surface', () => {
  for (const name of readdirSync(surfaces).filter(name => name.endsWith('.html') && !name.startsWith('_'))) {
    const body = readFileSync(resolve(surfaces, name), 'utf8').split('</head>').slice(1).join('</head>');
    expect(body).not.toContain('async function dress(');
    expect(body.split('K.dress = ').length - 1).toBeLessThanOrEqual(1);
  }
});

live('authored templates resolve and characters still own their palettes', async () => {
  const known = new Set((await templates()).map(template => template.template));
  for (const world of await worlds()) {
    if (world.template) expect(known.has(world.template)).toBe(true);
    const block = reader.match(new RegExp(`body\\.${world.character}\\{([^}]*)`))?.[1];
    expect(block).toContain('--w-ground:');
  }
});
