import { test, expect } from 'bun:test';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { resolve, basename } from 'node:path';
import { liveSurfaceEnabled, surface } from './live-surface.mjs';

const repo = resolve(import.meta.dir, '../../..');
const surfaces = resolve(repo, 'site/matrix/surfaces');
const extents = new Set(['chip', 'card', 'section', 'page', 'workspace']);
const actingExtents = new Set(['page', 'workspace']);
const declarations = readdirSync(surfaces).filter(name => name.endsWith('.json'))
  .map(name => [name.slice(0, -5), JSON.parse(readFileSync(resolve(surfaces, name), 'utf8'))])
  .filter(([, doc]) => doc.version === 'bonfyre-surface-pattern-v1');

test('surface patterns are declared', () => {
  expect(declarations.length).toBeGreaterThan(0);
});

for (const [name, declaration] of declarations) {
  test(`${name} reads exactly one owner`, () => {
    expect(declaration.owner).toBeTruthy();
    expect(['type', 'organ']).toContain(declaration.owner.kind);
    expect(declaration.owner.name).toBeTruthy();
    expect(declaration).not.toHaveProperty('reads_type');
  });

  test(`${name} declares an extent from the authored vocabulary`, () => {
    expect(extents.has(declaration.extent)).toBe(true);
  });

  test(`${name} extent constrains its action layer`, () => {
    const mayAct = actingExtents.has(declaration.extent);
    if (!mayAct) expect(declaration.carries_action_layer).toBe(false);
    if (!declaration.carries_action_layer || !mayAct) expect(declaration.action_layer || []).toHaveLength(0);
  });

  test(`${name} declares authority for any action`, () => {
    if (!declaration.action_layer?.length) return;
    expect(declaration.requires_authority).toBeTruthy();
    expect(declaration.requires_authority.length).toBeGreaterThan(20);
  });

  test(`${name} page renders only declared actions`, () => {
    const page = readFileSync(resolve(surfaces, `${name}.html`), 'utf8');
    for (const verb of declaration.action_layer || []) expect(page).toContain(verb);
  });

  test(`${name} lineage can be checked against its donor`, () => {
    const lineage = declaration.lineage;
    expect(lineage).toBeDefined();
    if (lineage.project == null) {
      expect(lineage.note).toBeTruthy();
      return;
    }
    expect(lineage.files).toBeTruthy();
    if (!lineage.licence_text) return;
    const directory = resolve(repo, lineage.licence_text);
    expect(statSync(directory).isDirectory()).toBe(true);
    expect(lineage.commit).toBeTruthy();
    expect(readFileSync(resolve(directory, 'COMMIT'), 'utf8').trim()).toBe(lineage.commit);
  });

  test(`${name} loads its declared donor asset`, () => {
    const lineage = declaration.lineage;
    if (!lineage.licence_text) return;
    const key = basename(lineage.licence_text.replace(/\/$/, ''));
    const page = readFileSync(resolve(surfaces, `${name}.html`), 'utf8');
    expect(page.includes(`donor-${key}.`) || key === 'maplibre').toBe(true);
  });
}

const live = test.skipIf(!liveSurfaceEnabled);

live('catalogue reports extent and action-layer flag for every pattern', async () => {
  const { status, body } = await surface('/_surface/');
  expect(status).toBe(200);
  expect(body.surfaces.length).toBeGreaterThan(0);
  for (const entry of body.surfaces) {
    expect(entry).toHaveProperty('extent');
    expect(entry).toHaveProperty('carries_action_layer');
  }
});

live('catalogue refuses action layer below page extent', async () => {
  const { status, body } = await surface('/_surface/');
  expect(status).toBe(200);
  for (const entry of body.surfaces) {
    if (!['chip', 'card', 'section'].includes(entry.extent)) continue;
    expect(entry.carries_action_layer).toBe(false);
    expect(entry.effect_class).toBeFalsy();
  }
});

live('catalogue reports lineage', async () => {
  const { status, body } = await surface('/_surface/');
  expect(status).toBe(200);
  expect(body.surfaces.some(entry => 'lineage' in entry)).toBe(true);
});
