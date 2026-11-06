import { test, expect } from 'bun:test';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const repo = resolve(import.meta.dir, '../../..');
const home = readFileSync(resolve(repo, 'site/matrix/surfaces/home.html'), 'utf8');
const main = readFileSync(resolve(repo, 'cmd/BonfyreWebAddress/src/main.c'), 'utf8');

test('public root is authored and search uses native surface', () => {
  expect(home).toContain('Prepare route');
  expect(home).toContain('id="estate-try"');
  expect(home).toContain('action="/_surface/external-estate"');
  expect(home).toContain('hx-target="#estate-results"');
  expect(home).not.toContain('/external-estate/v1/');
});

test('home leads with one native bounded operation', () => {
  for (const token of ['action="/_surface/intake"', 'method="get"', 'name="url"',
    'This prepares the address for a person to review.', 'does not fetch, publish or claim it.']) {
    expect(home).toContain(token);
  }
  expect(home.toLowerCase()).not.toContain('method="post"');
});

test('home does not recreate an estate catalogue', () => {
  const source = home.toLowerCase();
  for (const absent of ['things it can do', 'capabilities in the book', "estate's own parts",
    'data-sec="machine"', 'data-sec="retention"', 'night-live', 'internet-census',
    'k.readsource', 'foreign/body']) {
    expect(source).not.toContain(absent);
  }
});

test('three next moves are existing owner doors', () => {
  for (const href of ['href="/discover"', 'href="/market"', 'href="/studio/"']) {
    expect(home).toContain(href);
  }
  expect(home).toContain('Offers with a stated price, from whoever is making them.');
  expect(home).toContain('once you are signed in');
});

test('territory identity comes from native presentation slots', () => {
  for (const slot of ['territory', 'eyebrow', 'title', 'sub', 'orbit-label', 'look-title', 'look-copy']) {
    expect(home).toContain(`id="${slot}"`);
  }
  const owner = readFileSync(resolve(repo, 'cmd/BonfyreWebAddress/src/territory_home.c'), 'utf8');
  expect(owner).toContain('bf_apply_territory_home_presentation');
  expect(owner).toContain('"<body class=\\"%s\\">"');
});

test('session indicator uses native HTTP-only session', () => {
  expect(home).toContain('href="/login" id="session-link"');
  expect(home).not.toContain('localStorage');
  expect(home).not.toContain('bonfyre_token');
});

test('public weather is a bounded observation read', () => {
  expect(home).toContain('hx-get="/_surface/home-weather"');
  expect(home).toContain('hx-trigger="load"');
  expect(home).toContain('Checking for public weather sources.');
  expect(home.toLowerCase()).not.toContain('forecast');
});

test('machine root contract remains owned by WebAddress', () => {
  const start = main.indexOf('static int serve_authored_page');
  expect(start).toBeGreaterThanOrEqual(0);
  const end = main.indexOf('\n    db = cms_open_ro();', start);
  expect(end).toBeGreaterThan(start);
  const fn = main.slice(start, end);
  expect(fn).toContain('surface_file(probe, sizeof(probe), root, "home")');
  expect(fn).toContain('!wants_json(accept)');
});

test('page is responsive without hiding its only operation', () => {
  expect(home).toContain('@media(max-width:760px)');
  expect(home).toContain('class="line url-form"');
  expect(home).toContain('.line button{width:100%}');
  expect(home).toContain('width:100%');
});
