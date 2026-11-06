import {test, expect} from 'bun:test';
import {readFileSync} from 'node:fs';
import {plannerFiles} from '../planner-material.mjs';
import {startingPlan} from '../planner-model.mjs';
import {projectRows, copyPlan, comparePlans, comparisonBrief, projectsView} from '../planner-projects.mjs';
const {catalogue} = plannerFiles(JSON.parse(readFileSync('packs/aurekai-work-samples/site/fixture-planner.json')));
const prefix = 'fixture-plan:test:';
test('saved project selection excludes different forms, identities and source generations', () => {
  const plan = startingPlan(catalogue), make = (key, value) => ({key, revision: 1, value: {plan: value}});
  const rows = projectRows([
    make(prefix + plan.id, plan), make('another:' + plan.id, plan),
    make(prefix + 'forged', plan), make(prefix + 'obsolete', {...plan, id: 'obsolete', catalogue: 'old'}),
    {key: prefix + 'latest', value: {id: plan.id}},
  ], prefix, catalogue);
  expect(rows.projects).toHaveLength(1); expect(rows.unavailable).toHaveLength(2);
  expect(rows.projects[0].value.plan).toEqual(plan);
});
test('copy creates an independent plan without changing the original', () => {
  const original = startingPlan(catalogue); original.title = 'A'.repeat(100);
  const copy = copyPlan(original, catalogue);
  expect(copy.id).not.toBe(original.id); expect(copy.title.length).toBeLessThanOrEqual(100);
  copy.items[0].x += 1; expect(copy.items[0].x).not.toBe(original.items[0].x);
  expect(Object.keys(copy)).toEqual(Object.keys(original));
});
test('comparison accounts for repeated fixtures and actual placement problems', () => {
  const a = startingPlan(catalogue), b = copyPlan(a, catalogue), item = a.items.find(i => catalogue.products.find(p => p.id === i.product).dimensions?.depth), p = catalogue.products.find(p => p.id === item.product);
  b.items.push({...item, key: 'additional'});
  const c = comparePlans(a, b, catalogue);
  expect(c.delta).toBe(p.price_cents);
  expect(c.items.find(row => row.product.id === p.id).right).toBe(c.items.find(row => row.product.id === p.id).left + 1);
  expect(c.right.issues.some(issue => issue.includes('overlaps'))).toBe(true);
  expect(c.right.questions.length).toBeGreaterThan(0);
  const brief = comparisonBrief(a, b, catalogue);
  expect(brief).toContain(p.source_url); expect(brief).toContain('PLAN A'); expect(brief).toContain('PLAN B');
  expect(brief).toContain('Freight, tax, availability');
  expect(() => comparePlans(a, {...b, catalogue: 'obsolete'}, catalogue)).toThrow();
});
test('project cards escape user text and preserve receipt distinction', () => {
  const plan = startingPlan(catalogue); plan.title = '<img src=x onerror=alert(1)>';
  const {projects} = projectRows([{key: prefix + plan.id, revision: 1, value: {plan, receipt: 'not-a-receipt'}}], prefix, catalogue);
  const html = projectsView(projects, plan.id);
  expect(html).not.toContain('<img'); expect(html).toContain('&lt;img'); expect(html).not.toContain('Sent to Aurekai');
  expect(projectsView(projects, plan.id, 'absent')).toContain('No projects match');
});
