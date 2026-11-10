import {validatePlan, assessPlan, money, planBrief} from './planner-model.mjs';
import {escapeHTML as esc} from './workspace-navigation.mjs';

export const plannerViews = ['layout', 'fixtures', 'projects', 'compare'].map((id, i) => ({
  id, label: ['Plan a space', 'Choose fixtures', 'Saved projects', 'Compare plans'][i], available: () => true,
}));

// Project views read the existing plan records. An obsolete source cannot enter a comparison.
export function projectRows(records, prefix, catalogue) {
  const projects = [], unavailable = [];
  for (const row of records) {
    if (!row.key.startsWith(prefix) || row.key === prefix + 'latest') continue;
    try {
      const plan = validatePlan(row.value?.plan, catalogue);
      if (row.key !== prefix + plan.id || !Number.isSafeInteger(row.revision) || row.revision < 1) throw Error('Invalid saved project.');
      projects.push({...row, assessment: assessPlan(plan, catalogue)});
    } catch { unavailable.push(row.key); }
  }
  projects.sort((a, b) => a.value.plan.title.localeCompare(b.value.plan.title) || a.key.localeCompare(b.key));
  return {projects, unavailable};
}

export function copyPlan(plan, catalogue, id = crypto.randomUUID()) {
  validatePlan(plan, catalogue);
  return validatePlan({...structuredClone(plan), id, title: `${plan.title.slice(0, 93)} (copy)`}, catalogue);
}

export function comparePlans(left, right, catalogue) {
  const a = assessPlan(left, catalogue), b = assessPlan(right, catalogue);
  const counts = plan => {
    const values = new Map();
    for (const item of plan.items) values.set(item.product, (values.get(item.product) ?? 0) + 1);
    return values;
  };
  const ac = counts(left), bc = counts(right);
  return {left: a, right: b, delta: b.subtotal - a.subtotal,
    items: catalogue.products.filter(p => ac.has(p.id) || bc.has(p.id)).map(product => ({
      product, left: ac.get(product.id) ?? 0, right: bc.get(product.id) ?? 0,
      delta: ((bc.get(product.id) ?? 0) - (ac.get(product.id) ?? 0)) * product.price_cents,
    })),
  };
}

export function comparisonBrief(left, right, catalogue) {
  const c = comparePlans(left, right, catalogue);
  return [`Fixture plan comparison: ${left.title} / ${right.title}`,
    `${right.title} is ${money(Math.abs(c.delta))} ${c.delta < 0 ? 'less' : 'more'} in listed fixtures.`,
    'Freight, tax, availability and site clearances still need confirmation.', '',
    ...c.items.map(row => `${row.product.label}: ${row.left} / ${row.right} · listed cost difference ${money(row.delta)}`),
    '', 'PLAN A', planBrief(left, catalogue), '', 'PLAN B', planBrief(right, catalogue)].join('\n');
}

export function projectsView(projects, current, query = '') {
  const shown = projects.filter(row => `${row.value.plan.title} ${row.value.plan.notes}`.toLowerCase().includes(query.trim().toLowerCase()));
  return shown.length ? `<div class="fp-project-grid">${shown.map(row => {
    const p = row.value.plan, a = row.assessment;
    return `<article class="fp-project-card"><h3>${esc(p.title)}</h3><p>${p.width} × ${p.depth} ft · ${p.items.length} fixtures</p><strong>${money(a.subtotal)}</strong><p>${a.issues.length ? `${a.issues.length} layout or budget questions` : 'No listed-footprint conflicts'} · ${a.questions.length} supplier questions</p><small>${/^lead:planner:[a-f0-9]{64}$/.test(row.value.receipt ?? '') ? 'Sent to Aurekai for review' : 'Saved in this browser'}${p.id === current ? ' · Current plan' : ''}</small><div class="fp-pair"><button type="button" data-project-open="${esc(p.id)}">Open plan</button><button type="button" data-project-compare="${esc(p.id)}">Compare</button></div></article>`;
  }).join('')}</div>` : `<p class="fp-empty">${projects.length ? 'No projects match your search.' : 'Save your first plan to keep it here. Each space and alternative can have its own plan.'}</p>`;
}

export function comparisonView(left, right, catalogue) {
  const c = comparePlans(left, right, catalogue);
  const summary = (plan, assessment) => `<article class="fp-project-card"><h3>${esc(plan.title)}</h3><p>${plan.width} × ${plan.depth} ft · ${plan.items.length} fixtures</p><strong>${money(assessment.subtotal)}</strong><p>${plan.budget === null ? 'Budget not set' : `${money(Math.abs(plan.budget - assessment.subtotal))} ${assessment.subtotal > plan.budget ? 'over' : 'under'} fixture budget`}</p><button type="button" data-project-open="${esc(plan.id)}">Open this plan</button><h4>Layout and budget</h4>${assessment.issues.length ? `<ul>${assessment.issues.map(v => `<li>${esc(v)}</li>`).join('')}</ul>` : '<p>No conflicts in the complete listed footprints. Site and aisle checks remain.</p>'}<details><summary>${assessment.questions.length} supplier questions</summary><ul>${assessment.questions.map(v => `<li>${esc(v)}</li>`).join('')}</ul></details></article>`;
  return `<p class="fp-comparison-delta">${esc(right.title)}: <strong>${money(Math.abs(c.delta))} ${c.delta < 0 ? 'less' : 'more'}</strong> in listed fixtures.</p><div class="fp-comparison-plans">${summary(left, c.left)}${summary(right, c.right)}</div><div class="fp-table-wrap"><table><caption>Fixture quantities and cost difference</caption><thead><tr><th scope="col">Fixture</th><th scope="col">${esc(left.title)}</th><th scope="col">${esc(right.title)}</th><th scope="col">Difference</th></tr></thead><tbody>${c.items.map(row => `<tr><th scope="row"><a href="${esc(row.product.source_url)}" target="_blank" rel="noopener noreferrer">${esc(row.product.label)}</a><small>${esc(row.product.option)}</small></th><td>${row.left}</td><td>${row.right}</td><td>${money(row.delta)}</td></tr>`).join('')}</tbody></table></div><p>Listed prices exclude freight, tax and custom work. Confirm dimensions and access before ordering.</p>`;
}
