#!/usr/bin/env bun
import { run as closure } from './closure-algebra.mjs';

const money = amount => `$${amount.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
export function decide(state, { ceiling = 10, minLeverage = 1, worldValue = 0, now = new Date().toISOString().replace('.000Z', 'Z') } = {}) {
  const decisions = state.resolver_leverage.map(leverage => {
    const { resolver, class: kind, closure_contribution: contribution, cost_usd: cost, success_probability: probability } = leverage;
    const row = { resolver, class: kind || '', closure_contribution: contribution, cost_usd: cost, success_probability: probability };
    if (contribution < minLeverage) return { ...row, admit: false, because: 'closes nothing measurable; a purchase that removes no condition is not cheap, it is waste' };
    if (leverage.free) return { ...row, admit: false, because: 'free, so there is nothing to authorise; it needs work or a commitment rather than money' };
    if (!leverage.priced) return { ...row, admit: false, because: 'no declared cost, so a purchase would have no ceiling; price it before admitting it' };
    if (cost > ceiling) return { ...row, admit: false, referred: true, because: `${money(cost)} exceeds the ${money(ceiling)} policy ceiling; referred rather than trimmed, because a ceiling that bends is a suggestion` };
    const expected = worldValue ? contribution * probability * worldValue : null;
    if (expected !== null && expected < cost) return { ...row, admit: false, expected_value_unlocked: expected, because: `within the ceiling, but the ${money(expected)} it would unlock is less than the ${money(cost)} it costs` };
    return { ...row, admit: true, expected_value_unlocked: expected, because: `${money(cost)} is within the ${money(ceiling)} ceiling and removes ${contribution} condition(s) at ${Math.round(probability * 100)}% likelihood` };
  });
  const admitted = decisions.filter(row => row.admit);
  return {
    schema: 'bonfyre-closure-spend.v1', domain: state.domain, subject: state.subject,
    decided_at: now, effect_class: 'PURE', money_moved: false,
    policy: { ceiling_usd: ceiling, min_leverage: minLeverage, world_value_usd: worldValue || null },
    decisions, admitted: admitted.map(row => row.resolver),
    admitted_total_usd: Math.round(admitted.reduce((sum, row) => sum + row.cost_usd, 0) * 100) / 100,
    referred_to_a_person: decisions.filter(row => row.referred).map(row => row.resolver),
    cannot_infer: [
      'an admitted spend -> an approved one',
      'an admitted spend -> a payment; nothing here holds a credential or moves money',
      'a closed condition -> a delivered result',
      'a policy ceiling -> a budget',
    ],
  };
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const option = (name, fallback) => args.includes(name) ? args[args.indexOf(name) + 1] : fallback;
  const domain = option('--domain'), subject = option('--subject');
  if (!domain || !subject) { console.error('usage: bun scripts/closure-spend.mjs --domain D --subject S [--ceiling N] [--min-leverage N] [--world-value N] [--json]'); process.exit(2); }
  try {
    const receipt = decide(closure(option('--db', process.env.BONFYRE_CMS_DB || '/var/lib/bonfyre/cms/bonfyre_cms.db'), domain, subject), {
      ceiling: Number(option('--ceiling', 10)), minLeverage: Number(option('--min-leverage', 1)), worldValue: Number(option('--world-value', 0)),
    });
    if (args.includes('--json')) console.log(JSON.stringify(receipt, null, 2));
    else {
      console.log(`  ${domain} / ${subject}`);
      console.log(`  policy: at most ${money(receipt.policy.ceiling_usd)} per resolver, at least ${receipt.policy.min_leverage} condition(s) closed`);
      for (const row of receipt.decisions) console.log(`  ${row.admit ? 'ADMIT' : row.referred ? 'refer' : 'no   '} ${row.resolver} ${row.cost_usd === null ? 'unpriced' : money(row.cost_usd)} ${row.because}`);
      console.log(`  admitted ${money(receipt.admitted_total_usd)} across ${receipt.admitted.length} resolver(s); no money moved`);
    }
  } catch (error) { console.error(`closure-spend: ${error.message}`); process.exit(2); }
}
