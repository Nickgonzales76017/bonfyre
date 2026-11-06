import { test, expect } from 'bun:test';
import { readFileSync } from 'node:fs';
import { compileSpec, expectedFoundries, SpecError } from './compile_foundry_behavior.mjs';

const spec = JSON.parse(readFileSync(new URL('../../../schemas/control/foundry-behavior-specs.json', import.meta.url), 'utf8'));
test('fourteen Foundries lower only to seven exact owner-bound contracts', () => {
  const result = compileSpec(spec);
  expect(result.foundries.length).toBe(14);
  expect(new Set(result.foundries.map(x => x.foundry))).toEqual(expectedFoundries);
  const contracts = new Map(result.contracts.map(x => [`${x.applies_to_kind}:${x.birth_operation}`, x]));
  expect(new Set(contracts.keys())).toEqual(new Set(['project:Project.support', 'project:Project.join', 'institution:Institution.connect', 'governance_proposal:GovernanceProposal.vote', 'community_body:Community.join', 'publication_occurrence:Occurrence.follow', 'household_opportunity:HouseholdOpportunity.save']));
  expect(contracts.get('project:Project.support')).toMatchObject({ submit_to: '/govern/support', owner_subject_param: 'id', owner_subject_value: 'subject_identifier', return_template: '/s/project/{subject_identifier}', birth_mode: 'INVOKE' });
  expect(contracts.get('institution:Institution.connect')).toMatchObject({ submit_to: '/govern/connect', owner_subject_param: 'institution' });
  expect(contracts.get('governance_proposal:GovernanceProposal.vote')).toMatchObject({ submit_to: '/govern/vote', human_fields: 'choice', return_template: '' });
  expect(contracts.get('publication_occurrence:Occurrence.follow')).toMatchObject({ submit_to: '/govern/occurrence-follow', return_template: '/s/publication_occurrence/{subject_identifier}' });
  expect(contracts.get('household_opportunity:HouseholdOpportunity.save').submit_to).toBe('/govern/opportunity-save');
  expect(new Set(result.unresolved.map(x => x.operation))).toEqual(new Set(['Project.propose', 'Project.vote', 'Project.settle', 'Institution.claim', 'Institution.adopt', 'GovernanceProposal.decide', 'Community.publish', 'Program.run', 'Occurrence.host', 'Occurrence.publish', 'HouseholdOpportunity.request', 'Provider.offer', 'PlaceAsset.observe', 'Actor.publish']));
  expect(new Set(result.projections.map(x => x.operation))).toEqual(new Set(['Project.viewNeeds', 'Project.viewFunding', 'Program.viewRequirements']));
});
test('a remote owner or fabricated return route is refused', () => {
  const bad = structuredClone(spec);
  bad.families[0].behaviors[0].owner_path = 'https://not-an-owner.example';
  expect(() => compileSpec(bad)).toThrow(SpecError);
  const badReturn = structuredClone(spec);
  const support = badReturn.families.flatMap(x => x.behaviors).find(x => x.operation === 'Project.support');
  support.return_template = '/s/project/other';
  expect(() => compileSpec(badReturn)).toThrow(SpecError);
});
