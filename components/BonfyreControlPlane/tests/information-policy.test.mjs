import { test, expect } from 'bun:test';
import { AUDIENCES, InformationPolicy, PolicyError, WITHHELD, evidenceVisible } from '../information-policy.mjs';

const parcel = {
  account: '00000051', address: '117 E BELKNAP ST', owner: 'FORT WORTH CITY OF',
  land_value: 450000, total_value: 450000,
  owner_mailing_address: '200 TEXAS ST, FORT WORTH TX',
};
function policy() {
  return new InformationPolicy('Parcel')
    .disclose('account', { to: AUDIENCES })
    .disclose('address', { to: AUDIENCES })
    .disclose('owner', { to: ['public', 'market', 'institution'] })
    .disclose('land_value', { to: ['public', 'market', 'institution'] })
    .disclose('total_value', { to: ['public', 'market', 'institution'] })
    .disclose('owner_mailing_address', { to: ['self', 'institution'],
      reason: 'a mailing address is contactable and is not released with the assessment roll' })
    .floor('owner_parcel_count', { minimumPopulation: 5 });
}

test('undeclared fields and contact details are explicitly withheld without shrinking the record', () => {
  const p = policy();
  expect(p.fieldDecision('assessor_internal_note', 'public')).toEqual([false, 'no disclosure rule declared for this field']);
  const publicParcel = p.project(parcel, 'public');
  const ownParcel = p.project(parcel, 'self');
  expect(publicParcel.account).toBe(parcel.account);
  expect(publicParcel.owner).toBe(parcel.owner);
  expect(publicParcel.owner_mailing_address).toEqual({ [WITHHELD]: true,
    reason: 'a mailing address is contactable and is not released with the assessment roll' });
  expect(p.withheldFields(publicParcel)).toEqual(['owner_mailing_address']);
  expect(Object.keys(publicParcel)).toEqual(Object.keys(parcel));
  expect(Object.keys(ownParcel)).toEqual(Object.keys(parcel));
  expect(ownParcel.owner_mailing_address).toBe(parcel.owner_mailing_address);
});

test('audiences are exact labels, not an inheritance ladder', () => {
  const p = policy();
  expect(p.fieldDecision('land_value', 'market')[0]).toBe(true);
  expect(p.fieldDecision('land_value', 'household')[0]).toBe(false);
  expect(p.fieldDecision('owner_mailing_address', 'self')[0]).toBe(true);
  expect(p.fieldDecision('owner_mailing_address', 'public')[0]).toBe(false);
  expect(p.fieldDecision('owner', 'self')[0]).toBe(false);
  expect(p.fieldDecision('owner', 'public')[0]).toBe(true);
  expect(() => p.fieldDecision('owner', 'everyone')).toThrow(PolicyError);
  expect(() => new InformationPolicy('Parcel').disclose('owner', { to: ['everybody'] })).toThrow(PolicyError);
});

test('aggregate floor withholds values and count without widening constituent disclosure', () => {
  const p = policy();
  const denied = p.releaseAggregate('owner_parcel_count', 2, 2, 'public');
  expect(denied[WITHHELD]).toBe(true);
  expect(denied.reason).toBe('below the disclosure floor of 5');
  expect(denied.reason).not.toContain('2');
  expect(denied).not.toHaveProperty('value');
  const released = p.releaseAggregate('owner_parcel_count', 812, 812, 'public');
  expect(released).toEqual({ statistic: 'owner_parcel_count', value: 812, population: 812 });
  expect(p.fieldDecision('owner_mailing_address', 'public')[0]).toBe(false);
  expect(p.releaseAggregate('median_sale_price', 1, 9999, 'public').reason).toContain('no aggregation floor');
  expect(() => p.floor('owner_parcel_count', { minimumPopulation: 1 })).toThrow('discloses the individual');
});

test('evidence release defaults to nothing and grants exact witness kinds', () => {
  const p = policy();
  expect(evidenceVisible(p, 'assessment_roll_receipt')).toBe(false);
  p.evidenceRelease = new Set(['assessment_roll_receipt']);
  expect(evidenceVisible(p, 'assessment_roll_receipt')).toBe(true);
  expect(evidenceVisible(p, 'owner_identity_proof')).toBe(false);
});

test('an explicit empty audience set denies everyone in both fields and aggregates', () => {
  const p = new InformationPolicy('Sealed').disclose('secret', { to: [] })
    .floor('secret_count', { minimumPopulation: 2, to: [] });
  for (const audience of AUDIENCES) {
    expect(p.fieldDecision('secret', audience)[0]).toBe(false);
    expect(p.aggregateDecision('secret_count', 100, audience)[0]).toBe(false);
  }
});
