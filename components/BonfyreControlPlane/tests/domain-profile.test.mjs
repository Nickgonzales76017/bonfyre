import { test, expect } from 'bun:test';
import { DomainProfile, ProfileError, parcelProfile, WITHHELD } from '../domain-profile.mjs';
import { InformationPolicy } from '../information-policy.mjs';

const parcel = {
  account:'00000051', address:'117 E BELKNAP ST', city:'026', postal:' ',
  owner:'FORT WORTH CITY OF', year_built:0, living_area:0.0, lot_sqft:5000.0,
  land_value:450000.0, total_value:450000.0,
  owner_mailing_address:'200 TEXAS ST, FORT WORTH TX',
};
const carriers = ['ogc','reso','mismo'];
function policy() {
  const p = new InformationPolicy('Parcel');
  for (const field of ['account','address','city','postal','year_built','living_area',
    'lot_sqft','land_value','total_value','owner']) p.disclose(field, { to:['public','market','institution'] });
  p.disclose('owner_mailing_address', { to:['self','institution'],
    reason:'a mailing address is contactable and is not released with the assessment roll' });
  return p;
}
const profile = () => parcelProfile(policy());

test('same row renders in three industry vocabularies', () => {
  const p = profile();
  const [ogc, reso, mismo] = carriers.map(c => p.render(parcel,c).fields);
  expect(ogc.account).toBe('00000051');
  expect(reso.ListingKey).toBe('00000051');
  expect(mismo.account).toBe('00000051');
  expect(ogc.total_value).toBe(450000);
  expect(reso.TaxAssessedValue).toBe(450000);
  expect(mismo.total_value).toBe(450000);
});
test('carriers cannot disagree on shared fact', () => expect(profile().disagreements(parcel,carriers)).toEqual([]));
test('agreement holds field by field', () => {
  const agreement = profile().semanticAgreement(parcel,carriers);
  expect(agreement.get('account')).toEqual(new Set(['00000051']));
  expect(agreement.get('total_value')).toEqual(new Set([450000]));
  expect(agreement.get('owner')).toEqual(new Set(['FORT WORTH CITY OF']));
});
test('parcel with distinct land and total values is detected by projection', () => {
  const p = profile();
  const row = { ...parcel, account:'00000086', total_value:675000 };
  expect(p.disagreements(row,carriers)).toEqual([]);
  const mapping = p.projections.get('reso').mapping;
  delete mapping.total_value;
  mapping.land_value = 'TaxAssessedValue';
  const agreement = p.semanticAgreement(row,carriers);
  expect(agreement.get('land_value')).toEqual(new Set([450000]));
  expect(agreement.get('total_value')).toEqual(new Set([675000]));
  expect(p.render(row,'reso').fields.TaxAssessedValue).toBe(450000);
});
test('carrier cannot project undeclared field', () => {
  expect(() => profile().projectInto('acme','Acme Feed',{ zestimate:'Zestimate' })).toThrow(ProfileError);
  expect(() => profile().projectInto('acme','Acme Feed',{ zestimate:'Zestimate' })).toThrow('does not declare');
});
test('grammar may omit field with no place for it', () => {
  const p = profile(), mismo = p.render(parcel,'mismo').fields;
  expect(mismo).not.toHaveProperty('owner');
  expect(mismo).not.toHaveProperty('OwnerName');
  expect(p.render(parcel,'reso').fields.OwnerName).toBe('FORT WORTH CITY OF');
});
test('withheld mailing address never leaks', () => {
  const p = profile();
  for (const carrier of carriers) expect(JSON.stringify(p.render(parcel,carrier))).not.toContain('200 TEXAS ST');
});
test('policy applies before every carrier projection', () => {
  const pol = policy().disclose('owner', { to:['institution'], reason:'owner name withheld from this audience' });
  const p = parcelProfile(pol);
  const reso = p.render(parcel,'reso','public').fields;
  const ogc = p.render(parcel,'ogc','public').fields;
  expect(reso.OwnerName[WITHHELD]).toBe(true);
  expect(ogc.owner[WITHHELD]).toBe(true);
  expect(p.withheldIn(p.render(parcel,'reso','public'))).toEqual(['OwnerName']);
  expect(p.render(parcel,'reso','institution').fields.OwnerName).toBe('FORT WORTH CITY OF');
});
test('withheld marker is not a dropped key', () => {
  const p = parcelProfile(policy().disclose('owner', { to:['institution'], reason:'withheld' }));
  const reso = p.render(parcel,'reso','public').fields;
  expect(reso).toHaveProperty('OwnerName');
  expect(reso.OwnerName[WITHHELD]).toBe(true);
});
test('profile does not claim standards conformance', () => {
  const r = profile().render(parcel,'reso');
  expect(r.standard).toBe('RESO Web API');
  expect(r).not.toHaveProperty('conformant');
  expect(r).not.toHaveProperty('certified');
});
test('one semantic profile serves three carriers', () => {
  const p = profile();
  expect(p.projections.size).toBe(3);
  for (const field of ['account','land_value','total_value']) expect(p.semanticFields.has(field)).toBe(true);
  for (const carrier of carriers) for (const field of Object.keys(p.projections.get(carrier).mapping)) expect(p.semanticFields.has(field)).toBe(true);
});
test('evidence requirement is declared by profile', () => expect(profile().evidenceRequirements.has('assessment_roll_receipt')).toBe(true));
test('unknown carrier is refused', () => expect(() => profile().render(parcel,'zillow')).toThrow(ProfileError));
