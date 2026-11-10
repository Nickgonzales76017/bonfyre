import { test, expect } from 'bun:test';
import { quoteAccept, get, allProtocols } from '../protocol-library.mjs';
import { ProgramError, lower, affectedPrograms, factDelta } from '../protocol-program.mjs';

const program = () => lower(quoteAccept, 'ex1');
const sorted = set => [...set].sort();

test('role reads what it requires and writes what it produces', () => {
  const seller = program().steps.find(step => step.role === 'seller');
  expect(sorted(seller.reads)).toEqual(['request']);
  expect(sorted(seller.writes)).toEqual(['quote']);
});
test('closure is union over roles', () => {
  expect(sorted(program().readClosure())).toEqual(['acceptance','buyer_identity','capability','quote','request']);
  expect(sorted(program().writeClosure())).toEqual(['acceptance','delivery','quote','request']);
});
test('each parameter has exactly one producer', () => {
  const p = program();
  for (const value of p.writeClosure()) expect(p.producerOf(value)).not.toBeNull();
  expect(p.producerOf('quote')).toBe('seller');
  expect(p.producerOf('nothing-writes-this')).toBeNull();
});
test('unread delta affects nothing', () => {
  const p = program();
  expect(p.affectedBy('weather')).toEqual([]);
  expect(p.invalidates('weather')).toBe(false);
  expect(p.unaffectedBy('weather').length).toBe(p.steps.length);
});
test('delta invalidates reader and dependents', () => {
  expect(program().affectedBy('quote')).toEqual(['deliverer','settlement']);
  expect(program().unaffectedBy('quote')).toEqual(['buyer','seller']);
});
test('upstream role is never invalidated by downstream change', () => {
  expect(program().affectedBy('quote')).not.toContain('buyer');
  expect(program().affectedBy('acceptance')).not.toContain('seller');
});
test('head delta cascades to every role', () => expect(program().affectedBy('capability')).toEqual(['buyer','deliverer','seller','settlement']));
test('tail delta affects only reader', () => {
  expect(program().affectedBy('acceptance')).toEqual(['deliverer']);
  expect(program().unaffectedBy('acceptance')).toEqual(['buyer','seller','settlement']);
});
test('FactDelta carries no global invalidation', () => {
  const delta = factDelta('quote','ex1');
  expect(delta).toEqual({ parameter:'quote', enactment:'ex1', global_invalidation:false });
  expect(delta).not.toHaveProperty('recompute');
});
test('FactDelta must name an address', () => {
  expect(() => factDelta('')).toThrow(ProgramError);
  expect(() => factDelta('')).toThrow('names the address');
});
test('enactments of one contract do not share addressed closure', () => {
  const a = lower(quoteAccept,'ex1'), b = lower(quoteAccept,'ex2');
  expect(sorted(a.readClosure())).toEqual(sorted(b.readClosure()));
  expect(a.enactment).not.toBe(b.enactment);
  expect(Object.keys(affectedPrograms([a,b],'quote')).sort()).toEqual(['ex1','ex2']);
  expect(affectedPrograms([a],'quote')).toEqual({ ex1:['deliverer','settlement'] });
});
test('program requires enactment address', () => {
  expect(() => lower(quoteAccept,'')).toThrow(ProgramError);
  expect(() => lower(quoteAccept,'')).toThrow('would share a closure');
});
test('fan-out across estate is usually empty', () => {
  const programs = Array.from({length:20},(_,i) => lower(quoteAccept,`ex${i}`));
  programs.push(lower(get('economic','escrow'),'e1'));
  expect(affectedPrograms(programs,'hold_confirmation')).toEqual({ e1:['deliverer','releaser'] });
});
test('nonconformant contract cannot be lowered', () => {
  const broken = { name:'broken', given:['request'], keys:[], roles:[
    {name:'buyer', requires:['request'], produces:['order']},
    {name:'shipper', requires:['payment_confirmation'], produces:['shipment']},
  ] };
  expect(() => lower(broken,'ex1')).toThrow(ProgramError);
  expect(() => lower(broken,'ex1')).toThrow('non-conformant');
});
for (const contract of allProtocols()) {
  test(`${contract.family}/${contract.name} lowers to a program`, () => {
    const p = lower(contract,`${contract.name}:1`);
    expect(p.steps.length).toBeGreaterThan(0);
    expect(p.readClosure().size).toBeGreaterThan(0);
    expect(p.writeClosure().size).toBeGreaterThan(0);
    for (const value of p.writeClosure()) expect(p.steps.filter(s => s.writes.has(value)).length).toBe(1);
  });
  test(`${contract.family}/${contract.name} ignores unrelated delta`, () => {
    expect(lower(contract,`${contract.name}:1`).affectedBy('an-address-this-program-never-reads')).toEqual([]);
  });
}
test('written parameter is not a produced effect', () => {
  const p = program();
  expect(p.writeClosure().has('delivery')).toBe(true);
  expect(p.producerOf('delivery')).toBe('deliverer');
  expect(p).not.toHaveProperty('occurred');
});
