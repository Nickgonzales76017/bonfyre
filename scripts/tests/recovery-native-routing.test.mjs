import {test,expect} from 'bun:test';
import {routeRecoveredNative,nativeRecoveryCoverage} from '../../integrations/browser/recovery-native-routing.mjs';
const record=(group,title,kind,originalKey='')=>({kind:'research',title,sources:[{id:'s',history:{kind,originalKey}}],recovery:{group,inputDigest:'b'.repeat(64)}});
const installed=(pack,ready=true)=>({pack,ready,binding:'ws-'+'2'.repeat(40),editor_url:'https://aurekai.work/apps/example/'});

test('Frappe controllers route to their native compiler instead of Research',()=>{
  const route=routeRecoveredNative(record('controller','Recovered · controller · 01','controller','erpnext/manufacturing/work_order'));
  expect(route.state).toBe('native_owner');expect(route.selected.owner).toBe('BonfyreFrappeCompiler');expect(route.selected.owner_type).toBe('native_family');
});

test('source families continue to the installed Source library',()=>{
  const pack='experience.source-library.workspace',route=routeRecoveredNative(record('source-family','Recovered · source family · 01','source-family'),{installed:[installed(pack)]});
  expect(route.state).toBe('owner_available');expect(route.selected.owner).toBe(pack);expect(route.selected.binding).toMatch(/^ws-/);
});

test('institution setup stays with the signed-in PackGraph surface',()=>{
  const route=routeRecoveredNative(record('institution-setup-flow','Recovered · institution setup flow · 01','institution-setup-flow'));
  expect(route.state).toBe('surface_owner');expect(route.selected.owner).toBe('BonfyreWebAddress');expect(route.selected.route).toBe('/packs/setup?context=self');
});

test('partner mechanisms expose installation gaps instead of pretending completion',()=>{
  const route=routeRecoveredNative(record('partner-mechanism','Recovered · partner mechanism · 01','partner-mechanism'));
  expect(route.state).toBe('owner_not_installed');expect(route.selected.owner).toBe('experience.connection-studio.workspace');
});

test('unknown skills fall back to skill instruments while lexical matches remain specific',()=>{
  const analysis='experience.skill-instruments.workspace',finance='experience.finance-workbooks.workspace';
  const generic=routeRecoveredNative(record('skills / specialist','Recovered · specialist rubric','skill','specialist/rubric'),{installed:[installed(analysis)]});
  expect(generic.state).toBe('owner_available');expect(generic.selected.owner).toBe(analysis);
  const specific=routeRecoveredNative(record('skills / finance','Recovered · payment ledger','skill','finance/payment'),{installed:[installed(finance)]});
  expect(specific.state).toBe('owner_available');expect(specific.selected.pack).toBe(finance);
});

test('coverage counts native, surface and workspace owner states separately',()=>{
  const rows=[record('controller','Controller','controller'),record('institution-continuity','Continuity','institution-continuity'),record('source-family','Sources','source-family')];
  const report=nativeRecoveryCoverage(rows,{installed:[installed('experience.source-library.workspace')]});
  expect(report.total).toBe(3);expect(report.states.native_owner).toBe(1);expect(report.states.surface_owner).toBe(1);expect(report.states.owner_available).toBe(1);
});
