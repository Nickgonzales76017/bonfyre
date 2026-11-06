import {test,expect} from 'bun:test';
import {recoveredBacklog} from '../../integrations/browser/recovery-backlog.mjs';
const record=(key,group,title,kind,originalKey='')=>({key,revision:1,value:{kind:'research',title,sources:[{id:key,history:{kind,originalKey}}],recovery:{group,inputDigest:key.padEnd(64,'0').slice(0,64)}}});
const installed=(pack,ready=true)=>({pack,ready,binding:'ws-'+'3'.repeat(40),editor_url:'https://aurekai.work/apps/example/'});

test('separates existing owner availability from completion',()=>{
  const report=recoveredBacklog([
    record('finance','skills / finance','Recovered finance','skill','finance/payment'),
    record('controller','controller','Recovered controller','controller','erpnext/accounts/payment_entry')
  ],{installed:[installed('experience.finance-workbooks.workspace')]});
  expect(report.total).toBe(2);
  expect(report.buckets.connect_existing_owner).toBe(1);
  expect(report.buckets.bind_native_owner).toBe(1);
  expect(report.boundary).toContain('not a completed handoff');
});

test('missing installation and missing bridge remain distinct work',()=>{
  const report=recoveredBacklog([
    record('partner','partner-mechanism','Recovered partner mechanism','partner-mechanism','Fyle'),
    record('compute','browser-compute','Recovered browser compute','browser-compute','browser/shard')
  ]);
  expect(report.buckets.publish_or_install_owner).toBe(1);
  expect(report.buckets.build_execution_bridge).toBe(1);
});

test('backlog is ordered by actionable native work before unresolved review',()=>{
  const report=recoveredBacklog([
    record('unknown','unknown','Recovered orbital procedure','historical-workflow','orbital/procedure'),
    record('sources','source-family','Recovered sources','source-family','events')
  ],{installed:[installed('experience.source-library.workspace')]});
  expect(report.rows[0].key).toBe('sources');
  expect(report.rows.at(-1).bucket).toBe('manual_routing_review');
});
