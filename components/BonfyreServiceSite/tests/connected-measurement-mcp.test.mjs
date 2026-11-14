import {test,expect} from 'bun:test';
import {mkdtempSync,readFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {buildConnectedMeasurementApp} from '../connected-measurement-mcp-build.mjs';
import {readConnectedApp,CONNECTED_APP_URI,CONNECTED_APP_RESOURCE} from '../../../integrations/mcp/connected_measurement_app_resource.mjs';
test('connected performance is a real bundled MCP app, not an external dashboard or static demo',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'connected-performance-test-'));
 try{
  const file=join(dir,'view.html'),built=await buildConnectedMeasurementApp(file);
  const html=readFileSync(file,'utf8');
  expect(built.bytes).toBe(Buffer.byteLength(html));
  expect(built.bytes).toBeLessThan(512*1024);
  expect(html).toContain('See what your connections are doing.');
  expect(html).toContain('Source details');
  expect(html).toContain('Manage connections');
  expect(html).toContain('Partial reporting periods');
  expect(html).toContain('bonfyre.account.measurement.portfolio.v1');
  expect(html).toContain('self_connected_measurements');
  expect(html).toContain('bonfyre_admin_connected_measurements');
  expect(html).not.toContain('<!-- CONNECTED_MEASUREMENT_APP_SCRIPT -->');
  expect(html).not.toContain('SUPABASE_SERVICE_ROLE_KEY');
  expect(html).not.toContain('locations/17255637652550187256');
  expect(CONNECTED_APP_RESOURCE._meta.ui.csp.connectDomains).toEqual([]);
  const reply=await readConnectedApp(CONNECTED_APP_URI,file);
  expect(reply.contents).toHaveLength(1);
  expect(reply.contents[0].text).toBe(html);
  expect(reply.contents[0].mimeType).toBe('text/html;profile=mcp-app');
  expect(await readConnectedApp('ui://aurekai/foreign.html',file)).toBeNull();
 }finally{rmSync(dir,{recursive:true,force:true});}
});
