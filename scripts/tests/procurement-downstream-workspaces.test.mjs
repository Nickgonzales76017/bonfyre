import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {workbookPackage} from '../../components/BonfyreServiceSite/workbook-package.mjs';
import {shipmentWorkspaceMaterial,shipmentWorkspaceSetup} from '../materialize_shipment_workspace.mjs';
import {projectFinanceWorkspaceSetup} from '../publish_experience_setups.mjs';
import {verifyProcurementOrderIntentRow,downstreamFalseFlags} from '../../integrations/browser/procurement-order-intent-handoff.mjs';
import {readWorkspaceFile} from '../workspace_material.mjs';
const intent={schema:'bonfyre.procurement.order-intent.v1',id:'intent-test',status:'prepared',authority_eligible:true,currency:'USD',known_total_cents:21000,source:{allocation_id:'allocation-test',allocation_revision:2,approval_id:'approval-test',approval_revision:3,decision_id:'decision-test'},order_lots:[{quote_id:'quote-test',known_total_cents:21000,supplier:{name:'Supplier Test'}}],...Object.fromEntries(downstreamFalseFlags.map(key=>[key,false]))};
test('finance and shipment packages expose the exact procurement order-intent continuation',async()=>{
 const root=mkdtempSync(join(tmpdir(),'procurement-downstream-'));
 try{
  const finance=await workbookPackage({packageRoot:root}),shipment=await shipmentWorkspaceMaterial({root});
  expect((await readWorkspaceFile(root,finance.id,'CONTINUATIONS.tsv')).toString()).toContain('procurement-finance\tProcurement finance handoff\tfinance/procurement/\tprocurement-order-intent:');
  expect((await readWorkspaceFile(root,shipment.id,'CONTINUATIONS.tsv')).toString()).toContain('procurement-shipment\tProcurement shipment handoff\tshipments/procurement/\tprocurement-order-intent:');
  expect(projectFinanceWorkspaceSetup(finance.id).some(r=>r.value?.fields==='procurement-order-intent:')).toBe(true);
  expect(shipmentWorkspaceSetup(shipment.id).some(r=>r.value?.fields==='procurement-order-intent:')).toBe(true);
 }finally{rmSync(root,{recursive:true,force:true});}
});
test('read-only downstream verifier requires exact prepared no-effect order intent',()=>{
 const key='procurement-order-intent:'+intent.id,row={key,revision:4,value:intent};expect(verifyProcurementOrderIntentRow(row,key)).toBe(row);
 expect(()=>verifyProcurementOrderIntentRow({...row,value:{...intent,shipped:true}},key)).toThrow('already claims an effect');
 expect(()=>verifyProcurementOrderIntentRow({...row,key:'finance-workbook:x'},'finance-workbook:x')).toThrow();
 expect(()=>verifyProcurementOrderIntentRow({...row,value:{...intent,schema:'wrong'}},key)).toThrow();
});
