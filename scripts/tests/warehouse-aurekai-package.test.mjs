import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {materializeSourceLibrary,sourceLibrarySetup,sourceLibraryPack}
  from '../materialize_source_library.mjs';
import {readWorkspaceFile,verifyWorkspacePackage}
  from '../workspace_material.mjs';
import {warehouseStudioPage} from '../warehouse_aurekai_page.mjs';
import {workspaceNavigation,workspaceNavigationCSS}
  from '../workspace_continuation.mjs';

test('Aurekai source library retains a review-only warehouse studio in its existing pack',async()=>{
 const root=mkdtempSync(join(tmpdir(),'warehouse-aurekai-package-'));
 try{
  const result=await materializeSourceLibrary({root});
  const verified=await verifyWorkspacePackage(root,result.id);
  expect(verified.files).toBeGreaterThan(15);
  const html=(await readWorkspaceFile(root,result.id,'sources/warehouse/index.html')).toString('utf8');
  const source=(await readWorkspaceFile(root,result.id,'sources/index.html')).toString('utf8');
  const studio=(await readWorkspaceFile(root,result.id,'sources/warehouse-studio.mjs')).toString('utf8');
  const bridge=(await readWorkspaceFile(root,result.id,'bridge/warehouse-portfolio.mjs')).toString('utf8');
  expect(html).toContain('data-warehouse-studio');
  expect(html).toContain('bonfyre-workspace-store');
  expect(html).toContain('bonfyre-workspace-context-selector');
  expect(html).toContain('../warehouse-studio.mjs');
  expect(source).toContain('./warehouse/');
  expect(html).toContain('data-repository-cut-studio');
  expect(html).toContain('repository-cut-studio.mjs');
  const cut=await readWorkspaceFile(root,result.id,'bridge/repository-cuts.mjs');
  const cutUI=await readWorkspaceFile(root,result.id,'sources/repository-cut-studio.mjs');
  const cutIntake=await readWorkspaceFile(root,result.id,'bridge/repository-cut-intake.mjs');
  const packIntake=await readWorkspaceFile(root,result.id,'bridge/repository-pack-intake.mjs');
  expect(cut.toString()).toContain('readRepositoryCutReceipt');
  expect(cutUI.toString()).toContain('readRepositoryCutCollection');
  expect(cutUI.toString()).toContain('Review retention in Source Library');
  expect(cutIntake.toString()).toContain('saveReviewedRepositoryCut');
  expect(cutUI.toString()).toContain('Prepare PackGraph demand draft');
  expect(packIntake.toString()).toContain('repositoryCutPackDemand');
  expect(studio).toContain('warehouseInstalledTools');
  expect(studio).toContain('bonfyre:account-changed');
  expect(studio).not.toContain('localStorage');
  expect(bridge).toContain('programServiceContinuations');
  const policy=(await readWorkspaceFile(root,result.id,'bridge/program-service-policy.mjs')).toString('utf8');
  expect(policy).toContain('experience.finance-workbooks.workspace');
  const continuation=(await readWorkspaceFile(root,result.id,'CONTINUATIONS.tsv')).toString('utf8');
  expect(continuation).toContain('source-material:');
  expect(continuation).not.toContain('warehouse-analysis:');
  const receipts=sourceLibrarySetup(result.id);
  expect(receipts.some(r=>r.type==='pack_artifact'&&r.key.pack===sourceLibraryPack)).toBe(true);
  expect(receipts.some(r=>r.type==='capability_program'&&r.key.program_id==='feed.publication.source-library')).toBe(true);
  // A review screen is not a new account installation or a new source material owner.
  expect(receipts.filter(r=>r.type==='pack_demand')).toHaveLength(2);
 }finally{rmSync(root,{recursive:true,force:true});}
});
test('warehouse template refuses to materialize without exact workspace navigation',()=>{
 expect(()=>warehouseStudioPage({navigation:'<nav>fake</nav>',navigationCSS:''})).toThrow();
 expect(warehouseStudioPage({navigation:workspaceNavigation,navigationCSS:workspaceNavigationCSS}))
   .toContain('Warehouse intelligence · Aurekai');
});

test('Aurekai ProgramFabric CUE produces the exact browser continuation policy',async()=>{
 const {readProgramServicePolicy,renderProgramServicePolicy}
   =await import('../project-program-service-policy.mjs');
 const {readFileSync}=await import('node:fs');
 const {resolve}=await import('node:path');
 const declared=readProgramServicePolicy();
 expect(readFileSync(resolve(import.meta.dir,'../../integrations/browser/program-service-policy.mjs'),'utf8'))
   .toBe(renderProgramServicePolicy(declared));
 expect(declared.continuations.programs.some(row=>row.pack==='experience.source-programs.workspace'))
   .toBe(true);
 expect(declared.continuations.campaigns.some(row=>row.pack==='experience.source-campaign.workspace'))
   .toBe(true);
 expect(declared.continuations.connections).toHaveLength(1);
 expect(declared.continuations.connections[0].kind).toBe('account');
});
