import {test,expect} from 'bun:test';
import {mkdtempSync,mkdirSync,copyFileSync,rmSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {Database} from 'bun:sqlite';

const root=resolve(import.meta.dir,'../../..');
const binary=process.env.BONFYRE_CMS_BIN??join(root,'cmd/BonfyreCMS/bonfyre-cms');
test('publisher placement is retained as review-only metadata without publishing or copying site pages',()=>{
 const home=mkdtempSync(join(tmpdir(),'publisher-placement-')),db=join(home,'cms.db'),schemas=join(home,'schemas');
 mkdirSync(schemas);copyFileSync(join(root,'cmd/BonfyreCMS/content-types/site_publication_placement.json'),join(schemas,'site_publication_placement.json'));
 const run=(...args)=>Bun.spawnSync([binary,...args,'--db',db,'--schemas',schemas],{stdout:'pipe',stderr:'pipe',timeout:12000});
 try{
  let result=run('schema','migrate');expect(result.exitCode).toBe(0);
  const base={placement_id:'fixture-publication-to-profile',origin_site:'aurekai-journal',origin_route:'/blog/example/',origin_page_digest:'a'.repeat(64),destination_site:'person-nick-work',destination_route:'/blog/example/',publisher_subject:'institution:aurekai-journal',destination_subject:'person:fixture',review_request_ref:'review:fixture-one',cannot_infer:'No cross-owner authority, publication consent or public route follows from this proposal.',status:'draft'};
  result=run('entry','create','site_publication_placement',JSON.stringify(base),'--ns','placement-fixture');expect(result.exitCode).toBe(0);
  const sql=new Database(db,{readonly:true});try{
   const row=sql.query("SELECT * FROM site_publication_placement WHERE namespace='placement-fixture'").get();expect(row.origin_site).toBe('aurekai-journal');expect(row.destination_site).toBe('person-nick-work');expect(row.publisher_subject).toBe('institution:aurekai-journal');expect(row.status).toBe('draft');
   expect(sql.query("SELECT name FROM sqlite_master WHERE type='table' AND name='site_page'").get()).toBeNull();
  }finally{sql.close()}
  result=run('entry','create','site_publication_placement',JSON.stringify({...base,placement_id:'fixture-missing-publisher',publisher_subject:undefined}),'--ns','placement-fixture');expect(result.exitCode).not.toBe(0);
 }finally{rmSync(home,{recursive:true,force:true})}
});
