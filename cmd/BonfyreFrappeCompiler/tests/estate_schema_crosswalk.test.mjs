import { test, expect } from 'bun:test';
import { mkdtempSync, mkdirSync, rmSync, readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { Database } from 'bun:sqlite';

const repo=resolve(import.meta.dir,'../../..');
const tool=join(repo,'cmd/BonfyreFrappeCompiler/tools/estate_schema_crosswalk.mjs');
test('full Frappe-native donor schemas index reproducibly with local sample billing store',()=>{
  const tmp=mkdtempSync(join(tmpdir(),'bonfyre-estate-schema-'));
  try{
    const estate=join(tmp,'estate'),out=join(tmp,'index');mkdirSync(estate,{recursive:true});
    const cms=new Database(join(estate,'cms.db'),{create:true});
    cms.exec('CREATE TABLE frappe_erpnext_sales_invoice(customer TEXT, grand_total REAL, outstanding_amount REAL, posting_date TEXT, due_date TEXT);');
    cms.close();
    const proc=Bun.spawnSync([process.execPath,tool,'--repo',repo,'--estate',estate,'--out',out,
      '--refresh-compiler'],{cwd:repo,stdout:'pipe',stderr:'pipe',timeout:90_000,maxBuffer:2e6});
    expect(proc.exitCode).toBe(0);
    const report=JSON.parse(readFileSync(join(out,'coverage-and-gaps.json'),'utf8'));
    expect(report.donor.apps).toHaveLength(9);
    expect(report.donor.doctypes).toBeGreaterThan(1100);
    expect(report.donor.fields).toBeGreaterThan(16000);
    expect(report.donor.declared_relationships).toBeGreaterThan(3000);
    expect(report.donor.apps.every(x=>x.field_mismatch===0)).toBe(true);
    expect(report.donor.fields_with_missing_names).toBe(4);
    expect(report.crosswalk.safe_to_execute_count).toBe(0);
    expect(report.crosswalk.semantic_hypotheses).toBe(9);
    expect(report.crosswalk.source_physical_schema_missing).toEqual([
      'JRSSv1 PostgreSQL','AiLtd_Loss ai_carol MySQL'
    ]);
    const idx=new Database(join(out,'schema-crosswalk.sqlite'),{readonly:true,create:false});
    try{
      expect(idx.query('SELECT COUNT(*) n FROM donor_field').get().n).toBe(report.donor.fields);
      expect(idx.query("SELECT COUNT(*) n FROM donor_field WHERE field=''").get().n).toBe(4);
      expect(idx.query("SELECT COUNT(*) n FROM donor_field WHERE family='currency_decimal_semantics'").get().n).toBeGreaterThan(100);
      const exact=idx.query("SELECT classification,warning FROM proposed_crosswalk WHERE donor_doc='erpnext:Sales Invoice' AND donor_field='customer' LIMIT 1").get();
      expect(exact.classification).toBe('exact_field_name_same_doc');
      const money=idx.query("SELECT classification,warning FROM proposed_crosswalk WHERE donor_doc='erpnext:Sales Invoice' AND donor_field='grand_total' LIMIT 1").get();
      expect(money.warning).toBe('currency_precision_mismatch');
      expect(money.classification).toBe('type_review_required');
      expect(idx.query("SELECT COUNT(*) n FROM physical_column WHERE store='active_estate:cms'").get().n).toBe(5);
    }finally{idx.close();}
    const anchor=readFileSync(join(out,'semantic-crosswalk-hypotheses.jsonl'),'utf8').trim().split('\n').map(JSON.parse);
    expect(anchor.some(x=>x.institution==='JRSS'&&x.concept==='Payment allocation'&&x.erpnext==='Payment Entry Reference')).toBe(true);
    expect(anchor.every(x=>x.authorized_to_execute===false)).toBe(true);
    expect(readFileSync(join(out,'recall-schema-candidates.jsonl'),'utf8').trim().split('\n').length).toBe(report.donor.doctypes);
  }finally{rmSync(tmp,{recursive:true,force:true});}
},90000);
