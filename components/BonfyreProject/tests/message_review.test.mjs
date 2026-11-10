import {afterEach,expect,test} from 'bun:test';
import {Database} from 'bun:sqlite';
import {createHash} from 'node:crypto';
import {existsSync,mkdtempSync,readFileSync,rmSync,statSync,symlinkSync,writeFileSync} from 'node:fs';
import {tmpdir,homedir} from 'node:os';
import {join,resolve} from 'node:path';
const root=resolve(import.meta.dir,'../../..'), bin=join(root,'components/BonfyreProject/bonfyre-project');
const cms=process.env.BONFYRE_CMS_BINARY||join(homedir(),'.bonfyre/bin/bonfyre-cms');
const schemas=join(root,'cmd/BonfyreCMS/content-types'),dirs=[];
afterEach(()=>{for(const d of dirs.splice(0))rmSync(d,{recursive:true,force:true});});
const sha=s=>createHash('sha256').update(s).digest('hex');
function fixture(){
 const dir=mkdtempSync(join(tmpdir(),'bonfyre-message-review-'));dirs.push(dir);const db=join(dir,'cms.db');
 const exec=args=>{const r=Bun.spawnSync([cms,...args,'--db',db,'--schemas',schemas]);expect(r.exitCode).toBe(0);const s=r.stdout.toString().trim();return s?JSON.parse(s):{};};
 for(const type of ['project','outbound_message'])exec(['schema','apply',join(schemas,type+'.json')]);
 const put=(type,body,ns='sales')=>exec(['entry','create',type,'--ns',ns,JSON.stringify(body)]);
 const project=(id,extra={},ns='sales')=>put('project',{project_id:id,subject_ref:'observed:'+id,title:'Client '+id,pitch:'Need and evidence',stage:'proposal_review',operating_stated:'Trial $250',currency:'USD',opened_at:'2026-10-07T00:00:00Z',cannot_infer:'No revenue',status:'draft',...extra},ns);
 const message=(id,about,extra={},ns='sales')=>put('outbound_message',{message_id:id,about,recipient:'buyer@example.invalid',subject_line:'One agreed trial',body:'Hello.\nA proposal.',state:'withheld',composed_by:'test',composed_at:'2026-10-07T00:00:00Z',would_be_sent_by:'a person',state_reason:'Needs approval',cannot_infer:'Not sent',status:'draft',...extra},ns);
 const run=(...args)=>Bun.spawnSync([bin,'message','review','--db',db,'--ns','sales',...args]);
 return {dir,db,exec,project,message,run};
}
test('review joins real CMS records within the namespace and preserves exact drafts without writing',()=>{
 const f=fixture();f.project('one');f.project('gone',{stage:'source_unavailable'});f.project('orphan',{title:'Foreign namespace secret'},'other');
 const body='A draft with "quotes", <markup>, £ and\n'+ 'full details '.repeat(700);
 f.message('one','one',{body});f.message('gone','gone');f.message('orphan','orphan');f.message('handed','one',{state:'handed_to_person'});f.message('published','one',{status:'published'});
 const before=sha(readFileSync(f.db)),r=f.run();expect(r.exitCode).toBe(0);const result=JSON.parse(r.stdout.toString());
 expect(result).toMatchObject({total:5,active:1,outside_active_batch:4});
 expect(result.source.messages.find(m=>m.message_id==='one').body).toBe(body);
 expect(result.source.messages.find(m=>m.message_id==='orphan').project_title).toBe('');
 expect(result.source.messages.find(m=>m.message_id==='gone').in_active_batch).toBe(false);
 expect(r.stdout.toString()).not.toContain('Foreign namespace secret');
 expect(sha(readFileSync(f.db))).toBe(before);
 expect(JSON.parse(f.run().stdout.toString()).snapshot_sha256).toBe(result.snapshot_sha256);
});
test('snapshot changes with the exact draft and preserves the held state',()=>{
 const f=fixture();f.project('one');const made=f.message('one','one'),id=made.id??made.data.id;
 const first=JSON.parse(f.run().stdout.toString());
 f.exec(['entry','update','outbound_message',String(id),'--ns','sales',JSON.stringify({body:'A revised offer.'})]);
 const second=JSON.parse(f.run().stdout.toString());
 expect(first.snapshot_sha256).not.toBe(second.snapshot_sha256);
 expect(second.source.messages[0].state).toBe('withheld');
 expect(f.run('--approve').exitCode).toBe(2);
});
test('review distinguishes recorded email from web and unresolved routes without claiming delivery readiness',()=>{
 const f=fixture();f.project('one');f.project('past',{stage:'source_unavailable'});
 const cases=[
  ['email','buyer@example.invalid','email_address'],
  ['relay','abc@reply.craigslist.org','email_address'],
  ['cl','https://www.craigslist.org/view/d/example/123','craigslist_post'],
  ['regional','https://losangeles.craigslist.org/reply/lax/cpg/123/__SERVICE_ID__','craigslist_post'],
  ['form','https://forms.gle/example','web_route'],
  ['suffix','https://craigslist.org.evil.invalid/post','web_route'],
  ['substring','https://notcraigslist.org/post','web_route'],
  ['path','https://example.invalid/craigslist.org/post','web_route'],
  ['credentials','https://craigslist.org@evil.invalid/post','unresolved'],
  ['multi','one@example.invalid,two@example.invalid','unresolved'],
  ['empty','','unresolved'],
  ['header','buyer@example.invalid\nBcc: other@example.invalid','unresolved']
 ];
 for(const [id,recipient] of cases)f.message(id,'one',{recipient});
 f.message('old-email','past');
 const before=sha(readFileSync(f.db)),result=JSON.parse(f.run().stdout.toString());
 for(const [id,,route] of cases)expect(result.source.messages.find(m=>m.message_id===id).contact_route).toBe(route);
 expect(result.active_contact_routes).toEqual({email_address:2,craigslist_post:2,web_route:4,unresolved:4});
 const html=f.run('--format','html').stdout.toString();
 expect(html).toContain('Email address recorded; mailbox and contact role still need review.');
 expect(html).toContain('Craigslist route; reply details still need to be resolved.');
 expect(html).toContain('4 other web routes');
 expect(html).not.toContain('Ready to send');
 expect(sha(readFileSync(f.db))).toBe(before);
});
test('research without a message stays visible, isolated and fingerprinted without becoming a draft',()=>{
 const f=fixture();
 const pending=f.project('research',{title:'Research <script>bad()</script>',stage:'research_hold',pitch:'Buyer requires a personal response.',operating_stated:'Portfolio evidence still needed.'});
 f.project('ready');f.message('ready','ready');
 f.project('past');f.message('past','past',{state:'abandoned'});
 f.project('published',{status:'published'});
 f.project('foreign',{title:'Private other namespace'},'other');
 f.message('foreign-reference','research',{},'other');
 const before=sha(readFileSync(f.db)),first=JSON.parse(f.run().stdout.toString());
 expect(first).toMatchObject({total:2,active:1,outside_active_batch:1,projects_without_messages:1});
 expect(first.source.projects_without_messages).toHaveLength(1);
 expect(first.source.projects_without_messages[0]).toMatchObject({project_id:'research',stage:'research_hold',pitch:'Buyer requires a personal response.'});
 expect(JSON.stringify(first)).not.toContain('Private other namespace');
 const html=f.run('--format','html').stdout.toString();
 expect(html).toContain('Projects without a message');
 expect(html).toContain('Research &lt;script&gt;bad()&lt;/script&gt;');
 expect(html).toContain('Portfolio evidence still needed.');
 expect(html).not.toContain('<script>');
 expect(sha(readFileSync(f.db))).toBe(before);
 f.exec(['entry','update','project',String(pending.id??pending.data.id),'--ns','sales',JSON.stringify({pitch:'Need reconfirmed.'})]);
 expect(JSON.parse(f.run().stdout.toString()).snapshot_sha256).not.toBe(first.snapshot_sha256);
 f.message('now-drafted','research');
 const drafted=JSON.parse(f.run().stdout.toString());
 expect(drafted.projects_without_messages).toBe(0);
 expect(drafted.total).toBe(3);expect(drafted.active).toBe(1);
});
test('too many projects without messages cannot silently truncate or replace a report',()=>{
 const f=fixture(),output=join(f.dir,'review.html');writeFileSync(output,'previous report');
 const db=new Database(f.db);
 db.transaction(()=>{for(let i=0;i<1001;i++)db.run("INSERT INTO project(namespace,created_at,updated_at,project_id,subject_ref,title,pitch,stage,opened_at,cannot_infer,status) VALUES('sales','now','now',?,'test','test','test','opportunity_research','now','test','draft')",String(i));})();db.close();
 expect(f.run('--out',output).exitCode).not.toBe(0);
 expect(readFileSync(output,'utf8')).toBe('previous report');
});
test('HTML escapes all stored content, includes full scope and has no action or remote resource',()=>{
 const f=fixture();f.project('one',{title:'<script>run()</script>',pitch:'Need <iframe src="https://bad.invalid">',operating_stated:'$250 & one correction'});
 f.message('one','one',{recipient:'<img src=x onerror=run()>',body:'<script>run()</script>\nExact text.'});
 const output=join(f.dir,'review.html'),r=f.run('--format','html','--out',output);expect(r.exitCode).toBe(0);
 const html=readFileSync(output,'utf8'),json=JSON.parse(f.run().stdout.toString());
 expect(html).toContain('&lt;script&gt;run()&lt;/script&gt;');
 expect(html).toContain('$250 &amp; one correction');
 expect(html).toContain(json.snapshot_sha256);
 expect(html).not.toMatch(/<(script|iframe|img|form|input|button)\b/i);
 expect(statSync(output).mode&0o777).toBe(0o600);
});
test('missing or incomplete data and oversized batches cannot replace an existing report',()=>{
 const f=fixture(),output=join(f.dir,'review.html');writeFileSync(output,'previous report');
 const absent=join(f.dir,'missing.db');
 const missing=Bun.spawnSync([bin,'message','review','--db',absent,'--ns','sales','--out',output]);
 expect(missing.exitCode).not.toBe(0);expect(existsSync(absent)).toBe(false);
 expect(readFileSync(output,'utf8')).toBe('previous report');
 const db=new Database(f.db);
 db.transaction(()=>{for(let i=0;i<1001;i++)db.run("INSERT INTO outbound_message(namespace,created_at,updated_at,message_id,about,recipient,subject_line,body,state,status,composed_by,composed_at,would_be_sent_by,cannot_infer) VALUES('sales','now','now',?,'missing','test','test','test','withheld','draft','test','now','test','test')",String(i));})();
 expect(f.run('--out',output).exitCode).not.toBe(0);expect(readFileSync(output,'utf8')).toBe('previous report');
 db.run('DROP TABLE project');db.close();
 expect(f.run('--out',output).exitCode).not.toBe(0);expect(readFileSync(output,'utf8')).toBe('previous report');
});
test('report refuses to replace the source database through either its path or a symlink',()=>{
 const f=fixture();f.project('one');f.message('one','one');const before=sha(readFileSync(f.db));
 const link=join(f.dir,'db-link');symlinkSync(f.db,link);
 expect(f.run('--out',f.db).exitCode).toBe(2);
 expect(f.run('--out',link).exitCode).toBe(2);
 expect(sha(readFileSync(f.db))).toBe(before);
});
