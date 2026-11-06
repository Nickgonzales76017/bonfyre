import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Database} from 'bun:sqlite';
import {compileHierarchy} from '../tools/compile_channel_hierarchy.mjs';

test('bounded root/family windows preserve exact readers, origins and hidden-tail counts',()=>{
  const dir=mkdtempSync(join(tmpdir(),'bonfyre-hierarchy-'));
  try{
    const db=new Database(join(dir,'frontier.db'));
    db.exec(`CREATE TABLE root(root_id TEXT PRIMARY KEY,host TEXT,name TEXT,identity TEXT);
      CREATE TABLE root_origin(root_id TEXT,label TEXT,listings INTEGER);
      CREATE TABLE family(family_id TEXT PRIMARY KEY,root_id TEXT,protocol TEXT);
      CREATE TABLE channel(channel_id TEXT PRIMARY KEY,family_id TEXT,root_id TEXT,address TEXT,exact_address TEXT,material TEXT,protocol TEXT,state TEXT,
        truth_level TEXT,virtual INTEGER,census_source TEXT,credential TEXT,rights TEXT,discovered_from TEXT,identity TEXT,first_seen TEXT,last_contributed TEXT,contributions INTEGER);
      CREATE TABLE channel_origin(channel_id TEXT,label TEXT);
      CREATE TABLE channel_census(channel_id TEXT,census_source TEXT,material TEXT);`);
    db.query('INSERT INTO root VALUES(?,?,?,?)').run('r1','publisher.test','Publisher','Q1');
    const ro=db.query('INSERT INTO root_origin VALUES(?,?,?)'); ro.run('r1','ESTATE',4);ro.run('r1','CATALOGUE',2);
    const fam=db.query('INSERT INTO family VALUES(?,?,?)');fam.run('f-rss','r1','rss');fam.run('f-ics','r1','ics');
    const ch=db.query('INSERT INTO channel VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)');
    for(const row of [
      ['c1','f-rss','https://publisher.test/feed','Main','publication','rss','active','observed',0,'reader-news','CC BY','homepage','2026-09-17',12],
      ['c2','f-rss','https://publisher.test/category/music/feed','Music','publication','rss','admitted','protocol_derived',0,'reader-music','','category:music','',0],
      ['c3','f-ics','https://publisher.test/events.ics','Events','event','ics','active','observed',0,'reader-events','','calendar-link','2026-09-17',5],
      ['c4','f-rss','https://publisher.test/archive/virtual','Archive','publication','rss','active','catalogue_member',1,'','','catalogue','',0],
    ]){const [id,fid,address,title,material,protocol,state,truth,virtual,reader,rights,discovered,last,shown]=row;
      ch.run(id,fid,'r1',address,address,material,protocol,state,truth,virtual,reader,'',rights,discovered,'Q1','2026',last,shown);
    }
    const co=db.query('INSERT INTO channel_origin VALUES(?,?)');for(const [id,label] of [['c1','ESTATE'],['c2','DISCOVERY'],['c3','ESTATE'],['c4','CATALOGUE']])co.run(id,label);
    const cr=db.query('INSERT INTO channel_census VALUES(?,?,?)');cr.run('c1','reader-news','publication');cr.run('c1','reader-news-alt','publication');cr.run('c3','reader-events','event');
    const doc=compileHierarchy(db,{roots:10,channels:2});
    expect(doc.schema).toBe('bonfyre-source-hierarchy.v1');expect(doc.root_count).toBe(1);
    const r=doc.roots[0];expect([r.channel_count,r.family_count]).toEqual([4,2]);
    expect(new Set(r.origins.map(x=>x.label))).toEqual(new Set(['ESTATE','CATALOGUE']));
    const f=Object.fromEntries(r.families.map(x=>[x.protocol,x]));
    expect([f.rss.channel_count,f.rss.visible_channels]).toEqual([3,2]);
    const c=f.rss.channels.find(x=>x.channel_id==='c1');
    expect(new Set(c.readers.map(x=>x.source))).toEqual(new Set(['reader-news','reader-news-alt']));
    expect(c.contributions).toBe(12);expect(c.origins).toContain('ESTATE');
    expect(f.ics.channels[0].material).toBe('event');expect(doc.cannot_infer).toContain('not endorsement');
    expect(compileHierarchy(db,{state:'refused'}).root_count).toBe(0);
    const extraRoot=db.query('INSERT INTO root VALUES(?,?,?,?)');
    for(let i=0;i<510;i++){
      const id=`extra-${i}`;extraRoot.run(id,`${id}.test`,id,'');fam.run(`family-${i}`,id,'rss');
      ch.run(`channel-${i}`,`family-${i}`,id,`https://${id}.test/feed`,`https://${id}.test/feed`,'publication','rss','active','observed',0,'','','','','','2026','',0);
    }
    expect(compileHierarchy(db,{roots:1000,channels:1}).root_count).toBe(511);
    db.close();
  }finally{rmSync(dir,{recursive:true,force:true})}
});
