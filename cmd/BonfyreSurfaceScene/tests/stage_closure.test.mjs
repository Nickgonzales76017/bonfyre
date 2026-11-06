import {test,expect} from 'bun:test';
import {mkdtempSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Database} from 'bun:sqlite';
import {compileStage,relationRows} from '../tools/compile_stage_closure.mjs';

test('event stage composes typed media, relations and descriptive vGPU candidates',()=>{
  const dir=mkdtempSync(join(tmpdir(),'bonfyre-stage-closure-'));
  try{
    const rows=[];
    for(const [kind,n,source] of [['event',4,'calendar'],['place',40,'places'],['photo',16,'commons'],['audio',14,'archive'],['video',12,'peertube'],['post',80,'forums'],['story',20,'news']])
      for(let i=0;i<n;i++)rows.push({source,key:`${kind}-${i}`,kind,title:`${kind} ${i}`,ident:['event','place'].includes(kind)?`Q${100+i}`:''});
    const dbPath=join(dir,'atlas.db'),db=new Database(dbPath);
    db.exec('CREATE TABLE relation(src TEXT,relation TEXT,dst TEXT,evidence TEXT)');
    const put=db.query('INSERT INTO relation VALUES(?,?,?,?)');
    put.run('event-0','happens_at','qid:Q16557','linker:fixture');
    put.run('calendar:event-1','mentions','qid:Q123','linker:fixture'); db.close();
    const keys=rows.flatMap(x=>[x.key,`${x.source}:${x.key}`]);
    const closure=compileStage(rows,{subject:'event:test',frontierRootCount:137,relations:relationRows(dbPath,keys)});
    expect(closure.stageable).toBe(true);
    expect(closure.typed_counts.events).toBe(4);
    expect(new Set(closure.grammars)).toEqual(new Set(['stage','agenda','map','gallery','listen','watch','channels','community','context']));
    expect(closure.frontier_root_count).toBe(137);
    expect(closure.compute_plan.filter(x=>x.backend_candidate==='vgpu').map(x=>x.job)).toEqual(['spatial-neighborhood','media-similarity','conversation-cluster','scene-score']);
    expect(closure.relations.length).toBe(2);
    expect(closure.cannot_infer.join(' ')).toContain('gpu candidate');
    const absent=compileStage(rows.filter(x=>x.kind!=='event'),{subject:'event:missing'});
    expect(absent.stageable).toBe(false);
    expect(absent.grammars).not.toContain('stage');
    expect(absent.grammars).toContain('watch');
    expect(absent.grammars).toContain('listen');
  }finally{rmSync(dir,{recursive:true,force:true})}
});
