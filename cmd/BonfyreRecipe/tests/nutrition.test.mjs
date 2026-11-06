import {test,expect} from 'bun:test';
import {Database} from 'bun:sqlite';
import {mkdtempSync,mkdirSync,readFileSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';
import {importUSDA,bindFood,csv} from '../tools/nutrition_data.mjs';
const root=resolve(dirname(fileURLToPath(import.meta.url)),'..'),cli=join(root,'build/bonfyre-recipe');
const sample=JSON.parse(readFileSync(join(root,'examples/preparation-family.jsonl'),'utf8'));
const source='https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_sr_legacy_food_csv_2018-04.zip';
function run(args,env={}){return spawnSync(cli,args,{encoding:'utf8',timeout:20000,maxBuffer:2000000,env:{...process.env,...env}});}
function ok(args,env={}){const r=run(args,env);expect(r.status).toBe(0);return r.stdout;}
async function fixture(fn){const dir=mkdtempSync(join(tmpdir(),'bf-nutrition-test-')),data=join(dir,'csv'),dbPath=join(dir,'recipe.db'),packPath=join(dir,'material.db'),input=join(dir,'source.jsonl');mkdirSync(data);let db;
 try{ok(['init','--db',dbPath]);writeFileSync(input,JSON.stringify(sample)+'\n');ok(['corpus','factor',input,packPath]);
  writeFileSync(join(data,'food.csv'),'fdc_id,description,data_type\n1,Peanut butter,sr_legacy_food\n2,Syrup,sr_legacy_food\n3,Oats,sr_legacy_food\n4,Sugar,sr_legacy_food\n');
  writeFileSync(join(data,'nutrient.csv'),'id,name,unit_name\n1008,Energy,KCAL\n1003,Protein,G\n');
  writeFileSync(join(data,'food_nutrient.csv'),'fdc_id,nutrient_id,amount,loq\n1,1008,100,\n2,1008,200,\n3,1008,300,\n4,1008,400,\n1,1003,10,\n2,1003,0,\n3,1003,5,\n');
  writeFileSync(join(data,'food_portion.csv'),'id,fdc_id,amount,gram_weight,modifier\n1,1,1,256,cup\n2,2,0,0,unknown\n');
  const imported=await importUSDA(dbPath,data,source);db=new Database(dbPath);
  for(const [ingredient,unit,fdcId,grams] of [['peanut-butter','cup',1,256],['syrup','cup',2,320],['crushed-oats','cup',3,90],['brown-sugar','tsp',4,4]])bindFood({dbPath,packPath,key:sample.recipe_id,ingredient,unit,fdcId,grams,basis:'reference_scenario',basisRef:'Synthetic quantity oracle; not observed USDA values'});
  const report=(copies=1)=>ok(['corpus','nutrition',packPath,sample.recipe_id+'*'+copies,dbPath]);
  await fn({dir,data,dbPath,db,packPath,input,report,imported});
 }finally{db?.close();rmSync(dir,{recursive:true,force:true})}}
function cells(text,id){return text.split('\n').find(line=>line.startsWith(id+'\t')).split('\t').slice(3);}
function energy(text){return cells(text,1008).map(c=>Number(c.split('|')[0]));}
test('native formulation allocates each binder portion, additions, and the unused remainder',()=>fixture(({report})=>{
 const one=report();expect(energy(one)).toEqual([165,165,181,197,181,165,181,197,181,197,50]);
 expect(energy(report(2))).toEqual(energy(one).map(n=>n*2));
 expect(energy(report(16)).at(-1)).toBe(0);
 expect(one).toContain('UNRESOLVED\tMexican vanilla');expect(one).toContain('UNRESOLVED\tCold brew extract');
 expect(cells(one,1008)[1]).toBe('165|3/4');expect(cells(one,1004)[0]).toBe('?|0/3');
 const total=text=>text.split('FORMULATION TOTALS')[1].split('\n').find(v=>v.startsWith('1008\t')).split('\t').slice(3).map(v=>Number(v.split('|')[0]));
 expect(total(one)).toEqual([1810,50]);expect(total(report(2))).toEqual([3620,100]);
 expect(total(report(16))).toEqual([28960,0]);
 const fat=one.split('FORMULATION TOTALS')[1].split('\n').find(v=>v.startsWith('1004\t')).split('\t').slice(3);
 expect(fat.every(v=>v.startsWith('?|0/'))).toBe(true);
 expect(one).toContain('Input composition, not cooked retention or food actually consumed');
}));
test('zero and missing nutrients stay distinct, and positive LOQ is not presented as an exact zero',()=>fixture(({db,report})=>{
 expect(cells(report(),1003)[0]).toBe('4.5|3/3');
 db.exec('UPDATE nutrition_value SET amount=0,loq=0.1 WHERE fdc_id=3 AND nutrient_id=1003');
 expect(cells(report(),1003)[0]).toBe('3|2/3');
}));
test('stale food or recipe bindings become unresolved, not reused silently',()=>fixture(({db,report})=>{
 db.exec("UPDATE recipe_food_binding SET recipe_digest='stale' WHERE ingredient_id='peanut-butter'");
 expect(report()).toContain('UNRESOLVED\tPeanut butter');
 db.exec("UPDATE nutrition_food SET dataset_digest='different' WHERE fdc_id=2");expect(report()).toContain('UNRESOLVED\tSyrup');
}));
test('negative quantity bindings fail before any partial nutritional report',()=>fixture(({db,packPath,dbPath})=>{
 db.exec('UPDATE recipe_food_binding SET grams_per_unit=-1');const r=run(['corpus','nutrition',packPath,sample.recipe_id+'*1',dbPath]);
 expect(r.status).not.toBe(0);expect(r.stdout).toBe('');
}));
test('import is atomic, preserves zero source portions, and unchanged imports are idempotent',()=>fixture(async({db,dbPath,data,imported})=>{
 expect(imported.foods).toBe(4);expect(imported.values).toBe(7);expect(db.query('SELECT amount FROM nutrition_portion WHERE id=2').get().amount).toBe(0);
 expect((await importUSDA(dbPath,data,source)).state).toBe('already_imported');
 writeFileSync(join(data,'food_nutrient.csv'),'fdc_id,nutrient_id,amount\n1,1008,999\n2,1008,-1\n');
 await expect(importUSDA(dbPath,data,source)).rejects.toThrow();expect(db.query('SELECT amount FROM nutrition_value WHERE fdc_id=1 AND nutrient_id=1008').get().amount).toBe(100);
}));
test('complete helper brief consumes the same native calculation without altering retained recipes',()=>fixture(({packPath,dbPath,input,report})=>{
 const before=readFileSync(packPath),brief=ok(['corpus','week',packPath,sample.recipe_id+'*1','brief'],{BONFYRE_NUTRITION_DB:dbPath});
 expect(brief).toContain(report());expect(readFileSync(packPath)).toEqual(before);expect(readFileSync(input,'utf8')).toBe(JSON.stringify(sample)+'\n');
}));
test('CSV preserves quoted commas, embedded newline, escaped quote, Unicode, and final row',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'bf-food-csv-'));try{const p=join(dir,'data.csv');writeFileSync(p,'id,text\r\n1,"café, \"\"quoted\"\"\nline"\r\n2,end');
  const rows=[];for await(const r of csv(p))rows.push(r);expect(rows).toEqual([['id','text'],['1','café, "quoted"\nline'],['2','end']]);
 }finally{rmSync(dir,{recursive:true,force:true})}
});

test('additional nutrient definitions are calculated and changing a binding changes the plan snapshot',()=>fixture(({db,report,packPath,dbPath})=>{
 db.exec("INSERT INTO nutrition_value VALUES(1,1089,'Iron, Fe','mg',2,NULL)");
 expect(cells(report(),1089)[0]).toBe('0.6|1/3');
 const env={BONFYRE_NUTRITION_DB:dbPath},args=['corpus','week',packPath,sample.recipe_id+'*1','brief'];
 const first=ok(args,env);db.exec("UPDATE recipe_food_binding SET grams_per_unit=512 WHERE ingredient_id='peanut-butter'");const second=ok(args,env);
 expect(second.match(/Plan identity: (.+)/)[1]).not.toBe(first.match(/Plan identity: (.+)/)[1]);
 expect(cells(report(),1089)[0]).toBe('1.2|1/3');
}));
