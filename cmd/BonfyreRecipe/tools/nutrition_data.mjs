#!/usr/bin/env bun
// USDA bulk CSV is an external boundary. Computation stays in Recipe's C ABI.
import {Database} from 'bun:sqlite';
import {createReadStream,existsSync,readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {join} from 'node:path';
const sha=v=>createHash('sha256').update(v).digest('hex');
export async function* csv(path){
 let row=[],field='',quoted=false,pending=false,cr=false,bytes=0;
 for await(const chunk of createReadStream(path,{encoding:'utf8'}))for(const c of chunk){
  if(++bytes>2**31)throw Error('CSV exceeds declared import capacity');
  if(cr){cr=false;if(c==='\n')continue;}
  if(pending){pending=false;if(c==='"'){field+='"';continue;}quoted=false;if(c!==','&&c!=='\r'&&c!=='\n')throw Error('invalid character after CSV quote');}
  if(quoted){if(c==='"')pending=true;else field+=c;}
  else if(c==='"'){if(field)throw Error('quote inside unquoted CSV field');quoted=true;}
  else if(c===','){row.push(field);field='';}
  else if(c==='\r'||c==='\n'){row.push(field);field='';yield row;row=[];cr=c==='\r';}
  else field+=c;
  if(field.length>1024*1024||row.length>512)throw Error('CSV cell/column capacity exceeded');
 }
 if(quoted&&!pending)throw Error('unterminated CSV string');
 if(field||row.length){row.push(field);yield row;}
}
async function* objects(path){let header;for await(const row of csv(path)){if(!header){header=row.map(v=>v.replace(/^\uFEFF/,''));if(new Set(header).size!==header.length)throw Error('duplicate CSV column');continue;}if(row.length===1&&!row[0])continue;if(row.length!==header.length)throw Error('CSV column count mismatch');yield Object.fromEntries(header.map((h,i)=>[h,row[i]]));}}
function number(v,{positive=false,integer=false}={}){if(v===''||v===null||v===undefined)return null;if(!['string','number'].includes(typeof v)||!/^[-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][-+]?[0-9]+)?$/.test(String(v)))throw Error('numeric source value required');const n=Number(v);if(!Number.isFinite(n)||n<0||(positive&&n<=0)||(integer&&!Number.isSafeInteger(n)))throw Error('invalid numeric source value '+JSON.stringify(v)+' positive='+positive+' integer='+integer);return n;}
function schema(db){db.exec(`
 CREATE TABLE IF NOT EXISTS nutrition_dataset(digest TEXT PRIMARY KEY,source_uri TEXT NOT NULL,source_dir TEXT NOT NULL,imported_at TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS nutrition_food(fdc_id INTEGER PRIMARY KEY,description TEXT NOT NULL,data_type TEXT NOT NULL,dataset_digest TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS nutrition_value(fdc_id INTEGER NOT NULL,nutrient_id INTEGER NOT NULL,name TEXT NOT NULL,unit TEXT NOT NULL,amount REAL NOT NULL,loq REAL,PRIMARY KEY(fdc_id,nutrient_id));
 CREATE TABLE IF NOT EXISTS nutrition_portion(id INTEGER PRIMARY KEY,fdc_id INTEGER NOT NULL,amount REAL NOT NULL,gram_weight REAL NOT NULL,description TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS recipe_food_binding(recipe_key TEXT NOT NULL,recipe_digest TEXT NOT NULL,ingredient_id TEXT NOT NULL,unit TEXT NOT NULL,fdc_id INTEGER NOT NULL,grams_per_unit REAL NOT NULL,basis TEXT NOT NULL,basis_ref TEXT NOT NULL,dataset_digest TEXT NOT NULL,PRIMARY KEY(recipe_key,recipe_digest,ingredient_id,unit));
 `);}
export async function importUSDA(dbPath,dir,source){
 if(!existsSync(dbPath))throw Error('use the existing Recipe owner database');
 const u=new URL(source);if(u.protocol!=='https:'||u.hostname!=='fdc.nal.usda.gov'||!u.pathname.startsWith('/fdc-datasets/'))throw Error('USDA source URI required');
 const files=['food.csv','nutrient.csv','food_nutrient.csv','food_portion.csv'];
 const hashes=[];for(const file of files){const h=createHash('sha256');for await(const chunk of createReadStream(join(dir,file)))h.update(chunk);hashes.push([file,h.digest('hex')]);}
 const digest=sha(JSON.stringify(hashes)),db=new Database(dbPath,{readwrite:true}),nutrients=new Map();let foods=0,values=0,portions=0;
 try{db.exec('PRAGMA busy_timeout=5000; BEGIN IMMEDIATE');schema(db);
  const prior=db.query('SELECT digest FROM nutrition_dataset WHERE digest=?').get(digest);if(prior){db.exec('ROLLBACK');return {digest,state:'already_imported'};}
  for await(const r of objects(join(dir,'nutrient.csv'))){const id=number(r.id,{positive:true,integer:true});if(!id||!r.name||!r.unit_name)throw Error('invalid nutrient identity');nutrients.set(id,{name:r.name,unit:r.unit_name.toLowerCase().replace('ug','µg')});}
  const food=db.query('INSERT INTO nutrition_food VALUES(?,?,?,?) ON CONFLICT(fdc_id) DO UPDATE SET description=excluded.description,data_type=excluded.data_type,dataset_digest=excluded.dataset_digest');
  const clearV=db.query('DELETE FROM nutrition_value WHERE fdc_id=?'),clearP=db.query('DELETE FROM nutrition_portion WHERE fdc_id=?');
  const admitted=new Set();
  for await(const r of objects(join(dir,'food.csv'))){const id=number(r.fdc_id,{positive:true,integer:true});if(!id||!r.description||r.data_type!=='sr_legacy_food'||admitted.has(id))throw Error('this importer requires unique SR Legacy edible-weight records');food.run(id,r.description,r.data_type,digest);clearV.run(id);clearP.run(id);admitted.add(id);foods++;}
  const value=db.query('INSERT INTO nutrition_value VALUES(?,?,?,?,?,?)');
  for await(const r of objects(join(dir,'food_nutrient.csv'))){const id=number(r.fdc_id,{positive:true,integer:true}),nid=number(r.nutrient_id,{positive:true,integer:true}),amount=number(r.amount);if(!admitted.has(id)||!nutrients.has(nid))throw Error('orphan nutrient row');if(amount===null)continue;const n=nutrients.get(nid),loq=number(r.loq);value.run(id,nid,n.name,n.unit,amount,loq);values++;}
  const portion=db.query('INSERT INTO nutrition_portion VALUES(?,?,?,?,?)');
  for await(const r of objects(join(dir,'food_portion.csv'))){const id=number(r.fdc_id,{positive:true,integer:true}),a=number(r.amount),g=number(r.gram_weight);if(!admitted.has(id))throw Error('orphan portion');if(a===null||g===null)continue;portion.run(number(r.id,{positive:true,integer:true}),id,a,g,r.portion_description||r.modifier||'');portions++;}
  for(const [file,expected] of hashes){const h=createHash('sha256');for await(const chunk of createReadStream(join(dir,file)))h.update(chunk);if(h.digest('hex')!==expected)throw Error('source file changed during import');}
  db.query('INSERT INTO nutrition_dataset VALUES(?,?,?,?)').run(digest,source,dir,new Date().toISOString());db.exec('COMMIT');
  return {digest,foods,values,portions,source,owner:'Recipe',source_files:hashes};
 }catch(e){try{db.exec('ROLLBACK')}catch{}throw e;}finally{db.close();}
}
export function bindFood({dbPath,packPath,key,ingredient,unit,fdcId,grams,basis,basisRef}){
 if(!['reference_scenario','measured_product'].includes(basis)||!basisRef||basisRef.length>2000||![key,ingredient,unit].every(v=>typeof v==='string'&&v.length>0&&v.length<256))throw Error('explicit quantity and identity basis required');
 const mass=number(grams,{positive:true});if(!mass)throw Error('positive grams per declared unit required');
 const pack=new Database(packPath,{readonly:true}),db=new Database(dbPath,{readwrite:true});
 try{schema(db);const record=pack.query('SELECT digest FROM record WHERE record_key=?').get(key),food=db.query('SELECT dataset_digest FROM nutrition_food WHERE fdc_id=?').get(number(fdcId,{positive:true,integer:true}));if(!record||!food)throw Error('exact retained recipe and food required');
  db.query('INSERT INTO recipe_food_binding(recipe_key,recipe_digest,ingredient_id,unit,fdc_id,grams_per_unit,basis,basis_ref,dataset_digest) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(recipe_key,recipe_digest,ingredient_id,unit) DO UPDATE SET fdc_id=excluded.fdc_id,grams_per_unit=excluded.grams_per_unit,basis=excluded.basis,basis_ref=excluded.basis_ref,dataset_digest=excluded.dataset_digest').run(key,record.digest,ingredient,unit,Number(fdcId),mass,basis,basisRef,food.dataset_digest);
  return {key,recipe_digest:record.digest,ingredient,unit,fdc_id:Number(fdcId),grams_per_unit:mass,basis,basis_ref:basisRef};
 }finally{pack.close();db.close();}
}
if(import.meta.main){try{const [op,...args]=process.argv.slice(2);let r;
 if(op==='import'&&args.length===3)r=await importUSDA(...args);
 else if(op==='bind'&&args.length===9){const [dbPath,packPath,key,ingredient,unit,fdcId,grams,basis,basisRef]=args;r=bindFood({dbPath,packPath,key,ingredient,unit,fdcId,grams,basis,basisRef});}
 else throw Error('nutrition_data.mjs import EXISTING_DB CSV_DIRECTORY USDA_SOURCE_URL | bind EXISTING_DB PACK KEY INGREDIENT UNIT FDC_ID GRAMS_PER_UNIT BASIS BASIS_REFERENCE');
 console.log(JSON.stringify(r));
 }catch(e){console.error(e.message);process.exitCode=1;}}
