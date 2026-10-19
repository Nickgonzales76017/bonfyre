// Publication of these source cards requires this build's actual browser results.
import {readFile,writeFile,mkdir,copyFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
const report=JSON.parse(await readFile('instrument-evidence/results.json','utf8'));
const required=['saved unfamiliar process reopens from existing shared store','source finding continues into actual product story map','exported file contains actual renamed column','schema fits mobile viewport without whole-page overflow','no uncaught JavaScript exceptions'];
if(report.errors?.length||report.passed!==report.checks?.length||report.passed<37||required.some(s=>!report.checks.includes(s)))throw Error('A complete passing instrument browser run is required.');
const manifest=await readFile('site/instruments/SOURCE.tsv','utf8');
for(const row of manifest.trim().split('\n').slice(1)){const [source,projection,sha,size]=row.split('\t'),bytes=await readFile('site/instruments/'+projection);if(bytes.length!==Number(size)||createHash('sha256').update(bytes).digest('hex')!==sha)throw Error('Tested projection changed: '+projection);if(!source.startsWith('sql.js@')){const original=await readFile(source);if(!original.equals(bytes))throw Error('Source changed after projection: '+source);}}
const path='site/index.html',before=await readFile(path,'utf8'),start='<!-- BEGIN tested instruments -->',end='<!-- END tested instruments -->';
if((before.includes(start))!==(before.includes(end)))throw Error('Incomplete instrument directory boundary.');
const old=before.replace(/<!-- BEGIN tested instruments -->[\s\S]*?<!-- END tested instruments -->\s*/,'');
const existing=[...old.matchAll(/<a\b[^>]*\bdata-demo(?:\s|>)[^>]*>[\s\S]*?<\/a>/g)].map(m=>m[0]);
if(existing.length!==13)throw Error('Review changed application directory membership before adding instruments.');
const cards=[
 ['traces','Process trace explorer','Observed work · interactive','Inspect real event logs, replay recorded paths and review overlapping work without fabricated process timings.','process-desktop.png','Aurekai displaying an imported event log with original records, held rows and observed case timings.'],
 ['research','Research synthesis editor','Source-based authoring · interactive','Select exact passages, write and review findings, compare themes and carry a sourced opportunity into product work.','research-desktop.png','Aurekai research editor showing an analyst-authored finding and its exact source passage.'],
 ['schema','Schema migration studio','SQLite · interactive','Inspect a real database, rehearse schema changes in SQLite and export the tested working copy.','schema-desktop.png','Aurekai SQLite studio showing actual relational tables, row data and a refused unique-index change.'],
 ['product','Product story map','Product planning · interactive','Arrange outcomes across releases, edit estimates, inspect prerequisites and review a capacity-aware proposal.','product-desktop.png','Aurekai story map with dependency-aware release allocations and source-linked research.']
];
await mkdir('site/instruments/shots',{recursive:true});
const base='https://nickgonzales76017.github.io/bonfyre/instruments/';
const escape=s=>s.replaceAll('&','&amp;').replaceAll('"','&quot;').replaceAll('<','&lt;');
const links=[];
for(let i=0;i<cards.length;i++){const [id,title,kind,detail,shot,alt]=cards[i],bytes=await readFile('instrument-evidence/'+shot);if(bytes.subarray(0,8).toString('hex')!=='89504e470d0a1a0a')throw Error('Actual PNG capture is missing: '+shot);await copyFile('instrument-evidence/'+shot,'site/instruments/shots/'+shot);links.push(`<a class="item" data-demo data-instrument="${id}" data-kind="${escape(kind)}" data-shot="${base}shots/${shot}" data-shot-alt="${escape(alt)}" href="${base}?tool=${id}"><span class="index">${String(existing.length+i+1).padStart(2,'0')}</span><strong>${escape(title)}</strong><span class="detail">${escape(detail)}</span><span class="arrow" aria-hidden="true">↗</span></a>`);}
const anchor=existing.at(-1);if(old.split(anchor).length!==2)throw Error('Ambiguous final existing card.');
const after=old.replace(anchor,anchor+'\n        '+start+'\n        '+links.join('\n        ')+'\n        '+end);
if(existing.some(row=>!after.includes(row))||(after.match(/\bdata-demo data-/g)||[]).length!==17)throw Error('Existing membership changed unexpectedly.');
await writeFile(path,after);
await writeFile('instrument-evidence/DISCOVERY.tsv','entry\turl\n'+cards.map(c=>c[0]+'\t'+base+'?tool='+c[0]).join('\n')+'\n');
console.log('Added four verified instrument entries; all thirteen existing applications retained.');
