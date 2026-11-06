// Keep the authored directory as the single source for its carousel.
// Called only after the same build's product/room browser journey has passed.
import {readFile,writeFile,copyFile,mkdir,readdir} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {join,relative} from 'node:path';
const path='site/index.html',before=await readFile(path,'utf8');let html=before;
const base='https://nickgonzales76017.github.io/bonfyre/spaces/';
const changes=[
 {title:'Retail Space Planner',next:'Room Design Studio',href:base+'product-room/',shot:'studio-room-selection.png',kind:'3D room planning · interactive',alt:'Aurekai Room Design Studio showing source-linked lamps, dimension labels and a calculated materials subtotal.',detail:'Place source-linked products in 2D or 3D, inspect dimensions, compare saved alternatives and prepare a review brief.'},
 {title:'Catalogue Workspace',next:'Catalogue &amp; 3D Configuration',href:base+'catalogue/?store=wayfair-reference&amp;workspace=inventory',shot:'studio-product.png',kind:'Product configuration · interactive',alt:'Aurekai product studio showing the retained lamp, model inspection and catalogue-to-room controls.',detail:'Choose an observed option, inspect model parts and finishes, set a quantity and continue in the same saved room plan.'}
];
await mkdir('site/spaces/shots',{recursive:true});
for(const c of changes){
 await copyFile('spaces-evidence/'+c.shot,'site/spaces/shots/'+c.shot);
 const titlePattern=[c.title,c.next].map(s=>s.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')).join('|');
 const re=new RegExp('<a\\b[^>]*data-demo(?:\\s|>)[^>]*><span class="index">(\\d+)</span><strong>(?:'+titlePattern+')</strong>[\\s\\S]*?</a>','g');
 let count=0;html=html.replace(re,(_,index)=>{count++;return `<a class="item" data-demo data-kind="${c.kind}" data-shot="${base}shots/${c.shot}" data-shot-alt="${c.alt}" href="${c.href}"><span class="index">${index}</span><strong>${c.next}</strong><span class="detail">${c.detail}</span><span class="arrow" aria-hidden="true">↗</span></a>`;});
 if(count!==1)throw Error('Expected exactly one authored card for '+c.title);
}
if((before.match(/\bdata-demo data-kind=/g)??[]).length!==(html.match(/\bdata-demo data-kind=/g)??[]).length)throw Error('Application directory membership changed.');
await writeFile(path,html);
async function walk(dir){const files=[];for(const e of await readdir(dir,{withFileTypes:true})){const p=join(dir,e.name);if(e.isDirectory())files.push(...await walk(p));else files.push(p);}return files;}
const root='site/spaces',entries=[];
for(const p of await walk(root)){if(p===join(root,'RELEASE.tsv'))continue;const body=await readFile(p);entries.push([relative(root,p),createHash('sha256').update(body).digest('hex'),body.length].join('\t'));}
await writeFile(join(root,'RELEASE.tsv'),'path\tsha256\tbytes\n'+entries.sort().join('\n')+'\n');
const report=['Updated the two existing source cards. All other showcase entries are preserved.',...changes.map(c=>c.next.replaceAll('&amp;','&')+'\t'+c.href.replaceAll('&amp;','&')),'directory_sha256\t'+createHash('sha256').update(html).digest('hex')].join('\n');
await writeFile('spaces-evidence/DISCOVERY.txt',report+'\n');console.log(report);
