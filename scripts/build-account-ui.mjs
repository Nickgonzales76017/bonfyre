import {readFile,mkdir,writeFile} from 'node:fs/promises';import path from 'node:path';import {createHash} from 'node:crypto';
const files=[['account-session.mjs','account-session.mjs'],['account-navigation.mjs','account-navigation.mjs'],['account-navigation.css','account-navigation.css'],['application-previews.js','showcase.js'],['application-previews.css','application-previews.css']];
const check=process.argv.includes('--check'),root=process.cwd(),rows=[];
for(const [source,target]of files){const src=path.join(root,'components/BonfyreServiceSite',source),dst=path.join(root,'site/assets',target),bytes=await readFile(src);if(check){const old=await readFile(dst);if(!bytes.equals(old))throw Error('Published projection differs from its source: '+target);}else{await mkdir(path.dirname(dst),{recursive:true});await writeFile(dst,bytes);}rows.push([source,target,createHash('sha256').update(bytes).digest('hex'),bytes.length]);}
const manifest='source\tprojection\tsha256\tbytes\n'+rows.map(r=>r.join('\t')).join('\n')+'\n';
if(check){if(await readFile('site/assets/account-ui-SOURCE.tsv','utf8')!==manifest)throw Error('Account UI manifest differs.');}else await writeFile('site/assets/account-ui-SOURCE.tsv',manifest);
console.log('ACCOUNT_UI_PROJECTION_PASS '+rows.length+' files; '+(check?'checked':'written'));
