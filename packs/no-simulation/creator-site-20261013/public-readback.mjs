import {account} from '../../../components/BonfyreServiceSite/cloud.mjs';
import {creatorFiles} from '../../../components/BonfyreServiceSite/creator-material.mjs';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
const origin='https://aurekai-work-samples.pages.dev';
const packed=await creatorFiles('packs/aurekai-work-samples/audio');
const files=['index.html','audio/index.html','assets/site.css',...Object.keys(packed.files).map(name=>'data/generated-audio/'+name)];
const sha=bytes=>createHash('sha256').update(bytes).digest('hex'),checks=[];
for(const file of files){
 const route=file.endsWith('index.html')?file.slice(0,-10):file;
 const response=await fetch(origin+'/'+route),body=new Uint8Array(await response.arrayBuffer());
 const local=readFileSync('packs/aurekai-work-samples/site/dist/'+file);
 checks.push({file,status:response.status,bytes:body.length,sha256:sha(body),local_sha256:sha(local),matches:sha(body)===sha(local)});
}
const project=(await account().api('GET','/pages/projects/aurekai-work-samples')).result;
const report={origin,deployment_id:project.canonical_deployment?.id,deployment_url:project.canonical_deployment?.url,checks,status:checks.every(c=>c.status===200&&c.matches)?'passed':'failed'};
await Bun.write(new URL('./public-readback.json',import.meta.url),JSON.stringify(report,null,2)+'\n');
console.log(JSON.stringify({status:report.status,deployment_id:report.deployment_id,files:checks.length}));
if(report.status!=='passed')process.exitCode=1;
