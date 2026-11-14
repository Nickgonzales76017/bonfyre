// Read-only, bounded public and local-origin health evidence.
import {readdirSync,existsSync} from 'node:fs';
import {join} from 'node:path';
import {homedir} from 'node:os';
const hosts=['aurekai.work','aurekai.life','aurekai.live','aurekai.shop'];
const run=(argv,timeout=10000)=>{
 try{const p=Bun.spawnSync(argv,{stdout:'pipe',stderr:'pipe',timeout,maxBuffer:256*1024});
 return {exit:p.exitCode,stdout:p.stdout.toString(),stderr:p.stderr.toString().slice(0,1500)};}
 catch(e){return {exit:-1,stdout:'',stderr:String(e).slice(0,400)};}
};
export async function audit(){
 const items=[],la=run(['/bin/launchctl','list']),ps=run(['/bin/ps','-axo','pid,command']);
 const launch=la.stdout.split('\n').filter(line=>/cloudflared|bonfyre.public-web-address|tunnel|bonfyre\.web/i.test(line))
  .slice(0,24).map(line=>line.split('\t').slice(0,3));
 const processes=ps.stdout.split('\n').filter(line=>/cloudflared|bonfyre-web-address|bonfyre-public-web/i.test(line))
  .filter(line=>!line.includes('edge_reachability_audit')).slice(0,24)
  .map(line=>{
    const m=/^\s*(\d+)\s+(\S+)/.exec(line);
    return m?{pid:Number(m[1]),executable:m[2].split('/').at(-1)}:{process:'unreadable'};
  });
 const agents=join(homedir(),'Library/LaunchAgents');
 const plists=existsSync(agents)?readdirSync(agents).filter(x=>/cloudflare|cloudflared|bonfyre.public-web/i.test(x)).slice(0,30):[];
 for(const host of hosts){
  const controller=new AbortController(),signal=AbortSignal.timeout(16000);
  try{
   const response=await fetch('https://'+host+'/',{redirect:'manual',signal,headers:{Accept:'text/html','Cache-Control':'no-cache'}});
   const contentType=response.headers.get('content-type')||'';
   const body=(contentType.includes('text/')?await response.text():'').slice(0,32768);
   const code=body.match(/(?:Error\s*code\s*[:#]?\s*|Cloudflare\s*)(1016|1033)/i)||
    body.match(/(?:^|[^\d])(1016|1033)(?:[^\d]|$)/);
   items.push({host,status:response.status,cfCode:code?.[1]||null,
    server:response.headers.get('server')||'',title:body.match(/<title[^>]*>([^<]{1,180})<\/title>/i)?.[1]||''});
  }catch(e){items.push({host,status:null,error:String(e.message||e).slice(0,210)});}
 }
 const remote=run(['/usr/bin/ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','-o','StrictHostKeyChecking=yes',
  'ubuntu@207.211.172.160','true'],9000);
 return {schema:'bonfyre.edge-reachability.readonly.v1',hosts:items,launch,processes,plists,
  launchctlExit:la.exit,psExit:ps.exit,remoteSSH:{exit:remote.exit,reason:remote.stderr.replace(/(?:token|password|private key)[^\n]*/gi,'[redacted]').slice(0,260)}};
}
if(import.meta.main)console.log(JSON.stringify(await audit()));
