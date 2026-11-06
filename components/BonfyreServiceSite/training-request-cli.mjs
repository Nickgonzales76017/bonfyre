import {readFileSync} from 'node:fs';
import {trainingMaterial} from './training-workspace.mjs';
import {requestStore} from './training-requests.mjs';
const [command,home,pack,...args]=process.argv.slice(2);
if(!['list','accept','renew','deliver'].includes(command)||!home||!pack)throw Error('Usage: training-request-cli.mjs list|accept|renew|deliver HOME PACK [studio|request-id] [worker|routine.json]');
const store=requestStore(home,trainingMaterial(pack));let result;
if(command==='list')result=store.list(args[0]);
else if(command==='accept')result=store.accept(args[0],args[1]);
else if(command==='renew')result=store.renew(args[0]);
else {const raw=readFileSync(args[1]);if(raw.length>512*1024)throw Error('Delivery exceeds 512 KB.');result=store.deliver(args[0],JSON.parse(raw.toString('utf8')));}
console.log(JSON.stringify(result,null,2));
