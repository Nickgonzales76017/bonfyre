import {WorkflowEntrypoint} from 'cloudflare:workers';
import {runActivation} from './workflow-run.mjs';
import {packetFor,hash} from './workflow-contract.mjs';
import {boundedBody} from './bounded-body.mjs';

export class WorkgraphActivation extends WorkflowEntrypoint {
  async run(event,step) {return runActivation(this.env,event,step);}
}
export default {
  async fetch(request,env) {
    const url=new URL(request.url);
    if(url.pathname==='/health'&&request.method==='GET')return Response.json({service:'bonfyre-power-user-continuation',version:2,transport:'authenticated-http-pull',queueBound:!!env.WORKGRAPH_QUEUE,workflowBound:!!env.WORKGRAPH_FLOW,execution:'native-owner'},{headers:{'cache-control':'no-store'}});
    // Activation is through the authenticated Cloudflare Workflow API, never an anonymous form.
    return new Response('Not found',{status:404});
  },
  async scheduled(event,env,ctx) {
    if(!env.WORKGRAPH_FLOW||env.SOURCE_WATCH_ENABLED!=='true')return;
    ctx.waitUntil((async()=>{
      // This is the owned sample site only; cloud messages cannot choose a fetch address or command.
      if(env.SOURCE_URL!=='https://aurekai-work-samples.pages.dev/'||env.SOURCE_MISSION!=='source:work-samples')throw Error('unapproved source watcher');
      const response=await fetch(env.SOURCE_URL,{headers:{'accept-encoding':'identity'},redirect:'error',signal:AbortSignal.timeout(10000)});
      if(!response.ok)throw Error('sample source unavailable');
      const digest=await hash(await boundedBody(response,1048576));
      const {id,packet}=await packetFor({mission:env.SOURCE_MISSION,digest},env.EDGE_SHARED_SECRET);
      try {await env.WORKGRAPH_FLOW.create({id,params:packet});}
      catch(error) {
        const existing=await env.WORKGRAPH_FLOW.get(id);await existing.status();
      }
    })());
  }
};
