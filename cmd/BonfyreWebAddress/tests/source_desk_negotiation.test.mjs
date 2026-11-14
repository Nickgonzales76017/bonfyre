// Public native source-desk representation: broad Accept is HTML, not a 404.
// Isolated localhost BonfyreAuth/CMS/WebAddress; no live accounts.
import {test,expect} from 'bun:test';
import {fixture} from './auth_continuation_fixture.mjs';

test('public connections and agent discovery render under HTML and wildcard Accept',async()=>{
 const f=await fixture();
 try{
  for(const [route,title] of [['/connections','Connections'],['/connect','Tools, agents and push feeds']]){
   for(const accept of ['text/html','*/*','']){
    const r=await f.request(route,{host:'aurekai.work',headers:{Accept:accept}});
    expect(r.status).toBe(200);
    expect(r.body).toContain('<h1>'+title+'</h1>');
    expect(r.body).not.toContain('data-session-state="signed-in"');
   }
   const machine=await f.request(route,{host:'aurekai.work',headers:{Accept:'application/json'}});
   expect(machine.body).not.toContain('<h1>'+title+'</h1>');
  }
 }finally{await f.close();}
},60000);
