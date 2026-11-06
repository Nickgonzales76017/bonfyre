import {test,expect} from 'bun:test';
import {loadWorkspaceFiles} from '../workspace_material.mjs';
import {repairRoomScaleFiles,roomScaleBase} from '../materialize_room_scale_repair.mjs';
const root=process.env.BONFYRE_ROOM_BASE_ROOT,retained=root?test:test.skip;
test('missing retained editor material cannot be repaired',()=>{expect(()=>repairRoomScaleFiles(new Map())).toThrow('Missing installed entry');});
retained('repair preserves historical bytes and limits additions to reviewed editor closures',async()=>{
 const old=await loadWorkspaceFiles(root,roomScaleBase),{files,projections}=repairRoomScaleFiles(old);
 expect(projections).toHaveLength(2);expect(files.size-old.size).toBe(projections.reduce((n,p)=>n+p.files,0));
 for(const[name,bytes]of old)if(!projections.some(p=>p.entry===name))expect(files.get(name).equals(bytes)).toBe(true);
 for(const p of projections){
  const names=[...old.keys()].filter(n=>n.startsWith(p.prior));
  expect(names.filter(n=>!old.get(n).equals(files.get(p.prefix+n.slice(p.prior.length)))).map(n=>n.slice(p.prior.length)).sort()).toEqual(['planner-client.mjs','planner-model.mjs','planner-spatial-model.mjs','planner-spatial.mjs']);
  expect(files.get(p.entry).toString()).toContain(p.prefix);expect(files.get(p.entry).toString()).not.toContain(p.prior);
 }
 expect(files.get('product-room/index.html').toString()).toContain('data-room-scale');
 const altered=new Map(old),p=projections[0];altered.set(p.prior+'planner-model.mjs',Buffer.from('unknown'));
 expect(()=>repairRoomScaleFiles(altered)).toThrow('Unreviewed installed module');
 expect(repairRoomScaleFiles(old).projections).toEqual(projections);
});
