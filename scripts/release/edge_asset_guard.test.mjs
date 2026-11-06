import { test,expect } from "bun:test";
import { mkdtempSync,mkdirSync,rmSync,symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { guardEdgePaths } from "./edge_asset_guard.mjs";
import { files } from "../build_edge_assets.mjs";
test("publication output cannot overwrite source or repo",()=>{
 const root=mkdtempSync(join(tmpdir(),"edge-p2-")),source=join(root,"export"),out=join(root,"edge-assets");
 try {
   mkdirSync(join(source,"hosts"),{recursive:true});
   expect(guardEdgePaths({export:source,out},root).out).toBe(out);
   expect(()=>guardEdgePaths({export:source,out:source},root)).toThrow("overlap");
   expect(()=>guardEdgePaths({export:source,out:join(source,"child")},root)).toThrow("overlap");
   expect(()=>guardEdgePaths({export:source,out:root},root)).toThrow("delete repository");
   expect(()=>guardEdgePaths({export:source,out:join(tmpdir())},root)).toThrow("delete repository");
   expect(()=>guardEdgePaths({export:source,out,maxFiles:20001},root)).toThrow("1 to 20000");
   expect(()=>guardEdgePaths({export:source,out,maxFiles:1.5},root)).toThrow("integer");
   const alias=join(root,"alias");
   symlinkSync(source,alias);
   expect(()=>guardEdgePaths({export:source,out:join(alias,"dist")},root)).toThrow("overlap");
   symlinkSync("/etc/hosts",join(source,"hosts","leak.html"));
   expect(()=>files(join(source,"hosts"))).toThrow("refuses symlink");
 } finally {rmSync(root,{recursive:true,force:true})}
});
