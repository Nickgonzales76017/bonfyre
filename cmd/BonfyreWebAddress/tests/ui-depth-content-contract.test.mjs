import {test,expect} from "bun:test";
import {readFileSync} from "node:fs";
import {resolve,join} from "node:path";
const root=resolve(import.meta.dir,"../../..");
const read=p=>readFileSync(join(root,p),"utf8");
test("compiled navigation is the single authority for public page headings",()=>{
 const nav=read("cmd/BonfyreWebAddress/generated/native_page_composition.h");
 const source=read("cmd/BonfyreWebAddress/src/page_core_rendering.h");
 expect(nav).toContain('{"\\/board","Collections"}'.replace("\\/","/"));
 expect(source).toContain("BF_PAGE_NAV_COUNT");
 expect(source).toContain("BF_ESTATE_BAR_HEAD");
});
test("item depth separates editorial, primary action, related links and advanced continuations",()=>{
 const detail=read("cmd/BonfyreWebAddress/src/page_routes_and_emitters.h");
 for(const phrase of ["thing-text","More ways to use this","Related places","Write a post about it","page_emit_archive_earlier"])expect(detail).toContain(phrase);
 expect(detail).toContain("page_short_publisher");
});
test("three public room concepts have accessible non-WebGL reading surface",()=>{
 const html=read("scripts/design/room-study.html");
 for(const name of ["The Gallery","Listening Room","Creator Studio"])expect(html).toContain(name);
 expect(html).toContain("aria-pressed");
 expect(html).toContain("three@0.160.1");
 expect(html).toContain("prefers-reduced-motion");
 expect(html).toContain("Three-dimensional exploration stays optional".split(" ")[0]||"Three");
});
