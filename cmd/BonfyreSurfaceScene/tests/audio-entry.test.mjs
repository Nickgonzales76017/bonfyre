import {test,expect} from "bun:test";
import {findAudioEditorLink} from "../../../components/BonfyreServiceSite/authoring/audio-entry.mjs";

const row=href=>({getAttribute:name=>name==="href"?href:null});
const doc=hrefs=>({querySelectorAll:selector=>{
 expect(selector).toBe("a[data-work-editor][href],a[data-instrument-entry][href]");
 return hrefs.map(row);
}});
const exact="/apps/ws-"+"a".repeat(40)+"/audio/";

test("audio discovery accepts the single-view installed editor and native subtool addresses",()=>{
 expect(findAudioEditorLink(doc([exact]))).toBe(exact);
 expect(findAudioEditorLink(doc(["/apps/ws-"+"b".repeat(40)+"/_work",exact]))).toBe(exact);
 expect(findAudioEditorLink(doc([]))).toBeNull();
});
test("audio continuation refuses external, unsafely scoped and fabricated addresses",()=>{
 for(const value of [
  "https://foreign.example/apps/ws-"+"a".repeat(40)+"/audio/",
  "//aurekai.work/apps/ws-"+"a".repeat(40)+"/audio/",
  "/apps/ws-"+"a".repeat(39)+"/audio/",
  "/apps/ws-"+"a".repeat(40)+"/audio/../evil",
  "/apps/ws-"+"a".repeat(40)+"/audio/?token=secret",
  "/apps/ws-"+"a".repeat(40)+"/narration/"
 ])expect(findAudioEditorLink(doc([value]))).toBeNull();
});
