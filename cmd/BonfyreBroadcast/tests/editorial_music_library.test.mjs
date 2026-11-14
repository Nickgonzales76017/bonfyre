import {test,expect} from "bun:test";
import {editorialMusicCatalogue,suggestedEditorialTrack}
 from "../../../components/BonfyreServiceSite/editorial-music-catalogue.mjs";

test("real recorded CC0 sources replace generic tone selection with seven pinned tracks",()=>{
 expect(editorialMusicCatalogue).toHaveLength(7);
 expect(new Set(editorialMusicCatalogue.map(x=>x.sha256)).size).toBe(7);
 expect(new Set(editorialMusicCatalogue.map(x=>x.id)).size).toBe(7);
 expect(new Set(editorialMusicCatalogue.map(x=>x.artist)).size).toBeGreaterThanOrEqual(2);
 expect(new Set(editorialMusicCatalogue.map(x=>x.profile)).size).toBe(5);
 for(const track of editorialMusicCatalogue){
  expect(track.title.length).toBeGreaterThan(3);
  expect(track.instruments.length).toBeGreaterThan(10);
  expect(track.durationSeconds).toBeGreaterThanOrEqual(90);
  expect(track.sha256).toMatch(/^[a-f0-9]{64}$/);
  expect(track.licence).toBe("CC0-1.0");
  expect(track.rightsReview).toBe("pending");
  expect(track.sourceUrl.startsWith("https://huggingface.co/datasets/SoundSafari/Public-Domain-Music/blob/main/")).toBe(true);
  expect(track.downloadUrl.startsWith("https://huggingface.co/datasets/SoundSafari/Public-Domain-Music/resolve/main/")).toBe(true);
  expect(track.downloadUrl.endsWith(".mp3")).toBe(true);
  expect(track.title).not.toMatch(/^(chime|pulse|harmony)$/i);
 }
});

test("an article gets a stable editorial profile candidate, not one universal music clip",()=>{
 for(const profile of ["newsroom","feature","essay","conversation","documentary"]){
  const a=suggestedEditorialTrack(profile,"article:the-council-agenda");
  expect(a.profile).toBe(profile);
  expect(suggestedEditorialTrack(profile,"article:the-council-agenda")).toBe(a);
 }
 const choices=new Set(Array.from({length:30},(_,i)=>
  suggestedEditorialTrack("conversation","article:post-"+i).id));
 expect(choices.size).toBeGreaterThan(1);
 expect(suggestedEditorialTrack("unknown","article:test")).toBeNull();
 expect(()=>suggestedEditorialTrack("essay","x".repeat(501))).toThrow();
});
