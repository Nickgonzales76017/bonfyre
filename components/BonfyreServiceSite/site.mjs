#!/usr/bin/env bun
// One command for a client's site. The record lives in the internal CMS; the
// site folder's content.json is its export, and the live page is its build.
//
//   bun site.mjs new <dir> <site-id> "<business>" "<contact name>" <phone> <email>
//   bun site.mjs build <dir>          content.json -> <dir>/dist
//   bun site.mjs push <dir>           content.json -> the CMS record (create or update)
//   bun site.mjs pull <dir>           the CMS record -> content.json
//   bun site.mjs publish <dir>        pull, build, deploy: an edit in the CMS becomes the live page
//   bun site.mjs leads [dir]          bring the requests live sites are holding into the CMS (every site, or one),
//                                     and for each new one hold a message for the business; nothing is sent
//   bun site.mjs list                 every service site on record
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { createSiteCMS } from "./site-cms.mjs";
// Product boundaries are resolved on demand. CMS inventory does not load
// the page renderer, document workspace, training runtime or lead handlers.
const compiler = () => import("./build.mjs");
const contract = () => import("./site-contract.mjs");

const here = import.meta.dir;
const { cms, rows, schemas, ns, type } = createSiteCMS();
const [verb, dirArg, ...rest] = process.argv.slice(2);
const verbs = ["new", "build", "push", "pull", "publish", "leads", "list", "documents", "recording", "training"];
if (!verbs.includes(verb) || (!["list", "leads"].includes(verb) && !dirArg)) { console.error(`usage: bun site.mjs ${verbs.join("|")} <site-dir>`); process.exit(2); }
const dir = dirArg ? resolve(dirArg) : null, file = dir ? join(dir, "content.json") : null;


const local = async () => {
  const content = JSON.parse(readFileSync(file, "utf8"));
  const { validate } = await contract();
  validate(content);
  return content;
};

if (verb === "recording") {
  if(rest.length<2)throw Error('recording requires <input-file> <new-collection-folder> <title> [--source-url URL] [--credit TEXT]');
  const options={title:rest[1]};
  for(let i=2;i<rest.length;i+=2){
    if(!['--source-url','--credit'].includes(rest[i])||!rest[i+1])throw Error('Use --source-url URL or --credit TEXT after the title.');
    options[rest[i]==='--source-url'?'sourceUrl':'credit']=rest[i+1];
  }
  const { importRecording } = await import("./document-recording.mjs");
  console.log(JSON.stringify(await importRecording(dir,rest[0],options)));
} else if (verb === "training") {
  if (rest.length && (rest.length !== 2 || rest[0] !== "--port")) throw Error('training accepts only --port <number>');
  const { serveTraining } = await import("./training-workspace.mjs");
  serveTraining(dir, rest.length ? Number(rest[1]) : 4190);
} else if (verb === "documents") {
  if (rest.length && (rest.length !== 2 || rest[0] !== "--port")) throw Error('documents accepts only --port <number>');
  const { serveDocuments } = await import("./document-workspace.mjs");
  await serveDocuments(dir, rest.length ? Number(rest[1]) : 4177);
} else if (verb === "list") {
  for (const row of rows()) console.log(JSON.stringify({ site_id: row.site_id, business: row.business_name, status: row.status, pages_project: row.pages_project ?? null }));
} else if (verb === "new") {
  const [site_id, business, contact, phone, email] = rest;
  if (!email) throw new Error('new needs: <site-id> "<business>" "<contact name>" <phone> <email>');
  if (existsSync(file)) throw new Error(`${file} already exists`);
  const { newSiteDraft } = await import("./site-draft.mjs");
  const content = newSiteDraft({ site_id, business, contact, phone, email });
  const { validate } = await contract();
  validate(content);
  mkdirSync(join(dir, "assets"), { recursive: true });
  writeFileSync(file, JSON.stringify(content, null, 2) + "\n");
  console.log(JSON.stringify({ created: file, next: `bun ${join(here, "site.mjs")} build ${dirArg}` }));
} else if (verb === "build") {
  const { build } = await compiler();
  console.log(JSON.stringify(await build(dir)));
} else if (verb === "leads") {
  const selectedId = dir ? (await local()).site_id : null;
  const { runSiteLeads } = await import("./site-leads.mjs");
  const result = await runSiteLeads({ selectedId, rows, cms, schemas, here,
    report: value => console.log(JSON.stringify(value)) });
  console.log(JSON.stringify(result));
  if (result.errors.length) process.exitCode = 1;
} else {
  const { runSitePublication } = await import("./site-publication.mjs");
  await runSitePublication({verb,file,dir,rows,cms,ns,type,local,
    validate: async content => (await contract()).validate(content),
    report: value => console.log(JSON.stringify(value))});
}
