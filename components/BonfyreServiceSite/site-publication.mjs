// The ServiceSite CMS-to-publication boundary. No interactive UI logic.
import { readFileSync, writeFileSync } from "node:fs";

export async function runSitePublication({
  verb,file,dir,rows,cms,ns,type,local,validate,report=()=>{},
  buildSite=null,deploySite=null,
}) {
  if (!["push","pull","publish"].includes(verb)) throw Error("unknown publication operation");
  const peek=JSON.parse(readFileSync(file,"utf8")).site_id;
  const row=rows().find(entry=>entry.site_id===peek);
  if (verb==="push") {
    const content=await local();
    // The canonical CMS text field is bounded; local source is not authority.
    if (JSON.stringify(JSON.stringify(content)).length>30000)
      throw Error("the record is larger than the CMS holds in one field; shorten it or split the site");
    const body=JSON.stringify({
      site_id:content.site_id,business_name:content.business.name,
      pages_project:content.pages_project??"",
      status:content.preview?"preview":"public",
      content_json:JSON.stringify(content),
      note:"exported to the live page by site.mjs publish",
    });
    const answer=row?
      cms(["entry","update",type,String(row.id),"--ns",ns,body]):
      cms(["entry","create",type,"--ns",ns,body]);
    const event={pushed:content.site_id,created:!row,id:row?.id??answer?.data?.id??answer?.id??null};
    report(event);return event;
  }
  if (!row) throw Error("no record for "+peek+" in the CMS yet; run push first");
  const content=JSON.parse(row.content_json);
  await validate(content);
  if(content.site_id!==peek) throw Error("the record's site_id does not match this folder");
  writeFileSync(file,JSON.stringify(content,null,2)+"\n");
  const event={pulled:content.site_id,id:row.id,preview:Boolean(content.preview)};
  report(event);
  if(verb==="publish") {
    if(!content.pages_project)throw Error("the record names no pages_project to publish to");
    const build=buildSite??(await import("./build.mjs")).build;
    const publish=deploySite??(await import("./cloud.mjs")).publish;
    const built=await build(dir);
    const receipt=await publish(built.out,content.pages_project,content.cloud_account);
    report(receipt);return receipt;
  }
  return event;
}
