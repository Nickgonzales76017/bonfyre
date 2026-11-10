// Service-site lead reconciliation, isolated from the CLI and page compiler.
// CMS owns stored records and admission; this hook cannot read arbitrary paths.
import { existsSync } from "node:fs";
import { join } from "node:path";

export const safeSiteId = id => typeof id === "string" && /^[a-z0-9][a-z0-9-]{1,56}$/.test(id);
export async function runSiteLeads({
  selectedId=null, rows, cms, schemas, here, report=()=>{},
  integrations=async () => {
    const [{syncLeads},{heldLeads}] = await Promise.all([import("./leads.mjs"),import("./cloud.mjs")]);
    return {syncLeads,heldLeads};
  },
  notify = (result) => {
    if (result.prepared && process.platform === "darwin" && !process.env.BONFYRE_QUIET)
      Bun.spawnSync(["osascript","-e",
        "display notification \""+result.prepared+" client requests ready for review. Held in CMS.\" with title \"Service sites\""]);
  },
}={}) {
  if (selectedId !== null && !safeSiteId(selectedId)) throw Error("invalid service site selection");
  const sites=rows().map(row=>JSON.parse(row.content_json))
    .filter(c=>c.leads==="server"&&c.pages_project&&(!selectedId||c.site_id===selectedId));
  const {syncLeads,heldLeads}=await integrations();
  cms(["schema","apply",join(schemas,"outbound_message.json")]);
  const result=await syncLeads({sites,cms,heldLeads,report,enrich:async(content,lead)=>{
    // A malformed CMS site id never becomes an arbitrary module import path.
    if (!safeSiteId(content.site_id))
      return {brief:"The site identifier is invalid; its enrichment hook was refused.",facts:{}};
    const hook=join(here,"../../packs",content.site_id,"site/lead.mjs");
    if (!existsSync(hook)) return null;
    try {return await (await import(hook)).onLead(lead)}
    catch(error){return {brief:"The business's own records could not be consulted: "+
      String(error.message||error).split("\n")[0],facts:{}}}
  }});
  notify(result);
  return result;
}
