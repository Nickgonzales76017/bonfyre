import {expect,test} from "bun:test";
import {runSiteLeads,safeSiteId} from "../site-leads.mjs";

const setup=contents=>{
  const seen={};
  return {
    seen,
    opts:{
      rows:()=>contents.map(content=>({content_json:JSON.stringify(content)})),
      cms:args=>{(seen.cms??=[]).push(args);return {}},
      schemas:"/fixture/schemas",here:"/fixture/component",
      integrations:async()=>({heldLeads:()=>null,syncLeads:async args=>{
        seen.args=args;
        seen.enrich=await args.enrich({site_id:"../private"}, {id:"example"});
        return {prepared:0,errors:[]};
      }}),
      notify:()=>{seen.notified=true}
    }
  };
};
test("trusted site IDs are required before importing local enrichment modules",()=>{
  expect(safeSiteId("example-site")).toBe(true);
  expect(safeSiteId("../private")).toBe(false);
  expect(safeSiteId("UPPER90")).toBe(false);
  expect(safeSiteId("a/b")).toBe(false);
});
test("only selected CMS sites are submitted to native lead processing",async()=>{
  const {seen,opts}=setup([
    {site_id:"first",leads:"server",pages_project:"one"},
    {site_id:"second",leads:"server",pages_project:"two"},
    {site_id:"third",leads:"mail",pages_project:"three"},
  ]);
  const result=await runSiteLeads({...opts,selectedId:"second"});
  expect(result.errors).toEqual([]);
  expect(seen.args.sites.map(x=>x.site_id)).toEqual(["second"]);
  expect(seen.enrich.brief).toContain("refused");
  expect(seen.cms).toEqual([["schema","apply","/fixture/schemas/outbound_message.json"]]);
  expect(seen.notified).toBe(true);
  await expect(runSiteLeads({...opts,selectedId:"../secret"})).rejects.toThrow("invalid service site selection");
});
