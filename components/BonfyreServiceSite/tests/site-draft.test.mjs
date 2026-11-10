import {expect,test} from "bun:test";
import {newSiteDraft} from "../site-draft.mjs";
import {validate} from "../site-contract.mjs";
test("draft factory produces the previous compatible page and intake shape",()=>{
 const d=newSiteDraft({site_id:"sample-business",business:"Sample Business",
   contact:"Jane Example",phone:"2145551234",email:"jane@example.org"});
 expect(()=>validate(d)).not.toThrow();
 expect(d.preview).toBe(true);
 expect(d.pages_project).toBe("sample-business");
 expect(d.sections.map(s=>s.kind)).toEqual(["hero","steps","reviews","person","call"]);
 expect(d.sections[0].form.id).toBe("request");
 expect(d.sections[3].text).toBe("You deal with Jane directly.");
});
test("the draft factory cannot publish a site itself",()=>{
 expect(()=>newSiteDraft({site_id:"no",business:"Test",contact:"",phone:"",email:""})).toThrow("contact name");
});
