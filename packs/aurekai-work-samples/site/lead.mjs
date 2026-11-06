import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { catalogueReviewForm, validateReviewDraft } from '../../../components/BonfyreServiceSite/catalogue-review.mjs';
import { prepareCatalogueRow } from '../../../components/BonfyreServiceSite/catalogue-baseline.mjs';
import { plannerFiles } from '../../../components/BonfyreServiceSite/planner-material.mjs';
import { validatePlan, planBrief, assessPlan } from '../../../components/BonfyreServiceSite/planner-model.mjs';

export async function onLead(lead) {
  if(lead.form==='planner-red3-fixtures'){
    const {catalogue}=plannerFiles(JSON.parse(readFileSync(join(import.meta.dir,'fixture-planner.json'),'utf8')));
    const plan=validatePlan(JSON.parse(lead.fields.find(f=>f.label==='Fixture plan').value),catalogue),assessment=assessPlan(plan,catalogue);
    return {brief:planBrief(plan,catalogue),facts:{kind:'fixture_plan',project_title:plan.title,plan_id:plan.id,catalogue_digest:catalogue.digest,item_count:plan.items.length,listed_subtotal_cents:assessment.subtotal,issue_count:assessment.issues.length,intake_receipt:lead.key,author_verified:false,supplier_contacted:false,quote_issued:false,order_placed:false}};
  }
  if (lead.form !== 'catalogue-product-workspace') return null;
  const input = JSON.parse(Buffer.from(Bun.gunzipSync(readFileSync(join(import.meta.dir, 'catalogue-input.json.gz')))).toString());
  const field = lead.fields.find(field => field.label === 'Product draft');
  const contract = catalogueReviewForm({ stores: input.stores, entries: input.rows.map(prepareCatalogueRow) }, '/catalogue/');
  const draft = await validateReviewDraft(JSON.parse(field.value), contract.catalogue);
  const product = input.rows.find(p => p.store === draft.store && p.id === draft.product_id);
  return {
    brief: `Review requested for ${product.title}.\nSource: ${product.source_url}\nThe product identity and source digest match the retained catalogue. Original values are checked for edits to imported fields; preparation fields contain proposals only. The sender and proposed changes still need review.\n\n` + draft.changes.map(change => `${change.field}\n${change.field.startsWith('prep:') ? 'Preparation proposal (merchant value not imported)' : 'Verified original: '+(change.before || '(empty)')}\nProposed: ${change.after || '(empty)'}`).join('\n\n'),
    facts: { kind: 'catalogue_review', store: draft.store, product_id: draft.product_id, product_title: product.title, source_sha256: draft.source_sha256, source_url: product.source_url, original_fields_verified: draft.changes.some(c=>!c.field.startsWith('prep:')), preparation_count: draft.changes.filter(c=>c.field.startsWith('prep:')).length, change_count: draft.changes.length, intake_receipt: lead.key, author_verified: false, changes_approved: false, merchant_published: false }
  };
}
