// Reconcile the two existing CMS records independently so an interrupted write
// can be recovered without another copy of the request at Cloudflare.
export async function syncLeads({ sites, cms, heldLeads, enrich = async () => null, report = () => {} }) {
  const ns = "service-site", limit = 5000;
  const list = type => {
    const result = cms(["entry", "list", type, "--ns", ns, "--limit", String(limit)]);
    if (!Array.isArray(result?.data) || result.data.length >= limit) throw new Error(`Cannot reconcile ${type}: incomplete CMS listing`);
    return result.data.map(row => row.attributes ?? row);
  };
  const create = (type, value) => {
    const result = cms(["entry", "create", type, "--ns", ns, JSON.stringify(value)]);
    if (result?.error || !(result?.id ?? result?.data?.id)) throw new Error(`CMS did not confirm ${type} creation`);
  };
  const saved = new Map(list("service_lead").map(lead => [lead.lead_key, lead]));
  const messages = new Set(list("outbound_message").map(message => message.message_id));
  const summary = { sites: sites.map(site => site.site_id), brought: 0, prepared: 0, recovered: 0, already_here: saved.size, errors: [] };
  const prepare = (content, lead, recovery) => {
    const message_id = `service-site:${lead.lead_key}`;
    if (messages.has(message_id) || lead.handled === "yes") return;
    const fields = JSON.parse(lead.fields_json);
    if (!Array.isArray(fields)) throw new Error("Saved request fields are not a list");
    const facts = JSON.parse(lead.facts_json || '{}'), catalogue = facts.kind === 'catalogue_review', planner=facts.kind==='fixture_plan';
    const lines = fields.map(field => (catalogue && field.label === 'Product draft') || (planner && field.label==='Fixture plan') ? `Full submission retained with request ${lead.lead_key}.` : `${field.label}: ${field.value}`).join("\n") + (lead.brief ? `\n\nWhat the records on file say:\n${lead.brief}` : "");
    create("outbound_message", { message_id, about: `${content.site_id} request (${lead.form})`, recipient: content.contact?.email ?? "the site's operator (no address on record)",
      subject_line: catalogue ? `Product draft for review: ${facts.product_title}` : planner ? `Fixture project for review: ${facts.project_title}` : `New request from your site: ${fields[0]?.value ?? lead.form}`, body: `Received ${lead.received_at} through the ${lead.form} form on ${content.business.name}'s site.\n\n${lines}`,
      state: "composed", composed_by: "service-site", composed_at: new Date().toISOString(), would_be_sent_by: "a person, using their own account and their own judgement, outside this estate",
      cannot_infer: "that the business has seen this request, that the person who typed it is who they say, or that anybody agreed to anything beyond being answered", status: "draft" });
    messages.add(message_id);
    summary.prepared++;
    if (recovery) summary.recovered++;
  };
  const failed = (site, phase, error) => summary.errors.push({ site_id: site.site_id, phase, error: error.message.split("\n")[0] });
  // Recover from retained CMS facts before any remote request can fail.
  for (const content of sites) for (const lead of saved.values()) {
    if (lead.site_id !== content.site_id) continue;
    try { prepare(content, lead, true); } catch (error) { failed(content, "recover", error); }
  }
  for (const content of sites) {
    let remote;
    try { remote = await heldLeads(content.pages_project, content.cloud_account); }
    catch (error) { failed(content, "fetch", error); continue; }
    for (const lead of remote) {
      if (lead.site_id !== content.site_id || saved.has(lead.key)) continue;
      try {
        const worked = await enrich(content, lead);
        const record = { lead_key: lead.key, site_id: lead.site_id, form: lead.form, page: lead.page ?? "", received_at: lead.received_at, fields_json: JSON.stringify(lead.fields),
          ...(worked ? { brief: worked.brief.slice(0, 6000), facts_json: JSON.stringify(worked.facts ?? {}).slice(0, 6000) } : {}), handled: "no" };
        create("service_lead", record);
        saved.set(lead.key, record);
        summary.brought++;
        prepare(content, record, false);
        report({ site_id: lead.site_id, received_at: lead.received_at, form: lead.form, fields: Object.fromEntries(lead.fields.map(field => [field.label, field.value])), worked: worked?.facts?.roll_match ?? Boolean(worked) });
      } catch (error) { failed(content, "import", error); }
    }
  }
  return summary;
}
