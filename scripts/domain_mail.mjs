#!/usr/bin/env bun
// Mail for the estate's own domains, through Cloudflare Email Routing: named
// addresses on a domain are delivered to an inbox a person already reads.
// This receives mail. It does not send as the domain; that needs a sending
// service and is said so in the status.
//
//   bun scripts/domain_mail.mjs status                      what each domain has now (public records + what the token may see)
//   bun scripts/domain_mail.mjs apply <domain> [--to <inbox>] [--names hello,support,billing,legal]
//
// apply turns routing on for the domain, registers the inbox as a destination
// (Cloudflare mails that inbox a link a person must click), and adds one rule
// per name. It refuses a domain that already has mail records it did not make.
// The inbox defaults to PROTON_EMAIL from the secrets file; no value is printed.
import { account } from "../components/BonfyreServiceSite/cloud.mjs";

const [verb, ...args] = process.argv.slice(2), flag = name => { const at = args.indexOf(name); return at >= 0 ? args[at + 1] : null; };
const cloud = account(), H = { authorization: "Bearer " + cloud.env.CLOUDFLARE_API_TOKEN, "content-type": "application/json" };
const cf = async (method, path, body) => { const r = await fetch("https://api.cloudflare.com/client/v4" + path, { method, headers: H, body: body ? JSON.stringify(body) : undefined }); const d = await r.json().catch(() => ({})); return { ok: r.ok && d.success !== false, status: r.status, result: d.result, why: (d.errors ?? []).map(e => e.message).join("; ") }; };
const publicRecords = async (name, type) => ((await fetch(`https://cloudflare-dns.com/dns-query?name=${name}&type=${type}`, { headers: { accept: "application/dns-json" } }).then(r => r.json())).Answer ?? []).map(a => a.data);
const masked = email => email.replace(/^(.).*(@.*)$/, "$1…$2");
const zones = (await cf("GET", "/zones?per_page=50")).result ?? [];

if (verb === "status") {
  for (const zone of zones) {
    const [mx, txt, dmarc, routing] = await Promise.all([publicRecords(zone.name, "MX"), publicRecords(zone.name, "TXT"), publicRecords("_dmarc." + zone.name, "TXT"), cf("GET", `/zones/${zone.id}/email/routing`)]);
    console.log(JSON.stringify({ domain: zone.name, receives_mail: mx.length > 0, mail_servers: mx, sender_policy: txt.find(t => /v=spf1/.test(t)) ?? null, dmarc: dmarc.find(t => /v=DMARC1/i.test(t)) ?? null,
      routing: routing.ok ? (routing.result.enabled ? "on" : "off") : `this token may not see it (${routing.why || routing.status})`, can_send_as_domain: false }));
  }
} else if (verb === "apply") {
  const domain = args[0], zone = zones.find(z => z.name === domain);
  if (!zone) throw new Error(`${domain} is not one of this account's domains: ${zones.map(z => z.name).join(", ")}`);
  const names = (flag("--names") ?? "hello,support,billing,legal").split(",").map(n => n.trim().toLowerCase()).filter(n => /^[a-z0-9][a-z0-9._-]*$/.test(n));
  let inbox = flag("--to");
  if (!inbox) { const read = Bun.spawnSync(["sh", "-c", 'set -a; . "$HOME/.bonfyre/secrets/cloud.refs.env" >/dev/null 2>&1; printf %s "$PROTON_EMAIL"'], { stdout: "pipe" }); inbox = read.stdout.toString().trim(); }
  if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(inbox)) throw new Error("no inbox to deliver to: pass --to, or put PROTON_EMAIL in the secrets file");
  const mx = await publicRecords(domain, "MX");
  if (mx.length && !mx.every(record => /mx\.cloudflare\.net\.?$/.test(record))) throw new Error(`${domain} already receives mail elsewhere (${mx.join(", ")}); not touched`);
  const routing = await cf("GET", `/zones/${zone.id}/email/routing`);
  if (!routing.ok) throw new Error(`the Cloudflare token on file may not manage mail for ${domain} (${routing.why || routing.status}). It needs: Zone > Email Routing Rules: Edit, Zone > DNS: Edit, Account > Email Routing Addresses: Edit. Nothing was changed.`);
  if (!routing.result.enabled) { const on = await cf("POST", `/zones/${zone.id}/email/routing/dns`); if (!on.ok) throw new Error(`routing could not be turned on: ${on.why}`); }
  const known = (await cloud.api("GET", "/email/routing/addresses")).result.find(a => a.email.toLowerCase() === inbox.toLowerCase());
  const destination = known ?? (await cloud.api("POST", "/email/routing/addresses", { email: inbox })).result;
  const rules = (await cf("GET", `/zones/${zone.id}/email/routing/rules?per_page=50`)).result ?? [], made = [];
  for (const name of names) {
    const address = `${name}@${domain}`;
    if (rules.some(rule => rule.matchers?.some(m => m.value === address))) continue;
    const rule = await cf("POST", `/zones/${zone.id}/email/routing/rules`, { name: `${address} to a person`, enabled: true, matchers: [{ type: "literal", field: "to", value: address }], actions: [{ type: "forward", value: [inbox] }] });
    if (!rule.ok) throw new Error(`${address} could not be added: ${rule.why}`);
    made.push(address);
  }
  console.log(JSON.stringify({ domain, delivered_to: masked(inbox), inbox_confirmed: Boolean(destination.verified), addresses_added: made, addresses_already_there: names.length - made.length,
    next: destination.verified ? "send a message to one of the addresses to see it arrive" : "Cloudflare has mailed the inbox a link; mail is held back until a person clicks it", can_send_as_domain: false }));
} else { console.error("usage: bun scripts/domain_mail.mjs status | apply <domain> [--to inbox] [--names a,b]"); process.exit(2); }
