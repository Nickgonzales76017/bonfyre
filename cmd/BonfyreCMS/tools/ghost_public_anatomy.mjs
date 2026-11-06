// Structural measurements only. Acquisition/retention is a separate native
// ForeignTwin boundary; this module never fetches or stores donor markup.
import { createHash, randomUUID } from 'node:crypto';
import { readFileSync, mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { acceptance } from './ghost_marketplace_templates.mjs';

const sha = value => createHash('sha256').update(value).digest('hex');
const tags = ['header','nav','main','section','article','h1','h2','h3','img','form','button','a','footer'];
export const observationCannotInfer = 'a public-page anatomy observation -> copied markup, CSS, JavaScript, image, Ghost runtime, a selected profile, semantic truth, source admission, audience entitlement, authority or permission to act';
export async function measureHtmlAnatomy(body, cssBodies = []) {
  const counts = Object.fromEntries(tags.map(tag => [tag,0])), sequence = [], ratios = {}, stylesheets = [];
  let headerDepth = 0, headerLinks = 0, firstArticle = null, firstH1 = null, imagesBeforeArticle = 0;
  const rewriter = new HTMLRewriter().on([...tags,'link'].join(','), { element(e) {
    const tag = e.tagName;
    if (tags.includes(tag)) { counts[tag]++; if(sequence.length<96) sequence.push(tag); }
    if(tag==='header'){ headerDepth++; e.onEndTag(()=>headerDepth--); }
    if(tag==='a'&&headerDepth) headerLinks++;
    if(tag==='h1'&&firstH1===null) firstH1=sequence.length;
    if(tag==='article'&&firstArticle===null) firstArticle=sequence.length;
    if(tag==='img'){
      if(firstArticle===null) imagesBeforeArticle++;
      const width=e.getAttribute('width'),height=e.getAttribute('height');
      if(/^\d+$/.test(width||'')&&/^\d+$/.test(height||'')&&Number(height)){
        const ratio=String(Math.round(Number(width)/Number(height)*100)/100); ratios[ratio]=(ratios[ratio]||0)+1;
      }
    }
    if(tag==='link'&&(e.getAttribute('rel')||'').toLowerCase()==='stylesheet'&&e.getAttribute('href')) stylesheets.push(e.getAttribute('href'));
  }});
  await rewriter.transform(new Response(body, { headers: { 'content-type':'text/html; charset=utf-8' } })).arrayBuffer();
  const cssDigests=cssBodies.map(sha);
  return { measurement_kind:'public_page_anatomy.v1', html_sha256:sha(body), stylesheet_sha256:cssDigests.length?sha(cssDigests.join('')):'',
    stylesheet_count_observed:cssDigests.length, landmark_counts:counts,
    masthead:{headers:counts.header,navs:counts.nav,links_in_header:headerLinks},
    hero:{h1_before_first_article:firstH1!==null&&(firstArticle===null||firstH1<firstArticle),images_before_first_article:imagesBeforeArticle},
    editorial_sequence:sequence, media_geometry:{images:counts.img,declared_aspect_ratios:ratios},
    interaction_posture:{links:counts.a,forms:counts.form,buttons:counts.button},
    typed_slots:Object.fromEntries(['h1','h2','h3','article','section'].map(key=>[key,counts[key]])),
    stylesheet_hrefs:stylesheets.slice(0,12),
  };
}
export async function observedRow(label, finalUrl, body, cssBodies=[]) {
  const vector=await measureHtmlAnatomy(body,cssBodies);
  const hrefs=vector.stylesheet_hrefs; delete vector.stylesheet_hrefs;
  const identity=sha(`${label}\n${finalUrl}`).slice(0,24);
  return { fingerprint_id:`ghost-observed:${identity}`, template:label, reference_source:'public-ghost-observation',reference_name:label,reference_url:finalUrl,
    family:'observed-reference',renderer_affinity:'unselected',density:'observed',visual_vector:vector,acceptance_contract:acceptance,
    cannot_infer:observationCannotInfer,status:'observed_reference' };
}
export async function nativeObserveOne(url, { project, foreignRoot, maxBytes }) {
  const parsed = new URL(url);
  if (parsed.protocol !== 'https:') throw Error('observation URL must be an absolute https URL');
  const scratch = mkdtempSync(join(tmpdir(), 'bonfyre-ghost-anatomy-'));
  const receipts = join(foreignRoot, 'receipts'); mkdirSync(receipts, { recursive: true });
  const identity = sha(`${url}\n${Date.now()}\n${randomUUID()}`).slice(0,24);
  const receiptPath = join(receipts, `ghost-anatomy-${identity}.jsonl`), bodyPath = join(scratch, 'body');
  try {
    const proc = Bun.spawn([project, 'foreign', 'observe', `ghost-anatomy:${identity}`, url, '--receipt', receiptPath, '--body', bodyPath,
      '--observe-right', 'public', '--full-max-bytes', String(maxBytes)], { stdout:'pipe', stderr:'pipe' });
    const code = await proc.exited, stderr = await new Response(proc.stderr).text();
    const line = readFileSync(receiptPath, 'utf8').trim().split(/\r?\n/).at(-1);
    const receipt = JSON.parse(line);
    if (code || receipt.policy_decision !== 'allow' || receipt.observation_kind !== 'source_observed' || receipt.http_status !== 200)
      throw Error(`native ForeignTwin refused complete public observation: ${receipt.policy_decision || receipt.observation_kind || code} ${stderr.slice(-160)}`);
    const body = readFileSync(bodyPath);
    if (body.length >= maxBytes || receipt.content_sha256 !== sha(body)) throw Error('native ForeignTwin bounded body/digest mismatch');
    return { body, receipt, receiptPath };
  } finally { rmSync(scratch, { recursive:true, force:true }); }
}
export async function observeGhostPage(label, url, { project, foreignRoot, observeOne = nativeObserveOne } = {}) {
  if (!label || !project || !foreignRoot) throw Error('label, native project and ForeignTwin root are required');
  const page = await observeOne(url, { project, foreignRoot, maxBytes:2_000_000 });
  const type=(page.receipt.http_evidence?.content_type || '').split(';')[0].trim().toLowerCase();
  if (!['text/html','application/xhtml+xml'].includes(type)) throw Error(`observation did not return HTML: ${type}`);
  const measured=await measureHtmlAnatomy(page.body), cssBodies=[], cssReceipts=[];
  for(const href of measured.stylesheet_hrefs){
    const assetUrl=new URL(href,url);
    if(assetUrl.protocol!=='https:'||assetUrl.origin!==new URL(url).origin)continue;
    try {
      const css=await observeOne(assetUrl.href,{project,foreignRoot,maxBytes:1_000_000});
      cssBodies.push(css.body); cssReceipts.push(css.receiptPath);
    } catch { /* missing CSS means incomplete evidence, never inferred style */ }
  }
  const row=await observedRow(label,url,page.body,cssBodies);
  row.observation_receipts={page:page.receiptPath,stylesheets:cssReceipts};
  row.cannot_infer += '; released temporary observation bytes -> the source or stylesheet remains currently available';
  return row;
}
