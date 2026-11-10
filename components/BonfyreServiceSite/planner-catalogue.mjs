export function validateFixtureCatalogue(input) {
  if (input.schema !== 'service-site.fixture-catalogue.v1' || !input.products?.length || input.products.length>32) throw Error('A fixture collection needs retained products.');
  const ids=new Set();
  for (const p of input.products) {
    if (!/^[a-z][a-z0-9-]+$/.test(p.id) || ids.has(p.id) || !/^\d+$/.test(p.product_id) || !/^\d+$/.test(p.variant_id)) throw Error('Invalid fixture identity.');
    ids.add(p.id);
    if (!Number.isSafeInteger(p.price_cents) || p.price_cents<0 || !/^[a-f0-9]{64}$/.test(p.source_sha256) || !Number.isFinite(Date.parse(p.observed_at))) throw Error('A fixture needs an observed price and source.');
    for(const field of ['source_url','image']){const u=new URL(p[field]);if(u.protocol!=='https:'||u.username||u.password||u.origin!==input.origin)throw Error('Unexpected fixture source.');}
    if (!Array.isArray(p.questions) || p.questions.some(q=>typeof q!=='string')) throw Error('Fixture questions must be text.');
    for(const d of [p.dimensions,p.allowance].filter(Boolean)) for(const n of [d.width,d.depth].filter(n=>n!==null)) if(!Number.isFinite(n)||n<=0||n>720)throw Error('Invalid fixture dimensions.');
  }
  return input;
}

export async function verifyFixtureCatalogue(catalogue) {
  if (!catalogue || typeof catalogue !== 'object') throw Error('Missing fixture source.');
  const {digest,...input}=catalogue;validateFixtureCatalogue(input);
  const bytes=new TextEncoder().encode(JSON.stringify(input));
  if(bytes.length>128*1024)throw Error('Fixture source exceeds its limit.');
  const actual=[...new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))].map(b=>b.toString(16).padStart(2,'0')).join('');
  if(actual!==digest)throw Error('Fixture source does not match its retained digest.');
  return catalogue;
}
