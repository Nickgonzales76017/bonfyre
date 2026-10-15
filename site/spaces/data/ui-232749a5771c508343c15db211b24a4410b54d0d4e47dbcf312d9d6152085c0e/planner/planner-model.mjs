// One proposal contract for the browser, intake and native review.
// Positions and room measurements express the visitor's intent, not verified site geometry.
export function validatePlan(plan, catalogue) {
  const fail = message => { throw Error(message); };
  const keys = (value, allowed) => value && typeof value === 'object' && !Array.isArray(value) && Object.keys(value).every(k => allowed.includes(k));
  const text = (value, max, required = false) => typeof value === 'string' && value.length <= max && (!required || value.trim().length > 0);
  if (!keys(plan, ['schema','catalogue','id','title','width','depth','budget','deadline','notes','items']) || plan.schema !== 'service-site.fixture-plan.v1' || plan.catalogue !== catalogue.digest) fail('This plan uses different product information. Open a new plan.');
  if (typeof plan.id !== 'string' || !/^[a-zA-Z0-9-]{8,80}$/.test(plan.id) || !text(plan.title, 100, true)) fail('Give your project a name (up to 100 characters).');
  if (![plan.width, plan.depth].every(n => typeof n === 'number' && Number.isFinite(n) && n >= 6 && n <= 60 && Number.isInteger(n * 4))) fail('Use room dimensions from 6 to 60 feet, in quarter-foot steps.');
  if (plan.budget !== null && (!Number.isSafeInteger(plan.budget) || plan.budget < 1 || plan.budget > 100000000)) fail('Enter a positive budget with no more than two decimal places.');
  if (!text(plan.deadline, 10) || (plan.deadline && (!/^\d{4}-\d{2}-\d{2}$/.test(plan.deadline) || new Date(plan.deadline+'T00:00:00Z').toISOString().slice(0,10) !== plan.deadline))) fail('Use a valid target date.');
  if (!text(plan.notes, 1500)) fail('Keep your questions within 1,500 characters.');
  if (!Array.isArray(plan.items) || !plan.items.length || plan.items.length > 12) fail('Choose between one and twelve fixtures.');
  const seen = new Set();
  for (const item of plan.items) {
    if (!keys(item, ['key','product','x','y','rotation']) || typeof item.key !== 'string' || !/^[a-zA-Z0-9-]{1,40}$/.test(item.key) || seen.has(item.key)) fail('Invalid fixture reference.');
    seen.add(item.key);
    if (!catalogue.products.some(p => p.id === item.product)) fail('A selected fixture is not in this collection.');
    if (![item.x,item.y].every(n => typeof n === 'number' && Number.isFinite(n) && n >= 0 && n <= 60 && Number.isInteger(n*4)) || ![0,90].includes(item.rotation)) fail('Invalid fixture position.');
  }
  return plan;
}

export function startingPlan(catalogue, mode = 'compact', id = crypto.randomUUID()) {
  if(!Object.hasOwn(catalogue.presets,mode))throw Error('Unknown starting layout.');
  return validatePlan({schema:'service-site.fixture-plan.v1',catalogue:catalogue.digest,id,...structuredClone(catalogue.defaults),items:structuredClone(catalogue.presets[mode])},catalogue);
}

export function footprint(product, item) {
  if (!product.dimensions?.width || !product.dimensions?.depth) return null;
  const {width,depth} = product.dimensions;
  return {x:item.x,y:item.y,w:(item.rotation ? depth : width)/12,h:(item.rotation ? width : depth)/12};
}

export function assessPlan(plan, catalogue) {
  validatePlan(plan,catalogue);
  const lines = plan.items.map(item => ({...item,product:catalogue.products.find(p=>p.id===item.product)}));
  const subtotal = lines.reduce((sum,item)=>sum+item.product.price_cents,0), issues=[], questions=new Set();
  const rectangles = [];
  for (const line of lines) {
    const p=line.product, rect=footprint(p,line);
    p.questions.forEach(q=>questions.add(q));
    if (!rect) {
      questions.add(p.dimensions ? `${p.label}: depth is missing; the dashed area is a planning allowance.` : `${p.label}: confirm the supporting surface and available space.`);
      continue;
    }
    if (rect.x+rect.w>plan.width || rect.y+rect.h>plan.depth) issues.push(`${p.label} extends beyond the room.`);
    for (const other of rectangles) if (rect.x<other.x+other.w && rect.x+rect.w>other.x && rect.y<other.y+other.h && rect.y+rect.h>other.y) issues.push(`${p.label} overlaps ${other.label}.`);
    rectangles.push({...rect,label:p.label});
  }
  if (plan.items.some(i=>i.x>plan.width||i.y>plan.depth)) issues.push('A fixture position is outside the room.');
  if (plan.budget !== null && subtotal > plan.budget) issues.push('The listed fixture subtotal exceeds your budget before delivery and tax.');
  questions.add('Confirm current prices, availability, freight, tax and delivery timing.');
  questions.add('Review aisles, access, site measurements and placement before ordering.');
  if (plan.deadline) questions.add(`Can these fixtures arrive by ${plan.deadline}?`);
  return {lines,subtotal,issues:[...new Set(issues)],questions:[...questions]};
}

export const money = cents => new Intl.NumberFormat('en-US',{style:'currency',currency:'USD'}).format(cents/100);
export function planBrief(plan,catalogue) {
  const a=assessPlan(plan,catalogue);
  return [plan.title,`${plan.width} × ${plan.depth} ft · ${plan.items.length} fixtures`,
    `Budget: ${plan.budget===null?'Not set':money(plan.budget)}${plan.deadline?' · Target date: '+plan.deadline:''}`,
    '',...a.lines.map((line,i)=>`${i+1}. ${line.product.label} (${line.product.option})\n   Listed ${money(line.product.price_cents)} · ${line.x}, ${line.y} ft from back-left · rotation ${line.rotation}°\n   Product ${line.product.product_id}, option ${line.product.variant_id}; observed ${line.product.observed_at.slice(0,10)}`),
    '',`Listed fixture subtotal: ${money(a.subtotal)}. Delivery, tax and any custom work are excluded. This is a request for review, not a quote.`,
    ...(a.issues.length?['','Layout and budget questions',...a.issues.map(q=>'• '+q)]:[]),
    '', 'For the supplier to confirm',...a.questions.map(q=>'• '+q),
    '', 'Customer notes',plan.notes||'None supplied.',
    '', 'Source listings',...[...new Set(a.lines.map(l=>l.product.source_url))],
    '', 'Independent Aurekai planning preview. No order, reservation or supplier approval.'].join('\n');
}

export function planReviewForm(catalogue,page) {
  return {page,max_bytes:24000,fields:{Name:{max_length:100},Email:{max_length:254,email:true},'Fixture plan':{max_length:16000}},required:['Name','Email','Fixture plan'],planner:catalogue};
}

export function planRequest(plan,catalogue,{form,page,name,email}) {
  validatePlan(plan,catalogue);
  if (!name.trim() || name.length>100 || !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email.trim())) throw Error('Add your name and email so we can reply.');
  return JSON.stringify({form,page,fields:[{label:'Name',value:name.trim()},{label:'Email',value:email.trim()},{label:'Fixture plan',value:JSON.stringify(plan)}]});
}
