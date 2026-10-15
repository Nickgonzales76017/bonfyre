import {validatePlan} from './planner-model.mjs';
export const quarter = n => Math.round(n * 4) / 4;
export function moveFixtures(plan, catalogue, keys, dx, dy) {
  const next = structuredClone(plan), chosen = next.items.filter(i => keys.includes(i.key));
  if (!chosen.length || !Number.isFinite(dx) || !Number.isFinite(dy)) throw Error('Select fixtures to move.');
  dx = Math.max(-Math.min(...chosen.map(i=>i.x)), Math.min(60-Math.max(...chosen.map(i=>i.x)), quarter(dx)));
  dy = Math.max(-Math.min(...chosen.map(i=>i.y)), Math.min(60-Math.max(...chosen.map(i=>i.y)), quarter(dy)));
  chosen.forEach(i => {i.x += dx; i.y += dy;}); return validatePlan(next,catalogue);
}
export function editFixtures(plan, catalogue, keys, action, newKey = () => crypto.randomUUID()) {
  const next = structuredClone(plan), chosen = next.items.filter(i => keys.includes(i.key));
  if (!chosen.length) throw Error('Select one or more fixtures.');
  if (action === 'duplicate') {
    if (next.items.length + chosen.length > 12) throw Error('This plan holds up to twelve fixtures.');
    next.items.push(...chosen.map(i => ({...i,key:newKey(),x:Math.min(60,i.x+.5),y:Math.min(60,i.y+.5)})));
  } else if (action === 'remove') {
    if (chosen.length === next.items.length) throw Error('Keep at least one fixture in the plan.');
    next.items = next.items.filter(i => !keys.includes(i.key));
  } else if (action === 'rotate') chosen.forEach(i=>i.rotation=i.rotation===90?0:90);
  else if (action === 'left') {const x=Math.min(...chosen.map(i=>i.x));chosen.forEach(i=>i.x=x);}
  else if (action === 'top') {const y=Math.min(...chosen.map(i=>i.y));chosen.forEach(i=>i.y=y);}
  else if (action === 'distribute-x' || action === 'distribute-y') {
    if (chosen.length < 3) throw Error('Select at least three fixtures to distribute.');
    const axis=action==='distribute-x'?'x':'y',sorted=[...chosen].sort((a,b)=>a[axis]-b[axis]),lo=sorted[0][axis],hi=sorted.at(-1)[axis];
    sorted.forEach((item,index)=>item[axis]=quarter(lo+(hi-lo)*index/(sorted.length-1)));
  } else throw Error('Unknown fixture action.');
  return validatePlan(next,catalogue);
}
export function displayFootprint(product,item) {
  const size=product.allowance;
  return size?{w:(item.rotation?size.depth:size.width)/12,h:(item.rotation?size.width:size.depth)/12,known:Boolean(product.dimensions?.width&&product.dimensions?.depth)}:{w:.5,h:.5,known:false};
}
