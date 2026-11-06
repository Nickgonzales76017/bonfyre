import {fromKg} from './training-model.mjs';
const format=n=>Number(n).toLocaleString(undefined,{maximumFractionDigits:2});
export function localDay(now=new Date()){return `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`;}
export function weightPanel(state){
  const rows=[...(state.bodyweight??[])].sort((a,b)=>a.day.localeCompare(b.day)),recent=rows.slice(-30),last=rows.at(-1),today=localDay(),current=rows.find(w=>w.day===today);
  let chart='';
  if(recent.length){
    const values=recent.map(w=>fromKg(w.weightKg,state.unit)),low=Math.min(...values)-0.5,high=Math.max(...values)+0.5;
    const firstDate=Date.parse(recent[0].day),lastDate=Date.parse(recent.at(-1).day),spread=lastDate-firstDate;
    const points=recent.map((w,i)=>({x:spread?48+(Date.parse(w.day)-firstDate)*484/spread:290,y:26+(high-values[i])*152/(high-low),label:`${w.day}: ${format(values[i])} ${state.unit}`}));
    chart=`<svg class="weight-chart" viewBox="0 0 580 220" role="img" aria-label="Body-weight history in ${state.unit}"><line x1="48" x2="532" y1="178" y2="178" class="chart-axis"/><text x="2" y="32">${format(high)}</text><text x="2" y="178">${format(low)}</text><polyline points="${points.map(p=>`${p.x},${p.y}`).join(' ')}" class="chart-line"/>${points.map(p=>`<circle cx="${p.x}" cy="${p.y}" r="4"><title>${p.label}</title></circle>`).join('')}<text x="48" y="210">${recent[0].day}</text><text x="532" y="210" text-anchor="end">${recent.at(-1).day}</text></svg>`;
  }
  return `<section class="panel weight-panel"><div class="section-title"><h2>Body weight</h2>${last?`<span>${format(fromKg(last.weightKg,state.unit))} ${state.unit} · ${last.day}</span>`:''}</div>${chart}<form id="weight-form"><div class="fields"><label>Date<input name="day" type="date" required value="${today}"></label><label>Weight (${state.unit})<input name="weight" type="number" step="any" min="0.1" max="${state.unit==='kg'?1000:2204}" required value="${current?Math.round(fromKg(current.weightKg,state.unit)*1000)/1000:''}"></label></div><button>Save weight</button></form>${rows.length?`<details class="weight-entries"><summary>${rows.length} ${rows.length===1?'entry':'entries'}</summary>${[...rows].reverse().slice(0,90).map(w=>`<div class="weight-row"><span>${w.day}</span><strong>${format(fromKg(w.weightKg,state.unit))} ${state.unit}</strong><button type="button" class="text-button" data-action="weight.edit" data-day="${w.day}">Edit</button><button type="button" class="text-button" data-action="weight.remove" data-day="${w.day}">Remove</button></div>`).join('')}</details>`:'<p class="muted">Your entries stay with your training history.</p>'}</section>`;
}
