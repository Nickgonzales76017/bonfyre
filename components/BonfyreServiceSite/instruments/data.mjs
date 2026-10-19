/** Bounded transport readers. They do not own persistence or external effects. */
export function text(value, label='Value', limit=10000) {
  if(typeof value!=='string'||value.length>limit||value.includes('\0'))throw Error(`${label} must be text of at most ${limit} characters without NULs.`);
  return value;
}
export function number(value,label,{min=0,max=1e12,integer=false}={}) {
  if(value===null||value===undefined||typeof value==='boolean'||typeof value==='object'||typeof value==='string'&&(!value.trim()||!/^[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?$/.test(value.trim())))throw Error(`${label} is missing.`);
  const n=Number(value);
  if(!Number.isFinite(n)||n<min||n>max||(integer&&!Number.isSafeInteger(n)))throw Error(`${label} must be ${integer?'an integer':'a number'} between ${min} and ${max}.`);
  return n;
}
export const escapeHTML=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
export function safeURL(value) { try {const u=new URL(value);return ['http:','https:'].includes(u.protocol)&&!u.username&&!u.password?u.href:null;}catch{return null;} }
export function csvRead(input,{maxRows=100000,maxBytes=16*1024*1024}={}){
  text(input,'Import',maxBytes);if(new TextEncoder().encode(input).length>maxBytes)throw Error('Import exceeds its byte limit.');
  input=input.replace(/^\uFEFF/,'');const lines=[];let row=[],field='',quoted=false,after=false;
  const cell=()=>{row.push(field);field='';after=false;if(row.length>256)throw Error('Too many columns.');};
  const line=()=>{cell();if(row.some(v=>v!==''))lines.push(row);row=[];if(lines.length>maxRows+1)throw Error('Too many rows.');};
  for(let i=0;i<input.length;i++){
    const c=input[i];
    if(quoted){if(c==='"'){if(input[i+1]==='"'){field+='"';i++;}else{quoted=false;after=true;}}else field+=c;continue;}
    if(c===','){cell();continue;}if(c==='\n'||c==='\r'){if(c==='\r'&&input[i+1]==='\n')i++;line();continue;}
    if(after)throw Error('Unexpected text after a quoted CSV field.');
    if(c==='"'){if(field)throw Error('A quote must start a CSV field.');quoted=true;}else field+=c;
  }
  if(quoted)throw Error('Unclosed quoted CSV field.');if(field||row.length||after)line();
  if(!lines.length)throw Error('The import has no header.');
  const headers=lines.shift().map(h=>h.trim());
  if(headers.some(h=>!h)||new Set(headers).size!==headers.length)throw Error('Header names must be nonempty and unique.');
  return {headers,rows:lines.map((values,i)=>({row:i+2,values,error:values.length===headers.length?null:`Expected ${headers.length} fields; received ${values.length}.`})),records:lines.filter(v=>v.length===headers.length).map(v=>Object.fromEntries(headers.map((h,i)=>[h,v[i]])))};
}
export function csvWrite(headers,rows){const cell=v=>'"'+String(v??'').replaceAll('"','""')+'"';return [headers,...rows].map(r=>r.map(cell).join(',')).join('\r\n')+'\r\n';}
export function quantile(values,p){if(!values.length)return null;const a=[...values].sort((a,b)=>a-b),x=(a.length-1)*p,i=Math.floor(x);return a[i]+(a[Math.min(i+1,a.length-1)]-a[i])*(x-i);}
export function isoTime(value){
  text(value,'Timestamp',64);const m=/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?(Z|[+-]\d{2}:\d{2})$/.exec(value);
  if(!m)throw Error('Use an ISO timestamp with seconds and an explicit timezone.');
  const [,y,mo,d,h,mi,s,,zone]=m;const year=+y,month=+mo,day=+d;
  const leap=year%4===0&&(year%100!==0||year%400===0),days=[31,leap?29:28,31,30,31,30,31,31,30,31,30,31];
  if(month<1||month>12||day<1||day>days[month-1]||+h>23||+mi>59||+s>59||zone!=='Z'&&(+zone.slice(1,3)>23||+zone.slice(4)>59))throw Error('Timestamp has an impossible calendar value.');
  const n=Date.parse(value);if(!Number.isSafeInteger(n))throw Error('Timestamp is outside the supported range.');return n;
}
export async function digest(value){return [...new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value)))].map(b=>b.toString(16).padStart(2,'0')).join('');}
