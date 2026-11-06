// Deterministic checks over supplied tables. Contract declarations, uploaded
// values and draft results never grant provider or operational authority.
export const dataChecksSchema='bonfyre.data-checks-contract.v1';
export const dataChecksLimits=Object.freeze({entities:16,rowsPerTable:1000,totalRows:4000,tableBytes:192*1024,recordBytes:240*1024,candidates:5000,resultRows:30000});
const utf8=new TextEncoder(),hashPattern=/^[a-f0-9]{64}$/;
const fail=(ok,message)=>{if(!ok)throw Error(message);};
const isText=(v,n=200)=>typeof v==='string'&&v.length<=n&&!/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(v);
const coordinate=v=>isText(v,200)&&/^[A-Za-z0-9][A-Za-z0-9:._-]*$/.test(v);
const plainObject=v=>v&&[Object.prototype,null].includes(Object.getPrototypeOf(v));
const closed=(v,required,optional=[])=>plainObject(v)&&required.every(k=>Object.hasOwn(v,k))&&Object.keys(v).every(k=>required.includes(k)||optional.includes(k));
const finite=v=>typeof v==='number'&&Number.isFinite(v);
const boundedArray=(v,n)=>Array.isArray(v)&&v.length<=n;
function snapshot(value,max=dataChecksLimits.recordBytes){
  let nodes=0,bytes=0;const active=new Set();
  function walk(v,depth=0){
    fail(depth<=24&&++nodes<=(max>dataChecksLimits.recordBytes?600000:60000),'Data Checks material exceeds its nesting or item limit.');
    if(v===null||typeof v==='boolean'){bytes++;return;}
    if(typeof v==='number'){fail(finite(v)&&(!Number.isInteger(v)||Number.isSafeInteger(v)),'Preserve large integers as decimal text.');bytes+=9;return;}
    if(typeof v==='string'){fail(!v.includes('\0'),'NUL is not supported in Data Checks text.');bytes+=utf8.encode(v).length+5;fail(bytes<=max,'Data Checks material exceeds its byte limit.');return;}
    fail(v&&typeof v==='object'&&!active.has(v)&&(Array.isArray(v)||plainObject(v)),'Data Checks accepts plain acyclic data only.');
    const array=Array.isArray(v),keys=Reflect.ownKeys(v),descriptors=Object.getOwnPropertyDescriptors(v);
    fail(keys.every(k=>typeof k==='string'&&(array&&k==='length'||descriptors[k].enumerable&&Object.hasOwn(descriptors[k],'value')))&&(!array||keys.length===v.length+1),'Data Checks properties must be ordinary dense data.');
    fail(keys.length<=12000,'Data Checks material has too many properties.');active.add(v);bytes+=5;
    for(const key of keys){if(array&&key==='length')continue;fail(!array||/^(0|[1-9][0-9]*)$/.test(key)&&Number(key)<v.length,'Data Checks arrays must be dense.');if(!array)bytes+=utf8.encode(key).length+5;walk(descriptors[key].value,depth+1);}
    active.delete(v);fail(bytes<=max,'Data Checks material exceeds its byte limit.');
  }
  walk(value);return structuredClone(value);
}
// Explicit scalar tags keep canonical row identity byte-complete, including -0.
const ordered=v=>v===null?['null']:typeof v==='number'?['number',Object.is(v,-0)?'-0':String(v)]:typeof v==='string'?['string',v]:typeof v==='boolean'?['boolean',v]:Array.isArray(v)?['array',v.map(ordered)]:['map',Object.keys(v).sort().map(k=>[k,ordered(v[k])])];
async function sha(bytes){return [...new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))].map(v=>v.toString(16).padStart(2,'0')).join('');}
const valueHash=v=>sha(utf8.encode(JSON.stringify(ordered(v))));
const distinct=(rows,label)=>{const ids=new Set();for(const row of rows){fail(coordinate(row.id)&&!ids.has(row.id),'Use distinct '+label+' identities.');ids.add(row.id);}return ids;};
const fieldTypes=new Set(['string','symbol','hash','currency','date','float64','int64','time_ns']);
const numericTypes=new Set(['float64','int64','time_ns']);

export function validateDataChecksContract(input){
  const c=snapshot(input);fail(closed(c,['schema','id','version','sources','entities','fields','relations','predicates','closures','labels'],['materialization','evidence_budget','numeric_compression'])&&c.schema===dataChecksSchema&&coordinate(c.id)&&coordinate(c.version),'Unsupported Data Checks contract.');
  for(const [name,limit]of [['sources',16],['entities',16],['fields',256],['relations',64],['predicates',256],['closures',128],['labels',256]])fail(boundedArray(c[name],limit),'The contract has too many '+name+'.');
  const sources=distinct(c.sources,'source'),entities=distinct(c.entities,'entity'),fieldIds=distinct(c.fields,'field'),relations=distinct(c.relations,'relation'),predicates=distinct(c.predicates,'predicate');distinct(c.closures,'closure');distinct(c.labels,'label');
  const names=new Map(c.entities.map(e=>[e.id,new Map()]));
  const witness=(v)=>{fail(boundedArray(v,16),'Too many declared witness requirements.');for(const x of v)fail(closed(x,['kind'])&&['sheet_row','transcript_segment','chat_message','media_probe','materialization_plan'].includes(x.kind),'Unsupported witness declaration.');};
  for(const s of c.sources){fail(closed(s,['id','role','kind','witness_requirements'])&&coordinate(s.role)&&['tabular','transcript','chat','media_probe'].includes(s.kind),'Unsupported source declaration.');witness(s.witness_requirements);}
  for(const e of c.entities)fail(closed(e,['id','source','role','key'])&&sources.has(e.source)&&coordinate(e.role)&&coordinate(e.key),'An entity requires a declared source and key.');
  for(const f of c.fields){fail(closed(f,['id','entity','name','type','required'],['latent_role','numeric_encoding'])&&entities.has(f.entity)&&coordinate(f.name)&&fieldTypes.has(f.type)&&typeof f.required==='boolean'&&!names.get(f.entity).has(f.name),'Unsupported or repeated field declaration.');if(Object.hasOwn(f,'latent_role'))fail(coordinate(f.latent_role),'Invalid latent role declaration.');if(Object.hasOwn(f,'numeric_encoding'))fail(['auto','q7','q10','float64'].includes(f.numeric_encoding),'Unsupported numeric encoding declaration.');names.get(f.entity).set(f.name,f);}
  for(const e of c.entities)fail(names.get(e.id).get(e.key)?.required===true,'Declare a required key field for every entity.');
  const ref=(r,allowed=null)=>{fail(closed(r,['entity','field'])&&names.get(r.entity)?.has(r.field)&&(!allowed||allowed.includes(r.entity)),'A predicate or join refers to an undeclared field.');return names.get(r.entity).get(r.field);};
  const operand=(o,allowed=null)=>{if(closed(o,['value']))fail(o.value===null||['string','boolean'].includes(typeof o.value)||finite(o.value),'Use an ordinary scalar comparison value.');else ref(o,allowed);};
  function strategy(s,left,right,depth=0){
    fail(plainObject(s)&&depth<=1,'Unsupported candidate strategy.');
    if(s.kind==='key_eq'){fail(closed(s,['kind','left','right']),'Unsupported exact join fields.');ref(s.left,[left]);ref(s.right,[right]);return;}
    if(s.kind==='cross'){fail(closed(s,['kind']),'Unsupported cross join fields.');return;}
    if(s.kind==='hybrid'){fail(closed(s,['kind','strategies'])&&boundedArray(s.strategies,8)&&s.strategies.length>0,'A hybrid declaration needs bounded strategies.');for(const x of s.strategies)strategy(x,left,right,depth+1);return;}
    fail(s.kind==='latent_nearest'&&closed(s,['kind','threshold','limit'],['left','right','left_fields','right_fields','boosts','lexical_weight','base_weight'])&&finite(s.threshold)&&s.threshold>=0&&s.threshold<=1&&Number.isInteger(s.limit)&&s.limit>0&&s.limit<=100,'Unsupported latent candidate declaration.');
    fail(Object.hasOwn(s,'left')===Object.hasOwn(s,'right')&&Object.hasOwn(s,'left_fields')===Object.hasOwn(s,'right_fields')&&(s.left||s.left_fields),'Declare both latent input sides.');if(s.left){ref(s.left,[left]);ref(s.right,[right]);}
    if(s.left_fields)for(const [side,entity]of [['left_fields',left],['right_fields',right]]){fail(boundedArray(s[side],16)&&s[side].length>0&&new Set(s[side]).size===s[side].length,'Unsupported latent input list.');for(const f of s[side])ref({entity,field:f});}
    for(const k of ['lexical_weight','base_weight'])if(Object.hasOwn(s,k))fail(finite(s[k])&&s[k]>=0&&s[k]<=1,'Unsupported latent weighting declaration.');
    if(s.boosts){fail(boundedArray(s.boosts,16),'Too many latent boost declarations.');for(const b of s.boosts){fail(closed(b,['kind','left','right','amount'])&&['eq','numeric_eq'].includes(b.kind)&&finite(b.amount)&&b.amount>=0&&b.amount<=1,'Unsupported latent boost declaration.');ref(b.left,[left]);ref(b.right,[right]);}}
  }
  for(const r of c.relations){
    fail(closed(r,['id','left_entity','right_entity','threshold','candidate_strategy'],['gates','ambiguity_epsilon','unmatched_left_label','unmatched_right_label','uncertain_label','ambiguous_label'])&&entities.has(r.left_entity)&&entities.has(r.right_entity)&&r.left_entity!==r.right_entity&&finite(r.threshold)&&r.threshold>=0&&r.threshold<=1,'Unsupported relation declaration.');strategy(r.candidate_strategy,r.left_entity,r.right_entity);
    if(r.gates)fail(Array.isArray(r.gates)&&r.gates.length===0,'This Data Checks version requires explicit predicate closures rather than relation gates.');if(Object.hasOwn(r,'ambiguity_epsilon'))fail(finite(r.ambiguity_epsilon)&&r.ambiguity_epsilon>=0&&r.ambiguity_epsilon<=1,'Invalid ambiguity tolerance.');for(const k of ['unmatched_left_label','unmatched_right_label','uncertain_label','ambiguous_label'])if(Object.hasOwn(r,k))fail(coordinate(r[k]),'Invalid relation label.');
  }
  for(const p of c.predicates){
    if(p.kind==='exists')fail(closed(p,['id','kind','field']),'Unsupported existence predicate.');
    else if(p.kind==='numeric_range'){fail(closed(p,['id','kind','field'],['min','max'])&&(Object.hasOwn(p,'min')||Object.hasOwn(p,'max')),'A numeric range needs a bound.');for(const k of ['min','max'])if(Object.hasOwn(p,k))fail(finite(p[k]),'Use finite numeric bounds.');if(Object.hasOwn(p,'min')&&Object.hasOwn(p,'max'))fail(p.min<=p.max,'The numeric range is reversed.');}
    else if(['eq','lt','lte','within'].includes(p.kind)){fail(closed(p,['id','kind','lhs','rhs'],p.kind==='within'?['tolerance']:[]),'Unsupported comparison fields.');operand(p.lhs);operand(p.rhs);if(p.kind==='within')fail(finite(p.tolerance)&&p.tolerance>=0,'Use a nonnegative tolerance.');}
    else if(p.kind==='member')fail(closed(p,['id','kind','field','set'])&&boundedArray(p.set,64)&&p.set.length>0&&p.set.every(v=>typeof v==='string'&&isText(v,1000)||finite(v)||typeof v==='boolean'),'Use a bounded scalar membership set.');
    else if(p.kind==='requires')fail(closed(p,['id','kind','when_predicate','field'])&&predicates.has(p.when_predicate)&&p.when_predicate!==p.id,'A requirement needs a declared condition.');
    else if(p.kind==='interval_non_overlap'){fail(closed(p,['id','kind','start','end']),'Unsupported interval policy.');ref(p.start);ref(p.end);fail(p.start.entity===p.end.entity,'Intervals must belong to the same entity.');}
    else fail(['relation_not_ambiguous','relation_threshold'].includes(p.kind)&&closed(p,['id','kind','relation'])&&closed(p.relation,['relation'])&&relations.has(p.relation.relation),'Unsupported predicate kind.');
    if(p.field)ref(p.field);
  }
  const pmap=new Map(c.predicates.map(p=>[p.id,p]));
  function references(id,seen=new Set()){
    fail(!seen.has(id),'Conditional predicates cannot form a cycle.');seen.add(id);const p=pmap.get(id),out=[];for(const k of ['field','lhs','rhs','start','end'])if(p[k]?.entity)out.push(p[k].entity);if(p.kind==='requires')out.push(...references(p.when_predicate,new Set(seen)));return out;
  }
  for(const p of c.predicates)references(p.id);
  for(const cl of c.closures){
    fail(closed(cl,['id','relation','when_all_pass','status_on_pass','status_on_fail','human_label_on_pass'])&&relations.has(cl.relation)&&boundedArray(cl.when_all_pass,64)&&cl.when_all_pass.length>0&&new Set(cl.when_all_pass).size===cl.when_all_pass.length&&cl.when_all_pass.every(id=>predicates.has(id))&&['closed','command','materialization_required'].includes(cl.status_on_pass)&&cl.status_on_fail==='residual'&&coordinate(cl.human_label_on_pass),'Unsupported closure declaration.');
    const relation=c.relations.find(r=>r.id===cl.relation),allowed=[relation.left_entity,relation.right_entity];for(const id of cl.when_all_pass){fail(references(id).every(e=>allowed.includes(e)),'A closure references a field outside its relation.');const p=pmap.get(id);if(p.relation)fail(p.relation.relation===cl.relation,'A closure cannot borrow another relation result.');}
  }
  for(const l of c.labels)fail(closed(l,['id','when_failure','human_label'])&&closed(l.when_failure,['predicate_id'],['failure_class'])&&(predicates.has(l.when_failure.predicate_id)||relations.has(l.when_failure.predicate_id))&&(!l.when_failure.failure_class||['missing_evidence','type_error','predicate_false'].includes(l.when_failure.failure_class))&&coordinate(l.human_label),'Unsupported failure label.');
  if(c.materialization){const m=c.materialization;fail(closed(m,['target_protocol','required_closures','required_source_roles','planned_steps','witness_schema','required_fields'])&&coordinate(m.target_protocol),'Unsupported materialization declaration.');for(const k of ['required_closures','required_source_roles','planned_steps','required_fields'])fail(boundedArray(m[k],64),'Too many materialization declarations.');fail(m.required_closures.every(id=>c.closures.some(x=>x.id===id))&&m.required_source_roles.every(role=>c.sources.some(x=>x.role===role))&&m.planned_steps.every(coordinate),'Materialization refers to an undeclared closure or source.');witness(m.witness_schema);m.required_fields.forEach(r=>ref(r));}
  if(c.evidence_budget){const b=c.evidence_budget;fail(closed(b,['max_intermediate_bytes','max_measurement_cost','max_provider_calls'],['forbidden_fields']),'Unsupported declared evidence budget.');for(const k of ['max_intermediate_bytes','max_measurement_cost','max_provider_calls'])fail(Number.isSafeInteger(b[k])&&b[k]>=0&&b[k]<=1e9,'Invalid declared budget.');if(b.forbidden_fields)fail(boundedArray(b.forbidden_fields,64)&&b.forbidden_fields.every(coordinate),'Invalid forbidden-field declaration.');}
  if(c.numeric_compression){const n=c.numeric_compression;fail(closed(n,['field_overrides'],['default_float_encoding'])&&plainObject(n.field_overrides)&&Object.keys(n.field_overrides).length<=256,'Unsupported numeric encoding declaration.');if(n.default_float_encoding)fail(['auto','q7','q10','float64'].includes(n.default_float_encoding),'Unsupported default encoding.');for(const [key,value]of Object.entries(n.field_overrides))fail(coordinate(key)&&['auto','q7','q10','float64'].includes(value),'Unsupported field encoding declaration.');}
  return c;
}
export function blankDataChecks(title='Data checks'){
  return {kind:'checks',title,contract:{schema:dataChecksSchema,id:'new-data-checks',version:'1',sources:[],entities:[],fields:[],relations:[],predicates:[],closures:[],labels:[]},tables:[],selected:null};
}
function columnsAndRows(body){
  fail(closed(body,['columns','rows'])&&boundedArray(body.columns,256)&&body.columns.length>0&&body.columns.every(h=>isText(h,200)&&h.trim())&&new Set(body.columns).size===body.columns.length&&boundedArray(body.rows,dataChecksLimits.rowsPerTable),'Use distinct bounded columns and rows.');
  for(const row of body.rows)fail(Array.isArray(row)&&row.length===body.columns.length&&row.every(v=>v===null||typeof v==='boolean'||typeof v==='string'&&isText(v,20000)||finite(v)&&(!Number.isInteger(v)||Number.isSafeInteger(v))),'Each input row must contain bounded ordinary scalar cells.');return body;
}
async function tableData(contract,table,{parseCSV}={}){
  fail(closed(table,['entity','name','format','body','source_sha256','mapping','example'])&&contract.entities.some(e=>e.id===table.entity)&&isText(table.name,200)&&table.name.trim()&&['csv','rows'].includes(table.format)&&hashPattern.test(table.source_sha256)&&typeof table.example==='boolean'&&plainObject(table.mapping),'Unsupported saved input table.');
  let body,hash,rowNumbers;
  if(table.format==='csv'){
    fail(typeof table.body==='string'&&utf8.encode(table.body).length<=dataChecksLimits.tableBytes&&typeof parseCSV==='function','Use the retained CSV reader for this bounded input.');hash=await sha(utf8.encode(table.body));
    const parsed=parseCSV(table.body,{maxRows:dataChecksLimits.rowsPerTable,maxBytes:dataChecksLimits.tableBytes});fail(parsed&&Array.isArray(parsed.headers)&&Array.isArray(parsed.rows)&&parsed.rows.every(row=>!row.error&&Number.isSafeInteger(row.row)&&row.row>=2&&row.row<=dataChecksLimits.rowsPerTable+1),'The CSV has a malformed row. Correct its field count before retaining it.');body=columnsAndRows({columns:parsed.headers,rows:parsed.rows.map(row=>row.values)});rowNumbers=parsed.rows.map(row=>row.row);fail(new Set(rowNumbers).size===rowNumbers.length,'The CSV reader repeated a source row.');
  }else{body=columnsAndRows(table.body);hash=await valueHash(body);rowNumbers=body.rows.map((_,i)=>i+1);}
  fail(hash===table.source_sha256,'The retained input hash does not match its actual table bytes.');
  const fields=contract.fields.filter(f=>f.entity===table.entity);for(const [field,column]of Object.entries(table.mapping))fail(fields.some(f=>f.name===field)&&body.columns.includes(column),'Map declared fields to actual input columns.');
  return {table,body,rowNumbers,input:{entity:table.entity,name:table.name,format:table.format,source_sha256:hash,mapping_sha256:await valueHash({entity:table.entity,mapping:table.mapping}),example:table.example,rows:body.rows.length,state:body.rows.length?'ready':'needs_input'}};
}
export async function importDataChecksCSV(contract,entity,text,{name='Uploaded table.csv',example=false,mapping={},parseCSV}={}){
  const c=validateDataChecksContract(contract),body=snapshot(text,dataChecksLimits.tableBytes),keptMapping=snapshot(mapping),table={entity,name,format:'csv',body,source_sha256:await sha(utf8.encode(body)),mapping:keptMapping,example};await tableData(c,table,{parseCSV});return table;
}
export async function importDataChecksRows(contract,entity,rows,{name='Mapped input rows',example=false,mapping={}}={}){
  const c=validateDataChecksContract(contract),body=columnsAndRows(snapshot(rows,dataChecksLimits.tableBytes)),keptMapping=snapshot(mapping),table={entity,name,format:'rows',body,source_sha256:await valueHash(body),mapping:keptMapping,example};await tableData(c,table);return table;
}
async function inspectRecord(input,options){
  const record=snapshot(input);fail(closed(record,['kind','title','contract','tables','selected'])&&record.kind==='checks'&&isText(record.title,200)&&record.title.trim()&&boundedArray(record.tables,16),'Unsupported Data Checks record.');const contract=validateDataChecksContract(record.contract);
  fail(record.selected===null||contract.entities.some(e=>e.id===record.selected),'Select a declared input entity.');fail(new Set(record.tables.map(t=>t.entity)).size===record.tables.length,'Use one retained table per input entity.');
  const tables=[];let count=0;for(const table of record.tables){const data=await tableData(contract,table,options);count+=data.body.rows.length;fail(count<=dataChecksLimits.totalRows,'Narrow the input tables before checking them.');tables.push(data);}return {record,contract,tables};
}
export async function validateDataChecksRecord(record,options={}){return (await inspectRecord(record,options)).record;}

const missing=v=>v===undefined||v===null||typeof v==='string'&&!v.trim();
function numeric(raw,{integer=false}={}){
  if(missing(raw)||typeof raw==='boolean'||typeof raw==='object')return null;
  if(integer){if(typeof raw==='number'&&!Number.isSafeInteger(raw))return null;const text=String(raw).trim();if(!/^[+-]?\d+$/.test(text))return null;const n=BigInt(text);return n>=-(1n<<63n)&&n<(1n<<63n)?n:null;}
  const text=String(raw).trim();if(!/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(text))return null;const n=Number(text);return Number.isFinite(n)?n:null;
}
function day(raw){if(typeof raw!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(raw)||raw.startsWith('0000'))return null;const n=Date.parse(raw+'T00:00:00.000Z');return Number.isFinite(n)&&new Date(n).toISOString().slice(0,10)===raw?n/86400000:null;}
function typed(field,raw){
  if(missing(raw))return {ok:!field.required,missing:true,raw,field,value:null,reason:field.required?'required_value_missing':'optional_value_absent'};
  let value=raw,ok=true;
  if(numericTypes.has(field.type)){value=numeric(raw,{integer:field.type!=='float64'});ok=value!==null;}
  else {ok=typeof raw==='string';if(ok&&field.type==='hash')ok=hashPattern.test(raw);if(ok&&field.type==='currency')ok=/^[A-Z]{3}$/.test(raw);if(ok&&field.type==='date'){value=day(raw);ok=value!==null;}if(ok&&field.type==='symbol')ok=coordinate(raw);}
  return {ok,missing:false,raw,field,value,reason:ok?'declared_type_valid':'invalid_'+field.type};
}
function compare(a,b){
  if(typeof a==='bigint'&&typeof b==='number'){if(!Number.isSafeInteger(b))return null;b=BigInt(b);}if(typeof b==='bigint'&&typeof a==='number'){if(!Number.isSafeInteger(a))return null;a=BigInt(a);}
  if(typeof a!==typeof b)return null;return a===b?0:a<b?-1:1;
}
const observed=v=>v===undefined?null:typeof v==='bigint'?v.toString():typeof v==='string'&&v.length>300?v.slice(0,300)+'… [truncated]':v;
const counts=rows=>({pass:rows.filter(r=>r.state==='pass').length,fail:rows.filter(r=>r.state==='fail').length,unresolved:rows.filter(r=>r.state==='unresolved').length});
const stateOf=rows=>rows.some(r=>r.state==='fail')?'fail':rows.some(r=>r.state==='unresolved')?'unresolved':'pass';

export async function evaluateDataChecks(input,options={}){
  const {record,contract:c,tables}=await inspectRecord(input,options),contract_sha256=await valueHash(c),tableMap=new Map(tables.map(t=>[t.table.entity,t])),byEntity=new Map(),field_checks=[],checks=[],relations=[],closures=[],draft_outputs=[],inputs=[];
  let resultRows=0;const append=(list,row)=>{fail(++resultRows<=dataChecksLimits.resultRows,'Narrow the tables or contract before producing more check rows.');list.push(row);return row;};
  for(const entity of c.entities){
    const source=tableMap.get(entity.id),fields=c.fields.filter(f=>f.entity===entity.id),rows=[];inputs.push(source?.input??{entity:entity.id,name:null,format:null,source_sha256:null,mapping_sha256:null,example:false,rows:0,state:'needs_input'});
    if(!source?.body.rows.length){for(const field of fields)append(field_checks,{id:field.id,entity:entity.id,field:field.name,state:'unresolved',reason:'needs_input',inputs:[]});byEntity.set(entity.id,rows);continue;}
    for(const [index,cells]of source.body.rows.entries()){
      const values=new Map();for(const field of fields){const column=source.table.mapping[field.name]??field.name,at=source.body.columns.indexOf(column);values.set(field.name,typed(field,at<0?undefined:cells[at]));}
      const key=values.get(entity.key),identity={entity:entity.id,key:key?.ok&&!key.missing?String(key.raw):null,row:source.rowNumbers[index],source_sha256:source.table.source_sha256,example:source.table.example};rows.push({identity,values,valid:true});
    }
    const keys=new Map();for(const row of rows){const key=row.identity.key;if(key!==null)keys.set(key,(keys.get(key)??0)+1);}
    for(const row of rows)for(const field of fields){const value=row.values.get(field.name),duplicate=field.name===entity.key&&row.identity.key!==null&&keys.get(row.identity.key)>1,state=!value.ok||duplicate?'fail':'pass';if(state==='fail')row.valid=false;append(field_checks,{id:field.id,entity:entity.id,field:field.name,state,reason:duplicate?'duplicate_entity_key':value.reason,inputs:[row.identity]});}
    byEntity.set(entity.id,rows);
  }
  const relationWork=new Map();let candidateCount=0;
  for(const relation of c.relations){
    const left=byEntity.get(relation.left_entity),right=byEntity.get(relation.right_entity),pairs=new Map(),unresolved=[];let complete=!!left.length&&!!right.length;
    if(!complete)unresolved.push('needs_input');
    const offer=(l,r,score,method)=>{const key=l.identity.row+':'+r.identity.row;if(!pairs.has(key)){fail(++candidateCount<=dataChecksLimits.candidates,'Narrow the join inputs before creating more candidates.');pairs.set(key,{id:relation.id+':'+key,left:l,right:r,score,method});}};
    function join(strategy){
      if(strategy.kind==='hybrid'){strategy.strategies.forEach(join);return;}
      if(strategy.kind==='latent_nearest'){complete=false;unresolved.push('latent_matching_not_run');return;}
      if(strategy.kind==='cross'){for(const l of left)for(const r of right)offer(l,r,relation.threshold===0?0:null,'cross');return;}
      const keyOf=(row,field)=>{const v=row.values.get(field);if(!v?.ok||v.missing){complete=false;if(!unresolved.includes('invalid_join_input'))unresolved.push('invalid_join_input');return null;}const value=v.value;if(typeof value==='number'||typeof value==='bigint')return 'n:'+String(value);return typeof value+':'+String(value);};
      const index=new Map();for(const r of right){const key=keyOf(r,strategy.right.field);if(key!==null){const list=index.get(key)??[];list.push(r);index.set(key,list);}}
      for(const l of left){const key=keyOf(l,strategy.left.field);if(key!==null)for(const r of index.get(key)??[])offer(l,r,1,'key_eq');}
    }
    join(relation.candidate_strategy);const candidates=[...pairs.values()],matchedLeft=new Set(candidates.map(p=>p.left)),matchedRight=new Set(candidates.map(p=>p.right)),unmatched_left=left.filter(r=>!matchedLeft.has(r)).map(r=>r.identity),unmatched_right=right.filter(r=>!matchedRight.has(r)).map(r=>r.identity);
    const projected={id:relation.id,state:!complete?'unresolved':unmatched_left.length||relation.unmatched_right_label&&unmatched_right.length?'fail':'pass',complete,candidate_count:candidates.length,unresolved:[...new Set(unresolved)],candidates:candidates.map(p=>({id:p.id,left:p.left.identity,right:p.right.identity,score:p.score,method:p.method})),unmatched_left,unmatched_right};relations.push(projected);relationWork.set(relation.id,{definition:relation,left,right,candidates,complete,projected});
  }
  const pmap=new Map(c.predicates.map(p=>[p.id,p])),intervals=new Map();
  function intervalStates(predicate){
    if(intervals.has(predicate.id))return intervals.get(predicate.id);const rows=byEntity.get(predicate.start.entity),valid=[],states=new Map();let unknown=false;
    for(const row of rows){const a=row.values.get(predicate.start.field),b=row.values.get(predicate.end.field),start=a?.ok&&!a.missing?numeric(a.raw,{integer:a.field.type!=='float64'}):null,end=b?.ok&&!b.missing?numeric(b.raw,{integer:b.field.type!=='float64'}):null,order=start!==null&&end!==null?compare(start,end):null;
      if(order===null){states.set(row,{state:'unresolved',reason:'invalid_interval_input'});unknown=true;}else if(order>=0)states.set(row,{state:'fail',reason:'invalid_interval'});else valid.push({row,start,end});}
    valid.sort((a,b)=>compare(a.start,b.start)??0);let max=null;const overlap=new Set();
    for(const interval of valid){if(max&&compare(interval.start,max.end)<0){overlap.add(interval.row);overlap.add(max.row);}if(!max||compare(interval.end,max.end)>0)max=interval;}
    for(const {row}of valid)states.set(row,overlap.has(row)?{state:'fail',reason:'interval_overlap'}:unknown?{state:'unresolved',reason:'other_interval_input_invalid'}:{state:'pass',reason:'intervals_do_not_overlap'});intervals.set(predicate.id,states);return states;
  }
  function evaluatePredicate(predicate,candidate,work,memo=new Map()){
    if(memo.has(predicate.id))return memo.get(predicate.id);const bindings=new Map([[candidate.left.identity.entity,candidate.left],[candidate.right.identity.entity,candidate.right]]),used=[],raw=[];
    const read=reference=>{if(Object.hasOwn(reference,'value'))return {ok:true,missing:missing(reference.value),value:reference.value,raw:reference.value,field:null};const row=bindings.get(reference.entity);if(row&&!used.includes(row.identity))used.push(row.identity);const value=row?.values.get(reference.field);raw.push({entity:reference.entity,field:reference.field,value:observed(value?.raw)});return value??{ok:false,missing:true,value:null,raw:undefined,field:null};};
    let state='unresolved',reason='missing_input';const set=(ok,yes='predicate_passed',no='predicate_false')=>{state=ok?'pass':'fail';reason=ok?yes:no;};
    if(predicate.kind==='exists'){const v=read(predicate.field);set(!v.missing,'value_present','required_value_missing');}
    else if(predicate.kind==='requires'){const condition=evaluatePredicate(pmap.get(predicate.when_predicate),candidate,work,memo),v=read(predicate.field);if(condition.state==='fail')set(true,'condition_not_required');else if(condition.state==='pass')set(!v.missing,'conditional_value_present','required_value_missing');else reason='condition_unresolved';}
    else if(predicate.kind==='numeric_range'){const v=read(predicate.field),value=numeric(v.raw,{integer:v.field?.type==='int64'||v.field?.type==='time_ns'});if(v.missing)reason='missing_input';else if(value===null)set(false,'','type_error');else {const lo=Object.hasOwn(predicate,'min')?compare(value,predicate.min):1,hi=Object.hasOwn(predicate,'max')?compare(value,predicate.max):-1;if(lo===null||hi===null)reason='numeric_precision_unresolved';else set(lo>=0&&hi<=0,'within_numeric_range','outside_numeric_range');}}
    else if(predicate.kind==='member'){const v=read(predicate.field);if(!v.missing&&v.ok)set(predicate.set.some(x=>x===v.value),'member_of_declared_set','not_in_declared_set');else if(!v.ok&&!v.missing)set(false,'','type_error');}
    else if(predicate.kind==='interval_non_overlap'){const row=bindings.get(predicate.start.entity);read(predicate.start);read(predicate.end);const found=intervalStates(predicate).get(row);if(found){state=found.state;reason=found.reason;}}
    else if(predicate.kind==='relation_not_ambiguous'){used.push(candidate.left.identity,candidate.right.identity);if(!work.complete)reason='candidate_search_unresolved';else set(work.candidates.filter(c=>c.left===candidate.left).length===1,'one_exact_candidate','ambiguous_candidates');}
    else if(predicate.kind==='relation_threshold'){used.push(candidate.left.identity,candidate.right.identity);if(!work.complete)reason='candidate_search_unresolved';else if(candidate.score===null)reason='strategy_has_no_measured_score';else set(candidate.score>=work.definition.threshold,'candidate_threshold_met','candidate_below_threshold');}
    else {
      const a=read(predicate.lhs),b=read(predicate.rhs);if(a.missing||b.missing)reason='missing_input';else if(!a.ok||!b.ok)set(false,'','type_error');
      else if(predicate.kind==='eq')set(typeof a.value===typeof b.value?a.value===b.value:typeof a.value==='bigint'&&typeof b.value==='number'||typeof b.value==='bigint'&&typeof a.value==='number'?compare(a.value,b.value)===0:false,'values_equal','values_differ');
      else if(predicate.kind==='within'){
        const numbers=a.field&&b.field&&numericTypes.has(a.field.type)&&numericTypes.has(b.field.type);let distance;
        if(numbers){const aa=a.value,bb=b.value;if(typeof aa==='bigint'&&typeof bb==='bigint')distance=aa>bb?aa-bb:bb-aa;else if(typeof aa==='number'&&typeof bb==='number')distance=Math.abs(aa-bb);else distance=null;}
        else {const da=day(a.raw),db=day(b.raw);distance=da!==null&&db!==null?Math.abs(da-db):null;}
        if(distance===null)reason=numbers?'numeric_precision_unresolved':'iso_date_required';else {const order=compare(distance,predicate.tolerance);if(order===null)reason='numeric_precision_unresolved';else set(order<=0,numbers?'within_numeric_tolerance':'within_day_tolerance',numbers?'outside_numeric_tolerance':'outside_day_tolerance');}
      }else {const numeric=(typeof a.value==='number'||typeof a.value==='bigint')&&(typeof b.value==='number'||typeof b.value==='bigint'),order=numeric?compare(a.value,b.value):null;if(order===null)reason='numeric_comparison_required';else set(predicate.kind==='lt'?order<0:order<=0);}
    }
    const result={id:predicate.id,kind:predicate.kind,relation:work.definition.id,candidate:candidate.id,state,reason,inputs:[...new Set(used)],observed:raw};memo.set(predicate.id,result);append(checks,result);return result;
  }
  for(const closure of c.closures){
    const work=relationWork.get(closure.relation);
    if(!work.candidates.length){for(const row of work.left.length?work.left:[null])append(closures,{id:closure.id,relation:closure.relation,state:work.complete?'fail':'unresolved',reason:work.complete?work.definition.unmatched_left_label??'no_matching_rows':'needs_input_or_candidate_resolution',inputs:row?[row.identity]:[],declared_status:closure.status_on_fail,output_label:closure.human_label_on_pass});continue;}
    for(const candidate of work.candidates){
      const memo=new Map(),tests=closure.when_all_pass.map(id=>evaluatePredicate(pmap.get(id),candidate,work,memo));let state=stateOf(tests),reason=state==='pass'?'all_declared_checks_passed':state==='fail'?'declared_check_failed':'declared_check_unresolved';
      if(!candidate.left.valid||!candidate.right.valid){state='fail';reason='input_field_check_failed';}else if(state==='pass'&&!work.complete){state='unresolved';reason='candidate_search_unresolved';}
      const row=append(closures,{id:closure.id,relation:closure.relation,candidate:candidate.id,state,reason,inputs:[candidate.left.identity,candidate.right.identity],declared_status:state==='pass'?closure.status_on_pass:closure.status_on_fail,output_label:closure.human_label_on_pass});
      if(state==='pass'){
        const values=Object.fromEntries([candidate.left,candidate.right].map(r=>[r.identity.entity,Object.fromEntries([...r.values].map(([key,value])=>[key,value.raw??null]))]));
        append(draft_outputs,{kind:'draft',label:closure.human_label_on_pass,closure_id:closure.id,inputs:row.inputs,values,declared_next_state:closure.status_on_pass,execution:'not_run'});
      }
    }
    for(const identity of work.projected.unmatched_left)append(closures,{id:closure.id,relation:closure.relation,state:work.complete?'fail':'unresolved',reason:work.complete?work.definition.unmatched_left_label??'no_matching_row':'candidate_search_unresolved',inputs:[identity],declared_status:closure.status_on_fail,output_label:closure.human_label_on_pass});
  }
  const missingEntities=inputs.filter(i=>i.state==='needs_input').map(i=>i.entity),summary={state:!c.entities.length||missingEntities.length?'needs_input':relations.some(r=>r.state==='unresolved')||closures.some(r=>r.state==='unresolved')?'unresolved':field_checks.some(r=>r.state==='fail')||relations.some(r=>r.state==='fail')||closures.some(r=>r.state==='fail')?'failed':'checked',rows:inputs.reduce((n,x)=>n+x.rows,0),examples:inputs.some(x=>x.example),missing_entities:missingEntities,fields:counts(field_checks),predicates:counts(checks),relations:counts(relations),closures:counts(closures),draft_outputs:draft_outputs.length};
  const result={schema:'bonfyre.data-checks-result.v1',contract_id:c.id,contract_sha256,inputs,field_checks,relations,checks,closures,draft_outputs,summary,scope:'supplied_input_checks',execution:'not_run',declarations:{sources:c.sources,numeric_compression:c.numeric_compression??null,materialization:c.materialization??null,evidence_budget:c.evidence_budget??null,labels:c.labels}};
  // Refuse oversized output before presenting an incomplete report.
  snapshot(result,16*1024*1024);result.result_sha256=await valueHash(result);return result;
}
async function resultValue(input){const result=snapshot(input,16*1024*1024);fail(result?.schema==='bonfyre.data-checks-result.v1'&&hashPattern.test(result.contract_sha256)&&hashPattern.test(result.result_sha256)&&result.execution==='not_run'&&Array.isArray(result.inputs)&&Array.isArray(result.draft_outputs),'Use a completed Data Checks result.');const claimed=result.result_sha256;delete result.result_sha256;fail(await valueHash(result)===claimed,'The check result changed. Run the current inputs again before exporting.');result.result_sha256=claimed;return result;}
export async function exportDataChecksReport(input){
  const r=await resultValue(input),rows=[['section','check','state','reason','input_rows','contract_sha256','result_sha256']];
  for(const [section,values]of [['field',r.field_checks],['predicate',r.checks],['closure',r.closures]])for(const row of values)rows.push([section,row.id,row.state,row.reason,row.inputs.map(i=>i.entity+' / '+(i.key??'missing key')+' / row '+i.row+' / '+i.source_sha256+(i.example?' / example':'')).join(' | '),r.contract_sha256,r.result_sha256]);
  const cell=value=>{let s=String(value??'');if(/^[\s]*[=+@-]/.test(s))s="'"+s;return '"'+s.replaceAll('"','""')+'"';};return rows.map(row=>row.map(cell).join(',')).join('\r\n')+'\r\n';
}
export async function exportDataChecksDraft(input){const r=await resultValue(input);return [1,'bonfyre.data-checks-draft.v1',{contract_id:r.contract_id,contract_sha256:r.contract_sha256,result_sha256:r.result_sha256,inputs:structuredClone(r.inputs),outputs:structuredClone(r.draft_outputs),summary:structuredClone(r.summary),execution:'not_run'}];}
