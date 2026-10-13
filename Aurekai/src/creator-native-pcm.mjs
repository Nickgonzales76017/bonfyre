// Pure lowering of an existing Creator cut to a bounded offline f32le mix
// candidate. Source-byte, effect and distribution authority stay with owners.
import {isAbsolute} from 'node:path';
const HEX=/^[0-9a-f]{64}$/;
const NUM=/^(0|[1-9][0-9]*)$/;
const invalid=message=>{throw new Error('creator native pcm: '+message);};
const count=(value,ceiling,name)=>{
  if(typeof value!=='string'||value.length>9||!NUM.test(value)||BigInt(value)>BigInt(ceiling))
    invalid('invalid '+name);
  return Number(value);
};

/** No IO, no decoder, no execution: it compiles the native CLI demand only. */
export function planNativePcmWindow(plan,bindings,{windowIndex,channels,output}={}) {
  if(!plan||plan.schema!=='aurekai.creator.render-program.v1'||
     plan.state!=='planned_unexecuted'||plan.effect_authorized!==false||
     plan.media_bytes_materialized!==0)invalid('unadmitted plan');
  for(const item of [plan.timeline_digest,plan.scope_digest])
    if(!HEX.test(item||''))invalid('unbound plan identity');
  if(!Number.isSafeInteger(windowIndex)||windowIndex<0)invalid('window index required');
  if(!Number.isSafeInteger(channels)||channels<1||channels>8)invalid('invalid channel count');
  if(typeof output!=='string'||!isAbsolute(output)||output.length>4000||output.includes('\0'))
    invalid('absolute output required');
  if(!bindings||typeof bindings!=='object'||Array.isArray(bindings))
    invalid('explicit source bindings required');
  const matching=plan.windows?.filter(row=>row.index===windowIndex);
  if(!matching||matching.length!==1)invalid('exact selected window required');
  const window=matching[0];
  if(!HEX.test(window.body_key||''))invalid('unbound window body');
  const samples=count(window.samples,100000000,'window samples');
  if(samples<1)invalid('empty window samples');
  const audio=window.segments?.filter(row=>row.kind==='audio');
  if(!audio?.length||audio.length>64)invalid('unsupported stem count');
  const argv=['--samples',String(samples),'--channels',String(channels),'--output',output];
  const required=new Map();
  for(const segment of audio){
    const source=plan.source_bases?.[segment.source_basis];
    if(!source||source.kind!=='audio'||!HEX.test(source.sha256||''))
      invalid('invalid audio source base');
    const binding=bindings[source.id];
    if(!binding||binding.sha256!==source.sha256||typeof binding.path!=='string'||
       !isAbsolute(binding.path)||binding.path.length>4000||binding.path.includes('\0'))
      invalid('unbound exact source: '+source.id);
    const at=count(segment.sample_offset,samples,'sample offset');
    const n=count(segment.samples,samples,'stem samples');
    const from=count(segment.source_sample,100000000,'source sample');
    if(n<1||at+n>samples||from+n>100000000)invalid('out-of-range stem');
    if(typeof segment.gain!=='number'||!Number.isFinite(segment.gain)||
       segment.gain<0||segment.gain>4)invalid('invalid gain');
    argv.push('--stem',binding.path,String(at),String(from),String(n),String(segment.gain));
    required.set(source.id,{id:source.id,sha256:source.sha256,path:binding.path});
  }
  return {
    schema:'aurekai.creator.native-pcm-candidate.v1',
    state:'candidate_requires_owner_approval',format:'interleaved-f32le',
    timeline_digest:plan.timeline_digest,scope_digest:plan.scope_digest,
    window_body_key:window.body_key,window_index:windowIndex,
    source_bindings:[...required.values()],argv,
    media_bytes_materialized:0,source_file_digest_verified:false,
    execution_authority_inferred:false,
    cannot_infer:'Source binding claims are not byte verification or media rights; an authorized owner must check source bodies and effects before invoking native execution.'
  };
}
