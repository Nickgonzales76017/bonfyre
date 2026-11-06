// Pure, non-materializing Creator render compiler. The media owners own bytes,
// Matrix/Authority own viewing rights, and execution needs a separate grant.
import { createHash } from 'node:crypto';

const HEX = /^[0-9a-f]{64}$/;
const KEY = /^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$/;
const KINDS = new Set(['audio', 'video', 'image']);
const TRACKS = new Set(['audio', 'video']);
const fail = message => { throw new Error(`creator program: ${message}`); };
const integer = (value, min, max, name) => {
  if (!Number.isSafeInteger(value) || value < min || value > max) fail(`invalid ${name}`);
  return value;
};
const key = (value, name) => {
  if (typeof value !== 'string' || !KEY.test(value)) fail(`invalid ${name}`);
  return value;
};
const digest = (value, name) => {
  if (typeof value !== 'string' || !HEX.test(value)) fail(`invalid ${name}`);
  return value;
};
const unit = (value, min, max, name) => {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < min || value > max)
    fail(`invalid ${name}`);
  return Object.is(value, -0) ? 0 : value;
};
const hash = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const cmp = (a, b) => a < b ? -1 : a > b ? 1 : 0;

/** Whole-project validation is intentionally separate from authorization. */
export function validateCreatorTimeline(input) {
  if (!input || input.schema !== 'aurekai.creator.timeline.v1') fail('unsupported timeline schema');
  const scope = input.scope;
  if (!scope || typeof scope !== 'object') fail('missing authority-scoped coordinate');
  key(scope.owner, 'scope.owner');
  key(scope.purpose, 'scope.purpose');
  digest(scope.authority_digest, 'scope.authority_digest');
  digest(scope.frontier_digest, 'scope.frontier_digest');
  const fps = input.fps;
  if (!fps || typeof fps !== 'object') fail('missing fps');
  const numerator = integer(fps.num, 1, 240000, 'fps numerator');
  const denominator = integer(fps.den, 1, 1001, 'fps denominator');
  if (numerator / denominator < 1 || numerator / denominator > 240) fail('fps outside supported envelope');
  integer(input.frames, 1, 10000000, 'timeline frames');
  integer(input.sample_rate, 8000, 384000, 'sample rate');
  if (!Array.isArray(input.sources) || input.sources.length > 32768) fail('invalid source population');
  if (!Array.isArray(input.tracks) || input.tracks.length > 2048) fail('invalid track population');
  const sources = new Map();
  for (const source of input.sources) {
    if (!source || typeof source !== 'object') fail('invalid source');
    key(source.id, 'source.id');
    if (sources.has(source.id)) fail(`duplicate source: ${source.id}`);
    if (!KINDS.has(source.kind)) fail('invalid source kind');
    digest(source.sha256, 'source.sha256');
    integer(source.frames, 1, 10000000, 'source frames');
    sources.set(source.id, source);
  }
  const seenTracks = new Set(), seenClips = new Set();
  let total = 0;
  for (const track of input.tracks) {
    if (!track || typeof track !== 'object') fail('invalid track');
    key(track.id, 'track.id');
    if (seenTracks.has(track.id)) fail(`duplicate track: ${track.id}`);
    seenTracks.add(track.id);
    if (!TRACKS.has(track.kind) || !Array.isArray(track.clips)) fail('invalid track kind/clips');
    for (const clip of track.clips) {
      if (!clip || typeof clip !== 'object') fail('invalid clip');
      key(clip.id, 'clip.id');
      if (seenClips.has(clip.id)) fail(`duplicate clip: ${clip.id}`);
      seenClips.add(clip.id);
      const source = sources.get(clip.source_id);
      if (!source) fail(`unresolved source: ${clip.source_id}`);
      if ((track.kind === 'audio') !== (source.kind === 'audio')) fail('track/source kind mismatch');
      const at = integer(clip.at, 0, input.frames - 1, 'clip.at');
      const from = integer(clip.in, 0, source.frames - 1, 'clip.in');
      const frames = integer(clip.frames, 1, input.frames, 'clip.frames');
      if (at + frames > input.frames || from + frames > source.frames) fail(`out-of-bounds clip: ${clip.id}`);
      if (clip.gain !== undefined) unit(clip.gain, 0, 4, 'clip.gain');
      if (clip.opacity !== undefined) unit(clip.opacity, 0, 1, 'clip.opacity');
      if (++total > 100000) fail('too many clips');
    }
  }
  return input;
}

export function creatorTimelineDigest(input) {
  validateCreatorTimeline(input);
  // Object key order inside authored objects is not a semantic change.
  const normalized = {
    scope: [input.scope.owner, input.scope.purpose, input.scope.authority_digest, input.scope.frontier_digest],
    fps: [input.fps.num, input.fps.den], frames: input.frames, sample_rate: input.sample_rate,
    sources: input.sources.map(s => [s.id, s.kind, s.sha256, s.frames]).sort((a,b)=>cmp(a[0],b[0])),
    tracks: input.tracks.map(t => [t.id, t.kind, t.clips.map(c =>
      [c.id,c.source_id,c.at,c.in,c.frames,c.gain??1,c.opacity??1])])
  };
  return hash(normalized);
}

/** One frame boundary, rounded down only once in rational arithmetic. */
export function samplesAtFrame(frame, fps, sampleRate) {
  integer(frame, 0, 10000000, 'frame');
  const num = BigInt(fps.num), den = BigInt(fps.den);
  if (num < 1n || den < 1n) fail('invalid frame rate');
  integer(sampleRate, 8000, 384000, 'sample rate');
  return ((BigInt(frame) * den * BigInt(sampleRate)) / num).toString();
}

/** Exact mutable editing without borrowing write authority from the plan. */
export function editCreatorTimeline(input, edit) {
  const before = creatorTimelineDigest(input);
  if (!edit || edit.expected_digest !== before) fail('stale or missing expected revision');
  const op = edit.op;
  if (!['move','trim','gain','opacity','remove','duplicate'].includes(op)) fail('unsupported edit');
  const trackIdx = input.tracks.findIndex(t => t.id === edit.track_id);
  if (trackIdx === -1) fail('track not found');
  const oldTrack = input.tracks[trackIdx];
  const clipIdx = oldTrack.clips.findIndex(c => c.id === edit.clip_id);
  if (clipIdx === -1) fail('clip not found');
  const oldClip = oldTrack.clips[clipIdx];
  let clips;
  if (op === 'remove') clips = oldTrack.clips.filter(c => c.id !== edit.clip_id);
  else if (op === 'duplicate') {
    key(edit.new_id, 'edit.new_id');
    clips = [...oldTrack.clips, {...oldClip, id: edit.new_id,
      at: integer(edit.at, 0, input.frames - 1, 'edit.at')}];
  } else {
    const update = {...oldClip};
    if (op === 'move') update.at = integer(edit.at, 0, input.frames - 1, 'edit.at');
    if (op === 'trim') {
      update.in = integer(edit.in, 0, 10000000, 'edit.in');
      update.frames = integer(edit.frames, 1, input.frames, 'edit.frames');
    }
    if (op === 'gain') update.gain = unit(edit.value, 0, 4, 'edit.gain');
    if (op === 'opacity') update.opacity = unit(edit.value, 0, 1, 'edit.opacity');
    clips = oldTrack.clips.map(c => c.id === edit.clip_id ? update : c);
  }
  const tracks = input.tracks.map((t,i)=>i===trackIdx?{...t,clips}:t);
  const result = {...input,tracks};
  const after = creatorTimelineDigest(result);
  return { timeline: result, edit: {schema:'aurekai.creator.edit.v1',op,from:before,to:after,
    state:'candidate_uncommitted', authority:'not_inferred'} };
}

/** Precompile one exact, immutable revision for repeated viewport/playhead cuts.
 * The source timeline is validated once and never referenced after preparation.
 * This is an uncommitted cache of the existing compiler's meaning, not a new
 * semantic owner or a grant to decode, render, publish or share media.
 */
export function prepareCreatorCompiler(input) {
  const fullDigest=creatorTimelineDigest(input);
  const extent=input.frames, fpsNum=input.fps.num, fpsDen=input.fps.den, sampleRate=input.sample_rate;
  const scopeKey=hash([input.scope.owner,input.scope.purpose,input.scope.authority_digest,input.scope.frontier_digest]);
  const bases=input.sources.map(s=>({id:s.id,kind:s.kind,sha256:s.sha256}))
    .sort((a,b)=>cmp(a.id,b.id));
  const sourceIndices=new Map(bases.map((s,i)=>[s.id,i]));
  const all=[];
  input.tracks.forEach((track,ti)=>track.clips.forEach((clip,ci)=>{
    all.push({track:track.id,kind:track.kind,clip:clip.id,
      at:clip.at,end:clip.at+clip.frames,in:clip.in,
      source_basis:sourceIndices.get(clip.source_id),
      gain:clip.gain??1,opacity:clip.opacity??1,priority:[ti,ci]});
  }));
  all.sort((a,b)=>a.at-b.at||a.priority[0]-b.priority[0]||a.priority[1]-b.priority[1]);
  // Prefix maxima let late viewport lookups skip expired earlier clips rather
  // than replay the full event stream. O(N) build once, O(overlap) in common
  // sparse timelines. A persistent long clip may still require a larger scan.
  const maxEnds=new Int32Array(all.length);
  let maximum=0;
  for(let i=0;i<all.length;i++){ maximum=Math.max(maximum,all[i].end); maxEnds[i]=maximum; }
  const samplesNumerator=BigInt(fpsDen)*BigInt(sampleRate), samplesDenominator=BigInt(fpsNum);
  const compile=({shardFrames=256,range=null,sourceProjection='all'}={})=>{
    if(!['all','used'].includes(sourceProjection))fail('invalid sourceProjection');
    integer(shardFrames,1,65536,'shardFrames');
    const start=range?.start??0,end=range?.end??extent;
    integer(start,0,extent-1,'range.start');
    integer(end,start+1,extent,'range.end');
    const first=Math.floor(start/shardFrames),last=Math.ceil(end/shardFrames);
    if(last-first>65536)fail('too many selected shards');
    // Bounded per-call memoization avoids redundant BigInt clock conversion
    // across heavily layered audio/video tracks. No global retained state.
    const samplesCache=new Map();
    const frameSamples=frame=>{
      if(samplesCache.has(frame))return samplesCache.get(frame);
      const value=((BigInt(frame)*samplesNumerator)/samplesDenominator).toString();
      if(samplesCache.size>=4096)samplesCache.clear();
      samplesCache.set(frame,value);
      return value;
    };
    const active=new Map(),firstFrame=first*shardFrames;
    const prefixEnd=Math.min(extent,(first+1)*shardFrames);
    // Exclusive upper bound for starting events in the first shard.
    let low=0,high=all.length;
    while(low<high){const mid=Math.floor((low+high)/2);
      if(all[mid].at<prefixEnd)low=mid+1;else high=mid;
    }
    let event=low;
    for(let i=event-1;i>=0&&maxEnds[i]>firstFrame;i--){
      const item=all[i];if(item.end>firstFrame)active.set(item.clip,item);
    }
    const windows=[],distinct=new Set();
    let totalSegments=0;
    for(let i=first;i<last;i++){
      const ws=i*shardFrames,we=Math.min(extent,ws+shardFrames);
      while(event<all.length&&all[event].at<we){
        const item=all[event++];if(item.end>ws)active.set(item.clip,item);
      }
      const segments=[];
      for(const item of active.values()){
        const overlapStart=Math.max(ws,item.at),overlapEnd=Math.min(we,item.end);
        if(overlapEnd<=overlapStart)continue;
        const sourceStart=item.in+(overlapStart-item.at);
        segments.push({track:item.track,kind:item.kind,clip:item.clip,source_basis:item.source_basis,
          priority:[item.priority[0],item.priority[1]],
          frame_offset:overlapStart-ws,source_frame:sourceStart,frames:overlapEnd-overlapStart,
          sample_offset:(BigInt(frameSamples(overlapStart))-BigInt(frameSamples(ws))).toString(),
          source_sample:frameSamples(sourceStart),
          samples:(BigInt(frameSamples(overlapEnd))-BigInt(frameSamples(overlapStart))).toString(),
          gain:item.gain,opacity:item.opacity});
      }
      segments.sort((a,b)=>a.priority[0]-b.priority[0]||a.priority[1]-b.priority[1]);
      const bodyKey=hash([scopeKey,fpsNum,fpsDen,sampleRate,we-ws,
        (BigInt(frameSamples(we))-BigInt(frameSamples(ws))).toString(),
        segments.map(s=>[s.kind,bases[s.source_basis].kind,bases[s.source_basis].sha256,
          s.frame_offset,s.source_frame,s.frames,s.sample_offset,s.source_sample,s.samples,s.gain,s.opacity])]);
      windows.push({index:i,start_frame:ws,frames:we-ws,
        start_sample:frameSamples(ws),samples:(BigInt(frameSamples(we))-BigInt(frameSamples(ws))).toString(),
        body_key:bodyKey,render_state:'planned_unexecuted',segments});
      distinct.add(bodyKey);totalSegments+=segments.length;
      for(const [id,item] of active)if(item.end<=we)active.delete(id);
    }
    // Demand-selected carriers project only the bases referenced by emitted
    // segments; a stable remap preserves every digest and body key. This
    // reduces browser/agent wire payload without duplicating media bodies.
    let projectedBases;
    if(sourceProjection==='used'){
      const used=new Set();
      for(const window of windows)for(const segment of window.segments)used.add(segment.source_basis);
      const ordered=[...used].sort((a,b)=>a-b);
      const remap=new Map(ordered.map((original,index)=>[original,index]));
      projectedBases=ordered.map(index=>({...bases[index]}));
      for(const window of windows)for(const segment of window.segments)
        segment.source_basis=remap.get(segment.source_basis);
    }else projectedBases=bases.map(base=>({...base}));
    return {
      schema:'aurekai.creator.render-program.v1',timeline_digest:fullDigest,scope_digest:scopeKey,
      state:'planned_unexecuted',effect_authorized:false,media_bytes_materialized:0,
      clock:{fps:{num:fpsNum,den:fpsDen},sample_rate:sampleRate},
      extent_frames:extent,shard_frames:shardFrames,
      // Never expose the retained precompiled array by reference.
      source_bases:projectedBases,range:{start_frame:start,end_frame:end},
      windows,
      ...(sourceProjection==='used'?{source_projection:'used',
        selected_source_bases:projectedBases.length}:{}),
      metrics:{source_count:sourceIndices.size,clip_count:all.length,selected_windows:windows.length,
        resolved_segments:totalSegments,unique_render_bodies:distinct.size,
        reused_window_bodies:windows.length-distinct.size,
        observed_peak_memory_bytes:null,measured_cpu_ms:null},
      cannot_infer:'A render plan does not grant source access, rights, provider capacity, real-time latency, delivered bytes or publication authority.'
    };
  };
  return Object.freeze({schema:'aurekai.creator.prepared-compiler.v1',
    timeline_digest:fullDigest,scope_digest:scopeKey,compile});
}

/** One-shot compatibility path is byte-semantic equivalent to a prepared cut. */
export function compileCreatorProgram(input,options={}) {
  return prepareCreatorCompiler(input).compile(options);
}

export function creatorInvalidations(before,after) {
  if(before?.schema!=='aurekai.creator.render-program.v1'||after?.schema!==before.schema)
    fail('invalid render plans for comparison');
  if(before.shard_frames!==after.shard_frames||before.extent_frames!==after.extent_frames||
     before.scope_digest!==after.scope_digest) fail('incomparable render coordinate');
  const old=new Map(before.windows.map(w=>[w.index,w.body_key]));
  return after.windows.filter(w=>old.get(w.index)!==w.body_key).map(w=>w.index);
}
