// Exact RepositoryFabric cut -> bounded Qwen FPQ review-only prompt/receipt.
// Native QUIC execution is exclusively owned by bonfyre_quic_fpq_generate.
// A self-consistent generation receipt is neither source verification nor authority.
import {readRepositoryCutReceipt} from './repository-cuts.mjs';

const hex=/^[a-f0-9]{64}$/;
const fail=(condition,message)=>{if(!condition)throw Error(message);};
const sha=async body=>Array.from(new Uint8Array(await crypto.subtle.digest(
  'SHA-256',new TextEncoder().encode(body))),b=>b.toString(16).padStart(2,'0')).join('');
const intents=Object.freeze({
  'source-evidence':'Evaluate how to review this exact source cut as retained research evidence.',
  'pack-demand':'Evaluate the canonical BonfyreCMS source-demand proposal for an already owned pack.'
});
const defaultModel='qwen2.5-coder-0.5b-instruct-native-fp16.fpq';
const nativeTool='bonfyre_quic_fpq_generate';

export async function prepareRepositoryFPQReview(receiptBody,{intent='source-evidence',
  pack='',model=defaultModel}={}){
 const cut=await readRepositoryCutReceipt(receiptBody);
 fail(Object.hasOwn(intents,intent),'Use a declared FPQ review task.');
 fail((intent==='pack-demand'&&/^[a-z0-9][a-z0-9._-]{0,94}$/.test(pack))||
      (intent==='source-evidence'&&pack===''),
      'Choose an existing canonical pack identifier only for a pack-demand review.');
 fail(typeof model==='string'&&model.length>0&&model.length<=128&&
   /^[A-Za-z0-9._-]+$/.test(model),'Choose the exact configured local FPQ model.');
 // No source bodies, secrets, local paths, or private workspace record content.
 const prompt=[
  'Bonfyre local Qwen FPQ: produce an evidence-review draft for Aurekai.',
  'Do not execute, publish, install, change Git, or assert rights/remote source access.',
  'This immutable RepositoryFabric cut is self-consistent, not provider-verified.',
  'Task: '+intents[intent],
  'Git provider: '+cut.provider,
  'Repository: '+cut.org+'/'+cut.repository,
  'Requested Git ref: '+cut.requestedRef,
  'JOSH filter: '+cut.filter,
  'Virtual commit: '+cut.virtualCommit,
  'Virtual tree: '+cut.virtualTree,
  'Source-cut digest: '+cut.cutDigest,
  'Upstream canonical mapping: '+cut.canonicalMapping,
  ...(pack?['Existing pack candidate (unverified): '+pack]:[]),
  'Explicitly separate observed cut coordinates from unknown source rights,',
  'upstream commit mapping, source-byte verification, licence, and account grants.',
  'Identify the next checks for a human reviewer. Keep this a candidate, not an',
  'installed app, verified source, research finding or executed PackGraph change.'
 ].join('\n');
 fail(prompt.length<=4096,'FPQ review prompt exceeds its bound.');
 return Object.freeze({
  schema:'aurekai.repository-fpq-review.v1',state:'prepared_unexecuted',
  sourceCutDigest:cut.cutDigest,intent,pack,model,
  route:'quic-fpq',nativeTool,modelOutput:'unverified_candidate',
  allowedSourceMaterial:'exact_repository_cut_coordinates_only',
  max_tokens:128,temperature:0,prompt,prompt_sha256:await sha(prompt),
  resultAuthorized:false,sourceBytesRead:false,gitModified:false,
  packInstalled:false,publicationAuthorized:false
 });
}

export async function reviewRepositoryFPQGeneration(receipt,prepared){
 fail(prepared?.schema==='aurekai.repository-fpq-review.v1'&&
   prepared.state==='prepared_unexecuted'&&hex.test(prepared.sourceCutDigest)&&
   hex.test(prepared.prompt_sha256)&&prepared.nativeTool===nativeTool,
   'Choose a current prepared FPQ cut review.');
 fail(receipt?.schema_version==='bonfyre.quic.fpq.generation.receipt.v1'&&
   receipt.status==='passed'&&receipt.event==='quic.fpq.generate'&&
   receipt.backend==='ngtcp2-quic-fpq-metal'&&
   /^quic-fpq-[a-f0-9]{32}$/.test(receipt.receipt_id??'')&&
   receipt.endpoint==='quic://127.0.0.1:9443'&&receipt.model===prepared.model&&
   receipt.request?.max_tokens===prepared.max_tokens&&
   receipt.request?.temperature===prepared.temperature&&
   receipt.prompt_sha256===prepared.prompt_sha256&&hex.test(receipt.output_sha256??'')&&
   typeof receipt.output==='string'&&receipt.output.trim()&&receipt.output.length<=32768,
   'FPQ receipt does not match the exact prepared local generation.');
 const fingerprint=await sha(JSON.stringify({
  route:'quic-fpq',host:'127.0.0.1',port:9443,model:prepared.model,
  prompt:prepared.prompt,maxTokens:prepared.max_tokens,temperature:prepared.temperature
 }));
 fail(receipt.request_fingerprint===fingerprint&&
   receipt.output_sha256===await sha(receipt.output),
   'The FPQ request or completion digest is inconsistent.');
 return Object.freeze({
  schema:'aurekai.repository-fpq-result.v1',
  state:'generation_receipt_self_consistent',sourceCutDigest:prepared.sourceCutDigest,
  intent:prepared.intent,pack:prepared.pack,
  nativeReceiptId:receipt.receipt_id,
  prompt_sha256:prepared.prompt_sha256,output_sha256:receipt.output_sha256,
  model:receipt.model,output:receipt.output,
  sourceTruthVerified:false,providerExecutionIndependentlyVerified:false,
  recommendationApproved:false,installAuthorized:false,
  gitWriteAuthorized:false,publicationAuthorized:false
 });
}
