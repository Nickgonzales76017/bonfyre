import {test,expect} from 'bun:test';
import {createHash} from 'node:crypto';
import {prepareRepositoryFPQReview,reviewRepositoryFPQGeneration}
 from '../repository-fpq-review.mjs';
const sha=s=>createHash('sha256').update(s).digest('hex');
const commit='a'.repeat(40),tree='b'.repeat(40),model='qwen2.5-coder-0.5b-instruct-native-fp16.fpq';
function nativeCut(filter=':/datasets'){
 const digest=sha(['bonfyre.repository-cut.v1','forgejo','aurekai','proof-repo',
  'HEAD',filter==='none'?'':filter,commit,tree,''].join('\n'));
 return [
  'schema: bonfyre.repository-cut.v1','source_provider: forgejo','source_org: aurekai',
  'source_repository: proof-repo','requested_ref: HEAD','josh_filter: '+filter,
  'virtual_commit: '+commit,'virtual_tree: '+tree,'cut_digest: '+digest,
  'canonical_mapping: '+(filter==='none'?'direct-git':'unverified'),
  'pack_root: uncompiled','install_authority: none','publication_authority: none',''
 ].join('\n');
}
function generation(request,output='Review upstream mapping, license and data access before use.'){
 return {
  schema_version:'bonfyre.quic.fpq.generation.receipt.v1',
  receipt_id:'quic-fpq-'+'c'.repeat(32),status:'passed',event:'quic.fpq.generate',
  endpoint:'quic://127.0.0.1:9443',model:request.model,
  backend:'ngtcp2-quic-fpq-metal',request:{max_tokens:128,temperature:0},
  prompt_sha256:request.prompt_sha256,output,output_sha256:sha(output),
  request_fingerprint:sha(JSON.stringify({route:'quic-fpq',host:'127.0.0.1',
    port:9443,model:request.model,prompt:request.prompt,maxTokens:128,temperature:0}))
 };
}
test('FPQ request binds virtual Git cut digest without source body or any execution',async()=>{
 const request=await prepareRepositoryFPQReview(nativeCut());
 expect(request).toMatchObject({state:'prepared_unexecuted',route:'quic-fpq',
   nativeTool:'bonfyre_quic_fpq_generate',max_tokens:128,temperature:0,
   model,gitModified:false,sourceBytesRead:false,resultAuthorized:false});
 expect(request.prompt).toContain('JOSH filter: :/datasets');
 expect(request.prompt).toContain('Upstream canonical mapping: unverified');
 expect(request.prompt_sha256).toBe(sha(request.prompt));
 expect(Object.isFrozen(request)).toBe(true);
 expect(request.prompt).not.toContain('token ');
});
test('pack demand FPQ review is explicit about the unverified existing pack and cannot publish',async()=>{
 const request=await prepareRepositoryFPQReview(nativeCut(),{intent:'pack-demand',pack:'studio'});
 expect(request.prompt).toContain('Existing pack candidate (unverified): studio');
 expect(request.packInstalled).toBe(false);
 expect(request.publicationAuthorized).toBe(false);
 await expect(prepareRepositoryFPQReview(nativeCut(),{intent:'pack-demand',pack:'../break'}))
  .rejects.toThrow();
 await expect(prepareRepositoryFPQReview(nativeCut(),{intent:'source-evidence',pack:'studio'}))
  .rejects.toThrow();
});
test('synthetic fixture validates only FPQ receipt consistency, not independent model execution',async()=>{
 const request=await prepareRepositoryFPQReview(nativeCut());
 const result=await reviewRepositoryFPQGeneration(generation(request),request);
 expect(result).toMatchObject({schema:'aurekai.repository-fpq-result.v1',
  state:'generation_receipt_self_consistent',sourceCutDigest:request.sourceCutDigest,
  model,providerExecutionIndependentlyVerified:false,sourceTruthVerified:false,
  installAuthorized:false,gitWriteAuthorized:false,publicationAuthorized:false});
 expect(Object.isFrozen(result)).toBe(true);
});
test('different JOSH subtree, prompt, output, model or endpoint fail closed',async()=>{
 const request=await prepareRepositoryFPQReview(nativeCut());
 const other=await prepareRepositoryFPQReview(nativeCut(':/other'));
 const full=generation(request);
 for(const bad of [
  {...full,prompt_sha256:other.prompt_sha256},
  {...full,output:'different'},
  {...full,request_fingerprint:'f'.repeat(64)},
  {...full,endpoint:'quic://example.com:9443'},
  {...full,model:'remote'},
  {...full,backend:'openai'},
  {...full,status:'error'},
  {...full,request:{max_tokens:129,temperature:0}}
 ])await expect(reviewRepositoryFPQGeneration(bad,request)).rejects.toThrow();
 await expect(reviewRepositoryFPQGeneration(full,other)).rejects.toThrow();
});
test('malformed source receipts, unsupported intent and false authority stop before FPQ request',async()=>{
 await expect(prepareRepositoryFPQReview(nativeCut().replace('install_authority: none',
   'install_authority: true'))).rejects.toThrow();
 await expect(prepareRepositoryFPQReview(nativeCut(),{intent:'git-push'}))
  .rejects.toThrow();
 await expect(prepareRepositoryFPQReview(nativeCut().replace('josh_filter: :/datasets',
   'josh_filter: :/private/../../secrets'))).rejects.toThrow();
});
