import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { homedir } from 'node:os';
import { createHash } from 'node:crypto';
import { serveDocuments } from './document-workspace.mjs';

const root = resolve(import.meta.dir, '../..'), bin = join(root,'bin');
const sha = path => createHash('sha256').update(readFileSync(path)).digest('hex');
export const modelFiles = [
  {path:'onnx/model.onnx',sha256:'b4342336debaea79de872370664b0aaeb67dea4605513d00ee236ea871a81f27',upstream:'onnx/model_quantized.onnx'},
  {path:'vocab.txt',sha256:'07eced375cec144d27c900241f3e339478dec958f92fddbc551f295c992038a3',upstream:'vocab.txt'}
];
async function command(argv) {
  const child=Bun.spawn(argv,{cwd:root,stdout:'pipe',stderr:'pipe'});
  const [out,err,code]=await Promise.all([new Response(child.stdout).text(),new Response(child.stderr).text(),child.exited]);
  if(code!==0) throw Error(argv[0]+' failed: '+err.slice(-6000));
  return out.trim();
}
function checkModel() {
  const dir=join(homedir(),'.cache/bonfyre/models/nomic-embed-text-onnx');
  for(const row of modelFiles) {
    const path=join(dir,row.path);
    if(!existsSync(path)||sha(path)!==row.sha256) throw Error('Missing or different model file: '+path+'. See README.txt before downloading or changing it.');
  }
  // Native Embed prefers these files when present. Refuse an unverified override.
  for(const name of ['model_qint8_arm64.onnx','model_O4.onnx'])
    if(existsSync(join(dir,'onnx',name))) throw Error('Another ONNX variant would override the checked model: '+name);
}
export async function buildDocumentRelease() {
  if(process.platform!=='darwin'||process.arch!=='arm64') throw Error('This build recipe currently supports Apple Silicon macOS only.');
  const sqlite=await command(['brew','--prefix','sqlite']), onnx=await command(['brew','--prefix','onnxruntime']);
  mkdirSync(bin,{recursive:true});
  const common=['cc','-O2','-std=c11','-I'+join(root,'cmd/BonfyreLex/src'),'-I'+join(sqlite,'include')];
  const sqliteLink=['-L'+join(sqlite,'lib'),'-lsqlite3','-lm'];
  const library=['src/bf_common.c','src/bf_sha256.c','bf_embed_cache.c','bf_embed_pack.c','bf_embed_steer.c','bf_embed_index.c','bf_embed_refs.c'].map(p=>join(root,'lib/libbonfyre',p));
  await command([...common,'-I'+join(root,'lib/libbonfyre/include'),'-I'+join(onnx,'include/onnxruntime'),
    join(root,'cmd/BonfyreEmbed/src/main.c'),...library,'-L'+join(onnx,'lib'),'-lonnxruntime',...sqliteLink,'-o',join(bin,'bonfyre-embed')]);
  await command([...common,join(root,'cmd/BonfyreVec/src/main.c'),...sqliteLink,'-o',join(bin,'bonfyre-vec')]);
  const receipt={schema:'bonfyre.document-release.build.v1',at:new Date().toISOString(),
    platform:process.platform,architecture:process.arch,bun:Bun.version,
    compiler:await command(['cc','--version']),sqlite_prefix:sqlite,onnx_prefix:onnx,
    binaries:['embed','vec'].map(name=>({name:'bonfyre-'+name,sha256:sha(join(bin,'bonfyre-'+name))})),
    model_bundled:false,external_libraries_bundled:false};
  writeFileSync(join(root,'native.build.json'),JSON.stringify(receipt,null,2)+'\n');
  return receipt;
}
export async function runDocumentRelease(args) {
  const [verb,portValue,...extra]=args;
  if(verb==='build'&&portValue===undefined) { console.log(JSON.stringify(await buildDocumentRelease()));return; }
  if(verb!=='serve'||extra.length||portValue!==undefined&&!/^\d+$/.test(portValue)) throw Error('Usage: bun run.mjs build | serve [port]');
  for(const name of ['embed','vec']) if(!existsSync(join(bin,'bonfyre-'+name))) throw Error('Run bun run.mjs build first.');
  checkModel();
  await serveDocuments(join(root,'packs/aurekai-work-samples/household'),portValue===undefined?4177:Number(portValue),{nativeDirectory:bin});
}
if(import.meta.main || process.argv[1] && resolve(process.argv[1])===join(root,'run.mjs')) await runDocumentRelease(process.argv.slice(2));

