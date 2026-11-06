import { existsSync, mkdirSync, readFileSync, writeFileSync, lstatSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { createHash } from 'node:crypto';

export const documentPackageSources = [
  'LICENSE',
  'components/BonfyreServiceSite/document-workspace.mjs',
  'components/BonfyreServiceSite/document-recording.mjs',
  'components/BonfyreServiceSite/document-client.mjs',
  'components/BonfyreServiceSite/document-workspace.css',
  ...['dm-sans.woff2','dm-mono.woff2','dm-mono-medium.woff2','instrument-serif.woff2','instrument-serif-italic.woff2','DM-SANS-LICENSE.txt','DM-MONO-LICENSE.txt','INSTRUMENT-SERIF-LICENSE.txt'].map(name => 'components/BonfyreServiceSite/fonts/' + name),
  'components/BonfyreServiceSite/document-release.mjs',
  'components/BonfyreServiceSite/document-release.txt',
  'integrations/browser/extension/store.mjs',
  'packs/aurekai-work-samples/household/documents.json',
  'cmd/BonfyreEmbed/src/main.c',
  'lib/libbonfyre/include/bf_bert_fold.h',
  'cmd/BonfyreEmbed/UNICODE-LICENSE.txt',
  'cmd/BonfyreVec/src/main.c',
  'cmd/BonfyreLex/src/bf_lex.h',
  'lib/libbonfyre/include/bonfyre.h',
  'lib/libbonfyre/include/bf_help.h',
  'lib/libbonfyre/include/bonfyre/bf_discipl.h',
  'lib/libbonfyre/src/bf_common.c',
  'lib/libbonfyre/src/bf_sha256.c',
  ...['cache','pack','steer','index','refs'].map(name => 'lib/libbonfyre/bf_embed_' + name + '.c')
];

export function packageDocuments(output) {
  const root = resolve(import.meta.dir, '../..'), destination = resolve(output);
  if (existsSync(destination)) throw Error('Choose a new output directory; existing files are never overwritten.');
  const rows = documentPackageSources.map(path => {
    const source = join(root, path);
    if (!lstatSync(source).isFile() || lstatSync(source).isSymbolicLink()) throw Error('Expected a regular source file: ' + path);
    const bytes = readFileSync(source);
    return { path, bytes, sha256:createHash('sha256').update(bytes).digest('hex') };
  });
  const sample = JSON.parse(rows.find(row => row.path.endsWith('/documents.json')).bytes);
  if (sample.synthetic !== true) throw Error('The release sample must be explicitly fictional.');
  mkdirSync(destination, { recursive:true });
  for (const row of rows) {
    const target = join(destination,row.path);
    mkdirSync(dirname(target), { recursive:true });
    writeFileSync(target,row.bytes);
  }
  writeFileSync(join(destination,'run.mjs'), "import './components/BonfyreServiceSite/document-release.mjs';\n");
  writeFileSync(join(destination,'README.txt'), readFileSync(join(root,'components/BonfyreServiceSite/document-release.txt')));
  writeFileSync(join(destination,'.gitignore'), 'bin/\n*.build.json\n**/.documents/\n');
  const manifest = { schema:'bonfyre.document-source-package.v1',
    license:'MIT; see dependency and generated-data notices in README.txt',
    files:rows.map(({path,sha256,bytes})=>({path,sha256,bytes:bytes.length})),
    sample_documents:sample.documents.length,
    includes_model_weights:false, includes_native_binaries:false, includes_private_records:false,
    native_runtime:'Externally installed Bun, SQLite and ONNX Runtime required.',
    tested_platform:'Verification evidence is separate; packaging alone proves no runtime behavior.' };
  writeFileSync(join(destination,'manifest.json'),JSON.stringify(manifest,null,2)+'\n');
  return manifest;
}
if (import.meta.main) {
  if (process.argv.length !== 3) throw Error('Usage: bun document-package.mjs <new-output-directory>');
  const manifest=packageDocuments(process.argv[2]);
  console.log(JSON.stringify({directory:resolve(process.argv[2]),files:manifest.files.length,bytes:manifest.files.reduce((n,f)=>n+f.bytes,0)}));
}

