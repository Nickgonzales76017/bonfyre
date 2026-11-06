// SourceFrontier material projected through the existing PackGraph and CMS owners.
import {readFile} from 'node:fs/promises';
import {join,dirname} from 'node:path';
import {homedir} from 'node:os';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {retainWorkspaceMaterial} from './workspace_material.mjs';
import {continuationManifest,workspaceNavigation,workspaceNavigationCSS} from './workspace_continuation.mjs';
const repo=join(dirname(fileURLToPath(import.meta.url)),'..');
export const sourceLibraryPack='experience.source-library.workspace';
export async function materializeSourceLibrary({root=join(homedir(),'.bonfyre/workspace-packages')}={}){
 const files=new Map(),inputs=[];
 const sources=[['cmd/BonfyreSourceFrontier/tools/source_material.mjs','sources/source-material.mjs'],['integrations/browser/source-library.mjs','sources/library.mjs'],['integrations/browser/source-library.css','sources/library.css'],...['workspace-store','workspace-value','workspace-msgpack','workspace-continuation','workspace-destinations','interaction-collection','publication-research'].map(name=>['integrations/browser/'+name+'.mjs','bridge/'+name+'.mjs'])];
 for(const[path,target]of sources){const body=await readFile(join(repo,path));files.set(target,body);inputs.push(path+'\t'+createHash('sha256').update(body).digest('hex'));}
 files.set('sources/index.html',Buffer.from('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Your source library \u00B7 Aurekai</title><meta name="bonfyre-workspace-store" content="/apps/__WORKSPACE__/_records"><meta name="bonfyre-workspace-context" content="__DESTINATION__"><meta name="bonfyre-workspace-context-selector" content="__CONTEXT__"><meta name="bonfyre-workspace-release" content="__RELEASE__"><link rel="stylesheet" href="./library.css"><style>'+workspaceNavigationCSS+'</style></head><body>'+workspaceNavigation+'<main data-source-library><noscript>JavaScript is needed to operate this editor. Native saved records remain available through Your work.</noscript></main><script type="module" src="./library.mjs"></script></body></html>'));
 files.set('CONTINUATIONS.tsv',continuationManifest([{view:'library',label:'Source library',entry:'sources/',prefix:'source-material:',title:'title',query:'record=$key'}],files));
 files.set('SOURCE.tsv',Buffer.from('source\tsha256\n'+inputs.join('\n')+'\n'));
 return retainWorkspaceMaterial(files,{root});
}
export function sourceLibrarySetup(release){
 if(!/^[a-f0-9]{64}$/.test(release))throw Error('Choose a verified source library package.');
 const pack=sourceLibraryPack,family='source-materials',common={pack,status:'published'},version=createHash('sha256').update('source-library-v1\n'+release).digest('hex').slice(0,24);
 return [
 {type:'pack_artifact',key:{pack},value:{...common,canonical_name:'Source library',publisher:'aurekai',layer:'official',kind:'application',purpose:'Import original files and web addresses, organize collections and tags, archive and restore sources, and prepare exact original text in an installed research workspace.',governed_by:'native-content-and-workspace-authority',cannot_infer:'Saving a file or address does not verify its claims, fetch an external page, connect an account, publish the material or grant reuse rights.'}},
 {type:'pack_version',key:{pack,version},value:{...common,version,root:release,changed:'Native source library with original-byte retention and research continuation',proof:'scripts/tests/source-library-browser.test.mjs'}},
 {type:'pack_demand',key:{pack,demand_kind:'workspace',needs:family},value:{...common,demand_kind:'workspace',needs:family,preferred_sources:release,fields:'sources/',body_regime:'LOCAL',reheat_when:'explicit reviewed source operation',optional:'false'}},
 {type:'pack_demand',key:{pack,demand_kind:'workspace-record',needs:family,fields:'source-material:'},value:{...common,demand_kind:'workspace-record',needs:family,fields:'source-material:',body_regime:'LOCAL',reheat_when:'explicit reviewed source save or continuation',optional:'false'}},{type:'capability_program',key:{program_id:'feed.publication.source-library'},value:{program_id:'feed.publication.source-library',application_kind:family,action:'feed-intake',consumes:['publication'],produces:{mode:'source-library',prefix:'source-material:'},carrier_candidates:['workspace-binding'],capability_requirements:{pack,record_prefix:'source-material:'},authority_requirement:'current-destination-write',transition_conditions:{review_required:true,canonical_url_required:true},failure_projection:'Show the source-library handoff as unavailable when the installed workspace cannot accept source material.',status:'published'}},
 {type:'pack_head',key:{pack,channel:'stable'},value:{...common,channel:'stable',version,advanced_by:'aurekai-workspace-publisher'}}];
}
if(import.meta.main){const r=await materializeSourceLibrary();for(const[k,v]of Object.entries(r))console.log(k+'\t'+v);}
