import {readFileSync} from 'node:fs';
import {resolve,join} from 'node:path';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {creatorFiles} from '../components/BonfyreServiceSite/creator-material.mjs';
import {render} from '../components/BonfyreServiceSite/sections/creator.mjs';
import {escapeHTML} from '../components/BonfyreServiceSite/workspace-navigation.mjs';
import {retainWorkspaceFiles} from './materialize_workspace_package.mjs';
import {continuationManifest,workspaceNavigation} from './workspace_continuation.mjs';
const repo=resolve(import.meta.dirname,'..'),base='/apps/__WORKSPACE__/',hash=b=>createHash('sha256').update(b).digest('hex');
// Compile the existing arrangement editor, native/Wasm kernel and retained
// generated sample. Only hosting, storage binding and ownership copy change.
export async function materializeCreatorWorkspace({root,directory=join(repo,'packs/aurekai-work-samples/audio')}={}){
 const packed=await creatorFiles(directory),files=new Map(),sources=[];
 const ownership=text=>text.replaceAll('stay in this browser','stay in this workspace').replaceAll('stays in this browser','stays in this workspace').replaceAll('stay on this device','stay in this workspace').replaceAll('saved in this browser','saved in this workspace').replaceAll('saved on this browser','saved in this workspace').replaceAll('Recordings and edits stay in this browser.','Recordings and edits stay in this workspace.').replaceAll('The browser could not save this session.','The native workspace could not save this session.');
 const body=render({id:'creator',title:'Arrange your recordings',data:'/data/creator/',asset_prefix:packed.asset_prefix,sample:packed.sample},{},{esc:escapeHTML});
 let html=`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Audio Studio · Editorial music and narration</title>
 <meta name="bonfyre-workspace-store" content="${base}_records">
 <meta name="bonfyre-workspace-context" content="__DESTINATION__"><meta name="bonfyre-workspace-release" content="__RELEASE__">
 <meta name="bonfyre-workspace-family" content="creator-audio"><script type="module" src="${base}bridge/workspace-adoption.mjs"></script>
 <style>
 html,body{margin:0;background:#101a17;color:#eef4e9;font-family:system-ui}
 [data-native-workspace]{display:flex;flex-wrap:wrap;gap:14px;align-items:center;padding:12px 18px;font-size:14px}
 [data-native-workspace] a{color:inherit;min-height:44px;display:inline-flex;align-items:center}
 .audio-studio-main{max-width:1440px;margin:0 auto;padding:clamp(20px,4vw,60px)}
 .audio-studio-hero{padding:clamp(24px,5vw,65px);border:1px solid #456051;border-radius:22px;background:linear-gradient(135deg,#244638,#122922 70%,#192b23)}
 .audio-studio-hero small,.audio-library-heading .eyebrow{color:#c7dba1;text-transform:uppercase;letter-spacing:.16em;font:700 12px system-ui}
 .audio-studio-hero h1{margin:13px 0;font:500 clamp(40px,6vw,88px)/1.04 Georgia,serif;letter-spacing:-.045em}
 .audio-studio-hero p{max-width:67ch;font:17px/1.7 system-ui;color:#d4e6d6}
 [data-editorial-library]{margin:clamp(30px,4vw,55px) auto}
 .audio-library-heading h2{max-width:20ch;margin:12px 0;font:500 clamp(32px,4.5vw,63px)/1.06 Georgia,serif}
 .audio-library-intro{max-width:84ch;font:16px/1.7 system-ui;color:#cfdfd0}
 .audio-library-tools{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:16px;margin:26px 0}
 .audio-library-tools select{font:600 14px system-ui;border:1px solid #617e68;background:#1d3729;color:#f6f5e9;border-radius:9px;padding:12px 16px;min-height:44px}
 .audio-library-count{color:#bfd8bf;font:13px system-ui}
 .audio-library-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(310px,100%),1fr));gap:19px}
 .audio-music-card{padding:clamp(18px,2vw,30px);border-radius:16px;background:#1b3028;border:1px solid #4b6755;display:flex;flex-direction:column;gap:10px}
 .audio-music-card[hidden]{display:none!important}
 .audio-music-card h3{font:500 30px/1.18 Georgia,serif;margin:2px 0}
 .audio-music-card p{margin:0}
 .music-genre{text-transform:uppercase;letter-spacing:.13em;color:#c9dbb2;font:700 11px system-ui}
 .music-artist{color:#e4eddc;font:14px/1.4 system-ui}
 .music-caption{min-height:52px;color:#b8cbbc;font:14px/1.6 system-ui}
 .music-player{display:block;width:100%;margin:12px 0 0;min-height:54px}
 .music-credit{font:12px/1.5 system-ui;color:#b6c9b8;overflow-wrap:anywhere}
 .music-credit a{color:#cdeacb;text-decoration:underline;text-underline-offset:3px}
 .music-retain{margin-top:auto;min-height:44px;align-self:start;border:1px solid #c2d7b4;background:#d9eac9;color:#1b3525;padding:12px 16px;border-radius:10px;font:700 13px system-ui;cursor:pointer}
 .music-retain:disabled{opacity:.6;cursor:default}
 .music-retain-status,.audio-library-disclaimer{font:12px/1.6 system-ui;color:#b3c8b6}
 .audio-library-disclaimer{margin-top:20px;max-width:85ch}
 .audio-demo-lab{margin:36px 0;border:1px solid #647262;border-radius:12px;padding:16px;background:#18251e}
 .audio-demo-lab summary{cursor:pointer;font:600 15px system-ui;padding:8px 4px;color:#cdd3b7}
 .audio-demo-lab .creator-workspace{margin:16px 0 0}
 @media(max-width:700px){.audio-studio-main{padding:12px}.audio-studio-hero{border-radius:14px}.audio-library-grid{grid-template-columns:1fr}}
 </style></head><body>${workspaceNavigation}
 <main class="audio-studio-main">
 <header class="audio-studio-hero"><small>Creator Studio / Editorial Audio</small><h1>Give every story a sound.</h1>
 <p>Record and review narration. Audition real instrumental music, retain credited recordings, build a distinct soundtrack for each article, and mix with native speech ducking. Publication always requires separate review.</p></header>
 <section data-editorial-library aria-label="Recorded editorial soundtrack library"></section>
 <script type="module" src="/data/creator/editorial-music-library-client.mjs"></script>
 <section data-narration-workspace aria-label="Narration productions"></section>
 <script type="module" src="/data/creator/narration-workspace-client.mjs"></script>
 <details class="audio-demo-lab"><summary>Technical mixer lab · original chime, pulse and harmony fixtures (not editorial music)</summary>${body}</details>
 </main></body></html>`;
 html=ownership(html).replaceAll('href="/#','href="https://aurekai-work-samples.pages.dev/#').replaceAll('/data/creator/',base+'data/creator/').replaceAll(base+base,base);
 files.set('audio/index.html',Buffer.from(html));
 for(const [name,body]of Object.entries(packed.files)){
  const path='data/creator/'+name;sources.push([path,hash(body),body.length]);
  let next=body;
  if(name.endsWith('store.mjs'))next=Buffer.from(readFileSync(join(repo,'integrations/browser/extension/store.mjs'),'utf8').replace("import('/apps/workspace-store.mjs')","import('"+base+"bridge/workspace-store.mjs')"));
  else if(/\.(mjs|js|css)$/.test(name))next=Buffer.from(ownership(body.toString('utf8')));
  files.set(path,next);
 }
  for(const name of ['workspace-store.mjs','workspace-msgpack.mjs','workspace-value.mjs','workspace-continuation.mjs','workspace-adoption.mjs','workspace-adoption-review.mjs','interaction-collection.mjs','record-comparison.mjs','narration-production.mjs','feed-narration-production.mjs'])files.set('bridge/'+name,readFileSync(join(repo,'integrations/browser',name)));
  files.set('data/creator/narration-workspace-client.mjs',readFileSync(join(repo,'components/BonfyreServiceSite/narration-workspace-client.mjs')));
  files.set('data/creator/creator-import.mjs',readFileSync(join(repo,'components/BonfyreServiceSite/creator-import.mjs')));
  const renderer=process.env.BONFYRE_BROADCAST_BINARY||join(repo,'cmd/BonfyreBroadcast/build/bonfyre-broadcast');
  const profiles=spawnSync(renderer,['profiles'],{encoding:'utf8',timeout:12000,maxBuffer:16384});
  const catalog=profiles.status===0&&typeof profiles.stdout==='string'?profiles.stdout:'';
  const profileRows=catalog.trim().split('\n');
  if(profileRows[0]!=='profile\tintro_ms\ttail_ms\tbed_gain\tduck_release_ms'||
     profileRows.length<6||!profileRows.slice(1).every(row=>
       /^[a-z]+\t[0-9]+\t[0-9]+\t0\.[0-9]{3}\t[0-9]+$/.test(row)))
    throw Error('Audio Studio needs the compiled BonfyreBroadcast sound profiles; the native owner is unavailable or inconsistent.');
  const profileBody=Buffer.from(catalog);
  files.set('data/creator/editorial-profiles.tsv',profileBody);
  sources.push(['data/creator/editorial-profiles.tsv',hash(profileBody),profileBody.length]);
  files.set('data/creator/editorial-sound-design-ui.mjs',readFileSync(join(repo,'components/BonfyreServiceSite/editorial-sound-design-ui.mjs')));
  files.set('data/creator/editorial-music-catalogue.mjs',readFileSync(join(repo,'components/BonfyreServiceSite/editorial-music-catalogue.mjs')));
  files.set('data/creator/editorial-music-library-client.mjs',readFileSync(join(repo,'components/BonfyreServiceSite/editorial-music-library-client.mjs')));
 files.set('CONTINUATIONS.tsv',continuationManifest([{view:'audio',label:'Recording session',entry:'audio/',prefix:'=draft:creator:'+packed.sample.id+':imports',title:'title',query:'workspace=@imports&view=@timeline'}],files));
 files.set('SOURCE.tsv',Buffer.from('owner\tcomponents/BonfyreServiceSite/creator-material.mjs\nsample_id\t'+packed.sample.id+'\nnative_mix_sha256\t'+packed.sample.native_sha256+'\ncompiler_sha256\t'+hash(readFileSync(import.meta.filename))+'\npath\tsha256\tbytes\n'+sources.map(row=>row.join('\t')).join('\n')+'\n'));
 return {...await retainWorkspaceFiles(files,{root}),entry:'audio/',family:'creator-audio',sample:packed.sample.id};
}
if(import.meta.main){const result=await materializeCreatorWorkspace({root:process.argv[2]});for(const [key,value]of Object.entries(result))console.log(key+'\t'+value);}
