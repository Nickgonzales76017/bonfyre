#!/usr/bin/env node
// Presentation-only code generation: Bonfyre C remains the renderer.
// Business copy is authored once at branding/voice.mjs.
import {readFileSync,writeFileSync} from 'node:fs';
import {join,resolve,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {AUREKAI_VOICE} from '../../../branding/voice.mjs';

const root=resolve(dirname(fileURLToPath(import.meta.url)),'..');
const target=join(root,'generated/native_brand_cuts.h');
const order=['work','life','live','shop'];
const quote=value=>{
 if(typeof value!=='string'||!value||value.length>420||/[\r\n\x00-\x1f]/u.test(value))
   throw Error('Native brand copy must be bounded and plain');
 return JSON.stringify(value);
};
const keys=values=>Object.keys(values).sort().join(',');
if(keys(AUREKAI_VOICE.channels)!==[...order].sort().join(',')||
   keys(AUREKAI_VOICE.fronts)!==[...order].sort().join(','))
 throw Error('Missing or unrecognized Aurekai territory');
const signature=AUREKAI_VOICE.signature.match(/^(.+\.) (.+\.)$/u);
if(!signature)throw Error('Unexpected signature grammar');
const lines=[
 '#ifndef BONFYRE_NATIVE_BRAND_CUTS_H',
 '#define BONFYRE_NATIVE_BRAND_CUTS_H',
 '/* GENERATED from branding/voice.mjs. Regenerate with compile_brand_cuts.mjs. */',
 'static const char *const front_brand_signature_first='+quote(signature[1])+';',
 'static const char *const front_brand_signature_second='+quote(signature[2])+';',
 'static const FrontBrandCut front_brand_cuts[]={'
];
for(const [index,territory] of order.entries()){
 const channel=AUREKAI_VOICE.channels[territory],cut=AUREKAI_VOICE.fronts[territory];
 if(cut.index!==String(index+1).padStart(2,'0')+' / 04')
   throw Error('Unexpected edition order: '+territory);
 if(!/^\/(?!\/)[a-z0-9/?=&-]+$/u.test(cut.action)||cut.action.includes('..'))
   throw Error('Brand action is not a safe local route: '+territory);
 lines.push('    {'+[territory,cut.index,cut.category,channel.title,cut.detail,cut.action,cut.label].map(quote).join(', ')+'},');
}
lines.push('};','#endif','');
const desired=lines.join('\n');
if(process.argv.includes('--check')){
 if(readFileSync(target,'utf8')!==desired){
   console.error('NATIVE_BRAND_DRIFT: generated C differs from branding/voice.mjs');
   process.exitCode=1;
 }else console.log('NATIVE_BRAND_PARITY_PASS four domains, five fields and signature');
}else{
 writeFileSync(target,desired);
 console.log('NATIVE_BRAND_COMPILED four domains, five fields and signature');
}
