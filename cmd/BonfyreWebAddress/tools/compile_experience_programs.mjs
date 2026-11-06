// Compile one declarative SurfaceScene experience grammar into native C zones.
// This deliberately consumes the canonical, already-compiled PageComposition
// region table instead of declaring a second material/role list.
import {readFileSync,writeFileSync} from 'node:fs';
import {resolve,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {EXPERIENCE_PROGRAMS,EXPERIENCE_APEX_ROUTES,experienceZone} from '../../BonfyreSurfaceScene/runtime/experience-programs.mjs';
import {STITCH_PATTERNS} from '../../BonfyreSurfaceScene/runtime/stitch-grammar.mjs';

const root=resolve(dirname(fileURLToPath(import.meta.url)),'..');
const input=process.argv[2]||resolve(root,'generated/native_page_composition.h');
const output=process.argv[3]||resolve(root,'generated/native_experience_composition.h');
const routesOutput=resolve(root,'generated/native_experience_routes.h');
const source=readFileSync(input,'utf8');
const group=source.match(/static const BfPageRegion bf_page_regions\[\] = \{([\s\S]*?)\n\};/);
if(!group)throw Error('Missing the native PageComposition region table');
const roles=[...group[1].matchAll(/^\s*\{"([a-z][a-z0-9_-]*)",/gm)].map(m=>m[1]);
if(roles.length<12||roles.length>64||roles.length!==new Set(roles).size)
 throw Error('Invalid or repeated native PageComposition regions');
const known=new Set(roles);
const table=[
 '/* Generated from SurfaceScene experience-programs.mjs and native PageComposition. Do not edit. */',
 '#ifndef BONFYRE_NATIVE_EXPERIENCE_COMPOSITION_H',
 '#define BONFYRE_NATIVE_EXPERIENCE_COMPOSITION_H',
 'typedef enum { BF_EXP_PRIMARY=0, BF_EXP_HERO=1, BF_EXP_RAIL=2, BF_EXP_WIDE=3 } BfExperienceLane;',
 'enum { BF_EXP_VIEW_FOCUS=1, BF_EXP_VIEW_COMPARE=2 };',
 'typedef struct {const char *pattern; const char *shell; unsigned views; unsigned char lane[BF_PAGE_REGION_COUNT];} BfNativeExperienceProgram;',
 'static const BfNativeExperienceProgram bf_native_experience_programs[] = {'
];
for(const [pattern,recipe]of Object.entries(EXPERIENCE_PROGRAMS)){
 for(const lane of ['hero','rail','wide'])for(const role of recipe[lane])
  if(!known.has(role))throw Error(`Experience ${pattern} uses unknown PageComposition region ${role}`);
 const codes={primary:'BF_EXP_PRIMARY',hero:'BF_EXP_HERO',rail:'BF_EXP_RAIL',wide:'BF_EXP_WIDE'};
 const flags=(recipe.views.includes('focus')?1:0)|(recipe.views.includes('compare')?2:0);
 const cells=roles.map(role=>codes[experienceZone(pattern,role)]);
 table.push('  {'+JSON.stringify(pattern)+','+JSON.stringify(STITCH_PATTERNS[pattern].shell)+','+flags+', {'+cells.join(',')+'}},');
}
table.push('};');
table.push('#define BF_NATIVE_EXPERIENCE_COUNT (sizeof(bf_native_experience_programs)/sizeof(bf_native_experience_programs[0]))');
table.push('#define BF_NATIVE_EXPERIENCE_REGION_COUNT '+roles.length);
table.push('#endif');
const rendered=table.join('\n')+'\n';
const routeTables=[
 '/* Generated from SurfaceScene experience-programs.mjs. Do not edit. */',
 '#ifndef BONFYRE_NATIVE_EXPERIENCE_ROUTES_H',
 '#define BONFYRE_NATIVE_EXPERIENCE_ROUTES_H'
];
for(const [apex,routes] of Object.entries(EXPERIENCE_APEX_ROUTES)){
 routeTables.push('static const BfStitchWorkRoute bf_stitch_'+apex+'_routes[] = {');
 for(const [route,pattern] of routes)routeTables.push('  {'+JSON.stringify(route)+','+JSON.stringify(pattern)+'},');
 routeTables.push('};');
}
routeTables.push('#endif');
const routesRendered=routeTables.join('\n')+'\n';
if(process.argv.includes('--check')){
 if(readFileSync(output,'utf8')!==rendered)throw Error('Native experience compilation drift');
 if(readFileSync(routesOutput,'utf8')!==routesRendered)throw Error('Native experience route compilation drift');
}else {writeFileSync(output,rendered);writeFileSync(routesOutput,routesRendered);}
