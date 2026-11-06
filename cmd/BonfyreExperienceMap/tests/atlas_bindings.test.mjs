import {test, expect} from 'bun:test';
import {atlasBindings, sourceClassification} from '../tools/atlas_bindings.mjs';

const chains = new Map([['learning',{}],['creator-publish',{}],['finance',{}]]);
const paths = ['cmd/BonfyreCMS/content-types/program.json','cmd/BonfyreCMS/content-types/payment.json'];
const record = (chain, path) => ({experience_chains:chain, source_paths:[path]});

test('exact source joins preserve multiple owners without classifying neighboring schemas or the CMS', () => {
  const bindings = atlasBindings({architectures:{
    lessons:record(['learning'],paths[0]),
    publishing:record(['creator-publish'],paths[0]),
    money:record(['finance'],paths[1]),
  }},chains,path=>paths.includes(path));
  expect(sourceClassification(bindings, paths[0], ['knowledge'])).toEqual({basis:'atlas-source-path',human_chains:['learning','creator-publish'],architecture_ids:['lessons','publishing']});
  expect(sourceClassification(bindings, paths[1], ['knowledge']).human_chains).toEqual(['finance']);
  for(const path of ['cmd/BonfyreCMS','cmd/BonfyreCMS/content-types/program_backup.json'])
    expect(sourceClassification(bindings,path,['knowledge'])).toEqual({basis:'name-hint-or-fallback',human_chains:['knowledge'],architecture_ids:[]});
});

test('source correction removes the previous workflow projection on the next compilation', () => {
  const index={architectures:{program:record(['learning'],paths[0])}};
  expect(sourceClassification(atlasBindings(index,chains,()=>true),paths[0],[]).human_chains).toEqual(['learning']);
  index.architectures.program.experience_chains=['creator-publish'];
  expect(sourceClassification(atlasBindings(index,chains,()=>true),paths[0],[]).human_chains).toEqual(['creator-publish']);
  delete index.architectures.program;
  expect(sourceClassification(atlasBindings(index,chains,()=>true),paths[0],['knowledge']).architecture_ids).toEqual([]);
});

test('unknown chains, duplicate declarations and missing source evidence refuse compilation', () => {
  for(const bad of [record(['invented'],paths[0]),record(['learning','learning'],paths[0]),{experience_chains:['learning'],source_paths:[]},record(['learning'],'absent')])
    expect(()=>atlasBindings({architectures:{bad}},chains,path=>paths.includes(path))).toThrow();
  expect(atlasBindings({architectures:{unrelated:{}}},chains,()=>false).architectures).toEqual([]);
});
