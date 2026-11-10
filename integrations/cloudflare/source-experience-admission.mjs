import {join,resolve} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {artifacts} from '../experience-handoffs/artifacts.mjs';
import {decode,one,line,sha256,validSha,validId} from '../experience-handoffs/contract.mjs';
import {nativeMission} from './native-queue-adapter.mjs';
import {materializeSourceExperience} from './source-experience-projection.mjs';
// Native source-to-experience review admission.

const SOURCE='https://aurekai-work-samples.pages.dev/';
const MISSION='source:work-samples';
const FAMILY='experience-source-review';
const OPERATOR='queue.external.experience.source-candidate';

export async function prepareSourceExperienceHandoffs({owner,root,digest,mission=MISSION}){
  if(mission!==MISSION||!validSha(digest)||!owner||typeof owner.read!=='function'||typeof owner.run!=='function')
    throw Error('Only the owned source-sync mission can fan out into experience review.');
  root=resolve(root);
  const parentId=nativeMission({mission,family:'source-sync',digest});
  const parent=(await owner.read(parentId))[0];
  if(!parent||parent.mission_id!==parentId||parent.node_id!=='job'||parent.family!=='source-sync'||
     parent.operator_id!=='queue.external.source.digest'||parent.input_uri!=='urn:sha256:'+digest||
     parent.status!=='complete')throw Error('Verified native source completion is required.');
  const checkpoint=(await owner.run(['checkpoint-read',parentId,'job']))[0];
  if(!checkpoint||!Number.isSafeInteger(checkpoint.revision)||checkpoint.revision<1||
     checkpoint.artifact_uri!==parent.output_uri)throw Error('Source checkpoint does not match its completion.');
  const source={digest,body:join(root,'source',digest+'.html'),receipt:join(root,'source',digest+'.receipt.json')};
  const projected=materializeSourceExperience({source,root});
  if(parent.output_uri!==projected.uri||fileURLToPath(parent.output_uri)!==
      join(root,'experience-projections',projected.digest+'.yaff'))
    throw Error('Native completion is not the exact typed source projection.');
  const objects=artifacts(join(root,'source-experience-handoffs'));
  const sourceRows=decode(objects.read(fileURLToPath(projected.uri)).toString('utf8'));
  if(one(sourceRows,'authority')!=='none'||one(sourceRows,'source')!==SOURCE||
     one(sourceRows,'source_digest')!==digest)throw Error('Source projection authority or identity changed.');
  const candidates=sourceRows.get('candidate')??[];
  if(!candidates.length||candidates.length>16||candidates.length!==projected.candidates)
    throw Error('Source experience list requires review.');
  const seen=new Set(),admitted=[];
  for(const row of candidates){
    if(row.length!==3||!validId(row[0])||seen.has(row[0]))
      throw Error('Invalid or duplicate source experience identity.');
    seen.add(row[0]);
    const [experience,address,identity]=row;
    const sourceKey=sha256([digest,experience,address,identity].map(x=>x.length+':'+x).join(''));
    const parts=['handoff aurekai.handoff.v1'];
    parts.push(line('source',SOURCE),line('source_digest',digest));
    parts.push(line('source_projection_digest',projected.digest));
    parts.push(line('source_native_mission',parentId),line('source_native_revision',checkpoint.revision));
    parts.push(line('destination_experience',experience),line('destination_url',address));
    parts.push(line('source_identity',identity),line('handoff_identity',sourceKey));
    parts.push(line('assertion','owned_page_declares_route'),line('authority','none'));
    parts.push(line('disposition','requires_destination_review'));
    const body=parts[0]+String.fromCharCode(10)+parts.slice(1).join('');
    const stored=objects.put(body,'yaff');
    const childId='source-review-'+sourceKey.slice(0,40);
    const uri=pathToFileURL(stored.path).href;
    admitted.push({experience,artifactUri:uri,artifactDigest:stored.sha256,authority:'none'});
  }
  return {sourceDigest:digest,parentMission:parentId,admitted,disposition:'materialized_review_only'};
}
