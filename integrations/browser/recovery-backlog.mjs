import {routeRecoveredNative} from './recovery-native-routing.mjs';

const workFor=Object.freeze({
  owner_available:{bucket:'connect_existing_owner',priority:0},
  surface_owner:{bucket:'connect_existing_surface',priority:0},
  native_owner:{bucket:'bind_native_owner',priority:0},
  owner_unready:{bucket:'repair_existing_owner',priority:1},
  owner_not_installed:{bucket:'publish_or_install_owner',priority:1},
  needs_bridge:{bucket:'build_execution_bridge',priority:2},
  project_review:{bucket:'promote_reusable_capability',priority:2},
  review:{bucket:'manual_routing_review',priority:3}
});

export function recoveredBacklog(records,{installed=[]}={}){
  if(!Array.isArray(records))throw Error('records must be an array');
  const rows=records.map(record=>{
    const value=record?.value??record,route=routeRecoveredNative(value,{installed}),work=workFor[route.state]??workFor.review;
    return {
      key:String(record?.key??''),
      revision:Number(record?.revision??0),
      title:route.identity.title,
      group:route.identity.group,
      state:route.state,
      bucket:work.bucket,
      priority:work.priority,
      owner_type:route.selected?.owner_type??null,
      owner:route.selected?.owner??route.selected?.pack??null,
      binding:route.selected?.binding??null,
      editor_url:route.selected?.editor_url??null,
      route:route.selected?.route??null,
      reason:route.selected?.reason??route.boundary,
      source_count:route.identity.sourceCount,
      input_digest:route.identity.inputDigest
    };
  });
  rows.sort((a,b)=>a.priority-b.priority||a.bucket.localeCompare(b.bucket)||a.title.localeCompare(b.title)||a.key.localeCompare(b.key));
  const buckets={},states={},owners={};
  for(const row of rows){
    buckets[row.bucket]=(buckets[row.bucket]??0)+1;
    states[row.state]=(states[row.state]??0)+1;
    if(row.owner)owners[row.owner]=(owners[row.owner]??0)+1;
  }
  return {
    schema:'bonfyre.recovered-backlog.v1',
    total:rows.length,
    buckets,states,owners,rows,
    boundary:'An available owner is not a completed handoff. This queue separates owner discovery from the missing transfer, installation, execution and review work.'
  };
}

export const recoveredBacklogWork=workFor;
