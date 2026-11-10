import {createWorkspaceHost} from '../../../shared/interaction-98777b6f3a622c96dbfe/workspace-host.mjs';
import {loadWorkspaceDocking} from './workspace-dependencies.mjs';

export async function composePlannerWorkspace({root,catalogue,read,save,undo,redo,select,review,fail}) {
  const docking=await loadWorkspaceDocking(),ui=name=>root.querySelector(`[data-ui="${name}"]`);
  const source='fixture-catalogue:'+catalogue.digest;
  const focusButton=()=>[...root.querySelectorAll('.ps-workbench button')].find(b=>b.textContent==='Focus selection');
  const definitions=[
    ['canvas','SpatialEditor','Room canvas',()=>root.querySelector('.fp-center')],
    ['details','PropertyInspector','Space & project',()=>root.querySelector('.fp-sidebar')],
    ['materials','MaterialSchedule','Fixtures & quantities',()=>root.querySelector('.fp-summary')],
    ['collection','ProductCollection','Source collection',()=>ui('fixtures')],
    ['projects','ProjectLibrary','Saved alternatives',()=>ui('projects')],
    ['comparison','ScenarioCompare','Compare plans',()=>ui('compare')]
  ];
  const commands={canvas:[
    {id:'layout.undo',label:'Undo layout edit',group:'Layout',enabled:()=>!ui('undo').disabled,run:undo},
    {id:'layout.redo',label:'Redo layout edit',group:'Layout',enabled:()=>!ui('redo').disabled,run:redo},
    {id:'layout.rotate',label:'Rotate selected fixture',group:'Layout',enabled:()=>read().selected&&!ui('rotate').disabled||'Select a fixture first.',run:()=>ui('rotate').click()},
    {id:'layout.focus',label:'Focus selected fixture in 3D',group:'Inspect',enabled:()=>Boolean(focusButton())||'Open the 3D room first.',run:()=>focusButton().click()}
  ],details:[{id:'project.save',label:'Save this plan',group:'Project',enabled:()=>!ui('save').disabled,run:save}],materials:[{id:'project.review',label:'Review source-linked project brief',group:'Review',enabled:()=>true,run:review}]};
  const contributions=new Map(definitions.map(([id,kind,label,element])=>[id,{kind,label,element,commands:commands[id]??[],
    dirty:()=>read().dirty,acceptSelection:key=>read().plan.items.some(i=>i.key===key)}]));
  const scene={schema:'bonfyre-surface-scene.v1',source_digest:catalogue.digest,
    focus:{subject_address:source,actor_scope:'browser-local'},
    slots:definitions.map(([id,kind],i)=>({slot_id:id,slot_name:id,ordinal:i,extent:'workspace',accepts:[kind]})),
    bindings:definitions.map(([id,kind])=>({binding_id:id,slot_id:id,application_kind:kind,source_collection:catalogue.digest,continuity_key:source,action_aperture:(commands[id]??[]).map(c=>c.id).join(',')})),
    interaction:{desktop_posture:{title:'Design workspace',initial_layout:'design',layouts:[
      {id:'design',label:'Design & source',panels:[{id:'canvas'},{id:'materials',relative:'canvas',direction:'right',width:300},{id:'details',relative:'materials',direction:'below',height:330}]},
      {id:'focus',label:'Focused canvas',panels:[{id:'canvas'},{id:'materials',relative:'canvas',direction:'right',width:280}]},
      {id:'compare',label:'Compare alternatives',panels:[{id:'comparison'},{id:'projects',relative:'comparison',direction:'left',width:320},{id:'materials',relative:'comparison',direction:'below',height:220}]},
      {id:'cost',label:'Quantities & specification',panels:[{id:'materials'},{id:'collection',relative:'materials',direction:'right',width:480},{id:'details',relative:'materials',direction:'below',height:240}]}
    ]},command_aperture:{shortcut:'Meta+K'}}};
  const work=createWorkspaceHost(root,{scene,contributions,docking,before:ui('layout'),report:fail});
  const unused=ui('layout');unused.hidden=true;unused.style.display='none';
  let lastView='layout';
  return {...work,
    renderView(view){for(const name of ['fixtures','projects','compare'])ui(name).hidden=false;ui('save-controls').hidden=false;
      if(view!==lastView){lastView=view;if(view==='compare')work.arrange('compare');else if(view==='layout')work.arrange('design');else work.open(view==='fixtures'?'collection':view,{side:true});}return true;},
    selected(key){work.session.select('canvas',key??null);},
    refresh(){work.refresh();}
  };
}
