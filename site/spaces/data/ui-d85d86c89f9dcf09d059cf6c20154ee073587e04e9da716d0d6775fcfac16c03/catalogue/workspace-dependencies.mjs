let pending;
export function loadWorkspaceDocking(){return pending??=(async()=>{
const href='/bonfyre/spaces/assets/workspace-docking/dockview-8.5.1.css';
if(!document.querySelector('link[data-workspace-docking]'))await new Promise((resolve,reject)=>{const link=document.createElement('link');link.dataset.workspaceDocking='true';link.rel='stylesheet';link.href=href;link.onload=resolve;link.onerror=()=>{link.remove();reject(Error('Workspace layout styles could not load.'));};document.head.append(link);});
return import('/bonfyre/spaces/assets/workspace-docking/dockview-8.5.1.mjs');
})().catch(error=>{pending=null;throw error;});}
