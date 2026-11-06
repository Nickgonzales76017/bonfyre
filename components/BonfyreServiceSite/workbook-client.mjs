import {mountWorkbookEditor} from './workbook-editor.mjs';
import {createWorkbookLifetime,clearWorkbookHost} from './workbook-lifetime.mjs';
await Promise.all([...document.querySelectorAll('[data-workbook-config]')].map(async host=>{
 const lifetime=createWorkbookLifetime();let editor;
 lifetime.onClose(()=>{editor?.dispose();editor=null;clearWorkbookHost(host);});
 try{
  const url=new URL(host.dataset.workbookConfig,location.href);if(url.origin!==location.origin)throw Error('Workbook configuration must belong to this site.');
  const response=await fetch(url,{credentials:'same-origin',cache:'no-store',signal:lifetime.signal});lifetime.assert();
  if(!response.ok)throw Error('The review workbook could not be opened.');
  const config=await response.json();lifetime.assert();
  editor=await mountWorkbookEditor(host,{...config,lifetime});lifetime.assert();host.dataset.ready='true';
 }catch(error){if(!lifetime.closed){host.textContent=error.message;host.setAttribute('role','alert');}}
}));
