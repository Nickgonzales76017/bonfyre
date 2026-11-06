// Presentation only. Returning or closing the panel never completes native work.
const button=document.getElementById('provider-return'),status=document.getElementById('provider-work-said');
button.addEventListener('click',async()=>{
 button.disabled=true;status.textContent='Returning to Aurekai…';
 try{
  const current=await chrome.windows.getCurrent();
  const answer=await chrome.runtime.sendMessage({channel:'aurekai-provider-panel-v1',operation:'return',windowId:current.id});
  status.textContent=answer?.ok?'Your native work remains available in Aurekai.':answer?.reason||'Open the exact work card in Aurekai to continue.';
 }catch{status.textContent='Open the exact work card in Aurekai to continue.';}
 finally{button.disabled=false;}
});
