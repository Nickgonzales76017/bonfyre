import {createNativeSession} from './native-session.mjs';
import {createProviderBridge} from './provider-continuation.mjs';
const native=createNativeSession(chrome.runtime),bridge=createProviderBridge({chrome,ask:(operation,more)=>native.ask(operation,more),privateContext:chrome.extension.inIncognitoContext===true});
chrome.sidePanel.setPanelBehavior({ openPanelOnActionClick: true }).catch(() => {});
chrome.runtime.onMessage.addListener((message,sender,respond)=>{
 if(message?.channel==='aurekai-provider-v1'){
  try{Promise.resolve(bridge.handle(message,sender)).then(result=>respond({ok:true,result}),error=>respond({ok:false,reason:error?.message||'Provider continuation was refused.',...([401,403].includes(error?.status)?{status:error.status}:{}),...(error?.code==='binding_changed'?{code:error.code}:{})}));}
  catch(error){respond({ok:false,reason:error?.message||'Provider continuation was refused.'});}
  return true;
 }
 if(message?.channel==='aurekai-provider-panel-v1'&&message.operation==='return'&&sender.id===chrome.runtime.id&&sender.url===chrome.runtime.getURL('panel.html?provider=1')){
  bridge.returnToAurekai(message.windowId).then(result=>respond({ok:true,result}),error=>respond({ok:false,reason:error?.message||'Return to Aurekai from its work card.'}));return true;
 }
});
chrome.tabs.onReplaced.addListener((added,removed)=>{bridge.replaced(added,removed).catch(()=>{});});
