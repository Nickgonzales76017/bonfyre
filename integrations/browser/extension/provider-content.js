// Isolated content script. It accepts work coordinates only. The background
// reads their native admission itself in this exact top-level document.
(()=>{
 if(window.top!==window||!['https://aurekai.work','https://aurekai.life','https://aurekai.live','https://aurekai.shop'].includes(location.origin))return;
 let gesture=null;
 document.addEventListener('click',event=>{const button=event.target?.closest?.('[data-native-provider-continue]');gesture=event.isTrusted&&button?{button,at:performance.now()}:null;},true);
 const reply=(id,value)=>window.dispatchEvent(new CustomEvent('aurekai:provider:reply',{detail:{requestId:id,...value}}));
 window.addEventListener('aurekai:provider:request',event=>{
  const request=event.detail;
  if(!request||typeof request!=='object'||Object.keys(request).some(k=>!['requestId','operation','review'].includes(k))||typeof request.requestId!=='string'||!/^\w{32}$/.test(request.requestId)||!['capabilities','continue','clear'].includes(request.operation))return;
  if(request.operation==='continue'){
   const button=gesture?.button,valid=navigator.userActivation?.isActive&&gesture&&performance.now()-gesture.at<1000&&button.isConnected&&button.dataset.nativeProviderContext===request.review?.context&&button.dataset.nativeProviderJob===request.review?.jobId&&button.dataset.nativeProviderRevision===String(request.review?.revision);
   gesture=null;if(!valid){reply(request.requestId,{ok:false,reason:'Use the provider button in the current Aurekai work card.'});return;}
  }
  // Neither the webpage nor a synthetic DOM event can supply gesture=true.
  // It is read from Chrome's isolated, transient activation state at dispatch.
  const message={channel:'aurekai-provider-v1',operation:request.operation};
  if(request.operation==='continue'){message.review=request.review;message.gesture=true;}
  chrome.runtime.sendMessage(message).then(value=>reply(request.requestId,value),()=>reply(request.requestId,{ok:false,reason:'The native browser companion is unavailable.'}));
 });
})();
