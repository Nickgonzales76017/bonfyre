// Provider continuation uses the existing companion without loading the separate
// operator enrollment or page-retention surfaces. Those retain their own entry.
const provider=new URL(location.href).searchParams.get('provider')==='1';
document.querySelector(provider?'#provider-work':'#page-tools').hidden=false;
if(provider)await import('./panel-provider.mjs');
else for(const [src,type] of [['panel.js','text/javascript'],['panel-enrollment.mjs','module'],['panel-commerce.mjs','module']]){
 const script=document.createElement('script');script.src=src;script.type=type;document.body.append(script);
}
