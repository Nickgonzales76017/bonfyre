// Bound, account-scoped page template for the Source Library's Warehouse view.
export function warehouseStudioPage({navigation,navigationCSS}={}){
 if(typeof navigation!=='string'||!navigation.includes('__WORKSPACE_NAV__')||
    typeof navigationCSS!=='string')throw Error('Use the native workspace navigation contract.');
 const css=[
  '[data-warehouse-studio]{font:15px/1.6 system-ui;max-width:1500px;margin:auto;padding:clamp(22px,4vw,50px);color:#163729}',
  '[data-warehouse-studio] h1{font:400 clamp(36px,5vw,62px)/1.05 Georgia,serif;letter-spacing:-.035em}',
  '[data-warehouse-studio] h2{font:400 29px/1.2 Georgia,serif}',
  '[data-warehouse-studio] label{display:block;margin:16px 0;font-weight:600}',
  '[data-warehouse-studio] input[type=file],[data-warehouse-studio] input[type=search]{display:block;max-width:100%;width:min(550px,100%);padding:12px;background:white;border:1px solid #a6b7ac;border-radius:10px;margin-top:6px}',
  '[data-warehouse-studio] button{padding:10px 15px;background:#d9e5d2;color:#163729;border:1px solid #9cab95;border-radius:9px;min-height:44px;cursor:pointer}',
  '[data-warehouse-studio] button:hover{background:#c6dec0}',
  '[data-warehouse-studio] .wp-actions{display:flex;align-items:center;flex-wrap:wrap;gap:16px}',
  '[data-warehouse-studio] .wp-split{display:grid;grid-template-columns:minmax(250px,1fr) minmax(300px,2fr);gap:24px}',
  '[data-warehouse-studio] .wp-list,[data-warehouse-studio] .wp-detail{border:1px solid #c2d1c2;background:#f9fbf7;border-radius:16px;padding:20px;min-width:0}',
  '[data-warehouse-studio] ul{list-style:none;margin:0;padding:0}',
  '[data-warehouse-studio] li{margin:8px 0}',
  '[data-warehouse-studio] .wp-source{width:100%;text-align:left;overflow-wrap:anywhere}',
  '[data-warehouse-studio] .wp-source[data-state=unavailable]{opacity:.6}',
  '[data-warehouse-studio] .wp-source[aria-current=true]{outline:2px solid #285d40}',
  '[data-warehouse-studio] dt{font-weight:700;margin-top:10px}',
  '[data-warehouse-studio] dd{overflow-wrap:anywhere;margin:0 0 8px}',
  '[data-warehouse-studio] fieldset{border:1px solid #b9cabb;border-radius:12px;margin:20px 0}',
  '[data-warehouse-studio] fieldset label{font-weight:400;margin:8px 0}',
  '[data-warehouse-studio] small{display:block;color:#4b6353}',
  '@media(max-width:800px){[data-warehouse-studio] .wp-split{grid-template-columns:1fr}}',
  '@media(prefers-reduced-motion:reduce){[data-warehouse-studio] *{scroll-behavior:auto}}'
 ].join('');
 const repositoryCSS=css.replaceAll('[data-warehouse-studio]','[data-repository-cut-studio]')+
  '[data-repository-cut-studio]{border-top:2px solid #97b4a0;margin-top:16px}'+
  '[data-repository-cut-studio] textarea{width:100%;max-width:100%;box-sizing:border-box;min-height:140px;overflow-wrap:anywhere}'+
  '[data-repository-cut-studio] select{min-height:44px;max-width:100%;padding:8px}'+
  '[data-repository-cut-studio] .repository-fpq-review{border-top:1px solid #9bb9a3;margin-top:24px;padding-top:14px}'+
  '[data-repository-cut-studio] pre{white-space:pre-wrap;overflow-wrap:anywhere;max-width:100%}'+
  '[data-repository-cut-studio] p,[data-repository-cut-studio] small,[data-repository-cut-studio] section{min-width:0;max-width:100%;overflow-wrap:anywhere}';
 const html='<html lang="en"><head><meta charset="utf-8">'
  +'<meta name="viewport" content="width=device-width,initial-scale=1">'
  +'<meta name="bonfyre-workspace-store" content="/apps/__WORKSPACE__/_records">'
  +'<meta name="bonfyre-workspace-context" content="__DESTINATION__">'
  +'<meta name="bonfyre-workspace-context-selector" content="__CONTEXT__">'
  +'<meta name="bonfyre-workspace-release" content="__RELEASE__">'
  +'<title>Warehouse intelligence · Aurekai</title>'
  +'<link rel="stylesheet" href="../library.css">'
  +'<style>'+navigationCSS+css+repositoryCSS+'</style></head><body>'
  +navigation
  +'<main data-warehouse-studio>'
  +'<noscript>JavaScript is required to review a native portfolio. No sources have been modified.</noscript>'
  +'</main><section data-repository-cut-studio></section>'+
  '<script type="module" src="../warehouse-studio.mjs"></script>'+
  '<script type="module" src="../repository-cut-studio.mjs"></script></body></html>';
 return '<!doctype html>'+html;
}
