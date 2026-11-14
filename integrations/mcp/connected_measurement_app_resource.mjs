import {readFile} from 'node:fs/promises';
export const CONNECTED_APP_URI='ui://aurekai/connected-performance/v1.html';
export const CONNECTED_APP_RESOURCE={
 uri:CONNECTED_APP_URI,name:'Aurekai connected performance',
 description:'Source-dated channel performance for the current Bonfyre account, with no provider effects.',
 mimeType:'text/html;profile=mcp-app',
 _meta:{ui:{csp:{connectDomains:[],resourceDomains:[],frameDomains:[],baseUriDomains:[]}}}
};
export async function readConnectedApp(uri,file=process.env.BONFYRE_CONNECTED_MCP_APP_FILE||new URL('./dist/connected-performance.html',import.meta.url)){
 if(uri!==CONNECTED_APP_URI)return null;
 const html=await readFile(file,'utf8');
 if(Buffer.byteLength(html)>512*1024||!html.startsWith('<!doctype html>'))throw Error('Invalid connected performance application bundle.');
 return {contents:[{...CONNECTED_APP_RESOURCE,text:html}]};
}
