// Loopback-only static server for browser acceptance, including HTTP byte ranges.
// Real recorded narration must be seekable under the same media transport
// expectations as public Pages; Python's basic HTTP server lacks Range support.
import {createServer} from 'node:http';
import {createReadStream,statSync} from 'node:fs';
import {resolve,extname,sep,join} from 'node:path';
import {pathToFileURL} from 'node:url';

const types={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.mjs':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json; charset=utf-8','.tsv':'text/tab-separated-values; charset=utf-8','.svg':'image/svg+xml','.png':'image/png','.jpg':'image/jpeg','.jpeg':'image/jpeg','.webp':'image/webp','.gif':'image/gif','.mp3':'audio/mpeg','.wav':'audio/wav','.woff2':'font/woff2'};
export function staticSiteServer(siteDir='site'){
  const root=resolve(siteDir);
  return createServer((req,res)=>{
    if(req.method!=='GET'&&req.method!=='HEAD'){
      res.writeHead(405,{'Allow':'GET, HEAD'});res.end();return;
    }
    let pathname;
    try{pathname=decodeURIComponent(new URL(req.url,'http://127.0.0.1').pathname);}
    catch{res.writeHead(400);res.end();return;}
    if(pathname.includes('\0')){res.writeHead(400);res.end();return;}
    const full=resolve(root,'.'+pathname);
    if(full!==root && !full.startsWith(root+sep)){
      res.writeHead(403);res.end();return;
    }
    let file=full,stat;
    try{
      stat=statSync(file);
      if(stat.isDirectory()){file=join(file,'index.html');stat=statSync(file);}
      if(!stat.isFile())throw Error('Not a file');
    }catch{res.writeHead(404,{'Content-Type':'text/plain'});res.end('Not found');return;}
    const headers={'Content-Type':types[extname(file).toLowerCase()]??'application/octet-stream',
      'Accept-Ranges':'bytes','Cache-Control':'no-store'};
    let start=0,end=stat.size-1,status=200;
    const range=req.headers.range;
    if(range){
      const m=/^bytes=(\d+)-(\d*)$/.exec(range);
      if(!m){res.writeHead(416,{'Content-Range':'bytes */'+stat.size});res.end();return;}
      start=Number(m[1]);end=m[2]?Number(m[2]):end;
      if(!Number.isSafeInteger(start)||!Number.isSafeInteger(end)||start>end||start>=stat.size){
        res.writeHead(416,{'Content-Range':'bytes */'+stat.size});res.end();return;
      }
      end=Math.min(end,stat.size-1);status=206;
      headers['Content-Range']='bytes '+start+'-'+end+'/'+stat.size;
    }
    headers['Content-Length']=String(Math.max(0,end-start+1));
    res.writeHead(status,headers);
    if(req.method==='HEAD'||stat.size===0){res.end();return;}
    const stream=createReadStream(file,{start,end});
    stream.on('error',error=>res.destroy(error));
    stream.pipe(res);
  });
}
if(process.argv[1] && import.meta.url===pathToFileURL(resolve(process.argv[1])).href){
  const path=process.argv[2]??'site',port=Number(process.argv[3]??8765);
  if(!Number.isInteger(port)||port<1||port>65535)throw Error('Invalid port');
  staticSiteServer(path).listen(port,'127.0.0.1',()=>{
    console.log('RANGE_STATIC_SITE_READY http://127.0.0.1:'+port+'/');
  });
}
