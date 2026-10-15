import {readFileSync,lstatSync} from 'node:fs';
import {join,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
/** Runs only in the existing ServiceSite packaging owner. */
export function productSceneFiles() {
  const files={},root=dirname(fileURLToPath(import.meta.url));
  for(const name of ['product-scene.mjs','product-scene-model.mjs','product-scene-store.mjs','product-scene-engine.mjs','planner-product-scene.mjs','catalogue-product-scene.mjs'])files[name]=readFileSync(join(root,name));
  const prefix='vendor/three-0.180.0',base=join(root,prefix);
  const sums=readFileSync(join(base,'SHA256SUMS.tsv'),'utf8').trim().split('\n').slice(1);let total=0;
  for(const line of sums){const [path,digest,length]=line.split('\t');if(!path||path.startsWith('/')||path.split('/').some(p=>!p||p==='..'||p==='.')||!/^[a-f0-9]{64}$/.test(digest)||!/^\d+$/.test(length))throw Error('Invalid renderer package inventory.');
    let parent=base;for(const part of path.split('/')){parent=join(parent,part);if(lstatSync(parent).isSymbolicLink())throw Error('Renderer package symlinks are refused.');}
    const body=readFileSync(join(base,path));total+=body.length;if(total>16*1024*1024||sums.length>100)throw Error('Renderer package exceeds its build budget.');if(body.length!==Number(length)||createHash('sha256').update(body).digest('hex')!==digest)throw Error('Renderer package content changed: '+path);files[prefix+'/'+path]=body;
  }
  for(const name of ['LICENSE','SOURCE.txt','SHA256SUMS.tsv'])files[prefix+'/'+name]=readFileSync(join(base,name));
  return files;
}
