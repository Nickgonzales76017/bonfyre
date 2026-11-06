import {openSync,closeSync,fstatSync,readSync,constants} from 'node:fs';
import {compileCreatorProgram,editCreatorTimeline} from './creator-render-program.mjs';
import {planNativePcmWindow} from './creator-native-pcm.mjs';

const LIMIT=16*1024*1024;
function metadata(path){
  if(!path)throw Error('creator requires a metadata file');
  const fd=openSync(path,constants.O_RDONLY|constants.O_NONBLOCK);
  try{
    const before=fstatSync(fd,{bigint:true});
    if(!before.isFile()||before.size>BigInt(LIMIT))throw Error('creator metadata must be a regular file of at most 16 MiB');
    const body=Buffer.alloc(Number(before.size)+1);let count=0;
    while(count<body.length){const n=readSync(fd,body,count,body.length-count,null);if(!n)break;count+=n;}
    const after=fstatSync(fd,{bigint:true});
    if(count!==Number(before.size)||after.size!==before.size||after.mtimeNs!==before.mtimeNs||after.ctimeNs!==before.ctimeNs)
      throw Error('creator metadata changed while reading');
    return JSON.parse(body.subarray(0,count).toString('utf8'));
  }finally{closeSync(fd);}
}

// Metadata compilation only. Source media and the requested output are never
// opened, and no native process is started by any of these commands.
export function runCreatorCommand(command,args){
  const allowed={
    'creator:compile':['--input','--shard-frames','--from','--to','--source-projection'],
    'creator:edit':['--input','--edit'],
    'creator:pcm-plan':['--plan','--bindings','--window-index','--channels','--output','--sparse-silence']
  }[command];
  if(!allowed)throw Error('unknown Creator command');
  const values=new Map();
  for(let i=0;i<args.length;i++){
    const flag=args[i];
    if(!allowed.includes(flag)||values.has(flag))throw Error('unknown or repeated Creator option: '+flag);
    if(flag==='--sparse-silence'){values.set(flag,true);continue;}
    const value=args[++i];
    if(value===undefined||value.startsWith('--'))throw Error('missing Creator option value: '+flag);
    values.set(flag,value);
  }
  const number=(flag,fallback)=>{
    if(!values.has(flag)){if(fallback!==undefined)return fallback;throw Error('Creator requires '+flag);}
    const raw=values.get(flag),n=Number(raw);
    if(!/^(0|[1-9][0-9]*)$/.test(raw)||!Number.isSafeInteger(n))throw Error('Creator requires an unsigned integer: '+flag);
    return n;
  };
  if(command==='creator:edit')return editCreatorTimeline(metadata(values.get('--input')),metadata(values.get('--edit')));
  if(command==='creator:compile'){
    if(values.has('--from')!==values.has('--to'))throw Error('--from and --to must be supplied together');
    return compileCreatorProgram(metadata(values.get('--input')),{
      shardFrames:number('--shard-frames',256),
      range:values.has('--from')?{start:number('--from'),end:number('--to')}:null,
      sourceProjection:values.get('--source-projection')??'all'
    });
  }
  return planNativePcmWindow(metadata(values.get('--plan')),metadata(values.get('--bindings')),{
    windowIndex:number('--window-index'),channels:number('--channels'),output:values.get('--output'),
    sparseSilence:values.has('--sparse-silence')
  });
}
