export async function boundedBody(request,limit=4096) {
  if(request.headers.get('content-encoding'))throw Error('encoded body refused');
  if(Number(request.headers.get('content-length')??0)>limit)throw Error('oversized intent');
  if(!request.body)return '';
  const reader=request.body.getReader(),chunks=[];let size=0;
  try {
    for(;;) {
      const {done,value}=await reader.read();if(done)break;
      size+=value.byteLength;
      if(size>limit){await reader.cancel();throw Error('oversized intent');}
      chunks.push(value);
    }
  } finally {reader.releaseLock();}
  const bytes=new Uint8Array(size);let offset=0;
  for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.byteLength;}
  return new TextDecoder('utf-8',{fatal:true}).decode(bytes);
}
