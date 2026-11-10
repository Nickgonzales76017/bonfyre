// Validate transport inputs before touching credentials or issuing codes.
export function authorizationRequestValid(params) {
  const get=key=>params?.get?.(key)??'';
  const redirect=get('redirect_uri'),verifier=get('code_challenge'),client=get('client_id');
  const scope=(get('scope')||'write').trim();
  if(typeof redirect!=='string'||redirect.length>2048||
     typeof client!=='string'||!client||client.length>512||
     typeof verifier!=='string'||!/^[A-Za-z0-9_-]{43}$/.test(verifier)||
     get('code_challenge_method')!=='S256'||
     !['read','write','read write','write read'].includes(scope))return false;
  try{
    const uri=new URL(redirect);
    return uri.protocol==='https:'&&Boolean(uri.hostname)&&!uri.username&&!uri.password&&!uri.hash&&
      uri.origin!=='null'&&redirect.startsWith('https://')&&!/\s/.test(redirect);
  }catch{return false;}
}
