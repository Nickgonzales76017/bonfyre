import {Database} from 'bun:sqlite';
import {randomUUID} from 'node:crypto';

// Transport replay state only. WorkGraph remains the authority for admitted work.
export function nonceStore(path,{clock=()=>Date.now(),leaseMs=30000}={}) {
  if(typeof path!=='string'||!path||!Number.isSafeInteger(leaseMs)||leaseMs<1) throw Error('invalid replay configuration');
  const db=new Database(path,{create:true,strict:true});
  db.run('PRAGMA busy_timeout=5000');
  db.run('PRAGMA journal_mode=WAL');
  db.run('PRAGMA synchronous=FULL');
  if(db.query('PRAGMA synchronous').get().synchronous!==2) { db.close();throw Error('replay durability refused'); }
  db.run("CREATE TABLE IF NOT EXISTS cloud_nonce(nonce TEXT PRIMARY KEY,expires_ms INTEGER NOT NULL,state TEXT NOT NULL DEFAULT 'pending',lease_until_ms INTEGER NOT NULL DEFAULT 0)");
  const cols=new Set(db.query('PRAGMA table_info(cloud_nonce)').all().map(x=>x.name));
  for(const [name,ddl] of [['state',"TEXT NOT NULL DEFAULT 'pending'"],['lease_until_ms','INTEGER NOT NULL DEFAULT 0'],['payload_digest',"TEXT NOT NULL DEFAULT ''"],['claim_token',"TEXT NOT NULL DEFAULT ''"],['receipt',"TEXT NOT NULL DEFAULT ''"]]) {
    if(!cols.has(name)) db.run(`ALTER TABLE cloud_nonce ADD COLUMN ${name} ${ddl}`);
  }
  const row=db.query('SELECT * FROM cloud_nonce WHERE nonce=?');
  const take=db.transaction((nonce,digest,expires)=>{
    const now=clock();
    if(typeof nonce!=='string'||nonce.length>512||!nonce||!/^[a-f0-9]{64}$/.test(digest)||!Number.isSafeInteger(expires)||expires<=now) throw Error('invalid replay claim');
    let r=row.get(nonce);
    if(r?.payload_digest&&r.payload_digest!==digest) throw Error('replay payload conflict');
    // Legacy receipts lacked payload identity and cannot attest admission. Revalidate them with the native owner.
    if(r&&!r.payload_digest) {db.query("UPDATE cloud_nonce SET state='pending',lease_until_ms=0,payload_digest=?,receipt='' WHERE nonce=?").run(digest,nonce);r=row.get(nonce);}
    if(r?.state==='accepted') return {state:'accepted',receipt:JSON.parse(r.receipt)};
    if(r&&r.lease_until_ms>now) return {state:'busy',retryAfterMs:r.lease_until_ms-now};
    const token=randomUUID();
    db.query("INSERT INTO cloud_nonce(nonce,expires_ms,state,lease_until_ms,payload_digest,claim_token) VALUES(?,?,'pending',?,?,?) ON CONFLICT(nonce) DO UPDATE SET expires_ms=max(expires_ms,excluded.expires_ms),lease_until_ms=excluded.lease_until_ms,claim_token=excluded.claim_token").run(nonce,expires,now+leaseMs,digest,token);
    return {state:'claimed',token};
  });
  const finish=db.query("UPDATE cloud_nonce SET state='accepted',receipt=?,lease_until_ms=0,claim_token='' WHERE nonce=? AND state='pending' AND claim_token=? AND lease_until_ms>?");
  const release=db.query("UPDATE cloud_nonce SET lease_until_ms=0,claim_token='' WHERE nonce=? AND state='pending' AND claim_token=?");
  return {
    begin:(nonce,digest,expires)=>take.immediate(nonce,digest,expires),
    accept(nonce,token,receipt) {
      const body=JSON.stringify(receipt);
      if(!receipt||typeof body!=='string'||Buffer.byteLength(body)>16384) throw Error('invalid admission receipt');
      if(finish.run(body,nonce,token,clock()).changes!==1) throw Error('stale replay lease');
    },
    release:(nonce,token)=>release.run(nonce,token).changes===1,
    reap:(now=clock())=>db.query('DELETE FROM cloud_nonce WHERE expires_ms<? AND lease_until_ms<?').run(now,now).changes,
    close:()=>db.close()
  };
}
