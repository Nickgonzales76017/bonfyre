import {randomUUID} from 'node:crypto';
import {pushEndpointHash,validatePushSubscription} from './fabric_push_carrier.mjs';

const uid=()=>randomUUID().replaceAll('-','').slice(0,24);
const same=(a,b)=>a.actor===b.actor&&a.subject===b.subject&&a.scope===b.scope;
const id=value=>typeof value==='string'&&/^[a-f0-9]{24}$/.test(value);
const publicSubscription=row=>({id:row.id,endpointHash:row.endpoint_hash,state:row.state,expiresAt:Math.round(row.expires_at*1000)});
const contextOf=binding=>binding.scope==='self'?'self':binding.subject;
const TTL=86400,CONSENT=30*86400;

// All helpers below use the already-open Fabric connection and transaction.
// Nothing in this module can open a second database or assign a task owner.
export function createFabricPushStore({get,all,write,atomic,clock,binding,fail}) {
  const coordinates=value=>{const b=binding(value);return [b.actor,b.subject,b.scope];};
  function expect(authority,payload,keys) {
    const b=binding(authority);
    fail(payload&&typeof payload==='object'&&!Array.isArray(payload)&&Object.keys(payload).length===keys.length+1&&
      Object.keys(payload).every(key=>key==='expectedBinding'||keys.includes(key)),'invalid','Use the reviewed push action.');
    fail(same(binding(payload.expectedBinding),b),'conflict','The signed-in account changed. Review this destination again.');
    return b;
  }
  function expire() {
    const at=clock();
    write("UPDATE push_subscriptions SET state='expired',reason='consent_expired',updated_at=? WHERE state='active' AND expires_at<=?",at,at);
    write("UPDATE push_deliveries SET state='expired',reason='delivery_expired',updated_at=? WHERE state IN ('queued','claimed','retry') AND expires_at<=?",at,at);
    write("UPDATE push_deliveries SET state='retry',reason='claim_interrupted',lease_until=0,lease_token='',next_attempt=?,updated_at=? WHERE state='claimed' AND lease_until<=?",at,at,at);
    write("UPDATE push_deliveries SET state='uncertain',reason='send_interrupted',lease_until=0,lease_token='',updated_at=? WHERE state='sending' AND lease_until<=?",at,at);
    write("UPDATE push_deliveries SET state='revoked',reason='subscription_inactive',updated_at=? WHERE state IN ('queued','claimed','retry') AND subscription_id IN (SELECT id FROM push_subscriptions WHERE state!='active')",at);
    write("UPDATE push_deliveries SET state='suppressed',reason='inbox_reviewed',updated_at=? WHERE state IN ('queued','claimed','retry') AND notification_id IN (SELECT id FROM task_notifications WHERE state!='unread')",at);
    write('DELETE FROM push_deliveries WHERE expires_at<?',at-CONSENT);
    write("DELETE FROM push_subscriptions WHERE state!='active' AND updated_at<? AND NOT EXISTS(SELECT 1 FROM push_deliveries d WHERE d.subscription_id=push_subscriptions.id)",at-CONSENT);
  }
  function status(authority) {
    const args=coordinates(authority);
    return atomic(()=>{
      expire();
      const subscriptions=all('SELECT id,endpoint_hash,state,expires_at FROM push_subscriptions WHERE recipient=? AND subject=? AND scope=? ORDER BY updated_at DESC LIMIT 128',...args).map(publicSubscription);
      const rows=all(`SELECT d.state,COUNT(*) AS n FROM push_deliveries d JOIN push_subscriptions p ON p.id=d.subscription_id
        WHERE p.recipient=? AND p.subject=? AND p.scope=? GROUP BY d.state`,...args);
      const counts={queued:0,accepted:0,shown:0,opened:0,uncertain:0};
      for(const row of rows){if(['queued','claimed','sending','retry'].includes(row.state))counts.queued+=row.n;else if(Object.hasOwn(counts,row.state))counts[row.state]+=row.n;}
      const seen=get(`SELECT COUNT(d.shown_at) AS shown,COUNT(d.opened_at) AS opened FROM push_deliveries d
        JOIN push_subscriptions p ON p.id=d.subscription_id WHERE p.recipient=? AND p.subject=? AND p.scope=?`,...args);
      counts.shown=seen.shown;counts.opened=seen.opened;
      return {subscriptions,counts};
    });
  }
  function subscribe(authority,payload,keyId) {
    const b=expect(authority,payload,['endpoint','expirationTime','keys']);
    const sub=validatePushSubscription({endpoint:payload.endpoint,expirationTime:payload.expirationTime,keys:payload.keys});
    fail(typeof keyId==='string'&&/^[a-f0-9]{64}$/.test(keyId),'unavailable','The push signing identity is unavailable.');
    return atomic(()=>{
      expire();const at=clock(),hash=pushEndpointHash(sub.endpoint),expires=Math.min(at+CONSENT,sub.expirationTime===null?Infinity:sub.expirationTime/1000);
      fail(expires>at+60,'invalid','This browser subscription has expired. Enable notifications again.');
      const other=get("SELECT id FROM push_subscriptions WHERE endpoint_hash=? AND recipient!=? AND state='active' LIMIT 1",hash,b.actor);
      fail(!other,'device_account_conflict','This browser subscription is bound to another account. Renew the browser subscription before enabling it here.');
      const old=get('SELECT * FROM push_subscriptions WHERE recipient=? AND subject=? AND scope=? AND endpoint_hash=?',...coordinates(b),hash);
      if(old){
        fail(old.p256dh===sub.keys.p256dh&&old.auth===sub.keys.auth&&old.key_id===keyId,'device_key_conflict','Renew this browser subscription before enabling notifications.');
        const after=old.state==='active'?old.after_seq:get('SELECT COALESCE(MAX(seq),0) AS n FROM task_notifications').n;
        write("UPDATE push_subscriptions SET state='active',reason='',after_seq=?,updated_at=?,expires_at=? WHERE id=?",after,at,expires,old.id);
        return {binding:b,subscription:publicSubscription({...old,state:'active',expires_at:expires})};
      }
      fail(get('SELECT COUNT(*) AS n FROM push_subscriptions WHERE recipient=?',b.actor).n<128,'capacity','Review retained notification devices before adding another.');
      fail(get("SELECT COUNT(*) AS n FROM push_subscriptions WHERE recipient=? AND state='active'",b.actor).n<32,'capacity','This account has reached its active notification device limit.');
      const row={id:uid(),endpoint_hash:hash,state:'active',expires_at:expires};
      write(`INSERT INTO push_subscriptions(id,recipient,subject,scope,endpoint_hash,endpoint,p256dh,auth,key_id,state,after_seq,created_at,updated_at,expires_at)
        VALUES(?,?,?,?,?,?,?,?,?,'active',?,?,?,?)`,row.id,...coordinates(b),hash,sub.endpoint,sub.keys.p256dh,sub.keys.auth,keyId,
        get('SELECT COALESCE(MAX(seq),0) AS n FROM task_notifications').n,at,at,expires);
      return {binding:b,subscription:publicSubscription(row)};
    });
  }
  function unsubscribe(authority,payload) {
    const b=expect(authority,payload,['subscriptionId']);fail(id(payload.subscriptionId),'invalid','Use the retained device identity.');
    return atomic(()=>{
      const row=get('SELECT * FROM push_subscriptions WHERE id=? AND recipient=? AND subject=? AND scope=?',payload.subscriptionId,...coordinates(b));
      fail(row,'not_found','This notification device is unavailable to the current account.');
      write("UPDATE push_subscriptions SET state='revoked',reason='recipient_disabled',updated_at=? WHERE id=?",clock(),row.id);expire();
      return {binding:b,subscription:publicSubscription({...row,state:'revoked'})};
    });
  }
  const joined=`SELECT d.*,p.recipient AS actor,p.subject,p.scope,p.endpoint,p.p256dh,p.auth,p.state AS subscription_state,
    p.expires_at AS subscription_expires,p.key_id,n.task_id,n.state AS notification_state
    FROM push_deliveries d JOIN push_subscriptions p ON p.id=d.subscription_id
    JOIN task_notifications n ON n.id=d.notification_id JOIN task_scopes s ON s.task_id=n.task_id
    AND s.subject=p.subject AND s.scope=p.scope AND n.recipient=p.recipient`;
  function recipientDelivery(b,deliveryId) {
    fail(id(deliveryId),'invalid','Use the retained delivery identity.');
    const row=get(joined+' WHERE d.id=? AND p.recipient=? AND p.subject=? AND p.scope=?',deliveryId,...coordinates(b));
    fail(row,'not_found','This notification is unavailable to the current account.');
    fail(row.expires_at>clock()&&row.subscription_state==='active'&&row.subscription_expires>clock(),'expired','This notification delivery expired or was disabled. Open the current Inbox.');
    fail(['sending','accepted','uncertain'].includes(row.state),'not_found','This delivery has not been sent.');
    return row;
  }
  function acknowledge(authority,payload) {
    const b=expect(authority,payload,['deliveryId','stage']);
    fail(['shown','opened'].includes(payload.stage),'invalid','Use an observed browser delivery stage.');
    return atomic(()=>{
      const row=recipientDelivery(b,payload.deliveryId),field=payload.stage==='shown'?'shown_at':'opened_at';
      write(`UPDATE push_deliveries SET ${field}=COALESCE(${field},?),updated_at=? WHERE id=?`,clock(),clock(),row.id);
      return {binding:b,deliveryId:row.id,stage:payload.stage};
    });
  }
  function resolve(authority,payload) {
    const b=expect(authority,payload,['deliveryId']),row=recipientDelivery(b,payload.deliveryId);
    return {binding:b,deliveryId:row.id,taskId:row.task_id,notificationState:row.notification_state,
      href:'/apps?'+new URLSearchParams({context:contextOf(b)})+'#fabric-task-'+row.task_id};
  }
  function plan(keyId) {
    return atomic(()=>{
      expire();const at=clock();
      if(get('SELECT COUNT(*) AS n FROM push_deliveries').n>=50000)return 0;
      const rows=all(`SELECT n.id,n.revision,n.created_at,p.id AS subscription_id,p.expires_at FROM task_notifications n
        JOIN task_scopes s ON s.task_id=n.task_id JOIN push_subscriptions p ON p.recipient=n.recipient AND p.subject=s.subject AND p.scope=s.scope
        WHERE n.state='unread' AND p.state='active' AND p.key_id=? AND p.expires_at>? AND n.seq>p.after_seq AND n.created_at>?
        AND NOT EXISTS(SELECT 1 FROM push_deliveries d WHERE d.notification_id=n.id AND d.subscription_id=p.id) ORDER BY n.seq LIMIT 64`,keyId,at,at-TTL);
      for(const row of rows)write(`INSERT INTO push_deliveries(id,notification_id,subscription_id,notification_revision,state,created_at,updated_at,expires_at,next_attempt)
        VALUES(?,?,?,?,'queued',?,?,?,?)`,uid(),row.id,row.subscription_id,row.revision,at,at,Math.min(row.created_at+TTL,row.expires_at),at);
      return rows.length;
    });
  }
  function claim() {
    return atomic(()=>{
      expire();const at=clock(),row=get(joined+" WHERE d.state IN ('queued','retry') AND d.next_attempt<=? AND d.expires_at>? AND p.state='active' AND p.expires_at>? AND n.state='unread' ORDER BY d.created_at,d.id LIMIT 1",at,at,at);
      if(!row)return null;const token=uid();
      write("UPDATE push_deliveries SET state='claimed',lease_token=?,lease_until=?,updated_at=? WHERE id=?",token,at+30,at,row.id);
      return {...row,state:'claimed',lease_token:token};
    });
  }
  function beginSend(claim,keyId) {
    return atomic(()=>{
      expire();const at=clock(),row=get(joined+" WHERE d.id=? AND d.lease_token=? AND d.state='claimed'",claim.id,claim.lease_token);
      if(row&&row.key_id!==keyId){
        write("UPDATE push_subscriptions SET state='revoked',reason='signing_identity_changed',updated_at=? WHERE id=?",at,row.subscription_id);
        expire();return null;
      }
      if(!row||row.subscription_state!=='active'||row.subscription_expires<=at||row.expires_at<=at||row.notification_state!=='unread')return null;
      write("UPDATE push_deliveries SET state='sending',attempts=attempts+1,lease_until=?,updated_at=? WHERE id=?",at+30,at,row.id);
      return {...row,state:'sending',attempts:row.attempts+1};
    });
  }
  function finish(claim,result) {
    return atomic(()=>{
      const row=get('SELECT * FROM push_deliveries WHERE id=? AND lease_token=? AND state IN (\'claimed\',\'sending\')',claim.id,claim.lease_token);
      if(!row)return false;
      const at=clock();let state=result.state;
      fail(['accepted','retry','uncertain','revoked','denied','failed'].includes(state),'invalid','Use the carrier observation.');
      if(state==='retry'&&row.attempts>=5)state='failed';
      const next=state==='retry'?at+Math.max(30,Math.min(TTL,result.retryAfter||0),Math.min(3600,30*2**row.attempts)):at;
      if(state==='retry'&&next>=row.expires_at)state='expired';
      write('UPDATE push_deliveries SET state=?,last_status=?,reason=?,accepted_at=CASE WHEN ?=\'accepted\' THEN ? ELSE accepted_at END,next_attempt=?,lease_until=0,lease_token=\'\',updated_at=? WHERE id=?',
        state,result.status||0,result.reason||'',state,at,next,at,row.id);
      if(state==='revoked')write("UPDATE push_subscriptions SET state='revoked',reason='endpoint_expired',updated_at=? WHERE id=?",at,row.subscription_id);
      if(state==='denied'){
        const b={actor:claim.actor,subject:claim.subject,scope:claim.scope};
        write("UPDATE push_subscriptions SET state='revoked',reason='access_removed',updated_at=? WHERE recipient=? AND subject=? AND scope=?",at,...coordinates(b));
      }
      expire();return true;
    });
  }
  return Object.freeze({status,subscribe,unsubscribe,acknowledge,resolve,plan,claim,beginSend,finish});
}
