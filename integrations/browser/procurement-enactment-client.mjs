import {encode,decode} from './workspace-msgpack.mjs';

const text=(value,max)=>typeof value==='string'&&value.length>0&&value.length<=max&&!/[\x00-\x1f\x7f]/.test(value);
const hex=(value,n)=>typeof value==='string'&&value.length===n&&new RegExp('^[a-f0-9]{'+n+'}$').test(value);
const utc=value=>typeof value==='string'&&/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(value);

export function enactableSupplierContracts(preflight){
 const rows=Array.isArray(preflight?.contracts)?preflight.contracts:[];
 return rows.filter(contract=>{
  const supplier=contract?.supplier_observation?.id,seller=contract?.proposed_actors?.seller;
  return contract?.supplier_actor_binding_verified===true&&typeof supplier==='string'&&supplier===seller&&/^ed25519:[a-f0-9]{64}$/.test(seller||'')&&
   text(contract?.quote_id,159)&&text(contract?.exchange_id,95)&&text(contract?.proposed_actors?.buyer,256)&&text(contract?.proposed_actors?.settlement,256)&&
   text(contract?.proposed_actors?.deliverer,256)&&text(contract?.capability_ref,512);
 });
}

export function encodeSupplierAcceptance({revision,orderDigest,contract,signature,acceptedAt}){
 if(!Number.isSafeInteger(revision)||revision<1||!hex(orderDigest,64))throw Error('Supplier acceptance must bind one exact saved order-intent revision and digest.');
 if(!enactableSupplierContracts({contracts:[contract]}).length)throw Error('This supplier lot is not cryptographically bound to its proposed seller actor.');
 const sig=String(signature??'').trim().toLowerCase();
 if(!hex(sig,128))throw Error('Import the supplier detached Ed25519 signature as exactly 128 lowercase hex characters.');
 if(!utc(acceptedAt))throw Error('Use the supplier acceptance timestamp in exact UTC seconds.');
 return encode([1,revision,orderDigest,contract.quote_id,contract.exchange_id,contract.proposed_actors.buyer,contract.proposed_actors.seller,contract.proposed_actors.settlement,contract.proposed_actors.deliverer,contract.capability_ref,sig,acceptedAt]);
}

export function decodeSupplierEnactment(bytes){
 if(!(bytes instanceof Uint8Array))bytes=new Uint8Array(bytes);
 if(bytes.byteLength>16384)throw Error('Procurement enactment reply exceeded its response bound.');
 const value=decode(bytes),receipt=Array.isArray(value)&&value.length===2&&value[0]===1?value[1]:null;
 if(!receipt||typeof receipt!=='object'||!['enacted','replayed'].includes(receipt.state)||!text(receipt.record_key,511)||
    !Number.isSafeInteger(receipt.revision)||receipt.revision<1||!hex(receipt.order_digest,64)||!text(receipt.quote_id,159)||!text(receipt.exchange,95)||
    !/^ed25519:[a-f0-9]{64}$/.test(receipt.supplier_id||'')||receipt.supplier_identity_binding_verified!==true||
    receipt.supplier_acceptance_verified!==true||receipt.protocol_enacted!==true||receipt.commitment_opened!==true||
    typeof receipt.replayed!=='boolean'||!utc(receipt.accepted_at))throw Error('Procurement enactment returned an invalid receipt.');
 return receipt;
}
