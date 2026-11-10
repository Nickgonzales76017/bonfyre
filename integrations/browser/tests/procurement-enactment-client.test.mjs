import {test,expect} from 'bun:test';
import {decode,encode} from '../workspace-msgpack.mjs';
import {enactableSupplierContracts,encodeSupplierAcceptance,decodeSupplierEnactment} from '../procurement-enactment-client.mjs';

const seller='ed25519:'+'a'.repeat(64);
const contract={quote_id:'quote-1',exchange_id:'procurement:'+'b'.repeat(48),capability_ref:'capability:fixture',supplier_observation:{id:seller,name:'Fixture Supplier'},supplier_actor_binding_verified:true,proposed_actors:{buyer:'buyer:fixture',seller,settlement:'settlement:fixture',deliverer:'deliverer:fixture'}};
test('supplier acceptance packet derives every commercial coordinate from one cryptographically bound preflight contract',()=>{
 expect(enactableSupplierContracts({contracts:[contract]})).toEqual([contract]);
 const bytes=encodeSupplierAcceptance({revision:4,orderDigest:'c'.repeat(64),contract,signature:'d'.repeat(128),acceptedAt:'2026-10-23T20:00:00Z'});
 expect(decode(bytes)).toEqual([1,4,'c'.repeat(64),'quote-1',contract.exchange_id,'buyer:fixture',seller,'settlement:fixture','deliverer:fixture','capability:fixture','d'.repeat(128),'2026-10-23T20:00:00Z']);
});
test('loose seller proposals, malformed signatures and non-exact timestamps cannot form an enactment packet',()=>{
 expect(enactableSupplierContracts({contracts:[{...contract,supplier_actor_binding_verified:false}]})).toEqual([]);
 expect(enactableSupplierContracts({contracts:[{...contract,proposed_actors:{...contract.proposed_actors,seller:'seller:loose'}}]})).toEqual([]);
 expect(()=>encodeSupplierAcceptance({revision:4,orderDigest:'c'.repeat(64),contract,signature:'not-a-signature',acceptedAt:'2026-10-23T20:00:00Z'})).toThrow();
 expect(()=>encodeSupplierAcceptance({revision:4,orderDigest:'c'.repeat(64),contract,signature:'d'.repeat(128),acceptedAt:'2026-10-23 20:00'})).toThrow();
});
test('verified enactment reply stays bounded and non-collapsible',()=>{
 const receipt={state:'enacted',record_key:'procurement-order-intent:fixture',revision:4,order_digest:'c'.repeat(64),quote_id:'quote-1',exchange:contract.exchange_id,supplier_id:seller,supplier_identity_binding_verified:true,supplier_acceptance_verified:true,protocol_enacted:true,commitment_opened:true,replayed:false,accepted_at:'2026-10-23T20:00:00Z',cannot_infer:'No purchase order or payment.'};
 expect(decodeSupplierEnactment(encode([1,receipt]))).toEqual(receipt);
 expect(()=>decodeSupplierEnactment(encode([1,{...receipt,payment_presented:true,supplier_acceptance_verified:false}]))).toThrow();
});
