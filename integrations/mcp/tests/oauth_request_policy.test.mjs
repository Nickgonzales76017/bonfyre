import {test,expect} from 'bun:test';
import {authorizationRequestValid} from '../oauth_request_policy.mjs';
const valid=new URLSearchParams({client_id:'client-one',redirect_uri:'https://client.example.test/oauth/callback?tenant=one',code_challenge:'a'.repeat(43),code_challenge_method:'S256',scope:'read write'});
test('a valid HTTPS callback and PKCE challenge stay in the transport lane',()=>{
 expect(authorizationRequestValid(valid)).toBe(true);
});
test('invalid redirects are refused before authentication and code storage',()=>{
 for(const redirect_uri of ['http://client.example.test/callback','https://u:p@client.example.test/callback','https://client.example.test/callback#fragment','https://','https://client.example.test/callback\nbad','javascript:alert(1)','//client.example.test/callback']){
  const p=new URLSearchParams(valid);p.set('redirect_uri',redirect_uri);expect(authorizationRequestValid(p)).toBe(false);
 }
});
test('client identity and exact S256 challenge are required',()=>{
 for(const [key,value] of [['client_id',''],['code_challenge','short'],['code_challenge','a'.repeat(44)],['code_challenge_method','plain'],['scope','admin'],['scope','read admin']]){
  const p=new URLSearchParams(valid);p.set(key,value);expect(authorizationRequestValid(p)).toBe(false);
 }
});
