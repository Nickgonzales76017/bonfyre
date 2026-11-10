import test from 'node:test';
import assert from 'node:assert/strict';
import {sign,verify} from './workgraph-envelope.mjs';
const secret='an-example-key-for-tests-with-32-bytes-plus';
const body={version:1,mission:'source:alpha',family:'source-sync',digest:'a'.repeat(64),issued:1700000000000,nonce:'nonce_1234567890123'};
test('valid cloud intent roundtrips',async()=>{const sig=await sign(body,secret);assert.equal((await verify(body,sig,secret,body.issued)).mission,body.mission)});
test('tamper, stale and extra effects are rejected',async()=>{const sig=await sign(body,secret);await assert.rejects(()=>verify({...body,mission:'source:beta'},sig,secret,body.issued));await assert.rejects(()=>verify(body,sig,secret,body.issued+360000));await assert.rejects(()=>sign({...body,command:'execute'},secret));});
