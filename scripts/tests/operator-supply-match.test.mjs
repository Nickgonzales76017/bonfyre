import {test,expect} from 'bun:test';
import {matchOperatorSupply} from '../operator-supply-match.mjs';

const need=(wanted,extra={})=>({need_id:'need:1',project_id:'project:1',role:wanted.workflow,
  wanted:JSON.stringify(wanted),filled:'unfilled',status:'draft',...extra});
const resource=(id,functions,extra={})=>({ext_id:id,name:id,url:'https://operators.example.test/'+id,
  grouping:'Dallas|TX|US',related:'fixture',functions:JSON.stringify(functions),status:'draft',...extra});

test('workflow overlap alone is insufficient when account standing is required',()=>{
  const n=need({workflow:'marketplace_listing',locality:'Garland/DFW',required_evidence:{
    platforms_contains:'facebook_marketplace',account_rating_min:4,multi_listing_capacity:true}});
  const generic=resource('generic-seller',{workflow_families:['marketplace_listing'],standing:{platforms:['ebay']}});
  const facebook=resource('facebook-seller',{workflow_families:['marketplace_listing'],
    standing:{platforms:['facebook_marketplace','ebay'],account_rating:4.7,multi_listing_capacity:true}});
  const result=matchOperatorSupply([n],[generic,facebook]);
  expect(result.summaries[0].candidates).toBe(1);
  expect(result.summaries[0].top[0].resource_id).toBe('facebook-seller');
  expect(result.receipts).toHaveLength(1);
  expect(result.receipts[0].occurrence_id).toBe('facebook-seller');
});
test('a lower rating or missing multi-listing proof stays outside eligibility',()=>{
  const n=need({workflow:'marketplace_listing',required_evidence:{platforms_contains:'facebook_marketplace',account_rating_min:4,multi_listing_capacity:true}});
  const rows=[
    resource('low-rating',{workflow_families:['marketplace_listing'],standing:{platforms:['facebook_marketplace'],account_rating:3.9,multi_listing_capacity:true}}),
    resource('no-capacity',{workflow_families:['marketplace_listing'],standing:{platforms:['facebook_marketplace'],account_rating:4.9}})
  ];
  expect(matchOperatorSupply([n],rows).receipts).toHaveLength(0);
});
test('remote account work is national and machine-verifiable, not local labor',()=>{
  const n=need({workflow:'marketplace_listing',inventory_locality:'Garland/DFW',operator_locality:'remote',
    search_scope:'national',fulfillment_mode:'remote_account',verification_mode:'machine_public_profile',
    human_presence_required:false,required_evidence:{platforms_contains:'facebook_marketplace',account_rating_min:4,multi_listing_capacity:true}});
  const miami=resource('miami-verified',{workflow_families:['marketplace_listing'],standing:{
    platforms:['facebook_marketplace'],account_rating:4.8,multi_listing_capacity:true}},{grouping:'Miami|FL|US'});
  const result=matchOperatorSupply([n],[miami]);
  expect(result.summaries[0].candidates).toBe(1);
  expect(result.summaries[0].search_scope).toBe('national');
  expect(result.summaries[0].human_presence_required).toBe(false);
  expect(result.receipts[0].matched_needs[0].verification_mode).toBe('machine_public_profile');
});
test('market-rate observations can inform price but rank below actual operators',()=>{
  const n=need({workflow:'b2b_calling',secondary:['appointment_setting'],locality:'remote/DFW'});
  const person=resource('caller',{workflow_families:['b2b_calling','appointment_setting'],source_kind:'operator_profile',
    availability:'remote',rate:{amount:'25',currency:'USD',unit:'hour'}});
  const market=resource('rate-market',{workflow_families:['b2b_calling','appointment_setting'],source_kind:'job_posting_rate',
    availability:'market observation',rate:{amount:'17',currency:'USD',unit:'hour'}});
  const result=matchOperatorSupply([n],[market,person]);
  expect(result.summaries[0].top[0].resource_id).toBe('caller');
  expect(result.summaries[0].top[1].resource_id).toBe('rate-market');
});
test('one operator can satisfy several needs without duplicate activation identity',()=>{
  const needs=[need({workflow:'video_capture',secondary:['podcast_capture'],locality:'Dallas'},{need_id:'video'}),
    need({workflow:'podcast_capture',secondary:['video_capture'],locality:'Dallas'},{need_id:'podcast'})];
  const both=resource('hybrid-media',{workflow_families:['video_capture','podcast_capture'],source_kind:'operator_profile'});
  const result=matchOperatorSupply(needs,[both]);
  expect(result.receipts).toHaveLength(1);
  expect(result.receipts[0].matched_needs.map(x=>x.need_id)).toEqual(['podcast','video']);
});
