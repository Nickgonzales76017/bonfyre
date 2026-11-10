import{readFileSync}from'node:fs';
import{join}from'node:path';
import{catalogueFiles}from'../../../components/BonfyreServiceSite/catalogue-material.mjs';
import{plannerFiles}from'../../../components/BonfyreServiceSite/planner-material.mjs';
export function fixtures(){return plannerFiles(JSON.parse(readFileSync(join(import.meta.dir,'fixture-planner.json'),'utf8')));}
export function products(){
 const x=JSON.parse(Buffer.from(Bun.gunzipSync(readFileSync(join(import.meta.dir,'catalogue-input.json.gz')))).toString());
 const sample=JSON.parse(readFileSync(join(import.meta.dir,'../../../.bonfyre/revenue/remote-trials-20261007/red3-easel-copy-review.json'),'utf8'));
 const copy=sample.proposed_copy;
 const samples={['red3-display:'+sample.source.legacy_id]:{source_url:sample.source.url,prepared_at:sample.source.observed_at,fields:{
  description:copy.description,
  'prep:seo_title':copy.seo_title,'prep:seo_description':copy.seo_description,
  'prep:image_filename':copy.image_filename,'prep:image_alt':copy.image_alt,
  'prep:collections':copy.collection_proposals.join('\n'),
  'prep:questions':[...sample.questions,'The public photo is 350 × 350 pixels. Please supply the original image.'].join('\n')
 }}};
 return catalogueFiles(x.rows,x.stores,{samples,fixtureCatalogues:[fixtures().catalogue]});
}

import {creatorFiles} from '../../../components/BonfyreServiceSite/creator-material.mjs';
export function audio(){return creatorFiles(join(import.meta.dir, '../audio'));}

import {shipmentCheckFiles} from '../../../components/BonfyreServiceSite/shipment-material.mjs';
export function shipments(){return shipmentCheckFiles();}

import {simulationFiles} from '../../../components/BonfyreServiceSite/process-sim-material.mjs';
export function simulation(){return simulationFiles(join(import.meta.dir,'nyc311-phone-20261006.json'));}

import {synthesisFiles} from '../../../components/BonfyreServiceSite/synthesis-material.mjs';
export function synthesis(){return synthesisFiles(join(import.meta.dir,'synthesis-four-day-week-20261019.json'));}

import {schemaFiles} from '../../../components/BonfyreServiceSite/schema-lab-material.mjs';
export function schema(){return schemaFiles(join(import.meta.dir,'synthesis-four-day-week-20261019.json'),join(import.meta.dir,'nyc311-phone-20261006.json'));}

import {planFiles} from '../../../components/BonfyreServiceSite/plan-network-material.mjs';
export function plan(){return planFiles(join(import.meta.dir,'../../../docs/engineering/handoff-backfill-20261019/demo-instruments.tsv'));}

import {pricingFiles} from '../../../components/BonfyreServiceSite/pricing-desk-material.mjs';
export function pricing(){return pricingFiles(join(import.meta.dir,'unpriced-capabilities-20261019.json'));}

import {opportunityFiles} from '../../../components/BonfyreServiceSite/opportunity-desk-material.mjs';
export function opportunity(){return opportunityFiles(join(import.meta.dir,'..'));}
