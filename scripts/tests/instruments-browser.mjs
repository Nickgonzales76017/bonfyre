// Read-only publication readiness precedes the user-interaction suites.
// Failed application actions are never replayed to manufacture a passing result.
import {readFile} from 'node:fs/promises';
import path from 'node:path';
import {verifyPublishedInstruments} from '../instruments-readiness.mjs';
if(process.env.INSTRUMENT_ORIGIN)await verifyPublishedInstruments({base:process.env.INSTRUMENT_ORIGIN,evidence:process.env.INSTRUMENT_EVIDENCE??'instrument-live-evidence'});
try{
  await import('./instruments-browser-journey.mjs');
  await import('./instruments-composition-browser.mjs');
}catch(error){
  const dir=process.env.INSTRUMENT_EVIDENCE??'instrument-evidence';
  for(const name of ['failure.json','composition-failure.json'])try{console.error(name+': '+(await readFile(path.join(dir,name),'utf8')).slice(0,12000));}catch{}
  throw error;
}
