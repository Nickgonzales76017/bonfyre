// Read-only publication readiness precedes the user-interaction suites.
// Failed application actions are never replayed to manufacture a passing result.
import {verifyPublishedInstruments} from '../instruments-readiness.mjs';
if(process.env.INSTRUMENT_ORIGIN)await verifyPublishedInstruments({base:process.env.INSTRUMENT_ORIGIN,evidence:process.env.INSTRUMENT_EVIDENCE??'instrument-live-evidence'});
await import('./instruments-browser-journey.mjs');
await import('./instruments-composition-browser.mjs');
