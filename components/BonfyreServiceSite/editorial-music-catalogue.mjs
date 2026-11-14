/* Source-pinned recorded music, not synthetic tones. These are candidates:
 * an artist/catalogue CC0 declaration is evidence to review, not a release grant.
 * The account explicitly imports exact, hash-matched bytes before mixing.
 */
const dataset="https://huggingface.co/datasets/SoundSafari/Public-Domain-Music/";
const details=[
 {id:"mellow-cafe-vibe",title:"Mellow Cafe Vibe",artist:"John Bartmann",
  file:"John Bartmann - Mellow Cafe Vibe.mp3",sha256:"94cf902ba591f130298e774ea155b137dffae3ced821f6e33a5cc1079c29402b",
  durationSeconds:100,profile:"conversation",style:"Acoustic jazz",instruments:"Guitar · upright bass · brushes",
  note:"Natural coffeehouse jazz for conversation and interview openings"},
 {id:"a-little-shade",title:"A Little Shade",artist:"HoliznaCC0",
  file:"HoliznaCC0 - A Little Shade.mp3",sha256:"89958d422cac586da882319e9129ce0b737a6aa65839970002df0cbb1cdced8f",
  durationSeconds:179,profile:"newsroom",style:"Warm lo-fi",instruments:"Electric piano · soft drums · bass",
  note:"Warm, restrained piano-driven reporting and explanatory narration"},
 {id:"autumn",title:"Autumn",artist:"HoliznaCC0",
  file:"HoliznaCC0 - Autumn.mp3",sha256:"c12190c1c3587e61580078d01c54d4ab7ab1f742857dc07b9d9d712735631b1a",
  durationSeconds:170,profile:"essay",style:"Reflective lo-fi",instruments:"Piano · analogue texture · drums",
  note:"Measured and reflective storytelling with a softer emotional character"},
 {id:"day-dreams",title:"Day Dreams",artist:"HoliznaCC0",
  file:"HoliznaCC0 - Day Dreams.mp3",sha256:"345324857ee7c8ffc6ae15b8d646036f3630fbc7c4470cc633907715bfaad44f",
  durationSeconds:198,profile:"feature",style:"Gentle guitar",instruments:"Guitar · keys · drums",
  note:"A fuller narrative bed with distinct guitar and keys"},
 {id:"above-the-clouds",title:"Above The Clouds",artist:"John Bartmann",
  file:"John Bartmann - above-the-clouds-master.mp3",sha256:"d13c13de3ff5ed65cf3a2392e4599ff70e96bc94f9e9a0d7e351e1d0ce53754f",
  durationSeconds:244,profile:"documentary",style:"Ambient piano",instruments:"Piano · sustained pads",
  note:"Low-key atmospheric underscore for documentary material"},
 {id:"fire-place",title:"Fire Place",artist:"HoliznaCC0",
  file:"HoliznaCC0 - Fire Place.mp3",sha256:"b26c049d6ac149c678e0f24b68416dc71c6cb2b4da62eb4cf7448432c26fa2a0",
  durationSeconds:170,profile:"essay",style:"Cozy interlude",instruments:"Keys · textured ambience · soft drums",
  note:"Gentle transitional treatment for a reflective chapter"},
 {id:"city-lights",title:"City Lights",artist:"HoliznaCC0",
  file:"HoliznaCC0 - City Lights.mp3",sha256:"5cef4e2511dca0a8363b79fbc513709103a68b0a153877cf4922bcc0214135d2",
  durationSeconds:277,profile:"conversation",style:"Chillhop",instruments:"Keys · steady drums · bass",
  note:"Rhythmic but relaxed music for informal discussions"}
];
const goodId=id=>/^[a-z][a-z0-9-]{1,60}$/.test(id);
export const editorialMusicCatalogue=Object.freeze(details.map(entry=>{
 if(!goodId(entry.id)||!/^[a-f0-9]{64}$/.test(entry.sha256)||
    !["newsroom","feature","essay","conversation","documentary"].includes(entry.profile))
  throw Error("Invalid pinned editorial music identity");
 const segment=encodeURIComponent(entry.file);
 return Object.freeze({...entry,licence:"CC0-1.0",rightsReview:"pending",
   downloadUrl:dataset+"resolve/main/"+segment,
   sourceUrl:dataset+"blob/main/"+segment,
   licenceUrl:"https://creativecommons.org/publicdomain/zero/1.0/"});
}));
export function suggestedEditorialTrack(profile,sourceIdentity){
 if(typeof sourceIdentity!=="string"||sourceIdentity.length>500)throw Error("Use a bounded article identity");
 const pool=editorialMusicCatalogue.filter(t=>t.profile===profile);
 if(!pool.length)return null;
 let code=2166136261;
 for(const c of sourceIdentity)code=Math.imul(code^c.charCodeAt(0),16777619)>>>0;
 return pool[code%pool.length];
}
