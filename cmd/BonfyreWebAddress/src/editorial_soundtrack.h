#ifndef BONFYRE_EDITORIAL_SOUNDTRACK_H
#define BONFYRE_EDITORIAL_SOUNDTRACK_H
/* Shared original instrumental score for authored article recordings and device reading.
 * No remote music, licensing assertion, external audio or automatic playback. */
#define BF_EDITORIAL_SCORE_JS \
"function bfEditorialSoundtrack(root,heading){\n" \
" const kinds=[\n" \
"  {roots:[48,53,55,50],chord:[0,4,7,11],beat:.66,voice:'triangle'},\n" \
"  {roots:[45,50,43,48],chord:[0,3,7,10],beat:.62,voice:'sine'},\n" \
"  {roots:[50,55,53,48],chord:[0,4,9,14],beat:.74,voice:'triangle'},\n" \
"  {roots:[46,53,51,44],chord:[0,5,7,12],beat:.88,voice:'sine'},\n" \
"  {roots:[52,47,50,45],chord:[0,3,9,12],beat:.79,voice:'triangle'}];\n" \
" let seed=0;function hashTitle(){seed=2166136261;const title=typeof heading==='function'?heading():heading;for(const c of String(title||''))seed=Math.imul(seed^c.charCodeAt(0),16777619)>>>0;}\n" \
" const musicChoice=root.querySelector('[data-listen-music], [data-deck-music]');\n" \
" let context=null,master=null,timer=null,step=0,nextAt=0,active=false,preset=null,speaking=false,volumeScale=1;\n" \
" const chosen=()=>{const value=musicChoice?.value||'auto';return value==='off'?null:kinds[value==='auto'?seed%kinds.length:Math.max(0,Math.min(kinds.length-1,Number(value)||0))];};\n" \
" function level(value){if(!context||!master)return;master.gain.setTargetAtTime(value*volumeScale,context.currentTime,.35);}\n" \
" function note(midi,at,seconds,volume){\n" \
"  const oscillator=context.createOscillator(),envelope=context.createGain();\n" \
"  oscillator.type=preset.voice;oscillator.frequency.setValueAtTime(440*Math.pow(2,(midi-69)/12),at);\n" \
"  envelope.gain.setValueAtTime(.0001,at);envelope.gain.exponentialRampToValueAtTime(volume,at+.04);\n" \
"  envelope.gain.exponentialRampToValueAtTime(.0001,at+seconds);\n" \
"  oscillator.connect(envelope);envelope.connect(master);oscillator.start(at);oscillator.stop(at+seconds+.02);\n" \
" }\n" \
" function pump(){\n" \
"  if(!active||!context||!preset||context.state!=='running')return;\n" \
"  let count=0;while(nextAt<context.currentTime+.35&&count++<4){\n" \
"   const bar=Math.floor(step/8),rootNote=preset.roots[(bar+Math.floor(seed/17)%4)%4];\n" \
"   if(step%8===0)for(const interval of preset.chord)note(rootNote+interval,nextAt,Math.min(5,preset.beat*7),.068);\n" \
"   if(step%2===0)note(rootNote+12+preset.chord[(step/2+seed)%4],nextAt,preset.beat*1.7,.087);\n" \
"   if(step%8===4)note(rootNote-12,nextAt,preset.beat*3.4,.07);\n" \
"   step++;nextAt+=preset.beat;\n" \
"  }\n" \
" }\n" \
" function stop(){active=false;if(timer!==null){clearInterval(timer);timer=null;}level(0);}\n" \
" function start(){\n  hashTitle();\n" \
"  preset=chosen();if(!preset){stop();return;}\n" \
"  if(!context){\n" \
"   const Audio=window.AudioContext||window.webkitAudioContext;if(!Audio)return;\n" \
"   try{context=new Audio();master=context.createGain();master.gain.value=0;master.connect(context.destination);}catch{return;}\n" \
"  }\n" \
"  if(!active){step=0;nextAt=context.currentTime+.05;active=true;timer=setInterval(pump,180);}\n" \
"  context.resume().then(()=>{if(active){level(speaking?.075:.12);pump();}}).catch(()=>{stop();});\n" \
" }\n" \
" function change(playing){stop();if(playing){start();duck(true);}}\n" \
" function dispose(){stop();if(context?.close)context.close().catch(()=>{});}\n" \
" function duck(value){speaking=value;level(value?.075:.13);}\n function volume(value){volumeScale=Math.max(0,Math.min(1,Number(value)||0));level(preset?(speaking?.075:.13):0);}\n return {start,duck,pause:()=>level(0),end:stop,change,volume,dispose};\n" \
"}\n"
#endif
