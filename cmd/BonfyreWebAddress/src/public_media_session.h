#ifndef BONFYRE_PUBLIC_MEDIA_SESSION_H
#define BONFYRE_PUBLIC_MEDIA_SESSION_H
/* Included in the existing player closure; no independent player or store. */
#define BF_PUBLIC_MEDIA_SESSION \
"// Optional device controls for the same public audio element. Browser support\n" \
"// changes the available carrier, never the selected source or account scope.\n" \
"(function bindDeviceControls(){\n" \
"  const session=navigator.mediaSession;\n" \
"  if(!session||typeof MediaMetadata!=='function')return;\n" \
"  let owned=null,active=false,mode='';\n" \
"  const actions=['play','pause','stop','previoustrack','nexttrack','seekbackward','seekforward','seekto'];\n" \
"  const owns=()=>active&&session.metadata===owned&&!disposed;\n" \
"  function action(name,fn){\n" \
"    try{session.setActionHandler(name,fn?details=>{if(owns())fn(details||{});}:null);}catch{}\n" \
"  }\n" \
"  function release(){\n" \
"    if(!active)return;active=false;mode='';\n" \
"    if(session.metadata!==owned){owned=null;return;}\n" \
"    for(const name of actions)try{session.setActionHandler(name,null);}catch{}\n" \
"    try{session.setPositionState();session.playbackState='none';session.metadata=null;}catch{}\n" \
"    owned=null;\n" \
"  }\n" \
"  function update(){\n" \
"    if(!owns())return;\n" \
"    try{\n" \
"      session.playbackState=audio.paused||audio.ended?'paused':'playing';\n" \
"      if(Number.isFinite(audio.duration)&&audio.duration>0&&Number.isFinite(audio.currentTime)&&audio.playbackRate>0)\n" \
"        session.setPositionState({duration:audio.duration,position:Math.max(0,Math.min(audio.duration,audio.currentTime)),playbackRate:audio.playbackRate});\n" \
"      else session.setPositionState();\n" \
"    }catch{}\n" \
"    const key=[selected,rows.length,!!bounds()].join(':');if(key===mode)return;mode=key;\n" \
"    action('play',()=>{if(audio.paused)void start();});\n" \
"    action('pause',()=>{epoch++;pending=false;audio.pause();say('Paused');controls();});\n" \
"    action('stop',()=>{epoch++;pending=false;audio.pause();seek(bounds()?.[0]||0);release();say('Stopped');controls();});\n" \
"    action('previoustrack',selected>0?()=>select(selected-1):null);\n" \
"    action('nexttrack',selected+1<rows.length?()=>select(selected+1):null);\n" \
"    const offset=details=>Number.isFinite(details.seekOffset)?Math.min(300,Math.max(0,details.seekOffset)):10;\n" \
"    action('seekbackward',bounds()?details=>seek(audio.currentTime-offset(details)):null);\n" \
"    action('seekforward',bounds()?details=>seek(audio.currentTime+offset(details)):null);\n" \
"    action('seekto',bounds()?details=>seek(details.seekTime):null);\n" \
"  }\n" \
"  function claim(){\n" \
"    if(disposed||selected<0)return;\n" \
"    // Starting another media surface first releases this player's old bindings.\n" \
"    const artwork=[];\n" \
"    try{const url=new URL(rows[selected].dataset.art||'');if(url.protocol==='https:'&&!url.username&&!url.password)artwork.push({src:url.href});}catch{}\n" \
"    try{owned=new MediaMetadata({title:title.textContent,artist:by.textContent,album:'Aurekai',artwork});session.metadata=owned;active=true;mode='';update();}catch{active=false;owned=null;}\n" \
"  }\n" \
"  on(audio,'play',claim);\n" \
"  for(const event of ['pause','ended','loadedmetadata','durationchange','timeupdate','ratechange','progress'])on(audio,event,update);\n" \
"  on(document,'bonfyre:public-audio-play',event=>{if(event.detail!==deck)release();});\n" \
"  on(window,'pagehide',release);\n" \
"  on(document,'htmx:beforeCleanupElement',event=>{const node=event.detail?.elt;if(node&&(node===deck||node.contains(deck)))release();});\n" \
"})();\n"
#endif
