/* Aurekai Journal — accessible reading tools, no third-party service or synthetic recording. */
(() => {
  'use strict';
  const outline = document.querySelector('[data-outline-menu]');
  if (outline) {
    const toggle = outline.querySelector('.reader-outline-trigger');
    const close = () => { outline.dataset.open = 'false'; toggle.setAttribute('aria-expanded', 'false'); };
    toggle.addEventListener('click', () => {
      const next = outline.dataset.open !== 'true';
      outline.dataset.open = String(next);
      toggle.setAttribute('aria-expanded', String(next));
    });
    outline.querySelectorAll('a').forEach(link => link.addEventListener('click', close));
    document.addEventListener('click', event => { if (!outline.contains(event.target)) close(); });
    outline.addEventListener('keydown', event => {
      if (event.key === 'Escape') { close(); toggle.focus(); }
    });
  }
  const widget = document.querySelector('[data-narration]');
  const article = document.querySelector('.article-body');
  if (!widget || !article) return;
  const scrollMeter = document.querySelector('[data-reader-scroll]');
  if (scrollMeter) {
    let pending = false;
    const measure = () => {
      pending = false;
      const pageTop = article.getBoundingClientRect().top + window.scrollY;
      const total = Math.max(1, article.offsetHeight);
      const readingPosition = window.scrollY + window.innerHeight * 0.3;
      const fraction = Math.max(0, Math.min(1, (readingPosition - pageTop) / total));
      scrollMeter.style.transform = 'scaleX(' + fraction.toFixed(4) + ')';
    };
    const update = () => {
      if (pending) return;
      pending = true;
      window.requestAnimationFrame(measure);
    };
    window.addEventListener('scroll', update, { passive: true });
    window.addEventListener('resize', update, { passive: true });
    update();
  }
  /* Journal audio edition: native recorded audio when a track exists;
     browser speech otherwise. The latter shows a text-position map, never
     purports to show measured audio amplitudes. */
  const play = widget.querySelector('[data-narration-play]');
  const back = widget.querySelector('[data-narration-back]');
  const forward = widget.querySelector('[data-narration-forward]');
  const stop = widget.querySelector('[data-narration-stop]');
  const rate = widget.querySelector('[data-narration-rate]');
  const status = widget.querySelector('[data-narration-status]');
  const seek = widget.querySelector('[data-narration-seek]');
  const copyLink = widget.querySelector('[data-narration-copy]');
  const elapsed = widget.querySelector('[data-narration-elapsed]');
  const total = widget.querySelector('[data-narration-total]');
  const progress = widget.querySelector('[data-narration-progress]');
  const wave = widget.querySelector('[data-narration-wave]');
  const symbol = widget.querySelector('[data-narration-symbol]');
  const audio = widget.querySelector('[data-narration-media]');
  const bars = [...wave.querySelectorAll('i')];
  const isRecording = Boolean(audio);
  const speechSupported = typeof speechSynthesis !== 'undefined' && typeof SpeechSynthesisUtterance !== 'undefined';
  const synth = speechSupported ? speechSynthesis : null;
  const recordingSupported = Boolean(audio && typeof audio.play === 'function');
  const supported = isRecording ? recordingSupported : speechSupported;
  const opening = document.querySelector('.ed-opening');
  let openingVisible = true;
  let pieces = [], prefix = [0], words = 0;
  let cursor = 0, generation = 0, paused = false, active = false;
  let currentNode = null, position = 0, duration = 0, seekUpdating = false;

  const fmt = seconds => {
    const n = Math.max(0, Math.round(Number(seconds) || 0));
    return Math.floor(n / 60) + ':' + String(n % 60).padStart(2, '0');
  };
  const clearHighlight = () => {
    if (currentNode) currentNode.classList.remove('narration-reading');
    currentNode = null;
  };
  const setHighlight = node => {
    if (currentNode === node) return;
    clearHighlight();
    currentNode = node;
    if (node) node.classList.add('narration-reading');
  };
  const textFor = node => {
    const copy = node.cloneNode(true);
    copy.querySelectorAll('.journal-cite,.journal-cite-back,.source-backlinks').forEach(n => n.remove());
    return (copy.textContent || '').replace(/\s+/g, ' ').trim();
  };
  const prepare = () => {
    if (pieces.length) return;
    const nodes = [document.querySelector('#article-title'), document.querySelector('.ed-deck'),
      ...article.querySelectorAll(':scope > h2, :scope > h3, :scope > p, :scope > ul, :scope > blockquote')].filter(Boolean);
    for (const node of nodes) {
      const text = textFor(node);
      if (!text) continue;
      const sentences = text.match(/[^.!?]+[.!?]+|[^.!?]+$/g) || [text];
      let chunk = '';
      for (const sentence of sentences) {
        for (const word of sentence.trim().split(/\s+/).filter(Boolean)) {
          if (chunk && chunk.length + word.length > 240) {
            pieces.push({ text: chunk.trim(), node });
            chunk = '';
          }
          chunk += (chunk ? ' ' : '') + word;
        }
      }
      if (chunk.trim()) pieces.push({ text: chunk.trim(), node });
    }
    prefix = [0];
    for (const part of pieces) {
      part.weight = Math.max(1,part.text.split(/\s+/).length);
      words += part.weight;
      prefix.push(words);
    }
    duration = (words / 165) * 60;
    total.textContent = isRecording ? '—:—' : '≈ ' + fmt(duration / Number(rate.value));
    back.disabled = false;
    forward.disabled = false;
  };
  const seekPercent = value => Math.max(0, Math.min(1, Number(value) || 0));
  const repaint = fraction => {
    position = seekPercent(fraction);
    if (!seekUpdating) seek.value = String(Math.round(position * 1000));
    progress.value = Math.round(position * 100);
    wave.style.setProperty('--audio-position', (position * 100).toFixed(2) + '%');
    bars.forEach((bar, i) => { bar.classList.toggle('is-read', (i + .5) / bars.length <= position); });
    seek.setAttribute('aria-valuetext', Math.round(position * 100) + '% through the article');
    const actualDuration = isRecording ? audio.duration : duration / Number(rate.value);
    elapsed.textContent = fmt(position * (Number.isFinite(actualDuration) ? actualDuration : duration));
  };
  const dock = () => {
    const visible = active && !openingVisible && supported;
    widget.classList.toggle('is-docked', visible);
    widget.dataset.docked = visible ? 'true' : 'false';
  };
  if (opening && typeof IntersectionObserver !== 'undefined') {
    const observer = new IntersectionObserver(entries => {
      openingVisible = entries[0]?.isIntersecting ?? true;
      dock();
    }, { threshold: .025 });
    observer.observe(opening);
    window.addEventListener('pagehide', () => observer.disconnect(), { once: true });
  }
  const state = (name, label) => {
    widget.dataset.state = name;
    status.textContent = label;
    symbol.textContent = name === 'playing' || name === 'starting' ? 'Ⅱ' : '▶';
    play.setAttribute('aria-label', name === 'playing' || name === 'starting' ? 'Pause article narration' : 'Play article narration');
    play.disabled = !supported || name === 'starting';
    stop.disabled = !(active || paused || name === 'finished');
    dock();
  };
  const selectedVoice = () => {
    const locale = (document.documentElement.lang || 'en').toLowerCase();
    const voices = synth.getVoices();
    return voices.find(v => v.lang.toLowerCase().startsWith(locale) && v.localService) ||
           voices.find(v => v.lang.toLowerCase().startsWith(locale)) || null;
  };
  const currentIndex = () => {
    for (let i = 0; i < pieces.length; i++)
      if (prefix[i + 1] / Math.max(1,words) > position) return i;
    return Math.max(0,pieces.length-1);
  };
  const finish = () => {
    active = false; paused = false;
    clearHighlight();
    repaint(1);
    state('finished', 'Finished · Read or listen again');
  };
  const speakNext = token => {
    if (token !== generation || paused || !active) return;
    if (cursor >= pieces.length) { finish(); return; }
    const part = pieces[cursor];
    const index = cursor++;
    setHighlight(part.node);
    repaint(prefix[index] / Math.max(1,words));
    const utterance = new SpeechSynthesisUtterance(part.text);
    utterance.lang = document.documentElement.lang || 'en-US';
    utterance.rate = Number(rate.value);
    const voice = selectedVoice();
    if (voice) utterance.voice = voice;
    utterance.onstart = () => { if (token === generation && !paused) state('playing','Reading passage '+(index+1)+' of '+pieces.length); };
    utterance.onboundary = event => {
      if (token !== generation || !active || paused || !part.text.length) return;
      const within = Math.max(0,Math.min(1,event.charIndex/part.text.length));
      repaint((prefix[index] + part.weight*within)/Math.max(1,words));
    };
    utterance.onend = () => {
      if (token !== generation || paused || !active) return;
      repaint(prefix[index+1] / Math.max(1,words));
      speakNext(token);
    };
    utterance.onerror = event => {
      if (token !== generation) return;
      if (event.error === 'interrupted' || event.error === 'canceled') return;
      active=false; paused=false; clearHighlight();
      state('idle','Audio could not start: '+(event.error || 'browser speech unavailable'));
    };
    synth.speak(utterance);
  };
  const speechStart = () => {
    prepare();
    if (!pieces.length) { state('idle','No readable article text'); return; }
    generation++;
    synth.cancel();
    active = true; paused = false;
    cursor = currentIndex();
    state('starting','Starting your browser’s voice…');
    speakNext(generation);
  };
  const speechPause = () => {
    if (!active || paused) return;
    paused=true;
    synth.pause();
    state('paused','Paused · Resume whenever you like');
  };
  const speechResume = () => {
    if (!paused) return;
    paused=false;
    synth.resume();
    state('playing','Resumed · Reading the article');
  };
  const stopAll = () => {
    generation++;
    if (isRecording) { audio.pause(); audio.currentTime = 0; }
    else if (synth) { synth.cancel(); }
    active=false;paused=false;cursor=0;
    clearHighlight();
    repaint(0);
    state('idle',isRecording?'Recorded edition · Ready':'Browser voice · Ready');
  };
  const jumpTo = fraction => {
    const f = seekPercent(fraction);
    if (isRecording) {
      if (Number.isFinite(audio.duration) && audio.duration > 0)
        audio.currentTime = f * audio.duration;
      repaint(f);
    } else {
      prepare();
      const wasPlaying = active && !paused;
      generation++;
      synth.cancel();
      cursor = pieces.length ? Math.min(pieces.length-1, Math.max(0,
        prefix.findIndex((v,i) => i > 0 && v/Math.max(1,words) >= f)-1)) : 0;
      // A speech synthesizer can only restart at a passage boundary.
      // Display that boundary rather than pretending to seek to a millisecond.
      repaint(prefix[cursor] / Math.max(1, words));
      paused = false;
      active = false;
      clearHighlight();
      if (wasPlaying) speechStart();
      else state('idle','Ready from passage '+(cursor+1)+' · Play to listen');
    }
  };
  const jumpPassages = offset => {
    if (isRecording) {
      if (Number.isFinite(audio.duration) && audio.duration > 0) {
        audio.currentTime = Math.max(0,Math.min(audio.duration,audio.currentTime+offset*15));
        repaint(audio.currentTime / audio.duration);
      }
    } else {
      prepare();
      const index = Math.max(0,Math.min(pieces.length-1,currentIndex()+offset));
      jumpTo(prefix[index]/Math.max(1,words));
    }
  };

  if (isRecording) {
    audio.playbackRate=Number(rate.value);
    audio.addEventListener('loadedmetadata',()=>{
      if (Number.isFinite(audio.duration) && audio.duration > 0) total.textContent=fmt(audio.duration);
    });
    audio.addEventListener('play',()=>{active=true;paused=false;state('playing','Playing the recorded article');});
    audio.addEventListener('pause',()=>{if (active && !audio.ended) {paused=true;state('paused','Paused · Recorded edition');}});
    audio.addEventListener('timeupdate',()=>{
      if (Number.isFinite(audio.duration) && audio.duration > 0) repaint(audio.currentTime/audio.duration);
    });
    audio.addEventListener('ended',finish);
    audio.addEventListener('error',()=>{active=false;paused=false;state('unavailable','Recording unavailable · Read the article below');});
    // True amplitude peaks: only for an actual audio asset.
    if (typeof AudioContext !== 'undefined' || typeof webkitAudioContext !== 'undefined') {
      const AudioCtx=window.AudioContext || window.webkitAudioContext;
      (async()=>{
        try {
          const response=await fetch(audio.currentSrc || audio.src);
          if (!response.ok) return;
          const context=new AudioCtx();
          const buffer=await context.decodeAudioData(await response.arrayBuffer());
          const channel=buffer.getChannelData(0);
          for(let i=0;i<bars.length;i++){
            const from=Math.floor(i*channel.length/bars.length);
            const to=Math.max(from+1,Math.floor((i+1)*channel.length/bars.length));
            const step=Math.max(1,Math.floor((to-from)/500));
            let sum=0,count=0;
            for(let j=from;j<to;j+=step){sum+=channel[j]*channel[j];count++;}
            const peak=Math.sqrt(sum/Math.max(1,count));
            bars[i].style.setProperty('--level',Math.round(9+Math.min(1,peak*5)*47)+'px');
          }
          wave.dataset.measured='true';
          await context.close();
        } catch { wave.dataset.measured='false'; }
      })();
    }
  } else {
    prepare();
  }
  // Useful continuation directly from the article: a reader can listen from
  // the exact section they have reached, without replaying the headline.
  if (!isRecording && supported) {
    for (const heading of article.querySelectorAll(':scope > h2')) {
      const index = pieces.findIndex(part => part.node === heading);
      if (index < 0) continue;
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'journal-listen-from';
      button.textContent = '▶ Listen from here';
      button.setAttribute('aria-label','Listen from section: ' + heading.textContent.trim());
      button.addEventListener('click', () => {
        jumpTo(prefix[index]/Math.max(1,words));
        if (!active) speechStart();
      });
      heading.appendChild(button);
    }
  }

  play.addEventListener('click', async()=>{
    if (!supported) return;
    if (isRecording) {
      if (!audio.paused) audio.pause();
      else {
        try { await audio.play(); } catch { state('unavailable','Recording could not play'); }
      }
    } else if (active && !paused) speechPause();
    else if (paused) speechResume();
    else speechStart();
  });
  back.addEventListener('click',()=>jumpPassages(-1));
  forward.addEventListener('click',()=>jumpPassages(1));
  stop.addEventListener('click',stopAll);
  seek.addEventListener('input',()=>{
    seekUpdating=true;
    const f=Number(seek.value)/1000;
    progress.value=Math.round(f*100);
    wave.style.setProperty('--audio-position',(f*100).toFixed(2)+'%');
    bars.forEach((bar,i)=>bar.classList.toggle('is-read',(i+.5)/bars.length<=f));
    seek.setAttribute('aria-valuetext',Math.round(f*100)+'% through article');
    seekUpdating=false;
  });
  seek.addEventListener('change',()=>jumpTo(Number(seek.value)/1000));
  rate.addEventListener('change',()=>{
    if (isRecording) audio.playbackRate=Number(rate.value);
    else total.textContent='≈ '+fmt(duration/Number(rate.value));
    if (active && !isRecording) status.textContent='Speed '+rate.value+'× applies from the next spoken passage';
  });
  // Preserve actual reader progress only in the page URL via navigation; no
  // analytics, cookies, or remote audio synthesis calls are added.
  window.addEventListener('pagehide',()=>{
    generation++;
    if (synth) synth.cancel();
    if (audio) audio.pause();
    clearHighlight();
  },{once:true});
  if (copyLink) copyLink.addEventListener('click',async()=>{
    const url = new URL(window.location.href);
    url.hash = 'audio-edition';
    url.searchParams.set('listen',String(Math.round(position*1000)));
    const value = url.toString();
    let copied = false;
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(value);
        copied = true;
      }
    } catch {}
    if (!copied) {
      const field = document.createElement('textarea');
      field.value = value;
      field.style.cssText = 'position:fixed;left:-9999px;top:0';
      document.body.appendChild(field);
      field.select();
      try { copied = document.execCommand('copy'); } catch {}
      field.remove();
    }
    status.textContent = copied ? 'Passage link copied' : 'Copy the page URL to share this article';
  });
  repaint(0);
  if (!supported) {
    back.disabled = true;forward.disabled = true;seek.disabled = true;rate.disabled = true;
    state('unavailable','This browser cannot play the audio edition');
  } else {
    state('idle',isRecording?'Recorded edition · Ready':'Browser voice · Ready');
    // A shared link can cue a passage, but playback always needs a user click.
    const cue = new URLSearchParams(window.location.search).get('listen');
    if (cue && /^\d{1,4}$/.test(cue) && Number(cue) <= 1000) {
      jumpTo(Number(cue)/1000);
      status.textContent='Shared passage ready · Press play to listen';
    }
  }
})();
