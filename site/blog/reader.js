/* Aurekai Journal: reading outline and recorded-audio edition.
   Editorial audio is a published file, never device TTS or a voice picker. */
(() => {
  'use strict';
  const outline = document.querySelector('[data-outline-menu]');
  if (outline) {
    const toggle = outline.querySelector('.reader-outline-trigger');
    const close = () => { outline.dataset.open = 'false'; toggle.setAttribute('aria-expanded', 'false'); };
    toggle.addEventListener('click', () => {
      const open = outline.dataset.open !== 'true';
      outline.dataset.open = String(open);
      toggle.setAttribute('aria-expanded', String(open));
    });
    outline.querySelectorAll('a').forEach(link => link.addEventListener('click', close));
    document.addEventListener('click', e => { if (!outline.contains(e.target)) close(); });
    outline.addEventListener('keydown', e => { if (e.key === 'Escape') { close(); toggle.focus(); } });
  }

  const article = document.querySelector('.article-body');
  const scrollMeter = document.querySelector('[data-reader-scroll]');
  if (scrollMeter && article) {
    let pending = false;
    const measure = () => {
      pending = false;
      const pageTop = article.getBoundingClientRect().top + window.scrollY;
      const readingPosition = window.scrollY + window.innerHeight * .3;
      const fraction = Math.min(1, Math.max(0, (readingPosition-pageTop)/Math.max(1,article.offsetHeight)));
      scrollMeter.style.transform = 'scaleX(' + fraction.toFixed(4) + ')';
    };
    const request = () => {
      if (pending) return;
      pending=true;
      window.requestAnimationFrame(measure);
    };
    window.addEventListener('scroll', request, {passive:true});
    window.addEventListener('resize', request, {passive:true});
    request();
  }

  const player = document.querySelector('[data-narration]');
  if (!player) return;
  const audio = player.querySelector('[data-audio-source]');
  const controls = player.querySelector('[data-audio-ui]');
  const play = player.querySelector('[data-audio-play]');
  const icon = player.querySelector('[data-audio-icon]');
  const seek = player.querySelector('[data-audio-seek]');
  const elapsed = player.querySelector('[data-audio-elapsed]');
  const duration = player.querySelector('[data-audio-duration]');
  const speed = player.querySelector('[data-audio-speed]');
  const wave = player.querySelector('[data-audio-wave]');
  const timeline = player.querySelector('.ed-audio-timeline');
  const status = player.querySelector('[data-audio-status]');
  const dismiss = player.querySelector('[data-audio-dismiss]');
  const opening = document.querySelector('.ed-opening');
  if (!(audio && controls && play && seek && wave && speed && status)) return;

  // Only enhance a real, authored MP3. The browser's native <audio controls>
  // remains the functional fallback if JavaScript is disabled.
  audio.controls = false;
  controls.hidden = false;
  const format = seconds => {
    if (!Number.isFinite(seconds)) return '–:––';
    const n = Math.max(0, Math.floor(seconds));
    return Math.floor(n / 60)+':'+String(n % 60).padStart(2,'0');
  };
  let visibleOpening = true;
  let dismissed = false;
  let isSeeking = false;
  let waveformStarted = false;
  const playable = () => Number.isFinite(audio.duration) && audio.duration > 0;
  const dock = () => {
    const playingOrPaused = !audio.ended && audio.currentTime > 0;
    player.classList.toggle('is-docked', Boolean(!visibleOpening && playingOrPaused && !dismissed));
  };
  const update = () => {
    const current = Number.isFinite(audio.currentTime) ? audio.currentTime : 0;
    elapsed.textContent = format(current);
    if (playable()) {
      seek.disabled = false;
      duration.textContent = format(audio.duration);
      if (!isSeeking) seek.value = String(Math.round(current / audio.duration * 1000));
      seek.setAttribute('aria-valuetext',format(current)+' of '+format(audio.duration));
    }
    const fraction = playable() ? current / audio.duration : 0;
    timeline.style.setProperty('--played',(fraction * 100).toFixed(2)+'%');
    wave.querySelectorAll('i').forEach((bar,i,bars)=>{
      bar.classList.toggle('is-read',(i+.5)/bars.length <= fraction);
    });
    dock();
  };
  const updateState = () => {
    const running = !audio.paused && !audio.ended;
    icon.textContent = running ? 'Ⅱ' : '▶';
    play.setAttribute('aria-label', running ? 'Pause Sarah narration' : 'Play Sarah narration');
    player.dataset.playback = running ? 'playing' : audio.ended ? 'ended' : 'paused';
  };
  const attempt = async () => {
    try {
      await audio.play();
      status.textContent = 'Sarah · Listening edition';
    } catch {
      status.textContent = 'The recording could not be played. Read the essay below.';
      audio.controls = true;
      controls.hidden = true;
    }
  };
  play.addEventListener('click', () => audio.paused ? attempt() : audio.pause());
  audio.addEventListener('play', () => { status.textContent='Sarah · Listening edition'; updateState(); dock(); });
  audio.addEventListener('pause', () => { updateState(); dock(); });
  audio.addEventListener('ended', () => { status.textContent='End of recording'; updateState(); update(); });
  audio.addEventListener('error', () => {
    status.textContent='Audio unavailable · Read the article instead';
    audio.controls=true;
    controls.hidden=true;
    player.classList.remove('is-docked');
  });
  audio.addEventListener('loadedmetadata', update);
  audio.addEventListener('durationchange', update);
  audio.addEventListener('timeupdate', update);
  seek.addEventListener('input', () => {
    isSeeking = true;
    if (playable()) {
      const preview = Number(seek.value)/1000*audio.duration;
      elapsed.textContent=format(preview);
      seek.setAttribute('aria-valuetext',format(preview)+' of '+format(audio.duration));
      timeline.style.setProperty('--played',(Number(seek.value)/10).toFixed(2)+'%');
    }
  });
  const commitSeek = () => {
    if (playable()) audio.currentTime = Number(seek.value)/1000*audio.duration;
    isSeeking = false;
    update();
  };
  seek.addEventListener('change', commitSeek);
  seek.addEventListener('pointerup', commitSeek);
  const rates=[1,1.25,1.5,0.85];
  speed.addEventListener('click', () => {
    const index=rates.findIndex(v=>Math.abs(v-audio.playbackRate)<.01);
    audio.playbackRate=rates[(index+1)%rates.length];
    speed.textContent=audio.playbackRate+'×';
    speed.setAttribute('aria-label','Playback speed '+audio.playbackRate+' times');
  });
  dismiss.addEventListener('click', () => {
    dismissed=true;
    player.classList.remove('is-docked');
    audio.pause();
  });
  if (opening && typeof IntersectionObserver !== 'undefined') {
    const observer = new IntersectionObserver(entries => {
      visibleOpening=entries[0]?.isIntersecting ?? true;
      if (visibleOpening) dismissed=false;
      dock();
    }, {threshold:0});
    observer.observe(opening);
    window.addEventListener('pagehide', () => observer.disconnect(), {once:true});
  }
  // On first actual playback, extract RMS amplitudes from the *recorded*
  // audio. Until decoded, the timeline is a plain seek rail, never fake bars.
  const drawRealWaveform = async () => {
    if (waveformStarted) return;
    waveformStarted=true;
    const Ctx = window.AudioContext || window.webkitAudioContext;
    if (!Ctx) return;
    let context;
    try {
      const response=await fetch(audio.currentSrc || audio.src);
      if (!response.ok) return;
      const bytes=await response.arrayBuffer();
      // Avoid expensive audio decoding on memory-constrained devices.
      if (bytes.byteLength>16000000) return;
      context=new Ctx();
      const buffer=await context.decodeAudioData(bytes);
      const count=64, channel=buffer.getChannelData(0);
      const fragment=document.createDocumentFragment();
      for (let i=0;i<count;i++) {
        const from=Math.floor(i*channel.length/count);
        const to=Math.max(from+1,Math.floor((i+1)*channel.length/count));
        const step=Math.max(1,Math.floor((to-from)/800));
        let sum=0, n=0;
        for (let k=from;k<to;k+=step) {sum+=channel[k]*channel[k];n++;}
        const rms=Math.sqrt(sum/Math.max(1,n));
        const bar=document.createElement('i');
        bar.style.height=(Math.max(8,Math.min(42,Math.round(8+rms*140))))+'px';
        fragment.appendChild(bar);
      }
      wave.replaceChildren(fragment);
      wave.classList.add('has-signal');
      update();
    } catch {
      // Still offer a valid play/pause/seek slider without decorative peaks.
    } finally {
      if (context) await context.close().catch(()=>{});
    }
  };
  play.addEventListener('click', drawRealWaveform, {once:true});

  if ('mediaSession' in navigator) {
    try {
      navigator.mediaSession.metadata = new MediaMetadata({
        title: document.querySelector('#article-title')?.textContent?.trim() || document.title,
        artist: 'Aurekai Journal · Sarah',
        album: 'Aurekai Journal · Recorded essays'
      });
      const handlers = {
        play: () => attempt(),
        pause: () => audio.pause(),
        seekbackward: details => {audio.currentTime=Math.max(0,audio.currentTime-(details.seekOffset||10));},
        seekforward: details => {audio.currentTime=Math.min(audio.duration || Infinity,audio.currentTime+(details.seekOffset||10));},
        seekto: details => {if (Number.isFinite(details.seekTime)) audio.currentTime=details.seekTime;}
      };
      for (const [name, fn] of Object.entries(handlers))
        try {navigator.mediaSession.setActionHandler(name,fn);} catch {}
    } catch {}
  }
  const requested = new URL(window.location.href).searchParams.get('listen');
  if (requested && /^\d{1,4}$/.test(requested) && Number(requested)<=1000) {
    audio.addEventListener('loadedmetadata',()=>{
      if (playable()) audio.currentTime=Number(requested)/1000*audio.duration;
    },{once:true});
  }
  audio.load();
  updateState();
  update();
  window.addEventListener('pagehide',()=>{
    audio.pause();
    if ('mediaSession' in navigator) navigator.mediaSession.playbackState='none';
  },{once:true});
})();


/* Original end-of-essay questions belong to the author; sharing points to
   the actual essay and never sends text to an analytics or social endpoint. */
(() => {
  "use strict";
  const share = document.querySelector("[data-reflection-share]");
  if (!share) return;
  const article = document.querySelector("#journal-afterword");
  const question = article?.querySelector("h2")?.textContent?.trim();
  const status = article?.querySelector("[data-reflection-status]");
  if (!question || !status) return;
  share.addEventListener("click", async () => {
    const url = new URL(location.href);
    url.search = "";
    url.hash = "journal-afterword";
    const text = question + "\n— Aurekai Journal";
    if (navigator.share) {
      try {
        await navigator.share({title: document.title, text, url: url.toString()});
        status.textContent = "Question shared";
        return;
      } catch (error) {
        if (error?.name === "AbortError") return;
      }
    }
    const value = text + "\n" + url.toString();
    let copied = false;
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(value);
        copied = true;
      }
    } catch (_) {}
    if (!copied) {
      const field = document.createElement("textarea");
      field.value = value;
      field.setAttribute("aria-hidden", "true");
      field.style.cssText = "position:fixed;left:-9999px;top:0";
      document.body.appendChild(field);
      field.select();
      try { copied = document.execCommand("copy"); } catch (_) {}
      field.remove();
    }
    status.textContent = copied ? "Question and article link copied" :
      "Copy this article's address to share the question";
  });
})();
