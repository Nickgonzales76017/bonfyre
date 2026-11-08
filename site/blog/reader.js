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
  const start = widget.querySelector('[data-narration-play]');
  const pause = widget.querySelector('[data-narration-pause]');
  const stop = widget.querySelector('[data-narration-stop]');
  const rate = widget.querySelector('[data-narration-rate]');
  const status = widget.querySelector('[data-narration-status]');
  const supported = typeof speechSynthesis !== 'undefined' && typeof SpeechSynthesisUtterance !== 'undefined';
  let pieces = [], cursor = 0, generation = 0, paused = false, active = false, currentNode = null;
  const clearNode = () => { if (currentNode) currentNode.classList.remove('narration-reading'); currentNode = null; };
  const setState = (state, label) => {
    widget.dataset.state = state;
    status.textContent = label;
    start.disabled = !supported || state === 'playing';
    start.textContent = state === 'paused' ? '▶ Resume' : state === 'playing' ? '▶ Playing' : '▶ Play';
    pause.disabled = state !== 'playing';
    stop.disabled = state === 'idle' || state === 'unavailable' || state === 'finished';
  };
  if (!supported) {
    setState('unavailable', 'Narration is unavailable in this browser. The essay remains readable below.');
    return;
  }
  const synth = speechSynthesis;
  const chooseVoice = () => {
    const options = synth.getVoices();
    const locale = document.documentElement.lang || 'en';
    return options.find(v => v.lang.toLowerCase().startsWith(locale.toLowerCase()) && v.localService) ||
           options.find(v => v.lang.toLowerCase().startsWith(locale.toLowerCase())) || null;
  };
  const textFor = node => {
    const copy = node.cloneNode(true);
    copy.querySelectorAll('.journal-cite,.journal-cite-back').forEach(n => n.remove());
    return (copy.textContent || '').replace(/\s+/g, ' ').trim();
  };
  const prepare = () => {
    const nodes = [document.querySelector('#article-title'), document.querySelector('.garden-subtitle'),
       ...article.querySelectorAll(':scope > h2, :scope > h3, :scope > p, :scope > ul')].filter(Boolean);
    const result = [];
    for (const node of nodes) {
      const text = textFor(node);
      if (!text) continue;
      const sentences = text.match(/[^.!?]+[.!?]+|[^.!?]+$/g) || [text];
      let chunk = '';
      for (const sentence of sentences) {
        const words = sentence.trim().split(/\s+/);
        for (let i = 0; i < words.length; i++) {
          if ((chunk + ' ' + words[i]).length > 260 && chunk) {
            result.push({ text: chunk.trim(), node });
            chunk = '';
          }
          chunk += (chunk ? ' ' : '') + words[i];
        }
      }
      if (chunk.trim()) result.push({ text: chunk.trim(), node });
    }
    return result;
  };
  const finished = () => {
    active = false; paused = false; clearNode();
    setState('finished', 'Finished · Read again or continue the essay below');
  };
  const speakNext = token => {
    if (token !== generation || paused) return;
    if (cursor >= pieces.length) { finished(); return; }
    const item = pieces[cursor++];
    clearNode();
    currentNode = item.node;
    currentNode.classList.add('narration-reading');
    const utterance = new SpeechSynthesisUtterance(item.text);
    utterance.lang = document.documentElement.lang || 'en-US';
    utterance.rate = Number(rate.value) || 1;
    const voice = chooseVoice();
    if (voice) utterance.voice = voice;
    utterance.onstart = () => { if (token === generation && !paused) setState('playing', 'Playing · Part ' + cursor + ' of ' + pieces.length); };
    utterance.onend = () => { if (token === generation && !paused) speakNext(token); };
    utterance.onerror = event => {
      if (token !== generation) return;
      if (event.error === 'canceled' || event.error === 'interrupted') return;
      active = false; clearNode();
      setState('idle', 'Narration stopped: ' + (event.error || 'speech service unavailable'));
    };
    synth.speak(utterance);
  };
  start.addEventListener('click', () => {
    if (paused) {
      paused = false;
      synth.resume();
      setState('playing', 'Playing · Part ' + cursor + ' of ' + pieces.length);
      return;
    }
    generation += 1;
    synth.cancel();
    pieces = prepare();
    cursor = 0;
    if (!pieces.length) { setState('idle', 'No readable article text was found'); return; }
    active = true; paused = false;
    setState('playing', 'Starting browser narration…');
    speakNext(generation);
  });
  pause.addEventListener('click', () => {
    if (!active || paused) return;
    paused = true;
    synth.pause();
    setState('paused', 'Paused · Part ' + cursor + ' of ' + pieces.length);
  });
  stop.addEventListener('click', () => {
    generation += 1; active = false; paused = false; synth.cancel(); clearNode();
    setState('idle', 'Browser voice · Ready');
  });
  window.addEventListener('pagehide', () => {
    generation += 1; synth.cancel(); clearNode();
  }, { once: true });
  setState('idle', 'Browser voice · Ready');
})();