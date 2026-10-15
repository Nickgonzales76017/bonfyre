/* Progressive enhancement of the authored link list. No second demo registry. */
(function () {
  'use strict';
  const list = document.querySelector('#experiences [data-demo-list]');
  if (!list || document.getElementById('demo-showcase')) return;
  const seen = new Set();
  const safeURL = value => {
    try {
      const url = new URL(value, document.baseURI);
      return url.protocol === 'https:' && !url.username && !url.password ? url.href : null;
    } catch { return null; }
  };
  const entries = Array.from(list.querySelectorAll('a[data-demo]')).flatMap(source => {
    const href = safeURL(source.getAttribute('href'));
    const title = source.querySelector('strong')?.textContent.trim();
    if (!href || !title || seen.has(href)) return [];
    seen.add(href);
    return [{href, title, summary: source.querySelector('.detail')?.textContent.trim() || '',
      kind: source.dataset.kind || 'Interactive workspace',
      image: source.dataset.shot ? safeURL(source.dataset.shot) : null,
      alt: source.dataset.shotAlt || '', action: source.dataset.action || 'Open workspace'}];
  });
  if (!entries.length) return;
  const node = (tag, className, text) => {
    const element = document.createElement(tag);
    element.className = className;
    if (text !== undefined) element.textContent = text;
    return element;
  };
  const section = node('section', 'showcase');
  section.id = 'demo-showcase';
  section.setAttribute('aria-label', 'Featured application previews');
  const head = node('div', 'showcase-head');
  const intro = node('div', 'showcase-intro');
  intro.append(node('div', 'eyebrow', 'Featured work / public previews'),
    node('p', '', 'Open an application or browse the complete list below.'));
  const adoption = node('a', 'action secondary', 'Review Aurekai setup');
  adoption.id = 'aurekai-adoption';
  adoption.href = 'https://aurekai.work/packs/setup';
  adoption.setAttribute('aria-describedby', 'aurekai-adoption-scope');
  const adoptionScope = node('p', '', 'Keep and share a versioned workflow configuration in your Aurekai account. Demo drafts and application backends remain separate.');
  adoptionScope.id = 'aurekai-adoption-scope';
  intro.append(adoption, adoptionScope);
  const controls = node('div', 'showcase-actions');
  const previous = node('button', '', '←'), next = node('button', '', '→');
  for (const [button, label] of [[previous, 'Previous preview'], [next, 'Next preview']]) {
    button.type = 'button';
    button.setAttribute('aria-label', label);
    button.setAttribute('aria-controls', 'demo-preview-track');
  }
  controls.append(previous, next); head.append(intro, controls); section.append(head);
  const track = node('div', 'showcase-scroll');
  track.id = 'demo-preview-track'; track.tabIndex = 0;
  track.setAttribute('role', 'region'); track.setAttribute('aria-label', 'Application previews');
  for (const item of entries) {
    const card = node('a', 'showcase-card'); card.href = item.href;
    // Only published captures with an explicit description are eligible. No mock UI.
    if (item.image && item.alt) {
      const image = node('img', ''); image.src = item.image; image.alt = item.alt;
      image.loading = 'lazy'; image.decoding = 'async';
      image.addEventListener('error', () => image.remove(), {once: true});
      card.append(image);
    }
    const body = node('div', 'card-body');
    body.append(node('span', 'card-type', item.kind), node('strong', 'heading', item.title),
      node('span', 'summary', item.summary), node('span', 'cta', item.action + ' ↗'));
    card.append(body); track.append(card);
  }
  section.append(track, node('p', 'showcase-note',
    'Screenshots appear only where a published capture is available. Preview edits do not authorize purchases, payments or external publication.'));
  list.parentNode.insertBefore(section, list);
  const updateControls = () => {
    const maximum = Math.max(0, track.scrollWidth - track.clientWidth);
    previous.disabled = track.scrollLeft <= 1;
    next.disabled = track.scrollLeft >= maximum - 1;
  };
  const scroll = direction => {
    const reduced = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;
    const distance = (track.firstElementChild?.getBoundingClientRect().width || 350) + 16;
    track.scrollBy({left: direction * distance, behavior: reduced ? 'auto' : 'smooth'});
    updateControls();
  };
  previous.addEventListener('click', () => scroll(-1));
  next.addEventListener('click', () => scroll(1));
  track.addEventListener('keydown', event => {
    if (event.target !== track || event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return;
    if (event.key === 'ArrowLeft' || event.key === 'ArrowRight') {
      event.preventDefault(); scroll(event.key === 'ArrowLeft' ? -1 : 1);
    }
  });
  track.addEventListener('scroll', updateControls, {passive: true});
  window.addEventListener('resize', updateControls, {passive: true});
  updateControls();
})();
