(() => {
  const host = window.location.hostname.toLowerCase();
  /* The edge places this only after the native Matrix resolve succeeds. The
     hostname remains a style fallback for static previews, never identity. */
  const matrix = window.BonfyreMatrixContext || null;
  const territory = matrix?.territory || (host.endsWith('.aurekai.life') || host === 'aurekai.life' ? 'life'
    : host.endsWith('.aurekai.live') || host === 'aurekai.live' ? 'live'
    : host.endsWith('.aurekai.shop') || host === 'aurekai.shop' ? 'shop'
    : host.endsWith('.aurekai.work') || host === 'aurekai.work' ? 'work'
    : host.includes('leaflounge') || host === 'theleaflounge.store' ? 'leaf' : 'aurekai');
  document.body.dataset.territory = territory;
  const content = {
    life: { label: 'Aurekai Life / personal reality with room to grow', kicker: 'Your life is already a world.', title: 'Keep it close.<br>Let it <em>become more.</em>', copy: 'Plans, collections, household reality, and personal projects can stay private—until you decide that a piece belongs with your people, a community, or a market.', action: 'Start from your reality', subject: 'Household world', kind: 'PERSONAL REALITY' },
    live: { label: 'Aurekai Live / participation in motion', kicker: 'Make room around what is happening.', title: 'A world that<br>knows how to <em>gather.</em>', copy: 'Events, creators, culture, attention, temporary rooms, and shared objects become legible without flattening people into a feed.', action: 'See what can gather', subject: 'Reality-born room', kind: 'OCCURRENCE' },
    shop: { label: 'Aurekai Shop / public economic participation', kicker: 'Markets begin with a real need.', title: 'Let demand find<br>its <em>way to supply.</em>', copy: 'Offers, wants, services, capacities, proof, and settlement can emerge from ordinary participation instead of being trapped inside a storefront.', action: 'Explore market formation', subject: 'Local market', kind: 'MARKET' },
    work: { label: 'Aurekai Work / institutional operation', kicker: 'Operate without losing the world around the work.', title: 'The institution<br>stays <em>attached to reality.</em>', copy: 'Companies, teams, collaborators, obligations, evidence, money, and external systems share a governed surface without becoming one generic ERP.', action: 'Open the operating surface', subject: 'Aurekai operating estate', kind: 'INSTITUTION' },
    leaf: { label: 'Leaf Lounge / a living plant institution', kicker: 'Plants are not inventory. They are lineage.', title: 'Plant culture<br>with <em>roots.</em>', copy: 'Collections, care, propagation, workshops, trade, provenance, and a physical place—Leaf Lounge is a living institution rather than a store with a blog.', action: 'Enter Leaf Lounge', subject: 'Leaf Lounge', kind: 'INSTITUTION' },
    aurekai: { label: 'Aurekai / the shared world beneath software', kicker: 'A shared world beneath software.', title: 'Make what matters <em>legible.</em>', copy: 'Bonfÿre gives people, institutions, objects, markets, media, evidence, and work one addressable reality—then projects the right local surface when it is needed.', action: 'Start with what exists', subject: 'Aurekai', kind: 'OPERATING COMPANY' }
  }[territory];
  const humanize = (value) => String(value || '')
    .split(':').pop().replace(/[-_]/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
  if (matrix?.subject) {
    const subject = humanize(matrix.subject);
    const kind = String(matrix.subject).split(':', 1)[0].replace(/[-_]/g, ' ').toUpperCase();
    const territoryName = humanize(matrix.territory);
    content.label = `${subject} / ${territoryName} presence`;
    content.kicker = `${subject} is present here.`;
    content.title = `${subject}<br><em>in its own context.</em>`;
    content.copy = `This address resolves one ${kind.toLowerCase()} through the ${territoryName} territory. Web, BonfÿreFS, API, MCP, and other lawful coordinates remain one subject; this surface does not broaden authority.`;
    content.action = territory === 'work' ? 'Open the operating surface' : 'Explore this presence';
    content.subject = subject;
    content.kind = kind;
    document.body.dataset.matrixSubject = matrix.subject;
    document.body.dataset.matrixPresence = matrix.presence || '';
    document.body.dataset.matrixWorld = matrix.world || '';
    document.body.dataset.matrixAudience = matrix.audience || '';
  }
  const set = (id, value) => { const el = document.getElementById(id); if (el) el.innerHTML = value; };
  set('territory-label', content.label); set('hero-kicker', content.kicker); set('hero-title', content.title); set('hero-copy', content.copy); set('primary-action', `${content.action} <span>↓</span>`); set('subject-name', content.subject); set('subject-kind', content.kind); set('web-address', matrix?.canonicalAddress || host);
  document.title = `${content.subject} · Bonfÿre`;
})();
