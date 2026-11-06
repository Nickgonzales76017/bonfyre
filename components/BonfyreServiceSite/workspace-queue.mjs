// URL-owned view state. Records and permissions remain with each workspace owner.
export function queueState(search) {
  const p = new URLSearchParams(search);
  return {
    filter: ['all', 'ready', 'mine', 'running', 'attention', 'complete'].includes(p.get('filter')) ? p.get('filter') : 'all',
    query: (p.get('q') ?? '').slice(0, 300),
    layout: p.get('layout') === 'board' ? 'board' : 'list',
    sort: p.get('sort') === 'oldest' ? 'oldest' : 'newest',
    selected: /^[a-f0-9-]{36}$/.test(p.get('request') ?? '') ? p.get('request') : '',
  };
}

export function queueSearch(state) {
  const p = new URLSearchParams();
  for (const [key, value, fallback] of [['filter', state.filter, 'all'], ['q', state.query, ''], ['layout', state.layout, 'list'], ['sort', state.sort, 'newest'], ['request', state.selected, '']]) {
    if (value && value !== fallback) p.set(key, value);
  }
  return p.size ? '?' + p.toString() : '';
}

export const queueMatches = (row, filter, worker) => filter === 'all' || (filter === 'mine' ? row.state === 'running' && row.worker === worker : row.state === filter);

export function queueRows(rows, state, worker) {
  const query = state.query.trim().toLocaleLowerCase();
  return rows.filter(row => queueMatches(row, state.filter, worker))
    .filter(row => `${row.brief.title} ${row.brief.details} ${row.delivery?.routine?.name ?? ''}`.toLocaleLowerCase().includes(query))
    .toSorted((a, b) => (state.sort === 'oldest' ? 1 : -1) * (Date.parse(a.submittedAt) - Date.parse(b.submittedAt)) || a.id.localeCompare(b.id));
}
