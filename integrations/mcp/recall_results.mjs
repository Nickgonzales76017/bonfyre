// Pure presentation of native Recall candidates; never reclassifies evidence.
export function collapseExactPassages(candidates, limit) {
  if (!Array.isArray(candidates) || !Number.isSafeInteger(limit) || limit < 1 || limit > 50)
    throw new Error('Invalid candidate projection.');
  const seen = new Map(), groups = [];
  let duplicateRows = 0;
  for (const row of candidates) {
    if (!row || typeof row.id !== 'string' || typeof row.text !== 'string')
      throw new Error('Recall candidate has no source or text.');
    const earlier = seen.get(row.text);
    if (earlier) {
      if (earlier.id !== row.id && !earlier.same_text_ids.includes(row.id))
        earlier.same_text_ids.push(row.id);
      duplicateRows++;
      continue;
    }
    const entry = {...row, same_text_ids: []};
    seen.set(row.text, entry);
    groups.push(entry);
  }
  return { passages: groups.slice(0, limit),
    selected: groups.length, input_candidates: candidates.length,
    exact_text_duplicate_rows: duplicateRows,
    source_scope: 'same bytes in this candidate result only; source identities remain distinct' };
}
