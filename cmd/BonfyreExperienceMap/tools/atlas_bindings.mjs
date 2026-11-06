// Explicit candidate relationships belong to the architecture that declares
// them. Source paths join exactly; a schema does not classify its entire CMS.
export function atlasBindings(index, chains, sourceExists) {
  const sources = new Map(), architectures = [];
  for (const [id, record] of Object.entries(index.architectures ?? {}).sort(([a], [b]) => a.localeCompare(b))) {
    const declared = record.experience_chains ?? [];
    if (!Array.isArray(declared)) throw new Error(`architecture ${id} has invalid experience chains`);
    if (!declared.length) continue;
    if (new Set(declared).size !== declared.length || declared.some(chain => !chains.has(chain)))
      throw new Error(`architecture ${id} has duplicate or unknown experience chains`);
    const paths = record.source_paths ?? [];
    if (!paths.length) throw new Error(`architecture ${id} needs a source for its experience chains`);
    for (const path of paths) {
      if (!sourceExists(path)) throw new Error(`architecture ${id} has an unavailable source: ${path}`);
      if (!sources.has(path)) sources.set(path, []);
      sources.get(path).push({architecture: id, chains: [...declared]});
    }
    architectures.push({id, record});
  }
  return {sources, architectures};
}

export function sourceClassification(bindings, source, fallback) {
  const matches = bindings.sources.get(source) ?? [];
  return matches.length ? {
    basis: 'atlas-source-path',
    human_chains: [...new Set(matches.flatMap(match => match.chains))],
    architecture_ids: matches.map(match => match.architecture),
  } : {basis: 'name-hint-or-fallback', human_chains: fallback, architecture_ids: []};
}
