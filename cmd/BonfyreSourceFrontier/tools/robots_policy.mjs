// RFC 9309 group and longest-match path resolution without regex on untrusted paths.
const robotsPathMatches = (raw, path) => {
  const anchored = raw.endsWith('$'), pattern = anchored ? raw.slice(0, -1) : raw;
  const parts = pattern.split('*');
  if (!path.startsWith(parts[0])) return false;
  if (parts.length === 1) return anchored ? path === pattern : true;
  let cursor = parts[0].length;
  for (let index = 1; index < parts.length; index++) {
    const part = parts[index];
    if (anchored && index === parts.length - 1) return part ? path.endsWith(part) &&
      path.length - part.length >= cursor : true;
    const found = path.indexOf(part, cursor);
    if (found < 0) return false;
    cursor = found + part.length;
  }
  return true;
};

export function robotsAllows(body, agent, address) {
  const groups = [];
  let group = { agents: [], rules: [] }, seenRule = false;
  const push = () => { if (group.agents.length) groups.push(group); group = { agents: [], rules: [] }; seenRule = false; };
  for (const raw of body.split(/\r?\n/)) {
    const line = raw.split('#', 1)[0].trim();
    if (!line) { if (seenRule) push(); continue; }
    const split = line.indexOf(':');
    if (split < 0) continue;
    const key = line.slice(0, split).trim().toLowerCase(), value = line.slice(split + 1).trim();
    if (key === 'user-agent') {
      if (seenRule) push();
      group.agents.push(value.toLowerCase());
    } else if ((key === 'allow' || key === 'disallow') && group.agents.length) {
      seenRule = true;
      if (value) group.rules.push({ allow: key === 'allow', path: value });
    }
  }
  push();
  const id = agent.toLowerCase();
  const exact = groups.filter(item => item.agents.some(name => name !== '*' && id.includes(name)));
  const selected = exact.length ? exact : groups.filter(item => item.agents.includes('*'));
  const path = new URL(address).pathname + new URL(address).search;
  const matches = selected.flatMap(item => item.rules).filter(rule => robotsPathMatches(rule.path, path))
    .toSorted((a, b) => b.path.length - a.path.length || Number(b.allow) - Number(a.allow));
  return matches[0]?.allow ?? true;
}
