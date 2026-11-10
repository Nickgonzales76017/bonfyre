// Restrict agent-authored estate reads to the three declared views. This is an
// admission lexer, not a SQL rewriter: uncertain syntax is refused.
const RELATIONS = new Set(['observation', 'page', 'parcel_flat']);
const FUNCTIONS = new Set([
  'abs', 'array_agg', 'avg', 'cast', 'ceil', 'coalesce', 'concat', 'count',
  'date_trunc', 'dense_rank', 'first', 'floor', 'greatest', 'ifnull',
  'json_extract', 'json_extract_string', 'last', 'least', 'length', 'list',
  'lower', 'max', 'median', 'min', 'nullif', 'quantile_cont', 'rank',
  'regexp_matches', 'replace', 'round', 'row_number', 'string_agg',
  'substr', 'substring', 'sum', 'trim', 'try_cast', 'upper',
]);
const CONTROL = new Set(['attach', 'call', 'checkpoint', 'copy', 'create', 'delete',
  'detach', 'drop', 'execute', 'export', 'import', 'insert', 'install', 'load',
  'pragma', 'reset', 'set', 'truncate', 'update', 'vacuum']);
const PAREN_KEYWORDS = new Set(['as', 'exists', 'filter', 'from', 'in', 'join', 'not', 'on', 'over', 'select', 'using', 'where', 'with']);
const END_FROM = new Set(['except', 'group', 'having', 'intersect', 'limit', 'order', 'qualify', 'union', 'where', 'window']);

export function admitEstateSql(source) {
  if (typeof source !== 'string' || !source.trim()) return { error: 'empty statement' };
  if (source.length > 50_000) return { error: 'query too large' };
  const tokens = [];
  let depth = 0;
  for (let i = 0; i < source.length;) {
    const char = source[i];
    if (/\s/.test(char)) { i++; continue; }
    if (char === '\0' || (char === '-' && source[i + 1] === '-') || (char === '/' && source[i + 1] === '*')) return { error: 'comments and control bytes are not permitted' };
    if (char === "'") {
      let j = i + 1, closed = false;
      while (j < source.length) {
        if (source[j] === "'") { if (source[j + 1] === "'") { j += 2; continue; } closed = true; j++; break; }
        j++;
      }
      if (!closed) return { error: 'unterminated string' };
      tokens.push({ text: source.slice(i, j), type: 'string', depth }); i = j; continue;
    }
    if (char === '"' || char === '`' || char === '$') return { error: 'quoted identifiers and dollar strings are not admitted' };
    if (/[A-Za-z_]/.test(char)) {
      let j = i + 1; while (j < source.length && /[A-Za-z0-9_]/.test(source[j])) j++;
      tokens.push({ text: source.slice(i, j).toLowerCase(), type: 'word', depth }); i = j; continue;
    }
    if (/[0-9]/.test(char)) {
      let j = i + 1; while (j < source.length && /[0-9.]/.test(source[j])) j++;
      tokens.push({ text: source.slice(i, j), type: 'number', depth }); i = j; continue;
    }
    if (char === '(') { tokens.push({ text: char, type: 'punct', depth }); depth++; i++; continue; }
    if (char === ')') { depth--; if (depth < 0) return { error: 'unbalanced parentheses' }; tokens.push({ text: char, type: 'punct', depth }); i++; continue; }
    if (';,.*+-/%<>=!|&[]:'.includes(char)) { tokens.push({ text: char, type: 'punct', depth }); i++; continue; }
    return { error: `unsupported SQL character: ${char}` };
  }
  if (depth) return { error: 'unbalanced parentheses' };
  while (tokens.at(-1)?.text === ';') tokens.pop();
  if (tokens.some(token => token.text === ';')) return { error: 'one statement only' };
  if (!tokens.length || !['select', 'with'].includes(tokens[0].text)) return { error: 'read-only: statement must begin SELECT or WITH' };
  const ctes = new Set(), fromDepths = new Set();
  if (tokens[0].text === 'with') {
    let index = tokens[1]?.text === 'recursive' ? 2 : 1;
    while (index < tokens.length && tokens[index]?.depth === 0 && tokens[index]?.type === 'word') {
      ctes.add(tokens[index].text);
      while (index < tokens.length && !(tokens[index].text === ')' && tokens[index].depth === 0)) index++;
      if (tokens[index + 1]?.text !== ',') break;
      index += 2;
    }
    if (!tokens.some(token => token.text === 'select' && token.depth === 0)) return { error: 'WITH must end in SELECT' };
  }
  for (let index = 0; index < tokens.length; index++) {
    const token = tokens[index], next = tokens[index + 1];
    if (token.type === 'word' && CONTROL.has(token.text)) return { error: `read-only: ${token.text.toUpperCase()} is not permitted here` };
    if (token.text === 'from' || token.text === 'join') {
      if (!next || (next.text !== '(' && !(next.type === 'word' && (RELATIONS.has(next.text) || ctes.has(next.text))))) return { error: 'relation outside declared estate views' };
      if (token.text === 'from') fromDepths.add(token.depth);
    }
    if (token.type === 'word' && next?.text === '(' && !PAREN_KEYWORDS.has(token.text) && !FUNCTIONS.has(token.text)) return { error: `function not admitted: ${token.text}` };
    if (token.type === 'word' && END_FROM.has(token.text)) fromDepths.delete(token.depth);
    if (token.text === ',' && fromDepths.has(token.depth)) return { error: 'comma joins are not admitted; use an explicit JOIN' };
    if (token.text === ')' && fromDepths.has(token.depth + 1)) fromDepths.delete(token.depth + 1);
  }
  return { sql: source.trim().replace(/;+$/, '').trim() };
}
