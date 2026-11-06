import { fileURLToPath } from 'node:url';

let cached;
export function publisherGrammarPolicy() {
  if (!cached) {
    const path = fileURLToPath(new URL('../publisher-grammars.cue', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', path, '-e', 'publisherGrammarPolicy']);
    if (result.exitCode !== 0) throw new Error(`CUE publisher grammar: ${new TextDecoder().decode(result.stderr).trim()}`);
    cached = JSON.parse(new TextDecoder().decode(result.stdout));
  }
  return cached;
}

export function publisherGrammar(id) {
  return publisherGrammarPolicy().grammars.find(grammar => grammar.id === id);
}
