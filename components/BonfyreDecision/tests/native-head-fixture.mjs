// Deterministic synthetic tensors, NOT released model weights or accuracy data.
export function headFixture(binding, { hidden_size = 6, width = 4, heads = 2,
  feedforward = 7, routing_layers = 1, layers = 2, seed = 1, zero = false } = {}) {
  const config = { hidden_size, width, heads, feedforward, routing_layers, layers };
  const values = (name, length) => {
    const salt = [...name].reduce((n, c) => n + c.codePointAt(0), seed);
    return Float32Array.from({ length }, (_, i) => zero ? 0 : ((i * 17 + salt * 7) % 97 - 48) / 100);
  };
  const tensors = Object.fromEntries(binding.layout(config).map(item => {
    const data = values(item.name, item.rows * item.columns);
    if (item.rank === 1 && item.name.endsWith('.weight'))
      for (let i = 0; i < data.length; i++) data[i] += 1;
    return [item.name, { shape: [item.rows, item.columns].slice(0, item.rank), data }];
  }));
  const fields = [
    { id: 'truth', type: 'noul', question_span: [1, 3], options: [
      { id: 'true', span: [3, 4] }, { id: 'false', span: [4, 6] }] },
    { id: 'route', type: 'choice', question_span: [6, 7], options: [
      { id: 'alpha', span: [7, 9] }, { id: 'zulu', span: [9, 10] }] },
    { id: 'priority', type: 'score', question_span: [10, 12], options: [
      { id: '0', span: [12, 13] }, { id: '1', span: [13, 15] }, { id: '2', span: [15, 16] }] },
  ];
  return { config, tensors, fields, hidden: values('hidden', 18 * hidden_size),
    lexical_options: values('lexical', 7 * hidden_size) };
}

export function packedSpans(fields) {
  const words = [], options = [];
  for (const field of fields) {
    words.push({ noul: 0, choice: 1, score: 2 }[field.type], ...field.question_span,
      options.length / 2, field.options.length);
    for (const option of field.options) options.push(...option.span);
  }
  return { fields: Uint32Array.from(words), options: Uint32Array.from(options) };
}

if (import.meta.main) {
  const { nativeDecisionBinding } = await import('../native-head.mjs');
  const cases = [
    { seed: 1 }, { seed: 23, routing_layers: 2, layers: 1 },
    { seed: 101, hidden_size: 9, width: 6, heads: 3, feedforward: 11 },
    { seed: 2, routing_layers: 0, layers: 0 }, { zero: true },
  ];
  console.log(JSON.stringify(cases.map(options => ({ options, ...headFixture(nativeDecisionBinding(), options) })),
    (_, value) => value instanceof Float32Array ? [...value] : value));
}
