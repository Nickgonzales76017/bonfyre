// Independent double-precision, materialized-matrix reference for the published
// Clef head equations. Test-only. This is not a PyTorch or trained-weight oracle.
export function referenceHead(fixture) {
  const { config: c, tensors: t, fields } = fixture;
  const rows = (data, n) => Array.from({ length: data.length / n }, (_, i) => Array.from(data.slice(i * n, (i + 1) * n)));
  const data = name => [...t[name].data];
  const matrix = name => rows(t[name].data, t[name].shape[1]);
  const add = (a, b) => a.map((row, i) => row.map((x, j) => x + b[i][j]));
  const dot = (a, b) => a.reduce((n, x, i) => n + x * b[i], 0);
  const softmax = x => { const m = Math.max(...x), v = x.map(n => Math.exp(n - m)), sum = v.reduce((a, b) => a + b); return v.map(n => n / sum); };
  const linear = (x, name) => {
    const weight = matrix(`${name}.weight`), bias = t[`${name}.bias`]?.data;
    return x.map(row => weight.map((col, i) => dot(row, col) + (bias?.[i] || 0)));
  };
  const norm = (x, name) => x.map(row => {
    const mean = row.reduce((a, b) => a + b) / row.length;
    const variance = row.reduce((n, v) => n + (v - mean) ** 2, 0) / row.length;
    return row.map((v, i) => (v - mean) / Math.sqrt(variance + 1e-5) * t[`${name}.weight`].data[i] + t[`${name}.bias`].data[i]);
  });
  // Abramowitz/Stegun erf approximation; error < 1.5e-7. Native uses libc erf.
  const gelu = x => {
    const z = Math.abs(x) / Math.SQRT2, v = 1 / (1 + 0.3275911 * z);
    const erf = Math.sign(x) * (1 - (((((1.061405429 * v - 1.453152027) * v) + 1.421413741) * v - 0.284496736) * v + 0.254829592) * v * Math.exp(-z * z));
    return 0.5 * x * (1 + erf);
  };
  const ff = (x, first, second) => linear(linear(x, first).map(row => row.map(gelu)), second);
  const attention = (query, memory, name) => {
    const weights = matrix(`${name}.in_proj_weight`), biases = data(`${name}.in_proj_bias`);
    const project = (x, offset) => x.map(row => weights.slice(offset, offset + c.width)
      .map((weight, j) => dot(row, weight) + biases[offset + j]));
    const q = project(query, 0), k = project(memory, c.width), v = project(memory, 2 * c.width), dim = c.width / c.heads;
    const result = q.map(row => Array.from({ length: c.heads }, (_, h) => {
      const start = h * dim, end = start + dim;
      const p = softmax(k.map(key => dot(row.slice(start, end), key.slice(start, end)) / Math.sqrt(dim)));
      return Array.from({ length: dim }, (_, j) => p.reduce((sum, prob, i) => sum + prob * v[i][start + j], 0));
    }).flat());
    return linear(result, `${name}.out_proj`);
  };
  const mean = (x, [start, end]) => x[0].map((_, i) => x.slice(start, end).reduce((sum, row) => sum + row[i], 0) / (end - start));
  const hidden = norm(rows(fixture.hidden, c.hidden_size), 'hidden_norm');
  const memory = linear(hidden, 'memory_projection'), global = hidden.at(-1);
  const questions = fields.map(field => mean(hidden, field.question_span));
  const contexts = fields.flatMap(field => field.options.map(option => mean(hidden, option.span)));
  const lexical = rows(fixture.lexical_options, c.hidden_size);
  const oq = linear(questions, 'option_question_projection');
  let options = add(linear(contexts, 'option_context_projection'), linear(lexical, 'option_lexical_projection'));
  options = add(options, fields.flatMap((field, i) => field.options.map(() => oq[i])));
  for (let i = 0; i < c.routing_layers; i++) {
    const p = `evidence_layers.${i}`;
    options = add(options, attention(norm(options, `${p}.query_norm`), norm(memory, `${p}.memory_norm`), `${p}.attention`));
    options = add(options, ff(norm(options, `${p}.feedforward_norm`), `${p}.feedforward.0`, `${p}.feedforward.3`));
  }
  const base = linear(questions, 'question_projection');
  let offset = 0;
  const summaries = fields.map((field, i) => {
    const opt = options.slice(offset, offset + field.options.length); offset += field.options.length;
    const p = softmax(opt.map(row => dot(row, base[i]) / Math.sqrt(c.width)));
    return base[i].map((_, j) => opt.reduce((sum, row, k) => sum + p[k] * row[j], 0));
  });
  const projectedGlobal = linear([global], 'global_projection')[0], types = matrix('type_embedding.weight');
  let fv = add(base, norm(summaries, 'option_summary_norm')).map((row, i) => row.map((v, j) => v + projectedGlobal[j] + types[{ noul: 0, choice: 1, score: 2 }[fields[i].type]][j]));
  for (let i = 0; i < c.layers; i++) {
    const p = `layers.${i}`, normalized = norm(fv, `${p}.norm1`);
    fv = add(fv, attention(normalized, normalized, `${p}.self_attn`));
    fv = add(fv, attention(norm(fv, `${p}.norm2`), memory, `${p}.multihead_attn`));
    fv = add(fv, ff(norm(fv, `${p}.norm3`), `${p}.linear1`, `${p}.linear2`));
  }
  fv = norm(fv, 'field_norm'); options = norm(options, 'option_norm');
  const cosine = (a, b, epsilon) => dot(a, b) / (Math.max(Math.sqrt(dot(a, a)), epsilon) * Math.max(Math.sqrt(dot(b, b)), epsilon));
  const scale = name => Math.exp(Math.min(t[name].data[0], Math.log(100)));
  const gate = 1 / (1 + Math.exp(-t.residual_gate.data[0]));
  offset = 0;
  return fields.flatMap((field, i) => field.options.map(() => {
    const oi = offset++, a = fv[i], b = options[oi], anchor = questions[i].map((v, j) => v + global[j]);
    const features = [...a, ...b, ...a.map((v, j) => v * b[j]), ...a.map((v, j) => Math.abs(v - b[j]))];
    const residual = ff([features], 'residual_scorer.0', 'residual_scorer.3')[0][0];
    return scale('prior_logit_scale') * cosine(lexical[oi], anchor, 1e-12) +
      gate * (scale('joint_logit_scale') * cosine(a, b, 1e-8) + residual);
  }));
}
