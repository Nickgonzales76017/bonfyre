// Machine-facing decision algebra. Probabilities are data, never permission.
// Clef emits per-field marginals despite its cross-field attention. Neither
// that joint computation nor Jev's independent evaluation implies independent
// events. Keep dependence-free bounds when composing judgments.
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const probability = value => typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 1;
const fail = message => { throw new Error(`typed decision: ${message}`); };

export async function runDecisionRecords(records, run, { concurrency, signal: parentSignal } = {}) {
  if (!Number.isSafeInteger(concurrency) || concurrency < 1) fail('invalid record concurrency');
  const controller = new AbortController();
  const signal = parentSignal ? AbortSignal.any([parentSignal, controller.signal]) : controller.signal;
  const results = Array.from({ length: records.length });
  let cursor = 0, failure;
  const worker = async () => {
    while (cursor < records.length && !signal.aborted) {
      const index = cursor++;
      try { results[index] = await run(records[index], index, signal); }
      catch (error) { failure ??= error; controller.abort(error); }
    }
  };
  await Promise.all(Array.from({ length: Math.min(concurrency, records.length) }, worker));
  if (failure) throw failure;
  signal.throwIfAborted();
  return results;
}

export function compileQuestionSchema(questions, { max_questions = 255, max_options = 255 } = {}) {
  if (!object(questions) || !Object.keys(questions).length || Object.keys(questions).length > max_questions)
    fail('question budget or empty schema');
  let offset = 0;
  const fields = Object.entries(questions).map(([id, question]) => {
    if (!id || !object(question)) fail('invalid question');
    let options;
    switch (question.type) {
      case 'noul': options = ['true', 'false']; break;
      case 'choice':
        if (!object(question.criteria)) fail(`${id}: choice requires criteria`);
        options = Object.keys(question.criteria);
        break;
      case 'score':
        if (!Array.isArray(question.criteria) || question.criteria.length < 2)
          fail(`${id}: score requires at least two ordered levels`);
        options = question.criteria.map((_, index) => String(index));
        break;
      default: fail(`${id}: unknown question type`);
    }
    if (!options.length || options.length > max_options || options.some(key => !key))
      fail(`${id}: option budget or empty option`);
    const field = Object.freeze({ id, type: question.type, offset, options: Object.freeze(options) });
    offset += options.length;
    return field;
  });
  return Object.freeze({ fields: Object.freeze(fields), width: offset });
}

// Return the same typed columns for local logits and hosted probabilities. No
// probabilities are silently repaired. The small tolerance is solely wire
// rounding (Clef rounds each component to four decimals), not model uncertainty.
export function decodeDecisionAnswers(schema, answers, { probability_decimals = 4 } = {}) {
  if (!Number.isInteger(probability_decimals) || probability_decimals < 1 || probability_decimals > 15)
    fail('invalid wire precision');
  if (!object(answers) || Object.keys(answers).length !== schema.fields.length)
    fail('answer shape does not match the request');
  const unit = 0.5 * 10 ** -probability_decimals;
  const columns = new Float64Array(schema.width), moments = Object.create(null);
  for (const field of schema.fields) {
    const answer = Object.hasOwn(answers, field.id) && answers[field.id];
    if (!answer || answer.type !== field.type) fail(`${field.id}: wrong answer type`);
    let values;
    if (field.type === 'noul') {
      if (!probability(answer.noul)) fail(`${field.id}: invalid noul`);
      values = [answer.noul, 1 - answer.noul];
    } else {
      const probs = answer.probabilities;
      if (!object(probs) || Object.keys(probs).length !== field.options.length ||
          field.options.some(key => !Object.hasOwn(probs, key) || !probability(probs[key])))
        fail(`${field.id}: invalid probability distribution`);
      values = field.options.map(key => probs[key]);
      if (!probability(answer.confidence)) fail(`${field.id}: invalid confidence`);
    }
    const mass = values.reduce((sum, p) => sum + p, 0);
    if (Math.abs(mass - 1) > unit * values.length + 1e-12)
      fail(`${field.id}: invalid probability distribution; probabilities do not sum to one`);
    const peak = Math.max(...values), winner = values.indexOf(peak);
    const mean = values.reduce((sum, p, index) => sum + p * index, 0);
    if (field.type === 'choice' && (!field.options.includes(answer.choice) ||
        peak - values[field.options.indexOf(answer.choice)] > 2 * unit + 1e-12))
      fail(`${field.id}: choice is not a maximum-probability option`);
    // Expectation uncertainty includes rounding both the levels and the score.
    if (field.type === 'score' && (typeof answer.score !== 'number' || !Number.isFinite(answer.score) ||
        answer.score < 0 || answer.score > values.length - 1 ||
        Math.abs(answer.score - mean) > unit * (1 + values.length * (values.length - 1) / 2) + 1e-12))
      fail(`${field.id}: score disagrees with its probability-weighted expectation`);
    columns.set(values, field.offset);
    const descending = [...values].sort((a, b) => b - a);
    // TypeSafe's public distribution statistics. These are concentration
    // measures, not calibration or a probability of correctness. Keep them
    // separate from Clef's peak-probability confidence and the raw vendor field.
    const concentration = values.length === 1 ? 1
      : Math.max(0, (peak - 1 / values.length) / (1 - 1 / values.length));
    const evenSpread = values.reduce((sum, _, i) => sum + Math.abs(i - (values.length - 1) / 2), 0) / values.length;
    const ordinalConcentration = field.type === 'score'
      ? Math.max(0, 1 - values.reduce((sum, p, i) => sum + p * Math.abs(i - winner), 0) / evenSpread) : null;
    moments[field.id] = Object.freeze({ mass, maximum: peak,
      winner: field.type === 'choice' ? answer.choice : field.options[winner],
      margin: peak - (descending[1] || 0), expectation: mean,
      entropy: -values.reduce((sum, p) => sum + (p ? p * Math.log(p) : 0), 0),
      concentration, ordinal_concentration: ordinalConcentration,
      provider_confidence: field.type === 'noul' ? null : answer.confidence });
  }
  return { schema, columns, moments };
}

// This is the head/output boundary, NOT a substitute for Clef's learned
// backbone, evidence-routing layers, lexical prior, or joint transformer.
export function answersFromLogits(schema, logits) {
  if (!(logits instanceof Float32Array || logits instanceof Float64Array) || logits.length !== schema.width)
    fail('logit columns do not match schema');
  const answers = Object.create(null);
  for (const field of schema.fields) {
    const row = logits.subarray(field.offset, field.offset + field.options.length);
    if ([...row].some(x => !Number.isFinite(x))) fail('nonfinite logit');
    const max = Math.max(...row), exp = [...row].map(x => Math.exp(x - max));
    const sum = exp.reduce((a, b) => a + b, 0), p = exp.map(x => x / sum);
    const winner = p.indexOf(Math.max(...p));
    if (field.type === 'noul') answers[field.id] = { type: 'noul', noul: p[0] };
    else {
      const value = { type: field.type, probabilities: Object.fromEntries(field.options.map((key, i) => [key, p[i]])),
        confidence: p[winner] };
      if (field.type === 'choice') value.choice = field.options[winner];
      else value.score = p.reduce((total, x, i) => total + x * i, 0);
      answers[field.id] = value;
    }
  }
  return answers;
}

export function conjunctionBounds(probabilities) {
  if (!Array.isArray(probabilities) || !probabilities.length || probabilities.some(p => !probability(p)))
    fail('conjunction needs finite marginal probabilities');
  return { lower: Math.max(0, probabilities.reduce((a, b) => a + b, 0) - probabilities.length + 1),
    upper: Math.min(...probabilities) };
}

// Proper losses on the complete marginal, not vendor confidence. The Brier
// convention here is sum_k (p_k-y_k)^2 (range 0..2); ordinal RPS is normalized
// by K-1. Zero mass on the observed outcome is an infinite log loss, not epsilon
// clipping; represent it explicitly so JSON cannot silently turn Infinity null.
export function scoreDecisionLabels(decoded, labels) {
  if (!object(labels) || Object.keys(labels).length !== decoded.schema.fields.length)
    fail('labels must cover every question');
  return Object.fromEntries(decoded.schema.fields.map(field => {
    const label = Object.hasOwn(labels, field.id) && labels[field.id];
    if (typeof label !== 'string' || !field.options.includes(label))
      fail(`${field.id}: label must name an allowed option`);
    const observed = field.options.indexOf(label);
    const p = decoded.columns.subarray(field.offset, field.offset + field.options.length);
    let brier = 0, cumulative = 0, ranked = 0;
    for (let k = 0; k < p.length; k++) {
      brier += (p[k] - Number(k === observed)) ** 2;
      cumulative += p[k];
      if (k < p.length - 1) ranked += (cumulative - Number(observed <= k)) ** 2;
    }
    return [field.id, { observed: label, correct: decoded.moments[field.id].winner === label,
      observed_probability: p[observed], brier,
      log_loss: p[observed] ? -Math.log(p[observed]) : null,
      infinite_log_loss: p[observed] === 0,
      ranked_probability_score: field.type === 'score' ? ranked / (p.length - 1) : null }];
  }));
}

// One-sided Clopper-Pearson bound: invert P[Binomial(n,p) <= errors] = alpha.
// Log-space recurrence avoids factorial overflow and probability underflow.
// Statistical coverage assumes an independent held-out sample and a fixed
// selection policy; distinct hashes alone do not establish either assumption.
export function binomialRiskUpper(errors, trials, alpha = 0.05) {
  if (!Number.isSafeInteger(trials) || trials < 0 || trials > 100000 ||
      !Number.isSafeInteger(errors) || errors < 0 || errors > trials ||
      !Number.isFinite(alpha) || alpha <= 0 || alpha >= 1)
    fail('invalid binomial risk counts or alpha');
  if (!trials || errors === trials) return 1;
  if (!errors) return -Math.expm1(Math.log(alpha) / trials);
  const target = Math.log(alpha);
  let lower = errors / trials, upper = 1;
  for (let iteration = 0; iteration < 60; iteration++) {
    const p = (lower + upper) / 2;
    let term = trials * Math.log1p(-p), sum = term;
    for (let k = 1; k <= errors; k++) {
      term += Math.log(trials - k + 1) - Math.log(k) + Math.log(p) - Math.log1p(-p);
      const high = Math.max(sum, term);
      sum = high + Math.log1p(Math.exp(Math.min(sum, term) - high));
    }
    if (sum > target) lower = p;
    else upper = p;
  }
  return upper;
}

// Compile authored projections once into column references. Adding an owner
// requires descriptors, not another program-name branch in the runtime.
export function compileDecisionProjection(schema, outputs = {}) {
  const fields = new Map(schema.fields.map(field => [field.id, field]));
  return Object.entries(outputs).map(([name, spec]) => {
    const references = spec.questions || (spec.question ? [spec.question] : []);
    const selected = references.map(id => fields.get(id));
    if (!selected.length || selected.some(field => !field)) fail(`${name}: unknown question reference`);
    const stat = spec.stat;
    if (['all_lower', 'all_upper'].includes(stat)) {
      if (selected.some(field => field.type !== 'noul') || new Set(references).size !== references.length)
        fail(`${name}: conjunction needs distinct noul questions`);
    } else {
      if (selected.length !== 1 || !['probability', 'winner', 'distribution', 'expectation'].includes(stat))
        fail(`${name}: invalid projection`);
      if (stat === 'expectation' && selected[0].type !== 'score') fail(`${name}: expectation needs score`);
      if (stat === 'probability' && !selected[0].options.includes(spec.option)) fail(`${name}: unknown option`);
    }
    return Object.freeze({ name, stat, fields: selected, option: spec.option });
  });
}

export function executeDecisionProjection(projection, decoded) {
  return Object.fromEntries(projection.map(({ name, stat, fields, option }) => {
    const field = fields[0], values = decoded.columns.subarray(field.offset, field.offset + field.options.length);
    let value;
    switch (stat) {
      case 'probability': value = values[field.options.indexOf(option)]; break;
      case 'winner': value = decoded.moments[field.id].winner; break;
      case 'distribution': value = Object.fromEntries(field.options.map((key, i) => [key, values[i]])); break;
      case 'expectation': value = decoded.moments[field.id].expectation; break;
      case 'all_lower': case 'all_upper':
        value = conjunctionBounds(fields.map(f => decoded.columns[f.offset]))[stat === 'all_lower' ? 'lower' : 'upper'];
        break;
      default: fail('uncompiled projection');
    }
    return [name, value];
  }));
}

// Bayes risk over one categorical marginal. Asymmetric costs are supplied by
// the semantic owner; abstention is an explicit action with its own cost.
// A selected action is a proposal only, and does not cross the effect boundary.
export function minimumExpectedLoss(decoded, questionId, losses) {
  const field = decoded.schema.fields.find(item => item.id === questionId);
  if (!field || !object(losses) || !Object.keys(losses).length) fail('loss program requires a question and actions');
  const risks = Object.create(null);
  for (const [action, row] of Object.entries(losses)) {
    if (!object(row) || Object.keys(row).length !== field.options.length || field.options.some(key =>
      !Object.hasOwn(row, key) || !Number.isFinite(row[key]) || row[key] < 0)) fail('loss row must cover every outcome');
    risks[action] = field.options.reduce((risk, key, i) => risk + decoded.columns[field.offset + i] * row[key], 0);
    if (!Number.isFinite(risks[action])) fail('loss overflow');
  }
  const minimum = Math.min(...Object.values(risks));
  return { risks, minimizing_actions: Object.keys(risks).filter(action => risks[action] === minimum),
    minimum_expected_loss: minimum, action: 'none' };
}

export function compileDecisionSelection(schema, selection) {
  const { question, abstain, error_cost, review_cost, review_question, maximum_review_probability } = selection;
  const field = schema.fields.find(item => item.id === question);
  const reviewField = schema.fields.find(item => item.id === review_question);
  if (!field || field.type !== 'choice' || !field.options.includes(abstain) ||
      !reviewField || reviewField.type !== 'noul' || !probability(maximum_review_probability) ||
      !Number.isFinite(error_cost) || error_cost <= 0 || !Number.isFinite(review_cost) ||
      review_cost < 0 || review_cost >= error_cost) fail('invalid authored selection costs or review question');
  const losses = Object.fromEntries(field.options.map(action => [action,
    Object.fromEntries(field.options.map(outcome => [outcome,
      action === abstain ? review_cost : action === outcome ? 0 : error_cost]))]));
  return { question, abstain, losses, review_offset: reviewField.offset, maximum_review_probability };
}

export function selectDecisionCandidate(decoded, selection) {
  const { question, abstain, losses, review_offset, maximum_review_probability } = selection;
  const evaluation = minimumExpectedLoss(decoded, question, losses);
  const review = decoded.columns[review_offset] >= maximum_review_probability;
  const unique = evaluation.minimizing_actions.length === 1;
  const candidate = !review && unique ? evaluation.minimizing_actions[0] : abstain;
  return { candidate, disposition: candidate === abstain ? 'review' : 'candidate',
    reason: review ? 'review_probability' : !unique ? 'risk_tie' : candidate === abstain ? 'abstention_cost' : 'minimum_expected_loss',
    ...evaluation };
}
