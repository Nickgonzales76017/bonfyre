/* SPDX-License-Identifier: Apache-2.0
 * Algorithm port of Cloudflare Clef JointSchemaHead
 * (17f0b0ad64efb65d273590632833508766b2aae6). Changed: F32 native ABI,
 * caller-owned arena, streaming attention softmax, explicit shape/error
 * contracts. Dropout is disabled (inference only).
 */
#include "bf_decision_head.h"
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef BF_DECISION_ACCELERATE
#include <Accelerate/Accelerate.h>
#endif

/* Bounded packing tile, not a limit on the number of schema options. */
enum { RESIDUAL_BATCH = 32 };
struct BfDecisionHeadModel {
  BfDecisionHeadConfig config;
  size_t count;
  float weights[];
};

static int valid(const BfDecisionHeadConfig *c) {
  return c && c->hidden_size && c->width && c->heads && c->feedforward &&
         c->width % c->heads == 0 && c->hidden_size <= INT_MAX &&
         c->width <= INT_MAX / 4 && c->feedforward <= INT_MAX &&
         c->routing_layers <= 64 && c->layers <= 64;
}
static int add_product(size_t *n, size_t a, size_t b) {
  if (a && b > (SIZE_MAX - *n) / a)
    return 0;
  *n += a * b;
  return *n <= SIZE_MAX / sizeof(float);
}
typedef struct {
  size_t count;
  int ok;
  BfDecisionWeightVisitor visit;
  void *ctx;
} Layout;
static void tensor(Layout *l, const char *prefix, const char *key, size_t rows,
                   size_t cols, size_t rank) {
  char name[160];
  int n = snprintf(name, sizeof(name), "%s%s", prefix, key);
  if (!l->ok || n < 0 || (size_t)n >= sizeof(name)) {
    l->ok = 0;
    return;
  }
  size_t offset = l->count;
  if (!add_product(&l->count, rows, cols) ||
      (l->visit && !l->visit(l->ctx, name, rows, cols, rank, offset)))
    l->ok = 0;
}
static void weight(Layout *l, const char *p, const char *key, size_t rows,
                   size_t cols) {
  tensor(l, p, key, rows, cols, 2);
}
static void vector(Layout *l, const char *p, const char *key, size_t n) {
  tensor(l, p, key, n, 1, 1);
}
static void scalar(Layout *l, const char *key) { tensor(l, "", key, 1, 1, 0); }
static void norm_layout(Layout *l, const char *prefix, const char *key,
                        size_t w) {
  char p[128];
  int n = snprintf(p, sizeof(p), "%s%s.", prefix, key);
  if (n < 0 || (size_t)n >= sizeof(p)) {
    l->ok = 0;
    return;
  }
  vector(l, p, "weight", w);
  vector(l, p, "bias", w);
}
static void attention_layout(Layout *l, const char *p, const char *key,
                             size_t w) {
  char s[128];
  int n = snprintf(s, sizeof(s), "%s%s.", p, key);
  if (n < 0 || (size_t)n >= sizeof(s)) {
    l->ok = 0;
    return;
  }
  weight(l, s, "in_proj_weight", 3 * w, w);
  vector(l, s, "in_proj_bias", 3 * w);
  weight(l, s, "out_proj.weight", w, w);
  vector(l, s, "out_proj.bias", w);
}
size_t bf_decision_head_layout(const BfDecisionHeadConfig *c,
                               BfDecisionWeightVisitor visit, void *ctx) {
  if (!valid(c))
    return 0;
  Layout l = {0, 1, visit, ctx};
  size_t h = c->hidden_size, w = c->width, f = c->feedforward;
  norm_layout(&l, "", "hidden_norm", h);
  const char *projections[] = {
      "memory_projection.weight",          "question_projection.weight",
      "option_question_projection.weight", "global_projection.weight",
      "option_context_projection.weight",  "option_lexical_projection.weight"};
  for (size_t i = 0; i < sizeof(projections) / sizeof(projections[0]); i++)
    weight(&l, "", projections[i], w, h);
  weight(&l, "", "type_embedding.weight", 3, w);
  for (uint32_t i = 0; i < c->routing_layers; i++) {
    char p[64];
    snprintf(p, sizeof(p), "evidence_layers.%u.", i);
    norm_layout(&l, p, "query_norm", w);
    norm_layout(&l, p, "memory_norm", w);
    attention_layout(&l, p, "attention", w);
    norm_layout(&l, p, "feedforward_norm", w);
    weight(&l, p, "feedforward.0.weight", f, w);
    vector(&l, p, "feedforward.0.bias", f);
    weight(&l, p, "feedforward.3.weight", w, f);
    vector(&l, p, "feedforward.3.bias", w);
  }
  norm_layout(&l, "", "option_summary_norm", w);
  for (uint32_t i = 0; i < c->layers; i++) {
    char p[64];
    snprintf(p, sizeof(p), "layers.%u.", i);
    // Evaluation order, not state_dict iteration order. Names bind exactly.
    norm_layout(&l, p, "norm1", w);
    attention_layout(&l, p, "self_attn", w);
    norm_layout(&l, p, "norm2", w);
    attention_layout(&l, p, "multihead_attn", w);
    norm_layout(&l, p, "norm3", w);
    weight(&l, p, "linear1.weight", f, w);
    vector(&l, p, "linear1.bias", f);
    weight(&l, p, "linear2.weight", w, f);
    vector(&l, p, "linear2.bias", w);
  }
  norm_layout(&l, "", "field_norm", w);
  norm_layout(&l, "", "option_norm", w);
  weight(&l, "", "residual_scorer.0.weight", w, 4 * w);
  vector(&l, "", "residual_scorer.0.bias", w);
  weight(&l, "", "residual_scorer.3.weight", 1, w);
  vector(&l, "", "residual_scorer.3.bias", 1);
  scalar(&l, "prior_logit_scale");
  scalar(&l, "joint_logit_scale");
  scalar(&l, "residual_gate");
  return l.ok ? l.count : 0;
}
size_t bf_decision_head_workspace(const BfDecisionHeadConfig *c, size_t s,
                                  size_t q, size_t o) {
  if (!valid(c) || !s || !q || !o || s > UINT32_MAX || q > UINT32_MAX ||
      o > UINT32_MAX || q > o)
    return 0;
  size_t n = 0, w = c->width;
  // Persistent matrices plus the largest live layer scratch region. Layers
  // reuse scratch, so depth does not increase residency. All sums checked.
  if (!add_product(&n, s, c->hidden_size) ||
      !add_product(&n, q, c->hidden_size) ||
      !add_product(&n, o, c->hidden_size) || !add_product(&n, s, w) ||
      !add_product(&n, o, 2 * w) || !add_product(&n, q, 3 * w) ||
      !add_product(&n, 1, w) || !add_product(&n, o, 1))
    return 0;
  size_t scratch = 0;
  if (!add_product(&scratch, o < RESIDUAL_BATCH ? o : RESIDUAL_BATCH, 5 * w))
    return 0;
  if (c->routing_layers) {
    size_t attention = 0, ff = 0;
    if (!add_product(&attention, o, 4 * w) ||
        !add_product(&attention, s, 3 * w) || !add_product(&ff, o, 2 * w) ||
        !add_product(&ff, s, w) || !add_product(&ff, o, c->feedforward))
      return 0;
    if (attention > scratch)
      scratch = attention;
    if (ff > scratch)
      scratch = ff;
  }
  if (c->layers) {
    size_t self = 0, cross = 0, ff = 0;
    if (!add_product(&self, q, 6 * w) || !add_product(&cross, q, 4 * w) ||
        !add_product(&cross, s, 2 * w) || !add_product(&ff, q, 2 * w) ||
        !add_product(&ff, q, c->feedforward))
      return 0;
    if (self > scratch)
      scratch = self;
    if (cross > scratch)
      scratch = cross;
    if (ff > scratch)
      scratch = ff;
  }
  if (!add_product(&n, 1, scratch))
    return 0;
  return n;
}
typedef struct {
  float *p;
  size_t used, cap;
} Arena;
static float *take(Arena *a, size_t rows, size_t cols) {
  size_t end = a->used;
  if (!add_product(&end, rows, cols) || end > a->cap)
    return NULL;
  float *p = a->p + a->used;
  a->used = end;
  return p;
}
static const float *consume(const float **p, size_t n) {
  const float *r = *p;
  *p += n;
  return r;
}
static void linear(float *y, const float *x, const float *weight,
                   const float *bias, size_t n, size_t in, size_t out) {
#ifdef BF_DECISION_ACCELERATE
  cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasTrans, (int)n, (int)out,
              (int)in, 1, x, (int)in, weight, (int)in, 0, y, (int)out);
  if (bias)
    for (size_t r = 0; r < n; r++)
      for (size_t j = 0; j < out; j++)
        y[r * out + j] += bias[j];
#else
  for (size_t r = 0; r < n; r++)
    for (size_t j = 0; j < out; j++) {
      double v = bias ? bias[j] : 0;
      for (size_t k = 0; k < in; k++)
        v += (double)x[r * in + k] * weight[j * in + k];
      y[r * out + j] = (float)v;
    }
#endif
}
static void norm(float *y, const float *x, size_t n, size_t w,
                 const float **p) {
  const float *scale = consume(p, w), *bias = consume(p, w);
  for (size_t r = 0; r < n; r++) {
    double mean = 0, var = 0;
    for (size_t j = 0; j < w; j++)
      mean += x[r * w + j];
    mean /= w;
    for (size_t j = 0; j < w; j++) {
      double d = x[r * w + j] - mean;
      var += d * d;
    }
    double inv = 1 / sqrt(var / w + 1e-5);
    for (size_t j = 0; j < w; j++)
      y[r * w + j] = (float)((x[r * w + j] - mean) * inv * scale[j] + bias[j]);
  }
}
static void plus(float *x, const float *y, size_t n) {
  for (size_t i = 0; i < n; i++)
    x[i] += y[i];
}
static void means(float *out, const float *x, BfDecisionSpan span, size_t w) {
  for (size_t j = 0; j < w; j++) {
    double v = 0;
    for (size_t t = span.start; t < span.end; t++)
      v += x[t * w + j];
    out[j] = (float)(v / (span.end - span.start));
  }
}
static int finite_values(const float *p, size_t n) {
  for (size_t i = 0; i < n; i++)
    if (!isfinite(p[i]))
      return 0;
  return 1;
}
static double dot(const float *a, const float *b, size_t n) {
  double v = 0;
  for (size_t i = 0; i < n; i++)
    v += (double)a[i] * b[i];
  return v;
}
static double cosine(const float *a, const float *b, size_t n, double eps) {
  return dot(a, b, n) /
         (fmax(sqrt(dot(a, a, n)), eps) * fmax(sqrt(dot(b, b, n)), eps));
}
static int attention(float *out, const float *query, const float *memory,
                     size_t nq, size_t nm, size_t w, size_t heads,
                     const float **p, Arena *a) {
  const float *iw = consume(p, 3 * w * w), *ib = consume(p, 3 * w);
  const float *ow = consume(p, w * w), *ob = consume(p, w);
  size_t mark = a->used, d = w / heads;
  float *q = take(a, nq, w), *k = take(a, nm, w), *v = take(a, nm, w),
        *r = take(a, nq, w);
  if (!q || !k || !v || !r)
    return 0;
  linear(q, query, iw, ib, nq, w, w);
  linear(k, memory, iw + w * w, ib + w, nm, w, w);
  linear(v, memory, iw + 2 * w * w, ib + 2 * w, nm, w, w);
  memset(r, 0, nq * w * sizeof(float));
  // Online softmax: O((nq+nm)*width) storage, never nq*nm attention scores.
  for (size_t row = 0; row < nq; row++)
    for (size_t h = 0; h < heads; h++) {
      double max = -INFINITY, sum = 0;
      float *target = r + row * w + h * d;
      for (size_t col = 0; col < nm; col++) {
        double score =
            dot(q + row * w + h * d, k + col * w + h * d, d) / sqrt((double)d);
        double next = fmax(max, score), old = exp(max - next),
               now = exp(score - next);
        for (size_t j = 0; j < d; j++)
          target[j] = (float)(target[j] * old + v[col * w + h * d + j] * now);
        sum = sum * old + now;
        max = next;
      }
      for (size_t j = 0; j < d; j++)
        target[j] = (float)(target[j] / sum);
    }
  linear(out, r, ow, ob, nq, w, w);
  a->used = mark;
  return 1;
}
static int feedforward(float *out, const float *x, size_t n, size_t w, size_t f,
                       const float **p, Arena *a) {
  const float *w1 = consume(p, f * w), *b1 = consume(p, f),
              *w2 = consume(p, w * f), *b2 = consume(p, w);
  // Unlike projection layers, both feed-forward Linears require biases.
  if (!w1 || !b1 || !w2 || !b2)
    return 0;
  size_t mark = a->used;
  float *middle = take(a, n, f);
  if (!middle)
    return 0;
  linear(middle, x, w1, b1, n, w, f);
  for (size_t i = 0; i < n * f; i++)
    middle[i] = (float)(0.5 * middle[i] * (1 + erf(middle[i] / sqrt(2.0))));
  linear(out, middle, w2, b2, n, f, w);
  a->used = mark;
  return 1;
}
static int validate_request(const BfDecisionHeadConfig *c, const float *weights,
                         size_t count, const float *hidden, size_t s,
                         const BfDecisionField *fields, size_t q,
                         const BfDecisionSpan *spans, size_t o,
                         const float *lexical, float *workspace,
                         size_t capacity, float *logits) {
  size_t required = bf_decision_head_workspace(c, s, q, o),
         expected = bf_decision_head_layout(c, NULL, NULL);
  if (!required || !expected || count != expected || !weights || !hidden ||
      !fields || !spans || !lexical || !workspace || !logits || s > INT_MAX ||
      q > INT_MAX || o > INT_MAX)
    return -1;
  if (capacity < required)
    return -2;
  size_t covered = 0, h = c->hidden_size;
  for (size_t i = 0; i < q; i++) {
    if (fields[i].type > 2 ||
        (fields[i].type == 0 && fields[i].option_count != 2) ||
        (fields[i].type == 2 && fields[i].option_count < 2) ||
        fields[i].question.start >= fields[i].question.end ||
        fields[i].question.end > s || fields[i].option_offset != covered ||
        !fields[i].option_count || fields[i].option_count > o - covered)
      return -1;
    covered += fields[i].option_count;
  }
  if (covered != o)
    return -1;
  for (size_t i = 0; i < o; i++)
    if (spans[i].start >= spans[i].end || spans[i].end > s)
      return -1;
  if (!finite_values(hidden, s * h) || !finite_values(lexical, o * h))
    return -3;
  return 0;
}
/* Private executor: callable only after the public entrypoint has validated
 * the request and either checked raw weights or selected its private owner.
 * There is no caller-selectable trust flag or mutable weight attachment API.
 */
static int run_head(const BfDecisionHeadConfig *c, const float *weights,
                    size_t count, const float *hidden, size_t s,
                    const BfDecisionField *fields, size_t q,
                    const BfDecisionSpan *spans, size_t o,
                    const float *lexical, float *workspace,
                    size_t capacity, float *logits) {
  size_t h = c->hidden_size, w = c->width, f = c->feedforward;
  Arena a = {workspace, 0, capacity};
  const float *p = weights;
  float *normal = take(&a, s, h), *memory = take(&a, s, w),
        *questions = take(&a, q, h), *contexts = take(&a, o, h);
  float *routed = take(&a, o, w), *base = take(&a, q, w),
        *field_values = take(&a, q, w), *global = take(&a, 1, w);
  float *temp = take(&a, o, w), *oq = take(&a, q, w), *scores = take(&a, o, 1);
  if (!normal || !memory || !questions || !contexts || !routed || !base ||
      !field_values || !global || !temp || !oq || !scores)
    return -2;
  norm(normal, hidden, s, h, &p);
  linear(memory, normal, consume(&p, w * h), NULL, s, h, w);
  for (size_t i = 0; i < q; i++)
    means(questions + i * h, normal, fields[i].question, h);
  for (size_t i = 0; i < o; i++)
    means(contexts + i * h, normal, spans[i], h);
  linear(base, questions, consume(&p, w * h), NULL, q, h, w);
  linear(oq, questions, consume(&p, w * h), NULL, q, h, w);
  linear(global, normal + (s - 1) * h, consume(&p, w * h), NULL, 1, h, w);
  linear(routed, contexts, consume(&p, w * h), NULL, o, h, w);
  linear(temp, lexical, consume(&p, w * h), NULL, o, h, w);
  plus(routed, temp, o * w);
  const float *types = consume(&p, 3 * w);
  for (size_t i = 0; i < q; i++)
    for (size_t j = 0; j < fields[i].option_count; j++)
      plus(routed + (fields[i].option_offset + j) * w, oq + i * w, w);
  size_t persistent = a.used;
  for (uint32_t i = 0; i < c->routing_layers; i++) {
    float *nq = take(&a, o, w), *nm = take(&a, s, w), *output = take(&a, o, w);
    if (!nq || !nm || !output)
      return -2;
    norm(nq, routed, o, w, &p);
    norm(nm, memory, s, w, &p);
    if (!attention(output, nq, nm, o, s, w, c->heads, &p, &a))
      return -2;
    plus(routed, output, o * w);
    norm(nq, routed, o, w, &p);
    if (!feedforward(output, nq, o, w, f, &p, &a))
      return -2;
    plus(routed, output, o * w);
    a.used = persistent;
  }
  for (size_t i = 0; i < q; i++) {
    const BfDecisionField *field = fields + i;
    double max = -INFINITY, sum = 0;
    for (size_t j = 0; j < field->option_count; j++) {
      size_t oi = field->option_offset + j;
      scores[oi] =
          (float)(dot(routed + oi * w, base + i * w, w) / sqrt((double)w));
      max = fmax(max, scores[oi]);
    }
    for (size_t j = 0; j < field->option_count; j++) {
      size_t oi = field->option_offset + j;
      scores[oi] = (float)exp(scores[oi] - max);
      sum += scores[oi];
    }
    for (size_t k = 0; k < w; k++) {
      double v = 0;
      for (size_t j = 0; j < field->option_count; j++) {
        size_t oi = field->option_offset + j;
        v += scores[oi] / sum * routed[oi * w + k];
      }
      field_values[i * w + k] = (float)v;
    }
  }
  norm(field_values, field_values, q, w, &p);
  plus(field_values, base, q * w);
  for (size_t i = 0; i < q; i++) {
    plus(field_values + i * w, global, w);
    plus(field_values + i * w, types + fields[i].type * w, w);
  }
  for (uint32_t i = 0; i < c->layers; i++) {
    float *nq = take(&a, q, w), *output = take(&a, q, w);
    if (!nq || !output)
      return -2;
    norm(nq, field_values, q, w, &p);
    if (!attention(output, nq, nq, q, q, w, c->heads, &p, &a))
      return -2;
    plus(field_values, output, q * w);
    norm(nq, field_values, q, w, &p);
    if (!attention(output, nq, memory, q, s, w, c->heads, &p, &a))
      return -2;
    plus(field_values, output, q * w);
    norm(nq, field_values, q, w, &p);
    if (!feedforward(output, nq, q, w, f, &p, &a))
      return -2;
    plus(field_values, output, q * w);
    a.used = persistent;
  }
  norm(field_values, field_values, q, w, &p);
  norm(temp, routed, o, w, &p);
  const float *rw1 = consume(&p, 4 * w * w), *rb1 = consume(&p, w),
              *rw2 = consume(&p, w), *rb2 = consume(&p, 1);
  double prior_scale = exp(fmin(*consume(&p, 1), log(100.0))),
         joint_scale = exp(fmin(*consume(&p, 1), log(100.0)));
  double gate = 1 / (1 + exp(-(double)*consume(&p, 1)));
  size_t batch = o < RESIDUAL_BATCH ? o : RESIDUAL_BATCH;
  float *features = take(&a, batch, 4 * w), *residual = take(&a, batch, w);
  if (!features || !residual)
    return -2;
  // The pooled question vectors are dead after decoder execution. Reuse them
  // as lexical anchors, preserving the original F32 sum and normalization.
  for (size_t i = 0; i < q; i++)
    for (size_t k = 0; k < h; k++)
      questions[i * h + k] += normal[(s - 1) * h + k];
  size_t field = 0;
  for (size_t start = 0; start < o; start += batch) {
    size_t rows = o - start < batch ? o - start : batch, first_field = field;
    for (size_t row = 0; row < rows; row++) {
      size_t oi = start + row;
      while (oi >= fields[field].option_offset + fields[field].option_count)
        field++;
      const float *fv = field_values + field * w, *ov = temp + oi * w;
      float *feature = features + row * 4 * w;
      for (size_t k = 0; k < w; k++) {
        feature[k] = fv[k];
        feature[w + k] = ov[k];
        feature[2 * w + k] = fv[k] * ov[k];
        feature[3 * w + k] = fabsf(fv[k] - ov[k]);
      }
    }
    // Pack across field boundaries: one SGEMM per bounded tile, not an SGEMV
    // (and reread of the residual weight matrix) for every option.
    linear(residual, features, rw1, rb1, rows, 4 * w, w);
    for (size_t k = 0; k < rows * w; k++)
      residual[k] = (float)(0.5 * residual[k] * (1 + erf(residual[k] / sqrt(2.0))));
    field = first_field;
    for (size_t row = 0; row < rows; row++) {
      size_t oi = start + row;
      while (oi >= fields[field].option_offset + fields[field].option_count)
        field++;
      const float *fv = field_values + field * w, *ov = temp + oi * w;
      double r = dot(residual + row * w, rw2, w) + *rb2;
      scores[oi] =
          (float)(prior_scale * cosine(lexical + oi * h, questions + field * h, h, 1e-12) +
                  gate * (joint_scale * cosine(fv, ov, w, 1e-8) + r));
    }
  }
  if ((size_t)(p - weights) != count)
    return -1;
  if (!finite_values(scores, o))
    return -3;
  memcpy(logits, scores, o * sizeof(float));
  return 0;
}

int bf_decision_head_run(const BfDecisionHeadConfig *c, const float *weights,
                         size_t count, const float *hidden, size_t s,
                         const BfDecisionField *fields, size_t q,
                         const BfDecisionSpan *spans, size_t o,
                         const float *lexical, float *workspace,
                         size_t capacity, float *logits) {
  int status = validate_request(c, weights, count, hidden, s, fields, q, spans,
                                 o, lexical, workspace, capacity, logits);
  if (status)
    return status;
  if (!finite_values(weights, count))
    return -3;
  return run_head(c, weights, count, hidden, s, fields, q, spans, o, lexical,
                  workspace, capacity, logits);
}

int bf_decision_head_model_create(const BfDecisionHeadConfig *c, const float *weights,
                                  size_t count, BfDecisionHeadModel **result) {
  if (!result)
    return -1;
  *result = NULL;
  size_t expected = bf_decision_head_layout(c, NULL, NULL);
  if (!expected || !weights || count != expected ||
      count > (SIZE_MAX - sizeof(BfDecisionHeadModel)) / sizeof(float))
    return -1;
  BfDecisionHeadModel *model = malloc(sizeof(*model) + count * sizeof(float));
  if (!model)
    return -4;
  model->config = *c;
  model->count = count;
  memcpy(model->weights, weights, count * sizeof(float));
  // Validate the copied snapshot itself, not a caller buffer that could change
  // between validation and copying. No mutable snapshot pointer is exported.
  if (!finite_values(model->weights, count)) {
    free(model);
    return -3;
  }
  *result = model;
  return 0;
}

void bf_decision_head_model_destroy(BfDecisionHeadModel *model) { free(model); }

int bf_decision_head_model_run(const BfDecisionHeadModel *model, const float *hidden,
                               size_t s, const BfDecisionField *fields, size_t q,
                               const BfDecisionSpan *spans, size_t o,
                               const float *lexical, float *workspace,
                               size_t capacity, float *logits) {
  if (!model)
    return -1;
  int status = validate_request(&model->config, model->weights, model->count,
                                 hidden, s, fields, q, spans, o, lexical,
                                 workspace, capacity, logits);
  if (status)
    return status;
  return run_head(&model->config, model->weights, model->count, hidden, s, fields,
                  q, spans, o, lexical, workspace, capacity, logits);
}
