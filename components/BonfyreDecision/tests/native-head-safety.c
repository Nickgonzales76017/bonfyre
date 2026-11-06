/* Standalone sanitizer witness; no Bun/Node/Python required. */
#include "bf_decision_head.h"
#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>

static void exercise(BfDecisionHeadConfig c, size_t tokens,
                     size_t fields_count) {
  size_t options = fields_count * 2;
  size_t count = bf_decision_head_layout(&c, NULL, NULL);
  size_t need = bf_decision_head_workspace(&c, tokens, fields_count, options);
  assert(count && need);
  float *weights = calloc(count, sizeof(float));
  float *hidden = calloc(tokens * c.hidden_size, sizeof(float));
  float *lexical = calloc(options * c.hidden_size, sizeof(float));
  float *workspace = calloc(need, sizeof(float));
  float *logits = calloc(options, sizeof(float));
  BfDecisionField *fields = calloc(fields_count, sizeof(*fields));
  BfDecisionSpan *spans = calloc(options, sizeof(*spans));
  assert(weights && hidden && lexical && workspace && logits && fields &&
         spans);
  for (size_t i = 0; i < fields_count; i++)
    fields[i] = (BfDecisionField){0, {0, 1}, (uint32_t)(i * 2), 2};
  for (size_t i = 0; i < options; i++)
    spans[i] = (BfDecisionSpan){0, 1};
  assert(bf_decision_head_run(&c, weights, count, hidden, tokens, fields,
                              fields_count, spans, options, lexical, workspace,
                              need, logits) == 0);
  BfDecisionHeadModel *model = NULL;
  assert(bf_decision_head_model_create(&c, weights, count, &model) == 0 && model);
  assert(bf_decision_head_model_run(model, hidden, tokens, fields, fields_count,
                                    spans, options, lexical, workspace, need, logits) == 0);
  for (size_t i = 0; i < options; i++)
    assert(logits[i] == 0);
  logits[0] = 123;
  assert(bf_decision_head_run(&c, weights, count, hidden, tokens, fields,
                              fields_count, spans, options, lexical, workspace,
                              need - 1, logits) == -2);
  assert(logits[0] == 123);
  assert(bf_decision_head_model_run(model, hidden, tokens, fields, fields_count,
                                    spans, options, lexical, workspace, need - 1, logits) == -2);
  assert(logits[0] == 123);
  weights[count - 1] = NAN;
  assert(bf_decision_head_run(&c, weights, count, hidden, tokens, fields,
                              fields_count, spans, options, lexical, workspace,
                              need, logits) == -3);
  assert(logits[0] == 123);
  BfDecisionHeadModel *bad_model = NULL;
  assert(bf_decision_head_model_create(&c, weights, count, &bad_model) == -3);
  assert(!bad_model);
  // Caller mutations do not affect the copied immutable owner. The raw ABI
  // above still refuses the same caller buffer on every call.
  assert(bf_decision_head_model_run(model, hidden, tokens, fields, fields_count,
                                    spans, options, lexical, workspace, need, logits) == 0);
  assert(logits[0] == 0);
  hidden[0] = NAN; logits[0] = 123;
  assert(bf_decision_head_model_run(model, hidden, tokens, fields, fields_count,
                                    spans, options, lexical, workspace, need, logits) == -3);
  assert(logits[0] == 123);
  hidden[0] = 0;
  weights[count - 1] = 0;
  spans[0].end = (uint32_t)tokens + 1;
  assert(bf_decision_head_run(&c, weights, count, hidden, tokens, fields,
                              fields_count, spans, options, lexical, workspace,
                              need, logits) == -1);
  assert(logits[0] == 123);
  assert(bf_decision_head_model_run(model, hidden, tokens, fields, fields_count,
                                    spans, options, lexical, workspace, need, logits) == -1);
  assert(logits[0] == 123);
  assert(bf_decision_head_model_create(&c, weights, count - 1, &bad_model) == -1);
  assert(!bad_model);
  // Neither caller storage nor the caller's config survives into the owner.
  free(weights); weights = NULL;
  c = (BfDecisionHeadConfig){0};
  spans[0].end = 1;
  assert(bf_decision_head_model_run(model, hidden, tokens, fields, fields_count,
                                    spans, options, lexical, workspace, need, logits) == 0);
  bf_decision_head_model_destroy(model);
  free(spans);
  free(fields);
  free(logits);
  free(workspace);
  free(lexical);
  free(hidden);
  free(weights);
}
int main(void) {
  for (uint32_t depth = 0; depth < 3; depth++) {
    for (size_t tokens = 1; tokens < 10; tokens += 2) {
      for (size_t fields = 1; fields < 5; fields++) {
        exercise((BfDecisionHeadConfig){7, 4, 2, 11, depth, 2 - depth}, tokens,
                 fields);
        exercise((BfDecisionHeadConfig){64, 1, 1, 3, depth, depth}, tokens,
                 fields);
        exercise((BfDecisionHeadConfig){1, 6, 3, 2, depth, depth}, tokens,
                 fields);
      }
    }
  }
  BfDecisionHeadConfig bad = {UINT32_MAX, UINT32_MAX, 1, UINT32_MAX, 64, 64};
  assert(!bf_decision_head_layout(&bad, NULL, NULL));
  assert(!bf_decision_head_workspace(&bad, SIZE_MAX, SIZE_MAX, SIZE_MAX));
  for (size_t fields = 16; fields <= 33; fields = fields == 16 ? 17 : fields + 16)
    exercise((BfDecisionHeadConfig){7, 6, 3, 11, 1, 1}, 5, fields);
  BfDecisionHeadModel *invalid = NULL;
  assert(bf_decision_head_model_create(NULL, NULL, 0, &invalid) == -1 && !invalid);
  assert(bf_decision_head_model_create(NULL, NULL, 0, NULL) == -1);
  bf_decision_head_model_destroy(NULL);
  puts("native head: 183 raw/immutable arena/shape configurations passed");
  return 0;
}
