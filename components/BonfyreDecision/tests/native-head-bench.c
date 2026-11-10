/* Synthetic native-head microbenchmark, NOT trained inference or end-to-end
 * application performance. Compile before/after with identical flags. Define
 * BF_DECISION_BENCH_OWNED to measure the immutable C owner once available.
 */
#define _POSIX_C_SOURCE 200809L
#include "bf_decision_head.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static double milliseconds(void) {
  struct timespec t;
  assert(clock_gettime(CLOCK_MONOTONIC, &t) == 0);
  return (double)t.tv_sec * 1000 + (double)t.tv_nsec / 1000000;
}
static int compare(const void *a, const void *b) {
  double x = *(const double *)a, y = *(const double *)b;
  return (x > y) - (x < y);
}
static void measure(const char *name, BfDecisionHeadConfig c, size_t tokens,
                    size_t field_count, size_t options_per_field) {
  enum { SAMPLES = 21, WARMUP = 3 };
  size_t options = field_count * options_per_field;
  size_t count = bf_decision_head_layout(&c, NULL, NULL);
  size_t need = bf_decision_head_workspace(&c, tokens, field_count, options);
  float *weights = malloc(count * sizeof(float));
  float *hidden = malloc(tokens * c.hidden_size * sizeof(float));
  float *lexical = malloc(options * c.hidden_size * sizeof(float));
  float *workspace = malloc(need * sizeof(float));
  float *output = malloc(options * sizeof(float));
  BfDecisionField *fields = malloc(field_count * sizeof(*fields));
  BfDecisionSpan *spans = malloc(options * sizeof(*spans));
  assert(weights && hidden && lexical && workspace && output && fields && spans);
  for (size_t i = 0; i < count; i++) weights[i] = ((int)(i % 101) - 50) * 0.001f;
  for (size_t i = 0; i < tokens * c.hidden_size; i++) hidden[i] = (float)(i % 31) / 31;
  for (size_t i = 0; i < options * c.hidden_size; i++) lexical[i] = (float)(i % 37) / 37;
  for (size_t i = 0; i < field_count; i++)
    fields[i] = (BfDecisionField){1, {1, 2}, (uint32_t)(i * options_per_field), (uint32_t)options_per_field};
  for (size_t i = 0; i < options; i++) spans[i] = (BfDecisionSpan){0, 1};
#ifdef BF_DECISION_BENCH_OWNED
  BfDecisionHeadModel *model = NULL;
  assert(bf_decision_head_model_create(&c, weights, count, &model) == 0);
  free(weights);
#endif
  double samples[SAMPLES];
  for (size_t i = 0; i < SAMPLES + WARMUP; i++) {
    double start = milliseconds();
#ifdef BF_DECISION_BENCH_OWNED
    int status = bf_decision_head_model_run(model, hidden, tokens, fields, field_count,
                                           spans, options, lexical, workspace, need, output);
#else
    int status = bf_decision_head_run(&c, weights, count, hidden, tokens, fields, field_count,
                                     spans, options, lexical, workspace, need, output);
#endif
    assert(status == 0);
    if (i >= WARMUP) samples[i - WARMUP] = milliseconds() - start;
  }
  qsort(samples, SAMPLES, sizeof(*samples), compare);
  double checksum = 0;
  for (size_t i = 0; i < options; i++) checksum += output[i];
  printf("{\"workload\":\"%s\",\"synthetic\":true,\"samples\":%u,"
         "\"tokens\":%zu,\"fields\":%zu,\"options\":%zu,\"weight_bytes\":%zu,"
         "\"workspace_bytes\":%zu,\"median_ms\":%.6f,\"p95_ms\":%.6f,\"checksum\":%.9g}\n",
         name, SAMPLES, tokens, field_count, options, count * sizeof(float), need * sizeof(float),
         samples[SAMPLES / 2], samples[19], checksum);
#ifdef BF_DECISION_BENCH_OWNED
  bf_decision_head_model_destroy(model);
#else
  free(weights);
#endif
  free(spans); free(fields); free(output); free(workspace); free(lexical); free(hidden);
}
int main(void) {
  measure("short-schema", (BfDecisionHeadConfig){512, 256, 8, 1024, 1, 2}, 32, 8, 4);
  measure("wide-options", (BfDecisionHeadConfig){512, 256, 8, 1024, 0, 0}, 32, 8, 16);
  measure("large-head-short-input", (BfDecisionHeadConfig){1024, 512, 8, 2048, 2, 4}, 8, 1, 2);
  return 0;
}
