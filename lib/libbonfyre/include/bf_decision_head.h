/* SPDX-License-Identifier: Apache-2.0
 * Native typed decision head, following Cloudflare Clef's JointSchemaHead.
 * See components/BonfyreDecision/native/NOTICE for source and scope.
 */
#ifndef BF_DECISION_HEAD_H
#define BF_DECISION_HEAD_H
#include <stddef.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  uint32_t hidden_size, width, heads, feedforward, routing_layers, layers;
} BfDecisionHeadConfig;
typedef struct {
  uint32_t start, end;
} BfDecisionSpan;
typedef struct {
  uint32_t type; /* noul=0, choice=1, score=2 */
  BfDecisionSpan question;
  uint32_t option_offset, option_count;
} BfDecisionField;
typedef int (*BfDecisionWeightVisitor)(void *ctx, const char *name, size_t rows,
                                       size_t columns, size_t rank,
                                       size_t offset);
/* A single native layout definition supplies binding names and F32 offsets.
 * Returns 0 on invalid config, overflow, or visitor refusal. Matrices are
 * row-major PyTorch Linear weights [output,input]; vectors have columns=1.
 */
size_t bf_decision_head_layout(const BfDecisionHeadConfig *,
                               BfDecisionWeightVisitor, void *);
/* Workspace size in floats. No allocation, globals or retained input pointers
 * in run(). Each concurrent request must own a distinct workspace/output.
 */
size_t bf_decision_head_workspace(const BfDecisionHeadConfig *, size_t tokens,
                                  size_t fields, size_t options);
/* lexical_options are pooled *output embedding* vectors [options,hidden_size],
 * not pooled hidden states. Spans refer to the complete encoded sequence.
 * Inputs/weights/workspace/output must not overlap. This is a trusted native
 * ABI; host bindings must enforce typed buffer sizes and non-overlap.
 * 0 success, -1 invalid input/shape, -2 short workspace, -3 nonfinite result.
 * logits are published only on success, in field/option order. No softmax,
 * authority, token generation, model loading or silent state truncation here.
 */
int bf_decision_head_run(const BfDecisionHeadConfig *, const float *weights,
                         size_t weight_count, const float *hidden,
                         size_t tokens, const BfDecisionField *, size_t fields,
                         const BfDecisionSpan *, size_t options,
                         const float *lexical_options, float *workspace,
                         size_t workspace_count, float *logits);
/* Immutable owner for repeated inference. Creation copies caller weights,
 * validates the private snapshot once, and never exposes a mutable pointer.
 * No caller weight/config pointers are retained. Destroy only after all runs
 * finish; concurrent runs require separate request workspaces and outputs.
 * create: 0 success, -1 invalid shape, -3 nonfinite snapshot, -4 allocation.
 * run preserves the raw ABI's input/output/refusal contract, but does not
 * rescan the immutable snapshot on every request. The raw API above continues
 * to validate caller-owned weights on EVERY invocation.
 */
typedef struct BfDecisionHeadModel BfDecisionHeadModel;
int bf_decision_head_model_create(const BfDecisionHeadConfig *, const float *,
                                  size_t weight_count, BfDecisionHeadModel **);
void bf_decision_head_model_destroy(BfDecisionHeadModel *);
int bf_decision_head_model_run(const BfDecisionHeadModel *, const float *hidden,
                               size_t tokens, const BfDecisionField *, size_t fields,
                               const BfDecisionSpan *, size_t options,
                               const float *lexical_options, float *workspace,
                               size_t workspace_count, float *logits);
#ifdef __cplusplus
}
#endif
#endif
