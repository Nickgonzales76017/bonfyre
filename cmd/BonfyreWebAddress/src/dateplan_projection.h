#ifndef BONFYRE_DATEPLAN_PROJECTION_H
#define BONFYRE_DATEPLAN_PROJECTION_H
#include <sqlite3.h>
#include <stddef.h>
#include <stdio.h>
/* AI date-plan generation, verification and history.
 *
 * Prompt and plan contract derive from start-again-06/AI-Integrated-Dating-App
 * (MIT, Copyright 2026 Anjan Mahapatra): location/intent/budget/interests in,
 * plans[{title,venue,time,reason,safety_note}] plus backup_plan out. Native
 * additions: per-plan cost bounds with budget admission, companion gating to
 * mutuals, accepted persisted from day one (the donor reads/writes it without
 * a schema column), and hashed generation receipts.
 *
 * Generation shells to the local qwen runner when one fits in memory; when no
 * model is available the call reports unavailable and nothing is fabricated.
 * Verification is pure and golden-tested: schema, required safety notes and
 * budget feasibility admit a subset, never the raw reply. */
#define BF_DATEPLAN_PROMPT_CAP 4096
#define BF_DATEPLAN_OUTPUT_CAP 32768
#define BF_DATEPLAN_ADMITTED_CAP 24576

typedef struct {
    char prompt[BF_DATEPLAN_PROMPT_CAP];
    char prompt_sha256[65];
    char output_sha256[65];
    char admitted[BF_DATEPLAN_ADMITTED_CAP];
    char model[128];
    char unavail_reason[256];
    long duration_ms;
    long output_bytes;
    long admitted_min;
    long admitted_max;
    int plans_admitted;
    int plans_dropped;
} BfDatePlanResult;

/* Build the fixed generation prompt. Returns 0 on success, -1 on misuse
 * (bad lengths, budget_min > budget_max, either bound above 100000). */
int bf_dateplan_prompt(const char *location, const char *intent, long budget_min,
                       long budget_max, const char *interests,
                       char *out, size_t cap, char sha_hex[65]);
/* Verify raw generator output against the budget; admit the feasible subset.
 * Returns 1 with result filled, 0 when nothing is admissible, -1 on misuse. */
int bf_dateplan_verify(const char *output, long budget_max, const char *model,
                       const char *prompt_sha, BfDatePlanResult *result);
/* Run local generation end to end. The model resolves through Model Commons:
 * BONFYRE_DATEPLAN_MODEL pins a model id, else the catalogue edge
 * capability:dateplan-generate uses_model names one. Execution uses the
 * owned local runner; hosted models are never auto-called. Returns 1 on
 * admitted plans, 0 when the reply admits nothing, -2 with unavail_reason
 * when no model is available, -1 on misuse. */
int bf_dateplan_generate(sqlite3 *db, const char *location, const char *intent,
                         long budget_min, long budget_max, const char *interests,
                         BfDatePlanResult *result);
/* Observed per-token USD rates for a model from the prices db. Returns 1
 * with both rates, 0 when no single observation exists, -1 on misuse.
 * No rate is estimated: absent means absent. */
int bf_dateplan_price(const char *model, double *in_rate, double *out_rate,
                      char *observed_at, size_t observed_cap);
/* Render the actor's plan history, newest first. Returns rows, -1 on misuse. */
int bf_dateplan_history(FILE *out, sqlite3 *db, const char *actor, unsigned limit);
#endif
