/* bf_trajectory_entropy.c — runtime entropy accumulator for HVCP trajectories.
 *
 * S_runtime = λ1·|H_t − H_0|          (Hamiltonian drift)
 *           + λ2·gap_count             (topological gaps encountered)
 *           + λ3·mean_gap_duration     (avg steps spent in each gap)
 *           + λ4·log(mean_candidates)  (candidate set breadth)
 *           + λ5·branch_divergence     (branch count × mean branch ΔH)
 *           + λ6·mount_count           (sub-cache mounts needed)
 *           + λ7·target_distance       (distance to target basin)
 *
 * Default λ values are calibrated so S ≈ 1.0 for a "neutral" run
 * (stable H, 1–2 mounts, moderate candidates).  A clean run scores
 * near 0; a chaotic run can score > 10.
 *
 * Usage:
 *   BfEntropyAccum a;
 *   bf_entropy_init(&a, H0);
 *   for each step:
 *     bf_entropy_update_step(&a, H, gap, new_mounts, candidates);
 *   float cost = bf_entropy_score(&a);
 */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include "bonfyre.h"

/* Default lambda weights */
static const float BF_ENTROPY_LAM_DEFAULTS[7] = {
    0.10f,  /* λ1  Hamiltonian drift */
    1.00f,  /* λ2  gap count */
    0.50f,  /* λ3  mean gap duration */
    0.10f,  /* λ4  log(mean_candidates) */
    2.00f,  /* λ5  branch divergence */
    0.50f,  /* λ6  mount count */
    1.00f,  /* λ7  target distance */
};

void bf_entropy_init(BfEntropyAccum *a, float H0) {
    if (!a) return;
    memset(a, 0, sizeof(*a));
    a->H_0      = H0;
    a->H_last   = H0;
    a->H_drift  = 0.0f;
    memcpy(a->lam, BF_ENTROPY_LAM_DEFAULTS, sizeof(a->lam));
}

void bf_entropy_set_lambdas(BfEntropyAccum *a, const float lam[7]) {
    if (!a || !lam) return;
    memcpy(a->lam, lam, 7 * sizeof(float));
}

void bf_entropy_update_step(BfEntropyAccum *a, float H,
                             int gap, int new_mounts, int candidates) {
    if (!a) return;
    a->step_count++;

    /* Hamiltonian drift: track max |H - H0| seen */
    float drift = fabsf(H - a->H_0);
    if (drift > a->H_drift) a->H_drift = drift;
    a->H_last = H;

    /* Gap tracking */
    if (gap) {
        if (!a->in_gap) {
            a->in_gap = 1;
            a->gap_count++;
            a->gap_start_step = a->step_count;
        }
        a->gap_steps_total++;
    } else {
        a->in_gap = 0;
    }

    /* Mounts */
    a->mount_count += (uint32_t)new_mounts;

    /* Candidate breadth: accumulate log(candidates+1) */
    if (candidates > 0) {
        a->log_cand_sum += log((double)(candidates + 1));
        a->log_cand_n++;
    }
}

void bf_entropy_update_branch(BfEntropyAccum *a, int new_branches) {
    if (!a) return;
    a->branch_count += (uint32_t)new_branches;
}

void bf_entropy_set_target(BfEntropyAccum *a, float target_distance) {
    if (!a) return;
    a->target_distance = target_distance;
}

float bf_entropy_score(const BfEntropyAccum *a) {
    if (!a) return 0.0f;

    /* T1: Hamiltonian drift */
    float t1 = a->lam[0] * a->H_drift;

    /* T2: gap count */
    float t2 = a->lam[1] * (float)a->gap_count;

    /* T3: mean gap duration (steps per gap) */
    float t3 = 0.0f;
    if (a->gap_count > 0) {
        float mean_dur = (float)a->gap_steps_total / (float)a->gap_count;
        t3 = a->lam[2] * mean_dur;
    }

    /* T4: log mean candidates */
    float t4 = 0.0f;
    if (a->log_cand_n > 0) {
        float mean_log_cand = (float)(a->log_cand_sum / a->log_cand_n);
        t4 = a->lam[3] * mean_log_cand;
    }

    /* T5: branch divergence (branch_count is a proxy without actual ΔH) */
    float t5 = a->lam[4] * (float)a->branch_count;

    /* T6: mount count */
    float t6 = a->lam[5] * (float)a->mount_count;

    /* T7: target distance */
    float t7 = a->lam[6] * a->target_distance;

    return t1 + t2 + t3 + t4 + t5 + t6 + t7;
}

int bf_entropy_report(const BfEntropyAccum *a, FILE *out) {
    if (!a || !out) return -1;
    float score = bf_entropy_score(a);
    float mean_dur = a->gap_count > 0
                   ? (float)a->gap_steps_total / (float)a->gap_count : 0.0f;
    double mean_log_cand = a->log_cand_n > 0
                         ? a->log_cand_sum / a->log_cand_n : 0.0;

    fprintf(out, "trajectory entropy report:\n");
    fprintf(out, "  steps          : %u\n",   a->step_count);
    fprintf(out, "  H drift        : %.4f  (λ1=%.2f → contrib %.4f)\n",
            a->H_drift, a->lam[0], a->lam[0]*a->H_drift);
    fprintf(out, "  gaps           : %u     (λ2=%.2f → contrib %.4f)\n",
            a->gap_count, a->lam[1], a->lam[1]*(float)a->gap_count);
    fprintf(out, "  mean gap dur   : %.2f  (λ3=%.2f → contrib %.4f)\n",
            mean_dur, a->lam[2], a->lam[2]*mean_dur);
    fprintf(out, "  log mean cand  : %.3f (λ4=%.2f → contrib %.4f)\n",
            mean_log_cand, a->lam[3], a->lam[3]*(float)mean_log_cand);
    fprintf(out, "  branches       : %u     (λ5=%.2f → contrib %.4f)\n",
            a->branch_count, a->lam[4], a->lam[4]*(float)a->branch_count);
    fprintf(out, "  mounts         : %u     (λ6=%.2f → contrib %.4f)\n",
            a->mount_count, a->lam[5], a->lam[5]*(float)a->mount_count);
    fprintf(out, "  target dist    : %.4f  (λ7=%.2f → contrib %.4f)\n",
            a->target_distance, a->lam[6], a->lam[6]*a->target_distance);
    fprintf(out, "  ─────────────────────────────────────\n");
    fprintf(out, "  S_runtime      = %.4f\n", score);

    /* qualitative tier */
    const char *tier = score < 1.0f ? "clean"
                     : score < 3.0f ? "moderate"
                     : score < 7.0f ? "chaotic"
                     : "unstable";
    fprintf(out, "  tier           = %s\n", tier);
    return 0;
}
