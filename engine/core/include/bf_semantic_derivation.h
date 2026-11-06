#ifndef BF_SEMANTIC_DERIVATION_H
#define BF_SEMANTIC_DERIVATION_H

/*
 * Bounded, native incremental derivation for semantic programs.
 *
 * The graph stores only declared addresses and constraints.  It does not
 * execute effects, infer authority, or turn a learned signal into proof.
 */

#include <stddef.h>
#include <stdint.h>

#define BF_DERIVATION_ID_MAX 128
#define BF_DERIVATION_REF_MAX 256
#define BF_DERIVATION_NODE_MAX 64
#define BF_DERIVATION_PROGRAM_MAX 64
#define BF_DERIVATION_REF_MAX_PER_PROGRAM 32
#define BF_DERIVATION_CONSTRAINT_MAX 32

typedef enum BfDerivationStatus {
    BF_DERIVATION_OK = 0,
    BF_DERIVATION_INVALID,
    BF_DERIVATION_CAPACITY,
    BF_DERIVATION_DUPLICATE_PROGRAM,
    BF_DERIVATION_DUPLICATE_REFERENCE
} BfDerivationStatus;

typedef struct BfHybridLogicalClock {
    uint64_t physical_ms;
    uint64_t logical;
    char node[BF_DERIVATION_ID_MAX];
} BfHybridLogicalClock;

typedef struct BfFactDelta {
    const char *delta_id;
    const char *const *writes;
    size_t write_count;
    const char *causal_frontier;
    BfHybridLogicalClock hlc;
} BfFactDelta;

typedef struct BfConstraintSpec {
    const char *constraint_id;
    int hard;
    int satisfied;
    const char *source_ref;
} BfConstraintSpec;

typedef struct BfSemanticProgramSpec {
    const char *program_id;
    const char *operator_name;
    const char *const *reads;
    size_t read_count;
    const char *const *writes;
    size_t write_count;
    const BfConstraintSpec *constraints;
    size_t constraint_count;
} BfSemanticProgramSpec;

typedef struct BfSemanticProgram {
    char program_id[BF_DERIVATION_ID_MAX];
    char operator_name[BF_DERIVATION_ID_MAX];
    char reads[BF_DERIVATION_REF_MAX_PER_PROGRAM][BF_DERIVATION_REF_MAX];
    size_t read_count;
    char writes[BF_DERIVATION_REF_MAX_PER_PROGRAM][BF_DERIVATION_REF_MAX];
    size_t write_count;
    struct {
        char constraint_id[BF_DERIVATION_ID_MAX];
        int hard;
        int satisfied;
        char source_ref[BF_DERIVATION_REF_MAX];
    } constraints[BF_DERIVATION_CONSTRAINT_MAX];
    size_t constraint_count;
} BfSemanticProgram;

typedef struct BfSemanticDerivationGraph {
    BfSemanticProgram programs[BF_DERIVATION_PROGRAM_MAX];
    size_t program_count;
} BfSemanticDerivationGraph;

typedef struct BfSemanticSlice {
    char delta_id[BF_DERIVATION_ID_MAX];
    char affected_programs[BF_DERIVATION_PROGRAM_MAX][BF_DERIVATION_ID_MAX];
    size_t affected_count;
    char read_closure[BF_DERIVATION_NODE_MAX][BF_DERIVATION_REF_MAX];
    size_t read_closure_count;
} BfSemanticSlice;

typedef struct BfDecisionSnapshot {
    char program_id[BF_DERIVATION_ID_MAX];
    int admitted;
    char hard_failures[BF_DERIVATION_CONSTRAINT_MAX][BF_DERIVATION_ID_MAX];
    size_t hard_failure_count;
    char learned_signals[BF_DERIVATION_CONSTRAINT_MAX][BF_DERIVATION_ID_MAX];
    size_t learned_signal_count;
    char read_closure_sha256[65];
} BfDecisionSnapshot;

void bf_hlc_tick(const BfHybridLogicalClock *clock, uint64_t physical_ms,
                 BfHybridLogicalClock *out);
void bf_hlc_merge(const BfHybridLogicalClock *left, const BfHybridLogicalClock *right,
                  uint64_t physical_ms, BfHybridLogicalClock *out);

void bf_semantic_derivation_init(BfSemanticDerivationGraph *graph);
BfDerivationStatus bf_semantic_derivation_add(BfSemanticDerivationGraph *graph,
                                               const BfSemanticProgramSpec *program);
BfDerivationStatus bf_semantic_derivation_slice(const BfSemanticDerivationGraph *graph,
                                                 const BfFactDelta *delta,
                                                 BfSemanticSlice *out);
BfDerivationStatus bf_semantic_derivation_decide(const BfSemanticProgram *program,
                                                  BfDecisionSnapshot *out);
const char *bf_derivation_status_name(BfDerivationStatus status);

#endif
