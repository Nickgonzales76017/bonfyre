#include "bf_semantic_derivation.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(void)
{
    BfSemanticDerivationGraph graph;
    BfSemanticSlice slice;
    BfDecisionSnapshot decision;
    BfHybridLogicalClock merged;
    const char *finance_reads[] = {"agreement:P"};
    const char *finance_writes[] = {"position:P"};
    const char *surface_reads[] = {"position:P"};
    const char *surface_writes[] = {"surface:P"};
    const char *unrelated_reads[] = {"other"};
    const char *unrelated_writes[] = {"other-out"};
    const char *delta_writes[] = {"agreement:P"};
    const BfConstraintSpec constraints[] = {
        {"authority", 1, 0, "authority:P"},
        {"latent-rank", 0, 1, "model:P"},
    };
    const BfSemanticProgramSpec finance = {
        "finance", "ALLOCATE", finance_reads, 1, finance_writes, 1, constraints, 2};
    const BfSemanticProgramSpec surface = {
        "surface", "PROJECT", surface_reads, 1, surface_writes, 1, NULL, 0};
    const BfSemanticProgramSpec unrelated = {
        "unrelated", "PROJECT", unrelated_reads, 1, unrelated_writes, 1, NULL, 0};
    const BfFactDelta delta = {"d1", delta_writes, 1, "frontier:1", {100, 0, "node"}};

    bf_semantic_derivation_init(&graph);
    assert(bf_semantic_derivation_add(&graph, &surface) == BF_DERIVATION_OK);
    assert(bf_semantic_derivation_add(&graph, &finance) == BF_DERIVATION_OK);
    assert(bf_semantic_derivation_add(&graph, &unrelated) == BF_DERIVATION_OK);
    assert(bf_semantic_derivation_slice(&graph, &delta, &slice) == BF_DERIVATION_OK);
    assert(slice.affected_count == 2);
    assert(strcmp(slice.affected_programs[0], "finance") == 0);
    assert(strcmp(slice.affected_programs[1], "surface") == 0);
    assert(slice.read_closure_count == 2);

    assert(bf_semantic_derivation_decide(&graph.programs[1], &decision) == BF_DERIVATION_OK);
    assert(!decision.admitted);
    assert(decision.hard_failure_count == 1);
    assert(strcmp(decision.hard_failures[0], "authority") == 0);
    assert(decision.learned_signal_count == 1);
    assert(strcmp(decision.learned_signals[0], "latent-rank") == 0);
    assert(strcmp(decision.read_closure_sha256,
                  "47d3244d9238bbb8447c5a1b03f246186d643712b65be97386a2a416ed0845f2") == 0);

    bf_hlc_merge(&(BfHybridLogicalClock){100, 2, "left"},
                 &(BfHybridLogicalClock){100, 4, "right"}, 99, &merged);
    assert(merged.physical_ms == 100 && merged.logical == 5);
    assert(strcmp(merged.node, "left") == 0);
    puts("semantic derivation native tests passed");
    return 0;
}
