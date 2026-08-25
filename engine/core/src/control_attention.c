/*
 * Attention cooling, as a property of a work node rather than a scheduler.
 *
 * Ported from 10-Code/BonfyreControlPlane/scheduling.py. Conformance vectors:
 * tests/conformance/control/vectors/cooling.
 */

#include "bf_control.h"

BfAttentionState bf_attention_check(BfAttentionState state, int changed)
{
    if (changed) {
        state.level = BF_ATTENTION_HOT;
        state.unchanged_checks = 0;
        return state;
    }

    if (state.level == BF_ATTENTION_COLD) return state;

    state.unchanged_checks++;
    if ((state.level == BF_ATTENTION_HOT || state.level == BF_ATTENTION_WARM) &&
        state.unchanged_checks >= BF_ATTENTION_COOL_AFTER_UNCHANGED) {
        /* Cooling without a way back is just forgetting, so a node with no
         * reheat condition stays warm and keeps being looked at. */
        state.level = state.has_reheat_condition ? BF_ATTENTION_COOL : BF_ATTENTION_WARM;
    }
    return state;
}

int bf_attention_in_context_cut(const BfAttentionState *state)
{
    if (state == NULL) return 0;
    return state->level == BF_ATTENTION_HOT || state->level == BF_ATTENTION_WARM;
}

const char *bf_attention_name(BfAttention level)
{
    switch (level) {
    case BF_ATTENTION_HOT: return "hot";
    case BF_ATTENTION_WARM: return "warm";
    case BF_ATTENTION_COOL: return "cool";
    case BF_ATTENTION_COLD: return "cold";
    }
    return "unknown";
}
