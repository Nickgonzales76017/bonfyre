#include "bf_quic.h"
#include <stdio.h>

/* This is deliberately a semantic transport test, not a socket mock: the
 * classifier is the gate used before an early-data stream is opened. */
int main(void)
{
    bf_quic_ctx_t *moqt = bf_quic_ctx_new_application(NULL, NULL, NULL,
                                                      "moqt-21", 1, 1);
    if (!moqt) {
        fputs("QUIC raw application profile failed\n", stderr);
        return 1;
    }
    bf_quic_ctx_free(moqt);
    if (!bf_quic_operation_allows_0rtt(BF_QUIC_OPERATION_OBSERVE) ||
        !bf_quic_operation_allows_0rtt(BF_QUIC_OPERATION_CACHE_FETCH) ||
        !bf_quic_operation_allows_0rtt(BF_QUIC_OPERATION_SUBSCRIPTION_RESUME) ||
        bf_quic_operation_allows_0rtt(BF_QUIC_OPERATION_UNKNOWN) ||
        bf_quic_operation_allows_0rtt(BF_QUIC_OPERATION_MUTATION) ||
        bf_quic_operation_allows_0rtt(BF_QUIC_OPERATION_COMMITMENT) ||
        bf_quic_operation_allows_0rtt(BF_QUIC_OPERATION_PAYMENT) ||
        bf_quic_operation_allows_0rtt(BF_QUIC_OPERATION_EFFECT)) {
        fputs("QUIC 0-RTT operation gate failed\n", stderr);
        return 1;
    }
    puts("libquic-transport operation phenotype: PASS");
    return 0;
}
