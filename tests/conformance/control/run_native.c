/*
 * Replay the control-plane conformance vectors against the native kernel.
 *
 * The vectors are generated from the Python reference implementation in
 * components/BonfyreControlPlane, which is frozen. Parity here is what allows the
 * runtime dependence on that reference to be dropped.
 *
 *   cc -std=c11 -Wall -Wextra -Werror -I engine/core/include \
 *      tests/conformance/control/run_native.c \
 *      engine/core/src/control_provider.c \
 *      engine/core/src/control_admission.c \
 *      engine/core/src/control_attention.c \
 *      engine/core/src/control_capability.c -o /tmp/bf-control-conformance
 */

#include "bf_control.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define GIB (1024LL * 1024LL * 1024LL)
#define MAX_EVENTS 32

static int failures;
static int checks;

static void fail(const char *suite, const char *name, const char *derived_from,
                 const char *detail)
{
    failures++;
    printf("FAIL  %s/%s\n", suite, name);
    if (derived_from[0] != '\0') printf("      reintroduces: %s\n", derived_from);
    printf("      %s\n", detail);
}

static int provider_kind_from_name(const char *name, BfProviderEventKind *out)
{
    if (strcmp(name, "success") == 0) { *out = BF_PROVIDER_SUCCESS; return 0; }
    if (strcmp(name, "transient_failure") == 0) { *out = BF_PROVIDER_TRANSIENT_FAILURE; return 0; }
    if (strcmp(name, "hard_capacity") == 0) { *out = BF_PROVIDER_HARD_CAPACITY; return 0; }
    if (strcmp(name, "manual_pause") == 0) { *out = BF_PROVIDER_MANUAL_PAUSE; return 0; }
    if (strcmp(name, "manual_resume") == 0) { *out = BF_PROVIDER_MANUAL_RESUME; return 0; }
    return -1;
}

static void trim_newline(char *line)
{
    size_t length = strlen(line);
    while (length > 0 && (line[length - 1] == '\n' || line[length - 1] == '\r')) {
        line[--length] = '\0';
    }
}

int main(int argc, char **argv)
{
    const char *path = argc > 1 ? argv[1]
                                : "tests/conformance/control/vectors/control.vec";
    FILE *file = fopen(path, "r");
    if (file == NULL) {
        fprintf(stderr, "cannot open %s\n", path);
        return 2;
    }

    char suite[32] = { 0 };
    char name[128] = { 0 };
    char derived_from[256] = { 0 };
    char reset_text[512] = { 0 };

    BfProviderObservation events[MAX_EVENTS];
    size_t event_count = 0;
    int64_t provider_now = 0;

    BfAdmissionPolicy policy;
    BfAdmissionRequest request;
    memset(&policy, 0, sizeof(policy));
    memset(&request, 0, sizeof(request));

    int cooling_has_condition = 0;
    int cooling_unchanged = 0;
    int cooling_reheat = 0;

    char line[1024];
    while (fgets(line, sizeof(line), file) != NULL) {
        trim_newline(line);
        if (line[0] == '\0' || line[0] == '#') continue;

        if (line[0] != ' ') {
            char kind[32];
            char value[128];
            if (sscanf(line, "%31s %127[^\n]", kind, value) != 2) continue;
            snprintf(suite, sizeof(suite), "%s", kind);
            snprintf(name, sizeof(name), "%s", value);
            derived_from[0] = '\0';
            event_count = 0;
            provider_now = 0;
            cooling_has_condition = cooling_unchanged = cooling_reheat = 0;
            memset(&policy, 0, sizeof(policy));
            memset(&request, 0, sizeof(request));
            reset_text[0] = '\0';
            continue;
        }

        const char *body = line;
        while (*body == ' ') body++;

        if (strncmp(body, "derived_from ", 13) == 0) {
            snprintf(derived_from, sizeof(derived_from), "%s", body + 13);
            continue;
        }

        if (strcmp(suite, "provider") == 0) {
            char kind_name[32];
            long long at = 0, reset = 0;
            if (sscanf(body, "event %31s %lld %lld", kind_name, &at, &reset) == 3) {
                if (event_count >= MAX_EVENTS) continue;
                BfProviderEventKind kind;
                if (provider_kind_from_name(kind_name, &kind) != 0) continue;
                events[event_count].kind = kind;
                events[event_count].observed_at_ms = (int64_t)at;
                events[event_count].reset_at_ms = (int64_t)reset;
                event_count++;
            } else if (sscanf(body, "now %lld", &at) == 1) {
                provider_now = (int64_t)at;
            } else {
                char expect_status[32];
                long long expect_circuit = 0;
                int expect_available = 0;
                if (sscanf(body, "expect %31s %lld %d", expect_status, &expect_circuit,
                           &expect_available) == 3) {
                    checks++;
                    BfProviderState state =
                        bf_provider_fold(events, event_count, provider_now);
                    int available = bf_provider_available(&state, provider_now);
                    const char *status = bf_provider_status_name(state.status);
                    if (strcmp(status, expect_status) != 0 ||
                        state.circuit_until_ms != (int64_t)expect_circuit ||
                        available != expect_available) {
                        char detail[512];
                        snprintf(detail, sizeof(detail),
                                 "expected %s circuit=%lld available=%d, got %s "
                                 "circuit=%" PRId64 " available=%d",
                                 expect_status, expect_circuit, expect_available,
                                 status, state.circuit_until_ms, available);
                        fail(suite, name, derived_from, detail);
                    }
                }
            }
            continue;
        }

        if (strcmp(suite, "admission") == 0) {
            long long a = 0, b = 0, c = 0, d = 0;
            if (sscanf(body, "policy %lld %lld %lld", &a, &b, &c) == 3) {
                policy.protected_floor_bytes = (int64_t)a * GIB;
                policy.per_plane_quota_bytes = (int64_t)b * GIB;
                policy.max_grant_bytes = (int64_t)c * GIB;
            } else if (sscanf(body, "request %lld %lld %lld %lld", &a, &b, &c, &d) == 4) {
                request.estimated_bytes = (int64_t)a * GIB;
                request.free_bytes = (int64_t)b * GIB;
                request.committed_bytes = (int64_t)c * GIB;
                request.plane_committed_bytes = (int64_t)d * GIB;
            } else {
                char expect_verdict[32];
                if (sscanf(body, "expect %31s", expect_verdict) == 1) {
                    checks++;
                    BfAdmissionDecision decision = bf_admission_decide(&request, &policy);
                    const char *verdict = bf_admission_verdict_name(decision.verdict);
                    if (strcmp(verdict, expect_verdict) != 0) {
                        char detail[512];
                        snprintf(detail, sizeof(detail), "expected %s, got %s (%s)",
                                 expect_verdict, verdict, decision.reason);
                        fail(suite, name, derived_from, detail);
                    }
                }
            }
            continue;
        }

        if (strcmp(suite, "cooling") == 0) {
            int value = 0;
            if (sscanf(body, "has_condition %d", &value) == 1) {
                cooling_has_condition = value;
            } else if (sscanf(body, "unchanged %d", &value) == 1) {
                cooling_unchanged = value;
            } else if (sscanf(body, "reheat %d", &value) == 1) {
                cooling_reheat = value;
            } else {
                char expect_level[32];
                int expect_in_cut = 0;
                if (sscanf(body, "expect %31s %d", expect_level, &expect_in_cut) == 2) {
                    checks++;
                    BfAttentionState state;
                    memset(&state, 0, sizeof(state));
                    state.level = BF_ATTENTION_WARM;
                    state.has_reheat_condition = cooling_has_condition;
                    for (int index = 0; index < cooling_unchanged; index++) {
                        state = bf_attention_check(state, 0);
                    }
                    if (cooling_reheat) state = bf_attention_check(state, 1);
                    const char *level = bf_attention_name(state.level);
                    int in_cut = bf_attention_in_context_cut(&state);
                    if (strcmp(level, expect_level) != 0 || in_cut != expect_in_cut) {
                        char detail[512];
                        snprintf(detail, sizeof(detail),
                                 "expected %s in_cut=%d, got %s in_cut=%d",
                                 expect_level, expect_in_cut, level, in_cut);
                        fail(suite, name, derived_from, detail);
                    }
                }
            }
            continue;
        }

        if (strcmp(suite, "reset") == 0) {
            if (strncmp(body, "text ", 5) == 0) {
                snprintf(reset_text, sizeof(reset_text), "%s", body + 5);
            } else {
                long long expected = 0;
                if (sscanf(body, "expect %lld", &expected) == 1) {
                    checks++;
                    int64_t parsed = bf_provider_parse_reset(reset_text);
                    if (parsed != (int64_t)expected) {
                        char detail[512];
                        snprintf(detail, sizeof(detail),
                                 "expected %lld, got %" PRId64, expected, parsed);
                        fail(suite, name, derived_from, detail);
                    }
                }
            }
            continue;
        }
    }

    fclose(file);
    printf("%d checks, %d failures\n", checks, failures);
    return failures == 0 ? 0 : 1;
}
