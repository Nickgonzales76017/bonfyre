// What this does not prove:
// - a dependency is replaced by an object built in this test, so the real one is not exercised.
/*
 * decode_row_shim.test.c
 *
 * Regression test for the fpq_decode_row() opt-out bug (fpq_compat_shim.c):
 * getenv("BONFYRE_QWEN_BOOTSTRAP_SYNTHETIC_ROWS") only checked for presence,
 * so an explicit "=0" opt-out was truthy and still fabricated a hash-noise
 * row whenever the real decoder failed. That silently fed random values in
 * place of real weights -- the reported root cause of incoherent generation.
 *
 * fpq_decode_row() depends on exactly four extern symbols (fpq_decode_one,
 * fpq_tensor_find, fpq_get_passthrough, fpq_decode_row_impl), all mocked
 * below with a controllable success/failure switch, so the shim's own
 * control flow can be exercised directly without a real FPQ pack.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <bonfyre.h>
#include "../include/libfpq.h"
#include "../src/fpq_active_cache.h"

static int g_impl_should_succeed = 0;
static const float g_impl_values[4] = {1.5f, -2.25f, 3.0f, 0.125f};

int fpq_decode_one(fpq_model_t *m, const char *tensor_name, float *out) {
    (void)m; (void)tensor_name; (void)out;
    return -1;
}

const fpq_tensor_info_t *fpq_tensor_find(fpq_model_t *m, const char *name) {
    (void)m; (void)name;
    static fpq_tensor_info_t info = {"mock", 2, 4, 0, 0.0f};
    return &info;
}

const float *fpq_get_passthrough(fpq_model_t *m, const char *tensor_name) {
    (void)m; (void)tensor_name;
    return NULL;
}

int fpq_decode_row_impl(fpq_model_t *m, const char *tensor_name, size_t row, float *out) {
    (void)m; (void)tensor_name; (void)row;
    if (!g_impl_should_succeed) return -1;
    memcpy(out, g_impl_values, sizeof(g_impl_values));
    return 0;
}

static int g_failures = 0;

static void expect_fail(const char *label, size_t row) {
    float out[4] = {0};
    int rc = fpq_decode_row((fpq_model_t *)1, "mock", row, out);
    if (rc == 0) {
        fprintf(stderr, "FAIL %s: expected safe failure, got fabricated success [%f %f %f %f]\n",
                label, (double)out[0], (double)out[1], (double)out[2], (double)out[3]);
        g_failures++;
        return;
    }
    printf("ok %s: correctly failed (rc=%d), no fabricated row leaked\n", label, rc);
}

static void expect_real(const char *label, size_t row) {
    float out[4] = {0};
    int rc = fpq_decode_row((fpq_model_t *)1, "mock", row, out);
    if (rc != 0) {
        fprintf(stderr, "FAIL %s: expected real decode success, got rc=%d\n", label, rc);
        g_failures++;
        return;
    }
    if (memcmp(out, g_impl_values, sizeof(out)) != 0) {
        fprintf(stderr, "FAIL %s: expected real decoded values, got [%f %f %f %f] -- looks synthetic\n",
                label, (double)out[0], (double)out[1], (double)out[2], (double)out[3]);
        g_failures++;
        return;
    }
    printf("ok %s: real decoded values returned, not synthetic noise\n", label);
}

static void expect_active_density(void) {
    uint8_t shared_body[9] = {0};
    uint8_t distinct_body[9] = {0};
    const uint16_t fp16_one = 0x3c00u;
    fpq_active_cache_t *cache = NULL;
    fpq_active_entry_t *a = NULL, *b = NULL, *c = NULL;
    fpq_active_density_t density = {0};

    memcpy(shared_body + 0, &fp16_one, sizeof(fp16_one));
    shared_body[2] = 1;
    shared_body[3] = 2;
    memcpy(shared_body + 4, &fp16_one, sizeof(fp16_one));
    shared_body[6] = 3;
    shared_body[7] = 4;
    shared_body[8] = 5;
    memcpy(distinct_body, shared_body, sizeof(shared_body));
    distinct_body[8] = 6;

    setenv("BONFYRE_ACTIVE_WEIGHT_CACHE", "1", 1);
    setenv("BONFYRE_ACTIVE_CACHE_PERSIST", "0", 1);
    cache = fpq_active_cache_init("/tmp", 1);
    if (!cache) {
        fprintf(stderr, "FAIL active-density: cache init failed\n");
        g_failures++;
        goto done;
    }

    a = fpq_active_cache_get(cache, "density.a", 2, 3, NULL, 0,
                             "v9-lr-active", 9);
    b = fpq_active_cache_get(cache, "density.b", 2, 3, NULL, 0,
                             "v9-lr-active", 9);
    c = fpq_active_cache_get(cache, "density.c", 2, 3, NULL, 0,
                             "v9-lr-active", 9);
    if (!a || !b || !c ||
        fpq_active_materialize_v9(a, shared_body, sizeof(shared_body), 0, 4, 1) != 0 ||
        fpq_active_materialize_v9(b, shared_body, sizeof(shared_body), 0, 4, 1) != 0 ||
        fpq_active_materialize_v9(c, distinct_body, sizeof(distinct_body), 0, 4, 1) != 0) {
        fprintf(stderr, "FAIL active-density: materialization failed\n");
        g_failures++;
        goto done;
    }

    {
        const float x[3] = {1.0f, 2.0f, 3.0f};
        float y[2] = {0.0f, 0.0f};
        if (a->active_qweight != NULL ||
            fpq_active_matvec(a, x, y) != 0 ||
            y[0] != 26.0f || y[1] != 52.0f) {
            fprintf(stderr,
                    "FAIL active-density: factor-only matvec expected [26,52], got [%f,%f], dense=%p\n",
                    (double)y[0], (double)y[1], (void *)a->active_qweight);
            g_failures++;
            goto done;
        }
        printf("ok active-density: factor-only matvec executed directly with no dense weight body\n");
    }
    fpq_active_cache_density(cache, &density);
    if (density.ready_entries != 3 ||
        density.unique_factors != 2 ||
        density.duplicate_factors != 1 ||
        density.resident_factor_bytes != 39 ||
        density.unique_factor_bytes != 26 ||
        density.duplicate_factor_bytes != 13 ||
        density.unique_payloads != 2 ||
        density.duplicate_payloads != 1 ||
        density.resident_payload_bytes != 39 ||
        density.unique_payload_bytes != 26 ||
        density.duplicate_payload_bytes != 13 ||
        strcmp(a->factor_sha256, b->factor_sha256) != 0 ||
        strcmp(a->factor_sha256, c->factor_sha256) == 0 ||
        strcmp(a->payload_sha256, b->payload_sha256) != 0 ||
        strcmp(a->payload_sha256, c->payload_sha256) == 0) {
        fprintf(stderr,
                "FAIL active-density: ready=%zu unique=%zu duplicate=%zu "
                "resident=%zu unique_bytes=%zu duplicate_bytes=%zu\n",
                density.ready_entries,
                density.unique_payloads,
                density.duplicate_payloads,
                density.resident_payload_bytes,
                density.unique_payload_bytes,
                density.duplicate_payload_bytes);
        g_failures++;
        goto done;
    }

    printf("ok active-density: 3 resident factor bodies -> 2 unique + 1 exact duplicate (%zu duplicate bytes)\n",
           density.duplicate_payload_bytes);

    {
        char passport_path[256];
        char line[8192];
        char bases[3][80] = {{0}};
        char bindings[3][80] = {{0}};
        char exceptions[3][80] = {{0}};
        char physical[3][80] = {{0}};
        FILE *pf = NULL;
        int n = 0;

        snprintf(passport_path, sizeof(passport_path),
                 "/tmp/bonfyre-active-passports-%ld.jsonl", (long)getpid());
        unlink(passport_path);
        if (fpq_active_cache_write_passports(cache, passport_path) != 3) {
            fprintf(stderr, "FAIL active-passports: expected 3 emitted passports\n");
            g_failures++;
            goto done;
        }
        pf = fopen(passport_path, "r");
        if (!pf) {
            fprintf(stderr, "FAIL active-passports: emitted JSONL is not readable\n");
            g_failures++;
            goto done;
        }
        while (n < 3 && fgets(line, sizeof(line), pf)) {
            char family[64] = {0}, exactness[32] = {0};
            if (!bf_json_str(line, "subject_family", family, sizeof(family)) ||
                !bf_json_str(line, "exactness_class", exactness, sizeof(exactness)) ||
                !bf_json_str(line, "shared_basis", bases[n], sizeof(bases[n])) ||
                !bf_json_str(line, "binding_digest", bindings[n], sizeof(bindings[n])) ||
                !bf_json_str(line, "exception_digest", exceptions[n], sizeof(exceptions[n])) ||
                !bf_json_str(line, "physical_digest", physical[n], sizeof(physical[n])) ||
                strcmp(family, "fpq-active-tensor") != 0 ||
                strcmp(exactness, "exact") != 0) {
                fprintf(stderr, "FAIL active-passports: malformed canonical passport line %d\n", n);
                fclose(pf);
                unlink(passport_path);
                g_failures++;
                goto done;
            }
            n++;
        }
        if (fgets(line, sizeof(line), pf) != NULL) n++;
        fclose(pf);
        unlink(passport_path);

        if (n != 3 ||
            strcmp(bases[0], bases[1]) != 0 ||
            strcmp(bases[0], bases[2]) == 0 ||
            strcmp(bindings[0], bindings[1]) == 0 ||
            strcmp(bindings[0], bindings[2]) == 0 ||
            strcmp(bindings[1], bindings[2]) == 0 ||
            exceptions[0][0] != '\0' ||
            exceptions[1][0] != '\0' ||
            exceptions[2][0] != '\0' ||
            strcmp(physical[0], physical[1]) != 0 ||
            strcmp(physical[0], physical[2]) == 0) {
            fprintf(stderr,
                    "FAIL active-passports: expected 3 bindings over 2 bases, sparse exceptions, 2 payloads\n");
            g_failures++;
            goto done;
        }
        printf("ok active-passports: 3 bindings -> 2 shared bases, 0 exceptions, 2 physical payloads\n");
    }


done:
    fpq_active_cache_free(cache);
    unsetenv("BONFYRE_ACTIVE_CACHE_PERSIST");
    unsetenv("BONFYRE_ACTIVE_WEIGHT_CACHE");
}

int main(void) {
    /* real decode fails; synthetic env unset -> must fail safely, never fabricate */
    g_impl_should_succeed = 0;
    unsetenv("BONFYRE_QWEN_BOOTSTRAP_SYNTHETIC_ROWS");
    expect_fail("unset-env", 1);

    /* the exact bug: explicit "=0" opt-out used to still be truthy and fabricate */
    setenv("BONFYRE_QWEN_BOOTSTRAP_SYNTHETIC_ROWS", "0", 1);
    expect_fail("explicit-zero-optout", 1);

    /* explicit empty string is also disabled */
    setenv("BONFYRE_QWEN_BOOTSTRAP_SYNTHETIC_ROWS", "", 1);
    expect_fail("explicit-empty", 1);

    /* the old switch is set -- there is no fabricated row for it to turn on */
    setenv("BONFYRE_QWEN_BOOTSTRAP_SYNTHETIC_ROWS", "1", 1);
    expect_fail("old-switch-fabricates-nothing", 1);

    /* real decode succeeding must win over synthetic env noise, always */
    g_impl_should_succeed = 1;
    setenv("BONFYRE_QWEN_BOOTSTRAP_SYNTHETIC_ROWS", "1", 1);
    expect_real("real-decode-wins-over-synthetic-env", 1);

    unsetenv("BONFYRE_QWEN_BOOTSTRAP_SYNTHETIC_ROWS");
    expect_real("real-decode-normal-path", 1);
    expect_active_density();

    if (g_failures > 0) {
        fprintf(stderr, "decode_row_shim: %d case(s) FAILED\n", g_failures);
        return 1;
    }
    printf("decode_row_shim: all cases passed\n");
    return 0;
}
