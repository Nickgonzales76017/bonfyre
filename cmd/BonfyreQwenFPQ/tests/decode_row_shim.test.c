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

typedef struct fpq_model fpq_model_t;

typedef struct {
    const char *name;
    size_t      rows;
    size_t      cols;
    int         has_sli;
    float       bpw;
} fpq_tensor_info_t;

extern int fpq_decode_row(fpq_model_t *m, const char *tensor_name, size_t row, float *out);

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

static void expect_synthetic(const char *label, size_t row) {
    float out[4] = {0};
    int rc = fpq_decode_row((fpq_model_t *)1, "mock", row, out);
    if (rc != 0) {
        fprintf(stderr, "FAIL %s: expected synthetic success, got rc=%d\n", label, rc);
        g_failures++;
        return;
    }
    if (memcmp(out, g_impl_values, sizeof(out)) == 0) {
        fprintf(stderr, "FAIL %s: synthetic row matched real values -- test setup broken\n", label);
        g_failures++;
        return;
    }
    printf("ok %s: synthetic fallback engaged as explicitly requested\n", label);
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

    /* explicitly requested (testing use) -- synthetic fallback still available */
    setenv("BONFYRE_QWEN_BOOTSTRAP_SYNTHETIC_ROWS", "1", 1);
    expect_synthetic("explicit-one", 1);

    /* real decode succeeding must win over synthetic env noise, always */
    g_impl_should_succeed = 1;
    setenv("BONFYRE_QWEN_BOOTSTRAP_SYNTHETIC_ROWS", "1", 1);
    expect_real("real-decode-wins-over-synthetic-env", 1);

    unsetenv("BONFYRE_QWEN_BOOTSTRAP_SYNTHETIC_ROWS");
    expect_real("real-decode-normal-path", 1);

    if (g_failures > 0) {
        fprintf(stderr, "decode_row_shim: %d case(s) FAILED\n", g_failures);
        return 1;
    }
    printf("decode_row_shim: all cases passed\n");
    return 0;
}
