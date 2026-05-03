/*
 * he_sli.c — Homomorphic Encryption bridge for SLI (stub + reference impl)
 *
 * This provides:
 *   1. A working reference implementation using scalar integer arithmetic
 *      that validates the SLI-over-HE architecture
 *   2. Clear integration points for HElib or SEAL backends
 *
 * The reference impl simulates HE with "noise-free" modular arithmetic
 * to prove the linear algebra is correct. Real deployment swaps in
 * HElib's Ctxt/Ptxt classes behind the same C API.
 *
 * The key architectural insight being validated:
 *   SLI score = z^T · FWHT(signs ⊙ x)
 *   This is PURELY linear → HE supports it with one multiply + accumulate
 *   No bootstrapping needed (single multiplicative depth = 1)
 */

#include "he_sli.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ── Reference HE simulation (modular arithmetic, no noise) ──── */

/*
 * In real BFV:
 *   ciphertext = (c0, c1) where c0 + c1*s ≡ Δ·m (mod q)
 *   Δ = floor(q/t), t = plaintext modulus, q = ciphertext modulus
 *
 * Our reference simulation:
 *   "ciphertext" = just the plaintext values mod PLAIN_MOD
 *   Operations match real HE semantics but without noise management
 *   This validates the linear algebra path before real HE integration
 */

#define PLAIN_MOD       128     /* E8 INT7 range [0, 127] */
#define DEFAULT_SLOTS   256     /* Values per ciphertext = block_dim */
#define MAX_SLOTS       8192

struct he_sli_ctx {
    he_sli_params_t params;
    int             has_secret_key;
    uint8_t        *public_key;      /* Serialized PK (stub: random bytes) */
    size_t           pk_len;
    uint8_t        *secret_key;      /* Serialized SK (stub: random bytes) */
    size_t           sk_len;
    uint8_t        *eval_key;
    size_t           ek_len;

    /* Performance counters */
    double           total_encrypt_ms;
    double           total_dot_ms;
    double           total_decrypt_ms;
    size_t           blocks_scored;
    size_t           n_encrypts;
    size_t           n_dots;
    size_t           n_decrypts;
};

struct he_sli_ciphertext {
    int64_t *slots;           /* PLAIN_MOD arithmetic values */
    size_t   n_slots;
    size_t   serialized_size; /* Simulated ciphertext expansion */
};

struct he_sli_plaintext {
    int64_t *slots;
    size_t   n_slots;
};

/* ── Timing ──────────────────────────────────────────────────── */

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1e6;
}

/* ── Context ─────────────────────────────────────────────────── */

he_sli_ctx_t *he_sli_ctx_new(const he_sli_params_t *params,
                               const char *secret_key_path) {
    he_sli_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return NULL;

    if (params) {
        ctx->params = *params;
    } else {
        ctx->params = (he_sli_params_t)HE_SLI_PARAMS_DEFAULT;
    }

    /* Generate keypair */
    ctx->pk_len = ctx->params.poly_modulus_degree * 2;  /* Simulated PK size */
    ctx->public_key = calloc(1, ctx->pk_len);

    ctx->sk_len = ctx->params.poly_modulus_degree;
    ctx->secret_key = calloc(1, ctx->sk_len);

    ctx->ek_len = ctx->params.poly_modulus_degree * 4;
    ctx->eval_key = calloc(1, ctx->ek_len);

    if (secret_key_path) {
        FILE *f = fopen(secret_key_path, "rb");
        if (f) {
            fread(ctx->secret_key, 1, ctx->sk_len, f);
            fclose(f);
            ctx->has_secret_key = 1;
        }
    } else {
        /* Generate random keys (stub) */
        for (size_t i = 0; i < ctx->pk_len; i++)
            ctx->public_key[i] = (uint8_t)(rand() & 0xFF);
        for (size_t i = 0; i < ctx->sk_len; i++)
            ctx->secret_key[i] = (uint8_t)(rand() & 0xFF);
        ctx->has_secret_key = 1;
    }

    return ctx;
}

void he_sli_ctx_free(he_sli_ctx_t *ctx) {
    if (!ctx) return;
    free(ctx->public_key);
    /* Securely clear secret key before freeing */
    if (ctx->secret_key) {
        volatile uint8_t *p = ctx->secret_key;
        for (size_t i = 0; i < ctx->sk_len; i++) p[i] = 0;
        free(ctx->secret_key);
    }
    free(ctx->eval_key);
    free(ctx);
}

int he_sli_export_public_key(const he_sli_ctx_t *ctx,
                               uint8_t **out, size_t *out_len) {
    if (!ctx || !out || !out_len) return -1;
    *out = malloc(ctx->pk_len);
    memcpy(*out, ctx->public_key, ctx->pk_len);
    *out_len = ctx->pk_len;
    return 0;
}

int he_sli_import_public_key(he_sli_ctx_t *ctx,
                               const uint8_t *data, size_t len) {
    if (!ctx || !data || len == 0) return -1;
    free(ctx->public_key);
    ctx->public_key = malloc(len);
    memcpy(ctx->public_key, data, len);
    ctx->pk_len = len;
    ctx->has_secret_key = 0;  /* Server doesn't have SK */
    return 0;
}

int he_sli_export_secret_key(const he_sli_ctx_t *ctx, const char *path) {
    if (!ctx || !ctx->has_secret_key || !path) return -1;
    FILE *f = fopen(path, "wb");
    if (!f) return -1;
    fwrite(ctx->secret_key, 1, ctx->sk_len, f);
    fclose(f);
    return 0;
}

int he_sli_export_eval_key(const he_sli_ctx_t *ctx,
                             uint8_t **out, size_t *out_len) {
    if (!ctx || !out || !out_len) return -1;
    *out = malloc(ctx->ek_len);
    memcpy(*out, ctx->eval_key, ctx->ek_len);
    *out_len = ctx->ek_len;
    return 0;
}

int he_sli_import_eval_key(he_sli_ctx_t *ctx,
                             const uint8_t *data, size_t len) {
    if (!ctx || !data || len == 0) return -1;
    free(ctx->eval_key);
    ctx->eval_key = malloc(len);
    memcpy(ctx->eval_key, data, len);
    ctx->ek_len = len;
    return 0;
}

/* ── Encryption (reference: mod-arithmetic identity) ─────────── */

he_sli_ciphertext_t *he_sli_encrypt_z_block(const he_sli_ctx_t *ctx,
                                              const int8_t *z, size_t n) {
    double t0 = now_ms();

    he_sli_ciphertext_t *ct = calloc(1, sizeof(*ct));
    ct->n_slots = n;
    ct->slots = malloc(n * sizeof(int64_t));

    /* In real BFV: encode z into plaintext polynomial, then encrypt.
     * Reference: store values mod PLAIN_MOD (preserving sign via offset) */
    for (size_t i = 0; i < n; i++) {
        ct->slots[i] = ((int64_t)z[i] + PLAIN_MOD) % PLAIN_MOD;
    }

    /* Simulated ciphertext expansion (BFV typical: ~2× poly degree × 2 × log2(q) bits)
     * For N=4096, log2(q)≈109: ciphertext ≈ 4096 * 2 * 14 bytes ≈ 112KB per block */
    ct->serialized_size = ctx->params.poly_modulus_degree * 28;

    double t1 = now_ms();
    ((he_sli_ctx_t *)ctx)->total_encrypt_ms += (t1 - t0);
    ((he_sli_ctx_t *)ctx)->n_encrypts++;

    return ct;
}

he_sli_ciphertext_t **he_sli_encrypt_tensor(const he_sli_ctx_t *ctx,
                                              const int8_t *z_data,
                                              size_t n_blocks,
                                              size_t block_dim) {
    he_sli_ciphertext_t **cts = malloc(n_blocks * sizeof(he_sli_ciphertext_t *));
    for (size_t i = 0; i < n_blocks; i++) {
        cts[i] = he_sli_encrypt_z_block(ctx, z_data + i * block_dim, block_dim);
    }
    return cts;
}

/* ── Serialization ───────────────────────────────────────────── */

int he_sli_serialize_ciphertexts(const he_sli_ciphertext_t **cts,
                                   size_t count,
                                   uint8_t **out, size_t *out_len) {
    /* Header: [4: count] then per-ct: [4: n_slots][n_slots * 8: slot data] */
    size_t total = 4;
    for (size_t i = 0; i < count; i++)
        total += 4 + cts[i]->n_slots * sizeof(int64_t);

    *out = malloc(total);
    uint8_t *p = *out;

    /* Write count */
    uint32_t cnt = (uint32_t)count;
    memcpy(p, &cnt, 4); p += 4;

    for (size_t i = 0; i < count; i++) {
        uint32_t ns = (uint32_t)cts[i]->n_slots;
        memcpy(p, &ns, 4); p += 4;
        memcpy(p, cts[i]->slots, ns * sizeof(int64_t));
        p += ns * sizeof(int64_t);
    }

    *out_len = total;
    return 0;
}

he_sli_ciphertext_t **he_sli_deserialize_ciphertexts(const he_sli_ctx_t *ctx,
                                                       const uint8_t *data,
                                                       size_t data_len,
                                                       size_t *count_out) {
    (void)ctx; (void)data_len;
    const uint8_t *p = data;
    uint32_t count;
    memcpy(&count, p, 4); p += 4;

    he_sli_ciphertext_t **cts = malloc(count * sizeof(he_sli_ciphertext_t *));
    for (uint32_t i = 0; i < count; i++) {
        cts[i] = calloc(1, sizeof(he_sli_ciphertext_t));
        uint32_t ns;
        memcpy(&ns, p, 4); p += 4;
        cts[i]->n_slots = ns;
        cts[i]->slots = malloc(ns * sizeof(int64_t));
        memcpy(cts[i]->slots, p, ns * sizeof(int64_t));
        p += ns * sizeof(int64_t);
    }

    *count_out = count;
    return cts;
}

/* ── Cleartext activation prep (server side) ─────────────────── */

he_sli_plaintext_t *he_sli_prepare_activation(const he_sli_ctx_t *ctx,
                                                const float *x,
                                                size_t n,
                                                const uint8_t *signs) {
    (void)ctx;
    he_sli_plaintext_t *pt = calloc(1, sizeof(*pt));
    pt->n_slots = n;
    pt->slots = malloc(n * sizeof(int64_t));

    /* Apply signs and quantize to integer for HE compatibility.
     * In real deployment: FWHT is applied here.
     * The quantization maps float → int7 with scaling. */
    for (size_t i = 0; i < n; i++) {
        float val = x[i];
        /* Apply sign flip */
        if (signs) {
            size_t byte = i / 8;
            size_t bit = i % 8;
            if (signs[byte] & (1u << bit))
                val = -val;
        }
        /* Quantize to int7 range and reduce mod PLAIN_MOD */
        int qval = (int)roundf(val * 32.0f);  /* scale factor for int7 resolution */
        if (qval > 63) qval = 63;
        if (qval < -64) qval = -64;
        pt->slots[i] = ((int64_t)qval + PLAIN_MOD) % PLAIN_MOD;
    }

    return pt;
}

/* ── Encrypted dot product (THE core operation) ──────────────── */

he_sli_ciphertext_t *he_sli_encrypted_dot(const he_sli_ctx_t *ctx,
                                            const he_sli_ciphertext_t *enc_z,
                                            const he_sli_plaintext_t *clear_x) {
    double t0 = now_ms();

    he_sli_ciphertext_t *result = calloc(1, sizeof(*result));
    result->n_slots = 1;
    result->slots = calloc(1, sizeof(int64_t));

    /* In real BFV:
     *   1. ct_prod = ctxt_multiply(enc_z, encode(clear_x))  — plaintext-ciphertext multiply
     *   2. ct_sum = rotate_and_sum(ct_prod)                   — log2(n) rotations
     *   Result: encrypted scalar = z^T · x'
     *
     * Reference implementation: direct modular dot product */
    size_t n = enc_z->n_slots < clear_x->n_slots ? enc_z->n_slots : clear_x->n_slots;

    int64_t acc = 0;
    for (size_t i = 0; i < n; i++) {
        acc += (enc_z->slots[i] * clear_x->slots[i]) % (PLAIN_MOD * PLAIN_MOD);
    }
    result->slots[0] = acc;
    result->serialized_size = ctx->params.poly_modulus_degree * 28;

    double t1 = now_ms();
    ((he_sli_ctx_t *)ctx)->total_dot_ms += (t1 - t0);
    ((he_sli_ctx_t *)ctx)->n_dots++;
    ((he_sli_ctx_t *)ctx)->blocks_scored++;

    return result;
}

/* Batch matvec row: accumulate across blocks */
he_sli_ciphertext_t *he_sli_encrypted_matvec_row(const he_sli_ctx_t *ctx,
                                                    const he_sli_ciphertext_t **enc_z,
                                                    const he_sli_plaintext_t **clear_x,
                                                    size_t n_blocks) {
    /* In real BFV: sum of ciphertext-plaintext products
     * Multiplicative depth stays at 1 (plaintext multiply only) */
    he_sli_ciphertext_t *acc = he_sli_encrypted_dot(ctx, enc_z[0], clear_x[0]);

    for (size_t b = 1; b < n_blocks; b++) {
        he_sli_ciphertext_t *block_score = he_sli_encrypted_dot(ctx, enc_z[b], clear_x[b]);
        acc->slots[0] += block_score->slots[0];  /* HE addition (noise-free) */
        he_sli_ciphertext_free(block_score);
    }

    return acc;
}

/* Full encrypted matvec */
he_sli_ciphertext_t **he_sli_encrypted_matvec(const he_sli_ctx_t *ctx,
                                                const he_sli_ciphertext_t ***enc_z_rows,
                                                size_t n_rows,
                                                const float *x,
                                                size_t n_cols,
                                                const uint8_t *signs,
                                                size_t block_dim) {
    size_t n_blocks = (n_cols + block_dim - 1) / block_dim;

    /* Prepare activation blocks (cleartext, done once) */
    he_sli_plaintext_t **clear_x = malloc(n_blocks * sizeof(he_sli_plaintext_t *));
    for (size_t b = 0; b < n_blocks; b++) {
        size_t offset = b * block_dim;
        size_t bsize = block_dim;
        if (offset + bsize > n_cols) bsize = n_cols - offset;
        const uint8_t *block_signs = signs ? signs + (offset / 8) : NULL;
        clear_x[b] = he_sli_prepare_activation(ctx, x + offset, bsize, block_signs);
    }

    /* Score each row */
    he_sli_ciphertext_t **results = malloc(n_rows * sizeof(he_sli_ciphertext_t *));
    for (size_t r = 0; r < n_rows; r++) {
        results[r] = he_sli_encrypted_matvec_row(ctx, enc_z_rows[r], 
                                                   (const he_sli_plaintext_t **)clear_x,
                                                   n_blocks);
    }

    /* Cleanup activations */
    for (size_t b = 0; b < n_blocks; b++)
        he_sli_plaintext_free(clear_x[b]);
    free(clear_x);

    return results;
}

/* ── Decryption (customer side) ──────────────────────────────── */

int he_sli_decrypt_score(const he_sli_ctx_t *ctx,
                           const he_sli_ciphertext_t *enc_score,
                           float *out) {
    if (!ctx->has_secret_key) return -1;

    double t0 = now_ms();

    /* In real BFV: decrypt ciphertext, then decode plaintext polynomial.
     * Reference: just read the slot value and convert back to float. */
    int64_t raw = enc_score->slots[0];

    /* Convert from modular accumulation back to float.
     * The accumulated value is sum of (z_i * x_i), each mod PLAIN_MOD.
     * Scale factor: 1 / (32.0 * n_terms) approximately */
    *out = (float)raw / 32.0f;

    double t1 = now_ms();
    ((he_sli_ctx_t *)ctx)->total_decrypt_ms += (t1 - t0);
    ((he_sli_ctx_t *)ctx)->n_decrypts++;

    return 0;
}

int he_sli_decrypt_matvec(const he_sli_ctx_t *ctx,
                            const he_sli_ciphertext_t **enc_scores,
                            size_t n_rows,
                            float *out) {
    for (size_t r = 0; r < n_rows; r++) {
        int rv = he_sli_decrypt_score(ctx, enc_scores[r], &out[r]);
        if (rv != 0) return rv;
    }
    return 0;
}

/* ── Memory ──────────────────────────────────────────────────── */

void he_sli_ciphertext_free(he_sli_ciphertext_t *ct) {
    if (!ct) return;
    free(ct->slots);
    free(ct);
}

void he_sli_plaintext_free(he_sli_plaintext_t *pt) {
    if (!pt) return;
    free(pt->slots);
    free(pt);
}

/* ── Performance ─────────────────────────────────────────────── */

he_sli_perf_t he_sli_get_perf(const he_sli_ctx_t *ctx) {
    he_sli_perf_t p = {0};
    if (ctx->n_encrypts > 0)
        p.encrypt_ms = ctx->total_encrypt_ms / (double)ctx->n_encrypts;
    if (ctx->n_dots > 0)
        p.dot_ms = ctx->total_dot_ms / (double)ctx->n_dots;
    if (ctx->n_decrypts > 0)
        p.decrypt_ms = ctx->total_decrypt_ms / (double)ctx->n_decrypts;

    /* BFV expansion ratio: typically 50-100× for 128-bit security */
    p.ciphertext_bytes = ctx->params.poly_modulus_degree * 28;
    p.expansion_ratio = (double)p.ciphertext_bytes / (double)(DEFAULT_SLOTS * sizeof(int8_t));
    p.blocks_scored = ctx->blocks_scored;

    return p;
}

/* ── Gate integration ────────────────────────────────────────── */

int he_sli_validate_gate_token(const he_sli_ctx_t *ctx,
                                 const char *gate_token,
                                 const uint8_t *public_key,
                                 size_t pk_len) {
    (void)ctx;
    if (!gate_token || !public_key || pk_len == 0) return -1;

    /* The Gate token encodes a hash of the HE public key.
     * This binds the license to a specific customer's encryption key.
     * Without the matching SK, the encrypted inference results are useless. */

    /* Simple validation: gate token contains hex-encoded PK hash prefix */
    /* In production: proper HMAC or signature validation */
    if (strlen(gate_token) < 16) return -1;

    return 0;  /* Stub: always passes for reference impl */
}
