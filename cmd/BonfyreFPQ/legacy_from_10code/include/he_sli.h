/*
 * he_sli.h — Homomorphic Encryption bridge for Spectral Lattice Inference
 *
 * The key insight: SLI operates in FWHT domain where the scoring is:
 *   score = z^T · FWHT(signs ⊙ x)
 *
 * This is a linear operation (dot product + element-wise multiply).
 * HE natively supports linear operations efficiently.
 *
 * Architecture:
 *   1. Customer encrypts their fine-tuned z vectors (model weights in FWHT domain)
 *   2. Bonfyre receives encrypted z vectors — never sees plaintext
 *   3. SLI scoring runs entirely on encrypted z:
 *      - FWHT(signs ⊙ x) computed in cleartext (activation side)
 *      - Encrypted dot product: HE_dot(enc_z, clear_x')
 *      - Result is encrypted score — customer decrypts with their secret key
 *   4. BonfyreMeter still tracks compute (number of blocks scored)
 *   5. BonfyreGate validates the HE public key as the license credential
 *
 * This gives Bonfyre a moat: inference-as-a-service on customer weights
 * without ever seeing the weights. No other quantization format supports this.
 *
 * HE scheme: BFV (Brakerski/Fan-Vercauteren) — best for integer arithmetic
 * on quantized coordinates. E8 coords are int7 → HE plaintext modulus = 128.
 *
 * Build with HElib: -lhelib -lntl -lgmp
 * Or with SEAL: -lseal (Microsoft SEAL, C++ but has C-compatible ABI)
 *
 * This header provides a C interface that wraps either backend.
 */
#ifndef HE_SLI_H
#define HE_SLI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── HE scheme parameters ────────────────────────────────────── */

typedef struct {
    uint32_t poly_modulus_degree;  /* Ring dimension: 4096 or 8192 */
    uint32_t plain_modulus;        /* 128 for E8 INT7, 256 for INT8 */
    uint32_t security_level;       /* 128-bit target security */
} he_sli_params_t;

/* Default params tuned for E8 SLI scoring */
#define HE_SLI_PARAMS_DEFAULT { \
    .poly_modulus_degree = 4096, \
    .plain_modulus = 128,        \
    .security_level = 128        \
}

/* ── Opaque handles ──────────────────────────────────────────── */

typedef struct he_sli_ctx        he_sli_ctx_t;      /* HE context + keys */
typedef struct he_sli_ciphertext he_sli_ciphertext_t; /* Encrypted vector */
typedef struct he_sli_plaintext  he_sli_plaintext_t;  /* Cleartext vector */

/* ── Key management ──────────────────────────────────────────── */

/* Create HE context with given parameters.
 * If secret_key_path is NULL, generates new keypair.
 * If non-NULL, loads existing secret key (customer side only). */
he_sli_ctx_t *he_sli_ctx_new(const he_sli_params_t *params,
                               const char *secret_key_path);

void he_sli_ctx_free(he_sli_ctx_t *ctx);

/* Export public key (customer sends to Bonfyre server) */
int he_sli_export_public_key(const he_sli_ctx_t *ctx,
                               uint8_t **out, size_t *out_len);

/* Import public key (Bonfyre server receives from customer) */
int he_sli_import_public_key(he_sli_ctx_t *ctx,
                               const uint8_t *data, size_t len);

/* Export secret key (customer stores locally) */
int he_sli_export_secret_key(const he_sli_ctx_t *ctx,
                               const char *path);

/* Export evaluation (relinearization) key for server-side ops */
int he_sli_export_eval_key(const he_sli_ctx_t *ctx,
                             uint8_t **out, size_t *out_len);

int he_sli_import_eval_key(he_sli_ctx_t *ctx,
                             const uint8_t *data, size_t len);

/* ── Encryption (customer side) ──────────────────────────────── */

/* Encrypt a block of E8 coordinates (z vector, 256 int8 values).
 * This is the prepared SLI z vector — already FWHT-transformed. */
he_sli_ciphertext_t *he_sli_encrypt_z_block(const he_sli_ctx_t *ctx,
                                              const int8_t *z, size_t n);

/* Encrypt all z vectors for a tensor (n_blocks × block_dim) */
he_sli_ciphertext_t **he_sli_encrypt_tensor(const he_sli_ctx_t *ctx,
                                              const int8_t *z_data,
                                              size_t n_blocks,
                                              size_t block_dim);

/* Serialize encrypted tensor for transmission */
int he_sli_serialize_ciphertexts(const he_sli_ciphertext_t **cts,
                                   size_t count,
                                   uint8_t **out, size_t *out_len);

/* Deserialize on server side */
he_sli_ciphertext_t **he_sli_deserialize_ciphertexts(const he_sli_ctx_t *ctx,
                                                       const uint8_t *data,
                                                       size_t data_len,
                                                       size_t *count_out);

/* ── Encrypted SLI scoring (server side, no secret key) ──────── */

/* Prepare cleartext activation for scoring.
 * This applies: signs ⊙ x → FWHT → plaintext encoding */
he_sli_plaintext_t *he_sli_prepare_activation(const he_sli_ctx_t *ctx,
                                                const float *x,
                                                size_t n,
                                                const uint8_t *signs);

/* Encrypted dot product: score = enc_z^T · clear_x'
 * Result is an encrypted scalar.
 * This is the SLI hot path — runs on Bonfyre server without
 * ever seeing the plaintext z vectors. */
he_sli_ciphertext_t *he_sli_encrypted_dot(const he_sli_ctx_t *ctx,
                                            const he_sli_ciphertext_t *enc_z,
                                            const he_sli_plaintext_t *clear_x);

/* Batch encrypted matvec: y = Σ enc_z[b] · clear_x'[b]
 * Accumulates across all blocks for one output row. */
he_sli_ciphertext_t *he_sli_encrypted_matvec_row(const he_sli_ctx_t *ctx,
                                                    const he_sli_ciphertext_t **enc_z,
                                                    const he_sli_plaintext_t **clear_x,
                                                    size_t n_blocks);

/* Full encrypted matvec: y[rows] = W_enc × x
 * Returns array of encrypted outputs (one per row). */
he_sli_ciphertext_t **he_sli_encrypted_matvec(const he_sli_ctx_t *ctx,
                                                const he_sli_ciphertext_t ***enc_z_rows,
                                                size_t n_rows,
                                                const float *x,
                                                size_t n_cols,
                                                const uint8_t *signs,
                                                size_t block_dim);

/* ── Decryption (customer side) ──────────────────────────────── */

/* Decrypt a single score.
 * Only the customer (with secret key) can do this. */
int he_sli_decrypt_score(const he_sli_ctx_t *ctx,
                           const he_sli_ciphertext_t *enc_score,
                           float *out);

/* Decrypt full matvec output (n_rows scores) */
int he_sli_decrypt_matvec(const he_sli_ctx_t *ctx,
                            const he_sli_ciphertext_t **enc_scores,
                            size_t n_rows,
                            float *out);

/* Serialize encrypted scores for return to customer */
int he_sli_serialize_scores(const he_sli_ciphertext_t **scores,
                              size_t n,
                              uint8_t **out, size_t *out_len);

/* ── Memory management ───────────────────────────────────────── */

void he_sli_ciphertext_free(he_sli_ciphertext_t *ct);
void he_sli_plaintext_free(he_sli_plaintext_t *pt);

/* ── Performance metrics ─────────────────────────────────────── */

typedef struct {
    double encrypt_ms;      /* Per-block encryption time */
    double dot_ms;          /* Per-block encrypted dot product time */
    double decrypt_ms;      /* Per-score decryption time */
    size_t ciphertext_bytes; /* Bytes per encrypted block */
    double expansion_ratio;  /* ciphertext_bytes / plaintext_bytes */
    size_t blocks_scored;    /* For BonfyreMeter integration */
} he_sli_perf_t;

/* Get perf counters (reset on each call) */
he_sli_perf_t he_sli_get_perf(const he_sli_ctx_t *ctx);

/* ── Integration with BonfyreGate ────────────────────────────── */

/* Validate that an HE public key matches a Gate license token.
 * The public key hash IS the credential — no separate auth needed. */
int he_sli_validate_gate_token(const he_sli_ctx_t *ctx,
                                 const char *gate_token,
                                 const uint8_t *public_key,
                                 size_t pk_len);

#ifdef __cplusplus
}
#endif

#endif /* HE_SLI_H */
