/*
 * bf_fountain.h — O(N) rateless fountain encoder/decoder
 *
 * Implements a Luby Transform (LT) fountain code with robust soliton
 * distribution.  Any K-of-N encoded symbols can reconstruct the
 * original K source blocks — no coordination between senders required.
 *
 * Use cases:
 *   - BonfyreSwarm: spray fountain symbols from multiple seeders
 *   - BonfyreDistribute: QUIC multistream with symbol interleaving
 *   - Lossy links: encode once, decode from any subset
 *
 * Wire format:
 *   [4B magic "BFLT"] [4B K] [4B symbol_len] [4B seed]
 *   [symbol_len bytes payload]
 *
 * Each symbol carries its PRNG seed so the decoder can reconstruct
 * the degree + neighbor list independently.
 */

#ifndef BF_FOUNTAIN_H
#define BF_FOUNTAIN_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define BF_FOUNTAIN_MAGIC   0x42464C54  /* "BFLT" */
#define BF_FOUNTAIN_OVERHEAD 1.05       /* 5% overhead target */

/* ── Symbol (encoded unit) ───────────────────────────────────── */

typedef struct {
    uint32_t    seed;           /* PRNG seed → degree + neighbors   */
    uint32_t    degree;         /* Number of source blocks XOR'd    */
    uint32_t   *neighbors;     /* Indices of source blocks          */
    uint8_t    *data;           /* XOR'd payload (symbol_len bytes) */
} bf_fountain_symbol_t;

/* ── Encoder ─────────────────────────────────────────────────── */

typedef struct bf_fountain_enc bf_fountain_enc_t;

/*
 * Create encoder from source data.
 *   data:       raw input buffer
 *   data_len:   input length
 *   block_size: size of each source block (e.g. 1024, 4096)
 *               last block is zero-padded
 * Returns NULL on failure.
 */
bf_fountain_enc_t *bf_fountain_enc_new(const uint8_t *data, size_t data_len,
                                        size_t block_size);

void bf_fountain_enc_free(bf_fountain_enc_t *enc);

/* Number of source blocks K */
uint32_t bf_fountain_enc_k(const bf_fountain_enc_t *enc);

/* Block size */
size_t bf_fountain_enc_block_size(const bf_fountain_enc_t *enc);

/*
 * Generate the next fountain symbol.  Caller must free sym->data
 * and sym->neighbors when done (or use bf_fountain_symbol_free).
 * Each call produces a unique symbol — can call indefinitely.
 */
int bf_fountain_enc_next(bf_fountain_enc_t *enc, bf_fountain_symbol_t *sym);

void bf_fountain_symbol_free(bf_fountain_symbol_t *sym);

/*
 * Serialize a symbol to wire format.
 * Returns bytes written, or 0 on error.
 */
size_t bf_fountain_symbol_pack(const bf_fountain_symbol_t *sym,
                                uint32_t K, size_t block_size,
                                uint8_t *out, size_t out_cap);

/*
 * Deserialize a symbol from wire format.
 * Returns 0 on success, -1 on error.  Fills K_out, block_size_out.
 */
int bf_fountain_symbol_unpack(const uint8_t *buf, size_t buf_len,
                               bf_fountain_symbol_t *sym,
                               uint32_t *K_out, size_t *block_size_out);

/* ── Decoder ─────────────────────────────────────────────────── */

typedef struct bf_fountain_dec bf_fountain_dec_t;

/*
 * Create decoder expecting K source blocks of block_size each.
 */
bf_fountain_dec_t *bf_fountain_dec_new(uint32_t K, size_t block_size);

void bf_fountain_dec_free(bf_fountain_dec_t *dec);

/*
 * Feed a symbol to the decoder.
 * Returns:
 *   1  — decoded! Call bf_fountain_dec_result() to get output.
 *   0  — symbol absorbed, still need more.
 *  -1  — error.
 */
int bf_fountain_dec_add(bf_fountain_dec_t *dec, const bf_fountain_symbol_t *sym);

/* Is decoding complete? */
int bf_fountain_dec_complete(const bf_fountain_dec_t *dec);

/* Number of symbols absorbed so far */
uint32_t bf_fountain_dec_received(const bf_fountain_dec_t *dec);

/*
 * Get decoded output.  Only valid after dec_complete() returns 1.
 * data_out: pointer to internal buffer (valid until dec_free).
 * Returns actual data length (may be < K * block_size due to padding).
 */
int bf_fountain_dec_result(bf_fountain_dec_t *dec,
                            const uint8_t **data_out, size_t *len_out);

#ifdef __cplusplus
}
#endif

#endif /* BF_FOUNTAIN_H */
