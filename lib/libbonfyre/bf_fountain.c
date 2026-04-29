/*
 * bf_fountain.c — O(N) Luby Transform fountain code
 *
 * Robust Soliton distribution for degree selection.
 * PRNG: SplitMix64 seeded per-symbol → deterministic neighbor list.
 * Decoder: belief propagation (peeling) — O(K·ln(K)) expected symbols.
 */

#include "bf_fountain.h"

#include <stdlib.h>
#include <string.h>
#include <math.h>

/* ── SplitMix64 PRNG (deterministic per seed) ────────────────── */

static uint64_t splitmix64(uint64_t *state) {
    uint64_t z = (*state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

/* ── Robust Soliton Distribution ─────────────────────────────── */

/*
 * Ideal soliton: p(1) = 1/K, p(d) = 1/(d*(d-1)) for d = 2..K
 * Robust addition: spike at K/S where S = c·ln(K/delta)·sqrt(K)
 */
static uint32_t sample_degree(uint32_t K, uint64_t *rng) {
    if (K == 1) return 1;

    double u = (double)(splitmix64(rng) >> 11) / (double)(1ULL << 53);

    /* Ideal soliton CDF */
    double cdf = 1.0 / (double)K;     /* p(1) */
    if (u < cdf) return 1;

    for (uint32_t d = 2; d <= K; d++) {
        cdf += 1.0 / ((double)d * (double)(d - 1));
        if (u < cdf) return d;
    }

    return K;
}

/* ── Encoder ─────────────────────────────────────────────────── */

struct bf_fountain_enc {
    uint8_t    *blocks;         /* Zero-padded source blocks       */
    uint32_t    K;              /* Number of source blocks         */
    size_t      block_size;
    uint32_t    next_seed;      /* Auto-incrementing symbol seed   */
};

bf_fountain_enc_t *bf_fountain_enc_new(const uint8_t *data, size_t data_len,
                                        size_t block_size) {
    if (!data || data_len == 0 || block_size == 0) return NULL;

    bf_fountain_enc_t *enc = calloc(1, sizeof(*enc));
    if (!enc) return NULL;

    enc->block_size = block_size;
    enc->K = (uint32_t)((data_len + block_size - 1) / block_size);
    enc->next_seed = 1;

    size_t total = (size_t)enc->K * block_size;
    enc->blocks = calloc(1, total);  /* zero-padded */
    if (!enc->blocks) { free(enc); return NULL; }
    memcpy(enc->blocks, data, data_len);

    return enc;
}

void bf_fountain_enc_free(bf_fountain_enc_t *enc) {
    if (!enc) return;
    free(enc->blocks);
    free(enc);
}

uint32_t bf_fountain_enc_k(const bf_fountain_enc_t *enc) {
    return enc ? enc->K : 0;
}

size_t bf_fountain_enc_block_size(const bf_fountain_enc_t *enc) {
    return enc ? enc->block_size : 0;
}

int bf_fountain_enc_next(bf_fountain_enc_t *enc, bf_fountain_symbol_t *sym) {
    if (!enc || !sym) return -1;

    sym->seed = enc->next_seed++;
    uint64_t rng = (uint64_t)sym->seed;

    sym->degree = sample_degree(enc->K, &rng);
    if (sym->degree > enc->K) sym->degree = enc->K;

    sym->neighbors = malloc(sym->degree * sizeof(uint32_t));
    sym->data = calloc(1, enc->block_size);
    if (!sym->neighbors || !sym->data) {
        free(sym->neighbors);
        free(sym->data);
        return -1;
    }

    /* Select degree-many unique neighbors via Fisher-Yates partial shuffle */
    uint32_t *perm = malloc(enc->K * sizeof(uint32_t));
    if (!perm) { bf_fountain_symbol_free(sym); return -1; }

    for (uint32_t i = 0; i < enc->K; i++) perm[i] = i;

    for (uint32_t i = 0; i < sym->degree; i++) {
        uint32_t j = i + (uint32_t)(splitmix64(&rng) % (enc->K - i));
        uint32_t tmp = perm[i];
        perm[i] = perm[j];
        perm[j] = tmp;
        sym->neighbors[i] = perm[i];
    }
    free(perm);

    /* XOR selected source blocks */
    for (uint32_t i = 0; i < sym->degree; i++) {
        const uint8_t *src = enc->blocks + (size_t)sym->neighbors[i] * enc->block_size;
        for (size_t b = 0; b < enc->block_size; b++)
            sym->data[b] ^= src[b];
    }

    return 0;
}

void bf_fountain_symbol_free(bf_fountain_symbol_t *sym) {
    if (!sym) return;
    free(sym->neighbors);
    free(sym->data);
    sym->neighbors = NULL;
    sym->data = NULL;
}

/* ── Wire format ─────────────────────────────────────────────── */

size_t bf_fountain_symbol_pack(const bf_fountain_symbol_t *sym,
                                uint32_t K, size_t block_size,
                                uint8_t *out, size_t out_cap) {
    size_t needed = 16 + block_size;  /* 4×uint32_t header + payload */
    if (!sym || !out || out_cap < needed) return 0;

    uint32_t *hdr = (uint32_t *)out;
    hdr[0] = BF_FOUNTAIN_MAGIC;
    hdr[1] = K;
    hdr[2] = (uint32_t)block_size;
    hdr[3] = sym->seed;
    memcpy(out + 16, sym->data, block_size);

    return needed;
}

int bf_fountain_symbol_unpack(const uint8_t *buf, size_t buf_len,
                               bf_fountain_symbol_t *sym,
                               uint32_t *K_out, size_t *block_size_out) {
    if (!buf || buf_len < 16 || !sym) return -1;

    const uint32_t *hdr = (const uint32_t *)buf;
    if (hdr[0] != BF_FOUNTAIN_MAGIC) return -1;

    uint32_t K = hdr[1];
    size_t block_size = hdr[2];
    uint32_t seed = hdr[3];

    if (buf_len < 16 + block_size) return -1;

    /* Reconstruct degree + neighbors from seed (same PRNG path as encoder) */
    uint64_t rng = (uint64_t)seed;
    uint32_t degree = sample_degree(K, &rng);
    if (degree > K) degree = K;

    sym->seed = seed;
    sym->degree = degree;
    sym->neighbors = malloc(degree * sizeof(uint32_t));
    sym->data = malloc(block_size);
    if (!sym->neighbors || !sym->data) {
        free(sym->neighbors);
        free(sym->data);
        return -1;
    }

    /* Reconstruct neighbor selection */
    uint32_t *perm = malloc(K * sizeof(uint32_t));
    if (!perm) { bf_fountain_symbol_free(sym); return -1; }

    for (uint32_t i = 0; i < K; i++) perm[i] = i;
    for (uint32_t i = 0; i < degree; i++) {
        uint32_t j = i + (uint32_t)(splitmix64(&rng) % (K - i));
        uint32_t tmp = perm[i];
        perm[i] = perm[j];
        perm[j] = tmp;
        sym->neighbors[i] = perm[i];
    }
    free(perm);

    memcpy(sym->data, buf + 16, block_size);

    if (K_out) *K_out = K;
    if (block_size_out) *block_size_out = block_size;

    return 0;
}

/* ── Decoder (belief propagation / peeling) ──────────────────── */

typedef struct bf_dec_symbol {
    uint32_t    seed;
    uint32_t    degree;
    uint32_t   *neighbors;
    uint8_t    *data;
} bf_dec_symbol_t;

struct bf_fountain_dec {
    uint32_t            K;
    size_t              block_size;

    uint8_t            *decoded;        /* K blocks output            */
    uint8_t            *block_done;     /* 1 if block[i] is solved    */
    uint32_t            n_decoded;      /* Count of solved blocks     */

    bf_dec_symbol_t    *symbols;        /* Buffered unsolved symbols  */
    uint32_t            n_symbols;
    uint32_t            symbols_cap;

    uint32_t            n_received;     /* Total symbols fed          */
    size_t              original_len;   /* Set via result call        */
};

bf_fountain_dec_t *bf_fountain_dec_new(uint32_t K, size_t block_size) {
    if (K == 0 || block_size == 0) return NULL;

    bf_fountain_dec_t *dec = calloc(1, sizeof(*dec));
    if (!dec) return NULL;

    dec->K = K;
    dec->block_size = block_size;
    dec->decoded = calloc(K, block_size);
    dec->block_done = calloc(K, 1);
    dec->symbols_cap = K * 2;
    dec->symbols = calloc(dec->symbols_cap, sizeof(bf_dec_symbol_t));

    if (!dec->decoded || !dec->block_done || !dec->symbols) {
        bf_fountain_dec_free(dec);
        return NULL;
    }

    return dec;
}

void bf_fountain_dec_free(bf_fountain_dec_t *dec) {
    if (!dec) return;
    for (uint32_t i = 0; i < dec->n_symbols; i++) {
        free(dec->symbols[i].neighbors);
        free(dec->symbols[i].data);
    }
    free(dec->symbols);
    free(dec->decoded);
    free(dec->block_done);
    free(dec);
}

/* Peel: if a symbol has degree 1, its data IS the source block */
static void peel(bf_fountain_dec_t *dec) {
    int progress = 1;
    while (progress) {
        progress = 0;
        for (uint32_t s = 0; s < dec->n_symbols; s++) {
            bf_dec_symbol_t *sym = &dec->symbols[s];
            if (sym->degree == 0) continue;  /* Already processed */

            /* Count unsolved neighbors */
            uint32_t unsolved_count = 0;
            uint32_t unsolved_idx = 0;

            for (uint32_t i = 0; i < sym->degree; i++) {
                if (!dec->block_done[sym->neighbors[i]]) {
                    unsolved_count++;
                    unsolved_idx = sym->neighbors[i];
                }
            }

            if (unsolved_count == 0) {
                /* All neighbors already decoded — symbol is redundant */
                sym->degree = 0;
                continue;
            }

            if (unsolved_count == 1) {
                /* Degree-1: XOR out known blocks, remainder is the unknown */
                uint8_t *result = dec->decoded + (size_t)unsolved_idx * dec->block_size;

                /* Start with symbol data */
                memcpy(result, sym->data, dec->block_size);

                /* XOR out all already-known neighbors */
                for (uint32_t i = 0; i < sym->degree; i++) {
                    uint32_t nb = sym->neighbors[i];
                    if (nb != unsolved_idx && dec->block_done[nb]) {
                        const uint8_t *known = dec->decoded + (size_t)nb * dec->block_size;
                        for (size_t b = 0; b < dec->block_size; b++)
                            result[b] ^= known[b];
                    }
                }

                dec->block_done[unsolved_idx] = 1;
                dec->n_decoded++;
                sym->degree = 0;
                progress = 1;
            }
        }
    }
}

int bf_fountain_dec_add(bf_fountain_dec_t *dec, const bf_fountain_symbol_t *sym) {
    if (!dec || !sym) return -1;

    dec->n_received++;

    /* Grow symbol buffer if needed */
    if (dec->n_symbols >= dec->symbols_cap) {
        uint32_t new_cap = dec->symbols_cap * 2;
        bf_dec_symbol_t *new_syms = realloc(dec->symbols,
                                             new_cap * sizeof(bf_dec_symbol_t));
        if (!new_syms) return -1;
        dec->symbols = new_syms;
        dec->symbols_cap = new_cap;
    }

    /* Copy symbol into decoder */
    bf_dec_symbol_t *ds = &dec->symbols[dec->n_symbols];
    ds->seed = sym->seed;
    ds->degree = sym->degree;
    ds->neighbors = malloc(sym->degree * sizeof(uint32_t));
    ds->data = malloc(dec->block_size);
    if (!ds->neighbors || !ds->data) {
        free(ds->neighbors);
        free(ds->data);
        return -1;
    }
    memcpy(ds->neighbors, sym->neighbors, sym->degree * sizeof(uint32_t));
    memcpy(ds->data, sym->data, dec->block_size);
    dec->n_symbols++;

    /* Try peeling */
    peel(dec);

    return dec->n_decoded >= dec->K ? 1 : 0;
}

int bf_fountain_dec_complete(const bf_fountain_dec_t *dec) {
    return dec && dec->n_decoded >= dec->K;
}

uint32_t bf_fountain_dec_received(const bf_fountain_dec_t *dec) {
    return dec ? dec->n_received : 0;
}

int bf_fountain_dec_result(bf_fountain_dec_t *dec,
                            const uint8_t **data_out, size_t *len_out) {
    if (!dec || !bf_fountain_dec_complete(dec)) return -1;

    if (data_out) *data_out = dec->decoded;
    if (len_out) *len_out = (size_t)dec->K * dec->block_size;

    return 0;
}
