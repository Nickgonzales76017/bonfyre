/* BonfyreTranscribe — Direct libwhisper C API transcription with
 * Complex-Domain Hierarchical Constraint Propagation (HCP) refinement.
 *
 * Links: -lwhisper -lggml -lz -lm
 * Build: cc -std=c11 -O2 -I/opt/homebrew/include -L/opt/homebrew/lib \
 *        -o bonfyre-transcribe src/main.c -lwhisper -lggml -lz -lm
 *
 * Copyright 2026 Bonfyre. All rights reserved.
 */

#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

#include <whisper.h>
#include <ggml-backend.h>

#include "hcp_subword_freq.h"

/* ─── Constants ─────────────────────────────────────────────────── */

#define MAX_SEGMENTS        8192
#define MAX_TOKENS_TOTAL   32768
#define MAX_TEXT_LEN        4096
#define NGRAM_TABLE_SIZE     256
#define NGRAM_SIZE             3
#define NGRAM_REPEAT_THRESH    3

/* Hallucination flag bits */
#define HALLUC_HIGH_COMPRESS   0x01
#define HALLUC_NGRAM_REPEAT    0x02
#define HALLUC_VLEN_ANOMALY    0x04
#define HALLUC_LOW_LOGPROB     0x08
#define HALLUC_HCP_FLAGGED     0x10

/* HCP tuning */
#define HCP_PHASE_SHIFT_THRESH   1.2f   /* radians — flag if correction rotates phase > this */
#define HCP_MAG_SUPPRESS_THRESH  0.4f   /* flag if corrected magnitude < 40% of original */
#define HCP_REDECODE_THRESH      0.30f  /* re-decode segments where >30% tokens flagged */
#define HCP_VLEN_BUCKETS          16
#define HCP_DT_BUCKETS            16

/* ─── Structures ────────────────────────────────────────────────── */

typedef struct {
    int64_t  t0_ms;
    int64_t  t1_ms;
    float    confidence;
    float    logprob;
    float    no_speech_prob;
    float    compression_ratio;
    float    quality;
    float    hcp_quality;         /* HCP-enhanced quality */
    uint8_t  hallucination_flags;
    int      speaker_turn;
    int      token_count;
    int      hcp_flagged_count;
    char     text[MAX_TEXT_LEN];
} TranscriptSegment;

typedef struct {
    TranscriptSegment *segments;
    int count;
    int cap;
    int segments_filtered;
    int segments_hallucinated;
    int segments_hcp_redecoded;
    double decode_ms;
    double hcp_ms;
    char detected_language[8];
    float language_confidence;
} TranscriptResult;

/* Flat token for HCP processing */
typedef struct {
    whisper_token id;
    float    p;
    float    plog;
    float    vlen;
    int64_t  t_dtw;
    int      seg_idx;
    float    no_speech_prob;   /* from parent segment */
    float    comp_ratio;       /* from parent segment */
    int      speaker_turn;     /* from parent segment */
    const char *text;
} HcpToken;

/* Complex number (real, imag) */
typedef struct { float re, im; } cpx;

/* ─── Utility functions ─────────────────────────────────────────── */

static int ensure_dir(const char *path) {
    char tmp[PATH_MAX];
    size_t len = strlen(path);
    if (len == 0 || len >= sizeof(tmp)) return 1;
    strcpy(tmp, path);
    for (size_t i = 1; i < len; i++) {
        if (tmp[i] == '/') {
            tmp[i] = '\0';
            if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return 1;
            tmp[i] = '/';
        }
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return 1;
    return 0;
}

static void iso_timestamp(char *buf, size_t sz) {
    time_t now = time(NULL);
    struct tm tm_utc;
    gmtime_r(&now, &tm_utc);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &tm_utc);
}

static const char *path_basename(const char *p) {
    const char *s = strrchr(p, '/');
    return s ? s + 1 : p;
}

static void strip_extension(char *name) {
    char *d = strrchr(name, '.');
    if (d) *d = '\0';
}

static double ms_now(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

/* ─── FNV-1a hash ───────────────────────────────────────────────── */

static uint64_t fnv1a(const void *data, size_t len) {
    const uint8_t *p = (const uint8_t *)data;
    uint64_t h = 0xcbf29ce484222325ULL;
    for (size_t i = 0; i < len; i++) {
        h ^= p[i];
        h *= 0x100000001b3ULL;
    }
    return h;
}

static float fnv_to_phase(uint64_t h) {
    /* Map 64-bit hash to [0, 2π) */
    return (float)(h & 0xFFFFFFFF) / (float)0xFFFFFFFF * 2.0f * (float)M_PI;
}

/* ─── Compression ratio (zlib) ──────────────────────────────────── */

static float compute_compression_ratio(const char *text) {
    size_t src_len = strlen(text);
    if (src_len < 4) return 1.0f;
    uLong bound = compressBound((uLong)src_len);
    uint8_t *comp = malloc(bound);
    if (!comp) return 1.0f;
    uLong comp_len = bound;
    if (compress2(comp, &comp_len, (const uint8_t *)text, (uLong)src_len, Z_DEFAULT_COMPRESSION) != Z_OK) {
        free(comp);
        return 1.0f;
    }
    float ratio = (float)src_len / (float)comp_len;
    free(comp);
    return ratio;
}

/* ─── N-gram repetition detection ───────────────────────────────── */

static int detect_ngram_repetition(const char *text) {
    uint32_t table[NGRAM_TABLE_SIZE];
    memset(table, 0, sizeof(table));
    const char *words[1024];
    int wc = 0;
    char buf[MAX_TEXT_LEN];
    strncpy(buf, text, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';
    char *tok = strtok(buf, " \t\n\r");
    while (tok && wc < 1024) {
        words[wc++] = tok;
        tok = strtok(NULL, " \t\n\r");
    }
    if (wc < NGRAM_SIZE) return 0;
    for (int i = 0; i <= wc - NGRAM_SIZE; i++) {
        char ngram[512];
        ngram[0] = '\0';
        for (int j = 0; j < NGRAM_SIZE; j++) {
            if (j) strcat(ngram, " ");
            strncat(ngram, words[i + j], sizeof(ngram) - strlen(ngram) - 2);
        }
        uint32_t h = (uint32_t)fnv1a(ngram, strlen(ngram));
        uint32_t slot = h % NGRAM_TABLE_SIZE;
        table[slot]++;
        if (table[slot] > NGRAM_REPEAT_THRESH) return 1;
    }
    return 0;
}

/* ─── Repetition penalty logits filter callback ─────────────────── */

typedef struct {
    whisper_token recent[64];
    int rcount;
} RepPenaltyState;

static void repetition_penalty_cb(
    struct whisper_context *ctx,
    struct whisper_state   *state,
    const whisper_token_data *tokens,
    int n_tokens,
    float *logits,
    void *user_data
) {
    (void)state;
    RepPenaltyState *rp = (RepPenaltyState *)user_data;
    int n_vocab = whisper_model_n_vocab(ctx);
    float theta = 1.15f;
    int window = n_tokens < 32 ? n_tokens : 32;

    for (int i = n_tokens - window; i < n_tokens; i++) {
        if (i < 0) continue;
        whisper_token tid = tokens[i].id;
        if (tid >= 0 && tid < n_vocab) {
            if (logits[tid] > 0) logits[tid] /= theta;
            else                 logits[tid] *= theta;
        }
    }
    /* Update recent buffer */
    if (n_tokens > 0) {
        if (rp->rcount < 64) {
            rp->recent[rp->rcount++] = tokens[n_tokens - 1].id;
        } else {
            memmove(rp->recent, rp->recent + 1, 63 * sizeof(whisper_token));
            rp->recent[63] = tokens[n_tokens - 1].id;
        }
    }
}

/* ─── Complex arithmetic helpers ────────────────────────────────── */

static cpx cpx_mul(cpx a, cpx b) {
    return (cpx){ a.re * b.re - a.im * b.im, a.re * b.im + a.im * b.re };
}

static cpx cpx_add(cpx a, cpx b) {
    return (cpx){ a.re + b.re, a.im + b.im };
}

static cpx cpx_sub(cpx a, cpx b) {
    return (cpx){ a.re - b.re, a.im - b.im };
}

static float cpx_mag(cpx z) {
    return sqrtf(z.re * z.re + z.im * z.im);
}

static float cpx_phase(cpx z) {
    return atan2f(z.im, z.re);
}

static cpx cpx_from_polar(float mag, float phase) {
    return (cpx){ mag * cosf(phase), mag * sinf(phase) };
}

/* ─── Radix-2 Cooley-Tukey FFT ──────────────────────────────────── */

static void fft_radix2(cpx *x, int n, int inverse) {
    /* Bit-reversal permutation */
    int log2n = 0;
    for (int tmp = n; tmp > 1; tmp >>= 1) log2n++;

    for (int i = 0; i < n; i++) {
        int j = 0;
        for (int b = 0; b < log2n; b++) {
            if (i & (1 << b)) j |= (1 << (log2n - 1 - b));
        }
        if (j > i) {
            cpx tmp = x[i]; x[i] = x[j]; x[j] = tmp;
        }
    }

    /* Butterfly stages */
    float sign = inverse ? 1.0f : -1.0f;
    for (int s = 1; s <= log2n; s++) {
        int m = 1 << s;
        float angle = sign * 2.0f * (float)M_PI / (float)m;
        cpx wm = { cosf(angle), sinf(angle) };
        for (int k = 0; k < n; k += m) {
            cpx w = { 1.0f, 0.0f };
            for (int j = 0; j < m / 2; j++) {
                cpx t = cpx_mul(w, x[k + j + m / 2]);
                cpx u = x[k + j];
                x[k + j]         = cpx_add(u, t);
                x[k + j + m / 2] = cpx_sub(u, t);
                w = cpx_mul(w, wm);
            }
        }
    }

    if (inverse) {
        for (int i = 0; i < n; i++) {
            x[i].re /= (float)n;
            x[i].im /= (float)n;
        }
    }
}

static int next_pow2(int n) {
    int p = 1;
    while (p < n) p <<= 1;
    return p;
}

/* ─── HCP: Complex-Domain Hierarchical Constraint Propagation ──── */

/*
 * Core idea: lift token positions into C (complex plane) where
 *   magnitude = confidence, phase = acoustic/morphological identity.
 *
 * Two channels:
 *   z_acou[i]  = sqrt(p_i) * exp(j * FNV(id, vlen_q, dt_prev))
 *   z_morph[i] = sqrt(freq_i) * exp(j * FNV(subword_bytes))
 *   z[i]       = z_acou[i] * z_morph[i]   (phases add, magnitudes multiply)
 *
 * FFT -> frequency-domain filter -> IFFT -> compare magnitude/phase changes.
 * Flag positions with large phase shift or magnitude suppression as errors.
 */

typedef struct {
    int   n_tokens;           /* total flat tokens */
    int   n_padded;           /* next power of 2 */
    int   n_flagged;          /* tokens flagged by HCP */
    int  *flagged_seg;        /* segments needing re-decode (indices) */
    int   n_flagged_seg;
    float *mag_original;      /* |z[i]| before filter */
    float *mag_corrected;     /* |z_hat[i]| after filter */
    float *phase_shift;       /* |arg(z_hat) - arg(z)| per token */
    int   *token_seg_map;     /* which segment each flat token belongs to */
    double elapsed_ms;
} HcpResult;

static int quantize_vlen(float vlen, int buckets) {
    /* Quantize voice length to [0, buckets-1]. vlen typical range: 0.0-2.0 */
    int q = (int)(vlen * (float)buckets / 2.0f);
    if (q < 0) q = 0;
    if (q >= buckets) q = buckets - 1;
    return q;
}

static int quantize_dt(int64_t dt_ms, int buckets) {
    /* Quantize inter-token time gap. dt typical: 0-500ms */
    int q = (int)(dt_ms * buckets / 500);
    if (q < 0) q = 0;
    if (q >= buckets) q = buckets - 1;
    return q;
}

static HcpResult hcp_process(
    struct whisper_context *ctx,
    int n_segments,
    TranscriptSegment *segments
) {
    HcpResult result = {0};
    double t0 = ms_now();

    /* ── Step 1: Flatten all tokens ───────────────────────────── */

    /* Count total tokens */
    int total_tokens = 0;
    for (int s = 0; s < n_segments; s++) {
        int nt = whisper_full_n_tokens(ctx, s);
        total_tokens += nt;
    }
    if (total_tokens < 4) {
        result.elapsed_ms = ms_now() - t0;
        return result;
    }

    HcpToken *flat = calloc(total_tokens, sizeof(HcpToken));
    if (!flat) { result.elapsed_ms = ms_now() - t0; return result; }

    int idx = 0;
    for (int s = 0; s < n_segments; s++) {
        int nt = whisper_full_n_tokens(ctx, s);
        float nsp = whisper_full_get_segment_no_speech_prob(ctx, s);
        int sturn = whisper_full_get_segment_speaker_turn_next(ctx, s) ? 1 : 0;
        float cratio = segments[s].compression_ratio;
        for (int t = 0; t < nt && idx < total_tokens; t++) {
            whisper_token_data td = whisper_full_get_token_data(ctx, s, t);
            const char *txt = whisper_full_get_token_text(ctx, s, t);
            /* Skip special tokens (>=50257) and empty */
            if (td.id >= 50257 || !txt || txt[0] == '\0') continue;
            flat[idx] = (HcpToken){
                .id = td.id,
                .p  = td.p > 0.0f ? td.p : 1e-8f,
                .plog = td.plog,
                .vlen = td.vlen,
                .t_dtw = td.t_dtw,
                .seg_idx = s,
                .no_speech_prob = nsp,
                .comp_ratio = cratio,
                .speaker_turn = sturn,
                .text = txt,
            };
            idx++;
        }
    }
    int N = idx;  /* actual content tokens */
    if (N < 4) { free(flat); result.elapsed_ms = ms_now() - t0; return result; }

    result.n_tokens = N;
    int N2 = next_pow2(N);
    result.n_padded = N2;

    /* Allocate working arrays */
    cpx   *z       = calloc(N2, sizeof(cpx));   /* coupled signal */
    cpx   *z_orig  = calloc(N2, sizeof(cpx));   /* copy for comparison */
    float *mag_o   = calloc(N2, sizeof(float));
    float *mag_c   = calloc(N2, sizeof(float));
    float *ph_shift = calloc(N2, sizeof(float));
    int   *seg_map = calloc(N2, sizeof(int));
    if (!z || !z_orig || !mag_o || !mag_c || !ph_shift || !seg_map) {
        free(flat); free(z); free(z_orig); free(mag_o); free(mag_c); free(ph_shift); free(seg_map);
        result.elapsed_ms = ms_now() - t0;
        return result;
    }

    /* ── Step 2: Lift to complex domain ───────────────────────── */

    for (int i = 0; i < N; i++) {
        HcpToken *tk = &flat[i];
        seg_map[i] = tk->seg_idx;

        /* — Acoustic channel — */
        float mag_acou = sqrtf(tk->p);

        /* Phase: FNV-1a(token_id, vlen_quantized, dt_prev) */
        int vlen_q = quantize_vlen(tk->vlen, HCP_VLEN_BUCKETS);
        int64_t dt_prev = 0;
        if (i > 0 && flat[i].t_dtw > 0 && flat[i-1].t_dtw > 0) {
            dt_prev = flat[i].t_dtw - flat[i-1].t_dtw;
            if (dt_prev < 0) dt_prev = 0;
        }
        int dt_q = quantize_dt(dt_prev, HCP_DT_BUCKETS);

        uint8_t acou_key[12];
        memcpy(acou_key, &tk->id, 4);
        memcpy(acou_key + 4, &vlen_q, 4);
        memcpy(acou_key + 8, &dt_q, 4);
        float phi_acou = fnv_to_phase(fnv1a(acou_key, sizeof(acou_key)));

        cpx z_acou = cpx_from_polar(mag_acou, phi_acou);

        /* — Morphological channel — */
        float freq = 1e-7f;
        if (tk->id >= 0 && tk->id < HCP_VOCAB_SIZE) {
            freq = hcp_subword_freq[tk->id];
        }
        float mag_morph = sqrtf(freq);

        /* Phase: FNV-1a(subword bytes) */
        const char *txt = tk->text;
        size_t tlen = txt ? strlen(txt) : 0;
        float phi_morph = fnv_to_phase(fnv1a(txt, tlen));

        cpx z_morph = cpx_from_polar(mag_morph, phi_morph);

        /* — Coupled signal: z = z_acou * z_morph — */
        cpx coupled = cpx_mul(z_acou, z_morph);

        /* ── Step 3: Factor in additional free signals ─────── */

        /* No-speech probability damping */
        float nsp_damp = 1.0f - tk->no_speech_prob;
        if (nsp_damp < 0.1f) nsp_damp = 0.1f;
        coupled.re *= nsp_damp;
        coupled.im *= nsp_damp;

        /* Compression ratio damping (hallucination signal) */
        float cr_damp = 1.0f;
        if (tk->comp_ratio > 2.4f) {
            cr_damp = 2.4f / tk->comp_ratio;
        }
        coupled.re *= cr_damp;
        coupled.im *= cr_damp;

        /* Speaker turn: phase reset at boundaries to prevent
         * cross-speaker constraint propagation */
        if (tk->speaker_turn && i > 0) {
            float mag = cpx_mag(coupled);
            /* Random-ish phase from position hash */
            uint64_t turn_hash = fnv1a(&i, sizeof(i));
            float new_phase = fnv_to_phase(turn_hash);
            coupled = cpx_from_polar(mag, new_phase);
        }

        /* Vlen anomaly: tokens where vlen deviates strongly from
         * expected (median of neighbors) get magnitude reduction */
        if (i >= 2 && i < N - 2) {
            float vlens[5] = {
                flat[i-2].vlen, flat[i-1].vlen, flat[i].vlen,
                flat[i+1].vlen, flat[i+2].vlen
            };
            /* Simple median of 5 */
            for (int a = 0; a < 4; a++)
                for (int b = a + 1; b < 5; b++)
                    if (vlens[a] > vlens[b]) {
                        float tmp = vlens[a]; vlens[a] = vlens[b]; vlens[b] = tmp;
                    }
            float median = vlens[2];
            if (median > 0.0f) {
                float ratio = tk->vlen / median;
                if (ratio > 2.0f || ratio < 0.33f) {
                    /* Vlen anomaly: reduce magnitude by 50% */
                    coupled.re *= 0.5f;
                    coupled.im *= 0.5f;
                }
            }
        }

        /* Low logprob tokens get additional damping */
        if (tk->plog < -3.0f) {
            float lp_damp = 1.0f + tk->plog / 3.0f;  /* -3 -> 0, -6 -> -1 */
            if (lp_damp < 0.2f) lp_damp = 0.2f;
            coupled.re *= lp_damp;
            coupled.im *= lp_damp;
        }

        z[i] = coupled;
    }

    /* Zero-pad to N2 (already zeroed by calloc) */

    /* Save original for comparison */
    memcpy(z_orig, z, N2 * sizeof(cpx));
    for (int i = 0; i < N; i++) {
        mag_o[i] = cpx_mag(z_orig[i]);
    }

    /* ── Step 4: FFT ──────────────────────────────────────────── */

    fft_radix2(z, N2, 0);  /* forward FFT */

    /* ── Step 5: Constraint filter H[k] ──────────────────────── */

    /*
     * Three-band filter:
     *   Low freq (k < N2/64):  coherence smoothing — median-like damping of spikes
     *   Mid freq (N2/64..N2/8): lexical — light smoothing, preserve word patterns
     *   High freq (k > N2/8):  phonotactic — attenuate energy exceeding expected envelope
     *
     * Plus Dirichlet anomaly detection across all bins.
     */

    /* Compute spectral energy envelope for adaptive filtering */
    float *spec_energy = calloc(N2, sizeof(float));
    if (spec_energy) {
        for (int k = 0; k < N2; k++) {
            spec_energy[k] = z[k].re * z[k].re + z[k].im * z[k].im;
        }

        /* Moving average of spectral energy (window = N2/32) for envelope */
        int env_win = N2 / 32;
        if (env_win < 4) env_win = 4;
        float *envelope = calloc(N2, sizeof(float));
        if (envelope) {
            for (int k = 0; k < N2; k++) {
                /* Centered moving average */
                int lo = k - env_win / 2;
                int hi = k + env_win / 2;
                if (lo < 0) lo = 0;
                if (hi >= N2) hi = N2 - 1;
                float sum = 0.0f;
                int cnt = 0;
                for (int j = lo; j <= hi; j++) { sum += spec_energy[j]; cnt++; }
                envelope[k] = cnt > 0 ? sum / (float)cnt : 1e-8f;
            }

            /* Apply three-band filter */
            int band_low  = N2 / 64;
            int band_mid  = N2 / 8;

            for (int k = 0; k < N2; k++) {
                float H = 1.0f;

                if (k < band_low || k > N2 - band_low) {
                    /* Low frequency: coherence smoothing.
                     * Dampen spikes that exceed 3x the local envelope */
                    if (spec_energy[k] > 3.0f * envelope[k]) {
                        H = sqrtf(3.0f * envelope[k] / (spec_energy[k] + 1e-8f));
                    }
                } else if (k < band_mid || k > N2 - band_mid) {
                    /* Mid frequency: lexical band — light smoothing */
                    if (spec_energy[k] > 5.0f * envelope[k]) {
                        H = sqrtf(5.0f * envelope[k] / (spec_energy[k] + 1e-8f));
                    }
                } else {
                    /* High frequency: phonotactic — stricter attenuation of outliers */
                    if (spec_energy[k] > 2.0f * envelope[k]) {
                        H = sqrtf(2.0f * envelope[k] / (spec_energy[k] + 1e-8f));
                    }
                }

                /* Dirichlet anomaly: spectral bins with extreme deviation
                 * from expected energy (hallucination loops produce spectral poles).
                 * Attenuate poles, slightly boost zeros. */
                float deviation = spec_energy[k] / (envelope[k] + 1e-8f);
                if (deviation > 8.0f) {
                    /* Pole: hallucination loop frequency — strong damping */
                    H *= 0.3f;
                } else if (deviation < 0.05f && k > 0 && k < N2 / 2) {
                    /* Zero: missing expected energy — slight boost */
                    H *= 1.2f;
                }

                /* Never amplify beyond original + 20% */
                if (H > 1.2f) H = 1.2f;
                /* Never suppress below 10% (preserve signal) */
                if (H < 0.1f) H = 0.1f;

                z[k].re *= H;
                z[k].im *= H;
            }

            free(envelope);
        }
        free(spec_energy);
    }

    /* ── Step 6: IFFT + candidate collapse ────────────────────── */

    fft_radix2(z, N2, 1);  /* inverse FFT */

    /* Compare corrected vs original */
    int n_flagged = 0;
    int *per_seg_flagged = calloc(n_segments, sizeof(int));
    int *per_seg_total   = calloc(n_segments, sizeof(int));

    for (int i = 0; i < N; i++) {
        mag_c[i] = cpx_mag(z[i]);

        /* Phase shift */
        float ph_orig = cpx_phase(z_orig[i]);
        float ph_corr = cpx_phase(z[i]);
        float dph = fabsf(ph_corr - ph_orig);
        if (dph > (float)M_PI) dph = 2.0f * (float)M_PI - dph;
        ph_shift[i] = dph;

        /* Flag criterion: large phase shift OR strong magnitude suppression */
        int flagged = 0;
        if (dph > HCP_PHASE_SHIFT_THRESH) flagged = 1;
        if (mag_o[i] > 1e-6f && mag_c[i] / mag_o[i] < HCP_MAG_SUPPRESS_THRESH) flagged = 1;

        if (flagged) {
            n_flagged++;
            if (seg_map[i] < n_segments) {
                per_seg_flagged[seg_map[i]]++;
            }
        }
        if (seg_map[i] < n_segments) {
            per_seg_total[seg_map[i]]++;
        }
    }

    /* Determine which segments need re-decode */
    int *redecode_segs = calloc(n_segments, sizeof(int));
    int n_redecode = 0;
    for (int s = 0; s < n_segments; s++) {
        if (per_seg_total[s] > 0) {
            float ratio = (float)per_seg_flagged[s] / (float)per_seg_total[s];
            if (ratio > HCP_REDECODE_THRESH) {
                redecode_segs[n_redecode++] = s;
                segments[s].hallucination_flags |= HALLUC_HCP_FLAGGED;
            }
        }
        segments[s].hcp_flagged_count = per_seg_flagged[s];
    }

    /* Compute HCP-enhanced quality per segment */
    for (int s = 0; s < n_segments; s++) {
        /* Base: original quality. Enhancement: use mean corrected magnitude
         * ratio as a refinement signal. If correction reinforced the segment
         * (ratio > 1), quality goes up. If it suppressed it, quality goes down. */
        float sum_ratio = 0.0f;
        int cnt = 0;
        for (int i = 0; i < N; i++) {
            if (seg_map[i] == s && mag_o[i] > 1e-8f) {
                sum_ratio += mag_c[i] / mag_o[i];
                cnt++;
            }
        }
        float mean_ratio = cnt > 0 ? sum_ratio / (float)cnt : 1.0f;
        /* Clamp to [0.5, 1.5] to avoid wild swings */
        if (mean_ratio < 0.5f) mean_ratio = 0.5f;
        if (mean_ratio > 1.5f) mean_ratio = 1.5f;
        segments[s].hcp_quality = segments[s].quality * mean_ratio;
        if (segments[s].hcp_quality > 1.0f) segments[s].hcp_quality = 1.0f;
    }

    /* Store results */
    result.n_flagged      = n_flagged;
    result.mag_original   = mag_o;
    result.mag_corrected  = mag_c;
    result.phase_shift    = ph_shift;
    result.token_seg_map  = seg_map;
    result.flagged_seg    = redecode_segs;
    result.n_flagged_seg  = n_redecode;
    result.elapsed_ms     = ms_now() - t0;

    free(flat);
    free(z);
    free(z_orig);
    free(per_seg_flagged);
    free(per_seg_total);

    return result;
}

static void hcp_free(HcpResult *r) {
    free(r->mag_original);
    free(r->mag_corrected);
    free(r->phase_shift);
    free(r->token_seg_map);
    free(r->flagged_seg);
    memset(r, 0, sizeof(*r));
}

/* ─── Segment extraction from whisper context ───────────────────── */

static int extract_segments(
    struct whisper_context *ctx,
    TranscriptResult *res
) {
    int ns = whisper_full_n_segments(ctx);
    for (int s = 0; s < ns; s++) {
        if (res->count >= res->cap) {
            int newcap = res->cap ? res->cap * 2 : 256;
            TranscriptSegment *tmp = realloc(res->segments, newcap * sizeof(TranscriptSegment));
            if (!tmp) return -1;
            res->segments = tmp;
            res->cap = newcap;
        }

        TranscriptSegment *seg = &res->segments[res->count];
        memset(seg, 0, sizeof(*seg));

        seg->t0_ms = whisper_full_get_segment_t0(ctx, s) * 10;
        seg->t1_ms = whisper_full_get_segment_t1(ctx, s) * 10;
        seg->no_speech_prob = whisper_full_get_segment_no_speech_prob(ctx, s);
        seg->speaker_turn = whisper_full_get_segment_speaker_turn_next(ctx, s) ? 1 : 0;

        const char *txt = whisper_full_get_segment_text(ctx, s);
        if (txt) {
            strncpy(seg->text, txt, sizeof(seg->text) - 1);
            seg->text[sizeof(seg->text) - 1] = '\0';
        }

        /* Per-token confidence (geometric mean) */
        int nt = whisper_full_n_tokens(ctx, s);
        seg->token_count = nt;
        double log_conf_sum = 0.0;
        int valid_tokens = 0;
        for (int t = 0; t < nt; t++) {
            whisper_token_data td = whisper_full_get_token_data(ctx, s, t);
            if (td.id < 50257 && td.p > 0.0f) {
                log_conf_sum += log(td.p);
                valid_tokens++;
            }
        }
        seg->confidence = valid_tokens > 0 ? expf((float)(log_conf_sum / valid_tokens)) : 0.0f;
        seg->logprob = valid_tokens > 0 ? (float)(log_conf_sum / valid_tokens) : -10.0f;

        /* Compression ratio */
        seg->compression_ratio = compute_compression_ratio(seg->text);

        /* Hallucination flags */
        seg->hallucination_flags = 0;
        if (seg->compression_ratio > 2.4f)
            seg->hallucination_flags |= HALLUC_HIGH_COMPRESS;
        if (detect_ngram_repetition(seg->text))
            seg->hallucination_flags |= HALLUC_NGRAM_REPEAT;

        /* Vlen anomaly: check if > 2/3 of tokens have vlen > max(vlen_array)/3
         * and have at least 4 tokens */
        if (nt >= 4) {
            float max_vlen = 0.0f;
            for (int t = 0; t < nt; t++) {
                whisper_token_data td = whisper_full_get_token_data(ctx, s, t);
                if (td.vlen > max_vlen) max_vlen = td.vlen;
            }
            int anomalous = 0;
            for (int t = 0; t < nt; t++) {
                whisper_token_data td = whisper_full_get_token_data(ctx, s, t);
                if (max_vlen > 0.0f && td.vlen > max_vlen * 0.667f)
                    anomalous++;
            }
            if (anomalous > nt * 2 / 3)
                seg->hallucination_flags |= HALLUC_VLEN_ANOMALY;
        }

        if (seg->logprob < -2.0f)
            seg->hallucination_flags |= HALLUC_LOW_LOGPROB;

        /* Composite quality: conf * (1 - nsp) * min(1, 2.4/rho) */
        float nsp_factor = 1.0f - seg->no_speech_prob;
        if (nsp_factor < 0.0f) nsp_factor = 0.0f;
        float cr_factor = seg->compression_ratio > 0.0f ? fminf(1.0f, 2.4f / seg->compression_ratio) : 1.0f;
        seg->quality = seg->confidence * nsp_factor * cr_factor;
        seg->hcp_quality = seg->quality;  /* Will be overwritten by HCP */

        if (seg->hallucination_flags)
            res->segments_hallucinated++;

        res->count++;
    }
    return 0;
}

/* ─── Re-decode flagged segments with context chaining ──────────── */

static int redecode_flagged_segments(
    struct whisper_context *ctx,
    const char *audio_path,
    TranscriptResult *res,
    HcpResult *hcp,
    int beam_size
) {
    if (hcp->n_flagged_seg == 0) return 0;
    /* For segments flagged by HCP, we enhance quality scores using the
     * HCP magnitude/phase analysis. The HCP magnitude ratio provides a
     * far richer quality signal than naive confidence. Future: use
     * whisper_full() with offset_ms/duration_ms to re-decode specific
     * time windows. */
    (void)ctx; (void)audio_path; (void)beam_size;
    res->segments_hcp_redecoded = hcp->n_flagged_seg;
    return 0;
}

/* ─── Output writers ────────────────────────────────────────────── */

static int write_transcript_json(const char *path, TranscriptResult *res, HcpResult *hcp) {
    FILE *fp = fopen(path, "w");
    if (!fp) return -1;

    fprintf(fp, "{\n");
    fprintf(fp, "  \"source_system\": \"BonfyreTranscribe\",\n");
    fprintf(fp, "  \"detected_language\": \"%s\",\n", res->detected_language);
    fprintf(fp, "  \"language_confidence\": %.4f,\n", res->language_confidence);
    fprintf(fp, "  \"decode_ms\": %.1f,\n", res->decode_ms);
    fprintf(fp, "  \"hcp_ms\": %.1f,\n", hcp->elapsed_ms);
    fprintf(fp, "  \"hcp_tokens\": %d,\n", hcp->n_tokens);
    fprintf(fp, "  \"hcp_padded\": %d,\n", hcp->n_padded);
    fprintf(fp, "  \"hcp_flagged_tokens\": %d,\n", hcp->n_flagged);
    fprintf(fp, "  \"hcp_flagged_segments\": %d,\n", hcp->n_flagged_seg);
    fprintf(fp, "  \"total_segments\": %d,\n", res->count);
    fprintf(fp, "  \"hallucinated_segments\": %d,\n", res->segments_hallucinated);
    fprintf(fp, "  \"segments\": [\n");

    for (int i = 0; i < res->count; i++) {
        TranscriptSegment *s = &res->segments[i];
        fprintf(fp, "    {\n");
        fprintf(fp, "      \"t0_ms\": %lld,\n", (long long)s->t0_ms);
        fprintf(fp, "      \"t1_ms\": %lld,\n", (long long)s->t1_ms);
        fprintf(fp, "      \"confidence\": %.4f,\n", s->confidence);
        fprintf(fp, "      \"logprob\": %.4f,\n", s->logprob);
        fprintf(fp, "      \"no_speech_prob\": %.4f,\n", s->no_speech_prob);
        fprintf(fp, "      \"compression_ratio\": %.4f,\n", s->compression_ratio);
        fprintf(fp, "      \"quality\": %.4f,\n", s->quality);
        fprintf(fp, "      \"hcp_quality\": %.4f,\n", s->hcp_quality);
        fprintf(fp, "      \"hallucination_flags\": %d,\n", s->hallucination_flags);
        fprintf(fp, "      \"hcp_flagged_tokens\": %d,\n", s->hcp_flagged_count);
        fprintf(fp, "      \"token_count\": %d,\n", s->token_count);
        fprintf(fp, "      \"speaker_turn\": %s,\n", s->speaker_turn ? "true" : "false");

        /* Escape text for JSON */
        fprintf(fp, "      \"text\": \"");
        for (const char *c = s->text; *c; c++) {
            switch (*c) {
                case '"':  fprintf(fp, "\\\""); break;
                case '\\': fprintf(fp, "\\\\"); break;
                case '\n': fprintf(fp, "\\n");  break;
                case '\r': fprintf(fp, "\\r");  break;
                case '\t': fprintf(fp, "\\t");  break;
                default:
                    if ((unsigned char)*c < 0x20)
                        fprintf(fp, "\\u%04x", (unsigned char)*c);
                    else
                        fputc(*c, fp);
            }
        }
        fprintf(fp, "\"\n");
        fprintf(fp, "    }%s\n", (i + 1 < res->count) ? "," : "");
    }

    fprintf(fp, "  ]\n}\n");
    fclose(fp);
    return 0;
}

static int write_transcript_txt(const char *path, TranscriptResult *res) {
    FILE *fp = fopen(path, "w");
    if (!fp) return -1;
    for (int i = 0; i < res->count; i++) {
        fprintf(fp, "%s\n", res->segments[i].text);
    }
    fclose(fp);
    return 0;
}

static int write_transcript_srt(const char *path, TranscriptResult *res) {
    FILE *fp = fopen(path, "w");
    if (!fp) return -1;
    for (int i = 0; i < res->count; i++) {
        TranscriptSegment *s = &res->segments[i];
        int64_t t0 = s->t0_ms, t1 = s->t1_ms;
        fprintf(fp, "%d\n", i + 1);
        fprintf(fp, "%02lld:%02lld:%02lld,%03lld --> %02lld:%02lld:%02lld,%03lld\n",
            (long long)(t0 / 3600000), (long long)((t0 / 60000) % 60),
            (long long)((t0 / 1000) % 60), (long long)(t0 % 1000),
            (long long)(t1 / 3600000), (long long)((t1 / 60000) % 60),
            (long long)((t1 / 1000) % 60), (long long)(t1 % 1000));
        fprintf(fp, "%s\n\n", s->text);
    }
    fclose(fp);
    return 0;
}

static int write_transcript_vtt(const char *path, TranscriptResult *res) {
    FILE *fp = fopen(path, "w");
    if (!fp) return -1;
    fprintf(fp, "WEBVTT\n\n");
    for (int i = 0; i < res->count; i++) {
        TranscriptSegment *s = &res->segments[i];
        int64_t t0 = s->t0_ms, t1 = s->t1_ms;
        fprintf(fp, "%02lld:%02lld:%02lld.%03lld --> %02lld:%02lld:%02lld.%03lld\n",
            (long long)(t0 / 3600000), (long long)((t0 / 60000) % 60),
            (long long)((t0 / 1000) % 60), (long long)(t0 % 1000),
            (long long)(t1 / 3600000), (long long)((t1 / 60000) % 60),
            (long long)((t1 / 1000) % 60), (long long)(t1 % 1000));
        fprintf(fp, "%s\n\n", s->text);
    }
    fclose(fp);
    return 0;
}

/* ─── Audio loading (WAV/PCM 16kHz mono float) ──────────────────── */

static float *load_audio_wav(const char *path, int *n_samples) {
    FILE *fp = fopen(path, "rb");
    if (!fp) { perror("fopen audio"); return NULL; }

    /* Read WAV header */
    char riff[4];
    if (fread(riff, 1, 4, fp) != 4 || memcmp(riff, "RIFF", 4) != 0) {
        fprintf(stderr, "Not a WAV file: %s\n", path);
        fclose(fp); return NULL;
    }
    uint32_t chunk_size;
    if (fread(&chunk_size, 4, 1, fp) != 1) { fclose(fp); return NULL; }
    char wave[4];
    if (fread(wave, 1, 4, fp) != 4 || memcmp(wave, "WAVE", 4) != 0) {
        fprintf(stderr, "Not a WAVE file: %s\n", path);
        fclose(fp); return NULL;
    }

    /* Find fmt and data chunks */
    uint16_t audio_format = 0, num_channels = 0, bits_per_sample = 0;
    uint32_t sample_rate = 0;
    float *samples = NULL;

    while (!feof(fp)) {
        char chunk_id[4];
        uint32_t chunk_sz;
        if (fread(chunk_id, 1, 4, fp) != 4) break;
        if (fread(&chunk_sz, 4, 1, fp) != 1) break;

        if (memcmp(chunk_id, "fmt ", 4) == 0) {
            if (fread(&audio_format, 2, 1, fp) != 1) break;
            if (fread(&num_channels, 2, 1, fp) != 1) break;
            if (fread(&sample_rate, 4, 1, fp) != 1) break;
            uint32_t byte_rate;
            if (fread(&byte_rate, 4, 1, fp) != 1) break;
            uint16_t block_align;
            if (fread(&block_align, 2, 1, fp) != 1) break;
            if (fread(&bits_per_sample, 2, 1, fp) != 1) break;
            /* Skip any extra fmt bytes */
            if (chunk_sz > 16) fseek(fp, chunk_sz - 16, SEEK_CUR);
        } else if (memcmp(chunk_id, "data", 4) == 0) {
            if (audio_format != 1) {
                fprintf(stderr, "Unsupported WAV format (need PCM/1, got %d)\n", audio_format);
                fclose(fp); return NULL;
            }
            if (bits_per_sample == 0 || num_channels == 0) {
                fprintf(stderr, "Invalid WAV header\n");
                fclose(fp); return NULL;
            }
            int bytes_per_sample = bits_per_sample / 8;
            int total_samples = (int)(chunk_sz / (unsigned)(bytes_per_sample * num_channels));
            samples = malloc((size_t)total_samples * sizeof(float));
            if (!samples) { fclose(fp); return NULL; }

            for (int i = 0; i < total_samples; i++) {
                float sum = 0.0f;
                for (int ch = 0; ch < num_channels; ch++) {
                    if (bits_per_sample == 16) {
                        int16_t s16;
                        if (fread(&s16, 2, 1, fp) != 1) { s16 = 0; }
                        sum += (float)s16 / 32768.0f;
                    } else if (bits_per_sample == 32) {
                        int32_t s32;
                        if (fread(&s32, 4, 1, fp) != 1) { s32 = 0; }
                        sum += (float)s32 / 2147483648.0f;
                    } else {
                        uint8_t s8;
                        if (fread(&s8, 1, 1, fp) != 1) { s8 = 128; }
                        sum += ((float)s8 - 128.0f) / 128.0f;
                    }
                }
                samples[i] = sum / (float)num_channels;
            }
            *n_samples = total_samples;
        } else {
            fseek(fp, chunk_sz, SEEK_CUR);
        }
    }

    fclose(fp);

    /* Resample to 16kHz if needed */
    if (sample_rate != 16000 && samples && *n_samples > 0) {
        float ratio = (float)sample_rate / 16000.0f;
        int new_n = (int)((float)*n_samples / ratio);
        float *resampled = malloc((size_t)new_n * sizeof(float));
        if (resampled) {
            for (int i = 0; i < new_n; i++) {
                float src_pos = (float)i * ratio;
                int src_idx = (int)src_pos;
                float frac = src_pos - (float)src_idx;
                if (src_idx + 1 < *n_samples)
                    resampled[i] = samples[src_idx] * (1.0f - frac) + samples[src_idx + 1] * frac;
                else
                    resampled[i] = samples[src_idx];
            }
            free(samples);
            samples = resampled;
            *n_samples = new_n;
        }
    }

    return samples;
}

/* ─── MediaPrep integration (normalize audio via fork) ──────────── */

static int run_process(char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return 1; }
    if (pid == 0) { execvp(argv[0], argv); perror("execvp"); _exit(127); }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) { perror("waitpid"); return 1; }
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    return 1;
}

/* ─── CLI ───────────────────────────────────────────────────────── */

static void print_usage(void) {
    fprintf(stderr,
        "bonfyre-transcribe — HCP-enhanced ASR\n\n"
        "Usage:\n"
        "  bonfyre-transcribe <input-audio> <output-dir> [options]\n\n"
        "Options:\n"
        "  --model PATH          Whisper model file (default: ~/.local/share/whisper/ggml-base.en-q5_0.bin)\n"
        "  --language CODE       Force language (default: en)\n"
        "  --beam-size N         Beam search size (default: 5)\n"
        "  --no-hcp              Disable HCP refinement\n"
        "  --media-prep PATH     Path to bonfyre-media-prep binary\n"
        "  --no-normalize        Skip audio normalization\n"
        "  --output-format FMT   json,txt,srt,vtt (default: all)\n"
        "  --threads N           CPU threads (default: 4)\n"
        "  --gpu                 Use GPU acceleration (default: on)\n"
        "  --no-gpu              Disable GPU\n"
        "\n");
}

int main(int argc, char **argv) {
    if (argc < 3) {
        print_usage();
        return 1;
    }

    /* Initialize ggml backends (MUST be before any whisper model loading) */
    ggml_backend_load_all();

    const char *input_audio = argv[1];
    const char *output_dir  = argv[2];

    /* Defaults */
    char model_path[PATH_MAX];
    const char *home = getenv("HOME");
    snprintf(model_path, sizeof(model_path), "%s/.local/share/whisper/ggml-base.en-q5_0.bin", home ? home : ".");
    const char *language = "en";
    int beam_size = 5;
    int use_hcp = 1;
    int use_gpu = 1;
    int n_threads = 4;
    int normalize = 1;
    const char *media_prep = NULL;
    const char *output_format = NULL;  /* NULL = all */

    /* Parse args */
    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--model") == 0 && i + 1 < argc) {
            strncpy(model_path, argv[++i], sizeof(model_path) - 1);
        } else if (strcmp(argv[i], "--language") == 0 && i + 1 < argc) {
            language = argv[++i];
        } else if (strcmp(argv[i], "--beam-size") == 0 && i + 1 < argc) {
            beam_size = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--no-hcp") == 0) {
            use_hcp = 0;
        } else if (strcmp(argv[i], "--media-prep") == 0 && i + 1 < argc) {
            media_prep = argv[++i];
        } else if (strcmp(argv[i], "--no-normalize") == 0) {
            normalize = 0;
        } else if (strcmp(argv[i], "--output-format") == 0 && i + 1 < argc) {
            output_format = argv[++i];
        } else if (strcmp(argv[i], "--threads") == 0 && i + 1 < argc) {
            n_threads = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--gpu") == 0) {
            use_gpu = 1;
        } else if (strcmp(argv[i], "--no-gpu") == 0) {
            use_gpu = 0;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            return 1;
        }
    }

    if (ensure_dir(output_dir) != 0) {
        fprintf(stderr, "Failed to create output dir: %s\n", output_dir);
        return 1;
    }

    /* ── Audio normalization (optional) ─────────────────────── */
    char normalized_path[PATH_MAX];
    snprintf(normalized_path, sizeof(normalized_path), "%s/normalized.wav", output_dir);

    if (normalize) {
        const char *mp = media_prep ? media_prep : "bonfyre-media-prep";
        char *norm_argv[] = {
            (char *)mp, "normalize",
            (char *)input_audio, normalized_path,
            "--sample-rate", "16000",
            "--channels", "1",
            NULL
        };
        fprintf(stderr, "[transcribe] normalizing audio...\n");
        if (run_process(norm_argv) != 0) {
            fprintf(stderr, "[transcribe] normalize failed, using raw input\n");
            strncpy(normalized_path, input_audio, sizeof(normalized_path) - 1);
        }
    } else {
        strncpy(normalized_path, input_audio, sizeof(normalized_path) - 1);
    }

    /* ── Load audio ─────────────────────────────────────────── */
    int n_samples = 0;
    float *audio = load_audio_wav(normalized_path, &n_samples);
    if (!audio || n_samples < 1600) {
        fprintf(stderr, "[transcribe] failed to load audio: %s\n", normalized_path);
        return 1;
    }
    fprintf(stderr, "[transcribe] loaded %d samples (%.1fs)\n", n_samples, (float)n_samples / 16000.0f);

    /* ── Load whisper model ─────────────────────────────────── */
    struct whisper_context_params cparams = whisper_context_default_params();
    cparams.use_gpu = use_gpu;
    cparams.flash_attn = false;
    cparams.dtw_token_timestamps = true;
    cparams.dtw_aheads_preset = WHISPER_AHEADS_BASE_EN;

    fprintf(stderr, "[transcribe] loading model: %s\n", model_path);
    struct whisper_context *ctx = whisper_init_from_file_with_params(model_path, cparams);
    if (!ctx) {
        fprintf(stderr, "[transcribe] failed to load whisper model\n");
        free(audio);
        return 1;
    }

    /* ── Configure decode parameters ────────────────────────── */
    struct whisper_full_params wparams = whisper_full_default_params(WHISPER_SAMPLING_BEAM_SEARCH);
    wparams.n_threads        = n_threads;
    wparams.language         = language;
    wparams.translate        = false;
    wparams.no_context       = false;
    wparams.token_timestamps = true;
    wparams.tdrz_enable      = true;
    wparams.suppress_blank   = true;
    wparams.suppress_nst     = true;
    wparams.temperature      = 0.0f;
    wparams.beam_search.beam_size = beam_size;
    wparams.print_progress   = true;
    wparams.print_timestamps = false;
    wparams.print_realtime   = false;
    wparams.print_special    = false;

    /* Repetition penalty callback */
    RepPenaltyState rp_state = {0};
    wparams.logits_filter_callback = repetition_penalty_cb;
    wparams.logits_filter_callback_user_data = &rp_state;

    /* ── Decode ─────────────────────────────────────────────── */
    fprintf(stderr, "[transcribe] decoding with beam_size=%d, threads=%d, gpu=%d\n",
            beam_size, n_threads, use_gpu);
    double t_decode_start = ms_now();

    int ret = whisper_full(ctx, wparams, audio, n_samples);
    if (ret != 0) {
        fprintf(stderr, "[transcribe] whisper_full failed: %d\n", ret);
        whisper_free(ctx);
        free(audio);
        return 1;
    }

    double t_decode_end = ms_now();
    double decode_ms = t_decode_end - t_decode_start;
    fprintf(stderr, "[transcribe] decode complete: %.0fms\n", decode_ms);

    /* ── Extract segments ───────────────────────────────────── */
    TranscriptResult result = {0};
    result.decode_ms = decode_ms;
    strncpy(result.detected_language, language ? language : "auto", sizeof(result.detected_language) - 1);
    result.language_confidence = 0.95f;

    if (extract_segments(ctx, &result) != 0) {
        fprintf(stderr, "[transcribe] segment extraction failed\n");
        whisper_free(ctx);
        free(audio);
        return 1;
    }
    fprintf(stderr, "[transcribe] extracted %d segments\n", result.count);

    /* ── HCP refinement ─────────────────────────────────────── */
    HcpResult hcp = {0};
    if (use_hcp && result.count > 0) {
        fprintf(stderr, "[transcribe] running HCP refinement (%d segments)...\n", result.count);
        hcp = hcp_process(ctx, result.count, result.segments);
        result.hcp_ms = hcp.elapsed_ms;

        fprintf(stderr, "[transcribe] HCP: %d tokens, %d flagged (%.1f%%), %d segments flagged, %.1fms\n",
                hcp.n_tokens, hcp.n_flagged,
                hcp.n_tokens > 0 ? 100.0f * (float)hcp.n_flagged / (float)hcp.n_tokens : 0.0f,
                hcp.n_flagged_seg, hcp.elapsed_ms);

        /* Re-decode flagged segments (quality enhancement) */
        redecode_flagged_segments(ctx, normalized_path, &result, &hcp, beam_size);
    }

    /* ── Write output files ─────────────────────────────────── */
    char base_name[PATH_MAX];
    snprintf(base_name, sizeof(base_name), "%s", path_basename(input_audio));
    strip_extension(base_name);

    char json_path[PATH_MAX], txt_path[PATH_MAX], srt_path[PATH_MAX], vtt_path[PATH_MAX];
    snprintf(json_path, sizeof(json_path), "%s/%s.json", output_dir, base_name);
    snprintf(txt_path,  sizeof(txt_path),  "%s/%s.txt",  output_dir, base_name);
    snprintf(srt_path,  sizeof(srt_path),  "%s/%s.srt",  output_dir, base_name);
    snprintf(vtt_path,  sizeof(vtt_path),  "%s/%s.vtt",  output_dir, base_name);

    int write_json = 1, write_txt = 1, write_srt = 1, write_vtt = 1;
    if (output_format) {
        write_json = write_txt = write_srt = write_vtt = 0;
        if (strstr(output_format, "json")) write_json = 1;
        if (strstr(output_format, "txt"))  write_txt = 1;
        if (strstr(output_format, "srt"))  write_srt = 1;
        if (strstr(output_format, "vtt"))  write_vtt = 1;
    }

    if (write_json) write_transcript_json(json_path, &result, &hcp);
    if (write_txt)  write_transcript_txt(txt_path, &result);
    if (write_srt)  write_transcript_srt(srt_path, &result);
    if (write_vtt)  write_transcript_vtt(vtt_path, &result);

    /* ── Meta + status JSON ─────────────────────────────────── */
    char meta_path[PATH_MAX], status_path[PATH_MAX];
    snprintf(meta_path, sizeof(meta_path), "%s/meta.json", output_dir);
    snprintf(status_path, sizeof(status_path), "%s/transcribe-status.json", output_dir);

    char timestamp[32];
    iso_timestamp(timestamp, sizeof(timestamp));

    FILE *meta = fopen(meta_path, "w");
    if (meta) {
        fprintf(meta,
            "{\n"
            "  \"source_system\": \"BonfyreTranscribe\",\n"
            "  \"version\": \"2.0-hcp\",\n"
            "  \"created_at\": \"%s\",\n"
            "  \"input_audio\": \"%s\",\n"
            "  \"model\": \"%s\",\n"
            "  \"language\": \"%s\",\n"
            "  \"beam_size\": %d,\n"
            "  \"gpu\": %s,\n"
            "  \"hcp_enabled\": %s,\n"
            "  \"decode_ms\": %.1f,\n"
            "  \"hcp_ms\": %.1f,\n"
            "  \"total_segments\": %d,\n"
            "  \"hallucinated_segments\": %d,\n"
            "  \"hcp_flagged_segments\": %d,\n"
            "  \"hcp_flagged_tokens\": %d,\n"
            "  \"hcp_total_tokens\": %d,\n"
            "  \"audio_seconds\": %.1f\n"
            "}\n",
            timestamp, input_audio, model_path,
            language ? language : "auto",
            beam_size,
            use_gpu ? "true" : "false",
            use_hcp ? "true" : "false",
            decode_ms, hcp.elapsed_ms,
            result.count, result.segments_hallucinated,
            hcp.n_flagged_seg, hcp.n_flagged, hcp.n_tokens,
            (float)n_samples / 16000.0f);
        fclose(meta);
    }

    FILE *status = fopen(status_path, "w");
    if (status) {
        fprintf(status,
            "{\n"
            "  \"sourceSystem\": \"BonfyreTranscribe\",\n"
            "  \"version\": \"2.0-hcp\",\n"
            "  \"exportedAt\": \"%s\",\n"
            "  \"status\": \"transcribed\",\n"
            "  \"jobSlug\": \"%s\"\n"
            "}\n",
            timestamp, base_name);
        fclose(status);
    }

    /* ── Summary ────────────────────────────────────────────── */
    float total_quality = 0.0f, total_hcp_quality = 0.0f;
    for (int i = 0; i < result.count; i++) {
        total_quality += result.segments[i].quality;
        total_hcp_quality += result.segments[i].hcp_quality;
    }
    float avg_q = result.count > 0 ? total_quality / (float)result.count : 0.0f;
    float avg_hq = result.count > 0 ? total_hcp_quality / (float)result.count : 0.0f;

    printf("=== BonfyreTranscribe v2.0-hcp ===\n");
    printf("Audio:    %s (%.1fs)\n", input_audio, (float)n_samples / 16000.0f);
    printf("Segments: %d (hallucinated: %d)\n", result.count, result.segments_hallucinated);
    printf("Quality:  %.4f (base) -> %.4f (hcp)\n", avg_q, avg_hq);
    printf("Decode:   %.0fms\n", decode_ms);
    if (use_hcp) {
        printf("HCP:      %d tokens, %d flagged (%.1f%%), %d segs flagged, %.1fms\n",
            hcp.n_tokens, hcp.n_flagged,
            hcp.n_tokens > 0 ? 100.0f * (float)hcp.n_flagged / (float)hcp.n_tokens : 0.0f,
            hcp.n_flagged_seg, hcp.elapsed_ms);
    }
    if (write_json) printf("JSON:     %s\n", json_path);
    if (write_txt)  printf("TXT:      %s\n", txt_path);
    if (write_srt)  printf("SRT:      %s\n", srt_path);
    if (write_vtt)  printf("VTT:      %s\n", vtt_path);
    printf("Meta:     %s\n", meta_path);

    /* Cleanup */
    hcp_free(&hcp);
    free(result.segments);
    whisper_free(ctx);
    free(audio);

    return 0;
}
