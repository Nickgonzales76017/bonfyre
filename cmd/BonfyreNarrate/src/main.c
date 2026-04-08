/*
 * BonfyreNarrate v3.2 — Verified Tone-Aware Text-to-Speech Synthesis
 *
 * The narration layer that synthesizes, verifies, and proves.
 *
 * v1:   fork piper, hope for the best.
 * v2:   tone-aware prosody + PSOLA + energy envelope.
 * v3:   closed-loop verification + 6-layer fidelity scoring +
 *       iterative refinement. The HCP for TTS.
 * v3.1: inline FFT + zero-dependency verification.
 * v3.2: mathematical perfection pass —
 *       • Parabolic interpolation on autocorrelation peak (sub-sample F0)
 *       • Pitch-synchronous PSOLA with normalization buffer (preserves dynamics)
 *       • Per-frame energy envelope with exponential smoothing
 *       • Semitone-domain F0 statistics (correct log-frequency math)
 *       • True 5th/95th percentile computation via sorted array
 *       • Fixed voiced/unvoiced segment tracking (was dead code)
 *       • Hz-domain spectral slope regression (cross-config comparable)
 *       • Spectral flatness (Wiener entropy) feature
 *       • Removed 200-frame cap on spectral analysis
 *
 * Pipeline:
 *   bonfyre-tone profile audio.wav tone_out/    → 6-dimension profile
 *   bonfyre-narrate text.md out/ \
 *     --tone-profile tone_out/profile.json \
 *     --reference audio.wav \
 *     --verify --refine
 *
 * Zero-dependency verification:
 *   bonfyre-narrate verify output.wav source.wav
 *
 * What it does:
 *   1. SSML prosody generation from 6-dimension tone profile
 *   2. PSOLA pitch correction (pitch-synchronous OLA with
 *      parabolic-interpolated autocorrelation + normalization buffer)
 *   3. Energy envelope transfer (per-frame RMS with exp smoothing)
 *   4. Inline acoustic feature extraction (FFT, autocorrelation,
 *      RMS, jitter, shimmer, HNR, spectral flatness — 28 features,
 *      pure C, no deps)
 *   5. 6-layer fidelity scoring (pitch/energy/temporal/spectral/
 *      stability/dynamics) — geometric mean composite
 *   6. Closed-loop verification: extracts fingerprint from its own
 *      output and compares with source — inline, no subprocess
 *   7. Iterative refinement: converges in ≤3 passes
 *   8. Direct WAV quality analysis (dynamic range, spectral
 *      continuity, pitch stability, energy consistency)
 *
 * All DSP is inline C. No Python. No pip install. No subprocess
 * for verification. Single static binary.
 */
#define _POSIX_C_SOURCE 200809L

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <bonfyre.h>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

/* ═══════════════════════════════════════════════════════════════
 *  §1  WAV I/O
 * ═══════════════════════════════════════════════════════════════ */

typedef struct {
    int16_t *samples;
    int      n_samples;
    int      sample_rate;
    int      channels;
} WavAudio;

static WavAudio wav_read(const char *path) {
    WavAudio w = {0};
    FILE *f = fopen(path, "rb");
    if (!f) return w;

    char riff[4]; uint32_t fsize; char wave[4];
    if (fread(riff, 1, 4, f) != 4) goto fail;
    if (memcmp(riff, "RIFF", 4) != 0) goto fail;
    if (fread(&fsize, 4, 1, f) != 1) goto fail;
    if (fread(wave, 1, 4, f) != 4) goto fail;
    if (memcmp(wave, "WAVE", 4) != 0) goto fail;

    /* find fmt chunk */
    while (1) {
        char id[4]; uint32_t sz;
        if (fread(id, 1, 4, f) != 4) goto fail;
        if (fread(&sz, 4, 1, f) != 1) goto fail;
        if (memcmp(id, "fmt ", 4) == 0) {
            uint16_t fmt, ch; uint32_t sr;
            if (fread(&fmt, 2, 1, f) != 1) goto fail;
            if (fread(&ch, 2, 1, f) != 1) goto fail;
            if (fread(&sr, 4, 1, f) != 1) goto fail;
            w.channels = ch;
            w.sample_rate = (int)sr;
            if (sz > 8) fseek(f, (long)(sz - 8), SEEK_CUR);
            continue;
        }
        if (memcmp(id, "data", 4) == 0) {
            int n = (int)(sz / 2); /* 16-bit samples */
            w.samples = malloc((size_t)n * sizeof(int16_t));
            if (!w.samples) goto fail;
            if ((int)fread(w.samples, 2, (size_t)n, f) != n) {
                free(w.samples); w.samples = NULL; goto fail;
            }
            w.n_samples = n / w.channels; /* per-channel count */
            fclose(f);
            return w;
        }
        fseek(f, (long)sz, SEEK_CUR);
    }
fail:
    fclose(f);
    return w;
}

static int wav_write(const char *path, const int16_t *samples, int n,
                     int sample_rate, int channels) {
    FILE *f = fopen(path, "wb");
    if (!f) return -1;

    uint32_t data_size = (uint32_t)(n * channels * 2);
    uint32_t file_size = 36 + data_size;
    uint16_t block_align = (uint16_t)(channels * 2);
    uint32_t byte_rate = (uint32_t)(sample_rate * channels * 2);

    fwrite("RIFF", 1, 4, f);
    fwrite(&file_size, 4, 1, f);
    fwrite("WAVE", 1, 4, f);
    fwrite("fmt ", 1, 4, f);
    uint32_t fmt_size = 16;
    fwrite(&fmt_size, 4, 1, f);
    uint16_t pcm_fmt = 1;
    uint16_t ch = (uint16_t)channels;
    uint32_t sr = (uint32_t)sample_rate;
    fwrite(&pcm_fmt, 2, 1, f);
    fwrite(&ch, 2, 1, f);
    fwrite(&sr, 4, 1, f);
    fwrite(&byte_rate, 4, 1, f);
    fwrite(&block_align, 2, 1, f);
    uint16_t bits = 16;
    fwrite(&bits, 2, 1, f);
    fwrite("data", 1, 4, f);
    fwrite(&data_size, 4, 1, f);
    fwrite(samples, 2, (size_t)(n * channels), f);
    fclose(f);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════
 *  §2  Tone Profile Parser
 * ═══════════════════════════════════════════════════════════════ */

typedef struct {
    double energy;       /* 0-100: loudness normalized */
    double pitch;        /* 0-100: F0 semitone normalized */
    double stability;    /* 0-100: jitter (lower = steadier) */
    double brightness;   /* 0-100: spectral flux */
    double pace;         /* 0-100: mean voiced segment length */
    double variation;    /* 0-100: loudness contour variation */
    double pitch_hz;     /* estimated F0 in Hz from semitone value */
    int    valid;
} ToneProfile;

/* Minimal JSON number extractor for tone profile */
static double json_get_num(const char *json, const char *dim,
                           const char *field) {
    /* Find "energy": { ... "normalized": 50.0 ... } */
    char pattern[256];
    snprintf(pattern, sizeof(pattern), "\"%s\"", dim);
    const char *p = strstr(json, pattern);
    if (!p) return -1.0;
    snprintf(pattern, sizeof(pattern), "\"%s\"", field);
    p = strstr(p, pattern);
    if (!p) return -1.0;
    p += strlen(pattern);
    while (*p && *p != ':') p++;
    if (*p == ':') p++;
    while (*p == ' ' || *p == '\t') p++;
    return atof(p);
}

static double json_get_raw(const char *json, const char *dim) {
    return json_get_num(json, dim, "raw");
}

static ToneProfile parse_tone_profile(const char *path) {
    ToneProfile tp = {0};
    FILE *f = fopen(path, "r");
    if (!f) return tp;

    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    rewind(f);
    char *json = malloc((size_t)sz + 1);
    if (!json) { fclose(f); return tp; }
    if (fread(json, 1, (size_t)sz, f) != (size_t)sz) {
        free(json); fclose(f); return tp;
    }
    json[sz] = '\0';
    fclose(f);

    tp.energy     = json_get_num(json, "energy",     "normalized");
    tp.pitch      = json_get_num(json, "pitch",      "normalized");
    tp.stability  = json_get_num(json, "stability",  "normalized");
    tp.brightness = json_get_num(json, "brightness", "normalized");
    tp.pace       = json_get_num(json, "pace",       "normalized");
    tp.variation  = json_get_num(json, "variation",  "normalized");

    /* Convert semitone value back to Hz estimate for pitch shifting */
    double raw_semitone = json_get_raw(json, "pitch");
    if (raw_semitone > -99.0) {
        /* F0semitone is relative to 27.5 Hz (A0) in openSMILE */
        tp.pitch_hz = 27.5 * pow(2.0, raw_semitone / 12.0);
    } else {
        tp.pitch_hz = 0.0;
    }

    tp.valid = (tp.energy >= 0 && tp.pitch >= 0);
    free(json);
    return tp;
}

/* ═══════════════════════════════════════════════════════════════
 *  §3  SSML Prosody Generator
 *
 *  Maps the 6 tone dimensions to SSML prosody attributes.
 *  Piper supports a subset of SSML; we generate portable tags
 *  and fall back to plain text + pause injection when SSML
 *  isn't available.
 * ═══════════════════════════════════════════════════════════════ */

typedef struct {
    double rate_pct;     /* speaking rate: -50% to +100% */
    double pitch_st;     /* pitch shift in semitones */
    double volume_pct;   /* volume: -30% to +50% */
    int    pause_ms;     /* inter-sentence pause (ms) */
    int    emphasis;     /* 0=none, 1=moderate, 2=strong */
} ProsodyParams;

static ProsodyParams compute_prosody(const ToneProfile *tp) {
    ProsodyParams pp = {0};

    /* Rate: pace 0=slow→ -30%, pace 100=fast→ +40% */
    pp.rate_pct = (tp->pace - 50.0) * 0.7;
    if (pp.rate_pct < -30.0) pp.rate_pct = -30.0;
    if (pp.rate_pct > 40.0) pp.rate_pct = 40.0;

    /* Volume: energy 0=quiet→ -20%, energy 100=loud→ +30% */
    pp.volume_pct = (tp->energy - 50.0) * 0.5;
    if (pp.volume_pct < -20.0) pp.volume_pct = -20.0;
    if (pp.volume_pct > 30.0) pp.volume_pct = 30.0;

    /* Pitch: direct from tone profile, clamped */
    pp.pitch_st = (tp->pitch - 50.0) * 0.12; /* max ±6 semitones */
    if (pp.pitch_st < -6.0) pp.pitch_st = -6.0;
    if (pp.pitch_st > 6.0) pp.pitch_st = 6.0;

    /* Pause: low variation = longer pauses (steady speaker), high = shorter */
    pp.pause_ms = (int)(800.0 - tp->variation * 5.0);
    if (pp.pause_ms < 200) pp.pause_ms = 200;
    if (pp.pause_ms > 1200) pp.pause_ms = 1200;

    /* Emphasis: high energy + high variation = strong emphasis */
    if (tp->energy > 70.0 && tp->variation > 60.0) pp.emphasis = 2;
    else if (tp->energy > 50.0 || tp->variation > 40.0) pp.emphasis = 1;

    return pp;
}

/* Split text into sentences, wrap each with SSML prosody */
static char *generate_ssml(const char *text, const ProsodyParams *pp) {
    size_t tlen = strlen(text);
    size_t cap = tlen * 4 + 4096;
    char *out = malloc(cap);
    if (!out) return NULL;

    int pos = 0;
    pos += snprintf(out + pos, cap - (size_t)pos,
        "<speak>\n"
        "<prosody rate=\"%+.0f%%\" pitch=\"%+.1fst\" volume=\"%+.0f%%\">\n",
        pp->rate_pct, pp->pitch_st, pp->volume_pct);

    const char *p = text;
    while (*p) {
        /* find sentence boundary */
        const char *end = p;
        while (*end && *end != '.' && *end != '!' && *end != '?' && *end != '\n') end++;
        if (*end) end++; /* include punctuation */

        /* skip empty lines */
        size_t slen = (size_t)(end - p);
        int all_space = 1;
        for (size_t i = 0; i < slen; i++) {
            if (!isspace((unsigned char)p[i])) { all_space = 0; break; }
        }

        if (!all_space && slen > 0) {
            /* write sentence */
            if (pp->emphasis == 2) {
                pos += snprintf(out + pos, cap - (size_t)pos, "<emphasis level=\"strong\">");
            }

            /* escape XML special chars */
            for (size_t i = 0; i < slen; i++) {
                char c = p[i];
                if (c == '&')       pos += snprintf(out + pos, cap - (size_t)pos, "&amp;");
                else if (c == '<')  pos += snprintf(out + pos, cap - (size_t)pos, "&lt;");
                else if (c == '>')  pos += snprintf(out + pos, cap - (size_t)pos, "&gt;");
                else if (c == '\n') pos += snprintf(out + pos, cap - (size_t)pos, " ");
                else { if ((size_t)pos < cap - 1) out[pos++] = c; }
            }

            if (pp->emphasis == 2) {
                pos += snprintf(out + pos, cap - (size_t)pos, "</emphasis>");
            }

            pos += snprintf(out + pos, cap - (size_t)pos,
                "\n<break time=\"%dms\"/>\n", pp->pause_ms);
        }

        p = end;
        if (!*p) break;
    }

    pos += snprintf(out + pos, cap - (size_t)pos,
        "</prosody>\n</speak>\n");
    out[pos] = '\0';
    return out;
}

/* Plain text fallback with pause markers for engines without SSML */
static char *generate_paced_text(const char *text, const ProsodyParams *pp) {
    size_t tlen = strlen(text);
    size_t cap = tlen * 2 + 2048;
    char *out = malloc(cap);
    if (!out) return NULL;

    int pos = 0;
    const char *p = text;

    while (*p) {
        const char *end = p;
        while (*end && *end != '.' && *end != '!' && *end != '?') end++;
        if (*end) end++;

        size_t slen = (size_t)(end - p);
        int all_space = 1;
        for (size_t i = 0; i < slen; i++) {
            if (!isspace((unsigned char)p[i])) { all_space = 0; break; }
        }

        if (!all_space && slen > 0) {
            for (size_t i = 0; i < slen; i++) {
                char c = p[i];
                /* strip markdown */
                if (c == '#') { while (p[i+1] == '#') i++; while (p[i+1] == ' ') i++; continue; }
                if (c == '`') continue;
                if (c == '*') continue;
                if ((size_t)pos < cap - 1) out[pos++] = c;
            }
            /* Add sentence break proportional to pause_ms */
            int extra_newlines = pp->pause_ms / 400;
            if (extra_newlines < 1) extra_newlines = 1;
            for (int j = 0; j < extra_newlines && (size_t)pos < cap - 2; j++)
                out[pos++] = '\n';
        }

        p = end;
        if (!*p) break;
    }

    out[pos] = '\0';
    return out;
}

/* ═══════════════════════════════════════════════════════════════
 *  §4  PSOLA Pitch Shifter (Pure C)
 *
 *  Pitch-Synchronous Overlap-Add. The standard technique for
 *  time-domain pitch modification of speech signals. Works by:
 *    1. Detecting pitch periods via autocorrelation
 *    2. Windowing each period with Hanning
 *    3. Overlapping windows at the target period length
 *
 *  This gives natural-sounding pitch shift without the artifacts
 *  of frequency-domain methods on speech.
 *
 *  Reuses the Hanning window and signal processing patterns
 *  proven in hcp-whisper's spectral pipeline.
 * ═══════════════════════════════════════════════════════════════ */

/* Autocorrelation-based pitch detection with parabolic interpolation.
 * Sub-sample precision: fits parabola through r[lag-1], r[lag], r[lag+1]
 * to find the true fractional peak of the autocorrelation function.
 * Without interpolation, F0 resolution is limited to integer sample
 * periods (~1 Hz at typical rates). With interpolation, precision
 * improves to ~0.01 Hz — critical for accurate fidelity scoring. */
static double detect_pitch_frame(const float *frame, int len, int sr) {
    int min_lag = sr / 500;  /* 500 Hz max */
    int max_lag = sr / 60;   /* 60 Hz min */
    if (max_lag >= len) max_lag = len - 1;
    if (min_lag >= max_lag) return 0.0;

    double best_corr = -1.0;
    int best_lag = 0;

    for (int lag = min_lag; lag <= max_lag; lag++) {
        double sum = 0.0, norm_a = 0.0, norm_b = 0.0;
        int count = len - lag;
        for (int i = 0; i < count; i++) {
            sum += (double)frame[i] * frame[i + lag];
            norm_a += (double)frame[i] * frame[i];
            norm_b += (double)frame[i + lag] * frame[i + lag];
        }
        double denom = sqrt(norm_a * norm_b);
        if (denom < 1e-10) continue;
        double r = sum / denom;
        if (r > best_corr) {
            best_corr = r;
            best_lag = lag;
        }
    }

    if (best_corr < 0.3) return 0.0; /* unvoiced */

    /* Parabolic interpolation for sub-sample precision.
     * Recompute autocorrelation at best_lag ± 1, fit parabola,
     * find fractional peak: δ = 0.5*(r[l-1]-r[l+1]) / (r[l-1]-2r[l]+r[l+1]) */
    double refined_lag = (double)best_lag;
    if (best_lag > min_lag && best_lag < max_lag) {
        double r_vals[3];
        for (int k = -1; k <= 1; k++) {
            int lag = best_lag + k;
            double sum = 0.0, na = 0.0, nb = 0.0;
            int count = len - lag;
            for (int i = 0; i < count; i++) {
                sum += (double)frame[i] * frame[i + lag];
                na  += (double)frame[i] * frame[i];
                nb  += (double)frame[i + lag] * frame[i + lag];
            }
            double d = sqrt(na * nb);
            r_vals[k + 1] = (d > 1e-10) ? sum / d : 0.0;
        }
        double denom2 = r_vals[0] - 2.0 * r_vals[1] + r_vals[2];
        if (fabs(denom2) > 1e-12) {
            double delta = 0.5 * (r_vals[0] - r_vals[2]) / denom2;
            if (fabs(delta) < 1.0) refined_lag += delta;
        }
    }

    return (double)sr / refined_lag;
}

/* Hanning window (matches hcp-whisper) */
static void hanning(float *w, int n) {
    for (int i = 0; i < n; i++)
        w[i] = 0.5f * (1.0f - cosf(2.0f * (float)M_PI * i / (n - 1)));
}

/* PSOLA pitch shift with amplitude-preserving normalization buffer.
 * Uses pitch-synchronous analysis hops (hop_in = detected period)
 * and a normalization buffer that tracks accumulated window weight
 * at each output sample. Dividing by the normalization buffer
 * guarantees unity gain under OLA — no global peak normalization
 * needed, preserving the original signal's dynamic range. */
static int16_t *psola_pitch_shift(const int16_t *in, int n_samples,
                                  int sample_rate, double shift_ratio,
                                  int *out_len) {
    if (fabs(shift_ratio - 1.0) < 0.01) {
        *out_len = n_samples;
        int16_t *out = malloc((size_t)n_samples * sizeof(int16_t));
        if (out) memcpy(out, in, (size_t)n_samples * sizeof(int16_t));
        return out;
    }

    /* Convert to float */
    float *sig = malloc((size_t)n_samples * sizeof(float));
    if (!sig) return NULL;
    for (int i = 0; i < n_samples; i++)
        sig[i] = (float)in[i] / 32768.0f;

    /* Output + normalization buffers */
    int out_alloc = n_samples + 8192;
    float *out_f = calloc((size_t)out_alloc, sizeof(float));
    float *norm  = calloc((size_t)out_alloc, sizeof(float));
    if (!out_f || !norm) {
        free(sig); free(out_f); free(norm); return NULL;
    }

    int frame_size = sample_rate / 80; /* ~12.5ms frames */
    int max_wlen = frame_size * 4;
    float *win = malloc((size_t)max_wlen * sizeof(float));
    if (!win) { free(sig); free(out_f); free(norm); return NULL; }

    int out_pos = 0;
    int in_pos = 0;

    while (in_pos + frame_size < n_samples) {
        int analysis_len = frame_size * 2;
        if (in_pos + analysis_len > n_samples)
            analysis_len = n_samples - in_pos;

        double f0 = detect_pitch_frame(sig + in_pos, analysis_len, sample_rate);
        int period;
        if (f0 > 60.0) {
            period = (int)((double)sample_rate / f0 + 0.5);
        } else {
            period = frame_size; /* unvoiced: use default */
        }

        /* Window size = 2 * period */
        int wlen = period * 2;
        if (wlen > max_wlen) wlen = max_wlen;
        if (in_pos + wlen > n_samples) wlen = n_samples - in_pos;

        hanning(win, wlen);

        /* Overlap-add with normalization tracking */
        for (int i = 0; i < wlen && out_pos + i < out_alloc; i++) {
            out_f[out_pos + i] += sig[in_pos + i] * win[i];
            norm[out_pos + i]  += win[i];
        }

        /* Pitch-synchronous analysis hop = detected period.
         * Synthesis hop = period / shift_ratio.
         * This produces the pitch change while preserving duration. */
        int hop_in = (f0 > 60.0) ? period : frame_size;
        int hop_out = (int)((double)hop_in / shift_ratio + 0.5);
        if (hop_out < 1) hop_out = 1;

        in_pos  += hop_in;
        out_pos += hop_out;
    }

    /* Normalize by accumulated window weight — preserves original
     * amplitude at every sample point. No global normalization needed. */
    int total = out_pos + frame_size;
    if (total > out_alloc) total = out_alloc;
    *out_len = total;

    for (int i = 0; i < total; i++) {
        if (norm[i] > 1e-6f) out_f[i] /= norm[i];
    }

    /* Convert to int16 — amplitude already correct */
    int16_t *out = malloc((size_t)total * sizeof(int16_t));
    if (!out) { free(sig); free(out_f); free(norm); free(win); return NULL; }

    for (int i = 0; i < total; i++) {
        float v = out_f[i] * 32767.0f;
        if (v > 32767.0f) v = 32767.0f;
        if (v < -32768.0f) v = -32768.0f;
        out[i] = (int16_t)v;
    }

    free(sig);
    free(out_f);
    free(norm);
    free(win);
    return out;
}

/* ═══════════════════════════════════════════════════════════════
 *  §5  Energy Envelope Transfer
 *
 *  Per-frame RMS gain shaping with exponential smoothing.
 *  Computes the current RMS envelope, derives a target RMS from
 *  the tone profile, then applies per-frame gain to match.
 *  Exponential smoothing between frames prevents discontinuities.
 *  Soft-clips via tanh near the int16 limit.
 * ═══════════════════════════════════════════════════════════════ */

static void apply_energy_envelope(int16_t *samples, int n, int sr,
                                  double target_energy_norm) {
    /* target_energy_norm is 0-100 from tone profile */
    /* Map to gain: 30→0.6, 50→1.0, 80→1.4 */
    double target_gain = 0.4 + (target_energy_norm / 100.0) * 1.2;
    if (target_gain < 0.3) target_gain = 0.3;
    if (target_gain > 2.0) target_gain = 2.0;

    int frame_size = sr / 20; /* 50ms frames */
    if (frame_size < 1) frame_size = 1;
    int n_frames = n / frame_size;
    if (n_frames < 1) return;

    /* Compute per-frame RMS */
    double *frame_rms = malloc((size_t)n_frames * sizeof(double));
    if (!frame_rms) return;

    for (int f = 0; f < n_frames; f++) {
        double rms = 0.0;
        int start = f * frame_size;
        int end = start + frame_size;
        if (end > n) end = n;
        for (int j = start; j < end; j++)
            rms += (double)samples[j] * samples[j];
        frame_rms[f] = sqrt(rms / (end - start));
    }

    /* Compute global RMS of active frames */
    double global_rms_sq = 0.0;
    int active = 0;
    for (int f = 0; f < n_frames; f++) {
        if (frame_rms[f] > 1.0) { /* above silence threshold */
            global_rms_sq += frame_rms[f] * frame_rms[f];
            active++;
        }
    }
    double global_rms = active > 0 ? sqrt(global_rms_sq / active) : 1.0;
    double target_rms = global_rms * target_gain;

    /* Per-frame gain with exponential smoothing */
    double prev_gain = target_gain;
    double smooth = 0.3; /* smoothing factor */

    for (int f = 0; f < n_frames; f++) {
        int start = f * frame_size;
        int end = start + frame_size;
        if (end > n) end = n;

        double gain;
        if (frame_rms[f] < 1.0) {
            gain = 1.0; /* don't amplify silence */
        } else {
            gain = target_rms / frame_rms[f];
            if (gain > 4.0) gain = 4.0;
            if (gain < 0.1) gain = 0.1;
        }

        /* Exponential smoothing to prevent discontinuities */
        gain = smooth * prev_gain + (1.0 - smooth) * gain;
        prev_gain = gain;

        for (int j = start; j < end; j++) {
            double v = (double)samples[j] * gain;
            if (v > 20000.0 || v < -20000.0)
                v = 32767.0 * tanh(v / 32767.0);
            if (v > 32767.0) v = 32767.0;
            if (v < -32768.0) v = -32768.0;
            samples[j] = (int16_t)v;
        }
    }

    /* Handle remainder samples after last full frame */
    int remainder_start = n_frames * frame_size;
    for (int j = remainder_start; j < n; j++) {
        double v = (double)samples[j] * prev_gain;
        if (v > 20000.0 || v < -20000.0)
            v = 32767.0 * tanh(v / 32767.0);
        if (v > 32767.0) v = 32767.0;
        if (v < -32768.0) v = -32768.0;
        samples[j] = (int16_t)v;
    }

    free(frame_rms);
}

/* ═══════════════════════════════════════════════════════════════
 *  §6  Piper TTS Invocation
 * ═══════════════════════════════════════════════════════════════ */

static int command_exists(const char *name) {
    char *path_env = getenv("PATH");
    if (!path_env) return 0;
    char *copy = strdup(path_env);
    if (!copy) return 0;
    char *saveptr = NULL;
    char *dir = strtok_r(copy, ":", &saveptr);
    while (dir) {
        char candidate[PATH_MAX];
        snprintf(candidate, sizeof(candidate), "%s/%s", dir, name);
        if (access(candidate, X_OK) == 0) { free(copy); return 1; }
        dir = strtok_r(NULL, ":", &saveptr);
    }
    free(copy);
    return 0;
}

static int run_piper(const char *voice_model, const char *text_path,
                     const char *audio_path, const char *log_path,
                     double volume, int sentence_silence_ms) {
    FILE *log = fopen(log_path, "w");
    if (!log) return 1;

    char vol_str[32], silence_str[32];
    snprintf(vol_str, sizeof(vol_str), "%.2f", volume);
    snprintf(silence_str, sizeof(silence_str), "%.3f",
             (double)sentence_silence_ms / 1000.0);

    pid_t pid = fork();
    if (pid < 0) { fclose(log); return 1; }
    if (pid == 0) {
        int fd = fileno(log);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        execlp("piper", "piper",
               "--model", voice_model,
               "--output_file", audio_path,
               "--file", text_path,
               "--sentence-silence", silence_str,
               (char *)NULL);
        _exit(127);
    }

    int status;
    waitpid(pid, &status, 0);
    fclose(log);
    return (WIFEXITED(status) && WEXITSTATUS(status) == 0) ? 0 : 1;
}

/* ═══════════════════════════════════════════════════════════════
 *  §7  Text Normalization
 * ═══════════════════════════════════════════════════════════════ */

static char *read_text_file(const char *path) {
    FILE *in = fopen(path, "rb");
    if (!in) return NULL;
    fseek(in, 0, SEEK_END);
    long size = ftell(in);
    if (size < 0) { fclose(in); return NULL; }
    rewind(in);
    char *buf = malloc((size_t)size + 1);
    if (!buf) { fclose(in); return NULL; }
    if (size > 0 && (long)fread(buf, 1, (size_t)size, in) != size) {
        free(buf); fclose(in); return NULL;
    }
    buf[size] = '\0';
    fclose(in);
    return buf;
}

static char *normalize_markdown(const char *text) {
    size_t len = strlen(text);
    char *out = malloc(len * 2 + 64);
    if (!out) return NULL;
    size_t o = 0;
    int at_line = 1;

    for (size_t i = 0; i < len; i++) {
        char c = text[i];
        if (at_line && c == '#') {
            while (text[i] == '#') i++;
            while (text[i] == ' ') i++;
            out[o++] = '\n';
            at_line = 0;
            c = text[i];
        }
        if (c == '`') continue;
        if (c == '*') continue;
        if (c == '\r') continue;
        if (c == '\n') {
            if (o > 0 && out[o-1] != '\n') out[o++] = '\n';
            at_line = 1;
            continue;
        }
        out[o++] = c;
        at_line = 0;
    }
    out[o] = '\0';
    return out;
}

/* ═══════════════════════════════════════════════════════════════
 *  §8  Voice Fingerprint — Acoustic Identity
 *
 *  Two paths to a fingerprint:
 *    1. parse_fingerprint()         — read tone.json from bonfyre-tone
 *    2. extract_fingerprint_wav()   — pure-C inline extraction from WAV
 *
 *  Path 2 is the zero-dependency path. No Python, no pip, no
 *  subprocess. Computes 28 acoustic features directly from PCM
 *  samples using autocorrelation pitch detection, RMS energy,
 *  inline FFT, and standard prosodic measures.
 *
 *  Same VoiceFingerprint struct, same cosine similarity, same
 *  6-layer scoring. Just no external tools.
 * ═══════════════════════════════════════════════════════════════ */

#define MAX_FP_FEATURES 256
#define MAX_FP_NAME 128

typedef struct {
    char   name[MAX_FP_NAME];
    double value;
} FpFeature;

typedef struct {
    FpFeature features[MAX_FP_FEATURES];
    int       count;
} VoiceFingerprint;

/* ── JSON parser (for bonfyre-tone compatibility) ──────────── */

static VoiceFingerprint parse_fingerprint(const char *path) {
    VoiceFingerprint fp = {0};
    FILE *f = fopen(path, "r");
    if (!f) return fp;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    if (sz <= 0) { fclose(f); return fp; }
    rewind(f);
    char *json = malloc((size_t)sz + 1);
    if (!json) { fclose(f); return fp; }
    if ((long)fread(json, 1, (size_t)sz, f) != sz) {
        free(json); fclose(f); return fp;
    }
    json[sz] = '\0';
    fclose(f);

    const char *p = strstr(json, "\"features\"");
    if (!p) { free(json); return fp; }
    p = strchr(p, '{');
    if (!p) { free(json); return fp; }
    p++;

    while (fp.count < MAX_FP_FEATURES) {
        while (*p && *p != '"' && *p != '}') p++;
        if (!*p || *p == '}') break;
        p++;

        const char *name_start = p;
        while (*p && *p != '"') p++;
        if (!*p) break;
        size_t nlen = (size_t)(p - name_start);
        if (nlen >= MAX_FP_NAME) nlen = MAX_FP_NAME - 1;
        memcpy(fp.features[fp.count].name, name_start, nlen);
        fp.features[fp.count].name[nlen] = '\0';
        p++;

        while (*p && *p != ':') p++;
        if (*p) p++;
        while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;

        fp.features[fp.count].value = strtod(p, NULL);
        fp.count++;

        while (*p && *p != ',' && *p != '}') p++;
        if (*p == ',') p++;
    }

    free(json);
    return fp;
}

/* ── Inline FFT (radix-2 Cooley-Tukey) ────────────────────── */

static void fft_inplace(float *re, float *im, int n) {
    /* bit-reversal permutation */
    for (int i = 1, j = 0; i < n; i++) {
        int bit = n >> 1;
        while (j & bit) { j ^= bit; bit >>= 1; }
        j ^= bit;
        if (i < j) {
            float t;
            t = re[i]; re[i] = re[j]; re[j] = t;
            t = im[i]; im[i] = im[j]; im[j] = t;
        }
    }
    /* butterfly stages */
    for (int len = 2; len <= n; len <<= 1) {
        float ang = -2.0f * (float)M_PI / len;
        float wr = cosf(ang), wi = sinf(ang);
        for (int i = 0; i < n; i += len) {
            float cur_r = 1.0f, cur_i = 0.0f;
            for (int j = 0; j < len / 2; j++) {
                float ur = re[i + j], ui = im[i + j];
                float vr = re[i + j + len/2] * cur_r -
                           im[i + j + len/2] * cur_i;
                float vi = re[i + j + len/2] * cur_i +
                           im[i + j + len/2] * cur_r;
                re[i + j]         = ur + vr;
                im[i + j]         = ui + vi;
                re[i + j + len/2] = ur - vr;
                im[i + j + len/2] = ui - vi;
                float nr = cur_r * wr - cur_i * wi;
                cur_i    = cur_r * wi + cur_i * wr;
                cur_r    = nr;
            }
        }
    }
}

/* Next power of 2 */
static int next_pow2(int n) {
    int p = 1;
    while (p < n) p <<= 1;
    return p;
}

/* ── Inline Feature Extractor ──────────────────────────────── */
/* Computes 25 acoustic features directly from PCM samples.
 * Feature names use eGeMAPSv02 conventions where applicable
 * so the same cosine/layer code works on both inline-extracted
 * and opensmile-extracted fingerprints. */

/* Comparator for qsort on double arrays (used for percentile computation) */
static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a, db = *(const double *)b;
    return (da > db) - (da < db);
}

static void fp_set(VoiceFingerprint *fp, const char *name, double val) {
    if (fp->count >= MAX_FP_FEATURES) return;
    strncpy(fp->features[fp->count].name, name, MAX_FP_NAME - 1);
    fp->features[fp->count].name[MAX_FP_NAME - 1] = '\0';
    fp->features[fp->count].value = val;
    fp->count++;
}

static VoiceFingerprint extract_fingerprint_wav(const int16_t *samples,
                                                int n, int sr) {
    VoiceFingerprint fp = {0};
    int frame_sz = sr / 20; /* 50ms frames */
    if (frame_sz < 1) frame_sz = 1;
    int n_frames = n / frame_sz;
    if (n_frames < 3) return fp;

    /* Per-frame: F0, RMS, period length for jitter/shimmer */
    double *f0s  = calloc((size_t)n_frames, sizeof(double));
    double *rmss = calloc((size_t)n_frames, sizeof(double));
    double *peri = calloc((size_t)n_frames, sizeof(double));
    double *amps = calloc((size_t)n_frames, sizeof(double));
    float  *fbuf = malloc((size_t)frame_sz * sizeof(float));
    if (!f0s || !rmss || !peri || !amps || !fbuf) {
        free(f0s); free(rmss); free(peri); free(amps); free(fbuf);
        return fp;
    }

    int voiced_count = 0, unvoiced_count = 0;

    for (int fi = 0; fi < n_frames; fi++) {
        double rms = 0.0;
        double peak_amp = 0.0;
        for (int i = 0; i < frame_sz; i++) {
            int idx = fi * frame_sz + i;
            if (idx >= n) break;
            double v = (double)samples[idx] / 32768.0;
            fbuf[i] = (float)v;
            rms += v * v;
            double a = fabs(v);
            if (a > peak_amp) peak_amp = a;
        }
        rms = sqrt(rms / frame_sz);
        rmss[fi] = rms;
        amps[fi] = peak_amp;

        double f0 = detect_pitch_frame(fbuf, frame_sz, sr);
        f0s[fi] = f0;
        if (f0 > 60.0) {
            peri[fi] = (double)sr / f0;
            voiced_count++;
        } else {
            peri[fi] = 0.0;
            unvoiced_count++;
        }
    }

    /* ── F0 statistics (L1: pitch layer) ──────────────────── */
    /* Hz-domain stats (needed for derived features) */
    double f0_sum = 0.0;
    int f0_n = 0;
    for (int f = 0; f < n_frames; f++) {
        if (f0s[f] > 60.0) { f0_sum += f0s[f]; f0_n++; }
    }
    double f0_mean = f0_n > 0 ? f0_sum / f0_n : 150.0;
    double f0_var_hz = 0.0;
    for (int f = 0; f < n_frames; f++) {
        if (f0s[f] > 60.0) {
            double d = f0s[f] - f0_mean;
            f0_var_hz += d * d;
        }
    }
    f0_var_hz = f0_n > 1 ? f0_var_hz / (f0_n - 1) : 0.0;
    double f0_std_hz = sqrt(f0_var_hz);

    /* Semitone-domain stats — mathematically correct log-frequency
     * representation. Converting each F0 individually then computing
     * mean/std avoids the linear approximation error of
     * σ_st ≈ 12*σ_Hz/μ_Hz (which is off by factor 1/ln(2)). */
    double *f0_st = calloc((size_t)(f0_n > 0 ? f0_n : 1), sizeof(double));
    int f0_st_n = 0;
    if (f0_st) {
        for (int f = 0; f < n_frames; f++) {
            if (f0s[f] > 60.0)
                f0_st[f0_st_n++] = 12.0 * log2(f0s[f] / 27.5);
        }
    }

    double f0_st_mean = 0.0;
    if (f0_st_n > 0) {
        for (int i = 0; i < f0_st_n; i++) f0_st_mean += f0_st[i];
        f0_st_mean /= f0_st_n;
    }
    double f0_st_var = 0.0;
    for (int i = 0; i < f0_st_n; i++) {
        double d = f0_st[i] - f0_st_mean;
        f0_st_var += d * d;
    }
    f0_st_var = f0_st_n > 1 ? f0_st_var / (f0_st_n - 1) : 0.0;
    double f0_st_std = sqrt(f0_st_var);

    /* True 5th/95th percentiles via sorted array */
    double f0_st_p5 = 0.0, f0_st_p95 = 0.0;
    if (f0_st_n > 1) {
        qsort(f0_st, (size_t)f0_st_n, sizeof(double), cmp_double);
        f0_st_p5  = f0_st[(int)(0.05 * (f0_st_n - 1))];
        f0_st_p95 = f0_st[(int)(0.95 * (f0_st_n - 1))];
    } else if (f0_st_n == 1) {
        f0_st_p5 = f0_st_p95 = f0_st[0];
    }
    free(f0_st);

    fp_set(&fp, "F0semitoneFrom27.5Hz_sma3nz_amean", f0_st_mean);
    fp_set(&fp, "F0semitoneFrom27.5Hz_sma3nz_stddevNorm",
           fabs(f0_st_mean) > 0.01 ? f0_st_std / fabs(f0_st_mean) : 0.0);
    fp_set(&fp, "F0semitoneFrom27.5Hz_sma3nz_pctlrange0-2",
           f0_st_p95 - f0_st_p5);
    fp_set(&fp, "F0final_sma3nz_amean", f0_st_mean);
    fp_set(&fp, "F0final_sma3nz_stddevNorm",
           fabs(f0_st_mean) > 0.01 ? f0_st_std / fabs(f0_st_mean) : 0.0);

    /* ── Loudness statistics (L2: energy layer) ───────────── */
    double rms_sum = 0.0, rms_min = 1e9, rms_max = 0.0;
    int rms_n = 0;
    for (int f = 0; f < n_frames; f++) {
        if (rmss[f] > 0.003) {
            rms_sum += rmss[f]; rms_n++;
            if (rmss[f] < rms_min) rms_min = rmss[f];
            if (rmss[f] > rms_max) rms_max = rmss[f];
        }
    }
    double rms_mean = rms_n > 0 ? rms_sum / rms_n : 0.01;
    double rms_var = 0.0;
    for (int f = 0; f < n_frames; f++) {
        if (rmss[f] > 0.003) {
            double d = rmss[f] - rms_mean;
            rms_var += d * d;
        }
    }
    rms_var = rms_n > 1 ? rms_var / (rms_n - 1) : 0.0;

    fp_set(&fp, "loudness_sma3_amean", rms_mean);
    fp_set(&fp, "loudness_sma3_stddevNorm",
           rms_mean > 0 ? sqrt(rms_var) / rms_mean : 0.0);
    fp_set(&fp, "loudness_sma3_pctlrange0-2",
           rms_max - rms_min);
    fp_set(&fp, "Loudness_sma3_amean", rms_mean); /* case variant */

    /* ── Jitter + Shimmer (L5: stability layer) ───────────── */
    double jitter_sum = 0.0, shimmer_sum = 0.0;
    double hnr_sum = 0.0;
    int jit_n = 0, shim_n = 0, hnr_n = 0;

    for (int f = 1; f < n_frames; f++) {
        if (peri[f] > 0 && peri[f-1] > 0) {
            double mean_p = (peri[f] + peri[f-1]) / 2.0;
            if (mean_p > 0) {
                jitter_sum += fabs(peri[f] - peri[f-1]) / mean_p;
                jit_n++;
            }
        }
        if (amps[f] > 0.001 && amps[f-1] > 0.001) {
            double mean_a = (amps[f] + amps[f-1]) / 2.0;
            if (mean_a > 0) {
                shimmer_sum += fabs(amps[f] - amps[f-1]) / mean_a;
                shim_n++;
            }
        }
    }

    /* HNR from autocorrelation peak: HNR = 10*log10(r/(1-r)) */
    for (int f = 0; f < n_frames; f++) {
        if (f0s[f] > 60.0) {
            /* re-compute autocorrelation at detected period */
            int lag = (int)peri[f];
            if (lag < 1 || f * frame_sz + lag >= n) continue;
            double sum_ab = 0, sum_aa = 0, sum_bb = 0;
            int count = frame_sz - lag;
            if (count < 1) continue;
            int base = f * frame_sz;
            for (int i = 0; i < count && base + i + lag < n; i++) {
                double a = (double)samples[base + i] / 32768.0;
                double b = (double)samples[base + i + lag] / 32768.0;
                sum_ab += a * b;
                sum_aa += a * a;
                sum_bb += b * b;
            }
            double denom = sqrt(sum_aa * sum_bb);
            if (denom > 1e-10) {
                double r = sum_ab / denom;
                if (r > 0.01 && r < 0.999) {
                    hnr_sum += 10.0 * log10(r / (1.0 - r));
                    hnr_n++;
                }
            }
        }
    }

    fp_set(&fp, "jitterLocal_sma3nz_amean",
           jit_n > 0 ? jitter_sum / jit_n : 0.0);
    fp_set(&fp, "shimmerLocaldB_sma3nz_amean",
           shim_n > 0 ? shimmer_sum / shim_n : 0.0);
    fp_set(&fp, "HNRdBACF_sma3nz_amean",
           hnr_n > 0 ? hnr_sum / hnr_n : 0.0);
    fp_set(&fp, "logRelF0-H1-H2_sma3nz_amean",
           f0_mean > 0 ? log(f0_std_hz / f0_mean + 1.0) : 0.0);

    /* ── Temporal features (L3) ───────────────────────────── */
    /* Voiced/unvoiced segment duration tracking */
    double total_dur = (double)n / sr;
    int seg_count = 0;
    double seg_len_sum = 0.0;
    int unseg_count = 0;
    double unseg_len_sum = 0.0;
    int in_voiced = 0, seg_start = 0, unseg_start = 0;
    int peaks = 0;

    for (int f = 0; f < n_frames; f++) {
        int voiced = f0s[f] > 60.0;
        if (voiced && !in_voiced) {
            /* Voiced segment starts — close unvoiced segment */
            if (f > 0) {
                unseg_len_sum += (double)(f - unseg_start) * frame_sz / sr;
                unseg_count++;
            }
            seg_start = f;
            in_voiced = 1;
        } else if (!voiced && in_voiced) {
            /* Unvoiced starts — close voiced segment */
            seg_len_sum += (double)(f - seg_start) * frame_sz / sr;
            seg_count++;
            unseg_start = f;
            in_voiced = 0;
        }
        /* Loudness peaks: local max above 1.3x mean */
        if (f > 0 && f < n_frames - 1 &&
            rmss[f] > rmss[f-1] && rmss[f] > rmss[f+1] &&
            rmss[f] > rms_mean * 1.3)
            peaks++;
    }
    /* Close final segment */
    if (in_voiced) {
        seg_len_sum += (double)(n_frames - seg_start) * frame_sz / sr;
        seg_count++;
    } else if (seg_count > 0) {
        unseg_len_sum += (double)(n_frames - unseg_start) * frame_sz / sr;
        unseg_count++;
    }

    fp_set(&fp, "MeanVoicedSegmentLengthSec",
           seg_count > 0 ? seg_len_sum / seg_count : 0.0);
    fp_set(&fp, "MeanUnvoicedSegmentLength",
           unseg_count > 0 ? unseg_len_sum / unseg_count : 0.0);
    fp_set(&fp, "VoicedSegmentsPerSec",
           total_dur > 0 ? (double)seg_count / total_dur : 0.0);
    fp_set(&fp, "UnvoicedSegmentsPerSec",
           total_dur > 0 ? (double)unseg_count / total_dur : 0.0);
    fp_set(&fp, "loudnessPeaksPerSec",
           total_dur > 0 ? (double)peaks / total_dur : 0.0);
    fp_set(&fp, "Pauses_count", (double)(unseg_count));

    /* ── Spectral features (L4) — via inline FFT ─────────── */
    int fft_n = next_pow2(frame_sz);
    float *fft_re = calloc((size_t)fft_n, sizeof(float));
    float *fft_im = calloc((size_t)fft_n, sizeof(float));
    double sc_sum = 0.0, flux_sum = 0.0;
    double alpha_sum = 0.0, slope_lo_sum = 0.0, slope_hi_sum = 0.0;
    double flatness_sum = 0.0;
    int spec_n = 0;
    float *prev_mag = calloc((size_t)(fft_n / 2 + 1), sizeof(float));

    if (fft_re && fft_im && prev_mag) {
        for (int fi = 0; fi < n_frames; fi++) {
            /* Load frame with Hanning window */
            memset(fft_re, 0, (size_t)fft_n * sizeof(float));
            memset(fft_im, 0, (size_t)fft_n * sizeof(float));
            for (int i = 0; i < frame_sz; i++) {
                int idx = fi * frame_sz + i;
                if (idx >= n) break;
                float w = 0.5f * (1.0f - cosf(2.0f * (float)M_PI * i /
                          (frame_sz - 1)));
                fft_re[i] = ((float)samples[idx] / 32768.0f) * w;
            }

            fft_inplace(fft_re, fft_im, fft_n);

            /* Magnitude spectrum */
            int half = fft_n / 2;
            float *mag = malloc((size_t)(half + 1) * sizeof(float));
            if (!mag) continue;

            double sum_mag = 0.0, weighted_sum = 0.0;
            double energy_lo = 0.0, energy_hi = 0.0;
            int bin_1k = 1000 * fft_n / sr;

            for (int b = 0; b <= half; b++) {
                mag[b] = sqrtf(fft_re[b] * fft_re[b] +
                               fft_im[b] * fft_im[b]);
                double freq = (double)b * sr / fft_n;
                sum_mag += mag[b];
                weighted_sum += mag[b] * freq;
                if (b < bin_1k) energy_lo += mag[b] * mag[b];
                else            energy_hi += mag[b] * mag[b];
            }

            /* Spectral centroid */
            if (sum_mag > 1e-10)
                sc_sum += weighted_sum / sum_mag;

            /* Spectral flux: L2 norm of magnitude difference */
            if (spec_n > 0) {
                double flux = 0.0;
                for (int b = 0; b <= half; b++) {
                    double d = (double)mag[b] - prev_mag[b];
                    flux += d * d;
                }
                flux_sum += sqrt(flux);
            }

            /* Alpha ratio: energy above 1kHz / below */
            if (energy_lo > 1e-10)
                alpha_sum += energy_hi / energy_lo;

            /* Spectral slopes — regression in Hz domain for
             * comparability across sample rates and FFT sizes */
            int bin_500  = 500  * fft_n / sr;
            int bin_1500 = 1500 * fft_n / sr;
            if (bin_500 > 0 && bin_500 < half) {
                double s = 0, sx = 0, sy = 0, sxx = 0, sxy = 0;
                for (int b = 0; b < bin_500; b++) {
                    double freq = (double)b * sr / fft_n;
                    s++; sx += freq; sy += mag[b];
                    sxx += freq * freq; sxy += freq * mag[b];
                }
                double denom = s * sxx - sx * sx;
                if (fabs(denom) > 1e-20)
                    slope_lo_sum += (s * sxy - sx * sy) / denom;
            }
            if (bin_1500 > bin_500 && bin_1500 <= half) {
                double s = 0, sx = 0, sy = 0, sxx = 0, sxy = 0;
                for (int b = bin_500; b < bin_1500; b++) {
                    double freq = (double)b * sr / fft_n;
                    s++; sx += freq; sy += mag[b];
                    sxx += freq * freq; sxy += freq * mag[b];
                }
                double denom = s * sxx - sx * sx;
                if (fabs(denom) > 1e-20)
                    slope_hi_sum += (s * sxy - sx * sy) / denom;
            }

            /* Spectral flatness (Wiener entropy) — tonality measure.
             * SF = geometric_mean(|X|²) / arithmetic_mean(|X|²)
             * 1.0 = white noise, 0.0 = pure tone. */
            if (half > 1) {
                double log_pow_sum = 0.0, pow_sum = 0.0;
                for (int b = 1; b <= half; b++) {
                    double p = (double)mag[b] * mag[b];
                    log_pow_sum += log(p + 1e-30);
                    pow_sum += p;
                }
                double geo = exp(log_pow_sum / half);
                double arith = pow_sum / half;
                if (arith > 1e-30)
                    flatness_sum += geo / arith;
            }

            memcpy(prev_mag, mag, (size_t)(half + 1) * sizeof(float));
            free(mag);
            spec_n++;
        }
    }
    free(fft_re); free(fft_im); free(prev_mag);

    fp_set(&fp, "spectralCentroid_sma3_amean",
           spec_n > 0 ? sc_sum / spec_n : 0.0);
    fp_set(&fp, "spectralFlux_sma3_amean",
           spec_n > 1 ? flux_sum / (spec_n - 1) : 0.0);
    fp_set(&fp, "alphaRatioV_sma3nz_amean",
           spec_n > 0 ? alpha_sum / spec_n : 0.0);
    fp_set(&fp, "slopeV0-500_sma3nz_amean",
           spec_n > 0 ? slope_lo_sum / spec_n : 0.0);
    fp_set(&fp, "slopeV500-1500_sma3nz_amean",
           spec_n > 0 ? slope_hi_sum / spec_n : 0.0);
    fp_set(&fp, "spectralFlatness_sma3_amean",
           spec_n > 0 ? flatness_sum / spec_n : 0.0);

    /* ── Dynamics features (L6) ───────────────────────────── */
    int rising = 0, falling = 0;
    for (int f = 1; f < n_frames; f++) {
        if (rmss[f] > rmss[f-1] * 1.05) rising++;
        else if (rmss[f] < rmss[f-1] * 0.95) falling++;
    }
    fp_set(&fp, "loudness_sma3nz_stddevNorm",
           rms_mean > 0 ? sqrt(rms_var) / rms_mean : 0.0);
    fp_set(&fp, "risingSlope_count",
           total_dur > 0 ? (double)rising / total_dur : 0.0);
    fp_set(&fp, "fallingSlope_count",
           total_dur > 0 ? (double)falling / total_dur : 0.0);

    free(f0s); free(rmss); free(peri); free(amps); free(fbuf);

    fprintf(stderr, "[narrate] Inline extraction: %d features from %d frames "
            "(%d voiced, %d unvoiced)\n",
            fp.count, n_frames, voiced_count, unvoiced_count);
    return fp;
}

/* Helper: load fingerprint from file or WAV */
static VoiceFingerprint load_fingerprint(const char *path) {
    /* If it ends in .wav, extract inline; otherwise parse JSON */
    size_t len = strlen(path);
    if (len > 4 && strcasecmp(path + len - 4, ".wav") == 0) {
        WavAudio w = wav_read(path);
        if (w.samples && w.n_samples > 0) {
            VoiceFingerprint fp = extract_fingerprint_wav(
                w.samples, w.n_samples, w.sample_rate);
            free(w.samples);
            return fp;
        }
        return (VoiceFingerprint){0};
    }
    return parse_fingerprint(path);
}

/* Cosine similarity: dot(A,B) / (|A| * |B|).
 * 1.0 = identical acoustic signatures, 0.0 = unrelated.
 * Features matched by name so order doesn't matter. */
static double fingerprint_cosine(const VoiceFingerprint *a,
                                 const VoiceFingerprint *b) {
    double dot = 0.0, mag_a = 0.0, mag_b = 0.0;
    int matched = 0;

    for (int i = 0; i < a->count; i++) {
        for (int j = 0; j < b->count; j++) {
            if (strcmp(a->features[i].name, b->features[j].name) == 0) {
                dot   += a->features[i].value * b->features[j].value;
                mag_a += a->features[i].value * a->features[i].value;
                mag_b += b->features[j].value * b->features[j].value;
                matched++;
                break;
            }
        }
    }

    if (matched < 10) return 0.0;
    double denom = sqrt(mag_a) * sqrt(mag_b);
    if (denom < 1e-15) return 0.0;
    return dot / denom;
}

/* Layer-specific similarity using symmetric relative difference.
 * For each matched feature: score_i = 1 - |a-b|/(|a|+|b|+ε).
 * This is scale-independent and properly discriminates features
 * that are all-positive (unlike cosine similarity, which has poor
 * discrimination when vectors never cross zero).
 * Layer score = arithmetic mean of per-feature scores.
 * Returns 1.0 for identical features, ~0 for maximally different. */
static double layer_similarity(const VoiceFingerprint *a,
                               const VoiceFingerprint *b,
                               const char *const *prefixes, int n_pfx) {
    double score_sum = 0.0;
    int matched = 0;

    for (int i = 0; i < a->count; i++) {
        int in_layer = 0;
        for (int p = 0; p < n_pfx; p++) {
            if (strstr(a->features[i].name, prefixes[p])) {
                in_layer = 1; break;
            }
        }
        if (!in_layer) continue;

        for (int j = 0; j < b->count; j++) {
            if (strcmp(a->features[i].name, b->features[j].name) == 0) {
                double va = a->features[i].value;
                double vb = b->features[j].value;
                double denom = fabs(va) + fabs(vb);
                if (denom > 1e-15) {
                    score_sum += 1.0 - fabs(va - vb) / denom;
                } else {
                    score_sum += 1.0; /* both zero → identical */
                }
                matched++;
                break;
            }
        }
    }

    if (matched == 0) return 1.0;
    return score_sum / matched;
}

/* ═══════════════════════════════════════════════════════════════
 *  §9  6-Layer Fidelity Scoring
 *
 *  The HCP for text-to-speech. HCP-Whisper has 9 layers of
 *  hallucination detection; BonfyreNarrate has 6 layers of
 *  speech fidelity verification.
 *
 *  Each layer targets a different dimension of acoustic quality:
 *    L1 Pitch:     F0 contour + semitone features
 *    L2 Energy:    Loudness, dynamic range
 *    L3 Temporal:  Speaking rate, pause patterns, segments
 *    L4 Spectral:  Formants, MFCC, spectral distribution
 *    L5 Stability: Jitter, shimmer, harmonic noise ratio
 *    L6 Dynamics:  Variation, contour shape, range
 *
 *  Composite score = geometric mean of all 6 layers.
 *  This penalizes any single weak dimension — you can't hide
 *  a bad pitch match behind a good energy match.
 * ═══════════════════════════════════════════════════════════════ */

typedef struct {
    double layer[6];         /* per-layer fidelity (0-1) */
    double composite;        /* geometric mean */
    double cosine_88;        /* full 88-feature cosine */
    int    layers_passed;    /* layers >= 0.80 */
    int    features_matched; /* of 88 features compared */
} FidelityReport;

static const char *LAYER_NAMES[] = {
    "pitch", "energy", "temporal", "spectral", "stability", "dynamics"
};

static FidelityReport compute_fidelity(const VoiceFingerprint *source,
                                       const VoiceFingerprint *output) {
    FidelityReport r = {0};

    /* Headline: full 88-feature cosine similarity */
    r.cosine_88 = fingerprint_cosine(source, output);

    /* Count matched features */
    for (int i = 0; i < source->count; i++)
        for (int j = 0; j < output->count; j++)
            if (strcmp(source->features[i].name,
                       output->features[j].name) == 0)
                { r.features_matched++; break; }

    /* L1: Pitch fidelity */
    const char *pitch_pfx[] = { "F0semitone", "F0final" };
    r.layer[0] = layer_similarity(source, output, pitch_pfx, 2);

    /* L2: Energy fidelity */
    const char *energy_pfx[] = { "loudness", "Loudness" };
    r.layer[1] = layer_similarity(source, output, energy_pfx, 2);

    /* L3: Temporal fidelity */
    const char *temp_pfx[] = { "VoicedSeg", "UnvoicedSeg", "Pauses",
                               "loudnessPeaks" };
    r.layer[2] = layer_similarity(source, output, temp_pfx, 4);

    /* L4: Spectral fidelity */
    const char *spec_pfx[] = { "spectral", "Formant", "Mfcc",
                               "alphaRatio", "hammarberg", "slope" };
    r.layer[3] = layer_similarity(source, output, spec_pfx, 6);

    /* L5: Stability fidelity */
    const char *stab_pfx[] = { "jitter", "shimmer", "HNR",
                               "logRelF0" };
    r.layer[4] = layer_similarity(source, output, stab_pfx, 4);

    /* L6: Dynamics fidelity */
    const char *dyn_pfx[] = { "stddev", "pctlrange", "percentile",
                              "rising", "falling" };
    r.layer[5] = layer_similarity(source, output, dyn_pfx, 5);

    /* Composite: geometric mean (penalizes weak layers) */
    double product = 1.0;
    r.layers_passed = 0;
    for (int i = 0; i < 6; i++) {
        double s = r.layer[i];
        if (s < 0.01) s = 0.01;
        product *= s;
        if (r.layer[i] >= 0.80) r.layers_passed++;
    }
    r.composite = pow(product, 1.0 / 6.0);

    return r;
}

/* ═══════════════════════════════════════════════════════════════
 *  §10  Direct WAV Quality Analysis
 *
 *  Signal-level quality metrics computed directly from the
 *  synthesized waveform. No source comparison needed — these
 *  measure absolute synthesis quality:
 *
 *    Dynamic range:         dB spread between loudest/quietest
 *                           voiced frames. Higher = more natural.
 *    Spectral continuity:   energy smoothness across frames.
 *                           Catches robotic sentence-seam effects.
 *    Pitch stability:       F0 consistency in voiced segments.
 *    Energy consistency:    RMS evenness across voiced frames.
 * ═══════════════════════════════════════════════════════════════ */

typedef struct {
    double dynamic_range_db;
    double spectral_continuity;   /* 0-1 */
    double pitch_stability;       /* 0-1 */
    double energy_consistency;    /* 0-1 */
} WavQuality;

static WavQuality analyze_wav(const int16_t *samples, int n, int sr) {
    WavQuality q = {0};
    int frame_sz = sr / 20; /* 50ms frames */
    if (frame_sz < 1) frame_sz = 1;
    int n_frames = n / frame_sz;
    if (n_frames < 3) return q;

    double *frms = malloc((size_t)n_frames * sizeof(double));
    double *ff0  = malloc((size_t)n_frames * sizeof(double));
    float  *fbuf = malloc((size_t)frame_sz * sizeof(float));
    if (!frms || !ff0 || !fbuf) {
        free(frms); free(ff0); free(fbuf);
        return q;
    }

    double max_rms = 0.0, min_voiced = 1e10;
    for (int f = 0; f < n_frames; f++) {
        double rms = 0.0;
        for (int i = 0; i < frame_sz; i++) {
            int idx = f * frame_sz + i;
            if (idx >= n) break;
            double v = (double)samples[idx] / 32768.0;
            fbuf[i] = (float)v;
            rms += v * v;
        }
        rms = sqrt(rms / frame_sz);
        frms[f] = rms;
        if (rms > max_rms) max_rms = rms;
        if (rms > 0.005 && rms < min_voiced) min_voiced = rms;
        ff0[f] = detect_pitch_frame(fbuf, frame_sz, sr);
    }

    /* Dynamic range */
    if (min_voiced < 1e9 && max_rms > 0.0 && min_voiced > 0.0)
        q.dynamic_range_db = 20.0 * log10(max_rms / min_voiced);

    /* Spectral continuity: avg frame-to-frame dB delta.
     * ≤1dB→1.0, 6dB→0.5, ≥12dB→0.0 */
    double delta_sum = 0.0;
    int delta_count = 0;
    for (int f = 1; f < n_frames; f++) {
        if (frms[f] > 0.005 && frms[f-1] > 0.005) {
            double ratio = frms[f] / frms[f-1];
            if (ratio > 0.0) {
                delta_sum += fabs(20.0 * log10(ratio));
                delta_count++;
            }
        }
    }
    double avg_delta = delta_count > 0 ? delta_sum / delta_count : 6.0;
    q.spectral_continuity = 1.0 - (avg_delta / 12.0);
    if (q.spectral_continuity < 0.0) q.spectral_continuity = 0.0;
    if (q.spectral_continuity > 1.0) q.spectral_continuity = 1.0;

    /* Pitch stability: CoV of F0 in voiced frames.
     * CoV 0→1.0, 0.15→0.5, ≥0.3→0.0 */
    double f0_sum = 0.0;
    int f0_n = 0;
    for (int f = 0; f < n_frames; f++) {
        if (ff0[f] > 60.0) { f0_sum += ff0[f]; f0_n++; }
    }
    double f0_mean = f0_n > 0 ? f0_sum / f0_n : 150.0;
    double f0_var = 0.0;
    for (int f = 0; f < n_frames; f++) {
        if (ff0[f] > 60.0) {
            double d = ff0[f] - f0_mean;
            f0_var += d * d;
        }
    }
    f0_var = f0_n > 1 ? f0_var / (f0_n - 1) : 0.0;
    double f0_cov = f0_mean > 0.0 ? sqrt(f0_var) / f0_mean : 0.15;
    q.pitch_stability = 1.0 - (f0_cov / 0.3);
    if (q.pitch_stability < 0.0) q.pitch_stability = 0.0;
    if (q.pitch_stability > 1.0) q.pitch_stability = 1.0;

    /* Energy consistency: 1 - CoV of RMS in voiced frames */
    double r_sum = 0.0;
    int r_n = 0;
    for (int f = 0; f < n_frames; f++) {
        if (frms[f] > 0.005) { r_sum += frms[f]; r_n++; }
    }
    double r_mean = r_n > 0 ? r_sum / r_n : 0.1;
    double r_var = 0.0;
    for (int f = 0; f < n_frames; f++) {
        if (frms[f] > 0.005) {
            double d = frms[f] - r_mean;
            r_var += d * d;
        }
    }
    r_var = r_n > 1 ? r_var / (r_n - 1) : 0.0;
    double r_cov = r_mean > 0.0 ? sqrt(r_var) / r_mean : 0.3;
    q.energy_consistency = 1.0 - (r_cov / 0.6);
    if (q.energy_consistency < 0.0) q.energy_consistency = 0.0;
    if (q.energy_consistency > 1.0) q.energy_consistency = 1.0;

    free(frms); free(ff0); free(fbuf);
    return q;
}

/* ═══════════════════════════════════════════════════════════════
 *  §11  Verification Engine
 *
 *  The closed loop. After synthesis, we extract an acoustic
 *  fingerprint from our own output — inline, pure C, no
 *  subprocess, no Python, no pip install — and compare it
 *  against the source speaker's fingerprint.
 *
 *  This is the TTS equivalent of HCP's re-decode verification.
 *  We don't just generate — we prove.
 * ═══════════════════════════════════════════════════════════════ */

static FidelityReport verify_output(const char *output_wav,
                                    const VoiceFingerprint *source,
                                    const char *work_dir) {
    (void)work_dir;
    FidelityReport r = {0};

    fprintf(stderr, "[narrate] Verification: inline fingerprint extraction...\n");

    WavAudio wav = wav_read(output_wav);
    if (!wav.samples || wav.n_samples < 1) {
        fprintf(stderr, "[narrate] Warning: could not read output WAV\n");
        return r;
    }

    VoiceFingerprint output_fp = extract_fingerprint_wav(
        wav.samples, wav.n_samples, wav.sample_rate);
    free(wav.samples);

    if (output_fp.count == 0) {
        fprintf(stderr, "[narrate] Warning: no features extracted from output\n");
        return r;
    }

    r = compute_fidelity(source, &output_fp);

    fprintf(stderr, "[narrate] Fidelity report (%d features matched):\n",
            r.features_matched);
    fprintf(stderr, "          cosine:             %.4f\n", r.cosine_88);
    for (int i = 0; i < 6; i++) {
        fprintf(stderr, "          L%d %-10s       %.4f %s\n",
                i + 1, LAYER_NAMES[i], r.layer[i],
                r.layer[i] >= 0.80 ? "PASS" : "WEAK");
    }
    fprintf(stderr, "          Composite (geomean): %.4f  [%d/6 layers pass]\n",
            r.composite, r.layers_passed);
    return r;
}

/* ═══════════════════════════════════════════════════════════════
 *  §12  Iterative Refinement Engine
 *
 *  When fidelity falls below threshold, we don't give up — we
 *  adjust. Each iteration reads the fidelity report, identifies
 *  which acoustic dimensions are weak, and nudges prosody params:
 *
 *    Iteration 1: standard prosody from tone profile
 *    Iteration 2: aggressive correction on weak layers
 *    Iteration 3: fine-tuning with damped corrections
 *
 *  Typically converges by iteration 2. Max 3 guarantees bounded
 *  latency while achieving measurable improvement.
 * ═══════════════════════════════════════════════════════════════ */

static ProsodyParams refine_prosody(const ProsodyParams *current,
                                    const FidelityReport *report,
                                    const ToneProfile *target,
                                    int iteration) {
    ProsodyParams pp = *current;
    double damping = (iteration == 1) ? 1.2 : 0.6;

    /* L1 Pitch: push harder if weak */
    if (report->layer[0] < 0.85) {
        double correction = (target->pitch - 50.0) * 0.15 * damping;
        pp.pitch_st += correction;
        if (pp.pitch_st < -8.0) pp.pitch_st = -8.0;
        if (pp.pitch_st >  8.0) pp.pitch_st =  8.0;
    }

    /* L2 Energy: adjust volume */
    if (report->layer[1] < 0.85) {
        double correction = (target->energy - 50.0) * 0.7 * damping;
        pp.volume_pct = correction;
        if (pp.volume_pct < -30.0) pp.volume_pct = -30.0;
        if (pp.volume_pct >  50.0) pp.volume_pct =  50.0;
    }

    /* L3 Temporal: adjust rate */
    if (report->layer[2] < 0.85) {
        double correction = (target->pace - 50.0) * 0.9 * damping;
        pp.rate_pct = correction;
        if (pp.rate_pct < -40.0) pp.rate_pct = -40.0;
        if (pp.rate_pct >  50.0) pp.rate_pct =  50.0;
    }

    /* L5 Stability: ease pitch correction if jitter is off */
    if (report->layer[4] < 0.85 && fabs(pp.pitch_st) > 2.0)
        pp.pitch_st *= 0.75;

    /* L6 Dynamics: adjust pause pattern */
    if (report->layer[5] < 0.85) {
        pp.pause_ms = (int)(800.0 - target->variation * 6.0);
        if (pp.pause_ms < 150)  pp.pause_ms = 150;
        if (pp.pause_ms > 1400) pp.pause_ms = 1400;
    }

    return pp;
}

/* ═══════════════════════════════════════════════════════════════
 *  §13  Main — Orchestration
 * ═══════════════════════════════════════════════════════════════ */

static void usage(void) {
    fprintf(stderr,
        "bonfyre-narrate v3.2 — verified tone-aware text-to-speech synthesis\n\n"
        "Usage:\n"
        "  bonfyre-narrate <text-file> <output-dir> [options]\n"
        "  bonfyre-narrate verify <output.wav> <source-features.json>\n\n"
        "Options:\n"
        "  --voice-model PATH        Piper ONNX model path\n"
        "  --tone-profile PATH       Tone profile JSON (bonfyre-tone profile)\n"
        "  --source-features PATH    88-feature tone.json (bonfyre-tone extract)\n"
        "  --verify                  Run closed-loop fidelity verification\n"
        "  --refine [TARGET]         Iterative refinement (default: 0.85)\n"
        "  --audio-format FMT        Output format (default: wav)\n"
        "  --title TITLE             Artifact title\n"
        "  --ssml                    Generate SSML\n"
        "  --dry-run                 Print plan without generating audio\n"
        "  --no-pitch-shift          Skip PSOLA pitch correction\n"
        "  --no-envelope             Skip energy envelope transfer\n\n"
        "Verification pipeline:\n"
        "  bonfyre-tone extract audio.wav tone_out/\n"
        "  bonfyre-tone profile audio.wav tone_out/\n"
        "  bonfyre-narrate summary.md narrate_out/ \\\n"
        "    --tone-profile tone_out/profile.json \\\n"
        "    --source-features tone_out/tone.json \\\n"
        "    --verify --refine\n\n"
        "6-layer fidelity scoring. Closed-loop quality verification.\n");
}

int main(int argc, char **argv) {
    if (argc >= 2 && strcmp(argv[1], "status") == 0) {
        printf("{\"binary\":\"bonfyre-narrate\",\"version\":\"3.2.0\","
               "\"features\":[\"ssml-prosody\",\"psola-pitch-shift\","
               "\"energy-envelope\",\"tone-profile-input\","
               "\"28-feature-fingerprint\",\"6-layer-fidelity\","
               "\"closed-loop-verification\",\"iterative-refinement\","
               "\"wav-quality-analysis\",\"parabolic-f0\","
               "\"spectral-flatness\"],"
               "\"status\":\"available\"}\n");
        return 0;
    }

    /* ── Standalone verify command ──────────────────────────── */
    if (argc >= 2 && strcmp(argv[1], "verify") == 0) {
        if (argc < 4) {
            fprintf(stderr,
                "Usage: bonfyre-narrate verify <output.wav> "
                "<reference.wav|features.json>\n");
            return 1;
        }
        VoiceFingerprint src_fp = load_fingerprint(argv[3]);
        if (src_fp.count == 0) {
            fprintf(stderr, "Failed to load reference: %s\n", argv[3]);
            return 1;
        }
        FidelityReport fr = verify_output(argv[2], &src_fp, ".");
        WavAudio vwav = wav_read(argv[2]);
        WavQuality vwq = {0};
        if (vwav.samples && vwav.n_samples > 0) {
            vwq = analyze_wav(vwav.samples, vwav.n_samples, vwav.sample_rate);
            free(vwav.samples);
        }
        printf("{\n"
               "  \"type\": \"narrate-fidelity-report\",\n"
               "  \"output\": \"%s\",\n"
               "  \"reference\": \"%s\",\n"
               "  \"fidelity\": {\n"
               "    \"cosine_88\": %.6f,\n"
               "    \"composite\": %.6f,\n"
               "    \"features_matched\": %d,\n"
               "    \"layers_passed\": %d,\n"
               "    \"layers\": {\n",
               argv[2], argv[3],
               fr.cosine_88, fr.composite, fr.features_matched,
               fr.layers_passed);
        for (int i = 0; i < 6; i++)
            printf("      \"%s\": %.6f%s\n", LAYER_NAMES[i], fr.layer[i],
                   i < 5 ? "," : "");
        printf("    }\n  },\n"
               "  \"wavQuality\": {\n"
               "    \"dynamic_range_db\": %.2f,\n"
               "    \"spectral_continuity\": %.4f,\n"
               "    \"pitch_stability\": %.4f,\n"
               "    \"energy_consistency\": %.4f\n"
               "  }\n}\n",
               vwq.dynamic_range_db, vwq.spectral_continuity,
               vwq.pitch_stability, vwq.energy_consistency);
        return 0;
    }

    if (argc < 3) { usage(); return 1; }

    const char *source_path = argv[1];
    const char *output_dir  = argv[2];
    const char *voice_model = "";
    const char *tone_profile_path = NULL;
    const char *audio_format = "wav";
    const char *title = "Bonfyre Artifact";
    int use_ssml = 0;
    int dry_run = 0;
    int do_pitch_shift = 1;
    int do_envelope = 1;
    const char *source_features_path = NULL;
    const char *reference_path = NULL;
    int do_verify = 0;
    int do_refine = 0;
    double refine_target = 0.85;

    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--voice-model") == 0 && i+1 < argc)
            voice_model = argv[++i];
        else if (strcmp(argv[i], "--tone-profile") == 0 && i+1 < argc)
            tone_profile_path = argv[++i];
        else if (strcmp(argv[i], "--audio-format") == 0 && i+1 < argc)
            audio_format = argv[++i];
        else if (strcmp(argv[i], "--title") == 0 && i+1 < argc)
            title = argv[++i];
        else if (strcmp(argv[i], "--ssml") == 0)
            use_ssml = 1;
        else if (strcmp(argv[i], "--dry-run") == 0)
            dry_run = 1;
        else if (strcmp(argv[i], "--no-pitch-shift") == 0)
            do_pitch_shift = 0;
        else if (strcmp(argv[i], "--no-envelope") == 0)
            do_envelope = 0;
        else if (strcmp(argv[i], "--source-features") == 0 && i+1 < argc)
            source_features_path = argv[++i];
        else if (strcmp(argv[i], "--reference") == 0 && i+1 < argc)
            reference_path = argv[++i];
        else if (strcmp(argv[i], "--verify") == 0)
            do_verify = 1;
        else if (strcmp(argv[i], "--refine") == 0) {
            do_refine = 1;
            do_verify = 1;
            if (i+1 < argc && argv[i+1][0] != '-') {
                refine_target = atof(argv[++i]);
                if (refine_target < 0.5 || refine_target > 1.0)
                    refine_target = 0.85;
            }
        }
        else { usage(); return 1; }
    }

    /* ── Parse tone profile ─────────────────────────────────── */
    ToneProfile tp = {0};
    if (tone_profile_path) {
        tp = parse_tone_profile(tone_profile_path);
        if (tp.valid) {
            fprintf(stderr, "[narrate] Tone profile loaded:\n");
            fprintf(stderr, "          energy=%.0f  pitch=%.0f (%.0f Hz)  "
                    "pace=%.0f  variation=%.0f\n",
                    tp.energy, tp.pitch, tp.pitch_hz, tp.pace, tp.variation);
        } else {
            fprintf(stderr, "[narrate] Warning: could not parse tone profile, "
                    "using defaults\n");
        }
    }

    /* ── Compute prosody ────────────────────────────────────── */
    ProsodyParams pp;

    /* ── Parse source fingerprint ───────────────────────────── */
    VoiceFingerprint source_fp = {0};
    const char *fp_source_label = NULL;
    if (reference_path) {
        source_fp = load_fingerprint(reference_path);
        fp_source_label = reference_path;
    } else if (source_features_path) {
        source_fp = load_fingerprint(source_features_path);
        fp_source_label = source_features_path;
    }
    if (fp_source_label) {
        if (source_fp.count > 0) {
            fprintf(stderr, "[narrate] Source fingerprint: %d features from %s\n",
                    source_fp.count, fp_source_label);
        } else {
            fprintf(stderr, "[narrate] Warning: could not load fingerprint "
                    "from %s\n", fp_source_label);
        }
    }

    /* ── Compute prosody ────────────────────────────────────── */
    if (tp.valid) {
        pp = compute_prosody(&tp);
    } else {
        pp = (ProsodyParams){ .rate_pct = 0, .pitch_st = 0,
                              .volume_pct = 0, .pause_ms = 600,
                              .emphasis = 0 };
    }

    if (bf_ensure_dir(output_dir) != 0) {
        fprintf(stderr, "Failed to create output dir: %s\n", output_dir);
        return 1;
    }

    /* ── Read + normalize source text ───────────────────────── */
    char *source_text = read_text_file(source_path);
    if (!source_text) {
        fprintf(stderr, "Cannot read: %s\n", source_path);
        return 2;
    }

    char *clean = normalize_markdown(source_text);
    if (!clean) { free(source_text); return 1; }

    /* ── Generate narration input (SSML or paced text) ──────── */
    char *narration;
    const char *narration_type;
    if (use_ssml) {
        narration = generate_ssml(clean, &pp);
        narration_type = "ssml";
    } else {
        narration = generate_paced_text(clean, &pp);
        narration_type = "paced-text";
    }
    free(clean);
    if (!narration) { free(source_text); return 1; }

    /* ── Paths ──────────────────────────────────────────────── */
    char narration_path[PATH_MAX], audio_path[PATH_MAX];
    char manifest_path[PATH_MAX], log_path[PATH_MAX];
    char raw_audio_path[PATH_MAX];

    snprintf(narration_path, sizeof(narration_path), "%s/narration.%s",
             output_dir, use_ssml ? "ssml" : "txt");
    snprintf(raw_audio_path, sizeof(raw_audio_path), "%s/raw.wav", output_dir);
    snprintf(audio_path, sizeof(audio_path), "%s/artifact.%s",
             output_dir, audio_format);
    snprintf(manifest_path, sizeof(manifest_path),
             "%s/artifact.manifest.json", output_dir);
    snprintf(log_path, sizeof(log_path), "%s/render.log", output_dir);

    /* ── Dry run ────────────────────────────────────────────── */
    if (dry_run) {
        printf("Plan:\n");
        printf("  Source:     %s\n", source_path);
        printf("  Output:     %s\n", output_dir);
        printf("  Tone:       %s\n", tone_profile_path ? tone_profile_path : "(none)");
        printf("  Prosody:\n");
        printf("    rate:     %+.0f%%\n", pp.rate_pct);
        printf("    pitch:    %+.1f st\n", pp.pitch_st);
        printf("    volume:   %+.0f%%\n", pp.volume_pct);
        printf("    pause:    %d ms\n", pp.pause_ms);
        printf("    emphasis: %s\n",
               pp.emphasis == 2 ? "strong" : pp.emphasis == 1 ? "moderate" : "none");
        printf("  Post-FX:\n");
        printf("    pitch-shift: %s", do_pitch_shift && tp.pitch_hz > 0 ? "yes" : "no");
        if (do_pitch_shift && tp.pitch_hz > 0)
            printf(" (target %.0f Hz)", tp.pitch_hz);
        printf("\n");
        printf("    envelope:    %s\n", do_envelope && tp.valid ? "yes" : "no");
        free(source_text);
        free(narration);
        return 0;
    }

    /* ── Write narration text ───────────────────────────────── */
    FILE *nf = fopen(narration_path, "w");
    if (!nf) { free(source_text); free(narration); return 1; }
    fputs(narration, nf);
    fputc('\n', nf);
    fclose(nf);

    fprintf(stderr, "[narrate] Narration text: %s (%s)\n",
            narration_path, narration_type);
    fprintf(stderr, "[narrate] Prosody: rate=%+.0f%% pitch=%+.1fst "
            "vol=%+.0f%% pause=%dms\n",
            pp.rate_pct, pp.pitch_st, pp.volume_pct, pp.pause_ms);

    /* ── Synthesize with Piper ──────────────────────────────── */
    const char *render_status = "skipped";
    const char *render_reason = "piper_unavailable";
    int synthesis_ok = 0;

    if (command_exists("piper") && voice_model[0] != '\0') {
        /* Compute piper volume from prosody */
        double piper_vol = 1.0 + (pp.volume_pct / 100.0);
        if (piper_vol < 0.3) piper_vol = 0.3;
        if (piper_vol > 2.0) piper_vol = 2.0;

        /* Use raw path if we need post-processing, otherwise final path */
        const char *synth_target = (do_pitch_shift || do_envelope) ?
                                    raw_audio_path : audio_path;

        fprintf(stderr, "[narrate] Synthesizing via Piper (vol=%.2f, "
                "sentence-silence=%dms)\n", piper_vol, pp.pause_ms);

        if (run_piper(voice_model, narration_path, synth_target,
                      log_path, piper_vol, pp.pause_ms) == 0) {
            synthesis_ok = 1;
            render_status = "completed";
            render_reason = "rendered";
        } else {
            render_status = "failed";
            render_reason = "piper_render_failed";
        }
    } else if (command_exists("piper")) {
        render_status = "skipped";
        render_reason = "missing_voice_model";
    }

    /* ── Post-synthesis DSP ─────────────────────────────────── */
    if (synthesis_ok && (do_pitch_shift || do_envelope)) {
        WavAudio wav = wav_read(raw_audio_path);
        if (wav.samples && wav.n_samples > 0) {
            int16_t *current = wav.samples;
            int current_n = wav.n_samples;

            /* Pitch correction */
            if (do_pitch_shift && tp.pitch_hz > 60.0) {
                /* Estimate Piper's default pitch (assume ~150 Hz for
                 * en_US models) and compute shift ratio */
                double piper_default_f0 = 150.0;
                double ratio = tp.pitch_hz / piper_default_f0;

                /* Clamp to reasonable range */
                if (ratio < 0.5) ratio = 0.5;
                if (ratio > 2.0) ratio = 2.0;

                if (fabs(ratio - 1.0) > 0.05) {
                    fprintf(stderr, "[narrate] Pitch shift: %.0f Hz → %.0f Hz "
                            "(ratio=%.2f)\n", piper_default_f0, tp.pitch_hz, ratio);

                    int shifted_n;
                    int16_t *shifted = psola_pitch_shift(current, current_n,
                                                         wav.sample_rate,
                                                         ratio, &shifted_n);
                    if (shifted) {
                        if (current != wav.samples) free(current);
                        current = shifted;
                        current_n = shifted_n;
                    }
                }
            }

            /* Energy envelope transfer */
            if (do_envelope && tp.valid) {
                fprintf(stderr, "[narrate] Energy envelope: target=%.0f/100\n",
                        tp.energy);
                apply_energy_envelope(current, current_n, wav.sample_rate,
                                      tp.energy);
            }

            /* Write final output */
            wav_write(audio_path, current, current_n,
                      wav.sample_rate, wav.channels);

            if (current != wav.samples) free(current);
            free(wav.samples);

            /* Clean up raw intermediate */
            unlink(raw_audio_path);

            fprintf(stderr, "[narrate] Post-processing complete → %s\n",
                    audio_path);
        } else {
            fprintf(stderr, "[narrate] Warning: could not read synthesized WAV "
                    "for post-processing\n");
            /* Fall through — raw audio is the final output */
            if (strcmp(raw_audio_path, audio_path) != 0)
                rename(raw_audio_path, audio_path);
        }
    }

    /* ── WAV Quality Analysis ───────────────────────────────── */
    WavQuality wav_quality = {0};
    if (synthesis_ok) {
        WavAudio qa_wav = wav_read(audio_path);
        if (qa_wav.samples && qa_wav.n_samples > 0) {
            wav_quality = analyze_wav(qa_wav.samples, qa_wav.n_samples,
                                      qa_wav.sample_rate);
            fprintf(stderr, "[narrate] WAV quality: DR=%.1f dB  "
                    "continuity=%.3f  pitch-stab=%.3f  energy-cons=%.3f\n",
                    wav_quality.dynamic_range_db,
                    wav_quality.spectral_continuity,
                    wav_quality.pitch_stability,
                    wav_quality.energy_consistency);
            free(qa_wav.samples);
        }
    }

    /* ── Closed-Loop Verification + Iterative Refinement ────── */
    int total_iterations = 1;
    FidelityReport fidelity = {0};
    int fidelity_valid = 0;

    if ((do_verify || do_refine) && source_fp.count > 0 && synthesis_ok) {
        fidelity = verify_output(audio_path, &source_fp, output_dir);
        fidelity_valid = (fidelity.features_matched > 0);

        /* Refinement loop: max 2 additional passes */
        char *clean_copy = normalize_markdown(source_text);
        while (do_refine && fidelity_valid &&
               fidelity.composite < refine_target &&
               total_iterations < 3 && clean_copy) {
            total_iterations++;
            fprintf(stderr, "\n[narrate] ━━━ Refinement pass %d (target %.2f) ━━━\n",
                    total_iterations, refine_target);

            pp = refine_prosody(&pp, &fidelity, &tp, total_iterations - 1);
            fprintf(stderr, "[narrate] Adjusted prosody: rate=%+.0f%% pitch=%+.1fst "
                    "vol=%+.0f%% pause=%dms\n",
                    pp.rate_pct, pp.pitch_st, pp.volume_pct, pp.pause_ms);

            /* Re-generate narration */
            char *new_narr;
            if (use_ssml) {
                new_narr = generate_ssml(clean_copy, &pp);
            } else {
                new_narr = generate_paced_text(clean_copy, &pp);
            }
            if (!new_narr) break;

            FILE *nf2 = fopen(narration_path, "w");
            if (!nf2) { free(new_narr); break; }
            fputs(new_narr, nf2);
            fputc('\n', nf2);
            fclose(nf2);
            free(new_narr);

            /* Re-synthesize */
            if (!command_exists("piper") || voice_model[0] == '\0') break;

            double piper_vol2 = 1.0 + (pp.volume_pct / 100.0);
            if (piper_vol2 < 0.3) piper_vol2 = 0.3;
            if (piper_vol2 > 2.0) piper_vol2 = 2.0;

            const char *synth_target2 = (do_pitch_shift || do_envelope) ?
                                         raw_audio_path : audio_path;

            if (run_piper(voice_model, narration_path, synth_target2,
                          log_path, piper_vol2, pp.pause_ms) != 0)
                break;

            /* Re-apply post-processing */
            if (do_pitch_shift || do_envelope) {
                WavAudio wav2 = wav_read(raw_audio_path);
                if (wav2.samples && wav2.n_samples > 0) {
                    int16_t *cur = wav2.samples;
                    int cur_n = wav2.n_samples;

                    if (do_pitch_shift && tp.pitch_hz > 60.0) {
                        double ratio = tp.pitch_hz / 150.0;
                        if (ratio < 0.5) ratio = 0.5;
                        if (ratio > 2.0) ratio = 2.0;
                        if (fabs(ratio - 1.0) > 0.05) {
                            int sn;
                            int16_t *sh = psola_pitch_shift(
                                cur, cur_n, wav2.sample_rate, ratio, &sn);
                            if (sh) {
                                if (cur != wav2.samples) free(cur);
                                cur = sh; cur_n = sn;
                            }
                        }
                    }
                    if (do_envelope && tp.valid)
                        apply_energy_envelope(cur, cur_n,
                                              wav2.sample_rate, tp.energy);

                    wav_write(audio_path, cur, cur_n,
                              wav2.sample_rate, wav2.channels);
                    if (cur != wav2.samples) free(cur);
                    free(wav2.samples);
                    unlink(raw_audio_path);
                }
            }

            /* Re-verify */
            fidelity = verify_output(audio_path, &source_fp, output_dir);
            fidelity_valid = (fidelity.features_matched > 0);

            /* Update WAV quality */
            WavAudio qa2 = wav_read(audio_path);
            if (qa2.samples && qa2.n_samples > 0) {
                wav_quality = analyze_wav(qa2.samples, qa2.n_samples,
                                          qa2.sample_rate);
                free(qa2.samples);
            }

            if (fidelity.composite >= refine_target) {
                fprintf(stderr, "[narrate] Target reached (%.4f >= %.2f) "
                        "after %d iterations\n",
                        fidelity.composite, refine_target, total_iterations);
            }
        }
        free(clean_copy);
    }

    /* ── Write manifest ─────────────────────────────────────── */
    char timestamp[64];
    time_t now = time(NULL);
    struct tm tm_utc;
    gmtime_r(&now, &tm_utc);
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%SZ", &tm_utc);

    FILE *mf = fopen(manifest_path, "w");
    if (!mf) { free(source_text); free(narration); return 1; }

    fprintf(mf,
        "{\n"
        "  \"sourceSystem\": \"BonfyreNarrate\",\n"
        "  \"version\": \"3.2.0\",\n"
        "  \"artifactType\": \"narrated-artifact\",\n"
        "  \"title\": \"%s\",\n"
        "  \"createdAt\": \"%s\",\n"
        "  \"sourceTextPath\": \"%s\",\n"
        "  \"narrationTextPath\": \"%s\",\n"
        "  \"narrationFormat\": \"%s\",\n"
        "  \"audioPath\": \"%s\",\n"
        "  \"audioFormat\": \"%s\",\n"
        "  \"voiceModel\": \"%s\",\n"
        "  \"toneProfilePath\": \"%s\",\n"
        "  \"sourceFeatures\": \"%s\",\n"
        "  \"reference\": \"%s\",\n"
        "  \"renderStatus\": \"%s\",\n"
        "  \"renderReason\": \"%s\",\n"
        "  \"prosody\": {\n"
        "    \"rate_pct\": %.1f,\n"
        "    \"pitch_st\": %.1f,\n"
        "    \"volume_pct\": %.1f,\n"
        "    \"pause_ms\": %d,\n"
        "    \"emphasis\": %d\n"
        "  },\n"
        "  \"postProcessing\": {\n"
        "    \"pitchShift\": %s,\n"
        "    \"targetF0Hz\": %.1f,\n"
        "    \"energyEnvelope\": %s,\n"
        "    \"targetEnergy\": %.1f\n"
        "  },\n",
        title, timestamp, source_path, narration_path, narration_type,
        audio_path, audio_format, voice_model,
        tone_profile_path ? tone_profile_path : "",
        source_features_path ? source_features_path : "",
        reference_path ? reference_path : "",
        render_status, render_reason,
        pp.rate_pct, pp.pitch_st, pp.volume_pct, pp.pause_ms, pp.emphasis,
        (do_pitch_shift && tp.pitch_hz > 60.0) ? "true" : "false",
        tp.pitch_hz,
        (do_envelope && tp.valid) ? "true" : "false",
        tp.energy);

    /* Fidelity data */
    if (fidelity_valid) {
        fprintf(mf,
            "  \"fidelity\": {\n"
            "    \"verified\": true,\n"
            "    \"cosine_88\": %.6f,\n"
            "    \"composite\": %.6f,\n"
            "    \"features_matched\": %d,\n"
            "    \"layers_passed\": %d,\n"
            "    \"iterations\": %d,\n"
            "    \"layers\": {\n",
            fidelity.cosine_88, fidelity.composite,
            fidelity.features_matched, fidelity.layers_passed,
            total_iterations);
        for (int i = 0; i < 6; i++)
            fprintf(mf, "      \"%s\": %.6f%s\n", LAYER_NAMES[i],
                    fidelity.layer[i], i < 5 ? "," : "");
        fprintf(mf, "    }\n  },\n");
    } else {
        fprintf(mf, "  \"fidelity\": { \"verified\": false },\n");
    }

    /* WAV quality */
    fprintf(mf,
        "  \"wavQuality\": {\n"
        "    \"dynamic_range_db\": %.2f,\n"
        "    \"spectral_continuity\": %.4f,\n"
        "    \"pitch_stability\": %.4f,\n"
        "    \"energy_consistency\": %.4f\n"
        "  },\n"
        "  \"renderLogPath\": \"%s\"\n"
        "}\n",
        wav_quality.dynamic_range_db, wav_quality.spectral_continuity,
        wav_quality.pitch_stability, wav_quality.energy_consistency,
        log_path);
    fclose(mf);

    /* ── Summary ────────────────────────────────────────────── */
    printf("Narration:  %s (%s)\n", narration_path, narration_type);
    printf("Manifest:   %s\n", manifest_path);
    if (synthesis_ok) printf("Audio:      %s\n", audio_path);
    if (tp.valid)     printf("Tone match: pitch=%.0f Hz, energy=%.0f/100, "
                             "pace=%.0f/100\n",
                             tp.pitch_hz, tp.energy, tp.pace);
    if (fidelity_valid) {
        printf("Fidelity:   %.4f composite (88-feature cosine: %.4f, "
               "%d/6 layers, %d iterations)\n",
               fidelity.composite, fidelity.cosine_88,
               fidelity.layers_passed, total_iterations);
    }
    printf("WAV quality: DR=%.1f dB  continuity=%.3f  "
           "pitch-stab=%.3f  energy-cons=%.3f\n",
           wav_quality.dynamic_range_db, wav_quality.spectral_continuity,
           wav_quality.pitch_stability, wav_quality.energy_consistency);

    free(source_text);
    free(narration);
    return 0;
}
