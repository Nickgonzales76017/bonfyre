/*
 * bf_voice_fingerprint.h — acoustic features of a voice, computed from PCM
 * samples in C with nothing else installed: pitch by autocorrelation, RMS
 * energy, jitter, shimmer, harmonics-to-noise, spectral and rhythm measures.
 *
 * bonfyre-narrate checks its own speech against a source with these, and
 * bonfyre-tone reports them when no OpenSMILE is configured. The code lived
 * inside the narrate organ; it is here so both read the same numbers.
 *
 * This is not the eGeMAPS feature set. It names the features it computes the
 * way eGeMAPS names its nearest equivalents, so a vector from here lines up
 * with one from OpenSMILE, but there are fewer of them and each is this
 * code's own estimate. A consumer that needs eGeMAPS itself needs OpenSMILE.
 *
 * Header-only: an organ includes it and needs nothing rebuilt.
 */
#ifndef BF_VOICE_FINGERPRINT_H
#define BF_VOICE_FINGERPRINT_H

#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BF_VOICE_FINGERPRINT_SET "bonfyre-voice-fingerprint"

/* ── WAV I/O ─────────────────────────────────────────────────── */

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

/* ── pitch ───────────────────────────────────────────────────── */

/* Autocorrelation-based pitch detection for a frame of audio */
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
    return (double)sr / best_lag;
}

/* ── fingerprint ─────────────────────────────────────────────── */


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
    double f0_sum = 0.0, f0_min = 1e9, f0_max = 0.0;
    int f0_n = 0;
    for (int f = 0; f < n_frames; f++) {
        if (f0s[f] > 60.0) {
            f0_sum += f0s[f]; f0_n++;
            if (f0s[f] < f0_min) f0_min = f0s[f];
            if (f0s[f] > f0_max) f0_max = f0s[f];
        }
    }
    double f0_mean = f0_n > 0 ? f0_sum / f0_n : 150.0;
    double f0_var = 0.0;
    for (int f = 0; f < n_frames; f++) {
        if (f0s[f] > 60.0) {
            double d = f0s[f] - f0_mean;
            f0_var += d * d;
        }
    }
    f0_var = f0_n > 1 ? f0_var / (f0_n - 1) : 0.0;
    double f0_std = sqrt(f0_var);

    /* Convert Hz to semitones re 27.5 Hz (eGeMAPSv02 convention) */
    double f0_st_mean = (f0_mean > 0) ? 12.0 * log2(f0_mean / 27.5) : 0.0;
    double f0_st_std  = (f0_mean > 0) ? 12.0 * (f0_std / f0_mean) : 0.0;
    double f0_st_p5   = (f0_min < 1e8) ? 12.0 * log2(f0_min / 27.5) : 0.0;
    double f0_st_p95  = (f0_max > 0) ? 12.0 * log2(f0_max / 27.5) : 0.0;

    fp_set(&fp, "F0semitoneFrom27.5Hz_sma3nz_amean", f0_st_mean);
    fp_set(&fp, "F0semitoneFrom27.5Hz_sma3nz_stddevNorm",
           f0_st_mean > 0 ? f0_st_std / fabs(f0_st_mean) : 0.0);
    fp_set(&fp, "F0semitoneFrom27.5Hz_sma3nz_pctlrange0-2",
           f0_st_p95 - f0_st_p5);
    fp_set(&fp, "F0final_sma3nz_amean", f0_st_mean);
    fp_set(&fp, "F0final_sma3nz_stddevNorm",
           f0_st_mean > 0 ? f0_st_std / fabs(f0_st_mean) : 0.0);

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
           f0_mean > 0 ? log(f0_std / f0_mean + 1.0) : 0.0);

    /* ── Temporal features (L3) ───────────────────────────── */
    /* Voiced segment lengths */
    double total_dur = (double)n / sr;
    int seg_count = 0;
    double seg_len_sum = 0.0;
    int in_voiced = 0, seg_start = 0;
    int unseg_count = 0;
    double unseg_len_sum = 0.0;
    int unseg_start = 0;
    int peaks = 0;

    for (int f = 0; f < n_frames; f++) {
        int voiced = f0s[f] > 60.0;
        if (voiced && !in_voiced) {
            seg_start = f;
            in_voiced = 1;
            if (f > 0 && !in_voiced) {
                unseg_len_sum += (double)(f - unseg_start) * frame_sz / sr;
                unseg_count++;
            }
        } else if (!voiced && in_voiced) {
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
    if (in_voiced && seg_count == 0) {
        seg_len_sum = total_dur;
        seg_count = 1;
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
    int spec_n = 0;
    float *prev_mag = calloc((size_t)(fft_n / 2 + 1), sizeof(float));

    if (fft_re && fft_im && prev_mag) {
        for (int fi = 0; fi < n_frames && fi < 200; fi++) {
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

            /* Spectral slopes */
            int bin_500  = 500  * fft_n / sr;
            int bin_1500 = 1500 * fft_n / sr;
            if (bin_500 > 0 && bin_500 < half) {
                double s = 0, sx = 0, sy = 0, sxx = 0, sxy = 0;
                for (int b = 0; b < bin_500; b++) {
                    s++; sx += b; sy += mag[b];
                    sxx += (double)b * b; sxy += b * mag[b];
                }
                if (s > 1) slope_lo_sum += (s * sxy - sx * sy) /
                                            (s * sxx - sx * sx);
            }
            if (bin_1500 > bin_500 && bin_1500 <= half) {
                double s = 0, sx = 0, sy = 0, sxx = 0, sxy = 0;
                for (int b = bin_500; b < bin_1500; b++) {
                    s++; sx += b; sy += mag[b];
                    sxx += (double)b * b; sxy += b * mag[b];
                }
                if (s > 1) slope_hi_sum += (s * sxy - sx * sy) /
                                            (s * sxx - sx * sx);
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

#endif /* BF_VOICE_FINGERPRINT_H */
