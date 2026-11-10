/* The SLI row-score kernel, as the source its specialisations are made from.
 *
 * It is compiled once, at build time, to LLVM IR with two sentinel constants
 * standing where a tensor's row count and blocks-per-row go. At run time the
 * sentinels are replaced with a real shape and the IR is optimised and
 * compiled in memory, so the loops are unrolled for that shape without
 * writing C to /tmp, starting a compiler process, or loading a shared object
 * from a path anybody could have written to.
 *
 * The arithmetic is exactly the kernel the temporary-C path emitted: no
 * fast-math, so a specialised kernel returns the same bits as this source
 * compiled ordinarily. */
#include <string.h>
#define N_ROWS 1000000007UL   /* sentinel: replaced with the tensor's rows */
#define BPR    1000000009UL   /* sentinel: replaced with blocks per row */
#define BDIM   256

void sli_jit_kernel(const float *z_fwht, const float *x, float *output,
                    unsigned long long haar_seed, unsigned long cols)
{
    unsigned long bpr_ = BPR;
    for (unsigned long i = 0; i < N_ROWS; i++) {
        float row_score = 0.0f;
        for (unsigned long bj = 0; bj < bpr_; bj++) {
            unsigned long b = i * bpr_ + bj;
            unsigned long col_off = bj * BDIM;
            unsigned long this_dim = (col_off + BDIM <= cols) ? BDIM : (cols - col_off);
            float xs[BDIM];
            memset(xs, 0, BDIM * sizeof(float));
            memcpy(xs, x + col_off, this_dim * sizeof(float));
            unsigned long long state = (haar_seed ^ b) ? (haar_seed ^ b) : 0x5DEECE66DULL;
            for (unsigned long si = 0; si < BDIM; si += 64) {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                for (unsigned long j = si; j < si + 64 && j < BDIM; j++)
                    if ((state >> (j - si)) & 1ULL) xs[j] = -xs[j];
            }
            const float *z_b = z_fwht + b * BDIM;
            float dot = 0.0f;
            for (unsigned long k = 0; k < BDIM; k++)
                dot += z_b[k] * xs[k];
            row_score += dot;
        }
        output[i] += row_score;
    }
}
