#ifndef FPQ_ORC_JIT_H
#define FPQ_ORC_JIT_H
#include <stddef.h>

typedef void (*fpq_sli_kernel_fn)(const float *z_fwht, const float *x, float *output,
                                  unsigned long long haar_seed, unsigned long cols);

/* The SLI kernel specialised for one shape, compiled in this process. The
 * same shape asked for again returns the same code without compiling.
 * Returns NULL when the JIT cannot be brought up; the caller keeps its
 * ordinary path. `compile_ms`, when given, receives what this call cost
 * (zero for a shape already compiled). */
fpq_sli_kernel_fn fpq_orc_sli_kernel(size_t rows, size_t blocks_per_row, double *compile_ms);

/* How many shapes have been compiled, and how many requests were answered
 * from those already compiled. */
void fpq_orc_stats(size_t *compiled, size_t *reused);
#endif
