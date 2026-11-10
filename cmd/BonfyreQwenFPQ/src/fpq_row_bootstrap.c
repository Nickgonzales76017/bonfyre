#include "libfpq.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int fpq_decode_row(fpq_model_t *m, const char *tensor_name,
                   size_t row, float *out) {
    if (!m || !tensor_name || !out) return -1;

    const fpq_tensor_info_t *info = fpq_tensor_find(m, tensor_name);
    if (!info || row >= info->rows) return -1;

    /* This file once answered every embedding lookup with a hash-noise row.
     * It is not built; the row decoder lives in fpq_compat_shim.c. What is
     * left here decodes a single-row tensor or fails. */
    if (info->rows == 1 && row == 0) {
        return fpq_decode_one(m, tensor_name, out);
    }

    fprintf(stderr,
            "fpq_decode_row: no safe row decoder for tensor=%s row=%zu rows=%zu cols=%zu\n",
            tensor_name, row, (size_t)info->rows, (size_t)info->cols);
    return -1;
}
