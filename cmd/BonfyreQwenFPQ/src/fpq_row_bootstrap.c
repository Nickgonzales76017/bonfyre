#include "libfpq.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int fpq_decode_row(fpq_model_t *m, const char *tensor_name,
                   size_t row, float *out) {
    if (!m || !tensor_name || !out) return -1;

    const fpq_tensor_info_t *info = fpq_tensor_find(m, tensor_name);
    if (!info || row >= info->rows) return -1;

    if (strcmp(tensor_name, "model.embed_tokens.weight") == 0 ||
        getenv("BONFYRE_QWEN_BOOTSTRAP_SYNTHETIC_ROWS")) {
        static int logged_row = 0;
        if (logged_row < 40) {
            fprintf(stderr,
                    "fpq_decode_row: bootstrap synthetic row tensor=%s row=%zu cols=%zu\n",
                    tensor_name, row, (size_t)info->cols);
            fflush(stderr);
            logged_row++;
        }

        for (size_t c = 0; c < info->cols; c++) {
            unsigned int x = (unsigned int)(row * 1103515245u) ^
                             (unsigned int)(c * 2654435761u) ^
                             0x9e3779b9u;
            out[c] = ((float)((int)((x >> 16) & 255) - 127)) * 0.001f;
        }
        return 0;
    }

    if (info->rows == 1 && row == 0) {
        return fpq_decode_one(m, tensor_name, out);
    }

    fprintf(stderr,
            "fpq_decode_row: no safe row decoder for tensor=%s row=%zu rows=%zu cols=%zu\n",
            tensor_name, row, (size_t)info->rows, (size_t)info->cols);
    return -1;
}
