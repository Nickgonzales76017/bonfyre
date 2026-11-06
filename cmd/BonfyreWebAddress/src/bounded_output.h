#ifndef BONFYRE_WEB_BOUNDED_OUTPUT_H
#define BONFYRE_WEB_BOUNDED_OUTPUT_H

#include <stddef.h>

/* Append one complete fragment or leave the buffer and offset unchanged.
 * reserve keeps room for a caller-owned closing delimiter and the NUL. */
int bf_bounded_appendf(char *buffer, size_t capacity, size_t *used,
                       size_t reserve, const char *format, ...);

typedef struct {
    char *buffer;
    size_t capacity;
    size_t used;
    int failed;
} BfBoundedOutput;

void bf_output_init(BfBoundedOutput *output, char *buffer, size_t capacity);
void bf_output_text(BfBoundedOutput *output, const char *text);

#endif
