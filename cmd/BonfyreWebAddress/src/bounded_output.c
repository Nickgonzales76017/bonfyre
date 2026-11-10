#include "bounded_output.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

static int appendv(char *buffer, size_t capacity, size_t *used,
                   size_t reserve, const char *format, va_list args) {
    int requested;
    size_t available;

    if (!buffer || !used || !format || *used >= capacity || reserve >= capacity - *used)
        return 0;
    available = capacity - *used - reserve;
    requested = vsnprintf(buffer + *used, available, format, args);
    if (requested < 0 || (size_t)requested >= available) {
        buffer[*used] = '\0';
        return 0;
    }
    *used += (size_t)requested;
    return 1;
}

int bf_bounded_appendf(char *buffer, size_t capacity, size_t *used,
                       size_t reserve, const char *format, ...) {
    va_list args;
    int complete;
    va_start(args, format);
    complete = appendv(buffer, capacity, used, reserve, format, args);
    va_end(args);
    return complete;
}

void bf_output_init(BfBoundedOutput *output, char *buffer, size_t capacity) {
    if (!output) return;
    *output = (BfBoundedOutput){buffer, capacity, 0, !buffer || !capacity};
    if (!output->failed) buffer[0] = '\0';
}

void bf_output_text(BfBoundedOutput *output, const char *text) {
    size_t length;
    if (!output || output->failed) return;
    if (!text || !output->buffer) { output->failed = 1; return; }
    length = strlen(text);
    if (output->used >= output->capacity ||
        length >= output->capacity - output->used) {
        output->failed = 1;
        return;
    }
    memcpy(output->buffer + output->used, text, length);
    output->used += length;
    output->buffer[output->used] = '\0';
}
