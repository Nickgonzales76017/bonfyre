#ifndef BONFYRE_MARKDOWN_FENCE_H
#define BONFYRE_MARKDOWN_FENCE_H

#include <stddef.h>

/* The supported backtick fence must close with at least its opening width.
 * A line carrying other text is content, even when it starts with backticks. */
static inline size_t bf_markdown_fence_width(const char *line) {
    size_t width = 0;
    while (line[width] == '`') width++;
    return width >= 3 ? width : 0;
}

static inline int bf_markdown_fence_closes(const char *line, size_t opening) {
    size_t width = bf_markdown_fence_width(line);
    if (!opening || width < opening) return 0;
    line += width;
    while (*line == ' ' || *line == '\t' || *line == '\r') line++;
    return !*line || *line == '\n';
}

#endif
