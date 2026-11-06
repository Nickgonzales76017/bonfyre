/* Identity keys for lines of text: each input line, a tab, its key. */
#define _POSIX_C_SOURCE 200809L
#include "bf_text.h"
#include <stdio.h>
#include <string.h>
int main(void) {
    char *line = NULL; size_t cap = 0; ssize_t got; char key[8192];
    while ((got = getline(&line, &cap, stdin)) >= 0) {
        if (got && line[got - 1] == '\n') line[--got] = '\0';
        if (bf_text_identity_key(line, key, sizeof(key)) < 0) { printf("%s\t\n", line); continue; }
        printf("%s\t%s\n", line, key);
    }
    return 0;
}
