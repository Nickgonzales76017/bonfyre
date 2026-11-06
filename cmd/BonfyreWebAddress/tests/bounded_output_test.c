#include "bounded_output.h"

#include <assert.h>
#include <string.h>

int main(void) {
    struct { char bytes[16]; char canary; } guarded = {{0}, 'Z'};
    size_t used = 0;
    assert(bf_bounded_appendf(guarded.bytes, sizeof(guarded.bytes), &used, 5,
                              "%s", "prefix"));
    assert(used == 6 && !strcmp(guarded.bytes, "prefix"));
    assert(!bf_bounded_appendf(guarded.bytes, sizeof(guarded.bytes), &used, 5,
                               "%s", "too-long"));
    assert(used == 6 && !strcmp(guarded.bytes, "prefix"));
    assert(bf_bounded_appendf(guarded.bytes, sizeof(guarded.bytes), &used, 0,
                              "%s", "</x>"));
    assert(!strcmp(guarded.bytes, "prefix</x>"));
    assert(guarded.canary == 'Z');
    assert(!bf_bounded_appendf(guarded.bytes, sizeof(guarded.bytes), &used,
                               sizeof(guarded.bytes), "%s", "x"));
    assert(guarded.canary == 'Z');
    assert(!bf_bounded_appendf(guarded.bytes, sizeof(guarded.bytes), &used, 0,
                               "%s", "123456789"));
    assert(used == 10 && !strcmp(guarded.bytes, "prefix</x>"));
    assert(!bf_bounded_appendf(guarded.bytes, 0, &used, 0, "%s", "x"));
    assert(guarded.canary == 'Z');
    BfBoundedOutput output;
    bf_output_init(&output, guarded.bytes, sizeof(guarded.bytes));
    bf_output_text(&output, "SELECT ");
    bf_output_text(&output, "one");
    assert(!output.failed && output.used == 10);
    bf_output_text(&output, " FROM a-very-long-table-name");
    assert(output.failed && output.used == 10);
    assert(!strcmp(guarded.bytes, "SELECT one"));
    bf_output_text(&output, " later");
    assert(output.failed && output.used == 10);
    assert(guarded.canary == 'Z');
    char exact[5];
    bf_output_init(&output, exact, sizeof(exact));
    bf_output_text(&output, "abcd");
    assert(!output.failed && output.used == 4 && !strcmp(exact, "abcd"));
    bf_output_text(&output, "e");
    assert(output.failed && output.used == 4 && !strcmp(exact, "abcd"));
    bf_output_init(&output, exact, 0);
    assert(output.failed);
    return 0;
}
