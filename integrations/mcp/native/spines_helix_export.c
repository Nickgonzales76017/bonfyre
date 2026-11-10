#define main spines_helix_cli_main
#include "spines_helix_vm.c"
#undef main

int main(void) { return 0; }

__attribute__((visibility("default")))
int bf_reduce(const char *term, size_t length, int fuel, double *result, int *steps, int *remaining) {
    if (!term || length > 65536 || fuel <= 0 || fuel > 1000000 || !result || !steps || !remaining) return 2;
    Parser parser = {term, 0, length, fuel, 0, 0};
    double value = expr(&parser);
    skip(&parser);
    if (parser.i != parser.n) parser.error = 1;
    *result = value;
    *steps = parser.steps;
    *remaining = parser.fuel;
    return parser.error ? 3 : 0;
}
