/* bf_help.h — the help contract every public organ keeps.
 *
 * `--help`, `-h` or `help` as the first argument prints the organ's usage on
 * stdout and exits 0, before any store, model, device or session is opened.
 * A person must be able to ask how a command works without it doing anything.
 *
 * Organs write their usage to stderr for errors; for an explicit help request
 * that same text goes to stdout, so it can be read, paged and searched:
 *
 *     int main(int argc, char **argv) {
 *         if (bf_help_requested(argc, argv)) { usage(); return 0; }
 *         ...
 *
 * Header-only, so an organ can keep it without linking libbonfyre.
 */
#ifndef BF_HELP_H
#define BF_HELP_H

#include <stdio.h>
#include <string.h>
#include <unistd.h>

static inline int bf_help_requested(int argc, char **argv) {
    if (argc < 2 || !argv[1]) return 0;
    if (strcmp(argv[1], "--help") != 0 && strcmp(argv[1], "-h") != 0 && strcmp(argv[1], "help") != 0) return 0;
    fflush(stdout);
    fflush(stderr);
    dup2(STDOUT_FILENO, STDERR_FILENO);
    return 1;
}

#endif
