/*
 * bonfyre — unified CLI dispatcher.
 *
 * Routes subcommands to their respective binaries:
 *   bonfyre ingest ...     → bonfyre-ingest ...
 *   bonfyre hash ...       → bonfyre-hash ...
 *   bonfyre brief ...      → bonfyre-brief ...
 *   bonfyre proof ...      → bonfyre-proof ...
 *   bonfyre offer ...      → bonfyre-offer ...
 *   bonfyre narrate ...    → bonfyre-narrate ...
 *   bonfyre pack ...       → bonfyre-pack ...
 *   bonfyre distribute ... → bonfyre-distribute ...
 *   bonfyre emit ...       → bonfyre-emit ...
 *   bonfyre compress ...   → bonfyre-compress ...
 *   bonfyre gate ...       → bonfyre-gate ...
 *   bonfyre meter ...      → bonfyre-meter ...
 *   bonfyre index ...      → bonfyre-index ...
 *   bonfyre stitch ...     → bonfyre-stitch ...
 *   bonfyre ledger ...     → bonfyre-ledger ...
 *   bonfyre queue ...      → bonfyre-queue ...
 *   bonfyre sync ...       → bonfyre-sync ...
 *   bonfyre transcribe ... → bonfyre-transcribe ...
 *   bonfyre mediaprep ...  → bonfyre-mediaprep ...
 *   bonfyre transcript-family ... → bonfyre-transcript-family ...
 *   bonfyre render ...     → bonfyre-render ...
 *   bonfyre runtime ...    → bonfyre-runtime ...
 *   bonfyre project ...    → bonfyre-project ...
 *
 * Searches for binaries in: same dir as this binary → PATH
 */
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct {
    const char *cmd;
    const char *binary;
    const char *sibling_dir;
    const char *desc;
} Route;

static const Route routes[] = {
    /* ── Pipeline core ── */
    {"ingest",     "bonfyre-ingest",     "BonfyreIngest", "Universal asset intake"},
    {"mediaprep",  "bonfyre-media-prep", "BonfyreMediaPrep", "Media normalization"},
    {"transcribe", "bonfyre-transcribe", "BonfyreTranscribe", "Audio → text"},
    {"clean",      "bonfyre-transcript-clean", "BonfyreTranscriptClean", "Transcript cleaning"},
    {"paragraph",  "bonfyre-paragraph",  "BonfyreParagraph", "Transcript paragraphizer"},
    {"brief",      "bonfyre-brief",      "BonfyreBrief", "Extract structured brief"},
    {"proof",      "bonfyre-proof",      "BonfyreProof", "Generate proof bundle"},
    {"offer",      "bonfyre-offer",      "BonfyreOffer", "Generate offer document"},
    {"narrate",    "bonfyre-narrate",    "BonfyreNarrate", "Text-to-speech narration"},
    {"pack",       "bonfyre-pack",       "BonfyrePack", "Package artifact family"},
    {"distribute", "bonfyre-distribute", "BonfyreDistribute", "Multi-channel distribution"},
    {"transcript-family", "bonfyre-transcript-family", "BonfyreTranscriptFamily", "Speech to cleaned transcript family"},
    {"render",     "bonfyre-render",     "BonfyreRender", "Universal artifact renderer"},
    /* ── Infrastructure ── */
    {"hash",       "bonfyre-hash",       "BonfyreHash", "Content-addressing (SHA-256)"},
    {"index",      "bonfyre-index",      "BonfyreIndex", "Artifact family indexer"},
    {"compress",   "bonfyre-compress",   "BonfyreCompress", "Family-aware compression"},
    {"emit",       "bonfyre-emit",       "BonfyreEmit", "Multi-format output engine"},
    {"stitch",     "bonfyre-stitch",     "BonfyreStitch", "DAG materializer"},
    {"queue",      "bonfyre-queue",      "BonfyreQueue", "Job queue management"},
    {"runtime",    "bonfyre-runtime",    "BonfyreRuntime", "Replayable pipeline runtime"},
    {"project",    "bonfyre-project",    "BonfyreProject", "Content graph projection engine"},
    {"sync",       "bonfyre-sync",       "BonfyreSync", "Artifact synchronization"},
    /* ── Value capture ── */
    {"gate",       "bonfyre-gate",       "BonfyreGate", "License enforcement"},
    {"meter",      "bonfyre-meter",      "BonfyreMeter", "Usage metering & billing"},
    {"ledger",     "bonfyre-ledger",     "BonfyreLedger", "Value accounting"},
    {NULL, NULL, NULL, NULL}
};

static void get_self_dir(char *buf, size_t sz) {
    /* Try to resolve self path to find sibling binaries */
    char self[PATH_MAX];
    memset(self, 0, sizeof(self));
#ifdef __APPLE__
    uint32_t bsz = sizeof(self);
    if (_NSGetExecutablePath(self, &bsz) != 0) self[0] = '\0';
#elif defined(__linux__)
    ssize_t n = readlink("/proc/self/exe", self, sizeof(self) - 1);
    if (n > 0) self[n] = '\0'; else self[0] = '\0';
#else
    self[0] = '\0';
#endif
    if (self[0]) {
        char *last = strrchr(self, '/');
        if (last) { *last = '\0'; snprintf(buf, sz, "%s", self); return; }
    }
    buf[0] = '\0';
}

static int try_exec(const char *binary, const char *sibling_dir, char **argv) {
    /* 1. Try in same directory as us */
    char self_dir[PATH_MAX];
    get_self_dir(self_dir, sizeof(self_dir));
    if (self_dir[0]) {
        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/%s", self_dir, binary);
        if (access(full, X_OK) == 0) {
            argv[0] = full;
            execv(full, argv);
            /* execv only returns on error */
        }
        if (sibling_dir && sibling_dir[0]) {
            snprintf(full, sizeof(full), "%s/../%s/%s", self_dir, sibling_dir, binary);
            if (access(full, X_OK) == 0) {
                argv[0] = full;
                execv(full, argv);
            }
        }
    }
    /* 2. Try repo-relative from current working directory */
    if (sibling_dir && sibling_dir[0]) {
        char full[PATH_MAX];
        snprintf(full, sizeof(full), "10-Code/%s/%s", sibling_dir, binary);
        if (access(full, X_OK) == 0) {
            argv[0] = full;
            execv(full, argv);
        }
        snprintf(full, sizeof(full), "./10-Code/%s/%s", sibling_dir, binary);
        if (access(full, X_OK) == 0) {
            argv[0] = full;
            execv(full, argv);
        }
    }
    /* 3. Fall back to PATH */
    execvp(binary, argv);
    return -1;
}

int main(int argc, char *argv[]) {
    if (argc < 2) goto usage;
    const char *cmd = argv[1];

    /* Special built-in commands */
    if (strcmp(cmd, "help") == 0 || strcmp(cmd, "--help") == 0 || strcmp(cmd, "-h") == 0)
        goto usage;

    if (strcmp(cmd, "version") == 0 || strcmp(cmd, "--version") == 0) {
        printf("bonfyre 0.1.0\n");
        return 0;
    }

    if (strcmp(cmd, "list") == 0) {
        printf("Available commands:\n\n");
        const char *section = "";
        for (const Route *r = routes; r->cmd; r++) {
            /* Detect section changes */
            const char *new_section = "";
            if (r == &routes[0]) new_section = "Pipeline";
            else if (strcmp(r->binary, "bonfyre-hash") == 0) new_section = "Infrastructure";
            else if (strcmp(r->binary, "bonfyre-gate") == 0) new_section = "Value Capture";
            if (strcmp(new_section, section) != 0) {
                if (new_section[0]) printf("  %s:\n", new_section);
                section = new_section;
            }
            printf("    %-14s %s\n", r->cmd, r->desc);
        }
        printf("\nRun 'bonfyre <command> --help' for command-specific help.\n");
        return 0;
    }

    /* Look up the route */
    for (const Route *r = routes; r->cmd; r++) {
        if (strcmp(cmd, r->cmd) == 0) {
            /* Build argv for the target binary: [binary, arg2, arg3, ..., NULL] */
            char **new_argv = malloc(sizeof(char *) * (size_t)(argc + 1));
            if (!new_argv) { perror("malloc"); return 1; }
            new_argv[0] = (char *)r->binary;
            for (int i = 2; i < argc; i++)
                new_argv[i - 1] = argv[i];
            new_argv[argc - 1] = NULL;

            try_exec(r->binary, r->sibling_dir, new_argv);
            fprintf(stderr, "bonfyre: cannot execute '%s': not found\n", r->binary);
            fprintf(stderr, "  (install %s or add its directory to PATH)\n", r->binary);
            free(new_argv);
            return 127;
        }
    }

    fprintf(stderr, "bonfyre: unknown command '%s'\n", cmd);
    fprintf(stderr, "Run 'bonfyre list' to see all commands.\n");
    return 1;

usage:
    fprintf(stderr,
        "bonfyre — artifact pipeline toolkit\n\n"
        "Usage: bonfyre <command> [args...]\n\n"
        "Commands:\n"
        "  list          Show all available commands\n"
        "  version       Print version\n"
        "  help          Show this help\n\n"
        "Pipeline:       ingest → mediaprep → transcribe → clean → paragraph → brief → proof → offer → narrate → pack → distribute\n"
        "Fusion:         transcript-family, render, runtime, project\n"
        "Infrastructure: hash, index, compress, emit, stitch, queue, sync\n"
        "Value Capture:  gate, meter, ledger\n\n"
        "Run 'bonfyre list' for full details or 'bonfyre <command> --help' for command help.\n"
    );
    return 0;
}
