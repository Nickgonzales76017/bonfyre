/*
 * bonfyre-reason — several answers to one prompt, from a model that was run.
 *
 * A session holds a prompt. Each branch is one way of asking a model about it:
 * a system instruction and a temperature. Running a branch sends the prompt to
 * a chat-completions endpoint and keeps exactly what came back, with the
 * digests of what was sent and received. Comparing two branches compares their
 * text.
 *
 *   bonfyre-reason start  --prompt <file|text> [--session ID]
 *   bonfyre-reason branch [--modes cautious,creative,adversarial] [--session ID]
 *   bonfyre-reason run    --endpoint URL --model NAME [--mode M] [--max-tokens N] [--session ID]
 *   bonfyre-reason diff   <mode-a> <mode-b> [--session ID]
 *   bonfyre-reason commit <mode> [--session ID]
 *   bonfyre-reason status [--session ID]
 *   bonfyre-reason log    [--session ID]
 *
 * Session state lives in ~/.local/share/bonfyre/reason/<session>/
 *   prompt.txt, branches/<mode>.branch, runs/<mode>.txt, runs/<mode>.receipt,
 *   answer.txt once a branch is committed.
 *
 * What stood here before embedded a hash of the prompt, moved that vector
 * through a Hamiltonian and named the trajectories after ways of thinking. No
 * model ran and nothing was reasoned about. A branch that has not been run has
 * no answer here, and none is produced for it.
 */

#define _POSIX_C_SOURCE 200809L
#include <ctype.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <bonfyre.h>

typedef struct { const char *name; const char *system; double temperature; } Mode;

/* The on-demand model on this machine (bonfyre-moe serve). */
#define LOCAL_ENDPOINT "http://127.0.0.1:7000"

static const Mode MODES[] = {
    {"cautious",    "Answer only with what follows from the prompt. Say plainly what is unknown or assumed.", 0.0},
    {"creative",    "Propose approaches the prompt does not mention. Give each one's cost and what could make it fail.", 0.9},
    {"adversarial", "Argue against the premise of the prompt. Name what is wrong, missing or risky in it.", 0.4},
};
#define N_MODES ((int)(sizeof(MODES) / sizeof(MODES[0])))

static const Mode *mode_named(const char *name) {
    for (int i = 0; i < N_MODES; i++) if (!strcmp(MODES[i].name, name)) return &MODES[i];
    return NULL;
}

static const char *opt(int argc, char **argv, const char *flag) {
    for (int i = 0; i + 1 < argc; i++) if (!strcmp(argv[i], flag)) return argv[i + 1];
    return NULL;
}

static int valid_id(const char *s) {
    if (!s || !*s || strlen(s) > 64) return 0;
    for (; *s; s++) if (!isalnum((unsigned char)*s) && *s != '-' && *s != '_') return 0;
    return 1;
}

static int session_dir(char *out, size_t n, int argc, char **argv) {
    const char *sid = opt(argc, argv, "--session");
    if (!sid) sid = "default";
    if (!valid_id(sid)) { fprintf(stderr, "bonfyre-reason: a session name is letters, digits, - and _\n"); return -1; }
    /* BONFYRE_REASON_DIR names where sessions are kept, for a caller that
     * runs without a home directory. */
    const char *root = getenv("BONFYRE_REASON_DIR");
    if (root && *root) { snprintf(out, n, "%s/%s", root, sid); return 0; }
    const char *home = getenv("HOME");
    if (!home || !*home) { fprintf(stderr, "bonfyre-reason: set HOME or BONFYRE_REASON_DIR\n"); return -1; }
    snprintf(out, n, "%s/.local/share/bonfyre/reason/%s", home, sid);
    return 0;
}

static char *read_all(const char *path, size_t *len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (size < 0) { fclose(f); return NULL; }
    char *buf = malloc((size_t)size + 1);
    if (!buf || fread(buf, 1, (size_t)size, f) != (size_t)size) { fclose(f); free(buf); return NULL; }
    fclose(f);
    buf[size] = '\0';
    if (len) *len = (size_t)size;
    return buf;
}

static int write_all(const char *path, const char *text, size_t len) {
    FILE *f = fopen(path, "wb");
    if (!f) return -1;
    size_t wrote = fwrite(text, 1, len, f);
    return (fclose(f) == 0 && wrote == len) ? 0 : -1;
}

static void sha_hex(const char *data, size_t len, char out[65]) {
    bf_sha256_hex((const uint8_t *)data, len, out);
}

static void json_escape(FILE *out, const char *s) {
    fputc('"', out);
    for (; *s; s++) {
        unsigned char c = (unsigned char)*s;
        if (c == '"' || c == '\\') { fputc('\\', out); fputc(c, out); }
        else if (c == '\n') fputs("\\n", out);
        else if (c == '\r') fputs("\\r", out);
        else if (c == '\t') fputs("\\t", out);
        else if (c < 0x20) fprintf(out, "\\u%04x", c);
        else fputc(c, out);
    }
    fputc('"', out);
}

/* The string value of the first "content" key after "message": what the model
 * said. Returns a newly allocated, unescaped string or NULL. */
static char *message_content(const char *response) {
    const char *at = strstr(response, "\"message\"");
    if (!at) return NULL;
    at = strstr(at, "\"content\"");
    if (!at) return NULL;
    at = strchr(at + 9, ':');
    if (!at) return NULL;
    at++;
    while (*at == ' ' || *at == '\n' || *at == '\t') at++;
    if (*at != '"') return NULL;
    at++;
    char *out = malloc(strlen(at) + 1);
    if (!out) return NULL;
    size_t n = 0;
    while (*at && *at != '"') {
        if (*at == '\\' && at[1]) {
            at++;
            switch (*at) {
                case 'n': out[n++] = '\n'; break;
                case 't': out[n++] = '\t'; break;
                case 'r': out[n++] = '\r'; break;
                case 'u': {
                    unsigned code = 0;
                    if (sscanf(at + 1, "%4x", &code) == 1) {
                        if (code < 0x80) out[n++] = (char)code;
                        else if (code < 0x800) { out[n++] = (char)(0xC0 | (code >> 6)); out[n++] = (char)(0x80 | (code & 0x3F)); }
                        else { out[n++] = (char)(0xE0 | (code >> 12)); out[n++] = (char)(0x80 | ((code >> 6) & 0x3F)); out[n++] = (char)(0x80 | (code & 0x3F)); }
                        at += 4;
                    }
                    break;
                }
                default: out[n++] = *at;
            }
            at++;
        } else out[n++] = *at++;
    }
    if (*at != '"') { free(out); return NULL; }
    out[n] = '\0';
    return out;
}

static int cmd_start(int argc, char **argv) {
    char dir[PATH_MAX], path[PATH_MAX];
    const char *prompt_arg = opt(argc, argv, "--prompt");
    if (!prompt_arg) { fprintf(stderr, "usage: bonfyre-reason start --prompt <file|text> [--session ID]\n"); return 1; }
    if (session_dir(dir, sizeof(dir), argc, argv) != 0) return 1;

    size_t len = 0;
    char *prompt = read_all(prompt_arg, &len);
    if (!prompt) { prompt = strdup(prompt_arg); len = strlen(prompt_arg); }
    if (!prompt || len == 0) { fprintf(stderr, "bonfyre-reason: the prompt is empty\n"); free(prompt); return 1; }

    snprintf(path, sizeof(path), "%s/prompt.txt", dir);
    if (access(path, F_OK) == 0) {
        fprintf(stderr, "bonfyre-reason: this session already holds a prompt; start another session\n");
        free(prompt); return 1;
    }
    snprintf(path, sizeof(path), "%s/branches", dir);
    if (bf_ensure_dir(path) != 0) { perror(path); free(prompt); return 1; }
    snprintf(path, sizeof(path), "%s/runs", dir);
    if (bf_ensure_dir(path) != 0) { perror(path); free(prompt); return 1; }
    snprintf(path, sizeof(path), "%s/prompt.txt", dir);
    if (write_all(path, prompt, len) != 0) { perror(path); free(prompt); return 1; }

    char digest[65];
    sha_hex(prompt, len, digest);
    printf("session: %s\nprompt_sha256: %s\nprompt_bytes: %zu\n", dir, digest, len);
    free(prompt);
    return 0;
}

static int cmd_branch(int argc, char **argv) {
    char dir[PATH_MAX], path[PATH_MAX];
    if (session_dir(dir, sizeof(dir), argc, argv) != 0) return 1;
    snprintf(path, sizeof(path), "%s/prompt.txt", dir);
    if (access(path, R_OK) != 0) { fprintf(stderr, "bonfyre-reason: no session here; run start first\n"); return 1; }

    const char *modes = opt(argc, argv, "--modes");
    char list[256];
    snprintf(list, sizeof(list), "%s", modes ? modes : "cautious,creative,adversarial");
    for (char *name = strtok(list, ","); name; name = strtok(NULL, ",")) {
        const Mode *m = mode_named(name);
        if (!m) {
            fprintf(stderr, "bonfyre-reason: unknown mode '%s' (cautious, creative, adversarial)\n", name);
            return 1;
        }
        snprintf(path, sizeof(path), "%s/branches/%s.branch", dir, m->name);
        FILE *f = fopen(path, "w");
        if (!f) { perror(path); return 1; }
        fprintf(f, "temperature %.2f\nsystem %s\n", m->temperature, m->system);
        fclose(f);
        printf("branch %s: temperature %.2f, not yet run\n", m->name, m->temperature);
    }
    return 0;
}

/* Send one branch to the model and keep what it said. */
static int run_branch(const char *dir, const Mode *m, const char *prompt, size_t prompt_len,
                      const char *endpoint, const char *model, int max_tokens) {
    char request_path[PATH_MAX], response_path[PATH_MAX], out_path[PATH_MAX], receipt_path[PATH_MAX], url[1024];
    snprintf(request_path, sizeof(request_path), "%s/runs/%s.request.json", dir, m->name);
    snprintf(response_path, sizeof(response_path), "%s/runs/%s.response.json", dir, m->name);
    snprintf(out_path, sizeof(out_path), "%s/runs/%s.txt", dir, m->name);
    snprintf(receipt_path, sizeof(receipt_path), "%s/runs/%s.receipt", dir, m->name);
    snprintf(url, sizeof(url), "%s%s", endpoint,
             strstr(endpoint, "/chat/completions") ? "" : "/v1/chat/completions");
    unlink(out_path);
    unlink(receipt_path);

    FILE *req = fopen(request_path, "w");
    if (!req) { perror(request_path); return 1; }
    fputs("{\"model\":", req); json_escape(req, model);
    fprintf(req, ",\"temperature\":%.2f,\"max_tokens\":%d,\"stream\":false,\"messages\":[{\"role\":\"system\",\"content\":", m->temperature, max_tokens);
    json_escape(req, m->system);
    fputs("},{\"role\":\"user\",\"content\":", req);
    json_escape(req, prompt);
    fputs("}]}", req);
    fclose(req);

    char data_arg[PATH_MAX + 2];
    snprintf(data_arg, sizeof(data_arg), "@%s", request_path);
    pid_t pid = fork();
    if (pid < 0) return 1;
    if (pid == 0) {
        execlp("curl", "curl", "-sS", "--fail-with-body", "--max-time", "600",
               "-H", "Content-Type: application/json",
               "--data-binary", data_arg, "-o", response_path, url, (char *)NULL);
        _exit(127);
    }
    int status = 0;
    waitpid(pid, &status, 0);
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        char *body = read_all(response_path, NULL);
        fprintf(stderr, "bonfyre-reason: %s did not answer for branch %s; the branch has no result\n", url, m->name);
        if (body && *body) fprintf(stderr, "  it said: %.600s\n", body);
        else if (strstr(url, LOCAL_ENDPOINT))
            fprintf(stderr, "  nothing is listening there; start the local model with launchctl kickstart gui/$(id -u)/com.bonfyre.moe\n"
                            "  or name another with --endpoint URL --model NAME\n");
        free(body);
        return 1;
    }

    size_t response_len = 0;
    char *response = read_all(response_path, &response_len);
    char *answer = response ? message_content(response) : NULL;
    if (!answer || !*answer) {
        fprintf(stderr, "bonfyre-reason: the reply for branch %s carried no message; the branch has no result\n", m->name);
        free(response); free(answer);
        return 1;
    }
    size_t answer_len = strlen(answer);
    if (write_all(out_path, answer, answer_len) != 0) { perror(out_path); free(response); free(answer); return 1; }

    /* The model asked for and the model that answered can differ: the local
     * server loads whichever of its checkpoints fits. Keep both, say the second. */
    char answered_by[200] = "";
    const char *named = strstr(response, "\"model\":\"");
    if (named) {
        size_t n = 0;
        for (named += 9; *named && *named != '"' && n + 1 < sizeof(answered_by); named++) answered_by[n++] = *named;
        answered_by[n] = '\0';
    }
    if (!answered_by[0]) snprintf(answered_by, sizeof(answered_by), "%s", model);
    char prompt_digest[65], answer_digest[65], response_digest[65], now[40];
    sha_hex(prompt, prompt_len, prompt_digest);
    sha_hex(answer, answer_len, answer_digest);
    sha_hex(response, response_len, response_digest);
    bf_iso_timestamp(now, sizeof(now));
    FILE *rec = fopen(receipt_path, "w");
    if (!rec) { perror(receipt_path); free(response); free(answer); return 1; }
    fprintf(rec, "mode %s\nendpoint %s\nmodel %s\ntemperature %.2f\nmax_tokens %d\nprompt_sha256 %s\n"
                 "response_sha256 %s\nanswer_sha256 %s\nanswer_bytes %zu\nran_at %s\nanswered_by %s\n",
            m->name, url, model, m->temperature, max_tokens, prompt_digest, response_digest,
            answer_digest, answer_len, now, answered_by);
    fclose(rec);
    printf("branch %s: %zu bytes from %s (%s)\n", m->name, answer_len, answered_by, answer_digest);
    free(response);
    free(answer);
    return 0;
}

static int cmd_run(int argc, char **argv) {
    char dir[PATH_MAX], path[PATH_MAX];
    const char *endpoint = opt(argc, argv, "--endpoint"), *model = opt(argc, argv, "--model");
    const char *only = opt(argc, argv, "--mode"), *max_arg = opt(argc, argv, "--max-tokens");
    if (!endpoint) endpoint = getenv("BONFYRE_REASON_ENDPOINT");
    if (!model) model = getenv("BONFYRE_REASON_MODEL");
    /* With nothing named, the model on this machine answers: bonfyre-moe serve
     * owns port 7000 and loads its checkpoint on demand, so no URL or model
     * name is something a new person has to know first. */
    if (!endpoint) endpoint = LOCAL_ENDPOINT;
    if (!model) model = "local";
    if (!endpoint || !model) {
        fprintf(stderr, "usage: bonfyre-reason run --endpoint URL --model NAME [--mode M] [--max-tokens N] [--session ID]\n"
                        "  a branch is a model run; without a model there is nothing to run\n");
        return 1;
    }
    if (session_dir(dir, sizeof(dir), argc, argv) != 0) return 1;
    int max_tokens = max_arg ? atoi(max_arg) : 512;
    if (max_tokens < 1) max_tokens = 1;

    snprintf(path, sizeof(path), "%s/prompt.txt", dir);
    size_t prompt_len = 0;
    char *prompt = read_all(path, &prompt_len);
    if (!prompt) { fprintf(stderr, "bonfyre-reason: no session here; run start first\n"); return 1; }

    int ran = 0, failed = 0;
    for (int i = 0; i < N_MODES; i++) {
        if (only && strcmp(only, MODES[i].name)) continue;
        snprintf(path, sizeof(path), "%s/branches/%s.branch", dir, MODES[i].name);
        if (access(path, R_OK) != 0) continue;
        if (run_branch(dir, &MODES[i], prompt, prompt_len, endpoint, model, max_tokens) == 0) ran++;
        else failed++;
    }
    free(prompt);
    if (ran + failed == 0) { fprintf(stderr, "bonfyre-reason: no such branch; run branch first\n"); return 1; }
    return failed ? 1 : 0;
}

/* Words of a text, lowercased, as a sorted unique list. */
static int cmp_str(const void *a, const void *b) { return strcmp(*(char *const *)a, *(char *const *)b); }

static size_t word_set(char *text, char ***out) {
    size_t cap = 256, n = 0;
    char **words = malloc(cap * sizeof(char *));
    if (!words) return 0;
    for (char *p = text; *p; ) {
        while (*p && !isalnum((unsigned char)*p)) p++;
        if (!*p) break;
        char *start = p;
        while (*p && (isalnum((unsigned char)*p) || *p == '\'')) { *p = (char)tolower((unsigned char)*p); p++; }
        if (*p) *p++ = '\0';
        if (n == cap) { cap *= 2; char **grown = realloc(words, cap * sizeof(char *)); if (!grown) break; words = grown; }
        words[n++] = start;
    }
    qsort(words, n, sizeof(char *), cmp_str);
    size_t unique = 0;
    for (size_t i = 0; i < n; i++) if (unique == 0 || strcmp(words[unique - 1], words[i])) words[unique++] = words[i];
    *out = words;
    return unique;
}

static int cmd_diff(int argc, char **argv) {
    char dir[PATH_MAX], pa[PATH_MAX], pb[PATH_MAX];
    if (argc < 2 || argv[0][0] == '-' || argv[1][0] == '-') { fprintf(stderr, "usage: bonfyre-reason diff <mode-a> <mode-b> [--session ID]\n"); return 1; }
    if (!mode_named(argv[0]) || !mode_named(argv[1])) { fprintf(stderr, "bonfyre-reason: unknown mode\n"); return 1; }
    if (session_dir(dir, sizeof(dir), argc, argv) != 0) return 1;
    snprintf(pa, sizeof(pa), "%s/runs/%s.txt", dir, argv[0]);
    snprintf(pb, sizeof(pb), "%s/runs/%s.txt", dir, argv[1]);
    size_t la = 0, lb = 0;
    char *a = read_all(pa, &la), *b = read_all(pb, &lb);
    if (!a || !b) {
        fprintf(stderr, "bonfyre-reason: branch %s has not been run; there is nothing to compare\n", a ? argv[1] : argv[0]);
        free(a); free(b);
        return 1;
    }
    int identical = (la == lb && memcmp(a, b, la) == 0);
    char **wa = NULL, **wb = NULL;
    size_t na = word_set(a, &wa), nb = word_set(b, &wb), shared = 0, i = 0, j = 0;
    while (i < na && j < nb) {
        int c = strcmp(wa[i], wb[j]);
        if (c == 0) { shared++; i++; j++; } else if (c < 0) i++; else j++;
    }
    size_t all = na + nb - shared;
    printf("%s: %zu bytes, %zu distinct words\n%s: %zu bytes, %zu distinct words\n", argv[0], la, na, argv[1], lb, nb);
    printf("identical: %s\nshared words: %zu of %zu (%.3f)\n", identical ? "yes" : "no", shared, all,
           all ? (double)shared / (double)all : 1.0);
    printf("only in %s:", argv[0]);
    i = j = 0;
    for (size_t shown = 0; i < na && shown < 24; ) {
        int c = j < nb ? strcmp(wa[i], wb[j]) : -1;
        if (c == 0) { i++; j++; } else if (c < 0) { if (strlen(wa[i]) > 4) { printf(" %s", wa[i]); shown++; } i++; } else j++;
    }
    printf("\nonly in %s:", argv[1]);
    i = j = 0;
    for (size_t shown = 0; j < nb && shown < 24; ) {
        int c = i < na ? strcmp(wa[i], wb[j]) : 1;
        if (c == 0) { i++; j++; } else if (c > 0) { if (strlen(wb[j]) > 4) { printf(" %s", wb[j]); shown++; } j++; } else i++;
    }
    printf("\n");
    free(wa); free(wb); free(a); free(b);
    return 0;
}

static int cmd_commit(int argc, char **argv) {
    char dir[PATH_MAX], from[PATH_MAX], to[PATH_MAX];
    if (argc < 1 || argv[0][0] == '-' || !mode_named(argv[0])) { fprintf(stderr, "usage: bonfyre-reason commit <mode> [--session ID]\n"); return 1; }
    if (session_dir(dir, sizeof(dir), argc, argv) != 0) return 1;
    snprintf(from, sizeof(from), "%s/runs/%s.txt", dir, argv[0]);
    size_t len = 0;
    char *answer = read_all(from, &len);
    if (!answer) { fprintf(stderr, "bonfyre-reason: branch %s has not been run; there is no answer to commit\n", argv[0]); return 1; }
    snprintf(to, sizeof(to), "%s/answer.txt", dir);
    int rc = write_all(to, answer, len);
    char digest[65];
    sha_hex(answer, len, digest);
    free(answer);
    if (rc != 0) { perror(to); return 1; }
    snprintf(to, sizeof(to), "%s/answer.from", dir);
    write_all(to, argv[0], strlen(argv[0]));
    printf("committed %s as the session's answer (%s)\n", argv[0], digest);
    return 0;
}

static int cmd_status(int argc, char **argv, int with_receipts) {
    char dir[PATH_MAX], path[PATH_MAX];
    if (session_dir(dir, sizeof(dir), argc, argv) != 0) return 1;
    snprintf(path, sizeof(path), "%s/prompt.txt", dir);
    size_t len = 0;
    char *prompt = read_all(path, &len);
    if (!prompt) { fprintf(stderr, "bonfyre-reason: no session here\n"); return 1; }
    char digest[65];
    sha_hex(prompt, len, digest);
    free(prompt);
    printf("session: %s\nprompt_sha256: %s\n", dir, digest);
    for (int i = 0; i < N_MODES; i++) {
        snprintf(path, sizeof(path), "%s/branches/%s.branch", dir, MODES[i].name);
        if (access(path, R_OK) != 0) continue;
        snprintf(path, sizeof(path), "%s/runs/%s.receipt", dir, MODES[i].name);
        char *receipt = read_all(path, NULL);
        printf("branch %s: %s\n", MODES[i].name, receipt ? "run" : "not run");
        if (receipt && with_receipts) fputs(receipt, stdout);
        free(receipt);
    }
    snprintf(path, sizeof(path), "%s/answer.from", dir);
    char *from = read_all(path, NULL);
    printf("answer: %s\n", from ? from : "none committed");
    free(from);
    return 0;
}

/* One question, one answer: start a session, make one branch, run it, and
 * print what the model returned. */
static int cmd_ask(int argc, char **argv) {
    const char *prompt = opt(argc, argv, "--prompt"), *mode = opt(argc, argv, "--mode");
    const char *session = opt(argc, argv, "--session");
    if (!mode) mode = "cautious";
    if (!prompt || !mode_named(mode)) {
        fprintf(stderr, "usage: bonfyre-reason ask --prompt <file|text> [--endpoint URL --model NAME] [--mode M] [--session ID]\n");
        return 1;
    }
    char *start_argv[] = { "--prompt", (char *)prompt, "--session", (char *)(session ? session : "default") };
    if (cmd_start(4, start_argv) != 0) return 1;
    char *branch_argv[] = { "--modes", (char *)mode, "--session", (char *)(session ? session : "default") };
    if (cmd_branch(4, branch_argv) != 0) return 1;
    if (cmd_run(argc, argv) != 0) return 1;

    char dir[PATH_MAX], path[PATH_MAX];
    if (session_dir(dir, sizeof(dir), argc, argv) != 0) return 1;
    snprintf(path, sizeof(path), "%s/runs/%s.txt", dir, mode);
    char *answer = read_all(path, NULL);
    if (!answer) return 1;
    printf("\n%s\n", answer);
    free(answer);
    return 0;
}

static void usage(void) {
    fprintf(stderr,
        "bonfyre-reason — several answers to one prompt, from a model that was run\n\n"
        "  start  --prompt <file|text> [--session ID]\n"
        "  branch [--modes cautious,creative,adversarial] [--session ID]\n"
        "  run    [--endpoint URL --model NAME] [--mode M] [--max-tokens N] [--session ID]\n"
        "  ask    --prompt <file|text> [--endpoint URL --model NAME] [--mode M] [--session ID]\n"
        "  diff   <mode-a> <mode-b> [--session ID]\n"
        "  commit <mode> [--session ID]\n"
        "  status [--session ID]\n"
        "  log    [--session ID]\n\n"
        "Without --endpoint the model on this machine answers (" LOCAL_ENDPOINT ",\n"
        "loaded on demand; bonfyre model local shows what can load now). Any\n"
        "chat-completions endpoint works; BONFYRE_REASON_ENDPOINT and\n"
        "BONFYRE_REASON_MODEL may name it. A branch that was not run has no\n"
        "answer, and none is made up for it.\n");
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    if (argc < 2) { usage(); return 1; }
    const char *cmd = argv[1];
    if (!strcmp(cmd, "start"))  return cmd_start(argc - 2, argv + 2);
    if (!strcmp(cmd, "branch")) return cmd_branch(argc - 2, argv + 2);
    if (!strcmp(cmd, "run"))    return cmd_run(argc - 2, argv + 2);
    if (!strcmp(cmd, "ask"))    return cmd_ask(argc - 2, argv + 2);
    if (!strcmp(cmd, "diff"))   return cmd_diff(argc - 2, argv + 2);
    if (!strcmp(cmd, "commit")) return cmd_commit(argc - 2, argv + 2);
    if (!strcmp(cmd, "status")) return cmd_status(argc - 2, argv + 2, 0);
    if (!strcmp(cmd, "log"))    return cmd_status(argc - 2, argv + 2, 1);
    fprintf(stderr, "bonfyre-reason: unknown command '%s'\n", cmd);
    usage();
    return 1;
}
