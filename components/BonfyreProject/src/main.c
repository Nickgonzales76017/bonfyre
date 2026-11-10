#define _POSIX_C_SOURCE 200809L

#include <limits.h>
#include <ctype.h>
#include "bf_lex.h"
#include <errno.h>
#include <sqlite3.h>
#include "bf_matrix.h"
/* The destination lifecycle, compiled from CUE. A state is a delivery only
 * because this table says so, which is the point of generating it: the field
 * that used to contradict a readiness figure was a literal zero. */
#include "native_destination_release.h"
/* Criterion kinds bound to the columns they read, and the verdicts a machine
 * is allowed to set. Compiled from CUE so neither can be invented here. */
#include "native_discovery_sets.h"
/* The four cells nothing owned: a snapshot, a held message, assent bound to
 * bytes, and a session that is live only while something says so. Each
 * contract's stated refusal is compiled in beside what it does. */
#include "native_missing_cells.h"
#include "bonfyre.h"   /* bf_sha256_hex: a delivery is its receipt's digest */
#include "bf_activation.h"

/* Declared with the includes: resolution and promotion both compare addresses,
 * and promotion's use sits well above the definition. */
static int foreign_same_resource(const char *a, const char *b);
static int foreign_body_is_boundary(const char *url, const char *body_path);
#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* Where this executable actually lives.
 *
 * A bare argv0 is searched on PATH, because the managed install is exactly the
 * case where the organ is invoked by name and its peers sit beside it. */
static int executable_dir(char *dir, size_t size, const char *argv0) {
    char full[PATH_MAX];
    if (!argv0 || !argv0[0]) return 0;
    if (argv0[0] == '/') {
        if (snprintf(full, sizeof(full), "%s", argv0) >= (int)sizeof(full)) return 0;
    } else if (strchr(argv0, '/')) {
        char cwd[PATH_MAX];
        if (!getcwd(cwd, sizeof(cwd))) return 0;
        if (snprintf(full, sizeof(full), "%s/%s", cwd, argv0) >= (int)sizeof(full)) return 0;
    } else {
        const char *path = getenv("PATH");
        int found = 0;
        if (!path) return 0;
        while (*path && !found) {
            const char *end = strchr(path, ':');
            size_t n = end ? (size_t)(end - path) : strlen(path);
            if (n > 0 && n + strlen(argv0) + 2 < sizeof(full)) {
                snprintf(full, sizeof(full), "%.*s/%s", (int)n, path, argv0);
                if (access(full, X_OK) == 0) found = 1;
            }
            path = end ? end + 1 : path + n;
        }
        if (!found) return 0;
    }
    {
        char *last = strrchr(full, '/');
        if (!last || last == full) return 0;
        *last = '\0';
    }
    return snprintf(dir, size, "%s", full) < (int)size;
}

/* A peer organ: same directory as this binary.
 *
 * Bonfyre organs ship in two layouts. A checkout nests each under its own
 * component directory (cmd/BonfyreProject/bonfyre-project, then
 * cmd/BonfyreCMS/bonfyre-cms); a managed install lays them out as peers in one
 * bin directory (~/.bonfyre/bin, /usr/local/bin). Only the checkout shape was
 * resolved -- strip two components, re-append <Component>/<binary> -- so in
 * every managed installation `bonfyre-project cms ...` looked for
 * ~/.bonfyre/BonfyreCMS/bonfyre-cms, missed, and fell through to a literal
 * relative path that missed too. The delegation was broken everywhere the
 * software is actually installed, which is why it looked like a CMS problem
 * rather than a resolver one. It affects every delegated organ, not just CMS. */
static void resolve_executable_peer(char *buffer, size_t size, const char *argv0, const char *binary_name) {
    char dir[PATH_MAX];
    buffer[0] = '\0';
    if (!executable_dir(dir, sizeof(dir), argv0)) return;
    snprintf(buffer, size, "%s/%s", dir, binary_name);
}

static void resolve_executable_sibling(char *buffer, size_t size, const char *argv0, const char *sibling_dir, const char *binary_name) {
    char dir[PATH_MAX];
    char *last;
    buffer[0] = '\0';
    if (!executable_dir(dir, sizeof(dir), argv0)) return;
    /* Up out of this organ's own component directory, then across. */
    last = strrchr(dir, '/');
    if (!last || last == dir) return;
    *last = '\0';
    /* The previous version passed `buffer` as both destination and source of
     * one snprintf, which is undefined; building from a separate `dir` is not
     * a style preference. */
    snprintf(buffer, size, "%s/%s/%s", dir, sibling_dir, binary_name);
}

/* Resolution order is declared, not incidental: an explicit environment
 * override, then a managed peer, then a checkout sibling, then the stated
 * fallback. Every step is a real access(X_OK) test, so a layout that is not
 * present is skipped rather than guessed at.
 *
 * Cannot infer: a resolved path -> authority to run what is there. Resolution
 * finds an executable; the organ still owns its own effect boundary. */
static const char *default_binary(const char *env_name, const char *argv0, char *resolved, size_t resolved_size, const char *dir, const char *name, const char *fallback) {
    const char *env = getenv(env_name);
    if (env && env[0] != '\0') return env;
    resolve_executable_peer(resolved, resolved_size, argv0, name);
    if (resolved[0] != '\0' && access(resolved, X_OK) == 0) return resolved;
    resolve_executable_sibling(resolved, resolved_size, argv0, dir, name);
    if (resolved[0] != '\0' && access(resolved, X_OK) == 0) return resolved;
    return fallback;
}

static int run_command(char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) return 1;
    if (pid == 0) {
        execv(argv[0], argv);
        perror(argv[0]);
        _exit(127);
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) return 1;
    if (!WIFEXITED(status)) return 1;
    return WEXITSTATUS(status);
}

static void print_usage(void) {
    fprintf(stderr,
            "bonfyre-project\n\n"
            "Usage:\n"
            "  bonfyre-project cms <cms args...>\n"
            "  bonfyre-project index <index args...>\n"
            "  bonfyre-project stitch <stitch args...>\n"
            "  bonfyre-project refresh <artifact-dir> [--db FILE]\n"
            "  bonfyre-project message review --db FILE --ns NAME [--format json|html] [--out FILE]\n"
"  bonfyre-project activation project-demand <demand-evidence.jsonl> --db FILE\n"
"                                                project SupplyMatch eligibility without authority\n"
"  bonfyre-project activation select <resource> --actor ACTOR [--mechanism NAME] [--evidence REF] --db FILE\n"
"                                                record an explicit activation selection, never authority\n"
"  bonfyre-project activation bind <resource> --service SERVICE --actor ACTOR --evidence REF --db FILE\n"
"                                                bind only through an existing AuthorityGraph grant\n"
"  bonfyre-project activation reconcile [resource] --db FILE\n"
"                                                read AuthorityGraph + ServiceLifecycle; writes no state\n"
            "  bonfyre-project foreign observe <twin-id> <https-url> [--receipt FILE] [--body FILE] [--observe-right public|denied] [--robots URL] [--max-bytes N] [--range-start N] [--full-max-bytes N]\n"
            "  bonfyre-project foreign pack <receipts-dir> <pack-file>          every loose receipt into one compressed store; the directory is only read\n"
        "  bonfyre-project foreign pack-verify <receipts-dir> <pack-file>   each packed receipt against its loose file, byte for byte\n"
        "  bonfyre-project foreign resolve <https-url>    what is currently believed, and whether it is still reachable\n"
            "  bonfyre-project foreign history <https-url>    every observation, with its relation to the one before\n"
            "  bonfyre-project foreign conflicts             resources whose observations disagree\n"
            "  bonfyre-project foreign promote [url] [--dry-run]  make observed resources addressable subjects\n"
            "  bonfyre-project foreign twins [--receipts] [--page OFFSET --limit N] [--query TEXT] [--expect-generation ID]  source-release catalogue; --receipts is forensic history\n"
            "  bonfyre-project foreign admissible            which observations may support a derived claim\n"
            "  bonfyre-project foreign hydration             what each twin is held as, and what it costs\n"
            "  bonfyre-project foreign relate <from> <predicate> <to> [--federation F] [--carrier C] [--evidence D] [--scope S]\n"
            "  bonfyre-project foreign edges [--scope S]     the observed external graph, filtered in the query\n"
            "  bonfyre-project foreign reheat [--dry-run] [--budget 40] [--per-host 2]\n"
            "                                                read the most overdue sources this run can afford\n");
}

static int is_http_url(const char *url) {
    return (strncmp(url, "https://", 8) == 0 && url[8] != '\0') ||
           (strncmp(url, "http://", 7) == 0 && url[7] != '\0');
}

/* Delegates to the one implementation. Nineteen copies of this shape existed
 * with sixteen different opinions about escaping.
 */
static void json_string(FILE *out, const char *text) {
    bf_lex_json_emit(out, text);
}

static int read_child_stdout(char *const child_argv[], char *out, size_t out_size) {
    int pipefd[2];
    if (pipe(pipefd) != 0) return 127;
    pid_t pid = fork();
    if (pid < 0) { close(pipefd[0]); close(pipefd[1]); return 127; }
    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        close(pipefd[1]);
        execvp(child_argv[0], child_argv);
        _exit(127);
    }
    close(pipefd[1]);
    ssize_t n = read(pipefd[0], out, out_size - 1);
    close(pipefd[0]);
    if (n < 0) n = 0;
    out[n] = '\0';
    int status = 0;
    if (waitpid(pid, &status, 0) < 0 || !WIFEXITED(status)) return 127;
    return WEXITSTATUS(status);
}

static int sha256_file(const char *path, char digest[65]) {
    char output[128] = {0};
    char *const command[] = {"shasum", "-a", "256", (char *)path, NULL};
    if (read_child_stdout(command, output, sizeof(output)) != 0) return -1;
    if (strlen(output) < 64) return -1;
    for (int i = 0; i < 64; i++) {
        char c = output[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return -1;
        digest[i] = c;
    }
    digest[64] = '\0';
    return 0;
}

/* The HTTP evidence envelope.
 *
 * A receipt recorded our own digest of the bytes and nothing the source itself
 * said about them. Those are different facts and both matter: our digest proves
 * the bytes have not drifted since we retained them, and the source's own
 * ETag, Last-Modified, Content-Location, Link rel=license and RFC 9530
 * Content-Digest are what the publisher asserted about the representation it
 * served. Keeping only ours loses every one of those, including the only
 * machine-readable statement of rights a source ever makes.
 *
 * Each is held separately and none of them is promoted:
 *
 *   a source-supplied Content-Digest matching ours  ->  the bytes agree, and
 *                                                       nothing about whether
 *                                                       the source is right
 *   robots allowing a read  ->  not a licence to republish; Link rel=license
 *                               is a different fact and usually absent
 *   a 304 Not Modified     ->  the representation we hold is still current,
 *                              and not that the thing stopped changing
 */
struct BfHttpEnvelope {
    char etag[256];
    char last_modified[128];
    char content_type[128];
    char content_location[1024];
    char link_license[1024];
    char link_canonical[1024];
    char content_digest[512];
    char repr_digest[512];
    char signature_input[512];
    int  has_signature;
};

/* The header and link readers live in bf_lex.h and are declared in
 * schemas/control/text-rules.cue as the `http-response` ruleset, so a witness
 * can read the same fields by the same names without a second parser. */
static void envelope_field(const char *line, const char *name, char *out, size_t cap) {
    char value[4096];
    if (out[0]) return;                     /* first value in a block wins */
    if (bf_lex_http_header(line, name, value, sizeof(value)))
        snprintf(out, cap, "%s", value);
}

static void envelope_link(const char *line, struct BfHttpEnvelope *out) {
    char value[4096];
    if (!out->link_license[0] && bf_lex_http_link_rel(line, "license", value, sizeof(value)))
        snprintf(out->link_license, sizeof(out->link_license), "%s", value);
    if (!out->link_canonical[0] && bf_lex_http_link_rel(line, "canonical", value, sizeof(value)))
        snprintf(out->link_canonical, sizeof(out->link_canonical), "%s", value);
}

static void parse_http_envelope(const char *header_path, struct BfHttpEnvelope *out) {
    FILE *fh = fopen(header_path, "rb");
    char line[4096];
    memset(out, 0, sizeof(*out));
    if (!fh) return;
    while (fgets(line, sizeof(line), fh)) {
        char *end = line + strlen(line);
        while (end > line && (end[-1] == '\n' || end[-1] == '\r')) *--end = '\0';
        if (!line[0]) continue;
        /* A redirect chain writes several header blocks; later blocks describe
         * the representation actually served, so they are allowed to win by
         * clearing nothing and letting the first value of the final block
         * stand. */
        if (!strncasecmp(line, "HTTP/", 5)) { memset(out, 0, sizeof(*out)); continue; }
        envelope_field(line, "ETag", out->etag, sizeof(out->etag));
        envelope_field(line, "Last-Modified", out->last_modified, sizeof(out->last_modified));
        envelope_field(line, "Content-Type", out->content_type, sizeof(out->content_type));
        envelope_field(line, "Content-Location", out->content_location, sizeof(out->content_location));
        envelope_field(line, "Content-Digest", out->content_digest, sizeof(out->content_digest));
        envelope_field(line, "Repr-Digest", out->repr_digest, sizeof(out->repr_digest));
        envelope_field(line, "Signature-Input", out->signature_input, sizeof(out->signature_input));
        if (!strncasecmp(line, "Signature:", 10)) out->has_signature = 1;
        envelope_link(line, out);
    }
    fclose(fh);
}

static void write_http_envelope(FILE *receipt, const struct BfHttpEnvelope *env) {
    fputs(",\"http_evidence\":{", receipt);
    fputs("\"etag\":", receipt);            json_string(receipt, env->etag);
    fputs(",\"last_modified\":", receipt);  json_string(receipt, env->last_modified);
    fputs(",\"content_type\":", receipt);   json_string(receipt, env->content_type);
    fputs(",\"content_location\":", receipt); json_string(receipt, env->content_location);
    fputs(",\"link_license\":", receipt);   json_string(receipt, env->link_license);
    fputs(",\"link_canonical\":", receipt); json_string(receipt, env->link_canonical);
    /* RFC 9530. The source's own digest of its representation, which is not
     * this estate's digest of the bytes it retained. */
    fputs(",\"content_digest\":", receipt); json_string(receipt, env->content_digest);
    fputs(",\"repr_digest\":", receipt);    json_string(receipt, env->repr_digest);
    /* RFC 9421. Communication authenticity, never factual correctness. */
    fputs(",\"signature_input\":", receipt); json_string(receipt, env->signature_input);
    fprintf(receipt, ",\"message_signed\":%s", env->has_signature ? "true" : "false");
    fputs(",\"cannot_infer\":[", receipt);
    fputs("\"a source-supplied digest matching ours -> the source being right\",", receipt);
    fputs("\"robots allowing a read -> a licence to republish\",", receipt);
    fputs("\"a valid message signature -> factual correctness or reuse permission\",", receipt);
    fputs("\"an absent license link -> permission; it means the source stated none\"", receipt);
    fputs("]}", receipt);
}

/* Headers are dumped beside the body because the source's own statements about
 * a representation are evidence in their own right, and a conditional request
 * needs the validator the source gave us last time. */
static int fetch_to_file_with_headers(const char *url, const char *body_path,
                                      const char *header_path, const char *if_none_match,
                                      int *http_status, long long range_start,
                                      long long max_bytes) {
    char status_text[32] = {0};
    char conditional[512] = {0};
    char range_text[80] = {0}, max_bytes_text[32] = {0};
    char *command[26];
    int n = 0;
    int curl_exit;
    command[n++] = (char *)"curl";
    /* A capped full representation cannot silently follow to another origin:
     * robots admission was for the named URL, not its redirect destination. */
    if (range_start >= 0) command[n++] = (char *)"--location";
    command[n++] = (char *)"--silent";
    command[n++] = (char *)"--show-error";
    /* An index that answers a query can take longer than a file takes to
     * send; a minute is still a bounded wait inside a five-minute tick. */
    command[n++] = (char *)"--max-time";      command[n++] = (char *)"60";
    /* The product token robots rules match, with the contact address public
     * APIs ask a client to carry. */
    command[n++] = (char *)"--user-agent";    command[n++] = (char *)"BonfyreProject/1.0 (+https://aurekai.work/)";
    command[n++] = (char *)"--output";        command[n++] = (char *)body_path;
    if (header_path) { command[n++] = (char *)"--dump-header"; command[n++] = (char *)header_path; }
    if (if_none_match && *if_none_match) {
        /* A validator the source issued, handed back verbatim. */
        snprintf(conditional, sizeof(conditional), "If-None-Match: %s", if_none_match);
        command[n++] = (char *)"--header";
        command[n++] = conditional;
    }
    if (max_bytes > 0) {
        snprintf(max_bytes_text, sizeof(max_bytes_text), "%lld", max_bytes);
        if (range_start >= 0) {
            snprintf(range_text, sizeof(range_text), "%lld-%lld", range_start,
                     range_start + max_bytes - 1);
            command[n++] = (char *)"--range"; command[n++] = range_text;
        }
        command[n++] = (char *)"--max-filesize"; command[n++] = max_bytes_text;
    }
    command[n++] = (char *)"--write-out";     command[n++] = (char *)"%{http_code}";
    command[n++] = (char *)url;
    command[n] = NULL;
    curl_exit = read_child_stdout(command, status_text, sizeof(status_text));
    *http_status = atoi(status_text);
    return curl_exit;
}

/* A source that needs a credential is fetched with the address and headers
 * the credential boundary issued, handed to curl on its standard input so no
 * value ever appears in a process's arguments.  The receipt keeps the address
 * without them. */
static const char *observe_credential_config = NULL;

static int fetch_with_config(const char *config, const char *body_path, const char *header_path,
                             const char *if_none_match, int *http_status,
                             long long range_start, long long max_bytes) {
    int in[2], out[2]; pid_t pid; char status_text[32] = {0}; ssize_t got; int status = 0;
    FILE *cfg;
    if (pipe(in) || pipe(out)) return 127;
    if ((pid = fork()) < 0) return 127;
    if (pid == 0) {
        dup2(in[0], STDIN_FILENO); dup2(out[1], STDOUT_FILENO);
        close(in[1]); close(out[0]);
        execlp("curl", "curl", "--config", "-", (char *)NULL);
        _exit(127);
    }
    close(in[0]); close(out[1]);
    cfg = fdopen(in[1], "w");
    if (range_start >= 0) fputs("location\n", cfg);
    fputs("silent\nshow-error\nmax-time = 60\nuser-agent = \"BonfyreProject/1.0 (+https://aurekai.work/)\"\n", cfg);
    fprintf(cfg, "output = \"%s\"\n", body_path);
    if (header_path) fprintf(cfg, "dump-header = \"%s\"\n", header_path);
    if (if_none_match && *if_none_match && !strchr(if_none_match, '"')) fprintf(cfg, "header = \"If-None-Match: %s\"\n", if_none_match);
    if (max_bytes > 0) {
        if (range_start >= 0)
            fprintf(cfg, "range = \"%lld-%lld\"\n", range_start, range_start + max_bytes - 1);
        fprintf(cfg, "max-filesize = %lld\n", max_bytes);
    }
    fputs("write-out = \"%{http_code}\"\n", cfg);
    fputs(config, cfg);
    fclose(cfg);
    got = read(out[0], status_text, sizeof(status_text) - 1);
    if (got > 0) status_text[got] = '\0';
    close(out[0]);
    if (waitpid(pid, &status, 0) < 0 || !WIFEXITED(status)) return 127;
    *http_status = atoi(status_text);
    return WEXITSTATUS(status);
}

static int fetch_to_file(const char *url, const char *body_path, int *http_status) {
    return fetch_to_file_with_headers(url, body_path, NULL, NULL, http_status, 0, 0);
}

static int derive_robots_url(const char *url, char *robots_url, size_t robots_url_size) {
    const char *scheme = strstr(url, "://");
    if (!scheme) return -1;
    const char *authority = scheme + 3;
    const char *path = strchr(authority, '/');
    size_t origin_len = path ? (size_t)(path - url) : strlen(url);
    if (origin_len + strlen("/robots.txt") + 1 > robots_url_size) return -1;
    memcpy(robots_url, url, origin_len);
    robots_url[origin_len] = '\0';
    strcat(robots_url, "/robots.txt");
    return 0;
}

static char *trim_ascii(char *text) {
    while (*text && isspace((unsigned char)*text)) text++;
    char *end = text + strlen(text);
    while (end > text && isspace((unsigned char)end[-1])) *--end = '\0';
    return text;
}

static int robots_user_agent_score(const char *value) {
    static const char agent[] = "BonfyreProject";
    if (strcmp(value, "*") == 0) return 1;
    size_t value_len = strlen(value);
    size_t agent_len = strlen(agent);
    size_t compare_len = value_len < agent_len ? value_len : agent_len;
    if (compare_len > 0 && strncasecmp(value, agent, compare_len) == 0)
        return 100 + (int)value_len;
    return 0;
}

static int robots_pattern_matches(const char *pattern, const char *source_path) {
    if (*pattern == '\0') return 1;
    if (*pattern == '$' && pattern[1] == '\0') return *source_path == '\0';
    if (*pattern == '*') {
        do {
            if (robots_pattern_matches(pattern + 1, source_path)) return 1;
        } while (*source_path++);
        return 0;
    }
    if (*pattern != *source_path) return 0;
    if (pattern[1] == '\0') return 1;
    return robots_pattern_matches(pattern + 1, source_path + 1);
}

/* Returns 1 when the BonfyreProject user agent is disallowed for source_url. */
static int robots_disallow_source(const char *robots_path, const char *source_url) {
    FILE *robots = fopen(robots_path, "r");
    if (!robots) return 0;
    const char *scheme = strstr(source_url, "://");
    const char *path = scheme ? strchr(scheme + 3, '/') : NULL;
    const char *source_path = path ? path : "/";
    char line[2048];
    int group_score = 0, best_group_score = 0, saw_rule = 0;
    size_t best_allow = 0, best_disallow = 0;
    while (fgets(line, sizeof(line), robots)) {
        char *comment = strchr(line, '#');
        if (comment) *comment = '\0';
        char *field = trim_ascii(line);
        if (*field == '\0') continue;
        char *colon = strchr(field, ':');
        if (!colon) continue;
        *colon = '\0';
        char *value = trim_ascii(colon + 1);
        field = trim_ascii(field);
        if (strcasecmp(field, "user-agent") == 0) {
            if (saw_rule) { group_score = 0; saw_rule = 0; }
            int score = robots_user_agent_score(value);
            if (score > group_score) group_score = score;
            continue;
        }
        if (group_score == 0 || (strcasecmp(field, "allow") != 0 && strcasecmp(field, "disallow") != 0))
            continue;
        saw_rule = 1;
        if (group_score < best_group_score) continue;
        if (group_score > best_group_score) {
            best_group_score = group_score;
            best_allow = 0;
            best_disallow = 0;
        }
        if (*value == '\0' || !robots_pattern_matches(value, source_path)) continue;
        size_t rule_len = strlen(value);
        if (strcasecmp(field, "allow") == 0) {
            if (rule_len > best_allow) best_allow = rule_len;
        } else if (rule_len > best_disallow) {
            best_disallow = rule_len;
        }
    }
    fclose(robots);
    return best_disallow > best_allow;
}

/* Resolving what we currently believe about an external resource.
 *
 * Every observation is already kept as its own receipt: a 404 at T1, a robots
 * refusal at T1 and a successful fetch at T2 are three files, and nothing
 * overwrites anything. What was missing is the relation between them. Thirty-
 * five receipts sat on disk with no way to ask which one is current, whether an
 * earlier belief still holds, or where two observations disagree.
 *
 * The rule this implements is deliberately not "latest row wins". A 404 today
 * does not erase the bytes fetched last week; it says the resource stopped
 * answering. Those are two questions with two answers:
 *
 *   current content       the most recent observation that actually retained
 *                         bytes under an allow policy
 *   current availability  the most recent observation of any kind
 *
 * When those two point at different observations, the institution is holding
 * content it can no longer re-fetch -- which is exactly the state a single
 * "latest" answer would hide.
 *
 * Relations between consecutive observations of one URL:
 *
 *   confirms    same content digest; the later observation adds confidence,
 *               not information, and must not be reported as a change
 *   supersedes  both retained bytes, digests differ; the resource changed
 *   qualifies   content, then a boundary response; we learned less, not more
 *   restores    a boundary response, then content; the resource came back
 *
 * Cannot infer: a current observation -> a true statement about the world. This
 * reports what was observed and when, never that the source was correct. */

typedef struct {
    char url[8192];
    char at[32];
    char digest[80];
    char kind[32];
    char policy[16];
    int  http_status;
    char observe_right[24];
    char fetch_attempted[8];
    int  robots_exit;
    int  robots_status;
    char body_path[1024];
    int  retained;      /* an observation that actually holds bytes */
    char receipt_name[256];  /* the receipt it was read from, loose or packed */
} ForeignObs;

/* The receipts are written by this program in one shape, so a field reader is
 * the whole requirement -- not a JSON parser. A field that is absent or shaped
 * unexpectedly yields an empty string and the observation is still counted,
 * because dropping a receipt would silently shrink the history. */
/* One implementation, shared.
 *
 * This was a hand-rolled reader that stopped at the first quote in a string
 * value. An HTTP ETag is `W/"abc"` and arrives escaped, so it came back as
 * `W/\` -- and conditional re-observation, which hands that value back to the
 * source, had never worked while appearing to. The estate had 163 readers like
 * it. Now there is one, in bf_lex.h, declared over in
 * schemas/control/text-rules.cue, and a call site names a field rather than
 * remembering an escaping rule. */
static void receipt_field(const char *json, const char *key, char *out, size_t cap) {
    if (!out || cap == 0) return;
    out[0] = '\0';
    if (bf_lex_json_string(json, key, out, cap)) return;
    (void)bf_lex_json_scalar(json, key, out, cap);
}


static int activation_project_demand(const char *evidence_path, const char *db_path) {
    FILE *in = NULL;
    sqlite3 *db = NULL;
    BfActivation *act = NULL;
    sqlite3_stmt *mark = NULL, *stale = NULL;
    char *line = NULL;
    size_t line_cap = 0;
    ssize_t line_len;
    char **retract_ids = NULL;
    size_t retract_n = 0, retract_cap = 0;
    int read_rows = 0, eligible_rows = 0, projected = 0, retracted = 0;
    int rc = 1;

    if (!evidence_path || !db_path) return 1;
    in = fopen(evidence_path, "r");
    if (!in) {
        fprintf(stderr, "activation: cannot open demand evidence %s: %s\n",
                evidence_path, strerror(errno));
        goto done;
    }
    if (sqlite3_open(db_path, &db) != SQLITE_OK) {
        fprintf(stderr, "activation: cannot open %s: %s\n",
                db_path, db ? sqlite3_errmsg(db) : "?");
        goto done;
    }
    sqlite3_busy_timeout(db, 5000);
    if (bf_activation_open_database(db, &act, stderr) != 0) goto done;
    if (sqlite3_exec(
            db,
            "BEGIN IMMEDIATE;"
            "CREATE TEMP TABLE IF NOT EXISTS _bf_supplymatch_current("
            "resource_id TEXT PRIMARY KEY);"
            "DELETE FROM _bf_supplymatch_current;",
            NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "activation: cannot begin projection: %s\n", sqlite3_errmsg(db));
        goto rollback;
    }
    if (sqlite3_prepare_v2(
            db,
            "INSERT OR IGNORE INTO _bf_supplymatch_current(resource_id) VALUES(?)",
            -1, &mark, NULL) != SQLITE_OK) {
        fprintf(stderr, "activation: cannot prepare current-set marker: %s\n", sqlite3_errmsg(db));
        goto rollback;
    }

    while ((line_len = getline(&line, &line_cap, in)) >= 0) {
        char schema[96], axis[64], state[64], owner[96], from_owner[96], to_owner[96];
        char occurrence[512], evidence[96], basis[96], detail[320];
        (void)line_len;
        read_rows++;
        receipt_field(line, "schema", schema, sizeof(schema));
        receipt_field(line, "axis", axis, sizeof(axis));
        receipt_field(line, "state", state, sizeof(state));
        receipt_field(line, "owner", owner, sizeof(owner));
        receipt_field(line, "from_owner", from_owner, sizeof(from_owner));
        receipt_field(line, "to_owner", to_owner, sizeof(to_owner));
        if (strcmp(schema, "bonfyre-owner-participation.v1") != 0 ||
            strcmp(axis, "capability") != 0 ||
            strcmp(state, "eligible") != 0 ||
            strcmp(owner, "SupplyMatch") != 0 ||
            strcmp(from_owner, "DemandBook") != 0 ||
            strcmp(to_owner, "SupplyMatch") != 0) {
            continue;
        }
        receipt_field(line, "occurrence_id", occurrence, sizeof(occurrence));
        receipt_field(line, "evidence_sha256", evidence, sizeof(evidence));
        receipt_field(line, "basis_sha256", basis, sizeof(basis));
        if (!occurrence[0] || !evidence[0]) {
            fprintf(stderr, "activation: eligible SupplyMatch row lacks identity/evidence\n");
            goto rollback;
        }

        sqlite3_reset(mark);
        sqlite3_clear_bindings(mark);
        sqlite3_bind_text(mark, 1, occurrence, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(mark) != SQLITE_DONE) {
            fprintf(stderr, "activation: cannot mark %s current: %s\n",
                    occurrence, sqlite3_errmsg(db));
            goto rollback;
        }
        if (sqlite3_changes(db) == 0) continue; /* duplicate evidence row */

        snprintf(detail, sizeof(detail),
                 "DemandBook/SupplyMatch:evidence=%s;basis=%s", evidence, basis);
        if (bf_activation_project_eligibility(act, occurrence, 1, detail, NULL) != 0) {
            fprintf(stderr, "activation: eligibility projection failed for %s\n", occurrence);
            goto rollback;
        }
        if (bf_activation_state(act, occurrence, 0, 0) != BF_ACT_ELIGIBLE) {
            fprintf(stderr, "activation: projected %s rose above/below eligible without authority\n",
                    occurrence);
            goto rollback;
        }
        eligible_rows++;
        projected++;
    }
    sqlite3_finalize(mark);
    mark = NULL;

    /*
     * DemandBook is a full current projection. Retract only rows this exact
     * bridge previously owned; unrelated ResourceActivation candidates are
     * untouched. The native projection API preserves activate_actor/mechanism.
     */
    if (sqlite3_prepare_v2(
            db,
            "SELECT resource_id FROM resource_candidates "
            "WHERE detail LIKE 'DemandBook/SupplyMatch:%' "
            "AND resource_id NOT IN (SELECT resource_id FROM _bf_supplymatch_current)",
            -1, &stale, NULL) != SQLITE_OK) {
        fprintf(stderr, "activation: cannot query stale SupplyMatch rows: %s\n",
                sqlite3_errmsg(db));
        goto rollback;
    }
    while (sqlite3_step(stale) == SQLITE_ROW) {
        const char *rid = (const char *)sqlite3_column_text(stale, 0);
        char *copy;
        if (!rid) continue;
        copy = strdup(rid);
        if (!copy) goto rollback;
        if (retract_n == retract_cap) {
            size_t next = retract_cap ? retract_cap * 2 : 32;
            char **grown = realloc(retract_ids, next * sizeof(*grown));
            if (!grown) { free(copy); goto rollback; }
            retract_ids = grown;
            retract_cap = next;
        }
        retract_ids[retract_n++] = copy;
    }
    sqlite3_finalize(stale);
    stale = NULL;

    for (size_t i = 0; i < retract_n; i++) {
        if (bf_activation_project_eligibility(
                act, retract_ids[i], 0, "DemandBook/SupplyMatch:retracted", NULL) != 0) {
            fprintf(stderr, "activation: eligibility retraction failed for %s\n", retract_ids[i]);
            goto rollback;
        }
        if (bf_activation_state(act, retract_ids[i], 0, 0) != BF_ACT_QUALIFIED) {
            fprintf(stderr, "activation: retracted %s did not fall to qualified\n", retract_ids[i]);
            goto rollback;
        }
        retracted++;
    }

    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(stderr, "activation: commit failed: %s\n", sqlite3_errmsg(db));
        goto rollback_after_commit;
    }
    printf("{\"schema\":\"bonfyre-activation-projection.v1\","
           "\"source\":\"DemandBook/SupplyMatch\","
           "\"evidence_rows_read\":%d,\"eligible\":%d,\"projected\":%d,"
           "\"retracted\":%d,\"authority_actor_written\":0,"
           "\"authority_inferred\":false,\"activated_inferred\":false,"
           "\"cannot_infer\":\"eligible -> authorized or activated; "
           "a payment carrier declaration -> spend approval\"}\n",
           read_rows, eligible_rows, projected, retracted);
    rc = 0;
    goto done;

rollback:
    sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    goto done;
rollback_after_commit:
    /* COMMIT already failed; make a best-effort rollback of the open txn. */
    sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);

done:
    if (mark) sqlite3_finalize(mark);
    if (stale) sqlite3_finalize(stale);
    if (act) bf_activation_close(act);
    if (db) sqlite3_close(db);
    if (in) fclose(in);
    free(line);
    for (size_t i = 0; i < retract_n; i++) free(retract_ids[i]);
    free(retract_ids);
    return rc;
}

static int activation_open_owners(const char *db_path, sqlite3 **database,
                                  BfActivation **activation, BfAuthority **authority,
                                  BfServiceLifecycle **lifecycle) {
    if (sqlite3_open(db_path, database) != SQLITE_OK) return -1;
    sqlite3_busy_timeout(*database, 5000);
    if (bf_activation_open_database(*database, activation, stderr) ||
        bf_authority_open_database(*database, authority, stderr) ||
        bf_service_lifecycle_open_database(*database, lifecycle, stderr)) {
        if (*lifecycle) bf_service_lifecycle_close(*lifecycle);
        if (*authority) bf_authority_close(*authority);
        if (*activation) bf_activation_close(*activation);
        sqlite3_close(*database);
        *database = NULL;
        return -1;
    }
    return 0;
}

static void activation_close_owners(sqlite3 *database, BfActivation *activation,
                                    BfAuthority *authority, BfServiceLifecycle *lifecycle) {
    bf_service_lifecycle_close(lifecycle);
    bf_authority_close(authority);
    bf_activation_close(activation);
    sqlite3_close(database);
}

static int activation_select(const char *resource, const char *actor, const char *mechanism,
                             const char *evidence, const char *db_path) {
    sqlite3 *database = NULL; BfActivation *activation = NULL; BfAuthority *authority = NULL;
    BfServiceLifecycle *lifecycle = NULL; int rc = 1;
    if (activation_open_owners(db_path, &database, &activation, &authority, &lifecycle)) goto done;
    if (bf_activation_select_actor(activation, resource, actor, mechanism,
                                   evidence ? evidence : "", NULL)) goto done;
    printf("{\"schema\":\"bonfyre-activation-selection.v1\",\"resource\":");
    json_string(stdout, resource); printf(",\"actor\":"); json_string(stdout, actor);
    printf(",\"authority_inferred\":false,\"activated_inferred\":false,"
           "\"cannot_infer\":\"selection -> authority, binding, or activation\"}\n");
    rc = 0;
done:
    if (database) activation_close_owners(database, activation, authority, lifecycle);
    return rc;
}

static int activation_bind(const char *resource, const char *service, const char *actor,
                           const char *evidence, const char *db_path) {
    sqlite3 *database = NULL; BfActivation *activation = NULL; BfAuthority *authority = NULL;
    BfServiceLifecycle *lifecycle = NULL; BfServiceBinding binding; int bound, rc = 1;
    if (activation_open_owners(db_path, &database, &activation, &authority, &lifecycle)) goto done;
    memset(&binding, 0, sizeof(binding));
    binding.resource_id = resource; binding.service_id = service; binding.actor = actor; binding.evidence = evidence;
    bound = bf_service_lifecycle_bind(lifecycle, authority, &binding, NULL);
    if (bound < 0) goto done;
    printf("{\"schema\":\"bonfyre-service-binding.v1\",\"resource\":"); json_string(stdout, resource);
    printf(",\"service\":"); json_string(stdout, service); printf(",\"actor\":"); json_string(stdout, actor);
    printf(",\"bound\":%s,\"authority_inferred\":false,"
           "\"cannot_infer\":\"binding -> activate authority or activation\"}\n", bound ? "true" : "false");
    rc = bound ? 0 : 3;
done:
    if (database) activation_close_owners(database, activation, authority, lifecycle);
    return rc;
}

static int activation_reconcile(const char *only_resource, const char *db_path) {
    sqlite3 *database = NULL; BfActivation *activation = NULL; BfAuthority *authority = NULL;
    BfServiceLifecycle *lifecycle = NULL; sqlite3_stmt *statement = NULL;
    int total = 0, eligible = 0, authorized = 0, activated = 0, rc = 1;
    if (activation_open_owners(db_path, &database, &activation, &authority, &lifecycle)) goto done;
    if (sqlite3_prepare_v2(database,
            only_resource ? "SELECT resource_id FROM resource_candidates WHERE resource_id=?" :
                            "SELECT resource_id FROM resource_candidates WHERE detail LIKE 'DemandBook/SupplyMatch:%'",
            -1, &statement, NULL) != SQLITE_OK) goto done;
    if (only_resource) sqlite3_bind_text(statement, 1, only_resource, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(statement) == SQLITE_ROW) {
        const char *resource = (const char *)sqlite3_column_text(statement, 0);
        BfActivationState state;
        if (!resource) continue;
        state = bf_activation_reconcile(activation, authority, lifecycle, resource, NULL);
        total++;
        if (state == BF_ACT_ELIGIBLE) eligible++;
        else if (state == BF_ACT_AUTHORIZED) authorized++;
        else if (state == BF_ACT_ACTIVATED) activated++;
    }
    printf("{\"schema\":\"bonfyre-activation-reconciliation.v1\",\"candidates\":%d,"
           "\"eligible\":%d,\"authorized\":%d,\"activated\":%d,"
           "\"writes\":false,\"cannot_infer\":\"eligible -> authorized or activated\"}\n",
           total, eligible, authorized, activated);
    rc = 0;
done:
    if (statement) sqlite3_finalize(statement);
    if (database) activation_close_owners(database, activation, authority, lifecycle);
    return rc;
}

static int obs_cmp(const void *a, const void *b) {
    const ForeignObs *x = a, *y = b;
    int c = strcmp(x->url, y->url);
    if (c) return c;
    return strcmp(x->at, y->at);   /* ISO-8601 sorts lexically */
}

/* One receipt's text into one observation row. The text is whatever a loose
 * receipt file would have held; where it came from does not change its meaning. */
static int obs_from_text(const char *buf, ForeignObs *row) {
    char num[32];
    memset(row, 0, sizeof(*row));
    receipt_field(buf, "source_url", row->url, sizeof(row->url));
    receipt_field(buf, "observed_at", row->at, sizeof(row->at));
    receipt_field(buf, "content_sha256", row->digest, sizeof(row->digest));
    receipt_field(buf, "observation_kind", row->kind, sizeof(row->kind));
    receipt_field(buf, "policy_decision", row->policy, sizeof(row->policy));
    receipt_field(buf, "body_path", row->body_path, sizeof(row->body_path));
    receipt_field(buf, "observe_right", row->observe_right, sizeof(row->observe_right));
    receipt_field(buf, "fetch_attempted", row->fetch_attempted, sizeof(row->fetch_attempted));
    receipt_field(buf, "robots_transport_exit", num, sizeof(num));
    row->robots_exit = atoi(num);
    receipt_field(buf, "robots_http_status", num, sizeof(num));
    row->robots_status = atoi(num);
    receipt_field(buf, "http_status", num, sizeof(num));
    row->http_status = atoi(num);
    row->retained = !strcmp(row->kind, "source_observed") &&
                    row->digest[0] && strcmp(row->digest, "null") != 0;
    return row->url[0] != '\0';
}

/* ---- the receipt pack ---------------------------------------------------
 *
 * A receipt is a kilobyte or two of JSON, and a hundred thousand of them as
 * files cost a filesystem block each: 136k receipts were 251 MB of text
 * occupying 607 MB, opened one by one on every question. They are also nearly
 * the same document over and over, which a dictionary trained on them knows.
 *
 * The pack is one SQLite file beside the loose receipts: each receipt stored
 * whole under its file name, compressed against that dictionary. A reader
 * takes the pack first and then the loose files, and a loose file with a
 * packed name wins, so an observation written after packing is never hidden
 * by an older packed copy. Packing changes where receipts live, not what any
 * of them says, and `pack-verify` proves that byte for byte. */
#ifdef BONFYRE_HAVE_ZSTD
#include <zstd.h>
#include <zdict.h>

static const char *receipt_pack_path(const char *dir, char *out, size_t size) {
    const char *named = getenv("BONFYRE_FOREIGN_RECEIPT_PACK");
    if (named && *named) return named;
    return snprintf(out, size, "%s/.receipts.pack", dir) < (int)size ? out : NULL;
}

/* The largest single file a pack takes. A body past this is left loose and
 * counted, not truncated and not a reason to abandon the pack. */
#define PACK_MEMBER_MAX (256L << 20)

static char *slurp_file(const char *path, size_t *len) {
    FILE *f = fopen(path, "rb");
    char *text = NULL;
    long size;
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) == 0 && (size = ftell(f)) >= 0 && size < PACK_MEMBER_MAX &&
        fseek(f, 0, SEEK_SET) == 0 && (text = malloc((size_t)size + 1))) {
        if (fread(text, 1, (size_t)size, f) != (size_t)size) { free(text); text = NULL; }
        else { text[size] = '\0'; *len = (size_t)size; }
    }
    fclose(f);
    return text;
}

/* Every packed receipt, decompressed, handed to `each` with its name. */
static int receipt_pack_each(const char *pack, int (*each)(void *, const char *, const char *, size_t), void *ctx) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    ZSTD_DDict *dict = NULL;
    ZSTD_DCtx *dctx = NULL;
    char *out = NULL;
    size_t cap = 0;
    int rc = -1;
    if (access(pack, R_OK) != 0) return 0;
    if (sqlite3_open_v2(pack, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) goto done;
    if (sqlite3_prepare_v2(db, "SELECT value FROM meta WHERE key='dictionary'", -1, &st, NULL) != SQLITE_OK) goto done;
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_bytes(st, 0) > 0)
        dict = ZSTD_createDDict(sqlite3_column_blob(st, 0), (size_t)sqlite3_column_bytes(st, 0));
    sqlite3_finalize(st); st = NULL;
    if (!(dctx = ZSTD_createDCtx())) goto done;
    if (sqlite3_prepare_v2(db, "SELECT name, bytes, z FROM receipt", -1, &st, NULL) != SQLITE_OK) goto done;
    rc = 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        size_t bytes = (size_t)sqlite3_column_int64(st, 1), got;
        if (bytes + 1 > cap) { char *bigger = realloc(out, bytes + 1); if (!bigger) { rc = -1; break; } out = bigger; cap = bytes + 1; }
        got = dict ? ZSTD_decompress_usingDDict(dctx, out, bytes, sqlite3_column_blob(st, 2), (size_t)sqlite3_column_bytes(st, 2), dict)
                   : ZSTD_decompressDCtx(dctx, out, bytes, sqlite3_column_blob(st, 2), (size_t)sqlite3_column_bytes(st, 2));
        /* A row that does not come back at its recorded length is damage, and
         * damage is reported rather than read around. */
        if (ZSTD_isError(got) || got != bytes) { rc = -1; break; }
        out[bytes] = '\0';
        if (each(ctx, (const char *)sqlite3_column_text(st, 0), out, bytes) != 0) { rc = -1; break; }
        rc++;
    }
done:
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    if (dict) ZSTD_freeDDict(dict);
    if (dctx) ZSTD_freeDCtx(dctx);
    free(out);
    return rc;
}

static int name_order(const void *a, const void *b) { return strcmp(*(char *const *)a, *(char *const *)b); }

/* One receipt by its file name: the loose file when there is one, else the
 * packed copy. This is the read every script that opened a receipt file can
 * use once the files are gone. */
typedef struct { const char *name; int found; } ReceiptWanted;
static int receipt_print_if_named(void *ctx, const char *name, const char *text, size_t len) {
    ReceiptWanted *want = ctx;
    if (!want->found && !strcmp(name, want->name)) { fwrite(text, 1, len, stdout); want->found = 1; }
    return 0;
}
static int foreign_receipt(const char *dir, const char *name) {
    char path[PATH_MAX], pack_buf[PATH_MAX], *text;
    size_t len = 0;
    const char *pack;
    ReceiptWanted want = { name, 0 };
    if (strchr(name, '/') || name[0] == '.') { fprintf(stderr, "a receipt is named by its file name alone\n"); return 2; }
    if (snprintf(path, sizeof(path), "%s/%s", dir, name) < (int)sizeof(path) && (text = slurp_file(path, &len))) {
        fwrite(text, 1, len, stdout); free(text); return 0;
    }
    pack = receipt_pack_path(dir, pack_buf, sizeof(pack_buf));
    if (pack && receipt_pack_each(pack, receipt_print_if_named, &want) < 0) { fprintf(stderr, "the receipt pack at %s is unreadable or damaged\n", pack); return 3; }
    if (!want.found) { fprintf(stderr, "no receipt named %s\n", name); return 2; }
    return 0;
}

/* Pack every loose receipt in `dir` into `pack`. The directory is only read. */
static int foreign_pack(const char *dir, const char *pack) {
    DIR *d = opendir(dir);
    struct dirent *e;
    char **names = NULL, *samples = NULL, tmp[PATH_MAX];
    size_t n = 0, cap = 0, *sizes = NULL, sample_bytes = 0, sample_count = 0, raw = 0, packed = 0, dict_len = 0;
    void *dict = NULL;
    sqlite3 *db = NULL;
    sqlite3_stmt *ins = NULL;
    ZSTD_CCtx *cctx = NULL;
    ZSTD_CDict *cdict = NULL;
    int rc = 1;
    if (!d) { fprintf(stderr, "cannot read receipts from %s\n", dir); return 1; }
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        if (n == cap) { size_t grow = cap ? cap * 2 : 1024; char **bigger = realloc(names, grow * sizeof(*names)); if (!bigger) goto done; names = bigger; cap = grow; }
        if (!(names[n] = strdup(e->d_name))) goto done;
        n++;
    }
    closedir(d); d = NULL;
    /* Sorted, so the same receipts always make the same pack. */
    if (n) qsort(names, n, sizeof(*names), name_order);
    /* The dictionary learns from an even spread of them, a few kilobytes each. */
    {
        size_t step = n > 20000 ? n / 20000 : 1, budget = 48u << 20;
        samples = malloc(budget); sizes = malloc((n / step + 1) * sizeof(*sizes));
        if (!samples || !sizes) goto done;
        for (size_t i = 0; i < n; i += step) {
            char path[PATH_MAX], *text; size_t len = 0, take;
            if (snprintf(path, sizeof(path), "%s/%s", dir, names[i]) >= (int)sizeof(path) || !(text = slurp_file(path, &len))) continue;
            take = len > 8192 ? 8192 : len;
            if (take && sample_bytes + take <= budget) { memcpy(samples + sample_bytes, text, take); sizes[sample_count++] = take; sample_bytes += take; }
            free(text);
        }
        if (sample_count >= 8) {
            /* A dictionary is paid for once per pack, so it is sized to what
             * it has to learn from: a small estate gets a small one. */
            size_t room = sample_bytes / 16;
            if (room < 4096) room = 4096;
            if (room > 112 * 1024) room = 112 * 1024;
            dict = malloc(room);
            if (!dict) goto done;
            dict_len = ZDICT_trainFromBuffer(dict, room, samples, sizes, (unsigned)sample_count);
            if (ZDICT_isError(dict_len)) { dict_len = 0; }
        }
    }
    if (snprintf(tmp, sizeof(tmp), "%s.packing", pack) >= (int)sizeof(tmp)) goto done;
    unlink(tmp);
    if (sqlite3_open(tmp, &db) != SQLITE_OK) goto done;
    if (sqlite3_exec(db, "PRAGMA page_size=8192; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;"
                         "CREATE TABLE meta(key TEXT PRIMARY KEY, value BLOB);"
                         "CREATE TABLE receipt(name TEXT PRIMARY KEY, bytes INTEGER NOT NULL, z BLOB NOT NULL) WITHOUT ROWID;"
                         "BEGIN", NULL, NULL, NULL) != SQLITE_OK) goto done;
    if (sqlite3_prepare_v2(db, "INSERT INTO meta VALUES('dictionary', ?1)", -1, &ins, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_blob(ins, 1, dict_len ? dict : "", (int)dict_len, SQLITE_STATIC);
    if (sqlite3_step(ins) != SQLITE_DONE) goto done;
    sqlite3_finalize(ins); ins = NULL;
    if (!(cctx = ZSTD_createCCtx())) goto done;
    if (dict_len && !(cdict = ZSTD_createCDict(dict, dict_len, 19))) goto done;
    /* Each receipt carries a checksum of what it should decompress to, so a
     * damaged row is refused rather than returned as some other text. */
    ZSTD_CCtx_setParameter(cctx, ZSTD_c_checksumFlag, 1);
    if (cdict) ZSTD_CCtx_refCDict(cctx, cdict);
    else ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, 19);
    if (sqlite3_prepare_v2(db, "INSERT INTO receipt VALUES(?1, ?2, ?3)", -1, &ins, NULL) != SQLITE_OK) goto done;
    for (size_t i = 0; i < n; i++) {
        char path[PATH_MAX], *text, *z; size_t len = 0, bound, zlen;
        if (snprintf(path, sizeof(path), "%s/%s", dir, names[i]) >= (int)sizeof(path)) goto done;
        if (!(text = slurp_file(path, &len))) { fprintf(stderr, "cannot read %s\n", path); goto done; }
        bound = ZSTD_compressBound(len);
        if (!(z = malloc(bound))) { free(text); goto done; }
        zlen = ZSTD_compress2(cctx, z, bound, text, len);
        if (ZSTD_isError(zlen)) { free(text); free(z); goto done; }
        sqlite3_reset(ins);
        sqlite3_bind_text(ins, 1, names[i], -1, SQLITE_STATIC);
        sqlite3_bind_int64(ins, 2, (sqlite3_int64)len);
        sqlite3_bind_blob(ins, 3, z, (int)zlen, SQLITE_TRANSIENT);
        if (sqlite3_step(ins) != SQLITE_DONE) { free(text); free(z); goto done; }
        raw += len; packed += zlen;
        free(text); free(z);
    }
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) goto done;
    sqlite3_finalize(ins); ins = NULL;
    sqlite3_close(db); db = NULL;
    /* Whole or absent: the pack takes its name only once it is complete. */
    if (rename(tmp, pack) != 0) goto done;
    {
        struct stat st;
        long long on_disk = stat(pack, &st) == 0 ? (long long)st.st_size : 0;
        printf("{\"schema\":\"bonfyre-foreign-receipt-pack.v1\",\"receipts\":%zu,\"raw_bytes\":%zu,\"compressed_bytes\":%zu,"
               "\"dictionary_bytes\":%zu,\"pack_bytes\":%lld,\"loose_files_changed\":false,"
               "\"cannot_infer\":\"a packed receipt -> a verified observation; the pack moves receipts, it does not check them\"}\n",
               n, raw, packed, dict_len, on_disk);
    }
    rc = 0;
done:
    if (rc != 0) { fprintf(stderr, "receipt pack was not written\n"); unlink(tmp); }
    if (d) closedir(d);
    if (ins) sqlite3_finalize(ins);
    if (db) sqlite3_close(db);
    if (cdict) ZSTD_freeCDict(cdict);
    if (cctx) ZSTD_freeCCtx(cctx);
    for (size_t i = 0; i < n; i++) free(names[i]);
    free(names); free(samples); free(sizes); free(dict);
    return rc;
}

typedef struct { const char *dir; size_t same, differ, loose_missing; } PackVerify;

static int pack_verify_each(void *ctx, const char *name, const char *text, size_t len) {
    PackVerify *v = ctx;
    char path[PATH_MAX], *loose;
    size_t loose_len = 0;
    if (snprintf(path, sizeof(path), "%s/%s", v->dir, name) >= (int)sizeof(path)) return 1;
    loose = slurp_file(path, &loose_len);
    if (!loose) v->loose_missing++;
    else if (loose_len == len && memcmp(loose, text, len) == 0) v->same++;
    else v->differ++;
    free(loose);
    return 0;
}

/* Retire the loose files the pack already holds exactly. A receipt file is
 * appended to when its address is observed again, so a loose file is removed
 * only if, at this moment, its bytes are the packed bytes; one that has grown
 * since packing is left, and stays the newer word a reader takes first. */
typedef struct { const char *dir; size_t removed, kept_newer, absent, bytes; } PackRetire;
static int pack_retire_each(void *ctx, const char *name, const char *text, size_t len) {
    PackRetire *r = ctx;
    char path[PATH_MAX], *loose;
    size_t loose_len = 0;
    if (snprintf(path, sizeof(path), "%s/%s", r->dir, name) >= (int)sizeof(path)) return 0;
    loose = slurp_file(path, &loose_len);
    if (!loose) { r->absent++; return 0; }
    if (loose_len == len && memcmp(loose, text, len) == 0) {
        if (unlink(path) == 0) { r->removed++; r->bytes += len; }
    } else r->kept_newer++;
    free(loose);
    return 0;
}
static int foreign_pack_retire(const char *dir, const char *pack) {
    PackRetire r = { dir, 0, 0, 0, 0 };
    int rows = receipt_pack_each(pack, pack_retire_each, &r);
    if (rows < 0) { fprintf(stderr, "the pack at %s is unreadable or damaged; nothing was removed past the damage\n", pack); return 1; }
    printf("{\"schema\":\"bonfyre-foreign-receipt-pack-retire.v1\",\"packed\":%d,\"loose_removed\":%zu,\"removed_bytes\":%zu,"
           "\"kept_because_changed_since_packing\":%zu,\"already_absent\":%zu}\n", rows, r.removed, r.bytes, r.kept_newer, r.absent);
    return 0;
}

/* Every packed receipt against the loose file of its name, byte for byte. */
static int foreign_pack_verify(const char *dir, const char *pack) {
    PackVerify v = { dir, 0, 0, 0 };
    DIR *d;
    struct dirent *e;
    size_t loose = 0;
    int rows = receipt_pack_each(pack, pack_verify_each, &v);
    if (rows < 0) { fprintf(stderr, "the pack at %s is unreadable or damaged\n", pack); return 1; }
    if ((d = opendir(dir))) { while ((e = readdir(d))) if (e->d_name[0] != '.') loose++; closedir(d); }
    printf("{\"schema\":\"bonfyre-foreign-receipt-pack-verify.v1\",\"packed\":%d,\"loose\":%zu,\"identical\":%zu,"
           "\"different\":%zu,\"packed_without_a_loose_file\":%zu,\"loose_not_packed\":%zu}\n",
           rows, loose, v.same, v.differ, v.loose_missing, loose > v.same + v.differ ? loose - v.same - v.differ : 0);
    return v.differ == 0 ? 0 : 3;
}

/* ---- the body pack --------------------------------------------------------
 *
 * Retained bodies are whole pages and feeds: large, unlike one another, and
 * sometimes the very same bytes kept under two names. The pack stores each
 * distinct content once, compressed and checksummed, under its digest, and
 * each name points at a digest. It is written beside the bodies and changes
 * none of them; `body` reads one back out and `pack-bodies-verify` proves
 * every name returns its file's bytes. */
static int foreign_pack_bodies(const char *dir, const char *pack) {
    DIR *d = opendir(dir);
    struct dirent *e;
    char **names = NULL, tmp[PATH_MAX];
    size_t n = 0, cap = 0, raw = 0, packed = 0, distinct = 0, left_loose = 0, left_loose_bytes = 0;
    sqlite3 *db = NULL;
    sqlite3_stmt *name_ins = NULL, *blob_ins = NULL, *have = NULL;
    ZSTD_CCtx *cctx = NULL;
    int rc = 1;
    if (!d) { fprintf(stderr, "cannot read bodies from %s\n", dir); return 1; }
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        if (n == cap) { size_t grow = cap ? cap * 2 : 1024; char **bigger = realloc(names, grow * sizeof(*names)); if (!bigger) goto done; names = bigger; cap = grow; }
        if (!(names[n] = strdup(e->d_name))) goto done;
        n++;
    }
    closedir(d); d = NULL;
    if (n) qsort(names, n, sizeof(*names), name_order);
    if (snprintf(tmp, sizeof(tmp), "%s.packing", pack) >= (int)sizeof(tmp)) goto done;
    unlink(tmp);
    if (sqlite3_open(tmp, &db) != SQLITE_OK) goto done;
    if (sqlite3_exec(db, "PRAGMA page_size=65536; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;"
                         "CREATE TABLE body(name TEXT PRIMARY KEY, sha256 TEXT NOT NULL) WITHOUT ROWID;"
                         "CREATE TABLE blob(sha256 TEXT PRIMARY KEY, bytes INTEGER NOT NULL, z BLOB NOT NULL);"
                         "BEGIN", NULL, NULL, NULL) != SQLITE_OK) goto done;
    if (sqlite3_prepare_v2(db, "INSERT INTO body VALUES(?1, ?2)", -1, &name_ins, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "INSERT INTO blob VALUES(?1, ?2, ?3)", -1, &blob_ins, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "SELECT 1 FROM blob WHERE sha256=?1", -1, &have, NULL) != SQLITE_OK) goto done;
    if (!(cctx = ZSTD_createCCtx())) goto done;
    ZSTD_CCtx_setParameter(cctx, ZSTD_c_checksumFlag, 1);
    ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, 19);
    for (size_t i = 0; i < n; i++) {
        char path[PATH_MAX], digest[65], *text;
        size_t len = 0;
        struct stat st;
        if (snprintf(path, sizeof(path), "%s/%s", dir, names[i]) >= (int)sizeof(path)) goto done;
        if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) continue;
        if (st.st_size >= PACK_MEMBER_MAX) { left_loose++; left_loose_bytes += (size_t)st.st_size; continue; }
        if (!(text = slurp_file(path, &len))) { fprintf(stderr, "cannot read %s\n", path); goto done; }
        bf_sha256_hex((const uint8_t *)text, len, digest);
        sqlite3_reset(have);
        sqlite3_bind_text(have, 1, digest, -1, SQLITE_STATIC);
        if (sqlite3_step(have) != SQLITE_ROW) {
            size_t bound = ZSTD_compressBound(len), zlen;
            char *z = malloc(bound);
            if (!z) { free(text); goto done; }
            zlen = ZSTD_compress2(cctx, z, bound, text, len);
            if (ZSTD_isError(zlen)) { free(text); free(z); goto done; }
            sqlite3_reset(blob_ins);
            sqlite3_bind_text(blob_ins, 1, digest, -1, SQLITE_STATIC);
            sqlite3_bind_int64(blob_ins, 2, (sqlite3_int64)len);
            sqlite3_bind_blob(blob_ins, 3, z, (int)zlen, SQLITE_TRANSIENT);
            if (sqlite3_step(blob_ins) != SQLITE_DONE) { free(text); free(z); goto done; }
            packed += zlen; distinct++;
            free(z);
        }
        sqlite3_reset(name_ins);
        sqlite3_bind_text(name_ins, 1, names[i], -1, SQLITE_STATIC);
        sqlite3_bind_text(name_ins, 2, digest, -1, SQLITE_STATIC);
        if (sqlite3_step(name_ins) != SQLITE_DONE) { free(text); goto done; }
        raw += len;
        free(text);
    }
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) goto done;
    sqlite3_finalize(name_ins); sqlite3_finalize(blob_ins); sqlite3_finalize(have); name_ins = blob_ins = have = NULL;
    sqlite3_close(db); db = NULL;
    if (rename(tmp, pack) != 0) goto done;
    {
        struct stat st;
        printf("{\"schema\":\"bonfyre-foreign-body-pack.v1\",\"bodies\":%zu,\"distinct_contents\":%zu,\"raw_bytes\":%zu,"
               "\"compressed_bytes\":%zu,\"pack_bytes\":%lld,\"too_large_left_loose\":%zu,\"left_loose_bytes\":%zu,\"loose_files_changed\":false,"
               "\"cannot_infer\":\"a packed body -> the page as it is now; it is what was retained when it was read\"}\n",
               n - left_loose, distinct, raw, packed, stat(pack, &st) == 0 ? (long long)st.st_size : 0LL, left_loose, left_loose_bytes);
    }
    rc = 0;
done:
    if (rc != 0) { fprintf(stderr, "body pack was not written\n"); unlink(tmp); }
    if (d) closedir(d);
    if (name_ins) sqlite3_finalize(name_ins);
    if (blob_ins) sqlite3_finalize(blob_ins);
    if (have) sqlite3_finalize(have);
    if (db) sqlite3_close(db);
    if (cctx) ZSTD_freeCCtx(cctx);
    for (size_t i = 0; i < n; i++) free(names[i]);
    free(names);
    return rc;
}

/* One body out of the pack, checked against the digest it is filed under.
 * Returns malloc'd bytes, or NULL when the name is absent or the row is damaged. */
static char *body_pack_read(sqlite3 *db, ZSTD_DCtx *dctx, const char *name, size_t *len, int *damaged) {
    sqlite3_stmt *st = NULL;
    char *out = NULL, digest[65];
    *damaged = 0;
    if (sqlite3_prepare_v2(db, "SELECT b.bytes, b.z, b.sha256 FROM body n JOIN blob b ON b.sha256 = n.sha256 WHERE n.name = ?1", -1, &st, NULL) != SQLITE_OK) return NULL;
    sqlite3_bind_text(st, 1, name, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        size_t bytes = (size_t)sqlite3_column_int64(st, 0), got;
        if ((out = malloc(bytes + 1))) {
            got = ZSTD_decompressDCtx(dctx, out, bytes, sqlite3_column_blob(st, 1), (size_t)sqlite3_column_bytes(st, 1));
            if (ZSTD_isError(got) || got != bytes) { free(out); out = NULL; *damaged = 1; }
            else {
                bf_sha256_hex((const uint8_t *)out, bytes, digest);
                if (strcmp(digest, (const char *)sqlite3_column_text(st, 2)) != 0) { free(out); out = NULL; *damaged = 1; }
                else *len = bytes;
            }
        }
    }
    sqlite3_finalize(st);
    return out;
}

static int foreign_body(const char *pack, const char *name) {
    sqlite3 *db = NULL;
    ZSTD_DCtx *dctx = ZSTD_createDCtx();
    size_t len = 0;
    int damaged = 0, rc = 1;
    char *body;
    if (!dctx || sqlite3_open_v2(pack, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { fprintf(stderr, "cannot open the body pack at %s\n", pack); goto done; }
    body = body_pack_read(db, dctx, name, &len, &damaged);
    if (!body) { fprintf(stderr, damaged ? "the packed body %s is damaged\n" : "no packed body named %s\n", name); rc = damaged ? 3 : 2; goto done; }
    rc = fwrite(body, 1, len, stdout) == len ? 0 : 1;
    free(body);
done:
    if (db) sqlite3_close(db);
    if (dctx) ZSTD_freeDCtx(dctx);
    return rc;
}

static int foreign_pack_bodies_verify(const char *dir, const char *pack) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    ZSTD_DCtx *dctx = ZSTD_createDCtx();
    size_t same = 0, differ = 0, missing = 0, damaged_rows = 0, loose = 0;
    DIR *d;
    struct dirent *e;
    if (!dctx || sqlite3_open_v2(pack, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "SELECT name FROM body", -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "the body pack at %s is unreadable\n", pack);
        if (st) sqlite3_finalize(st);
        if (db) sqlite3_close(db);
        if (dctx) ZSTD_freeDCtx(dctx);
        return 1;
    }
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(st, 0);
        char path[PATH_MAX], *packed_body, *loose_body;
        size_t plen = 0, llen = 0;
        int damaged = 0;
        packed_body = body_pack_read(db, dctx, name, &plen, &damaged);
        if (!packed_body) { damaged_rows++; continue; }
        if (snprintf(path, sizeof(path), "%s/%s", dir, name) >= (int)sizeof(path)) { free(packed_body); continue; }
        loose_body = slurp_file(path, &llen);
        if (!loose_body) missing++;
        else if (llen == plen && memcmp(loose_body, packed_body, plen) == 0) same++;
        else differ++;
        free(loose_body); free(packed_body);
    }
    sqlite3_finalize(st); sqlite3_close(db); ZSTD_freeDCtx(dctx);
    if ((d = opendir(dir))) { while ((e = readdir(d))) if (e->d_name[0] != '.') loose++; closedir(d); }
    printf("{\"schema\":\"bonfyre-foreign-body-pack-verify.v1\",\"loose\":%zu,\"identical\":%zu,\"different\":%zu,"
           "\"damaged\":%zu,\"packed_without_a_loose_file\":%zu,\"loose_not_packed\":%zu}\n",
           loose, same, differ, damaged_rows, missing, loose > same + differ ? loose - same - differ : 0);
    return (differ || damaged_rows) ? 3 : 0;
}

typedef struct { ForeignObs **rows; size_t *n, *cap; } PackLoad;

static int pack_load_each(void *ctx, const char *name, const char *text, size_t len) {
    PackLoad *l = ctx;
    char head[8192];
    size_t take = len < sizeof(head) - 1 ? len : sizeof(head) - 1;
    if (*l->n == *l->cap) {
        size_t grow = *l->cap ? *l->cap * 2 : 64;
        ForeignObs *bigger = realloc(*l->rows, grow * sizeof(**l->rows));
        if (!bigger) return 1;
        *l->rows = bigger; *l->cap = grow;
    }
    /* The same first eight kilobytes a loose file would have yielded. */
    memcpy(head, text, take); head[take] = '\0';
    if (obs_from_text(head, &(*l->rows)[*l->n])) {
        snprintf((*l->rows)[*l->n].receipt_name, sizeof((*l->rows)[*l->n].receipt_name), "%s", name);
        (*l->n)++;
    }
    return 0;
}
#endif

static int obs_name_cmp(const void *a, const void *b) {
    return strcmp(((const ForeignObs *)a)->receipt_name, ((const ForeignObs *)b)->receipt_name);
}

static int load_observations(const char *dir, ForeignObs **out) {
    DIR *d = opendir(dir);
    struct dirent *e;
    ForeignObs *rows = NULL;
    size_t n = 0, cap = 0, packed = 0;
#ifdef BONFYRE_HAVE_ZSTD
    {
        char pack_buf[PATH_MAX];
        const char *pack = receipt_pack_path(dir, pack_buf, sizeof(pack_buf));
        PackLoad load = { &rows, &n, &cap };
        if (pack && receipt_pack_each(pack, pack_load_each, &load) < 0) {
            fprintf(stderr, "the receipt pack at %s is unreadable or damaged\n", pack);
            free(rows); if (d) closedir(d);
            return -1;
        }
        packed = n;
        /* Packed rows are put in name order once, so finding whether a loose
         * file shadows one is a search rather than a walk of the whole pack:
         * an estate with a pack and its loose files both present is 136,000
         * of each, and a walk per file is eighteen billion comparisons. */
        if (packed) qsort(rows, packed, sizeof(*rows), obs_name_cmp);
    }
#endif
    if (!d) {
        /* A directory holding only a pack is still a readable estate. */
        if (!packed) return -1;
        if (rows) qsort(rows, n, sizeof(*rows), obs_cmp);
        *out = rows;
        return (int)n;
    }
    while ((e = readdir(d))) {
        char path[PATH_MAX], buf[8192];
        FILE *f;
        size_t got;
        ForeignObs row;
        if (e->d_name[0] == '.') continue;
        if (snprintf(path, sizeof(path), "%s/%s", dir, e->d_name) >= (int)sizeof(path)) continue;
        f = fopen(path, "rb");
        if (!f) continue;
        got = fread(buf, 1, sizeof(buf) - 1, f);
        fclose(f);
        buf[got] = '\0';
        if (!obs_from_text(buf, &row)) continue;
        snprintf(row.receipt_name, sizeof(row.receipt_name), "%s", e->d_name);
        /* A loose receipt with a packed name is the newer word on that receipt. */
        if (packed) {
            ForeignObs *hit = bsearch(&row, rows, packed, sizeof(*rows), obs_name_cmp);
            if (hit) { *hit = row; continue; }
        }
        if (n == cap) {
            size_t grow = cap ? cap * 2 : 64;
            ForeignObs *bigger = realloc(rows, grow * sizeof(*rows));
            if (!bigger) { free(rows); closedir(d); return -1; }
            rows = bigger; cap = grow;
        }
        rows[n++] = row;
    }
    closedir(d);
    if (rows) qsort(rows, n, sizeof(*rows), obs_cmp);
    *out = rows;
    return (int)n;
}

static const char *relation_between(const ForeignObs *prev, const ForeignObs *cur) {
    if (prev->retained && cur->retained)
        return strcmp(prev->digest, cur->digest) == 0 ? "confirms" : "supersedes";
    if (prev->retained && !cur->retained) return "qualifies";
    if (!prev->retained && cur->retained) return "restores";
    return strcmp(prev->digest, cur->digest) == 0 ? "confirms" : "supersedes";
}

static void print_obs(const ForeignObs *o, const char *relation, int first) {
    printf("%s{\"observed_at\":\"%s\",\"http_status\":%d,\"observation_kind\":\"%s\","
           "\"policy_decision\":\"%s\",\"content_sha256\":\"%s\",\"retained\":%s",
           first ? "" : ",", o->at, o->http_status, o->kind, o->policy,
           o->digest[0] ? o->digest : "", o->retained ? "true" : "false");
    if (relation) printf(",\"relation_to_previous\":\"%s\"", relation);
    printf("}");
}

/* Two subjects identified by the same retained bytes are one thing named twice.
 *
 * This happened here: a twin promoted by hand before the promotion path existed
 * took its id from a receipt filename, and the real path derives it from the
 * observed URL. Both were legitimate ids for the same digest, which is exactly
 * the duplicate-identity failure an attachment refuses to create -- reached
 * instead through an ad-hoc step outside the path that has the rule.
 *
 * Reported rather than merged: which of two identities is canonical is not this
 * function's call, and silently collapsing them would destroy whichever
 * references already point at the loser. */

/* A 200 is not an answer.
 *
 * Four dallasnews sitemap URLs -- /sitemap.xml, /news-sitemap.xml,
 * /projects/sitemap_projects.xml, /web-sitemap-index.xml -- each answered 200
 * with the same 3038 bytes of HTML carrying a Content-Security-Policy meta: a
 * bot wall, not a sitemap. All four passed admission law, because that law asks
 * whether the fetch was permitted and whether it completed, and both were true.
 * Nothing asked whether the bytes were the thing requested.
 *
 * So the estate held four twins it believed were observed, admissible and
 * retained, containing no evidence whatsoever about the resources named, and
 * every ceiling built on them was inflated by four.
 *
 * The test is deliberately narrow. Guessing that a body "looks like an error"
 * from its prose would misfire on any page that discusses errors; what is
 * checked instead is a contradiction between what was asked for and what came
 * back. A request for .xml or .json answered with an HTML document is not a
 * judgement about quality, it is a type mismatch, and the server has answered a
 * different question than the one asked.
 *
 * Cannot infer: a body that is not a boundary -> a body that is correct, or
 * current, or complete. This separates a non-answer from an answer, and says
 * nothing about the answer. */
static int foreign_body_is_boundary(const char *url, const char *body_path) {
    FILE *f;
    char head[512];
    size_t got;
    const char *dot, *q;
    char ext[16] = {0};
    size_t i = 0;

    if (!url || !body_path || !*body_path) return 0;

    /* The extension actually requested, ignoring any query. */
    q = strchr(url, '?');
    dot = NULL;
    for (const char *c = url; *c && (!q || c < q); c++)
        if (*c == '.') dot = c;
    if (!dot) return 0;
    for (const char *c = dot + 1; *c && (!q || c < q) && i + 1 < sizeof(ext); c++)
        ext[i++] = (char)tolower((unsigned char)*c);
    ext[i] = '\0';
    /* Only structured formats can contradict an HTML body. An .html or an
     * extensionless path answering with HTML is exactly what was asked for. */
    if (strcmp(ext, "xml") && strcmp(ext, "json") && strcmp(ext, "csv") &&
        strcmp(ext, "geojson") && strcmp(ext, "txt") && strcmp(ext, "rss"))
        return 0;

    f = fopen(body_path, "rb");
    if (!f) return 0;
    got = fread(head, 1, sizeof(head) - 1, f);
    fclose(f);
    head[got] = '\0';
    /* Leading whitespace and a BOM are not content. */
    i = 0;
    if (got >= 3 && (unsigned char)head[0] == 0xEF) i = 3;
    while (head[i] == ' ' || head[i] == '\n' || head[i] == '\r' || head[i] == '\t') i++;

    if (!strncasecmp(head + i, "<!doctype html", 14) ||
        !strncasecmp(head + i, "<html", 5))
        return 1;
    return 0;
}

/* Which Matrix the foreign verbs address.
 *
 * A declared BONFYRE_MATRIX_DB always wins. Without one, the store is the first
 * existing, non-empty matrix among: $BONFYRE_STATE_ROOT/matrix.db,
 * /var/lib/bonfyre/matrix.db, and the Mac estate's own
 * $HOME/.bonfyre/estate-local/matrix.db (the store the live web address is
 * started with). Hard-wiring /var/lib meant every promote on the Mac failed
 * with "cannot open matrix" while the estate's real Matrix sat one directory
 * away; an empty placeholder file is not a Matrix, so it is skipped rather
 * than promoted into. Nothing here creates a store: when no candidate exists
 * the /var/lib path is returned so the refusal still names where it looked. */
static const char *foreign_matrix_db(char *buf, size_t n) {
    const char *env = getenv("BONFYRE_MATRIX_DB"), *home;
    struct stat st;
    if (env && *env) return env;
    env = getenv("BONFYRE_STATE_ROOT");
    if (env && *env) {
        snprintf(buf, n, "%s/matrix.db", env);
        if (stat(buf, &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0) return buf;
    }
    if (stat("/var/lib/bonfyre/matrix.db", &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0)
        return "/var/lib/bonfyre/matrix.db";
    home = getenv("HOME");
    if (home && *home) {
        snprintf(buf, n, "%s/.bonfyre/estate-local/matrix.db", home);
        if (stat(buf, &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0) return buf;
    }
    return "/var/lib/bonfyre/matrix.db";
}

static int foreign_duplicate_identities(const char *matrix_db) {
    BfMatrix *m = NULL;
    BfMatrixDuplicateIdentity pairs[64];
    int n, i;
    if (bf_matrix_open(matrix_db, &m, stderr) || !m) return 2;
    n = bf_matrix_duplicate_identities(m, "ForeignTwin", pairs, 64);
    bf_matrix_close(m);
    if (n < 0) return 2;
    printf("{\"schema\":\"bonfyre.foreign.duplicate-identity.v1\",\"duplicates\":[");
    for (i = 0; i < n; i++)
        printf("%s{\"a\":\"%s\",\"b\":\"%s\",\"identity_digest\":\"%.16s\"}",
               i ? "," : "", pairs[i].a, pairs[i].b, pairs[i].identity_digest);
    printf("],\"count\":%d,\"note\":\"two twins sharing an identity digest were identified "
           "by the same retained bytes and are one thing named twice\"}\n", n);
    return 0;
}

/* The twin registry: what this institution has observed, and what became of it.
 *
 * Three facts about one external thing live in three places -- the receipts say
 * what was seen, the Matrix says whether it became a subject, and the relations
 * say whether anyone claimed it. Answering "what do we observe, and what is its
 * standing" meant reading all three by hand.
 *
 * Cannot infer: a listed twin -> a thing this institution has any right over.
 * Observation is not ownership, promotion is not a claim, and a claim is an
 * assertion. */
/* Admission law for a derived claim.
 *
 * A body digest proves what a parser read. It does not prove the read was
 * authorized under the policy in force, and those are different questions: a
 * receipt can hold perfectly good bytes that were fetched without a current
 * observe right, and a candidate derived from it would carry real content and
 * no permission.
 *
 * The rule was written for housing AvailabilityCandidates and lived in an
 * archived Python module, so nothing running could ask it. It is not
 * housing-shaped: any claim derived from an observation needs the same five
 * facts to be current, and now anything can ask.
 *
 * A 404 robots response is a successful preflight, not a failure -- the
 * observer treats it as an available policy with no deny rule. Reading it as a
 * failure would refuse admission for every source that simply has no
 * robots.txt.
 *
 * Cannot infer: an admissible receipt -> a true or useful claim. It says the
 * read that produced these bytes was authorized under the policy recorded at
 * the time, and nothing about what the bytes say. */
static const char *admission_failure(const ForeignObs *o) {
    if (strcmp(o->observe_right, "public")) return "missing_public_observe_right";
    if (strcmp(o->policy, "allow"))         return "missing_allow_policy_decision";
    if (strcmp(o->fetch_attempted, "true")) return "missing_fetch_attempt";
    if (o->robots_exit != 0)                return "robots_preflight_not_successful";
    if (o->robots_status != 200 && o->robots_status != 404)
        return "robots_preflight_not_successful";
    /* Everything above asks whether the read was permitted and whether it
     * completed. Both can be true of a bot wall that answered 200 with an HTML
     * page where a sitemap was requested -- and four such observations were
     * admissible here, supporting derived claims about documents they did not
     * contain. Permission is not an answer. */
    if (foreign_body_is_boundary(o->url, o->body_path))
        return "boundary_response_not_content";
    return NULL;
}

/* Hydration: what a twin is held as, and what that costs.
 *
 * Every observation currently retains its body, which is the MIRROR-LIKE regime
 * applied by default to everything -- including things observed once for a
 * single decision. Mirroring is justified when external access is unreliable,
 * repeated local analysis is valuable, and the licence permits retention. None
 * of those is true merely because a fetch succeeded.
 *
 * The four regimes differ in what they keep:
 *
 *   MIRROR-LIKE  substantial representations, retained
 *   INDEXED      sparse structured observations, hydrate deeper on demand
 *   REMOTE       identity, capability, schema, auth, receipts; query when needed
 *   EPHEMERAL    observe for one decision, keep the evidence, cool the body
 *
 * Selection is economic and semantic, so this reports both: which regime each
 * twin is held under, and how many bytes that decision is currently costing.
 * A twin with no authored policy is reported as unpoliced rather than assigned
 * a default -- an unmade decision is not the same as a decision to mirror, and
 * showing it as MIRROR-LIKE would hide that nobody chose.
 *
 * Cannot infer: a regime -> a licence. Retaining bytes because a regime permits
 * it does not make retention lawful. */
/* Does this twin's authored regime retain a body?
 *
 * Returns 1 to retain, 0 to cool, and 1 when no policy exists -- an unmade
 * decision must not silently start discarding evidence, so the safe direction
 * is to keep. Reads the CMS through sqlite because the policy is authored state
 * and this organ already links sqlite for the Matrix. */
/* The subject id a URL is registered under. One rule, used by promotion and by
 * every lookup keyed on a twin, because two ways of naming the same thing is
 * how a policy stops applying to the twin it was written for. */
static void foreign_subject_id(const char *url, char *out, size_t cap) {
    const char *p = strstr(url, "://");
    size_t w = 0;
    char slug[512];
    p = p ? p + 3 : url;
    for (; *p && w + 1 < sizeof(slug); p++) {
        if (isalnum((unsigned char)*p)) slug[w++] = (char)tolower((unsigned char)*p);
        else if (w && slug[w - 1] != '-') slug[w++] = '-';
    }
    while (w && slug[w - 1] == '-') w--;
    slug[w] = '\0';
    snprintf(out, cap, "foreign_twin:%.200s", slug);
}

static int foreign_regime_retains(const char *url) {
    const char *cms = getenv("BONFYRE_CMS_DB");
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    int retain = 1;
    char sid[600];
    foreign_subject_id(url, sid, sizeof(sid));
    if (!cms || !*cms) cms = "/var/lib/bonfyre/cms/bonfyre_cms.db";
    if (sqlite3_open_v2(cms, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) return 1;
    if (sqlite3_prepare_v2(db,
            "SELECT retain_body FROM twin_policy WHERE twin_subject=? AND status='published' LIMIT 1",
            -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, sid, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) retain = sqlite3_column_int(st, 0) != 0;
        sqlite3_finalize(st);
    }
    sqlite3_close(db);
    return retain;
}

static int foreign_hydration(const char *dir) {
    ForeignObs *rows = NULL;
    int n = load_observations(dir, &rows), i, listed = 0;
    long long total_bytes = 0;
    if (n < 0) { fprintf(stderr, "cannot read observations from %s\n", dir); return 2; }

    printf("{\"schema\":\"bonfyre.foreign.hydration.v1\",\"twins\":[");
    for (i = 0; i < n; i++) {
        int j = i, k, retained_here = 0;
        long long bytes = 0;
        while (j + 1 < n && !strcmp(rows[j + 1].url, rows[i].url)) j++;
        for (k = i; k <= j; k++) {
            if (!rows[k].retained || !rows[k].body_path[0]) continue;
            {
                struct stat st;
                if (stat(rows[k].body_path, &st) == 0) { bytes += (long long)st.st_size; retained_here++; }
            }
        }
        total_bytes += bytes;
        printf("%s{\"url\":\"%s\",\"observations\":%d,\"bodies_retained\":%d,"
               "\"retained_bytes\":%lld,\"policy\":\"unpoliced\"}",
               listed ? "," : "", rows[i].url, j - i + 1, retained_here, bytes);
        listed++;
        i = j;
    }
    printf("],\"twins_count\":%d,\"retained_bytes_total\":%lld,"
           "\"note\":\"a twin with no authored policy is unpoliced, not defaulted; an "
           "unmade decision is not a decision to mirror\","
           "\"forbidden_inference\":\"a regime is not a licence; retaining bytes because a "
           "regime permits it does not make retention lawful\"}\n", listed, total_bytes);
    free(rows);
    return 0;
}


static int foreign_admissible(const char *dir) {
    ForeignObs *rows = NULL;
    int n = load_observations(dir, &rows), i, admissible = 0, refused = 0, listed = 0;
    if (n < 0) { fprintf(stderr, "cannot read observations from %s\n", dir); return 2; }
    printf("{\"schema\":\"bonfyre.foreign.admissible.v1\",\"refused\":[");
    for (i = 0; i < n; i++) {
        const char *why = admission_failure(&rows[i]);
        if (!why) { admissible++; continue; }
        refused++;
        if (listed < 24) {
            printf("%s{\"url\":\"%s\",\"observed_at\":\"%s\",\"reason\":\"%s\"}",
                   listed ? "," : "", rows[i].url, rows[i].at, why);
            listed++;
        }
    }
    printf("],\"observations\":%d,\"admissible\":%d,\"refused_count\":%d,"
           "\"note\":\"admission asks whether the read that produced these bytes was "
           "authorized under the policy recorded at the time; it says nothing about what "
           "the bytes contain\"}\n", n, admissible, refused);
    free(rows);
    return 0;
}


/* The product projection of one ForeignTwin is deliberately assembled from
 * owners that already exist:
 *
 *   receipt       what this estate observed
 *   Matrix        whether the resource became addressable / claimed
 *   SourceFrontier whether a declared source is actually admitted
 *   ItemArchive   whether that admitted source yielded retained material
 *
 * This is not a second source registry.  In particular, a public-program
 * binding is emitted as unbound until an owner has actually declared one;
 * source admission and archive depth do not grant a source a page, a public
 * distribution route, or a right to be republished.
 */
typedef struct {
    /* Source contracts include canonical API URLs with long query templates;
     * this release index is an identity map, so truncation would make five
     * real estate contracts silently unmatchable. */
    char url[4096];
    char source[128];
    char family[96];
    char carries[256];
    char body_regime[32];
    char tier[24];
    char last_seen[64];
    char place_field[128];
    char media_field[128];
    char rights_field[128];
    char publisher_ident[256];
    long cadence_seconds;
    int source_contracts;
    int frontier_channels;
    int frontier_admitted;
    int frontier_eligible;
    long long archive_items;
    int public_program_bindings;
    char public_program_samples[4][128];
    /* Economic observations are deliberately attached only after their
     * authored source coordinate matches this source contract exactly. */
    long long economic_observations;
    char economic_latest_observed_at[64];
    int next_by_url;
    int next_by_source;
} ForeignTwinRelease;

#define FOREIGN_RELEASE_BUCKETS 8191
typedef struct {
    ForeignTwinRelease *rows;
    size_t count;
    size_t cap;
    int by_url[FOREIGN_RELEASE_BUCKETS];
    int by_source[FOREIGN_RELEASE_BUCKETS];
} ForeignTwinReleaseIndex;

/* Financial observations are useful only after the incoming price source has
 * an explicit relationship to a ForeignTwin contract.  Counting them here
 * makes that boundary visible: neither a common publisher name nor a price
 * item's wording is an identity rule. */
typedef struct {
    long long observations;
    int source_identities;
    int bound_source_contracts;
    int unbound_source_identities;
    char latest_observed_at[64];
} ForeignEconomicAlignment;

static unsigned long foreign_release_hash(const char *s) {
    unsigned long h = 2166136261u;
    for (; s && *s; s++) { h ^= (unsigned char)*s; h *= 16777619u; }
    return h;
}

static void foreign_release_index_init(ForeignTwinReleaseIndex *index) {
    size_t i;
    memset(index, 0, sizeof(*index));
    for (i = 0; i < FOREIGN_RELEASE_BUCKETS; i++) {
        index->by_url[i] = -1;
        index->by_source[i] = -1;
    }
}

static void foreign_release_index_free(ForeignTwinReleaseIndex *index) {
    free(index->rows);
    memset(index, 0, sizeof(*index));
}

static ForeignTwinRelease *foreign_release_find(ForeignTwinReleaseIndex *index,
                                                 const char *url) {
    unsigned long bucket;
    int i;
    if (!index || !url || !*url) return NULL;
    bucket = foreign_release_hash(url) % FOREIGN_RELEASE_BUCKETS;
    for (i = index->by_url[bucket]; i >= 0; i = index->rows[i].next_by_url)
        if (!strcmp(index->rows[i].url, url)) return &index->rows[i];
    return NULL;
}

static ForeignTwinRelease *foreign_release_add(ForeignTwinReleaseIndex *index,
                                                const char *url) {
    ForeignTwinRelease *row;
    if (!index || !url || !*url) return NULL;
    row = foreign_release_find(index, url);
    if (row) return row;
    if (index->count == index->cap) {
        size_t next = index->cap ? index->cap * 2 : 256;
        ForeignTwinRelease *grown = realloc(index->rows, next * sizeof(*grown));
        if (!grown) return NULL;
        index->rows = grown;
        index->cap = next;
    }
    row = &index->rows[index->count++];
    memset(row, 0, sizeof(*row));
    snprintf(row->url, sizeof(row->url), "%s", url);
    {
        unsigned long bucket = foreign_release_hash(url) % FOREIGN_RELEASE_BUCKETS;
        row->next_by_url = index->by_url[bucket];
        index->by_url[bucket] = (int)(index->count - 1);
    }
    return row;
}

/* Archive and economic owners identify their material by the authored source
 * coordinate, not the public URL.  Keep a second exact index so those owner
 * reductions remain linear when a full estate contains millions of sources. */
static void foreign_release_index_source(ForeignTwinReleaseIndex *index,
                                         ForeignTwinRelease *row) {
    unsigned long bucket;
    if (!index || !row || !row->source[0]) return;
    bucket = foreign_release_hash(row->source) % FOREIGN_RELEASE_BUCKETS;
    row->next_by_source = index->by_source[bucket];
    index->by_source[bucket] = (int)(row - index->rows);
}

static void foreign_release_add_program(ForeignTwinRelease *row, const char *program_id) {
    int i;
    if (!row || !program_id || !*program_id) return;
    for (i = 0; i < row->public_program_bindings && i < 4; i++)
        if (!strcmp(row->public_program_samples[i], program_id)) return;
    if (row->public_program_bindings < 4)
        snprintf(row->public_program_samples[row->public_program_bindings],
                 sizeof(row->public_program_samples[0]), "%s", program_id);
    row->public_program_bindings++;
}

static int sqlite_table_has_column(sqlite3 *db, const char *table, const char *column) {
    sqlite3_stmt *st = NULL;
    int found = 0;
    char sql[256];
    if (!db || !table || !column) return 0;
    snprintf(sql, sizeof(sql), "PRAGMA table_info(%s)", table);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(st, 1);
        if (name && !strcmp(name, column)) { found = 1; break; }
    }
    sqlite3_finalize(st);
    return found;
}

/* A SiteProgram does not name a source directly.  It declares collections and
 * each collection declares exact source_refs.  This reads that authored bridge
 * without treating program input as a rendered route or public delivery. */
static void foreign_program_bindings_load(const char *cms_path,
                                          ForeignTwinReleaseIndex *index) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    const char *sql =
        "SELECT DISTINCT program.program_id, source_ref.value "
        "FROM source_collection AS collection "
        "JOIN site_program AS program "
        "  ON collection.status='published' AND program.status='published' "
        " AND json_valid(program.source_collections) "
        " AND EXISTS (SELECT 1 FROM json_each(program.source_collections) AS selected "
        "             WHERE selected.value=collection.collection_id) "
        "JOIN json_each(collection.source_selector,'$.source_refs') AS source_ref "
        "WHERE json_valid(collection.source_selector)";
    if (!cms_path || !*cms_path || !index) return;
    if (sqlite3_open_v2(cms_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) return;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *program_id = (const char *)sqlite3_column_text(st, 0);
            const char *source = (const char *)sqlite3_column_text(st, 1);
            int i;
            unsigned long bucket;
            if (!source || !*source) continue;
            bucket = foreign_release_hash(source) % FOREIGN_RELEASE_BUCKETS;
            for (i = index->by_source[bucket]; i >= 0; i = index->rows[i].next_by_source)
                if (!strcmp(index->rows[i].source, source))
                    foreign_release_add_program(&index->rows[i], program_id);
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(db);
}

/* Build the release projection with bounded owner scans.  The former
 * implementation issued three unindexed queries for every observed URL; a
 * large receipt history could therefore turn one status read into millions of
 * row comparisons.  This index keeps ForeignTwin's product view proportional
 * to the three owner tables, not the receipt history multiplied by them. */
static void foreign_release_index_load(ForeignTwinReleaseIndex *index,
                                       const char *cms_path,
                                       const char *frontier_path,
                                       const char *items_path) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    foreign_release_index_init(index);
    if (cms_path && *cms_path && sqlite3_open_v2(cms_path, &db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK &&
        sqlite3_prepare_v2(db,
            "SELECT url, COALESCE(source,''), COALESCE(family,''), COALESCE(carries,''), "
            "COALESCE(body_regime,''), COALESCE(tier,''), COALESCE(last_seen,''), "
            "COALESCE(cadence_seconds,0), COALESCE(place_field,''), "
            "COALESCE(media_field,''), COALESCE(rights_field,''), "
            "COALESCE(publisher_ident,'') FROM internet_source "
            "WHERE status='published' AND url<>''", -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *url = (const char *)sqlite3_column_text(st, 0);
            ForeignTwinRelease *r = foreign_release_add(index, url);
            if (!r) continue;
            r->source_contracts++;
            if (r->source_contracts == 1) {
                snprintf(r->source, sizeof(r->source), "%s", (const char *)sqlite3_column_text(st, 1));
                snprintf(r->family, sizeof(r->family), "%s", (const char *)sqlite3_column_text(st, 2));
                snprintf(r->carries, sizeof(r->carries), "%s", (const char *)sqlite3_column_text(st, 3));
                snprintf(r->body_regime, sizeof(r->body_regime), "%s", (const char *)sqlite3_column_text(st, 4));
                snprintf(r->tier, sizeof(r->tier), "%s", (const char *)sqlite3_column_text(st, 5));
                snprintf(r->last_seen, sizeof(r->last_seen), "%s", (const char *)sqlite3_column_text(st, 6));
                r->cadence_seconds = sqlite3_column_int64(st, 7);
                snprintf(r->place_field, sizeof(r->place_field), "%s", (const char *)sqlite3_column_text(st, 8));
                snprintf(r->media_field, sizeof(r->media_field), "%s", (const char *)sqlite3_column_text(st, 9));
                snprintf(r->rights_field, sizeof(r->rights_field), "%s", (const char *)sqlite3_column_text(st, 10));
                snprintf(r->publisher_ident, sizeof(r->publisher_ident), "%s", (const char *)sqlite3_column_text(st, 11));
                foreign_release_index_source(index, r);
            }
        }
    }
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    db = NULL; st = NULL;
    foreign_program_bindings_load(cms_path, index);
    if (frontier_path && *frontier_path && sqlite3_open_v2(frontier_path, &db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
        /* SourceFrontier's exact_address is the authored input coordinate.
         * address is a useful normalized frontier route, but joining it to a
         * CMS contract silently loses e.g. www/redirect distinctions.  Older
         * fixtures and pre-migration ledgers have only address, so retain that
         * compatibility branch instead of inventing a normalization rule. */
        const char *coordinate = sqlite_table_has_column(db, "channel", "exact_address") ?
                                 "exact_address" : "address";
        char sql[192];
        snprintf(sql, sizeof(sql), "SELECT %s, state FROM channel WHERE state IN ('admitted','eligible')", coordinate);
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) st = NULL;
    }
    if (st) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *address = (const char *)sqlite3_column_text(st, 0);
            const char *state = (const char *)sqlite3_column_text(st, 1);
            ForeignTwinRelease *r = foreign_release_find(index, address);
            if (!r) continue;
            r->frontier_channels++;
            if (!strcmp(state, "admitted")) r->frontier_admitted++;
            else if (!strcmp(state, "eligible")) r->frontier_eligible++;
        }
    }
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    db = NULL; st = NULL;
    if (items_path && *items_path && sqlite3_open_v2(items_path, &db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK &&
        sqlite3_prepare_v2(db, "SELECT source, COUNT(*) FROM item GROUP BY source", -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *source = (const char *)sqlite3_column_text(st, 0);
            int i;
            unsigned long bucket;
            if (!source || !*source) continue;
            bucket = foreign_release_hash(source) % FOREIGN_RELEASE_BUCKETS;
            for (i = index->by_source[bucket]; i >= 0; i = index->rows[i].next_by_source)
                if (!strcmp(index->rows[i].source, source))
                    index->rows[i].archive_items += sqlite3_column_int64(st, 1);
        }
    }
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
}

static void foreign_economic_alignment_load(const char *prices_path,
                                            ForeignTwinReleaseIndex *index,
                                            ForeignEconomicAlignment *out) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    memset(out, 0, sizeof(*out));
    if (!prices_path || !*prices_path || !index) return;
    if (sqlite3_open_v2(prices_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) return;
    if (sqlite3_prepare_v2(db,
            "SELECT source, COUNT(*), MAX(observed_at) FROM observed_price GROUP BY source",
            -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *source = (const char *)sqlite3_column_text(st, 0);
            const char *latest = (const char *)sqlite3_column_text(st, 2);
            long long observations = sqlite3_column_int64(st, 1);
            int i;
            unsigned long bucket;
            int bound = 0;
            out->source_identities++;
            out->observations += observations;
            if (latest && strcmp(latest, out->latest_observed_at) > 0)
                snprintf(out->latest_observed_at, sizeof(out->latest_observed_at), "%s", latest);
            if (!source || !*source) continue;
            bucket = foreign_release_hash(source) % FOREIGN_RELEASE_BUCKETS;
            for (i = index->by_source[bucket]; i >= 0; i = index->rows[i].next_by_source) {
                if (!strcmp(index->rows[i].source, source)) {
                    ForeignTwinRelease *r = &index->rows[i];
                    bound = 1;
                    r->economic_observations += observations;
                    if (latest && strcmp(latest, r->economic_latest_observed_at) > 0)
                        snprintf(r->economic_latest_observed_at,
                                 sizeof(r->economic_latest_observed_at), "%s", latest);
                }
            }
            if (bound) out->bound_source_contracts++;
            else out->unbound_source_identities++;
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(db);
}

static void foreign_release_json(const ForeignTwinRelease *maybe_release) {
    ForeignTwinRelease zero;
    const ForeignTwinRelease *r = maybe_release;
    if (!r) { memset(&zero, 0, sizeof(zero)); r = &zero; }
    const char *frontier = r->frontier_admitted > 0 ? "current" :
                           r->frontier_eligible > 0 ? "eligible" : "unbound";
    const char *archive = r->archive_items > 0 ? "current" :
                          r->source_contracts ? "empty" : "unbound";
    const char *spatial_readiness = !r->place_field[0] ? "unbound" :
                                    r->archive_items ? "ready_for_extraction" : "awaiting_material";
    const char *media_readiness = !r->media_field[0] ? "unbound" :
                                  r->archive_items ? "ready_for_extraction" : "awaiting_material";
    const char *rights_readiness = !r->rights_field[0] ? "unbound" :
                                   r->archive_items ? "ready_for_extraction" : "awaiting_material";
    printf("\"release\":{\"source_contract\":");
    if (!r->source_contracts) {
        printf("null");
    } else {
        printf("{");
        printf("\"source\":"); json_string(stdout, r->source);
        printf(",\"family\":"); json_string(stdout, r->family);
        printf(",\"carries\":"); json_string(stdout, r->carries);
        printf(",\"body_regime\":"); json_string(stdout, r->body_regime);
        printf(",\"tier\":"); json_string(stdout, r->tier);
        printf(",\"last_seen\":"); json_string(stdout, r->last_seen);
        printf(",\"cadence_seconds\":%ld}", r->cadence_seconds);
    }
    printf(",\"stages\":{");
    printf("\"frontier\":{\"state\":"); json_string(stdout, frontier);
    printf(",\"channels\":%d,\"admitted\":%d,\"eligible\":%d}",
           r->frontier_channels, r->frontier_admitted, r->frontier_eligible);
    printf(",\"archive\":{\"state\":"); json_string(stdout, archive);
    printf(",\"items\":%lld}", r->archive_items);
    /* The absence of a SourceFrontier->Program binding is evidence, not an
     * invitation for a renderer to infer one from a family or item count. */
    printf(",\"public_program\":{\"state\":");
    json_string(stdout, r->public_program_bindings ? "input_bound" : "unbound");
    printf(",\"bindings\":%d,\"programs\":[", r->public_program_bindings);
    {
        int i, shown = r->public_program_bindings < 4 ? r->public_program_bindings : 4;
        for (i = 0; i < shown; i++) {
            if (i) printf(",");
            json_string(stdout, r->public_program_samples[i]);
        }
        printf("],\"additional_programs\":%d,\"delivery_state\":\"not_verified\"}",
               r->public_program_bindings > shown ? r->public_program_bindings - shown : 0);
    }
    printf("},\"facets\":{");
    printf("\"spatial\":{\"state\":"); json_string(stdout, r->place_field[0] ? "declared" : "unbound");
    printf(",\"extractor\":"); json_string(stdout, r->place_field);
    printf("},\"media\":{\"state\":"); json_string(stdout, r->media_field[0] ? "declared" : "unbound");
    printf(",\"extractor\":"); json_string(stdout, r->media_field);
    printf("},\"rights\":{\"state\":"); json_string(stdout, r->rights_field[0] ? "declared" : "unbound");
    printf(",\"extractor\":"); json_string(stdout, r->rights_field);
    printf("},\"publisher\":{\"state\":"); json_string(stdout, r->publisher_ident[0] ? "declared" : "unbound");
    printf(",\"identifier\":"); json_string(stdout, r->publisher_ident);
    printf("}},\"readiness\":{");
    printf("\"spatial\":{\"state\":"); json_string(stdout, spatial_readiness);
    printf(",\"blocked_by\":"); json_string(stdout, !r->place_field[0] ? "extractor_not_declared" : r->archive_items ? "" : "no_archived_material");
    printf("},\"media\":{\"state\":"); json_string(stdout, media_readiness);
    printf(",\"blocked_by\":"); json_string(stdout, !r->media_field[0] ? "extractor_not_declared" : r->archive_items ? "" : "no_archived_material");
    printf("},\"rights\":{\"state\":"); json_string(stdout, rights_readiness);
    printf(",\"blocked_by\":"); json_string(stdout, !r->rights_field[0] ? "extractor_not_declared" : r->archive_items ? "" : "no_archived_material");
    printf("},\"publisher\":{\"state\":"); json_string(stdout, r->publisher_ident[0] ? "declared" : "unbound");
    printf(",\"blocked_by\":"); json_string(stdout, r->publisher_ident[0] ? "" : "publisher_identifier_not_declared");
    printf("},\"economics\":{\"state\":"); json_string(stdout, r->economic_observations ? "observed" : "unbound");
    printf(",\"observations\":%lld,\"latest_observed_at\":", r->economic_observations);
    json_string(stdout, r->economic_latest_observed_at);
    printf(",\"blocked_by\":"); json_string(stdout, r->economic_observations ? "" : "no_exact_source_binding");
    printf("},\"public_program\":{\"state\":");
    json_string(stdout, r->public_program_bindings ? "input_bound" : "unbound");
    printf(",\"blocked_by\":");
    json_string(stdout, r->public_program_bindings ? "delivery_not_verified" : "source_collection_not_bound_to_program");
    printf("}");
    printf("},\"cannot_infer\":\"a readiness path means only that independently owned inputs are present for a later compiler; it does not create a spatial, media, rights, publisher, economic, or public-program fact, distribution route, reuse licence, offer, purchase authority, or external authority\"}");
}

/* The receipt ledger is intentionally available for forensic work, but it is
 * not an HTTP listing primitive: production has 135k receipts and a public
 * surface must not hydrate a 575 MB evidence directory merely to draw a
 * source catalogue. */
static int foreign_twins_receipts(const char *dir, const char *matrix_db) {
    ForeignObs *rows = NULL;
    int n = load_observations(dir, &rows), i, listed = 0;
    BfMatrix *m = NULL;
    ForeignTwinReleaseIndex release_index;
    const char *cms_path = getenv("BONFYRE_CMS_DB");
    const char *frontier_path = getenv("BONFYRE_SOURCE_FRONTIER_DB");
    const char *items_path = getenv("BONFYRE_ITEM_ARCHIVE_DB");
    int contracts = 0, frontier_current = 0, archive_current = 0;
    int have_matrix = (bf_matrix_open(matrix_db, &m, NULL) == 0 && m != NULL);

    if (n < 0) { fprintf(stderr, "cannot read observations from %s\n", dir); return 2; }
    foreign_release_index_load(&release_index, cms_path, frontier_path, items_path);
    printf("{\"schema\":\"bonfyre.foreign.twins.v1\",\"twins\":[");
    for (i = 0; i < n; i++) {
        int j = i, best = -1, k, changes = 0, promoted = 0, claims = 0;
        char twin[512], sid[600];
        while (j + 1 < n && !strcmp(rows[j + 1].url, rows[i].url)) j++;
        for (k = i; k <= j; k++) {
            if (rows[k].retained) best = k;
            if (k > i && !strcmp(relation_between(&rows[k-1], &rows[k]), "supersedes")) changes++;
        }
        {
            const char *u = rows[i].url, *p = strstr(u, "://");
            size_t w = 0;
            p = p ? p + 3 : u;
            for (; *p && w + 1 < sizeof(twin); p++) {
                if (isalnum((unsigned char)*p)) twin[w++] = (char)tolower((unsigned char)*p);
                else if (w && twin[w-1] != '-') twin[w++] = '-';
            }
            while (w && twin[w-1] == '-') w--;
            twin[w] = '\0';
        }
        snprintf(sid, sizeof(sid), "foreign_twin:%.200s", twin);
        if (have_matrix) {
            promoted = bf_matrix_subject_exists(m, sid) == 1;
            /* A claim points away from the twin: twin claimed_by institution. */
            claims = bf_matrix_subject_relation_from_count(m, sid, "claimed_by");
            if (claims < 0) claims = 0;
        }
        {
            ForeignTwinRelease *release = foreign_release_find(&release_index, rows[i].url);
            if (release && release->source_contracts) contracts++;
            if (release && release->frontier_admitted) frontier_current++;
            if (release && release->archive_items) archive_current++;
            printf("%s{\"subject_id\":\"%s\",\"url\":\"%s\",\"observations\":%d,"
                   "\"content_changes\":%d,\"ever_retained\":%s,\"promoted\":%s,\"claims\":%d,"
                   "\"latest_status\":%d,\"latest_kind\":\"%s\",\"last_seen\":\"%s\",",
                   listed ? "," : "", sid, rows[i].url, j - i + 1, changes,
                   best >= 0 ? "true" : "false", promoted ? "true" : "false", claims,
                   rows[j].http_status, rows[j].kind, rows[j].at);
            foreign_release_json(release);
            printf("}");
        }
        listed++;
        i = j;
    }
    printf("],\"count\":%d,\"release_summary\":{\"source_contracts\":%d,\"frontier_current\":%d,\"archive_current\":%d,\"public_program_bound\":0},"
           "\"forbidden_inference\":\"a listed twin is not a thing this institution has any "
           "right over: observation is not ownership, promotion is not a claim, and a claim "
           "is an assertion\"}\n", listed, contracts, frontier_current, archive_current);
    if (m) bf_matrix_close(m);
    foreign_release_index_free(&release_index);
    free(rows);
    return 0;
}

static void foreign_subject_for_url(const char *url, char *sid, size_t sid_cap) {
    char twin[512];
    const char *p = strstr(url ? url : "", "://");
    size_t w = 0;
    p = p ? p + 3 : (url ? url : "");
    for (; *p && w + 1 < sizeof(twin); p++) {
        if (isalnum((unsigned char)*p)) twin[w++] = (char)tolower((unsigned char)*p);
        else if (w && twin[w - 1] != '-') twin[w++] = '-';
    }
    while (w && twin[w - 1] == '-') w--;
    twin[w] = '\0';
    snprintf(sid, sid_cap, "foreign_twin:%.200s", twin);
}

static int foreign_page_number(const char *text, long *out) {
    char *end = NULL;
    long value;
    if (!text || !*text || !out) return 0;
    errno = 0;
    value = strtol(text, &end, 10);
    if (errno || !end || *end || value < 0) return 0;
    *out = value;
    return 1;
}

/* Product feed: one compact row per authored external-source contract. It is
 * deliberately sourced from the owner tables, so a 135k-row receipt history
 * cannot turn /foreign/twins into an accidental bulk-export endpoint. Exact
 * receipt chronology remains available through `foreign twins --receipts`,
 * `foreign history URL`, and `/foreign/receipts` for a drill-down. */
/* This is deliberately a literal, case-insensitive source-contract search:
 * it narrows an operator's working set without claiming relevance, semantic
 * similarity, ownership, or a new classification.  Search engines may make
 * those richer claims later, but the carrier itself must stay auditable. */
static int foreign_contains_casefold(const char *haystack, const char *needle) {
    size_t n, i;
    if (!needle || !*needle) return 1;
    if (!haystack) return 0;
    n = strlen(needle);
    for (i = 0; haystack[i]; i++) {
        size_t j = 0;
        while (j < n && haystack[i + j] &&
               tolower((unsigned char)haystack[i + j]) ==
               tolower((unsigned char)needle[j])) j++;
        if (j == n) return 1;
    }
    return 0;
}

static int foreign_release_matches_query(const ForeignTwinRelease *release, const char *query) {
    if (!query || !*query) return 1;
    return foreign_contains_casefold(release->url, query) ||
           foreign_contains_casefold(release->source, query) ||
           foreign_contains_casefold(release->family, query) ||
           foreign_contains_casefold(release->carries, query) ||
           foreign_contains_casefold(release->publisher_ident, query);
}

static int foreign_release_compare_url(const void *left, const void *right) {
    const ForeignTwinRelease *a = left, *b = right;
    return strcmp(a->url, b->url);
}

/* A page offset is meaningful only against one ordered owner projection.
 * This is a version marker, not a content digest or a statement that remote
 * sources have not changed: it tells a carrier whether its next page would be
 * drawn from the same locally observed release closure. */
static void foreign_release_generation(const ForeignTwinReleaseIndex *index,
                                       char out[32]) {
    unsigned long h = 1469598103934665603UL;
    size_t i;
    const char *fields[15];
    if (!index) { snprintf(out, 32, "foreign-v1-0"); return; }
    for (i = 0; i < index->count; i++) {
        const ForeignTwinRelease *r = &index->rows[i];
        int j;
        if (!r->source_contracts) continue;
        fields[0] = r->url; fields[1] = r->source; fields[2] = r->family;
        fields[3] = r->carries; fields[4] = r->body_regime; fields[5] = r->tier;
        fields[6] = r->last_seen; fields[7] = r->place_field; fields[8] = r->media_field;
        fields[9] = r->rights_field; fields[10] = r->publisher_ident;
        for (j = 0; j < 11; j++) {
            const unsigned char *p = (const unsigned char *)fields[j];
            for (; p && *p; p++) { h ^= *p; h *= 1099511628211UL; }
            h ^= 0xff; h *= 1099511628211UL;
        }
        h ^= (unsigned long)r->cadence_seconds; h *= 1099511628211UL;
        h ^= (unsigned long)r->frontier_channels; h *= 1099511628211UL;
        h ^= (unsigned long)r->frontier_admitted; h *= 1099511628211UL;
        h ^= (unsigned long)r->frontier_eligible; h *= 1099511628211UL;
        h ^= (unsigned long)r->archive_items; h *= 1099511628211UL;
        h ^= (unsigned long)r->public_program_bindings; h *= 1099511628211UL;
        h ^= (unsigned long)r->economic_observations; h *= 1099511628211UL;
        for (j = 0; j < r->public_program_bindings && j < 4; j++) {
            const unsigned char *p = (const unsigned char *)r->public_program_samples[j];
            for (; p && *p; p++) { h ^= *p; h *= 1099511628211UL; }
            h ^= 0xfe; h *= 1099511628211UL;
        }
    }
    snprintf(out, 32, "foreign-v1-%016lx", h);
}

/* How many destinations have actually delivered, and in which state.
 *
 * This replaced a printed literal. `public_program_delivery_verified` was the
 * one field in the release summary capable of contradicting the readiness
 * counts beside it, and it was the constant 0 -- so an operator reading
 * "spatial_ready_for_extraction: 1,204" next to "delivery_verified: 0" had no
 * way to tell whether nothing had been delivered or nothing was being counted.
 *
 * A row counts as delivered only if the compiled lifecycle says its state
 * delivers and it carries the receipt that names what was materialized. A state
 * the lifecycle has never heard of counts as nothing and is reported as
 * unrecognised rather than silently treated as pending.
 */
typedef struct {
    int rows;                 /* destination_release rows at all */
    int delivered;            /* in a state the lifecycle calls a delivery */
    int receipted;            /* ... and carrying a receipt reference */
    int open;                 /* may still become a delivery */
    int blocked;              /* a named binding is missing */
    int failed;
    int stale;
    int unrecognised;         /* a state not in the compiled lifecycle */
    int store_present;        /* the record type exists in this CMS */
} ForeignDeliveryStanding;

static void foreign_delivery_standing(const char *cms_path, ForeignDeliveryStanding *out) {
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL; char uri[PATH_MAX];
    memset(out, 0, sizeof(*out));
    if (!cms_path || !*cms_path) return;
    if (snprintf(uri, sizeof(uri), "file:%s?mode=ro", cms_path) >= (int)sizeof(uri)) return;
    if (sqlite3_open_v2(uri, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return;
    }
    if (sqlite3_prepare_v2(db,
          "SELECT state, COALESCE(receipt_ref,''), COUNT(*) FROM destination_release"
          " WHERE status='published' GROUP BY state, receipt_ref<>''", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return;
    }
    out->store_present = 1;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *state = (const char *)sqlite3_column_text(st, 0);
        const char *receipt = (const char *)sqlite3_column_text(st, 1);
        int n = sqlite3_column_int(st, 2);
        const BfDestinationState *decl = bf_destination_state(state);
        out->rows += n;
        if (!decl) { out->unrecognised += n; continue; }
        if (decl->delivered) {
            out->delivered += n;
            if (receipt && *receipt) out->receipted += n;
        }
        if (decl->open) out->open += n;
        if (!strcmp(decl->state, "blocked")) out->blocked += n;
        if (!strcmp(decl->state, "failed")) out->failed += n;
        if (!strcmp(decl->state, "stale")) out->stale += n;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
}

/* What changed between this estate's own observations, by consequence.
 *
 * Refreshing on a timer treats every byte that moved as work to redo. Most
 * bytes that move are a footer or a rotated banner, so a delta is typed by what
 * it bears on: only an invalidating consequence can make a delivered
 * destination stale, and a boundary or disappearance additionally stops for a
 * person. */
typedef struct {
    int rows;
    int invalidating;
    int needs_review;
    int cosmetic;
    int unrecognised;
    int store_present;
} ForeignDeltaStanding;

static void foreign_delta_standing(const char *cms_path, ForeignDeltaStanding *out) {
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL; char uri[PATH_MAX];
    memset(out, 0, sizeof(*out));
    if (!cms_path || !*cms_path) return;
    if (snprintf(uri, sizeof(uri), "file:%s?mode=ro", cms_path) >= (int)sizeof(uri)) return;
    if (sqlite3_open_v2(uri, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return;
    }
    if (sqlite3_prepare_v2(db,
          "SELECT consequence, COUNT(*) FROM source_release_delta"
          " WHERE status='published' GROUP BY consequence", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return;
    }
    out->store_present = 1;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(st, 0);
        int n = sqlite3_column_int(st, 1);
        const BfChangeConsequence *decl = bf_change_consequence(name);
        out->rows += n;
        if (!decl) { out->unrecognised += n; continue; }
        if (decl->invalidates) out->invalidating += n;
        if (decl->needs_review) out->needs_review += n;
        if (!decl->invalidates && !decl->needs_review) out->cosmetic += n;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
}

/* ------------------------------------------------- release compilation
 *
 * Readiness was the end of the road. A source contract with a declared
 * extractor and retained material read ready_for_extraction, and then nothing:
 * no record that any destination had been selected, admitted, attempted or
 * delivered, and a printed zero where a delivery count belonged.
 *
 * This compiles the part that was missing, and refuses the part that would be
 * a lie. A destination becomes `admissible` when every binding it needs is
 * present, and `blocked` with the missing binding named when one is not.
 * Neither is a delivery. Nothing in this function can reach a delivering state,
 * because reaching one requires a receipt naming materialized bytes and this
 * function materializes nothing.
 */
static const char *release_blocker(const ForeignTwinRelease *r, const char *destination) {
    if (!r->source_contracts) return "no published source contract";
    if (!strcmp(destination, "spatial")) {
        if (!r->place_field[0]) return "no declared spatial extractor";
        if (!r->archive_items) return "no retained archive material to extract from";
        return NULL;
    }
    if (!strcmp(destination, "media")) {
        if (!r->media_field[0]) return "no declared media extractor";
        if (!r->archive_items) return "no retained archive material to extract from";
        return NULL;
    }
    if (!strcmp(destination, "rights")) {
        if (!r->rights_field[0]) return "no declared rights extractor";
        if (!r->archive_items) return "no retained archive material to extract from";
        return NULL;
    }
    if (!strcmp(destination, "shop")) {
        if (!r->economic_observations) return "no observed price bound to this source coordinate";
        return NULL;
    }
    if (!strcmp(destination, "search")) {
        if (!r->archive_items) return "nothing retained from this source to index";
        return NULL;
    }
    return "unknown destination";
}

static const char *const RELEASE_DESTINATIONS[] = {"search", "shop", "spatial", "media", "rights"};
#define RELEASE_DESTINATION_COUNT (sizeof(RELEASE_DESTINATIONS) / sizeof(RELEASE_DESTINATIONS[0]))

static void release_ref(char *out, size_t cap, const char *a, const char *b) {
    unsigned long h = 1469598103934665603UL;
    const char *p;
    for (p = a; p && *p; p++) { h ^= (unsigned char)*p; h *= 1099511628211UL; }
    h ^= '|'; h *= 1099511628211UL;
    for (p = b; p && *p; p++) { h ^= (unsigned char)*p; h *= 1099511628211UL; }
    snprintf(out, cap, "%016lx", h);
}

/* Typed deltas over this estate's own observations.
 *
 * The estate holds 23,954 observed prices across nine observation times, and
 * 1,519 items it has seen carry more than one amount. Those are real changes in
 * material a destination would carry, and until now they were rows nobody
 * compared. A price that moved is a `material` consequence on the amount field:
 * it invalidates a delivered shop destination and stops for nobody.
 *
 * What this cannot do is call a change cosmetic on the strength of not having
 * noticed it. A consequence is only written for a field actually compared, so
 * the absence of a delta is the absence of a comparison rather than a claim
 * that nothing moved.
 */
static int release_compile(const char *cms_path, const char *prices_path,
                           const char *frontier_path, const char *items_path,
                           int write) {
    ForeignTwinReleaseIndex index;
    sqlite3 *cms = NULL, *prices = NULL;
    sqlite3_stmt *st = NULL, *ins = NULL;
    char generation[32], now[32];
    int admissible = 0, blocked = 0, deltas = 0, wrote = 0, delta_wrote = 0;
    int considered = 0;
    size_t i, d;
    time_t t = time(NULL);

    if (!cms_path || !*cms_path) {
        fprintf(stderr, "release compile: no BONFYRE_CMS_DB\n");
        return 1;
    }
    strftime(now, sizeof(now), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
    foreign_release_index_load(&index, cms_path, frontier_path, items_path);
    {
        ForeignEconomicAlignment economics;
        foreign_economic_alignment_load(prices_path, &index, &economics);
    }
    foreign_release_generation(&index, generation);

    if (write && sqlite3_open(cms_path, &cms) != SQLITE_OK) {
        fprintf(stderr, "release compile: cannot open the record store for writing\n");
        foreign_release_index_free(&index);
        if (cms) sqlite3_close(cms);
        return 1;
    }
    if (write) {
        sqlite3_busy_timeout(cms, 5000);
        sqlite3_exec(cms, "BEGIN", NULL, NULL, NULL);
        if (sqlite3_prepare_v2(cms,
              /* This compiler owns exactly two states and must not touch any
               * other. REPLACE reset a delivered destination to admissible on
               * the next run -- recompiling admissibility destroyed the
               * delivery and its receipt reference, which is the same class of
               * mistake as letting readiness claim to be delivery, in the
               * opposite direction. */
              "INSERT INTO destination_release(namespace,created_at,updated_at,"
              " published_at,status,release_id,source_ref,destination,state,generation,"
              " blocked_binding,receipt_ref,materialized_count,admitted_by,state_changed_at,"
              " reason,cannot_infer)"
              " VALUES('root',?1,?1,?1,'published',?2,?3,?4,?5,?6,?7,'','','',?1,?8,?9)"
              " ON CONFLICT(namespace,release_id) DO UPDATE SET"
              "   updated_at=excluded.updated_at, state=excluded.state,"
              "   generation=excluded.generation, blocked_binding=excluded.blocked_binding,"
              "   state_changed_at=excluded.state_changed_at, reason=excluded.reason"
              " WHERE destination_release.state IN ('admissible','blocked')",
              -1, &ins, NULL) != SQLITE_OK) {
            fprintf(stderr, "release compile: %s\n", sqlite3_errmsg(cms));
            sqlite3_exec(cms, "ROLLBACK", NULL, NULL, NULL);
            sqlite3_close(cms);
            foreign_release_index_free(&index);
            return 1;
        }
    }

    for (i = 0; i < index.count; i++) {
        const ForeignTwinRelease *r = &index.rows[i];
        if (!r->source_contracts) continue;
        considered++;
        for (d = 0; d < RELEASE_DESTINATION_COUNT; d++) {
            const char *dest = RELEASE_DESTINATIONS[d];
            const char *why = release_blocker(r, dest);
            const char *state = why ? "blocked" : "admissible";
            char id[96], ref[32];
            if (why) blocked++; else admissible++;
            if (!write) continue;
            release_ref(ref, sizeof(ref), r->url, dest);
            snprintf(id, sizeof(id), "rel-%s", ref);
            sqlite3_reset(ins);
            sqlite3_bind_text(ins, 1, now, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 2, id, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 3, r->url, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 4, dest, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 5, state, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 6, generation, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 7, why ? why : "", -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 8,
              why ? "a binding this destination needs is absent; naming it is how it stops being blocked"
                  : "every binding this destination needs is present and no work has been done",
              -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 9,
              "admissible is not delivered, not published, and not permission that survives a "
              "change in the source; nothing here materialized anything",
              -1, SQLITE_STATIC);
            if (sqlite3_step(ins) != SQLITE_DONE) {
                fprintf(stderr, "release compile: %s\n", sqlite3_errmsg(cms));
                sqlite3_exec(cms, "ROLLBACK", NULL, NULL, NULL);
                sqlite3_finalize(ins);
                sqlite3_close(cms);
                foreign_release_index_free(&index);
                return 1;
            }
            wrote++;
        }
    }
    if (ins) { sqlite3_finalize(ins); ins = NULL; }

    /* An amount this estate observed at two different values is a material
     * change in what a shop destination would carry. */
    if (prices_path && *prices_path) {
        char uri[PATH_MAX];
        if (snprintf(uri, sizeof(uri), "file:%s?mode=ro", prices_path) < (int)sizeof(uri) &&
            sqlite3_open_v2(uri, &prices, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) == SQLITE_OK &&
            /* Keyed on the source identity the release index already binds
             * prices by, so a delta can be joined to the destinations that
             * carry the field it changed. Keying it on publisher|item read well
             * and could be joined to nothing. */
            sqlite3_prepare_v2(prices,
              "SELECT source, COUNT(DISTINCT amount), MIN(observed_at), MAX(observed_at),"
              " COUNT(DISTINCT item), COUNT(*) FROM observed_price GROUP BY source"
              " HAVING COUNT(DISTINCT amount) > 1 ORDER BY 2 DESC, 1", -1, &st, NULL) == SQLITE_OK) {
            if (write && sqlite3_prepare_v2(cms,
                  "INSERT OR REPLACE INTO source_release_delta(namespace,created_at,updated_at,"
                  " published_at,status,delta_id,source_ref,from_generation,to_generation,"
                  " consequence,field,added,removed,changed,observed_at,cannot_infer)"
                  " VALUES('root',?1,?1,?1,'published',?2,?3,?4,?5,'material','amount','','',?6,?7,?8)",
                  -1, &ins, NULL) != SQLITE_OK) {
                fprintf(stderr, "release compile: %s\n", sqlite3_errmsg(cms));
                ins = NULL;
            }
            while (sqlite3_step(st) == SQLITE_ROW) {
                const char *src = (const char *)sqlite3_column_text(st, 0);
                int distinct = sqlite3_column_int(st, 1);
                const char *first = (const char *)sqlite3_column_text(st, 2);
                const char *last = (const char *)sqlite3_column_text(st, 3);
                char id[96], ref[32], changed[24];
                deltas++;
                if (!write || !ins || !src || !*src) continue;
                release_ref(ref, sizeof(ref), src, "amount");
                snprintf(id, sizeof(id), "delta-%s", ref);
                snprintf(changed, sizeof(changed), "%d", distinct);
                sqlite3_reset(ins);
                sqlite3_bind_text(ins, 1, now, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 2, id, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins, 3, src, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins, 4, first ? first : "", -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins, 5, last ? last : "", -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins, 6, changed, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins, 7, last ? last : now, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins, 8,
                  "an amount observed at two values is a change in what this estate saw, never a "
                  "current offer, a price available to any viewer, or evidence the publisher "
                  "changed it deliberately", -1, SQLITE_STATIC);
                if (sqlite3_step(ins) == SQLITE_DONE) delta_wrote++;
            }
            sqlite3_finalize(st); st = NULL;
            if (ins) { sqlite3_finalize(ins); ins = NULL; }
        }
        if (prices) sqlite3_close(prices);
    }

    if (write) {
        sqlite3_exec(cms, "COMMIT", NULL, NULL, NULL);
        sqlite3_close(cms);
    }

    printf("{\"schema\":\"bonfyre.foreign.release_compile.v1\",\"mode\":");
    json_string(stdout, write ? "written" : "preview");
    printf(",\"catalogue_generation\":"); json_string(stdout, generation);
    printf(",\"source_contracts\":%d,\"destinations_per_source\":%d,"
           "\"admissible\":%d,\"blocked\":%d,\"delivered\":0,"
           "\"destination_rows_written\":%d,"
           "\"deltas_found\":%d,\"deltas_written\":%d,"
           "\"cannot_infer\":\"this compiles admissibility and typed change, and delivers nothing: "
           "no destination here reached a delivering state, because reaching one requires a receipt "
           "naming materialized bytes and nothing was materialized. A delta is a statement about "
           "the fields this estate compared, so the absence of one is the absence of a comparison "
           "rather than a claim the source did not move.\"}\n",
           considered, (int)RELEASE_DESTINATION_COUNT, admissible, blocked,
           wrote, deltas, delta_wrote);
    foreign_release_index_free(&index);
    return 0;
}

/* One destination, actually delivered.
 *
 * Everything above this point stops at admissibility, and that is the correct
 * place for it to stop: a state machine that could mark itself delivered would
 * be the readiness-as-published defect wearing a new field name. A delivery is
 * only true if bytes exist and something names them, so this is the one path
 * that writes bytes, and it earns the state by producing the digest of what it
 * wrote.
 *
 * Five refusals, each of which is the interesting case rather than the happy
 * one:
 *
 *   - a destination that is not admissible is refused with its blocker, so a
 *     missing binding cannot be walked past by asking twice;
 *   - a caller naming a generation this catalogue has moved past is refused,
 *     because continuing across generations would deliver one snapshot under
 *     another's name;
 *   - a source with nothing retained delivers nothing, since a fragment
 *     composed from no material would be a file with a receipt and no subject;
 *   - a failure writes no state at all and never touches what was observed;
 *   - and delivering does not re-open the question of rights: the material was
 *     already retained under a regime, and this arranges it rather than
 *     fetching it again.
 */
static int search_fragment_compose(sqlite3 *items, const char *url, const char *identity,
                                   const char *generation, char **out, size_t *out_len,
                                   int *out_rows);

static int release_deliver(const char *cms_path, const char *items_path,
                           const char *frontier_path, const char *prices_path,
                           const char *out_root, const char *url,
                           const char *destination, const char *expect_generation) {
    ForeignTwinReleaseIndex index;
    const ForeignTwinRelease *target = NULL;
    sqlite3 *cms = NULL, *items = NULL;
    sqlite3_stmt *st = NULL;
    char generation[32], now[32], ref[32], id[96];
    char path[PATH_MAX], tmp[PATH_MAX], hex[65];
    char *body = NULL;
    size_t body_len = 0;
    int rows = 0, rc = 1;
    size_t i;
    time_t t = time(NULL);
    FILE *f = NULL;

    if (!cms_path || !*cms_path || !items_path || !*items_path || !out_root || !*out_root ||
        !url || !*url || !destination || !*destination) {
        fprintf(stderr, "release deliver: needs a record store, an archive, an output root, a source and a destination\n");
        return 1;
    }
    /* Only search is deliverable here. The other four destinations name
     * extraction this estate has declared and not built, and offering to
     * deliver one would be the same lie in a different field. */
    if (strcmp(destination, "search")) {
        printf("{\"schema\":\"bonfyre.foreign.release_deliver.v1\",\"state\":\"refused\",\"reason\":");
        json_string(stdout, "only the search destination has a materializer; the others declare extraction this estate has not built, and delivering one would state work that did not happen");
        printf(",\"destination\":"); json_string(stdout, destination);
        printf("}\n");
        return 1;
    }
    strftime(now, sizeof(now), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
    foreign_release_index_load(&index, cms_path, frontier_path, items_path);
    {
        ForeignEconomicAlignment economics;
        foreign_economic_alignment_load(prices_path, &index, &economics);
    }
    foreign_release_generation(&index, generation);
    if (expect_generation && *expect_generation && strcmp(expect_generation, generation)) {
        printf("{\"schema\":\"bonfyre.foreign.release_deliver.v1\",\"state\":\"snapshot_mismatch\","
               "\"expected_generation\":");
        json_string(stdout, expect_generation);
        printf(",\"catalogue_generation\":"); json_string(stdout, generation);
        printf(",\"cannot_infer\":\"a delivery is refused across generations rather than continued: "
               "materializing one snapshot under another's name would make the receipt name bytes "
               "that were never in that release\"}\n");
        foreign_release_index_free(&index);
        return 1;
    }
    for (i = 0; i < index.count; i++)
        if (!strcmp(index.rows[i].url, url)) { target = &index.rows[i]; break; }
    if (!target) {
        printf("{\"schema\":\"bonfyre.foreign.release_deliver.v1\",\"state\":\"refused\","
               "\"reason\":\"no published source contract at that coordinate\"}\n");
        foreign_release_index_free(&index);
        return 1;
    }
    {
        const char *why = release_blocker(target, destination);
        if (why) {
            printf("{\"schema\":\"bonfyre.foreign.release_deliver.v1\",\"state\":\"blocked\","
                   "\"destination\":");
            json_string(stdout, destination);
            printf(",\"blocked_binding\":"); json_string(stdout, why);
            printf(",\"cannot_infer\":\"a blocked destination is refused rather than queued; asking "
                   "again does not supply the binding\"}\n");
            foreign_release_index_free(&index);
            return 1;
        }
    }

    /* The fragment: what this estate retained from that source, arranged for
     * search. It is composed from the archive and reaches no network. */
    {
        char uri[PATH_MAX];
        if (snprintf(uri, sizeof(uri), "file:%s?mode=ro", items_path) >= (int)sizeof(uri) ||
            sqlite3_open_v2(uri, &items, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
            fprintf(stderr, "release deliver: cannot read the archive\n");
            goto done;
        }
    }
    /* Composed through the one canonical composer, so the bytes a receipt names
     * are the bytes a later drift check will rebuild. */
    if (!search_fragment_compose(items, url, target->source, generation, &body, &body_len, &rows)) {
        printf("{\"schema\":\"bonfyre.foreign.release_deliver.v1\",\"state\":\"refused\","
               "\"reason\":\"nothing retained from this source, so there is no fragment to deliver\","
               "\"cannot_infer\":\"an empty fragment is not a delivery; a file with a receipt and no "
               "subject would make the count true and the delivery false\"}\n");
        goto done;
    }

    bf_sha256_hex((const unsigned char *)body, body_len, hex);
    release_ref(ref, sizeof(ref), url, destination);
    snprintf(id, sizeof(id), "rel-%s", ref);
    /* Named by the digest of what it holds, so a receipt cannot point at a file
     * whose bytes have since changed. */
    if (snprintf(path, sizeof(path), "%s/search-%.16s.json", out_root, hex) >= (int)sizeof(path) ||
        snprintf(tmp, sizeof(tmp), "%s.writing", path) >= (int)sizeof(tmp)) goto done;
    if (!(f = fopen(tmp, "wb"))) {
        fprintf(stderr, "release deliver: cannot write beneath %s\n", out_root);
        goto done;
    }
    if (fwrite(body, 1, body_len, f) != body_len) { fclose(f); f = NULL; unlink(tmp); goto done; }
    if (fclose(f) != 0) { f = NULL; unlink(tmp); goto done; }
    f = NULL;
    if (rename(tmp, path) != 0) { unlink(tmp); goto done; }

    /* State last. A failure above this line leaves the row where it was, so a
     * failed delivery never mutates what the estate believes. */
    if (sqlite3_open(cms_path, &cms) != SQLITE_OK) goto done;
    sqlite3_busy_timeout(cms, 5000);
    if (sqlite3_prepare_v2(cms,
          "UPDATE destination_release SET state='delivered', receipt_ref=?1,"
          " materialized_count=?2, generation=?3, state_changed_at=?4, updated_at=?4,"
          " blocked_binding='', reason=?5, cannot_infer=?6"
          " WHERE release_id=?7 AND state='admissible'", -1, &st, NULL) != SQLITE_OK) goto done;
    {
        char count[24];
        snprintf(count, sizeof(count), "%d", rows);
        sqlite3_bind_text(st, 1, hex, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, count, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, generation, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 4, now, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 5, path, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 6,
          "a delivered fragment is material this estate already retained, arranged and hashed; it "
          "is not a reuse licence, not the destination agreeing it is correct, and not authority to "
          "publish it anywhere", -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 7, id, -1, SQLITE_TRANSIENT);
    }
    if (sqlite3_step(st) != SQLITE_DONE) goto done;
    if (!sqlite3_changes(cms)) {
        printf("{\"schema\":\"bonfyre.foreign.release_deliver.v1\",\"state\":\"refused\","
               "\"reason\":\"that destination was not admissible when the write landed\","
               "\"receipt_written\":"); json_string(stdout, path);
        printf(",\"cannot_infer\":\"the fragment exists on disk and the release did not move; a "
               "materialization is not a delivery until a row in an admissible state accepts it\"}\n");
        goto done;
    }
    rc = 0;
    printf("{\"schema\":\"bonfyre.foreign.release_deliver.v1\",\"state\":\"delivered\",\"source\":");
    json_string(stdout, url);
    printf(",\"destination\":"); json_string(stdout, destination);
    printf(",\"release_id\":"); json_string(stdout, id);
    printf(",\"generation\":"); json_string(stdout, generation);
    printf(",\"receipt_digest\":"); json_string(stdout, hex);
    printf(",\"fragment\":"); json_string(stdout, path);
    printf(",\"materialized\":%d,\"bytes\":%zu,\"delivering_state\":%s,"
           "\"cannot_infer\":\"delivered means a receipt names these bytes and where they came from. "
           "It is not a reuse licence, not the destination agreeing the material is correct, not "
           "current availability, and not permission to publish it again after the source moves.\"}\n",
           rows, body_len, bf_destination_delivered("delivered") ? "true" : "false");

done:
    if (st) sqlite3_finalize(st);
    if (f) fclose(f);
    if (items) sqlite3_close(items);
    if (cms) sqlite3_close(cms);
    free(body);
    foreign_release_index_free(&index);
    return rc;
}

/* Which deliveries a change actually invalidates.
 *
 * This is the step that makes typed change worth having. Without it a delta is
 * a row nobody acts on, and the alternative to acting on it is refreshing
 * everything on a timer -- which is how an estate with a hundred thousand
 * addresses spends its whole budget re-reading footers.
 *
 * The rule that keeps it honest is the one that refuses work: a destination is
 * staled only by a change to a field it actually carries, and what each one
 * carries is declared in CUE rather than assumed here. A price that moved is a
 * real material change and a delivered search fragment holds no amount, so the
 * search delivery stands. Marking it stale anyway would be the timer wearing a
 * state machine's clothes.
 *
 * Nothing here re-reads a source, and staling a delivery does not withdraw it:
 * what was delivered is still there and its receipt still stands.
 */
/* Recompose a delivered search fragment and compare it to its receipt.
 *
 * `stale` was unreachable without this. The price deltas this estate can compute
 * bear on amounts, a search fragment carries no amount, and the shop
 * destination has no materializer -- so nothing could ever have been delivered
 * into a state a price change was allowed to invalidate, and a lifecycle state
 * no path can reach is decoration.
 *
 * The comparison that does work is the estate against itself: compose the
 * fragment again from the archive as it stands now, and see whether it still
 * hashes to what the receipt recorded. A different digest means the material a
 * delivery carries has moved, which is exactly what `stale` is for, and it needs
 * no second generation of anything to be kept on disk.
 */
/* The canonical bytes of a search fragment.
 *
 * Both the delivery and the drift check compose through here, and that is the
 * whole point. The first version composed them separately and the delivered
 * fragment carried a materialized_at stamp the recomposition left out, so every
 * recomposition differed and every delivery read as stale within a second of
 * being made -- a staleness detector that could only ever say yes.
 *
 * When the fragment was written is not part of what it says, so it is not part
 * of what is hashed. The record keeps that time in state_changed_at, where it
 * belongs.
 */
static int search_fragment_compose(sqlite3 *items, const char *url, const char *identity,
                                   const char *generation, char **out, size_t *out_len,
                                   int *out_rows) {
    sqlite3_stmt *st = NULL;
    char *body = NULL;
    size_t len = 0, cap = 65536;
    int rows = 0, ok = 0;
    if (out) *out = NULL;
    if (out_len) *out_len = 0;
    if (out_rows) *out_rows = 0;
    if (!items || !(body = malloc(cap))) return 0;
    if (sqlite3_prepare_v2(items,
          "SELECT COALESCE(title,''), COALESCE(href,''), COALESCE(publisher,''),"
          " COALESCE(at,''), COALESCE(kind,'') FROM item WHERE source=?1"
          " ORDER BY at DESC, key LIMIT 500", -1, &st, NULL) != SQLITE_OK) { free(body); return 0; }
    sqlite3_bind_text(st, 1, identity, -1, SQLITE_TRANSIENT);
    len = (size_t)snprintf(body, cap,
      "{\"schema\":\"bonfyre.search_fragment.v1\",\"source\":\"%s\",\"source_identity\":\"%s\","
      "\"generation\":\"%s\",\"items\":[", url, identity, generation);
    while (sqlite3_step(st) == SQLITE_ROW) {
        char row[3072];
        int n = snprintf(row, sizeof(row),
          "%s{\"title\":\"%.400s\",\"href\":\"%.600s\",\"publisher\":\"%.200s\",\"at\":\"%.64s\",\"kind\":\"%.64s\"}",
          rows ? "," : "",
          (const char *)sqlite3_column_text(st, 0), (const char *)sqlite3_column_text(st, 1),
          (const char *)sqlite3_column_text(st, 2), (const char *)sqlite3_column_text(st, 3),
          (const char *)sqlite3_column_text(st, 4));
        if (n < 0) continue;
        while (len + (size_t)n + 512 > cap) {
            char *bigger = realloc(body, cap * 2);
            if (!bigger) goto out;
            body = bigger; cap *= 2;
        }
        memcpy(body + len, row, (size_t)n);
        len += (size_t)n;
        rows++;
    }
    len += (size_t)snprintf(body + len, cap - len,
      "],\"count\":%d,\"cannot_infer\":\"a search fragment is material this estate already retained, "
      "arranged; it is not a reuse licence, not current availability, and not the source's own "
      "publication\"}\n", rows);
    ok = rows > 0;
out:
    if (st) sqlite3_finalize(st);
    if (ok && out) { *out = body; if (out_len) *out_len = len; if (out_rows) *out_rows = rows; }
    else free(body);
    return ok;
}

static int search_fragment_digest(sqlite3 *items, const char *url, const char *identity,
                                  const char *generation, char hex[65], int *count) {
    char *body = NULL;
    size_t len = 0;
    if (!search_fragment_compose(items, url, identity, generation, &body, &len, count)) return 0;
    bf_sha256_hex((const unsigned char *)body, len, hex);
    free(body);
    return 1;
}

static int release_impact(const char *cms_path, const char *frontier_path,
                          const char *items_path, const char *prices_path, int write) {
    ForeignTwinReleaseIndex index;
    sqlite3 *cms = NULL;
    sqlite3_stmt *st = NULL, *upd = NULL;
    char generation[32], now[32];
    int deltas = 0, invalidating = 0, review = 0;
    int deliveries = 0, staled = 0, spared = 0, unjoined = 0;
    int recomposed = 0, matched = 0, drifted = 0, printed_any = 0;
    time_t t = time(NULL);
    size_t i;

    if (!cms_path || !*cms_path) { fprintf(stderr, "release impact: no BONFYRE_CMS_DB\n"); return 1; }
    strftime(now, sizeof(now), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
    foreign_release_index_load(&index, cms_path, frontier_path, items_path);
    {
        ForeignEconomicAlignment economics;
        foreign_economic_alignment_load(prices_path, &index, &economics);
    }
    foreign_release_generation(&index, generation);

    if (sqlite3_open(write ? cms_path : cms_path, &cms) != SQLITE_OK) {
        fprintf(stderr, "release impact: cannot open the record store\n");
        foreign_release_index_free(&index);
        if (cms) sqlite3_close(cms);
        return 1;
    }
    sqlite3_busy_timeout(cms, 5000);
    if (write && sqlite3_prepare_v2(cms,
          "UPDATE destination_release SET state='stale', state_changed_at=?1, updated_at=?1,"
          " reason=?2, cannot_infer=?3 WHERE release_id=?4 AND state='delivered'",
          -1, &upd, NULL) != SQLITE_OK) {
        fprintf(stderr, "release impact: %s\n", sqlite3_errmsg(cms));
        sqlite3_close(cms);
        foreign_release_index_free(&index);
        return 1;
    }

    printf("{\"schema\":\"bonfyre.foreign.release_impact.v1\",\"mode\":");
    json_string(stdout, write ? "written" : "preview");
    printf(",\"catalogue_generation\":"); json_string(stdout, generation);
    printf(",\"affected\":[");

    if (sqlite3_prepare_v2(cms,
          "SELECT d.source_ref, d.consequence, d.field, COALESCE(d.changed,''),"
          " r.release_id, r.destination, r.state"
          " FROM source_release_delta d"
          " LEFT JOIN destination_release r ON r.source_ref IN ("
          "   SELECT source_ref FROM destination_release WHERE source_ref = r.source_ref)"
          " WHERE d.status='published' AND r.status='published'"
          " ORDER BY d.source_ref, r.destination", -1, &st, NULL) != SQLITE_OK) {
        /* The join above is deliberately not the one that matters: a delta is
         * keyed on the source identity prices bind by, and a release row on the
         * source URL, so the two are related through the loaded index rather
         * than through SQL. */
        sqlite3_finalize(st); st = NULL;
    }
    if (st) { sqlite3_finalize(st); st = NULL; }

    /* Deltas first, then the releases of every source contract carrying that
     * identity, joined through the index the projection already built. */
    if (sqlite3_prepare_v2(cms,
          "SELECT source_ref, consequence, field, COALESCE(changed,'') FROM source_release_delta"
          " WHERE status='published' ORDER BY source_ref", -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *identity = (const char *)sqlite3_column_text(st, 0);
            const char *consequence = (const char *)sqlite3_column_text(st, 1);
            const char *field = (const char *)sqlite3_column_text(st, 2);
            const char *changed = (const char *)sqlite3_column_text(st, 3);
            const BfChangeConsequence *decl = bf_change_consequence(consequence);
            char bare[64];
            const char *colon;
            deltas++;
            if (!decl) continue;
            if (decl->invalidates) invalidating++;
            if (decl->needs_review) review++;
            /* A field may be recorded as "amount" or narrowed to "amount:item". */
            colon = field ? strchr(field, ':') : NULL;
            if (colon && (size_t)(colon - field) < sizeof(bare)) {
                memcpy(bare, field, (size_t)(colon - field));
                bare[colon - field] = '\0';
            } else snprintf(bare, sizeof(bare), "%s", field ? field : "");

            for (i = 0; i < index.count; i++) {
                const ForeignTwinRelease *r = &index.rows[i];
                size_t d;
                if (!r->source_contracts || strcmp(r->source, identity)) continue;
                for (d = 0; d < RELEASE_DESTINATION_COUNT; d++) {
                    const char *dest = RELEASE_DESTINATIONS[d];
                    char id[96], ref[32];
                    int carries = bf_destination_carries_field(dest, bare);
                    sqlite3_stmt *q = NULL;
                    const char *state = NULL;
                    release_ref(ref, sizeof(ref), r->url, dest);
                    snprintf(id, sizeof(id), "rel-%s", ref);
                    if (sqlite3_prepare_v2(cms, "SELECT state FROM destination_release"
                                                " WHERE release_id=?1 AND status='published'",
                                           -1, &q, NULL) != SQLITE_OK) continue;
                    sqlite3_bind_text(q, 1, id, -1, SQLITE_TRANSIENT);
                    if (sqlite3_step(q) == SQLITE_ROW)
                        state = (const char *)sqlite3_column_text(q, 0);
                    if (state && bf_destination_delivered(state)) {
                        deliveries++;
                        if (!carries) spared++;
                        else if (!strcmp(state, "delivered")) {
                            printf("%s{\"source\":", printed_any ? "," : "");
                            json_string(stdout, r->url);
                            printf(",\"destination\":"); json_string(stdout, dest);
                            printf(",\"field\":"); json_string(stdout, bare);
                            printf(",\"consequence\":"); json_string(stdout, consequence);
                            printf(",\"observations_changed\":"); json_string(stdout, changed);
                            printf(",\"was\":"); json_string(stdout, state);
                            printf(",\"becomes\":\"stale\"}");
                            printed_any = 1;
                            if (write && upd) {
                                sqlite3_reset(upd);
                                sqlite3_bind_text(upd, 1, now, -1, SQLITE_STATIC);
                                sqlite3_bind_text(upd, 2,
                                  "a field this destination carries changed at the source since it "
                                  "was delivered", -1, SQLITE_STATIC);
                                sqlite3_bind_text(upd, 3,
                                  "stale is not withdrawn: what was delivered is still there and its "
                                  "receipt still stands; it says only that the estate's own later "
                                  "observation no longer matches it", -1, SQLITE_STATIC);
                                sqlite3_bind_text(upd, 4, id, -1, SQLITE_TRANSIENT);
                                if (sqlite3_step(upd) == SQLITE_DONE && sqlite3_changes(cms)) staled++;
                            }
                        }
                    }
                    sqlite3_finalize(q);
                }
            }
        }
        sqlite3_finalize(st); st = NULL;
    }
    /* Second pass: the estate compared against itself. Every delivered search
     * fragment is recomposed from the archive as it stands and checked against
     * the digest its receipt recorded. */
    {
        sqlite3 *items = NULL;
        char uri[PATH_MAX];
        if (items_path && *items_path &&
            snprintf(uri, sizeof(uri), "file:%s?mode=ro", items_path) < (int)sizeof(uri) &&
            sqlite3_open_v2(uri, &items, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) == SQLITE_OK &&
            sqlite3_prepare_v2(cms,
              "SELECT release_id, source_ref, COALESCE(receipt_ref,''), COALESCE(generation,'')"
              " FROM destination_release WHERE destination='search' AND state='delivered'"
              "   AND status='published'", -1, &st, NULL) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW) {
                const char *id = (const char *)sqlite3_column_text(st, 0);
                const char *url = (const char *)sqlite3_column_text(st, 1);
                const char *receipt = (const char *)sqlite3_column_text(st, 2);
                const char *gen = (const char *)sqlite3_column_text(st, 3);
                const ForeignTwinRelease *r = NULL;
                char now_hex[65];
                int rows_now = 0;
                for (i = 0; i < index.count; i++)
                    if (!strcmp(index.rows[i].url, url)) { r = &index.rows[i]; break; }
                if (!r) continue;
                recomposed++;
                if (!search_fragment_digest(items, url, r->source, gen, now_hex, &rows_now))
                    continue;
                if (!receipt[0] || !strncmp(receipt, now_hex, 64)) { matched++; continue; }
                printf("%s{\"source\":", printed_any ? "," : "");
                json_string(stdout, url);
                printf(",\"destination\":\"search\",\"field\":\"href\","
                       "\"consequence\":\"material\",\"receipt_digest\":");
                json_string(stdout, receipt);
                printf(",\"recomposes_to\":"); json_string(stdout, now_hex);
                printf(",\"items_now\":%d,\"was\":\"delivered\",\"becomes\":\"stale\"}", rows_now);
                printed_any = 1;
                drifted++;
                if (write && upd) {
                    char id_copy[96];
                    snprintf(id_copy, sizeof(id_copy), "%s", id);
                    sqlite3_reset(upd);
                    sqlite3_bind_text(upd, 1, now, -1, SQLITE_STATIC);
                    sqlite3_bind_text(upd, 2,
                      "recomposing this fragment from the archive no longer produces the bytes its "
                      "receipt recorded", -1, SQLITE_STATIC);
                    sqlite3_bind_text(upd, 3,
                      "stale is not withdrawn: the delivered fragment and its receipt both stand, and "
                      "this says only that the estate's own material has moved since",
                      -1, SQLITE_STATIC);
                    sqlite3_bind_text(upd, 4, id_copy, -1, SQLITE_TRANSIENT);
                    if (sqlite3_step(upd) == SQLITE_DONE && sqlite3_changes(cms)) staled++;
                }
            }
            sqlite3_finalize(st); st = NULL;
        }
        if (items) sqlite3_close(items);
    }

    printf("],\"deltas\":%d,\"invalidating\":%d,\"needs_review\":%d,"
           "\"deliveries_examined\":%d,\"staled\":%d,"
           "\"spared_field_not_carried\":%d,\"deltas_with_no_release\":%d,"
           "\"deliveries_recomposed\":%d,\"still_hash_to_their_receipt\":%d,\"drifted\":%d,"
           "\"cannot_infer\":\"a staled destination is not withdrawn and its receipt still stands; "
           "this compares the estate's own observations and re-reads no source. A destination is "
           "staled only by a change to a field it is declared to carry, so a delivery this leaves "
           "alone has not been judged current -- only unaffected by these deltas.\"}\n",
           deltas, invalidating, review, deliveries, staled, spared, unjoined,
           recomposed, matched, drifted);

    if (upd) sqlite3_finalize(upd);
    sqlite3_close(cms);
    foreign_release_index_free(&index);
    return 0;
}

/* ------------------------------------------------------------- the four cells
 *
 * Nine of the thirty interaction grammars could not be built because each needed
 * one of these and nothing held it. What follows is deliberately the smallest
 * honest version of each rather than a clone of the product that made it famous,
 * and each one's refusal is as load-bearing as what it does.
 */

/* A snapshot of a namespace, by digest.
 *
 * lakeFS branches an object estate; this does not, and the reason is in the
 * contract: a merge would have to reconstruct derived material this estate
 * cannot rebuild, and a tree assembled from two snapshots would have provenance
 * from neither. What is genuinely useful without that is a reading of what was
 * there, and a diff between two readings.
 */
static int version_walk(sqlite3 *cms, const char *version_id, const char *root,
                        const char *rel, const char *now,
                        long *files, long long *bytes, unsigned char tree[32],
                        sqlite3_stmt *ins, int depth) {
    char path[PATH_MAX];
    DIR *dir = NULL;
    struct dirent *ent;
    int rc = 0;
    if (depth > 12) return 0;
    if (snprintf(path, sizeof(path), "%s%s%s", root, rel[0] ? "/" : "", rel) >= (int)sizeof(path))
        return 0;
    if (!(dir = opendir(path))) return 0;
    while ((ent = readdir(dir)) != NULL) {
        char child_rel[PATH_MAX], child_abs[PATH_MAX];
        struct stat sb;
        if (ent->d_name[0] == '.') continue;   /* dotfiles are the tool's, not the estate's */
        if (snprintf(child_rel, sizeof(child_rel), "%s%s%s", rel, rel[0] ? "/" : "", ent->d_name)
              >= (int)sizeof(child_rel)) continue;
        if (snprintf(child_abs, sizeof(child_abs), "%s/%s", path, ent->d_name)
              >= (int)sizeof(child_abs)) continue;
        if (stat(child_abs, &sb) != 0) continue;
        if (S_ISDIR(sb.st_mode)) {
            rc |= version_walk(cms, version_id, root, child_rel, now, files, bytes, tree, ins, depth + 1);
            continue;
        }
        if (!S_ISREG(sb.st_mode) || sb.st_size > 64 * 1024 * 1024) continue;
        {
            FILE *f = fopen(child_abs, "rb");
            unsigned char *buf = NULL;
            char hex[65], id[160], ref[32], size_text[32];
            size_t got;
            if (!f) continue;
            buf = malloc((size_t)sb.st_size + 1);
            if (!buf) { fclose(f); continue; }
            got = fread(buf, 1, (size_t)sb.st_size, f);
            fclose(f);
            bf_sha256_hex(buf, got, hex);
            free(buf);
            /* The tree digest folds every path and its digest together, so two
             * snapshots of identical trees agree and any difference shows. */
            {
                char line[PATH_MAX + 80];
                unsigned char one[32];
                char onehex[65];
                size_t li = (size_t)snprintf(line, sizeof(line), "%s %s\n", hex, child_rel);
                bf_sha256_hex((const unsigned char *)line, li, onehex);
                /* XOR-fold so the order files are walked in cannot change the
                 * result: a tree digest that depended on readdir order would
                 * differ between two identical trees. */
                {
                    size_t k;
                    for (k = 0; k < 32; k++) {
                        unsigned v = 0;
                        sscanf(onehex + k * 2, "%2x", &v);
                        one[k] = (unsigned char)v;
                        tree[k] ^= one[k];
                    }
                }
            }
            (*files)++;
            *bytes += (long long)got;
            if (!ins) continue;
            release_ref(ref, sizeof(ref), version_id, child_rel);
            snprintf(id, sizeof(id), "vp-%s", ref);
            snprintf(size_text, sizeof(size_text), "%lld", (long long)got);
            sqlite3_reset(ins);
            sqlite3_bind_text(ins, 1, now, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 2, id, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 3, version_id, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 4, child_rel, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 5, hex, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 6, size_text, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 7,
              "a recorded digest is what this estate read at that moment; the bytes it names may "
              "since be gone, and nothing here can put them back", -1, SQLITE_STATIC);
            sqlite3_step(ins);
        }
    }
    closedir(dir);
    return rc;
}

static int cell_version_take(const char *cms_path, const char *root, const char *label,
                            const char *actor) {
    sqlite3 *cms = NULL; sqlite3_stmt *ins = NULL, *st = NULL;
    char now[32], version_id[160], ref[32], treehex[65];
    unsigned char tree[32];
    long files = 0; long long bytes = 0;
    time_t t = time(NULL);
    struct stat sb;
    size_t k;

    if (!cms_path || !*cms_path || !root || !*root || !label || !*label || !actor || !*actor) {
        fprintf(stderr, "version take: needs a record store, a namespace root, a label and an actor\n");
        return 1;
    }
    if (stat(root, &sb) != 0 || !S_ISDIR(sb.st_mode)) {
        printf("{\"schema\":\"bonfyre.cell.version.v1\",\"state\":\"refused\","
               "\"reason\":\"that namespace root is not a directory this estate can read\"}\n");
        return 1;
    }
    strftime(now, sizeof(now), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
    memset(tree, 0, sizeof(tree));
    if (sqlite3_open(cms_path, &cms) != SQLITE_OK) { if (cms) sqlite3_close(cms); return 1; }
    sqlite3_busy_timeout(cms, 5000);
    release_ref(ref, sizeof(ref), root, now);
    snprintf(version_id, sizeof(version_id), "ver-%s", ref);

    sqlite3_exec(cms, "BEGIN", NULL, NULL, NULL);
    if (sqlite3_prepare_v2(cms,
          "INSERT OR REPLACE INTO version_path(namespace,created_at,updated_at,published_at,status,"
          " path_id,version_id,path,digest,bytes,cannot_infer)"
          " VALUES('root',?1,?1,?1,'published',?2,?3,?4,?5,?6,?7)", -1, &ins, NULL) != SQLITE_OK) {
        fprintf(stderr, "version take: %s\n", sqlite3_errmsg(cms));
        sqlite3_exec(cms, "ROLLBACK", NULL, NULL, NULL);
        sqlite3_close(cms);
        return 1;
    }
    version_walk(cms, version_id, root, "", now, &files, &bytes, tree, ins, 0);
    sqlite3_finalize(ins); ins = NULL;

    for (k = 0; k < 32; k++) snprintf(treehex + k * 2, 3, "%02x", tree[k]);
    treehex[64] = '\0';

    if (!files) {
        sqlite3_exec(cms, "ROLLBACK", NULL, NULL, NULL);
        sqlite3_close(cms);
        printf("{\"schema\":\"bonfyre.cell.version.v1\",\"state\":\"refused\","
               "\"reason\":\"no readable file under that root, so there is nothing to snapshot\"}\n");
        return 1;
    }
    if (sqlite3_prepare_v2(cms,
          "INSERT OR REPLACE INTO estate_version(namespace,created_at,updated_at,published_at,status,"
          " version_id,namespace_root,label,tree_digest,file_count,total_bytes,taken_by,taken_at,cannot_infer)"
          " VALUES('root',?1,?1,?1,'published',?2,?3,?4,?5,?6,?7,?8,?1,?9)", -1, &st, NULL) == SQLITE_OK) {
        char fc[24], tb[32];
        snprintf(fc, sizeof(fc), "%ld", files);
        snprintf(tb, sizeof(tb), "%lld", bytes);
        sqlite3_bind_text(st, 1, now, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, version_id, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 3, root, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 4, label, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 5, treehex, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 6, fc, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 7, tb, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 8, actor, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 9,
          "a snapshot is a reading of what was there: not a backup, not a restore point, and it "
          "cannot be merged with another", -1, SQLITE_STATIC);
        sqlite3_step(st);
        sqlite3_finalize(st); st = NULL;
    }
    sqlite3_exec(cms, "COMMIT", NULL, NULL, NULL);
    sqlite3_close(cms);

    printf("{\"schema\":\"bonfyre.cell.version.v1\",\"state\":\"taken\",\"version\":");
    json_string(stdout, version_id);
    printf(",\"root\":"); json_string(stdout, root);
    printf(",\"label\":"); json_string(stdout, label);
    printf(",\"tree_digest\":"); json_string(stdout, treehex);
    printf(",\"files\":%ld,\"bytes\":%lld,\"taken_by\":", files, bytes);
    json_string(stdout, actor);
    printf(",\"cannot_infer\":\"a snapshot names bytes it read; it does not hold them. This is not a "
           "backup, cannot restore anything, and cannot be merged with another snapshot.\"}\n");
    return 0;
}

static int cell_version_diff(const char *cms_path, const char *from, const char *to) {
    sqlite3 *cms = NULL; sqlite3_stmt *st = NULL;
    long added = 0, removed = 0, changed = 0, unchanged = 0;
    int shown = 0;
    if (!cms_path || !*cms_path || !from || !to) return 1;
    if (sqlite3_open_v2(cms_path, &cms, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        if (cms) sqlite3_close(cms);
        return 1;
    }
    /* Two snapshots of one namespace only. Diffing across roots would compare
     * trees that were never the same thing. */
    if (sqlite3_prepare_v2(cms,
          "SELECT (SELECT namespace_root FROM estate_version WHERE version_id=?1),"
          " (SELECT namespace_root FROM estate_version WHERE version_id=?2)", -1, &st, NULL) == SQLITE_OK) {
        const char *ra = NULL, *rb = NULL;
        sqlite3_bind_text(st, 1, from, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, to, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            ra = (const char *)sqlite3_column_text(st, 0);
            rb = (const char *)sqlite3_column_text(st, 1);
        }
        if (!ra || !rb) {
            printf("{\"schema\":\"bonfyre.cell.version_diff.v1\",\"state\":\"refused\","
                   "\"reason\":\"one of those snapshots does not exist\"}\n");
            sqlite3_finalize(st); sqlite3_close(cms);
            return 1;
        }
        if (strcmp(ra, rb)) {
            printf("{\"schema\":\"bonfyre.cell.version_diff.v1\",\"state\":\"refused\","
                   "\"reason\":\"those snapshots are of different namespaces, which were never the "
                   "same tree to compare\"}\n");
            sqlite3_finalize(st); sqlite3_close(cms);
            return 1;
        }
        sqlite3_finalize(st); st = NULL;
    }

    printf("{\"schema\":\"bonfyre.cell.version_diff.v1\",\"from\":");
    json_string(stdout, from);
    printf(",\"to\":"); json_string(stdout, to);
    printf(",\"changes\":[");
    if (sqlite3_prepare_v2(cms,
          /* `later` is the snapshot being compared to, `earlier` the one being
           * compared from. Naming them rather than aliasing them a and b is why
           * this now reads correctly: the first version labelled a path missing
           * from the later snapshot as added. */
          "SELECT COALESCE(later.path, earlier.path),"
          " CASE WHEN earlier.path IS NULL THEN 'added'"
          "      WHEN later.path IS NULL THEN 'removed'"
          "      WHEN later.digest <> earlier.digest THEN 'changed' ELSE 'unchanged' END"
          " FROM (SELECT path, digest FROM version_path WHERE version_id=?2) later"
          " FULL OUTER JOIN (SELECT path, digest FROM version_path WHERE version_id=?1) earlier"
          "   ON later.path = earlier.path"
          " ORDER BY 2, 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, from, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, to, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *path = (const char *)sqlite3_column_text(st, 0);
            const char *change = (const char *)sqlite3_column_text(st, 1);
            if (!strcmp(change, "added")) added++;
            else if (!strcmp(change, "removed")) removed++;
            else if (!strcmp(change, "changed")) changed++;
            else { unchanged++; continue; }   /* an unchanged path is not news */
            if (shown < 40) {
                if (shown) fputc(',', stdout);
                printf("{\"path\":"); json_string(stdout, path);
                printf(",\"change\":"); json_string(stdout, change);
                printf("}");
                shown++;
            }
        }
        sqlite3_finalize(st); st = NULL;
    }
    sqlite3_close(cms);
    printf("],\"added\":%ld,\"removed\":%ld,\"changed\":%ld,\"unchanged\":%ld,\"shown\":%d,"
           "\"cannot_infer\":\"a diff compares two readings this estate took. It cannot merge them, "
           "cannot restore either, and says nothing about why a path changed.\"}\n",
           added, removed, changed, unchanged, shown);
    return 0;
}

/* A message, composed and held.
 *
 * Every product that needed this cell needed sending, and this refuses to send.
 * That is not a limitation to be fixed later by adding a transport: the estate's
 * standing rule is that reachability stops at the human line, and an outbox that
 * delivered would move the line while looking like a feature. So the record says
 * who would have to send it, and the state table has no state that claims
 * anything left.
 */
static int cell_message_compose(const char *cms_path, const char *about, const char *recipient,
                                const char *subject_line, const char *body, const char *actor) {
    sqlite3 *cms = NULL; sqlite3_stmt *st = NULL;
    char now[32], id[160], ref[32];
    time_t t = time(NULL);
    if (!cms_path || !*cms_path || !about || !recipient || !subject_line || !body ||
        !actor || !*actor) {
        fprintf(stderr, "message compose: needs a record store, a subject, a recipient, a line, a body"
                        " and whose composition it is (BONFYRE_ACTOR)\n");
        return 1;
    }
    strftime(now, sizeof(now), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
    if (sqlite3_open(cms_path, &cms) != SQLITE_OK) { if (cms) sqlite3_close(cms); return 1; }
    sqlite3_busy_timeout(cms, 5000);
    release_ref(ref, sizeof(ref), recipient, subject_line);
    snprintf(id, sizeof(id), "msg-%s", ref);
    if (sqlite3_prepare_v2(cms,
          "INSERT OR REPLACE INTO outbound_message(namespace,created_at,updated_at,published_at,"
          " status,message_id,about,recipient,subject_line,body,state,composed_by,composed_at,"
          " would_be_sent_by,state_reason,cannot_infer)"
          " VALUES('root',?1,?1,?1,'published',?2,?3,?4,?5,?6,'composed',?7,?1,?8,'',?9)",
          -1, &st, NULL) != SQLITE_OK) { sqlite3_close(cms); return 1; }
    sqlite3_bind_text(st, 1, now, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, about, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 4, recipient, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 5, subject_line, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 6, body, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 7, actor, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 8,
      "a person, using their own account and their own judgement, outside this estate",
      -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 9,
      "this estate has no transport. The message is written and held: it has not been sent, not "
      "delivered, and the recipient does not know it exists", -1, SQLITE_STATIC);
    sqlite3_step(st);
    sqlite3_finalize(st);
    sqlite3_close(cms);
    printf("{\"schema\":\"bonfyre.cell.message.v1\",\"state\":\"composed\",\"message\":");
    json_string(stdout, id);
    printf(",\"recipient\":"); json_string(stdout, recipient);
    printf(",\"composed_by\":"); json_string(stdout, actor);
    printf(",\"transmitted\":%s,\"would_be_sent_by\":",
           bf_message_transmitted("composed") ? "true" : "false");
    json_string(stdout, "a person, using their own account and their own judgement, outside this estate");
    printf(",\"cannot_infer\":\"nothing left this estate. Composing is not sending, and no state this "
           "record can hold claims otherwise.\"}\n");
    return 0;
}

/* Assent bound to exact bytes.
 *
 * A signature that survives a change to what was signed is worse than none, so
 * this is bound to a digest and reports itself broken when the bytes move. It is
 * not a legal signature and says so; what it is good for is the estate being
 * able to tell whether the thing somebody agreed to is the thing in front of you
 * now.
 */
static int cell_sign(const char *cms_path, const char *about, const char *file,
                     const char *statement, const char *actor) {
    sqlite3 *cms = NULL; sqlite3_stmt *st = NULL;
    char now[32], id[160], ref[32], hex[65];
    unsigned char *buf = NULL;
    struct stat sb;
    FILE *f = NULL;
    size_t got;
    time_t t = time(NULL);

    if (!cms_path || !*cms_path || !about || !file || !statement || !actor || !*actor) {
        fprintf(stderr, "sign: needs a record store, a subject, a file, a statement and whose assent"
                        " it is (BONFYRE_ACTOR)\n");
        return 1;
    }
    if (stat(file, &sb) != 0 || !S_ISREG(sb.st_mode) || sb.st_size > 64 * 1024 * 1024) {
        printf("{\"schema\":\"bonfyre.cell.signature.v1\",\"state\":\"refused\","
               "\"reason\":\"there is no readable file at that path to assent to\"}\n");
        return 1;
    }
    if (!(f = fopen(file, "rb"))) return 1;
    buf = malloc((size_t)sb.st_size + 1);
    if (!buf) { fclose(f); return 1; }
    got = fread(buf, 1, (size_t)sb.st_size, f);
    fclose(f);
    bf_sha256_hex(buf, got, hex);
    free(buf);

    strftime(now, sizeof(now), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
    if (sqlite3_open(cms_path, &cms) != SQLITE_OK) { if (cms) sqlite3_close(cms); return 1; }
    sqlite3_busy_timeout(cms, 5000);
    release_ref(ref, sizeof(ref), hex, actor);
    snprintf(id, sizeof(id), "sig-%s", ref);
    if (sqlite3_prepare_v2(cms,
          "INSERT OR REPLACE INTO assent_signature(namespace,created_at,updated_at,published_at,"
          " status,signature_id,about,digest,assented_by,assented_at,statement,cannot_infer)"
          " VALUES('root',?1,?1,?1,'published',?2,?3,?4,?5,?1,?6,?7)", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(cms);
        return 1;
    }
    sqlite3_bind_text(st, 1, now, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, about, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 4, hex, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 5, actor, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 6, statement, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 7,
      "this is not a legal signature, not notarised, not identity verification, and not evidence "
      "the signer read what they assented to", -1, SQLITE_STATIC);
    sqlite3_step(st);
    sqlite3_finalize(st);
    sqlite3_close(cms);
    printf("{\"schema\":\"bonfyre.cell.signature.v1\",\"state\":\"recorded\",\"signature\":");
    json_string(stdout, id);
    printf(",\"digest\":"); json_string(stdout, hex);
    printf(",\"assented_by\":"); json_string(stdout, actor);
    printf(",\"cannot_infer\":\"assent is bound to these exact bytes and dies with them. It is not a "
           "legal signature, not notarised, and not evidence the signer read what they assented "
           "to.\"}\n");
    return 0;
}

static int cell_sign_check(const char *cms_path, const char *signature_id, const char *file) {
    sqlite3 *cms = NULL; sqlite3_stmt *st = NULL;
    char recorded[80] = {0}, by[96] = {0}, at[64] = {0}, hex[65];
    unsigned char *buf = NULL;
    struct stat sb;
    FILE *f = NULL;
    size_t got;
    int intact;

    if (!cms_path || !signature_id || !file) return 1;
    if (sqlite3_open_v2(cms_path, &cms, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        if (cms) sqlite3_close(cms);
        return 1;
    }
    if (sqlite3_prepare_v2(cms, "SELECT digest, assented_by, assented_at FROM assent_signature"
                                " WHERE signature_id=?1 AND status='published'", -1, &st, NULL) != SQLITE_OK ||
        (sqlite3_bind_text(st, 1, signature_id, -1, SQLITE_STATIC), sqlite3_step(st)) != SQLITE_ROW) {
        if (st) sqlite3_finalize(st);
        sqlite3_close(cms);
        printf("{\"schema\":\"bonfyre.cell.signature_check.v1\",\"state\":\"refused\","
               "\"reason\":\"no such signature\"}\n");
        return 1;
    }
    snprintf(recorded, sizeof(recorded), "%s", (const char *)sqlite3_column_text(st, 0));
    snprintf(by, sizeof(by), "%s", (const char *)sqlite3_column_text(st, 1));
    snprintf(at, sizeof(at), "%s", (const char *)sqlite3_column_text(st, 2));
    sqlite3_finalize(st);
    sqlite3_close(cms);

    if (stat(file, &sb) != 0 || !S_ISREG(sb.st_mode) || !(f = fopen(file, "rb"))) {
        printf("{\"schema\":\"bonfyre.cell.signature_check.v1\",\"state\":\"broken\","
               "\"reason\":\"the file that was assented to cannot be read now\",\"signature\":");
        json_string(stdout, signature_id);
        printf(",\"assented_by\":"); json_string(stdout, by);
        printf(",\"cannot_infer\":\"a missing file does not withdraw the assent; it means nothing here "
               "can show what was assented to\"}\n");
        return 1;
    }
    buf = malloc((size_t)sb.st_size + 1);
    if (!buf) { fclose(f); return 1; }
    got = fread(buf, 1, (size_t)sb.st_size, f);
    fclose(f);
    bf_sha256_hex(buf, got, hex);
    free(buf);
    intact = !strcmp(hex, recorded);

    printf("{\"schema\":\"bonfyre.cell.signature_check.v1\",\"state\":%s,\"signature\":",
           intact ? "\"intact\"" : "\"broken\"");
    json_string(stdout, signature_id);
    printf(",\"assented_digest\":"); json_string(stdout, recorded);
    printf(",\"file_digest_now\":"); json_string(stdout, hex);
    printf(",\"assented_by\":"); json_string(stdout, by);
    printf(",\"assented_at\":"); json_string(stdout, at);
    printf(",\"cannot_infer\":\"%s\"}\n",
           intact
             ? "the bytes still hash to what was assented to. That is all: it is not a legal "
               "signature and says nothing about whether the signer was entitled to assent"
             : "the bytes changed since the assent was recorded, so the assent no longer applies to "
               "this file. It was not withdrawn; it was outlived");
    return intact ? 0 : 1;
}

/* A session that is live only while something keeps saying so.
 *
 * Everything that made streaming products worth cloning is the media path, and
 * this has none: no bytes flow through the estate. What it can honestly hold is
 * whether a session is still going, which is the one fact every surface around a
 * stream needs and the one most often faked. A session with no heartbeat inside
 * the window is not live, whatever its record says.
 */
#define LIVE_HEARTBEAT_WINDOW_SECONDS 120

static int cell_session(const char *cms_path, const char *action, const char *session_id,
                        const char *label, const char *actor, const char *replay_ref) {
    sqlite3 *cms = NULL; sqlite3_stmt *st = NULL;
    char now[32];
    time_t t = time(NULL);
    if (!cms_path || !*cms_path || !action || !session_id) return 1;
    strftime(now, sizeof(now), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
    if (sqlite3_open(cms_path, &cms) != SQLITE_OK) { if (cms) sqlite3_close(cms); return 1; }
    sqlite3_busy_timeout(cms, 5000);

    if (!strcmp(action, "open")) {
        if (!label || !*label || !actor || !*actor) {
            sqlite3_close(cms);
            fprintf(stderr, "session open: needs a label and a host (BONFYRE_ACTOR)\n");
            return 1;
        }
        if (sqlite3_prepare_v2(cms,
              "INSERT OR REPLACE INTO live_session(namespace,created_at,updated_at,published_at,"
              " status,session_id,label,host_actor,opened_at,last_heartbeat_at,ended_at,replay_ref,"
              " cannot_infer) VALUES('root',?1,?1,?1,'published',?2,?3,?4,?1,?1,'','',?5)",
              -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, now, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, session_id, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 3, label, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 4, actor, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 5,
              "no media flows through this estate. A session names where something is happening; it "
              "is not an audience, not a viewer count, and not a claim anybody is watching",
              -1, SQLITE_STATIC);
            sqlite3_step(st);
            sqlite3_finalize(st); st = NULL;
        }
    } else if (!strcmp(action, "beat")) {
        if (sqlite3_prepare_v2(cms, "UPDATE live_session SET last_heartbeat_at=?1, updated_at=?1"
                                    " WHERE session_id=?2 AND status='published' AND ended_at=''",
                               -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, now, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, session_id, -1, SQLITE_STATIC);
            sqlite3_step(st);
            if (!sqlite3_changes(cms)) {
                sqlite3_finalize(st);
                sqlite3_close(cms);
                printf("{\"schema\":\"bonfyre.cell.session.v1\",\"state\":\"refused\","
                       "\"reason\":\"no open session with that identifier; an ended session cannot be "
                       "made live again\"}\n");
                return 1;
            }
            sqlite3_finalize(st); st = NULL;
        }
    } else if (!strcmp(action, "end")) {
        if (sqlite3_prepare_v2(cms, "UPDATE live_session SET ended_at=?1, updated_at=?1,"
                                    " replay_ref=?3 WHERE session_id=?2 AND status='published'"
                                    " AND ended_at=''", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, now, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, session_id, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 3, replay_ref ? replay_ref : "", -1, SQLITE_STATIC);
            sqlite3_step(st);
            sqlite3_finalize(st); st = NULL;
        }
    } else if (strcmp(action, "status")) {
        sqlite3_close(cms);
        fprintf(stderr, "session: open, beat, end or status\n");
        return 1;
    }
    /* status falls through to the report below and writes nothing. Asking
     * whether a session is live had to beat it to find out, which meant every
     * reader kept it alive by looking. */

    /* Whatever the action, report what the record now supports rather than what
     * the caller asked for. */
    if (sqlite3_prepare_v2(cms,
          "SELECT label, host_actor, opened_at, COALESCE(last_heartbeat_at,''),"
          " COALESCE(ended_at,''), COALESCE(replay_ref,''),"
          " CAST(strftime('%s','now') - strftime('%s', COALESCE(NULLIF(last_heartbeat_at,''), opened_at)) AS INTEGER)"
          " FROM live_session WHERE session_id=?1 AND status='published'", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, session_id, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const char *ended = (const char *)sqlite3_column_text(st, 4);
            long since = (long)sqlite3_column_int64(st, 6);
            int live = !ended[0] && since <= LIVE_HEARTBEAT_WINDOW_SECONDS;
            printf("{\"schema\":\"bonfyre.cell.session.v1\",\"session\":");
            json_string(stdout, session_id);
            printf(",\"label\":"); json_string(stdout, (const char *)sqlite3_column_text(st, 0));
            printf(",\"host\":"); json_string(stdout, (const char *)sqlite3_column_text(st, 1));
            printf(",\"standing\":%s", live ? "\"live\"" : (ended[0] ? "\"ended\"" : "\"silent\""));
            printf(",\"seconds_since_heartbeat\":%ld,\"window_seconds\":%d",
                   since, LIVE_HEARTBEAT_WINDOW_SECONDS);
            printf(",\"replay\":"); json_string(stdout, (const char *)sqlite3_column_text(st, 5));
            printf(",\"cannot_infer\":\"live means something said so inside the window, and nothing "
                   "more: no media passes through this estate, and this is not an audience, a viewer "
                   "count, or a claim anybody is watching.\"}\n");
        }
        sqlite3_finalize(st); st = NULL;
    }
    sqlite3_close(cms);
    return 0;
}

/* ------------------------------------------------------- discovery sets
 *
 * Criteria that run, against 109,767 channels at 29,256 publishers.
 *
 * The estate's existing collections are frozen lists: a source_collection names
 * its members in a source_refs array somebody typed, so "the sources about X"
 * cannot grow, shrink, or be argued with, and nothing can say why a publisher is
 * in it. A working set is the other shape -- criteria evaluated against every
 * candidate, a verdict per candidate naming which criteria decided it, and a
 * person who accepts or rejects what the criteria proposed.
 *
 * Two things keep it from becoming a search box. First, a criterion may only
 * read a column declared for it in CUE, so a set cannot smuggle in a judgement
 * the estate holds no data for. Second, a run may only write the verdicts the
 * contract lets a machine write: it proposes, and accepting is somebody's act
 * with their name on it. The reachability is computed; it stops at the human
 * line.
 */
typedef struct {
    char kind[32];
    char value[192];
} DiscoveryCriterion;

#define DISCOVERY_CRITERIA_MAX 12

/* "host_suffix=.edu;publishes_plane=calendar;min_channels=3" */
static int discovery_parse(const char *text, DiscoveryCriterion *out, int cap,
                           char *why, size_t why_cap) {
    int n = 0;
    const char *p = text;
    if (!text || !*text) {
        snprintf(why, why_cap, "a set with no criteria would propose the whole estate");
        return -1;
    }
    while (*p && n < cap) {
        const char *eq = strchr(p, '=');
        const char *end = strchr(p, ';');
        size_t klen, vlen;
        if (!end) end = p + strlen(p);
        if (!eq || eq > end) {
            snprintf(why, why_cap, "each criterion is kind=value; '%.40s' is not", p);
            return -1;
        }
        klen = (size_t)(eq - p);
        vlen = (size_t)(end - eq - 1);
        if (klen == 0 || klen >= sizeof(out[0].kind) || vlen == 0 || vlen >= sizeof(out[0].value)) {
            snprintf(why, why_cap, "a criterion's kind or value is empty or too long");
            return -1;
        }
        memcpy(out[n].kind, p, klen); out[n].kind[klen] = '\0';
        memcpy(out[n].value, eq + 1, vlen); out[n].value[vlen] = '\0';
        if (!bf_criterion_kind(out[n].kind)) {
            snprintf(why, why_cap, "no criterion kind called '%s' is declared", out[n].kind);
            return -1;
        }
        n++;
        p = (*end == ';') ? end + 1 : end;
    }
    if (*p) { snprintf(why, why_cap, "more than %d criteria", cap); return -1; }
    return n;
}

/* One criterion becomes one comparison or EXISTS against the frontier. The
 * column comes from the compiled table and never from this function, so a
 * criterion cannot read a field nobody declared. */
static void discovery_sql_for(const DiscoveryCriterion *c, char *out, size_t cap) {
    const BfCriterionKind *k = bf_criterion_kind(c->kind);
    out[0] = '\0';
    if (!k) return;
    if (!strcmp(k->over, "root")) {
        if (!strcmp(k->how, "suffix"))
            snprintf(out, cap, "lower(r.%s) LIKE '%%' || lower(?) ", k->column);
        else if (!strcmp(k->how, "contains"))
            snprintf(out, cap, "lower(r.%s) LIKE '%%' || lower(?) || '%%' ", k->column);
        else if (!strcmp(k->how, "at_least"))
            snprintf(out, cap, "(SELECT COUNT(*) FROM channel c WHERE c.root_id = r.root_id)"
                               " >= CAST(? AS INTEGER) ");
        else if (!strcmp(k->how, "plane"))
            /* The same classifier the institution room counts planes with:
             * extension before path keyword, path only. */
            snprintf(out, cap,
              "EXISTS (SELECT 1 FROM channel c WHERE c.root_id = r.root_id AND ("
              " CASE"
              "  WHEN lower(c.exact_address) LIKE '%%.ics' THEN 'calendar'"
              "  WHEN lower(c.exact_address) LIKE '%%.pdf' OR lower(c.exact_address) LIKE '%%.doc'"
              "       OR lower(c.exact_address) LIKE '%%.docx' THEN 'document'"
              "  WHEN lower(c.exact_address) LIKE '%%.csv'"
              "       OR lower(c.exact_address) LIKE '%%.xlsx' THEN 'data'"
              "  WHEN lower(c.exact_address) LIKE '%%.rss' OR lower(c.exact_address) LIKE '%%.atom'"
              "       OR lower(c.exact_address) LIKE '%%.xml' THEN 'feed'"
              "  WHEN c.protocol = 'ics' THEN 'calendar'"
              "  WHEN c.protocol IN ('rss','atom','rss-xml','json_feed') THEN 'feed'"
              "  WHEN c.protocol IN ('dcat','socrata','ckan') THEN 'data'"
              "  WHEN lower(c.exact_address) LIKE '%%/calendar%%' THEN 'calendar'"
              "  WHEN lower(c.exact_address) LIKE '%%/agenda%%'"
              "       OR lower(c.exact_address) LIKE '%%/minutes%%' THEN 'document'"
              "  WHEN lower(c.exact_address) LIKE '%%/feed%%' THEN 'feed'"
              "  ELSE 'page' END) = lower(?)) ");
        return;
    }
    /* Over channel: an EXISTS, so a publisher qualifies when any of its
     * channels does. */
    if (!strcmp(k->how, "contains"))
        snprintf(out, cap, "EXISTS (SELECT 1 FROM channel c WHERE c.root_id = r.root_id"
                           " AND lower(c.%s) LIKE '%%' || lower(?) || '%%') ", k->column);
    else if (!strcmp(k->how, "equals"))
        snprintf(out, cap, "EXISTS (SELECT 1 FROM channel c WHERE c.root_id = r.root_id"
                           " AND lower(c.%s) = lower(?)) ", k->column);
    else if (!strcmp(k->how, "in_set"))
        snprintf(out, cap, "EXISTS (SELECT 1 FROM channel c WHERE c.root_id = r.root_id"
                           " AND instr(',' || lower(?) || ',', ',' || lower(c.%s) || ',') > 0) ",
                 k->column);
}

static int discovery_run(const char *cms_path, const char *frontier_path,
                         const char *set_id, long limit, int write) {
    sqlite3 *cms = NULL, *fr = NULL;
    sqlite3_stmt *st = NULL, *ins = NULL;
    DiscoveryCriterion crit[DISCOVERY_CRITERIA_MAX];
    char criteria[1024] = {0}, label[256] = {0}, why[256] = {0}, now[32];
    char sql[8192]; size_t sw = 0;
    int count = 0, shown = 0, matched = 0, wrote = 0, i;
    long considered = 0, total_matched = 0;
    time_t t = time(NULL);

    if (!cms_path || !*cms_path || !frontier_path || !*frontier_path || !set_id || !*set_id) {
        fprintf(stderr, "discovery run: needs a record store, a frontier and a set\n");
        return 1;
    }
    strftime(now, sizeof(now), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
    if (sqlite3_open(cms_path, &cms) != SQLITE_OK) {
        fprintf(stderr, "discovery run: cannot open the record store\n");
        if (cms) sqlite3_close(cms);
        return 1;
    }
    sqlite3_busy_timeout(cms, 5000);
    if (sqlite3_prepare_v2(cms, "SELECT criteria, label FROM discovery_set"
                                " WHERE set_id=?1 AND status='published'", -1, &st, NULL) != SQLITE_OK ||
        (sqlite3_bind_text(st, 1, set_id, -1, SQLITE_STATIC), sqlite3_step(st)) != SQLITE_ROW) {
        printf("{\"schema\":\"bonfyre.discovery.run.v1\",\"state\":\"refused\","
               "\"reason\":\"no published set with that identifier\"}\n");
        if (st) sqlite3_finalize(st);
        sqlite3_close(cms);
        return 1;
    }
    snprintf(criteria, sizeof(criteria), "%s", (const char *)sqlite3_column_text(st, 0));
    snprintf(label, sizeof(label), "%s", (const char *)sqlite3_column_text(st, 1));
    sqlite3_finalize(st); st = NULL;

    count = discovery_parse(criteria, crit, DISCOVERY_CRITERIA_MAX, why, sizeof(why));
    if (count <= 0) {
        printf("{\"schema\":\"bonfyre.discovery.run.v1\",\"state\":\"refused\",\"reason\":");
        json_string(stdout, why);
        printf("}\n");
        sqlite3_close(cms);
        return 1;
    }

    {
        char uri[PATH_MAX];
        if (snprintf(uri, sizeof(uri), "file:%s?mode=ro", frontier_path) >= (int)sizeof(uri) ||
            sqlite3_open_v2(uri, &fr, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
            fprintf(stderr, "discovery run: cannot read the frontier\n");
            if (fr) sqlite3_close(fr);
            sqlite3_close(cms);
            return 1;
        }
    }

    sw = (size_t)snprintf(sql, sizeof(sql),
      "SELECT r.host, COALESCE(NULLIF(r.name,''), r.host) FROM root r WHERE ");
    for (i = 0; i < count; i++) {
        char clause[3072];
        discovery_sql_for(&crit[i], clause, sizeof(clause));
        if (!clause[0]) {
            printf("{\"schema\":\"bonfyre.discovery.run.v1\",\"state\":\"refused\","
                   "\"reason\":\"criterion '%s' declares a test this evaluator cannot build\"}\n",
                   crit[i].kind);
            sqlite3_close(fr); sqlite3_close(cms);
            return 1;
        }
        sw += (size_t)snprintf(sql + sw, sizeof(sql) - sw, "%s%s", i ? "AND " : "", clause);
    }
    /* The page is bounded and the count is not. Reporting the row count of a
     * limited query as the number matched made a set that matched thousands
     * report exactly the limit, which reads as a measurement and is an artefact
     * of the page size. */
    {
        char count_sql[8192];
        snprintf(count_sql, sizeof(count_sql), "SELECT COUNT(*) FROM root r WHERE %s",
                 sql + strlen("SELECT r.host, COALESCE(NULLIF(r.name,''), r.host) FROM root r WHERE "));
        if (sqlite3_prepare_v2(fr, count_sql, -1, &st, NULL) == SQLITE_OK) {
            for (i = 0; i < count; i++)
                sqlite3_bind_text(st, i + 1, crit[i].value, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_ROW) total_matched = (long)sqlite3_column_int64(st, 0);
            sqlite3_finalize(st); st = NULL;
        }
    }
    sw += (size_t)snprintf(sql + sw, sizeof(sql) - sw, "ORDER BY r.host LIMIT %ld",
                           limit > 0 ? limit : 500);

    if (sqlite3_prepare_v2(fr, sql, -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "discovery run: %s\n", sqlite3_errmsg(fr));
        sqlite3_close(fr); sqlite3_close(cms);
        return 1;
    }
    for (i = 0; i < count; i++)
        sqlite3_bind_text(st, i + 1, crit[i].value, -1, SQLITE_TRANSIENT);

    if (write) {
        sqlite3_exec(cms, "BEGIN", NULL, NULL, NULL);
        /* A run may write only the verdicts a machine is allowed to write, and
         * may never disturb one a person has already decided. */
        if (sqlite3_prepare_v2(cms,
              "INSERT INTO discovery_candidate(namespace,created_at,updated_at,published_at,status,"
              " candidate_id,set_id,root_host,display_name,verdict,matched,failed_criterion,"
              " decided_by,decided_at,reason,evaluated_at,cannot_infer)"
              " VALUES('root',?1,?1,?1,'published',?2,?3,?4,?5,'pending',?6,'','','','',?1,?7)"
              " ON CONFLICT(namespace,candidate_id) DO UPDATE SET"
              "   updated_at=excluded.updated_at, evaluated_at=excluded.evaluated_at,"
              "   matched=excluded.matched"
              " WHERE discovery_candidate.verdict IN ('pending','missed')",
              -1, &ins, NULL) != SQLITE_OK) {
            fprintf(stderr, "discovery run: %s\n", sqlite3_errmsg(cms));
            sqlite3_exec(cms, "ROLLBACK", NULL, NULL, NULL);
            sqlite3_finalize(st);
            sqlite3_close(fr); sqlite3_close(cms);
            return 1;
        }
    }

    printf("{\"schema\":\"bonfyre.discovery.run.v1\",\"set\":");
    json_string(stdout, set_id);
    printf(",\"label\":"); json_string(stdout, label);
    printf(",\"mode\":"); json_string(stdout, write ? "written" : "preview");
    printf(",\"criteria\":[");
    for (i = 0; i < count; i++) {
        const BfCriterionKind *k = bf_criterion_kind(crit[i].kind);
        if (i) fputc(',', stdout);
        printf("{\"kind\":"); json_string(stdout, crit[i].kind);
        printf(",\"label\":"); json_string(stdout, k->label);
        printf(",\"over\":"); json_string(stdout, k->over);
        printf(",\"column\":"); json_string(stdout, k->column);
        printf(",\"value\":"); json_string(stdout, crit[i].value);
        printf(",\"cannot_infer\":"); json_string(stdout, k->cannot_infer);
        printf("}");
    }
    printf("],\"proposed\":[");
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *host = (const char *)sqlite3_column_text(st, 0);
        const char *name = (const char *)sqlite3_column_text(st, 1);
        matched++;
        if (shown < 25) {
            if (shown) fputc(',', stdout);
            printf("{\"host\":"); json_string(stdout, host ? host : "");
            printf(",\"name\":"); json_string(stdout, name ? name : "");
            printf("}");
            shown++;
        }
        if (write && ins) {
            char id[128], ref[32];
            release_ref(ref, sizeof(ref), set_id, host ? host : "");
            snprintf(id, sizeof(id), "cand-%s", ref);
            sqlite3_reset(ins);
            sqlite3_bind_text(ins, 1, now, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 2, id, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 3, set_id, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 4, host ? host : "", -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 5, name ? name : "", -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 6, criteria, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 7,
              "matching every criterion is a filter over facts this estate recorded: it is not "
              "semantic relevance, not a reading of the publisher, and not permission to fetch, "
              "contact or enrol anything", -1, SQLITE_STATIC);
            if (sqlite3_step(ins) == SQLITE_DONE) wrote++;
        }
    }
    sqlite3_finalize(st); st = NULL;
    if (ins) { sqlite3_finalize(ins); ins = NULL; }

    if (sqlite3_prepare_v2(fr, "SELECT COUNT(*) FROM root", -1, &st, NULL) == SQLITE_OK &&
        sqlite3_step(st) == SQLITE_ROW) considered = (long)sqlite3_column_int64(st, 0);
    if (st) { sqlite3_finalize(st); st = NULL; }

    if (write) {
        sqlite3_exec(cms, "COMMIT", NULL, NULL, NULL);
        if (sqlite3_prepare_v2(cms, "UPDATE discovery_set SET last_run_at=?1, updated_at=?1"
                                    " WHERE set_id=?2", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, now, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, set_id, -1, SQLITE_STATIC);
            sqlite3_step(st);
            sqlite3_finalize(st); st = NULL;
        }
    }

    {
        int human = 0;
        size_t v;
        for (v = 0; v < BF_CANDIDATE_VERDICT_COUNT; v++)
            if (!bf_candidate_verdicts[v].machine_may_set) human++;
        printf("],\"publishers_considered\":%ld,\"matched\":%ld,\"page_rows\":%d,\"shown\":%d,\"recorded\":%d,"
               "\"verdict_written\":\"pending\",\"verdicts_needing_a_person\":%d,"
               "\"page_is_bounded\":true,"
               "\"cannot_infer\":\"a run proposes and nothing more. Every candidate is written as "
               "pending, because %d of the declared verdicts may not be set by a machine: accepting "
               "or rejecting is somebody's act with their name on it. Matching is a filter over "
               "recorded facts, never semantic relevance, and never permission to fetch, read, "
               "contact or enrol anything.\"}\n",
               considered, total_matched, matched, shown, wrote, human, human);
    }
    sqlite3_close(fr);
    sqlite3_close(cms);
    return 0;
}

/* A person deciding. The criteria could not do this, and the contract says so. */
static int discovery_decide(const char *cms_path, const char *set_id, const char *host,
                            const char *verdict, const char *actor, const char *reason) {
    sqlite3 *cms = NULL; sqlite3_stmt *st = NULL;
    char id[128], ref[32], now[32];
    time_t t = time(NULL);
    if (!cms_path || !*cms_path || !set_id || !host || !verdict || !actor || !*actor) {
        fprintf(stderr, "discovery decide: needs a set, a publisher, a verdict, and whose decision"
                        " it is (BONFYRE_ACTOR)\n");
        return 1;
    }
    if (bf_verdict_machine_may_set(verdict)) {
        printf("{\"schema\":\"bonfyre.discovery.decide.v1\",\"state\":\"refused\",\"reason\":");
        json_string(stdout, "that verdict is one a run writes; a person's decision is accepted or rejected");
        printf(",\"verdict\":"); json_string(stdout, verdict);
        printf("}\n");
        return 1;
    }
    if (strcmp(verdict, "accepted") && strcmp(verdict, "rejected")) {
        printf("{\"schema\":\"bonfyre.discovery.decide.v1\",\"state\":\"refused\","
               "\"reason\":\"no such verdict is declared\"}\n");
        return 1;
    }
    strftime(now, sizeof(now), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
    if (sqlite3_open(cms_path, &cms) != SQLITE_OK) { if (cms) sqlite3_close(cms); return 1; }
    sqlite3_busy_timeout(cms, 5000);
    release_ref(ref, sizeof(ref), set_id, host);
    snprintf(id, sizeof(id), "cand-%s", ref);
    if (sqlite3_prepare_v2(cms,
          "UPDATE discovery_candidate SET verdict=?1, decided_by=?2, decided_at=?3,"
          " updated_at=?3, reason=?4 WHERE candidate_id=?5 AND status='published'",
          -1, &st, NULL) != SQLITE_OK) { sqlite3_close(cms); return 1; }
    sqlite3_bind_text(st, 1, verdict, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, actor, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, now, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 4, reason ? reason : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 5, id, -1, SQLITE_TRANSIENT);
    sqlite3_step(st);
    if (!sqlite3_changes(cms)) {
        printf("{\"schema\":\"bonfyre.discovery.decide.v1\",\"state\":\"refused\",\"reason\":"
               "\"no candidate for that publisher in that set; a run has to propose it first\"}\n");
        sqlite3_finalize(st); sqlite3_close(cms);
        return 1;
    }
    sqlite3_finalize(st);
    sqlite3_close(cms);
    printf("{\"schema\":\"bonfyre.discovery.decide.v1\",\"state\":\"recorded\",\"set\":");
    json_string(stdout, set_id);
    printf(",\"publisher\":"); json_string(stdout, host);
    printf(",\"verdict\":"); json_string(stdout, verdict);
    printf(",\"decided_by\":"); json_string(stdout, actor);
    printf(",\"cannot_infer\":\"accepting a publisher into a set is one person's judgement recorded "
           "with their name. It reads nothing, fetches nothing, contacts nobody, and is not a claim "
           "the publisher knows this set exists.\"}\n");
    return 0;
}

static int foreign_twins(const char *matrix_db, long page_offset, long page_limit,
                         const char *query, const char *expected_generation) {
    ForeignTwinReleaseIndex index;
    ForeignEconomicAlignment economics;
    BfMatrix *m = NULL;
    const char *cms_path = getenv("BONFYRE_CMS_DB");
    const char *frontier_path = getenv("BONFYRE_SOURCE_FRONTIER_DB");
    const char *items_path = getenv("BONFYRE_ITEM_ARCHIVE_DB");
    const char *prices_path = getenv("BONFYRE_PRICES_DB");
    int frontier_current = 0, archive_current = 0, listed = 0;
    int estate_total = 0, result_total = 0, result_seen = 0;
    int spatial_ready = 0, media_ready = 0, rights_ready = 0;
    int publisher_declared = 0, economic_observed = 0, program_input_bound = 0;
    char generation[32];
    size_t i;
    ForeignDeliveryStanding delivery;
    ForeignDeltaStanding delta;
    int delivering_states = 0;
    int have_matrix = (bf_matrix_open(matrix_db, &m, NULL) == 0 && m != NULL);

    foreign_delivery_standing(cms_path, &delivery);
    foreign_delta_standing(cms_path, &delta);
    for (i = 0; i < BF_DESTINATION_STATE_COUNT; i++)
        if (bf_destination_states[i].delivered) delivering_states++;

    foreign_release_index_load(&index, cms_path, frontier_path, items_path);
    foreign_economic_alignment_load(prices_path, &index, &economics);
    qsort(index.rows, index.count, sizeof(*index.rows), foreign_release_compare_url);
    foreign_release_generation(&index, generation);
    if (expected_generation && *expected_generation && strcmp(expected_generation, generation)) {
        printf("{\"schema\":\"bonfyre.foreign.twins.v2\",\"state\":\"snapshot_mismatch\",\"expected_generation\":");
        json_string(stdout, expected_generation);
        printf(",\"catalogue_generation\":"); json_string(stdout, generation);
        printf(",\"cannot_infer\":\"a changed local release generation does not identify which remote fact changed, only that this offset cannot safely continue the prior working set\"}\n");
        if (m) bf_matrix_close(m);
        foreign_release_index_free(&index);
        return 0;
    }
    /* Summaries are whole-estate facts, never properties of the requested
     * page.  That lets a 200-row carrier remain small without turning 200
     * sources into a false census of the estate. */
    for (i = 0; i < index.count; i++) {
        ForeignTwinRelease *release = &index.rows[i];
        if (!release->source_contracts) continue;
        estate_total++;
        if (release->frontier_admitted) frontier_current++;
        if (release->archive_items) archive_current++;
        if (release->place_field[0] && release->archive_items) spatial_ready++;
        if (release->media_field[0] && release->archive_items) media_ready++;
        if (release->rights_field[0] && release->archive_items) rights_ready++;
        if (release->publisher_ident[0]) publisher_declared++;
        if (release->economic_observations) economic_observed++;
        if (release->public_program_bindings) program_input_bound++;
    }
    for (i = 0; i < index.count; i++) {
        ForeignTwinRelease *release = &index.rows[i];
        if (release->source_contracts && foreign_release_matches_query(release, query)) result_total++;
    }
    if (page_offset < 0) page_offset = 0;
    if (page_offset > result_total) page_offset = result_total;
    if (page_limit < 0) page_limit = 0;
    printf("{\"schema\":\"bonfyre.foreign.twins.v2\",\"state\":\"current\",\"catalogue_generation\":");
    json_string(stdout, generation);
    printf(",\"offset\":%ld,\"limit\":%ld,\"query\":",
           page_offset, page_limit);
    json_string(stdout, query ? query : "");
    printf(",\"total\":%d,\"estate_total\":%d,\"twins\":[", result_total, estate_total);
    for (i = 0; i < index.count; i++) {
        ForeignTwinRelease *release = &index.rows[i];
        char sid[600];
        int promoted = 0, claims = 0;
        if (!release->source_contracts || !foreign_release_matches_query(release, query)) continue;
        if (result_seen++ < page_offset) continue;
        if (page_limit && listed >= page_limit) continue;
        foreign_subject_for_url(release->url, sid, sizeof(sid));
        if (have_matrix) {
            promoted = bf_matrix_subject_exists(m, sid) == 1;
            claims = bf_matrix_subject_relation_from_count(m, sid, "claimed_by");
            if (claims < 0) claims = 0;
        }
        printf("%s{\"subject_id\":", listed ? "," : "");
        json_string(stdout, sid);
        printf(",\"url\":"); json_string(stdout, release->url);
        printf(",\"observations\":0,\"content_changes\":0,\"ever_retained\":false,"
               "\"promoted\":%s,\"claims\":%d,\"latest_status\":0,"
               "\"latest_kind\":\"receipt_not_enumerated\",\"last_seen\":",
               promoted ? "true" : "false", claims);
        json_string(stdout, release->last_seen);
        printf(",\"receipt_projection\":\"not_enumerated\",");
        foreign_release_json(release);
        printf("}");
        listed++;
    }
    printf("],\"count\":%d,\"next_offset\":", listed);
    if (page_limit && page_offset + listed < result_total) printf("%ld", page_offset + listed);
    else printf("null");
    printf(",\"release_summary\":{\"source_contracts\":%d,"
           "\"frontier_current\":%d,\"archive_current\":%d,\"public_program_bound\":0,"
           "\"public_program_input_bound\":%d,\"public_program_delivery_verified\":%d,"
           "\"readiness\":{\"spatial_ready_for_extraction\":%d,"
           "\"media_ready_for_extraction\":%d,\"rights_ready_for_extraction\":%d,"
           "\"publisher_declared\":%d,\"economic_observed\":%d,"
           "\"public_program_input_bound\":%d,\"public_program_delivery_verified\":%d}},",
           estate_total, frontier_current, archive_current, program_input_bound,
           delivery.receipted,
           spatial_ready, media_ready, rights_ready, publisher_declared, economic_observed,
           program_input_bound, delivery.receipted);
    /* Readiness and delivery, side by side and computed the same way, because
     * the whole defect this replaces was a readiness figure with an unbacked
     * zero beside it. */
    printf("\"destination_releases\":{\"state\":");
    json_string(stdout, delivery.store_present ? "projected" : "no_release_store");
    printf(",\"rows\":%d,\"delivered\":%d,\"delivered_with_receipt\":%d,"
           "\"open\":%d,\"blocked\":%d,\"failed\":%d,\"stale\":%d,"
           "\"unrecognised_state\":%d,\"lifecycle_states\":%d,\"delivering_states\":%d,"
           "\"cannot_infer\":\"a delivered destination is not a reuse licence, not the "
           "destination agreeing the material is correct, and not authority to publish it again; "
           "a state this lifecycle does not declare is counted as nothing rather than as pending\"},",
           delivery.rows, delivery.delivered, delivery.receipted, delivery.open,
           delivery.blocked, delivery.failed, delivery.stale, delivery.unrecognised,
           (int)BF_DESTINATION_STATE_COUNT, delivering_states);
    printf("\"source_deltas\":{\"state\":");
    json_string(stdout, delta.store_present ? "projected" : "no_delta_store");
    printf(",\"rows\":%d,\"invalidating\":%d,\"needs_review\":%d,\"cosmetic\":%d,"
           "\"unrecognised_consequence\":%d,"
           "\"cannot_infer\":\"a delta is a statement about the fields this estate extracts, "
           "never about the source's own intent; a cosmetic delta is not evidence the source did "
           "nothing, and no delta alters what was already observed\"},",
           delta.rows, delta.invalidating, delta.needs_review, delta.cosmetic,
           delta.unrecognised);
    printf("\"economic_observations\":{\"state\":");
    json_string(stdout, prices_path && *prices_path ?
                (economics.unbound_source_identities ? "partially_unbound" : "bound") : "not_projected");
    printf(",\"observations\":%lld,\"source_identities\":%d,\"bound_source_contracts\":%d,"
           "\"unbound_source_identities\":%d,\"latest_observed_at\":",
           economics.observations, economics.source_identities,
           economics.bound_source_contracts, economics.unbound_source_identities);
    json_string(stdout, economics.latest_observed_at);
    printf(",\"cannot_infer\":\"an observed amount is not a current offer, price available to this viewer, purchase authority, payment, funding commitment, or settlement\"},"
           "\"receipt_ledger\":{\"state\":\"separate_forensic_projection\","
           "\"access\":\"foreign twins --receipts | foreign history <url>\"},"
           "\"forbidden_inference\":\"a source contract, admitted channel, or archived item is not a public-program binding, reuse licence, external authority, or proof that every receipt was enumerated here\"}\n");
    if (m) bf_matrix_close(m);
    foreign_release_index_free(&index);
    return 0;
}

/* Promotion: an observed resource becomes a subject the institution can address.
 *
 * Observation and subjecthood are deliberately different acts. Fetching bytes
 * from a URL says only that something answered; it does not make that thing a
 * participant in this institution, and treating every fetch as a subject would
 * fill the Matrix with sitemaps and favicons. Promotion is the act that says
 * this observed thing is one we intend to refer to.
 *
 * What makes it safe is that it is evidence-bound rather than asserted: the
 * subject's identity digest is the content digest we actually retained, and its
 * evidence_ref names the receipt file that retained it. A twin that has never
 * yielded bytes cannot be promoted, because there would be nothing to identify
 * it by -- an identity whose digest we invented would be a claim wearing the
 * shape of an observation.
 *
 * Idempotent: promoting twice re-binds the same identity from the same
 * evidence, so a promotion run can be repeated over a growing receipt corpus.
 *
 * Cannot infer: a promoted twin -> a claimed, verified, or owned one. It is a
 * thing we observed and chose to be able to name. */
/* Which closure an observation belongs to, from where its receipt lives.
 *
 * Public observations sit under <base>/receipts. A private one sits under
 * <base>/private/<estate>/<scope> or <base>/private/self/actor-N, and the
 * contract is explicit that a sibling closure cannot enumerate it.
 *
 * Promotion had no notion of this: it walked one directory and bound a public
 * LIVE presence for everything it found. Pointed at a private root -- by an
 * operator, or by a changed environment variable -- it would have published
 * observations an estate took privately. No private observation exists on this
 * estate yet, which is the only reason that is a hazard rather than an incident.
 */
static int foreign_closure_of(const char *dir, char *estate, size_t estate_sz) {
    const char *priv = strstr(dir, "/private/");
    estate[0] = '\0';
    if (!priv) return 0;                       /* public */
    priv += strlen("/private/");
    if (!strncmp(priv, "self/", 5)) return 2;  /* an actor's own closure */
    {
        size_t n = 0;
        while (priv[n] && priv[n] != '/' && n + 1 < estate_sz) { estate[n] = priv[n]; n++; }
        estate[n] = '\0';
    }
    return 1;                                  /* an institution's closure */
}

static int foreign_promote(const char *dir, const char *matrix_db, const char *only,
                           int dry_run, int quiet) {
    ForeignObs *rows = NULL;
    int n = load_observations(dir, &rows), i, promoted = 0, skipped = 0, unretained = 0, drifted = 0;
    BfMatrix *m = NULL;

    if (n < 0) { fprintf(stderr, "cannot read observations from %s\n", dir); return 2; }
    {
        char pest[128];
        if (foreign_closure_of(dir, pest, sizeof(pest))) {
            fprintf(stderr,
                "refusing to promote from a private closure (%s) into public subjects.\n"
                "A private observation belongs to the estate or actor that took it, and a\n"
                "sibling closure cannot enumerate it; publishing one here would make it\n"
                "readable by everything that can read the Matrix.\n", dir);
            free(rows);
            return 2;
        }
    }
    if (!dry_run && (bf_matrix_open(matrix_db, &m, stderr) || !m)) {
        fprintf(stderr, "cannot open matrix at %s\n", matrix_db);
        free(rows);
        return 2;
    }

    if (!quiet) printf("{\"schema\":\"bonfyre.foreign.promote.v1\",\"promoted\":[");
    for (i = 0; i < n; i++) {
        /* One subject per URL, identified by the most recent observation that
         * actually retained bytes. Walking to the end of this URL's run first
         * means a later boundary response does not erase an earlier identity. */
        int j = i, best = -1, k;
        char twin[512], sid[600], address[700];
        while (j + 1 < n && !strcmp(rows[j + 1].url, rows[i].url)) j++;
        for (k = i; k <= j; k++) if (rows[k].retained) best = k;

        if (best < 0) { unretained++; i = j; continue; }
        if (only && *only && !foreign_same_resource(rows[i].url, only)) { i = j; continue; }
        /* The receipt says what the bytes hashed to when they were retained. A
         * body that no longer hashes to that is no longer the thing the receipt
         * describes, and an identity derived from it would be attested by a
         * receipt for different content. The estate already holds this rule for
         * parsing a retained body; it applies at least as strongly to deriving
         * an identity from one. */
        /* Absent and changed are different facts.
         *
         * A body present and hashing differently is drift: the bytes are not
         * what the receipt attests, and an identity taken from them would be
         * wrong. A body that is simply gone was cooled under a regime that said
         * not to keep it, and the receipt still carries the digest that
         * identifies the thing -- so promotion proceeds on the evidence, which
         * is what a receipt is for. Conflating the two made every lawfully
         * cooled twin look tampered with. */
        if (rows[best].body_path[0] && access(rows[best].body_path, R_OK) == 0) {
            char nowd[65];
            if (sha256_file(rows[best].body_path, nowd) != 0 ||
                strcmp(nowd, rows[best].digest)) {
                fprintf(stderr, "[foreign] %s: retained body no longer hashes to its "
                                "receipt; not promoting\n", rows[i].url);
                drifted++;
                i = j;
                continue;
            }
        }

        /* A stable twin id from the URL: the address is how a person names the
         * thing, so it is what the identity is derived from -- not the receipt
         * filename, which changes every observation. */
        {
            const char *u = rows[i].url;
            size_t w = 0;
            const char *p = strstr(u, "://");
            p = p ? p + 3 : u;
            for (; *p && w + 1 < sizeof(twin); p++) {
                if (isalnum((unsigned char)*p)) twin[w++] = (char)tolower((unsigned char)*p);
                else if (w && twin[w - 1] != '-') twin[w++] = '-';
            }
            while (w && twin[w - 1] == '-') w--;
            twin[w] = '\0';
        }
        snprintf(sid, sizeof(sid), "foreign_twin:%.200s", twin);
        snprintf(address, sizeof(address), "%s", rows[i].url);

        if (!dry_run) {
            BfMatrixSubjectSpec spec = { sid, "ForeignTwin", rows[best].digest, "" };
            char ev[256];
            snprintf(ev, sizeof(ev), "observation:%.32s:%.40s", rows[best].digest, rows[best].at);
            spec.evidence_ref = ev;
            if (bf_matrix_subject_bind(m, &spec) != BF_MATRIX_OK) { skipped++; i = j; continue; }
            /* The observed URL is the twin's address: the thing is reachable at
             * the place we observed it, and that is the whole claim. */
            (void)bf_matrix_address_bind(m, sid, "web_url", address, ev);
            /* No presence is bound here.
             *
             * Registration establishes that a stable foreign identity exists.
             * It does not make the thing present: not an active public
             * presence, not a partner, provider, opportunity or offer. Binding
             * one at registration is how every observed favicon and parcel
             * source acquired an active projection into the live world merely
             * by having been fetched once.
             *
             * Activation is a separate act, because deciding that something
             * external is now *present* in this institution's world is a
             * decision, not a side effect of having looked at it. */
        }
        if (!quiet)
            printf("%s{\"subject_id\":\"%s\",\"url\":\"%s\",\"observations\":%d,"
                   "\"identified_by\":\"%.16s\",\"observed_at\":\"%s\"}",
                   promoted ? "," : "", sid, rows[i].url, j - i + 1,
                   rows[best].digest, rows[best].at);
        promoted++;
        i = j;
    }
    if (!quiet)
        printf("],\"promoted_count\":%d,\"skipped\":%d,\"never_retained_content\":%d,"
               "\"body_drifted\":%d,\"dry_run\":%s,"
               "\"forbidden_inference\":\"a promoted twin is not a claimed, verified or owned "
               "one; it is a thing we observed and chose to be able to name\"}\n",
               promoted, skipped, unretained, drifted, dry_run ? "true" : "false");

    if (m) bf_matrix_close(m);
    free(rows);
    return 0;
}



/* One address for one resource.
 *
 * Resolution compared the requested URL to the stored one with strcmp, so a
 * reference that differed by a tracking parameter, a default port, a fragment,
 * an upper-case host or a missing trailing slash at the root resolved to
 * nothing. Measured over eight subjects and their lawful deformations, that was
 * 8 closures out of 41: the institution held the resource and answered that it
 * did not.
 *
 * Every fold below is either required by RFC 3986 or declared here as policy:
 *
 *   scheme, host       lowercased          RFC 3986 3.1, 3.2.2 -- case-insensitive
 *   default port       removed             carries no information
 *   empty path -> "/"                      RFC 3986 6.2.3 -- equivalent for http(s)
 *   fragment           removed             never sent to the server
 *   tracking params    removed             POLICY, not a standard; a server may
 *                                          treat them as significant, so this is
 *                                          a declared choice and not a silent one
 *
 * The path is never folded. Paths are case-sensitive, and "/a/" is not "/a" once
 * the path is non-empty: a server may lawfully serve different bytes at each.
 * A canonicalizer that folds those merges two resources, which is worse than
 * failing to close -- a miss is visible, a false merge is a wrong answer wearing
 * a right answer's clothes.
 *
 * Cannot infer: two addresses that canonicalize alike are the same resource.
 * This is a strong heuristic over addressing, not proof of identity; identity is
 * settled by the retained bytes. */
static const char *FOREIGN_TRACKING_PARAMS[] = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "fbclid", "gclid", "msclkid", "mc_cid", "mc_eid",
    "igshid", "ref_src", "ref_url", "_ga", "yclid", NULL
};

static int foreign_param_is_tracking(const char *key, size_t len) {
    int i;
    for (i = 0; FOREIGN_TRACKING_PARAMS[i]; i++) {
        size_t klen = strlen(FOREIGN_TRACKING_PARAMS[i]);
        if (klen == len && !strncasecmp(key, FOREIGN_TRACKING_PARAMS[i], len)) return 1;
    }
    return 0;
}

static void foreign_canonical_url(const char *in, char *out, size_t cap) {
    char work[2048];
    char *host_start, *path_start, *query, *frag, *colon;
    size_t n;
    out[0] = '\0';
    if (!in || !*in) return;
    if (snprintf(work, sizeof(work), "%s", in) >= (int)sizeof(work)) {
        snprintf(out, cap, "%s", in);
        return;
    }
    /* A fragment is never sent to the server. */
    frag = strchr(work, '#');
    if (frag) *frag = '\0';

    host_start = strstr(work, "://");
    if (!host_start) { snprintf(out, cap, "%s", work); return; }
    /* Scheme is case-insensitive. */
    for (char *c = work; c < host_start; c++) *c = (char)tolower((unsigned char)*c);
    host_start += 3;

    path_start = strchr(host_start, '/');
    /* Host is case-insensitive; the path is emphatically not. */
    for (char *c = host_start; *c && (!path_start || c < path_start); c++)
        *c = (char)tolower((unsigned char)*c);

    /* A default port carries no information. */
    colon = NULL;
    for (char *c = host_start; *c && (!path_start || c < path_start); c++)
        if (*c == ':') colon = c;
    if (colon) {
        const char *port = colon + 1;
        size_t plen = path_start ? (size_t)(path_start - port) : strlen(port);
        int is_default =
            (!strncmp(work, "http://", 7)  && plen == 2 && !strncmp(port, "80", 2)) ||
            (!strncmp(work, "https://", 8) && plen == 3 && !strncmp(port, "443", 3));
        if (is_default) memmove(colon, colon + 1 + plen, strlen(colon + 1 + plen) + 1);
    }

    query = strchr(host_start, '?');
    if (query) *query++ = '\0';

    n = (size_t)snprintf(out, cap, "%s", work);
    /* RFC 3986 6.2.3: an empty path and "/" are equivalent for http(s). */
    if (!strchr(host_start, '/') && n + 1 < cap) { out[n++] = '/'; out[n] = '\0'; }

    if (query && *query) {
        int first = 1;
        char *save = NULL, *pair;
        for (pair = strtok_r(query, "&", &save); pair; pair = strtok_r(NULL, "&", &save)) {
            const char *eq = strchr(pair, '=');
            size_t klen = eq ? (size_t)(eq - pair) : strlen(pair);
            if (foreign_param_is_tracking(pair, klen)) continue;
            n += (size_t)snprintf(out + n, cap - n, "%s%s", first ? "?" : "&", pair);
            first = 0;
            if (n >= cap - 1) break;
        }
    }
}

/* Two references to compare, folded the same way before comparing. */
static int foreign_same_resource(const char *a, const char *b) {
    char ca[2048], cb[2048];
    if (!a || !b) return 0;
    if (!strcmp(a, b)) return 1;
    foreign_canonical_url(a, ca, sizeof(ca));
    foreign_canonical_url(b, cb, sizeof(cb));
    return ca[0] && cb[0] && !strcmp(ca, cb);
}

static int foreign_resolution(const char *dir, const char *mode, const char *want_url) {
    ForeignObs *rows = NULL;
    int n = load_observations(dir, &rows), i, printed = 0;
    if (n < 0) { fprintf(stderr, "cannot read observations from %s\n", dir); return 2; }

    if (!strcmp(mode, "history") || !strcmp(mode, "resolve")) {
        int start = -1, end = -1, content = -1, availability = -1;
        for (i = 0; i < n; i++) {
            if (want_url && !foreign_same_resource(rows[i].url, want_url)) continue;
            if (start < 0) start = i;
            end = i;
            if (rows[i].retained) content = i;
            availability = i;
        }
        if (start < 0) {
            printf("{\"source_url\":\"%s\",\"observations\":0,"
                   "\"note\":\"no observation of this resource has been recorded\"}\n",
                   want_url ? want_url : "");
            free(rows);
            return 1;
        }
        printf("{\"source_url\":\"%s\",\"observations\":%d,", rows[start].url, end - start + 1);
        printf("\"current_content\":");
        if (content >= 0) print_obs(&rows[content], NULL, 1);
        else printf("null");
        printf(",\"current_availability\":");
        print_obs(&rows[availability], NULL, 1);
        /* The disagreement worth naming: bytes we still hold from a resource
         * that has since stopped answering.
         *
         * Null when no observation ever retained bytes. Reporting false there
         * would say "the content we hold is unreachable" about content that was
         * never held -- a different and much more alarming claim than "this
         * resource has only ever answered with a boundary response". */
        printf(",\"content_still_reachable\":%s",
               content < 0 ? "null" : (content == availability ? "true" : "false"));
        if (!strcmp(mode, "history")) {
            printf(",\"history\":[");
            for (i = start; i <= end; i++)
                print_obs(&rows[i], i > start ? relation_between(&rows[i-1], &rows[i]) : NULL, i == start);
            printf("]");
        }
        printf(",\"forbidden_inference\":\"a current observation is not a true statement "
               "about the world; it reports what was observed and when\"}\n");
        free(rows);
        return 0;
    }

    if (!strcmp(mode, "conflicts")) {
        printf("{\"conflicts\":[");
        for (i = 0; i < n; i++) {
            int j = i, content = -1, availability = -1, changes = 0;
            while (j + 1 < n && !strcmp(rows[j+1].url, rows[i].url)) j++;
            {
                int k;
                for (k = i; k <= j; k++) {
                    if (rows[k].retained) content = k;
                    availability = k;
                    if (k > i && !strcmp(relation_between(&rows[k-1], &rows[k]), "supersedes")) changes++;
                }
            }
            if (j > i && (changes > 0 || (content >= 0 && content != availability))) {
                printf("%s{\"source_url\":\"%s\",\"observations\":%d,\"content_changes\":%d,"
                       "\"content_still_reachable\":%s,\"ever_retained_content\":%s,"
                       "\"latest_kind\":\"%s\",\"latest_status\":%d}",
                       printed ? "," : "", rows[i].url, j - i + 1, changes,
                       content < 0 ? "null" : (content == availability ? "true" : "false"),
                       content < 0 ? "false" : "true",
                       rows[availability].kind, rows[availability].http_status);
                printed++;
            }
            i = j;
        }
        printf("],\"count\":%d,\"note\":\"a conflict is a resource whose observations "
               "disagree: its content changed, or bytes are held that it no longer serves\"}\n",
               printed);
        free(rows);
        return 0;
    }

    free(rows);
    fprintf(stderr, "unknown resolution mode: %s\n", mode);
    return 2;
}

static void write_policy_receipt(FILE *receipt, const char *twin_id, const char *url,
                                 const char *observed_at, const char *observe_right,
                                 const char *robots_url, int robots_exit, int robots_status,
                                 const char *policy_decision, int fetch_attempted,
                                 const char *observation_kind, int transport_exit,
                                 int http_status, long long body_bytes,
                                 const char *digest, const char *retain_body_path,
                                 const struct BfHttpEnvelope *envelope,
                                 const char *unchanged_from_digest) {
    fputs("{\"schema\":\"bonfyre-foreign-observation.v1\",\"twin_id\":", receipt);
    json_string(receipt, twin_id);
    fputs(",\"source_url\":", receipt); json_string(receipt, url);
    fputs(",\"observed_at\":", receipt); json_string(receipt, observed_at);
    fputs(",\"observe_right\":", receipt); json_string(receipt, observe_right);
    fputs(",\"robots_url\":", receipt);
    if (robots_url) json_string(receipt, robots_url); else fputs("null", receipt);
    fprintf(receipt, ",\"robots_transport_exit\":%d,\"robots_http_status\":%d", robots_exit, robots_status);
    fputs(",\"policy_decision\":", receipt); json_string(receipt, policy_decision);
    fputs(fetch_attempted ? ",\"fetch_attempted\":true" : ",\"fetch_attempted\":false", receipt);
    fprintf(receipt, ",\"transport_exit\":%d,\"http_status\":%d,\"body_bytes\":%lld,\"content_sha256\":",
            transport_exit, http_status, body_bytes);
    if (digest) json_string(receipt, digest); else fputs("null", receipt);
    if (retain_body_path && fetch_attempted) {
        fputs(",\"body_path\":", receipt); json_string(receipt, retain_body_path);
    }
    if (unchanged_from_digest && *unchanged_from_digest) {
        /* A 304 is a new observation at a new knowledge time over the same
         * representation. Retaining the bytes again would store a duplicate and
         * claim a second body where the source said there was one. */
        fputs(",\"unchanged_from_content_sha256\":", receipt);
        json_string(receipt, unchanged_from_digest);
        fputs(",\"conditional\":true", receipt);
    }
    if (envelope) write_http_envelope(receipt, envelope);
    fputs(",\"availability_claim\":false,\"observation_kind\":", receipt);
    json_string(receipt, observation_kind);
    fputs("}\n", receipt);
}

/* A retained body is read far more often than it changes, and most refetches
 * of a feed return the same bytes. Settling one into place therefore:
 *   - keeps the file already there when the new bytes are identical, so an
 *     unchanged feed costs no write and keeps whatever storage it settled into;
 *   - on APFS, stores a changed body of 64 KiB or more with transparent
 *     compression. The path and every byte a reader sees are the same; only
 *     the blocks on disk shrink. Elsewhere, or if compression fails, the plain
 *     file is moved into place exactly as before.
 * This is the write-time half of scripts/reclaim_space.mjs: a body settled
 * here never needs reclaiming later. Returns rename's result. */
static int settle_retained_body(const char *staged, const char *retained, long long bytes,
                                const char *digest, const char *prior_digest) {
    struct stat current;
    if (digest && digest[0] && prior_digest && prior_digest[0] && strcmp(digest, prior_digest) == 0 &&
        stat(retained, &current) == 0 && (long long)current.st_size == bytes) {
        int settled = 1;
#ifdef __APPLE__
#ifndef UF_COMPRESSED
#define UF_COMPRESSED 0x00000020 /* <sys/stat.h>, hidden under _POSIX_C_SOURCE */
#endif
        /* A body retained before this rule is still raw; its next identical
         * refetch replaces it once with the same bytes, compressed. */
        settled = bytes < 64 * 1024 || (current.st_flags & UF_COMPRESSED);
#endif
        if (settled) {
            (void)remove(staged);
            return 0;
        }
    }
#ifdef __APPLE__
    if (bytes >= 64 * 1024) {
        char packed[PATH_MAX];
        pid_t pid;
        int status = 0;
        if (snprintf(packed, sizeof(packed), "%s.afsc", staged) < (int)sizeof(packed)) {
            pid = fork();
            if (pid == 0) {
                execl("/usr/bin/ditto", "ditto", "--hfsCompression", staged, packed, (char *)NULL);
                _exit(127);
            }
            if (pid > 0 && waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0 &&
                stat(packed, &current) == 0 && (long long)current.st_size == bytes && rename(packed, retained) == 0) {
                (void)remove(staged);
                return 0;
            }
            (void)remove(packed);
        }
    }
#endif
    return rename(staged, retained);
}

static int observe_foreign(const char *twin_id, const char *url, const char *receipt_path,
                           const char *retain_body_path, const char *observe_right,
                           const char *robots_url_override, long long range_start,
                           long long max_bytes) {
    if (!is_http_url(url)) {
        fprintf(stderr, "foreign observe requires an http(s) URL\n");
        return 2;
    }

    time_t now = time(NULL);
    struct tm utc;
    char observed_at[32] = "";
    if (gmtime_r(&now, &utc) != NULL)
        strftime(observed_at, sizeof(observed_at), "%Y-%m-%dT%H:%M:%SZ", &utc);
    else
        snprintf(observed_at, sizeof(observed_at), "unknown");

    char derived_robots_url[PATH_MAX];
    const char *robots_url = robots_url_override;
    if (!robots_url && derive_robots_url(url, derived_robots_url, sizeof(derived_robots_url)) == 0)
        robots_url = derived_robots_url;
    int robots_exit = 0, robots_status = 0;
    const char *policy_decision = "allow";
    if (strcmp(observe_right, "denied") == 0) {
        policy_decision = "observe_right_denied";
    } else if (!robots_url) {
        policy_decision = "robots_url_unavailable";
    } else {
        char robots_path[] = "/tmp/bonfyre-robots-XXXXXX";
        int robots_fd = mkstemp(robots_path);
        if (robots_fd < 0) { perror("mkstemp"); return 1; }
        close(robots_fd);
        robots_exit = fetch_to_file(robots_url, robots_path, &robots_status);
        if (robots_exit != 0 || (robots_status != 200 && robots_status != 404)) {
            policy_decision = "robots_preflight_unavailable";
        } else if (robots_status == 200 && robots_disallow_source(robots_path, url)) {
            policy_decision = "robots_disallow";
        }
        unlink(robots_path);
    }

    FILE *receipt = receipt_path ? fopen(receipt_path, "a") : stdout;
    if (!receipt) { perror(receipt_path); return 1; }
    if (strcmp(policy_decision, "allow") != 0) {
        int excluded = strcmp(policy_decision, "observe_right_denied") == 0 ||
                       strcmp(policy_decision, "robots_disallow") == 0;
        write_policy_receipt(receipt, twin_id, url, observed_at, observe_right, robots_url,
                             robots_exit, robots_status, policy_decision, 0,
                             excluded ? "policy_excluded" : "policy_indeterminate",
                             /* Nothing was fetched, so there is no envelope to
                              * record; an empty one would imply a response. */
                             0, 0, 0, NULL, retain_body_path, NULL, NULL);
        if (receipt_path) fclose(receipt);
        return excluded ? 0 : 3;
    }

    char scratch_path[] = "/tmp/bonfyre-foreign-body-XXXXXX";
    /* A retained body is fetched beside its destination and moved into place.
     *
     * It was fetched straight into the file the estate serves, so for as long
     * as the network took -- seconds, on a slow source -- a reader saw a
     * truncated file or none at all. The reheat wakes every five minutes and
     * rewrites the hot twins, so /foreign/body answered 404 for whichever twin
     * was mid-fetch, intermittently and only under load. rename is atomic
     * within a filesystem: a reader sees the previous observation or the new
     * one, never half of one.
     *
     * The temporary lives beside the target rather than in /tmp, because a
     * rename across filesystems is a copy and stops being atomic. */
    char staged[PATH_MAX];
    const char *body_path = retain_body_path;
    int staged_in_place = 0;
    if (!body_path) {
        int body_fd = mkstemp(scratch_path);
        if (body_fd < 0) { perror("mkstemp"); return 1; }
        close(body_fd);
        body_path = scratch_path;
    } else if (snprintf(staged, sizeof(staged), "%s.staging", retain_body_path)
                   < (int)sizeof(staged)) {
        body_path = staged;
        staged_in_place = 1;
    }

    int http_status = 0;
    struct BfHttpEnvelope envelope;
    char header_path[PATH_MAX];
    char prior_etag[256] = "";
    char prior_digest[65] = "";
    int curl_exit;
    int conditional_hit = 0;
    snprintf(header_path, sizeof(header_path), "%s.headers", body_path);
    /* A prior receipt's validator makes re-observation cheap for the source as
     * well as for us; asking unconditionally every time is the rude default. */
    if (receipt_path) {
        /* Receipts append, so the file is a log and the newest line is the one
         * carrying a usable validator. Reading the first line asks the source
         * to revalidate against something it may have superseded long ago. */
        FILE *prior = fopen(receipt_path, "rb");
        if (prior) {
            char line[16384];
            char newest[16384] = "";
            while (fgets(line, sizeof(line), prior))
                if (line[0] == '{') snprintf(newest, sizeof(newest), "%s", line);
            fclose(prior);
            if (newest[0]) {
                receipt_field(newest, "etag", prior_etag, sizeof(prior_etag));
                receipt_field(newest, "content_sha256", prior_digest, sizeof(prior_digest));
                /* A prior 304 carries the digest of the body it referred to
                 * rather than one of its own. */
                if (!prior_digest[0])
                    receipt_field(newest, "unchanged_from_content_sha256",
                                  prior_digest, sizeof(prior_digest));
            }
        }
    }
    curl_exit = observe_credential_config
        ? fetch_with_config(observe_credential_config, body_path, header_path,
                            max_bytes > 0 ? NULL : (prior_etag[0] ? prior_etag : NULL),
                            &http_status, range_start, max_bytes)
        : fetch_to_file_with_headers(url, body_path, header_path,
                                     max_bytes > 0 ? NULL : (prior_etag[0] ? prior_etag : NULL),
                                     &http_status, range_start, max_bytes);
    parse_http_envelope(header_path, &envelope);
    (void)remove(header_path);
    conditional_hit = http_status == 304 && prior_digest[0];
    struct stat body_stat;
    long long body_bytes = stat(body_path, &body_stat) == 0 ? (long long)body_stat.st_size : 0;
    char digest[65] = "";
    int has_digest = body_bytes >= 0 && sha256_file(body_path, digest) == 0;

    if (staged_in_place) {
        /* Only a fetch that actually produced bytes replaces what is served.
         * A failed fetch that moved an empty file into place would delete the
         * last good observation and report success at doing it. */
        if (curl_exit == 0 && http_status >= 200 && http_status < 300 && body_bytes > 0) {
            if (settle_retained_body(staged, retain_body_path, body_bytes,
                                     has_digest ? digest : NULL, prior_digest) != 0) perror("rename");
        } else if (conditional_hit) {
            /* Nothing new was served, so nothing replaces what is retained. */
            (void)remove(staged);
        } else {
            (void)remove(staged);
        }
        body_path = retain_body_path;
    }

    write_policy_receipt(receipt, twin_id, url, observed_at, observe_right, robots_url,
                         robots_exit, robots_status, policy_decision, 1,
                         (max_bytes > 0 && range_start >= 0 && curl_exit == 0 && http_status >= 200 && http_status < 300)
                             ? "sample_observed" : conditional_hit ? "repeat_observed"
                             : (curl_exit == 0 && http_status >= 200 && http_status < 300)
                                 ? "source_observed" : "boundary_response",
                         curl_exit, http_status,
                         conditional_hit ? 0 : body_bytes,
                         conditional_hit ? prior_digest : (has_digest ? digest : NULL),
                         retain_body_path, &envelope,
                         conditional_hit ? prior_digest : NULL);
    if (receipt_path) fclose(receipt);

    /* An observation that retained content makes its resource nameable, so name
     * it now.
     *
     * Promotion was a manual verb, which meant a newly observed thing was
     * addressable only after someone remembered to run a command -- and the one
     * moment its identity is unambiguous is this one, when the bytes that
     * identify it are in hand. Deferring it invites exactly the failure already
     * found in this estate: an identity derived later, by a different rule,
     * beside the one derived here.
     *
     * Only on a retained observation. A boundary response has no digest to be
     * identified by, and promoting on one would mean inventing an identity for
     * something that never answered with content.
     *
     * Failure here does not fail the observation. The receipt is written and
     * the bytes are kept; a twin that could not be promoted stays observable
     * and `foreign promote` will pick it up. */
    if (max_bytes == 0 && curl_exit == 0 && http_status >= 200 && http_status < 300 && has_digest) {
        char mdb_buf[PATH_MAX]; const char *mdb = foreign_matrix_db(mdb_buf, sizeof(mdb_buf));
        const char *dir = getenv("BONFYRE_FOREIGN_RECEIPTS");
        if (!dir || !*dir) dir = "/var/lib/bonfyre/foreign/receipts";
        if (access(mdb, W_OK) == 0) {
            /* Quiet, because the caller's stdout belongs to whatever it is
             * doing. Redirecting stdout to suppress this would have worked
             * until the first time it ran without a controlling terminal. */
            if (foreign_promote(dir, mdb, url, 0, 1) == 0)
                fprintf(stderr, "[foreign] promoted on observation: %s\n", url);
        }
    }

    /* Enforce the twin's regime where the body is written.
     *
     * Cooling afterwards is a cleanup, and a cleanup is undone by the next
     * observation: a REMOTE twin re-observed retained its body again, so the
     * decision not to keep it survived exactly until someone looked once more.
     * A regime has to hold at the moment of retention or it is advice.
     *
     * The receipt is already written and still records the digest, so refusing
     * to keep the body costs nothing that was known. */
    if (retain_body_path && foreign_regime_retains(url) == 0) {
        unlink(retain_body_path);
        fprintf(stderr, "[foreign] regime says not to retain: %s\n", url);
    }

    if (!retain_body_path) unlink(body_path);
    return (curl_exit == 0 && http_status >= 200 && http_status < 300) ? 0 : 3;
}


/* The foreign relationship graph.
 *
 * Federation state is full of edges -- A follows B, B is hosted by C, C
 * federates with D. They are real observations and they are not institutional
 * facts, and the difference is the whole point of keeping them here:
 *
 *     follows                  != friend
 *     room join                != institution membership
 *     an org listing on GitHub != employment authority
 *     a signed Nostr event     != legal identity
 *     network participant      != admitted provider
 *
 * So this store is separate from matrix_subject_relations by construction, not
 * by convention. An edge observed here never becomes an institutional relation
 * on its own; `foreign admit` is a second, deliberate act, and it is the only
 * writer that crosses the line.
 *
 * Every edge keeps what would otherwise be lost the moment it is read back:
 * which federation said it, over which carrier, when, against which retained
 * evidence, and at what scope it may be seen. An edge without its provenance is
 * a claim with no way to check who made it.
 */
static int foreign_graph_open(const char *path, sqlite3 **out) {
    sqlite3 *db = NULL;
    if (sqlite3_open(path, &db) != SQLITE_OK) return 0;
    if (sqlite3_exec(db,
            "CREATE TABLE IF NOT EXISTS foreign_relations ("
            " id INTEGER PRIMARY KEY,"
            " subject_from TEXT NOT NULL,"
            " predicate TEXT NOT NULL,"
            " subject_to TEXT NOT NULL,"
            " source_federation TEXT NOT NULL,"
            " carrier TEXT NOT NULL,"
            " observed_at TEXT NOT NULL,"
            " evidence TEXT NOT NULL,"
            " scope TEXT NOT NULL DEFAULT 'public',"
            " claim_state TEXT NOT NULL DEFAULT 'observed',"
            " admitted_as TEXT,"
            " UNIQUE(subject_from, predicate, subject_to, source_federation));"
            "CREATE INDEX IF NOT EXISTS foreign_relations_scope"
            "  ON foreign_relations(scope);", NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    *out = db;
    return 1;
}

/* The scopes an observation may be held at.
 *
 * Public/private is not enough: one foreign subject can be observed publicly
 * from its own site, privately for a client, and under a partner arrangement,
 * and those are different closures over the same lawful subject. Declaring the
 * set here means an unknown scope is refused at the writer rather than becoming
 * a row nothing knows how to filter. */
static int foreign_scope_known(const char *scope) {
    static const char *known[] = {
        "public", "institution-private", "self-private", "partner-shared", "restricted", NULL
    };
    int i;
    for (i = 0; known[i]; i++) if (!strcmp(known[i], scope)) return 1;
    return 0;
}

/* Which scopes a caller admitted at `have` may see.
 *
 * This is a widening ladder and it is deliberately not a comparison on a
 * string: `restricted` sorts before `public` alphabetically, and a filter that
 * got that backwards would disclose rather than withhold. */
static int foreign_scope_visible(const char *have, const char *row) {
    if (!strcmp(row, "public")) return 1;
    if (!have || !*have) return 0;
    if (!strcmp(have, "restricted")) return 1;                 /* sees everything */
    if (!strcmp(have, "partner-shared"))
        return !strcmp(row, "partner-shared") || !strcmp(row, "institution-private");
    if (!strcmp(have, "institution-private")) return !strcmp(row, "institution-private");
    if (!strcmp(have, "self-private")) return !strcmp(row, "self-private");
    return 0;
}

static int foreign_relate(const char *gdb, const char *from, const char *pred, const char *to,
                          const char *federation, const char *carrier, const char *evidence,
                          const char *scope) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char now[32];
    time_t t = time(NULL);
    struct tm g;
    int rc;

    if (!foreign_scope_known(scope)) {
        fprintf(stderr, "unknown observation scope: %s\n", scope);
        return 2;
    }
    gmtime_r(&t, &g);
    strftime(now, sizeof(now), "%Y-%m-%dT%H:%M:%SZ", &g);
    if (!foreign_graph_open(gdb, &db)) {
        fprintf(stderr, "cannot open foreign graph at %s\n", gdb);
        return 2;
    }
    if (sqlite3_prepare_v2(db,
            "INSERT OR REPLACE INTO foreign_relations"
            " (subject_from,predicate,subject_to,source_federation,carrier,observed_at,evidence,scope,claim_state)"
            " VALUES (?,?,?,?,?,?,?,?,'observed')", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 2;
    }
    sqlite3_bind_text(st, 1, from, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, pred, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, to, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 4, federation, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 5, carrier, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 6, now, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 7, evidence, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 8, scope, -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(st);
    sqlite3_finalize(st);
    sqlite3_close(db);
    if (rc != SQLITE_DONE) return 2;
    printf("{\"schema\":\"bonfyre-foreign-edge.v1\",\"from\":\"%s\",\"predicate\":\"%s\","
           "\"to\":\"%s\",\"federation\":\"%s\",\"carrier\":\"%s\",\"scope\":\"%s\","
           "\"claim_state\":\"observed\",\"observed_at\":\"%s\","
           "\"forbidden_inference\":\"an observed edge is not an institutional relation\"}\n",
           from, pred, to, federation, carrier, scope, now);
    return 0;
}

/* Read the graph at a scope. The filter is applied in the query, never after.
 *
 * Loading every edge and hiding the ones a caller may not see is the failure
 * this is written to avoid: the rows have already left the closure by the time
 * anything filters them, and every later reader -- a cache, a log, a template,
 * a JSON body -- is another place the disclosure can escape. The closure
 * enforces the policy or the policy is decoration. */
static int foreign_edges(const char *gdb, const char *have_scope) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    int listed = 0, withheld = 0;
    const char *scope = (have_scope && *have_scope) ? have_scope : "public";

    if (!foreign_graph_open(gdb, &db)) {
        fprintf(stderr, "cannot open foreign graph at %s\n", gdb);
        return 2;
    }
    if (sqlite3_prepare_v2(db,
            "SELECT subject_from,predicate,subject_to,source_federation,carrier,"
            "       observed_at,evidence,scope,claim_state FROM foreign_relations"
            "  ORDER BY id", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 2;
    }
    printf("{\"schema\":\"bonfyre-foreign-graph.v1\",\"scope\":\"%s\",\"edges\":[", scope);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *row_scope = (const char *)sqlite3_column_text(st, 7);
        if (!foreign_scope_visible(scope, row_scope ? row_scope : "public")) { withheld++; continue; }
        printf("%s{\"from\":\"%s\",\"predicate\":\"%s\",\"to\":\"%s\","
               "\"federation\":\"%s\",\"carrier\":\"%s\",\"observed_at\":\"%s\","
               "\"evidence\":\"%s\",\"scope\":\"%s\",\"claim_state\":\"%s\"}",
               listed ? "," : "",
               sqlite3_column_text(st, 0), sqlite3_column_text(st, 1),
               sqlite3_column_text(st, 2), sqlite3_column_text(st, 3),
               sqlite3_column_text(st, 4), sqlite3_column_text(st, 5),
               sqlite3_column_text(st, 6), row_scope, sqlite3_column_text(st, 8));
        listed++;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    printf("],\"listed\":%d,\"withheld_for_scope\":%d,\"forbidden_inference\":["
           "\"an observed edge -> an institutional relation\","
           "\"a withheld count -> the content of what was withheld\","
           "\"a federation's assertion -> a fact about the world\"]}\n", listed, withheld);
    return 0;
}


/* ---------------------------------------------------------------- reheat
 *
 * ResourceWeather: ask a source when the source is worth asking.
 *
 * A crawler asks everything on a schedule. This reads the authored census --
 * what each source is, how often it is worth asking, and the contract for
 * reading it back -- then asks only what is due, and stops asking what nobody
 * needs.
 *
 * Pressure has three parts and none of them is stored here:
 *
 *   cadence    authored on the census row. A federal register that publishes
 *              once a day is not hot because the machine is fast.
 *   standing   the last line of the source's own receipt. A refusal freezes;
 *              a failure demotes; success holds.
 *   demand     whether an activated pack's internet demand names this family.
 *              Not an installed pack: holding one is not running it, and a
 *              held pack should not keep the network warm.
 *
 * Cannot infer: a reheated source -> a fresh fact. The receipt says when it was
 * read, never that what it says is true.
 */

/* The census grows by itself now, so the reheat allocates for what it finds
 * rather than reading the first few hundred rows and calling that the estate. */
#define REHEAT_CHUNK 512

typedef struct {
    char source[96];
    char family[96];
    char url[8192];     /* a query source carries its whole query in its address */
    char twin[128];
    char tier[16];      /* authored floor */
    char label[64];
    char credential[64];   /* the credential provider a read needs, if any */
    char host[128];     /* so one publisher cannot take a whole run */
    long cadence;
    long age, due;      /* measured once, in the first pass */
    int rank, wanted, read_now;
} ReheatRow;

/* How many sources one run may read, and how many of them may belong to one
 * publisher.  A run is a budget, not a sweep: with the timer every few minutes
 * a census of thousands is still read inside its cadence, and no host meets a
 * burst from this estate. */
static int reheat_budget = 40;
static int reheat_per_host = 2;

static int reheat_tier_rank(const char *t) {
    if (!t) return 1;
    if (!strcmp(t, "FROZEN")) return 0;
    if (!strcmp(t, "COLD"))   return 1;
    if (!strcmp(t, "WARM"))   return 2;
    if (!strcmp(t, "HOT"))    return 3;
    return 1;
}

static const char *reheat_tier_name(int rank) {
    static const char *n[] = { "FROZEN", "COLD", "WARM", "HOT" };
    if (rank < 0) rank = 0;
    if (rank > 3) rank = 3;
    return n[rank];
}

/* The multiplier on a source's own cadence. FROZEN never asks. */
static double reheat_factor(int rank) {
    switch (rank) {
    case 3:  return 1.0;
    case 2:  return 3.0;
    case 1:  return 6.0;
    default: return 0.0;
    }
}

/* Some sources put the day in the path. A census row cannot hold today's date,
 * and hardcoding one means the source 404s tomorrow and is demoted for being
 * unreliable -- the scheduler blaming a source for the row's own staleness. */
static void reheat_resolve_url(const char *in, char *out, size_t cap) {
    time_t now = time(NULL);
    struct tm utc;
    char today[16] = "", yday[16] = "";
    time_t prev = now - 86400;

    char today_iso[16] = "", yday_iso[16] = "", week_iso[16] = "", week[16] = "", month_iso[16] = "";
    time_t back = now - 7 * 86400, month_back = now - 30 * 86400;

    if (gmtime_r(&now, &utc)) strftime(today, sizeof(today), "%Y/%m/%d", &utc);
    if (gmtime_r(&prev, &utc)) strftime(yday, sizeof(yday), "%Y/%m/%d", &utc);
    if (gmtime_r(&now, &utc)) strftime(today_iso, sizeof(today_iso), "%Y-%m-%d", &utc);
    if (gmtime_r(&prev, &utc)) strftime(yday_iso, sizeof(yday_iso), "%Y-%m-%d", &utc);
    if (gmtime_r(&back, &utc)) strftime(week_iso, sizeof(week_iso), "%Y-%m-%d", &utc);
    if (gmtime_r(&back, &utc)) strftime(week, sizeof(week), "%Y/%m/%d", &utc);
    if (gmtime_r(&month_back, &utc)) strftime(month_iso, sizeof(month_iso), "%Y-%m-%d", &utc);

    /* Both halves of a census row can need a date: the URL that asks for
     * today, and the path into an answer that is keyed by today. One
     * vocabulary for both, or a row is true for a day and empty after. */
    size_t o = 0;
    for (const char *p = in; *p && o + 1 < cap; ) {
        if (!strncmp(p, "{month-ago-iso}", 15)) {
            o += (size_t)snprintf(out + o, cap - o, "%s", month_iso);
            p += 15;
        } else if (!strncmp(p, "{week-ago-iso}", 14)) {
            o += (size_t)snprintf(out + o, cap - o, "%s", week_iso);
            p += 14;
        } else if (!strncmp(p, "{week-ago}", 10)) {
            o += (size_t)snprintf(out + o, cap - o, "%s", week);
            p += 10;
        } else if (!strncmp(p, "{today-iso}", 11)) {
            o += (size_t)snprintf(out + o, cap - o, "%s", today_iso);
            p += 11;
        } else if (!strncmp(p, "{yesterday-iso}", 15)) {
            o += (size_t)snprintf(out + o, cap - o, "%s", yday_iso);
            p += 15;
        } else if (!strncmp(p, "{today}", 7)) {
            o += (size_t)snprintf(out + o, cap - o, "%s", today);
            p += 7;
        } else if (!strncmp(p, "{yesterday}", 11)) {
            o += (size_t)snprintf(out + o, cap - o, "%s", yday);
            p += 11;
        } else {
            out[o++] = *p++;
        }
    }
    out[o < cap ? o : cap - 1] = '\0';
}

/* The last line of a twin's receipt: what happened the last time it was read.
 * A receipt file is a history, one observation per line, appended -- reading it
 * from the start answers with the first time the source was ever seen. */
/* What the public surfaces actually showed, from the source ledger this estate
 * keeps.  A source a page carried in the last few days is warm whatever its
 * authored tier says: demand is measured here, not declared.  The ledger is
 * opened read-only and its absence changes nothing. */
static int reheat_demanded(const char *source) {
    static sqlite3 *ledger = NULL;
    static int opened = 0;
    sqlite3_stmt *st = NULL;
    int demanded = 0;
    if (!opened) {
        const char *path = getenv("BONFYRE_SOURCE_FRONTIER_DB");
        char uri[PATH_MAX];
        opened = 1;
        if (path && *path && snprintf(uri, sizeof(uri), "file:%s?mode=ro", path) < (int)sizeof(uri)) {
            if (sqlite3_open_v2(uri, &ledger, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) != SQLITE_OK) {
                sqlite3_close(ledger); ledger = NULL;
            } else sqlite3_busy_timeout(ledger, 1000);
        }
    }
    if (!ledger || !source || !*source) return 0;
    if (sqlite3_prepare_v2(ledger, "SELECT 1 FROM contribution co JOIN channel c ON c.channel_id=co.channel_id"
                                   " LEFT JOIN channel_census cc ON cc.channel_id=c.channel_id"
                                   " WHERE (c.census_source=?1 OR cc.census_source=?1)"
                                   "   AND co.day >= date('now','-3 day') LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT);
        demanded = sqlite3_step(st) == SQLITE_ROW;
    }
    sqlite3_finalize(st);
    return demanded;
}

static int reheat_last_receipt(const char *dir, const char *twin,
                               char *at, size_t at_cap,
                               char *policy, size_t policy_cap, int *status) {
    char path[PATH_MAX];
    char line[8192], last[8192];
    FILE *f;

    at[0] = policy[0] = '\0';
    *status = 0;
    if (snprintf(path, sizeof(path), "%s/%s.json", dir, twin) >= (int)sizeof(path))
        return 0;
    if (!(f = fopen(path, "r"))) return 0;
    last[0] = '\0';
    while (fgets(line, sizeof(line), f))
        if (line[0] == '{') snprintf(last, sizeof(last), "%s", line);
    fclose(f);
    if (!last[0]) return 0;

    receipt_field(last, "observed_at", at, at_cap);
    receipt_field(last, "policy_decision", policy, policy_cap);
    {
        char s[16];
        receipt_field(last, "http_status", s, sizeof(s));
        *status = atoi(s);
    }
    return 1;
}

/* Age of an ISO-8601 Zulu stamp, in seconds.
 *
 * timegm is a GNU/BSD extension that -std=c11 hides on glibc, and reaching for
 * it here would have built on the dev machine and failed on the deploy host --
 * the same class of break the feature macro at the top of this file exists to
 * prevent. The arithmetic is small enough to do honestly: the receipt stamps
 * are always UTC, so days-since-epoch plus the time of day is the whole
 * conversion. */
static long reheat_age(const char *iso) {
    int y, mo, d, h, mi, s;
    long days, secs;
    static const int cum[12] = { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };

    if (!iso || sscanf(iso, "%d-%d-%dT%d:%d:%dZ", &y, &mo, &d, &h, &mi, &s) != 6)
        return 1000000000L;
    if (mo < 1 || mo > 12 || d < 1 || d > 31) return 1000000000L;

    days = (long)(y - 1970) * 365 + ((y - 1969) / 4) - ((y - 1901) / 100)
         + ((y - 1601) / 400) + cum[mo - 1] + (d - 1);
    if (mo > 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) days += 1;
    secs = days * 86400L + h * 3600L + mi * 60L + s;
    return (long)time(NULL) - secs;
}

/* `only` names one source to read now, whatever its tier says.
 *
 * The cadence is right for a census that is not changing: silence demotes,
 * demand promotes, and nothing is read because somebody is impatient. But a
 * row that was just re-authored -- a URL corrected, an extraction path fixed --
 * is a different case: the stored observation is of the old row, and waiting
 * six hours to find out whether the correction worked means authoring is done
 * blind. This reads one named row and leaves the schedule alone. */
/* Asks the credential boundary for the request a credentialed source needs.
 * Answers 0 with a curl config (address and header lines), 4 when a required
 * field is not set, anything else when the boundary is unavailable. */
static int reheat_credential_config(const char *provider, const char *url, char *config, size_t cap) {
    const char *bin = getenv("BONFYRE_CREDENTIALS_BINARY"), *db = getenv("BONFYRE_CREDENTIALS_DB");
    const char *key = getenv("BONFYRE_CREDENTIALS_KEY"), *providers = getenv("BONFYRE_CREDENTIAL_PROVIDERS");
    char answer[65536]; int rc; size_t used = 0;
    if (!bin || !*bin) bin = "/usr/local/bin/bonfyre-credentials";
    if (!db || !*db) db = "/var/lib/bonfyre/credentials/credentials.db";
    if (!key || !*key) key = "/etc/bonfyre/credentials.key";
    if (!providers || !*providers) providers = "/usr/local/share/bonfyre/provider-credentials.json";
    {
        char *const argv[] = {(char *)bin, "apply", "--db", (char *)db, "--key", (char *)key, "--providers", (char *)providers,
                              "--provider", (char *)provider, "--url", (char *)url, "--actor", "reheat", NULL};
        rc = read_child_stdout(argv, answer, sizeof(answer));
    }
    if (rc) { memset(answer, 0, sizeof(answer)); return rc; }
    {
        /* {"url":"...","headers":["Name: value",...]} -> curl config lines */
        char *p = strstr(answer, "\"url\":\""), *end;
        if (!p) { memset(answer, 0, sizeof(answer)); return 3; }
        p += 7;
        if (!(end = strchr(p, '"'))) { memset(answer, 0, sizeof(answer)); return 3; }
        used += (size_t)snprintf(config + used, cap - used, "url = \"%.*s\"\n", (int)(end - p), p);
        if ((p = strstr(end, "\"headers\":[")) != NULL) {
            p += 11;
            for (;;) {
                char *q;
                while (*p == ' ' || *p == ',') p++;
                if (*p != '"') break;
                if (!(q = strchr(p + 1, '"')) || used >= cap) break;
                used += (size_t)snprintf(config + used, cap - used, "header = \"%.*s\"\n", (int)(q - p - 1), p + 1);
                p = q + 1;
            }
        }
    }
    memset(answer, 0, sizeof(answer));
    return used < cap ? 0 : 3;
}

static const char *reheat_only_credential = NULL;

static int foreign_reheat(const char *cms_db, const char *receipts, const char *bodies,
                          const char *weather_path, int dry, const char *only) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    ReheatRow *rows = NULL; int rows_cap = 0;
    static char demand[16384];
    int n = 0, i, read_n = 0, skipped = 0, first = 1;
    char out_at[32];
    FILE *w = NULL;
    time_t now = time(NULL);
    struct tm utc;

    if (sqlite3_open_v2(cms_db, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        fprintf(stderr, "reheat: cannot read the census at %s\n", cms_db);
        if (db) sqlite3_close(db);
        return 3;
    }

    /* What an activated pack asks the outside world for. An installed pack is
     * not a running one and demands nothing. */
    demand[0] = '\0';
    if (sqlite3_prepare_v2(db,
            "SELECT LOWER(d.needs) FROM pack_demand d "
            "JOIN pack_activation a ON a.pack = d.pack AND a.status='published' "
            "WHERE d.demand_kind='internet' AND d.status='published'",
            -1, &st, NULL) == SQLITE_OK) {
        size_t used = 0;
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *needs = (const char *)sqlite3_column_text(st, 0);
            if (!needs) continue;
            used += (size_t)snprintf(demand + used, sizeof(demand) - used, " %s", needs);
            if (used >= sizeof(demand) - 64) break;
        }
        sqlite3_finalize(st);
        st = NULL;
    }

    /* A census written before rows could name a credential still reads. */
    if (sqlite3_prepare_v2(db,
            "SELECT source, family, url, twin, COALESCE(tier,'COLD'), "
            "       COALESCE(cadence_seconds,0), COALESCE(label,''), COALESCE(credential,'') "
            "FROM internet_source "
            "WHERE status='published' AND COALESCE(twin,'') != '' "
            "AND COALESCE(url,'') != '' ORDER BY id", -1, &st, NULL) == SQLITE_OK ||
        sqlite3_prepare_v2(db,
            "SELECT source, family, url, twin, COALESCE(tier,'COLD'), "
            "       COALESCE(cadence_seconds,0), COALESCE(label,''), '' "
            "FROM internet_source "
            "WHERE status='published' AND COALESCE(twin,'') != '' "
            "AND COALESCE(url,'') != '' ORDER BY id", -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            ReheatRow *r;
            if (n >= rows_cap) {
                int grow = rows_cap ? rows_cap * 2 : REHEAT_CHUNK;
                ReheatRow *bigger = realloc(rows, (size_t)grow * sizeof(*rows));
                if (!bigger) break;
                memset(bigger + rows_cap, 0, (size_t)(grow - rows_cap) * sizeof(*rows));
                rows = bigger; rows_cap = grow;
            }
            r = &rows[n];
            snprintf(r->source, sizeof(r->source), "%s", (const char *)sqlite3_column_text(st, 0));
            snprintf(r->family, sizeof(r->family), "%s", (const char *)sqlite3_column_text(st, 1));
            snprintf(r->url,    sizeof(r->url),    "%s", (const char *)sqlite3_column_text(st, 2));
            snprintf(r->twin,   sizeof(r->twin),   "%s", (const char *)sqlite3_column_text(st, 3));
            snprintf(r->tier,   sizeof(r->tier),   "%s", (const char *)sqlite3_column_text(st, 4));
            r->cadence = (long)sqlite3_column_int64(st, 5);
            snprintf(r->label,  sizeof(r->label),  "%s", (const char *)sqlite3_column_text(st, 6));
            snprintf(r->credential, sizeof(r->credential), "%s", (const char *)sqlite3_column_text(st, 7));
            {
                const char *p = strstr(r->url, "://");
                size_t k = 0;
                p = p ? p + 3 : r->url;
                while (p[k] && p[k] != '/' && p[k] != ':' && k + 1 < sizeof(r->host)) { r->host[k] = (char)tolower((unsigned char)p[k]); k++; }
                r->host[k] = '\0';
            }
            n++;
        }
        sqlite3_finalize(st);
        st = NULL;
    }
    sqlite3_close(db);
    if (!rows) return 0;

    if (gmtime_r(&now, &utc))
        strftime(out_at, sizeof(out_at), "%Y-%m-%dT%H:%M:%SZ", &utc);
    else
        snprintf(out_at, sizeof(out_at), "unknown");

    /* A named read reports on one row and must not publish that as the
     * weather. The weather is the whole census's standing, and a file saying
     * "census 65, sources: [one]" is read by the surfaces as sixty-four
     * sources having gone away. */
    if (weather_path && *weather_path && !only && !reheat_only_credential) {
        w = fopen(weather_path, "w");
        if (!w) fprintf(stderr, "reheat: weather not writable at %s\n", weather_path);
    }

#define REHEAT_EMIT(...) do { printf(__VA_ARGS__); if (w) fprintf(w, __VA_ARGS__); } while (0)

    REHEAT_EMIT("{\"schema\":\"bonfyre-resource-weather.v1\",\"at\":\"%s\","
                "\"census\":%d,\"sources\":[", out_at, n);

    /* First, what is true of every row: how long since it was read, what its
     * standing is, and how overdue that makes it.  Nothing is read in this
     * pass, so the choice below sees the whole census rather than whichever
     * rows came first. */
    for (i = 0; i < n; i++) {
        ReheatRow *r = &rows[i];
        char at[32], policy[32];
        int status = 0;
        reheat_last_receipt(receipts, r->twin, at, sizeof(at), policy, sizeof(policy), &status);
        r->age = reheat_age(at);
        r->rank = reheat_tier_rank(r->tier);
        if (policy[0] && (!strcmp(policy, "robots_disallow") || !strcmp(policy, "observe_right_denied"))) r->rank = 0;
        else if (at[0] && !(status >= 200 && status < 300) && strcmp(policy, "allow")) r->rank = r->rank > 0 ? r->rank - 1 : 0;
        else if (at[0] && status >= 200 && status < 300 && r->rank < 3 && r->age < 3600) r->rank++;
        r->wanted = 0;
        if (r->family[0]) {
            char lower[96]; size_t k;
            snprintf(lower, sizeof(lower), "%s", r->family);
            for (k = 0; lower[k]; k++) lower[k] = (char)tolower((unsigned char)lower[k]);
            if (strstr(demand, lower)) { r->wanted = 1; if (r->rank > 0 && r->rank < 3) r->rank++; }
        }
        /* A source a page actually showed is warm, whatever it was authored as. */
        if (r->rank > 0 && r->rank < 2 && reheat_demanded(r->source)) { r->wanted = 1; r->rank = 2; }
        r->due = r->cadence > 0 ? (long)(r->cadence * reheat_factor(r->rank)) : 0;
        r->read_now = 0;
    }

    /* Then the choice: the most overdue first, up to this run's budget, with
     * no publisher taking more than its share of one run.  A source nobody has
     * ever read is the most overdue thing there is, so a newly admitted census
     * row is read on the next tick rather than in a week. */
    if (!only && !reheat_only_credential) {
        /* A publisher's share of one run is whichever is larger: the standing
         * per-host limit, or an equal division of the budget among the
         * publishers that actually have something due.  So a census of two
         * hosts is not throttled to four reads, and a census of two thousand
         * hosts still cannot let one of them take the run. */
        int picked = 0, hosts_due = 0, share, j;
        for (i = 0; i < n; i++) {
            ReheatRow *r = &rows[i];
            int seen = 0;
            if (r->rank == 0 || r->due <= 0 || r->age < r->due) continue;
            for (j = 0; j < i; j++)
                if (rows[j].rank > 0 && rows[j].due > 0 && rows[j].age >= rows[j].due &&
                    rows[j].host[0] && !strcmp(rows[j].host, r->host)) { seen = 1; break; }
            if (!seen) hosts_due++;
        }
        share = hosts_due > 0 ? (reheat_budget + hosts_due - 1) / hosts_due : reheat_budget;
        if (share < reheat_per_host) share = reheat_per_host;
        while (picked < reheat_budget) {
            ReheatRow *best = NULL; double worst = 0;
            for (j = 0; j < n; j++) {
                ReheatRow *r = &rows[j];
                double overdue; int from_host = 0, k;
                if (r->read_now || r->rank == 0 || r->due <= 0 || r->age < r->due) continue;
                for (k = 0; k < n; k++)
                    if (rows[k].read_now == 1 && rows[k].host[0] && !strcmp(rows[k].host, r->host)) from_host++;
                if (from_host >= share) continue;
                overdue = (double)r->age / (double)r->due;
                if (!best || overdue > worst) { best = r; worst = overdue; }
            }
            if (!best) break;
            best->read_now = 1;
            picked++;
        }
    }

    for (i = 0; i < n; i++) {
        ReheatRow *r = &rows[i];
        char at[32], policy[32], url[8192];
        int status = 0, rank, wanted = r->wanted;
        long age, due;
        const char *decision;

        reheat_last_receipt(receipts, r->twin, at, sizeof(at),
                            policy, sizeof(policy), &status);
        age = r->age;

        /* Standing and dueness, as the first pass measured them. */
        rank = r->rank;
        due = r->due;

        if (reheat_only_credential && strcmp(r->credential, reheat_only_credential)) {
            /* Reading what one credential unlocked, and nothing else. */
            continue;
        } else if (only && strcmp(r->source, only)) {
            /* A named read is a read of that row and of nothing else; the rest
             * of the census keeps its schedule and is not even reported. */
            continue;
        } else if (rank == 0 && !only) {
            decision = "frozen; not asked again until something changes there";
            skipped++;
        } else if (only || reheat_only_credential) {
            /* Asked for by name. A refusal still wins: a frozen source is
             * frozen because it said no, and a name does not overrule that. */
            if (rank == 0) {
                decision = "frozen; a name does not overrule a refusal";
                skipped++;
            } else if (dry) {
                decision = "would read, by name";
                read_n++;
            } else {
                static char config[16384];
                int waiting = 0;
                reheat_resolve_url(r->url, url, sizeof(url));
                if (r->credential[0] && reheat_credential_config(r->credential, url, config, sizeof(config)) != 0) waiting = 1;
                if (waiting) {
                    decision = "waiting for a credential";
                    skipped++;
                } else {
                    char rp[PATH_MAX], bp[PATH_MAX];
                    snprintf(rp, sizeof(rp), "%s/%s.json", receipts, r->twin);
                    snprintf(bp, sizeof(bp), "%s/%s.body", bodies, r->twin);
                    observe_credential_config = r->credential[0] ? config : NULL;
                    (void)observe_foreign(r->twin, url, rp, bp, "public", NULL, 0, 0);
                    observe_credential_config = NULL;
                    memset(config, 0, sizeof(config));
                    decision = "read, by name";
                    read_n++;
                }
            }
        } else if (due <= 0 || age < due) {
            decision = "not due";
            skipped++;
        } else if (!r->read_now) {
            /* Due, and waiting its turn: this run's budget is spent, or its
             * publisher has already answered enough questions this run. */
            decision = "due; waiting its turn";
            skipped++;
        } else if (dry) {
            decision = "due";
            read_n++;
        } else {
            static char config[16384];
            reheat_resolve_url(r->url, url, sizeof(url));
            if (r->credential[0] && reheat_credential_config(r->credential, url, config, sizeof(config)) != 0) {
                /* Not a refusal by the source and not a failure: nobody has
                 * given this estate the credential yet. */
                REHEAT_EMIT("%s{\"source\":\"%s\",\"family\":\"%s\",\"label\":\"%s\",\"tier\":\"%s\",\"authored_tier\":\"%s\",\"demanded\":%s,"
                            "\"age_seconds\":%ld,\"due_seconds\":%ld,\"last_seen\":\"%s\",\"http_status\":%d,\"decision\":\"waiting for a credential\"}",
                            first ? "" : ",", r->source, r->family, r->label, reheat_tier_name(rank), r->tier, wanted ? "true" : "false",
                            age >= 1000000000L ? -1 : age, due, at, status);
                first = 0; skipped++;
                continue;
            }
            {
                char rp[PATH_MAX], bp[PATH_MAX];
                snprintf(rp, sizeof(rp), "%s/%s.json", receipts, r->twin);
                snprintf(bp, sizeof(bp), "%s/%s.body", bodies, r->twin);
                observe_credential_config = r->credential[0] ? config : NULL;
                (void)observe_foreign(r->twin, url, rp, bp, "public", NULL, 0, 0);
                observe_credential_config = NULL;
                memset(config, 0, sizeof(config));
            }
            /* The outcome is the receipt's, read back the same way anything
             * else reads it. */
            reheat_last_receipt(receipts, r->twin, at, sizeof(at),
                                policy, sizeof(policy), &status);
            age = reheat_age(at);
            decision = policy[0] ? policy : "unknown";
            read_n++;
        }

        REHEAT_EMIT("%s{\"source\":\"%s\",\"family\":\"%s\",\"label\":\"%s\","
                    "\"tier\":\"%s\",\"authored_tier\":\"%s\",\"demanded\":%s,"
                    "\"age_seconds\":%ld,\"due_seconds\":%ld,\"last_seen\":\"%s\","
                    "\"http_status\":%d,\"decision\":\"%s\"}",
                    first ? "" : ",", r->source, r->family, r->label,
                    reheat_tier_name(rank), r->tier, wanted ? "true" : "false",
                    age >= 1000000000L ? -1 : age, due, at, status, decision);
        first = 0;
    }

    {
        int due_now = 0, waiting = 0, never = 0;
        for (i = 0; i < n; i++) {
            if (rows[i].rank > 0 && rows[i].due > 0 && rows[i].age >= rows[i].due) due_now++;
            if (rows[i].age >= 1000000000L) never++;
        }
        waiting = due_now - read_n;
        if (waiting < 0) waiting = 0;
        REHEAT_EMIT("],\"read\":%d,\"skipped\":%d,\"due\":%d,\"waiting_its_turn\":%d,\"never_read\":%d,\"budget\":%d,"
                    "\"cannot_infer\":\"a reheated source is a fresh observation and never a "
                    "fresh fact; the receipt says when it was read, not that what it says is "
                    "true\"}\n", read_n, skipped, due_now, waiting, never, reheat_budget);
    }
#undef REHEAT_EMIT
    if (w) fclose(w);
    free(rows);
    return 0;
}

/* Every ForeignTwin verb shares the configured store. Explicit leaf paths
 * retain precedence for installations that keep receipts and bodies apart. */
static int foreign_store_defaults(void) {
    const char *root = getenv("BONFYRE_FOREIGN_ROOT");
    const char *names[] = { "BONFYRE_FOREIGN_RECEIPTS", "BONFYRE_FOREIGN_BODIES" };
    const char *leaves[] = { "receipts", "bodies" };
    char resolved[PATH_MAX];
    if (!root || !*root) return 0;
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        const char *explicit_path = getenv(names[i]);
        if (explicit_path && *explicit_path) continue;
        int size = snprintf(resolved, sizeof(resolved), "%s/%s", root, leaves[i]);
        if (size < 0 || (size_t)size >= sizeof(resolved) || setenv(names[i], resolved, 1)) {
            fprintf(stderr, "foreign: cannot derive %s from configured root\n", names[i]);
            return 2;
        }
    }
    return 0;
}

#include "message_review.h"

int main(int argc, char **argv) {
    if (argc < 2) {
        print_usage();
        return 1;
    }
    if (!strcmp(argv[1], "foreign") && foreign_store_defaults()) return 2;
    if (argc >= 3 && !strcmp(argv[1], "message") && !strcmp(argv[2], "review"))
        return message_review(argc, argv);

    char cms_resolved[PATH_MAX];
    char index_resolved[PATH_MAX];
    char stitch_resolved[PATH_MAX];
    const char *cms_bin = default_binary("BONFYRE_CMS_BINARY", argv[0], cms_resolved, sizeof(cms_resolved), "BonfyreCMS", "bonfyre-cms", "../BonfyreCMS/bonfyre-cms");
    const char *index_bin = default_binary("BONFYRE_INDEX_BINARY", argv[0], index_resolved, sizeof(index_resolved), "BonfyreIndex", "bonfyre-index", "../BonfyreIndex/bonfyre-index");
    const char *stitch_bin = default_binary("BONFYRE_STITCH_BINARY", argv[0], stitch_resolved, sizeof(stitch_resolved), "BonfyreStitch", "bonfyre-stitch", "../BonfyreStitch/bonfyre-stitch");

    if (strcmp(argv[1], "cms") == 0 || strcmp(argv[1], "index") == 0 || strcmp(argv[1], "stitch") == 0) {
        if (argc < 3) return 1;
        const char *target = strcmp(argv[1], "cms") == 0 ? cms_bin : (strcmp(argv[1], "index") == 0 ? index_bin : stitch_bin);
        char **child = calloc((size_t)argc, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)target;
        for (int i = 2; i < argc; i++) child[i - 1] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "refresh") == 0) {
        if (argc < 3) {
            print_usage();
            return 1;
        }
        char **child = calloc((size_t)argc + 2, sizeof(char *));
        if (!child) return 1;
        child[0] = (char *)index_bin;
        child[1] = "build";
        child[2] = argv[2];
        for (int i = 3; i < argc; i++) child[i] = argv[i];
        int rc = run_command(child);
        free(child);
        return rc;
    }

    if (strcmp(argv[1], "activation") == 0 && argc >= 4 &&
        strcmp(argv[2], "project-demand") == 0) {
        const char *db_path = NULL;
        for (int i = 4; i < argc; i++) {
            if (strcmp(argv[i], "--db") == 0 && i + 1 < argc) db_path = argv[++i];
            else {
                print_usage();
                return 1;
            }
        }
        if (!db_path || !*db_path) {
            fprintf(stderr, "activation project-demand requires --db FILE\n");
            return 1;
        }
        return activation_project_demand(argv[3], db_path);
    }

    if (strcmp(argv[1], "activation") == 0 && argc >= 3 &&
        (strcmp(argv[2], "select") == 0 || strcmp(argv[2], "bind") == 0 ||
         strcmp(argv[2], "reconcile") == 0)) {
        const char *resource = NULL, *service = NULL, *actor = NULL, *mechanism = "service";
        const char *evidence = "", *db_path = NULL;
        int first = !strcmp(argv[2], "reconcile") ? 3 : 4;
        if (strcmp(argv[2], "reconcile") != 0) resource = argc >= 4 ? argv[3] : NULL;
        else if (argc >= 4 && strncmp(argv[3], "--", 2) != 0) { resource = argv[3]; first = 4; }
        for (int i = first; i < argc; i++) {
            if (!strcmp(argv[i], "--db") && i + 1 < argc) db_path = argv[++i];
            else if (!strcmp(argv[i], "--actor") && i + 1 < argc) actor = argv[++i];
            else if (!strcmp(argv[i], "--service") && i + 1 < argc) service = argv[++i];
            else if (!strcmp(argv[i], "--mechanism") && i + 1 < argc) mechanism = argv[++i];
            else if (!strcmp(argv[i], "--evidence") && i + 1 < argc) evidence = argv[++i];
            else { print_usage(); return 1; }
        }
        if (!db_path || !*db_path || ((!strcmp(argv[2], "select") || !strcmp(argv[2], "bind")) &&
            (!resource || !*resource || !actor || !*actor)) ||
            (!strcmp(argv[2], "bind") && (!service || !*service || !evidence || !*evidence))) {
            print_usage(); return 1;
        }
        if (!strcmp(argv[2], "select")) return activation_select(resource, actor, mechanism, evidence, db_path);
        if (!strcmp(argv[2], "bind")) return activation_bind(resource, service, actor, evidence, db_path);
        return activation_reconcile(resource, db_path);
    }

    if (strcmp(argv[1], "foreign") == 0 && argc >= 3 && strcmp(argv[2], "duplicates") == 0) {
        char mdb_buf[PATH_MAX]; const char *mdb = foreign_matrix_db(mdb_buf, sizeof(mdb_buf));
        return foreign_duplicate_identities(mdb);
    }
    if (strcmp(argv[1], "foreign") == 0 && argc >= 5 && (strcmp(argv[2], "receipt") == 0 || strcmp(argv[2], "pack-retire") == 0)) {
#ifdef BONFYRE_HAVE_ZSTD
        if (!strcmp(argv[2], "pack-retire")) return foreign_pack_retire(argv[3], argv[4]);
        return foreign_receipt(argv[3], argv[4]);
#else
        fprintf(stderr, "this build carries no zstd, so it reads no receipt pack\n");
        return 1;
#endif
    }
    if (strcmp(argv[1], "foreign") == 0 && argc >= 5 && (strcmp(argv[2], "pack") == 0 || strcmp(argv[2], "pack-verify") == 0)) {
#ifdef BONFYRE_HAVE_ZSTD
        return strcmp(argv[2], "pack") == 0 ? foreign_pack(argv[3], argv[4]) : foreign_pack_verify(argv[3], argv[4]);
#else
        fprintf(stderr, "this build carries no zstd, so it neither writes nor reads a receipt pack\n");
        return 1;
#endif
    }
    if (strcmp(argv[1], "foreign") == 0 && argc >= 5 && (strcmp(argv[2], "pack-bodies") == 0 || strcmp(argv[2], "pack-bodies-verify") == 0 || strcmp(argv[2], "body") == 0)) {
#ifdef BONFYRE_HAVE_ZSTD
        if (!strcmp(argv[2], "body")) return foreign_body(argv[3], argv[4]);
        return strcmp(argv[2], "pack-bodies") == 0 ? foreign_pack_bodies(argv[3], argv[4]) : foreign_pack_bodies_verify(argv[3], argv[4]);
#else
        fprintf(stderr, "this build carries no zstd, so it neither writes nor reads a receipt pack\n");
        return 1;
#endif
    }
    if (strcmp(argv[1], "foreign") == 0 && argc >= 3 && strcmp(argv[2], "hydration") == 0) {
        const char *dir = getenv("BONFYRE_FOREIGN_RECEIPTS");
        if (!dir || !*dir) dir = "/var/lib/bonfyre/foreign/receipts";
        return foreign_hydration(dir);
    }


    if (strcmp(argv[1], "foreign") == 0 && argc >= 3 && strcmp(argv[2], "edges") == 0) {
        const char *gdb = getenv("BONFYRE_FOREIGN_GRAPH");
        const char *scope = "public";
        int a;
        if (!gdb || !*gdb) gdb = "/var/lib/bonfyre/foreign/graph.db";
        for (a = 3; a < argc; a++)
            if (!strcmp(argv[a], "--scope") && a + 1 < argc) scope = argv[++a];
        return foreign_edges(gdb, scope);
    }
    if (strcmp(argv[1], "foreign") == 0 && argc >= 6 && strcmp(argv[2], "relate") == 0) {
        const char *gdb = getenv("BONFYRE_FOREIGN_GRAPH");
        const char *federation = "unknown", *carrier = "unknown";
        const char *evidence = "", *scope = "public";
        int a;
        if (!gdb || !*gdb) gdb = "/var/lib/bonfyre/foreign/graph.db";
        for (a = 6; a < argc; a++) {
            if (!strcmp(argv[a], "--federation") && a + 1 < argc) federation = argv[++a];
            else if (!strcmp(argv[a], "--carrier") && a + 1 < argc) carrier = argv[++a];
            else if (!strcmp(argv[a], "--evidence") && a + 1 < argc) evidence = argv[++a];
            else if (!strcmp(argv[a], "--scope") && a + 1 < argc) scope = argv[++a];
        }
        return foreign_relate(gdb, argv[3], argv[4], argv[5], federation, carrier, evidence, scope);
    }
    if (strcmp(argv[1], "foreign") == 0 && argc >= 3 && strcmp(argv[2], "admissible") == 0) {
        const char *dir = getenv("BONFYRE_FOREIGN_RECEIPTS");
        if (!dir || !*dir) dir = "/var/lib/bonfyre/foreign/receipts";
        return foreign_admissible(dir);
    }

    if (strcmp(argv[1], "version") == 0 && argc >= 3) {
        if (!strcmp(argv[2], "take") && argc >= 5)
            return cell_version_take(getenv("BONFYRE_CMS_DB"), argv[3], argv[4],
                                     getenv("BONFYRE_ACTOR"));
        if (!strcmp(argv[2], "diff") && argc >= 5)
            return cell_version_diff(getenv("BONFYRE_CMS_DB"), argv[3], argv[4]);
        fprintf(stderr, "version take <root> <label> | version diff <from> <to>\n");
        return 1;
    }
    if (strcmp(argv[1], "message") == 0 && argc >= 7 && strcmp(argv[2], "compose") == 0) {
        return cell_message_compose(getenv("BONFYRE_CMS_DB"), argv[3], argv[4], argv[5], argv[6],
                                    getenv("BONFYRE_ACTOR"));
    }
    if (strcmp(argv[1], "sign") == 0 && argc >= 3) {
        if (!strcmp(argv[2], "record") && argc >= 6)
            return cell_sign(getenv("BONFYRE_CMS_DB"), argv[3], argv[4], argv[5],
                             getenv("BONFYRE_ACTOR"));
        if (!strcmp(argv[2], "check") && argc >= 5)
            return cell_sign_check(getenv("BONFYRE_CMS_DB"), argv[3], argv[4]);
        fprintf(stderr, "sign record <about> <file> <statement> | sign check <signature> <file>\n");
        return 1;
    }
    if (strcmp(argv[1], "session") == 0 && argc >= 4) {
        /* open takes a label; end takes what it points at for replay. Passing
         * one as the other put a replay reference in the label slot and then
         * reported the session as having no replay. */
        const char *label = (!strcmp(argv[2], "open") && argc >= 5) ? argv[4] : "";
        const char *replay = (!strcmp(argv[2], "end") && argc >= 5) ? argv[4] : "";
        return cell_session(getenv("BONFYRE_CMS_DB"), argv[2], argv[3], label,
                            getenv("BONFYRE_ACTOR"), replay);
    }
    if (strcmp(argv[1], "discovery") == 0 && argc >= 4 && strcmp(argv[2], "run") == 0) {
        long limit = 0;
        int write = 0, a;
        for (a = 4; a < argc; a++) {
            if (!strcmp(argv[a], "--write")) write = 1;
            else if (!strcmp(argv[a], "--limit") && a + 1 < argc) limit = strtol(argv[++a], NULL, 10);
        }
        return discovery_run(getenv("BONFYRE_CMS_DB"), getenv("BONFYRE_SOURCE_FRONTIER_DB"),
                             argv[3], limit, write);
    }
    if (strcmp(argv[1], "discovery") == 0 && argc >= 6 && strcmp(argv[2], "decide") == 0) {
        return discovery_decide(getenv("BONFYRE_CMS_DB"), argv[3], argv[4], argv[5],
                                getenv("BONFYRE_ACTOR"), argc >= 7 ? argv[6] : "");
    }
    if (strcmp(argv[1], "foreign") == 0 && argc >= 3 && strcmp(argv[2], "release-impact") == 0) {
        int write = (argc >= 4 && !strcmp(argv[3], "--write"));
        return release_impact(getenv("BONFYRE_CMS_DB"), getenv("BONFYRE_SOURCE_FRONTIER_DB"),
                              getenv("BONFYRE_ITEM_ARCHIVE_DB"), getenv("BONFYRE_PRICES_DB"), write);
    }
    if (strcmp(argv[1], "foreign") == 0 && argc >= 3 && strcmp(argv[2], "release-deliver") == 0) {
        const char *root = getenv("BONFYRE_RELEASE_ROOT");
        const char *expect = "";
        int a;
        if (argc < 5) { print_usage(); return 1; }
        for (a = 5; a + 1 < argc; a += 2)
            if (!strcmp(argv[a], "--expect-generation")) expect = argv[a + 1];
        return release_deliver(getenv("BONFYRE_CMS_DB"), getenv("BONFYRE_ITEM_ARCHIVE_DB"),
                               getenv("BONFYRE_SOURCE_FRONTIER_DB"), getenv("BONFYRE_PRICES_DB"),
                               root && *root ? root : "", argv[3], argv[4], expect);
    }
    if (strcmp(argv[1], "foreign") == 0 && argc >= 3 && strcmp(argv[2], "release-compile") == 0) {
        int write = (argc >= 4 && !strcmp(argv[3], "--write"));
        return release_compile(getenv("BONFYRE_CMS_DB"), getenv("BONFYRE_PRICES_DB"),
                               getenv("BONFYRE_SOURCE_FRONTIER_DB"),
                               getenv("BONFYRE_ITEM_ARCHIVE_DB"), write);
    }
    if (strcmp(argv[1], "foreign") == 0 && argc >= 3 && strcmp(argv[2], "twins") == 0) {
        const char *dir = getenv("BONFYRE_FOREIGN_RECEIPTS");
        char mdb_buf[PATH_MAX]; const char *mdb = foreign_matrix_db(mdb_buf, sizeof(mdb_buf));
        long page_offset = 0, page_limit = 0;
        const char *query = "";
        const char *expected_generation = "";
        int a;
        if (!dir || !*dir) dir = "/var/lib/bonfyre/foreign/receipts";
        if (argc >= 4 && !strcmp(argv[3], "--receipts")) return foreign_twins_receipts(dir, mdb);
        if (argc >= 4) {
            for (a = 3; a < argc; ) {
                if (a + 1 >= argc) { print_usage(); return 1; }
                if (!strcmp(argv[a], "--page") && foreign_page_number(argv[a + 1], &page_offset)) {
                    a += 2; continue;
                }
                if (!strcmp(argv[a], "--limit") && foreign_page_number(argv[a + 1], &page_limit)) {
                    a += 2; continue;
                }
                if (!strcmp(argv[a], "--query") && strlen(argv[a + 1]) <= 240) {
                    query = argv[a + 1]; a += 2; continue;
                }
                if (!strcmp(argv[a], "--expect-generation") && strlen(argv[a + 1]) < 32) {
                    expected_generation = argv[a + 1]; a += 2; continue;
                }
                print_usage(); return 1;
            }
            if (page_limit < 1 || page_limit > 500) { print_usage(); return 1; }
        }
        return foreign_twins(mdb, page_offset, page_limit, query, expected_generation);
    }
    if (strcmp(argv[1], "foreign") == 0 && argc >= 3 && strcmp(argv[2], "promote") == 0) {
        const char *dir = getenv("BONFYRE_FOREIGN_RECEIPTS");
        char mdb_buf[PATH_MAX]; const char *mdb = foreign_matrix_db(mdb_buf, sizeof(mdb_buf));
        const char *only = NULL;
        int dry = 0, a;
        if (!dir || !*dir) dir = "/var/lib/bonfyre/foreign/receipts";
        for (a = 3; a < argc; a++) {
            if (!strcmp(argv[a], "--dry-run")) dry = 1;
            else if (argv[a][0] != '-') only = argv[a];
        }
        return foreign_promote(dir, mdb, only, dry, 0);
    }
    if (strcmp(argv[1], "foreign") == 0 && argc >= 3 &&
        (strcmp(argv[2], "resolve") == 0 || strcmp(argv[2], "history") == 0 ||
         strcmp(argv[2], "conflicts") == 0)) {
        const char *dir = getenv("BONFYRE_FOREIGN_RECEIPTS");
        if (!dir || !*dir) dir = "/var/lib/bonfyre/foreign/receipts";
        return foreign_resolution(dir, argv[2], argc >= 4 ? argv[3] : NULL);
    }
    if (strcmp(argv[1], "foreign") == 0 && argc >= 3 && strcmp(argv[2], "reheat") == 0) {
        const char *receipts = getenv("BONFYRE_FOREIGN_RECEIPTS");
        const char *bodies = getenv("BONFYRE_FOREIGN_BODIES");
        const char *cms = getenv("BONFYRE_CMS_DB");
        const char *weather = getenv("BONFYRE_WEATHER");
        int dry = 0;
        const char *only = NULL;
        for (int i = 3; i < argc; i++) {
            if (strcmp(argv[i], "--dry-run") == 0) dry = 1;
            else if (strcmp(argv[i], "--source") == 0 && i + 1 < argc) only = argv[++i];
            else if (strcmp(argv[i], "--credential") == 0 && i + 1 < argc) reheat_only_credential = argv[++i];
            else if (strcmp(argv[i], "--budget") == 0 && i + 1 < argc) reheat_budget = atoi(argv[++i]);
            else if (strcmp(argv[i], "--per-host") == 0 && i + 1 < argc) reheat_per_host = atoi(argv[++i]);
        }
        {
            /* Every other organ here is pointed at one foreign root; the reheat
             * used to take only its two leaf directories, so a caller that set
             * the root saw a scheduler that believed nothing had ever been
             * read. */
            static char from_root_receipts[PATH_MAX], from_root_bodies[PATH_MAX];
            const char *root = getenv("BONFYRE_FOREIGN_ROOT");
            if (root && *root) {
                snprintf(from_root_receipts, sizeof(from_root_receipts), "%s/receipts", root);
                snprintf(from_root_bodies, sizeof(from_root_bodies), "%s/bodies", root);
                if (!receipts || !*receipts) receipts = from_root_receipts;
                if (!bodies || !*bodies) bodies = from_root_bodies;
            }
        }
        if (!receipts || !*receipts) receipts = "/var/lib/bonfyre/foreign/receipts";
        if (!bodies || !*bodies) bodies = "/var/lib/bonfyre/foreign/bodies";
        if (!cms || !*cms) cms = "/var/lib/bonfyre/cms/bonfyre_cms.db";
        if (!weather || !*weather) weather = "/var/lib/bonfyre/foreign/weather.json";
        return foreign_reheat(cms, receipts, bodies, weather, dry, only);
    }
    if (strcmp(argv[1], "foreign") == 0 && argc >= 5 && strcmp(argv[2], "observe") == 0) {
        const char *receipt_path = NULL;
        const char *body_path = NULL;
        const char *observe_right = "public";
        const char *robots_url = NULL;
        long long max_bytes = 0, range_start = 0;
        int bounded_full = 0, bounded_range = 0;
        for (int i = 5; i < argc; i++) {
            if (strcmp(argv[i], "--receipt") == 0 && i + 1 < argc) receipt_path = argv[++i];
            else if (strcmp(argv[i], "--body") == 0 && i + 1 < argc) body_path = argv[++i];
            else if (strcmp(argv[i], "--observe-right") == 0 && i + 1 < argc) observe_right = argv[++i];
            else if (strcmp(argv[i], "--robots") == 0 && i + 1 < argc) robots_url = argv[++i];
            else if (strcmp(argv[i], "--max-bytes") == 0 && i + 1 < argc) { max_bytes = atoll(argv[++i]); bounded_range = 1; }
            else if (strcmp(argv[i], "--range-start") == 0 && i + 1 < argc) { range_start = atoll(argv[++i]); bounded_range = 1; }
            else if (strcmp(argv[i], "--full-max-bytes") == 0 && i + 1 < argc) { max_bytes = atoll(argv[++i]); range_start = -1; bounded_full = 1; }
            else {
                print_usage();
                return 1;
            }
        }
        if (strcmp(observe_right, "public") != 0 && strcmp(observe_right, "denied") != 0) {
            fprintf(stderr, "--observe-right must be public or denied\n");
            return 1;
        }
        if (robots_url && !is_http_url(robots_url)) {
            fprintf(stderr, "--robots requires an http(s) URL\n");
            return 1;
        }
        if ((bounded_full && bounded_range) || max_bytes < 0 || max_bytes > 12000000 ||
            (bounded_full && max_bytes == 0) || range_start < -1 ||
            (range_start >= 0 && range_start > LLONG_MAX - max_bytes) || (range_start > 0 && max_bytes == 0)) {
            fprintf(stderr, "bounded observation requires 1..12000000 bytes and one valid mode\n");
            return 1;
        }
        return observe_foreign(argv[3], argv[4], receipt_path, body_path,
                               observe_right, robots_url, range_start, max_bytes);
    }

    print_usage();
    return 1;
}
