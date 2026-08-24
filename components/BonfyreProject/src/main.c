#define _POSIX_C_SOURCE 200809L

#include <limits.h>
#include <ctype.h>
#include <errno.h>
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

static void resolve_executable_sibling(char *buffer, size_t size, const char *argv0, const char *sibling_dir, const char *binary_name) {
    if (argv0 && argv0[0] == '/') snprintf(buffer, size, "%s", argv0);
    else if (argv0 && strstr(argv0, "/")) {
        char cwd[PATH_MAX];
        if (getcwd(cwd, sizeof(cwd))) snprintf(buffer, size, "%s/%s", cwd, argv0);
        else snprintf(buffer, size, "%s", argv0);
    } else {
        buffer[0] = '\0';
        return;
    }
    char *last = strrchr(buffer, '/');
    if (!last) { buffer[0] = '\0'; return; }
    *last = '\0';
    last = strrchr(buffer, '/');
    if (!last) { buffer[0] = '\0'; return; }
    *last = '\0';
    snprintf(buffer, size, "%s/%s/%s", buffer, sibling_dir, binary_name);
}

static const char *default_binary(const char *env_name, const char *argv0, char *resolved, size_t resolved_size, const char *dir, const char *name, const char *fallback) {
    const char *env = getenv(env_name);
    if (env && env[0] != '\0') return env;
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
            "  bonfyre-project foreign observe <twin-id> <https-url> [--receipt FILE] [--body FILE] [--observe-right public|denied] [--robots URL]\n");
}

static int is_http_url(const char *url) {
    return (strncmp(url, "https://", 8) == 0 && url[8] != '\0') ||
           (strncmp(url, "http://", 7) == 0 && url[7] != '\0');
}

static void json_string(FILE *out, const char *text) {
    fputc('"', out);
    for (const unsigned char *p = (const unsigned char *)text; *p; p++) {
        switch (*p) {
            case '"': fputs("\\\"", out); break;
            case '\\': fputs("\\\\", out); break;
            case '\b': fputs("\\b", out); break;
            case '\f': fputs("\\f", out); break;
            case '\n': fputs("\\n", out); break;
            case '\r': fputs("\\r", out); break;
            case '\t': fputs("\\t", out); break;
            default:
                if (*p < 0x20) fprintf(out, "\\u%04x", *p);
                else fputc(*p, out);
        }
    }
    fputc('"', out);
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

static int fetch_to_file(const char *url, const char *body_path, int *http_status) {
    char status_text[32] = {0};
    char *const curl_command[] = {
        "curl", "--location", "--silent", "--show-error", "--max-time", "20",
        "--user-agent", "BonfyreProject/1.0", "--output", (char *)body_path,
        "--write-out", "%{http_code}", (char *)url, NULL
    };
    int curl_exit = read_child_stdout(curl_command, status_text, sizeof(status_text));
    *http_status = atoi(status_text);
    return curl_exit;
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

static void write_policy_receipt(FILE *receipt, const char *twin_id, const char *url,
                                 const char *observed_at, const char *observe_right,
                                 const char *robots_url, int robots_exit, int robots_status,
                                 const char *policy_decision, int fetch_attempted,
                                 const char *observation_kind, int transport_exit,
                                 int http_status, long long body_bytes,
                                 const char *digest, const char *retain_body_path) {
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
    fputs(",\"availability_claim\":false,\"observation_kind\":", receipt);
    json_string(receipt, observation_kind);
    fputs("}\n", receipt);
}

static int observe_foreign(const char *twin_id, const char *url, const char *receipt_path,
                           const char *retain_body_path, const char *observe_right,
                           const char *robots_url_override) {
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
                             0, 0, 0, NULL, retain_body_path);
        if (receipt_path) fclose(receipt);
        return excluded ? 0 : 3;
    }

    char scratch_path[] = "/tmp/bonfyre-foreign-body-XXXXXX";
    const char *body_path = retain_body_path;
    if (!body_path) {
        int body_fd = mkstemp(scratch_path);
        if (body_fd < 0) { perror("mkstemp"); return 1; }
        close(body_fd);
        body_path = scratch_path;
    }

    int http_status = 0;
    int curl_exit = fetch_to_file(url, body_path, &http_status);
    struct stat body_stat;
    long long body_bytes = stat(body_path, &body_stat) == 0 ? (long long)body_stat.st_size : 0;
    char digest[65] = "";
    int has_digest = body_bytes >= 0 && sha256_file(body_path, digest) == 0;

    write_policy_receipt(receipt, twin_id, url, observed_at, observe_right, robots_url,
                         robots_exit, robots_status, policy_decision, 1,
                         (curl_exit == 0 && http_status >= 200 && http_status < 300)
                             ? "source_observed" : "boundary_response",
                         curl_exit, http_status, body_bytes, has_digest ? digest : NULL,
                         retain_body_path);
    if (receipt_path) fclose(receipt);
    if (!retain_body_path) unlink(body_path);
    return (curl_exit == 0 && http_status >= 200 && http_status < 300) ? 0 : 3;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        print_usage();
        return 1;
    }

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

    if (strcmp(argv[1], "foreign") == 0 && argc >= 5 && strcmp(argv[2], "observe") == 0) {
        const char *receipt_path = NULL;
        const char *body_path = NULL;
        const char *observe_right = "public";
        const char *robots_url = NULL;
        for (int i = 5; i < argc; i++) {
            if (strcmp(argv[i], "--receipt") == 0 && i + 1 < argc) receipt_path = argv[++i];
            else if (strcmp(argv[i], "--body") == 0 && i + 1 < argc) body_path = argv[++i];
            else if (strcmp(argv[i], "--observe-right") == 0 && i + 1 < argc) observe_right = argv[++i];
            else if (strcmp(argv[i], "--robots") == 0 && i + 1 < argc) robots_url = argv[++i];
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
        return observe_foreign(argv[3], argv[4], receipt_path, body_path, observe_right, robots_url);
    }

    print_usage();
    return 1;
}
