#define _POSIX_C_SOURCE 200809L
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <sqlite3.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define LABEL "com.bonfyre.project-mcp"
#define PORT "3051"

static void usage(FILE *out) {
    fprintf(out,
        "bonfyre-project-mcp — scoped Project MCP lifecycle owner\n\n"
        "Usage:\n"
        "  bonfyre-project-mcp status\n"
        "  bonfyre-project-mcp verify\n"
        "  bonfyre-project-mcp reload --apply\n"
        "  bonfyre-project-mcp ratchet\n"
        "  bonfyre-project-mcp ratchet-top\n"
        "  bonfyre-project-mcp organ-sync <bonfyre-target> --apply\n"
        "  bonfyre-project-mcp estate-maturity-refresh --apply\n"
        "  bonfyre-project-mcp public-candidates\n"
        "  bonfyre-project-mcp public-status\n"
        "  bonfyre-project-mcp public-self-test\n"
        "  bonfyre-project-mcp public-verify <activity-public-release>\n"
        "  bonfyre-project-mcp public-stage --apply\n"
        "  bonfyre-project-mcp public-verify-group <activity-public-release> <index> --apply\n"
        "  bonfyre-project-mcp public-finalize <activity-public-release> --apply\n"
        "  bonfyre-project-mcp public-readback <activity-public-release>\n"
        "  bonfyre-project-mcp public-rollout --apply\n"
        "  bonfyre-project-mcp public-activate <activity-public-release> --apply\n"
        "  bonfyre-project-mcp work-session-open <goal-id> <agent> <role> <session-id> --apply\n"
        "  bonfyre-project-mcp work-session-step <run-id> <tool> <state> <receipt> --apply\n"
        "  bonfyre-project-mcp work-session-close <run-id> <state> <summary> --apply\n"
        "  bonfyre-project-mcp self-test\n\n"
        "reload only kickstarts " LABEL " after an ephemeral parity witness; it never rewrites the plist or operator STOP.\n");
}

static bool regular_file(const char *path) {
    struct stat st;
    return lstat(path, &st) == 0 && S_ISREG(st.st_mode) && !S_ISLNK(st.st_mode);
}

static int wait_code(pid_t pid) {
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) return 127;
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    if (WIFSIGNALED(status)) return 128 + WTERMSIG(status);
    return 127;
}

static int run_inherit(char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) return 127;
    if (pid == 0) {
        execv(argv[0], argv);
        _exit(127);
    }
    return wait_code(pid);
}

static int run_capture(char *const argv[], char *out, size_t capacity, int allow_one) {
    int fds[2];
    if (!out || capacity < 2 || pipe(fds) != 0) return 127;
    pid_t pid = fork();
    if (pid < 0) { close(fds[0]); close(fds[1]); return 127; }
    if (pid == 0) {
        close(fds[0]);
        dup2(fds[1], STDOUT_FILENO);
        dup2(fds[1], STDERR_FILENO);
        close(fds[1]);
        execv(argv[0], argv);
        _exit(127);
    }
    close(fds[1]);
    size_t used = 0;
    while (used + 1 < capacity) {
        ssize_t n = read(fds[0], out + used, capacity - used - 1);
        if (n > 0) used += (size_t)n;
        else break;
    }
    out[used] = '\0';
    char drain[4096];
    while (read(fds[0], drain, sizeof drain) > 0) {}
    close(fds[0]);
    int rc = wait_code(pid);
    if (rc != 0 && !(allow_one && rc == 1)) return rc;
    return rc;
}

static int root_paths(const char *argv0, char *root, size_t rootn, char *home, size_t homen, char *bun, size_t bunn) {
    (void)rootn; (void)bunn;
    const char *configured_root = getenv("BONFYRE_ROOT");
    if (configured_root && *configured_root) {
        if (!realpath(configured_root, root)) { perror("realpath BONFYRE_ROOT"); return 1; }
    } else {
        char executable[PATH_MAX];
        if (!realpath(argv0, executable)) { perror("realpath executable"); return 1; }
        for (int i = 0; i < 3; i++) {
            char *slash = strrchr(executable, '/');
            if (!slash || slash == executable) { fprintf(stderr, "cannot derive repository root from executable\n"); return 1; }
            *slash = '\0';
        }
        if (!realpath(executable, root)) { perror("realpath derived repo"); return 1; }
    }
    char marker[PATH_MAX];
    if (snprintf(marker, sizeof marker, "%s/integrations/mcp/project_node.mjs", root) >= (int)sizeof marker
        || !regular_file(marker)) {
        fprintf(stderr, "resolved Bonfyre root does not own Project MCP\n");
        return 1;
    }
    const char *h = getenv("HOME");
    if (!h || !*h || snprintf(home, homen, "%s", h) >= (int)homen) return 1;
    const char *configured = getenv("BONFYRE_BUN");
    char candidate[PATH_MAX];
    if (configured && *configured) {
        if (snprintf(candidate, sizeof candidate, "%s", configured) >= (int)sizeof candidate) return 1;
    } else {
        if (snprintf(candidate, sizeof candidate, "%s/.bun/bin/bun", home) >= (int)sizeof candidate) return 1;
    }
    if (!realpath(candidate, bun) || access(bun, X_OK) != 0) {
        fprintf(stderr, "Bun executable unavailable: %s\n", candidate);
        return 1;
    }
    return 0;
}

static int plist_value(const char *plist, const char *key, const char *format, char *out, size_t outn) {
    char *const argv[] = {
        "/usr/bin/plutil", "-extract", (char *)key, (char *)format, "-o", "-", (char *)plist, NULL
    };
    int rc = run_capture(argv, out, outn, 0);
    if (rc != 0) fprintf(stderr, "plutil could not read %s\n", key);
    return rc;
}

static void normalize_plutil_json(char *value) {
    char *read = value, *write = value;
    while (*read) {
        if (read[0] == '\\' && read[1] == '/') read++;
        *write++ = *read++;
    }
    *write = '\0';
    value[strcspn(value, "\r\n")] = '\0';
}

static int validate_plist(const char *root, const char *home, const char *bun) {
    char plist[PATH_MAX], entry[PATH_MAX], value[16384];
    if (snprintf(plist, sizeof plist, "%s/Library/LaunchAgents/%s.plist", home, LABEL) >= (int)sizeof plist) return 1;
    if (!regular_file(plist)) { fprintf(stderr, "Project MCP LaunchAgent is not a regular file\n"); return 1; }
    if (snprintf(entry, sizeof entry, "%s/integrations/mcp/project_node.mjs", root) >= (int)sizeof entry) return 1;
    if (plist_value(plist, "Label", "raw", value, sizeof value) != 0 || strncmp(value, LABEL, strlen(LABEL)) != 0) {
        fprintf(stderr, "Project MCP LaunchAgent label mismatch\n"); return 1;
    }
    if (plist_value(plist, "ProgramArguments", "json", value, sizeof value) != 0) return 1;
    normalize_plutil_json(value);
    char expectedProgram[2 * PATH_MAX + 16];
    if (snprintf(expectedProgram, sizeof expectedProgram, "[\"%s\",\"%s\"]", bun, entry) >= (int)sizeof expectedProgram
        || strcmp(value, expectedProgram) != 0) {
        fprintf(stderr, "Project MCP LaunchAgent program mismatch; expected=%s observed=%s\n", expectedProgram, value);
        return 1;
    }
    if (plist_value(plist, "EnvironmentVariables.BONFYRE_ROOT", "raw", value, sizeof value) != 0
        || !strstr(value, root)) {
        fprintf(stderr, "Project MCP root mismatch\n"); return 1;
    }
    if (plist_value(plist, "EnvironmentVariables.BONFYRE_PROJECT_MCP_PORT", "raw", value, sizeof value) != 0
        || strncmp(value, PORT, strlen(PORT)) != 0) {
        fprintf(stderr, "Project MCP port mismatch\n"); return 1;
    }
    if (plist_value(plist, "EnvironmentVariables.BONFYRE_PROJECT_MCP_HOST", "raw", value, sizeof value) != 0
        || !strstr(value, "127.0.0.1")) {
        fprintf(stderr, "Project MCP host mismatch\n"); return 1;
    }
    return 0;
}

static int service_state(char *buffer, size_t n, pid_t *pid) {
    char domain[128];
    if (snprintf(domain, sizeof domain, "gui/%u/%s", (unsigned)getuid(), LABEL) >= (int)sizeof domain) return 1;
    char *const argv[] = { "/bin/launchctl", "print", domain, NULL };
    int rc = run_capture(argv, buffer, n, 0);
    if (rc != 0) return rc;
    const char *p = strstr(buffer, "pid = ");
    if (!p || sscanf(p, "pid = %d", pid) != 1 || *pid <= 1 || !strstr(buffer, "state = running")) return 1;
    return 0;
}

static int quiet_listener(pid_t pid) {
    char pidbuf[32], output[32768];
    snprintf(pidbuf, sizeof pidbuf, "%d", pid);
    char *const argv[] = { "/usr/sbin/lsof", "-nP", "-a", "-p", pidbuf, "-iTCP:" PORT, NULL };
    int rc = run_capture(argv, output, sizeof output, 1);
    if (rc != 0 && rc != 1) return rc;
    int established = 0;
    for (char *line = output; *line;) {
        char *line_end = strchr(line, '\n');
        if (line_end) *line_end = '\0';
        if (strstr(line, "(ESTABLISHED)") && strstr(line, "127.0.0.1:" PORT "->127.0.0.1:")) {
            established++;
        } else if (strstr(line, "(ESTABLISHED)")) {
            fprintf(stderr, "Project MCP unexpected established connection; reload refused: %s\n", line);
            return 2;
        }
        if (!line_end) break;
        line = line_end + 1;
    }
    if (established > 16) {
        fprintf(stderr, "Project MCP has %d loopback connections, exceeding the bounded reload aperture\n", established);
        return 2;
    }
    return 0;
}

static int witness(const char *root, const char *bun, int baseline) {
    char path[PATH_MAX];
    if (snprintf(path, sizeof path, "%s/integrations/mcp/tests/project_live_witness.mjs", root) >= (int)sizeof path) return 1;
    if (!regular_file(path)) { fprintf(stderr, "Project MCP witness unavailable\n"); return 1; }
    char *argv_base[] = { (char *)bun, path, "--baseline", NULL };
    char *argv_live[] = { (char *)bun, path, NULL };
    return run_inherit(baseline ? argv_base : argv_live);
}

static int health_current(char *out, size_t n) {
    char *const argv[] = {
        "/usr/bin/curl", "--silent", "--show-error", "--fail", "--max-time", "2",
        "http://127.0.0.1:" PORT "/health", NULL
    };
    int rc = run_capture(argv, out, n, 0);
    if (rc != 0) return rc;
    return strstr(out, "\"runtime\":\"bun\"") && strstr(out, "\"state\":\"current\"") ? 0 : 1;
}

static int cmd_status(const char *root, const char *home, const char *bun) {
    if (validate_plist(root, home, bun) != 0) return 1;
    char state[65536], health[65536]; pid_t pid = 0;
    if (service_state(state, sizeof state, &pid) != 0) { fprintf(stderr, "Project MCP service is not running\n"); return 1; }
    int current = health_current(health, sizeof health) == 0;
    printf("PROJECT_MCP_STATUS pid=%d source=%s\n", pid, current ? "current" : "not-current");
    if (!current) printf("%s\n", health);
    return current ? 0 : 2;
}

static int cmd_verify(const char *root, const char *home, const char *bun) {
    if (validate_plist(root, home, bun) != 0) return 1;
    char state[65536]; pid_t pid = 0;
    if (service_state(state, sizeof state, &pid) != 0 || quiet_listener(pid) != 0) return 1;
    int rc = witness(root, bun, 1);
    if (rc == 0) printf("PROJECT_MCP_VERIFY candidate-compatible-with-live pid=%d\n", pid);
    return rc;
}

static int cmd_reload(const char *root, const char *home, const char *bun) {
    if (validate_plist(root, home, bun) != 0) return 1;
    char state[65536]; pid_t previous = 0;
    if (service_state(state, sizeof state, &previous) != 0 || quiet_listener(previous) != 0) return 1;
    if (witness(root, bun, 1) != 0) { fprintf(stderr, "ephemeral Project MCP witness failed; reload refused\n"); return 1; }
    if (quiet_listener(previous) != 0) return 1;
    char domain[128];
    snprintf(domain, sizeof domain, "gui/%u/%s", (unsigned)getuid(), LABEL);
    char *const kick[] = { "/bin/launchctl", "kickstart", "-k", domain, NULL };
    if (run_inherit(kick) != 0) { fprintf(stderr, "Project MCP kickstart failed\n"); return 1; }

    pid_t current = 0;
    char health[65536];
    int ready = 0;
    for (int i = 0; i < 100; i++) {
        usleep(200000);
        if (service_state(state, sizeof state, &current) == 0 && current != previous
            && health_current(health, sizeof health) == 0) { ready = 1; break; }
    }
    if (!ready) { fprintf(stderr, "Project MCP did not return with current source identity\n"); return 1; }
    if (witness(root, bun, 0) != 0) { fprintf(stderr, "live Project MCP witness failed after reload\n"); return 1; }
    printf("PROJECT_MCP_RELOADED previous_pid=%d pid=%d source=current\n", previous, current);
    return 0;
}

static bool safe_organ_target(const char *target) {
    static const char prefix[] = "bonfyre-";
    size_t n = target ? strlen(target) : 0;
    if (n <= sizeof(prefix) - 1 || n > 96 || strncmp(target, prefix, sizeof(prefix) - 1) != 0) return false;
    for (const unsigned char *p = (const unsigned char *)target; *p; p++)
        if (!(islower(*p) || isdigit(*p) || *p == '-')) return false;
    return target[n - 1] != '-';
}

static int cmd_organ_sync(const char *root, const char *home, const char *target) {
    char script[PATH_MAX], stop[PATH_MAX];
    if (!safe_organ_target(target)) { fprintf(stderr, "invalid organ target\n"); return 2; }
    if (snprintf(stop, sizeof stop, "%s/.bonfyre/dev-fabric/STOP", home) >= (int)sizeof stop) return 1;
    if (regular_file(stop)) {
        fprintf(stderr, "STOP active; organ drift sync refused\n");
        return 3;
    }
    if (snprintf(script, sizeof script, "%s/scripts/organ_drift.sh", root) >= (int)sizeof script || !regular_file(script)) {
        fprintf(stderr, "canonical organ drift owner unavailable\n");
        return 1;
    }
    char *const sync_argv[] = { script, "--sync", "--only", (char *)target, "--json", NULL };
    int rc = run_inherit(sync_argv);
    if (rc != 0) { fprintf(stderr, "organ drift sync failed for %s\n", target); return rc; }
    char *const verify_argv[] = { script, "--verify", "--only", (char *)target, "--json", NULL };
    rc = run_inherit(verify_argv);
    if (rc != 0) fprintf(stderr, "organ drift verify failed for %s\n", target);
    return rc;
}

static bool safe_release_name(const char *name) {
    static const char prefix[] = "activity-public-";
    size_t n = name ? strlen(name) : 0;
    if (n <= sizeof(prefix) - 1 || n > 111 || strncmp(name, prefix, sizeof(prefix) - 1) != 0) return false;
    for (const unsigned char *p = (const unsigned char *)name; *p; p++)
        if (!(isalnum(*p) || *p == '.' || *p == '_' || *p == '-')) return false;
    return true;
}
static int public_release_path(const char *home, const char *name, char resolved[PATH_MAX]) {
    if (!safe_release_name(name)) { fprintf(stderr, "invalid activity-public release basename\n"); return 2; }
    char release[PATH_MAX], parent[PATH_MAX], expectedParent[PATH_MAX], check[PATH_MAX];
    if (snprintf(release, sizeof release, "%s/.bonfyre/releases/%s", home, name) >= (int)sizeof release) return 1;
    if (!realpath(release, resolved)) { fprintf(stderr, "public release unavailable\n"); return 1; }
    if (snprintf(parent, sizeof parent, "%s/.bonfyre/releases", home) >= (int)sizeof parent || !realpath(parent, expectedParent)) return 1;
    snprintf(check, sizeof check, "%s", resolved);
    char *slash = strrchr(check, '/'); if (!slash) return 1; *slash = '\0';
    if (strcmp(check, expectedParent) != 0) { fprintf(stderr, "public release escapes owned directory\n"); return 1; }
    return 0;
}

static bool safe_group_index(const char *text) {
    if (!text || !*text) return false;
    long value = 0;
    for (const unsigned char *p = (const unsigned char *)text; *p; p++) {
        if (!isdigit(*p)) return false;
        value = value * 10 + (*p - '0');
        if (value > 63) return false;
    }
    return true;
}


static int cmd_public_status(const char *root, const char *bun) {
    char script[PATH_MAX];
    if (snprintf(script, sizeof script, "%s/scripts/release/public_surface_rollout.mjs", root) >= (int)sizeof script
        || !regular_file(script)) return 1;
    char *const argv[] = { (char *)bun, script, "status", NULL };
    return run_inherit(argv);
}

static int cmd_public_self_test(const char *root, const char *bun) {
    char test[PATH_MAX];
    if (snprintf(test, sizeof test, "%s/scripts/release/tests/public_surface_rollout.test.mjs", root) >= (int)sizeof test || !regular_file(test)) return 1;
    char *const argv[] = { (char *)bun, "test", test, NULL };
    return run_inherit(argv);
}

static int cmd_public_stage(const char *root, const char *bun) {
    char script[PATH_MAX];
    if (snprintf(script, sizeof script, "%s/scripts/release/public_surface_rollout.mjs", root) >= (int)sizeof script || !regular_file(script)) return 1;
    char *const argv[] = { (char *)bun, script, "stage", NULL };
    return run_inherit(argv);
}

static int cmd_public_verify(const char *root, const char *home, const char *bun, const char *name) {
    char resolved[PATH_MAX], script[PATH_MAX];
    int rc = public_release_path(home, name, resolved); if (rc != 0) return rc;
    if (snprintf(script, sizeof script, "%s/scripts/release/public_surface_rollout.mjs", root) >= (int)sizeof script || !regular_file(script)) return 1;
    char *const argv[] = { (char *)bun, script, "verify-frozen", resolved, NULL };
    return run_inherit(argv);
}

static int cmd_public_verify_group(const char *root, const char *home, const char *bun, const char *name, const char *index) {
    char resolved[PATH_MAX], script[PATH_MAX];
    if (!safe_group_index(index)) { fprintf(stderr, "invalid public acceptance group index\n"); return 2; }
    int rc = public_release_path(home, name, resolved); if (rc != 0) return rc;
    if (snprintf(script, sizeof script, "%s/scripts/release/public_surface_rollout.mjs", root) >= (int)sizeof script || !regular_file(script)) return 1;
    char *const argv[] = { (char *)bun, script, "verify-group", resolved, (char *)index, NULL };
    return run_inherit(argv);
}
static int cmd_public_finalize(const char *root, const char *home, const char *bun, const char *name) {
    char resolved[PATH_MAX], script[PATH_MAX];
    int rc = public_release_path(home, name, resolved); if (rc != 0) return rc;
    if (snprintf(script, sizeof script, "%s/scripts/release/public_surface_rollout.mjs", root) >= (int)sizeof script || !regular_file(script)) return 1;
    char *const argv[] = { (char *)bun, script, "verify", resolved, NULL };
    return run_inherit(argv);
}


static int cmd_public_readback(const char *root, const char *home, const char *bun, const char *name) {
    char resolved[PATH_MAX], script[PATH_MAX];
    int rc = public_release_path(home, name, resolved); if (rc != 0) return rc;
    if (snprintf(script, sizeof script, "%s/scripts/release/public_surface_rollout.mjs", root) >= (int)sizeof script || !regular_file(script)) return 1;
    char *const argv[] = { (char *)bun, script, "readback", resolved, NULL };
    return run_inherit(argv);
}

static int cmd_public_activate(const char *root, const char *home, const char *bun, const char *name) {
    char resolved[PATH_MAX], script[PATH_MAX];
    int rc = public_release_path(home, name, resolved); if (rc != 0) return rc;
    if (snprintf(script, sizeof script, "%s/scripts/release/public_surface_rollout.mjs", root) >= (int)sizeof script || !regular_file(script)) return 1;
    char *const argv[] = { (char *)bun, script, "activate", resolved, NULL };
    return run_inherit(argv);
}

static int cmd_public_rollout(const char *root, const char *bun) {
    char script[PATH_MAX];
    if (snprintf(script, sizeof script, "%s/scripts/release/public_surface_rollout.mjs", root) >= (int)sizeof script
        || !regular_file(script)) return 1;
    char *const argv[] = { (char *)bun, script, "rollout", NULL };
    return run_inherit(argv);
}

static double work_now(void) { struct timeval tv; if (gettimeofday(&tv, NULL) != 0) return 0; return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0; }
static bool work_atom(const char *value, size_t max) {
    size_t n = value ? strlen(value) : 0; if (!n || n > max) return false;
    for (const unsigned char *p = (const unsigned char *)value; *p; p++) if (!(isalnum(*p) || *p=='.' || *p=='_' || *p==':' || *p=='-')) return false;
    return true;
}
static bool work_label(const char *value, size_t max) {
    size_t n = value ? strlen(value) : 0; if (!n || n > max) return false;
    for (const unsigned char *p = (const unsigned char *)value; *p; p++) if (*p < 32 || *p == 127) return false;
    return true;
}
static int work_exec(sqlite3 *db, const char *sql) { char *error=NULL; int rc=sqlite3_exec(db,sql,NULL,NULL,&error); if(rc!=SQLITE_OK){fprintf(stderr,"work session sqlite: %s\n",error?error:"error");sqlite3_free(error);return 1;} return 0; }
static int work_db(const char *home, sqlite3 **db) {
    const char *configured=getenv("BONFYRE_OPERATOR_HOME"); char dir[PATH_MAX],path[PATH_MAX]; struct stat st;
    const char *raw=configured&&*configured?configured:NULL;
    if(raw){if(!realpath(raw,dir))return 1;}else if(snprintf(dir,sizeof dir,"%s/.bonfyre/operator",home)>=(int)sizeof dir)return 1;
    if(lstat(dir,&st)!=0||!S_ISDIR(st.st_mode)||S_ISLNK(st.st_mode))return 1;
    if(snprintf(path,sizeof path,"%s/operator.sqlite3",dir)>=(int)sizeof path||!regular_file(path))return 1;
    if(sqlite3_open_v2(path,db,SQLITE_OPEN_READWRITE|SQLITE_OPEN_FULLMUTEX,NULL)!=SQLITE_OK)return 1;sqlite3_busy_timeout(*db,2000);
    return work_exec(*db,"CREATE TABLE IF NOT EXISTS operator_run_context(run_id TEXT PRIMARY KEY,agent TEXT NOT NULL,role TEXT NOT NULL,session_id TEXT NOT NULL,interactive INTEGER NOT NULL DEFAULT 1);CREATE UNIQUE INDEX IF NOT EXISTS operator_run_context_session ON operator_run_context(session_id);");
}
static int cmd_work_session_open(const char *home,const char *goal,const char *agent,const char *role,const char *session) {
    if(!work_atom(goal,128)||!work_label(agent,80)||!work_label(role,80)||!work_atom(session,96)){fprintf(stderr,"invalid interactive work coordinates\n");return 2;}
    char run[128]; if(snprintf(run,sizeof run,"interactive-%s",session)>=(int)sizeof run)return 2;sqlite3 *db=NULL;sqlite3_stmt *q=NULL;int rc=1,reused=0;double now=work_now();
    if(now<=0||work_db(home,&db)||work_exec(db,"BEGIN IMMEDIATE;"))goto done;
    if(sqlite3_prepare_v2(db,"SELECT 1 FROM goals WHERE id=?1",-1,&q,NULL)!=SQLITE_OK)goto rollback;sqlite3_bind_text(q,1,goal,-1,SQLITE_STATIC);if(sqlite3_step(q)!=SQLITE_ROW){fprintf(stderr,"interactive work goal unavailable\n");goto rollback;}sqlite3_finalize(q);q=NULL;
    if(sqlite3_prepare_v2(db,"SELECT r.goal_id,r.completed_at,c.agent,c.role,c.session_id FROM runs r LEFT JOIN operator_run_context c ON c.run_id=r.id WHERE r.id=?1",-1,&q,NULL)!=SQLITE_OK)goto rollback;sqlite3_bind_text(q,1,run,-1,SQLITE_STATIC);
    if(sqlite3_step(q)==SQLITE_ROW){const char *g=(const char *)sqlite3_column_text(q,0),*a=(const char *)sqlite3_column_text(q,2),*r=(const char *)sqlite3_column_text(q,3),*s=(const char *)sqlite3_column_text(q,4);if(sqlite3_column_type(q,1)!=SQLITE_NULL||!g||!a||!r||!s||strcmp(g,goal)||strcmp(a,agent)||strcmp(r,role)||strcmp(s,session)){fprintf(stderr,"interactive work session conflicts with retained run\n");goto rollback;}reused=1;}sqlite3_finalize(q);q=NULL;
    if(!reused){if(sqlite3_prepare_v2(db,"INSERT INTO runs(id,goal_id,started_at,state) VALUES(?1,?2,?3,'running')",-1,&q,NULL)!=SQLITE_OK)goto rollback;sqlite3_bind_text(q,1,run,-1,SQLITE_STATIC);sqlite3_bind_text(q,2,goal,-1,SQLITE_STATIC);sqlite3_bind_double(q,3,now);if(sqlite3_step(q)!=SQLITE_DONE)goto rollback;sqlite3_finalize(q);q=NULL;if(sqlite3_prepare_v2(db,"INSERT INTO operator_run_context(run_id,agent,role,session_id,interactive) VALUES(?1,?2,?3,?4,1)",-1,&q,NULL)!=SQLITE_OK)goto rollback;sqlite3_bind_text(q,1,run,-1,SQLITE_STATIC);sqlite3_bind_text(q,2,agent,-1,SQLITE_STATIC);sqlite3_bind_text(q,3,role,-1,SQLITE_STATIC);sqlite3_bind_text(q,4,session,-1,SQLITE_STATIC);if(sqlite3_step(q)!=SQLITE_DONE)goto rollback;}
    sqlite3_finalize(q);q=NULL;if(work_exec(db,"COMMIT;"))goto done;printf("WORK_SESSION %s %s agent=%s role=%s\n",run,reused?"reused":"opened",agent,role);rc=0;goto done;
rollback: sqlite3_finalize(q);q=NULL;work_exec(db,"ROLLBACK;");
done: sqlite3_finalize(q);if(db)sqlite3_close(db);return rc;
}
static bool work_step_state(const char *state){return state&&(!strcmp(state,"passed")||!strcmp(state,"completed")||!strcmp(state,"failed")||!strcmp(state,"refused")||!strcmp(state,"outcome_uncertain")||!strcmp(state,"reverted"));}
static int cmd_work_session_step(const char *home,const char *run,const char *tool,const char *state,const char *receipt){
    if(!work_atom(run,127)||!work_atom(tool,127)||!work_step_state(state)||!work_atom(receipt,180)){fprintf(stderr,"invalid interactive work step\n");return 2;}sqlite3 *db=NULL;sqlite3_stmt *q=NULL;int rc=1;double now=work_now();char result[256];if(now<=0||snprintf(result,sizeof result,"{\"receipt\":\"%s\"}",receipt)>=(int)sizeof result||work_db(home,&db)||work_exec(db,"BEGIN IMMEDIATE;"))goto done;
    if(sqlite3_prepare_v2(db,"SELECT 1 FROM runs r JOIN operator_run_context c ON c.run_id=r.id AND c.interactive=1 WHERE r.id=?1 AND r.completed_at IS NULL AND r.state='running'",-1,&q,NULL)!=SQLITE_OK)goto rollback;sqlite3_bind_text(q,1,run,-1,SQLITE_STATIC);if(sqlite3_step(q)!=SQLITE_ROW){fprintf(stderr,"interactive work run is not active\n");goto rollback;}sqlite3_finalize(q);q=NULL;
    if(sqlite3_prepare_v2(db,"INSERT INTO actions(run_id,tool,args_json,state,result_json,started_at,duration_ms) VALUES(?1,?2,'{\"interactive\":true}',?3,?4,?5,0)",-1,&q,NULL)!=SQLITE_OK)goto rollback;sqlite3_bind_text(q,1,run,-1,SQLITE_STATIC);sqlite3_bind_text(q,2,tool,-1,SQLITE_STATIC);sqlite3_bind_text(q,3,state,-1,SQLITE_STATIC);sqlite3_bind_text(q,4,result,-1,SQLITE_STATIC);sqlite3_bind_double(q,5,now);if(sqlite3_step(q)!=SQLITE_DONE)goto rollback;sqlite3_int64 seq=sqlite3_last_insert_rowid(db);sqlite3_finalize(q);q=NULL;if(work_exec(db,"COMMIT;"))goto done;printf("WORK_STEP %lld %s %s receipt=%s\n",(long long)seq,run,tool,receipt);rc=0;goto done;
rollback: sqlite3_finalize(q);q=NULL;work_exec(db,"ROLLBACK;");
done: sqlite3_finalize(q);if(db)sqlite3_close(db);return rc;
}
static bool work_close_state(const char *state){return state&&(!strcmp(state,"resolved")||!strcmp(state,"failed")||!strcmp(state,"blocked")||!strcmp(state,"deferred"));}
static int cmd_work_session_close(const char *home,const char *run,const char *state,const char *summary){
    if(!work_atom(run,127)||!work_close_state(state)||!work_label(summary,1000)){fprintf(stderr,"invalid interactive work close\n");return 2;}sqlite3 *db=NULL;sqlite3_stmt *q=NULL;int rc=1;double now=work_now();if(now<=0||work_db(home,&db)||work_exec(db,"BEGIN IMMEDIATE;"))goto done;
    if(sqlite3_prepare_v2(db,"UPDATE runs SET completed_at=?1,state=?2,summary=?3 WHERE id=?4 AND completed_at IS NULL AND EXISTS(SELECT 1 FROM operator_run_context c WHERE c.run_id=runs.id AND c.interactive=1)",-1,&q,NULL)!=SQLITE_OK)goto rollback;sqlite3_bind_double(q,1,now);sqlite3_bind_text(q,2,state,-1,SQLITE_STATIC);sqlite3_bind_text(q,3,summary,-1,SQLITE_STATIC);sqlite3_bind_text(q,4,run,-1,SQLITE_STATIC);if(sqlite3_step(q)!=SQLITE_DONE||sqlite3_changes(db)!=1){fprintf(stderr,"interactive work run is not active\n");goto rollback;}sqlite3_finalize(q);q=NULL;if(work_exec(db,"COMMIT;"))goto done;printf("WORK_SESSION %s closed state=%s\n",run,state);rc=0;goto done;
rollback: sqlite3_finalize(q);q=NULL;work_exec(db,"ROLLBACK;");
done: sqlite3_finalize(q);if(db)sqlite3_close(db);return rc;
}

static int cmd_ratchet(const char *root) {
    char script[PATH_MAX];
    if (snprintf(script, sizeof script, "%s/scripts/native-ratchet.sh", root) >= (int)sizeof script || !regular_file(script)) return 1;
    char *const argv[] = { script, "check", NULL };
    return run_inherit(argv);
}

static int cmd_ratchet_top(const char *root) {
    char script[PATH_MAX];
    if (snprintf(script, sizeof script, "%s/scripts/native-ratchet.sh", root) >= (int)sizeof script || !regular_file(script)) return 1;
    char *const argv[] = { script, "top", NULL };
    return run_inherit(argv);
}

static int cmd_estate_maturity_refresh(const char *root, const char *home) {
    char script[PATH_MAX];
    if (snprintf(script, sizeof script, "%s/scripts/release/estate_maturity_refresh.sh", root) >= (int)sizeof script ||
        !regular_file(script)) {
        fprintf(stderr, "estate maturity refresh owner unavailable\n");
        return 1;
    }
    char *const argv[] = { "/bin/sh", script, "--root", (char *)root, "--home", (char *)home, "--apply", NULL };
    return run_inherit(argv);
}


#include "public_candidates.h"
static int self_test(void) {
    if (!safe_release_name("activity-public-20261023-fixture")) return 1;
    if (!safe_organ_target("bonfyre-surface-scene") || !safe_organ_target("bonfyre-semantic-optimizer")) return 1;
    const char *bad[] = { "", "activity-public-", "../activity-public-x", "other-release", "activity-public-a/b", "activity-public-a b", NULL };
    for (int i = 0; bad[i]; i++) if (safe_release_name(bad[i])) return 1;
    const char *bad_organs[] = { "", "bonfyre-", "../bonfyre-x", "Bonfyre-X", "bonfyre-a/b", "bonfyre-a_b", "bonfyre-a-", "other-x", NULL };
    for (int i = 0; bad_organs[i]; i++) if (safe_organ_target(bad_organs[i])) return 1;
    puts("project mcp lifecycle self-test: PASS");
    return 0;
}

int main(int argc, char **argv) {
    enum {
        PMC_HELP, PMC_ESTATE_MATURITY_REFRESH, PMC_ORGAN_SYNC, PMC_PUBLIC_ACTIVATE, PMC_PUBLIC_CANDIDATES,
        PMC_PUBLIC_FINALIZE, PMC_PUBLIC_READBACK, PMC_PUBLIC_ROLLOUT, PMC_PUBLIC_SELF_TEST,
        PMC_PUBLIC_STAGE, PMC_PUBLIC_STATUS, PMC_PUBLIC_VERIFY, PMC_PUBLIC_VERIFY_GROUP,
        PMC_RATCHET, PMC_RATCHET_TOP, PMC_RELOAD, PMC_SELF_TEST, PMC_STATUS, PMC_VERIFY,
        PMC_WORK_SESSION_CLOSE, PMC_WORK_SESSION_OPEN, PMC_WORK_SESSION_STEP,
    };
    static const BfCommandEntry project_mcp_commands[] = {
        {"--help", PMC_HELP, 0, NULL},
        {"-h", PMC_HELP, 0, NULL},
        {"estate-maturity-refresh", PMC_ESTATE_MATURITY_REFRESH, 0, NULL},
        {"help", PMC_HELP, 0, NULL},
        {"organ-sync", PMC_ORGAN_SYNC, 0, NULL},
        {"public-activate", PMC_PUBLIC_ACTIVATE, 0, NULL},
        {"public-candidates", PMC_PUBLIC_CANDIDATES, 0, NULL},
        {"public-finalize", PMC_PUBLIC_FINALIZE, 0, NULL},
        {"public-readback", PMC_PUBLIC_READBACK, 0, NULL},
        {"public-rollout", PMC_PUBLIC_ROLLOUT, 0, NULL},
        {"public-self-test", PMC_PUBLIC_SELF_TEST, 0, NULL},
        {"public-stage", PMC_PUBLIC_STAGE, 0, NULL},
        {"public-status", PMC_PUBLIC_STATUS, 0, NULL},
        {"public-verify", PMC_PUBLIC_VERIFY, 0, NULL},
        {"public-verify-group", PMC_PUBLIC_VERIFY_GROUP, 0, NULL},
        {"ratchet", PMC_RATCHET, 0, NULL},
        {"ratchet-top", PMC_RATCHET_TOP, 0, NULL},
        {"reload", PMC_RELOAD, 0, NULL},
        {"self-test", PMC_SELF_TEST, 0, NULL},
        {"status", PMC_STATUS, 0, NULL},
        {"verify", PMC_VERIFY, 0, NULL},
        {"work-session-close", PMC_WORK_SESSION_CLOSE, 0, NULL},
        {"work-session-open", PMC_WORK_SESSION_OPEN, 0, NULL},
        {"work-session-step", PMC_WORK_SESSION_STEP, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(project_mcp_commands,
        sizeof(project_mcp_commands) / sizeof(project_mcp_commands[0]), argv[1]);
    if (argc < 2 || (command && command->id == PMC_HELP)) { usage(argc < 2 ? stderr : stdout); return argc < 2; }
    if (command && command->id == PMC_SELF_TEST) return self_test();

    char root[PATH_MAX], home[PATH_MAX], bun[PATH_MAX];
    if (root_paths(argv[0], root, sizeof root, home, sizeof home, bun, sizeof bun) != 0) return 1;

    if (command) switch (command->id) {
    case PMC_STATUS: if (argc == 2) return cmd_status(root, home, bun); break;
    case PMC_VERIFY: if (argc == 2) return cmd_verify(root, home, bun); break;
    case PMC_RELOAD: if (argc == 3 && !strcmp(argv[2], "--apply")) return cmd_reload(root, home, bun); break;
    case PMC_RATCHET: if (argc == 2) return cmd_ratchet(root); break;
    case PMC_RATCHET_TOP: if (argc == 2) return cmd_ratchet_top(root); break;
    case PMC_ORGAN_SYNC: if (argc == 4 && !strcmp(argv[3], "--apply")) return cmd_organ_sync(root, home, argv[2]); break;
    case PMC_ESTATE_MATURITY_REFRESH: if (argc == 3 && !strcmp(argv[2], "--apply")) return cmd_estate_maturity_refresh(root, home); break;
    case PMC_PUBLIC_CANDIDATES: if (argc == 2) return cmd_public_candidates(home); break;
    case PMC_PUBLIC_STATUS: if (argc == 2) return cmd_public_status(root, bun); break;
    case PMC_PUBLIC_SELF_TEST: if (argc == 2) return cmd_public_self_test(root, bun); break;
    case PMC_PUBLIC_VERIFY: if (argc == 3) return cmd_public_verify(root, home, bun, argv[2]); break;
    case PMC_PUBLIC_STAGE: if (argc == 3 && !strcmp(argv[2], "--apply")) return cmd_public_stage(root, bun); break;
    case PMC_PUBLIC_VERIFY_GROUP: if (argc == 5 && !strcmp(argv[4], "--apply")) return cmd_public_verify_group(root, home, bun, argv[2], argv[3]); break;
    case PMC_PUBLIC_READBACK: if (argc == 3) return cmd_public_readback(root, home, bun, argv[2]); break;
    case PMC_PUBLIC_FINALIZE: if (argc == 4 && !strcmp(argv[3], "--apply")) return cmd_public_finalize(root, home, bun, argv[2]); break;
    case PMC_PUBLIC_ROLLOUT: if (argc == 3 && !strcmp(argv[2], "--apply")) return cmd_public_rollout(root, bun); break;
    case PMC_PUBLIC_ACTIVATE: if (argc == 4 && !strcmp(argv[3], "--apply")) return cmd_public_activate(root, home, bun, argv[2]); break;
    case PMC_WORK_SESSION_OPEN: if (argc == 7 && !strcmp(argv[6], "--apply")) return cmd_work_session_open(home, argv[2], argv[3], argv[4], argv[5]); break;
    case PMC_WORK_SESSION_STEP: if (argc == 7 && !strcmp(argv[6], "--apply")) return cmd_work_session_step(home, argv[2], argv[3], argv[4], argv[5]); break;
    case PMC_WORK_SESSION_CLOSE: if (argc == 6 && !strcmp(argv[5], "--apply")) return cmd_work_session_close(home, argv[2], argv[3], argv[4]); break;
    default: break;
    }

    usage(stderr);
    return 2;
}
