/*
 * Proves crash-consistent effect commit: forced termination after the
 * external mutation lands but before the durable commit transaction runs,
 * followed by restart reconciliation, leaves external and durable state
 * consistent for each of publish-local, archive-local, namespace-alias,
 * git-worktree, and git-candidate-commit.
 */
#include "../include/bf_workgraph.h"

#include <sqlite3.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int g_failures = 0;

static void fail(const char *what) {
    fprintf(stderr, "FAIL: %s\n", what);
    g_failures++;
}

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (!f) { fprintf(stderr, "FAIL: could not create fixture %s\n", path); exit(1); }
    fputs(content, f);
    fclose(f);
}

static int run_git(const char *dir, const char *arg1, const char *arg2, const char *arg3,
                   const char *arg4, const char *arg5, const char *arg6) {
    char *argv[16];
    int index = 0;
    pid_t child;
    int status = 0;

    argv[index++] = "git";
    argv[index++] = "-C";
    argv[index++] = (char *)dir;
    if (arg1) argv[index++] = (char *)arg1;
    if (arg2) argv[index++] = (char *)arg2;
    if (arg3) argv[index++] = (char *)arg3;
    if (arg4) argv[index++] = (char *)arg4;
    if (arg5) argv[index++] = (char *)arg5;
    if (arg6) argv[index++] = (char *)arg6;
    argv[index] = NULL;

    child = fork();
    if (child == 0) {
        freopen("/dev/null", "w", stdout);
        freopen("/dev/null", "w", stderr);
        execvp("git", argv);
        _exit(127);
    }
    waitpid(child, &status, 0);
    return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : -1;
}

static int git_head(const char *dir, char output[128]) {
    char path[512];
    FILE *pipe_file;

    snprintf(path, sizeof(path), "/tmp/bonfyre-crash-test-head-%d.txt", getpid());
    {
        char command[1024];
        snprintf(command, sizeof(command), "git -C '%s' rev-parse HEAD > '%s' 2>/dev/null", dir, path);
        if (system(command) != 0) return -1;
    }
    pipe_file = fopen(path, "r");
    if (!pipe_file || !fgets(output, 128, pipe_file)) {
        if (pipe_file) fclose(pipe_file);
        remove(path);
        return -1;
    }
    fclose(pipe_file);
    remove(path);
    output[strcspn(output, "\r\n")] = '\0';
    return output[0] ? 0 : -1;
}

/* archive-local's target is a tar archive of the input (see
 * effect_prepare_adapter's "tar -cf prepared_state -C dir name"), not a
 * byte-identical copy -- extracts the named member to stdout and captures it. */
static int tar_extract_content(const char *archive_path, const char *member_name,
                               char output[256]) {
    char path[512];
    FILE *pipe_file;
    size_t read_len;

    snprintf(path, sizeof(path), "/tmp/bonfyre-crash-test-tar-%d.txt", getpid());
    {
        char command[1024];
        snprintf(command, sizeof(command), "tar -xOf '%s' '%s' > '%s' 2>/dev/null",
                 archive_path, member_name, path);
        if (system(command) != 0) { remove(path); return -1; }
    }
    pipe_file = fopen(path, "r");
    if (!pipe_file) { remove(path); return -1; }
    read_len = fread(output, 1, 255, pipe_file);
    output[read_len] = '\0';
    fclose(pipe_file);
    remove(path);
    return 0;
}

/* Runs prepare+commit for the given effect in a forked child that crashes
 * (SIGKILL) right after the external mutation, simulating a hard process
 * death before the durable commit transaction. Returns 0 if the child died
 * from SIGKILL as expected. */
static int crash_after_mutation(const char *db_path, const char *mission_id,
                                const char *node_id, const BfWorkgraphEffectSpec *spec) {
    pid_t child = fork();
    int status = 0;

    if (child == 0) {
        BfWorkgraph *graph = NULL;
        BfWorkgraphResult result;
        char claim_token[65];

        setenv("BONFYRE_TEST_CRASH_AFTER_EFFECT_MUTATION", "1", 1);
        if (bf_workgraph_open(db_path, &graph, stderr) != 0) _exit(2);
        result = bf_workgraph_create_mission(graph, mission_id);
        if (result.status != BF_WORKGRAPH_OK) _exit(3);
        {
            BfWorkgraphNodeSpec node_spec = {
                .mission_id = mission_id, .node_id = node_id, .operator_id = "op.test",
                .family = "default", .priority = 100, .retry_limit = 0,
                .timeout_seconds = 60, .backoff_base_ms = 1000, .backoff_multiplier = 2.0,
                .backoff_max_ms = 60000, .jitter_percent = 0, .fanin_required = 0,
            };
            result = bf_workgraph_add_node(graph, &node_spec);
            if (result.status != BF_WORKGRAPH_OK) _exit(4);
        }
        {
            BfWorkgraphClaimSpec claim_spec = { .worker_id = "worker-crash", .lease_ms = 60000 };
            result = bf_workgraph_claim_node(graph, mission_id, node_id, &claim_spec);
            if (result.status != BF_WORKGRAPH_OK) _exit(5);
            snprintf(claim_token, sizeof(claim_token), "%s", result.claim_token);
        }
        result = bf_workgraph_prepare_effect(graph, mission_id, node_id, "worker-crash",
                                             claim_token, spec);
        if (result.status != BF_WORKGRAPH_OK) _exit(6);
        /* This call performs the external mutation and then self-SIGKILLs before
         * ever returning, because of the env var set above. */
        bf_workgraph_commit_effect(graph, mission_id, node_id, "worker-crash",
                                   claim_token, spec->effect_id);
        /* Unreachable if the crash hook fired as expected. */
        _exit(7);
    }
    waitpid(child, &status, 0);
    if (!WIFSIGNALED(status) || WTERMSIG(status) != SIGKILL) {
        fprintf(stderr, "FAIL: expected child to die from SIGKILL, status=%d\n", status);
        return -1;
    }
    return 0;
}

static char *query_text(sqlite3 *db, const char *sql, const char *a, const char *b, const char *c) {
    static char buffer[512];
    sqlite3_stmt *statement = NULL;

    buffer[0] = '\0';
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) != SQLITE_OK) return buffer;
    int index = 1;
    if (a) sqlite3_bind_text(statement, index++, a, -1, SQLITE_TRANSIENT);
    if (b) sqlite3_bind_text(statement, index++, b, -1, SQLITE_TRANSIENT);
    if (c) sqlite3_bind_text(statement, index++, c, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) == SQLITE_ROW) {
        const unsigned char *text = sqlite3_column_text(statement, 0);
        snprintf(buffer, sizeof(buffer), "%s", text ? (const char *)text : "");
    }
    sqlite3_finalize(statement);
    return buffer;
}

static void verify_reconciliation(const char *db_path, const char *mission_id, const char *node_id,
                                  const char *effect_id, const char *case_name) {
    sqlite3 *db = NULL;
    BfWorkgraph *graph = NULL;
    char state_before[64];
    int inspected;
    char state_after[64];
    char node_effect_state[64];

    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READWRITE, NULL) != SQLITE_OK) {
        fprintf(stderr, "FAIL: %s: could not open verification connection\n", case_name);
        g_failures++;
        return;
    }
    snprintf(state_before, sizeof(state_before), "%s",
             query_text(db, "SELECT state FROM workgraph_compensations WHERE mission_id=? "
                            "AND node_id=? AND effect_id=?", mission_id, node_id, effect_id));
    if (strcmp(state_before, "commit_started") != 0) {
        fprintf(stderr, "FAIL: %s: expected state=commit_started before reconciliation, got %s\n",
                case_name, state_before);
        g_failures++;
    }
    sqlite3_close(db);

    if (bf_workgraph_open(db_path, &graph, stderr) != 0) {
        fprintf(stderr, "FAIL: %s: could not reopen workgraph for reconciliation\n", case_name);
        g_failures++;
        return;
    }
    inspected = bf_workgraph_reconcile_effects(graph, mission_id);
    bf_workgraph_close(graph);
    if (inspected != 1) {
        fprintf(stderr, "FAIL: %s: expected reconcile_effects to inspect 1 effect, got %d\n",
                case_name, inspected);
        g_failures++;
    }

    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READWRITE, NULL) != SQLITE_OK) {
        fprintf(stderr, "FAIL: %s: could not reopen verification connection\n", case_name);
        g_failures++;
        return;
    }
    snprintf(state_after, sizeof(state_after), "%s",
             query_text(db, "SELECT state FROM workgraph_compensations WHERE mission_id=? "
                           "AND node_id=? AND effect_id=?", mission_id, node_id, effect_id));
    snprintf(node_effect_state, sizeof(node_effect_state), "%s",
             query_text(db, "SELECT effect_state FROM workgraph_nodes WHERE mission_id=? AND node_id=?",
                       mission_id, node_id, NULL));
    if (strcmp(state_after, "committed") != 0) {
        fprintf(stderr, "FAIL: %s: expected state=committed after reconciliation, got %s\n",
                case_name, state_after);
        g_failures++;
    }
    if (strcmp(node_effect_state, "committed") != 0) {
        fprintf(stderr, "FAIL: %s: expected node effect_state=committed after reconciliation, got %s\n",
                case_name, node_effect_state);
        g_failures++;
    }
    sqlite3_close(db);
}

static void test_publish_local(void) {
    const char *db_path = "/tmp/bonfyre-crash-publish-local.sqlite";
    const char *input_path = "/tmp/bonfyre-crash-publish-local-input.txt";
    const char *target_path = "/tmp/bonfyre-crash-publish-local-target.txt";
    char target_uri[256], input_uri[256];
    BfWorkgraphEffectSpec spec;
    FILE *check;

    remove(db_path); remove(input_path); remove(target_path);
    write_file(input_path, "publish-local crash fixture\n");
    snprintf(target_uri, sizeof(target_uri), "file://%s", target_path);
    snprintf(input_uri, sizeof(input_uri), "file://%s", input_path);
    memset(&spec, 0, sizeof(spec));
    spec.effect_id = "e-publish"; spec.adapter_id = "publish-local";
    spec.target_uri = target_uri; spec.input_artifact_uri = input_uri;
    spec.verification_policy = "sha256"; spec.rollback_contract = "remove-created-target";
    spec.authority_identity = "tester";

    if (crash_after_mutation(db_path, "m-publish", "n-publish", &spec) != 0) {
        fail("publish-local: crash injection did not behave as expected");
        return;
    }
    check = fopen(target_path, "r");
    if (!check) {
        fail("publish-local: target file missing immediately after crash (mutation did not land)");
    } else {
        fclose(check);
    }
    verify_reconciliation(db_path, "m-publish", "n-publish", "e-publish", "publish-local");
    check = fopen(target_path, "r");
    if (!check) {
        fail("publish-local: target file missing after reconciliation");
    } else {
        char buffer[256] = {0};
        fread(buffer, 1, sizeof(buffer) - 1, check);
        fclose(check);
        if (strcmp(buffer, "publish-local crash fixture\n") != 0) {
            fail("publish-local: target content changed by reconciliation");
        }
    }
    remove(db_path); remove(input_path); remove(target_path);
}

static void test_archive_local(void) {
    const char *db_path = "/tmp/bonfyre-crash-archive-local.sqlite";
    const char *input_path = "/tmp/bonfyre-crash-archive-local-input.txt";
    const char *target_path = "/tmp/bonfyre-crash-archive-local-target.txt";
    char target_uri[256], input_uri[256];
    BfWorkgraphEffectSpec spec;
    FILE *check;

    remove(db_path); remove(input_path); remove(target_path);
    write_file(input_path, "archive-local crash fixture\n");
    snprintf(target_uri, sizeof(target_uri), "file://%s", target_path);
    snprintf(input_uri, sizeof(input_uri), "file://%s", input_path);
    memset(&spec, 0, sizeof(spec));
    spec.effect_id = "e-archive"; spec.adapter_id = "archive-local";
    spec.target_uri = target_uri; spec.input_artifact_uri = input_uri;
    spec.verification_policy = "sha256"; spec.rollback_contract = "remove-created-target";
    spec.authority_identity = "tester";

    if (crash_after_mutation(db_path, "m-archive", "n-archive", &spec) != 0) {
        fail("archive-local: crash injection did not behave as expected");
        return;
    }
    check = fopen(target_path, "r");
    if (!check) {
        fail("archive-local: target file missing immediately after crash (mutation did not land)");
    } else {
        fclose(check);
    }
    verify_reconciliation(db_path, "m-archive", "n-archive", "e-archive", "archive-local");
    check = fopen(target_path, "r");
    if (!check) {
        fail("archive-local: target file missing after reconciliation");
    } else {
        char buffer[256] = {0};
        fclose(check);
        /* target is a tar archive of input_path (archive-local semantics),
         * not a raw copy -- verify the archived member's content matches. */
        if (tar_extract_content(target_path, "bonfyre-crash-archive-local-input.txt", buffer) != 0) {
            fail("archive-local: target is not a readable tar archive after reconciliation");
        } else if (strcmp(buffer, "archive-local crash fixture\n") != 0) {
            fail("archive-local: archived content changed by reconciliation");
        }
    }
    remove(db_path); remove(input_path); remove(target_path);
}

static void test_namespace_alias(void) {
    const char *db_path = "/tmp/bonfyre-crash-namespace-alias.sqlite";
    BfWorkgraphEffectSpec spec;
    sqlite3 *bootstrap = NULL;

    remove(db_path);
    /* namespace_objects normally lives in the fabric-level schema; create it
     * directly so this test stays self-contained without linking fabric.c. */
    if (sqlite3_open(db_path, &bootstrap) != SQLITE_OK) {
        fail("namespace-alias: could not create bootstrap connection");
        return;
    }
    sqlite3_exec(bootstrap,
        "CREATE TABLE IF NOT EXISTS namespace_objects("
        " uri TEXT PRIMARY KEY,kind TEXT NOT NULL,owner TEXT NOT NULL,source_authority TEXT NOT NULL,"
        " native_id TEXT,version TEXT NOT NULL,locator TEXT NOT NULL,policy TEXT NOT NULL,"
        " sensitivity TEXT NOT NULL,freshness TEXT NOT NULL,evidence_state TEXT NOT NULL,"
        " operations TEXT NOT NULL,content_contract TEXT NOT NULL,query_contract TEXT NOT NULL,"
        " effect_contract TEXT NOT NULL,created_at TEXT NOT NULL);",
        NULL, NULL, NULL);
    sqlite3_close(bootstrap);

    memset(&spec, 0, sizeof(spec));
    spec.effect_id = "e-namespace"; spec.adapter_id = "namespace-alias";
    spec.target_uri = "bonfyre://ns/crash-test-alias";
    spec.input_artifact_uri = "bonfyre://artifact/crash-test-fixture";
    spec.verification_policy = "sha256"; spec.rollback_contract = "remove-created-target";
    spec.authority_identity = "tester";

    if (crash_after_mutation(db_path, "m-namespace", "n-namespace", &spec) != 0) {
        fail("namespace-alias: crash injection did not behave as expected");
        return;
    }
    {
        sqlite3 *reader = NULL;
        char found[16];
        sqlite3_open_v2(db_path, &reader, SQLITE_OPEN_READONLY, NULL);
        snprintf(found, sizeof(found), "%s",
                 query_text(reader, "SELECT '1' FROM namespace_objects WHERE uri=?",
                           spec.target_uri, NULL, NULL));
        if (strcmp(found, "1") != 0) {
            fail("namespace-alias: namespace_objects row missing immediately after crash");
        }
        sqlite3_close(reader);
    }
    verify_reconciliation(db_path, "m-namespace", "n-namespace", "e-namespace", "namespace-alias");
    {
        sqlite3 *reader = NULL;
        char found[16];
        sqlite3_open_v2(db_path, &reader, SQLITE_OPEN_READONLY, NULL);
        snprintf(found, sizeof(found), "%s",
                 query_text(reader, "SELECT '1' FROM namespace_objects WHERE uri=?",
                           spec.target_uri, NULL, NULL));
        if (strcmp(found, "1") != 0) {
            fail("namespace-alias: namespace_objects row missing after reconciliation");
        }
        sqlite3_close(reader);
    }
    remove(db_path);
}

static void test_git_worktree(void) {
    const char *db_path = "/tmp/bonfyre-crash-git-worktree.sqlite";
    const char *source_dir = "/tmp/bonfyre-crash-git-worktree-source";
    const char *target_dir = "/tmp/bonfyre-crash-git-worktree-target";
    char target_uri[256], input_uri[256];
    char expected_head[128];
    BfWorkgraphEffectSpec spec;

    remove(db_path);
    { char cmd[512]; snprintf(cmd, sizeof(cmd), "rm -rf '%s' '%s'", source_dir, target_dir); system(cmd); }
    { char cmd[512]; snprintf(cmd, sizeof(cmd), "mkdir -p '%s'", source_dir); system(cmd); }
    run_git(source_dir, "init", "-q", NULL, NULL, NULL, NULL);
    run_git(source_dir, "-c", "user.name=T", "commit", "--allow-empty", "-m", "init");
    if (git_head(source_dir, expected_head) != 0) {
        fail("git-worktree: could not read source repo HEAD");
        return;
    }

    snprintf(target_uri, sizeof(target_uri), "file://%s", target_dir);
    snprintf(input_uri, sizeof(input_uri), "file://%s", source_dir);
    memset(&spec, 0, sizeof(spec));
    spec.effect_id = "e-worktree"; spec.adapter_id = "git-worktree";
    spec.target_uri = target_uri; spec.input_artifact_uri = input_uri;
    spec.verification_policy = "sha256"; spec.rollback_contract = "remove-created-target";
    spec.authority_identity = "tester";

    if (crash_after_mutation(db_path, "m-worktree", "n-worktree", &spec) != 0) {
        fail("git-worktree: crash injection did not behave as expected");
        return;
    }
    {
        char head[128];
        if (git_head(target_dir, head) != 0 || strcmp(head, expected_head) != 0) {
            fail("git-worktree: worktree HEAD does not match source HEAD immediately after crash");
        }
    }
    verify_reconciliation(db_path, "m-worktree", "n-worktree", "e-worktree", "git-worktree");
    {
        char head[128];
        if (git_head(target_dir, head) != 0 || strcmp(head, expected_head) != 0) {
            fail("git-worktree: worktree HEAD changed or vanished after reconciliation");
        }
    }
    remove(db_path);
    { char cmd[512]; snprintf(cmd, sizeof(cmd), "rm -rf '%s' '%s'", source_dir, target_dir); system(cmd); }
}

static void test_git_candidate_commit(void) {
    const char *db_path = "/tmp/bonfyre-crash-git-candidate.sqlite";
    const char *repo_dir = "/tmp/bonfyre-crash-git-candidate-repo";
    char target_uri[256];
    char pre_head[128];
    BfWorkgraphEffectSpec spec;

    remove(db_path);
    { char cmd[512]; snprintf(cmd, sizeof(cmd), "rm -rf '%s' && mkdir -p '%s'", repo_dir, repo_dir); system(cmd); }
    run_git(repo_dir, "init", "-q", NULL, NULL, NULL, NULL);
    run_git(repo_dir, "-c", "user.name=T", "commit", "--allow-empty", "-m", "init");
    if (git_head(repo_dir, pre_head) != 0) {
        fail("git-candidate-commit: could not read pre-commit HEAD");
        return;
    }

    snprintf(target_uri, sizeof(target_uri), "file://%s", repo_dir);
    memset(&spec, 0, sizeof(spec));
    spec.effect_id = "e-candidate"; spec.adapter_id = "git-candidate-commit";
    spec.target_uri = target_uri; spec.input_artifact_uri = "Bonfyre candidate crash test";
    spec.verification_policy = "sha256"; spec.rollback_contract = "remove-created-target";
    spec.authority_identity = "tester";

    if (crash_after_mutation(db_path, "m-candidate", "n-candidate", &spec) != 0) {
        fail("git-candidate-commit: crash injection did not behave as expected");
        return;
    }
    {
        char head[128];
        if (git_head(repo_dir, head) != 0 || strcmp(head, pre_head) == 0) {
            fail("git-candidate-commit: HEAD did not advance immediately after crash");
        }
    }
    verify_reconciliation(db_path, "m-candidate", "n-candidate", "e-candidate", "git-candidate-commit");
    {
        char head_before_check[128];
        if (git_head(repo_dir, head_before_check) != 0 || strcmp(head_before_check, pre_head) == 0) {
            fail("git-candidate-commit: HEAD reverted or vanished after reconciliation");
        }
    }
    remove(db_path);
    { char cmd[512]; snprintf(cmd, sizeof(cmd), "rm -rf '%s'", repo_dir); system(cmd); }
}

int main(void) {
    test_publish_local();
    test_archive_local();
    test_namespace_alias();
    test_git_worktree();
    test_git_candidate_commit();

    if (g_failures) {
        fprintf(stderr, "\n%d crash-consistency check(s) failed\n", g_failures);
        return 1;
    }
    printf("all effect commit crash-consistency checks passed\n");
    return 0;
}
