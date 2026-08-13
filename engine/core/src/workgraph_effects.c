#include "workgraph_internal.h"

#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

BfWorkgraphResult bf_workgraph_create_fanout(BfWorkgraph *graph,
                                             const BfWorkgraphFanoutSpec *spec) {
    BfWorkgraphResult result;
    sqlite3_stmt *statement = NULL;
    int64_t now = bf_workgraph_now_ms();

    if (graph == NULL || spec == NULL || spec->mission_id == NULL ||
        spec->parent_node_id == NULL || spec->group_id == NULL ||
        spec->operator_id == NULL || spec->child_count < 1) {
        return bf_workgraph_result(BF_WORKGRAPH_INVALID, NULL, NULL,
                                   "invalid_fanout", "fan-out specification is invalid");
    }
    result = bf_workgraph_result(BF_WORKGRAPH_OK, spec->mission_id,
                                 spec->parent_node_id, NULL, NULL);
    if (bf_workgraph_begin(graph, &result) != 0) return result;
    if (sqlite3_prepare_v2(graph->db,
            "INSERT INTO workgraph_fanout_groups(mission_id,group_id,parent_node_id,child_count,"
            "failure_policy,state,created_at_ms,updated_at_ms) VALUES(?,?,?,?,?,'active',?,?)",
            -1, &statement, NULL) != SQLITE_OK) goto storage_failure;
    sqlite3_bind_text(statement, 1, spec->mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, spec->group_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, spec->parent_node_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(statement, 4, spec->child_count);
    sqlite3_bind_text(statement, 5, spec->failure_policy != NULL ? spec->failure_policy : "fail",
                      -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement, 6, now);
    sqlite3_bind_int64(statement, 7, now);
    if (sqlite3_step(statement) != SQLITE_DONE) { sqlite3_finalize(statement); goto storage_failure; }
    sqlite3_finalize(statement); statement = NULL;

    for (int index = 0; index < spec->child_count; ++index) {
        char child_id[128];
        snprintf(child_id, sizeof(child_id), "%s-%d",
                 spec->child_prefix != NULL ? spec->child_prefix : "child", index + 1);
        if (sqlite3_prepare_v2(graph->db,
                "INSERT INTO workgraph_nodes(mission_id,node_id,operator_id,status,attempt,retry_limit,"
                "timeout_seconds,depends_on,backoff_policy,backoff_base_ms,backoff_multiplier,"
                "backoff_max_ms,backoff_jitter_percent,cancellation_state,parent_node,fanout_group,"
                "family,priority,ready_at_ms,created_at_ms,updated_at_ms) "
                "VALUES(?,?,?,'blocked',0,1,30,'[]','exponential',1000,2.0,60000,0,'none',?,?,?,100,NULL,?,?)",
                -1, &statement, NULL) != SQLITE_OK) goto storage_failure;
        sqlite3_bind_text(statement, 1, spec->mission_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 2, child_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 3, spec->operator_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 4, spec->parent_node_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 5, spec->group_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 6, spec->family != NULL ? spec->family : "default", -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(statement, 7, now);
        sqlite3_bind_int64(statement, 8, now);
        if (sqlite3_step(statement) != SQLITE_DONE) { sqlite3_finalize(statement); goto storage_failure; }
        sqlite3_finalize(statement); statement = NULL;
        if (sqlite3_prepare_v2(graph->db,
                "INSERT INTO workgraph_dependencies(mission_id,node_id,depends_on_node_id,dependency_policy) "
                "VALUES(?,?,?,'require_success')",
                -1, &statement, NULL) != SQLITE_OK) goto storage_failure;
        sqlite3_bind_text(statement, 1, spec->mission_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 2, child_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 3, spec->parent_node_id, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(statement) != SQLITE_DONE) { sqlite3_finalize(statement); goto storage_failure; }
        sqlite3_finalize(statement); statement = NULL;
    }
    if (bf_workgraph_promote_dependents(graph, spec->mission_id, &result) != 0 ||
        bf_workgraph_write_evidence(graph, &result, "fanout_created", "scheduler", NULL) != 0 ||
        bf_workgraph_commit(graph, &result) != 0) goto storage_failure;
    return result;

storage_failure:
    if (statement != NULL) sqlite3_finalize(statement);
    bf_workgraph_rollback(graph);
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "fanout_storage_failed");
    snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
    return result;
}

BfWorkgraphResult bf_workgraph_evaluate_fanin(BfWorkgraph *graph, const char *mission_id,
                                              const char *node_id) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, node_id, NULL, NULL);
    sqlite3_stmt *statement = NULL;
    char failure_policy[32];
    int expected;
    int complete_count;
    int terminal_count;
    int failed_count;
    const char *new_status = "blocked";
    const char *transition = "fanin_blocked";
    int64_t now = bf_workgraph_now_ms();

    if (graph == NULL || mission_id == NULL || node_id == NULL) {
        return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_id, node_id,
                                   "invalid_fanin", "fan-in node is required");
    }
    if (bf_workgraph_begin(graph, &result) != 0) return result;
    if (sqlite3_prepare_v2(graph->db,
            "SELECT g.child_count,g.failure_policy,"
            "SUM(CASE WHEN c.status='complete' THEN 1 ELSE 0 END),"
            "SUM(CASE WHEN c.status IN ('complete','dead_letter','cancelled') THEN 1 ELSE 0 END),"
            "SUM(CASE WHEN c.status IN ('dead_letter','cancelled') THEN 1 ELSE 0 END) "
            "FROM workgraph_nodes n JOIN workgraph_fanout_groups g ON g.mission_id=n.mission_id "
            "AND g.group_id=n.fanout_group LEFT JOIN workgraph_nodes c ON c.mission_id=g.mission_id "
            "AND c.fanout_group=g.group_id AND c.node_id!=n.node_id "
            "WHERE n.mission_id=? AND n.node_id=? GROUP BY g.child_count,g.failure_policy",
            -1, &statement, NULL) != SQLITE_OK) goto storage_failure;
    sqlite3_bind_text(statement, 1, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, node_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_ROW) {
        sqlite3_finalize(statement);
        bf_workgraph_rollback(graph);
        return bf_workgraph_result(BF_WORKGRAPH_NOT_FOUND, mission_id, node_id,
                                   "fanin_not_found", "fan-in group was not found");
    }
    expected = sqlite3_column_int(statement, 0);
    snprintf(failure_policy, sizeof(failure_policy), "%s", sqlite3_column_text(statement, 1));
    complete_count = sqlite3_column_int(statement, 2);
    terminal_count = sqlite3_column_int(statement, 3);
    failed_count = sqlite3_column_int(statement, 4);
    sqlite3_finalize(statement); statement = NULL;

    if (complete_count == expected) {
        new_status = "ready";
        transition = "fanin_released";
    } else if (failed_count > 0 && !strcmp(failure_policy, "fail")) {
        new_status = "dead_letter";
        transition = "fanin_failed";
    } else if (terminal_count == expected && !strcmp(failure_policy, "continue")) {
        new_status = "ready";
        transition = "fanin_released";
    }
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_nodes SET status=?,ready_at_ms=?,terminal_at_ms=?,updated_at_ms=? "
            "WHERE mission_id=? AND node_id=? AND status NOT IN ('running','complete','cancelled')",
            -1, &statement, NULL) != SQLITE_OK) goto storage_failure;
    sqlite3_bind_text(statement, 1, new_status, -1, SQLITE_TRANSIENT);
    if (!strcmp(new_status, "ready")) sqlite3_bind_int64(statement, 2, now); else sqlite3_bind_null(statement, 2);
    if (!strcmp(new_status, "dead_letter")) sqlite3_bind_int64(statement, 3, now); else sqlite3_bind_null(statement, 3);
    sqlite3_bind_int64(statement, 4, now);
    sqlite3_bind_text(statement, 5, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 6, node_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) { sqlite3_finalize(statement); goto storage_failure; }
    sqlite3_finalize(statement);
    if (bf_workgraph_update_mission(graph, mission_id, &result) != 0 ||
        bf_workgraph_write_evidence(graph, &result, transition, "scheduler",
                                    failed_count > 0 ? "child_failed" : NULL) != 0 ||
        bf_workgraph_commit(graph, &result) != 0) goto storage_failure;
    return result;

storage_failure:
    if (statement != NULL) sqlite3_finalize(statement);
    bf_workgraph_rollback(graph);
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "fanin_storage_failed");
    snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
    return result;
}

static int effect_adapter_known(const char *adapter_id) {
    static const char *adapters[] = {
        "derive-file", "publish-local", "archive-local", "namespace-alias",
        "git-worktree", "git-candidate-commit", NULL
    };

    for (size_t index = 0; adapters[index] != NULL; ++index) {
        if (strcmp(adapter_id, adapters[index]) == 0) {
            return 1;
        }
    }
    return 0;
}

static int effect_local_path(BfWorkgraph *graph, const char *uri,
                             char output[PATH_MAX]) {
    sqlite3_stmt *statement = NULL;
    const char *path = uri;

    if (uri == NULL || uri[0] == '\0') {
        return -1;
    }
    if (strncmp(uri, "file://", 7) == 0) {
        path = uri + 7;
    } else if (strncmp(uri, "bonfyre://artifact/", 19) == 0) {
        if (sqlite3_prepare_v2(graph->db,
                "SELECT locator FROM artifacts WHERE uri=?",
                -1, &statement, NULL) != SQLITE_OK) {
            return -1;
        }
        sqlite3_bind_text(statement, 1, uri, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(statement) != SQLITE_ROW) {
            sqlite3_finalize(statement);
            return -1;
        }
        path = (const char *)sqlite3_column_text(statement, 0);
        if (snprintf(output, PATH_MAX, "%s", path) >= PATH_MAX) {
            sqlite3_finalize(statement);
            return -1;
        }
        sqlite3_finalize(statement);
        return 0;
    }
    if (snprintf(output, PATH_MAX, "%s", path) >= PATH_MAX) {
        return -1;
    }
    return output[0] != '\0' && strcmp(output, "/") != 0 ? 0 : -1;
}

static int effect_copy_file(const char *source, const char *target) {
    unsigned char buffer[65536];
    FILE *input = fopen(source, "rb");
    FILE *output = NULL;
    size_t bytes;
    int failed = 0;

    if (input == NULL) {
        return -1;
    }
    output = fopen(target, "wb");
    if (output == NULL) {
        fclose(input);
        return -1;
    }
    while ((bytes = fread(buffer, 1, sizeof(buffer), input)) > 0) {
        if (fwrite(buffer, 1, bytes, output) != bytes) {
            failed = 1;
            break;
        }
    }
    if (ferror(input) || fflush(output) != 0 || fsync(fileno(output)) != 0) {
        failed = 1;
    }
    if (fclose(output) != 0) {
        failed = 1;
    }
    fclose(input);
    if (failed) {
        unlink(target);
        return -1;
    }
    return 0;
}

static int effect_run(char *const arguments[], char *output, size_t output_size) {
    int descriptors[2];
    pid_t child;
    size_t used = 0;
    int status = 0;

    if (pipe(descriptors) != 0) {
        return -1;
    }
    child = fork();
    if (child == 0) {
        close(descriptors[0]);
        dup2(descriptors[1], STDOUT_FILENO);
        dup2(descriptors[1], STDERR_FILENO);
        close(descriptors[1]);
        execvp(arguments[0], arguments);
        _exit(127);
    }
    close(descriptors[1]);
    if (child < 0) {
        close(descriptors[0]);
        return -1;
    }
    for (;;) {
        char buffer[512];
        ssize_t bytes = read(descriptors[0], buffer, sizeof(buffer));

        if (bytes <= 0) {
            break;
        }
        if (output != NULL && output_size > 0 && used < output_size - 1) {
            size_t available = output_size - 1 - used;
            size_t copied = (size_t)bytes < available ? (size_t)bytes : available;
            memcpy(output + used, buffer, copied);
            used += copied;
        }
    }
    close(descriptors[0]);
    if (output != NULL && output_size > 0) {
        while (used > 0 && (output[used - 1] == '\n' || output[used - 1] == '\r')) {
            --used;
        }
        output[used] = '\0';
    }
    if (waitpid(child, &status, 0) != child) {
        return -1;
    }
    return WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : -1;
}

static int effect_git_head(const char *repository, char output[1024]) {
    char *arguments[] = {"git", "-C", (char *)repository, "rev-parse", "HEAD", NULL};

    return effect_run(arguments, output, 1024);
}

static int effect_prepare_adapter(BfWorkgraph *graph, const BfWorkgraphEffectSpec *spec,
                                  char prepared_state[1024], char simulation[1024]) {
    char input[PATH_MAX];
    char target[PATH_MAX];
    struct stat metadata;

    if (!effect_adapter_known(spec->adapter_id) || spec->authority_identity == NULL ||
        spec->authority_identity[0] == '\0') {
        return -1;
    }
    if (strcmp(spec->adapter_id, "namespace-alias") == 0) {
        if (strncmp(spec->target_uri, "bonfyre://", 10) != 0 ||
            spec->input_artifact_uri == NULL || spec->input_artifact_uri[0] == '\0') {
            return -1;
        }
        snprintf(prepared_state, 1024, "%s", spec->input_artifact_uri);
        snprintf(simulation, 1024, "create namespace alias %s -> %s",
                 spec->target_uri, spec->input_artifact_uri);
        return 0;
    }
    if (strcmp(spec->adapter_id, "git-candidate-commit") == 0) {
        if (effect_local_path(graph, spec->target_uri, target) != 0 ||
            effect_git_head(target, prepared_state) != 0) {
            return -1;
        }
        snprintf(simulation, 1024, "create candidate commit in %s from %s",
                 target, prepared_state);
        return 0;
    }
    if (effect_local_path(graph, spec->target_uri, target) != 0 ||
        effect_local_path(graph, spec->input_artifact_uri, input) != 0) {
        return -1;
    }
    if (strcmp(spec->adapter_id, "git-worktree") == 0) {
        if (access(target, F_OK) == 0 || effect_git_head(input, prepared_state) != 0) {
            return -1;
        }
        snprintf(simulation, 1024, "create detached worktree %s from %s at %s",
                 target, input, prepared_state);
        return 0;
    }
    if (stat(input, &metadata) != 0 || !S_ISREG(metadata.st_mode) ||
        access(target, F_OK) == 0) {
        return -1;
    }
    if (snprintf(prepared_state, 1024, "%s.bonfyre-%s.prepared",
                 target, spec->effect_id) >= 1024) {
        return -1;
    }
    if (strcmp(spec->adapter_id, "archive-local") == 0) {
        char input_copy[PATH_MAX];
        char *slash;
        char *directory;
        char *name;
        char *arguments[7];

        snprintf(input_copy, sizeof(input_copy), "%s", input);
        slash = strrchr(input_copy, '/');
        directory = slash != NULL ? input_copy : ".";
        name = slash != NULL ? slash + 1 : input_copy;
        if (slash != NULL) {
            *slash = '\0';
            if (directory[0] == '\0') {
                directory = "/";
            }
        }
        arguments[0] = "tar";
        arguments[1] = "-cf";
        arguments[2] = prepared_state;
        arguments[3] = "-C";
        arguments[4] = directory;
        arguments[5] = name;
        arguments[6] = NULL;
        if (effect_run(arguments, NULL, 0) != 0) {
            unlink(prepared_state);
            return -1;
        }
    } else if (effect_copy_file(input, prepared_state) != 0) {
        return -1;
    }
    snprintf(simulation, 1024, "materialize %s at %s using %s",
             input, target, spec->adapter_id);
    return 0;
}

static int effect_verify_file(BfWorkgraph *graph, const BfWorkgraphEffectRecord *record) {
    char input[PATH_MAX];
    char target[PATH_MAX];
    char input_digest[65];
    char target_digest[65];
    struct stat metadata;

    if (effect_local_path(graph, record->target_uri, target) != 0 ||
        stat(target, &metadata) != 0 || !S_ISREG(metadata.st_mode) || metadata.st_size == 0) {
        return -1;
    }
    if (strcmp(record->verification_policy, "sha256") != 0 ||
        strcmp(record->adapter_id, "archive-local") == 0) {
        return 0;
    }
    if (effect_local_path(graph, record->input_artifact_uri, input) != 0 ||
        bf_sha256_file(input, input_digest) != 0 || bf_sha256_file(target, target_digest) != 0) {
        return -1;
    }
    return strcmp(input_digest, target_digest) == 0 ? 0 : -1;
}

static int effect_commit_adapter(BfWorkgraph *graph, const BfWorkgraphEffectRecord *record) {
    char input[PATH_MAX];
    char target[PATH_MAX];
    char timestamp[32];

    if (strcmp(record->adapter_id, "namespace-alias") == 0) {
        sqlite3_stmt *statement = NULL;

        bf_workgraph_timestamp(timestamp);
        if (sqlite3_prepare_v2(graph->db,
                "INSERT INTO namespace_objects(uri,kind,owner,source_authority,native_id,version,"
                "locator,policy,sensitivity,freshness,evidence_state,operations,content_contract,"
                "query_contract,effect_contract,created_at) "
                "VALUES(?,'alias',?,'workgraph-effect',?,'1',?,'mission-scoped','standard',"
                "'current','verified','read,resolve','namespace.alias.v1','namespace.resolve.v1',"
                "'reversible-local-write',?)",
                -1, &statement, NULL) != SQLITE_OK) {
            return -1;
        }
        sqlite3_bind_text(statement, 1, record->target_uri, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 2, record->authority_identity, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 3, record->effect_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 4, record->input_artifact_uri, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 5, timestamp, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(statement) != SQLITE_DONE) {
            sqlite3_finalize(statement);
            return -1;
        }
        sqlite3_finalize(statement);
        return 0;
    }
    if (strcmp(record->adapter_id, "git-worktree") == 0) {
        char *arguments[] = {"git", "-C", input, "worktree", "add", "--detach", target,
                             (char *)record->prepared_state, NULL};

        if (effect_local_path(graph, record->input_artifact_uri, input) != 0 ||
            effect_local_path(graph, record->target_uri, target) != 0) {
            return -1;
        }
        return effect_run(arguments, NULL, 0);
    }
    if (strcmp(record->adapter_id, "git-candidate-commit") == 0) {
        const char *message = record->input_artifact_uri[0] != '\0' ?
            record->input_artifact_uri : "Bonfyre candidate";
        char *add_arguments[] = {"git", "-C", target, "add", "-A", NULL};
        char *commit_arguments[] = {
            "git", "-C", target, "-c", "user.name=Bonfyre",
            "-c", "user.email=bonfyre@local", "commit", "--allow-empty", "-m",
            (char *)message, NULL
        };

        if (effect_local_path(graph, record->target_uri, target) != 0 ||
            effect_run(add_arguments, NULL, 0) != 0 ||
            effect_run(commit_arguments, NULL, 0) != 0) {
            return -1;
        }
        return 0;
    }
    if (effect_local_path(graph, record->target_uri, target) != 0 ||
        rename(record->prepared_state, target) != 0) {
        return -1;
    }
    return effect_verify_file(graph, record);
}

static int effect_verify_adapter(BfWorkgraph *graph,
                                 const BfWorkgraphEffectRecord *record) {
    if (strcmp(record->adapter_id, "namespace-alias") == 0) {
        sqlite3_stmt *statement = NULL;
        int found;

        if (sqlite3_prepare_v2(graph->db,
                "SELECT 1 FROM namespace_objects WHERE uri=? AND native_id=? "
                "AND locator=? AND source_authority='workgraph-effect'",
                -1, &statement, NULL) != SQLITE_OK) {
            return -1;
        }
        sqlite3_bind_text(statement, 1, record->target_uri, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 2, record->effect_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 3, record->input_artifact_uri, -1, SQLITE_TRANSIENT);
        found = sqlite3_step(statement) == SQLITE_ROW;
        sqlite3_finalize(statement);
        return found ? 0 : -1;
    }
    if (strcmp(record->adapter_id, "git-worktree") == 0) {
        char target[PATH_MAX];
        char head[1024];

        if (effect_local_path(graph, record->target_uri, target) != 0 ||
            effect_git_head(target, head) != 0) {
            return -1;
        }
        return strcmp(head, record->prepared_state) == 0 ? 0 : -1;
    }
    if (strcmp(record->adapter_id, "git-candidate-commit") == 0) {
        char target[PATH_MAX];
        char head[1024];

        if (effect_local_path(graph, record->target_uri, target) != 0 ||
            effect_git_head(target, head) != 0) {
            return -1;
        }
        return strcmp(head, record->prepared_state) != 0 ? 0 : -1;
    }
    return effect_verify_file(graph, record);
}

static int effect_rollback_adapter(BfWorkgraph *graph,
                                   const BfWorkgraphEffectRecord *record,
                                   int committed) {
    char input[PATH_MAX];
    char target[PATH_MAX];

    if (strcmp(record->adapter_id, "namespace-alias") == 0) {
        sqlite3_stmt *statement = NULL;

        if (sqlite3_prepare_v2(graph->db,
                "DELETE FROM namespace_objects WHERE uri=? AND native_id=? "
                "AND source_authority='workgraph-effect'",
                -1, &statement, NULL) != SQLITE_OK) {
            return -1;
        }
        sqlite3_bind_text(statement, 1, record->target_uri, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 2, record->effect_id, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(statement) != SQLITE_DONE) {
            sqlite3_finalize(statement);
            return -1;
        }
        sqlite3_finalize(statement);
        return 0;
    }
    if (strcmp(record->adapter_id, "git-worktree") == 0) {
        char *arguments[] = {"git", "-C", input, "worktree", "remove", "--force", target, NULL};

        if (!committed) {
            return 0;
        }
        if (effect_local_path(graph, record->input_artifact_uri, input) != 0 ||
            effect_local_path(graph, record->target_uri, target) != 0) {
            return -1;
        }
        return effect_run(arguments, NULL, 0);
    }
    if (strcmp(record->adapter_id, "git-candidate-commit") == 0) {
        char *arguments[] = {"git", "-C", target, "reset", "--hard",
                             (char *)record->prepared_state, NULL};

        if (!committed) {
            return 0;
        }
        if (effect_local_path(graph, record->target_uri, target) != 0) {
            return -1;
        }
        return effect_run(arguments, NULL, 0);
    }
    if (effect_local_path(graph, record->target_uri, target) != 0) {
        return -1;
    }
    if (unlink(record->prepared_state) != 0 && errno != ENOENT) {
        return -1;
    }
    if (committed && unlink(target) != 0 && errno != ENOENT) {
        return -1;
    }
    return 0;
}

static int effect_update_receipt(BfWorkgraph *graph, const BfWorkgraphResult *result) {
    sqlite3_stmt *statement = NULL;

    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_compensations SET receipt_id=? "
            "WHERE mission_id=? AND node_id=? AND effect_id=?",
            -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(statement, 1, result->receipt_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, result->mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, result->node_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 4, result->effect_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        sqlite3_finalize(statement);
        return -1;
    }
    sqlite3_finalize(statement);
    return 0;
}

static int effect_load(BfWorkgraph *graph, const char *mission_id, const char *node_id,
                       const char *effect_id, BfWorkgraphEffectRecord *record) {
    sqlite3_stmt *statement = NULL;

    memset(record, 0, sizeof(*record));
    if (sqlite3_prepare_v2(graph->db,
            "SELECT effect_id,adapter_id,target_uri,COALESCE(input_artifact_uri,''),"
            "prepared_state,verification_policy,rollback_contract,authority_identity,simulation,"
            "state,attempt,COALESCE(receipt_id,''),recovery_action,COALESCE(commit_nonce,''),"
            "COALESCE(commit_started_at_ms,0),COALESCE(verified_at_ms,0),"
            "COALESCE(expected_postcondition,'') "
            "FROM workgraph_compensations WHERE mission_id=? AND node_id=? AND effect_id=?",
            -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(statement, 1, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, node_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, effect_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_ROW) {
        sqlite3_finalize(statement);
        return 1;
    }
    snprintf(record->effect_id, sizeof(record->effect_id), "%s", sqlite3_column_text(statement, 0));
    snprintf(record->adapter_id, sizeof(record->adapter_id), "%s", sqlite3_column_text(statement, 1));
    snprintf(record->target_uri, sizeof(record->target_uri), "%s", sqlite3_column_text(statement, 2));
    snprintf(record->input_artifact_uri, sizeof(record->input_artifact_uri), "%s", sqlite3_column_text(statement, 3));
    snprintf(record->prepared_state, sizeof(record->prepared_state), "%s", sqlite3_column_text(statement, 4));
    snprintf(record->verification_policy, sizeof(record->verification_policy), "%s", sqlite3_column_text(statement, 5));
    snprintf(record->rollback_contract, sizeof(record->rollback_contract), "%s", sqlite3_column_text(statement, 6));
    snprintf(record->authority_identity, sizeof(record->authority_identity), "%s", sqlite3_column_text(statement, 7));
    snprintf(record->simulation, sizeof(record->simulation), "%s", sqlite3_column_text(statement, 8));
    snprintf(record->state, sizeof(record->state), "%s", sqlite3_column_text(statement, 9));
    record->attempt = sqlite3_column_int(statement, 10);
    snprintf(record->receipt_id, sizeof(record->receipt_id), "%s", sqlite3_column_text(statement, 11));
    snprintf(record->recovery_action, sizeof(record->recovery_action), "%s", sqlite3_column_text(statement, 12));
    snprintf(record->commit_nonce, sizeof(record->commit_nonce), "%s", sqlite3_column_text(statement, 13));
    record->commit_started_at_ms = sqlite3_column_int64(statement, 14);
    record->verified_at_ms = sqlite3_column_int64(statement, 15);
    snprintf(record->expected_postcondition, sizeof(record->expected_postcondition), "%s",
             sqlite3_column_text(statement, 16));
    sqlite3_finalize(statement);
    return 0;
}

static void effect_describe_postcondition(const BfWorkgraphEffectRecord *record,
                                          char output[256]) {
    if (strcmp(record->adapter_id, "namespace-alias") == 0) {
        snprintf(output, 256, "namespace-object-exists:%s", record->target_uri);
    } else if (strcmp(record->adapter_id, "git-worktree") == 0) {
        snprintf(output, 256, "git-head-equals:%s:%s", record->target_uri, record->prepared_state);
    } else if (strcmp(record->adapter_id, "git-candidate-commit") == 0) {
        snprintf(output, 256, "git-head-changed-from:%s:%s", record->target_uri, record->prepared_state);
    } else {
        snprintf(output, 256, "%s-file-matches-input:%s", record->verification_policy, record->target_uri);
    }
}

/*
 * Whether it is safe to attempt an automatic rollback of an effect stuck in
 * 'commit_started' after a crash. Safe means the external mutation provably
 * never took effect, so cleanup cannot destroy anything the mutation created.
 * When unsafe, the caller must fall back to reconciliation_required rather
 * than guess.
 */
static int effect_reconciliation_safe(BfWorkgraph *graph, const BfWorkgraphEffectRecord *record) {
    char target[PATH_MAX];

    if (strcmp(record->adapter_id, "namespace-alias") == 0) {
        /* Deleting a namespace alias that was never created is a no-op. */
        return 1;
    }
    if (strcmp(record->adapter_id, "git-candidate-commit") == 0) {
        char head[1024];

        if (effect_local_path(graph, record->target_uri, target) != 0 ||
            effect_git_head(target, head) != 0) {
            return 0;
        }
        /* HEAD unchanged from the pre-commit snapshot means no commit landed. */
        return strcmp(head, record->prepared_state) == 0;
    }
    if (effect_local_path(graph, record->target_uri, target) != 0) {
        return 0;
    }
    /* git-worktree and the generic file adapters all materialize target_uri;
     * if it never appeared, the mutation never ran. */
    return access(target, F_OK) != 0;
}

BfWorkgraphResult bf_workgraph_prepare_effect(BfWorkgraph *graph, const char *mission_id,
                                              const char *node_id, const char *worker_id,
                                              const char *claim_token,
                                              const BfWorkgraphEffectSpec *spec) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, node_id, NULL, NULL);
    sqlite3_stmt *statement = NULL;
    char prepared_state[1024];
    char simulation[1024];
    int64_t now = bf_workgraph_now_ms();
    int adapter_prepared = 0;

    if (graph == NULL || spec == NULL || spec->effect_id == NULL ||
        spec->adapter_id == NULL || spec->target_uri == NULL ||
        spec->input_artifact_uri == NULL) {
        return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_id, node_id,
                                   "invalid_effect", "effect specification is incomplete");
    }
    snprintf(result.effect_id, sizeof(result.effect_id), "%s", spec->effect_id);
    if (bf_workgraph_begin(graph, &result) != 0) {
        return result;
    }
    if (bf_workgraph_validate_claim(graph, &result, worker_id, claim_token, 0) != 0) {
        goto failure;
    }
    if (sqlite3_prepare_v2(graph->db,
            "SELECT 1 FROM workgraph_compensations WHERE mission_id=? AND node_id=? "
            "AND state IN ('commit_started','reconciliation_required') LIMIT 1",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    sqlite3_bind_text(statement, 1, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, node_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) == SQLITE_ROW) {
        sqlite3_finalize(statement);
        statement = NULL;
        result.status = BF_WORKGRAPH_NOT_ELIGIBLE;
        snprintf(result.error_code, sizeof(result.error_code), "node_reconciliation_required");
        snprintf(result.error_message, sizeof(result.error_message),
                 "an unresolved effect commit on this node is blocking further mutation");
        goto failure;
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (effect_prepare_adapter(graph, spec, prepared_state, simulation) != 0) {
        result.status = BF_WORKGRAPH_INVALID;
        snprintf(result.error_code, sizeof(result.error_code), "effect_prepare_failed");
        snprintf(result.error_message, sizeof(result.error_message),
                 "effect adapter validation, simulation, authorization, or prepare failed");
        goto failure;
    }
    adapter_prepared = 1;
    if (sqlite3_prepare_v2(graph->db,
            "INSERT INTO workgraph_compensations(mission_id,node_id,effect_id,state,attempt,"
            "created_at_ms,updated_at_ms,adapter_id,target_uri,input_artifact_uri,prepared_state,"
            "verification_policy,rollback_contract,authority_identity,simulation,recovery_action) "
            "VALUES(?,?,?,'prepared',0,?,?,?,?,?,?,?,?,?,?,'rollback')",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    sqlite3_bind_text(statement, 1, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, node_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, spec->effect_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement, 4, now);
    sqlite3_bind_int64(statement, 5, now);
    sqlite3_bind_text(statement, 6, spec->adapter_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 7, spec->target_uri, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 8, spec->input_artifact_uri, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 9, prepared_state, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 10,
                      spec->verification_policy != NULL ? spec->verification_policy : "sha256",
                      -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 11,
                      spec->rollback_contract != NULL ? spec->rollback_contract :
                      "remove-created-target", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 12, spec->authority_identity, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 13, simulation, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        goto storage_failure;
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_nodes SET effect_state='prepared',updated_at_ms=? "
            "WHERE mission_id=? AND node_id=?",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    sqlite3_bind_int64(statement, 1, now);
    sqlite3_bind_text(statement, 2, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, node_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        goto storage_failure;
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (bf_workgraph_write_evidence(graph, &result, "effect_prepared", worker_id, NULL) != 0 ||
        effect_update_receipt(graph, &result) != 0 ||
        bf_workgraph_commit(graph, &result) != 0) {
        goto storage_failure;
    }
    return result;

failure:
    bf_workgraph_rollback(graph);
    return result;

storage_failure:
    if (statement != NULL) {
        sqlite3_finalize(statement);
    }
    bf_workgraph_rollback(graph);
    if (adapter_prepared) {
        BfWorkgraphEffectRecord cleanup;

        memset(&cleanup, 0, sizeof(cleanup));
        snprintf(cleanup.effect_id, sizeof(cleanup.effect_id), "%s", spec->effect_id);
        snprintf(cleanup.adapter_id, sizeof(cleanup.adapter_id), "%s", spec->adapter_id);
        snprintf(cleanup.target_uri, sizeof(cleanup.target_uri), "%s", spec->target_uri);
        snprintf(cleanup.input_artifact_uri, sizeof(cleanup.input_artifact_uri), "%s",
                 spec->input_artifact_uri);
        snprintf(cleanup.prepared_state, sizeof(cleanup.prepared_state), "%s", prepared_state);
        effect_rollback_adapter(graph, &cleanup, 0);
    }
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "effect_storage_failed");
    snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
    return result;
}

typedef enum EffectReconcileOutcome {
    EFFECT_RECONCILE_COMMITTED,
    EFFECT_RECONCILE_RESET,
    EFFECT_RECONCILE_BLOCKED,
    EFFECT_RECONCILE_STORAGE_ERROR
} EffectReconcileOutcome;

/*
 * Resolves a single effect stuck in 'commit_started' (whether just persisted by
 * this process or discovered on resume after a crash). Never assumes the
 * external mutation did or did not happen -- it re-verifies the postcondition
 * and only cleans up when that is provably safe; otherwise it blocks further
 * mutation of this effect until an operator resolves it.
 */
static EffectReconcileOutcome effect_reconcile_commit_started(
        BfWorkgraph *graph, const char *mission_id, const char *node_id,
        const BfWorkgraphEffectRecord *record, const char *actor) {
    int64_t now = bf_workgraph_now_ms();
    int verified = effect_verify_adapter(graph, record) == 0;
    int safe = 0;
    const char *to_state;
    const char *evidence_name;
    sqlite3_stmt *statement = NULL;
    BfWorkgraphResult evidence_result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, node_id, NULL, NULL);
    BfWorkgraphEvidence evidence = {
        .transition_domain = BF_WORKGRAPH_TRANSITION_EFFECT,
        .execution_attempt = -1,
        .compensation_attempt = -1,
        .from_state = "commit_started",
        .to_state = NULL,
    };

    if (!verified) {
        safe = effect_reconciliation_safe(graph, record);
        if (safe) {
            effect_rollback_adapter(graph, record, 0);
        }
    }
    to_state = verified ? "committed" : (safe ? "prepared" : "reconciliation_required");
    evidence_name = verified ? "effect_committed" :
        (safe ? "effect_commit_reset" : "effect_reconciliation_required");
    evidence.to_state = evidence_name;

    snprintf(evidence_result.effect_id, sizeof(evidence_result.effect_id), "%s", record->effect_id);
    evidence_result.attempt = record->attempt;

    if (bf_workgraph_begin(graph, &evidence_result) != 0) {
        return EFFECT_RECONCILE_STORAGE_ERROR;
    }
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_compensations SET state=?,"
            "recovery_action=CASE WHEN ?='committed' THEN 'compensation' ELSE recovery_action END,"
            "verified_at_ms=CASE WHEN ?='committed' THEN ? ELSE verified_at_ms END,"
            "commit_nonce=CASE WHEN ?='reconciliation_required' THEN commit_nonce ELSE NULL END,"
            "commit_started_at_ms=CASE WHEN ?='reconciliation_required' THEN commit_started_at_ms ELSE NULL END,"
            "expected_postcondition=CASE WHEN ?='reconciliation_required' THEN expected_postcondition ELSE NULL END,"
            "updated_at_ms=? "
            "WHERE mission_id=? AND node_id=? AND effect_id=? AND state='commit_started'",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    sqlite3_bind_text(statement, 1, to_state, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, to_state, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, to_state, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement, 4, now);
    sqlite3_bind_text(statement, 5, to_state, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 6, to_state, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 7, to_state, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement, 8, now);
    sqlite3_bind_text(statement, 9, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 10, node_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 11, record->effect_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE || sqlite3_changes(graph->db) != 1) {
        goto storage_failure;
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (verified) {
        if (sqlite3_prepare_v2(graph->db,
                "UPDATE workgraph_nodes SET effect_state='committed',updated_at_ms=? "
                "WHERE mission_id=? AND node_id=?",
                -1, &statement, NULL) != SQLITE_OK) {
            goto storage_failure;
        }
        sqlite3_bind_int64(statement, 1, now);
        sqlite3_bind_text(statement, 2, mission_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 3, node_id, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(statement) != SQLITE_DONE) {
            goto storage_failure;
        }
        sqlite3_finalize(statement);
        statement = NULL;
    }
    if (bf_workgraph_write_evidence_ex(graph, &evidence_result, &evidence, actor,
                                       verified || safe ? NULL : "effect_reconciliation_required") != 0 ||
        effect_update_receipt(graph, &evidence_result) != 0 ||
        bf_workgraph_commit(graph, &evidence_result) != 0) {
        goto storage_failure;
    }
    return verified ? EFFECT_RECONCILE_COMMITTED :
        (safe ? EFFECT_RECONCILE_RESET : EFFECT_RECONCILE_BLOCKED);

storage_failure:
    if (statement != NULL) {
        sqlite3_finalize(statement);
    }
    bf_workgraph_rollback(graph);
    return EFFECT_RECONCILE_STORAGE_ERROR;
}

BfWorkgraphResult bf_workgraph_commit_effect(BfWorkgraph *graph, const char *mission_id,
                                             const char *node_id, const char *worker_id,
                                             const char *claim_token, const char *effect_id) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, node_id, NULL, NULL);
    BfWorkgraphEffectRecord record;
    sqlite3_stmt *statement = NULL;
    char nonce[65];
    char nonce_digest[65];
    char postcondition[256];
    int64_t now = bf_workgraph_now_ms();
    EffectReconcileOutcome outcome;

    if (graph == NULL || effect_id == NULL) {
        return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_id, node_id,
                                   "invalid_effect", "effect identifier is required");
    }
    snprintf(result.effect_id, sizeof(result.effect_id), "%s", effect_id);

    /* Phase A: persist commit intent (state, adapter, target, prepared_state,
     * expected postcondition, rollback contract, operation nonce) and commit it
     * durably before touching anything external. */
    if (bf_workgraph_begin(graph, &result) != 0) {
        return result;
    }
    if (bf_workgraph_validate_claim(graph, &result, worker_id, claim_token, 0) != 0) {
        goto failure;
    }
    if (effect_load(graph, mission_id, node_id, effect_id, &record) != 0) {
        result.status = BF_WORKGRAPH_NOT_FOUND;
        snprintf(result.error_code, sizeof(result.error_code), "effect_not_found");
        goto failure;
    }
    if (strcmp(record.state, "prepared") != 0) {
        result.status = BF_WORKGRAPH_NOT_ELIGIBLE;
        snprintf(result.error_code, sizeof(result.error_code), "effect_state_conflict");
        goto failure;
    }
    if (bf_workgraph_generate_token(nonce, nonce_digest) != 0) {
        goto storage_failure;
    }
    effect_describe_postcondition(&record, postcondition);
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_compensations SET state='commit_started',commit_nonce=?,"
            "commit_started_at_ms=?,expected_postcondition=?,updated_at_ms=? "
            "WHERE mission_id=? AND node_id=? AND effect_id=? AND state='prepared'",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    sqlite3_bind_text(statement, 1, nonce, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement, 2, now);
    sqlite3_bind_text(statement, 3, postcondition, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement, 4, now);
    sqlite3_bind_text(statement, 5, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 6, node_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 7, effect_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE || sqlite3_changes(graph->db) != 1) {
        goto storage_failure;
    }
    sqlite3_finalize(statement);
    statement = NULL;
    {
        BfWorkgraphEvidence evidence = {
            .transition_domain = BF_WORKGRAPH_TRANSITION_EFFECT,
            .execution_attempt = -1,
            .compensation_attempt = -1,
            .from_state = "prepared",
            .to_state = "effect_commit_started",
        };
        if (bf_workgraph_write_evidence_ex(graph, &result, &evidence, worker_id, NULL) != 0 ||
            bf_workgraph_commit(graph, &result) != 0) {
            goto storage_failure;
        }
    }
    snprintf(record.commit_nonce, sizeof(record.commit_nonce), "%s", nonce);
    record.commit_started_at_ms = now;
    snprintf(record.expected_postcondition, sizeof(record.expected_postcondition), "%s", postcondition);
    snprintf(record.state, sizeof(record.state), "commit_started");

    /* Phase B: perform the external mutation with no open database transaction,
     * so a crash here can never roll back the durable commit_started intent. */
    if (effect_verify_adapter(graph, &record) != 0) {
        effect_commit_adapter(graph, &record);
    }

    if (getenv("BONFYRE_TEST_CRASH_AFTER_EFFECT_MUTATION") != NULL) {
        /* Test-only crash injection point: proves restart reconciliation recovers
         * correctly when the process dies after the external mutation lands but
         * before the durable commit/verify transaction runs. */
        raise(SIGKILL);
    }

    /* Phase C: reconcile against the postcondition and record committed+verified,
     * or a safe reset / reconciliation_required, in a fresh transaction. */
    outcome = effect_reconcile_commit_started(graph, mission_id, node_id, &record, worker_id);
    switch (outcome) {
        case EFFECT_RECONCILE_COMMITTED:
            result.status = BF_WORKGRAPH_OK;
            return result;
        case EFFECT_RECONCILE_RESET:
            result.status = BF_WORKGRAPH_INVALID;
            snprintf(result.error_code, sizeof(result.error_code), "effect_verification_failed");
            snprintf(result.error_message, sizeof(result.error_message),
                     "effect commit or verification failed and was safely reset for retry");
            return result;
        case EFFECT_RECONCILE_BLOCKED:
            result.status = BF_WORKGRAPH_INVALID;
            snprintf(result.error_code, sizeof(result.error_code), "effect_reconciliation_required");
            snprintf(result.error_message, sizeof(result.error_message),
                     "effect postcondition could not be verified and rollback was not provably safe; "
                     "further mutation is blocked pending reconciliation");
            return result;
        case EFFECT_RECONCILE_STORAGE_ERROR:
        default:
            result.status = BF_WORKGRAPH_STORAGE_ERROR;
            snprintf(result.error_code, sizeof(result.error_code), "effect_reconciliation_storage_failed");
            snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
            return result;
    }

failure:
    bf_workgraph_rollback(graph);
    return result;

storage_failure:
    if (statement != NULL) {
        sqlite3_finalize(statement);
    }
    bf_workgraph_rollback(graph);
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "effect_storage_failed");
    snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
    return result;
}

int bf_workgraph_reconcile_effects(BfWorkgraph *graph, const char *mission_id) {
    sqlite3_stmt *statement = NULL;
    int inspected = 0;

    if (graph == NULL) {
        return -1;
    }
    if (sqlite3_prepare_v2(graph->db,
            "SELECT mission_id,node_id,effect_id FROM workgraph_compensations "
            "WHERE state='commit_started' AND (? IS NULL OR mission_id=?)",
            -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    if (mission_id != NULL) {
        sqlite3_bind_text(statement, 1, mission_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 2, mission_id, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(statement, 1);
        sqlite3_bind_null(statement, 2);
    }
    for (;;) {
        char row_mission[128];
        char row_node[128];
        char row_effect[128];
        BfWorkgraphEffectRecord record;
        int step_result = sqlite3_step(statement);

        if (step_result == SQLITE_DONE) {
            break;
        }
        if (step_result != SQLITE_ROW) {
            sqlite3_finalize(statement);
            return -1;
        }
        snprintf(row_mission, sizeof(row_mission), "%s", sqlite3_column_text(statement, 0));
        snprintf(row_node, sizeof(row_node), "%s", sqlite3_column_text(statement, 1));
        snprintf(row_effect, sizeof(row_effect), "%s", sqlite3_column_text(statement, 2));
        if (effect_load(graph, row_mission, row_node, row_effect, &record) != 0) {
            continue;
        }
        if (effect_reconcile_commit_started(graph, row_mission, row_node, &record,
                                            "reconciliation") == EFFECT_RECONCILE_STORAGE_ERROR) {
            sqlite3_finalize(statement);
            return -1;
        }
        ++inspected;
    }
    sqlite3_finalize(statement);
    return inspected;
}

BfWorkgraphResult bf_workgraph_claim_compensation(BfWorkgraph *graph,
                                                  const char *mission_id,
                                                  const BfWorkgraphClaimSpec *spec) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, NULL, NULL, NULL);
    sqlite3_stmt *statement = NULL;
    char selected_mission[128];
    char selected_node[128];
    char selected_effect[128];
    char selected_state[32];
    char token_digest[65];
    int compensation_attempt;
    int64_t now = bf_workgraph_now_ms();
    int64_t lease_expires_at_ms;

    if (graph == NULL || spec == NULL || spec->worker_id == NULL || spec->lease_ms <= 0) {
        return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_id, NULL,
                                   "invalid_compensation_claim",
                                   "worker and positive compensation lease are required");
    }
    if (bf_workgraph_begin(graph, &result) != 0) {
        return result;
    }
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_compensation_attempts SET finished_at_ms=?,outcome='lease_expired',"
            "error_code='lease_expired' WHERE finished_at_ms IS NULL AND lease_expires_at_ms<=?",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    sqlite3_bind_int64(statement, 1, now);
    sqlite3_bind_int64(statement, 2, now);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        goto storage_failure;
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_compensations AS c SET state=CASE "
            "WHEN recovery_action='rollback' THEN 'rollback_required' "
            "ELSE 'compensation_required' END,updated_at_ms=? "
            "WHERE state='compensating' AND EXISTS(SELECT 1 FROM workgraph_compensation_attempts a "
            "WHERE a.mission_id=c.mission_id AND a.node_id=c.node_id AND a.effect_id=c.effect_id "
            "AND a.attempt=c.attempt AND a.outcome='lease_expired')",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    sqlite3_bind_int64(statement, 1, now);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        goto storage_failure;
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (sqlite3_prepare_v2(graph->db,
            "SELECT c.mission_id,c.node_id,c.effect_id,c.attempt+1,c.state "
            "FROM workgraph_compensations c JOIN missions m ON m.id=c.mission_id "
            "WHERE c.state IN ('rollback_required','compensation_required','compensation_failed') "
            "AND (? IS NULL OR c.mission_id=?) "
            "ORDER BY c.updated_at_ms,c.mission_id,c.node_id,c.effect_id LIMIT 1",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    if (mission_id != NULL) {
        sqlite3_bind_text(statement, 1, mission_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(statement, 2, mission_id, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(statement, 1);
        sqlite3_bind_null(statement, 2);
    }
    if (sqlite3_step(statement) != SQLITE_ROW) {
        sqlite3_finalize(statement);
        bf_workgraph_rollback(graph);
        return bf_workgraph_result(BF_WORKGRAPH_NOT_ELIGIBLE, mission_id, NULL,
                                   "no_compensation_available",
                                   "no compensation is ready to claim");
    }
    snprintf(selected_mission, sizeof(selected_mission), "%s", sqlite3_column_text(statement, 0));
    snprintf(selected_node, sizeof(selected_node), "%s", sqlite3_column_text(statement, 1));
    snprintf(selected_effect, sizeof(selected_effect), "%s", sqlite3_column_text(statement, 2));
    compensation_attempt = sqlite3_column_int(statement, 3);
    snprintf(selected_state, sizeof(selected_state), "%s", sqlite3_column_text(statement, 4));
    sqlite3_finalize(statement);
    statement = NULL;
    if (bf_workgraph_generate_token(result.claim_token, token_digest) != 0) {
        goto storage_failure;
    }
    lease_expires_at_ms = now + spec->lease_ms;
    snprintf(result.mission_id, sizeof(result.mission_id), "%s", selected_mission);
    snprintf(result.node_id, sizeof(result.node_id), "%s", selected_node);
    snprintf(result.effect_id, sizeof(result.effect_id), "%s", selected_effect);
    snprintf(result.worker_id, sizeof(result.worker_id), "%s", spec->worker_id);
    result.attempt = compensation_attempt;
    result.lease_expires_at_ms = lease_expires_at_ms;
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_compensations SET state='compensating',attempt=?,updated_at_ms=? "
            "WHERE mission_id=? AND node_id=? AND effect_id=? "
            "AND state IN ('rollback_required','compensation_required','compensation_failed')",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    sqlite3_bind_int(statement, 1, compensation_attempt);
    sqlite3_bind_int64(statement, 2, now);
    sqlite3_bind_text(statement, 3, selected_mission, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 4, selected_node, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 5, selected_effect, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE || sqlite3_changes(graph->db) != 1) {
        goto storage_failure;
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (sqlite3_prepare_v2(graph->db,
            "INSERT INTO workgraph_compensation_attempts(mission_id,node_id,effect_id,attempt,"
            "worker_id,claim_token_digest,started_at_ms,lease_expires_at_ms) VALUES(?,?,?,?,?,?,?,?)",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    sqlite3_bind_text(statement, 1, selected_mission, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, selected_node, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, selected_effect, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(statement, 4, compensation_attempt);
    sqlite3_bind_text(statement, 5, spec->worker_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 6, token_digest, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement, 7, now);
    sqlite3_bind_int64(statement, 8, lease_expires_at_ms);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        goto storage_failure;
    }
    sqlite3_finalize(statement);
    statement = NULL;
    {
        BfWorkgraphEvidence evidence = {
            .transition_domain = BF_WORKGRAPH_TRANSITION_COMPENSATION,
            .execution_attempt = -1,
            .compensation_attempt = compensation_attempt,
            .from_state = selected_state,
            .to_state = "compensation_claimed",
        };
        if (bf_workgraph_write_evidence_ex(graph, &result, &evidence,
                                           spec->worker_id, NULL) != 0 ||
            effect_update_receipt(graph, &result) != 0) {
            goto storage_failure;
        }
    }
    result.attempt = compensation_attempt;
    result.lease_expires_at_ms = lease_expires_at_ms;
    snprintf(result.worker_id, sizeof(result.worker_id), "%s", spec->worker_id);
    if (bf_workgraph_commit(graph, &result) != 0) {
        goto storage_failure;
    }
    return result;

storage_failure:
    if (statement != NULL) {
        sqlite3_finalize(statement);
    }
    bf_workgraph_rollback(graph);
    result.claim_token[0] = '\0';
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "compensation_claim_failed");
    snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
    return result;
}

static int effect_validate_compensation_claim(BfWorkgraph *graph,
                                              BfWorkgraphResult *result,
                                              const char *worker_id,
                                              const char *claim_token,
                                              int *terminal) {
    sqlite3_stmt *statement = NULL;
    char stored_worker[128];
    char stored_digest[65];
    char supplied_digest[65];
    char state[32];
    int64_t lease_expires_at_ms;
    int64_t now = bf_workgraph_now_ms();

    if (worker_id == NULL || claim_token == NULL || terminal == NULL) {
        return -1;
    }
    if (sqlite3_prepare_v2(graph->db,
            "SELECT c.state,c.attempt,a.worker_id,a.claim_token_digest,a.lease_expires_at_ms "
            "FROM workgraph_compensations c JOIN workgraph_compensation_attempts a "
            "ON a.mission_id=c.mission_id AND a.node_id=c.node_id AND a.effect_id=c.effect_id "
            "AND a.attempt=c.attempt WHERE c.mission_id=? AND c.node_id=? AND c.effect_id=?",
            -1, &statement, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(statement, 1, result->mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, result->node_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, result->effect_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_ROW) {
        sqlite3_finalize(statement);
        result->status = BF_WORKGRAPH_NOT_FOUND;
        snprintf(result->error_code, sizeof(result->error_code), "compensation_claim_not_found");
        return -1;
    }
    snprintf(state, sizeof(state), "%s", sqlite3_column_text(statement, 0));
    result->attempt = sqlite3_column_int(statement, 1);
    snprintf(stored_worker, sizeof(stored_worker), "%s", sqlite3_column_text(statement, 2));
    snprintf(stored_digest, sizeof(stored_digest), "%s", sqlite3_column_text(statement, 3));
    lease_expires_at_ms = sqlite3_column_int64(statement, 4);
    sqlite3_finalize(statement);
    *terminal = strcmp(state, "rolled_back") == 0 || strcmp(state, "compensated") == 0;
    bf_workgraph_hash_token(claim_token, supplied_digest);
    if (strcmp(stored_worker, worker_id) != 0 || strcmp(stored_digest, supplied_digest) != 0 ||
        (!*terminal && (strcmp(state, "compensating") != 0 || lease_expires_at_ms <= now))) {
        result->status = BF_WORKGRAPH_STALE_CLAIM;
        snprintf(result->error_code, sizeof(result->error_code), "stale_compensation_claim");
        snprintf(result->error_message, sizeof(result->error_message),
                 "compensation worker, token, state, or lease is stale");
        return -1;
    }
    snprintf(result->worker_id, sizeof(result->worker_id), "%s", worker_id);
    result->lease_expires_at_ms = lease_expires_at_ms;
    return 0;
}

BfWorkgraphResult bf_workgraph_compensate(BfWorkgraph *graph, const char *mission_id,
                                          const char *node_id, const char *worker_id,
                                          const char *claim_token, const char *effect_id,
                                          int succeeded) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, node_id, NULL, NULL);
    BfWorkgraphEffectRecord record;
    sqlite3_stmt *statement = NULL;
    const char *new_state;
    int terminal = 0;
    int rollback;
    int64_t now = bf_workgraph_now_ms();

    if (graph == NULL || effect_id == NULL) {
        return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_id, node_id,
                                   "invalid_effect", "effect identifier is required");
    }
    snprintf(result.effect_id, sizeof(result.effect_id), "%s", effect_id);
    if (bf_workgraph_begin(graph, &result) != 0) {
        return result;
    }
    if (effect_validate_compensation_claim(graph, &result, worker_id,
                                           claim_token, &terminal) != 0) {
        goto failure;
    }
    if (effect_load(graph, mission_id, node_id, effect_id, &record) != 0) {
        result.status = BF_WORKGRAPH_NOT_FOUND;
        goto failure;
    }
    if (terminal) {
        int compensation_attempt = result.attempt;
        int64_t compensation_lease = result.lease_expires_at_ms;

        bf_workgraph_rollback(graph);
        if (bf_workgraph_load_result(graph, &result) != 0) {
            result.status = BF_WORKGRAPH_STORAGE_ERROR;
        }
        result.attempt = compensation_attempt;
        result.lease_expires_at_ms = compensation_lease;
        snprintf(result.worker_id, sizeof(result.worker_id), "%s", worker_id);
        return result;
    }
    rollback = strcmp(record.recovery_action, "rollback") == 0;
    if (succeeded && effect_rollback_adapter(graph, &record, !rollback) != 0) {
        succeeded = 0;
    }
    new_state = succeeded ? (rollback ? "rolled_back" : "compensated") :
        "compensation_failed";
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_compensations SET state=?,updated_at_ms=?,last_error=? "
            "WHERE mission_id=? AND node_id=? AND effect_id=? AND state='compensating'",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    sqlite3_bind_text(statement, 1, new_state, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement, 2, now);
    if (succeeded) {
        sqlite3_bind_null(statement, 3);
    } else {
        sqlite3_bind_text(statement, 3, "adapter rollback or compensation failed",
                          -1, SQLITE_STATIC);
    }
    sqlite3_bind_text(statement, 4, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 5, node_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 6, effect_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE || sqlite3_changes(graph->db) != 1) {
        goto storage_failure;
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_compensation_attempts SET finished_at_ms=?,outcome=?,error_code=? "
            "WHERE mission_id=? AND node_id=? AND effect_id=? AND attempt=?",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    sqlite3_bind_int64(statement, 1, now);
    sqlite3_bind_text(statement, 2, new_state, -1, SQLITE_TRANSIENT);
    if (succeeded) {
        sqlite3_bind_null(statement, 3);
    } else {
        sqlite3_bind_text(statement, 3, "compensation_failed", -1, SQLITE_STATIC);
    }
    sqlite3_bind_text(statement, 4, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 5, node_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 6, effect_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(statement, 7, result.attempt);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        goto storage_failure;
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (sqlite3_prepare_v2(graph->db,
            "UPDATE workgraph_nodes SET effect_state=?,"
            "status=CASE WHEN ? THEN 'cancelled' ELSE status END,"
            "terminal_at_ms=CASE WHEN ? THEN ? ELSE terminal_at_ms END,"
            "lease_owner=CASE WHEN ? THEN NULL ELSE lease_owner END,"
            "claim_token_hash=CASE WHEN ? THEN NULL ELSE claim_token_hash END,updated_at_ms=? "
            "WHERE mission_id=? AND node_id=?",
            -1, &statement, NULL) != SQLITE_OK) {
        goto storage_failure;
    }
    sqlite3_bind_text(statement, 1, new_state, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(statement, 2, succeeded);
    sqlite3_bind_int(statement, 3, succeeded);
    sqlite3_bind_int64(statement, 4, now);
    sqlite3_bind_int(statement, 5, succeeded);
    sqlite3_bind_int(statement, 6, succeeded);
    sqlite3_bind_int64(statement, 7, now);
    sqlite3_bind_text(statement, 8, mission_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 9, node_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE) {
        goto storage_failure;
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (bf_workgraph_update_mission(graph, mission_id, &result) != 0) {
        goto storage_failure;
    }
    {
        BfWorkgraphEvidence evidence = {
            .transition_domain = BF_WORKGRAPH_TRANSITION_COMPENSATION,
            .execution_attempt = -1,
            .compensation_attempt = result.attempt,
            .from_state = record.state,
            .to_state = new_state,
        };
        if (bf_workgraph_write_evidence_ex(graph, &result, &evidence, worker_id,
                                           succeeded ? NULL : "compensation_failed") != 0 ||
            effect_update_receipt(graph, &result) != 0) {
            goto storage_failure;
        }
    }
    result.attempt = record.attempt;
    if (bf_workgraph_commit(graph, &result) != 0) {
        goto storage_failure;
    }
    return result;

failure:
    bf_workgraph_rollback(graph);
    return result;

storage_failure:
    if (statement != NULL) {
        sqlite3_finalize(statement);
    }
    bf_workgraph_rollback(graph);
    result.status = BF_WORKGRAPH_STORAGE_ERROR;
    snprintf(result.error_code, sizeof(result.error_code), "compensation_storage_failed");
    snprintf(result.error_message, sizeof(result.error_message), "%s", sqlite3_errmsg(graph->db));
    return result;
}

BfWorkgraphResult bf_workgraph_effect_status(BfWorkgraph *graph, const char *mission_id,
                                             const char *node_id, const char *effect_id,
                                             BfWorkgraphEffectRecord *record) {
    BfWorkgraphResult result = bf_workgraph_result(BF_WORKGRAPH_OK, mission_id, node_id, NULL, NULL);
    int loaded;

    if (graph == NULL || mission_id == NULL || node_id == NULL || effect_id == NULL ||
        record == NULL) {
        return bf_workgraph_result(BF_WORKGRAPH_INVALID, mission_id, node_id,
                                   "invalid_effect_status", "effect status arguments are required");
    }
    snprintf(result.effect_id, sizeof(result.effect_id), "%s", effect_id);
    loaded = effect_load(graph, mission_id, node_id, effect_id, record);
    if (loaded != 0) {
        result.status = loaded > 0 ? BF_WORKGRAPH_NOT_FOUND : BF_WORKGRAPH_STORAGE_ERROR;
        snprintf(result.error_code, sizeof(result.error_code), "effect_not_found");
        return result;
    }
    if (bf_workgraph_load_result(graph, &result) != 0) {
        result.status = BF_WORKGRAPH_STORAGE_ERROR;
        snprintf(result.error_code, sizeof(result.error_code), "effect_node_not_found");
    }
    result.attempt = record->attempt;
    snprintf(result.receipt_id, sizeof(result.receipt_id), "%s", record->receipt_id);
    return result;
}
