/* Native conformance: cross-harness run evidence enters the claimed node's own
 * hash-chained transition receipts. No trace store exists; the attestation is a
 * WorkGraph transition that changes no work state. */
#define _POSIX_C_SOURCE 200809L
#include "bf_workgraph.h"
#include "bonfyre.h"
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define CHECK(condition) do { if (!(condition)) { fprintf(stderr,"run evidence failure at line %d: %s\n",__LINE__,#condition); return 1; } } while (0)

static long long scalar(sqlite3 *db, const char *sql) {
    sqlite3_stmt *q = NULL; long long v = -1;
    if (sqlite3_prepare_v2(db, sql, -1, &q, NULL) == SQLITE_OK && sqlite3_step(q) == SQLITE_ROW) v = sqlite3_column_int64(q, 0);
    sqlite3_finalize(q); return v;
}
static int text(sqlite3 *db, const char *sql, char *out, size_t cap) {
    sqlite3_stmt *q = NULL; int ok = 0; out[0] = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &q, NULL) == SQLITE_OK && sqlite3_step(q) == SQLITE_ROW && sqlite3_column_text(q, 0)) {
        snprintf(out, cap, "%s", (const char *)sqlite3_column_text(q, 0)); ok = 1;
    }
    sqlite3_finalize(q); return ok;
}

#define HANDOFF "sha256:ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"

int main(void) {
    char path[] = "/tmp/bf-run-evidence-XXXXXX";
    int fd = mkstemp(path); CHECK(fd >= 0); close(fd); unlink(path);
    BfWorkgraph *graph = NULL;
    CHECK(bf_workgraph_open(path, &graph, stderr) == 0);
    CHECK(bf_workgraph_create_mission(graph, "m").status == BF_WORKGRAPH_OK);
    BfWorkgraphNodeSpec spec = {0}; spec.mission_id = "m"; spec.operator_id = "developer.task";
    spec.node_id = "a"; CHECK(bf_workgraph_add_node(graph, &spec).status == BF_WORKGRAPH_OK);
    spec.node_id = "b"; CHECK(bf_workgraph_add_node(graph, &spec).status == BF_WORKGRAPH_OK);
    BfWorkgraphRunEvidence run = {"claude", "14eafccc-76cc-42de-b864-6a6384a90d85", "claude-opus-5", HANDOFF};

    /* Unclaimed work cannot be attested. */
    CHECK(bf_workgraph_attest_run(graph, "m", "a", "dev", "0000000000000000000000000000000000000000000000000000000000000000", &run).status == BF_WORKGRAPH_STALE_CLAIM);
    BfWorkgraphResult claim = bf_workgraph_claim_node(graph, "m", "a", &(BfWorkgraphClaimSpec){"dev", NULL, 60000});
    CHECK(claim.status == BF_WORKGRAPH_OK);
    CHECK(bf_workgraph_attest_run(graph, "m", "a", "other", claim.claim_token, &run).status == BF_WORKGRAPH_STALE_CLAIM);

    /* Each field is bounded to the vocabulary Meter joins on; nothing is escaped or guessed. */
    BfWorkgraphRunEvidence bad = run; bad.agent = "gpt";
    CHECK(!strcmp(bf_workgraph_attest_run(graph, "m", "a", "dev", claim.claim_token, &bad).error_code, "invalid_run_evidence"));
    bad = run; bad.provider_session_ref = "../etc/passwd";
    CHECK(!strcmp(bf_workgraph_attest_run(graph, "m", "a", "dev", claim.claim_token, &bad).error_code, "invalid_session_ref"));
    bad = run; bad.provider_session_ref = "short";
    CHECK(bf_workgraph_attest_run(graph, "m", "a", "dev", claim.claim_token, &bad).status == BF_WORKGRAPH_INVALID);
    bad = run; bad.model_id = "model\"}";
    CHECK(!strcmp(bf_workgraph_attest_run(graph, "m", "a", "dev", claim.claim_token, &bad).error_code, "invalid_model_id"));
    bad = run; bad.handoff_receipt = "sha256:ABC";
    CHECK(!strcmp(bf_workgraph_attest_run(graph, "m", "a", "dev", claim.claim_token, &bad).error_code, "invalid_handoff_receipt"));

    sqlite3 *db = NULL; CHECK(sqlite3_open(path, &db) == SQLITE_OK);
    long long receipts_before = scalar(db, "SELECT count(*) FROM receipts");
    /* A storage fault rolls the whole attestation back. */
    CHECK(sqlite3_exec(db, "CREATE TRIGGER refuse_event BEFORE INSERT ON events WHEN NEW.status='run_attested' BEGIN SELECT RAISE(ABORT,'fixture failure'); END", NULL, NULL, NULL) == SQLITE_OK);
    CHECK(bf_workgraph_attest_run(graph, "m", "a", "dev", claim.claim_token, &run).status == BF_WORKGRAPH_STORAGE_ERROR);
    CHECK(scalar(db, "SELECT count(*) FROM receipts") == receipts_before);
    CHECK(sqlite3_exec(db, "DROP TRIGGER refuse_event", NULL, NULL, NULL) == SQLITE_OK);

    BfWorkgraphResult attested = bf_workgraph_attest_run(graph, "m", "a", "dev", claim.claim_token, &run);
    CHECK(attested.status == BF_WORKGRAPH_OK && attested.receipt_id[0]);
    CHECK(!strcmp(bf_workgraph_status(graph, "m", "a").node_status, "running"));

    /* The binding is in the node's chained receipt, linked to the claim's receipt. */
    char sql[512], value[512], prev_id[80], prev_chain[80], content[80], chain[80], material[200], expect[65];
    snprintf(sql, sizeof(sql), "SELECT json_extract(payload,'$.agent')||'|'||json_extract(payload,'$.provider_session_ref')||'|'||"
             "json_extract(payload,'$.model_id')||'|'||json_extract(payload,'$.handoff_receipt')||'|'||json_extract(payload,'$.attempt') "
             "FROM receipts WHERE id='%s'", attested.receipt_id);
    CHECK(text(db, sql, value, sizeof(value)));
    CHECK(!strcmp(value, "claude|14eafccc-76cc-42de-b864-6a6384a90d85|claude-opus-5|" HANDOFF "|1"));
    snprintf(sql, sizeof(sql), "SELECT previous_receipt_id FROM receipts WHERE id='%s'", attested.receipt_id);
    CHECK(text(db, sql, prev_id, sizeof(prev_id)));
    snprintf(sql, sizeof(sql), "SELECT receipt_id FROM workgraph_transitions WHERE mission_id='m' AND to_status='running' AND receipt_id<>'%s' ORDER BY sequence DESC LIMIT 1", attested.receipt_id);
    CHECK(text(db, sql, value, sizeof(value)) && !strcmp(value, prev_id));
    snprintf(sql, sizeof(sql), "SELECT COALESCE(chain_hash,content_hash) FROM receipts WHERE id='%s'", prev_id);
    CHECK(text(db, sql, prev_chain, sizeof(prev_chain)));
    snprintf(sql, sizeof(sql), "SELECT content_hash FROM receipts WHERE id='%s'", attested.receipt_id);
    CHECK(text(db, sql, content, sizeof(content)));
    snprintf(sql, sizeof(sql), "SELECT chain_hash FROM receipts WHERE id='%s'", attested.receipt_id);
    CHECK(text(db, sql, chain, sizeof(chain)));
    snprintf(material, sizeof(material), "%s|%s", prev_chain, content);
    bf_sha256_hex((const uint8_t *)material, strlen(material), expect);
    CHECK(!strcmp(expect, chain));
    snprintf(sql, sizeof(sql), "SELECT provider_id||'|'||model_id||'|'||input_uri||'|'||effect_class FROM events WHERE receipt_id='%s'", attested.receipt_id);
    CHECK(text(db, sql, value, sizeof(value)) && !strcmp(value, "claude|claude-opus-5|" HANDOFF "|governed-workgraph"));
    CHECK(scalar(db, "SELECT count(*) FROM sqlite_master WHERE name LIKE '%trace%'") == 0);

    /* One run per attempt; a session never names two attempts. */
    BfWorkgraphRunEvidence second = {"codex", "019a0000-1111-2222-3333-444455556666", NULL, NULL};
    CHECK(!strcmp(bf_workgraph_attest_run(graph, "m", "a", "dev", claim.claim_token, &second).error_code, "run_already_attested"));
    CHECK(bf_workgraph_complete(graph, "m", "a", "dev", claim.claim_token, "file:///result-a").status == BF_WORKGRAPH_OK);
    CHECK(bf_workgraph_attest_run(graph, "m", "a", "dev", claim.claim_token, &second).status == BF_WORKGRAPH_STALE_CLAIM);
    BfWorkgraphResult claim_b = bf_workgraph_claim_node(graph, "m", "b", &(BfWorkgraphClaimSpec){"dev2", NULL, 60000});
    CHECK(claim_b.status == BF_WORKGRAPH_OK);
    CHECK(!strcmp(bf_workgraph_attest_run(graph, "m", "b", "dev2", claim_b.claim_token, &run).error_code, "session_bound_elsewhere"));
    CHECK(bf_workgraph_attest_run(graph, "m", "b", "dev2", claim_b.claim_token, &second).status == BF_WORKGRAPH_OK);
    CHECK(scalar(db, "SELECT count(*) FROM events WHERE status='run_attested'") == 2);
    CHECK(scalar(db, "SELECT count(*) FROM events WHERE status='run_attested' AND provider_id='codex' AND model_id IS NULL AND input_uri IS NULL") == 1);
    sqlite3_close(db);
    bf_workgraph_close(graph); unlink(path);
    puts("native run evidence: claim-bound, chained, one session per attempt, rollback and refusal checks passed");
    return 0;
}
