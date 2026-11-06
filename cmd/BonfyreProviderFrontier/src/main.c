#define _POSIX_C_SOURCE 200809L

/*
 * BonfyreProviderFrontier
 *
 * A provider vector is a candidate-reduction representation, not the owner of
 * the candidate it names.  This consumer makes that boundary executable:
 *
 * ProviderEmbeddingRank
 *        + AddressPlane typed advertisement
 *        + independently retained ForeignTwin receipts
 *        -> exact-owner query ledger
 *
 * It deliberately does not emit a lowerer handoff.  Retained bytes are not a
 * selection, an authority decision, evidence admission, or a request to read
 * another source.  A later policy may consume this receipt, but must make that
 * decision through its own owner.
 */

#include <bonfyre.h>
#include "bf_foreign_twin.h"
#include <sqlite3.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

static const char *option_value(int argc, char **argv, const char *name)
{
    for (int i = 2; i + 1 < argc; i++) if (strcmp(argv[i], name) == 0) return argv[i + 1];
    return NULL;
}

static int json_string(FILE *out, const char *text)
{
    const unsigned char *p = (const unsigned char *)(text ? text : "");
    if (fputc('"', out) == EOF) return 0;
    for (; *p; p++) {
        if (*p == '"' || *p == '\\') { if (fputc('\\', out) == EOF || fputc(*p, out) == EOF) return 0; }
        else if (*p == '\n') { if (fputs("\\n", out) == EOF) return 0; }
        else if (*p == '\r') { if (fputs("\\r", out) == EOF) return 0; }
        else if (*p == '\t') { if (fputs("\\t", out) == EOF) return 0; }
        else if (*p < 0x20) { if (fprintf(out, "\\u%04x", *p) < 0) return 0; }
        else if (fputc(*p, out) == EOF) return 0;
    }
    return fputc('"', out) != EOF;
}

static int nullable_string(FILE *out, const char *text)
{
    return text && *text ? json_string(out, text) : fputs("null", out) >= 0;
}

static int ingest_foreign_receipts(BfForeignTwin *twin, const char *path, int *accepted)
{
    FILE *in = NULL;
    sqlite3 *json = NULL;
    sqlite3_stmt *parse = NULL;
    char *line = NULL;
    size_t capacity = 0;
    ssize_t length;
    int ok = 0;
    if (!(in = fopen(path, "r")) || sqlite3_open(":memory:", &json) != SQLITE_OK) goto done;
    if (sqlite3_prepare_v2(json,
        "SELECT json_extract(?1,'$.twin_id'),json_extract(?1,'$.source_url'),"
        "json_extract(?1,'$.observed_at'),json_extract(?1,'$.content_sha256') "
        "WHERE json_valid(?1) AND json_extract(?1,'$.schema')='bonfyre-foreign-observation.v1' "
        "AND json_extract(?1,'$.observation_kind') IN ('source_observed','boundary_response') "
        "AND json_extract(?1,'$.policy_decision')='allow' "
        "AND length(json_extract(?1,'$.content_sha256'))=64",
        -1, &parse, NULL) != SQLITE_OK) goto done;
    while ((length = getline(&line, &capacity, in)) > 0) {
        const char *subject, *url, *at, *digest;
        BfForeignTwinObservation observation = {0};
        char receipt_digest[65], receipt_id[80];
        if (line[length - 1] == '\n') line[--length] = '\0';
        sqlite3_bind_text(parse, 1, line, (int)length, SQLITE_TRANSIENT);
        if (sqlite3_step(parse) == SQLITE_ROW) {
            subject = (const char *)sqlite3_column_text(parse, 0);
            url = (const char *)sqlite3_column_text(parse, 1);
            at = (const char *)sqlite3_column_text(parse, 2);
            digest = (const char *)sqlite3_column_text(parse, 3);
            if (subject && url && at && digest) {
                bf_sha256_hex((const uint8_t *)line, (size_t)length, receipt_digest);
                snprintf(receipt_id, sizeof(receipt_id), "foreign:%s", receipt_digest);
                observation.receipt_id = receipt_id;
                observation.subject = subject;
                observation.predicate = "retained_body";
                observation.source_url = url;
                observation.body_sha256 = digest;
                observation.observed_at = at;
                observation.observer = "ForeignTwin:provider_frontier_verifier";
                if (bf_foreign_twin_record(twin, &observation) != 0) goto done;
                (*accepted)++;
            }
        }
        sqlite3_reset(parse);
        sqlite3_clear_bindings(parse);
    }
    ok = 1;
done:
    free(line);
    if (parse) sqlite3_finalize(parse);
    if (json) sqlite3_close(json);
    if (in) fclose(in);
    return ok;
}

static int verify(int argc, char **argv)
{
    const char *provider_path = option_value(argc, argv, "--provider-db");
    const char *run_id = option_value(argc, argv, "--run-id");
    const char *query_id = option_value(argc, argv, "--query-id");
    const char *address_path = option_value(argc, argv, "--address-db");
    const char *receipts_path = option_value(argc, argv, "--foreign-evidence");
    const char *foreign_path = option_value(argc, argv, "--foreign-db");
    const char *out_path = option_value(argc, argv, "--out");
    sqlite3 *provider = NULL, *address = NULL;
    sqlite3_stmt *run = NULL, *query = NULL, *rank = NULL, *advertisement = NULL;
    BfForeignTwin *twin = NULL;
    FILE *out = NULL;
    char representation[256] = {0}, temporary[PATH_MAX] = {0};
    int expected = 0, seen = 0, observed = 0, receipt_count = 0, fd = -1, ok = 0;
    const char *stage = "arguments";
    if (!provider_path || !run_id || !query_id || !address_path || !receipts_path || !foreign_path || !out_path) goto done;
    stage = "open";
    if (sqlite3_open_v2(provider_path, &provider, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK ||
        sqlite3_open_v2(address_path, &address, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK ||
        bf_foreign_twin_open(foreign_path, &twin, stderr) != 0) goto done;
    stage = "complete_provider_run";
    if (sqlite3_prepare_v2(provider,
        "SELECT representation_id FROM provider_embedding_run WHERE run_id=?1 AND status='complete' "
        "AND requested_count>0 AND stored_count=requested_count AND failed_count=0 AND dimensions>0",
        -1, &run, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(run, 1, run_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(run) != SQLITE_ROW || !sqlite3_column_text(run, 0)) goto done;
    snprintf(representation, sizeof(representation), "%s", sqlite3_column_text(run, 0));
    sqlite3_finalize(run); run = NULL;
    stage = "provider_query";
    if (sqlite3_prepare_v2(provider,
        "SELECT top_k FROM provider_embedding_query WHERE query_id=?1 AND run_id=?2 AND candidate_count>0 AND top_k>0",
        -1, &query, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(query, 1, query_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(query, 2, run_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(query) != SQLITE_ROW) goto done;
    expected = sqlite3_column_int(query, 0);
    sqlite3_finalize(query); query = NULL;
    stage = "foreign_receipt_ingest";
    if (!ingest_foreign_receipts(twin, receipts_path, &receipt_count)) goto done;
    stage = "rank_and_address_prepare";
    if (sqlite3_prepare_v2(provider,
        "SELECT rank,subject_id,source_digest FROM provider_embedding_rank WHERE query_id=?1 ORDER BY rank",
        -1, &rank, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(address,
        "SELECT candidate_kind,owner_ref,exact_verifier,lowerer_ref FROM candidate_advertisement WHERE identity=?1",
        -1, &advertisement, NULL) != SQLITE_OK ||
        snprintf(temporary, sizeof(temporary), "%s.tmp.XXXXXX", out_path) >= (int)sizeof(temporary) ||
        (fd = mkstemp(temporary)) < 0 || (out = fdopen(fd, "w")) == NULL) goto done;
    fd = -1;
    sqlite3_bind_text(rank, 1, query_id, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(rank) == SQLITE_ROW) {
        int position = sqlite3_column_int(rank, 0);
        const char *identity = (const char *)sqlite3_column_text(rank, 1);
        const char *source_digest = (const char *)sqlite3_column_text(rank, 2);
        const char *kind, *owner, *verifier, *lowerer;
        int found;
        char receipt[128] = {0};
        stage = "provider_rank_row";
        if (!identity || !source_digest || position < 1) goto done;
        sqlite3_bind_text(advertisement, 1, identity, -1, SQLITE_TRANSIENT);
        stage = "address_advertisement";
        if (sqlite3_step(advertisement) != SQLITE_ROW) goto done;
        kind = (const char *)sqlite3_column_text(advertisement, 0);
        owner = (const char *)sqlite3_column_text(advertisement, 1);
        verifier = (const char *)sqlite3_column_text(advertisement, 2);
        lowerer = (const char *)sqlite3_column_text(advertisement, 3);
        if (!kind || !owner || !verifier || !lowerer || strcmp(kind, "external_occurrence") != 0 ||
            strcmp(verifier, "ForeignTwin:admission") != 0) goto done;
        stage = "foreign_exact_owner";
        found = bf_foreign_twin_observed(twin, identity, "retained_body");
        if (found < 0 || (found && bf_foreign_twin_latest_receipt(twin, identity, "retained_body", receipt, sizeof(receipt)) < 0)) goto done;
        if (found) observed++;
        if (fputs("{\"schema\":\"bonfyre-provider-exact-owner-evidence.v1\",\"run_id\":", out) < 0 ||
            !json_string(out, run_id) || fputs(",\"query_id\":", out) < 0 || !json_string(out, query_id) ||
            fputs(",\"representation_id\":", out) < 0 || !json_string(out, representation) ||
            fprintf(out, ",\"rank\":%d,\"candidate_identity\":", position) < 0 || !json_string(out, identity) ||
            fputs(",\"candidate_kind\":", out) < 0 || !json_string(out, kind) ||
            fputs(",\"owner_ref\":", out) < 0 || !json_string(out, owner) ||
            fputs(",\"exact_verifier_ref\":", out) < 0 || !json_string(out, verifier) ||
            fputs(",\"lowerer_ref\":", out) < 0 || !json_string(out, lowerer) ||
            fputs(",\"source_digest\":", out) < 0 || !json_string(out, source_digest) ||
            fputs(",\"query_state\":\"queried\",\"outcome\":", out) < 0 || !json_string(out, found ? "observed" : "unobserved") ||
            fputs(",\"receipt_id\":", out) < 0 || !nullable_string(out, found ? receipt : NULL) ||
            fputs(",\"truth\":\"exact_retained_observation_query_only_not_relevance_authority_selection_or_lowerer\"}\n", out) < 0) goto done;
        sqlite3_reset(advertisement);
        sqlite3_clear_bindings(advertisement);
        seen++;
    }
    stage = "complete_ranked_frontier";
    {
        int flush_result = fflush(out);
        int sync_result = flush_result == 0 ? fsync(fileno(out)) : -1;
        int close_result = fclose(out);
        int rename_result = close_result == 0 ? rename(temporary, out_path) : -1;
        out = NULL;
        if (seen != expected || seen == 0 || flush_result != 0 || sync_result != 0 || close_result != 0 || rename_result != 0) {
            fprintf(stderr, "bonfyre-provider-frontier: expected=%d seen=%d flush=%d sync=%d close=%d rename=%d errno=%d\n",
                    expected, seen, flush_result, sync_result, close_result, rename_result, errno);
            goto done;
        }
    }
    printf("{\"schema\":\"bonfyre-provider-frontier-verification.v1\",\"run_id\":");
    json_string(stdout, run_id);
    fputs(",\"query_id\":", stdout); json_string(stdout, query_id);
    fputs(",\"representation_id\":", stdout); json_string(stdout, representation);
    printf(",\"ranked_handles\":%d,\"exact_owner_queries\":%d,\"retained_observations\":%d,\"receipts_recorded_idempotently\":%d,\"output\":", seen, seen, observed, receipt_count);
    json_string(stdout, out_path);
    puts(",\"truth\":\"provider_rank_to_addressplane_to_exact_ForeignTwin_only; no lowerer handoff, selection, authority, evidence admission, or observation request\"}");
    ok = 1;
done:
    if (out) fclose(out);
    if (fd >= 0) close(fd);
    if (!ok) fprintf(stderr, "bonfyre-provider-frontier: verification refused at %s\n", stage);
    if (!ok && temporary[0]) unlink(temporary);
    if (advertisement) sqlite3_finalize(advertisement);
    if (rank) sqlite3_finalize(rank);
    if (query) sqlite3_finalize(query);
    if (run) sqlite3_finalize(run);
    if (twin) bf_foreign_twin_close(twin);
    if (address) sqlite3_close(address);
    if (provider) sqlite3_close(provider);
    return ok ? 0 : 1;
}

/* Build the strict material-manifest contract already consumed by
 * BonfyreContextCut.  This is deliberately a second phase after `verify`:
 * a provider rank is only allowed to contribute a body after every current
 * rank row rejoined its AddressPlane descriptor and the exact ForeignTwin
 * query ledger says that precise retained body exists.  The output remains a
 * diagnostic representation input; it is not a selection or lowerer handoff.
 */
static int material_manifest(int argc, char **argv)
{
    const char *provider_path = option_value(argc, argv, "--provider-db");
    const char *run_id = option_value(argc, argv, "--run-id");
    const char *query_id = option_value(argc, argv, "--query-id");
    const char *address_path = option_value(argc, argv, "--address-db");
    const char *receipts_path = option_value(argc, argv, "--foreign-evidence");
    const char *exact_path = option_value(argc, argv, "--exact-owner-evidence");
    const char *policy = option_value(argc, argv, "--information-policy-class");
    const char *out_path = option_value(argc, argv, "--out");
    sqlite3 *provider = NULL, *address = NULL, *memory = NULL;
    sqlite3_stmt *run = NULL, *query = NULL, *parse_exact = NULL, *parse_receipt = NULL, *rows = NULL, *advertisement = NULL;
    FILE *exact = NULL, *receipts = NULL, *out = NULL;
    char *line = NULL, *representation = NULL, temporary[PATH_MAX] = {0};
    size_t capacity = 0;
    ssize_t length;
    int fd = -1, expected = 0, ranked = 0, materialized = 0, ok = 0;
    const char *stage = "arguments";
    if (!provider_path || !run_id || !query_id || !address_path || !receipts_path || !exact_path || !policy || !*policy || !out_path) goto done;
    stage = "open";
    if (sqlite3_open_v2(provider_path, &provider, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK ||
        sqlite3_open_v2(address_path, &address, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK ||
        sqlite3_open(":memory:", &memory) != SQLITE_OK ||
        !(exact = fopen(exact_path, "r")) || !(receipts = fopen(receipts_path, "r"))) goto done;
    if (sqlite3_exec(memory,
        "CREATE TABLE exact_owner(rank INTEGER PRIMARY KEY,identity TEXT UNIQUE NOT NULL,source_digest TEXT NOT NULL,owner_ref TEXT NOT NULL,lowerer_ref TEXT NOT NULL,receipt_id TEXT,outcome TEXT NOT NULL CHECK(outcome IN ('observed','unobserved')),evidence_digest TEXT NOT NULL);"
        "CREATE TABLE receipt(receipt_id TEXT PRIMARY KEY,identity TEXT NOT NULL,observed_at TEXT NOT NULL,content_sha256 TEXT NOT NULL,body_path TEXT NOT NULL);",
        NULL, NULL, NULL) != SQLITE_OK) goto done;
    stage = "complete_provider_run";
    if (sqlite3_prepare_v2(provider,
        "SELECT representation_id FROM provider_embedding_run WHERE run_id=?1 AND status='complete' "
        "AND requested_count>0 AND stored_count=requested_count AND failed_count=0 AND dimensions>0",
        -1, &run, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(run, 1, run_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(run) != SQLITE_ROW || !sqlite3_column_text(run, 0)) goto done;
    representation = strdup((const char *)sqlite3_column_text(run, 0));
    if (!representation) goto done;
    sqlite3_finalize(run); run = NULL;
    stage = "provider_query";
    if (sqlite3_prepare_v2(provider,
        "SELECT top_k FROM provider_embedding_query WHERE query_id=?1 AND run_id=?2 AND candidate_count>0 AND top_k>0",
        -1, &query, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(query, 1, query_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(query, 2, run_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(query) != SQLITE_ROW) goto done;
    expected = sqlite3_column_int(query, 0);
    sqlite3_finalize(query); query = NULL;
    stage = "exact_owner_evidence_parse";
    if (sqlite3_prepare_v2(memory,
        "SELECT json_extract(?1,'$.rank'),json_extract(?1,'$.candidate_identity'),json_extract(?1,'$.source_digest'),json_extract(?1,'$.owner_ref'),json_extract(?1,'$.lowerer_ref'),json_extract(?1,'$.receipt_id'),json_extract(?1,'$.outcome') "
        "WHERE json_valid(?1) AND json_extract(?1,'$.schema')='bonfyre-provider-exact-owner-evidence.v1' "
        "AND json_extract(?1,'$.run_id')=?2 AND json_extract(?1,'$.query_id')=?3 AND json_extract(?1,'$.representation_id')=?4 "
        "AND json_extract(?1,'$.query_state')='queried' AND json_extract(?1,'$.outcome') IN ('observed','unobserved') "
        "AND json_extract(?1,'$.candidate_kind')='external_occurrence' "
        "AND json_extract(?1,'$.exact_verifier_ref')='ForeignTwin:admission' AND json_type(?1,'$.owner_ref')='text' AND json_type(?1,'$.lowerer_ref')='text' "
        "AND (json_type(?1,'$.receipt_id')='text' OR json_type(?1,'$.receipt_id')='null')", -1, &parse_exact, NULL) != SQLITE_OK) goto done;
    while ((length = getline(&line, &capacity, exact)) > 0) {
        const char *identity, *source, *owner_ref, *lowerer_ref, *receipt_id, *outcome;
        int rank;
        char digest[65];
        if (line[length - 1] == '\n') line[--length] = '\0';
        sqlite3_bind_text(parse_exact, 1, line, (int)length, SQLITE_TRANSIENT);
        sqlite3_bind_text(parse_exact, 2, run_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(parse_exact, 3, query_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(parse_exact, 4, representation, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(parse_exact) == SQLITE_ROW) {
            rank = sqlite3_column_int(parse_exact, 0);
            identity = (const char *)sqlite3_column_text(parse_exact, 1);
            source = (const char *)sqlite3_column_text(parse_exact, 2);
            owner_ref = (const char *)sqlite3_column_text(parse_exact, 3);
            lowerer_ref = (const char *)sqlite3_column_text(parse_exact, 4);
            receipt_id = (const char *)sqlite3_column_text(parse_exact, 5);
            outcome = (const char *)sqlite3_column_text(parse_exact, 6);
            if (!identity || !source || !owner_ref || !lowerer_ref || !outcome || rank < 1 || (strcmp(outcome, "observed") && strcmp(outcome, "unobserved")) ||
                (!strcmp(outcome, "observed") && !receipt_id) || (!strcmp(outcome, "unobserved") && receipt_id)) goto done;
            bf_sha256_hex((const uint8_t *)line, (size_t)length, digest);
            if (sqlite3_exec(memory, "BEGIN", NULL, NULL, NULL) != SQLITE_OK) goto done;
            {
                sqlite3_stmt *insert = NULL;
                int inserted = 0;
                if (sqlite3_prepare_v2(memory, "INSERT INTO exact_owner VALUES(?1,?2,?3,?4,?5,?6,?7,?8)", -1, &insert, NULL) == SQLITE_OK) {
                    sqlite3_bind_int(insert, 1, rank);
                    sqlite3_bind_text(insert, 2, identity, -1, SQLITE_TRANSIENT);
                    sqlite3_bind_text(insert, 3, source, -1, SQLITE_TRANSIENT);
                    sqlite3_bind_text(insert, 4, owner_ref, -1, SQLITE_TRANSIENT);
                    sqlite3_bind_text(insert, 5, lowerer_ref, -1, SQLITE_TRANSIENT);
                    if (receipt_id) sqlite3_bind_text(insert, 6, receipt_id, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(insert, 6);
                    sqlite3_bind_text(insert, 7, outcome, -1, SQLITE_TRANSIENT);
                    sqlite3_bind_text(insert, 8, digest, -1, SQLITE_TRANSIENT);
                    inserted = sqlite3_step(insert) == SQLITE_DONE;
                }
                if (insert) sqlite3_finalize(insert);
                if (!inserted || sqlite3_exec(memory, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) { sqlite3_exec(memory, "ROLLBACK", NULL, NULL, NULL); goto done; }
            }
        }
        sqlite3_reset(parse_exact); sqlite3_clear_bindings(parse_exact);
    }
    sqlite3_finalize(parse_exact); parse_exact = NULL;
    free(line); line = NULL; capacity = 0;
    stage = "foreign_receipt_parse";
    if (sqlite3_prepare_v2(memory,
        "SELECT json_extract(?1,'$.twin_id'),json_extract(?1,'$.observed_at'),json_extract(?1,'$.content_sha256'),json_extract(?1,'$.body_path') "
        "WHERE json_valid(?1) AND json_extract(?1,'$.schema')='bonfyre-foreign-observation.v1' "
        "AND json_extract(?1,'$.observation_kind') IN ('source_observed','boundary_response') "
        "AND json_extract(?1,'$.policy_decision')='allow' AND length(json_extract(?1,'$.content_sha256'))=64 "
        "AND json_type(?1,'$.body_path')='text'", -1, &parse_receipt, NULL) != SQLITE_OK) goto done;
    while ((length = getline(&line, &capacity, receipts)) > 0) {
        const char *identity, *at, *content, *body;
        char digest[65], receipt_id[80];
        if (line[length - 1] == '\n') line[--length] = '\0';
        sqlite3_bind_text(parse_receipt, 1, line, (int)length, SQLITE_TRANSIENT);
        if (sqlite3_step(parse_receipt) == SQLITE_ROW) {
            identity = (const char *)sqlite3_column_text(parse_receipt, 0);
            at = (const char *)sqlite3_column_text(parse_receipt, 1);
            content = (const char *)sqlite3_column_text(parse_receipt, 2);
            body = (const char *)sqlite3_column_text(parse_receipt, 3);
            if (!identity || !at || !content || !body) goto done;
            bf_sha256_hex((const uint8_t *)line, (size_t)length, digest);
            snprintf(receipt_id, sizeof(receipt_id), "foreign:%s", digest);
            {
                sqlite3_stmt *insert = NULL;
                int inserted = 0;
                if (sqlite3_prepare_v2(memory, "INSERT OR REPLACE INTO receipt VALUES(?1,?2,?3,?4,?5)", -1, &insert, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(insert, 1, receipt_id, -1, SQLITE_TRANSIENT);
                    sqlite3_bind_text(insert, 2, identity, -1, SQLITE_TRANSIENT);
                    sqlite3_bind_text(insert, 3, at, -1, SQLITE_TRANSIENT);
                    sqlite3_bind_text(insert, 4, content, -1, SQLITE_TRANSIENT);
                    sqlite3_bind_text(insert, 5, body, -1, SQLITE_TRANSIENT);
                    inserted = sqlite3_step(insert) == SQLITE_DONE;
                }
                if (insert) sqlite3_finalize(insert);
                if (!inserted) goto done;
            }
        }
        sqlite3_reset(parse_receipt); sqlite3_clear_bindings(parse_receipt);
    }
    sqlite3_finalize(parse_receipt); parse_receipt = NULL;
    free(line); line = NULL; capacity = 0;
    stage = "rank_exact_owner_join";
    if (sqlite3_prepare_v2(provider,
        "SELECT r.rank,r.subject_id,r.source_digest FROM provider_embedding_rank r WHERE r.query_id=?1 ORDER BY r.rank",
        -1, &rows, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(address,
        "SELECT candidate_kind,owner_ref,exact_verifier,lowerer_ref FROM candidate_advertisement WHERE identity=?1",
        -1, &advertisement, NULL) != SQLITE_OK ||
        snprintf(temporary, sizeof(temporary), "%s.tmp.XXXXXX", out_path) >= (int)sizeof(temporary) ||
        (fd = mkstemp(temporary)) < 0 || !(out = fdopen(fd, "w"))) goto done;
    fd = -1;
    sqlite3_bind_text(rows, 1, query_id, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(rows) == SQLITE_ROW) {
        int rank = sqlite3_column_int(rows, 0);
        const char *identity = (const char *)sqlite3_column_text(rows, 1);
        const char *source = (const char *)sqlite3_column_text(rows, 2);
        sqlite3_stmt *joined = NULL;
        const char *e_identity, *e_source, *e_owner, *e_lowerer, *receipt_id, *outcome, *evidence_digest, *kind, *owner, *verifier, *lowerer, *r_identity, *observed_at, *content, *body;
        char actual[65];
        if (!identity || !source || rank != ranked + 1) goto done;
        if (sqlite3_prepare_v2(memory,
            "SELECT e.identity,e.source_digest,e.owner_ref,e.lowerer_ref,e.receipt_id,e.outcome,e.evidence_digest,r.identity,r.observed_at,r.content_sha256,r.body_path FROM exact_owner e "
            "LEFT JOIN receipt r ON r.receipt_id=e.receipt_id WHERE e.rank=?1",
            -1, &joined, NULL) != SQLITE_OK) goto done;
        sqlite3_bind_int(joined, 1, rank);
        if (sqlite3_step(joined) != SQLITE_ROW) { sqlite3_finalize(joined); goto done; }
        e_identity = (const char *)sqlite3_column_text(joined, 0);
        e_source = (const char *)sqlite3_column_text(joined, 1);
        e_owner = (const char *)sqlite3_column_text(joined, 2);
        e_lowerer = (const char *)sqlite3_column_text(joined, 3);
        receipt_id = (const char *)sqlite3_column_text(joined, 4);
        outcome = (const char *)sqlite3_column_text(joined, 5);
        evidence_digest = (const char *)sqlite3_column_text(joined, 6);
        r_identity = (const char *)sqlite3_column_text(joined, 7);
        observed_at = (const char *)sqlite3_column_text(joined, 8);
        content = (const char *)sqlite3_column_text(joined, 9);
        body = (const char *)sqlite3_column_text(joined, 10);
        if (!e_identity || !e_source || !e_owner || !e_lowerer || !outcome || !evidence_digest || strcmp(identity, e_identity) || strcmp(source, e_source)) { sqlite3_finalize(joined); goto done; }
        sqlite3_bind_text(advertisement, 1, identity, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(advertisement) != SQLITE_ROW) goto done;
        kind = (const char *)sqlite3_column_text(advertisement, 0);
        owner = (const char *)sqlite3_column_text(advertisement, 1);
        verifier = (const char *)sqlite3_column_text(advertisement, 2);
        lowerer = (const char *)sqlite3_column_text(advertisement, 3);
        if (!kind || !owner || !verifier || !lowerer || strcmp(kind, "external_occurrence") || strcmp(verifier, "ForeignTwin:admission") || strcmp(owner, e_owner) || strcmp(lowerer, e_lowerer)) { sqlite3_finalize(joined); goto done; }
        ranked++;
        if (!strcmp(outcome, "unobserved")) { sqlite3_finalize(joined); sqlite3_reset(advertisement); sqlite3_clear_bindings(advertisement); continue; }
        if (strcmp(outcome, "observed") || !receipt_id || !r_identity || !observed_at || !content || !body || strcmp(identity, r_identity) ||
            bf_sha256_file(body, actual) != 0 || strcmp(actual, content)) { sqlite3_finalize(joined); goto done; }
        if (fputs("{\"candidate_identity\":", out) < 0 || !json_string(out, identity) ||
            fputs(",\"body_path\":", out) < 0 || !json_string(out, body) ||
            fputs(",\"content_sha256\":\"sha256:", out) < 0 || fputs(content, out) < 0 ||
            fputs("\",\"receipt_digest\":", out) < 0 || !json_string(out, receipt_id) ||
            fputs(",\"evidence_ref\":\"sha256:", out) < 0 || fputs(evidence_digest, out) < 0 ||
            fputs("\",\"information_policy_class\":", out) < 0 || !json_string(out, policy) ||
            fputs(",\"temporal_frontier\":", out) < 0 || !json_string(out, observed_at) ||
            fputs("}\n", out) < 0) { sqlite3_finalize(joined); goto done; }
        sqlite3_finalize(joined); joined = NULL;
        sqlite3_reset(advertisement); sqlite3_clear_bindings(advertisement);
        materialized++;
    }
    if (ranked != expected || materialized == 0 || fflush(out) != 0 || fsync(fileno(out)) != 0 || fclose(out) != 0 || rename(temporary, out_path) != 0) { out = NULL; goto done; }
    out = NULL;
    printf("{\"schema\":\"bonfyre-provider-context-material-manifest.v1\",\"run_id\":");
    json_string(stdout, run_id); fputs(",\"query_id\":", stdout); json_string(stdout, query_id);
    fputs(",\"representation_id\":", stdout); json_string(stdout, representation);
    printf(",\"ranked_handles\":%d,\"exactly_retained_materials\":%d,\"output\":", ranked, materialized);
    json_string(stdout, out_path);
    puts(",\"truth\":\"receipt_bound_context_material_only_not_selection_authority_evidence_admission_lowerer_or_model_execution\"}");
    ok = 1;
done:
    free(line);
    if (out) fclose(out);
    if (fd >= 0) close(fd);
    if (!ok && temporary[0]) unlink(temporary);
    if (advertisement) sqlite3_finalize(advertisement);
    if (rows) sqlite3_finalize(rows);
    if (parse_receipt) sqlite3_finalize(parse_receipt);
    if (parse_exact) sqlite3_finalize(parse_exact);
    if (query) sqlite3_finalize(query);
    if (run) sqlite3_finalize(run);
    if (receipts) fclose(receipts);
    if (exact) fclose(exact);
    if (memory) sqlite3_close(memory);
    if (address) sqlite3_close(address);
    if (provider) sqlite3_close(provider);
    free(representation);
    if (!ok) fprintf(stderr, "bonfyre-provider-frontier: material manifest refused at %s\n", stage);
    return ok ? 0 : 1;
}

static void usage(void)
{
    fputs("usage: bonfyre-provider-frontier verify --provider-db FILE --run-id ID --query-id ID --address-db FILE --foreign-evidence JSONL --foreign-db FILE --out JSONL\n"
          "       bonfyre-provider-frontier material-manifest --provider-db FILE --run-id ID --query-id ID --address-db FILE --foreign-evidence JSONL --exact-owner-evidence JSONL --information-policy-class CLASS --out JSONL\n", stderr);
}

int main(int argc, char **argv)
{
    if (argc < 2) { usage(); return 2; }
    enum { PF_MATERIAL_MANIFEST, PF_VERIFY };
    static const BfCommandEntry frontier_commands[] = {
        {"material-manifest", PF_MATERIAL_MANIFEST, 0, NULL},
        {"verify", PF_VERIFY, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(frontier_commands,
        sizeof(frontier_commands) / sizeof(frontier_commands[0]), argv[1]);
    if (command && command->id == PF_VERIFY) return verify(argc, argv);
    if (command && command->id == PF_MATERIAL_MANIFEST) return material_manifest(argc, argv);
    usage(); return 2;
}
