#define _POSIX_C_SOURCE 200809L

#include "bf_control.h"
#include "bf_command_lookup.h"
#include <bonfyre.h>

#include <ctype.h>
#include <curl/curl.h>
#include <errno.h>
#include <fcntl.h>
#include <float.h>
#include <math.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "bf_lex.h"
#include "../../../lib/libbonfyre/include/bf_help.h"

/* bf_control.h predates the grant ledger and carries the same verdict/policy
 * value types.  It cannot include bf_admission.h yet because both historical
 * headers declare those types.  Keep the newer request/decision ABI local
 * rather than selecting the old in-memory-only control calculation. */
typedef struct BfAdmissionGrants BfAdmissionGrants;
typedef struct BfResourceRequest {
    const char *plane, *kind;
    int64_t estimated_bytes;
    const char *volume, *detail;
} BfResourceRequest;
typedef struct BfNativeAdmissionDecision {
    BfAdmissionVerdict verdict;
    char reason[256];
    int64_t free_bytes, committed_bytes, spendable_bytes;
} BfNativeAdmissionDecision;
extern void bf_admission_policy_defaults(BfAdmissionPolicy *policy);
extern int64_t bf_admission_disk_free(const char *path);
extern int bf_admission_open(const char *database_path, BfAdmissionGrants **out, FILE *err);
extern void bf_admission_close(BfAdmissionGrants *grants);
extern BfAdmissionVerdict bf_admission_request_grant(BfAdmissionGrants *grants,
    const BfResourceRequest *request, const BfAdmissionPolicy *policy,
    int64_t free_bytes, const char *now, BfNativeAdmissionDecision *out,
    int64_t *out_grant_id);
extern int bf_admission_release_grant(BfAdmissionGrants *grants, int64_t grant_id,
                                      const char *now);
static int64_t days_from_civil(int64_t year, int64_t month, int64_t day)
{
    int64_t era;
    int64_t year_of_era;
    int64_t day_of_year;
    int64_t day_of_era;
    year -= month <= 2;
    era = (year >= 0 ? year : year - 399) / 400;
    year_of_era = year - era * 400;
    day_of_year = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
    day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    return era * 146097 + day_of_era - 719468;
}

static int parse_iso_ms(const char *text, int64_t *out)
{
    int year, month, day, hour, minute, second;
    char separator;
    if (text == NULL || sscanf(text, "%4d-%2d-%2d%c%2d:%2d:%2d",
                               &year, &month, &day, &separator, &hour, &minute, &second) != 7 ||
        (separator != 'T' && separator != ' ') || month < 1 || month > 12 ||
        day < 1 || day > 31 || hour < 0 || hour > 23 || minute < 0 || minute > 59 ||
        second < 0 || second > 59) return -1;
    *out = ((days_from_civil(year, month, day) * 24 + hour) * 60 + minute) * 60000LL +
           (int64_t)second * 1000LL;
    return 0;
}

static void now_iso(char out[40])
{
    time_t current = time(NULL);
    struct tm utc;
    gmtime_r(&current, &utc);
    strftime(out, 40, "%Y-%m-%dT%H:%M:%S+00:00", &utc);
}

static BfProviderEventKind event_kind(const char *name, int *ok)
{
    *ok = 1;
    if (strcmp(name, "success") == 0) return BF_PROVIDER_SUCCESS;
    if (strcmp(name, "transient_failure") == 0) return BF_PROVIDER_TRANSIENT_FAILURE;
    if (strcmp(name, "hard_capacity") == 0) return BF_PROVIDER_HARD_CAPACITY;
    if (strcmp(name, "manual_pause") == 0) return BF_PROVIDER_MANUAL_PAUSE;
    if (strcmp(name, "manual_resume") == 0) return BF_PROVIDER_MANUAL_RESUME;
    *ok = 0;
    return BF_PROVIDER_SUCCESS;
}

static const char *event_name(BfProviderEventKind kind)
{
    switch (kind) {
    case BF_PROVIDER_SUCCESS: return "success";
    case BF_PROVIDER_TRANSIENT_FAILURE: return "transient_failure";
    case BF_PROVIDER_HARD_CAPACITY: return "hard_capacity";
    case BF_PROVIDER_MANUAL_PAUSE: return "manual_pause";
    case BF_PROVIDER_MANUAL_RESUME: return "manual_resume";
    }
    return "";
}

/* Delegates to the one implementation. Three byte-identical copies of this
 * existed; they agreed, which was luck rather than design. */
static const char *option_value(int argc, char **argv, const char *option)
{
    return bf_lex_argv_option(argc, argv, option);
}

static int atom(const char *value, size_t limit)
{
    size_t index;
    if (value == NULL || value[0] == '\0' || strlen(value) > limit) return 0;
    for (index = 0; value[index] != '\0'; index++) {
        unsigned char c = (unsigned char)value[index];
        if (!(isalnum(c) || c == ':' || c == '_' || c == '.' || c == '-' || c == '/')) return 0;
    }
    return 1;
}

static int hex_digest(const char *value)
{
    size_t index;
    if (value == NULL || strlen(value) != 64) return 0;
    for (index = 0; index < 64; index++) if (!isxdigit((unsigned char)value[index])) return 0;
    return 1;
}

/* Delegates to the one implementation. Nineteen copies of this shape existed
 * with sixteen different opinions about escaping.
 */
static void json_string(const char *value)
{
    bf_lex_json_emit(stdout, value);
}

/* ProviderState owns the completion edge.  The outbox names only that exact
 * owner coordinate; AddressPlane later performs dependency forwarding and the
 * ProviderCompiler still reopens this row before retaining a derived artifact. */
static int queue_embedding_delta(sqlite3 *db, const char *run_id)
{
    sqlite3_stmt *statement = NULL;
    char delta_id[256], write_ref[256], now[40];
    int ok = 0;
    if (!atom(run_id, 160) ||
        snprintf(delta_id, sizeof(delta_id), "provider-embedding:%s", run_id) >= (int)sizeof(delta_id) ||
        snprintf(write_ref, sizeof(write_ref), "ProviderEmbeddingRun:%s", run_id) >= (int)sizeof(write_ref)) return 0;
    now_iso(now);
    if (sqlite3_prepare_v2(db,
        "INSERT OR IGNORE INTO provider_fact_delta(delta_id,run_id,write_ref,state,queued_at) "
        "SELECT ?1,run_id,?2,'queued',?3 FROM provider_embedding_run "
        "WHERE run_id=?4 AND status='complete' AND requested_count>0 "
        "AND stored_count=requested_count AND dimensions>0",
        -1, &statement, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(statement, 1, delta_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, write_ref, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, now, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 4, run_id, -1, SQLITE_TRANSIENT);
    ok = sqlite3_step(statement) == SQLITE_DONE;
    sqlite3_finalize(statement);
    return ok;
}

static void delta_path_atom(const char *input, char *output, size_t capacity)
{
    size_t out = 0;
    for (size_t in = 0; input != NULL && input[in] != '\0' && out + 1 < capacity; in++) {
        unsigned char c = (unsigned char)input[in];
        output[out++] = isalnum(c) || c == '-' || c == '_' ? (char)c : '-';
    }
    if (out == 0 && capacity > 1) output[out++] = 'x';
    output[out] = '\0';
}

static int emit_queued_embedding_deltas(sqlite3 *db, const char *directory)
{
    sqlite3_stmt *rows = NULL, *mark = NULL;
    int emitted = 0;
    if (directory == NULL || !*directory || (mkdir(directory, 0750) != 0 && errno != EEXIST)) return -1;
    if (sqlite3_prepare_v2(db,
        "SELECT delta_id,write_ref FROM provider_fact_delta WHERE state='queued' ORDER BY queued_at,delta_id",
        -1, &rows, NULL) != SQLITE_OK) return -1;
    while (sqlite3_step(rows) == SQLITE_ROW) {
        const char *delta = (const char *)sqlite3_column_text(rows, 0);
        const char *write = (const char *)sqlite3_column_text(rows, 1);
        char atomized[320], path[1024], temporary[1024], now[40];
        int fd = -1, directory_fd = -1, written = 0;
        FILE *file = NULL;
        if (!delta || !write) { emitted = -1; break; }
        delta_path_atom(delta, atomized, sizeof(atomized));
        if (snprintf(path, sizeof(path), "%s/%s.json", directory, atomized) >= (int)sizeof(path) ||
            snprintf(temporary, sizeof(temporary), "%s/.provider-delta-XXXXXX", directory) >= (int)sizeof(temporary)) { emitted = -1; break; }
        if (access(path, F_OK) == 0) written = 1;
        else if ((fd = mkstemp(temporary)) >= 0 && (file = fdopen(fd, "w")) != NULL) {
            fd = -1;
            if (fprintf(file, "{\"schema\":\"bonfyre-fact-delta.v1\",\"delta_id\":") >= 0) {
                bf_lex_json_emit(file, delta);
                if (fprintf(file, ",\"writes\":[") >= 0) {
                    bf_lex_json_emit(file, write);
                    if (fputs("],\"truth\":\"provider completion changed a latent representation availability; dependency forwarding only\"}\n", file) != EOF &&
                        fflush(file) == 0 && fsync(fileno(file)) == 0) {
                        int close_ok = fclose(file) == 0;
                        file = NULL;
                        if (close_ok && rename(temporary, path) == 0) {
                            directory_fd = open(directory, O_RDONLY);
                            written = directory_fd >= 0 && fsync(directory_fd) == 0;
                        }
                    }
                }
            }
            if (directory_fd >= 0) close(directory_fd);
            if (!written) { if (file != NULL) fclose(file); unlink(temporary); }
        }
        if (fd >= 0) close(fd);
        if (!written) { emitted = -1; break; }
        now_iso(now);
        if (sqlite3_prepare_v2(db, "UPDATE provider_fact_delta SET state='dispatched',dispatched_at=?1 WHERE delta_id=?2 AND state='queued'", -1, &mark, NULL) != SQLITE_OK) { emitted = -1; break; }
        sqlite3_bind_text(mark, 1, now, -1, SQLITE_TRANSIENT); sqlite3_bind_text(mark, 2, delta, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(mark) != SQLITE_DONE || sqlite3_changes(db) != 1) emitted = -1;
        sqlite3_finalize(mark); mark = NULL;
        if (emitted < 0) break;
        emitted++;
    }
    if (mark) sqlite3_finalize(mark);
    sqlite3_finalize(rows);
    return emitted;
}

static int reconcile_embedding_deltas(sqlite3 *db)
{
    sqlite3_stmt *runs = NULL;
    int queued = 0;
    if (sqlite3_prepare_v2(db,
        "SELECT run_id FROM provider_embedding_run WHERE status='complete' AND requested_count>0 "
        "AND stored_count=requested_count AND dimensions>0 ORDER BY completed_at,run_id",
        -1, &runs, NULL) != SQLITE_OK) return -1;
    while (sqlite3_step(runs) == SQLITE_ROW) {
        const char *run_id = (const char *)sqlite3_column_text(runs, 0);
        if (!run_id || !queue_embedding_delta(db, run_id)) { queued = -1; break; }
        queued += sqlite3_changes(db) > 0;
    }
    sqlite3_finalize(runs);
    return queued;
}

static int apply_schema(sqlite3 *db)
{
    return sqlite3_exec(db,
        "PRAGMA journal_mode=WAL;"
        "CREATE TABLE IF NOT EXISTS provider_observations("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "provider TEXT NOT NULL,event_kind TEXT NOT NULL,observed_at TEXT NOT NULL,"
        "attempt_id TEXT,reset_at TEXT,confidence REAL NOT NULL DEFAULT 1.0,detail TEXT"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_provider_observations_provider "
        "ON provider_observations(provider,id);"
        "CREATE TABLE IF NOT EXISTS provider_materializations("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "provider TEXT NOT NULL,artifact_sid TEXT NOT NULL,representation_digest TEXT NOT NULL,"
        "external_ref TEXT NOT NULL,kind TEXT NOT NULL,purpose TEXT NOT NULL,scope TEXT NOT NULL,"
        "capabilities TEXT NOT NULL DEFAULT '',authority_ref TEXT NOT NULL,created_at TEXT NOT NULL,"
        "expires_at TEXT,released_at TEXT,release_reason TEXT,"
        "UNIQUE(provider,scope,external_ref)"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_provider_materializations_active "
        "ON provider_materializations(provider,artifact_sid,representation_digest,released_at,expires_at);"
        /* A body is reusable only when its physical bytes still read back as
         * the receipt-bound digest.  This table deliberately has no semantic
         * owner, authority, or provider handle: it is an availability
         * projection below those distinct owners.  A different body requires
         * a different derivation identity; it cannot replace a current body
         * under the same derivation by accident. */
        "CREATE TABLE IF NOT EXISTS body_availability("
        "derivation_key TEXT NOT NULL,representation_ref TEXT NOT NULL,"
        "body_digest TEXT NOT NULL,location TEXT NOT NULL,receipt_ref TEXT NOT NULL,"
        "validity TEXT NOT NULL CHECK(validity IN ('CURRENT','STALE')),"
        "observed_at TEXT NOT NULL,invalidated_at TEXT,invalidated_by TEXT,"
        "PRIMARY KEY(derivation_key,representation_ref)"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_body_availability_current "
        "ON body_availability(derivation_key,representation_ref,validity);"
        /* A provider embedding is a derived latent body.  It intentionally
         * has no subject-owner, evidence, authority, or ranking-selection
         * column: this store can prove what was physically returned, not what
         * it means.  The model passport and exact owner remain elsewhere. */
        "CREATE TABLE IF NOT EXISTS provider_embedding_run("
        "run_id TEXT PRIMARY KEY,provider TEXT NOT NULL,model TEXT NOT NULL,"
        "representation_id TEXT NOT NULL,input_digest TEXT NOT NULL,"
        "requested_count INTEGER NOT NULL,stored_count INTEGER NOT NULL DEFAULT 0,"
        "failed_count INTEGER NOT NULL DEFAULT 0,dimensions INTEGER NOT NULL DEFAULT 0,"
        "response_bytes INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL,"
        "started_at TEXT NOT NULL,completed_at TEXT,detail TEXT NOT NULL DEFAULT ''"
        ");"
        "CREATE TABLE IF NOT EXISTS provider_embedding_projection("
        "run_id TEXT NOT NULL REFERENCES provider_embedding_run(run_id),"
        "subject_id TEXT NOT NULL,source_digest TEXT NOT NULL,vector_digest TEXT NOT NULL,"
        "dimensions INTEGER NOT NULL,vector BLOB NOT NULL,"
        "PRIMARY KEY(run_id,subject_id)"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_provider_embedding_projection_subject "
        "ON provider_embedding_projection(subject_id,run_id);"
        /* Query text and its vector are deliberately not retained here.  A
         * rank receipt persists only bounded physical coordinates and ranked
         * handles, so this plane cannot quietly become a second owner of
         * caller context or semantic selection. */
        "CREATE TABLE IF NOT EXISTS provider_embedding_query("
        "query_id TEXT PRIMARY KEY,run_id TEXT NOT NULL REFERENCES provider_embedding_run(run_id),"
        "query_digest TEXT NOT NULL,vector_digest TEXT NOT NULL,dimensions INTEGER NOT NULL,"
        "candidate_count INTEGER NOT NULL,top_k INTEGER NOT NULL,response_bytes INTEGER NOT NULL,"
        "observed_at TEXT NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS provider_embedding_rank("
        "query_id TEXT NOT NULL REFERENCES provider_embedding_query(query_id),"
        "rank INTEGER NOT NULL,subject_id TEXT NOT NULL,source_digest TEXT NOT NULL,score REAL NOT NULL,"
        "PRIMARY KEY(query_id,rank)"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_provider_embedding_rank_subject "
        "ON provider_embedding_rank(subject_id,query_id);"
        /* Completion is owner state.  The outbox preserves that fact until
         * the shared AddressPlane delta directory has it durably; neither
         * vectors nor a semantic result are carried across this boundary. */
        "CREATE TABLE IF NOT EXISTS provider_fact_delta("
        "delta_id TEXT PRIMARY KEY,run_id TEXT NOT NULL UNIQUE REFERENCES provider_embedding_run(run_id),"
        "write_ref TEXT NOT NULL UNIQUE,state TEXT NOT NULL CHECK(state IN ('queued','dispatched')),"
        "queued_at TEXT NOT NULL,dispatched_at TEXT);",
        NULL, NULL, NULL) == SQLITE_OK ? 0 : -1;
}

typedef struct { char *data; size_t length; size_t capacity; } HttpBuffer;
typedef struct { char *id; char *text; char digest[65]; } EmbedInput;

static size_t http_collect(char *ptr, size_t size, size_t count, void *opaque)
{
    HttpBuffer *buffer = opaque;
    size_t bytes = size * count;
    size_t required = buffer->length + bytes + 1;
    if (required > buffer->capacity) {
        size_t next = buffer->capacity ? buffer->capacity : 4096;
        char *grown;
        while (next < required) next *= 2;
        grown = realloc(buffer->data, next);
        if (grown == NULL) return 0;
        buffer->data = grown;
        buffer->capacity = next;
    }
    memcpy(buffer->data + buffer->length, ptr, bytes);
    buffer->length += bytes;
    buffer->data[buffer->length] = '\0';
    return bytes;
}

static int json_append(HttpBuffer *buffer, const char *value)
{
    const unsigned char *cursor = (const unsigned char *)(value ? value : "");
    if (http_collect("\"", 1, 1, buffer) != 1) return 0;
    for (; *cursor; cursor++) {
        char escaped[7]; const char *piece = NULL; size_t length = 0;
        if (*cursor == '\"') piece = "\\\"";
        else if (*cursor == '\\') piece = "\\\\";
        else if (*cursor == '\n') piece = "\\n";
        else if (*cursor == '\r') piece = "\\r";
        else if (*cursor == '\t') piece = "\\t";
        if (piece) length = strlen(piece);
        else if (*cursor < 0x20) {
            snprintf(escaped, sizeof(escaped), "\\u%04x", *cursor);
            piece = escaped; length = 6;
        } else { escaped[0] = (char)*cursor; escaped[1] = '\0'; piece = escaped; length = 1; }
        if (http_collect((char *)piece, 1, length, buffer) != length) return 0;
    }
    return http_collect("\"", 1, 1, buffer) == 1;
}

static void embed_input_free(EmbedInput *input)
{
    if (!input) return;
    free(input->id); free(input->text); memset(input, 0, sizeof(*input));
}

static int parse_input_json(sqlite3 *json, const char *line, EmbedInput *out)
{
    sqlite3_stmt *statement = NULL;
    const char *id, *text;
    int ok = 0;
    if (!json || !line || !out ||
        sqlite3_prepare_v2(json,
            "SELECT json_extract(?1,'$.id'),json_extract(?1,'$.text') "
            "WHERE json_valid(?1) AND json_type(?1,'$.id') IN ('text','integer') "
            "AND json_type(?1,'$.text')='text'", -1, &statement, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(statement, 1, line, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) == SQLITE_ROW &&
        (id = (const char *)sqlite3_column_text(statement, 0)) != NULL &&
        (text = (const char *)sqlite3_column_text(statement, 1)) != NULL &&
        atom(id, 240)) {
        out->id = strdup(id); out->text = strdup(text);
        if (out->id && out->text) {
            bf_sha256_hex((const uint8_t *)out->text, strlen(out->text), out->digest);
            ok = 1;
        }
    }
    sqlite3_finalize(statement);
    if (!ok) embed_input_free(out);
    return ok;
}

static int parse_embedding_vector(const char *json, float **vector, int *dimensions)
{
    char *cursor = (char *)(json ? json : "");
    char *end = NULL;
    float *out = NULL;
    int count = 0, capacity = 0;
    if (!json || !vector || !dimensions) return 0;
    while (*cursor) {
        float value;
        while (*cursor && !((*cursor >= '0' && *cursor <= '9') || *cursor == '-' || *cursor == '+')) cursor++;
        if (!*cursor) break;
        value = strtof(cursor, &end);
        if (end == cursor || value != value || value > FLT_MAX || value < -FLT_MAX) { free(out); return 0; }
        if (count == capacity) {
            int next = capacity ? capacity * 2 : 1024;
            float *grown = realloc(out, (size_t)next * sizeof(*out));
            if (!grown) { free(out); return 0; }
            out = grown; capacity = next;
        }
        out[count++] = value; cursor = end;
    }
    if (count < 1 || (*dimensions && count != *dimensions)) { free(out); return 0; }
    *dimensions = count; *vector = out;
    return 1;
}

static int provider_embeddings(const char *endpoint, const char *api_key,
                               const char *model, EmbedInput *inputs, int count,
                               float ***vectors, int *dimensions, size_t *response_bytes,
                               long *response_status)
{
    CURL *curl = NULL; struct curl_slist *headers = NULL;
    HttpBuffer request = {0}, response = {0}; char auth[4096]; char url[1024];
    sqlite3 *json = NULL; sqlite3_stmt *rows = NULL; float **out = NULL;
    int received = 0, ok = 0; CURLcode result;
    if (response_status) *response_status = 0;
    if (!endpoint || !api_key || !*api_key || !model || !inputs || count < 1 ||
        !vectors || !dimensions || snprintf(url, sizeof(url), "%s/v1/embeddings", endpoint) >= (int)sizeof(url) ||
        snprintf(auth, sizeof(auth), "Authorization: Bearer %s", api_key) >= (int)sizeof(auth)) goto done;
    if (http_collect("{\"model\":", 1, 9, &request) != 9 || !json_append(&request, model) ||
        http_collect(",\"input\":[", 1, 10, &request) != 10) goto done;
    for (int index = 0; index < count; index++) {
        if ((index && http_collect(",", 1, 1, &request) != 1) || !json_append(&request, inputs[index].text)) goto done;
    }
    if (http_collect("]}", 1, 2, &request) != 2) goto done;
    if (!(curl = curl_easy_init())) goto done;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, auth);
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request.data);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)request.length);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, http_collect);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 5000L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 120000L);
    curl_easy_setopt(curl, CURLOPT_PROTOCOLS_STR, "https");
    result = curl_easy_perform(curl);
    if (result != CURLE_OK) goto done;
    { long code = 0; curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
      if (response_status) *response_status = code; if (code != 200) goto done; }
    if (sqlite3_open(":memory:", &json) != SQLITE_OK ||
        sqlite3_prepare_v2(json,
            "SELECT json_extract(value,'$.index'),json_extract(value,'$.embedding') "
            "FROM json_each(?1,'$.data') ORDER BY json_extract(value,'$.index')", -1, &rows, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(rows, 1, response.data, (int)response.length, SQLITE_TRANSIENT);
    out = calloc((size_t)count, sizeof(*out)); if (!out) goto done;
    while (sqlite3_step(rows) == SQLITE_ROW) {
        int index = sqlite3_column_int(rows, 0);
        const char *body = (const char *)sqlite3_column_text(rows, 1);
        if (index < 0 || index >= count || !body || out[index] ||
            !parse_embedding_vector(body, &out[index], dimensions)) goto done;
        received++;
    }
    if (received != count) goto done;
    *vectors = out; out = NULL; *response_bytes = response.length; ok = 1;
done:
    if (rows) sqlite3_finalize(rows); if (json) sqlite3_close(json);
    if (curl) curl_easy_cleanup(curl); if (headers) curl_slist_free_all(headers);
    if (out) { for (int i = 0; i < count; i++) free(out[i]); free(out); }
    free(request.data); free(response.data);
    return ok;
}

/* Provider capacity is an observation, not a reason to degrade geometry or
 * substitute a different model.  Retry only transient HTTP capacity/server
 * responses; every other response is a hard refusal for this exact route. */
static int provider_embeddings_with_retry(const char *endpoint, const char *api_key,
                                          const char *model, EmbedInput *inputs, int count,
                                          float ***vectors, int *dimensions,
                                          size_t *response_bytes, long *final_status)
{
    long status = 0;
    for (int attempt = 0; attempt < 5; attempt++) {
        if (provider_embeddings(endpoint, api_key, model, inputs, count, vectors,
                                dimensions, response_bytes, &status)) {
            if (final_status) *final_status = 200;
            return 1;
        }
        if (status != 429 && (status < 500 || status > 599)) break;
        /* 429 usually has a one-minute quota window.  The cumulative wait
         * makes a full corpus run demand-shaped rather than hammering the
         * supplier or silently losing its tail. */
        sleep((unsigned int)(10 * (attempt + 1)));
    }
    if (final_status) *final_status = status;
    return 0;
}

static int provider_embedding_admission(const char *path, const char *volume,
                                        const char *plane, const char *estimate,
                                        const char *protected_floor,
                                        BfAdmissionGrants **out, int64_t *grant_id)
{
    BfAdmissionPolicy policy; BfResourceRequest request; BfNativeAdmissionDecision decision;
    int64_t bytes, free_bytes; char *end = NULL;
    if (!path || !volume || !plane || !estimate || !out || !grant_id ||
        (bytes = strtoll(estimate, &end, 10)) <= 0 || !end || *end ||
        bf_admission_open(path, out, stderr) != 0) return 0;
    free_bytes = bf_admission_disk_free(volume);
    if (free_bytes < 0) { bf_admission_close(*out); *out = NULL; return 0; }
    bf_admission_policy_defaults(&policy);
    /* A bounded provider operation may use an explicitly declared local
     * floor. This is placement policy only: it cannot alter provider
     * authority, geometry, semantic selection, or any exact-owner gate. */
    if (protected_floor != NULL) {
        int64_t floor;
        errno = 0; end = NULL; floor = strtoll(protected_floor, &end, 10);
        if (errno != 0 || end == protected_floor || *end != '\0' || floor < -1) {
            bf_admission_close(*out); *out = NULL; return 0;
        }
        policy.protected_floor_bytes = floor;
    }
    memset(&request, 0, sizeof(request));
    request.plane = plane; request.kind = "provider_embedding"; request.estimated_bytes = bytes;
    request.volume = volume; request.detail = "provider_embedding_projection";
    if (bf_admission_request_grant(*out, &request, &policy, free_bytes, NULL,
                                   &decision, grant_id) != BF_ADMISSION_ADMIT) {
        fprintf(stderr, "provider embedding ResourceAdmission refused: %s\n", decision.reason);
        bf_admission_close(*out); *out = NULL; *grant_id = 0; return 0;
    }
    return 1;
}

static int embed_provider(sqlite3 *db, int argc, char **argv)
{
    const char *provider = option_value(argc, argv, "--provider");
    const char *endpoint = option_value(argc, argv, "--endpoint");
    const char *model = option_value(argc, argv, "--model");
    const char *key_name = option_value(argc, argv, "--api-key-env");
    const char *representation = option_value(argc, argv, "--representation-id");
    const char *input_path = option_value(argc, argv, "--input-jsonl");
    const char *admission_path = option_value(argc, argv, "--admission-db");
    const char *admission_volume = option_value(argc, argv, "--admission-volume");
    const char *admission_plane = option_value(argc, argv, "--admission-plane");
    const char *admission_estimate = option_value(argc, argv, "--admission-estimated-bytes");
    const char *admission_floor = option_value(argc, argv, "--admission-protected-floor-bytes");
    const char *batch_text = option_value(argc, argv, "--batch-size");
    const char *api_key; FILE *file = NULL; sqlite3 *json = NULL; sqlite3_stmt *insert = NULL;
    BfAdmissionGrants *grants = NULL; int64_t grant_id = 0; EmbedInput *batch = NULL;
    size_t line_cap = 0, response_total = 0; ssize_t line_length; char *line = NULL;
    char input_digest[65], started[40], completed[40], run_id[160]; int batch_size = 32;
    int requested = 0, stored = 0, failed = 0, dimensions = 0, batch_count = 0, ok = 0;
    if (!atom(provider, 96) || !endpoint || strncmp(endpoint, "https://", 8) || !atom(model, 160) ||
        !atom(key_name, 160) || !atom(representation, 160) || !input_path ||
        !admission_path || !admission_volume || !admission_plane || !admission_estimate) return -1;
    if (batch_text) { batch_size = atoi(batch_text); if (batch_size < 1 || batch_size > 128) return -1; }
    api_key = getenv(key_name); if (!api_key || !*api_key) { fprintf(stderr, "provider embedding credential reference is absent\n"); return -1; }
    if (bf_sha256_file(input_path, input_digest) != 0 || !(file = fopen(input_path, "r")) ||
        sqlite3_open(":memory:", &json) != SQLITE_OK ||
        !provider_embedding_admission(admission_path, admission_volume, admission_plane, admission_estimate, admission_floor, &grants, &grant_id)) goto done;
    now_iso(started); snprintf(run_id, sizeof(run_id), "embedding:%s:%ld", provider, (long)time(NULL));
    if (sqlite3_prepare_v2(db,
        "INSERT INTO provider_embedding_run(run_id,provider,model,representation_id,input_digest,requested_count,status,started_at) VALUES(?,?,?,?,?,?,?,?)",
        -1, &insert, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(insert, 1, run_id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(insert, 2, provider, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(insert, 3, model, -1, SQLITE_TRANSIENT); sqlite3_bind_text(insert, 4, representation, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(insert, 5, input_digest, -1, SQLITE_TRANSIENT); sqlite3_bind_int(insert, 6, 0);
    sqlite3_bind_text(insert, 7, "running", -1, SQLITE_STATIC); sqlite3_bind_text(insert, 8, started, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(insert) != SQLITE_DONE) goto done; sqlite3_finalize(insert); insert = NULL;
    batch = calloc((size_t)batch_size, sizeof(*batch)); if (!batch) goto done;
    while ((line_length = getline(&line, &line_cap, file)) >= 0) {
        EmbedInput item = {0};
        if (line_length && line[line_length - 1] == '\n') line[line_length - 1] = '\0';
        if (!*line) continue;
        if (!parse_input_json(json, line, &item)) { failed++; continue; }
        batch[batch_count++] = item; requested++;
        if (batch_count == batch_size) {
            float **vectors = NULL; size_t response_bytes = 0;
            { long status = 0;
              if (!provider_embeddings_with_retry(endpoint, api_key, model, batch, batch_count, &vectors, &dimensions, &response_bytes, &status)) {
                  failed += batch_count; fprintf(stderr, "provider embedding batch refused after bounded retry: http=%ld\n", status); break;
              } }
            if (sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) { for (int i=0;i<batch_count;i++)free(vectors[i]);free(vectors);goto done; }
            for (int i = 0; i < batch_count; i++) {
                char vector_digest[65]; sqlite3_stmt *projection = NULL;
                bf_sha256_hex((const uint8_t *)vectors[i], (size_t)dimensions * sizeof(float), vector_digest);
                if (sqlite3_prepare_v2(db, "INSERT INTO provider_embedding_projection VALUES(?,?,?,?,?,?)", -1, &projection, NULL) != SQLITE_OK) { sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL); for(int j=0;j<batch_count;j++)free(vectors[j]);free(vectors);goto done; }
                sqlite3_bind_text(projection,1,run_id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(projection,2,batch[i].id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(projection,3,batch[i].digest,-1,SQLITE_TRANSIENT);sqlite3_bind_text(projection,4,vector_digest,-1,SQLITE_TRANSIENT);sqlite3_bind_int(projection,5,dimensions);sqlite3_bind_blob(projection,6,vectors[i],dimensions*(int)sizeof(float),SQLITE_TRANSIENT);
                if(sqlite3_step(projection)!=SQLITE_DONE){sqlite3_finalize(projection);sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);for(int j=0;j<batch_count;j++)free(vectors[j]);free(vectors);goto done;}sqlite3_finalize(projection);stored++;
            }
            if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) { for(int i=0;i<batch_count;i++)free(vectors[i]);free(vectors);goto done; }
            response_total += response_bytes; for(int i=0;i<batch_count;i++){free(vectors[i]);embed_input_free(&batch[i]);} free(vectors); batch_count=0;
        }
    }
    if (batch_count && failed == 0) {
        float **vectors = NULL; size_t response_bytes = 0;
        long status = 0;
        int tail_ok = provider_embeddings_with_retry(endpoint, api_key, model, batch,
            batch_count, &vectors, &dimensions, &response_bytes, &status);
        if (!tail_ok) {
            failed += batch_count;
            fprintf(stderr, "provider embedding batch refused after bounded retry: http=%ld\n", status);
        } else {
            if (sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL)!=SQLITE_OK) {for(int i=0;i<batch_count;i++)free(vectors[i]);free(vectors);goto done;}
            for(int i=0;i<batch_count;i++){char vector_digest[65];sqlite3_stmt *projection=NULL;bf_sha256_hex((const uint8_t*)vectors[i],(size_t)dimensions*sizeof(float),vector_digest);if(sqlite3_prepare_v2(db,"INSERT INTO provider_embedding_projection VALUES(?,?,?,?,?,?)",-1,&projection,NULL)!=SQLITE_OK){sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);for(int j=0;j<batch_count;j++)free(vectors[j]);free(vectors);goto done;}sqlite3_bind_text(projection,1,run_id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(projection,2,batch[i].id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(projection,3,batch[i].digest,-1,SQLITE_TRANSIENT);sqlite3_bind_text(projection,4,vector_digest,-1,SQLITE_TRANSIENT);sqlite3_bind_int(projection,5,dimensions);sqlite3_bind_blob(projection,6,vectors[i],dimensions*(int)sizeof(float),SQLITE_TRANSIENT);if(sqlite3_step(projection)!=SQLITE_DONE){sqlite3_finalize(projection);sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);for(int j=0;j<batch_count;j++)free(vectors[j]);free(vectors);goto done;}sqlite3_finalize(projection);stored++;}
            if(sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK){for(int i=0;i<batch_count;i++)free(vectors[i]);free(vectors);goto done;}response_total+=response_bytes;for(int i=0;i<batch_count;i++){free(vectors[i]);embed_input_free(&batch[i]);}free(vectors);
        }
    }
    now_iso(completed);
    if (sqlite3_prepare_v2(db,"UPDATE provider_embedding_run SET requested_count=?,stored_count=?,failed_count=?,dimensions=?,response_bytes=?,status=?,completed_at=?,detail=? WHERE run_id=?",-1,&insert,NULL)!=SQLITE_OK) goto done;
    sqlite3_bind_int(insert,1,requested);sqlite3_bind_int(insert,2,stored);sqlite3_bind_int(insert,3,failed);sqlite3_bind_int(insert,4,dimensions);sqlite3_bind_int64(insert,5,(sqlite3_int64)response_total);sqlite3_bind_text(insert,6,(failed==0&&stored==requested)?"complete":"partial",-1,SQLITE_STATIC);sqlite3_bind_text(insert,7,completed,-1,SQLITE_TRANSIENT);sqlite3_bind_text(insert,8,"provider_vectors_are_latent_not_owner_truth",-1,SQLITE_STATIC);sqlite3_bind_text(insert,9,run_id,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(insert)!=SQLITE_DONE)goto done;sqlite3_finalize(insert);insert=NULL;
    if (failed == 0 && stored == requested && !queue_embedding_delta(db, run_id)) goto done;
    if (stored > 0) { sqlite3_stmt *event=NULL; if(sqlite3_prepare_v2(db,"INSERT INTO provider_observations(provider,event_kind,observed_at,detail) VALUES(?,?,?,?)",-1,&event,NULL)==SQLITE_OK){sqlite3_bind_text(event,1,provider,-1,SQLITE_TRANSIENT);sqlite3_bind_text(event,2,"success",-1,SQLITE_STATIC);sqlite3_bind_text(event,3,completed,-1,SQLITE_TRANSIENT);sqlite3_bind_text(event,4,"provider_embedding_projection",-1,SQLITE_STATIC);sqlite3_step(event);sqlite3_finalize(event);} }
    printf("{\"schema\":\"bonfyre-provider-embedding-run.v1\",\"run_id\":");json_string(run_id);printf(",\"provider\":");json_string(provider);printf(",\"model\":");json_string(model);printf(",\"representation_id\":");json_string(representation);printf(",\"input_digest\":");json_string(input_digest);printf(",\"requested\":%d,\"stored\":%d,\"failed\":%d,\"dimensions\":%d,\"response_bytes\":%zu,\"admission_grant_id\":%lld,\"truth\":\"provider vectors are a latent representation; exact owners, evidence, authorization and selection remain separate\"}\n",requested,stored,failed,dimensions,response_total,(long long)grant_id);
    ok = failed == 0 && stored == requested;
done:
    if(insert)sqlite3_finalize(insert); if(file)fclose(file); if(json)sqlite3_close(json); free(line);
    if(batch){for(int i=0;i<batch_size;i++)embed_input_free(&batch[i]);free(batch);} if(grants){if(grant_id>0)bf_admission_release_grant(grants,grant_id,NULL);bf_admission_close(grants);} return ok?0:-1;
}

typedef struct {
    /* SQLite column text is borrowed only until the next step/reset.  A rank
     * survivor crosses that boundary into a receipt, so retain its compact
     * physical handles by value rather than dangling cursor pointers. */
    char subject_id[256];
    char source_digest[65];
    double score;
} ProviderRankHit;

static double cosine_similarity(const float *left, const float *right, int dimensions)
{
    double dot = 0.0, left_norm = 0.0, right_norm = 0.0;
    for (int index = 0; index < dimensions; index++) {
        dot += (double)left[index] * (double)right[index];
        left_norm += (double)left[index] * (double)left[index];
        right_norm += (double)right[index] * (double)right[index];
    }
    if (left_norm <= 0.0 || right_norm <= 0.0) return -2.0;
    return dot / sqrt(left_norm * right_norm);
}

static void rank_hit_insert(ProviderRankHit *hits, int *count, int maximum,
                            const char *subject_id, const char *source_digest,
                            double score)
{
    int at;
    if (!hits || !count || maximum < 1 || !subject_id || !source_digest ||
        !isfinite(score)) return;
    for (at = 0; at < *count && hits[at].score >= score; at++);
    if (at >= maximum) return;
    if (*count < maximum) (*count)++;
    for (int move = *count - 1; move > at; move--) hits[move] = hits[move - 1];
    memset(&hits[at], 0, sizeof(hits[at]));
    snprintf(hits[at].subject_id, sizeof(hits[at].subject_id), "%s", subject_id);
    snprintf(hits[at].source_digest, sizeof(hits[at].source_digest), "%s", source_digest);
    hits[at].score = score;
}

/* This is an intentionally exact physical scan over one already-addressed
 * representation run.  It is a bridge from a provider vector body to a
 * bounded candidate handle set, not a replacement for AddressPlane's hard
 * masks, an exact verifier, evidence, authority, or a lowerer. */
static int rank_provider(sqlite3 *db, int argc, char **argv)
{
    const char *run_id = option_value(argc, argv, "--run-id");
    const char *provider = option_value(argc, argv, "--provider");
    const char *endpoint = option_value(argc, argv, "--endpoint");
    const char *model = option_value(argc, argv, "--model");
    const char *key_name = option_value(argc, argv, "--api-key-env");
    const char *representation = option_value(argc, argv, "--representation-id");
    const char *query = option_value(argc, argv, "--query");
    const char *top_text = option_value(argc, argv, "--top-k");
    const char *admission_path = option_value(argc, argv, "--admission-db");
    const char *admission_volume = option_value(argc, argv, "--admission-volume");
    const char *admission_plane = option_value(argc, argv, "--admission-plane");
    const char *admission_estimate = option_value(argc, argv, "--admission-estimated-bytes");
    const char *admission_floor = option_value(argc, argv, "--admission-protected-floor-bytes");
    const char *api_key; sqlite3_stmt *statement = NULL, *write_query = NULL, *write_rank = NULL;
    BfAdmissionGrants *grants = NULL; int64_t grant_id = 0;
    EmbedInput input = {0}; float **vectors = NULL; ProviderRankHit hits[100];
    int dimensions = 0, expected_dimensions = 0, top_k = 20, hit_count = 0, candidate_count = 0;
    size_t response_bytes = 0; long status = 0; char query_digest[65], vector_digest[65];
    char query_id[192], observed[40]; int result = -1;
    const char *stored_provider = NULL, *stored_model = NULL, *stored_representation = NULL, *stored_status = NULL;
    if (!atom(run_id, 160) || !atom(provider, 96) || !endpoint || strncmp(endpoint, "https://", 8) ||
        !atom(model, 160) || !atom(key_name, 160) || !atom(representation, 160) || !query || !*query ||
        !admission_path || !admission_volume || !admission_plane || !admission_estimate) return -1;
    if (top_text) { top_k = atoi(top_text); if (top_k < 1 || top_k > 100) return -1; }
    api_key = getenv(key_name); if (!api_key || !*api_key) { fprintf(stderr, "provider rank credential reference is absent\n"); return -1; }
    if (sqlite3_prepare_v2(db,
        "SELECT provider,model,representation_id,dimensions,status FROM provider_embedding_run WHERE run_id=?",
        -1, &statement, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(statement, 1, run_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_ROW) goto done;
    stored_provider = (const char *)sqlite3_column_text(statement, 0);
    stored_model = (const char *)sqlite3_column_text(statement, 1);
    stored_representation = (const char *)sqlite3_column_text(statement, 2);
    expected_dimensions = sqlite3_column_int(statement, 3);
    stored_status = (const char *)sqlite3_column_text(statement, 4);
    if (!stored_provider || !stored_model || !stored_representation || !stored_status ||
        strcmp(stored_provider, provider) || strcmp(stored_model, model) ||
        strcmp(stored_representation, representation) || strcmp(stored_status, "complete") ||
        expected_dimensions < 1) goto done;
    sqlite3_finalize(statement); statement = NULL;
    if (!provider_embedding_admission(admission_path, admission_volume, admission_plane,
                                      admission_estimate, admission_floor, &grants, &grant_id)) goto done;
    input.id = strdup("query"); input.text = strdup(query);
    if (!input.id || !input.text) goto done;
    bf_sha256_hex((const uint8_t *)query, strlen(query), query_digest);
    if (!provider_embeddings_with_retry(endpoint, api_key, model, &input, 1, &vectors,
                                        &dimensions, &response_bytes, &status) ||
        dimensions != expected_dimensions || !vectors || !vectors[0]) {
        fprintf(stderr, "provider rank refused: exact representation geometry unavailable (http=%ld)\n", status);
        goto done;
    }
    bf_sha256_hex((const uint8_t *)vectors[0], (size_t)dimensions * sizeof(float), vector_digest);
    if (sqlite3_prepare_v2(db,
        "SELECT subject_id,source_digest,dimensions,vector FROM provider_embedding_projection WHERE run_id=? ORDER BY subject_id",
        -1, &statement, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(statement, 1, run_id, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(statement) == SQLITE_ROW) {
        const char *subject_id = (const char *)sqlite3_column_text(statement, 0);
        const char *source_digest = (const char *)sqlite3_column_text(statement, 1);
        int row_dimensions = sqlite3_column_int(statement, 2);
        const void *body = sqlite3_column_blob(statement, 3);
        int body_bytes = sqlite3_column_bytes(statement, 3);
        double score;
        if (!subject_id || !source_digest || !body || row_dimensions != dimensions ||
            body_bytes != dimensions * (int)sizeof(float)) { sqlite3_finalize(statement); statement = NULL; goto done; }
        score = cosine_similarity(vectors[0], (const float *)body, dimensions);
        if (score <= -1.5) continue;
        candidate_count++;
        rank_hit_insert(hits, &hit_count, top_k, subject_id, source_digest, score);
    }
    sqlite3_finalize(statement); statement = NULL;
    if (candidate_count == 0 || hit_count == 0) goto done;
    /* A provider rank receipt is durable work.  Seconds-only identities
     * collided under a normal batched evaluation, overwriting neither query
     * but refusing the later one after it had already spent a provider call.
     * Keep the provider-local physical identity monotonic enough for burst
     * work without deriving it from caller text. */
    {
        struct timespec stamp;
        clock_gettime(CLOCK_REALTIME, &stamp);
        now_iso(observed);
        snprintf(query_id, sizeof(query_id), "embedding-rank:%s:%lld-%09ld",
                 provider, (long long)stamp.tv_sec, stamp.tv_nsec);
    }
    if (sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) goto done;
    if (sqlite3_prepare_v2(db,
        "INSERT INTO provider_embedding_query(query_id,run_id,query_digest,vector_digest,dimensions,candidate_count,top_k,response_bytes,observed_at) VALUES(?,?,?,?,?,?,?,?,?)",
        -1, &write_query, NULL) != SQLITE_OK) { sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL); goto done; }
    sqlite3_bind_text(write_query, 1, query_id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(write_query, 2, run_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(write_query, 3, query_digest, -1, SQLITE_TRANSIENT); sqlite3_bind_text(write_query, 4, vector_digest, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(write_query, 5, dimensions); sqlite3_bind_int(write_query, 6, candidate_count); sqlite3_bind_int(write_query, 7, hit_count);
    sqlite3_bind_int64(write_query, 8, (sqlite3_int64)response_bytes); sqlite3_bind_text(write_query, 9, observed, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(write_query) != SQLITE_DONE || sqlite3_prepare_v2(db,
        "INSERT INTO provider_embedding_rank(query_id,rank,subject_id,source_digest,score) VALUES(?,?,?,?,?)",
        -1, &write_rank, NULL) != SQLITE_OK) { sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL); goto done; }
    for (int index = 0; index < hit_count; index++) {
        sqlite3_reset(write_rank); sqlite3_clear_bindings(write_rank);
        sqlite3_bind_text(write_rank, 1, query_id, -1, SQLITE_TRANSIENT); sqlite3_bind_int(write_rank, 2, index + 1);
        sqlite3_bind_text(write_rank, 3, hits[index].subject_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(write_rank, 4, hits[index].source_digest, -1, SQLITE_TRANSIENT);
        sqlite3_bind_double(write_rank, 5, hits[index].score);
        if (sqlite3_step(write_rank) != SQLITE_DONE) { sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL); goto done; }
    }
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) goto done;
    printf("{\"schema\":\"bonfyre-provider-embedding-rank.v1\",\"query_id\":"); json_string(query_id);
    printf(",\"run_id\":"); json_string(run_id); printf(",\"query_digest\":"); json_string(query_digest);
    printf(",\"vector_digest\":"); json_string(vector_digest);
    printf(",\"representation_id\":"); json_string(representation);
    printf(",\"dimensions\":%d,\"candidate_count\":%d,\"top_k\":%d,\"response_bytes\":%zu,\"admission_grant_id\":%lld,\"exact_owner_queried\":false,\"truth\":\"ranked handles are latent survivors only; AddressPlane masks, exact owners, evidence, authority and lowerers remain separate\"}\n",
           dimensions, candidate_count, hit_count, response_bytes, (long long)grant_id);
    result = 0;
done:
    if (statement) sqlite3_finalize(statement); if (write_query) sqlite3_finalize(write_query); if (write_rank) sqlite3_finalize(write_rank);
    if (vectors) { free(vectors[0]); free(vectors); } embed_input_free(&input);
    if (grants) { if (grant_id > 0) bf_admission_release_grant(grants, grant_id, NULL); bf_admission_close(grants); }
    return result;
}

static int record_materialization(sqlite3 *db, int argc, char **argv)
{
    const char *provider = option_value(argc, argv, "--provider");
    const char *artifact = option_value(argc, argv, "--artifact-sid");
    const char *digest = option_value(argc, argv, "--representation-digest");
    const char *external = option_value(argc, argv, "--external-ref");
    const char *kind = option_value(argc, argv, "--kind");
    const char *purpose = option_value(argc, argv, "--purpose");
    const char *scope = option_value(argc, argv, "--scope");
    const char *capabilities = option_value(argc, argv, "--capabilities");
    const char *authority = option_value(argc, argv, "--authority-ref");
    const char *expires = option_value(argc, argv, "--expires-at");
    sqlite3_stmt *statement = NULL;
    char created[40];
    int64_t ignored;
    int existing_id = 0;
    const char *existing_digest = NULL;
    if (!atom(provider, 96) || !atom(artifact, 160) || !hex_digest(digest) ||
        !atom(external, 240) || !atom(kind, 96) || !atom(purpose, 160) ||
        !atom(scope, 160) || !atom(authority, 160) ||
        (capabilities != NULL && !atom(capabilities, 512)) ||
        (expires != NULL && parse_iso_ms(expires, &ignored) != 0)) return -1;
    if (sqlite3_prepare_v2(db,
            "SELECT id,representation_digest FROM provider_materializations "
            "WHERE provider=? AND scope=? AND external_ref=? AND released_at IS NULL",
            -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, provider, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, scope, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, external, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) == SQLITE_ROW) {
        existing_id = sqlite3_column_int(statement, 0);
        existing_digest = (const char *)sqlite3_column_text(statement, 1);
        if (existing_digest == NULL || strcmp(existing_digest, digest) != 0) {
            sqlite3_finalize(statement);
            return -1; /* an opaque provider handle may never be rebound */
        }
    }
    sqlite3_finalize(statement);
    if (existing_id) {
        printf("{\"receipt_id\":%d,\"reused\":true,\"provider\":", existing_id);
        json_string(provider); printf(",\"artifact_sid\":"); json_string(artifact);
        printf(",\"external_ref\":"); json_string(external); printf("}\n");
        return 0;
    }
    now_iso(created);
    if (sqlite3_prepare_v2(db,
            "INSERT INTO provider_materializations(provider,artifact_sid,representation_digest,external_ref,kind,purpose,scope,capabilities,authority_ref,created_at,expires_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)", -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, provider, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, artifact, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, digest, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 4, external, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 5, kind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 6, purpose, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 7, scope, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 8, capabilities != NULL ? capabilities : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 9, authority, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 10, created, -1, SQLITE_TRANSIENT);
    if (expires != NULL) sqlite3_bind_text(statement, 11, expires, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(statement, 11);
    if (sqlite3_step(statement) != SQLITE_DONE) { sqlite3_finalize(statement); return -1; }
    sqlite3_finalize(statement);
    printf("{\"receipt_id\":%lld,\"reused\":false,\"provider\":", (long long)sqlite3_last_insert_rowid(db));
    json_string(provider); printf(",\"artifact_sid\":"); json_string(artifact);
    printf(",\"representation_digest\":"); json_string(digest);
    printf(",\"external_ref\":"); json_string(external);
    printf(",\"limit\":\"A provider materialization is replaceable physical residency; it is not artifact identity, canonical knowledge, credential, authority, or an admitted provider result.\"}\n");
    return 0;
}

static int release_materialization(sqlite3 *db, int argc, char **argv)
{
    const char *id_text = option_value(argc, argv, "--receipt-id");
    const char *reason = option_value(argc, argv, "--reason");
    sqlite3_stmt *statement = NULL;
    char released[40];
    char *end = NULL;
    long id;
    if (id_text == NULL || reason == NULL || !atom(reason, 160)) return -1;
    id = strtol(id_text, &end, 10);
    if (id < 1 || end == NULL || *end != '\0') return -1;
    now_iso(released);
    if (sqlite3_prepare_v2(db,
            "UPDATE provider_materializations SET released_at=?,release_reason=? WHERE id=? AND released_at IS NULL",
            -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, released, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, reason, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(statement, 3, id);
    if (sqlite3_step(statement) != SQLITE_DONE || sqlite3_changes(db) != 1) { sqlite3_finalize(statement); return -1; }
    sqlite3_finalize(statement);
    printf("{\"receipt_id\":%ld,\"released\":true,\"released_at\":", id);
    json_string(released); printf(",\"limit\":\"Release deletes no canonical artifact, receipt, or provider account state; it records that this local materialization is no longer reusable.\"}\n");
    return 0;
}

static int digest_regular_file(const char *path, char out[65])
{
    FILE *file;
    struct stat state;
    BfSha256 hash;
    uint8_t digest[32];
    unsigned char buffer[32768];
    size_t read_count;
    if (path == NULL || !*path || out == NULL || stat(path, &state) != 0 ||
        !S_ISREG(state.st_mode) || !(file = fopen(path, "rb"))) return 0;
    bf_sha256_init(&hash);
    for (;;) {
        read_count = fread(buffer, 1, sizeof(buffer), file);
        if (read_count > 0) bf_sha256_update(&hash, buffer, read_count);
        if (read_count == sizeof(buffer)) continue;
        if (ferror(file)) { fclose(file); return 0; }
        break;
    }
    fclose(file);
    bf_sha256_final(&hash, digest);
    bf_sha256_digest_hex(digest, out);
    return 1;
}

/* Record physical availability only after the offered local body has been
 * read back.  A derivation key is the complete physical identity boundary;
 * a new compiler/input closure must mint a new key rather than overwrite a
 * valid body under the old one. */
static int record_body_availability(sqlite3 *db, int argc, char **argv)
{
    const char *derivation = option_value(argc, argv, "--derivation-key");
    const char *representation = option_value(argc, argv, "--representation-ref");
    const char *path = option_value(argc, argv, "--body-file");
    const char *declared = option_value(argc, argv, "--body-digest");
    const char *receipt = option_value(argc, argv, "--receipt-ref");
    sqlite3_stmt *statement = NULL;
    const char *existing = NULL;
    char measured[65], observed[40];
    int result = -1;
    if (!atom(derivation, 240) || !atom(representation, 160) || !atom(receipt, 240) ||
        !hex_digest(declared) || !digest_regular_file(path, measured) ||
        strcmp(measured, declared) != 0) return -1;
    if (sqlite3_prepare_v2(db,
            "SELECT body_digest FROM body_availability WHERE derivation_key=? AND representation_ref=?",
            -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, derivation, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, representation, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) == SQLITE_ROW) existing = (const char *)sqlite3_column_text(statement, 0);
    if (existing != NULL && strcmp(existing, measured) != 0) { sqlite3_finalize(statement); return -1; }
    sqlite3_finalize(statement); statement = NULL;
    now_iso(observed);
    if (sqlite3_prepare_v2(db,
            "INSERT INTO body_availability(derivation_key,representation_ref,body_digest,location,receipt_ref,validity,observed_at,invalidated_at,invalidated_by) "
            "VALUES(?,?,?,?,?,'CURRENT',?,NULL,NULL) "
            "ON CONFLICT(derivation_key,representation_ref) DO UPDATE SET "
            "location=excluded.location,receipt_ref=excluded.receipt_ref,validity='CURRENT',observed_at=excluded.observed_at,invalidated_at=NULL,invalidated_by=NULL",
            -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, derivation, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, representation, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, measured, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 4, path, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 5, receipt, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 6, observed, -1, SQLITE_TRANSIENT);
    result = sqlite3_step(statement) == SQLITE_DONE ? 0 : -1;
    sqlite3_finalize(statement);
    if (result == 0) {
        fputs("{\"derivation_key\":", stdout); json_string(derivation);
        fputs(",\"representation_ref\":", stdout); json_string(representation);
        fputs(",\"body_digest\":", stdout); json_string(measured);
        fputs(",\"availability\":\"CURRENT\",\"limit\":\"Physical body availability is a readback-verified reuse projection; it is not semantic success, authority, or an effect receipt.\"}\n", stdout);
    }
    return result;
}

static int show_body_availability(sqlite3 *db, int argc, char **argv)
{
    const char *derivation = option_value(argc, argv, "--derivation-key");
    const char *representation = option_value(argc, argv, "--representation-ref");
    sqlite3_stmt *statement = NULL;
    const char *digest, *location;
    char measured[65];
    int available = 0;
    if (!atom(derivation, 240) || !atom(representation, 160)) return -1;
    if (sqlite3_prepare_v2(db,
            "SELECT body_digest,location FROM body_availability WHERE derivation_key=? AND representation_ref=? AND validity='CURRENT'",
            -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, derivation, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, representation, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) == SQLITE_ROW) {
        digest = (const char *)sqlite3_column_text(statement, 0);
        location = (const char *)sqlite3_column_text(statement, 1);
        available = digest != NULL && location != NULL && digest_regular_file(location, measured) && strcmp(digest, measured) == 0;
    }
    sqlite3_finalize(statement);
    fputs("{\"derivation_key\":", stdout); json_string(derivation);
    fputs(",\"representation_ref\":", stdout); json_string(representation);
    printf(",\"available\":%s", available ? "true" : "false");
    if (available) { fputs(",\"body_digest\":", stdout); json_string(measured); }
    fputs(",\"limit\":\"Unavailable means no current readback-verified body at this physical location; it does not retract semantic derivation history.\"}\n", stdout);
    return 0;
}

static int invalidate_body_availability(sqlite3 *db, int argc, char **argv)
{
    const char *derivation = option_value(argc, argv, "--derivation-key");
    const char *representation = option_value(argc, argv, "--representation-ref");
    const char *cause = option_value(argc, argv, "--cause-ref");
    sqlite3_stmt *statement = NULL;
    char observed[40];
    if (!atom(derivation, 240) || !atom(representation, 160) || !atom(cause, 240)) return -1;
    now_iso(observed);
    if (sqlite3_prepare_v2(db,
            "UPDATE body_availability SET validity='STALE',invalidated_at=?,invalidated_by=? "
            "WHERE derivation_key=? AND representation_ref=? AND validity='CURRENT'",
            -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, observed, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, cause, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, derivation, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 4, representation, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_DONE || sqlite3_changes(db) != 1) { sqlite3_finalize(statement); return -1; }
    sqlite3_finalize(statement);
    fputs("{\"derivation_key\":", stdout); json_string(derivation);
    fputs(",\"representation_ref\":", stdout); json_string(representation);
    fputs(",\"availability\":\"STALE\",\"cause_ref\":", stdout); json_string(cause);
    fputs(",\"limit\":\"Invalidation withdraws reuse eligibility only; it does not erase the body, its receipt, semantic derivation, or historical effect.\"}\n", stdout);
    return 0;
}

static int list_materializations(sqlite3 *db, int argc, char **argv)
{
    const char *provider = option_value(argc, argv, "--provider");
    const char *artifact = option_value(argc, argv, "--artifact-sid");
    const char *now = option_value(argc, argv, "--now");
    sqlite3_stmt *statement = NULL;
    char generated[40];
    int first = 1;
    int64_t ignored;
    if (provider != NULL && !atom(provider, 96)) return -1;
    if (artifact != NULL && !atom(artifact, 160)) return -1;
    if (now == NULL) { now_iso(generated); now = generated; }
    if (parse_iso_ms(now, &ignored) != 0) return -1;
    if (sqlite3_prepare_v2(db,
            "SELECT id,provider,artifact_sid,representation_digest,external_ref,kind,purpose,scope,capabilities,COALESCE(expires_at,'') "
            "FROM provider_materializations WHERE released_at IS NULL AND (expires_at IS NULL OR expires_at>?) "
            "AND (?='' OR provider=?) AND (?='' OR artifact_sid=?) ORDER BY id",
            -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, now, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, provider != NULL ? provider : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 3, provider != NULL ? provider : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 4, artifact != NULL ? artifact : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 5, artifact != NULL ? artifact : "", -1, SQLITE_TRANSIENT);
    printf("{\"materializations\":[");
    while (sqlite3_step(statement) == SQLITE_ROW) {
        if (!first) putchar(',');
        first = 0;
        printf("{\"receipt_id\":%d,\"provider\":", sqlite3_column_int(statement, 0)); json_string((const char *)sqlite3_column_text(statement, 1));
        printf(",\"artifact_sid\":"); json_string((const char *)sqlite3_column_text(statement, 2));
        printf(",\"representation_digest\":"); json_string((const char *)sqlite3_column_text(statement, 3));
        printf(",\"external_ref\":"); json_string((const char *)sqlite3_column_text(statement, 4));
        printf(",\"kind\":"); json_string((const char *)sqlite3_column_text(statement, 5));
        printf(",\"purpose\":"); json_string((const char *)sqlite3_column_text(statement, 6));
        printf(",\"scope\":"); json_string((const char *)sqlite3_column_text(statement, 7));
        printf(",\"capabilities\":"); json_string((const char *)sqlite3_column_text(statement, 8));
        printf(",\"expires_at\":"); json_string((const char *)sqlite3_column_text(statement, 9)); putchar('}');
    }
    sqlite3_finalize(statement);
    printf("],\"limit\":\"Only active, unexpired replaceable provider residency is listed; credentials and canonical bodies are never returned.\"}\n");
    return 0;
}

static int record_observation(sqlite3 *db, int argc, char **argv)
{
    const char *provider = option_value(argc, argv, "--provider");
    const char *kind_name = option_value(argc, argv, "--kind");
    const char *observed_at = option_value(argc, argv, "--observed-at");
    const char *reset_at = option_value(argc, argv, "--reset-at");
    const char *detail = option_value(argc, argv, "--detail");
    BfProviderEventKind kind;
    sqlite3_stmt *statement = NULL;
    char generated_now[40];
    int valid;
    int64_t ignored;
    if (provider == NULL || kind_name == NULL) return -1;
    kind = event_kind(kind_name, &valid);
    if (!valid) return -1;
    if (observed_at == NULL) {
        now_iso(generated_now);
        observed_at = generated_now;
    }
    if (parse_iso_ms(observed_at, &ignored) != 0 ||
        (reset_at != NULL && parse_iso_ms(reset_at, &ignored) != 0)) return -1;
    if (sqlite3_prepare_v2(db,
            "INSERT INTO provider_observations(provider,event_kind,observed_at,reset_at,detail)"
            " VALUES(?,?,?,?,?)", -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, provider, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(statement, 2, event_name(kind), -1, SQLITE_STATIC);
    sqlite3_bind_text(statement, 3, observed_at, -1, SQLITE_TRANSIENT);
    if (reset_at != NULL) sqlite3_bind_text(statement, 4, reset_at, -1, SQLITE_TRANSIENT);
    else sqlite3_bind_null(statement, 4);
    sqlite3_bind_text(statement, 5, detail != NULL ? detail : "", -1, SQLITE_TRANSIENT);
    valid = sqlite3_step(statement) == SQLITE_DONE;
    sqlite3_finalize(statement);
    if (!valid) return -1;
    printf("{\"provider\":\"%s\",\"recorded\":\"%s\",\"observed_at\":\"%s\"}\n",
           provider, kind_name, observed_at);
    return 0;
}

static int print_status(sqlite3 *db, int argc, char **argv)
{
    const char *provider = option_value(argc, argv, "--provider");
    const char *now = option_value(argc, argv, "--now");
    sqlite3_stmt *statement = NULL;
    BfProviderObservation *observations = NULL;
    size_t count = 0;
    size_t capacity = 0;
    char generated_now[40];
    int64_t now_ms;
    if (provider == NULL) return -1;
    if (now == NULL) {
        now_iso(generated_now);
        now = generated_now;
    }
    if (parse_iso_ms(now, &now_ms) != 0 ||
        sqlite3_prepare_v2(db,
            "SELECT event_kind,observed_at,COALESCE(reset_at,'') FROM provider_observations "
            "WHERE provider=? ORDER BY id", -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(statement, 1, provider, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(statement) == SQLITE_ROW) {
        const char *kind_name = (const char *)sqlite3_column_text(statement, 0);
        const char *observed_at = (const char *)sqlite3_column_text(statement, 1);
        const char *reset_at = (const char *)sqlite3_column_text(statement, 2);
        int valid;
        BfProviderEventKind kind = event_kind(kind_name, &valid);
        int64_t observed_ms;
        int64_t reset_ms = 0;
        if (!valid || parse_iso_ms(observed_at, &observed_ms) != 0 ||
            (reset_at[0] != '\0' && parse_iso_ms(reset_at, &reset_ms) != 0)) continue;
        if (count == capacity) {
            size_t next = capacity == 0 ? 16 : capacity * 2;
            BfProviderObservation *grown = realloc(observations, next * sizeof(*grown));
            if (grown == NULL) {
                free(observations);
                sqlite3_finalize(statement);
                return -1;
            }
            observations = grown;
            capacity = next;
        }
        observations[count++] = (BfProviderObservation){
            .kind = kind, .observed_at_ms = observed_ms, .reset_at_ms = reset_ms};
    }
    sqlite3_finalize(statement);
    BfProviderState state = bf_provider_fold(observations, count, now_ms);
    free(observations);
    printf("{\"provider\":\"%s\",\"status\":\"%s\",\"available\":%s,"
           "\"circuit_until_ms\":%lld,\"consecutive_failures\":%d,\"hard_capacity_hits\":%d}\n",
           provider, bf_provider_status_name(state.status),
           bf_provider_available(&state, now_ms) ? "true" : "false",
           (long long)state.circuit_until_ms, state.consecutive_failures, state.hard_capacity_hits);
    return 0;
}

static void usage(void)
{
    fputs("usage: bonfyre-provider record --db FILE --provider ID --kind KIND [--observed-at ISO] [--reset-at ISO] [--detail TEXT]\n"
          "       bonfyre-provider status --db FILE --provider ID [--now ISO]\n"
          "       bonfyre-provider materialize --db FILE --provider ID --artifact-sid SID --representation-digest BLAKE3 --external-ref REF --kind KIND --purpose PURPOSE --scope SCOPE --authority-ref REF [--capabilities LIST] [--expires-at ISO]\n"
          "       bonfyre-provider materializations --db FILE [--provider ID] [--artifact-sid SID] [--now ISO]\n"
          "       bonfyre-provider release --db FILE --receipt-id ID --reason REASON\n"
          "       bonfyre-provider body-record --db FILE --derivation-key ID --representation-ref ID --body-file FILE --body-digest SHA256 --receipt-ref ID\n"
          "       bonfyre-provider body-available --db FILE --derivation-key ID --representation-ref ID\n"
          "       bonfyre-provider body-invalidate --db FILE --derivation-key ID --representation-ref ID --cause-ref ID\n"
          "       bonfyre-provider embed --db FILE --provider ID --endpoint https://HOST --model MODEL --api-key-env ENV --representation-id ID --input-jsonl FILE --admission-db FILE --admission-volume PATH --admission-plane ID --admission-estimated-bytes N [--admission-protected-floor-bytes N] [--batch-size N]\n"
          "       bonfyre-provider reconcile-embedding-deltas --db FILE\n"
          "       bonfyre-provider emit-embedding-deltas --db FILE --dir DIRECTORY\n"
          "       bonfyre-provider rank --db FILE --run-id ID --provider ID --endpoint https://HOST --model MODEL --api-key-env ENV --representation-id ID --query TEXT --admission-db FILE --admission-volume PATH --admission-plane ID --admission-estimated-bytes N [--admission-protected-floor-bytes N] [--top-k N]\n", stderr);
}

#define PROVIDER_COMMANDS(X) \
    X("body-available", PC_BODY_AVAILABLE) \
    X("body-invalidate", PC_BODY_INVALIDATE) \
    X("body-record", PC_BODY_RECORD) \
    X("embed", PC_EMBED) \
    X("emit-embedding-deltas", PC_EMIT_EMBEDDING_DELTAS) \
    X("materializations", PC_MATERIALIZATIONS) \
    X("materialize", PC_MATERIALIZE) \
    X("rank", PC_RANK) \
    X("reconcile-embedding-deltas", PC_RECONCILE_EMBEDDING_DELTAS) \
    X("record", PC_RECORD) \
    X("release", PC_RELEASE) \
    X("status", PC_STATUS)

enum {
#define PROVIDER_ENUM(name, id) id,
    PROVIDER_COMMANDS(PROVIDER_ENUM)
#undef PROVIDER_ENUM
};
static const BfCommandEntry provider_commands[] = {
#define PROVIDER_ENTRY(name, id) {name, id, 0, NULL},
    PROVIDER_COMMANDS(PROVIDER_ENTRY)
#undef PROVIDER_ENTRY
};

int main(int argc, char **argv)
{
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    const char *database_path;
    sqlite3 *db = NULL;
    int result;
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(provider_commands,
        sizeof(provider_commands) / sizeof(provider_commands[0]), argv[1]);
    if (!command) {
        usage();
        return 2;
    }
    database_path = option_value(argc, argv, "--db");
    if (database_path == NULL || sqlite3_open(database_path, &db) != SQLITE_OK ||
        apply_schema(db) != 0) {
        fprintf(stderr, "bonfyre-provider: cannot open provider store\n");
        sqlite3_close(db);
        return 1;
    }
    switch (command->id) {
    case PC_RECORD: result = record_observation(db, argc, argv); break;
    case PC_STATUS: result = print_status(db, argc, argv); break;
    case PC_MATERIALIZE: result = record_materialization(db, argc, argv); break;
    case PC_MATERIALIZATIONS: result = list_materializations(db, argc, argv); break;
    case PC_RELEASE: result = release_materialization(db, argc, argv); break;
    case PC_BODY_RECORD: result = record_body_availability(db, argc, argv); break;
    case PC_BODY_AVAILABLE: result = show_body_availability(db, argc, argv); break;
    case PC_BODY_INVALIDATE: result = invalidate_body_availability(db, argc, argv); break;
    case PC_EMBED: result = embed_provider(db, argc, argv); break;
    case PC_RECONCILE_EMBEDDING_DELTAS: {
        int queued = reconcile_embedding_deltas(db);
        if (queued >= 0) printf("{\"provider_embedding_deltas_queued\":%d,\"truth\":\"completed provider runs only; dependency forwarding still required\"}\n", queued);
        result = queued < 0 ? -1 : 0;
        break;
    }
    case PC_EMIT_EMBEDDING_DELTAS: {
        int emitted = emit_queued_embedding_deltas(db, option_value(argc, argv, "--dir"));
        if (emitted >= 0) printf("{\"provider_embedding_deltas_dispatched\":%d,\"truth\":\"FactDelta outbox only; no candidate selection or provider call\"}\n", emitted);
        result = emitted < 0 ? -1 : 0;
        break;
    }
    case PC_RANK: result = rank_provider(db, argc, argv); break;
    default: result = -1; break;
    }
    sqlite3_close(db);
    if (result != 0) {
        usage();
        return 2;
    }
    return 0;
}
