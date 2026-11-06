/* Machine-readable views of the existing admitted-rate and physical-attempt
 * owner. No new price, invoice, account grant or parallel meter store lives here.
 * These local commands require an explicit store coordinate. New wire output is
 * a single MessagePack [1, ok, value] packet; legacy CLI forms stay compatible. */
#ifndef BONFYRE_METER_PACKET_H
#define BONFYRE_METER_PACKET_H
#include <math.h>
#include "bf_msgpack.h"

typedef struct {
    const char *key, *op, *provider, *receipt, *attempt, *realization, *at, *db;
    long bytes;
    int duration;
} MeterPacketInput;

static int meter_packet_emit(bf_mp_writer_t *out, int bad, int status) {
    if (bad || fwrite(out->buf, 1, out->len, stdout) != out->len || fflush(stdout)) status = 73;
    bf_mp_writer_free(out);
    return status;
}

static int meter_packet_error(const char *code, int status) {
    bf_mp_writer_t out;
    bf_mp_writer_init(&out, 256);
    int bad = bf_mp_pack_array(&out, 3) || bf_mp_pack_uint(&out, 1) || bf_mp_pack_bool(&out, 0) ||
        bf_mp_pack_map(&out, 2) || bf_mp_pack_kv_str(&out, "code", code) ||
        bf_mp_pack_kv_str(&out, "message", "The native meter observation could not complete.");
    return meter_packet_emit(&out, bad, status);
}

static int meter_packet_text(const char *s, size_t maximum) {
    if (!s || !*s || strlen(s) > maximum) return 0;
    for (; *s; ++s) if ((unsigned char)*s < 0x20 || (unsigned char)*s == 0x7f) return 0;
    return 1;
}

static int meter_packet_at(const char *s) {
    if (!s || strlen(s) != 20 || s[4] != '-' || s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':' || s[19] != 'Z') return 0;
    for (int i = 0; i < 19; ++i)
        if (i != 4 && i != 7 && i != 10 && i != 13 && i != 16 && (s[i] < '0' || s[i] > '9')) return 0;
    int y, m, d, h, n, sec;
    if (sscanf(s, "%4d-%2d-%2dT%2d:%2d:%2dZ", &y, &m, &d, &h, &n, &sec) != 6) return 0;
    static const int days[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
    if (y < 1970 || m < 1 || m > 12 || d < 1 || h > 23 || n > 59 || sec > 59) return 0;
    int limit = days[m] + (m == 2 && y % 4 == 0 && (y % 100 != 0 || y % 400 == 0));
    return d <= limit;
}

static int meter_packet_number(const char *s, unsigned long maximum, unsigned long *out) {
    if (!s || !*s) return 0;
    unsigned long n = 0;
    for (; *s; ++s) {
        if (*s < '0' || *s > '9' || n > (maximum - (unsigned)(*s - '0')) / 10) return 0;
        n = n * 10 + (unsigned)(*s - '0');
        if (n > maximum) return 0;
    }
    *out = n;
    return 1;
}

static int meter_packet_parse(int argc, char **argv, MeterPacketInput *p, unsigned required) {
    const char *names[] = {"--key","--op","--provider","--receipt","--attempt-ref","--realization-ref","--at","--bytes","--duration","--db"};
    const char *values[10] = {0};
    unsigned seen = 0;
    for (int i = 2; i < argc; i += 2) {
        if (i + 1 >= argc) return 0;
        int field = -1;
        for (int j = 0; j < 10; ++j) if (!strcmp(argv[i], names[j])) field = j;
        if (field < 0 || (seen & (1u << field)) || !(required & (1u << field))) return 0;
        seen |= 1u << field; values[field] = argv[i + 1];
        if (!meter_packet_text(values[field], field == 9 ? 4095 : 512)) return 0;
    }
    if (seen != required || !values[9] || values[9][0] != '/') return 0;
    p->key = values[0]; p->op = values[1]; p->provider = values[2]; p->receipt = values[3];
    p->attempt = values[4]; p->realization = values[5]; p->at = values[6]; p->db = values[9];
    if (p->at && !meter_packet_at(p->at)) return 0;
    unsigned long n;
    if (values[7]) { if (!meter_packet_number(values[7], 64ul * 1024 * 1024, &n)) return 0; p->bytes = (long)n; }
    if (values[8]) { if (!meter_packet_number(values[8], 3600000ul, &n)) return 0; p->duration = (int)n; }
    return 1;
}

static int meter_packet_open(const char *path, sqlite3 **db, int *absent) {
    struct stat metadata;
    *absent = 0;
    if (lstat(path, &metadata)) { if (errno == ENOENT) { *absent = 1; return 0; } return -1; }
    if (!S_ISREG(metadata.st_mode)) return -1;
    int flags = SQLITE_OPEN_READONLY;
#ifdef SQLITE_OPEN_NOFOLLOW
    flags |= SQLITE_OPEN_NOFOLLOW;
#endif
    if (sqlite3_open_v2(path, db, flags, NULL) != SQLITE_OK) return -1;
    sqlite3_busy_timeout(*db, 3000);
    return 0;
}

/* Rate columns: id, amount, currency, unit, effective_at, evidence_ref. */
static int meter_packet_rate(bf_mp_writer_t *out, sqlite3_stmt *row, int column, int has_cost, double cost) {
    sqlite3_int64 id = sqlite3_column_int64(row, column);
    double amount = sqlite3_column_double(row, column + 1), probe = 0;
    const char *currency = (const char *)sqlite3_column_text(row, column + 2);
    const char *unit = (const char *)sqlite3_column_text(row, column + 3);
    const char *at = (const char *)sqlite3_column_text(row, column + 4);
    const char *evidence = (const char *)sqlite3_column_text(row, column + 5);
    if (id < 1 || id > 9007199254740991LL || !isfinite(amount) || amount < 0 || !currency || strlen(currency) != 3 ||
        currency[0] < 'A' || currency[0] > 'Z' || currency[1] < 'A' || currency[1] > 'Z' || currency[2] < 'A' || currency[2] > 'Z' ||
        !meter_packet_text(unit, 32) || !apply_rate(unit, amount, 1, 1, &probe) || !meter_packet_at(at) ||
        !meter_packet_text(evidence, 512) || (has_cost && (!isfinite(cost) || cost < 0))) return -1;
    int bad = bf_mp_pack_map(out, has_cost ? 8 : 7) || bf_mp_pack_kv_str(out, "state", "priced") ||
        bf_mp_pack_kv_int(out, "rateId", id) || bf_mp_pack_kv_double(out, "amount", amount) ||
        bf_mp_pack_kv_str(out, "currency", currency) || bf_mp_pack_kv_str(out, "unit", unit) ||
        bf_mp_pack_kv_str(out, "effectiveAt", at) || bf_mp_pack_kv_str(out, "evidenceRef", evidence);
    if (has_cost) bad = bad || bf_mp_pack_kv_double(out, "cost", cost);
    return bad ? -1 : 0;
}

static int meter_packet_quote(const MeterPacketInput *p) {
    sqlite3 *db = NULL; sqlite3_stmt *row = NULL;
    int absent = 0, step = SQLITE_DONE, bad = 0;
    if (meter_packet_open(p->db, &db, &absent)) { if (db) sqlite3_close(db); return meter_packet_error("unavailable", 75); }
    if (!absent) {
        if (sqlite3_prepare_v2(db, "SELECT id,amount,currency,unit,effective_at,evidence_ref FROM rate WHERE provider=? AND op=? AND effective_at<=? ORDER BY effective_at DESC,id DESC LIMIT 1", -1, &row, NULL) != SQLITE_OK) goto unavailable;
        sqlite3_bind_text(row, 1, p->provider, -1, SQLITE_STATIC); sqlite3_bind_text(row, 2, p->op, -1, SQLITE_STATIC); sqlite3_bind_text(row, 3, p->at, -1, SQLITE_STATIC);
        step = sqlite3_step(row);
        if (step != SQLITE_ROW && step != SQLITE_DONE) goto unavailable;
    }
    bf_mp_writer_t out;
    bf_mp_writer_init(&out, 1024);
    bad = bf_mp_pack_array(&out, 3) || bf_mp_pack_uint(&out, 1) || bf_mp_pack_bool(&out, 1) || bf_mp_pack_map(&out, 4) ||
        bf_mp_pack_kv_str(&out, "provider", p->provider) || bf_mp_pack_kv_str(&out, "operation", p->op) ||
        bf_mp_pack_kv_str(&out, "at", p->at) || bf_mp_pack_str_cstr(&out, "pricing");
    if (step == SQLITE_ROW) bad = bad || meter_packet_rate(&out, row, 0, 0, 0);
    else bad = bad || bf_mp_pack_map(&out, 1) || bf_mp_pack_kv_str(&out, "state", "unknown");
    sqlite3_finalize(row); if (db) sqlite3_close(db);
    if (bad) { bf_mp_writer_free(&out); return meter_packet_error("invalid_rate", 75); }
    return meter_packet_emit(&out, 0, 0);
unavailable:
    sqlite3_finalize(row); if (db) sqlite3_close(db);
    return meter_packet_error("unavailable", 75);
}

static int meter_packet_event(const MeterPacketInput *p) {
    sqlite3 *db = NULL; sqlite3_stmt *row = NULL; int absent = 0;
    if (meter_packet_open(p->db, &db, &absent) || absent) { if (db) sqlite3_close(db); return meter_packet_error("unavailable", 75); }
    if (sqlite3_prepare_v2(db,
        "SELECT e.id,e.key_id,e.op,e.provider,e.physical_attempt_ref,e.receipt_ref,e.realization_ref,e.timestamp,e.bytes,e.duration_ms,e.cost,e.rate_id,"
        "r.id,r.amount,r.currency,r.unit,r.effective_at,r.evidence_ref FROM events e LEFT JOIN rate r ON r.id=e.rate_id WHERE e.physical_attempt_ref=? AND e.key_id=?", -1, &row, NULL) != SQLITE_OK) goto unavailable;
    sqlite3_bind_text(row, 1, p->attempt, -1, SQLITE_STATIC); sqlite3_bind_text(row, 2, p->key, -1, SQLITE_STATIC);
    int step = sqlite3_step(row);
    if (step == SQLITE_DONE) { sqlite3_finalize(row); sqlite3_close(db); return meter_packet_error("not_found", 77); }
    if (step != SQLITE_ROW) goto unavailable;
    const char *fields[] = {"key","operation","provider","attemptRef","receiptRef","realizationRef","at"};
    for (int i = 1; i <= 7; ++i) if (!meter_packet_text((const char *)sqlite3_column_text(row, i), 512)) goto unavailable;
    sqlite3_int64 id = sqlite3_column_int64(row, 0), bytes = sqlite3_column_int64(row, 8), duration = sqlite3_column_int64(row, 9);
    if (id < 1 || id > 9007199254740991LL || bytes < 0 || bytes > 67108864 || duration < 0 || duration > 3600000 ||
        !meter_packet_at((const char *)sqlite3_column_text(row, 7))) goto unavailable;
    bf_mp_writer_t out;
    bf_mp_writer_init(&out, 2048);
    int bad = bf_mp_pack_array(&out, 3) || bf_mp_pack_uint(&out, 1) || bf_mp_pack_bool(&out, 1) ||
        bf_mp_pack_map(&out, 12) || bf_mp_pack_kv_str(&out, "state", "recorded") || bf_mp_pack_kv_int(&out, "eventId", id);
    for (int i = 1; i <= 7 && !bad; ++i) bad = bf_mp_pack_kv_str(&out, fields[i - 1], (const char *)sqlite3_column_text(row, i));
    bad = bad || bf_mp_pack_kv_int(&out, "outputBytes", bytes) || bf_mp_pack_kv_int(&out, "durationMs", duration) || bf_mp_pack_str_cstr(&out, "pricing");
    if (sqlite3_column_type(row, 10) == SQLITE_NULL && sqlite3_column_type(row, 11) == SQLITE_NULL)
        bad = bad || bf_mp_pack_map(&out, 1) || bf_mp_pack_kv_str(&out, "state", "unknown");
    else if (sqlite3_column_type(row, 10) == SQLITE_NULL || sqlite3_column_type(row, 12) == SQLITE_NULL) bad = 1;
    else bad = bad || meter_packet_rate(&out, row, 12, 1, sqlite3_column_double(row, 10));
    sqlite3_finalize(row); sqlite3_close(db);
    if (bad) { bf_mp_writer_free(&out); return meter_packet_error("unavailable", 75); }
    return meter_packet_emit(&out, 0, 0);
unavailable:
    sqlite3_finalize(row); sqlite3_close(db); return meter_packet_error("unavailable", 75);
}

static int meter_packet_command(int argc, char **argv, int quote, int event) {
    MeterPacketInput p = {0};
    if (!meter_packet_parse(argc, argv, &p, quote ? 582u : event ? 529u : 1023u)) return meter_packet_error("invalid", 64);
    if (quote) return meter_packet_quote(&p);
    if (!event) {
        struct stat metadata;
        if (!lstat(p.db, &metadata) && !S_ISREG(metadata.st_mode)) return meter_packet_error("unavailable", 75);
        int rc = cmd_record(p.db, p.key, p.op, p.bytes, p.duration, p.provider, p.receipt, p.attempt, p.realization, p.at, NULL, NULL, NULL, NULL, NULL);
        if (rc) return meter_packet_error(rc == 2 ? "record_refused" : "unavailable", rc == 2 ? 77 : 75);
    }
    return meter_packet_event(&p);
}
#endif
