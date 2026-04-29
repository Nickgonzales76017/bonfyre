#include "wire.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <bonfyre.h>

#define BF_SPACE_DB_ENV "BONFYRE_SPACE_DB"
#define BF_SPACE_DB_SUBPATH "/.local/share/bonfyre/space.db"

typedef struct {
    uint32_t magic;
    uint16_t major;
    uint16_t minor;
    int32_t thiszone;
    uint32_t sigfigs;
    uint32_t snaplen;
    uint32_t network;
} PcapGlobalHeader;

typedef struct {
    uint32_t ts_sec;
    uint32_t ts_usec;
    uint32_t incl_len;
    uint32_t orig_len;
} PcapPacketHeader;

static void now_iso8601(char out[32]) {
    time_t now = time(NULL);
    struct tm tmv;
    gmtime_r(&now, &tmv);
    strftime(out, 32, "%Y-%m-%dT%H:%M:%SZ", &tmv);
}

static void join_path(char *buf, size_t sz, const char *a, const char *b) {
    if (!a || !a[0]) snprintf(buf, sz, "%s", b ? b : "");
    else if (!b || !b[0]) snprintf(buf, sz, "%s", a);
    else if (a[strlen(a) - 1] == '/') snprintf(buf, sz, "%s%s", a, b);
    else snprintf(buf, sz, "%s/%s", a, b);
}

static int starts_with_ci(const char *s, const char *prefix) {
    size_t n = strlen(prefix);
    return strncasecmp(s ? s : "", prefix, n) == 0;
}

static int contains_ci(const char *hay, const char *needle) {
    size_t n;
    if (!hay || !needle || !needle[0]) return 0;
    n = strlen(needle);
    for (; *hay; hay++) {
        if (strncasecmp(hay, needle, n) == 0) return 1;
    }
    return 0;
}

static int env_truthy(const char *name) {
    const char *v = getenv(name);
    if (!v || !v[0]) return 0;
    return strcmp(v, "1") == 0 || strcmp(v, "true") == 0 || strcmp(v, "TRUE") == 0 || strcmp(v, "yes") == 0;
}

static int json_find_key(const char *json, const char *key, const char **out) {
    char needle[128];
    const char *p;
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    p = strstr(json, needle);
    if (!p) return 1;
    p = strchr(p + strlen(needle), ':');
    if (!p) return 1;
    p++;
    while (*p == ' ' || *p == '\t') p++;
    *out = p;
    return 0;
}

static int json_extract_str(const char *json, const char *key, char *out, size_t out_sz) {
    const char *p;
    size_t w = 0;
    if (!out || out_sz == 0) return 1;
    out[0] = '\0';
    if (json_find_key(json, key, &p) != 0) return 1;
    if (*p != '"') return 1;
    p++;
    while (*p && *p != '"' && w < out_sz - 1) {
        if (*p == '\\' && p[1]) {
            p++;
            if (*p == 'n') out[w++] = '\n';
            else if (*p == 'r') out[w++] = '\r';
            else if (*p == 't') out[w++] = '\t';
            else out[w++] = *p;
            p++;
            continue;
        }
        out[w++] = *p++;
    }
    out[w] = '\0';
    return out[0] ? 0 : 1;
}

static int json_extract_int(const char *json, const char *key, int *out) {
    const char *p;
    if (!out) return 1;
    if (json_find_key(json, key, &p) != 0) return 1;
    *out = atoi(p);
    return 0;
}

static void json_escape_print(const char *s) {
    const unsigned char *p = (const unsigned char *)(s ? s : "");
    putchar('"');
    while (*p) {
        unsigned char c = *p++;
        switch (c) {
            case '\\': fputs("\\\\", stdout); break;
            case '"': fputs("\\\"", stdout); break;
            case '\n': fputs("\\n", stdout); break;
            case '\r': fputs("\\r", stdout); break;
            case '\t': fputs("\\t", stdout); break;
            default:
                if (c < 0x20) printf("\\u%04x", (unsigned)c);
                else putchar((char)c);
                break;
        }
    }
    putchar('"');
}

static uint16_t be16(const uint8_t *p) {
    return (uint16_t)(((uint16_t)p[0] << 8) | p[1]);
}

static uint32_t be32(const uint8_t *p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3];
}

static void space_db_path(char *buf, size_t sz) {
    const char *env = getenv(BF_SPACE_DB_ENV);
    if (env && env[0]) {
        snprintf(buf, sz, "%s", env);
        return;
    }
    {
        const char *home = getenv("HOME");
        if (!home) home = "/tmp";
        snprintf(buf, sz, "%s%s", home, BF_SPACE_DB_SUBPATH);
    }
}

static int ensure_parent_dirs(const char *root) {
    char wire_dir[PATH_MAX], reports_dir[PATH_MAX], buffers_dir[PATH_MAX], raw_dir[PATH_MAX];
    if (bf_ensure_dir(root) != 0) return 1;
    join_path(wire_dir, sizeof(wire_dir), root, "wire");
    join_path(reports_dir, sizeof(reports_dir), wire_dir, "reports");
    join_path(buffers_dir, sizeof(buffers_dir), wire_dir, "buffers");
    join_path(raw_dir, sizeof(raw_dir), wire_dir, "raw");
    if (bf_ensure_dir(wire_dir) != 0) return 1;
    if (bf_ensure_dir(reports_dir) != 0) return 1;
    if (bf_ensure_dir(buffers_dir) != 0) return 1;
    if (bf_ensure_dir(raw_dir) != 0) return 1;
    return 0;
}

int bf_wire_ensure_schema(const char *root, char *db_path, size_t db_path_sz) {
    sqlite3 *db = NULL;
    char wire_dir[PATH_MAX];
    const char *schema =
        "CREATE TABLE IF NOT EXISTS wire_captures("
        " capture_id TEXT PRIMARY KEY,"
        " source_kind TEXT, source_path TEXT, interface_name TEXT,"
        " authorized INTEGER, dumb_device INTEGER DEFAULT 0,"
        " metadata_only INTEGER, payload_enabled INTEGER,"
        " unencrypted_only INTEGER, save_raw INTEGER,"
        " packet_count INTEGER, byte_count INTEGER, status TEXT, created_at TEXT"
        ");"
        "CREATE TABLE IF NOT EXISTS wire_flows("
        " capture_id TEXT NOT NULL, flow_id TEXT NOT NULL,"
        " src_ip TEXT, dst_ip TEXT, src_port INTEGER, dst_port INTEGER,"
        " l4_proto TEXT, app_proto TEXT, media_kind TEXT,"
        " domain TEXT, dns_name TEXT, sni TEXT, process_name TEXT,"
        " first_ts TEXT, last_ts TEXT, packets INTEGER, bytes INTEGER,"
        " encrypted INTEGER, probable_media INTEGER, probable_control INTEGER,"
        " hls_manifest INTEGER, dash_manifest INTEGER, websocket_media INTEGER,"
        " pipeline_trigger INTEGER, vendor_dependency TEXT, suggested_integration_point TEXT,"
        " payload_buffer_path TEXT,"
        " PRIMARY KEY(capture_id, flow_id)"
        ");"
        "CREATE TABLE IF NOT EXISTS wire_meter("
        " capture_id TEXT, flow_id TEXT, record_hash TEXT PRIMARY KEY, record_json TEXT"
        ");"
        "CREATE TABLE IF NOT EXISTS wire_scaling("
        " capture_id TEXT, flow_id TEXT, event_hash TEXT PRIMARY KEY, event_json TEXT"
        ");"
        "CREATE TABLE IF NOT EXISTS wire_route("
        " capture_id TEXT, flow_id TEXT, event_hash TEXT PRIMARY KEY, event_json TEXT"
        ");"
        /* Device-level aggregation — one row per discovered IP per capture.
         * Built on demand by probe/artifacts/recipe; never populated by ingest. */
        "CREATE TABLE IF NOT EXISTS wire_devices("
        " device_id TEXT PRIMARY KEY,"
        " capture_id TEXT,"
        " ip TEXT, vendor_hint TEXT, device_class TEXT,"
        " open_ports TEXT, protocol_fingerprint TEXT,"
        " media_capable INTEGER, asr_candidate INTEGER, encrypted_only INTEGER,"
        " total_flows INTEGER, total_bytes INTEGER, total_packets INTEGER,"
        " first_seen TEXT, last_seen TEXT,"
        " bonfyre_chain TEXT, artifact_id TEXT"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_wire_devices_capture ON wire_devices(capture_id);";
    if (ensure_parent_dirs(root) != 0) return 1;
    join_path(wire_dir, sizeof(wire_dir), root, "wire");
    join_path(db_path, db_path_sz, wire_dir, "wire.db");
    if (sqlite3_open(db_path, &db) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return 1;
    }
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);
    if (sqlite3_exec(db, schema, NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    /* migration: add dumb_device to pre-existing DBs; error ignored if column already exists */
    sqlite3_exec(db, "ALTER TABLE wire_captures ADD COLUMN dumb_device INTEGER DEFAULT 0;", NULL, NULL, NULL);
    sqlite3_close(db);
    return 0;
}

static int open_wire_db(const char *root, sqlite3 **db, char *db_path, size_t db_path_sz) {
    if (bf_wire_ensure_schema(root, db_path, db_path_sz) != 0) return 1;
    if (sqlite3_open(db_path, db) != SQLITE_OK) {
        if (*db) sqlite3_close(*db);
        *db = NULL;
        return 1;
    }
    return 0;
}

static int copy_file(const char *src, const char *dst) {
    FILE *in = fopen(src, "rb");
    FILE *out;
    char buf[8192];
    size_t n;
    if (!in) return 1;
    out = fopen(dst, "wb");
    if (!out) { fclose(in); return 1; }
    while ((n = fread(buf, 1, sizeof(buf), in)) > 0) {
        if (fwrite(buf, 1, n, out) != n) { fclose(in); fclose(out); return 1; }
    }
    fclose(in);
    fclose(out);
    return 0;
}

static long long file_size_bytes(const char *path) {
    struct stat st;
    if (!path || stat(path, &st) != 0) return -1;
    return (long long)st.st_size;
}

int bf_wire_save_raw_copy(const char *root, const char *src_path, char *out_path, size_t out_path_sz) {
    char raw_dir[PATH_MAX], ts[32], digest[65], seed[PATH_MAX + 64], name[PATH_MAX];
    const char *base;
    now_iso8601(ts);
    join_path(raw_dir, sizeof(raw_dir), root, "wire/raw");
    if (bf_ensure_dir(raw_dir) != 0) return 1;
    base = strrchr(src_path, '/');
    base = base ? base + 1 : src_path;
    snprintf(seed, sizeof(seed), "%s:%s", ts, base);
    bf_sha256_hex((const uint8_t *)seed, strlen(seed), digest);
    snprintf(name, sizeof(name), "%s/%s_%s", raw_dir, digest, base);
    if (copy_file(src_path, name) != 0) return 1;
    snprintf(out_path, out_path_sz, "%s", name);
    return 0;
}

static void seed_capture_id(BfWireCapture *capture) {
    char seed[PATH_MAX + 128];
    struct timespec ts;
    if (!capture->created_at[0]) now_iso8601(capture->created_at);
    clock_gettime(CLOCK_REALTIME, &ts);
    snprintf(seed, sizeof(seed), "%s:%s:%s:%s:%d:%d:%ld:%ld", capture->created_at,
             capture->source_kind, capture->source_path, capture->interface_name,
             capture->metadata_only, capture->payload_enabled,
             (long)getpid(), (long)ts.tv_nsec);
    bf_sha256_hex((const uint8_t *)seed, strlen(seed), capture->capture_id);
}

int bf_wire_capture_create(const char *root, BfWireCapture *capture) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char db_path[PATH_MAX];
    if (!capture) return 1;
    seed_capture_id(capture);
    if (open_wire_db(root, &db, db_path, sizeof(db_path)) != 0) return 1;
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO wire_captures(capture_id,source_kind,source_path,interface_name,authorized,dumb_device,metadata_only,payload_enabled,unencrypted_only,save_raw,packet_count,byte_count,status,created_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_text(st, 1, capture->capture_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, capture->source_kind, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, capture->source_path, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 4, capture->interface_name, -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 5, capture->authorized);
    sqlite3_bind_int(st, 6, capture->dumb_device);
    sqlite3_bind_int(st, 7, capture->metadata_only);
    sqlite3_bind_int(st, 8, capture->payload_enabled);
    sqlite3_bind_int(st, 9, capture->unencrypted_only);
    sqlite3_bind_int(st, 10, capture->save_raw);
    sqlite3_bind_int64(st, 11, capture->packet_count);
    sqlite3_bind_int64(st, 12, capture->byte_count);
    sqlite3_bind_text(st, 13, capture->status, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 14, capture->created_at, -1, SQLITE_STATIC);
    sqlite3_step(st);
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

int bf_wire_capture_update_summary(const char *root, const BfWireCapture *capture) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char db_path[PATH_MAX];
    if (!capture) return 1;
    if (open_wire_db(root, &db, db_path, sizeof(db_path)) != 0) return 1;
    if (sqlite3_prepare_v2(db,
        "UPDATE wire_captures SET packet_count=?, byte_count=?, status=? WHERE capture_id=?",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_int64(st, 1, capture->packet_count);
    sqlite3_bind_int64(st, 2, capture->byte_count);
    sqlite3_bind_text(st, 3, capture->status, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 4, capture->capture_id, -1, SQLITE_STATIC);
    sqlite3_step(st);
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

static int load_flow(sqlite3 *db, const char *capture_id, const char *flow_id, BfWireFlow *flow) {
    sqlite3_stmt *st = NULL;
    memset(flow, 0, sizeof(*flow));
    if (sqlite3_prepare_v2(db,
        "SELECT src_ip,dst_ip,src_port,dst_port,l4_proto,app_proto,media_kind,domain,dns_name,sni,process_name,first_ts,last_ts,packets,bytes,encrypted,probable_media,probable_control,hls_manifest,dash_manifest,websocket_media,pipeline_trigger,vendor_dependency,suggested_integration_point,payload_buffer_path "
        "FROM wire_flows WHERE capture_id=? AND flow_id=?", -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, flow_id, -1, SQLITE_STATIC);
    if (sqlite3_step(st) != SQLITE_ROW) {
        sqlite3_finalize(st);
        return 1;
    }
    snprintf(flow->flow_id, sizeof(flow->flow_id), "%s", flow_id);
    snprintf(flow->src_ip, sizeof(flow->src_ip), "%s", sqlite3_column_text(st,0) ? (const char *)sqlite3_column_text(st,0) : "");
    snprintf(flow->dst_ip, sizeof(flow->dst_ip), "%s", sqlite3_column_text(st,1) ? (const char *)sqlite3_column_text(st,1) : "");
    flow->src_port = sqlite3_column_int(st, 2);
    flow->dst_port = sqlite3_column_int(st, 3);
    snprintf(flow->l4_proto, sizeof(flow->l4_proto), "%s", sqlite3_column_text(st,4) ? (const char *)sqlite3_column_text(st,4) : "");
    snprintf(flow->app_proto, sizeof(flow->app_proto), "%s", sqlite3_column_text(st,5) ? (const char *)sqlite3_column_text(st,5) : "");
    snprintf(flow->media_kind, sizeof(flow->media_kind), "%s", sqlite3_column_text(st,6) ? (const char *)sqlite3_column_text(st,6) : "");
    snprintf(flow->domain, sizeof(flow->domain), "%s", sqlite3_column_text(st,7) ? (const char *)sqlite3_column_text(st,7) : "");
    snprintf(flow->dns_name, sizeof(flow->dns_name), "%s", sqlite3_column_text(st,8) ? (const char *)sqlite3_column_text(st,8) : "");
    snprintf(flow->sni, sizeof(flow->sni), "%s", sqlite3_column_text(st,9) ? (const char *)sqlite3_column_text(st,9) : "");
    snprintf(flow->process_name, sizeof(flow->process_name), "%s", sqlite3_column_text(st,10) ? (const char *)sqlite3_column_text(st,10) : "");
    snprintf(flow->first_ts, sizeof(flow->first_ts), "%s", sqlite3_column_text(st,11) ? (const char *)sqlite3_column_text(st,11) : "");
    snprintf(flow->last_ts, sizeof(flow->last_ts), "%s", sqlite3_column_text(st,12) ? (const char *)sqlite3_column_text(st,12) : "");
    flow->packets = sqlite3_column_int64(st, 13);
    flow->bytes = sqlite3_column_int64(st, 14);
    flow->encrypted = sqlite3_column_int(st, 15);
    flow->probable_media = sqlite3_column_int(st, 16);
    flow->probable_control = sqlite3_column_int(st, 17);
    flow->hls_manifest = sqlite3_column_int(st, 18);
    flow->dash_manifest = sqlite3_column_int(st, 19);
    flow->websocket_media = sqlite3_column_int(st, 20);
    flow->pipeline_trigger = sqlite3_column_int(st, 21);
    snprintf(flow->vendor_dependency, sizeof(flow->vendor_dependency), "%s", sqlite3_column_text(st,22) ? (const char *)sqlite3_column_text(st,22) : "");
    snprintf(flow->suggested_integration_point, sizeof(flow->suggested_integration_point), "%s", sqlite3_column_text(st,23) ? (const char *)sqlite3_column_text(st,23) : "");
    snprintf(flow->payload_buffer_path, sizeof(flow->payload_buffer_path), "%s", sqlite3_column_text(st,24) ? (const char *)sqlite3_column_text(st,24) : "");
    sqlite3_finalize(st);
    return 0;
}

static int save_flow(sqlite3 *db, const char *capture_id, const BfWireFlow *flow) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO wire_flows(capture_id,flow_id,src_ip,dst_ip,src_port,dst_port,l4_proto,app_proto,media_kind,domain,dns_name,sni,process_name,first_ts,last_ts,packets,bytes,encrypted,probable_media,probable_control,hls_manifest,dash_manifest,websocket_media,pipeline_trigger,vendor_dependency,suggested_integration_point,payload_buffer_path)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, flow->flow_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, flow->src_ip, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 4, flow->dst_ip, -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 5, flow->src_port);
    sqlite3_bind_int(st, 6, flow->dst_port);
    sqlite3_bind_text(st, 7, flow->l4_proto, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 8, flow->app_proto, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 9, flow->media_kind, -1, SQLITE_STATIC);
    sqlite3_bind_text(st,10, flow->domain, -1, SQLITE_STATIC);
    sqlite3_bind_text(st,11, flow->dns_name, -1, SQLITE_STATIC);
    sqlite3_bind_text(st,12, flow->sni, -1, SQLITE_STATIC);
    sqlite3_bind_text(st,13, flow->process_name, -1, SQLITE_STATIC);
    sqlite3_bind_text(st,14, flow->first_ts, -1, SQLITE_STATIC);
    sqlite3_bind_text(st,15, flow->last_ts, -1, SQLITE_STATIC);
    sqlite3_bind_int64(st,16, flow->packets);
    sqlite3_bind_int64(st,17, flow->bytes);
    sqlite3_bind_int(st,18, flow->encrypted);
    sqlite3_bind_int(st,19, flow->probable_media);
    sqlite3_bind_int(st,20, flow->probable_control);
    sqlite3_bind_int(st,21, flow->hls_manifest);
    sqlite3_bind_int(st,22, flow->dash_manifest);
    sqlite3_bind_int(st,23, flow->websocket_media);
    sqlite3_bind_int(st,24, flow->pipeline_trigger);
    sqlite3_bind_text(st,25, flow->vendor_dependency, -1, SQLITE_STATIC);
    sqlite3_bind_text(st,26, flow->suggested_integration_point, -1, SQLITE_STATIC);
    sqlite3_bind_text(st,27, flow->payload_buffer_path, -1, SQLITE_STATIC);
    sqlite3_step(st);
    sqlite3_finalize(st);
    return 0;
}

static void vendor_from_domain(BfWireFlow *flow) {
    const char *d = flow->domain[0] ? flow->domain : (flow->sni[0] ? flow->sni : flow->dns_name);
    if (!d || !d[0]) return;
    if (contains_ci(d, "twilio")) snprintf(flow->vendor_dependency, sizeof(flow->vendor_dependency), "twilio");
    else if (contains_ci(d, "zoom")) snprintf(flow->vendor_dependency, sizeof(flow->vendor_dependency), "zoom");
    else if (contains_ci(d, "google") || contains_ci(d, "gstatic") || contains_ci(d, "googleapis")) snprintf(flow->vendor_dependency, sizeof(flow->vendor_dependency), "google");
    else if (contains_ci(d, "amazonaws") || contains_ci(d, "aws")) snprintf(flow->vendor_dependency, sizeof(flow->vendor_dependency), "aws");
    else if (contains_ci(d, "cloudflare")) snprintf(flow->vendor_dependency, sizeof(flow->vendor_dependency), "cloudflare");
    else if (contains_ci(d, "openai")) snprintf(flow->vendor_dependency, sizeof(flow->vendor_dependency), "openai");
    else if (contains_ci(d, "microsoft") || contains_ci(d, "teams")) snprintf(flow->vendor_dependency, sizeof(flow->vendor_dependency), "microsoft");
    else if (contains_ci(d, "fastly")) snprintf(flow->vendor_dependency, sizeof(flow->vendor_dependency), "fastly");
}

static void classify_payload(BfWireFlow *flow, const uint8_t *payload, size_t len) {
    char text[512];
    size_t n = len < sizeof(text) - 1 ? len : sizeof(text) - 1;
    memcpy(text, payload, n);
    text[n] = '\0';
    if ((flow->src_port == 5060 || flow->dst_port == 5060) &&
        (starts_with_ci(text, "INVITE ") || starts_with_ci(text, "REGISTER ") || contains_ci(text, "SIP/2.0"))) {
        snprintf(flow->app_proto, sizeof(flow->app_proto), "SIP");
        flow->probable_control = 1;
        snprintf(flow->suggested_integration_point, sizeof(flow->suggested_integration_point), "speech-loop");
    }
    if ((flow->src_port == 554 || flow->dst_port == 554) && contains_ci(text, "RTSP/1.")) {
        snprintf(flow->app_proto, sizeof(flow->app_proto), "RTSP");
        flow->probable_media = 1;
        snprintf(flow->media_kind, sizeof(flow->media_kind), "media_stream");
        snprintf(flow->suggested_integration_point, sizeof(flow->suggested_integration_point), "mediaprep");
    }
    if ((starts_with_ci(text, "GET ") || starts_with_ci(text, "POST ") || starts_with_ci(text, "HTTP/1.")) &&
        (contains_ci(text, "Host:") || contains_ci(text, "Content-Type:") || contains_ci(text, "Upgrade: websocket"))) {
        if (!flow->app_proto[0]) snprintf(flow->app_proto, sizeof(flow->app_proto), "HTTP");
        if (contains_ci(text, ".m3u8")) {
            flow->hls_manifest = 1;
            flow->probable_media = 1;
            snprintf(flow->media_kind, sizeof(flow->media_kind), "hls_manifest");
            snprintf(flow->suggested_integration_point, sizeof(flow->suggested_integration_point), "transcribe");
        }
        if (contains_ci(text, ".mpd")) {
            flow->dash_manifest = 1;
            flow->probable_media = 1;
            snprintf(flow->media_kind, sizeof(flow->media_kind), "dash_manifest");
            snprintf(flow->suggested_integration_point, sizeof(flow->suggested_integration_point), "transcribe");
        }
        if (contains_ci(text, "Upgrade: websocket") || contains_ci(text, "Sec-WebSocket-Key:")) {
            flow->websocket_media = 1;
            snprintf(flow->app_proto, sizeof(flow->app_proto), "WebSocket");
            if (!flow->media_kind[0]) snprintf(flow->media_kind, sizeof(flow->media_kind), "websocket_media");
            flow->probable_media = 1;
            snprintf(flow->suggested_integration_point, sizeof(flow->suggested_integration_point), "speech-loop");
        }
        if (contains_ci(text, "Content-Type: audio/") || contains_ci(text, "Content-Type: video/")) {
            flow->probable_media = 1;
            if (!flow->media_kind[0]) snprintf(flow->media_kind, sizeof(flow->media_kind), "http_media");
            if (!flow->suggested_integration_point[0]) snprintf(flow->suggested_integration_point, sizeof(flow->suggested_integration_point), "transcribe");
        }
        {
            char *host = strcasestr(text, "Host:");
            if (host) {
                host += 5;
                while (*host == ' ' || *host == '\t') host++;
                size_t i = 0;
                while (host[i] && host[i] != '\r' && host[i] != '\n' && i < sizeof(flow->domain) - 1) {
                    flow->domain[i] = host[i];
                    i++;
                }
                flow->domain[i] = '\0';
            }
        }
    }
    if (!flow->app_proto[0] && len >= 12 && (payload[0] & 0xC0) == 0x80 && flow->l4_proto[0] && strcmp(flow->l4_proto, "UDP") == 0) {
        snprintf(flow->app_proto, sizeof(flow->app_proto), "RTP");
        flow->probable_media = 1;
        snprintf(flow->media_kind, sizeof(flow->media_kind), "rtp");
        snprintf(flow->suggested_integration_point, sizeof(flow->suggested_integration_point), "speech-loop");
    }
    if (!flow->app_proto[0] && len >= 20 && be16(payload) < 0x4000 && be32(payload + 4) == 0x2112A442) {
        snprintf(flow->app_proto, sizeof(flow->app_proto), "STUN");
        flow->probable_control = 1;
        snprintf(flow->media_kind, sizeof(flow->media_kind), "webrtc_control");
        snprintf(flow->suggested_integration_point, sizeof(flow->suggested_integration_point), "speech-loop");
    }
    if ((flow->dst_port == 443 || flow->src_port == 443) && len >= 3 && payload[0] == 0x16 && payload[1] == 0x03) {
        flow->encrypted = 1;
        if (!flow->app_proto[0]) snprintf(flow->app_proto, sizeof(flow->app_proto), "TLS");
    }
}

static void classify_flow_finalize(BfWireFlow *flow) {
    vendor_from_domain(flow);
    if (flow->encrypted && flow->probable_media) {
        snprintf(flow->suggested_integration_point, sizeof(flow->suggested_integration_point), "app_integration");
    }
    if (!flow->app_proto[0] && flow->encrypted) snprintf(flow->app_proto, sizeof(flow->app_proto), "Encrypted");
    if (flow->probable_media && !flow->pipeline_trigger && !flow->encrypted) flow->pipeline_trigger = 1;
}

/* ================================================================
 * Device-level fingerprinting engine
 *
 * Sits above the flow model. Groups all observed src_ip values
 * into device profiles using:
 *   - Port patterns (RTSP=camera, MQTT=sensor, SIP=voip, ...)
 *   - Protocol fingerprints (RTP, STUN, WebSocket, ...)
 *   - Vendor pattern matching from DNS/SNI/domain
 *
 * Every device gets a bonfyre_chain suggestion that maps directly
 * to the bonfyre pipeline order. Operator ownership = full pipeline
 * access. This is what makes dumb devices first-class bonfyre nodes.
 * ================================================================ */

/* DNS/SNI/domain → vendor + device class pattern table. */
static const struct {
    const char *pattern;
    const char *vendor;
    const char *device_class;
} VENDOR_HINTS[] = {
    /* Cameras */
    {"hikvision",   "Hikvision",      "camera"},
    {"dahua",       "Dahua",          "camera"},
    {"axis",        "Axis",           "camera"},
    {"wyze",        "Wyze",           "camera"},
    {"ring",        "Ring",           "camera"},
    {"arlo",        "Arlo",           "camera"},
    {"reolink",     "Reolink",        "camera"},
    {"amcrest",     "Amcrest",        "camera"},
    {"hanwha",      "Hanwha",         "camera"},
    /* Smart speakers */
    {"sonos",       "Sonos",          "speaker"},
    {"bose",        "Bose",           "speaker"},
    {"echo",        "Amazon Echo",    "speaker"},
    {"alexa",       "Amazon Echo",    "speaker"},
    {"homepod",     "Apple HomePod",  "speaker"},
    /* Streaming */
    {"roku",        "Roku",           "streaming_device"},
    {"chromecast",  "Chromecast",     "streaming_device"},
    {"appletv",     "Apple TV",       "streaming_device"},
    {"firetv",      "Fire TV",        "streaming_device"},
    {"shield",      "Nvidia Shield",  "streaming_device"},
    /* Smart home / sensors */
    {"philips-hue", "Philips Hue",    "smart_home"},
    {"hue",         "Philips Hue",    "smart_home"},
    {"lutron",      "Lutron",         "smart_home"},
    {"smartthings", "SmartThings",    "smart_home"},
    {"particle",    "Particle",       "sensor"},
    {"arduino",     "Arduino",        "sensor"},
    {"raspberry",   "Raspberry Pi",   "sensor"},
    {"espressif",   "Espressif",      "sensor"},
    {"esp32",       "Espressif",      "sensor"},
    {"esp8266",     "Espressif",      "sensor"},
    /* VoIP / conferencing */
    {"twilio",      "Twilio",         "voip_service"},
    {"zoom",        "Zoom",           "voip_service"},
    {"webex",       "Webex",          "voip_service"},
    {"teams",       "Teams",          "voip_service"},
    {"vonage",      "Vonage",         "voip_service"},
    /* Media infrastructure */
    {"akamai",      "Akamai",         "media_cdn"},
    {"fastly",      "Fastly",         "media_cdn"},
    {"cloudfront",  "CloudFront",     "media_cdn"},
    {"youtube",     "YouTube",        "media_server"},
    {"netflix",     "Netflix",        "media_server"},
    {"spotify",     "Spotify",        "media_server"},
    {NULL, NULL, NULL}
};

/* Apply VENDOR_HINTS patterns; only fills fields that are still empty. */
static void vendor_hint_from_name(const char *name,
                                   char *vendor_out, size_t v_sz,
                                   char *class_out,  size_t c_sz) {
    int i;
    if (!name || !name[0]) return;
    for (i = 0; VENDOR_HINTS[i].pattern; i++) {
        if (contains_ci(name, VENDOR_HINTS[i].pattern)) {
            if (vendor_out && !vendor_out[0])
                snprintf(vendor_out, v_sz, "%s", VENDOR_HINTS[i].vendor);
            if (class_out && !class_out[0])
                snprintf(class_out,  c_sz, "%s", VENDOR_HINTS[i].device_class);
            return;
        }
    }
}

/* Check if a numeric port appears as a whole token in a CSV string. */
static int port_in_csv(const char *csv, int port) {
    char needle[16];
    const char *p;
    size_t n;
    if (!csv || !csv[0]) return 0;
    snprintf(needle, sizeof(needle), "%d", port);
    n = strlen(needle);
    p = csv;
    while ((p = strstr(p, needle)) != NULL) {
        int before = (p == csv || p[-1] == ',');
        int after  = (p[n] == ',' || p[n] == '\0');
        if (before && after) return 1;
        p++;
    }
    return 0;
}

/* Return device class string from port pattern, protocol fingerprint,
 * vendor hint, or domain. Checks in priority order. */
static const char *classify_device_class(const char *ports, const char *protos,
                                          const char *vendor, const char *domain) {
    if (ports && ports[0]) {
        if (port_in_csv(ports, 554)  || port_in_csv(ports, 8554))  return "camera";
        if (port_in_csv(ports, 5060) || port_in_csv(ports, 5061))  return "voip_phone";
        if (port_in_csv(ports, 1883) || port_in_csv(ports, 8883))  return "sensor";   /* MQTT */
        if (port_in_csv(ports, 5683))                               return "sensor";   /* CoAP */
        if (port_in_csv(ports, 502))                                return "industrial_device"; /* Modbus */
        if (port_in_csv(ports, 1900))                               return "smart_home";  /* SSDP */
        if (port_in_csv(ports, 5353))                               return "apple_device"; /* mDNS */
        if (port_in_csv(ports, 7000) || port_in_csv(ports, 7100))  return "airplay_device";
        if (port_in_csv(ports, 8009))                               return "chromecast";
        if (port_in_csv(ports, 9100))                               return "printer";
        if (port_in_csv(ports, 4070))                               return "streaming_device"; /* Spotify */
        if (port_in_csv(ports, 6881) || port_in_csv(ports, 6969))  return "streaming_device";
    }
    if (protos && protos[0]) {
        if (contains_ci(protos, "RTP") || contains_ci(protos, "RTSP")) return "media_device";
        if (contains_ci(protos, "SIP"))                                  return "voip_phone";
    }
    if (vendor && vendor[0]) {
        if (contains_ci(vendor, "ring")    || contains_ci(vendor, "hikvision") ||
            contains_ci(vendor, "axis")    || contains_ci(vendor, "dahua"))       return "camera";
        if (contains_ci(vendor, "sonos")   || contains_ci(vendor, "bose")     ||
            contains_ci(vendor, "echo")    || contains_ci(vendor, "alexa"))       return "speaker";
        if (contains_ci(vendor, "roku")    || contains_ci(vendor, "chromecast") ||
            contains_ci(vendor, "firetv")  || contains_ci(vendor, "appletv"))     return "streaming_device";
        if (contains_ci(vendor, "twilio")  || contains_ci(vendor, "zoom")     ||
            contains_ci(vendor, "webex"))                                          return "voip_service";
        if (contains_ci(vendor, "hue")     || contains_ci(vendor, "smartthings")) return "smart_home";
        if (contains_ci(vendor, "raspberry") || contains_ci(vendor, "arduino") ||
            contains_ci(vendor, "particle"))                                       return "sensor";
    }
    (void)domain;
    return "generic_iot";
}

/* Map a device profile to the optimal bonfyre pipeline chain.
 * This is the core of the pyramid: Wire discovery → pipeline suggestion
 * → stitch execution → full bonfyre run. */
static const char *device_bonfyre_chain(const char *device_class,
                                         int media_capable, int asr_candidate,
                                         int encrypted_only) {
    /* Encrypted-only: can't process media, metadata indexing only */
    if (encrypted_only) return "ingest,hash,index";
    /* VoIP / conferencing — full speech → value pipeline */
    if (asr_candidate ||
        (device_class && (strcmp(device_class, "voip_phone")   == 0 ||
                          strcmp(device_class, "voip_service")  == 0)))
        return "ingest,hash,media-prep,transcribe,clean,brief,proof,pack,distribute";
    /* Camera with media — full video-to-value pipeline */
    if (device_class && (strcmp(device_class, "camera") == 0 ||
                         strcmp(device_class, "media_device") == 0)) {
        if (media_capable)
            return "ingest,hash,media-prep,transcribe,clean,brief,proof,pack,distribute";
        return "ingest,hash,media-prep,compress,index,meter,ledger";
    }
    /* CDN / media server — compress + index + meter */
    if (device_class && (strcmp(device_class, "media_cdn")    == 0 ||
                         strcmp(device_class, "media_server")  == 0 ||
                         strcmp(device_class, "streaming_device") == 0 ||
                         strcmp(device_class, "airplay_device")   == 0 ||
                         strcmp(device_class, "chromecast")       == 0))
        return "ingest,hash,media-prep,compress,index,meter,ledger";
    /* Smart speaker — transcription pipeline */
    if (device_class && strcmp(device_class, "speaker") == 0)
        return "ingest,hash,media-prep,transcribe,clean,brief";
    /* IoT sensors / industrial — lightweight metadata + metering */
    if (device_class && (strcmp(device_class, "sensor")           == 0 ||
                         strcmp(device_class, "industrial_device") == 0 ||
                         strcmp(device_class, "smart_home")        == 0))
        return "ingest,hash,index,meter";
    /* Everything else: minimum viable pipeline */
    return "ingest,hash,index";
}

/* Aggregate wire_flows by src_ip into wire_devices for a given capture.
 * Idempotent — safe to call multiple times (INSERT OR REPLACE). */
static int build_device_table(sqlite3 *db, const char *capture_id) {
    sqlite3_stmt *st = NULL;
    const char *sql =
        "SELECT src_ip,"
        "  GROUP_CONCAT(DISTINCT CAST(src_port AS TEXT)) AS src_ports,"
        "  GROUP_CONCAT(DISTINCT CAST(dst_port AS TEXT)) AS dst_ports,"
        "  GROUP_CONCAT(DISTINCT NULLIF(app_proto,''))       AS protos,"
        "  GROUP_CONCAT(DISTINCT NULLIF(vendor_dependency,'')) AS vendors,"
        "  GROUP_CONCAT(DISTINCT NULLIF(domain,''))          AS domains,"
        "  GROUP_CONCAT(DISTINCT NULLIF(sni,''))             AS snis,"
        "  SUM(bytes)    AS total_bytes,"
        "  SUM(packets)  AS total_packets,"
        "  COUNT(*)      AS total_flows,"
        "  MAX(probable_media) AS has_media,"
        "  MAX(CASE WHEN app_proto IN ('RTP','SIP','RTSP','WebSocket') THEN 1 ELSE 0 END) AS asr_hint,"
        "  MIN(CASE WHEN encrypted=0 THEN 0 ELSE 1 END) AS all_encrypted,"
        "  MIN(first_ts) AS first_seen,"
        "  MAX(last_ts)  AS last_seen"
        " FROM wire_flows"
        " WHERE capture_id=? AND src_ip != '' AND src_ip NOT LIKE '%.0'"
        " GROUP BY src_ip";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        char all_ports[512], vendor_out[64], class_out[32], device_id[65], seed[320], chain_buf[256];
        sqlite3_stmt *ins = NULL;
        const char *chain;
        const char *ip         = sqlite3_column_text(st,0)  ? (const char *)sqlite3_column_text(st,0)  : "";
        const char *src_ports  = sqlite3_column_text(st,1)  ? (const char *)sqlite3_column_text(st,1)  : "";
        const char *dst_ports  = sqlite3_column_text(st,2)  ? (const char *)sqlite3_column_text(st,2)  : "";
        const char *protos     = sqlite3_column_text(st,3)  ? (const char *)sqlite3_column_text(st,3)  : "";
        const char *vendors    = sqlite3_column_text(st,4)  ? (const char *)sqlite3_column_text(st,4)  : "";
        const char *domains    = sqlite3_column_text(st,5)  ? (const char *)sqlite3_column_text(st,5)  : "";
        const char *snis       = sqlite3_column_text(st,6)  ? (const char *)sqlite3_column_text(st,6)  : "";
        long long total_bytes   = sqlite3_column_int64(st,7);
        long long total_packets = sqlite3_column_int64(st,8);
        long long total_flows   = sqlite3_column_int64(st,9);
        int has_media           = sqlite3_column_int(st,10);
        int asr_hint            = sqlite3_column_int(st,11);
        int all_encrypted       = sqlite3_column_int(st,12);
        const char *first_seen  = sqlite3_column_text(st,13) ? (const char *)sqlite3_column_text(st,13) : "";
        const char *last_seen   = sqlite3_column_text(st,14) ? (const char *)sqlite3_column_text(st,14) : "";
        /* Merge src+dst ports for port-pattern fingerprinting */
        snprintf(all_ports, sizeof(all_ports), "%s%s%s",
                 src_ports, (src_ports[0] && dst_ports[0]) ? "," : "", dst_ports);
        /* Vendor + class from DNS/SNI/domain pattern matching */
        vendor_out[0] = class_out[0] = '\0';
        vendor_hint_from_name(vendors, vendor_out, sizeof(vendor_out), class_out, sizeof(class_out));
        vendor_hint_from_name(domains, vendor_out, sizeof(vendor_out), class_out, sizeof(class_out));
        vendor_hint_from_name(snis,    vendor_out, sizeof(vendor_out), class_out, sizeof(class_out));
        if (!class_out[0]) {
            const char *c = classify_device_class(all_ports, protos, vendors, domains);
            snprintf(class_out, sizeof(class_out), "%s", c ? c : "generic_iot");
        }
        /* Bonfyre chain suggestion */
        chain = device_bonfyre_chain(class_out, has_media, asr_hint, all_encrypted);
        snprintf(chain_buf, sizeof(chain_buf), "%s", chain);
        /* Stable device_id: content-addressed on ip+ports+class+capture */
        snprintf(seed, sizeof(seed), "%s:%s:%s:%s", ip, all_ports, class_out, capture_id);
        bf_sha256_hex((const uint8_t *)seed, strlen(seed), device_id);
        /* Upsert device profile */
        if (sqlite3_prepare_v2(db,
            "INSERT OR REPLACE INTO wire_devices"
            "(device_id,capture_id,ip,vendor_hint,device_class,"
            " open_ports,protocol_fingerprint,media_capable,asr_candidate,"
            " encrypted_only,total_flows,total_bytes,total_packets,"
            " first_seen,last_seen,bonfyre_chain,artifact_id)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'')",
            -1, &ins, NULL) == SQLITE_OK) {
            sqlite3_bind_text(ins, 1,  device_id,   -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 2,  capture_id,  -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 3,  ip,          -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 4,  vendor_out,  -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 5,  class_out,   -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 6,  all_ports,   -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 7,  protos,      -1, SQLITE_STATIC);
            sqlite3_bind_int(ins,  8,  has_media);
            sqlite3_bind_int(ins,  9,  asr_hint);
            sqlite3_bind_int(ins,  10, all_encrypted);
            sqlite3_bind_int64(ins,11, total_flows);
            sqlite3_bind_int64(ins,12, total_bytes);
            sqlite3_bind_int64(ins,13, total_packets);
            sqlite3_bind_text(ins, 14, first_seen,  -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 15, last_seen,   -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 16, chain_buf,   -1, SQLITE_STATIC);
            sqlite3_step(ins);
            sqlite3_finalize(ins);
        }
    }
    sqlite3_finalize(st);
    return 0;
}

static int append_payload_buffer(const char *root, const char *capture_id, BfWireFlow *flow, const uint8_t *payload, size_t len) {
    char path[PATH_MAX];
    FILE *fp;
    if (flow->encrypted) return 1;
    if (!flow->payload_buffer_path[0]) {
        snprintf(path, sizeof(path), "%s/wire/buffers/%s_%s.bin", root, capture_id, flow->flow_id);
        snprintf(flow->payload_buffer_path, sizeof(flow->payload_buffer_path), "%s", path);
    }
    fp = fopen(flow->payload_buffer_path, "ab");
    if (!fp) return 1;
    fwrite(payload, 1, len, fp);
    fclose(fp);
    return 0;
}

static void ensure_flow_seed(BfWireFlow *flow) {
    char seed[512];
    if (flow->flow_id[0]) return;
    snprintf(seed, sizeof(seed), "%s:%d:%s:%d:%s", flow->src_ip, flow->src_port, flow->dst_ip, flow->dst_port, flow->l4_proto);
    bf_sha256_hex((const uint8_t *)seed, strlen(seed), flow->flow_id);
}

static void ensure_opaque_flow_seed(BfWireFlow *flow, const uint8_t *pkt, size_t len, const char *tag) {
    char digest[65];
    char seed[256];
    size_t prefix_len;
    if (flow->flow_id[0]) return;
    prefix_len = len < 48 ? len : 48;
    bf_sha256_hex(pkt, prefix_len, digest);
    snprintf(seed, sizeof(seed), "%s:%s:%lld", tag ? tag : "opaque", digest, (long long)len);
    bf_sha256_hex((const uint8_t *)seed, strlen(seed), flow->flow_id);
}

static int upsert_packet_observation(const char *root, sqlite3 *db, const char *capture_id, BfWireFlow *obs,
                                     const uint8_t *payload, size_t payload_len, int payload_requested) {
    BfWireFlow flow;
    if (!capture_id || !obs) return 1;
    ensure_flow_seed(obs);
    if (load_flow(db, capture_id, obs->flow_id, &flow) != 0) {
        memset(&flow, 0, sizeof(flow));
        memcpy(&flow, obs, sizeof(flow));
    } else {
        if (!flow.src_ip[0]) snprintf(flow.src_ip, sizeof(flow.src_ip), "%s", obs->src_ip);
        if (!flow.dst_ip[0]) snprintf(flow.dst_ip, sizeof(flow.dst_ip), "%s", obs->dst_ip);
        if (!flow.src_port) flow.src_port = obs->src_port;
        if (!flow.dst_port) flow.dst_port = obs->dst_port;
        if (!flow.l4_proto[0]) snprintf(flow.l4_proto, sizeof(flow.l4_proto), "%s", obs->l4_proto);
        if (!flow.first_ts[0]) snprintf(flow.first_ts, sizeof(flow.first_ts), "%s", obs->first_ts);
    }
    if (obs->domain[0] && !flow.domain[0]) snprintf(flow.domain, sizeof(flow.domain), "%s", obs->domain);
    if (obs->dns_name[0] && !flow.dns_name[0]) snprintf(flow.dns_name, sizeof(flow.dns_name), "%s", obs->dns_name);
    if (obs->sni[0] && !flow.sni[0]) snprintf(flow.sni, sizeof(flow.sni), "%s", obs->sni);
    if (obs->process_name[0] && !flow.process_name[0]) snprintf(flow.process_name, sizeof(flow.process_name), "%s", obs->process_name);
    if (obs->app_proto[0] && !flow.app_proto[0]) snprintf(flow.app_proto, sizeof(flow.app_proto), "%s", obs->app_proto);
    if (obs->media_kind[0] && !flow.media_kind[0]) snprintf(flow.media_kind, sizeof(flow.media_kind), "%s", obs->media_kind);
    if (obs->vendor_dependency[0] && !flow.vendor_dependency[0]) snprintf(flow.vendor_dependency, sizeof(flow.vendor_dependency), "%s", obs->vendor_dependency);
    if (obs->suggested_integration_point[0] && !flow.suggested_integration_point[0]) snprintf(flow.suggested_integration_point, sizeof(flow.suggested_integration_point), "%s", obs->suggested_integration_point);
    flow.encrypted = flow.encrypted || obs->encrypted;
    flow.probable_media = flow.probable_media || obs->probable_media;
    flow.probable_control = flow.probable_control || obs->probable_control;
    flow.hls_manifest = flow.hls_manifest || obs->hls_manifest;
    flow.dash_manifest = flow.dash_manifest || obs->dash_manifest;
    flow.websocket_media = flow.websocket_media || obs->websocket_media;
    flow.packets += 1;
    flow.bytes += obs->bytes;
    snprintf(flow.last_ts, sizeof(flow.last_ts), "%s", obs->last_ts);
    if (payload && payload_len) classify_payload(&flow, payload, payload_len);
    classify_flow_finalize(&flow);
    if (payload_requested && payload && payload_len && flow.probable_media && !flow.encrypted) {
        append_payload_buffer(root, capture_id, &flow, payload, payload_len);
    }
    return save_flow(db, capture_id, &flow);
}

static int parse_synthetic_line(const char *line, BfWireFlow *flow, uint8_t *payload, size_t *payload_len) {
    char tmp[1024];
    int size = 0, sport = 0, dport = 0;
    memset(flow, 0, sizeof(*flow));
    *payload_len = 0;
    json_extract_str(line, "src_ip", flow->src_ip, sizeof(flow->src_ip));
    json_extract_str(line, "dst_ip", flow->dst_ip, sizeof(flow->dst_ip));
    json_extract_int(line, "src_port", &sport);
    json_extract_int(line, "dst_port", &dport);
    json_extract_int(line, "size", &size);
    flow->src_port = sport;
    flow->dst_port = dport;
    flow->bytes = size > 0 ? size : 0;
    json_extract_str(line, "proto", flow->l4_proto, sizeof(flow->l4_proto));
    if (!flow->l4_proto[0]) snprintf(flow->l4_proto, sizeof(flow->l4_proto), "UDP");
    json_extract_str(line, "app_proto", flow->app_proto, sizeof(flow->app_proto));
    json_extract_str(line, "domain", flow->domain, sizeof(flow->domain));
    json_extract_str(line, "dns_name", flow->dns_name, sizeof(flow->dns_name));
    json_extract_str(line, "sni", flow->sni, sizeof(flow->sni));
    json_extract_str(line, "process_name", flow->process_name, sizeof(flow->process_name));
    json_extract_str(line, "ts", flow->first_ts, sizeof(flow->first_ts));
    snprintf(flow->last_ts, sizeof(flow->last_ts), "%s", flow->first_ts);
    if (strstr(line, "\"encrypted\":true")) flow->encrypted = 1;
    if (json_extract_str(line, "payload_ascii", tmp, sizeof(tmp)) == 0) {
        *payload_len = strlen(tmp);
        memcpy(payload, tmp, *payload_len);
    }
    if (json_extract_str(line, "hint", tmp, sizeof(tmp)) == 0) {
        if (contains_ci(tmp, "rtp")) { snprintf(flow->app_proto, sizeof(flow->app_proto), "RTP"); flow->probable_media = 1; snprintf(flow->media_kind, sizeof(flow->media_kind), "rtp"); }
        if (contains_ci(tmp, "sip")) { snprintf(flow->app_proto, sizeof(flow->app_proto), "SIP"); flow->probable_control = 1; }
        if (contains_ci(tmp, "hls")) { flow->hls_manifest = 1; flow->probable_media = 1; snprintf(flow->media_kind, sizeof(flow->media_kind), "hls_manifest"); }
        if (contains_ci(tmp, "dash")) { flow->dash_manifest = 1; flow->probable_media = 1; snprintf(flow->media_kind, sizeof(flow->media_kind), "dash_manifest"); }
        if (contains_ci(tmp, "webrtc") || contains_ci(tmp, "stun") || contains_ci(tmp, "turn") || contains_ci(tmp, "ice")) { snprintf(flow->app_proto, sizeof(flow->app_proto), "STUN"); flow->probable_control = 1; }
    }
    return 0;
}

static int ingest_synthetic_stream(FILE *in, const char *root, BfWireCapture *capture, int payload_requested) {
    sqlite3 *db = NULL;
    char db_path[PATH_MAX];
    char line[4096];
    if (open_wire_db(root, &db, db_path, sizeof(db_path)) != 0) return 1;
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    while (fgets(line, sizeof(line), in) != NULL) {
        BfWireFlow flow;
        uint8_t payload[1024];
        size_t payload_len = 0;
        parse_synthetic_line(line, &flow, payload, &payload_len);
        upsert_packet_observation(root, db, capture->capture_id, &flow, payload, payload_len, payload_requested);
        capture->packet_count += 1;
        capture->byte_count += flow.bytes;
    }
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    sqlite3_close(db);
    return 0;
}

static int parse_dns_qname(const uint8_t *payload, size_t len, char *out, size_t out_sz) {
    size_t pos = 12, w = 0;
    if (len < 13) return 1;
    out[0] = '\0';
    while (pos < len) {
        uint8_t l = payload[pos++];
        if (l == 0) break;
        if ((l & 0xC0) != 0 || pos + l > len) return 1;
        if (w && w < out_sz - 1) out[w++] = '.';
        for (uint8_t i = 0; i < l && w < out_sz - 1; i++) out[w++] = (char)payload[pos + i];
        pos += l;
    }
    out[w] = '\0';
    return out[0] ? 0 : 1;
}

static int ingest_pcap_stream(FILE *fp, const char *root, BfWireCapture *capture, int payload_requested) {
    PcapGlobalHeader gh;
    sqlite3 *db = NULL;
    char db_path[PATH_MAX];
    int swapped = 0;
    if (fread(&gh, 1, sizeof(gh), fp) != sizeof(gh)) return 1;
    if (gh.magic == 0xd4c3b2a1u) swapped = 0;
    else if (gh.magic == 0xa1b2c3d4u) swapped = 1;
    else return 1;
    if (open_wire_db(root, &db, db_path, sizeof(db_path)) != 0) return 1;
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    for (;;) {
        PcapPacketHeader ph;
        uint8_t *pkt = NULL;
        uint32_t incl_len;
        if (fread(&ph, 1, sizeof(ph), fp) != sizeof(ph)) break;
        incl_len = swapped ? __builtin_bswap32(ph.incl_len) : ph.incl_len;
        pkt = (uint8_t *)malloc(incl_len);
        if (!pkt) break;
        if (fread(pkt, 1, incl_len, fp) != incl_len) { free(pkt); break; }
        if (incl_len >= 14 && be16(pkt + 12) == 0x0800 && incl_len >= 34) {
            uint8_t ihl = (pkt[14] & 0x0F) * 4;
            uint8_t proto = pkt[23];
            BfWireFlow flow;
            uint8_t *l4 = pkt + 14 + ihl;
            uint8_t *payload = NULL;
            size_t payload_len = 0;
            memset(&flow, 0, sizeof(flow));
            inet_ntop(AF_INET, pkt + 26, flow.src_ip, sizeof(flow.src_ip));
            inet_ntop(AF_INET, pkt + 30, flow.dst_ip, sizeof(flow.dst_ip));
            snprintf(flow.first_ts, sizeof(flow.first_ts), "%u.%06u", swapped ? __builtin_bswap32(ph.ts_sec) : ph.ts_sec,
                     swapped ? __builtin_bswap32(ph.ts_usec) : ph.ts_usec);
            snprintf(flow.last_ts, sizeof(flow.last_ts), "%s", flow.first_ts);
            flow.bytes = incl_len;
            if (proto == 17 && incl_len >= (size_t)(14 + ihl + 8)) {
                snprintf(flow.l4_proto, sizeof(flow.l4_proto), "UDP");
                flow.src_port = be16(l4);
                flow.dst_port = be16(l4 + 2);
                payload = l4 + 8;
                payload_len = incl_len - (size_t)(14 + ihl + 8);
                if (flow.src_port == 53 || flow.dst_port == 53) {
                    parse_dns_qname(payload, payload_len, flow.dns_name, sizeof(flow.dns_name));
                }
            } else if (proto == 6 && incl_len >= (size_t)(14 + ihl + 20)) {
                uint8_t off = ((l4[12] >> 4) & 0x0F) * 4;
                snprintf(flow.l4_proto, sizeof(flow.l4_proto), "TCP");
                flow.src_port = be16(l4);
                flow.dst_port = be16(l4 + 2);
                if (incl_len >= (size_t)(14 + ihl + off)) {
                    payload = l4 + off;
                    payload_len = incl_len - (size_t)(14 + ihl + off);
                }
            }
            upsert_packet_observation(root, db, capture->capture_id, &flow, payload, payload_len, payload_requested);
            capture->packet_count += 1;
            capture->byte_count += incl_len;
        } else {
            BfWireFlow flow;
            memset(&flow, 0, sizeof(flow));
            snprintf(flow.l4_proto, sizeof(flow.l4_proto), "RAW");
            snprintf(flow.app_proto, sizeof(flow.app_proto), "opaque_frame");
            snprintf(flow.media_kind, sizeof(flow.media_kind), "unsupported_l2");
            snprintf(flow.first_ts, sizeof(flow.first_ts), "%u.%06u", swapped ? __builtin_bswap32(ph.ts_sec) : ph.ts_sec,
                     swapped ? __builtin_bswap32(ph.ts_usec) : ph.ts_usec);
            snprintf(flow.last_ts, sizeof(flow.last_ts), "%s", flow.first_ts);
            flow.bytes = incl_len;
            if (incl_len < 14) {
                snprintf(flow.suggested_integration_point, sizeof(flow.suggested_integration_point), "wire_metadata_only");
                ensure_opaque_flow_seed(&flow, pkt, incl_len, "short_frame");
            } else {
                uint16_t ethertype = be16(pkt + 12);
                char kind[64];
                snprintf(kind, sizeof(kind), "ethertype_0x%04x", (unsigned)ethertype);
                snprintf(flow.suggested_integration_point, sizeof(flow.suggested_integration_point), "wire_metadata_only");
                snprintf(flow.domain, sizeof(flow.domain), "%s", kind);
                ensure_opaque_flow_seed(&flow, pkt, incl_len, kind);
            }
            upsert_packet_observation(root, db, capture->capture_id, &flow, NULL, 0, 0);
            capture->packet_count += 1;
            capture->byte_count += incl_len;
        }
        free(pkt);
    }
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    sqlite3_close(db);
    return 0;
}

int bf_wire_ingest_file(const char *root, BfWireCapture *capture, const char *path, int payload_requested) {
    FILE *fp;
    char start[4] = {0};
    if (!path || !capture) return 1;
    fp = fopen(path, "rb");
    if (!fp) return 1;
    if (fread(start, 1, 3, fp) < 1) { fclose(fp); return 1; }
    rewind(fp);
    if (start[0] == '{') {
        int rc = ingest_synthetic_stream(fp, root, capture, payload_requested);
        fclose(fp);
        return rc;
    }
    {
        int rc = ingest_pcap_stream(fp, root, capture, payload_requested);
        fclose(fp);
        return rc;
    }
}

int bf_wire_ingest_stream(FILE *in, const char *root, BfWireCapture *capture, int payload_requested) {
    return ingest_synthetic_stream(in, root, capture, payload_requested);
}

static int foreach_flow(const char *root, const char *capture_id, int (*cb)(sqlite3_stmt *, void *), void *ctx) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char db_path[PATH_MAX];
    int rc = 1;
    if (open_wire_db(root, &db, db_path, sizeof(db_path)) != 0) return 1;
    if (sqlite3_prepare_v2(db,
        "SELECT flow_id,src_ip,dst_ip,src_port,dst_port,l4_proto,app_proto,media_kind,domain,dns_name,sni,process_name,first_ts,last_ts,packets,bytes,encrypted,probable_media,probable_control,hls_manifest,dash_manifest,websocket_media,pipeline_trigger,vendor_dependency,suggested_integration_point,payload_buffer_path "
        "FROM wire_flows WHERE capture_id=? ORDER BY bytes DESC", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
    rc = 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        if (cb(st, ctx) != 0) { rc = 1; break; }
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return rc;
}

static int cb_print_flows(sqlite3_stmt *st, void *ctx) {
    int *first = (int *)ctx;
    if (!*first) printf(",");
    *first = 0;
    printf("{\"flow_id\":"); json_escape_print((const char *)sqlite3_column_text(st, 0));
    printf(",\"src_ip\":"); json_escape_print((const char *)sqlite3_column_text(st, 1));
    printf(",\"dst_ip\":"); json_escape_print((const char *)sqlite3_column_text(st, 2));
    printf(",\"src_port\":%d,\"dst_port\":%d", sqlite3_column_int(st,3), sqlite3_column_int(st,4));
    printf(",\"l4_proto\":"); json_escape_print((const char *)sqlite3_column_text(st, 5));
    printf(",\"app_proto\":"); json_escape_print((const char *)sqlite3_column_text(st, 6));
    printf(",\"media_kind\":"); json_escape_print((const char *)sqlite3_column_text(st, 7));
    printf(",\"domain\":"); json_escape_print((const char *)sqlite3_column_text(st, 8));
    printf(",\"bytes\":%lld,\"packets\":%lld", sqlite3_column_int64(st,15), sqlite3_column_int64(st,14));
    printf(",\"encrypted\":%s,\"probable_media\":%s,\"probable_control\":%s",
           sqlite3_column_int(st,16) ? "true" : "false",
           sqlite3_column_int(st,17) ? "true" : "false",
           sqlite3_column_int(st,18) ? "true" : "false");
    printf(",\"vendor_dependency\":"); json_escape_print((const char *)sqlite3_column_text(st, 23));
    printf(",\"suggested_integration_point\":"); json_escape_print((const char *)sqlite3_column_text(st, 24));
    printf("}");
    return 0;
}

int bf_wire_emit_flows_json(const char *root, const char *capture_id) {
    int first = 1;
    printf("{\"capture_id\":"); json_escape_print(capture_id);
    printf(",\"flows\":[");
    foreach_flow(root, capture_id, cb_print_flows, &first);
    printf("]}\n");
    return 0;
}

typedef struct {
    int first;
    int media_count;
} MediaCtx;

static int cb_media(sqlite3_stmt *st, void *ctxv) {
    MediaCtx *ctx = (MediaCtx *)ctxv;
    if (!sqlite3_column_int(st, 17) && !sqlite3_column_int(st, 18)) return 0;
    if (!ctx->first) printf(",");
    ctx->first = 0;
    ctx->media_count++;
    printf("{\"flow_id\":"); json_escape_print((const char *)sqlite3_column_text(st,0));
    printf(",\"app_proto\":"); json_escape_print((const char *)sqlite3_column_text(st,6));
    printf(",\"media_kind\":"); json_escape_print((const char *)sqlite3_column_text(st,7));
    printf(",\"encrypted\":%s", sqlite3_column_int(st,16) ? "true" : "false");
    printf(",\"bytes_seen\":%lld", sqlite3_column_int64(st,15));
    printf(",\"duration_estimate_seconds\":%.3f", sqlite3_column_int64(st,14) > 0 ? (double)sqlite3_column_int64(st,14) / 50.0 : 0.0);
    printf(",\"vendor_dependency\":"); json_escape_print((const char *)sqlite3_column_text(st,23));
    printf(",\"suggested_integration_point\":"); json_escape_print((const char *)sqlite3_column_text(st,24));
    printf(",\"events\":[");
    printf("\"media_flow_detected\"");
    if (sqlite3_column_int(st,17)) printf(",\"probable_asr_candidate\"");
    printf(",\"bytes_seen\",\"duration_estimate\"");
    if (((const char *)sqlite3_column_text(st,23)) && ((const char *)sqlite3_column_text(st,23))[0]) printf(",\"vendor_dependency\"");
    if (((const char *)sqlite3_column_text(st,24)) && ((const char *)sqlite3_column_text(st,24))[0]) printf(",\"suggested_integration_point\"");
    printf("]}");
    return 0;
}

int bf_wire_emit_media_detect_json(const char *root, const char *capture_id) {
    MediaCtx ctx = {1, 0};
    printf("{\"capture_id\":"); json_escape_print(capture_id);
    printf(",\"media_candidates\":[");
    foreach_flow(root, capture_id, cb_media, &ctx);
    printf("],\"count\":%d}\n", ctx.media_count);
    return 0;
}

static int upsert_json_record(sqlite3 *db, const char *table, const char *capture_id, const char *flow_id, const char *json) {
    sqlite3_stmt *st = NULL;
    char digest[65];
    char sql[256];
    bf_sha256_hex((const uint8_t *)json, strlen(json), digest);
    snprintf(sql, sizeof(sql), "INSERT OR REPLACE INTO %s(capture_id,flow_id,%s,record_json) VALUES(?,?,?,?)",
             table, strcmp(table, "wire_scaling") == 0 ? "event_hash" : (strcmp(table, "wire_route") == 0 ? "event_hash" : "record_hash"));
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, flow_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, digest, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 4, json, -1, SQLITE_STATIC);
    sqlite3_step(st);
    sqlite3_finalize(st);
    return 0;
}

int bf_wire_emit_meter_json(const char *root, const char *capture_id) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char db_path[PATH_MAX];
    int first = 1;
    if (open_wire_db(root, &db, db_path, sizeof(db_path)) != 0) return 1;
    if (sqlite3_prepare_v2(db, "SELECT flow_id,bytes,packets,l4_proto,app_proto,domain,vendor_dependency,suggested_integration_point FROM wire_flows WHERE capture_id=? ORDER BY bytes DESC", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
    printf("{\"capture_id\":"); json_escape_print(capture_id); printf(",\"meter_records\":[");
    while (sqlite3_step(st) == SQLITE_ROW) {
        char json[1024], digest[65];
        const char *flow_id = (const char *)sqlite3_column_text(st,0);
        snprintf(json, sizeof(json),
                 "{\"flow_id\":\"%s\",\"protocol\":\"%s\",\"app_proto\":\"%s\",\"byte_count\":%lld,\"packet_count\":%lld,\"duration_estimate_seconds\":%.3f,\"artifact_hash_basis\":\"metadata_only\",\"pipeline_run_id\":\"wire:%s\",\"integration\":[\"meter\",\"ledger\",\"economy\",\"tier\",\"pay\"]}",
                 flow_id ? flow_id : "",
                 sqlite3_column_text(st,3) ? (const char *)sqlite3_column_text(st,3) : "",
                 sqlite3_column_text(st,4) ? (const char *)sqlite3_column_text(st,4) : "",
                 sqlite3_column_int64(st,1), sqlite3_column_int64(st,2),
                 sqlite3_column_int64(st,2) / 50.0,
                 capture_id);
        bf_sha256_hex((const uint8_t *)json, strlen(json), digest);
        upsert_json_record(db, "wire_meter", capture_id, flow_id ? flow_id : "", json);
        if (!first) printf(",");
        first = 0;
        printf("{\"record_hash\":\"%s\",\"record\":%s}", digest, json);
    }
    printf("]}\n");
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

int bf_wire_emit_scale_json(const char *root, const char *capture_id) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char db_path[PATH_MAX];
    int first = 1;
    if (open_wire_db(root, &db, db_path, sizeof(db_path)) != 0) return 1;
    if (sqlite3_prepare_v2(db, "SELECT flow_id,bytes,packets,app_proto,probable_media,encrypted,hls_manifest,dash_manifest,websocket_media FROM wire_flows WHERE capture_id=?", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
    printf("{\"capture_id\":"); json_escape_print(capture_id); printf(",\"scaling_events\":[");
    while (sqlite3_step(st) == SQLITE_ROW) {
        char json[1024];
        const char *flow_id = (const char *)sqlite3_column_text(st,0);
        long long bytes = sqlite3_column_int64(st,1);
        int packets = sqlite3_column_int(st,2);
        int encrypted = sqlite3_column_int(st,5);
        int probable_media = sqlite3_column_int(st,4);
        const char *mode = bytes > 5000000 ? "batch_mode" : "low_latency_mode";
        const char *model = encrypted ? "tiny_model_mode" : "high_accuracy_model_mode";
        int asr_workers = probable_media ? (bytes > 1000000 ? 4 : 1) : 0;
        int compress_workers = bytes > 2000000 ? 2 : 0;
        int embedding_workers = probable_media && packets > 100 ? 1 : 0;
        int transcode_workers = sqlite3_column_int(st,6) || sqlite3_column_int(st,7) ? 1 : 0;
        snprintf(json, sizeof(json),
                 "{\"flow_id\":\"%s\",\"signals\":[\"%s\",\"%s\"],\"asr_workers_needed\":%d,\"compression_workers_needed\":%d,\"embedding_workers_needed\":%d,\"transcode_workers_needed\":%d,\"integration\":[\"queue\",\"runtime\",\"pipeline\",\"flow\",\"swarm\",\"precision\"]}",
                 flow_id ? flow_id : "", mode, model, asr_workers, compress_workers, embedding_workers, transcode_workers);
        upsert_json_record(db, "wire_scaling", capture_id, flow_id ? flow_id : "", json);
        if (!first) printf(",");
        first = 0;
        printf("%s", json);
    }
    printf("]}\n");
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

int bf_wire_emit_route_json(const char *root, const char *capture_id) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char db_path[PATH_MAX];
    int first = 1;
    if (open_wire_db(root, &db, db_path, sizeof(db_path)) != 0) return 1;
    if (sqlite3_prepare_v2(db, "SELECT flow_id,app_proto,media_kind,encrypted,probable_media,hls_manifest,dash_manifest,websocket_media,suggested_integration_point FROM wire_flows WHERE capture_id=?", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 1;
    }
    sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
    printf("{\"capture_id\":"); json_escape_print(capture_id); printf(",\"routes\":[");
    while (sqlite3_step(st) == SQLITE_ROW) {
        char json[1024];
        const char *flow_id = (const char *)sqlite3_column_text(st,0);
        const char *route = "segment";
        if (sqlite3_column_int(st,3)) route = "app_integration";
        else if (sqlite3_column_int(st,6) || sqlite3_column_int(st,7)) route = "transcribe";
        else if (sqlite3_column_int(st,4) && sqlite3_column_text(st,1) && contains_ci((const char *)sqlite3_column_text(st,1), "rtp")) route = "speech-loop";
        else if (sqlite3_column_int(st,4)) route = "transcribe";
        snprintf(json, sizeof(json),
                 "{\"flow_id\":\"%s\",\"smallest_sufficient_local\":\"%s\",\"fallback_chain\":[\"segment\",\"speech-loop\",\"transcribe\",\"clean\",\"paragraph\",\"brief\",\"proof\",\"offer\",\"narrate\",\"pack\",\"distribute\"],\"integration\":[\"model\",\"gen\",\"segment\",\"speech-loop\",\"precision\",\"capabilities\"]}",
                 flow_id ? flow_id : "", route);
        upsert_json_record(db, "wire_route", capture_id, flow_id ? flow_id : "", json);
        if (!first) printf(",");
        first = 0;
        printf("%s", json);
    }
    printf("]}\n");
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

int bf_wire_write_report(const char *root, const char *capture_id) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char db_path[PATH_MAX], report_dir[PATH_MAX], path[PATH_MAX];
    FILE *flows_fp = NULL, *scaling_fp = NULL, *media_fp = NULL, *meter_fp = NULL;
    if (open_wire_db(root, &db, db_path, sizeof(db_path)) != 0) return 1;
    snprintf(report_dir, sizeof(report_dir), "%s/wire/reports/%s", root, capture_id);
    if (bf_ensure_dir(report_dir) != 0) { sqlite3_close(db); return 1; }
    snprintf(path, sizeof(path), "%s/wire_flows.jsonl", report_dir);
    flows_fp = fopen(path, "wb");
    if (!flows_fp) { sqlite3_close(db); return 1; }
    sqlite3_prepare_v2(db, "SELECT flow_id,src_ip,dst_ip,src_port,dst_port,l4_proto,app_proto,media_kind,bytes,encrypted,probable_media,vendor_dependency,suggested_integration_point FROM wire_flows WHERE capture_id=? ORDER BY bytes DESC", -1, &st, NULL);
    sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        fprintf(flows_fp,
                "{\"flow_id\":\"%s\",\"src_ip\":\"%s\",\"dst_ip\":\"%s\",\"src_port\":%d,\"dst_port\":%d,\"l4_proto\":\"%s\",\"app_proto\":\"%s\",\"media_kind\":\"%s\",\"bytes\":%lld,\"encrypted\":%s,\"probable_media\":%s,\"vendor_dependency\":\"%s\",\"suggested_integration_point\":\"%s\"}\n",
                sqlite3_column_text(st,0) ? (const char *)sqlite3_column_text(st,0) : "",
                sqlite3_column_text(st,1) ? (const char *)sqlite3_column_text(st,1) : "",
                sqlite3_column_text(st,2) ? (const char *)sqlite3_column_text(st,2) : "",
                sqlite3_column_int(st,3), sqlite3_column_int(st,4),
                sqlite3_column_text(st,5) ? (const char *)sqlite3_column_text(st,5) : "",
                sqlite3_column_text(st,6) ? (const char *)sqlite3_column_text(st,6) : "",
                sqlite3_column_text(st,7) ? (const char *)sqlite3_column_text(st,7) : "",
                sqlite3_column_int64(st,8),
                sqlite3_column_int(st,9) ? "true" : "false",
                sqlite3_column_int(st,10) ? "true" : "false",
                sqlite3_column_text(st,11) ? (const char *)sqlite3_column_text(st,11) : "",
                sqlite3_column_text(st,12) ? (const char *)sqlite3_column_text(st,12) : "");
    }
    sqlite3_finalize(st);
    fclose(flows_fp);

    snprintf(path, sizeof(path), "%s/media_candidates.json", report_dir);
    media_fp = fopen(path, "wb");
    if (media_fp) {
        int first = 1;
        sqlite3_prepare_v2(db, "SELECT flow_id,app_proto,media_kind,encrypted,bytes,vendor_dependency,suggested_integration_point,probable_media,probable_control,packets FROM wire_flows WHERE capture_id=? ORDER BY bytes DESC", -1, &st, NULL);
        sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
        fprintf(media_fp, "{\"capture_id\":\"%s\",\"media_candidates\":[", capture_id);
        while (sqlite3_step(st) == SQLITE_ROW) {
            if (!sqlite3_column_int(st,7) && !sqlite3_column_int(st,8)) continue;
            if (!first) fputc(',', media_fp);
            first = 0;
            fprintf(media_fp,
                    "{\"flow_id\":\"%s\",\"app_proto\":\"%s\",\"media_kind\":\"%s\",\"encrypted\":%s,\"bytes_seen\":%lld,\"duration_estimate_seconds\":%.3f,\"vendor_dependency\":\"%s\",\"suggested_integration_point\":\"%s\"}",
                    sqlite3_column_text(st,0) ? (const char *)sqlite3_column_text(st,0) : "",
                    sqlite3_column_text(st,1) ? (const char *)sqlite3_column_text(st,1) : "",
                    sqlite3_column_text(st,2) ? (const char *)sqlite3_column_text(st,2) : "",
                    sqlite3_column_int(st,3) ? "true" : "false",
                    sqlite3_column_int64(st,4),
                    sqlite3_column_int64(st,9) / 50.0,
                    sqlite3_column_text(st,5) ? (const char *)sqlite3_column_text(st,5) : "",
                    sqlite3_column_text(st,6) ? (const char *)sqlite3_column_text(st,6) : "");
        }
        fprintf(media_fp, "]}");
        sqlite3_finalize(st);
        fclose(media_fp);
    }
    snprintf(path, sizeof(path), "%s/usage_meter.json", report_dir);
    meter_fp = fopen(path, "wb");
    if (meter_fp) {
        int first = 1;
        sqlite3_prepare_v2(db, "SELECT flow_id,bytes,packets,l4_proto,app_proto FROM wire_flows WHERE capture_id=? ORDER BY bytes DESC", -1, &st, NULL);
        sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
        fprintf(meter_fp, "{\"capture_id\":\"%s\",\"meter_records\":[", capture_id);
        while (sqlite3_step(st) == SQLITE_ROW) {
            char json[1024], digest[65];
            snprintf(json, sizeof(json),
                     "{\"flow_id\":\"%s\",\"protocol\":\"%s\",\"app_proto\":\"%s\",\"byte_count\":%lld,\"packet_count\":%lld,\"duration_estimate_seconds\":%.3f,\"artifact_hash_basis\":\"metadata_only\",\"pipeline_run_id\":\"wire:%s\",\"integration\":[\"meter\",\"ledger\",\"economy\",\"tier\",\"pay\"]}",
                     sqlite3_column_text(st,0) ? (const char *)sqlite3_column_text(st,0) : "",
                     sqlite3_column_text(st,3) ? (const char *)sqlite3_column_text(st,3) : "",
                     sqlite3_column_text(st,4) ? (const char *)sqlite3_column_text(st,4) : "",
                     sqlite3_column_int64(st,1), sqlite3_column_int64(st,2),
                     sqlite3_column_int64(st,2) / 50.0, capture_id);
            bf_sha256_hex((const uint8_t *)json, strlen(json), digest);
            if (!first) fputc(',', meter_fp);
            first = 0;
            fprintf(meter_fp, "{\"record_hash\":\"%s\",\"record\":%s}", digest, json);
        }
        fprintf(meter_fp, "]}");
        sqlite3_finalize(st);
        fclose(meter_fp);
    }
    snprintf(path, sizeof(path), "%s/scaling_events.jsonl", report_dir);
    scaling_fp = fopen(path, "wb");
    if (scaling_fp) {
        sqlite3_prepare_v2(db, "SELECT event_json FROM wire_scaling WHERE capture_id=?", -1, &st, NULL);
        sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW) fprintf(scaling_fp, "%s\n", sqlite3_column_text(st,0) ? (const char *)sqlite3_column_text(st,0) : "{}");
        sqlite3_finalize(st);
        fclose(scaling_fp);
    }
    snprintf(path, sizeof(path), "%s/dependency_map.json", report_dir);
    sqlite3_prepare_v2(db, "SELECT flow_id,domain,sni,dns_name,vendor_dependency,encrypted FROM wire_flows WHERE capture_id=? ORDER BY bytes DESC", -1, &st, NULL);
    sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
    {
        FILE *fp = fopen(path, "wb");
        int first = 1;
        fprintf(fp, "{\"capture_id\":\"%s\",\"dependencies\":[", capture_id);
        while (sqlite3_step(st) == SQLITE_ROW) {
            if (!first) fputc(',', fp);
            first = 0;
            fprintf(fp, "{\"flow_id\":\"%s\",\"domain\":\"%s\",\"sni\":\"%s\",\"dns_name\":\"%s\",\"vendor_dependency\":\"%s\",\"encrypted\":%s}",
                    sqlite3_column_text(st,0) ? (const char *)sqlite3_column_text(st,0) : "",
                    sqlite3_column_text(st,1) ? (const char *)sqlite3_column_text(st,1) : "",
                    sqlite3_column_text(st,2) ? (const char *)sqlite3_column_text(st,2) : "",
                    sqlite3_column_text(st,3) ? (const char *)sqlite3_column_text(st,3) : "",
                    sqlite3_column_text(st,4) ? (const char *)sqlite3_column_text(st,4) : "",
                    sqlite3_column_int(st,5) ? "true" : "false");
        }
        fprintf(fp, "]}");
        fclose(fp);
    }
    sqlite3_finalize(st);
    snprintf(path, sizeof(path), "%s/sovereignty_report.md", report_dir);
    {
        FILE *fp = fopen(path, "wb");
        int dep_count = 0, media_count = 0, enc_count = 0;
        sqlite3_prepare_v2(db, "SELECT count(*), sum(probable_media), sum(encrypted) FROM wire_flows WHERE capture_id=?", -1, &st, NULL);
        sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            dep_count = sqlite3_column_int(st,0);
            media_count = sqlite3_column_int(st,1);
            enc_count = sqlite3_column_int(st,2);
        }
        sqlite3_finalize(st);
        fprintf(fp, "# Bonfyre Wire Sovereignty Report\n\n");
        fprintf(fp, "- capture_id: `%s`\n", capture_id);
        fprintf(fp, "- metadata_only_source_of_truth: yes\n");
        fprintf(fp, "- flows_seen: %d\n", dep_count);
        fprintf(fp, "- media_candidates: %d\n", media_count);
        fprintf(fp, "- encrypted_flows: %d\n", enc_count);
        fprintf(fp, "- decryption_bypass: no\n");
        fprintf(fp, "- credential_capture: no\n");
        fprintf(fp, "- suggested_posture: prefer local first, app-level integrations for encrypted vendors, and pipeline triggering only on authorized unencrypted media.\n");
        fclose(fp);
    }
    sqlite3_close(db);
    printf("{\"capture_id\":\"%s\",\"report_dir\":\"%s\"}\n", capture_id, report_dir);
    return 0;
}

/* ================================================================
 * bf_wire_probe — deep device fingerprinting
 *
 * Aggregates all flows by src_ip → device profiles → JSON manifest.
 * Uses libbonfyre's classify chain to suggest the exact pipeline
 * each device feeds into. Output is immediately actionable.
 * ================================================================ */
int bf_wire_probe(const char *root, const char *capture_id) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char db_path[PATH_MAX];
    int first_item = 1, device_count = 0;
    if (open_wire_db(root, &db, db_path, sizeof(db_path)) != 0) return 1;
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    build_device_table(db, capture_id);
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    if (sqlite3_prepare_v2(db,
        "SELECT device_id,ip,vendor_hint,device_class,open_ports,protocol_fingerprint,"
        "  media_capable,asr_candidate,encrypted_only,"
        "  total_flows,total_bytes,total_packets,"
        "  first_seen,last_seen,bonfyre_chain"
        " FROM wire_devices WHERE capture_id=? ORDER BY total_bytes DESC",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db); return 1;
    }
    sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
    printf("{\"capture_id\":"); json_escape_print(capture_id);
    printf(",\"devices\":[");
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *dev_id   = sqlite3_column_text(st,0)  ? (const char *)sqlite3_column_text(st,0)  : "";
        const char *ip       = sqlite3_column_text(st,1)  ? (const char *)sqlite3_column_text(st,1)  : "";
        const char *vendor   = sqlite3_column_text(st,2)  ? (const char *)sqlite3_column_text(st,2)  : "";
        const char *class_   = sqlite3_column_text(st,3)  ? (const char *)sqlite3_column_text(st,3)  : "";
        const char *ports    = sqlite3_column_text(st,4)  ? (const char *)sqlite3_column_text(st,4)  : "";
        const char *protos   = sqlite3_column_text(st,5)  ? (const char *)sqlite3_column_text(st,5)  : "";
        int  media           = sqlite3_column_int(st,6);
        int  asr             = sqlite3_column_int(st,7);
        int  enc             = sqlite3_column_int(st,8);
        long long flows      = sqlite3_column_int64(st,9);
        long long bytes      = sqlite3_column_int64(st,10);
        long long pkts       = sqlite3_column_int64(st,11);
        const char *ts_first = sqlite3_column_text(st,12) ? (const char *)sqlite3_column_text(st,12) : "";
        const char *ts_last  = sqlite3_column_text(st,13) ? (const char *)sqlite3_column_text(st,13) : "";
        const char *chain    = sqlite3_column_text(st,14) ? (const char *)sqlite3_column_text(st,14) : "";
        if (!first_item) printf(",");
        first_item = 0;
        device_count++;
        printf("{\"device_id\":");             json_escape_print(dev_id);
        printf(",\"ip\":");                    json_escape_print(ip);
        printf(",\"vendor_hint\":");           json_escape_print(vendor);
        printf(",\"device_class\":");          json_escape_print(class_);
        printf(",\"open_ports\":");            json_escape_print(ports);
        printf(",\"protocol_fingerprint\":"); json_escape_print(protos);
        printf(",\"media_capable\":%s",        media ? "true" : "false");
        printf(",\"asr_candidate\":%s",        asr   ? "true" : "false");
        printf(",\"encrypted_only\":%s",       enc   ? "true" : "false");
        printf(",\"total_flows\":%lld",        flows);
        printf(",\"total_bytes\":%lld",        bytes);
        printf(",\"total_packets\":%lld",      pkts);
        printf(",\"first_seen\":");            json_escape_print(ts_first);
        printf(",\"last_seen\":");             json_escape_print(ts_last);
        printf(",\"bonfyre_chain\":");         json_escape_print(chain);
        printf(",\"integration_ready\":%s}",   (media || asr) ? "true" : "false");
    }
    printf("],\"device_count\":%d}\n", device_count);
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

/* ================================================================
 * bf_wire_artifacts — materialize discovered devices as BfArtifacts
 *
 * Every discovered device becomes a canonical BfArtifact JSON file
 * under <root>/wire/artifacts/<capture_id>/device_<id>.json.
 * Uses libbonfyre's BfArtifact contract — artifact_id, family_key,
 * canonical_key, atoms/operators/realizations counts — so any
 * downstream bonfyre binary (ingest, index, stitch) can consume
 * them immediately without any translation layer.
 * ================================================================ */
int bf_wire_artifacts(const char *root, const char *capture_id) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char db_path[PATH_MAX], artifact_dir[PATH_MAX], artifact_path[PATH_MAX];
    int first_item = 1, written = 0;
    char created_at[32];
    if (open_wire_db(root, &db, db_path, sizeof(db_path)) != 0) return 1;
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    build_device_table(db, capture_id);
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    snprintf(artifact_dir, sizeof(artifact_dir), "%s/wire/artifacts/%s", root, capture_id);
    if (bf_ensure_dir(artifact_dir) != 0) { sqlite3_close(db); return 1; }
    bf_iso_timestamp(created_at, sizeof(created_at));
    if (sqlite3_prepare_v2(db,
        "SELECT device_id,ip,vendor_hint,device_class,open_ports,protocol_fingerprint,"
        "  media_capable,asr_candidate,encrypted_only,"
        "  total_flows,total_bytes,total_packets,first_seen,last_seen,bonfyre_chain"
        " FROM wire_devices WHERE capture_id=? ORDER BY total_bytes DESC",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db); return 1;
    }
    sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
    printf("{\"capture_id\":"); json_escape_print(capture_id);
    printf(",\"artifact_dir\":"); json_escape_print(artifact_dir);
    printf(",\"artifacts\":[");
    while (sqlite3_step(st) == SQLITE_ROW) {
        FILE *fp;
        BfArtifact art;
        char root_seed[512], artifact_id[65];
        int n_chain_steps;
        const char *c;
        const char *dev_id   = sqlite3_column_text(st,0)  ? (const char *)sqlite3_column_text(st,0)  : "";
        const char *ip       = sqlite3_column_text(st,1)  ? (const char *)sqlite3_column_text(st,1)  : "";
        const char *vendor   = sqlite3_column_text(st,2)  ? (const char *)sqlite3_column_text(st,2)  : "";
        const char *class_   = sqlite3_column_text(st,3)  ? (const char *)sqlite3_column_text(st,3)  : "";
        const char *ports    = sqlite3_column_text(st,4)  ? (const char *)sqlite3_column_text(st,4)  : "";
        const char *protos   = sqlite3_column_text(st,5)  ? (const char *)sqlite3_column_text(st,5)  : "";
        int  media           = sqlite3_column_int(st,6);
        int  asr             = sqlite3_column_int(st,7);
        int  enc             = sqlite3_column_int(st,8);
        long long flows      = sqlite3_column_int64(st,9);
        long long bytes      = sqlite3_column_int64(st,10);
        long long pkts       = sqlite3_column_int64(st,11);
        const char *ts_first = sqlite3_column_text(st,12) ? (const char *)sqlite3_column_text(st,12) : "";
        const char *ts_last  = sqlite3_column_text(st,13) ? (const char *)sqlite3_column_text(st,13) : "";
        const char *chain    = sqlite3_column_text(st,14) ? (const char *)sqlite3_column_text(st,14) : "";
        /* Build BfArtifact using libbonfyre canonical contract */
        bf_artifact_init(&art);
        snprintf(root_seed, sizeof(root_seed), "wire_device:%s:%s", dev_id, capture_id);
        bf_sha256_hex((const uint8_t *)root_seed, strlen(root_seed), artifact_id);
        snprintf(art.artifact_id,    sizeof(art.artifact_id),    "%s", artifact_id);
        snprintf(art.artifact_type,  sizeof(art.artifact_type),  "wire_device");
        snprintf(art.source_system,  sizeof(art.source_system),  "BonfyreWire");
        snprintf(art.created_at,     sizeof(art.created_at),     "%s", created_at);
        /* root_hash = content address of ip+chain+class */
        snprintf(root_seed, sizeof(root_seed), "%s:%s:%s", ip, chain, class_);
        bf_sha256_hex((const uint8_t *)root_seed, strlen(root_seed), art.root_hash);
        /* Count pipeline steps */
        n_chain_steps = 1;
        for (c = chain; *c; c++) if (*c == ',') n_chain_steps++;
        art.atoms_count        = 1;              /* the device node itself    */
        art.operators_count    = n_chain_steps;  /* bonfyre pipeline steps    */
        art.realizations_count = (int)flows;     /* observed network flows    */
        art.component_total    = art.atoms_count + art.operators_count + art.realizations_count;
        bf_artifact_compute_keys(&art);
        /* Write artifact JSON — standard BfArtifact envelope + wire extension */
        snprintf(artifact_path, sizeof(artifact_path), "%s/device_%s.json", artifact_dir, dev_id);
        fp = fopen(artifact_path, "wb");
        if (fp) {
            fprintf(fp, "{\n");
            fprintf(fp, "  \"artifact_id\": \"%s\",\n",     art.artifact_id);
            fprintf(fp, "  \"artifact_type\": \"%s\",\n",   art.artifact_type);
            fprintf(fp, "  \"source_system\": \"%s\",\n",   art.source_system);
            fprintf(fp, "  \"created_at\": \"%s\",\n",      art.created_at);
            fprintf(fp, "  \"root_hash\": \"%s\",\n",       art.root_hash);
            fprintf(fp, "  \"family_key\": \"%s\",\n",      art.family_key);
            fprintf(fp, "  \"canonical_key\": \"%s\",\n",   art.canonical_key);
            fprintf(fp, "  \"atoms_count\": %d,\n",         art.atoms_count);
            fprintf(fp, "  \"operators_count\": %d,\n",     art.operators_count);
            fprintf(fp, "  \"realizations_count\": %d,\n",  art.realizations_count);
            fprintf(fp, "  \"component_total\": %d,\n",     art.component_total);
            fprintf(fp, "  \"wire_device\": {\n");
            fprintf(fp, "    \"device_id\": \"%s\",\n",     dev_id);
            fprintf(fp, "    \"capture_id\": \"%s\",\n",    capture_id);
            fprintf(fp, "    \"ip\": \"%s\",\n",            ip);
            fprintf(fp, "    \"vendor_hint\": \"%s\",\n",   vendor);
            fprintf(fp, "    \"device_class\": \"%s\",\n",  class_);
            fprintf(fp, "    \"open_ports\": \"%s\",\n",    ports);
            fprintf(fp, "    \"protocol_fingerprint\": \"%s\",\n", protos);
            fprintf(fp, "    \"media_capable\": %s,\n",     media ? "true" : "false");
            fprintf(fp, "    \"asr_candidate\": %s,\n",     asr   ? "true" : "false");
            fprintf(fp, "    \"encrypted_only\": %s,\n",    enc   ? "true" : "false");
            fprintf(fp, "    \"total_flows\": %lld,\n",     flows);
            fprintf(fp, "    \"total_bytes\": %lld,\n",     bytes);
            fprintf(fp, "    \"total_packets\": %lld,\n",   pkts);
            fprintf(fp, "    \"first_seen\": \"%s\",\n",    ts_first);
            fprintf(fp, "    \"last_seen\": \"%s\",\n",     ts_last);
            fprintf(fp, "    \"bonfyre_chain\": \"%s\"\n",  chain);
            fprintf(fp, "  }\n}\n");
            fclose(fp);
        }
        /* Persist artifact_id back to wire_devices */
        {
            sqlite3_stmt *upd = NULL;
            if (sqlite3_prepare_v2(db,
                "UPDATE wire_devices SET artifact_id=? WHERE device_id=?",
                -1, &upd, NULL) == SQLITE_OK) {
                sqlite3_bind_text(upd, 1, artifact_id, -1, SQLITE_STATIC);
                sqlite3_bind_text(upd, 2, dev_id,      -1, SQLITE_STATIC);
                sqlite3_step(upd);
                sqlite3_finalize(upd);
            }
        }
        if (!first_item) printf(",");
        first_item = 0;
        written++;
        printf("{\"artifact_id\":");   json_escape_print(artifact_id);
        printf(",\"artifact_type\":\"wire_device\"");
        printf(",\"device_class\":"); json_escape_print(class_);
        printf(",\"ip\":");           json_escape_print(ip);
        printf(",\"path\":");         json_escape_print(artifact_path);
        printf(",\"bonfyre_chain\":"); json_escape_print(chain);
        printf(",\"family_key\":");   json_escape_print(art.family_key);
        printf(",\"operators_count\":%d}", art.operators_count);
    }
    printf("],\"written\":%d}\n", written);
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

/* ================================================================
 * bf_wire_recipe — generate stitch-compatible pipeline recipe
 *
 * For every discovered device, walks its bonfyre_chain and emits
 * one RecipeStage object per step, chained via depends_on.
 * The output JSON pipes directly into:
 *   bonfyre stitch plan  <recipe.json>
 *   bonfyre stitch compile <recipe.json> --output <binary>
 *
 * This closes the loop: Wire discovers → recipe → stitch executes
 * → full bonfyre pipeline runs on every device in the sandbox.
 * ================================================================ */
int bf_wire_recipe(const char *root, const char *capture_id) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char db_path[PATH_MAX];
    char recipe_id[65], created_at[32], recipe_seed[128];
    int first_item = 1, stage_count = 0;
    if (open_wire_db(root, &db, db_path, sizeof(db_path)) != 0) return 1;
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    build_device_table(db, capture_id);
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    snprintf(recipe_seed, sizeof(recipe_seed), "wire_recipe:%s", capture_id);
    bf_sha256_hex((const uint8_t *)recipe_seed, strlen(recipe_seed), recipe_id);
    bf_iso_timestamp(created_at, sizeof(created_at));
    if (sqlite3_prepare_v2(db,
        "SELECT device_id,ip,vendor_hint,device_class,bonfyre_chain,"
        "  media_capable,asr_candidate,encrypted_only,total_bytes"
        " FROM wire_devices WHERE capture_id=? ORDER BY total_bytes DESC",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db); return 1;
    }
    sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
    printf("{\"recipe_id\":");    json_escape_print(recipe_id);
    printf(",\"name\":\"BonfyreWire Device Pipeline\"");
    printf(",\"generated_by\":\"BonfyreWire\"");
    printf(",\"capture_id\":"); json_escape_print(capture_id);
    printf(",\"created_at\":"); json_escape_print(created_at);
    printf(",\"stages\":[");
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *dev_id  = sqlite3_column_text(st,0) ? (const char *)sqlite3_column_text(st,0) : "";
        const char *ip      = sqlite3_column_text(st,1) ? (const char *)sqlite3_column_text(st,1) : "";
        const char *vendor  = sqlite3_column_text(st,2) ? (const char *)sqlite3_column_text(st,2) : "";
        const char *class_  = sqlite3_column_text(st,3) ? (const char *)sqlite3_column_text(st,3) : "";
        const char *chain   = sqlite3_column_text(st,4) ? (const char *)sqlite3_column_text(st,4) : "ingest,hash,index";
        int  asr            = sqlite3_column_int(st,6);
        char dev_short[9];
        char prev_stage[128] = "";
        char *chain_copy, *tok, *saveptr = NULL;
        /* 8-char device short-ID for stage name disambiguation */
        snprintf(dev_short, sizeof(dev_short), "%.8s", dev_id);
        chain_copy = strdup(chain);
        if (!chain_copy) continue;
        tok = strtok_r(chain_copy, ",", &saveptr);
        while (tok) {
            char stage_id[128];
            snprintf(stage_id, sizeof(stage_id), "%s_%s", tok, dev_short);
            if (!first_item) printf(",");
            first_item = 0;
            stage_count++;
            printf("{\"id\":");       json_escape_print(stage_id);
            printf(",\"operator\":"); json_escape_print(tok);
            printf(",\"args\":[");
            /* Enrich ingest stage with device metadata */
            if (strcmp(tok, "ingest") == 0) {
                printf("\"--type\",\"wire_device\"");
                printf(",\"--device-ip\","); json_escape_print(ip);
                printf(",\"--device-class\","); json_escape_print(class_);
                if (vendor[0]) { printf(",\"--vendor\","); json_escape_print(vendor); }
            }
            /* Steer transcribe model based on ASR classification */
            if (strcmp(tok, "transcribe") == 0 && asr)
                printf("\"--model\",\"speech\"");
            /* Pass device class to media-prep for format hints */
            if (strcmp(tok, "media-prep") == 0 && class_[0]) {
                printf("\"--device-class\","); json_escape_print(class_);
            }
            printf("]");
            printf(",\"depends_on\":[");
            if (prev_stage[0]) json_escape_print(prev_stage);
            printf("]}");
            snprintf(prev_stage, sizeof(prev_stage), "%s", stage_id);
            tok = strtok_r(NULL, ",", &saveptr);
        }
        free(chain_copy);
    }
    printf("],\"stage_count\":%d}\n", stage_count);
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

int bf_wire_doctor_json(const char *root) {
    char db_path[PATH_MAX];
    char raw_dir[PATH_MAX];
    char reports_dir[PATH_MAX];
    int have_tcpdump = (access("/usr/sbin/tcpdump", X_OK) == 0 || access("/usr/bin/tcpdump", X_OK) == 0);
    snprintf(db_path, sizeof(db_path), "%s/wire/wire.db", root);
    snprintf(raw_dir, sizeof(raw_dir), "%s/wire/raw", root);
    snprintf(reports_dir, sizeof(reports_dir), "%s/wire/reports", root);
    printf("{\"surface\":\"wire\",\"root\":");
    json_escape_print(root);
    printf(",\"permissions\":{\"live_capture\":\"packet-capture/BPF or libpcap privileges may be required\",\"offline_ingest\":\"read access to authorized pcap or synthetic event files\"}");
    printf(",\"backends\":{\"synthetic_ingest\":true,\"pcap_ingest\":true,\"live_interface_adapter\":%s}", have_tcpdump ? "\"external_tcpdump_or_bpf\"" : "\"not_detected\"");
    printf(",\"paths\":{\"db\":"); json_escape_print(db_path);
    printf(",\"raw_dir\":"); json_escape_print(raw_dir);
    printf(",\"reports_dir\":"); json_escape_print(reports_dir);
    printf("},\"policy\":{\"lab_unrestricted_env\":%s}", env_truthy("BONFYRE_LAB_UNRESTRICTED") ? "true" : "false");
    printf(",\"safety\":{\"authorized_required\":true,\"dumb_device_supported\":true,\"metadata_only_default\":true,\"payload_requires\":[\"--payload\",\"--authorized\",\"--unencrypted-only\"],\"encrypted_payload_refused\":true,\"refusal_bypass\":false}}\n");
    return 0;
}

static int space_open(sqlite3 **db) {
    char path[PATH_MAX];
    char dir[PATH_MAX];
    const char *schema =
        "PRAGMA journal_mode=WAL;"
        "CREATE TABLE IF NOT EXISTS spaces("
        "  name TEXT PRIMARY KEY,"
        "  created INTEGER NOT NULL,"
        "  accessed INTEGER NOT NULL,"
        "  mode TEXT NOT NULL DEFAULT 'rw'"
        ");"
        "CREATE TABLE IF NOT EXISTS entries("
        "  space TEXT NOT NULL REFERENCES spaces(name) ON DELETE CASCADE,"
        "  key TEXT NOT NULL,"
        "  value BLOB NOT NULL,"
        "  written INTEGER NOT NULL,"
        "  ttl_s INTEGER,"
        "  PRIMARY KEY(space,key)"
        ");"
        "CREATE TABLE IF NOT EXISTS attachments("
        "  space TEXT NOT NULL REFERENCES spaces(name) ON DELETE CASCADE,"
        "  stage TEXT NOT NULL,"
        "  attached INTEGER NOT NULL,"
        "  detached INTEGER,"
        "  PRIMARY KEY(space,stage)"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_entries_space ON entries(space);";
    char *err = NULL;
    space_db_path(path, sizeof(path));
    snprintf(dir, sizeof(dir), "%s", path);
    {
        char *slash = strrchr(dir, '/');
        if (slash) {
            *slash = '\0';
            if (dir[0] && bf_ensure_dir(dir) != 0) return 1;
        }
    }
    if (sqlite3_open(path, db) != SQLITE_OK) {
        if (*db) sqlite3_close(*db);
        *db = NULL;
        return 1;
    }
    if (sqlite3_exec(*db, schema, NULL, NULL, &err) != SQLITE_OK) {
        if (err) sqlite3_free(err);
        sqlite3_close(*db);
        *db = NULL;
        return 1;
    }
    return 0;
}

static int space_touch(sqlite3 *db, const char *space_name) {
    sqlite3_stmt *st = NULL;
    time_t now = time(NULL);
    if (sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO spaces(name,created,accessed) VALUES(?,?,?)", -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(st, 1, space_name, -1, SQLITE_STATIC);
    sqlite3_bind_int64(st, 2, (sqlite3_int64)now);
    sqlite3_bind_int64(st, 3, (sqlite3_int64)now);
    sqlite3_step(st);
    sqlite3_finalize(st);
    if (sqlite3_prepare_v2(db, "UPDATE spaces SET accessed=? WHERE name=?", -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_int64(st, 1, (sqlite3_int64)now);
    sqlite3_bind_text(st, 2, space_name, -1, SQLITE_STATIC);
    sqlite3_step(st);
    sqlite3_finalize(st);
    return 0;
}

static int space_put(sqlite3 *db, const char *space_name, const char *key, const char *value) {
    sqlite3_stmt *st = NULL;
    time_t now = time(NULL);
    if (space_touch(db, space_name) != 0) return 1;
    if (sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO entries(space,key,value,written) VALUES(?,?,?,?)", -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(st, 1, space_name, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, key, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, value ? value : "{}", -1, SQLITE_STATIC);
    sqlite3_bind_int64(st, 4, (sqlite3_int64)now);
    sqlite3_step(st);
    sqlite3_finalize(st);
    return 0;
}

int bf_wire_space_export_capture(const char *root, const char *capture_id, const char *space_name) {
    sqlite3 *wire_db = NULL, *space_db = NULL;
    sqlite3_stmt *st = NULL;
    char db_path[PATH_MAX];
    int exported = 0;
    if (!root || !capture_id || !space_name) return 1;
    if (open_wire_db(root, &wire_db, db_path, sizeof(db_path)) != 0) return 1;
    if (space_open(&space_db) != 0) {
        sqlite3_close(wire_db);
        return 1;
    }

    if (sqlite3_prepare_v2(wire_db,
        "SELECT source_kind,source_path,interface_name,authorized,dumb_device,metadata_only,payload_enabled,unencrypted_only,save_raw,packet_count,byte_count,status,created_at "
        "FROM wire_captures WHERE capture_id=?", -1, &st, NULL) == SQLITE_OK) {
        char key[256];
        char value[2048];
        sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            snprintf(key, sizeof(key), "wire/%s/capture", capture_id);
            snprintf(value, sizeof(value),
                     "{\"capture_id\":\"%s\",\"source_kind\":\"%s\",\"source_path\":\"%s\",\"interface_name\":\"%s\","
                     "\"authorized\":%s,\"dumb_device\":%s,\"metadata_only\":%s,\"payload_enabled\":%s,\"unencrypted_only\":%s,\"save_raw\":%s,"
                     "\"packet_count\":%lld,\"byte_count\":%lld,\"status\":\"%s\",\"created_at\":\"%s\"}",
                     capture_id,
                     sqlite3_column_text(st,0) ? (const char *)sqlite3_column_text(st,0) : "",
                     sqlite3_column_text(st,1) ? (const char *)sqlite3_column_text(st,1) : "",
                     sqlite3_column_text(st,2) ? (const char *)sqlite3_column_text(st,2) : "",
                     sqlite3_column_int(st,3) ? "true" : "false",
                     sqlite3_column_int(st,4) ? "true" : "false",
                     sqlite3_column_int(st,5) ? "true" : "false",
                     sqlite3_column_int(st,6) ? "true" : "false",
                     sqlite3_column_int(st,7) ? "true" : "false",
                     sqlite3_column_int(st,8) ? "true" : "false",
                     sqlite3_column_int64(st,9),
                     sqlite3_column_int64(st,10),
                     sqlite3_column_text(st,11) ? (const char *)sqlite3_column_text(st,11) : "",
                     sqlite3_column_text(st,12) ? (const char *)sqlite3_column_text(st,12) : "");
            space_put(space_db, space_name, key, value);
        }
    }
    sqlite3_finalize(st);
    st = NULL;

    if (sqlite3_prepare_v2(wire_db,
        "SELECT flow_id,src_ip,dst_ip,src_port,dst_port,l4_proto,app_proto,media_kind,domain,dns_name,sni,process_name,first_ts,last_ts,packets,bytes,encrypted,probable_media,probable_control,hls_manifest,dash_manifest,websocket_media,pipeline_trigger,vendor_dependency,suggested_integration_point,payload_buffer_path "
        "FROM wire_flows WHERE capture_id=? ORDER BY bytes DESC", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(space_db);
        sqlite3_close(wire_db);
        return 1;
    }
    sqlite3_bind_text(st, 1, capture_id, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        char key[256];
        char value[4096];
        const char *flow_id = sqlite3_column_text(st,0) ? (const char *)sqlite3_column_text(st,0) : "";
        const char *payload_path = sqlite3_column_text(st,25) ? (const char *)sqlite3_column_text(st,25) : "";
        long long payload_size = file_size_bytes(payload_path);
        snprintf(key, sizeof(key), "wire/%s/flow/%s", capture_id, flow_id);
        snprintf(value, sizeof(value),
                 "{\"capture_id\":\"%s\",\"flow_id\":\"%s\",\"src_ip\":\"%s\",\"dst_ip\":\"%s\",\"src_port\":%d,\"dst_port\":%d,"
                 "\"l4_proto\":\"%s\",\"app_proto\":\"%s\",\"media_kind\":\"%s\",\"domain\":\"%s\",\"dns_name\":\"%s\",\"sni\":\"%s\","
                 "\"process_name\":\"%s\",\"first_ts\":\"%s\",\"last_ts\":\"%s\",\"packets\":%lld,\"bytes\":%lld,"
                 "\"encrypted\":%s,\"probable_media\":%s,\"probable_control\":%s,\"hls_manifest\":%s,\"dash_manifest\":%s,"
                 "\"websocket_media\":%s,\"pipeline_trigger\":%s,\"vendor_dependency\":\"%s\",\"suggested_integration_point\":\"%s\","
                 "\"buffer\":{\"path\":\"%s\",\"size_bytes\":%lld,\"kind\":\"authorized_unencrypted_payload_file\"}}",
                 capture_id, flow_id,
                 sqlite3_column_text(st,1) ? (const char *)sqlite3_column_text(st,1) : "",
                 sqlite3_column_text(st,2) ? (const char *)sqlite3_column_text(st,2) : "",
                 sqlite3_column_int(st,3), sqlite3_column_int(st,4),
                 sqlite3_column_text(st,5) ? (const char *)sqlite3_column_text(st,5) : "",
                 sqlite3_column_text(st,6) ? (const char *)sqlite3_column_text(st,6) : "",
                 sqlite3_column_text(st,7) ? (const char *)sqlite3_column_text(st,7) : "",
                 sqlite3_column_text(st,8) ? (const char *)sqlite3_column_text(st,8) : "",
                 sqlite3_column_text(st,9) ? (const char *)sqlite3_column_text(st,9) : "",
                 sqlite3_column_text(st,10) ? (const char *)sqlite3_column_text(st,10) : "",
                 sqlite3_column_text(st,11) ? (const char *)sqlite3_column_text(st,11) : "",
                 sqlite3_column_text(st,12) ? (const char *)sqlite3_column_text(st,12) : "",
                 sqlite3_column_text(st,13) ? (const char *)sqlite3_column_text(st,13) : "",
                 sqlite3_column_int64(st,14), sqlite3_column_int64(st,15),
                 sqlite3_column_int(st,16) ? "true" : "false",
                 sqlite3_column_int(st,17) ? "true" : "false",
                 sqlite3_column_int(st,18) ? "true" : "false",
                 sqlite3_column_int(st,19) ? "true" : "false",
                 sqlite3_column_int(st,20) ? "true" : "false",
                 sqlite3_column_int(st,21) ? "true" : "false",
                 sqlite3_column_int(st,22) ? "true" : "false",
                 sqlite3_column_text(st,23) ? (const char *)sqlite3_column_text(st,23) : "",
                 sqlite3_column_text(st,24) ? (const char *)sqlite3_column_text(st,24) : "",
                 payload_path, payload_size);
        space_put(space_db, space_name, key, value);
        exported++;
    }
    sqlite3_finalize(st);
    sqlite3_close(space_db);
    sqlite3_close(wire_db);
    printf("{\"capture_id\":\"%s\",\"space\":\"%s\",\"exported_flows\":%d}\n", capture_id, space_name, exported);
    return 0;
}
