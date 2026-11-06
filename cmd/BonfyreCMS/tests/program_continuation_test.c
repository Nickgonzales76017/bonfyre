#define _XOPEN_SOURCE 700
#define _DARWIN_C_SOURCE
#define _DEFAULT_SOURCE
#include "bf_jsonw.h"
#include "bf_msgpack.h"
#include "bonfyre.h"
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <limits.h>
#include <signal.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* Real native admission, then a read under another connection's writer lock.
 * Only the deliberate missing startup table is changed directly with SQL. */
static char home[PATH_MAX], database[PATH_MAX], schemas[PATH_MAX], writer[PATH_MAX];
static sqlite3 *sql;
static pid_t child = -1;
static unsigned checks;
static const char *reader;
static const char key[] = "draft:program:working-plan";
typedef struct { unsigned char *data; size_t size; } Bytes;
typedef struct { int code, timed_out; double seconds; Bytes out, err; } Result;

static int remove_one(const char *path, const struct stat *st, int type, struct FTW *walk) {
    (void)st; (void)type; (void)walk; return remove(path);
}
static void cleanup(void) {
    if (child > 0) { kill(child, SIGKILL); waitpid(child, NULL, 0); }
    if (sql) sqlite3_close(sql);
    if (home[0]) nftw(home, remove_one, 16, FTW_DEPTH | FTW_PHYS);
}
static void require(int ok, const char *message) {
    if (!ok) { fprintf(stderr, "program continuation: FAIL: %s\n", message); exit(1); }
}
static void check(int ok, const char *message) {
    require(ok, message); checks++; printf("PASS %s\n", message);
}
static void path_join(char out[PATH_MAX], const char *base, const char *name) {
    int n = snprintf(out, PATH_MAX, "%s/%s", base, name);
    require(n > 0 && n < PATH_MAX, "fixture path fits");
}
static Bytes read_file(const char *path) {
    FILE *f = fopen(path, "rb"); require(f != NULL, "open captured file");
    require(!fseek(f, 0, SEEK_END), "seek captured file"); long size = ftell(f);
    require(size >= 0 && size <= 1024 * 1024, "captured file remains bounded");
    rewind(f); Bytes b = {malloc((size_t)size + 1), (size_t)size};
    require(b.data != NULL, "allocate captured file");
    require(fread(b.data, 1, b.size, f) == b.size && !ferror(f), "read captured file");
    b.data[b.size] = 0; fclose(f); return b;
}
static void write_file(const char *path, const void *data, size_t size) {
    FILE *f = fopen(path, "wb"); require(f != NULL, "open fixture file");
    require(fwrite(data, 1, size, f) == size, "write fixture file");
    require(!fclose(f), "close fixture file");
}
static void copy_file(const char *source, const char *target) {
    Bytes b = read_file(source); write_file(target, b.data, b.size); free(b.data);
}
static double monotonic(void) {
    struct timespec t; require(!clock_gettime(CLOCK_MONOTONIC, &t), "read clock");
    return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}
static Result run(const char *binary, const char *const *args, const void *input, size_t size, int timeout_ms) {
    char in[PATH_MAX], out[PATH_MAX], err[PATH_MAX];
    path_join(in, home, "stdin"); path_join(out, home, "stdout"); path_join(err, home, "stderr");
    write_file(in, input, size);
    int in_fd = open(in, O_RDONLY), out_fd = open(out, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    int err_fd = open(err, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    require(in_fd >= 0 && out_fd >= 0 && err_fd >= 0, "open subprocess streams");
    char *argv[64]; size_t n = 0; argv[n++] = (char *)binary;
    for (size_t i = 0; args[i]; i++) { require(n + 1 < 64, "bounded argument list"); argv[n++] = (char *)args[i]; }
    argv[n] = NULL; double started = monotonic(); child = fork();
    require(child >= 0, "fork CMS process");
    if (!child) {
        if (dup2(in_fd, 0) < 0 || dup2(out_fd, 1) < 0 || dup2(err_fd, 2) < 0 || setenv("BONFYRE_FACT_DELTAS", "", 1)) _exit(126);
        close(in_fd); close(out_fd); close(err_fd); execv(binary, argv); _exit(127);
    }
    close(in_fd); close(out_fd); close(err_fd);
    Result result = {0}; int status = 0;
    for (;;) {
        pid_t done = waitpid(child, &status, WNOHANG);
        if (done == child) break;
        require(done == 0 || (done < 0 && errno == EINTR), "wait for CMS process");
        if ((monotonic() - started) * 1000 >= timeout_ms) {
            result.timed_out = 1; kill(child, SIGKILL);
            while (waitpid(child, &status, 0) < 0 && errno == EINTR) {}
            break;
        }
        struct timespec pause = {0, 1000000}; nanosleep(&pause, NULL);
    }
    child = -1; result.seconds = monotonic() - started;
    result.code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    result.out = read_file(out); result.err = read_file(err); return result;
}
static void release(Result *r) { free(r->out.data); free(r->err.data); memset(r, 0, sizeof(*r)); }
static void success(const Result *r) {
    if (r->timed_out || r->code) {
        fprintf(stderr, "CMS exit=%d timeout=%d\n%.*s\n%.*s\n", r->code, r->timed_out,
                (int)r->out.size, r->out.data, (int)r->err.size, r->err.data);
        require(0, "native CMS command succeeds");
    }
}
static void migrate(void) {
    const char *args[] = {"schema", "migrate", "--db", database, "--schemas", schemas, NULL};
    Result r = run(writer, args, "", 0, 15000); success(&r); release(&r);
}
static void add(const char *type, const char *const *fields) {
    bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
    for (size_t i = 0; fields[i]; i += 2) { bf_jsonw_key(&w, fields[i]); bf_jsonw_str(&w, fields[i + 1]); }
    bf_jsonw_obj_end(&w); require(!w.oom, "encode fixture JSON");
    const char *args[] = {"entry", "create", type, w.buf, "--ns", "root", "--db", database, "--schemas", schemas, NULL};
    Result r = run(writer, args, "", 0, 15000); success(&r); release(&r); bf_jsonw_free(&w);
}
static Result lifecycle_for(const char *pack, const char *operation, const char *revision, const char *digest) {
    const char *args[] = {"pack-lifecycle", "--operation", operation, "--pack", pack,
        "--subject", "user:101", "--scope", "self", "--actor", "user:101", "--version", "",
        "--expected-revision", revision, "--expected-digest", digest, "--db", database, "--schemas", schemas, NULL};
    Result r = run(writer, args, "", 0, 15000); success(&r); return r;
}
static Result lifecycle(const char *operation, const char *revision, const char *digest) {
    return lifecycle_for("program-continuation", operation, revision, digest);
}
static int field(const Result *r, const char *name, char *out, size_t cap) {
    const char *p = (const char *)r->out.data; size_t n = strlen(name);
    while (*p) {
        const char *end = strchr(p, '\n'); if (!end) end = p + strlen(p);
        if ((size_t)(end - p) > n && !memcmp(p, name, n) && p[n] == '\t') {
            size_t length = (size_t)(end - p) - n - 1; require(length < cap, "lifecycle field fits");
            memcpy(out, p + n + 1, length); out[length] = 0; return 1;
        }
        p = *end ? end + 1 : end;
    }
    return 0;
}
static void exec_sql(const char *statement) { require(sqlite3_exec(sql, statement, NULL, NULL, NULL) == SQLITE_OK, statement); }
static char *schema_snapshot(void) {
    sqlite3_stmt *q = NULL; bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_arr(&w);
    require(sqlite3_prepare_v2(sql, "SELECT type,name,sql FROM sqlite_master ORDER BY type,name", -1, &q, NULL) == SQLITE_OK, "read schema snapshot");
    int rc;
    while ((rc = sqlite3_step(q)) == SQLITE_ROW) {
        bf_jsonw_arr(&w);
        for (int col = 0; col < 3; col++) {
            if (sqlite3_column_type(q, col) == SQLITE_NULL) bf_jsonw_null(&w);
            else bf_jsonw_str(&w, (const char *)sqlite3_column_text(q, col));
        }
        bf_jsonw_arr_end(&w);
    }
    require(rc == SQLITE_DONE, "finish schema snapshot"); sqlite3_finalize(q); bf_jsonw_arr_end(&w);
    require(!w.oom, "encode schema snapshot"); char *snapshot = strdup(w.buf);
    require(snapshot != NULL, "retain schema snapshot"); bf_jsonw_free(&w); return snapshot;
}
static void file_hash(const char *path, char out[65]) {
    if (access(path, F_OK)) { require(errno == ENOENT, "inspect database file"); strcpy(out, "absent"); }
    else require(!bf_sha256_file(path, out), "hash database file");
}
static Result workspace(const char *binary, const char *operation, const char *binding,
                        const char *subject, const char *db, const void *input, size_t size, int timeout) {
    const char *args[] = {"pack-workspace", "--binding", binding, "--operation", operation,
        "--subject", subject, "--scope", "self", "--actor", subject, "--prefix", key,
        "--db", db, "--schemas", schemas, NULL};
    return run(binary, args, input, size, timeout);
}
static int array(bf_mp_reader_t *r, uint32_t count) {
    bf_mp_value_t v; return bf_mp_read_next(r, &v) == BF_MP_ARRAY && v.v.count == count;
}
static int number(bf_mp_reader_t *r, uint64_t value) {
    bf_mp_value_t v; bf_mp_type_t t = bf_mp_read_next(r, &v);
    return (t == BF_MP_UINT && v.v.u == value) || (t == BF_MP_INT && v.v.i >= 0 && (uint64_t)v.v.i == value);
}
static int string(bf_mp_reader_t *r, const char *value) {
    bf_mp_value_t v; if (bf_mp_read_next(r, &v) != BF_MP_STR) return 0;
    return value ? v.v.raw.len == strlen(value) && !memcmp(v.v.raw.ptr, value, v.v.raw.len) : v.v.raw.len > 0;
}
static int context(bf_mp_reader_t *r, const char *binding) {
    return array(r, 9) && number(r, 1) && string(r, binding) && string(r, "user:101") &&
        string(r, "self") && string(r, "program-continuation") && string(r, NULL) &&
        string(r, "workbench/") && string(r, "programs") && string(r, NULL);
}

static void advertisement_checks(const char *estate) {
    const unsigned first = checks;
    const char *artifact[] = {"pack", "reusable-source", "canonical_name", "reusable-source", "publisher", "fixture-publisher", "layer", "official", "kind", "base", "purpose", "Explicitly fictional workflow configuration for native integration tests.", "governed_by", "fixture-owner", "cannot_infer", "Fixture configuration is not a provisioned real product.", "status", "published", NULL};
    add("pack_artifact", artifact);
    char root[65]; const char seed[] = "reusable-source:1";
    bf_sha256_hex((const uint8_t *)seed, strlen(seed), root);
    const char *version[] = {"pack", "reusable-source", "version", "1", "root", root, "changed", "Initial fixture version", "status", "published", NULL}; add("pack_version", version);
    const char *head[] = {"pack", "reusable-source", "channel", "stable", "version", "1", "advanced_by", "fixture-owner", "status", "published", NULL}; add("pack_head", head);
    char revision[64], digest[256];
    Result planned = lifecycle_for("reusable-source", "plan", "0", "");
    require(field(&planned, "digest", digest, sizeof(digest)), "advertised pack plan digest"); release(&planned);
    Result installed = lifecycle_for("reusable-source", "install", "0", digest);
    require(field(&installed, "revision", revision, sizeof(revision)) && field(&installed, "digest", digest, sizeof(digest)), "advertised pack installation coordinates"); release(&installed);
    Result activated = lifecycle_for("reusable-source", "activate", revision, digest); release(&activated);
    const char *public_activation[] = {"pack", "reusable-source", "activated_for", "public-fixture", "scope", "public", "activated_by", "fixture-owner", "conditions", "[]", "status", "published", NULL};
    add("pack_activation", public_activation);
    const char *args[] = {"emit-cms", "--cms-db", database, NULL};
    Result emitted = run(estate, args, "", 0, 15000); success(&emitted);
    /* Validate every JSONL row, as the former fixture did before inspecting it. */
    for (const char *p = (const char *)emitted.out.data; *p;) {
        const char *end = strchr(p, '\n'); if (!end) end = p + strlen(p);
        if (end != p) {
            char error[160]; bf_json_doc_t *doc = bf_json_parse(p, (size_t)(end - p), error, sizeof(error));
            require(doc && bf_json_root(doc), "ExternalEstate emits valid JSONL"); bf_json_free(doc);
        }
        p = *end ? end + 1 : end;
    }
    check(strstr((const char *)emitted.out.data, "cms:pack_activation:reusable-source:public-fixture:public") != NULL,
          "public forwarding retains the explicit public pack activation");
    check(strstr((const char *)emitted.out.data, "user:101") == NULL,
          "public forwarding excludes the private account");
    check(strstr((const char *)emitted.out.data, "pack-lock:") == NULL,
          "public forwarding excludes private pinned activation locks");
    release(&emitted);
    sqlite3_stmt *q = NULL;
    require(sqlite3_prepare_v2(sql, "SELECT count(*) FROM pack_activation WHERE pack='reusable-source' AND status='published'", -1, &q, NULL) == SQLITE_OK,
            "read advertised pack activations");
    check(sqlite3_step(q) == SQLITE_ROW && sqlite3_column_int(q, 0) == 2,
          "public forwarding preserves both public and private activation records");
    sqlite3_finalize(q);
    Result current = lifecycle_for("reusable-source", "current", "0", ""); char state[64];
    check(field(&current, "state", state, sizeof(state)) && !strcmp(state, "active"),
          "scoped native pack resolution remains active after public forwarding"); release(&current);
    printf("PACK_LOCK_ADVERTISEMENT_PASS\t%u\tpublic forwarding excludes private locks; scoped native resolution remains active\n", checks - first);
}

int main(int argc, char **argv) {
    require(argc == 2, "usage: program-continuation-test REPOSITORY");
    char repo[PATH_MAX]; require(realpath(argv[1], repo) != NULL, "resolve repository");
    path_join(writer, repo, "cmd/BonfyreCMS/bonfyre-cms");
    reader = getenv("BONFYRE_TEST_PROGRAM_CMS_BINARY"); if (!reader || !*reader) reader = writer;
    require(access(writer, X_OK) == 0 && access(reader, X_OK) == 0, "current writer and selected reader exist");
    char estate_default[PATH_MAX]; path_join(estate_default, repo, "cmd/BonfyreExternalEstate/bonfyre-external-estate");
    const char *estate = getenv("BONFYRE_TEST_PACK_ESTATE_BINARY"); if (!estate || !*estate) estate = estate_default;
    require(access(estate, X_OK) == 0, "selected native ExternalEstate emitter exists");
    const char *tmp = getenv("TMPDIR"); if (!tmp || !*tmp) tmp = "/tmp";
    path_join(home, tmp, "program-continuation-native-XXXXXX");
    require(mkdtemp(home) != NULL, "create disposable fixture"); require(!atexit(cleanup), "register cleanup");
    path_join(database, home, "cms.db"); path_join(schemas, home, "schemas"); require(!mkdir(schemas, 0700), "create schemas");
    const char *types[] = {"pack_artifact", "pack_version", "pack_head", "pack_dependency", "pack_overlay", "pack_demand", "pack_installation", "pack_activation", NULL};
    for (size_t i = 0; types[i]; i++) {
        char name[256], source[PATH_MAX], target[PATH_MAX];
        snprintf(name, sizeof(name), "cmd/BonfyreCMS/content-types/%s.json", types[i]); path_join(source, repo, name);
        snprintf(name, sizeof(name), "%s.json", types[i]); path_join(target, schemas, name); copy_file(source, target);
    }
    migrate(); require(sqlite3_open(database, &sql) == SQLITE_OK, "open fixture connection");
    char source[PATH_MAX], target[PATH_MAX]; path_join(source, repo, "cmd/BonfyreCMS/content-types/application_binding.json");
    path_join(target, schemas, "application_binding.json"); copy_file(source, target); migrate();
    const char *artifact[] = {"pack", "program-continuation", "canonical_name", "program-continuation", "publisher", "fixture-publisher", "layer", "official", "kind", "base", "purpose", "Explicitly fictional workflow configuration for native integration tests.", "governed_by", "fixture-owner", "cannot_infer", "Fixture configuration is not a provisioned real product.", "status", "published", NULL};
    add("pack_artifact", artifact);
    char root[65]; const char version_seed[] = "program-continuation:1";
    bf_sha256_hex((const uint8_t *)version_seed, strlen(version_seed), root);
    const char *version[] = {"pack", "program-continuation", "version", "1", "root", root, "changed", "Initial fixture version", "status", "published", NULL}; add("pack_version", version);
    const char *head[] = {"pack", "program-continuation", "channel", "stable", "version", "1", "advanced_by", "fixture-owner", "status", "published", NULL}; add("pack_head", head);
    char preferred[65]; memset(preferred, 'a', 64); preferred[64] = 0;
    const char *demand[] = {"pack", "program-continuation", "demand_kind", "workspace", "needs", "programs", "preferred_sources", preferred, "fields", "workbench/", "status", "published", NULL}; add("pack_demand", demand);
    const char *record[] = {"pack", "program-continuation", "demand_kind", "workspace-record", "needs", "programs", "fields", "draft:program:", "status", "published", NULL}; add("pack_demand", record);
    char revision[64], digest[256], installation[64], binding[256];
    Result planned = lifecycle("plan", "0", ""); require(field(&planned, "revision", revision, sizeof(revision)) && field(&planned, "digest", digest, sizeof(digest)), "plan coordinates"); release(&planned);
    Result installed = lifecycle("install", revision, digest);
    require(field(&installed, "revision", revision, sizeof(revision)) && (field(&installed, "digest", digest, sizeof(digest)) || field(&installed, "pinned_digest", digest, sizeof(digest))) && field(&installed, "installation", installation, sizeof(installation)), "installation coordinates"); release(&installed);
    Result activated = lifecycle("activate", revision, digest); release(&activated);
    sqlite3_stmt *q = NULL; require(sqlite3_prepare_v2(sql, "SELECT binding_id FROM _application_workspace WHERE installation_id=?1", -1, &q, NULL) == SQLITE_OK, "read admitted binding");
    sqlite3_bind_int64(q, 1, strtoll(installation, NULL, 10)); require(sqlite3_step(q) == SQLITE_ROW, "installation produced binding");
    const char *id = (const char *)sqlite3_column_text(q, 0); require(id && strlen(id) < sizeof(binding), "binding fits"); strcpy(binding, id); sqlite3_finalize(q);
    bf_mp_writer_t body, packet; bf_mp_writer_init(&body, 256); bf_mp_writer_init(&packet, 256);
    require(!bf_mp_pack_array(&body, 3) && !bf_mp_pack_uint(&body, 2) && !bf_mp_pack_map(&body, 2) && !bf_mp_pack_kv_str(&body, "title", "Program plan") && !bf_mp_pack_kv_str(&body, "source", "native fixture") && !bf_mp_pack_array(&body, 0), "encode retained program body");
    require(!bf_mp_pack_array(&packet, 3) && !bf_mp_pack_uint(&packet, 1) && !bf_mp_pack_str_cstr(&packet, "program-plan-save") && !bf_mp_pack_array(&packet, 1) && !bf_mp_pack_array(&packet, 4) && !bf_mp_pack_str_cstr(&packet, key) && !bf_mp_pack_uint(&packet, 0) && !bf_mp_pack_uint(&packet, 0) && !bf_mp_pack_bin(&packet, body.buf, (uint32_t)body.len), "encode workspace write");
    Result saved = workspace(writer, "write", binding, "user:101", database, packet.buf, packet.len, 15000); success(&saved); release(&saved); bf_mp_writer_free(&packet);
    exec_sql("DROP TABLE _hooks"); char *before = schema_snapshot();
    char wal[PATH_MAX], db_before[65], wal_before[65], db_after[65], wal_after[65];
    require(snprintf(wal, sizeof(wal), "%s-wal", database) < (int)sizeof(wal), "WAL path fits");
    file_hash(database, db_before); file_hash(wal, wal_before); exec_sql("BEGIN IMMEDIATE");
    Result opened = workspace(reader, "continuation", binding, "user:101", database, "", 0, 2500); success(&opened);
    bf_mp_reader_t r; bf_mp_reader_init(&r, opened.out.data, opened.out.size);
    require(array(&r, 4) && number(&r, 1), "continuation MessagePack envelope"); size_t context_start = r.pos;
    int valid_context = context(&r, binding); size_t context_size = r.pos - context_start;
    check(!opened.timed_out && opened.seconds < 2.5 && valid_context, "actual CMS continuation reads while another connection holds BEGIN IMMEDIATE");
    bf_mp_value_t value;
    check(array(&r, 3) && string(&r, key) && number(&r, 1) && bf_mp_read_next(&r, &value) == BF_MP_BIN && value.v.raw.len == body.len && !memcmp(value.v.raw.ptr, body.buf, body.len), "the continuation returns the exact retained record revision and body");
    int compatible = array(&r, 1) && array(&r, 2); size_t candidate_start = r.pos;
    compatible = compatible && context(&r, binding) && r.pos - candidate_start == context_size && !memcmp(opened.out.data + context_start, opened.out.data + candidate_start, context_size) && string(&r, "program-continuation") && !bf_mp_reader_remaining(&r);
    check(compatible, "compatible workspace contexts are references to the same account");
    char *after = schema_snapshot(); file_hash(database, db_after); file_hash(wal, wal_after);
    require(sqlite3_prepare_v2(sql, "SELECT 1 FROM sqlite_master WHERE name='_hooks'", -1, &q, NULL) == SQLITE_OK, "inspect missing startup table");
    check(!strcmp(before, after) && !strcmp(db_before, db_after) && !strcmp(wal_before, wal_after) && sqlite3_step(q) == SQLITE_DONE, "readonly continuation preserves database and WAL bytes and never bootstraps schema");
    sqlite3_finalize(q); free(before); free(after); release(&opened); bf_mp_writer_free(&body);
    Result foreign = workspace(reader, "continuation", binding, "user:202", database, "", 0, 2500);
    check(!foreign.timed_out && foreign.seconds < 2.5 && foreign.code == 4 && !foreign.out.size, "the readonly path retains exact account admission"); release(&foreign);
    exec_sql("ROLLBACK"); char absent[PATH_MAX]; path_join(absent, home, "absent.db");
    Result missing = workspace(reader, "continuation", binding, "user:101", absent, "", 0, 2500);
    check(!missing.timed_out && missing.seconds < 2.5 && missing.code != 0 && access(absent, F_OK) != 0 && errno == ENOENT, "continuation refuses a missing database without creating one"); release(&missing);
    printf("PROGRAM_CONTINUATION_PASS\t%u\n", checks);
    advertisement_checks(estate); return 0;
}
