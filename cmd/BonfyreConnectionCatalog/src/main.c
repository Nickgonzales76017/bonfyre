#define _XOPEN_SOURCE 700
#include <duckdb.h>
#include <bf_jsonw.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static int local_file(const char *path, char out[PATH_MAX]) {
  struct stat st;
  return path && path[0] == '/' && realpath(path, out) &&
    stat(out, &st) == 0 && S_ISREG(st.st_mode);
}
static int number(const char *s, long max, long *out) {
  char *end; long n;
  if (!s || !*s) return 0;
  for (const unsigned char *p = (const unsigned char *)s; *p; p++)
    if (!isdigit(*p)) return 0;
  errno = 0; n = strtol(s, &end, 10);
  if (errno || *end || n > max) return 0;
  *out = n; return 1;
}
static int plain(const char *s, size_t max) {
  size_t n = s ? strlen(s) : 0;
  if (!n || n > max) return 0;
  for (size_t i = 0; i < n; i++)
    if ((unsigned char)s[i] < 32 || (unsigned char)s[i] == 127) return 0;
  return 1;
}
/* Strict web coordinates: reject credentials, ports, escapes in authority,
 * whitespace and backslashes. HTTP is retained, but never a safe link. */
static int url(const char *s) {
  const char *p, *host;
  if (!plain(s, 4096)) return 0;
  if (!strncmp(s, "https://", 8)) p = s + 8;
  else if (!strncmp(s, "http://", 7)) p = s + 7;
  else return 0;
  host = p;
  while (*p && *p != '/' && *p != '?' && *p != '#') {
    if (!(isalnum((unsigned char)*p) || *p == '.' || *p == '-')) return 0;
    p++;
  }
  if (p == host || *host == '.' || *(p-1) == '.') return 0;
  for (const unsigned char *q = (const unsigned char *)s; *q; q++) {
    if (isspace(*q) || *q == '\\' || *q == '"' || *q == '<' || *q == '>') return 0;
    if (*q == '%' && (!q[1] || !q[2] || !isxdigit(q[1]) || !isxdigit(q[2]))) return 0;
  }
  return 1;
}
static int hex(const char *s, size_t n) {
  if (!s || strlen(s) != n) return 0;
  for (size_t i = 0; i < n; i++)
    if (!((s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'f'))) return 0;
  return 1;
}
static void field(bf_jsonw *w, const char *k, const char *v) {
  bf_jsonw_key(w, k); bf_jsonw_str(w, v);
}
int main(int argc, char **argv) {
  const char *facets = NULL, *entries = NULL, *ecosystem = "", *search = NULL, *view = NULL;
  char fp[PATH_MAX], ep[PATH_MAX]; long offset = 0, limit = 50;
  duckdb_database db = NULL; duckdb_connection conn = NULL;
  duckdb_prepared_statement stmt = NULL; duckdb_result res = {0};
  duckdb_config config = NULL; bf_jsonw out; int ok = 0, initialized = 0;
  const char *sql =
    "WITH f AS (SELECT * FROM (SELECT *, count(*) OVER (PARTITION BY source_goal_id,original_ordinal) AS copies "
    "FROM read_parquet(?)) WHERE (?='' OR catalogue=?) AND (?='' OR contains(lower(raw_key),lower(?)))), s AS (SELECT *, "
    "count(*) OVER (PARTITION BY source_goal_id,original_ordinal) AS copies FROM read_parquet(?)) "
    "SELECT f.catalogue,f.raw_key,f.source_url_candidate,f.source_goal_id,f.source_goal_key,"
    "cast(f.original_ordinal as varchar),f.row_sha256,s.raw_line,s.goal_source,f.status,"
    "CASE WHEN s.row_sha256=f.row_sha256 AND sha256(s.raw_line)=f.row_sha256 "
    "AND s.source_entry=f.source_entry AND contains(s.raw_line,s.source_entry) AND s.source_goal_key=f.source_goal_key "
    "AND s.source_url_candidate=f.source_url_candidate "
    "AND list_contains(string_split(f.source_entry,' | '),f.raw_key) "
    "AND list_contains(string_split(f.source_entry,' | '),f.source_url_candidate) "
    "AND f.copies=1 AND s.copies=1 THEN '1' ELSE '0' END "
    "FROM f LEFT JOIN s ON f.source_goal_id=s.source_goal_id AND f.original_ordinal=s.original_ordinal "
    "ORDER BY f.catalogue,f.source_goal_id,f.original_ordinal LIMIT ? OFFSET ?";
  if (argc < 2 || strcmp(argv[1], "list")) goto usage;
  for (int i = 2; i < argc; i++) {
    if (i + 1 >= argc) goto usage;
    const char *key = argv[i++], *value = argv[i];
    if (!strcmp(key, "--facets") && !facets) facets = value;
    else if (!strcmp(key, "--entries") && !entries) entries = value;
    else if (!strcmp(key, "--ecosystem") && !*ecosystem) ecosystem = value;
    else if (!strcmp(key, "--search") && !search) search = value;
    else if (!strcmp(key, "--view") && !view) { if (strcmp(value, "app")) goto usage; view = value; }
    else if (!strcmp(key, "--offset")) { if (!number(value, 1000000, &offset)) goto usage; }
    else if (!strcmp(key, "--limit")) { if (!number(value, 100, &limit) || !limit) goto usage; }
    else goto usage;
  }
  if (!local_file(facets, fp) || !local_file(entries, ep) ||
      (*ecosystem && !plain(ecosystem, 128)) || (search && !plain(search, 128))) goto usage;
  if (duckdb_create_config(&config) != DuckDBSuccess ||
      duckdb_set_config(config, "autoinstall_known_extensions", "false") != DuckDBSuccess ||
      duckdb_set_config(config, "autoload_known_extensions", "false") != DuckDBSuccess ||
      duckdb_open_ext(NULL, &db, config, NULL) != DuckDBSuccess ||
      duckdb_connect(db, &conn) != DuckDBSuccess ||
      duckdb_prepare(conn, sql, &stmt) != DuckDBSuccess ||
      duckdb_bind_varchar(stmt, 1, fp) != DuckDBSuccess ||
      duckdb_bind_varchar(stmt, 2, ecosystem) != DuckDBSuccess ||
      duckdb_bind_varchar(stmt, 3, ecosystem) != DuckDBSuccess ||
      duckdb_bind_varchar(stmt, 4, search ? search : "") != DuckDBSuccess ||
      duckdb_bind_varchar(stmt, 5, search ? search : "") != DuckDBSuccess ||
      duckdb_bind_varchar(stmt, 6, ep) != DuckDBSuccess ||
      duckdb_bind_int64(stmt, 7, limit + 1) != DuckDBSuccess ||
      duckdb_bind_int64(stmt, 8, offset) != DuckDBSuccess ||
      duckdb_execute_prepared(stmt, &res) != DuckDBSuccess) goto done;
  bf_jsonw_init(&out, 0); initialized = 1;
  bf_jsonw_obj(&out); field(&out, "schema", "bonfyre.connection-catalog.v1");
  bf_jsonw_key(&out, "entries"); bf_jsonw_arr(&out);
  idx_t count = duckdb_row_count(&res);
  for (idx_t row = 0; row < count && row < (idx_t)limit; row++) {
    char *v[11];
    for (idx_t col = 0; col < 11; col++) v[col] = duckdb_value_varchar(&res, col, row);
    long ordinal = 0;
    int valid = plain(v[0], 128) && plain(v[1], 512) && url(v[2]) &&
      hex(v[3], 32) && plain(v[4], 512) && number(v[5], INT_MAX, &ordinal) && ordinal > 0 &&
      hex(v[6], 64) && plain(v[7], 16384) && v[8] &&
      !strcmp(v[8], "user-direct-20261102-connection-studio-hunt") &&
      v[9] && !strcmp(v[9], "unverified_source_candidate") && v[10] && !strcmp(v[10], "1");
    if (valid) {
      bf_jsonw_obj(&out); field(&out, "ecosystem", v[0]); field(&out, "name", v[1]);
      if (!view) field(&out, "source_url", v[2]); bf_jsonw_key(&out, "safe_link");
      if (!strncmp(v[2], "https://", 8)) bf_jsonw_str(&out, v[2]); else bf_jsonw_null(&out);
      if (!view) {
        field(&out, "goal_id", v[3]); field(&out, "goal_key", v[4]);
        bf_jsonw_key(&out, "ordinal"); bf_jsonw_int(&out, ordinal);
        field(&out, "row_sha256", v[6]); field(&out, "raw_line", v[7]);
        field(&out, "goal_source", v[8]);
      }
      field(&out, "status", v[9]); bf_jsonw_obj_end(&out);
    }
    for (idx_t col = 0; col < 11; col++) duckdb_free(v[col]);
    if (!valid) { fprintf(stderr, "catalog: malformed or unjoined source coordinate\n"); goto done; }
  }
  bf_jsonw_arr_end(&out); bf_jsonw_key(&out, "offset"); bf_jsonw_int(&out, offset);
  bf_jsonw_key(&out, "next_offset"); if (count > (idx_t)limit) bf_jsonw_int(&out, offset + limit); else bf_jsonw_null(&out);
  field(&out, "boundary", "Source candidate only; installation, licensing and authorization are not established.");
  bf_jsonw_obj_end(&out);
  if (out.oom || !out.buf || fwrite(out.buf, 1, out.len, stdout) != out.len || fputc('\n', stdout) == EOF) goto done;
  ok = 1;
done:
  if (!ok) fprintf(stderr, "catalog: read failed or data was refused\n");
  if (initialized) bf_jsonw_free(&out);
  duckdb_destroy_result(&res);
  if (stmt) duckdb_destroy_prepare(&stmt);
  if (conn) duckdb_disconnect(&conn);
  if (db) duckdb_close(&db);
  if (config) duckdb_destroy_config(&config);
  return ok ? 0 : 1;
usage:
  fputs("usage: bonfyre-connection-catalog list --facets ABSOLUTE.parquet --entries ABSOLUTE.parquet [--ecosystem NAME] [--search TEXT] [--offset 0..1000000] [--limit 1..100] [--view app]\n", stderr);
  return 2;
}
