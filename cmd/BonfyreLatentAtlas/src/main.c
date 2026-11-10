/* BonfyreLatentAtlas -- an addressed atlas of interoperable coordinate spaces.
 *
 * Not an embedding space.  An exact address (a Wikidata item, an archive
 * identifier, a census item key) may carry projections in several charts whose
 * mathematics match the structure each preserves:
 *
 *   dense         learned or hashed direction          cosine
 *   hdc           role-bound hypervector (bits)        normalised Hamming
 *   hyperbolic    point in the Poincare ball           geodesic
 *   geo           latitude/longitude                    great-circle km
 *   circular      phases in [0,1)                       wrapped phase distance
 *   distribution  normalised histogram                  Hellinger
 *   subspace      orthonormal rows                      principal-angle (chordal)
 *   fingerprint   32-bit frames                         best-aligned bit error rate
 *   trajectory    ordered values                        normalised DTW
 *
 * Laws enforced here, not merely described:
 *   - two projections are compared only in one chart, or through a declared
 *     bridge that carries its calibration error; arrays of numbers in
 *     different charts are refused as incomparable.
 *   - relations are typed (same_subject_as, performance_of, depicts, happens_at,
 *     member_of,
 *     ...); similarity never writes a relation.
 *   - a group is addressed twice: a logical address that survives and a
 *     snapshot digest over universe, lens, chart versions, exact member
 *     closure (each member's source digest) and child snapshots.
 *   - a group's representation is not its members' type: points lift to
 *     modes and a subspace (dense), a mixture (distribution), a bundle (hdc),
 *     a medoid (fingerprint); groups of groups lift to a hierarchy node with
 *     hyperbolic child coordinates and a topology signature.
 *   - after a mutation only groups whose exact closure changed recompute; a
 *     parent recomputes only when a child's representation moved beyond the
 *     preservation envelope, otherwise the hold is recorded.
 *   - every consequential read reopens the exact member closure and reports
 *     members whose current projection no longer matches the snapshot.
 *
 * Cannot infer: nearness -> identity, relation, evidence or truth; a group ->
 * a category anyone declared; a fingerprint match -> the same performance or
 * rights; a hold -> the parent being exactly current.
 */
#define _POSIX_C_SOURCE 200809L
#include <bonfyre.h>
#include <bf_json.h>
#include <bf_command_lookup.h>
#include <ctype.h>
#include <math.h>
#include <sqlite3.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define LA_PI 3.14159265358979323846
#define LA_MAX_DIMS 8192
#define LA_MAX_MEMBERS 512

static const char *arg(int argc, char **argv, const char *name) {
    int i;
    for (i = 2; i + 1 < argc; i++) if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}

static void json_str(const char *s) {
    putchar('"');
    for (; s && *s; s++) {
        unsigned char c = (unsigned char)*s;
        if (c == '"' || c == '\\') { putchar('\\'); putchar(c); }
        else if (c == '\n') fputs("\\n", stdout);
        else if (c < 0x20) printf("\\u%04x", c);
        else putchar(c);
    }
    putchar('"');
}

static int exec_sql(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) { fprintf(stderr, "sqlite: %s\n", err ? err : "error"); sqlite3_free(err); return 0; }
    return 1;
}

static const char *schema_sql =
    "PRAGMA journal_mode=WAL;"
    "CREATE TABLE IF NOT EXISTS chart(chart_id TEXT PRIMARY KEY, family TEXT NOT NULL CHECK(family IN ('dense','hdc','hyperbolic','geo','circular','distribution','subspace','fingerprint','trajectory')),"
    " dims INTEGER NOT NULL, version TEXT NOT NULL, encoder TEXT NOT NULL, universe TEXT NOT NULL DEFAULT '');"
    "CREATE TABLE IF NOT EXISTS projection(address TEXT NOT NULL, chart_id TEXT NOT NULL REFERENCES chart(chart_id), coordinate BLOB NOT NULL,"
    " source_digest TEXT NOT NULL, frontier TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL, PRIMARY KEY(address, chart_id));"
    "CREATE TABLE IF NOT EXISTS label(address TEXT PRIMARY KEY, label TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS bridge(from_chart TEXT NOT NULL, to_chart TEXT NOT NULL, operator TEXT NOT NULL, calibration_error REAL NOT NULL,"
    " loss_envelope TEXT NOT NULL, validity_frontier TEXT NOT NULL, provenance TEXT NOT NULL, PRIMARY KEY(from_chart, to_chart));"
    "CREATE TABLE IF NOT EXISTS relation(src TEXT NOT NULL, relation TEXT NOT NULL CHECK(relation IN ('same_subject_as','materializes','observed_as','performance_of',"
    " 'depicts','mentions','derived_from','fingerprints_to','projected_into','member_of','located_in','version_of','happens_at')), dst TEXT NOT NULL, evidence TEXT NOT NULL,"
    " PRIMARY KEY(src, relation, dst));"
    "CREATE TABLE IF NOT EXISTS neighbourhood(logical TEXT PRIMARY KEY, lens TEXT NOT NULL, level INTEGER NOT NULL, chart_id TEXT NOT NULL,"
    " representation_type TEXT NOT NULL, current_snapshot TEXT, parent TEXT);"
    "CREATE TABLE IF NOT EXISTS snapshot(snapshot_id TEXT PRIMARY KEY, logical TEXT NOT NULL, universe TEXT NOT NULL, lens TEXT NOT NULL, chart_id TEXT NOT NULL,"
    " member_closure TEXT NOT NULL, children TEXT NOT NULL DEFAULT '', representation TEXT NOT NULL, created_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS snapshot_member(snapshot_id TEXT NOT NULL, address TEXT NOT NULL, source_digest TEXT NOT NULL, PRIMARY KEY(snapshot_id, address));"
    "CREATE TABLE IF NOT EXISTS latent_delta(address TEXT NOT NULL, chart_id TEXT NOT NULL, from_digest TEXT NOT NULL, to_digest TEXT NOT NULL, moved REAL NOT NULL, at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS envelope_hold(parent TEXT NOT NULL, child TEXT NOT NULL, child_from TEXT NOT NULL, child_to TEXT NOT NULL, delta REAL NOT NULL, envelope REAL NOT NULL, at TEXT NOT NULL);";

static void now_text(char *out, size_t cap) {
    time_t t = time(NULL); struct tm tm;
    if (gmtime_r(&t, &tm)) strftime(out, cap, "%Y-%m-%dT%H:%M:%SZ", &tm); else snprintf(out, cap, "unknown");
}

/* An atlas written before a relation type existed still refuses that type,
 * because the type list is a CHECK constraint and SQLite cannot alter one.
 * The relations themselves are kept: the table is rebuilt with the current
 * list and every existing row copied into it. */
static int migrate_relation_types(sqlite3 *db) {
    sqlite3_stmt *st = NULL; const char *sql = NULL; int needs = 0;
    if (sqlite3_prepare_v2(db, "SELECT sql FROM sqlite_master WHERE type='table' AND name='relation'", -1, &st, NULL) == SQLITE_OK &&
        sqlite3_step(st) == SQLITE_ROW) {
        sql = (const char *)sqlite3_column_text(st, 0);
        needs = sql && !strstr(sql, "'happens_at'");
    }
    sqlite3_finalize(st);
    if (!needs) return 1;
    return exec_sql(db,
        "BEGIN;"
        "ALTER TABLE relation RENAME TO relation_before_happens_at;"
        "CREATE TABLE relation(src TEXT NOT NULL, relation TEXT NOT NULL CHECK(relation IN ('same_subject_as','materializes','observed_as','performance_of',"
        " 'depicts','mentions','derived_from','fingerprints_to','projected_into','member_of','located_in','version_of','happens_at')), dst TEXT NOT NULL,"
        " evidence TEXT NOT NULL, PRIMARY KEY(src, relation, dst));"
        "INSERT OR IGNORE INTO relation(src,relation,dst,evidence) SELECT src,relation,dst,evidence FROM relation_before_happens_at;"
        "DROP TABLE relation_before_happens_at;"
        "COMMIT;");
}

static sqlite3 *open_db(const char *path) {
    sqlite3 *db = NULL;
    if (!path || sqlite3_open(path, &db) != SQLITE_OK) { fprintf(stderr, "open %s failed\n", path ? path : "(none)"); sqlite3_close(db); return NULL; }
    if (!exec_sql(db, schema_sql) || !migrate_relation_types(db)) { sqlite3_close(db); return NULL; }
    return db;
}

/* ------------------------------------------------------------ charts */

typedef struct { char id[128], family[16], version[32]; int dims; } Chart;

static int chart_get(sqlite3 *db, const char *id, Chart *c) {
    sqlite3_stmt *st = NULL; int ok = 0;
    if (sqlite3_prepare_v2(db, "SELECT chart_id,family,dims,version FROM chart WHERE chart_id=?", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        snprintf(c->id, sizeof(c->id), "%s", sqlite3_column_text(st, 0));
        snprintf(c->family, sizeof(c->family), "%s", sqlite3_column_text(st, 1));
        c->dims = sqlite3_column_int(st, 2);
        snprintf(c->version, sizeof(c->version), "%s", sqlite3_column_text(st, 3));
        ok = 1;
    }
    sqlite3_finalize(st);
    return ok;
}

typedef struct { float *f; uint8_t *bits; int n; int nbits; } Coord;

static void coord_free(Coord *c) { free(c->f); free(c->bits); c->f = NULL; c->bits = NULL; c->n = c->nbits = 0; }

static int family_is_bits(const char *family) { return !strcmp(family, "hdc") || !strcmp(family, "fingerprint"); }

static int coord_load(sqlite3 *db, const char *address, const Chart *chart, Coord *out, char *digest, size_t digest_cap) {
    sqlite3_stmt *st = NULL; int ok = 0;
    memset(out, 0, sizeof(*out));
    if (sqlite3_prepare_v2(db, "SELECT coordinate,source_digest FROM projection WHERE address=? AND chart_id=?", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, address, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, chart->id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const void *blob = sqlite3_column_blob(st, 0); int len = sqlite3_column_bytes(st, 0);
        if (digest) snprintf(digest, digest_cap, "%s", sqlite3_column_text(st, 1));
        if (family_is_bits(chart->family)) {
            if ((out->bits = malloc((size_t)len + 1))) { memcpy(out->bits, blob, (size_t)len); out->nbits = len * 8; ok = 1; }
        } else if (len % 4 == 0 && (out->f = malloc((size_t)len + 4))) {
            memcpy(out->f, blob, (size_t)len); out->n = len / 4; ok = 1;
        }
    }
    sqlite3_finalize(st);
    return ok;
}

static int coord_store(sqlite3 *db, const char *address, const Chart *chart, const Coord *c, const char *digest, const char *frontier) {
    sqlite3_stmt *st = NULL; char at[32]; int ok;
    now_text(at, sizeof(at));
    if (sqlite3_prepare_v2(db, "INSERT INTO projection(address,chart_id,coordinate,source_digest,frontier,updated_at) VALUES(?,?,?,?,?,?) "
          "ON CONFLICT(address,chart_id) DO UPDATE SET coordinate=excluded.coordinate,source_digest=excluded.source_digest,frontier=excluded.frontier,updated_at=excluded.updated_at",
          -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, address, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, chart->id, -1, SQLITE_TRANSIENT);
    if (c->bits) sqlite3_bind_blob(st, 3, c->bits, c->nbits / 8, SQLITE_TRANSIENT);
    else sqlite3_bind_blob(st, 3, c->f, c->n * 4, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 4, digest, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 5, frontier ? frontier : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 6, at, -1, SQLITE_TRANSIENT);
    ok = sqlite3_step(st) == SQLITE_DONE;
    sqlite3_finalize(st);
    return ok;
}

/* ------------------------------------------------------------ metrics */

static double metric_dense(const float *a, const float *b, int n) {
    double dot = 0, na = 0, nb = 0; int i;
    for (i = 0; i < n; i++) { dot += a[i] * b[i]; na += a[i] * a[i]; nb += b[i] * b[i]; }
    if (na <= 0 || nb <= 0) return 1.0;
    return 1.0 - dot / (sqrt(na) * sqrt(nb));
}

static int popcount8(uint8_t v) { int c = 0; while (v) { c += v & 1; v >>= 1; } return c; }

static double metric_hamming(const uint8_t *a, const uint8_t *b, int nbytes) {
    int i, diff = 0;
    for (i = 0; i < nbytes; i++) diff += popcount8((uint8_t)(a[i] ^ b[i]));
    return nbytes ? (double)diff / (nbytes * 8.0) : 1.0;
}

static double metric_hyperbolic(const float *u, const float *v, int n) {
    double uu = 0, vv = 0, d = 0; int i;
    for (i = 0; i < n; i++) { uu += u[i] * u[i]; vv += v[i] * v[i]; d += (u[i] - v[i]) * (u[i] - v[i]); }
    if (uu >= 1.0) uu = 0.999999;
    if (vv >= 1.0) vv = 0.999999;
    return acosh(1.0 + 2.0 * d / ((1.0 - uu) * (1.0 - vv)));
}

static double metric_geo(const float *a, const float *b) {
    double lat1 = a[0] * LA_PI / 180, lat2 = b[0] * LA_PI / 180, dlat = lat2 - lat1, dlon = (b[1] - a[1]) * LA_PI / 180;
    double h = sin(dlat / 2) * sin(dlat / 2) + cos(lat1) * cos(lat2) * sin(dlon / 2) * sin(dlon / 2);
    return 2 * 6371.0 * asin(sqrt(h > 1 ? 1 : h));
}

static double metric_circular(const float *a, const float *b, int n) {
    double sum = 0; int i;
    for (i = 0; i < n; i++) { double d = fabs(a[i] - b[i]); d = d - floor(d); if (d > 0.5) d = 1.0 - d; sum += d; }
    return n ? sum / n : 1.0;
}

static double metric_hellinger(const float *a, const float *b, int n) {
    double sa = 0, sb = 0, bc = 0; int i;
    for (i = 0; i < n; i++) { sa += a[i] > 0 ? a[i] : 0; sb += b[i] > 0 ? b[i] : 0; }
    if (sa <= 0 || sb <= 0) return 1.0;
    for (i = 0; i < n; i++) bc += sqrt((a[i] > 0 ? a[i] / sa : 0) * (b[i] > 0 ? b[i] / sb : 0));
    return sqrt(fabs(1.0 - bc));
}

/* Principal angles between two row spaces (rank <= 3) via the singular
 * values of A B^T; the distance is the chordal sqrt(sum sin^2). */
static double symmetric_eigen_sum_cos2(double m[3][3], int r) {
    /* trace of M M^T equals the sum of squared singular values = sum cos^2 */
    double t = 0; int i, j;
    for (i = 0; i < r; i++) for (j = 0; j < r; j++) t += m[i][j] * m[i][j];
    return t;
}

static double metric_subspace(const float *a, const float *b, int dims, int ra, int rb) {
    double m[3][3] = {{0}}; int i, j, k, r = ra < rb ? ra : rb;
    if (ra < 1 || rb < 1 || ra > 3 || rb > 3) return 1.0;
    for (i = 0; i < ra; i++) for (j = 0; j < rb; j++) { double dot = 0; for (k = 0; k < dims; k++) dot += a[i * dims + k] * b[j * dims + k]; m[i][j] = dot; }
    {
        double cos2 = symmetric_eigen_sum_cos2(m, ra > rb ? ra : rb);
        double sin2 = r - cos2;
        return sqrt(sin2 > 0 ? sin2 : 0) / sqrt((double)r);
    }
}

static double metric_fingerprint(const uint8_t *a, int abytes, const uint8_t *b, int bbytes) {
    /* 32-bit frames; the best alignment within eight frames either way. */
    int fa = abytes / 4, fb = bbytes / 4, shift, best_frames = 0; double best = 1.0;
    for (shift = -8; shift <= 8; shift++) {
        int i, diff = 0, frames = 0;
        for (i = 0; i < fa; i++) {
            int j = i + shift;
            if (j < 0 || j >= fb) continue;
            diff += popcount8((uint8_t)(a[i * 4] ^ b[j * 4])) + popcount8((uint8_t)(a[i * 4 + 1] ^ b[j * 4 + 1])) +
                    popcount8((uint8_t)(a[i * 4 + 2] ^ b[j * 4 + 2])) + popcount8((uint8_t)(a[i * 4 + 3] ^ b[j * 4 + 3]));
            frames++;
        }
        if (frames >= 8 && (double)diff / (frames * 32.0) < best) { best = (double)diff / (frames * 32.0); best_frames = frames; }
    }
    return best_frames ? best : 1.0;
}

static double metric_dtw(const float *a, int na, const float *b, int nb) {
    double *prev, *cur, result; int i, j;
    if (na <= 0 || nb <= 0) return 1e9;
    prev = malloc(sizeof(double) * (size_t)(nb + 1));
    cur = malloc(sizeof(double) * (size_t)(nb + 1));
    if (!prev || !cur) { free(prev); free(cur); return 1e9; }
    for (j = 0; j <= nb; j++) prev[j] = 1e30;
    prev[0] = 0;
    for (i = 1; i <= na; i++) {
        cur[0] = 1e30;
        for (j = 1; j <= nb; j++) {
            double cost = fabs(a[i - 1] - b[j - 1]), best = prev[j - 1];
            if (prev[j] < best) best = prev[j];
            if (cur[j - 1] < best) best = cur[j - 1];
            cur[j] = cost + best;
        }
        { double *t = prev; prev = cur; cur = t; }
    }
    result = prev[nb] / (na + nb);
    free(prev); free(cur);
    return result;
}

static const char *metric_name(const char *family) {
    if (!strcmp(family, "dense")) return "cosine-distance";
    if (!strcmp(family, "hdc")) return "normalized-hamming";
    if (!strcmp(family, "hyperbolic")) return "poincare-geodesic";
    if (!strcmp(family, "geo")) return "great-circle-km";
    if (!strcmp(family, "circular")) return "wrapped-phase";
    if (!strcmp(family, "distribution")) return "hellinger";
    if (!strcmp(family, "subspace")) return "chordal-principal-angles";
    if (!strcmp(family, "fingerprint")) return "aligned-bit-error-rate";
    return "normalized-dtw";
}

static int chart_distance(const Chart *chart, const Coord *a, const Coord *b, double *out) {
    const char *f = chart->family;
    if (!strcmp(f, "dense")) { if (a->n != b->n) return 0; *out = metric_dense(a->f, b->f, a->n); return 1; }
    if (!strcmp(f, "hdc")) { if (a->nbits != b->nbits) return 0; *out = metric_hamming(a->bits, b->bits, a->nbits / 8); return 1; }
    if (!strcmp(f, "hyperbolic")) { if (a->n != b->n) return 0; *out = metric_hyperbolic(a->f, b->f, a->n); return 1; }
    if (!strcmp(f, "geo")) { if (a->n < 2 || b->n < 2) return 0; *out = metric_geo(a->f, b->f); return 1; }
    if (!strcmp(f, "circular")) { if (a->n != b->n) return 0; *out = metric_circular(a->f, b->f, a->n); return 1; }
    if (!strcmp(f, "distribution")) { if (a->n != b->n) return 0; *out = metric_hellinger(a->f, b->f, a->n); return 1; }
    if (!strcmp(f, "subspace")) { if (!chart->dims || a->n % chart->dims || b->n % chart->dims) return 0; *out = metric_subspace(a->f, b->f, chart->dims, a->n / chart->dims, b->n / chart->dims); return 1; }
    if (!strcmp(f, "fingerprint")) { *out = metric_fingerprint(a->bits, a->nbits / 8, b->bits, b->nbits / 8); return 1; }
    if (!strcmp(f, "trajectory")) { *out = metric_dtw(a->f, a->n, b->f, b->n); return 1; }
    return 0;
}

/* ------------------------------------------------------------ encoders */

static uint64_t splitmix(uint64_t *state) {
    uint64_t z = (*state += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

static uint64_t text_seed(const char *text) {
    char hex[65]; uint64_t seed = 0; int i;
    bf_sha256_hex((const uint8_t *)text, strlen(text), hex);
    for (i = 0; i < 16; i++) seed = seed * 16 + (uint64_t)(isdigit((unsigned char)hex[i]) ? hex[i] - '0' : hex[i] - 'a' + 10);
    return seed;
}

/* Hashed character trigrams, signed, L2-normalised: cheap geometry that
 * measured close to learned encoders for near-duplicate text in this estate. */
static void encode_text(const char *text, float *out, int dims) {
    char norm[2048]; size_t n = 0, i; double len = 0;
    memset(out, 0, sizeof(float) * (size_t)dims);
    norm[n++] = ' ';
    for (i = 0; text[i] && n + 2 < sizeof(norm); i++) norm[n++] = (char)tolower((unsigned char)text[i]);
    norm[n++] = ' '; norm[n] = '\0';
    for (i = 0; i + 3 <= n; i++) {
        char gram[4] = {norm[i], norm[i + 1], norm[i + 2], 0};
        uint64_t h = text_seed(gram);
        out[h % (uint64_t)dims] += (h >> 63) ? 1.0f : -1.0f;
    }
    for (i = 0; i < (size_t)dims; i++) len += out[i] * out[i];
    if (len > 0) for (i = 0; i < (size_t)dims; i++) out[i] = (float)(out[i] / sqrt(len));
}

static void hv_random(const char *name, uint8_t *out, int nbytes) {
    uint64_t state = text_seed(name); int i;
    for (i = 0; i < nbytes; i += 8) { uint64_t r = splitmix(&state); int k; for (k = 0; k < 8 && i + k < nbytes; k++) out[i + k] = (uint8_t)(r >> (8 * k)); }
}

/* role=value pairs bound by XOR and bundled by majority: the roles survive,
 * so "venue X, artist Y" is not "artist X, venue Y". */
static int encode_roles(const char *roles, uint8_t *out, int nbytes) {
    int counts[LA_MAX_DIMS] = {0}, pairs = 0, i, bit;
    char copy[4096], *save = NULL, *pair;
    uint8_t *rv = malloc((size_t)nbytes), *vv = malloc((size_t)nbytes);
    if (!rv || !vv || nbytes * 8 > LA_MAX_DIMS) { free(rv); free(vv); return -1; }
    snprintf(copy, sizeof(copy), "%s", roles);
    for (pair = strtok_r(copy, ";", &save); pair; pair = strtok_r(NULL, ";", &save)) {
        char *eq = strchr(pair, '='), role[256], value[1024];
        if (!eq) continue;
        snprintf(role, sizeof(role), "role:%.*s", (int)(eq - pair), pair);
        snprintf(value, sizeof(value), "value:%s", eq + 1);
        hv_random(role, rv, nbytes); hv_random(value, vv, nbytes);
        for (i = 0; i < nbytes; i++) for (bit = 0; bit < 8; bit++) counts[i * 8 + bit] += ((rv[i] ^ vv[i]) >> bit) & 1;
        pairs++;
    }
    memset(out, 0, (size_t)nbytes);
    for (i = 0; i < nbytes * 8; i++) if (counts[i] * 2 > pairs || (counts[i] * 2 == pairs && (i % 2))) out[i / 8] |= (uint8_t)(1 << (i % 8));
    free(rv); free(vv);
    return pairs;
}

/* An iterative radix-2 FFT for the acoustic charts. */
static void fft(double *re, double *im, int n) {
    int i, j, len, bit;
    for (i = 1, j = 0; i < n; i++) {
        for (bit = n >> 1; j & bit; bit >>= 1) j ^= bit;
        j ^= bit;
        if (i < j) { double t = re[i]; re[i] = re[j]; re[j] = t; t = im[i]; im[i] = im[j]; im[j] = t; }
    }
    for (len = 2; len <= n; len <<= 1) {
        double ang = -2 * LA_PI / len, wr = cos(ang), wi = sin(ang);
        for (i = 0; i < n; i += len) {
            double cr = 1, ci = 0; int k;
            for (k = 0; k < len / 2; k++) {
                double ur = re[i + k], ui = im[i + k];
                double vr = re[i + k + len / 2] * cr - im[i + k + len / 2] * ci;
                double vi = re[i + k + len / 2] * ci + im[i + k + len / 2] * cr;
                double t;
                re[i + k] = ur + vr; im[i + k] = ui + vi;
                re[i + k + len / 2] = ur - vr; im[i + k + len / 2] = ui - vi;
                t = cr * wr - ci * wi; ci = cr * wi + ci * wr; cr = t;
            }
        }
    }
}

/* From 11025 Hz mono 16-bit PCM: a Haitsma-Kalker style fingerprint (33
 * log-spaced bands 300-2000 Hz, one 32-bit frame per hop, each bit the sign of
 * the band-energy difference's change), a 24-band spectral distribution and a
 * 32-point loudness trajectory. */
static int acoustic(const char *pcm_path, uint8_t **fp, int *fp_bytes, float *dist, float *traj) {
    enum { N = 2048, HOP = 512, BANDS = 33, DBANDS = 24, TPOINTS = 32 };
    FILE *f = fopen(pcm_path, "rb"); long size; int16_t *samples; int count, frames, i, fi, b;
    double re[N], im[N], window[N], prev[BANDS] = {0}, rms_total[TPOINTS] = {0}; int rms_count[TPOINTS] = {0};
    double edges[BANDS + 1], dedges[DBANDS + 1], dsum[DBANDS] = {0}, max_rms = 0, total = 0;
    if (!f || fseek(f, 0, SEEK_END) || (size = ftell(f)) < N * 2 || fseek(f, 0, SEEK_SET)) { if (f) fclose(f); return 0; }
    count = (int)(size / 2);
    if (!(samples = malloc(sizeof(int16_t) * (size_t)count)) || fread(samples, 2, (size_t)count, f) != (size_t)count) { fclose(f); free(samples); return 0; }
    fclose(f);
    frames = (count - N) / HOP;
    if (frames < 16 || !(*fp = calloc((size_t)frames, 4))) { free(samples); return 0; }
    for (i = 0; i < N; i++) window[i] = 0.5 - 0.5 * cos(2 * LA_PI * i / (N - 1));
    for (b = 0; b <= BANDS; b++) edges[b] = 300.0 * pow(2000.0 / 300.0, (double)b / BANDS);
    for (b = 0; b <= DBANDS; b++) dedges[b] = 60.0 * pow(5000.0 / 60.0, (double)b / DBANDS);
    for (fi = 0; fi < frames; fi++) {
        double energy[BANDS] = {0}, rms = 0; int t;
        for (i = 0; i < N; i++) { double s = samples[fi * HOP + i] / 32768.0; rms += s * s; re[i] = s * window[i]; im[i] = 0; }
        fft(re, im, N);
        for (i = 1; i < N / 2; i++) {
            double hz = i * 11025.0 / N, power = re[i] * re[i] + im[i] * im[i];
            for (b = 0; b < BANDS; b++) if (hz >= edges[b] && hz < edges[b + 1]) { energy[b] += power; break; }
            for (b = 0; b < DBANDS; b++) if (hz >= dedges[b] && hz < dedges[b + 1]) { dsum[b] += power; total += power; break; }
        }
        for (b = 0; b < 32; b++) {
            double d = (energy[b] - energy[b + 1]) - (prev[b] - prev[b + 1]);
            if (fi > 0 && d > 0) (*fp)[fi * 4 + b / 8] |= (uint8_t)(1 << (b % 8));
        }
        memcpy(prev, energy, sizeof(prev));
        t = fi * TPOINTS / frames;
        rms_total[t] += sqrt(rms / N); rms_count[t]++;
    }
    for (b = 0; b < DBANDS; b++) dist[b] = total > 0 ? (float)(dsum[b] / total) : 0;
    for (i = 0; i < TPOINTS; i++) { double v = rms_count[i] ? rms_total[i] / rms_count[i] : 0; traj[i] = (float)v; if (v > max_rms) max_rms = v; }
    if (max_rms > 0) for (i = 0; i < TPOINTS; i++) traj[i] = (float)(traj[i] / max_rms);
    *fp_bytes = frames * 4;
    free(samples);
    return frames;
}

/* ------------------------------------------------------------ import */

static int parse_values(const bf_json_doc_t *doc, const bf_json_node_t *arr, Coord *out) {
    const bf_json_node_t *v; int n = 0;
    if (!arr || arr->type != BF_JSON_ARRAY) return 0;
    for (v = bf_json_child_first(doc, arr); v; v = bf_json_child_next(doc, v)) n++;
    if (!n || n > LA_MAX_DIMS * 3 || !(out->f = malloc(sizeof(float) * (size_t)n))) return 0;
    n = 0;
    for (v = bf_json_child_first(doc, arr); v; v = bf_json_child_next(doc, v))
        out->f[n++] = (float)(v->type == BF_JSON_INT ? (double)v->val.i : v->type == BF_JSON_DOUBLE ? v->val.d : 0);
    out->n = n;
    return 1;
}

static int parse_hex_n(const char *hex, size_t len, Coord *out) {
    size_t i;
    if (!len || len % 2 || !(out->bits = malloc(len / 2))) return 0;
    for (i = 0; i < len / 2; i++) {
        unsigned v;
        if (sscanf(hex + 2 * i, "%2x", &v) != 1) {
            free(out->bits);
            out->bits = NULL;
            return 0;
        }
        out->bits[i] = (uint8_t)v;
    }
    out->nbits = (int)(len / 2) * 8;
    return 1;
}

static int parse_hex(const char *hex, Coord *out) { return hex && parse_hex_n(hex, strlen(hex), out); }

/* A bit chart's hex can be thousands of characters; it is read in place. */
static int parse_hex_node(const bf_json_doc_t *doc, const bf_json_node_t *obj, const char *key, Coord *out) {
    const bf_json_node_t *n = bf_json_obj_get(doc, obj, key); const char *s; int len;
    if (!n || n->type != BF_JSON_STRING || !(s = bf_json_get_str(n, &len)) || len <= 0) return 0;
    return parse_hex_n(s, (size_t)len, out);
}

static const char *jstr(const bf_json_doc_t *doc, const bf_json_node_t *obj, const char *key, char *buf, size_t cap) {
    const bf_json_node_t *n = bf_json_obj_get(doc, obj, key);
    buf[0] = '\0';
    if (n && n->type == BF_JSON_STRING) bf_json_get_str_copy(n, buf, cap);
    return buf[0] ? buf : NULL;
}

static int import_jsonl(sqlite3 *db, const char *path) {
    FILE *f = fopen(path, "r"); char *line = NULL; size_t cap = 0; ssize_t len; int rows = 0, bad = 0;
    if (!f) { perror(path); return 0; }
    exec_sql(db, "BEGIN");
    while ((len = getline(&line, &cap, f)) > 0) {
        char err[160]; bf_json_doc_t *doc = bf_json_parse(line, (size_t)len, err, sizeof(err));
        const bf_json_node_t *root = doc ? bf_json_root(doc) : NULL, *body;
        char a[512], b[512], c[512], d[512], e[512], g[512];
        if (!root) { bad++; continue; }
        if ((body = bf_json_obj_get(doc, root, "chart"))) {
            sqlite3_stmt *st = NULL; const bf_json_node_t *dims = bf_json_obj_get(doc, body, "dims");
            if (jstr(doc, body, "id", a, sizeof(a)) && jstr(doc, body, "family", b, sizeof(b)) && dims &&
                sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO chart(chart_id,family,dims,version,encoder,universe) VALUES(?,?,?,?,?,?)", -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, a, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, b, -1, SQLITE_TRANSIENT);
                sqlite3_bind_int(st, 3, (int)bf_json_get_int(dims));
                sqlite3_bind_text(st, 4, jstr(doc, body, "version", c, sizeof(c)) ? c : "1", -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(st, 5, jstr(doc, body, "encoder", d, sizeof(d)) ? d : "declared", -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(st, 6, jstr(doc, body, "universe", e, sizeof(e)) ? e : "", -1, SQLITE_TRANSIENT);
                if (sqlite3_step(st) == SQLITE_DONE) rows++; else bad++;
            } else bad++;
            sqlite3_finalize(st);
        } else if ((body = bf_json_obj_get(doc, root, "project"))) {
            Chart chart; Coord coord = {0};
            if (jstr(doc, body, "address", a, sizeof(a)) && jstr(doc, body, "chart", b, sizeof(b)) && chart_get(db, b, &chart) &&
                jstr(doc, body, "source_digest", c, sizeof(c)) &&
                (family_is_bits(chart.family) ? parse_hex_node(doc, body, "bits", &coord) : parse_values(doc, bf_json_obj_get(doc, body, "values"), &coord)) &&
                coord_store(db, a, &chart, &coord, c, jstr(doc, body, "frontier", e, sizeof(e)))) rows++;
            else bad++;
            coord_free(&coord);
        } else if ((body = bf_json_obj_get(doc, root, "relate"))) {
            sqlite3_stmt *st = NULL;
            if (jstr(doc, body, "src", a, sizeof(a)) && jstr(doc, body, "relation", b, sizeof(b)) && jstr(doc, body, "dst", c, sizeof(c)) &&
                sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO relation(src,relation,dst,evidence) VALUES(?,?,?,?)", -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, a, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, b, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 3, c, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(st, 4, jstr(doc, body, "evidence", d, sizeof(d)) ? d : "declared", -1, SQLITE_TRANSIENT);
                if (sqlite3_step(st) == SQLITE_DONE) rows++; else bad++;
            } else bad++;
            sqlite3_finalize(st);
        } else if ((body = bf_json_obj_get(doc, root, "label"))) {
            sqlite3_stmt *st = NULL;
            if (jstr(doc, body, "address", a, sizeof(a)) && jstr(doc, body, "label", b, sizeof(b)) &&
                sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO label(address,label) VALUES(?,?)", -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, a, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, b, -1, SQLITE_TRANSIENT);
                if (sqlite3_step(st) == SQLITE_DONE) rows++; else bad++;
            } else bad++;
            sqlite3_finalize(st);
        } else if ((body = bf_json_obj_get(doc, root, "bridge"))) {
            sqlite3_stmt *st = NULL; const bf_json_node_t *err_node = bf_json_obj_get(doc, body, "calibration_error");
            if (jstr(doc, body, "from", a, sizeof(a)) && jstr(doc, body, "to", b, sizeof(b)) && jstr(doc, body, "operator", c, sizeof(c)) && err_node &&
                jstr(doc, body, "loss_envelope", d, sizeof(d)) && jstr(doc, body, "validity_frontier", e, sizeof(e)) && jstr(doc, body, "provenance", g, sizeof(g)) &&
                sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO bridge VALUES(?,?,?,?,?,?,?)", -1, &st, NULL) == SQLITE_OK) {
                sqlite3_bind_text(st, 1, a, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, b, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 3, c, -1, SQLITE_TRANSIENT);
                sqlite3_bind_double(st, 4, bf_json_get_double(err_node)); sqlite3_bind_text(st, 5, d, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(st, 6, e, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 7, g, -1, SQLITE_TRANSIENT);
                if (sqlite3_step(st) == SQLITE_DONE) rows++; else bad++;
            } else bad++;
            sqlite3_finalize(st);
        } else bad++;
        bf_json_free(doc);
    }
    exec_sql(db, bad ? "ROLLBACK" : "COMMIT");
    free(line); fclose(f);
    printf("{\"imported\":%d,\"refused\":%d}\n", bad ? 0 : rows, bad);
    return !bad;
}

/* ------------------------------------------------------------ compare */

static int cmd_compare(sqlite3 *db, const char *a, const char *b, const char *chart_a, const char *chart_b) {
    Chart ca, cb; Coord xa = {0}, xb = {0}; double d; char da[128], db_[128];
    if (!chart_b) chart_b = chart_a;
    if (!chart_get(db, chart_a, &ca) || !chart_get(db, chart_b, &cb)) { printf("{\"refused\":\"unknown_chart\"}\n"); return 3; }
    if (strcmp(ca.id, cb.id)) {
        /* Different charts: only through a declared bridge between them. */
        sqlite3_stmt *st = NULL; int bridged = 0; double calibration = 0; char op[256] = "";
        if (sqlite3_prepare_v2(db, "SELECT operator,calibration_error FROM bridge WHERE from_chart=? AND to_chart=?", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, ca.id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, cb.id, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_ROW) { bridged = 1; snprintf(op, sizeof(op), "%s", sqlite3_column_text(st, 0)); calibration = sqlite3_column_double(st, 1); }
        }
        sqlite3_finalize(st);
        if (!bridged || strcmp(ca.family, cb.family) || ca.dims != cb.dims) {
            printf("{\"refused\":\"incomparable\",\"from_chart\":"); json_str(ca.id); printf(",\"to_chart\":"); json_str(cb.id);
            printf(",\"reason\":\"%s\"}\n", !bridged ? "no declared bridge between these charts" : "the bridge does not map these families onto one metric");
            return 3;
        }
        if (!coord_load(db, a, &ca, &xa, da, sizeof(da)) || !coord_load(db, b, &cb, &xb, db_, sizeof(db_)) || !chart_distance(&ca, &xa, &xb, &d)) {
            coord_free(&xa); coord_free(&xb); printf("{\"refused\":\"missing_projection\"}\n"); return 3;
        }
        printf("{\"distance\":%.6f,\"metric\":\"%s\",\"bridge\":", d, metric_name(ca.family)); json_str(op);
        printf(",\"calibration_error\":%.6f,\"truth\":\"near is not same\"}\n", calibration);
        coord_free(&xa); coord_free(&xb);
        return 0;
    }
    if (!coord_load(db, a, &ca, &xa, da, sizeof(da)) || !coord_load(db, b, &ca, &xb, db_, sizeof(db_)) || !chart_distance(&ca, &xa, &xb, &d)) {
        coord_free(&xa); coord_free(&xb); printf("{\"refused\":\"missing_projection\"}\n"); return 3;
    }
    printf("{\"distance\":%.6f,\"metric\":\"%s\",\"chart\":", d, metric_name(ca.family)); json_str(ca.id);
    printf(",\"truth\":\"near is not same\"}\n");
    coord_free(&xa); coord_free(&xb);
    return 0;
}

/* ------------------------------------------------------------ groups */

typedef struct { char address[256]; char digest[128]; Coord c; } Member;

static int members_of(sqlite3 *db, const char *group, const Chart *chart, Member **out) {
    sqlite3_stmt *st = NULL; int n = 0;
    Member *m = calloc(LA_MAX_MEMBERS, sizeof(Member));
    if (!m || sqlite3_prepare_v2(db, "SELECT r.src FROM relation r JOIN projection p ON p.address=r.src AND p.chart_id=? "
          "WHERE r.relation='member_of' AND r.dst=? ORDER BY r.src", -1, &st, NULL) != SQLITE_OK) { free(m); return -1; }
    sqlite3_bind_text(st, 1, chart->id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, group, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW && n < LA_MAX_MEMBERS) {
        snprintf(m[n].address, sizeof(m[n].address), "%s", sqlite3_column_text(st, 0));
        if (coord_load(db, m[n].address, chart, &m[n].c, m[n].digest, sizeof(m[n].digest))) n++;
    }
    sqlite3_finalize(st);
    *out = m;
    return n;
}

static void members_free(Member *m, int n) { int i; for (i = 0; i < n; i++) coord_free(&m[i].c); free(m); }

/* A text buffer the representation is written into, as canonical JSON. */
typedef struct { char *s; size_t n, cap; } Buf;
static void buf_add(Buf *b, const char *fmt, ...) {
    va_list ap; int need;
    va_start(ap, fmt); need = vsnprintf(NULL, 0, fmt, ap); va_end(ap);
    if (need < 0) return;
    if (b->n + (size_t)need + 1 > b->cap) { size_t cap = (b->cap + (size_t)need + 1) * 2; char *s = realloc(b->s, cap); if (!s) return; b->s = s; b->cap = cap; }
    va_start(ap, fmt); vsnprintf(b->s + b->n, b->cap - b->n, fmt, ap); va_end(ap);
    b->n += (size_t)need;
}

static void buf_floats(Buf *b, const float *v, int n) {
    int i;
    buf_add(b, "[");
    for (i = 0; i < n; i++) buf_add(b, "%s%.5f", i ? "," : "", v[i]);
    buf_add(b, "]");
}

/* Dense points lift to modes (k-means, k chosen where another mode stops
 * explaining a third of what is left), a rank <= 3 subspace of the centred
 * members by power iteration with deflation, dispersion, boundary exemplars
 * and outliers. */
static void lift_dense(const Member *m, int n, int dims, Buf *rep, float *basis, int *rank) {
    int k, best_k = 1, i, j, it, r;
    double sse_prev = -1;
    float *centroids = calloc((size_t)(3 * dims), sizeof(float)), *mean = calloc((size_t)dims, sizeof(float));
    int *assign = calloc((size_t)n, sizeof(int)), best_assign[LA_MAX_MEMBERS];
    float best_centroids[3 * 512];
    double dists[LA_MAX_MEMBERS], mu = 0, sd = 0;
    if (!centroids || !mean || !assign || dims > 512) { free(centroids); free(mean); free(assign); return; }
    for (i = 0; i < n; i++) for (j = 0; j < dims; j++) mean[j] += m[i].c.f[j] / n;
    for (k = 1; k <= 3 && k <= n; k++) {
        double sse = 0;
        for (i = 0; i < k; i++) memcpy(centroids + i * dims, m[(i * n) / k].c.f, sizeof(float) * (size_t)dims);
        for (it = 0; it < 12; it++) {
            int counts[3] = {0};
            for (i = 0; i < n; i++) {
                double best = 1e9; int bi = 0;
                for (j = 0; j < k; j++) { double d = metric_dense(m[i].c.f, centroids + j * dims, dims); if (d < best) { best = d; bi = j; } }
                assign[i] = bi;
            }
            memset(centroids, 0, sizeof(float) * (size_t)(k * dims));
            for (i = 0; i < n; i++) { counts[assign[i]]++; for (j = 0; j < dims; j++) centroids[assign[i] * dims + j] += m[i].c.f[j]; }
            for (j = 0; j < k; j++) if (counts[j]) for (i = 0; i < dims; i++) centroids[j * dims + i] /= counts[j];
        }
        for (i = 0; i < n; i++) sse += metric_dense(m[i].c.f, centroids + assign[i] * dims, dims);
        if (k == 1 || (sse_prev > 0 && sse < sse_prev * 0.66)) {
            best_k = k; memcpy(best_assign, assign, sizeof(int) * (size_t)n);
            memcpy(best_centroids, centroids, sizeof(float) * (size_t)(k * dims));
            sse_prev = sse;
        } else break;
    }
    /* subspace of centred members */
    r = n - 1 < 3 ? n - 1 : 3;
    if (r < 1) r = 1;
    for (int q = 0; q < r; q++) {
        float *v = basis + q * dims; double len;
        uint64_t seed = 0x5bd1e995ULL + (uint64_t)q;
        for (j = 0; j < dims; j++) v[j] = (float)((splitmix(&seed) & 0xffff) / 65535.0 - 0.5);
        for (it = 0; it < 30; it++) {
            float *next = calloc((size_t)dims, sizeof(float));
            if (!next) break;
            for (i = 0; i < n; i++) {
                double dot = 0;
                for (j = 0; j < dims; j++) dot += (m[i].c.f[j] - mean[j]) * v[j];
                for (j = 0; j < dims; j++) next[j] += (float)(dot * (m[i].c.f[j] - mean[j]));
            }
            for (int p = 0; p < q; p++) {
                double dot = 0; float *u = basis + p * dims;
                for (j = 0; j < dims; j++) dot += next[j] * u[j];
                for (j = 0; j < dims; j++) next[j] -= (float)(dot * u[j]);
            }
            len = 0; for (j = 0; j < dims; j++) len += next[j] * next[j];
            len = sqrt(len);
            if (len < 1e-9) { free(next); break; }
            for (j = 0; j < dims; j++) v[j] = (float)(next[j] / len);
            free(next);
        }
    }
    *rank = r;
    for (i = 0; i < n; i++) { dists[i] = metric_dense(m[i].c.f, best_centroids + best_assign[i] * dims, dims); mu += dists[i] / n; }
    for (i = 0; i < n; i++) sd += (dists[i] - mu) * (dists[i] - mu) / n;
    sd = sqrt(sd);
    buf_add(rep, "{\"type\":\"modes+subspace\",\"modes\":%d,\"mode_sizes\":[", best_k);
    for (j = 0; j < best_k; j++) { int c = 0; for (i = 0; i < n; i++) c += best_assign[i] == j; buf_add(rep, "%s%d", j ? "," : "", c); }
    buf_add(rep, "],\"rank\":%d,\"dispersion\":%.5f,\"boundary\":[", r, mu);
    {
        int shown = 0, used[LA_MAX_MEMBERS] = {0};
        for (int pick = 0; pick < 2 && pick < n; pick++) {
            int far = -1; for (i = 0; i < n; i++) if (!used[i] && (far < 0 || dists[i] > dists[far])) far = i;
            if (far < 0) break;
            used[far] = 1; buf_add(rep, "%s\"%s\"", shown++ ? "," : "", m[far].address);
        }
    }
    buf_add(rep, "],\"outliers\":[");
    { int shown = 0; for (i = 0; i < n; i++) if (sd > 0 && dists[i] > mu + 2 * sd) buf_add(rep, "%s\"%s\"", shown++ ? "," : "", m[i].address); }
    buf_add(rep, "],\"centroid\":");
    buf_floats(rep, mean, dims);
    buf_add(rep, "}");
    free(centroids); free(mean); free(assign);
}

static void lift_distribution(const Member *m, int n, int dims, Buf *rep, float *mixture) {
    int i, j; double spread = 0;
    memset(mixture, 0, sizeof(float) * (size_t)dims);
    for (i = 0; i < n; i++) {
        double s = 0; for (j = 0; j < dims; j++) s += m[i].c.f[j] > 0 ? m[i].c.f[j] : 0;
        for (j = 0; j < dims; j++) mixture[j] += (float)((s > 0 && m[i].c.f[j] > 0 ? m[i].c.f[j] / s : 0) / n);
    }
    for (i = 0; i < n; i++) spread += metric_hellinger(m[i].c.f, mixture, dims) / n;
    buf_add(rep, "{\"type\":\"mixture\",\"components\":%d,\"spread\":%.5f,\"mean\":", n, spread);
    buf_floats(rep, mixture, dims);
    buf_add(rep, "}");
}

static void lift_bits(const Member *m, int n, int nbytes, const char *family, Buf *rep, uint8_t *proto) {
    int i, j;
    if (!strcmp(family, "hdc")) {
        int *counts = calloc((size_t)(nbytes * 8), sizeof(int)); double spread = 0;
        if (!counts) return;
        for (i = 0; i < n; i++) for (j = 0; j < nbytes * 8; j++) counts[j] += (m[i].c.bits[j / 8] >> (j % 8)) & 1;
        memset(proto, 0, (size_t)nbytes);
        for (j = 0; j < nbytes * 8; j++) if (counts[j] * 2 > n) proto[j / 8] |= (uint8_t)(1 << (j % 8));
        for (i = 0; i < n; i++) spread += metric_hamming(m[i].c.bits, proto, nbytes) / n;
        buf_add(rep, "{\"type\":\"bundle\",\"members\":%d,\"spread\":%.5f}", n, spread);
        free(counts);
    } else {
        int medoid = 0; double best = 1e9;
        for (i = 0; i < n; i++) {
            double sum = 0;
            for (j = 0; j < n; j++) if (i != j) sum += metric_fingerprint(m[i].c.bits, m[i].c.nbits / 8, m[j].c.bits, m[j].c.nbits / 8);
            if (sum < best) { best = sum; medoid = i; }
        }
        memcpy(proto, m[medoid].c.bits, (size_t)(m[medoid].c.nbits / 8 < nbytes ? m[medoid].c.nbits / 8 : nbytes));
        buf_add(rep, "{\"type\":\"medoid\",\"medoid\":\"%s\",\"mean_error\":%.5f}", m[medoid].address, n > 1 ? best / (n - 1) : 0.0);
    }
}

static void snapshot_digest(const char *logical, const char *universe, const char *lens, const Chart *chart,
                            const Member *m, int n, const char *children, char *out) {
    Buf text = {0}; int i;
    buf_add(&text, "logical=%s\nuniverse=%s\nlens=%s\nchart=%s@%s\n", logical, universe, lens, chart->id, chart->version);
    for (i = 0; i < n; i++) buf_add(&text, "member=%s@%s\n", m[i].address, m[i].digest);
    buf_add(&text, "children=%s\n", children ? children : "");
    bf_sha256_hex((const uint8_t *)(text.s ? text.s : ""), text.n, out);
    free(text.s);
}

static int store_snapshot(sqlite3 *db, const char *snap, const char *logical, const char *universe, const char *lens, const Chart *chart,
                          const char *closure, const char *children, const char *representation, const Member *m, int n) {
    sqlite3_stmt *st = NULL; char at[32]; int i, ok = 1;
    now_text(at, sizeof(at));
    if (sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO snapshot VALUES(?,?,?,?,?,?,?,?,?)", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, snap, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, logical, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, universe, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 4, lens, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 5, chart->id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 6, closure, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 7, children ? children : "", -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 8, representation, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 9, at, -1, SQLITE_TRANSIENT);
    ok = sqlite3_step(st) == SQLITE_DONE; sqlite3_finalize(st); st = NULL;
    if (ok && sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO snapshot_member VALUES(?,?,?)", -1, &st, NULL) == SQLITE_OK) {
        for (i = 0; i < n; i++) {
            sqlite3_bind_text(st, 1, snap, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, m[i].address, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 3, m[i].digest, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) != SQLITE_DONE) ok = 0;
            sqlite3_reset(st);
        }
        sqlite3_finalize(st);
    }
    return ok;
}

static int compute_group(sqlite3 *db, const char *logical, const char *lens, const char *chart_id, const char *universe,
                         char *snap_out, char *type_out, size_t type_cap, int announce) {
    Chart chart; Member *m = NULL; int n; Buf rep = {0}; char closure[65];
    if (!chart_get(db, chart_id, &chart)) { fprintf(stderr, "unknown chart %s\n", chart_id); return 0; }
    n = members_of(db, logical, &chart, &m);
    if (n < 1) { members_free(m, n > 0 ? n : 0); fprintf(stderr, "group %s has no members projected in %s\n", logical, chart_id); return 0; }
    if (!strcmp(chart.family, "dense")) {
        float *basis = calloc((size_t)(3 * chart.dims), sizeof(float)); int rank = 0;
        if (basis) { lift_dense(m, n, chart.dims, &rep, basis, &rank); free(basis); }
        snprintf(type_out, type_cap, "modes+subspace");
    } else if (!strcmp(chart.family, "distribution")) {
        float *mix = calloc((size_t)chart.dims, sizeof(float));
        if (mix) { lift_distribution(m, n, chart.dims, &rep, mix); free(mix); }
        snprintf(type_out, type_cap, "mixture");
    } else if (family_is_bits(chart.family)) {
        uint8_t *proto = calloc((size_t)(chart.dims / 8 + 1), 1);
        int nbytes = !strcmp(chart.family, "hdc") ? chart.dims / 8 : m[0].c.nbits / 8;
        uint8_t *big = !strcmp(chart.family, "hdc") ? proto : calloc((size_t)nbytes + 1, 1);
        if (proto && big) lift_bits(m, n, nbytes, chart.family, &rep, big);
        if (big != proto) free(big);
        free(proto);
        snprintf(type_out, type_cap, !strcmp(chart.family, "hdc") ? "bundle" : "medoid");
    } else {
        buf_add(&rep, "{\"type\":\"members\",\"members\":%d}", n);
        snprintf(type_out, type_cap, "members");
    }
    {
        Buf text = {0}; int i;
        for (i = 0; i < n; i++) buf_add(&text, "%s@%s\n", m[i].address, m[i].digest);
        bf_sha256_hex((const uint8_t *)(text.s ? text.s : ""), text.n, closure);
        free(text.s);
    }
    snapshot_digest(logical, universe, lens, &chart, m, n, "", snap_out);
    if (!store_snapshot(db, snap_out, logical, universe, lens, &chart, closure, "", rep.s ? rep.s : "{}", m, n)) { members_free(m, n); free(rep.s); return 0; }
    {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(db, "INSERT INTO neighbourhood(logical,lens,level,chart_id,representation_type,current_snapshot) VALUES(?,?,1,?,?,?) "
              "ON CONFLICT(logical) DO UPDATE SET lens=excluded.lens,chart_id=excluded.chart_id,representation_type=excluded.representation_type,current_snapshot=excluded.current_snapshot",
              -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, logical, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, lens, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 3, chart_id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 4, type_out, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 5, snap_out, -1, SQLITE_TRANSIENT);
            sqlite3_step(st);
        }
        sqlite3_finalize(st);
    }
    if (announce) {
        printf("{\"logical\":"); json_str(logical); printf(",\"snapshot\":\"%s\",\"member_closure\":\"%s\",\"members\":%d,\"member_type\":", snap_out, closure, n);
        json_str(chart.family); printf(",\"representation\":%s}\n", rep.s ? rep.s : "{}");
    }
    members_free(m, n); free(rep.s);
    return 1;
}

static int snapshot_representation(sqlite3 *db, const char *snap, char *out, size_t cap, char *chart_id, size_t chart_cap) {
    sqlite3_stmt *st = NULL; int ok = 0;
    if (sqlite3_prepare_v2(db, "SELECT representation,chart_id FROM snapshot WHERE snapshot_id=?", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, snap, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) { snprintf(out, cap, "%s", sqlite3_column_text(st, 0)); if (chart_id) snprintf(chart_id, chart_cap, "%s", sqlite3_column_text(st, 1)); ok = 1; }
    sqlite3_finalize(st);
    return ok;
}

/* The vector a group snapshot carries for comparing groups: a dense group's
 * centroid, or a mixture's mean distribution. */
static int representation_centroid(const char *rep, float *out, int dims) {
    const char *p = strstr(rep, "\"centroid\":["); int n = 0;
    if (!p && (p = strstr(rep, "\"mean\":["))) p -= 4;
    if (!p) return 0;
    p += 12;
    while (*p && *p != ']' && n < dims) { char *stop; out[n++] = (float)strtod(p, &stop); p = stop; if (*p == ',') p++; }
    return n == dims;
}

static int group_parent_children(sqlite3 *db, const char *parent, char children[][256], int cap) {
    sqlite3_stmt *st = NULL; int n = 0;
    if (sqlite3_prepare_v2(db, "SELECT logical FROM neighbourhood WHERE parent=? ORDER BY logical", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, parent, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW && n < cap) snprintf(children[n++], 256, "%s", sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return n;
}

/* A group of groups is a hierarchy node: each child's current snapshot, a
 * hyperbolic coordinate for each child (radius by level, angle by order),
 * inter-child distances between their lifted representations, and a topology
 * signature naming bridges (pairs closer than the median) and isolates. */
static int compute_parent(sqlite3 *db, const char *parent, const char *lens, const char *universe, char *snap_out, int announce) {
    char children[32][256], snaps[32][65], child_list[4096] = "", rep[65536], chart_id[128] = "";
    int n = group_parent_children(db, parent, children, 32), i, j, dims = 0;
    Buf out = {0}; double dist[32][32], sorted[512], median = 0; int pairs = 0;
    float *centroids = NULL; Chart chart;
    if (n < 2) { fprintf(stderr, "a hierarchy node needs at least two child groups\n"); return 0; }
    for (i = 0; i < n; i++) {
        sqlite3_stmt *st = NULL;
        snaps[i][0] = '\0';
        if (sqlite3_prepare_v2(db, "SELECT current_snapshot,chart_id FROM neighbourhood WHERE logical=?", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, children[i], -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) {
                snprintf(snaps[i], sizeof(snaps[i]), "%s", sqlite3_column_text(st, 0));
                snprintf(chart_id, sizeof(chart_id), "%s", sqlite3_column_text(st, 1));
            }
        }
        sqlite3_finalize(st);
        if (!snaps[i][0]) { fprintf(stderr, "child %s has no snapshot\n", children[i]); return 0; }
        { size_t len = strlen(child_list); snprintf(child_list + len, sizeof(child_list) - len, "%s%s", i ? "," : "", snaps[i]); }
    }
    if (chart_get(db, chart_id, &chart) && (!strcmp(chart.family, "dense") || !strcmp(chart.family, "distribution"))) {
        dims = chart.dims;
        centroids = calloc((size_t)(n * dims), sizeof(float));
        for (i = 0; centroids && i < n; i++) if (!snapshot_representation(db, snaps[i], rep, sizeof(rep), NULL, 0) || !representation_centroid(rep, centroids + i * dims, dims)) { free(centroids); centroids = NULL; }
    }
    for (i = 0; i < n; i++) for (j = 0; j < n; j++)
        dist[i][j] = !centroids ? (i == j ? 0 : 1) : !strcmp(chart.family, "distribution") ? metric_hellinger(centroids + i * dims, centroids + j * dims, dims)
                                                                                         : metric_dense(centroids + i * dims, centroids + j * dims, dims);
    for (i = 0; i < n; i++) for (j = i + 1; j < n; j++) sorted[pairs++] = dist[i][j];
    for (i = 1; i < pairs; i++) for (j = i; j > 0 && sorted[j] < sorted[j - 1]; j--) { double t = sorted[j]; sorted[j] = sorted[j - 1]; sorted[j - 1] = t; }
    median = pairs ? sorted[pairs / 2] : 0;
    buf_add(&out, "{\"type\":\"hierarchy\",\"children\":%d,\"child_coordinates\":[", n);
    for (i = 0; i < n; i++) {
        double radius = tanh(0.5), angle = 2 * LA_PI * i / n;
        buf_add(&out, "%s{\"logical\":\"%s\",\"snapshot\":\"%s\",\"poincare\":[%.5f,%.5f]}", i ? "," : "", children[i], snaps[i], radius * cos(angle), radius * sin(angle));
    }
    buf_add(&out, "],\"distances\":[");
    for (i = 0; i < n; i++) { buf_add(&out, "%s[", i ? "," : ""); for (j = 0; j < n; j++) buf_add(&out, "%s%.5f", j ? "," : "", dist[i][j]); buf_add(&out, "]"); }
    buf_add(&out, "],\"topology\":{\"bridges\":[");
    { int shown = 0; for (i = 0; i < n; i++) for (j = i + 1; j < n; j++) if (pairs > 1 && dist[i][j] < median) buf_add(&out, "%s[\"%s\",\"%s\"]", shown++ ? "," : "", children[i], children[j]); }
    buf_add(&out, "],\"isolates\":[");
    { int shown = 0; for (i = 0; i < n; i++) { int near = 0; for (j = 0; j < n; j++) if (i != j && dist[i][j] <= median) near = 1; if (!near) buf_add(&out, "%s\"%s\"", shown++ ? "," : "", children[i]); } }
    buf_add(&out, "]}}");
    {
        Buf text = {0};
        buf_add(&text, "logical=%s\nuniverse=%s\nlens=%s\nchildren=%s\n", parent, universe, lens, child_list);
        bf_sha256_hex((const uint8_t *)text.s, text.n, snap_out);
        free(text.s);
    }
    if (!chart_get(db, chart_id, &chart)) snprintf(chart.id, sizeof(chart.id), "%s", chart_id), snprintf(chart.version, sizeof(chart.version), "1");
    store_snapshot(db, snap_out, parent, universe, lens, &chart, "", child_list, out.s ? out.s : "{}", NULL, 0);
    {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(db, "INSERT INTO neighbourhood(logical,lens,level,chart_id,representation_type,current_snapshot) VALUES(?,?,2,?,'hierarchy',?) "
              "ON CONFLICT(logical) DO UPDATE SET current_snapshot=excluded.current_snapshot,representation_type='hierarchy'", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, parent, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, lens, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 3, chart_id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 4, snap_out, -1, SQLITE_TRANSIENT);
            sqlite3_step(st);
        }
        sqlite3_finalize(st);
    }
    if (announce) { printf("{\"logical\":"); json_str(parent); printf(",\"snapshot\":\"%s\",\"member_type\":\"group\",\"representation\":%s}\n", snap_out, out.s ? out.s : "{}"); }
    free(out.s); free(centroids);
    return 1;
}

static int attach_children(sqlite3 *db, const char *parent, const char *children) {
    char copy[4096], *save = NULL, *child; sqlite3_stmt *st = NULL; int ok = 1;
    snprintf(copy, sizeof(copy), "%s", children);
    if (sqlite3_prepare_v2(db, "UPDATE neighbourhood SET parent=? WHERE logical=?", -1, &st, NULL) != SQLITE_OK) return 0;
    for (child = strtok_r(copy, ",", &save); child; child = strtok_r(NULL, ",", &save)) {
        sqlite3_bind_text(st, 1, parent, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, child, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) != SQLITE_DONE || sqlite3_changes(db) != 1) ok = 0;
        sqlite3_reset(st);
    }
    sqlite3_finalize(st);
    return ok;
}

/* ------------------------------------------------------------ mutate / refresh */

static int cmd_mutate(sqlite3 *db, const char *address, const char *chart_id, const char *values, const char *bits, const char *digest) {
    Chart chart; Coord old = {0}, next = {0}; char from[128] = ""; double moved = 0; char at[32];
    if (!chart_get(db, chart_id, &chart) || !digest) { fprintf(stderr, "mutate needs a known chart and a source digest\n"); return 0; }
    coord_load(db, address, &chart, &old, from, sizeof(from));
    if (family_is_bits(chart.family)) {
        if (!parse_hex(bits, &next)) { coord_free(&old); coord_free(&next); return 0; }
    }
    else {
        char copy[65536], *save = NULL, *tok; int n = 0;
        snprintf(copy, sizeof(copy), "%s", values ? values : "");
        if (!(next.f = malloc(sizeof(float) * LA_MAX_DIMS))) { coord_free(&old); return 0; }
        for (tok = strtok_r(copy, ",", &save); tok && n < LA_MAX_DIMS; tok = strtok_r(NULL, ",", &save)) next.f[n++] = (float)atof(tok);
        next.n = n;
    }
    if ((old.f || old.bits) && !chart_distance(&chart, &old, &next, &moved)) moved = -1;
    if (!coord_store(db, address, &chart, &next, digest, "")) { coord_free(&old); coord_free(&next); return 0; }
    now_text(at, sizeof(at));
    {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(db, "INSERT INTO latent_delta VALUES(?,?,?,?,?,?)", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, address, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, chart_id, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 3, from, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 4, digest, -1, SQLITE_TRANSIENT);
            sqlite3_bind_double(st, 5, moved); sqlite3_bind_text(st, 6, at, -1, SQLITE_TRANSIENT);
            sqlite3_step(st);
        }
        sqlite3_finalize(st);
    }
    printf("{\"address\":"); json_str(address); printf(",\"chart\":"); json_str(chart_id);
    printf(",\"from\":\"%s\",\"to\":", from); json_str(digest); printf(",\"moved\":%.6f}\n", moved);
    coord_free(&old); coord_free(&next);
    return 1;
}

/* Only groups whose exact member closure changed recompute; a parent
 * recomputes only if a changed child moved beyond the envelope. */
static int cmd_refresh(sqlite3 *db, const char *universe, double envelope) {
    sqlite3_stmt *st = NULL; char changed[64][256], unchanged[256][256]; int nc = 0, nu = 0, i;
    char parents[64][256]; int np = 0;
    Buf out = {0};
    if (sqlite3_prepare_v2(db, "SELECT n.logical,n.lens,n.chart_id,n.current_snapshot,COALESCE(n.parent,'') FROM neighbourhood n WHERE n.level=1 ORDER BY n.logical", -1, &st, NULL) != SQLITE_OK) return 0;
    buf_add(&out, "{\"groups\":[");
    while (sqlite3_step(st) == SQLITE_ROW) {
        char logical[256], lens[256], chart_id[128], snap[65], parent[256], closure_now[65], closure_then[128] = "";
        Chart chart; Member *m = NULL; int n;
        snprintf(logical, sizeof(logical), "%s", sqlite3_column_text(st, 0)); snprintf(lens, sizeof(lens), "%s", sqlite3_column_text(st, 1));
        snprintf(chart_id, sizeof(chart_id), "%s", sqlite3_column_text(st, 2)); snprintf(snap, sizeof(snap), "%s", sqlite3_column_text(st, 3) ? (const char *)sqlite3_column_text(st, 3) : "");
        snprintf(parent, sizeof(parent), "%s", sqlite3_column_text(st, 4));
        if (!chart_get(db, chart_id, &chart)) continue;
        n = members_of(db, logical, &chart, &m);
        { Buf text = {0}; for (i = 0; i < n; i++) buf_add(&text, "%s@%s\n", m[i].address, m[i].digest); bf_sha256_hex((const uint8_t *)(text.s ? text.s : ""), text.n, closure_now); free(text.s); }
        members_free(m, n > 0 ? n : 0);
        {
            sqlite3_stmt *q = NULL;
            if (sqlite3_prepare_v2(db, "SELECT member_closure FROM snapshot WHERE snapshot_id=?", -1, &q, NULL) == SQLITE_OK) {
                sqlite3_bind_text(q, 1, snap, -1, SQLITE_TRANSIENT);
                if (sqlite3_step(q) == SQLITE_ROW) snprintf(closure_then, sizeof(closure_then), "%s", sqlite3_column_text(q, 0));
            }
            sqlite3_finalize(q);
        }
        if (!strcmp(closure_now, closure_then)) { if (nu < 256) snprintf(unchanged[nu++], 256, "%s", logical); continue; }
        {
            char new_snap[65], type[32], old_rep[65536] = "", new_rep[65536] = ""; double delta = 1.0;
            snapshot_representation(db, snap, old_rep, sizeof(old_rep), NULL, 0);
            if (!compute_group(db, logical, lens, chart_id, universe, new_snap, type, sizeof(type), 0)) continue;
            snapshot_representation(db, new_snap, new_rep, sizeof(new_rep), NULL, 0);
            if (!strcmp(chart.family, "dense") || !strcmp(chart.family, "distribution")) {
                float *a = calloc((size_t)chart.dims, sizeof(float)), *b = calloc((size_t)chart.dims, sizeof(float));
                if (a && b && representation_centroid(old_rep, a, chart.dims) && representation_centroid(new_rep, b, chart.dims))
                    delta = !strcmp(chart.family, "distribution") ? metric_hellinger(a, b, chart.dims) : metric_dense(a, b, chart.dims);
                free(a); free(b);
            }
            buf_add(&out, "%s{\"logical\":\"%s\",\"from\":\"%s\",\"to\":\"%s\",\"representation_moved\":%.6f}", nc ? "," : "", logical, snap, new_snap, delta);
            if (nc < 64) snprintf(changed[nc++], 256, "%s", logical);
            if (parent[0]) {
                char at[32]; int seen = 0;
                for (i = 0; i < np; i++) if (!strcmp(parents[i], parent)) seen = 1;
                if (delta <= envelope) {
                    sqlite3_stmt *h = NULL;
                    now_text(at, sizeof(at));
                    if (sqlite3_prepare_v2(db, "INSERT INTO envelope_hold VALUES(?,?,?,?,?,?,?)", -1, &h, NULL) == SQLITE_OK) {
                        sqlite3_bind_text(h, 1, parent, -1, SQLITE_TRANSIENT); sqlite3_bind_text(h, 2, logical, -1, SQLITE_TRANSIENT);
                        sqlite3_bind_text(h, 3, snap, -1, SQLITE_TRANSIENT); sqlite3_bind_text(h, 4, new_snap, -1, SQLITE_TRANSIENT);
                        sqlite3_bind_double(h, 5, delta); sqlite3_bind_double(h, 6, envelope); sqlite3_bind_text(h, 7, at, -1, SQLITE_TRANSIENT);
                        sqlite3_step(h);
                    }
                    sqlite3_finalize(h);
                } else if (!seen && np < 64) snprintf(parents[np++], 256, "%s", parent);
            }
        }
    }
    sqlite3_finalize(st);
    buf_add(&out, "],\"unchanged\":[");
    for (i = 0; i < nu; i++) buf_add(&out, "%s\"%s\"", i ? "," : "", unchanged[i]);
    buf_add(&out, "],\"parents_recomputed\":[");
    for (i = 0; i < np; i++) {
        char lens[256] = "", snap[65];
        sqlite3_stmt *q = NULL;
        if (sqlite3_prepare_v2(db, "SELECT lens FROM neighbourhood WHERE logical=?", -1, &q, NULL) == SQLITE_OK) {
            sqlite3_bind_text(q, 1, parents[i], -1, SQLITE_TRANSIENT);
            if (sqlite3_step(q) == SQLITE_ROW) snprintf(lens, sizeof(lens), "%s", sqlite3_column_text(q, 0));
        }
        sqlite3_finalize(q);
        if (compute_parent(db, parents[i], lens, universe, snap, 0)) buf_add(&out, "%s{\"logical\":\"%s\",\"to\":\"%s\"}", i ? "," : "", parents[i], snap);
    }
    buf_add(&out, "],\"parents_held\":[");
    {
        sqlite3_stmt *q = NULL; int shown = 0;
        if (sqlite3_prepare_v2(db, "SELECT DISTINCT parent FROM envelope_hold WHERE child_to IN (SELECT current_snapshot FROM neighbourhood)", -1, &q, NULL) == SQLITE_OK) {
            while (sqlite3_step(q) == SQLITE_ROW) {
                int recomputed = 0;
                for (i = 0; i < np; i++) if (!strcmp(parents[i], (const char *)sqlite3_column_text(q, 0))) recomputed = 1;
                if (!recomputed) buf_add(&out, "%s\"%s\"", shown++ ? "," : "", sqlite3_column_text(q, 0));
            }
        }
        sqlite3_finalize(q);
    }
    buf_add(&out, "],\"envelope\":%.6f}", envelope);
    printf("%s\n", out.s);
    free(out.s);
    return 1;
}

/* ------------------------------------------------------------ reopen */

static int cmd_reopen(sqlite3 *db, const char *logical) {
    sqlite3_stmt *st = NULL; char snap[65] = "", chart_id[128] = ""; int exact = 0, drifted = 0, missing = 0, shown = 0;
    if (sqlite3_prepare_v2(db, "SELECT current_snapshot,chart_id FROM neighbourhood WHERE logical=?", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, logical, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) { snprintf(snap, sizeof(snap), "%s", sqlite3_column_text(st, 0)); snprintf(chart_id, sizeof(chart_id), "%s", sqlite3_column_text(st, 1)); }
    sqlite3_finalize(st); st = NULL;
    if (!snap[0]) { printf("{\"refused\":\"unknown_neighbourhood\"}\n"); return 0; }
    printf("{\"logical\":"); json_str(logical); printf(",\"snapshot\":\"%s\",\"members\":[", snap);
    if (sqlite3_prepare_v2(db, "SELECT m.address,m.source_digest,p.source_digest FROM snapshot_member m LEFT JOIN projection p ON p.address=m.address AND p.chart_id=? "
          "WHERE m.snapshot_id=? ORDER BY m.address", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, chart_id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, snap, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *address = (const char *)sqlite3_column_text(st, 0), *then = (const char *)sqlite3_column_text(st, 1), *now = (const char *)sqlite3_column_text(st, 2);
            const char *state = !now ? "missing" : strcmp(then, now) ? "drifted" : "exact";
            if (!now) missing++; else if (strcmp(then, now)) drifted++; else exact++;
            printf("%s{\"address\":", shown++ ? "," : ""); json_str(address); printf(",\"state\":\"%s\"}", state);
        }
    }
    sqlite3_finalize(st);
    printf("],\"exact\":%d,\"drifted\":%d,\"missing\":%d,\"truth\":\"membership is exact; nearness inside it is not identity\"}\n", exact, drifted, missing);
    return 1;
}

/* ------------------------------------------------------------ neighbours / lens */

typedef struct { char address[256]; double score; int rank[8]; } Candidate;

static int cmp_candidate(const void *a, const void *b) {
    const Candidate *x = a, *y = b;
    return x->score < y->score ? -1 : x->score > y->score ? 1 : strcmp(x->address, y->address);
}

static int cmd_neighbours(sqlite3 *db, const char *address, const char *chart_id, int k, const char *within) {
    Chart chart; Coord q = {0}; sqlite3_stmt *st = NULL; Candidate *c = calloc(4096, sizeof(Candidate)); int n = 0, i; char digest[128];
    if (!c || !chart_get(db, chart_id, &chart) || !coord_load(db, address, &chart, &q, digest, sizeof(digest))) { free(c); printf("{\"refused\":\"missing_projection\"}\n"); return 0; }
    if (sqlite3_prepare_v2(db, within ? "SELECT p.address FROM projection p JOIN relation r ON r.src=p.address AND r.relation='member_of' AND r.dst=? WHERE p.chart_id=? AND p.address<>?"
                                      : "SELECT address FROM projection WHERE chart_id=? AND address<>? AND ?1 IS NOT NULL", -1, &st, NULL) != SQLITE_OK) { free(c); coord_free(&q); return 0; }
    if (within) { sqlite3_bind_text(st, 1, within, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, chart_id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 3, address, -1, SQLITE_TRANSIENT); }
    else { sqlite3_bind_text(st, 1, chart_id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, address, -1, SQLITE_TRANSIENT); }
    while (sqlite3_step(st) == SQLITE_ROW && n < 4096) {
        Coord o = {0}; char od[128];
        snprintf(c[n].address, sizeof(c[n].address), "%s", sqlite3_column_text(st, 0));
        if (coord_load(db, c[n].address, &chart, &o, od, sizeof(od)) && chart_distance(&chart, &q, &o, &c[n].score)) n++;
        coord_free(&o);
    }
    sqlite3_finalize(st);
    qsort(c, (size_t)n, sizeof(Candidate), cmp_candidate);
    printf("{\"address\":"); json_str(address); printf(",\"chart\":"); json_str(chart_id); printf(",\"metric\":\"%s\",\"considered\":%d,\"neighbours\":[", metric_name(chart.family), n);
    for (i = 0; i < n && i < k; i++) { printf("%s{\"address\":", i ? "," : ""); json_str(c[i].address); printf(",\"distance\":%.6f}", c[i].score); }
    printf("],\"truth\":\"near is not same\"}\n");
    free(c); coord_free(&q);
    return 1;
}

/* A lens is a small typed query program: exact eligibility first (members of
 * a neighbourhood), then per-chart rankings from the query's own projection,
 * then a fusion rule (reciprocal-rank, intersection of each chart's slice, or
 * Pareto non-domination), then an attempt slice that widens when too few
 * survive, then the exact reopen list.  Charts that the query address is not
 * projected in are skipped and reported, never imputed. */
static int cmd_lens(sqlite3 *db, const char *program_path) {
    FILE *f = fopen(program_path, "rb"); char *text = NULL; long size; char err[160]; bf_json_doc_t *doc = NULL; const bf_json_node_t *root, *charts, *ch;
    char query[256], within[256], fusion[32], buf[256]; int slice = 8, widen = 1, nc = 0, i, j, k, n = 0;
    Chart chart_list[8]; Candidate *cand = calloc(2048, sizeof(Candidate)); char skipped[1024] = "";
    if (!f || !cand || fseek(f, 0, SEEK_END) || (size = ftell(f)) <= 0 || fseek(f, 0, SEEK_SET) || !(text = malloc((size_t)size + 1)) || fread(text, 1, (size_t)size, f) != (size_t)size) { if (f) fclose(f); free(text); free(cand); return 0; }
    fclose(f); text[size] = '\0';
    if (!(doc = bf_json_parse_str(text, err, sizeof(err))) || !(root = bf_json_root(doc))) { if (doc) bf_json_free(doc); free(text); free(cand); fprintf(stderr, "lens: %s\n", err); return 0; }
    snprintf(query, sizeof(query), "%.255s", jstr(doc, root, "query", buf, sizeof(buf)) ? buf : "");
    snprintf(within, sizeof(within), "%.255s", jstr(doc, root, "within", buf, sizeof(buf)) ? buf : "");
    snprintf(fusion, sizeof(fusion), "%.31s", jstr(doc, root, "fusion", buf, sizeof(buf)) ? buf : "rrf");
    if (bf_json_obj_get(doc, root, "slice")) slice = (int)bf_json_get_int(bf_json_obj_get(doc, root, "slice"));
    if (bf_json_obj_get(doc, root, "widen")) widen = bf_json_get_bool(bf_json_obj_get(doc, root, "widen"));
    charts = bf_json_obj_get(doc, root, "charts");
    for (ch = charts ? bf_json_child_first(doc, charts) : NULL; ch && nc < 8; ch = bf_json_child_next(doc, ch)) {
        char id[128]; Coord probe = {0}; char d[128];
        if (ch->type != BF_JSON_STRING || !bf_json_get_str_copy(ch, id, sizeof(id)) || !chart_get(db, id, &chart_list[nc])) continue;
        if (!coord_load(db, query, &chart_list[nc], &probe, d, sizeof(d))) { size_t l = strlen(skipped); snprintf(skipped + l, sizeof(skipped) - l, "%s\"%s\"", l ? "," : "", id); continue; }
        coord_free(&probe); nc++;
    }
    {   /* exact eligibility */
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(db, "SELECT src FROM relation WHERE relation='member_of' AND dst=? AND src<>? ORDER BY src", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, within, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, query, -1, SQLITE_TRANSIENT);
            while (sqlite3_step(st) == SQLITE_ROW && n < 2048) { snprintf(cand[n].address, sizeof(cand[n].address), "%s", sqlite3_column_text(st, 0)); for (j = 0; j < 8; j++) cand[n].rank[j] = 1 << 20; n++; }
        }
        sqlite3_finalize(st);
    }
    for (k = 0; k < nc && n > 0; k++) {
        Coord q = {0}; char d[128]; double *score = calloc((size_t)n, sizeof(double)); int *order = calloc((size_t)n, sizeof(int)), m = 0;
        if (!score || !order) {
            free(score); free(order); bf_json_free(doc); free(text); free(cand);
            return 0;
        }
        coord_load(db, query, &chart_list[k], &q, d, sizeof(d));
        for (i = 0; i < n; i++) {
            Coord o = {0}; char od[128];
            if (coord_load(db, cand[i].address, &chart_list[k], &o, od, sizeof(od)) && chart_distance(&chart_list[k], &q, &o, &score[i])) order[m++] = i;
            coord_free(&o);
        }
        for (i = 1; i < m; i++) for (j = i; j > 0 && score[order[j]] < score[order[j - 1]]; j--) { int t = order[j]; order[j] = order[j - 1]; order[j - 1] = t; }
        for (i = 0; i < m; i++) cand[order[i]].rank[k] = i + 1;
        free(score); free(order); coord_free(&q);
    }
    for (i = 0; i < n; i++) {
        double s = 0;
        if (!strcmp(fusion, "intersection")) { s = 0; for (k = 0; k < nc; k++) if (cand[i].rank[k] > slice) s = 1e9; if (s < 1e9) for (k = 0; k < nc; k++) s += cand[i].rank[k]; }
        else if (!strcmp(fusion, "pareto")) {
            int dominated = 0;
            for (j = 0; j < n && !dominated; j++) {
                int better = 0, worse = 0;
                if (i == j) continue;
                for (k = 0; k < nc; k++) { if (cand[j].rank[k] < cand[i].rank[k]) better = 1; if (cand[j].rank[k] > cand[i].rank[k]) worse = 1; }
                if (better && !worse) dominated = 1;
            }
            s = dominated ? 1e9 : 0; for (k = 0; k < nc; k++) s += cand[i].rank[k] * 1e-6;
        } else { for (k = 0; k < nc; k++) s -= cand[i].rank[k] < (1 << 20) ? 1.0 / (60 + cand[i].rank[k]) : 0; }
        cand[i].score = s;
    }
    qsort(cand, (size_t)n, sizeof(Candidate), cmp_candidate);
    {
        int survivors = 0, widened = 0;
        for (i = 0; i < n; i++) if (cand[i].score < 1e8) survivors++;
        if (survivors < slice && widen && strcmp(fusion, "rrf")) {
            /* Too few survived the strict rule: widen to reciprocal-rank so a
             * narrow slice never reads as nothing being there. */
            for (i = 0; i < n; i++) { double s = 0; for (k = 0; k < nc; k++) s -= cand[i].rank[k] < (1 << 20) ? 1.0 / (60 + cand[i].rank[k]) : 0; cand[i].score = s; }
            qsort(cand, (size_t)n, sizeof(Candidate), cmp_candidate);
            widened = 1;
        }
        printf("{\"query\":"); json_str(query); printf(",\"within\":"); json_str(within); printf(",\"fusion\":\"%s\",\"charts\":[", fusion);
        for (k = 0; k < nc; k++) { printf("%s", k ? "," : ""); json_str(chart_list[k].id); }
        printf("],\"skipped_charts\":[%s],\"eligible\":%d,\"widened\":%s,\"slice\":[", skipped, n, widened ? "true" : "false");
        for (i = 0, j = 0; i < n && j < slice; i++) {
            if (!widened && cand[i].score >= 1e8) continue;
            printf("%s{\"address\":", j++ ? "," : ""); json_str(cand[i].address); printf(",\"ranks\":[");
            for (k = 0; k < nc; k++) printf("%s%d", k ? "," : "", cand[i].rank[k] < (1 << 20) ? cand[i].rank[k] : -1);
            printf("]}");
        }
        printf("],\"reopen\":\"every address in the slice is reopened against its exact owner before use\",\"truth\":\"an attempt slice omits nothing from existence\"}\n");
    }
    bf_json_free(doc); free(text); free(cand);
    return 1;
}

/* ------------------------------------------------------------ supervision */

/* Exact joins write typed training anchors: positives carry the relation that
 * joined them (a live performance is performance_of a work, not the same
 * material), and hard negatives are the same words naming different exact
 * addresses. */
static int cmd_anchors(sqlite3 *db) {
    sqlite3_stmt *st = NULL; int positives = 0, negatives = 0;
    if (sqlite3_prepare_v2(db, "SELECT src,relation,dst,evidence FROM relation WHERE relation<>'member_of' ORDER BY src,relation,dst", -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            printf("{\"anchor\":\"positive\",\"relation\":"); json_str((const char *)sqlite3_column_text(st, 1));
            printf(",\"a\":"); json_str((const char *)sqlite3_column_text(st, 0)); printf(",\"b\":"); json_str((const char *)sqlite3_column_text(st, 2));
            printf(",\"evidence\":"); json_str((const char *)sqlite3_column_text(st, 3)); printf("}\n");
            positives++;
        }
    }
    sqlite3_finalize(st); st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT a.address,b.address,a.label FROM label a JOIN label b ON lower(trim(a.label))=lower(trim(b.label)) AND a.address<b.address "
          "WHERE NOT EXISTS(SELECT 1 FROM relation r WHERE r.relation<>'member_of' AND ((r.src=a.address AND r.dst=b.address) OR (r.src=b.address AND r.dst=a.address))) "
          "ORDER BY a.address,b.address", -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            printf("{\"anchor\":\"hard_negative\",\"reason\":\"same words, different exact address\",\"a\":"); json_str((const char *)sqlite3_column_text(st, 0));
            printf(",\"b\":"); json_str((const char *)sqlite3_column_text(st, 1)); printf(",\"words\":"); json_str((const char *)sqlite3_column_text(st, 2)); printf("}\n");
            negatives++;
        }
    }
    sqlite3_finalize(st);
    fprintf(stderr, "{\"positives\":%d,\"hard_negatives\":%d}\n", positives, negatives);
    return 1;
}

/* ------------------------------------------------------------ automation */

/* Relations a compiler writes are replaced as a whole on each run, so a
 * mention that is no longer found does not linger. */
static int cmd_forget(sqlite3 *db, const char *relation, const char *evidence_prefix, const char *source) {
    sqlite3_stmt *st = NULL; int ok;
    char like[256], prefix[768];
    snprintf(like, sizeof(like), "%s%%", evidence_prefix ? evidence_prefix : "");
    if (source && (!*source || strchr(source, '/'))) return 0;
    if (source && snprintf(prefix, sizeof(prefix), "o/%s/", source) >= (int)sizeof(prefix)) return 0;
    ok = sqlite3_prepare_v2(db, source
        ? "DELETE FROM relation WHERE relation=?1 AND evidence LIKE ?2 AND substr(src,1,length(?3))=?3"
        : "DELETE FROM relation WHERE relation=?1 AND evidence LIKE ?2", -1, &st, NULL) == SQLITE_OK;
    if (ok) { sqlite3_bind_text(st, 1, relation, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, like, -1, SQLITE_TRANSIENT); if (source) sqlite3_bind_text(st, 3, prefix, -1, SQLITE_TRANSIENT); ok = sqlite3_step(st) == SQLITE_DONE; }
    sqlite3_finalize(st);
    printf("{\"forgotten\":%d,\"relation\":", ok ? sqlite3_changes(db) : 0); json_str(relation); printf("}\n");
    return ok;
}

/* Exact, atomic source-scoped linker materialization. The input ledger is a
 * derived frontier inside Atlas, not a new fact owner. One changed source may
 * replace its own linker links, never another source or authored evidence. */
typedef struct {
    char source[128], input[65], context[65], result[65];
} LinkerScope;

static int linker_hash_ok(const char *s) {
    if (!s || strlen(s)!=64) return 0;
    for (const unsigned char *p=(const unsigned char*)s; *p; p++) if (!isxdigit(*p)) return 0;
    return 1;
}
static int linker_scope_cmp(const void *a,const void *b) {
    return strcmp(((const LinkerScope*)a)->source,((const LinkerScope*)b)->source);
}
static int linker_source_ok(const char *s) {
    if (!s || !*s || strlen(s)>=128) return 0;
    for (const unsigned char *p=(const unsigned char*)s; *p; p++)
        if (!isalnum(*p) && *p!='_' && *p!='-' && *p!='.') return 0;
    return 1;
}
static int linker_swap(sqlite3 *db,const char *scope_path,const char *relations_path) {
    FILE *sf=NULL,*rf=NULL;
    LinkerScope *scopes=calloc(4096,sizeof(*scopes));
    char *line=NULL; size_t cap=0; ssize_t len;
    sqlite3_stmt *del=NULL,*up=NULL,*retire=NULL,*insert=NULL;
    int n=0,rows=0,preserved=0,retired=0,ok=0,tx=0;
    if (!scopes || !scope_path || !relations_path ||
        !(sf=fopen(scope_path,"r")) || !(rf=fopen(relations_path,"r"))) goto done;
    while ((len=getline(&line,&cap,sf))>0) {
        char *parts[4],*save=NULL,*part=strtok_r(line,"\t\r\n",&save);
        if (!part || n>=4096) goto done;
        for (int k=0;k<4;k++) {
            if (!part) goto done;
            parts[k]=part;
            part=strtok_r(NULL,"\t\r\n",&save);
        }
        if (part || !linker_source_ok(parts[0]) ||
            (strcmp(parts[1],"-") && !linker_hash_ok(parts[1])) ||
            (strcmp(parts[1],"-")==0 && strcmp(parts[2],"-")) ||
            (strcmp(parts[1],"-") && !linker_hash_ok(parts[2])) ||
            !linker_hash_ok(parts[3])) goto done;
        snprintf(scopes[n].source,sizeof(scopes[n].source),"%s",parts[0]);
        snprintf(scopes[n].input,sizeof(scopes[n].input),"%s",parts[1]);
        snprintf(scopes[n].context,sizeof(scopes[n].context),"%s",parts[2]);
        snprintf(scopes[n].result,sizeof(scopes[n].result),"%s",parts[3]);
        n++;
    }
    if (ferror(sf)) goto done;
    free(line); line=NULL; cap=0;
    qsort(scopes,(size_t)n,sizeof(*scopes),linker_scope_cmp);
    for (int i=1;i<n;i++) if (!strcmp(scopes[i-1].source,scopes[i].source)) goto done;
    if (!exec_sql(db,"BEGIN IMMEDIATE;")) goto done;
    tx=1;
    if (!exec_sql(db,"CREATE TABLE IF NOT EXISTS _linker_source_generation("
        "source TEXT PRIMARY KEY,input_digest TEXT NOT NULL,"
        "context_digest TEXT NOT NULL,relation_digest TEXT NOT NULL);")) goto done;
    if (sqlite3_prepare_v2(db,"DELETE FROM relation WHERE src>=?1 AND src<?2 AND evidence LIKE 'linker:%'",-1,&del,NULL)!=SQLITE_OK ||
        sqlite3_prepare_v2(db,"INSERT INTO _linker_source_generation(source,input_digest,context_digest,relation_digest) "
            "VALUES(?1,?2,?3,?4) ON CONFLICT(source) DO UPDATE SET "
            "input_digest=excluded.input_digest,context_digest=excluded.context_digest,"
            "relation_digest=excluded.relation_digest",-1,&up,NULL)!=SQLITE_OK ||
        sqlite3_prepare_v2(db,"DELETE FROM _linker_source_generation WHERE source=?1",-1,&retire,NULL)!=SQLITE_OK ||
        sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO relation(src,relation,dst,evidence) VALUES(?1,?2,?3,?4)",-1,&insert,NULL)!=SQLITE_OK) goto done;
    for (int i=0;i<n;i++) {
        char low[256],high[256];
        if (snprintf(low,sizeof(low),"o/%s/",scopes[i].source)>=(int)sizeof(low) ||
            snprintf(high,sizeof(high),"o/%s0",scopes[i].source)>=(int)sizeof(high)) goto done;
        sqlite3_bind_text(del,1,low,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(del,2,high,-1,SQLITE_TRANSIENT);
        if (sqlite3_step(del)!=SQLITE_DONE) goto done;
        sqlite3_reset(del); sqlite3_clear_bindings(del);
    }
    while ((len=getline(&line,&cap,rf))>0) {
        char err[160],src[512],kind[128],dst[512],evidence[512],owner[128];
        bf_json_doc_t *doc=bf_json_parse(line,(size_t)len,err,sizeof(err));
        const bf_json_node_t *root=doc?bf_json_root(doc):NULL;
        const bf_json_node_t *rel=root?bf_json_obj_get(doc,root,"relate"):NULL;
        const char *end=NULL; LinkerScope key={0};
        if (!rel || !jstr(doc,rel,"src",src,sizeof(src)) ||
            !jstr(doc,rel,"relation",kind,sizeof(kind)) ||
            !jstr(doc,rel,"dst",dst,sizeof(dst)) ||
            !jstr(doc,rel,"evidence",evidence,sizeof(evidence)) ||
            strncmp(src,"o/",2) || !(end=strchr(src+2,'/')) ||
            end==src+2 || (size_t)(end-(src+2))>=sizeof(owner) ||
            strncmp(evidence,"linker:",7)) { bf_json_free(doc); goto done; }
        memcpy(owner,src+2,(size_t)(end-(src+2)));
        owner[end-(src+2)]='\0';
        snprintf(key.source,sizeof(key.source),"%s",owner);
        LinkerScope *admitted=bsearch(&key,scopes,(size_t)n,sizeof(*scopes),linker_scope_cmp);
        if (!admitted || !strcmp(admitted->input,"-")) { bf_json_free(doc); goto done; }
        sqlite3_bind_text(insert,1,src,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(insert,2,kind,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(insert,3,dst,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(insert,4,evidence,-1,SQLITE_TRANSIENT);
        int step=sqlite3_step(insert);
        if (step==SQLITE_DONE) { if (sqlite3_changes(db)) rows++; else preserved++; }
        sqlite3_reset(insert); sqlite3_clear_bindings(insert);
        bf_json_free(doc);
        if (step!=SQLITE_DONE) goto done;
    }
    if (ferror(rf)) goto done;
    for (int i=0;i<n;i++) {
        LinkerScope *s=&scopes[i]; sqlite3_stmt *st=NULL;
        if (!strcmp(s->input,"-")) {
            st=retire;
            sqlite3_bind_text(st,1,s->source,-1,SQLITE_TRANSIENT);
            retired++;
        } else {
            st=up;
            sqlite3_bind_text(st,1,s->source,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(st,2,s->input,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(st,3,s->context,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(st,4,s->result,-1,SQLITE_TRANSIENT);
        }
        if (sqlite3_step(st)!=SQLITE_DONE) goto done;
        sqlite3_reset(st); sqlite3_clear_bindings(st);
    }
    if (!exec_sql(db,"COMMIT;")) goto done;
    tx=0; ok=1;
    printf("{\"schema\":\"bonfyre.atlas.linker-source-cut.v1\","
           "\"source_cuts\":%d,\"sources_retired\":%d,"
           "\"relations_written\":%d,\"authored_relations_preserved\":%d}\n",
           n,retired,rows,preserved);
done:
    if (tx) exec_sql(db,"ROLLBACK;");
    if (!ok) fprintf(stderr,"[atlas] scoped linker batch refused; no source generation committed\n");
    sqlite3_finalize(del); sqlite3_finalize(up); sqlite3_finalize(retire); sqlite3_finalize(insert);
    free(line); if (sf) fclose(sf); if (rf) fclose(rf); free(scopes);
    return ok;
}
/* Every census item (bonfyre-web-address census-items) projected into the
 * words chart in one pass: hashed trigrams of its title and words, a source
 * digest over exactly those words, and membership of its family's group.
 * Items whose words have not changed keep their projection untouched. */
static int cmd_project_items(sqlite3 *db, const char *items_path, const char *chart_id, int dims) {
    FILE *f = fopen(items_path, "r"); char *line = NULL; size_t cap = 0; ssize_t len; int projected = 0, unchanged = 0, members = 0;
    Chart chart; float *v = malloc(sizeof(float) * (size_t)dims);
    sqlite3_stmt *seen = NULL, *rel = NULL;
    if (!f || !v) { if (f) fclose(f); free(v); return 0; }
    {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO chart(chart_id,family,dims,version,encoder,universe) VALUES(?,'dense',?,'trigram-hash-v1','bonfyre-latent-atlas project-items','census-items')", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, chart_id, -1, SQLITE_TRANSIENT); sqlite3_bind_int(st, 2, dims); sqlite3_step(st);
        }
        sqlite3_finalize(st);
    }
    if (!chart_get(db, chart_id, &chart) || chart.dims != dims) { fclose(f); free(v); fprintf(stderr, "chart %s is not a %d-dimension dense chart\n", chart_id, dims); return 0; }
    sqlite3_prepare_v2(db, "SELECT source_digest FROM projection WHERE address=? AND chart_id=?", -1, &seen, NULL);
    sqlite3_prepare_v2(db, "INSERT OR REPLACE INTO relation(src,relation,dst,evidence) VALUES(?,'member_of',?,'census family')", -1, &rel, NULL);
    exec_sql(db, "BEGIN");
    while ((len = getline(&line, &cap, f)) > 0) {
        char err[160], source[128], key[40], title[1024], body[1024], family[96], address[200], group[200], digest[65], text[2100];
        bf_json_doc_t *doc = bf_json_parse(line, (size_t)len, err, sizeof(err)); const bf_json_node_t *root = doc ? bf_json_root(doc) : NULL;
        Coord coord = {0};
        if (!root || !jstr(doc, root, "source", source, sizeof(source)) || !jstr(doc, root, "key", key, sizeof(key)) || !jstr(doc, root, "title", title, sizeof(title))) { if (doc) bf_json_free(doc); continue; }
        if (!jstr(doc, root, "body", body, sizeof(body))) body[0] = '\0';
        if (!jstr(doc, root, "family", family, sizeof(family))) snprintf(family, sizeof(family), "unfiled");
        bf_json_free(doc);
        snprintf(address, sizeof(address), "o/%s/%s", source, key);
        snprintf(text, sizeof(text), "%s | %s", title, body);
        bf_sha256_hex((const uint8_t *)text, strlen(text), digest);
        sqlite3_reset(seen); sqlite3_bind_text(seen, 1, address, -1, SQLITE_TRANSIENT); sqlite3_bind_text(seen, 2, chart_id, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(seen) == SQLITE_ROW && !strcmp((const char *)sqlite3_column_text(seen, 0), digest)) unchanged++;
        else {
            encode_text(text, v, dims);
            coord.f = v; coord.n = dims;
            if (coord_store(db, address, &chart, &coord, digest, "census-items")) projected++;
        }
        snprintf(group, sizeof(group), "family:%s@%s", family, chart_id);
        sqlite3_reset(rel); sqlite3_bind_text(rel, 1, address, -1, SQLITE_TRANSIENT); sqlite3_bind_text(rel, 2, group, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(rel) == SQLITE_DONE) members++;
    }
    exec_sql(db, "COMMIT");
    sqlite3_finalize(seen); sqlite3_finalize(rel); free(line); fclose(f); free(v);
    printf("{\"projected\":%d,\"unchanged\":%d,\"memberships\":%d,\"chart\":", projected, unchanged, members); json_str(chart_id); printf("}\n");
    return 1;
}

/* ------------------------------------------------------------ main */

static void usage(void) {
    fputs("usage: bonfyre-latent-atlas init --db DB\n"
          "       bonfyre-latent-atlas import --db DB --jsonl FILE        (chart|project|relate|label|bridge lines)\n"
          "       bonfyre-latent-atlas compare --db DB --a ADDR --b ADDR --chart C [--to-chart C2]\n"
          "       bonfyre-latent-atlas group --db DB --logical L --lens LENS --chart C [--universe U]\n"
          "       bonfyre-latent-atlas lift --db DB --logical PARENT --lens LENS --children L1,L2,... [--universe U]\n"
          "       bonfyre-latent-atlas mutate --db DB --address A --chart C (--values v,v,.. | --bits HEX) --source-digest D\n"
          "       bonfyre-latent-atlas refresh --db DB [--universe U] [--envelope E]\n"
          "       bonfyre-latent-atlas reopen --db DB --logical L\n"
          "       bonfyre-latent-atlas neighbours --db DB --address A --chart C [--k K] [--within L]\n"
          "       bonfyre-latent-atlas lens --db DB --program FILE\n"
          "       bonfyre-latent-atlas anchors --db DB\n"
          "       bonfyre-latent-atlas forget --db DB --relation R [--evidence-prefix P] [--source SOURCE]\n"
          "       bonfyre-latent-atlas project-items --db DB --items CENSUS_ITEMS.jsonl [--chart words-256 --dims 256]\n"
          "       bonfyre-latent-atlas encode-text --text T [--dims N]\n"
          "       bonfyre-latent-atlas encode-roles --roles 'role=value;role=value' [--dims N]\n"
          "       bonfyre-latent-atlas acoustic --pcm FILE    (11025 Hz mono s16le)\n", stderr);
}

static void print_floats(const float *v, int n) { int i; putchar('['); for (i = 0; i < n; i++) printf("%s%.6f", i ? "," : "", v[i]); putchar(']'); }
static void print_hex(const uint8_t *b, int n) { int i; putchar('"'); for (i = 0; i < n; i++) printf("%02x", b[i]); putchar('"'); }

#define LATENT_COMMANDS(X) \
    X("acoustic", LC_ACOUSTIC) \
    X("anchors", LC_ANCHORS) \
    X("compare", LC_COMPARE) \
    X("encode-roles", LC_ENCODE_ROLES) \
    X("encode-text", LC_ENCODE_TEXT) \
    X("forget", LC_FORGET) \
    X("group", LC_GROUP) \
    X("import", LC_IMPORT) \
    X("init", LC_INIT) \
    X("lens", LC_LENS) \
    X("lift", LC_LIFT) \
    X("mutate", LC_MUTATE) \
    X("neighbours", LC_NEIGHBOURS) \
    X("project-items", LC_PROJECT_ITEMS) \
    X("refresh", LC_REFRESH) \
    X("reopen", LC_REOPEN)

enum {
#define LATENT_ENUM(name, id) id,
    LATENT_COMMANDS(LATENT_ENUM)
#undef LATENT_ENUM
};
static const BfCommandEntry latent_commands[] = {
#define LATENT_ENTRY(name, id) {name, id, 0, NULL},
    LATENT_COMMANDS(LATENT_ENTRY)
#undef LATENT_ENTRY
};

int main(int argc, char **argv) {
    const char *dbpath = argc > 2 ? arg(argc, argv, "--db") : NULL; sqlite3 *db = NULL; int ok = 0;
    if (argc < 2) { usage(); return 2; }
    const BfCommandEntry *command = bf_command_lookup(latent_commands,
        sizeof(latent_commands) / sizeof(latent_commands[0]), argv[1]);
    if (!command) { usage(); return 2; }
    if (command->id == LC_ENCODE_TEXT) {
        int dims = arg(argc, argv, "--dims") ? atoi(arg(argc, argv, "--dims")) : 256; float *v;
        if (!arg(argc, argv, "--text") || dims < 8 || dims > LA_MAX_DIMS || !(v = malloc(sizeof(float) * (size_t)dims))) { usage(); return 2; }
        encode_text(arg(argc, argv, "--text"), v, dims); print_floats(v, dims); putchar('\n'); free(v); return 0;
    }
    if (command->id == LC_ENCODE_ROLES) {
        int dims = arg(argc, argv, "--dims") ? atoi(arg(argc, argv, "--dims")) : 4096; uint8_t *v;
        if (!arg(argc, argv, "--roles") || dims < 64 || dims > LA_MAX_DIMS || dims % 8 || !(v = malloc((size_t)dims / 8))) { usage(); return 2; }
        if (encode_roles(arg(argc, argv, "--roles"), v, dims / 8) < 0) { free(v); return 1; }
        print_hex(v, dims / 8); putchar('\n'); free(v); return 0;
    }
    if (command->id == LC_ACOUSTIC) {
        uint8_t *fp = NULL; int bytes = 0, frames; float dist[24], traj[32];
        if (!arg(argc, argv, "--pcm")) { usage(); return 2; }
        frames = acoustic(arg(argc, argv, "--pcm"), &fp, &bytes, dist, traj);
        if (!frames) { fprintf(stderr, "acoustic: too little audio\n"); return 1; }
        printf("{\"frames\":%d,\"fingerprint\":", frames); print_hex(fp, bytes);
        printf(",\"distribution\":"); print_floats(dist, 24); printf(",\"trajectory\":"); print_floats(traj, 32); printf("}\n");
        free(fp); return 0;
    }
    if (!dbpath || !(db = open_db(dbpath))) { usage(); return 2; }
    if (command->id == LC_INIT) ok = 1;
    else if (command->id == LC_IMPORT && arg(argc, argv, "--jsonl") && arg(argc, argv, "--linker-sources"))
        ok = linker_swap(db, arg(argc, argv, "--linker-sources"), arg(argc, argv, "--jsonl"));
    else if (command->id == LC_IMPORT && arg(argc, argv, "--jsonl")) ok = import_jsonl(db, arg(argc, argv, "--jsonl"));
    else if (command->id == LC_COMPARE && arg(argc, argv, "--a") && arg(argc, argv, "--b") && arg(argc, argv, "--chart")) {
        int rc = cmd_compare(db, arg(argc, argv, "--a"), arg(argc, argv, "--b"), arg(argc, argv, "--chart"), arg(argc, argv, "--to-chart"));
        sqlite3_close(db); return rc;
    } else if (command->id == LC_GROUP && arg(argc, argv, "--logical") && arg(argc, argv, "--lens") && arg(argc, argv, "--chart")) {
        char snap[65], type[32];
        ok = compute_group(db, arg(argc, argv, "--logical"), arg(argc, argv, "--lens"), arg(argc, argv, "--chart"),
                           arg(argc, argv, "--universe") ? arg(argc, argv, "--universe") : "", snap, type, sizeof(type), 1);
    } else if (command->id == LC_LIFT && arg(argc, argv, "--logical") && arg(argc, argv, "--lens") && arg(argc, argv, "--children")) {
        char snap[65];
        ok = attach_children(db, arg(argc, argv, "--logical"), arg(argc, argv, "--children")) &&
             compute_parent(db, arg(argc, argv, "--logical"), arg(argc, argv, "--lens"), arg(argc, argv, "--universe") ? arg(argc, argv, "--universe") : "", snap, 1);
    } else if (command->id == LC_MUTATE && arg(argc, argv, "--address") && arg(argc, argv, "--chart")) {
        ok = cmd_mutate(db, arg(argc, argv, "--address"), arg(argc, argv, "--chart"), arg(argc, argv, "--values"), arg(argc, argv, "--bits"), arg(argc, argv, "--source-digest"));
    } else if (command->id == LC_REFRESH) {
        ok = cmd_refresh(db, arg(argc, argv, "--universe") ? arg(argc, argv, "--universe") : "", arg(argc, argv, "--envelope") ? atof(arg(argc, argv, "--envelope")) : 0.02);
    } else if (command->id == LC_REOPEN && arg(argc, argv, "--logical")) ok = cmd_reopen(db, arg(argc, argv, "--logical"));
    else if (command->id == LC_NEIGHBOURS && arg(argc, argv, "--address") && arg(argc, argv, "--chart"))
        ok = cmd_neighbours(db, arg(argc, argv, "--address"), arg(argc, argv, "--chart"), arg(argc, argv, "--k") ? atoi(arg(argc, argv, "--k")) : 8, arg(argc, argv, "--within"));
    else if (command->id == LC_LENS && arg(argc, argv, "--program")) ok = cmd_lens(db, arg(argc, argv, "--program"));
    else if (command->id == LC_ANCHORS) ok = cmd_anchors(db);
    else if (command->id == LC_FORGET && arg(argc, argv, "--relation")) ok = cmd_forget(db, arg(argc, argv, "--relation"), arg(argc, argv, "--evidence-prefix"), arg(argc, argv, "--source"));
    else if (command->id == LC_PROJECT_ITEMS && arg(argc, argv, "--items"))
        ok = cmd_project_items(db, arg(argc, argv, "--items"), arg(argc, argv, "--chart") ? arg(argc, argv, "--chart") : "words-256", arg(argc, argv, "--dims") ? atoi(arg(argc, argv, "--dims")) : 256);
    else usage();
    sqlite3_close(db);
    return ok ? 0 : 1;
}
