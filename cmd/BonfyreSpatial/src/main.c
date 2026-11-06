/* The spatial owner: an append-only observation log, and the exact closure that
 * reads it.
 *
 * The cheap half of spatial work is showing where things are now. The expensive
 * half is going backwards: saying what a place looked like at an instant, and
 * being able to say it again tomorrow and get the same answer. Almost every
 * system that claims to do this actually stores current state and overwrites it,
 * so "what did we think at 22:00" is unanswerable the moment 22:01 arrives.
 *
 * Two independent time axes, because one is not enough to be honest:
 *
 *   valid time      effective_at .. valid_until -- when the position holds in
 *                   the world
 *   knowledge time  recorded_at -- when this estate learned it
 *
 * A fix observed at 23:10 and received at 23:20 is real at 23:10 and unknown to
 * us until 23:20. A reconstruction has to be able to answer both "where was it
 * at 23:10" and "where did we believe it was at 23:15", and those are different
 * questions with different answers. Collapsing them is the cheat this file
 * exists to refuse.
 *
 * Nothing is ever updated or deleted. Retraction appends a record that retracts
 * an earlier fact, so a reconstruction as of a knowledge time before the
 * retraction still sees the retracted fact -- which is what makes a past answer
 * reproducible rather than merely remembered. Triggers enforce that at the
 * store, not by convention.
 *
 * What this file does not own: identity, authority, disclosure, provider
 * admission, proof. A position is a statement about where an existing subject
 * was observed. It creates no subject and permits nothing.
 */
#define _POSIX_C_SOURCE 200809L
#include <ctype.h>
#include <math.h>
#include <limits.h>
#include <time.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sqlite3.h>
#include "bonfyre.h"
#include "bf_sqlite_path.h"
#include "bf_command_lookup.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "bf_lex.h"
#define EARTH_RADIUS_M 6371008.8

/* ---------------------------------------------------------------- time ---- */

/* Instants are compared lexically, which is only sound if every instant has the
 * same shape. A loosely parsed timestamp compares wrong silently and the whole
 * reconstruction quietly returns the wrong era, so anything that is not exactly
 * YYYY-MM-DDTHH:MM:SSZ is refused rather than coerced. */
static int instant_valid(const char *t) {
    if (!t || strlen(t) != 20) return 0;
    static const int digit[] = {0,1,2,3,5,6,8,9,11,12,14,15,17,18};
    for (size_t i = 0; i < sizeof(digit)/sizeof(*digit); i++)
        if (!isdigit((unsigned char)t[digit[i]])) return 0;
    return t[4]=='-' && t[7]=='-' && t[10]=='T' && t[13]==':' && t[16]==':' && t[19]=='Z';
}

static int require_instant(const char *label, const char *t) {
    if (instant_valid(t)) return 1;
    fprintf(stderr, "%s must be an instant shaped YYYY-MM-DDTHH:MM:SSZ, got '%s'\n",
            label, t ? t : "(none)");
    return 0;
}

/* ------------------------------------------------------------ geometry ---- */

static double haversine_m(double lat1, double lon1, double lat2, double lon2) {
    double p = M_PI / 180.0;
    double dlat = (lat2 - lat1) * p, dlon = (lon2 - lon1) * p;
    double a = sin(dlat/2) * sin(dlat/2) +
               cos(lat1*p) * cos(lat2*p) * sin(dlon/2) * sin(dlon/2);
    return 2 * EARTH_RADIUS_M * asin(fmin(1.0, sqrt(a)));
}

/* ------------------------------------------------------------- extents ---- */

/* An extent is not a point and must not be measured as one.
 *
 * The cheap move is to store a centroid and let every distance query treat a
 * fire perimeter, a delivery zone or a rail line as the dot at its middle. That
 * answers "how far to the middle of it", reports it as "how far to it", and is
 * wrong by the size of the thing -- most wrong exactly where the object is
 * largest and the question matters most. So `lon`/`lat` stay NULL for anything
 * that is not a point, and distance is computed against the real geometry.
 *
 * A bounding box is stored too, but only ever to narrow: a box is a cheap
 * rejection and never an answer.
 */
typedef struct { double lon, lat; int part, ring; } BfVertex;

typedef struct {
    BfVertex *v;
    size_t n, cap;
    int vertex_depth;
} BfShape;

static void shape_free(BfShape *s) { free(s->v); s->v = NULL; s->n = s->cap = 0; }

static int shape_push(BfShape *s, double lon, double lat, int part, int ring) {
    if (s->n == s->cap) {
        size_t grown = s->cap ? s->cap * 2 : 64;
        BfVertex *bigger = realloc(s->v, grown * sizeof(*bigger));
        if (!bigger) return 0;
        s->v = bigger; s->cap = grown;
    }
    s->v[s->n].lon = lon; s->v[s->n].lat = lat;
    s->v[s->n].part = part; s->v[s->n].ring = ring;
    s->n++;
    return 1;
}

/* How deep the numbers sit is a property of the declared type, so it is read
 * from the type rather than guessed from the nesting -- a guess would silently
 * mis-read a degenerate shape. */
static int geometry_vertex_depth(const char *type) {
    /* Case-insensitive because the same type arrives twice in two spellings: as
     * GeoJSON declares it on the way in, and as `geometry_kind` stores it on the
     * way out. A case-sensitive match here reads a stored polygon as a shape it
     * knows nothing about and quietly answers that nothing contains anything. */
    if (!type) return 0;
    if (!strcasecmp(type, "Point")) return 1;
    if (!strcasecmp(type, "MultiPoint") || !strcasecmp(type, "LineString")) return 2;
    if (!strcasecmp(type, "Polygon") || !strcasecmp(type, "MultiLineString")) return 3;
    if (!strcasecmp(type, "MultiPolygon")) return 4;
    return 0;
}

/* Walks the coordinate text once, recording each position with the part and
 * ring it belongs to. Deliberately not a general JSON parser: it reads exactly
 * the nested-array-of-numbers shape GeoJSON coordinates are defined to have,
 * and anything else yields no vertices rather than a plausible misreading. */
static int shape_parse(const char *geometry, const char *type, BfShape *out) {
    const char *coords = geometry ? strstr(geometry, "\"coordinates\"") : NULL;
    int depth = 0, part = 0, ring = 0;
    double pending[2];
    int have = 0;
    memset(out, 0, sizeof(*out));
    out->vertex_depth = geometry_vertex_depth(type);
    if (!coords || !out->vertex_depth) return 0;
    coords = strchr(coords, '[');
    if (!coords) return 0;

    for (const char *p = coords; *p; p++) {
        if (*p == '[') { depth++; have = 0; continue; }
        if (*p == ']') {
            if (have >= 2 && depth == out->vertex_depth)
                if (!shape_push(out, pending[0], pending[1], part, ring)) { shape_free(out); return 0; }
            have = 0;
            depth--;
            /* Closing a coordinate pair leaves the scanner inside its ring, so a
             * ring ends one level shallower than that and a part one shallower
             * again. Counting at the pair level makes every vertex its own ring,
             * which reads a polygon as a scatter of single points and answers
             * that nothing contains anything. */
            if (depth == out->vertex_depth - 2 && out->vertex_depth >= 3) ring++;
            if (depth == out->vertex_depth - 3 && out->vertex_depth >= 4) { part++; ring = 0; }
            if (depth <= 0) break;
            continue;
        }
        if (*p == '-' || *p == '.' || isdigit((unsigned char)*p)) {
            char *end = NULL;
            double value = strtod(p, &end);
            if (end == p) continue;
            if (depth == out->vertex_depth && have < 2) pending[have++] = value;
            else if (depth == out->vertex_depth && have >= 2) { /* altitude: ignored */ }
            p = end - 1;
            continue;
        }
        if (*p == ',' && depth == out->vertex_depth && have >= 2) {
            if (!shape_push(out, pending[0], pending[1], part, ring)) { shape_free(out); return 0; }
            have = 0;
        }
    }
    return out->n > 0;
}

static void shape_bbox(const BfShape *s, double *min_lon, double *min_lat,
                       double *max_lon, double *max_lat) {
    *min_lon = *min_lat = 1e18; *max_lon = *max_lat = -1e18;
    for (size_t i = 0; i < s->n; i++) {
        if (s->v[i].lon < *min_lon) *min_lon = s->v[i].lon;
        if (s->v[i].lat < *min_lat) *min_lat = s->v[i].lat;
        if (s->v[i].lon > *max_lon) *max_lon = s->v[i].lon;
        if (s->v[i].lat > *max_lat) *max_lat = s->v[i].lat;
    }
}

/* Ray casting per ring. Ring 0 of a part is its outer boundary and the rest are
 * holes, which is why a hole has to subtract rather than be ignored: a point in
 * the courtyard of a building is not in the building. */
static int shape_contains(const BfShape *s, double lon, double lat) {
    int current_part = -1, inside_outer = 0, in_hole = 0, answer = 0;
    size_t i = 0;
    if (s->vertex_depth < 3) return 0;   /* only an area can contain */
    while (i < s->n) {
        int part = s->v[i].part, ring = s->v[i].ring, crossings = 0;
        size_t start = i;
        while (i < s->n && s->v[i].part == part && s->v[i].ring == ring) i++;
        for (size_t a = start, b = i - 1; a < i; b = a++) {
            double lat_a = s->v[a].lat, lat_b = s->v[b].lat;
            double lon_a = s->v[a].lon, lon_b = s->v[b].lon;
            if ((lat_a > lat) != (lat_b > lat) &&
                lon < (lon_b - lon_a) * (lat - lat_a) / (lat_b - lat_a) + lon_a)
                crossings = !crossings;
        }
        if (part != current_part) {
            if (current_part >= 0 && inside_outer && !in_hole) answer = 1;
            current_part = part; inside_outer = 0; in_hole = 0;
        }
        if (ring == 0) inside_outer = crossings;
        else if (crossings) in_hole = 1;
    }
    if (inside_outer && !in_hole) answer = 1;
    return answer;
}

/* Metres from a point to a segment, in a local tangent plane. Good to well
 * under a metre at the extents this estate works over, and stated as an
 * approximation rather than presented as geodesic truth. */
static double point_segment_m(double lat, double lon,
                              double lat_a, double lon_a, double lat_b, double lon_b) {
    double scale = cos(lat * M_PI / 180.0);
    double px = (lon - lon_a) * scale, py = lat - lat_a;
    double bx = (lon_b - lon_a) * scale, by = lat_b - lat_a;
    double len2 = bx * bx + by * by, t = 0;
    if (len2 > 0) {
        t = (px * bx + py * by) / len2;
        if (t < 0) t = 0; else if (t > 1) t = 1;
    }
    {
        double dx = px - t * bx, dy = py - t * by;
        return sqrt(dx * dx + dy * dy) * (M_PI / 180.0) * EARTH_RADIUS_M;
    }
}

/* Distance to the shape itself: zero inside an area, otherwise the nearest
 * point on any boundary or line. */
static double shape_distance_m(const BfShape *s, double lat, double lon) {
    double best = 1e18;
    if (s->n == 0) return best;
    if (s->vertex_depth == 1 || s->n == 1)
        return haversine_m(lat, lon, s->v[0].lat, s->v[0].lon);
    if (shape_contains(s, lon, lat)) return 0.0;
    {
        size_t i = 0;
        while (i < s->n) {
            int part = s->v[i].part, ring = s->v[i].ring;
            size_t start = i;
            while (i < s->n && s->v[i].part == part && s->v[i].ring == ring) i++;
            if (i - start == 1) {
                double d = haversine_m(lat, lon, s->v[start].lat, s->v[start].lon);
                if (d < best) best = d;
                continue;
            }
            for (size_t a = start + 1; a < i; a++) {
                double d = point_segment_m(lat, lon, s->v[a-1].lat, s->v[a-1].lon,
                                           s->v[a].lat, s->v[a].lon);
                if (d < best) best = d;
            }
            /* An area's boundary closes; a line's does not. */
            if (s->vertex_depth >= 3 && i - start > 2) {
                double d = point_segment_m(lat, lon, s->v[i-1].lat, s->v[i-1].lon,
                                           s->v[start].lat, s->v[start].lon);
                if (d < best) best = d;
            }
        }
    }
    return best;
}

/* Closest approach between two shapes. Exact for the non-crossing case, which
 * is checked for separately: two segments that cross are at distance zero and
 * the endpoint-to-segment minimum would report the gap between their ends. */
static int segments_cross(double ax, double ay, double bx, double by,
                          double cx, double cy, double dx, double dy) {
    double d1 = (bx-ax)*(cy-ay) - (by-ay)*(cx-ax);
    double d2 = (bx-ax)*(dy-ay) - (by-ay)*(dx-ax);
    double d3 = (dx-cx)*(ay-cy) - (dy-cy)*(ax-cx);
    double d4 = (dx-cx)*(by-cy) - (dy-cy)*(bx-cx);
    return ((d1 > 0) != (d2 > 0)) && ((d3 > 0) != (d4 > 0));
}

static double shape_to_shape_m(const BfShape *a, const BfShape *b) {
    double best = 1e18;
    if (!a->n || !b->n) return best;
    /* Either containing the other is zero distance, not the gap to an edge. */
    if (a->vertex_depth >= 3 && shape_contains(a, b->v[0].lon, b->v[0].lat)) return 0.0;
    if (b->vertex_depth >= 3 && shape_contains(b, a->v[0].lon, a->v[0].lat)) return 0.0;
    for (size_t i = 0; i + 1 < a->n; i++) {
        if (a->v[i].part != a->v[i+1].part || a->v[i].ring != a->v[i+1].ring) continue;
        for (size_t k = 0; k + 1 < b->n; k++) {
            if (b->v[k].part != b->v[k+1].part || b->v[k].ring != b->v[k+1].ring) continue;
            if (segments_cross(a->v[i].lon, a->v[i].lat, a->v[i+1].lon, a->v[i+1].lat,
                               b->v[k].lon, b->v[k].lat, b->v[k+1].lon, b->v[k+1].lat))
                return 0.0;
            {
                double d = point_segment_m(a->v[i].lat, a->v[i].lon,
                                           b->v[k].lat, b->v[k].lon, b->v[k+1].lat, b->v[k+1].lon);
                if (d < best) best = d;
                d = point_segment_m(b->v[k].lat, b->v[k].lon,
                                    a->v[i].lat, a->v[i].lon, a->v[i+1].lat, a->v[i+1].lon);
                if (d < best) best = d;
            }
        }
    }
    /* Degenerate single-vertex shapes have no segment to measure against. */
    if (best > 1e17) {
        for (size_t i = 0; i < a->n; i++)
            for (size_t k = 0; k < b->n; k++) {
                double d = haversine_m(a->v[i].lat, a->v[i].lon, b->v[k].lat, b->v[k].lon);
                if (d < best) best = d;
            }
    }
    return best;
}

/* A 2D frustum: inside the range and inside the angular sweep.
 *
 * This is what a camera, a stage, a delivery radius or a radar cell can be
 * asked in the absence of terrain. It is deliberately not a line-of-sight
 * answer: nothing here holds an elevation model, so a wall, a building or a
 * hill does not occlude, and the result says what lies within the declared
 * reach rather than what could actually be seen. */
static int within_frustum(double lat, double lon, double olat, double olon,
                          double heading_deg, double fov_deg, double range_m,
                          double *out_distance) {
    double d = haversine_m(olat, olon, lat, lon);
    double bearing, delta;
    if (out_distance) *out_distance = d;
    if (d > range_m) return 0;
    if (fov_deg >= 360.0) return 1;
    if (d < 1e-9) return 1;
    {
        double y = sin((lon - olon) * M_PI / 180.0) * cos(lat * M_PI / 180.0);
        double x = cos(olat * M_PI / 180.0) * sin(lat * M_PI / 180.0) -
                   sin(olat * M_PI / 180.0) * cos(lat * M_PI / 180.0) *
                   cos((lon - olon) * M_PI / 180.0);
        bearing = atan2(y, x) * 180.0 / M_PI;
        if (bearing < 0) bearing += 360.0;
    }
    delta = bearing - heading_deg;
    while (delta > 180.0) delta -= 360.0;
    while (delta < -180.0) delta += 360.0;
    return fabs(delta) <= fov_deg / 2.0;
}

/* ----------------------------------------------------------------- json ---- */

/* Delegates to the one implementation. Nineteen copies of this shape existed
 * with sixteen different opinions about escaping.
 */
static void json_text(const char *s) {
    bf_lex_json_emit(stdout, s);
}

/* ---------------------------------------------------------------- store ---- */

static int exec_sql(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "sqlite: %s\n", err ? err : "unknown");
        sqlite3_free(err);
        return 0;
    }
    return 1;
}

/* Append-only is enforced here rather than trusted. A convention that only the
 * writing code honours is a convention that the next writer breaks. */
/* Adding a column to a table that already has it is an error, and a store that
 * predates the extent work does not have them. The migration is therefore run
 * statement by statement and a duplicate-column complaint is the expected
 * answer on every open after the first. */
static void migrate_extent_columns(sqlite3 *db) {
    static const char *const columns[] = { "min_lon", "min_lat", "max_lon", "max_lat" };
    for (size_t i = 0; i < sizeof(columns)/sizeof(*columns); i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "ALTER TABLE spatial_fact ADD COLUMN %s REAL", columns[i]);
        sqlite3_exec(db, sql, NULL, NULL, NULL);
    }
}

static int init_schema(sqlite3 *db) {
    int ok = exec_sql(db,
      "PRAGMA journal_mode=WAL;"
      "CREATE TABLE IF NOT EXISTS spatial_fact("
      "  fact_id INTEGER PRIMARY KEY AUTOINCREMENT,"
      "  observation_id TEXT NOT NULL,"
      "  subject TEXT NOT NULL,"
      "  geometry_kind TEXT NOT NULL,"
      "  lon REAL, lat REAL,"
      "  geometry TEXT NOT NULL,"
      "  crs TEXT NOT NULL,"
      "  observed_at TEXT NOT NULL,"
      "  effective_at TEXT NOT NULL,"
      "  valid_until TEXT,"
      "  recorded_at TEXT NOT NULL,"
      "  observation_mode TEXT NOT NULL,"
      "  source_occurrence TEXT NOT NULL,"
      "  derived_from TEXT NOT NULL DEFAULT '[]',"
      "  retracts INTEGER,"
      "  retraction_reason TEXT,"
      "  accuracy_h REAL,"
      "  provenance TEXT NOT NULL);"
      /* An extent's box. Stored only to narrow -- a box is a cheap rejection
       * and never an answer, so nothing reports a distance to one. */

      "CREATE INDEX IF NOT EXISTS spatial_fact_subject"
      "  ON spatial_fact(subject, effective_at);"
      "CREATE INDEX IF NOT EXISTS spatial_fact_knowledge"
      "  ON spatial_fact(recorded_at);"
      /* Retractions are rare and the check for them is asked once per candidate
       * row. Without this the check falls onto the recorded_at index, which at a
       * late knowledge frontier matches nearly every fact ever written, and a
       * single-subject reconstruction degrades to scanning the whole log tens of
       * times over. Partial, so it stays the size of the retractions and not of
       * the log. */
      "CREATE INDEX IF NOT EXISTS spatial_fact_retraction"
      "  ON spatial_fact(retracts, recorded_at) WHERE retracts IS NOT NULL;"
      /* An observation coordinate is looked up by name twice: once when a
       * derivation asks whether its basis still stands, and once on every
       * ingested feature to decide whether this document has been observed
       * before. Both were scanning the whole log, which made intake quadratic in
       * the size of the estate and made a single reconstruction pay for every
       * fact ever written. */
      "CREATE INDEX IF NOT EXISTS spatial_fact_observation"
      "  ON spatial_fact(observation_id);"
      /* Derivations are a small minority of facts and only a derivation can be
       * orphaned, so the set of them is worth naming. Partial, so it stays the
       * size of the derivations rather than of the log. */
      "CREATE INDEX IF NOT EXISTS spatial_fact_derived"
      "  ON spatial_fact(fact_id) WHERE derived_from <> '[]';"
      /* Derived state, maintained from the delta rather than recomputed. It
       * holds the latest fact this estate knows for each subject; expiry is
       * still applied at read time against the instant actually asked about,
       * because "current" is a question and not a stored answer. */
      "CREATE TABLE IF NOT EXISTS current_spatial_state("
      "  subject TEXT PRIMARY KEY,"
      "  fact_id INTEGER NOT NULL,"
      "  effective_at TEXT NOT NULL,"
      "  maintained_through INTEGER NOT NULL);"
      /* Identifier lineage. A source may change the identifier it prefers for a
       * thing while still publishing the identifiers it used before: USGS says
       * exactly that about its GeoJSON event `id` and its `ids` list. Without
       * this table a preferred-id change splits one earthquake into two
       * subjects, and nothing afterwards can tell they were ever one.
       *
       * What is recorded is that a source declared these identifiers together
       * at an observed time. It is not an assertion that two events are the
       * same physical event -- the source said so, and the source can be
       * wrong. */
      "CREATE TABLE IF NOT EXISTS subject_alias("
      "  alias TEXT PRIMARY KEY,"
      "  subject TEXT NOT NULL,"
      "  first_seen TEXT NOT NULL,"
      "  last_seen TEXT NOT NULL,"
      "  declared_by TEXT NOT NULL);"
      "CREATE INDEX IF NOT EXISTS subject_alias_subject ON subject_alias(subject);"
      "CREATE TABLE IF NOT EXISTS maintenance_watermark("
      "  name TEXT PRIMARY KEY,"
      "  last_fact_id INTEGER NOT NULL);"
      "CREATE TRIGGER IF NOT EXISTS spatial_fact_no_update BEFORE UPDATE ON spatial_fact"
      "  BEGIN SELECT RAISE(ABORT,'spatial facts are append-only: retract, do not update'); END;"
      "CREATE TRIGGER IF NOT EXISTS spatial_fact_no_delete BEFORE DELETE ON spatial_fact"
      "  BEGIN SELECT RAISE(ABORT,'spatial facts are append-only: retract, do not delete'); END;");
    if (ok) migrate_extent_columns(db);
    return ok;
}

static const char *arg(int argc, char **argv, const char *flag) {
    for (int i = 2; i + 1 < argc; i++) if (!strcmp(argv[i], flag)) return argv[i+1];
    return NULL;
}

static int has_flag(int argc, char **argv, const char *flag) {
    for (int i = 2; i < argc; i++) if (!strcmp(argv[i], flag)) return 1;
    return 0;
}

/* A source claim and an estate derivation are different kinds of statement. The
 * split is made here once so no query has to remember it. */
static int mode_is_source_claim(const char *mode) {
    return mode && (!strcmp(mode, "observed") || !strcmp(mode, "reported"));
}

static int mode_valid(const char *mode) {
    static const char *const modes[] = {"observed","reported","propagated",
                                        "interpolated","inferred","simulated"};
    if (!mode) return 0;
    for (size_t i = 0; i < sizeof(modes)/sizeof(*modes); i++)
        if (!strcmp(mode, modes[i])) return 1;
    return 0;
}

/* --------------------------------------------------------------- observe -- */

static int cmd_observe(sqlite3 *db, int argc, char **argv) {
    const char *id = arg(argc, argv, "--observation");
    const char *subject = arg(argc, argv, "--subject");
    const char *kind = arg(argc, argv, "--geometry-kind");
    const char *geometry = arg(argc, argv, "--geometry");
    const char *crs = arg(argc, argv, "--crs");
    const char *observed = arg(argc, argv, "--observed-at");
    const char *effective = arg(argc, argv, "--effective-at");
    const char *until = arg(argc, argv, "--valid-until");
    const char *recorded = arg(argc, argv, "--recorded-at");
    const char *mode = arg(argc, argv, "--mode");
    const char *occurrence = arg(argc, argv, "--source-occurrence");
    const char *derived = arg(argc, argv, "--derived-from");
    const char *provenance = arg(argc, argv, "--provenance");
    const char *lon = arg(argc, argv, "--lon"), *lat = arg(argc, argv, "--lat");
    sqlite3_stmt *st = NULL;

    if (!id || !subject || !geometry || !crs || !occurrence || !provenance) {
        fprintf(stderr, "observe requires --observation --subject --geometry --crs "
                        "--source-occurrence --provenance\n");
        return 2;
    }
    if (!mode_valid(mode)) {
        fprintf(stderr, "--mode must be one of observed reported propagated "
                        "interpolated inferred simulated\n");
        return 2;
    }
    if (!require_instant("--observed-at", observed)) return 2;
    if (!require_instant("--recorded-at", recorded)) return 2;
    /* effective_at defaults to the moment the source saw it; a position that
     * claims to hold before it was observed is a modelling error, not a
     * shorthand, so it is refused rather than reordered. */
    if (!effective) effective = observed;
    if (!require_instant("--effective-at", effective)) return 2;
    if (strcmp(effective, observed) < 0) {
        fprintf(stderr, "--effective-at precedes --observed-at: a position cannot "
                        "hold before it was observed\n");
        return 2;
    }
    if (until && !require_instant("--valid-until", until)) return 2;
    if (until && strcmp(until, effective) <= 0) {
        fprintf(stderr, "--valid-until does not follow --effective-at: the claim "
                        "would expire before it holds\n");
        return 2;
    }
    /* A derived position that names no basis is indistinguishable from an
     * observation the moment it is written. Refuse it at the door. */
    if (!mode_is_source_claim(mode) && (!derived || !strcmp(derived, "[]"))) {
        fprintf(stderr, "--mode %s is a derivation and must name --derived-from; "
                        "an unattributed derived position cannot be told from an "
                        "observation afterwards\n", mode);
        return 2;
    }
    if (mode_is_source_claim(mode) && derived && strcmp(derived, "[]")) {
        fprintf(stderr, "--mode %s is a source claim and cannot be derived from "
                        "other facts\n", mode);
        return 2;
    }

    if (sqlite3_prepare_v2(db,
        "INSERT INTO spatial_fact(observation_id,subject,geometry_kind,lon,lat,geometry,"
        "crs,observed_at,effective_at,valid_until,recorded_at,observation_mode,"
        "source_occurrence,derived_from,accuracy_h,provenance) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, kind ? kind : "point", -1, SQLITE_TRANSIENT);
    if (lon) sqlite3_bind_double(st, 4, atof(lon)); else sqlite3_bind_null(st, 4);
    if (lat) sqlite3_bind_double(st, 5, atof(lat)); else sqlite3_bind_null(st, 5);
    sqlite3_bind_text(st, 6, geometry, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 7, crs, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 8, observed, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 9, effective, -1, SQLITE_TRANSIENT);
    if (until) sqlite3_bind_text(st, 10, until, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(st, 10);
    sqlite3_bind_text(st, 11, recorded, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 12, mode, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 13, occurrence, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 14, derived ? derived : "[]", -1, SQLITE_TRANSIENT);
    {
        const char *acc = arg(argc, argv, "--accuracy-h");
        if (acc) sqlite3_bind_double(st, 15, atof(acc)); else sqlite3_bind_null(st, 15);
    }
    sqlite3_bind_text(st, 16, provenance, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_DONE) {
        fprintf(stderr, "observe failed: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(st); return 1;
    }
    sqlite3_finalize(st);
    printf("{\"appended\":%lld,\"observation\":", (long long)sqlite3_last_insert_rowid(db));
    json_text(id);
    printf(",\"mode\":"); json_text(mode);
    printf(",\"truth\":\"append_only_observation_not_authority\"}\n");
    return 0;
}

/* --------------------------------------------------------------- retract -- */

static int cmd_retract(sqlite3 *db, int argc, char **argv) {
    const char *fact = arg(argc, argv, "--fact");
    const char *reason = arg(argc, argv, "--reason");
    const char *recorded = arg(argc, argv, "--recorded-at");
    sqlite3_stmt *st = NULL;
    if (!fact || !reason) {
        fprintf(stderr, "retract requires --fact and --reason\n");
        return 2;
    }
    if (!require_instant("--recorded-at", recorded)) return 2;

    /* A retraction is itself a fact with a knowledge time. It does not remove
     * anything: a reconstruction as of a knowledge time before this instant
     * still sees what was retracted, which is what makes an earlier answer
     * reproducible instead of merely remembered. */
    if (sqlite3_prepare_v2(db,
        "INSERT INTO spatial_fact(observation_id,subject,geometry_kind,geometry,crs,"
        "observed_at,effective_at,recorded_at,observation_mode,source_occurrence,"
        "derived_from,retracts,retraction_reason,provenance) "
        "SELECT 'retraction:'||observation_id,subject,geometry_kind,geometry,crs,"
        "observed_at,effective_at,?,observation_mode,source_occurrence,'[]',fact_id,?,provenance "
        "FROM spatial_fact WHERE fact_id=? AND retracts IS NULL",
        -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(st, 1, recorded, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, reason, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(st, 3, atoll(fact));
    if (sqlite3_step(st) != SQLITE_DONE) {
        fprintf(stderr, "retract failed: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(st); return 1;
    }
    sqlite3_finalize(st);
    if (sqlite3_changes(db) == 0) {
        fprintf(stderr, "no such unretracted fact: %s\n", fact);
        return 1;
    }
    printf("{\"retracted_fact\":%s,\"recorded_at\":", fact);
    json_text(recorded);
    printf(",\"truth\":\"appended_retraction_earlier_knowledge_unchanged\"}\n");
    return 0;
}

/* ----------------------------------------------------------- reconstruct -- */

/* The bitemporal cut, written once and shared by every read below.
 *
 *   recorded_at <= :as_of        we had learned it
 *   not retracted by :as_of      and had not unlearned it
 *   effective_at <= :at          it had taken effect
 *   valid_until  >  :at          and had not expired
 *
 * The expiry clause is the one that is usually dropped, and dropping it is how
 * a stale position silently becomes a current one. It is kept separate below so
 * "we never knew" and "we knew and the claim has expired" stay different
 * answers. */
#define KNOWN_BY \
    "f.recorded_at <= :as_of AND f.retracts IS NULL AND NOT EXISTS(" \
    "  SELECT 1 FROM spatial_fact r WHERE r.retracts = f.fact_id AND r.recorded_at <= :as_of)"

#define EFFECTIVE_AT \
    "f.effective_at <= :at AND (f.valid_until IS NULL OR f.valid_until > :at)"

/* A derivation whose basis has been retracted is not quietly kept. It is
 * reported as orphaned, because the alternative is an interpolation that
 * outlives the fixes that justified it and looks exactly like a real one.
 *
 * Computed once as a set rather than asked per row. Asked per row it costs a
 * json_each setup for every subject in the answer, which measured at 3.4
 * seconds over twenty thousand subjects while the rest of the query took
 * seventy milliseconds -- the check was the query. Only a derivation can be
 * orphaned, and derivations are a small minority, so the set is small.
 *
 * MATERIALIZED is load-bearing rather than decorative. The set references the
 * knowledge frontier, and without the hint SQLite runs it as a co-routine and
 * recomputes it once per row of the answer, which measured slower than asking
 * per row in the first place. Declared once, it is computed once. */
/* Built once per invocation into a temp table rather than expressed as a CTE.
 * As a CTE -- with or without MATERIALIZED -- the planner re-ran it against the
 * knowledge frontier while walking the answer: measured at 8.5 seconds over
 * twenty thousand subjects for a set that is computed in nineteen milliseconds
 * and, in that store, is empty. A temp table removes the question. */
static int build_orphan_set(sqlite3 *db, const char *as_of) {
    sqlite3_stmt *st = NULL;
    int ok;
    if (!exec_sql(db, "DROP TABLE IF EXISTS temp.orphan;"
                      "CREATE TEMP TABLE orphan(fact_id INTEGER PRIMARY KEY);")) return 0;
    if (sqlite3_prepare_v2(db,
        "INSERT OR IGNORE INTO temp.orphan(fact_id) "
        "SELECT DISTINCT d0.fact_id FROM spatial_fact d0, json_each(d0.derived_from) d "
        "JOIN spatial_fact b ON b.observation_id = d.value AND b.retracts IS NULL "
        "WHERE d0.derived_from <> '[]' "
        "  AND EXISTS(SELECT 1 FROM spatial_fact r WHERE r.retracts = b.fact_id "
        "               AND r.recorded_at <= ?1)", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, as_of, -1, SQLITE_TRANSIENT);
    ok = sqlite3_step(st) == SQLITE_DONE;
    sqlite3_finalize(st);
    return ok;
}

#define ORPHAN_JOIN "LEFT JOIN temp.orphan o ON o.fact_id = f.fact_id"
#define ORPHANED_COL "(o.fact_id IS NOT NULL)"

static void bind_frontier(sqlite3_stmt *st, const char *at, const char *as_of) {
    int i_at = sqlite3_bind_parameter_index(st, ":at");
    int i_as = sqlite3_bind_parameter_index(st, ":as_of");
    if (i_at) sqlite3_bind_text(st, i_at, at, -1, SQLITE_TRANSIENT);
    if (i_as) sqlite3_bind_text(st, i_as, as_of, -1, SQLITE_TRANSIENT);
}

static void emit_position(sqlite3_stmt *st, int orphaned) {
    printf("{\"subject\":"); json_text((const char *)sqlite3_column_text(st, 0));
    printf(",\"observation\":"); json_text((const char *)sqlite3_column_text(st, 1));
    printf(",\"geometry\":"); json_text((const char *)sqlite3_column_text(st, 2));
    printf(",\"crs\":"); json_text((const char *)sqlite3_column_text(st, 3));
    printf(",\"mode\":"); json_text((const char *)sqlite3_column_text(st, 4));
    printf(",\"observed_at\":"); json_text((const char *)sqlite3_column_text(st, 5));
    printf(",\"effective_at\":"); json_text((const char *)sqlite3_column_text(st, 6));
    if (sqlite3_column_type(st, 7) != SQLITE_NULL) {
        printf(",\"valid_until\":"); json_text((const char *)sqlite3_column_text(st, 7));
    }
    printf(",\"fact\":%lld", (long long)sqlite3_column_int64(st, 8));
    if (orphaned) printf(",\"derivation\":\"orphaned_basis_retracted\"");
    printf("}");
}

static const char *SELECT_COLS =
    "f.subject,f.observation_id,f.geometry,f.crs,f.observation_mode,"
    "f.observed_at,f.effective_at,f.valid_until,f.fact_id,f.lon,f.lat,"
    "f.geometry_kind,f.min_lon,f.min_lat,f.max_lon,f.max_lat,";
#define COL_ORPHANED 16


/* The frontier's one-position-per-subject set, built once into a temp table.
 *
 * Expressed as a CTE the planner re-ran the subject walk while probing the join
 * back, so the closure paid for the walk once per row of the answer rather than
 * once -- which is exactly where the superlinear scaling came from: four times
 * the subjects cost fifteen times the time while four times the bytes cost four.
 * MATERIALIZED did not fix it and made it worse once the rest of the query was
 * present. This is the second place in this file where a planner hint was not
 * enough and an explicit table was; the pattern is now deliberate rather than
 * incidental.
 *
 * `subject_source` is the SQL naming the subjects to consider: every subject in
 * the log for an open query, or the advertised candidates for a World.
 */
static int build_latest_set(sqlite3 *db, const char *subject_source,
                            const char *at, const char *as_of, int source_only) {
    sqlite3_stmt *st = NULL;
    char sql[4096];
    int ok;
    if (!exec_sql(db, "DROP TABLE IF EXISTS temp.frontier_latest;"
                      "CREATE TEMP TABLE frontier_latest(fact_id INTEGER PRIMARY KEY);"))
        return 0;
    snprintf(sql, sizeof(sql),
        "INSERT OR IGNORE INTO temp.frontier_latest(fact_id) "
        "SELECT fact_id FROM (%s) WHERE fact_id IS NOT NULL", subject_source);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "frontier: %s\n", sqlite3_errmsg(db));
        return 0;
    }
    bind_frontier(st, at, as_of);
    (void)source_only;
    ok = sqlite3_step(st) == SQLITE_DONE;
    if (!ok) fprintf(stderr, "frontier: %s\n", sqlite3_errmsg(db));
    sqlite3_finalize(st);
    return ok;
}

/* ---------------------------------------------------------- durable tier -- */

/* The cold tier. SQLite is where the log is written and queried; it is not
 * where a million facts should be kept forever.
 *
 * Measured on a million-fact store: 254 MB of SQLite becomes 3.3 MB of
 * zstd-compressed Parquet with every field intact. That is the compression law
 * as it applies here -- the same facts in a cheaper representation, chosen for
 * this purpose, and lossless.
 *
 * `thaw` exists because a cold tier you cannot restore is not a tier, it is a
 * deletion with extra steps. `verify` compares the two representations by
 * content rather than by size, so "exported" means the same facts and not
 * merely the same number of them.
 *
 * Zarr is deliberately not written. Zarr chunks N-dimensional arrays -- a
 * gridded field sampled over space and time -- and an observation log is not
 * that shape. This owner holds no raster and no continuous surface, so a Zarr
 * tier here would be a container with nothing to put in it. When a gridded
 * field arrives (a weather surface, an elevation model) it will want one, and
 * that is a different record than this one.
 *
 * Cannot infer: a durable copy is not a retention decision, an authority to
 * delete the hot store, or evidence that anything in it was admitted.
 */
static const char *duckdb_binary(void) {
    const char *configured = getenv("BONFYRE_DUCKDB");
    return (configured && *configured) ? configured : "/opt/homebrew/bin/duckdb";
}

/* Captures stdout when `out` is given so the caller can compare what came back
 * rather than reprint it. No shell: the SQL carries paths from the caller. */
static int run_duckdb(const char *sql, char *out, size_t cap) {
    const char *bin = duckdb_binary();
    char *const argv[] = { (char *)bin, (char *)"-c", (char *)sql, NULL };
    int fds[2] = { -1, -1 };
    pid_t pid;
    int status = 0;
    size_t used = 0;
    if (out && pipe(fds) != 0) return 0;
    pid = fork();
    if (pid < 0) { if (out) { close(fds[0]); close(fds[1]); } return 0; }
    if (pid == 0) {
        if (out) { close(fds[0]); if (dup2(fds[1], STDOUT_FILENO) < 0) _exit(127); close(fds[1]); }
        execv(bin, argv);
        _exit(127);
    }
    if (out) {
        close(fds[1]);
        for (;;) {
            ssize_t n = read(fds[0], out + used, cap - used - 1);
            if (n <= 0) break;
            used += (size_t)n;
            if (used + 1 >= cap) break;
        }
        out[used] = '\0';
        while (used && (out[used-1] == '\n' || out[used-1] == '\r')) out[--used] = '\0';
        close(fds[0]);
    }
    if (waitpid(pid, &status, 0) < 0) return 0;
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fprintf(stderr, "duckdb did not complete; set BONFYRE_DUCKDB if it is elsewhere\n");
        return 0;
    }
    return 1;
}

/* One expression, used on both representations, so equality is equality of
 * content. A row count matching proves only that nothing was dropped. */
#define FIDELITY_SELECT \
    "SELECT count(*)::VARCHAR || ' ' || COALESCE(sum(fact_id)::VARCHAR,'0') || ' ' || " \
    "  COALESCE(md5(string_agg(observation_id || '|' || subject || '|' || observed_at || '|' || " \
    "    COALESCE(valid_until,'') || '|' || observation_mode || '|' || " \
    "    COALESCE(lon::VARCHAR,'') || '|' || COALESCE(lat::VARCHAR,''), ',' ORDER BY fact_id)),'-') "

static int cmd_durable(sqlite3 *db, int argc, char **argv, const char *db_path) {
    const char *out = arg(argc, argv, "--out");
    const char *from = arg(argc, argv, "--from");
    int verify = has_flag(argc, argv, "--verify");
    int thaw = has_flag(argc, argv, "--thaw");
    char sql[4096];
    (void)db;

    if (thaw) {
        if (!from || !out) {
            fprintf(stderr, "durable --thaw requires --from <parquet> and --out <new sqlite path>\n");
            return 2;
        }
        snprintf(sql, sizeof(sql),
            "INSTALL sqlite; LOAD sqlite;"
            "ATTACH '%s' AS thawed (TYPE sqlite);"
            "CREATE TABLE thawed.spatial_fact AS SELECT * FROM read_parquet('%s');", out, from);
        if (!run_duckdb(sql, NULL, 0)) return 1;
        printf("{\"thawed_to\":"); json_text(out);
        printf(",\"from\":"); json_text(from);
        printf(",\"truth\":\"restored_representation_not_a_retention_decision\"}\n");
        return 0;
    }

    if (!out) { fprintf(stderr, "durable requires --out <directory or parquet path>\n"); return 2; }

    if (verify) {
        char hot_sql[PATH_MAX + 512], cold_sql[PATH_MAX + 512];
        char hot[256] = {0}, cold[256] = {0};
        snprintf(hot_sql, sizeof(hot_sql),
            "INSTALL sqlite; LOAD sqlite; ATTACH '%s' AS hot (TYPE sqlite, READ_ONLY);"
            "COPY (" FIDELITY_SELECT "AS fidelity FROM hot.spatial_fact) TO '/dev/stdout' (FORMAT csv, HEADER false);",
            db_path);
        snprintf(cold_sql, sizeof(cold_sql),
            "COPY (" FIDELITY_SELECT "AS fidelity FROM read_parquet('%s')) TO '/dev/stdout' (FORMAT csv, HEADER false);",
            out);
        if (!run_duckdb(hot_sql, hot, sizeof(hot))) return 1;
        if (!run_duckdb(cold_sql, cold, sizeof(cold))) return 1;
        /* Both digests are printed beside the verdict so the claim can be
         * checked rather than taken from a boolean this process computed about
         * itself. */
        printf("{\"identical\":%s,\"hot\":", strcmp(hot, cold) ? "false" : "true");
        json_text(hot);
        printf(",\"cold\":"); json_text(cold);
        printf(",\"compare\":\"row count, fact_id sum, and md5 over every field that "
               "carries meaning; equal means the same facts and not the same number of them\"}\n");
        return strcmp(hot, cold) ? 4 : 0;
    }

    snprintf(sql, sizeof(sql),
        "INSTALL sqlite; LOAD sqlite;"
        "ATTACH '%s' AS hot (TYPE sqlite, READ_ONLY);"
        "COPY (SELECT * FROM hot.spatial_fact ORDER BY fact_id) TO '%s' "
        "  (FORMAT parquet, COMPRESSION zstd);", db_path, out);
    if (!run_duckdb(sql, NULL, 0)) return 1;
    printf("{\"exported\":"); json_text(out);
    printf(",\"representation\":\"parquet+zstd\","
           "\"truth\":\"lossless_cold_representation_not_an_authority_to_delete_the_hot_store\"}\n");
    return 0;
}

/* ------------------------------------------------- maintained live state -- */

/* The live frontier, maintained incrementally instead of recomputed.
 *
 * A historical frontier has to be recomputed -- that is what makes it
 * historical, and no stored view can stand in for it. The live one does not: an
 * append-only log means the latest fact per subject only ever moves forward, so
 * the work is proportional to what arrived rather than to everything that ever
 * did.
 *
 * Two things stop that being a lie. A retraction does not move forward, so the
 * subject it touches is recomputed from the log rather than patched. And
 * "current" is still evaluated at read time against the instant asked about, so
 * a claim past its valid_until expires out of the maintained view exactly as it
 * expires out of a recomputation -- the view stores the latest known fact, not
 * the assertion that it holds now.
 *
 * Cannot infer: this is derived state and is never authority. It is also not a
 * second source of truth -- the witness proves a maintained read and a
 * recomputed read agree, and a disagreement is a bug in here and never a new
 * fact about the world.
 */
static int cmd_maintain(sqlite3 *db, int argc, char **argv) {
    sqlite3_stmt *st = NULL;
    long long from = 0, through = 0, advanced = 0, recomputed = 0;
    int reset = has_flag(argc, argv, "--rebuild");

    if (reset) {
        if (!exec_sql(db, "DELETE FROM current_spatial_state;"
                          "DELETE FROM maintenance_watermark;")) return 1;
    }
    if (sqlite3_prepare_v2(db, "SELECT last_fact_id FROM maintenance_watermark "
                               "WHERE name='current_spatial_state'", -1, &st, NULL) != SQLITE_OK) return 1;
    if (sqlite3_step(st) == SQLITE_ROW) from = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st); st = NULL;

    if (sqlite3_prepare_v2(db, "SELECT COALESCE(MAX(fact_id),0) FROM spatial_fact", -1, &st, NULL) != SQLITE_OK) return 1;
    if (sqlite3_step(st) == SQLITE_ROW) through = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st); st = NULL;

    if (through <= from) {
        printf("{\"maintained_through\":%lld,\"advanced\":0,\"recomputed_subjects\":0,"
               "\"truth\":\"derived_state_not_authority\"}\n", through);
        return 0;
    }
    if (!exec_sql(db, "BEGIN IMMEDIATE")) return 1;

    /* A retraction moves the answer backwards, so the subjects it touches are
     * recomputed from the log. Patching them forward would leave the view
     * asserting a fact the log no longer supports. */
    if (sqlite3_prepare_v2(db,
        "SELECT COUNT(*) FROM ("
        "  SELECT DISTINCT subject FROM spatial_fact"
        "  WHERE fact_id > ?1 AND fact_id <= ?2 AND retracts IS NOT NULL)", -1, &st, NULL) != SQLITE_OK)
        goto fail;
    sqlite3_bind_int64(st, 1, from); sqlite3_bind_int64(st, 2, through);
    if (sqlite3_step(st) == SQLITE_ROW) recomputed = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st); st = NULL;

    if (recomputed) {
        if (!exec_sql(db,
            "DELETE FROM current_spatial_state WHERE subject IN ("
            "  SELECT DISTINCT subject FROM spatial_fact WHERE retracts IS NOT NULL)")) goto fail;
        if (sqlite3_prepare_v2(db,
            "INSERT OR REPLACE INTO current_spatial_state(subject,fact_id,effective_at,maintained_through) "
            "SELECT f.subject, f.fact_id, f.effective_at, ?2 FROM spatial_fact f "
            "WHERE f.subject IN (SELECT DISTINCT subject FROM spatial_fact WHERE retracts IS NOT NULL) "
            "  AND f.retracts IS NULL AND f.fact_id <= ?2 "
            "  AND NOT EXISTS(SELECT 1 FROM spatial_fact r WHERE r.retracts = f.fact_id AND r.fact_id <= ?2) "
            "  AND f.fact_id = (SELECT g.fact_id FROM spatial_fact g "
            "                   WHERE g.subject = f.subject AND g.retracts IS NULL AND g.fact_id <= ?2 "
            "                     AND NOT EXISTS(SELECT 1 FROM spatial_fact r2 WHERE r2.retracts = g.fact_id AND r2.fact_id <= ?2) "
            "                   ORDER BY g.effective_at DESC, g.fact_id DESC LIMIT 1)", -1, &st, NULL) != SQLITE_OK)
            goto fail;
        sqlite3_bind_int64(st, 2, through);
        if (sqlite3_step(st) != SQLITE_DONE) goto fail;
        sqlite3_finalize(st); st = NULL;
    }

    /* Everything else only moves forward: a new fact replaces the stored one
     * when it takes effect later, and is ignored when it does not. */
    if (sqlite3_prepare_v2(db,
        "INSERT INTO current_spatial_state(subject,fact_id,effective_at,maintained_through) "
        "SELECT f.subject, f.fact_id, f.effective_at, ?2 FROM spatial_fact f "
        "WHERE f.fact_id > ?1 AND f.fact_id <= ?2 AND f.retracts IS NULL "
        "  AND f.fact_id = (SELECT g.fact_id FROM spatial_fact g "
        "                   WHERE g.subject = f.subject AND g.fact_id > ?1 AND g.fact_id <= ?2 "
        "                     AND g.retracts IS NULL "
        "                   ORDER BY g.effective_at DESC, g.fact_id DESC LIMIT 1) "
        "ON CONFLICT(subject) DO UPDATE SET "
        "  fact_id = excluded.fact_id, effective_at = excluded.effective_at, "
        "  maintained_through = excluded.maintained_through "
        "WHERE excluded.effective_at > current_spatial_state.effective_at "
        "   OR (excluded.effective_at = current_spatial_state.effective_at "
        "       AND excluded.fact_id > current_spatial_state.fact_id)", -1, &st, NULL) != SQLITE_OK)
        goto fail;
    sqlite3_bind_int64(st, 1, from); sqlite3_bind_int64(st, 2, through);
    if (sqlite3_step(st) != SQLITE_DONE) goto fail;
    advanced = sqlite3_changes(db);
    sqlite3_finalize(st); st = NULL;

    if (sqlite3_prepare_v2(db,
        "INSERT INTO maintenance_watermark(name,last_fact_id) "
        "VALUES('current_spatial_state',?1) "
        "ON CONFLICT(name) DO UPDATE SET last_fact_id=excluded.last_fact_id", -1, &st, NULL) != SQLITE_OK)
        goto fail;
    sqlite3_bind_int64(st, 1, through);
    if (sqlite3_step(st) != SQLITE_DONE) goto fail;
    sqlite3_finalize(st); st = NULL;

    if (!exec_sql(db, "COMMIT")) return 1;
    printf("{\"maintained_from\":%lld,\"maintained_through\":%lld,\"advanced\":%lld,"
           "\"recomputed_subjects\":%lld,\"truth\":\"derived_state_not_authority\"}\n",
           from, through, advanced, recomputed);
    return 0;
fail:
    if (st) sqlite3_finalize(st);
    fprintf(stderr, "maintain: %s\n", sqlite3_errmsg(db));
    exec_sql(db, "ROLLBACK");
    return 1;
}

static int cmd_reconstruct(sqlite3 *db, int argc, char **argv) {
    const char *subject = arg(argc, argv, "--subject");
    const char *at = arg(argc, argv, "--at");
    const char *as_of = arg(argc, argv, "--as-of");
    int source_only = has_flag(argc, argv, "--source-claims-only");
    sqlite3_stmt *st = NULL;
    char sql[2048];

    if (!subject) { fprintf(stderr, "reconstruct requires --subject\n"); return 2; }
    if (!require_instant("--at", at)) return 2;
    /* Knowledge time defaults to the valid instant: "what did we believe then,
     * at the time" rather than "what do we believe now about then". Defaulting
     * to now would quietly answer a different question than the one asked. */
    if (!as_of) as_of = at;
    if (!require_instant("--as-of", as_of)) return 2;
    if (!build_orphan_set(db, as_of)) return 1;

    snprintf(sql, sizeof(sql),
        "SELECT %s %s AS orphaned FROM spatial_fact f %s "
        "WHERE f.subject = :subject AND %s AND %s %s "
        "ORDER BY f.effective_at DESC, f.fact_id DESC LIMIT 1",
        SELECT_COLS, ORPHANED_COL, ORPHAN_JOIN, KNOWN_BY, EFFECTIVE_AT,
        source_only ? "AND f.observation_mode IN ('observed','reported')" : "");
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "reconstruct: %s\n", sqlite3_errmsg(db)); return 1;
    }
    bind_frontier(st, at, as_of);
    sqlite3_bind_text(st, sqlite3_bind_parameter_index(st, ":subject"), subject, -1, SQLITE_TRANSIENT);

    if (sqlite3_step(st) == SQLITE_ROW) {
        printf("{\"frontier\":{\"at\":"); json_text(at);
        printf(",\"as_of\":"); json_text(as_of);
        printf("},\"position\":");
        emit_position(st, sqlite3_column_int(st, COL_ORPHANED));
        printf(",\"truth\":\"reconstructed_from_append_only_observations\"}\n");
        sqlite3_finalize(st);
        return 0;
    }
    sqlite3_finalize(st); st = NULL;

    /* Nothing satisfied the window. Whether that is ignorance or expiry is a
     * materially different answer and the caller is told which. */
    snprintf(sql, sizeof(sql),
        "SELECT COUNT(*) FROM spatial_fact f WHERE f.subject = :subject AND %s "
        "AND f.effective_at <= :at %s",
        KNOWN_BY, source_only ? "AND f.observation_mode IN ('observed','reported')" : "");
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 1;
    bind_frontier(st, at, as_of);
    sqlite3_bind_text(st, sqlite3_bind_parameter_index(st, ":subject"), subject, -1, SQLITE_TRANSIENT);
    int had = (sqlite3_step(st) == SQLITE_ROW) ? sqlite3_column_int(st, 0) : 0;
    sqlite3_finalize(st);

    printf("{\"frontier\":{\"at\":"); json_text(at);
    printf(",\"as_of\":"); json_text(as_of);
    printf("},\"position\":null,\"state\":");
    json_text(had ? "expired" : "unknown");
    printf(",\"truth\":\"%s\"}\n",
           had ? "claim_expired_no_position_is_current"
               : "no_observation_known_at_this_frontier");
    return 0;
}

/* --------------------------------------------- exact closure on survivors -- */

struct Hit { double distance; char json[1024]; };

static int hit_by_distance(const void *a, const void *b) {
    double x = ((const struct Hit *)a)->distance, y = ((const struct Hit *)b)->distance;
    return x < y ? -1 : x > y ? 1 : 0;
}

/* near / nearest / bbox run at a reconstruction frontier rather than over
 * current state, so "what was within 500 m of here at 22:00, as we understood
 * it at 22:05" is a single question. Each subject contributes at most its one
 * position at that frontier; a subject with ten fixes is one thing in a place,
 * not ten. */
static int spatial_query(sqlite3 *db, int argc, char **argv, const char *shape) {
    const char *at = arg(argc, argv, "--at");
    const char *as_of = arg(argc, argv, "--as-of");
    int source_only = has_flag(argc, argv, "--source-claims-only");
    int live = has_flag(argc, argv, "--live");
    sqlite3_stmt *st = NULL;
    char sql[3072];
    double clon = 0, clat = 0, radius = 0;
    double min_lon = 0, min_lat = 0, max_lon = 0, max_lat = 0;
    double corridor = 0, heading = 0, fov = 0, range_m = 0;
    BfShape route;
    int have_route = 0;
    int limit = 0, emitted = 0, considered = 0;
    memset(&route, 0, sizeof(route));

    {
        static char now[24];
        time_t t = time(NULL);
        struct tm utc;
        if (live && !at) {
            gmtime_r(&t, &utc);
            strftime(now, sizeof(now), "%Y-%m-%dT%H:%M:%SZ", &utc);
            at = now;
        }
    }
    if (!require_instant("--at", at)) return 2;
    if (!as_of) as_of = at;
    if (!require_instant("--as-of", as_of)) return 2;
    if (!build_orphan_set(db, as_of)) return 1;

    if (!strcmp(shape, "along-route")) {
        /* The route is supplied as a GeoJSON geometry rather than a list of
         * numbers, so the same shape a source publishes can be handed straight
         * back as a question. */
        const char *path = arg(argc, argv, "--route-geojson");
        const char *width = arg(argc, argv, "--corridor-m");
        char *doc = NULL; long size = 0; FILE *fh = NULL;
        char type[32] = {0};
        if (!path || !width) {
            fprintf(stderr, "along-route requires --route-geojson and --corridor-m\n");
            return 2;
        }
        corridor = atof(width);
        if (!(fh = fopen(path, "rb"))) { fprintf(stderr, "cannot read %s\n", path); return 1; }
        fseek(fh, 0, SEEK_END); size = ftell(fh); fseek(fh, 0, SEEK_SET);
        if (size < 0 || !(doc = malloc((size_t)size + 1))) { fclose(fh); return 1; }
        if (fread(doc, 1, (size_t)size, fh) != (size_t)size) { free(doc); fclose(fh); return 1; }
        doc[size] = '\0'; fclose(fh);
        {   /* The declared type decides how deep the coordinates sit. */
            const char *t = strstr(doc, "\"type\"");
            const char *q = t ? strchr(t + 6, '"') : NULL;
            const char *e = q ? strchr(q + 1, '"') : NULL;
            if (e && (size_t)(e - q - 1) < sizeof(type)) {
                memcpy(type, q + 1, (size_t)(e - q - 1));
                type[e - q - 1] = '\0';
            }
        }
        if (!shape_parse(doc, type, &route)) {
            fprintf(stderr, "the route carried no coordinates this owner can read\n");
            free(doc); return 2;
        }
        free(doc);
        have_route = 1;
    } else if (!strcmp(shape, "visible-from")) {
        const char *a = arg(argc, argv, "--lon"), *b = arg(argc, argv, "--lat");
        const char *h = arg(argc, argv, "--heading"), *f = arg(argc, argv, "--fov");
        const char *r = arg(argc, argv, "--range-m");
        if (!a || !b || !h || !f || !r) {
            fprintf(stderr, "visible-from requires --lon --lat --heading --fov --range-m\n");
            return 2;
        }
        clon = atof(a); clat = atof(b);
        heading = atof(h); fov = atof(f); range_m = atof(r);
    } else if (!strcmp(shape, "bbox")) {
        const char *a = arg(argc, argv, "--min-lon"), *b = arg(argc, argv, "--min-lat");
        const char *c = arg(argc, argv, "--max-lon"), *d = arg(argc, argv, "--max-lat");
        if (!a || !b || !c || !d) {
            fprintf(stderr, "bbox requires --min-lon --min-lat --max-lon --max-lat\n");
            return 2;
        }
        min_lon = atof(a); min_lat = atof(b); max_lon = atof(c); max_lat = atof(d);
    } else {
        const char *a = arg(argc, argv, "--lon"), *b = arg(argc, argv, "--lat");
        if (!a || !b) { fprintf(stderr, "%s requires --lon and --lat\n", shape); return 2; }
        clon = atof(a); clat = atof(b);
        if (!strcmp(shape, "contains")) { /* no radius, no count: it is or it is not */ }
        else if (!strcmp(shape, "near")) {
            const char *r = arg(argc, argv, "--radius-m");
            if (!r) { fprintf(stderr, "near requires --radius-m\n"); return 2; }
            radius = atof(r);
        } else {
            const char *n = arg(argc, argv, "--n");
            limit = n ? atoi(n) : 1;
            if (limit < 1) limit = 1;
        }
    }

    /* One position per subject at the frontier: the latest effective fact that
     * was known and had not expired.
     *
     * Written as a walk over distinct subjects rather than a window function.
     * A window partitioned by subject has to sort every fact the frontier
     * admits, which at a late instant is most of the log; walking the subject
     * index and seeking the latest fact for each one costs a seek per subject
     * instead. The difference is the difference between a query that scales
     * with the history and one that scales with the population. */
    /* --live reads the maintained set instead of walking the log. It is refused
     * for anything but the newest knowledge, because a maintained view knows
     * only what has been maintained: served for a past `--as-of` it would
     * answer with facts that had not yet arrived then. The historical frontier
     * recomputes, and that is not a limitation to work around. */
    if (live) {
        sqlite3_stmt *check = NULL;
        long long stale = -1;
        if (sqlite3_prepare_v2(db,
            "SELECT (SELECT COALESCE(MAX(fact_id),0) FROM spatial_fact) - "
            "       COALESCE((SELECT last_fact_id FROM maintenance_watermark "
            "                 WHERE name='current_spatial_state'), -1)", -1, &check, NULL) != SQLITE_OK)
            return 1;
        if (sqlite3_step(check) == SQLITE_ROW) stale = sqlite3_column_int64(check, 0);
        sqlite3_finalize(check);
        if (stale != 0) {
            fprintf(stderr, "--live needs the maintained state to be current; %lld facts "
                            "have not been maintained. Run `maintain` first, or ask for an "
                            "instant and have it recomputed.\n", stale < 0 ? 0 : stale);
            return 1;
        }
        if (!exec_sql(db, "DROP TABLE IF EXISTS temp.frontier_latest;"
                          "CREATE TEMP TABLE frontier_latest(fact_id INTEGER PRIMARY KEY);"
                          "INSERT OR IGNORE INTO temp.frontier_latest(fact_id) "
                          "SELECT fact_id FROM current_spatial_state;")) return 1;
    } else {
        char subject_source[2048];
        snprintf(subject_source, sizeof(subject_source),
            "WITH RECURSIVE subj(s) AS ("
            "  SELECT MIN(subject) FROM spatial_fact"
            "  UNION ALL"
            "  SELECT (SELECT MIN(subject) FROM spatial_fact WHERE subject > s)"
            "  FROM subj WHERE s IS NOT NULL)"
            " SELECT (SELECT f.fact_id FROM spatial_fact f"
            "         WHERE f.subject = subj.s AND %s AND %s %s"
            "         ORDER BY f.effective_at DESC, f.fact_id DESC LIMIT 1) AS fact_id"
            " FROM subj WHERE subj.s IS NOT NULL",
            KNOWN_BY, EFFECTIVE_AT,
            source_only ? "AND f.observation_mode IN ('observed','reported')" : "");
        if (!build_latest_set(db, subject_source, at, as_of, source_only)) return 1;
    }
    /* Whether the set came from the log or from the maintained view, "current"
     * is decided here against the instant asked about. A stored latest fact is
     * not a stored assertion that it still holds. */
    snprintf(sql, sizeof(sql),
        "SELECT %s %s AS orphaned FROM spatial_fact f"
        " JOIN temp.frontier_latest l ON l.fact_id = f.fact_id %s"
        " WHERE (f.lon IS NOT NULL OR f.min_lon IS NOT NULL)"
        "   AND f.effective_at <= :at AND (f.valid_until IS NULL OR f.valid_until > :at)"
        "   %s",
        SELECT_COLS, ORPHANED_COL, ORPHAN_JOIN,
        source_only ? "AND f.observation_mode IN ('observed','reported')" : "");
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "%s: %s\n", shape, sqlite3_errmsg(db)); return 1;
    }
    bind_frontier(st, at, as_of);

    struct Hit *hits = NULL;
    size_t cap = 0, n = 0;

    while (sqlite3_step(st) == SQLITE_ROW) {
        int is_point = sqlite3_column_type(st, 9) != SQLITE_NULL;
        double lon = sqlite3_column_double(st, 9), lat = sqlite3_column_double(st, 10);
        double box[4] = { sqlite3_column_double(st, 12), sqlite3_column_double(st, 13),
                          sqlite3_column_double(st, 14), sqlite3_column_double(st, 15) };
        const char *gkind = (const char *)sqlite3_column_text(st, 11);
        const char *gjson = (const char *)sqlite3_column_text(st, 2);
        double distance = 0;
        int matched = 1;
        considered++;
        if (!is_point && sqlite3_column_type(st, 12) == SQLITE_NULL) continue;
        if (!strcmp(shape, "along-route")) {
            /* Distance to the route itself, so a corridor along a rail line or a
             * delivery round means the corridor and not a circle round its
             * midpoint. */
            BfShape parsed;
            double slack = corridor / 111000.0 + 1e-6;
            double rb[4]; shape_bbox(&route, &rb[0], &rb[1], &rb[2], &rb[3]);
            {
                double a0 = is_point ? lon : box[0], a1 = is_point ? lat : box[1];
                double a2 = is_point ? lon : box[2], a3 = is_point ? lat : box[3];
                if (a2 < rb[0] - slack || a0 > rb[2] + slack ||
                    a3 < rb[1] - slack || a1 > rb[3] + slack) continue;
            }
            if (is_point) {
                distance = shape_distance_m(&route, lat, lon);
            } else {
                if (!shape_parse(gjson, gkind, &parsed)) continue;
                distance = shape_to_shape_m(&parsed, &route);
                shape_free(&parsed);
            }
            if (distance > corridor) continue;
        } else if (!strcmp(shape, "visible-from")) {
            /* Within the declared reach. Not a line-of-sight answer: no
             * elevation model is held here, so nothing occludes. */
            if (is_point) {
                if (!within_frustum(lat, lon, clat, clon, heading, fov, range_m, &distance)) continue;
            } else {
                BfShape parsed;
                int any = 0;
                double closest = 1e18;
                if (!shape_parse(gjson, gkind, &parsed)) continue;
                for (size_t v = 0; v < parsed.n && !any; v++) {
                    double d;
                    if (within_frustum(parsed.v[v].lat, parsed.v[v].lon, clat, clon,
                                       heading, fov, range_m, &d)) { any = 1; closest = d; }
                }
                shape_free(&parsed);
                if (!any) continue;
                distance = closest;
            }
        } else if (!strcmp(shape, "bbox")) {
            /* A box against a box: they overlap or they do not. A point is the
             * degenerate case of its own box. */
            double a0 = is_point ? lon : box[0], a1 = is_point ? lat : box[1];
            double a2 = is_point ? lon : box[2], a3 = is_point ? lat : box[3];
            if (a2 < min_lon || a0 > max_lon || a3 < min_lat || a1 > max_lat) continue;
        } else if (!strcmp(shape, "contains")) {
            /* Only an area can contain a point, and a point that merely sits in
             * the same bounding box is not inside anything. */
            BfShape parsed;
            if (is_point) continue;
            if (clon < box[0] || clon > box[2] || clat < box[1] || clat > box[3]) continue;
            if (!shape_parse(gjson, gkind, &parsed)) continue;
            matched = shape_contains(&parsed, clon, clat);
            if (matched) distance = 0;
            shape_free(&parsed);
            if (!matched) continue;
        } else {
            if (is_point) distance = haversine_m(clat, clon, lat, lon);
            else {
                /* The box rejects cheaply; the shape answers exactly. Measuring
                 * to a centroid would be wrong by the size of the thing. */
                BfShape parsed;
                if (!strcmp(shape, "near")) {
                    double slack = radius / 111000.0 + 1e-6;
                    if (clon < box[0] - slack || clon > box[2] + slack ||
                        clat < box[1] - slack || clat > box[3] + slack) continue;
                }
                if (!shape_parse(gjson, gkind, &parsed)) continue;
                distance = shape_distance_m(&parsed, clat, clon);
                shape_free(&parsed);
            }
            if (!strcmp(shape, "near") && distance > radius) continue;
        }
        if (n == cap) {
            size_t grown = cap ? cap * 2 : 32;
            struct Hit *bigger = realloc(hits, grown * sizeof(*hits));
            if (!bigger) { free(hits); sqlite3_finalize(st); return 1; }
            hits = bigger; cap = grown;
        }
        hits[n].distance = distance;
        {   /* Render into the hit so ordering can happen before output. */
            char *buf = hits[n].json; size_t len = sizeof(hits[n].json);
            snprintf(buf, len, "{\"subject\":\"%s\",\"observation\":\"%s\",\"mode\":\"%s\","
                     "\"geometry_kind\":\"%s\",\"effective_at\":\"%s\",\"fact\":%lld,"
                     "\"distance_m\":%.1f%s%s}",
                     sqlite3_column_text(st, 0), sqlite3_column_text(st, 1),
                     sqlite3_column_text(st, 4), gkind ? gkind : "",
                     sqlite3_column_text(st, 6),
                     (long long)sqlite3_column_int64(st, 8), distance,
                     is_point ? "" : ",\"measured_against\":\"the_shape_itself_not_a_centroid\"",
                     sqlite3_column_int(st, COL_ORPHANED) ? ",\"derivation\":\"orphaned_basis_retracted\"" : "");
        }
        n++;
    }
    sqlite3_finalize(st);

    if (strcmp(shape, "bbox") && strcmp(shape, "contains")) {
        /* qsort rather than an insertion sort. `nearest` over a whole population
         * sorts every survivor, so an O(n^2) sort made the closure quadratic in
         * the number of subjects: four times the subjects cost sixteen times the
         * time, which is exactly the superlinear scaling that looked like a
         * database problem and was not one. */
        qsort(hits, n, sizeof(*hits), hit_by_distance);
    }
    if (!strcmp(shape, "nearest") && (size_t)limit < n) n = (size_t)limit;

    printf("{\"frontier\":{\"at\":"); json_text(at);
    printf(",\"as_of\":"); json_text(as_of);
    printf("},\"shape\":"); json_text(shape);
    printf(",\"considered\":%d,\"selected\":[", considered);
    for (size_t i = 0; i < n; i++) { if (emitted++) putchar(','); fputs(hits[i].json, stdout); }
    printf("],\"truth\":\"exact_spatial_closure_not_authority_or_disclosure\"");
    if (!strcmp(shape, "visible-from"))
        printf(",\"cannot_infer\":\"within_declared_reach_is_not_line_of_sight_no_terrain_occludes\"");
    printf("}\n");
    if (have_route) shape_free(&route);
    free(hits);
    return 0;
}

/* --------------------------------------------- owner-transition evidence -- */

/* An owner handoff that leaves no occurrence-keyed evidence is a handoff nobody
 * can measure.
 *
 * The participation report keys every realized fact by ExternalEstate
 * occurrence, and it was reporting "no occurrence-keyed owner evidence
 * supplied" for a chain that was in fact running end to end. Two things caused
 * that: the coordinate was being synthesized instead of carried, and no owner
 * wrote down that it had taken the handoff. This writes the second.
 *
 * Cannot infer: a transition receipt says this owner accepted a handoff for
 * this occurrence at this instant. It is not a claim that the interpretation is
 * correct, that the source is admitted, or that anything downstream is
 * authorized. `interpreted` is the honest rung -- intake turned a source
 * representation into typed facts; `profile_bound` would need a DomainProfile
 * and none is bound here yet. */
static void emit_owner_transition(const char *path, const char *occurrence,
                                  const char *digest, const char *profile,
                                  const char *observed_at, long long facts) {
    FILE *fh;
    if (!path || !*path || !occurrence || !*occurrence) return;
    if (!(fh = fopen(path, "a"))) {
        fprintf(stderr, "could not append owner-transition evidence to %s\n", path);
        return;
    }
    fprintf(fh,
        "{\"schema\":\"bonfyre-owner-transition.v1\""
        ",\"occurrence_id\":\"%s\""
        ",\"axis\":\"semantic\""
        ",\"owner\":\"SpatialObservation\""
        ",\"state\":\"interpreted\""
        ",\"from_owner\":\"ForeignTwin\""
        ",\"to_owner\":\"SpatialObservation\""
        ",\"receipt\":\"%s\""
        ",\"profile\":\"%s\""
        ",\"observed_at\":\"%s\""
        ",\"facts_written\":%lld"
        ",\"truth\":\"owner_accepted_a_handoff_not_a_claim_that_the_interpretation_is_correct\"}\n",
        occurrence, digest ? digest : "", profile ? profile : "",
        observed_at ? observed_at : "", facts);
    fclose(fh);
}

/* ------------------------------------------------- identifier lineage ----- */

/* A source may change the identifier it prefers while still publishing the ones
 * it used before. USGS states this directly: the GeoJSON `id` is the preferred
 * identifier and can change, and `ids` carries the associated network
 * identifiers. Deriving a subject from the preferred id alone therefore splits
 * one earthquake into two subjects the moment the preference moves, and nothing
 * afterwards can tell they were ever one thing.
 *
 * So the declared identifiers are resolved against what has been seen before,
 * and the subject follows the lineage rather than the latest label.
 *
 * Cannot infer: this records that a source declared these identifiers together
 * at an observed time. It is not an assertion that two events are the same
 * physical event -- a source can be wrong about that, and when it is, the
 * record of what it said remains the evidence. */
static int alias_lookup(sqlite3 *db, const char *alias, char *subject, size_t cap) {
    sqlite3_stmt *st = NULL;
    int found = 0;
    if (sqlite3_prepare_v2(db, "SELECT subject FROM subject_alias WHERE alias=?1",
                           -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, alias, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *v = (const char *)sqlite3_column_text(st, 0);
        if (v) { snprintf(subject, cap, "%s", v); found = 1; }
    }
    sqlite3_finalize(st);
    return found;
}

static void alias_record(sqlite3 *db, const char *alias, const char *subject,
                         const char *seen, const char *declared_by) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db,
        "INSERT INTO subject_alias(alias,subject,first_seen,last_seen,declared_by) "
        "VALUES(?1,?2,?3,?3,?4) "
        "ON CONFLICT(alias) DO UPDATE SET last_seen=excluded.last_seen",
        -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, alias, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, seen, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 4, declared_by, -1, SQLITE_TRANSIENT);
    sqlite3_step(st);
    sqlite3_finalize(st);
}

/* Splits a delimited identifier list, resolves the subject through whatever was
 * already known, then records every identifier against the subject that won. */
static void lineage_resolve(sqlite3 *db, const char *raw_list, const char *separator,
                            const char *kind, const char *seen, const char *declared_by,
                            char *subject, size_t cap, int *rejoined) {
    char buffer[2048];
    char qualified[512];
    const char *sep = (separator && *separator) ? separator : ",";
    *rejoined = 0;
    if (!raw_list || !*raw_list) return;
    snprintf(buffer, sizeof(buffer), "%s", raw_list);
    for (char *token = strtok(buffer, sep); token; token = strtok(NULL, sep)) {
        while (*token == ' ') token++;
        if (!*token) continue;
        snprintf(qualified, sizeof(qualified), "subject:%s:%s", kind, token);
        if (!*rejoined && alias_lookup(db, qualified, subject, cap)) *rejoined = 1;
    }
    snprintf(buffer, sizeof(buffer), "%s", raw_list);
    for (char *token = strtok(buffer, sep); token; token = strtok(NULL, sep)) {
        while (*token == ' ') token++;
        if (!*token) continue;
        snprintf(qualified, sizeof(qualified), "subject:%s:%s", kind, token);
        alias_record(db, qualified, subject, seen, declared_by);
    }
}

/* ---------------------------------------------------------------- intake -- */

/* Intake turns a receipted foreign observation into spatial facts.
 *
 * Fetching is not this owner's job -- ForeignTwin observes a source and produces
 * a receipt, and what arrives here is that document plus the occurrence and
 * digest that identify it. Ingest is the mapping step, and the mapping is
 * declared rather than guessed: which property carries the subject, which
 * carries the source's own timestamp, and how that timestamp is shaped.
 *
 * The cheat this refuses is the common one. When a feature has no usable source
 * timestamp it is easy to substitute the moment of ingest, and the result looks
 * identical to a real observation forever afterwards: the estate ends up
 * believing a source said "now" when the source said nothing. Such a feature is
 * refused and counted instead. Same for a geometry this owner cannot place
 * exactly, and same for any attempt to ingest a derivation -- intake produces
 * source claims only, because a derivation has to name the fixes it came from
 * and a source did not compute one.
 *
 * Ingest is idempotent by source digest. Observing the same document twice is a
 * repeated observation of one thing, not two observations. */
static int cmd_ingest(sqlite3 *db, int argc, char **argv) {
    const char *path = arg(argc, argv, "--geojson");
    const char *profile = arg(argc, argv, "--profile");
    const char *occurrence = arg(argc, argv, "--source-occurrence");
    const char *digest = arg(argc, argv, "--source-digest");
    const char *recorded = arg(argc, argv, "--recorded-at");
    const char *provenance = arg(argc, argv, "--provenance");
    const char *subject_prop = arg(argc, argv, "--subject-property");
    const char *subject_kind = arg(argc, argv, "--subject-kind");
    const char *time_prop = arg(argc, argv, "--time-property");
    const char *time_format = arg(argc, argv, "--time-format");
    const char *mode = arg(argc, argv, "--mode");
    const char *valid_for = arg(argc, argv, "--valid-for-seconds");
    const char *alias_prop = arg(argc, argv, "--alias-property");
    const char *evidence_out = arg(argc, argv, "--evidence-out");
    const char *alias_sep = arg(argc, argv, "--alias-separator");
    long long rejoined_subjects = 0;
    sqlite3_stmt *st = NULL;
    char sql[4096];
    long long ingested = 0, no_time = 0, bad_geometry = 0, already = 0, no_subject = 0, from_future = 0;
    FILE *fh = NULL;
    char *body = NULL;
    long size = 0;

    if (!path || !profile || !occurrence || !digest || !provenance ||
        !subject_prop || !subject_kind || !time_prop || !time_format) {
        fprintf(stderr, "ingest requires --geojson --profile --source-occurrence "
                        "--source-digest --provenance --subject-property "
                        "--subject-kind --time-property --time-format\n");
        return 2;
    }
    if (!require_instant("--recorded-at", recorded)) return 2;
    if (!mode) mode = "observed";
    if (!mode_is_source_claim(mode)) {
        fprintf(stderr, "intake produces source claims only; --mode %s is a "
                        "derivation and must name the facts it came from\n", mode);
        return 2;
    }
    if (strcmp(time_format, "iso8601") && strcmp(time_format, "epoch_s") &&
        strcmp(time_format, "epoch_ms")) {
        fprintf(stderr, "--time-format must be iso8601, epoch_s or epoch_ms\n");
        return 2;
    }
    /* The whole digest, not a prefix of it. A shortened digest makes two
     * different documents share an observation coordinate, and the second one's
     * features are then silently counted as already present -- an observation
     * dropped without anything saying so. */

    if (!(fh = fopen(path, "rb"))) { fprintf(stderr, "cannot read %s\n", path); return 1; }
    fseek(fh, 0, SEEK_END); size = ftell(fh); fseek(fh, 0, SEEK_SET);
    if (size < 0 || !(body = malloc((size_t)size + 1))) { fclose(fh); return 1; }
    if (fread(body, 1, (size_t)size, fh) != (size_t)size) { free(body); fclose(fh); return 1; }
    body[size] = '\0';
    fclose(fh);

    /* A malformed document is a failed observation, never an empty one: an
     * empty ingest would look like a source that reported nothing. */
    if (sqlite3_prepare_v2(db,
        "SELECT json_valid(?1) AND json_type(?1,'$.features')='array'", -1, &st, NULL) != SQLITE_OK) {
        free(body); return 1;
    }
    sqlite3_bind_text(st, 1, body, -1, SQLITE_TRANSIENT);
    int ok = (sqlite3_step(st) == SQLITE_ROW) ? sqlite3_column_int(st, 0) : 0;
    sqlite3_finalize(st);
    if (!ok) {
        fprintf(stderr, "not a GeoJSON FeatureCollection; refusing to record an "
                        "empty observation of a document that failed to parse\n");
        free(body); return 1;
    }

    /* The source's own instant, normalised. A source that did not state one
     * yields NULL here and the feature is refused below rather than stamped. */
    /* GeoJSON puts a feature's canonical identifier at the feature root and
     * everything else under properties, so both have to be reachable. A leading
     * '@' names a feature-level key; anything else is a property. */
    const char *subject_path = (*subject_prop == '@')
        ? "json_extract(f.value,'$.\"' || :sprop || '\"')"
        : "json_extract(f.value,'$.properties.\"' || :sprop || '\"')";
    const char *time_path = (*time_prop == '@')
        ? "json_extract(f.value,'$.\"' || :tprop || '\"')"
        : "json_extract(f.value,'$.properties.\"' || :tprop || '\"')";
    /* The alias list is read from wherever the caller says it lives, on the
     * same feature-root-or-properties rule as everything else. */
    char alias_path[256];
    if (!alias_prop) snprintf(alias_path, sizeof(alias_path), "NULL");
    else if (*alias_prop == '@')
        snprintf(alias_path, sizeof(alias_path), "json_extract(f.value,'$.\"' || :aprop || '\"')");
    else
        snprintf(alias_path, sizeof(alias_path), "json_extract(f.value,'$.properties.\"' || :aprop || '\"')");
    if (alias_prop && *alias_prop == '@') alias_prop++;
    if (*subject_prop == '@') subject_prop++;
    if (*time_prop == '@') time_prop++;

    char observed_expr[512];
    if (!strcmp(time_format, "epoch_ms"))
        snprintf(observed_expr, sizeof(observed_expr),
                 "strftime('%%Y-%%m-%%dT%%H:%%M:%%SZ', CAST(%s AS REAL)/1000.0, 'unixepoch')", time_path);
    else if (!strcmp(time_format, "epoch_s"))
        snprintf(observed_expr, sizeof(observed_expr),
                 "strftime('%%Y-%%m-%%dT%%H:%%M:%%SZ', CAST(%s AS REAL), 'unixepoch')", time_path);
    else
        snprintf(observed_expr, sizeof(observed_expr), "%s", time_path);

    snprintf(sql, sizeof(sql),
        "WITH feature AS ("
        "  SELECT %s AS subject_key,"
        "         %s AS alias_list,"
        "         json_extract(f.value,'$.geometry.type') AS gtype,"
        "         json_extract(f.value,'$.geometry.coordinates[0]') AS lon,"
        "         json_extract(f.value,'$.geometry.coordinates[1]') AS lat,"
        "         json_extract(f.value,'$.geometry') AS geometry,"
        "         %s AS observed_at"
        "  FROM json_each(:doc,'$.features') f)"
        " SELECT subject_key, gtype, lon, lat, observed_at, geometry, alias_list FROM feature",
        subject_path, alias_path, observed_expr);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "ingest: %s\n", sqlite3_errmsg(db)); free(body); return 1;
    }
    sqlite3_bind_text(st, sqlite3_bind_parameter_index(st, ":doc"), body, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, sqlite3_bind_parameter_index(st, ":sprop"), subject_prop, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, sqlite3_bind_parameter_index(st, ":tprop"), time_prop, -1, SQLITE_TRANSIENT);
    if (alias_prop)
        sqlite3_bind_text(st, sqlite3_bind_parameter_index(st, ":aprop"), alias_prop, -1, SQLITE_TRANSIENT);

    sqlite3_stmt *ins = NULL;
    if (sqlite3_prepare_v2(db,
        "INSERT INTO spatial_fact(observation_id,subject,geometry_kind,lon,lat,geometry,"
        "crs,observed_at,effective_at,valid_until,recorded_at,observation_mode,"
        "source_occurrence,derived_from,provenance,min_lon,min_lat,max_lon,max_lat) "
        "SELECT ?1,?2,?12,?3,?4,?5,'EPSG:4326',?6,?6,?7,?8,?9,?10,'[]',?11,?13,?14,?15,?16 "
        "WHERE NOT EXISTS(SELECT 1 FROM spatial_fact WHERE observation_id=?1)",
        -1, &ins, NULL) != SQLITE_OK) {
        sqlite3_finalize(st); free(body); return 1;
    }

    exec_sql(db, "BEGIN");
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *key = (const char *)sqlite3_column_text(st, 0);
        const char *gtype = (const char *)sqlite3_column_text(st, 1);
        const char *observed = (const char *)sqlite3_column_text(st, 4);
        const char *geometry_json = (const char *)sqlite3_column_text(st, 5);
        char observation_id[512], subject[512], until[24];
        char kind[32];
        double lon = 0, lat = 0, min_lon = 0, min_lat = 0, max_lon = 0, max_lat = 0;
        int is_point;
        BfShape shape;

        if (!key || !*key) { no_subject++; continue; }
        if (!gtype || !geometry_vertex_depth(gtype)) { bad_geometry++; continue; }
        is_point = !strcmp(gtype, "Point");
        /* The refusal that matters. No source instant means no observation. */
        if (!observed || !instant_valid(observed)) { no_time++; continue; }
        /* An estate cannot have learned a position before it was observed. A
         * feature whose source instant is later than the moment of ingest means
         * either a clock is wrong or the ingest is being back-dated, and both
         * produce a log that reconstructs to impossible pasts. Refuse it and say
         * how many rather than storing it and discovering this at read time. */
        if (strcmp(observed, recorded) > 0) { from_future++; continue; }

        /* The box comes from the shape's own vertices; nothing is invented and
         * no representative point is chosen for an extent. */
        if (!shape_parse(geometry_json, gtype, &shape)) { bad_geometry++; continue; }
        shape_bbox(&shape, &min_lon, &min_lat, &max_lon, &max_lat);
        if (is_point) {
            lon = sqlite3_column_double(st, 2);
            lat = sqlite3_column_double(st, 3);
        }
        shape_free(&shape);
        {   /* geometry_kind mirrors the declared GeoJSON type, lowercased. */
            size_t k = 0;
            for (const char *g = gtype; *g && k + 1 < sizeof(kind); g++)
                kind[k++] = (char)tolower((unsigned char)*g);
            kind[k] = '\0';
        }
        snprintf(subject, sizeof(subject), "subject:%s:%s", subject_kind, key);
        if (alias_prop) {
            const char *aliases = (const char *)sqlite3_column_text(st, 6);
            int rejoined = 0;
            lineage_resolve(db, aliases, alias_sep, subject_kind, observed, occurrence,
                            subject, sizeof(subject), &rejoined);
            if (rejoined) rejoined_subjects++;
            /* The preferred identifier is recorded too, so a later feed that
             * only carries the new label still resolves to this subject. */
            alias_record(db, subject, subject, observed, occurrence);
            {
                char self[512];
                snprintf(self, sizeof(self), "subject:%s:%s", subject_kind, key);
                alias_record(db, self, subject, observed, occurrence);
            }
        }
        snprintf(observation_id, sizeof(observation_id), "%s:%s:%s", profile, digest, key);

        sqlite3_bind_text(ins, 1, observation_id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 2, subject, -1, SQLITE_TRANSIENT);
        /* An extent has no point, and pretending otherwise is what makes a
         * distance query answer about a centroid while reporting about a fire. */
        if (is_point) { sqlite3_bind_double(ins, 3, lon); sqlite3_bind_double(ins, 4, lat); }
        else { sqlite3_bind_null(ins, 3); sqlite3_bind_null(ins, 4); }
        sqlite3_bind_text(ins, 5, geometry_json, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 6, observed, -1, SQLITE_TRANSIENT);
        if (valid_for) {
            /* A source that says how long its report holds gets an expiry; one
             * that does not is left open rather than given an invented one. */
            sqlite3_stmt *calc = NULL;
            until[0] = '\0';
            if (sqlite3_prepare_v2(db, "SELECT strftime('%Y-%m-%dT%H:%M:%SZ', ?1, '+' || ?2 || ' seconds')",
                                   -1, &calc, NULL) == SQLITE_OK) {
                sqlite3_bind_text(calc, 1, observed, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(calc, 2, valid_for, -1, SQLITE_TRANSIENT);
                if (sqlite3_step(calc) == SQLITE_ROW)
                    snprintf(until, sizeof(until), "%s", (const char *)sqlite3_column_text(calc, 0));
                sqlite3_finalize(calc);
            }
            if (until[0]) sqlite3_bind_text(ins, 7, until, -1, SQLITE_TRANSIENT);
            else sqlite3_bind_null(ins, 7);
        } else sqlite3_bind_null(ins, 7);
        sqlite3_bind_text(ins, 8, recorded, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 9, mode, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 10, occurrence, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 11, provenance, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 12, kind, -1, SQLITE_TRANSIENT);
        sqlite3_bind_double(ins, 13, min_lon); sqlite3_bind_double(ins, 14, min_lat);
        sqlite3_bind_double(ins, 15, max_lon); sqlite3_bind_double(ins, 16, max_lat);
        if (sqlite3_step(ins) != SQLITE_DONE) {
            fprintf(stderr, "ingest insert: %s\n", sqlite3_errmsg(db));
            sqlite3_finalize(ins); sqlite3_finalize(st); exec_sql(db, "ROLLBACK");
            free(body); return 1;
        }
        if (sqlite3_changes(db) == 0) already++; else ingested++;
        sqlite3_reset(ins); sqlite3_clear_bindings(ins);
    }
    exec_sql(db, "COMMIT");
    sqlite3_finalize(ins);
    sqlite3_finalize(st);
    free(body);

    printf("{\"profile\":"); json_text(profile);
    printf(",\"source_occurrence\":"); json_text(occurrence);
    printf(",\"source_digest\":"); json_text(digest);
    printf(",\"ingested\":%lld,\"already_present\":%lld,"
           "\"refused_no_source_instant\":%lld,\"refused_geometry\":%lld,"
           "\"refused_no_subject\":%lld,\"refused_observed_after_recorded\":%lld,"
           "\"rejoined_by_identifier_lineage\":%lld",
           ingested, already, no_time, bad_geometry, no_subject, from_future, rejoined_subjects);
    printf(",\"truth\":\"source_claims_only_ingest_time_never_substituted_for_observation_time\"}\n");
    /* Written only when something was actually interpreted: an empty ingest is
     * not a handoff, and recording one would inflate a braid with nothing in
     * it. */
    if (ingested > 0)
        emit_owner_transition(evidence_out, occurrence, digest, profile, recorded, ingested);
    return 0;
}

/* --------------------------------------------------------- from a receipt -- */

/* Intake, driven by what ForeignTwin already observed rather than by hand.
 *
 * The occurrence and the digest were being supplied on the command line, which
 * meant nothing checked that they described the bytes actually being read. A
 * receipt carries both, and the bytes are on disk beside it, so the check is
 * available and therefore mandatory: the body is hashed and compared to
 * `content_sha256` before a single feature is read.
 *
 * Refusing on drift is the point, and it is the same rule the feed parser
 * applies. Bytes that no longer hash to their receipt are not the bytes that
 * were observed, and positions derived from them would carry a receipt that
 * does not describe them. Re-observe the source instead.
 *
 * Cannot infer: a verified receipt says these bytes are the ones observed at
 * that instant. It does not say the source was right, that the estate may
 * republish what it said, or that anything in it is admitted.
 */
static int cmd_from_receipt(sqlite3 *db, int argc, char **argv) {
    const char *receipt_path = arg(argc, argv, "--receipt");
    sqlite3 *mem = NULL;
    sqlite3_stmt *st = NULL;
    char *doc = NULL;
    long size = 0;
    FILE *fh = NULL;
    char body_path[PATH_MAX] = {0}, expected[96] = {0}, source_url[512] = {0};
    char observed_at[64] = {0}, actual[65] = {0}, twin_id[256] = {0};
    int rc = 1;

    if (!receipt_path) { fprintf(stderr, "from-receipt requires --receipt\n"); return 2; }
    if (!(fh = fopen(receipt_path, "rb"))) {
        fprintf(stderr, "no receipt at %s\n", receipt_path); return 1;
    }
    fseek(fh, 0, SEEK_END); size = ftell(fh); fseek(fh, 0, SEEK_SET);
    if (size < 0 || !(doc = malloc((size_t)size + 1))) { fclose(fh); return 1; }
    if (fread(doc, 1, (size_t)size, fh) != (size_t)size) { free(doc); fclose(fh); return 1; }
    doc[size] = '\0';
    fclose(fh);

    if (sqlite3_open(":memory:", &mem) != SQLITE_OK) { free(doc); return 1; }
    if (sqlite3_prepare_v2(mem,
        "SELECT json_extract(?1,'$.body_path'), json_extract(?1,'$.content_sha256'),"
        "       json_extract(?1,'$.source_url'), json_extract(?1,'$.observed_at'),"
        "       json_extract(?1,'$.twin_id')",
        -1, &st, NULL) != SQLITE_OK) { sqlite3_close(mem); free(doc); return 1; }
    sqlite3_bind_text(st, 1, doc, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *a = (const char *)sqlite3_column_text(st, 0);
        const char *b = (const char *)sqlite3_column_text(st, 1);
        const char *c = (const char *)sqlite3_column_text(st, 2);
        const char *d = (const char *)sqlite3_column_text(st, 3);
        if (a) snprintf(body_path, sizeof(body_path), "%s", a);
        if (b) snprintf(expected, sizeof(expected), "%s", b);
        if (c) snprintf(source_url, sizeof(source_url), "%s", c);
        if (d) snprintf(observed_at, sizeof(observed_at), "%s", d);
        {
            const char *e = (const char *)sqlite3_column_text(st, 4);
            if (e) snprintf(twin_id, sizeof(twin_id), "%s", e);
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(mem);
    free(doc);

    if (!body_path[0] || !expected[0]) {
        fprintf(stderr, "receipt names no body_path and content_sha256; it cannot "
                        "support a reading of anything\n");
        return 1;
    }
    if (bf_sha256_file(body_path, actual) != 0) {
        fprintf(stderr, "cannot read the receipted body at %s\n", body_path);
        return 1;
    }
    if (strcmp(actual, expected)) {
        printf("{\"error\":\"body_does_not_match_receipt\",\"receipt\":");
        json_text(receipt_path);
        printf(",\"expected_sha256\":"); json_text(expected);
        printf(",\"actual_sha256\":"); json_text(actual);
        printf(",\"detail\":\"Nothing was ingested. Re-observe the source; a body that "
               "has drifted from its receipt cannot support any position.\"}\n");
        return 3;
    }

    /* The receipt supplies what was being typed by hand, and supplies it as
     * observed rather than as asserted.
     *
     * The occurrence coordinate is the receipt's own `twin_id`. Synthesizing
     * one from the URL instead -- which is what this did -- broke the only
     * thing that makes a braid measurable: every owner downstream inherited a
     * coordinate that no ExternalEstate occurrence had ever used, so the
     * participation report found no occurrence-keyed evidence and measured the
     * whole chain at depth zero while it was in fact running. A coordinate that
     * nobody else uses is not an identity. */
    {
        char *forwarded[64];
        int n = 0, i;
        char occurrence[600];
        if (twin_id[0]) {
            snprintf(occurrence, sizeof(occurrence), "%s", twin_id);
        } else {
            /* Said out loud rather than papered over: without a twin_id this
             * observation cannot be joined to the occurrence it came from. */
            fprintf(stderr, "receipt names no twin_id; the resulting facts carry a "
                            "synthesized coordinate and will not join to any "
                            "ExternalEstate occurrence\n");
            snprintf(occurrence, sizeof(occurrence), "foreign:%s",
                     source_url[0] ? source_url : receipt_path);
        }
        forwarded[n++] = argv[0];
        forwarded[n++] = (char *)"ingest";
        forwarded[n++] = (char *)"--geojson";           forwarded[n++] = body_path;
        forwarded[n++] = (char *)"--source-digest";     forwarded[n++] = actual;
        forwarded[n++] = (char *)"--source-occurrence"; forwarded[n++] = occurrence;
        if (!arg(argc, argv, "--recorded-at") && observed_at[0]) {
            /* The estate learned of these bytes when it observed them. */
            forwarded[n++] = (char *)"--recorded-at";   forwarded[n++] = observed_at;
        }
        if (!arg(argc, argv, "--provenance")) {
            forwarded[n++] = (char *)"--provenance";
            forwarded[n++] = source_url[0] ? source_url : (char *)"foreign-twin-receipt";
        }
        /* Everything the caller declared about the mapping is carried through
         * untouched: a receipt says what was observed, not how to read it. */
        for (i = 2; i < argc && n < 60; i++) {
            if (!strcmp(argv[i], "--receipt")) { i++; continue; }
            forwarded[n++] = argv[i];
        }
        forwarded[n] = NULL;
        rc = cmd_ingest(db, n, forwarded);
    }
    return rc;
}

/* ------------------------------------------------ addressable frontier ---- */

/* A frontier is two instants, and an address is where they travel. The pair has
 * to mean the same thing on both sides of a link or the whole reconstruction is
 * a story: if the surface and the owner disagree about what `at` denotes, two
 * people opening the same URL see different pasts and neither of them is told.
 * So parsing and rendering live here, beside the reconstruction they govern,
 * and both go through the same strict instant check.
 *
 * What an address carries is deliberately only projection coordinates -- world,
 * frontier, extent, region, cell, an optional subject of interest. It never
 * carries a population. A recipient recompiles the closure under their own
 * actor and policy, so a link cannot hand anyone a subject, a position or a
 * layer they could not have asked for themselves. Serialising the result would
 * turn a shareable coordinate into a leak with a nice URL. */

struct Frontier {
    char at[24];
    char as_of[24];
    char world[128];
    char extent[32];
    char region[64];
    char cell[64];
    char subject[256];
};

static int url_decode(const char *in, char *out, size_t cap) {
    size_t j = 0;
    for (size_t i = 0; in[i]; i++) {
        if (j + 1 >= cap) return 0;
        if (in[i] == '%' && isxdigit((unsigned char)in[i+1]) && isxdigit((unsigned char)in[i+2])) {
            char hex[3] = { in[i+1], in[i+2], 0 };
            out[j++] = (char)strtol(hex, NULL, 16);
            i += 2;
        } else if (in[i] == '+') out[j++] = ' ';
        else out[j++] = in[i];
    }
    out[j] = '\0';
    return 1;
}

static void copy_field(char *dst, size_t cap, const char *src) {
    snprintf(dst, cap, "%s", src);
}

/* Refuses rather than repairs. A frontier that half-parsed is worse than one
 * that failed: the reader would believe they were looking at an instant they
 * never asked for. */
static int frontier_parse(const char *address, struct Frontier *f) {
    const char *q = strchr(address, '?');
    const char *path = address;
    char buf[2048];
    memset(f, 0, sizeof(*f));

    if (strncmp(path, "/w/", 3) != 0) {
        fprintf(stderr, "address must begin /w/<world>/spatial\n");
        return 0;
    }
    {
        const char *start = path + 3;
        const char *slash = strchr(start, '/');
        if (!slash || strncmp(slash, "/spatial", 8) != 0) {
            fprintf(stderr, "address must name the spatial projection of a world\n");
            return 0;
        }
        if ((size_t)(slash - start) >= sizeof(f->world)) return 0;
        memcpy(f->world, start, (size_t)(slash - start));
        f->world[slash - start] = '\0';
    }
    if (!f->world[0]) { fprintf(stderr, "address names no world\n"); return 0; }
    if (!q) { fprintf(stderr, "address carries no frontier; `at` is required\n"); return 0; }
    snprintf(buf, sizeof(buf), "%s", q + 1);

    for (char *pair = strtok(buf, "&"); pair; pair = strtok(NULL, "&")) {
        char *eq = strchr(pair, '=');
        char value[512];
        if (!eq) continue;
        *eq = '\0';
        if (!url_decode(eq + 1, value, sizeof(value))) return 0;
        if (!strcmp(pair, "at")) copy_field(f->at, sizeof(f->at), value);
        else if (!strcmp(pair, "as-of")) copy_field(f->as_of, sizeof(f->as_of), value);
        else if (!strcmp(pair, "extent")) copy_field(f->extent, sizeof(f->extent), value);
        else if (!strcmp(pair, "region")) copy_field(f->region, sizeof(f->region), value);
        else if (!strcmp(pair, "cell")) copy_field(f->cell, sizeof(f->cell), value);
        else if (!strcmp(pair, "subject")) copy_field(f->subject, sizeof(f->subject), value);
        else {
            /* An unknown coordinate is refused, not ignored. Ignoring it is how
             * a link that meant something specific silently becomes a link that
             * meant something broader. */
            fprintf(stderr, "unknown projection coordinate in address: %s\n", pair);
            return 0;
        }
    }
    if (!require_instant("at", f->at)) return 0;
    if (!f->as_of[0]) copy_field(f->as_of, sizeof(f->as_of), f->at);
    if (!require_instant("as-of", f->as_of)) return 0;
    if (f->as_of[0] && strcmp(f->as_of, f->at) < 0) {
        /* Knowledge earlier than the instant asked about is legitimate and
         * usually means "what did we believe before we knew"; it is allowed and
         * left alone here precisely because it is a real question. */
    }
    return 1;
}

static void frontier_render(const struct Frontier *f) {
    printf("/w/%s/spatial?at=%s&as-of=%s", f->world, f->at, f->as_of);
    if (f->extent[0]) printf("&extent=%s", f->extent);
    if (f->region[0]) printf("&region=%s", f->region);
    if (f->cell[0]) printf("&cell=%s", f->cell);
    if (f->subject[0]) printf("&subject=%s", f->subject);
}

static int cmd_address(int argc, char **argv) {
    const char *parse = arg(argc, argv, "--parse");
    struct Frontier f;
    if (parse) {
        if (!frontier_parse(parse, &f)) return 2;
        printf("{\"world\":"); json_text(f.world);
        printf(",\"frontier\":{\"at\":"); json_text(f.at);
        printf(",\"as_of\":"); json_text(f.as_of);
        printf("},\"projection\":{");
        printf("\"extent\":"); json_text(f.extent);
        printf(",\"region\":"); json_text(f.region);
        printf(",\"cell\":"); json_text(f.cell);
        printf(",\"subject\":"); json_text(f.subject);
        printf("},\"address\":");
        putchar('"'); frontier_render(&f); putchar('"');
        printf(",\"truth\":\"projection_coordinates_only_recipient_recompiles_the_closure\"}\n");
        return 0;
    }
    memset(&f, 0, sizeof(f));
    {
        const char *world = arg(argc, argv, "--world");
        const char *at = arg(argc, argv, "--at"), *as_of = arg(argc, argv, "--as-of");
        const char *extent = arg(argc, argv, "--extent"), *region = arg(argc, argv, "--region");
        const char *cell = arg(argc, argv, "--cell"), *subject = arg(argc, argv, "--subject");
        if (!world) { fprintf(stderr, "address requires --world or --parse\n"); return 2; }
        if (!require_instant("--at", at)) return 2;
        if (!as_of) as_of = at;
        if (!require_instant("--as-of", as_of)) return 2;
        copy_field(f.world, sizeof(f.world), world);
        copy_field(f.at, sizeof(f.at), at);
        copy_field(f.as_of, sizeof(f.as_of), as_of);
        if (extent) copy_field(f.extent, sizeof(f.extent), extent);
        if (region) copy_field(f.region, sizeof(f.region), region);
        if (cell) copy_field(f.cell, sizeof(f.cell), cell);
        if (subject) copy_field(f.subject, sizeof(f.subject), subject);
    }
    putchar('"'); frontier_render(&f); putchar('"'); putchar('\n');
    return 0;
}

/* ------------------------------------------- the world closure, composed -- */

/* Both stages, run in order, over one frontier.
 *
 * The plane says which subjects could possibly participate: it holds coarse
 * coordinates only -- region, cell, scale, mobility -- and it knows nothing
 * about where anything actually is. The owner then answers from its own
 * complete log, and this is the part worth being careful about: the surviving
 * advertisements are used to name candidate subjects and are never used as the
 * set of facts about them. Reconstructing only from advertised observations
 * would let the forwarding plane decide what is true, which is the one thing it
 * must never do.
 *
 * So a subject that survives the cut is reconstructed from every fact the owner
 * holds for it, including facts that were never advertised. */
static int cmd_world(sqlite3 *db, int argc, char **argv) {
    const char *index_path = arg(argc, argv, "--index");
    const char *world = arg(argc, argv, "--world");
    const char *at = arg(argc, argv, "--at"), *as_of = arg(argc, argv, "--as-of");
    const char *region = arg(argc, argv, "--region"), *cell = arg(argc, argv, "--cell");
    const char *mobility = arg(argc, argv, "--mobility"), *scale = arg(argc, argv, "--scale");
    const char *lon = arg(argc, argv, "--lon"), *lat = arg(argc, argv, "--lat");
    const char *radius = arg(argc, argv, "--radius-m");
    int source_only = has_flag(argc, argv, "--source-claims-only");
    sqlite3_stmt *st = NULL;
    char sql[4096];
    int coarse_considered = 0, coarse_survivors = 0, emitted = 0;

    if (!index_path || !world) {
        fprintf(stderr, "world requires --index and --world\n");
        return 2;
    }
    if (!require_instant("--at", at)) return 2;
    if (!as_of) as_of = at;
    if (!require_instant("--as-of", as_of)) return 2;
    if ((lon || lat || radius) && !(lon && lat && radius)) {
        fprintf(stderr, "a radius filter needs --lon, --lat and --radius-m together\n");
        return 2;
    }

    if (!build_orphan_set(db, as_of)) return 1;
    {
        char uri[PATH_MAX * 3];
        sqlite3_stmt *attach = NULL;
        if (bf_sqlite_readonly_uri(uri, sizeof(uri), index_path) >= (int)sizeof(uri) ||
            sqlite3_prepare_v2(db, "ATTACH DATABASE ?1 AS plane", -1, &attach, NULL) != SQLITE_OK ||
            sqlite3_bind_text(attach, 1, uri, -1, SQLITE_TRANSIENT) != SQLITE_OK ||
            sqlite3_step(attach) != SQLITE_DONE) {
            fprintf(stderr, "spatial plane attach failed: %s\n", sqlite3_errmsg(db));
            sqlite3_finalize(attach);
            return 1;
        }
        sqlite3_finalize(attach);
    }

    /* How large the spatial population was before any narrowing, so the cut can
     * be shown to have done something rather than asserted to have. */
    if (sqlite3_prepare_v2(db,
        "SELECT COUNT(*) FROM plane.candidate_advertisement "
        "WHERE candidate_kind='spatial_observation'", -1, &st, NULL) == SQLITE_OK) {
        if (sqlite3_step(st) == SQLITE_ROW) coarse_considered = sqlite3_column_int(st, 0);
        sqlite3_finalize(st); st = NULL;
    }

    {
        char subject_source[3072];
        snprintf(subject_source, sizeof(subject_source),
            "WITH coarse AS ("
            "  SELECT DISTINCT substr(a.identity, length('cms:spatial_observation:') + 1) AS observation_id"
            "  FROM plane.candidate_advertisement a"
            "  WHERE a.candidate_kind = 'spatial_observation'"
            "    AND a.identity LIKE 'cms:spatial_observation:%%'"
            "    %s %s %s %s),"
            /* Advertisements name candidate subjects. They are not the facts. */
            "candidate_subject AS ("
            "  SELECT DISTINCT f.subject FROM spatial_fact f"
            "  JOIN coarse c ON c.observation_id = f.observation_id)"
            " SELECT (SELECT f.fact_id FROM spatial_fact f"
            "         WHERE f.subject = cs.subject AND %s AND %s %s"
            "         ORDER BY f.effective_at DESC, f.fact_id DESC LIMIT 1) AS fact_id"
            " FROM candidate_subject cs",
            region   ? "AND EXISTS(SELECT 1 FROM plane.candidate_advertisement_term t WHERE t.identity=a.identity AND t.axis='region' AND t.term=:region)" : "",
            cell     ? "AND EXISTS(SELECT 1 FROM plane.candidate_advertisement_term t WHERE t.identity=a.identity AND t.axis='cell' AND t.term=:cell)" : "",
            mobility ? "AND EXISTS(SELECT 1 FROM plane.candidate_advertisement_term t WHERE t.identity=a.identity AND t.axis='mobility' AND t.term=:mobility)" : "",
            scale    ? "AND EXISTS(SELECT 1 FROM plane.candidate_advertisement_term t WHERE t.identity=a.identity AND t.axis='scale' AND t.term=:scale)" : "",
            KNOWN_BY, EFFECTIVE_AT,
            source_only ? "AND f.observation_mode IN ('observed','reported')" : "");
        {
            sqlite3_stmt *bindable = NULL;
            char insert[4096];
            snprintf(insert, sizeof(insert),
                "INSERT OR IGNORE INTO temp.frontier_latest(fact_id) "
                "SELECT fact_id FROM (%s) WHERE fact_id IS NOT NULL", subject_source);
            if (!exec_sql(db, "DROP TABLE IF EXISTS temp.frontier_latest;"
                              "CREATE TEMP TABLE frontier_latest(fact_id INTEGER PRIMARY KEY);") ||
                sqlite3_prepare_v2(db, insert, -1, &bindable, NULL) != SQLITE_OK) {
                fprintf(stderr, "world: %s\n", sqlite3_errmsg(db));
                exec_sql(db, "DETACH DATABASE plane;");
                return 1;
            }
            bind_frontier(bindable, at, as_of);
            if (region)   sqlite3_bind_text(bindable, sqlite3_bind_parameter_index(bindable, ":region"), region, -1, SQLITE_TRANSIENT);
            if (cell)     sqlite3_bind_text(bindable, sqlite3_bind_parameter_index(bindable, ":cell"), cell, -1, SQLITE_TRANSIENT);
            if (mobility) sqlite3_bind_text(bindable, sqlite3_bind_parameter_index(bindable, ":mobility"), mobility, -1, SQLITE_TRANSIENT);
            if (scale)    sqlite3_bind_text(bindable, sqlite3_bind_parameter_index(bindable, ":scale"), scale, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(bindable) != SQLITE_DONE) {
                fprintf(stderr, "world: %s\n", sqlite3_errmsg(db));
                sqlite3_finalize(bindable);
                exec_sql(db, "DETACH DATABASE plane;");
                return 1;
            }
            sqlite3_finalize(bindable);
        }
    }
    snprintf(sql, sizeof(sql),
        "SELECT %s %s AS orphaned FROM spatial_fact f"
        " JOIN temp.frontier_latest l ON l.fact_id = f.fact_id %s",
        SELECT_COLS, ORPHANED_COL, ORPHAN_JOIN);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "world: %s\n", sqlite3_errmsg(db));
        exec_sql(db, "DETACH DATABASE plane;");
        return 1;
    }
    bind_frontier(st, at, as_of);
    if (region)   sqlite3_bind_text(st, sqlite3_bind_parameter_index(st, ":region"), region, -1, SQLITE_TRANSIENT);
    if (cell)     sqlite3_bind_text(st, sqlite3_bind_parameter_index(st, ":cell"), cell, -1, SQLITE_TRANSIENT);
    if (mobility) sqlite3_bind_text(st, sqlite3_bind_parameter_index(st, ":mobility"), mobility, -1, SQLITE_TRANSIENT);
    if (scale)    sqlite3_bind_text(st, sqlite3_bind_parameter_index(st, ":scale"), scale, -1, SQLITE_TRANSIENT);

    printf("{\"world\":"); json_text(world);
    printf(",\"frontier\":{\"at\":"); json_text(at);
    printf(",\"as_of\":"); json_text(as_of);
    printf("},\"present\":[");
    while (sqlite3_step(st) == SQLITE_ROW) {
        double distance = -1;
        coarse_survivors++;
        if (radius) {
            if (sqlite3_column_type(st, 9) == SQLITE_NULL) continue;
            distance = haversine_m(atof(lat), atof(lon),
                                   sqlite3_column_double(st, 10), sqlite3_column_double(st, 9));
            if (distance > atof(radius)) continue;
        }
        if (emitted++) putchar(',');
        emit_position(st, sqlite3_column_int(st, COL_ORPHANED));
    }
    sqlite3_finalize(st);
    printf("],\"coarse_considered\":%d,\"coarse_survivors\":%d,\"selected\":%d",
           coarse_considered, coarse_survivors, emitted);
    printf(",\"truth\":\"plane_named_candidate_subjects_owner_answered_from_its_own_log\"}\n");
    exec_sql(db, "DETACH DATABASE plane;");
    return 0;
}

/* --------------------------------------------------------------- history -- */

static int cmd_history(sqlite3 *db, int argc, char **argv) {
    const char *subject = arg(argc, argv, "--subject");
    sqlite3_stmt *st = NULL;
    int first = 1;
    if (!subject) { fprintf(stderr, "history requires --subject\n"); return 2; }
    if (sqlite3_prepare_v2(db,
        "SELECT fact_id,observation_id,observation_mode,observed_at,effective_at,"
        "valid_until,recorded_at,retracts,retraction_reason FROM spatial_fact "
        "WHERE subject=? ORDER BY fact_id", -1, &st, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_text(st, 1, subject, -1, SQLITE_TRANSIENT);
    printf("{\"subject\":"); json_text(subject); printf(",\"facts\":[");
    while (sqlite3_step(st) == SQLITE_ROW) {
        if (!first) putchar(','); first = 0;
        printf("{\"fact\":%lld,\"observation\":", (long long)sqlite3_column_int64(st, 0));
        json_text((const char *)sqlite3_column_text(st, 1));
        printf(",\"mode\":"); json_text((const char *)sqlite3_column_text(st, 2));
        printf(",\"observed_at\":"); json_text((const char *)sqlite3_column_text(st, 3));
        printf(",\"effective_at\":"); json_text((const char *)sqlite3_column_text(st, 4));
        if (sqlite3_column_type(st, 5) != SQLITE_NULL) {
            printf(",\"valid_until\":"); json_text((const char *)sqlite3_column_text(st, 5));
        }
        printf(",\"recorded_at\":"); json_text((const char *)sqlite3_column_text(st, 6));
        if (sqlite3_column_type(st, 7) != SQLITE_NULL) {
            printf(",\"retracts\":%lld,\"reason\":", (long long)sqlite3_column_int64(st, 7));
            json_text((const char *)sqlite3_column_text(st, 8));
        }
        putchar('}');
    }
    sqlite3_finalize(st);
    printf("],\"truth\":\"append_only_log_nothing_here_was_rewritten\"}\n");
    return 0;
}

/* ------------------------------------------------------------------ main -- */

static void usage(void) {
    fputs(
      "bonfyre-spatial --db <path> <command>\n"
      "  observe      append an observation or a declared derivation\n"
      "  retract      append a retraction of an earlier fact\n"
      "  reconstruct  --subject --at [--as-of] [--source-claims-only]\n"
      "  near         --at [--as-of] --lon --lat --radius-m\n"
      "  nearest      --at [--as-of] --lon --lat --n\n"
      "  bbox         --at [--as-of] --min-lon --min-lat --max-lon --max-lat\n"
      "  contains     --at [--as-of] --lon --lat   which areas contain this point\n"
      "  along-route  --at [--as-of] --route-geojson <file> --corridor-m N\n"
      "  visible-from --at [--as-of] --lon --lat --heading --fov --range-m\n"
      "  history      --subject\n"
      "  maintain     [--rebuild]   advance the maintained live state\n"
      "  durable      --out <parquet>            write the cold representation\n"
      "               --verify --out <parquet>   compare hot and cold by content\n"
      "               --thaw --from <parquet> --out <sqlite>\n"
      "  from-receipt --receipt <foreign-twin receipt> --profile P\n"
      "               --subject-property K --subject-kind K --time-property T\n"
      "               --time-format iso8601|epoch_s|epoch_ms\n"
      "  ingest       --geojson <doc> --profile P --source-occurrence O\n"
      "               --source-digest D --recorded-at T --provenance P\n"
      "               --subject-property K --subject-kind K --time-property T\n"
      "               --time-format iso8601|epoch_s|epoch_ms [--valid-for-seconds N]\n"
      "               [--alias-property P --alias-separator S]  identifier lineage\n"
      "               [--evidence-out <jsonl>]  append owner-transition evidence\n"
      "  world        --index <address-plane.db> --world W --at [--as-of]\n"
      "               [--region --cell --mobility --scale] [--lon --lat --radius-m]\n"
      "  address      --world W --at [--as-of] [--extent --region --cell --subject]\n"
      "               or --parse <address>\n"
      "\n"
      "--at is valid time (when a position holds); --as-of is knowledge time\n"
      "(what this estate had learned). They are separate questions.\n", stderr);
}

#define SPATIAL_COMMANDS(X) \
    X("address", SC_ADDRESS) \
    X("along-route", SC_ALONG_ROUTE) \
    X("bbox", SC_BBOX) \
    X("contains", SC_CONTAINS) \
    X("durable", SC_DURABLE) \
    X("from-receipt", SC_FROM_RECEIPT) \
    X("history", SC_HISTORY) \
    X("ingest", SC_INGEST) \
    X("init", SC_INIT) \
    X("maintain", SC_MAINTAIN) \
    X("near", SC_NEAR) \
    X("nearest", SC_NEAREST) \
    X("observe", SC_OBSERVE) \
    X("reconstruct", SC_RECONSTRUCT) \
    X("retract", SC_RETRACT) \
    X("visible-from", SC_VISIBLE_FROM) \
    X("world", SC_WORLD)

enum {
#define SC_ENUM(name, id) id,
    SPATIAL_COMMANDS(SC_ENUM)
#undef SC_ENUM
};
static const BfCommandEntry spatial_commands[] = {
#define SC_ENTRY(name, id) {name, id, 0, NULL},
    SPATIAL_COMMANDS(SC_ENTRY)
#undef SC_ENTRY
};

int main(int argc, char **argv) {
    sqlite3 *db = NULL;
    const char *path;
    const BfCommandEntry *command;
    int rc;
    if (argc < 2 || !(command = bf_command_lookup(spatial_commands,
            sizeof(spatial_commands) / sizeof(spatial_commands[0]), argv[1]))) {
        usage();
        return 2;
    }
    /* An address is a pure function of its coordinates. Requiring a store to
     * render or parse one would imply the address knows something about the
     * population, which is exactly what it must not. */
    if (command->id == SC_ADDRESS) return cmd_address(argc, argv);
    path = arg(argc, argv, "--db");
    if (!path) { usage(); return 2; }
    if (sqlite3_open(path, &db) != SQLITE_OK) {
        fprintf(stderr, "cannot open %s\n", path); return 1;
    }
    if (!init_schema(db)) { sqlite3_close(db); return 1; }

    switch (command->id) {
    case SC_OBSERVE: rc = cmd_observe(db, argc, argv); break;
    case SC_RETRACT: rc = cmd_retract(db, argc, argv); break;
    case SC_RECONSTRUCT: rc = cmd_reconstruct(db, argc, argv); break;
    case SC_NEAR: case SC_NEAREST: case SC_BBOX: case SC_CONTAINS:
    case SC_ALONG_ROUTE: case SC_VISIBLE_FROM:
        rc = spatial_query(db, argc, argv, command->name); break;
    case SC_HISTORY: rc = cmd_history(db, argc, argv); break;
    case SC_MAINTAIN: rc = cmd_maintain(db, argc, argv); break;
    case SC_DURABLE: rc = cmd_durable(db, argc, argv, path); break;
    case SC_WORLD: rc = cmd_world(db, argc, argv); break;
    case SC_INGEST: rc = cmd_ingest(db, argc, argv); break;
    case SC_FROM_RECEIPT: rc = cmd_from_receipt(db, argc, argv); break;
    case SC_INIT: rc = 0; break;
    default: usage(); rc = 2; break;
    }

    sqlite3_close(db);
    return rc;
}
