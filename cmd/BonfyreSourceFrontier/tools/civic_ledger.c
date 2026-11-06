/*
 * civic_ledger.c - plate-reader public records, from ForeignTwin receipts to
 * the Markdown a WebAddress site page takes (`bonfyre-web-address site page`).
 *
 *   civic-ledger towns   RECEIPT...                 county and town counts
 *   civic-ledger council PLACE RECORDS-HOST RECEIPT...  one place's agenda record
 *   civic-ledger reads   LABEL=RECEIPT...           every read tried, including refused ones
 *
 * A receipt is a `bonfyre-project foreign observe` receipt. Its body is read
 * only when robots allowed the read, the source answered 200 and the body still
 * hashes to content_sha256; anything else is refused (exit 3).
 *
 * `towns` accepts only Overpass statistics elements (type "city"/"county" made
 * with `make`). A body carrying any position (lat, lon, center, geometry,
 * bounds, nodes) is refused whole: these pages count by town on purpose and
 * never hold where a single camera stands.
 * `council` reads Legistar matters and keeps the item's title line, file
 * number, status, introduction date and the city's own record address.
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bonfyre.h"
#include "bf_json.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

typedef struct { char *receipt, *body; size_t body_len; char observed[32], source[1024]; } Read;

static char *slurp(const char *path, size_t *len) {
    FILE *f = fopen(path, "rb"); char *buf; long n;
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) || (n = ftell(f)) < 0 || fseek(f, 0, SEEK_SET)) { fclose(f); return NULL; }
    buf = malloc((size_t)n + 1);
    if (buf && fread(buf, 1, (size_t)n, f) != (size_t)n) { free(buf); buf = NULL; }
    fclose(f);
    if (buf) { buf[n] = 0; if (len) *len = (size_t)n; }
    return buf;
}

static int refuse(const char *path, const char *why) {
    fprintf(stderr, "civic-ledger: refused %s: %s\n", path, why);
    return 3;
}

static int str_is(const bf_json_doc_t *d, const bf_json_node_t *o, const char *k, const char *want) {
    char v[256]; const bf_json_node_t *n = bf_json_obj_get(d, o, k);
    return n && n->type == BF_JSON_STRING && bf_json_get_str_copy(n, v, sizeof v) >= 0 && !strcmp(v, want);
}

/* Load one receipt and the body it vouches for. */
static int load(const char *path, Read *r) {
    char err[128], want[65], got[65], body_path[2048], rel[1024];
    bf_json_doc_t *d; const bf_json_node_t *root, *n;
    memset(r, 0, sizeof *r);
    if (!(r->receipt = slurp(path, NULL))) return refuse(path, "receipt unreadable");
    if (!(d = bf_json_parse_str(r->receipt, err, sizeof err))) return refuse(path, "receipt is not JSON");
    root = bf_json_root(d);
    if (!str_is(d, root, "schema", "bonfyre-foreign-observation.v1")) { bf_json_free(d); return refuse(path, "not a ForeignTwin observation"); }
    if (!str_is(d, root, "policy_decision", "allow")) { bf_json_free(d); return refuse(path, "robots did not allow this read"); }
    n = bf_json_obj_get(d, root, "http_status");
    if (!n || bf_json_get_int(n) != 200) { bf_json_free(d); return refuse(path, "source did not answer 200"); }
    if (!(n = bf_json_obj_get(d, root, "content_sha256")) || bf_json_get_str_copy(n, want, sizeof want) != 64) { bf_json_free(d); return refuse(path, "no digest"); }
    if (!(n = bf_json_obj_get(d, root, "body_path")) || bf_json_get_str_copy(n, rel, sizeof rel) <= 0) { bf_json_free(d); return refuse(path, "no body path"); }
    if ((n = bf_json_obj_get(d, root, "observed_at"))) bf_json_get_str_copy(n, r->observed, sizeof r->observed);
    if ((n = bf_json_obj_get(d, root, "source_url"))) bf_json_get_str_copy(n, r->source, sizeof r->source);
    bf_json_free(d);
    if (rel[0] == '/') snprintf(body_path, sizeof body_path, "%s", rel);
    else {
        const char *slash = strrchr(path, '/');
        if (slash) snprintf(body_path, sizeof body_path, "%.*s/%s", (int)(slash - path), path, rel);
        else snprintf(body_path, sizeof body_path, "%s", rel);
    }
    if (!(r->body = slurp(body_path, &r->body_len))) return refuse(path, "body unreadable");
    bf_sha256_hex((const uint8_t *)r->body, r->body_len, got);
    if (strcmp(got, want)) return refuse(path, "body no longer hashes to its receipt");
    return 0;
}

static void day(const char *iso, char out[11]) { snprintf(out, 11, "%.10s", iso && *iso ? iso : "unknown"); }

/* Markdown-safe text: drop characters the page reader would treat as markup. */
static void put_text(const char *s, int len) {
    for (int i = 0; i < len && s[i]; i++) {
        char c = s[i];
        if (c == '\\' && i + 1 < len) {          /* strings are zero-copy: undo JSON escapes */
            char e = s[++i];
            if (e == 'r' || e == 'n') break;
            if (e == 'u' && i + 4 < len) {
                unsigned u = (unsigned)strtoul((char[5]){s[i+1], s[i+2], s[i+3], s[i+4], 0}, NULL, 16); i += 4;
                if (u < 0x20) continue;
                if (u < 0x80) putchar((int)u);
                else if (u < 0x800) { putchar(0xC0 | (u >> 6)); putchar(0x80 | (u & 0x3F)); }
                else { putchar(0xE0 | (u >> 12)); putchar(0x80 | ((u >> 6) & 0x3F)); putchar(0x80 | (u & 0x3F)); }
                continue;
            }
            c = e == 't' ? ' ' : e;
        }
        if (c == '\r' || c == '\n') break;
        if (c == '[' || c == ']' || c == '`' || c == '|' || c == '*' || c == '<' || c == '>') c = c == '[' ? '(' : c == ']' ? ')' : ' ';
        putchar(c);
    }
}

/* ---- towns ------------------------------------------------------------ */

typedef struct { char name[96]; long cameras; char counties[256]; } Town;

static const char *positional[] = {"lat", "lon", "center", "geometry", "bounds", "nodes", "members"};

static int towns(int argc, char **argv) {
    Town *t = calloc(1024, sizeof *t), *c = calloc(64, sizeof *c);
    int nt = 0, nc = 0, rc; char latest[32] = "";
    if (!t || !c) return 1;
    for (int a = 0; a < argc; a++) {
        Read r; char err[128]; bf_json_doc_t *d; const bf_json_node_t *root, *els, *e, *cr;
        if ((rc = load(argv[a], &r))) return rc;
        if (!(d = bf_json_parse(r.body, r.body_len, err, sizeof err))) return refuse(argv[a], "body is not JSON");
        root = bf_json_root(d);
        cr = bf_json_dotpath(d, "osm3s.copyright");
        { int l = 0; const char *s = cr ? bf_json_get_str(cr, &l) : NULL;
          if (!s || !memmem(s, (size_t)l, "ODbL", 4)) { bf_json_free(d); return refuse(argv[a], "no ODbL notice; not an OpenStreetMap answer"); } }
        if (!(els = bf_json_obj_get(d, root, "elements")) || els->type != BF_JSON_ARRAY) { bf_json_free(d); return refuse(argv[a], "no elements"); }
        for (e = bf_json_child_first(d, els); e; e = bf_json_child_next(d, e)) {
            const bf_json_node_t *tags = bf_json_obj_get(d, e, "tags"), *v;
            char kind[16] = "", name[96] = "", county[96] = "", num[32] = "";
            for (size_t p = 0; p < sizeof positional / sizeof *positional; p++)
                if (bf_json_obj_get(d, e, positional[p])) { bf_json_free(d); return refuse(argv[a], "an element carries a position; only counts by town are taken"); }
            if ((v = bf_json_obj_get(d, e, "type"))) bf_json_get_str_copy(v, kind, sizeof kind);
            if (strcmp(kind, "city") && strcmp(kind, "county")) { bf_json_free(d); return refuse(argv[a], "an element is not a town or county count"); }
            if (!tags) continue;
            if ((v = bf_json_obj_get(d, tags, "name"))) bf_json_get_str_copy(v, name, sizeof name);
            if ((v = bf_json_obj_get(d, tags, "county"))) bf_json_get_str_copy(v, county, sizeof county);
            if ((v = bf_json_obj_get(d, tags, "cameras"))) bf_json_get_str_copy(v, num, sizeof num);
            if (!name[0] || strstr(name, "< multiple")) continue;
            long cams = strtol(num, NULL, 10);
            if (!strcmp(kind, "county")) {
                int i; for (i = 0; i < nc && strcmp(c[i].name, name); i++) {}
                if (i == nc && nc < 64) snprintf(c[nc++].name, sizeof c->name, "%s", name);
                if (i < 64 && cams > c[i].cameras) c[i].cameras = cams;
                continue;
            }
            if (cams <= 0) continue;
            int i; for (i = 0; i < nt && strcmp(t[i].name, name); i++) {}
            if (i == nt) { if (nt == 1024) continue; snprintf(t[nt++].name, sizeof t->name, "%s", name); }
            if (cams > t[i].cameras) t[i].cameras = cams;
            if (county[0] && !strstr(t[i].counties, county)) {
                size_t l = strlen(t[i].counties);
                snprintf(t[i].counties + l, sizeof t->counties - l, "%s%s", l ? ", " : "", county);
            }
        }
        bf_json_free(d);
        if (strcmp(r.observed, latest) > 0) snprintf(latest, sizeof latest, "%s", r.observed);
    }
    for (int i = 1; i < nt; i++) for (int j = i; j > 0 && (t[j].cameras > t[j-1].cameras || (t[j].cameras == t[j-1].cameras && strcmp(t[j].name, t[j-1].name) < 0)); j--) { Town x = t[j]; t[j] = t[j-1]; t[j-1] = x; }
    for (int i = 1; i < nc; i++) for (int j = i; j > 0 && c[j].cameras > c[j-1].cameras; j--) { Town x = c[j]; c[j] = c[j-1]; c[j-1] = x; }
    char when[11]; day(latest, when);
    long total = 0; for (int i = 0; i < nc; i++) total += c[i].cameras;
    printf("# Cameras by town\n\n");
    printf("Volunteers have put %ld license plate readers on the public map across %d counties. Here they are counted town by town, as the map stood on %s.\n\n", total, nc, when);
    printf("## By county\n\n| County | Readers on the map |\n|---|---:|\n");
    for (int i = 0; i < nc; i++) printf("| %s | %ld |\n", c[i].name, c[i].cameras);
    printf("\n## Town by town\n\n%d towns have at least one reader on the map. Each town is counted once, inside its own city limits, even where it crosses a county line.\n\n| Town | Readers on the map |\n|---|---:|\n", nt);
    for (int i = 0; i < nt; i++) printf("| %s | %ld |\n", t[i].name, t[i].cameras);
    printf("\n## How sure this is\n\n");
    printf("These numbers come from OpenStreetMap, where people tag a pole as a license plate reader after seeing it. A city has not confirmed them. A reader can be missing because nobody has mapped it yet, and a mapped one can be gone. Use a number here to ask a question, and use the city's own records for the answer.\n\n");
    printf("## Why there is no map\n\n");
    printf("This page counts by town and stops there, so nobody can use it to find or steer around a single camera. A town count is enough to ask a council how many cameras it pays for and who can search them.\n\n");
    printf("Map data from [OpenStreetMap](https://www.openstreetmap.org/copyright) contributors, available under the Open Database License. Counted on %s.\n", when);
    free(t); free(c);
    return 0;
}

/* ---- council ---------------------------------------------------------- */

/* A title search for "license plate" also finds registration ordinances; keep
 * only items that name a reader system or its vendor. */
static int about_readers(const char *t, int len) {
    static const char *says[] = {"flock", "plate read", "plate camera", "plate recogn", "alpr", "lpr ", "plate-read"};
    char low[2048]; int n = len < (int)sizeof low - 1 ? len : (int)sizeof low - 1;
    for (int i = 0; i < n; i++) low[i] = (char)((t[i] >= 'A' && t[i] <= 'Z') ? t[i] + 32 : t[i]);
    low[n] = 0;
    for (size_t i = 0; i < sizeof says / sizeof *says; i++) if (strstr(low, says[i])) return 1;
    return 0;
}

typedef struct { long id; char guid[64], file[64], status[64], date[11]; const char *title; int title_len; } Item;

static int council(const char *place, const char *host, int argc, char **argv) {
    Item *it = calloc(512, sizeof *it); int n = 0, rc; char latest[32] = "";
    Read *reads = calloc((size_t)argc, sizeof *reads); bf_json_doc_t **docs = calloc((size_t)argc, sizeof *docs);
    if (!it || !reads || !docs) return 1;
    for (const char *p = host; *p; p++) if (!strchr("abcdefghijklmnopqrstuvwxyz0123456789.-", *p)) return refuse(host, "records host is not a plain host name");
    for (int a = 0; a < argc; a++) {
        char err[128]; const bf_json_node_t *root, *m, *v;
        if ((rc = load(argv[a], &reads[a]))) return rc;
        if (!(docs[a] = bf_json_parse(reads[a].body, reads[a].body_len, err, sizeof err))) return refuse(argv[a], "body is not JSON");
        root = bf_json_root(docs[a]);
        if (root->type != BF_JSON_ARRAY) return refuse(argv[a], "not a list of agenda items");
        if (strcmp(reads[a].observed, latest) > 0) snprintf(latest, sizeof latest, "%s", reads[a].observed);
        for (m = bf_json_child_first(docs[a], root); m; m = bf_json_child_next(docs[a], m)) {
            long id = (v = bf_json_obj_get(docs[a], m, "MatterId")) ? (long)bf_json_get_int(v) : 0; int i;
            if (!id) continue;
            for (i = 0; i < n && it[i].id != id; i++) {}
            if (i < n || n == 512) continue;
            it[n].id = id;
            if ((v = bf_json_obj_get(docs[a], m, "MatterGuid"))) bf_json_get_str_copy(v, it[n].guid, sizeof it->guid);
            if ((v = bf_json_obj_get(docs[a], m, "MatterFile"))) bf_json_get_str_copy(v, it[n].file, sizeof it->file);
            if ((v = bf_json_obj_get(docs[a], m, "MatterStatusName"))) bf_json_get_str_copy(v, it[n].status, sizeof it->status);
            if ((v = bf_json_obj_get(docs[a], m, "MatterIntroDate")) && v->type == BF_JSON_STRING) { int l; const char *s = bf_json_get_str(v, &l); snprintf(it[n].date, sizeof it->date, "%.*s", l < 10 ? l : 10, s); }
            if ((v = bf_json_obj_get(docs[a], m, "MatterTitle"))) it[n].title = bf_json_get_str(v, &it[n].title_len);
            for (char *g = it[n].guid; *g; g++) if (!strchr("0123456789ABCDEFabcdef-", *g)) { it[n].guid[0] = 0; break; }
            if (it[n].title && about_readers(it[n].title, it[n].title_len)) n++;
        }
    }
    for (int i = 1; i < n; i++) for (int j = i; j > 0 && strcmp(it[j].date, it[j-1].date) > 0; j--) { Item x = it[j]; it[j] = it[j-1]; it[j-1] = x; }
    char when[11]; day(latest, when);
    printf("## %s\n\n", place);
    if (!n) { printf("Nothing in %s's agenda record mentions plate readers or Flock by name, as read on %s. That is worth a records request of its own.\n\n", place, when); goto out; }
    printf("%d %s in %s's agenda record %s plate readers or Flock, newest first, as read on %s.\n\n", n, n == 1 ? "item" : "items", place, n == 1 ? "names" : "name", when);
    for (int i = 0; i < n; i++) {
        printf("- %s: ", it[i].date[0] ? it[i].date : "undated");
        put_text(it[i].title, it[i].title_len);
        printf(" (item ");
        put_text(it[i].file, (int)strlen(it[i].file));
        if (it[i].status[0]) { printf(", "); put_text(it[i].status, (int)strlen(it[i].status)); }
        printf(")");
        if (it[i].guid[0]) printf(" [Read it on the city's site](https://%s/LegislationDetail.aspx?ID=%ld&GUID=%s)", host, it[i].id, it[i].guid);
        printf("\n");
    }
    printf("\n");
out:
    for (int a = 0; a < argc; a++) { if (docs[a]) bf_json_free(docs[a]); free(reads[a].receipt); free(reads[a].body); }
    free(docs); free(reads); free(it);
    return 0;
}

/* ---- reads: what was asked of whom, and what came back ------------------- */

static int reads(int argc, char **argv) {
    printf("| Source | Read on | What came back |\n|---|---|---|\n");
    for (int a = 0; a < argc; a++) {
        char *eq = strchr(argv[a], '='), err[128], decision[64] = "", observed[32] = "", url[1024] = "", host[256] = "";
        size_t len; char *text; bf_json_doc_t *d; const bf_json_node_t *root, *v; long status = 0;
        if (!eq) return refuse(argv[a], "expected LABEL=RECEIPT");
        *eq = 0;
        if (!(text = slurp(eq + 1, &len)) || !(d = bf_json_parse(text, len, err, sizeof err))) return refuse(eq + 1, "receipt unreadable");
        root = bf_json_root(d);
        if (!str_is(d, root, "schema", "bonfyre-foreign-observation.v1")) return refuse(eq + 1, "not a ForeignTwin observation");
        if ((v = bf_json_obj_get(d, root, "policy_decision"))) bf_json_get_str_copy(v, decision, sizeof decision);
        if ((v = bf_json_obj_get(d, root, "observed_at"))) bf_json_get_str_copy(v, observed, sizeof observed);
        if ((v = bf_json_obj_get(d, root, "source_url"))) bf_json_get_str_copy(v, url, sizeof url);
        if ((v = bf_json_obj_get(d, root, "http_status"))) status = (long)bf_json_get_int(v);
        if (!strncmp(url, "https://", 8)) { size_t n = strcspn(url + 8, "/?#"); snprintf(host, sizeof host, "%.*s", (int)n, url + 8); }
        char when[11]; day(observed, when);
        printf("| "); put_text(argv[a], (int)strlen(argv[a])); printf(" (%s) | %s | ", host[0] ? host : "unknown", when);
        if (strcmp(decision, "allow")) printf("Its robots rules ask readers to stay out, so it was not read");
        else if (status == 200) printf("Read in full");
        else printf("The server answered with an error; try again later");
        printf(" |\n");
        bf_json_free(d); free(text);
    }
    return 0;
}

int main(int argc, char **argv) {
    enum { CL_COUNCIL, CL_READS, CL_TOWNS };
    static const BfCommandEntry civic_commands[] = {
        {"council", CL_COUNCIL, 0, NULL},
        {"reads", CL_READS, 0, NULL},
        {"towns", CL_TOWNS, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(civic_commands,
        sizeof(civic_commands) / sizeof(civic_commands[0]), argv[1]);
    if (argc >= 3 && command && command->id == CL_READS) return reads(argc - 2, argv + 2);
    if (argc >= 3 && command && command->id == CL_TOWNS) return towns(argc - 2, argv + 2);
    if (argc >= 5 && command && command->id == CL_COUNCIL) return council(argv[2], argv[3], argc - 4, argv + 4);
    fprintf(stderr, "usage: civic-ledger towns RECEIPT...\n       civic-ledger council PLACE RECORDS-HOST RECEIPT...\n       civic-ledger reads LABEL=RECEIPT...\n");
    return 2;
}
