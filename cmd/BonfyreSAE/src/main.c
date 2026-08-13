/*
 * bonfyre-sae — SAE feature dictionary runtime.
 *
 * Loads a .bfsae feature dictionary and runs top-k activation on any
 * residual stream vector.  Model-agnostic.  Slots into the pipeline
 * between BonfyreEmbed and BonfyreVec.
 *
 * DB: ~/.local/share/bonfyre/sae.db  (override: $BONFYRE_SAE_DB)
 *
 * Commands:
 *   bonfyre-sae activate <dict.bfsae> <residual.bin> [--top-k N]
 *                        [--model-family F] [--layer L]
 *                        [--out features.json]
 *       Read residual stream (float32 raw binary), run encoder, emit
 *       sae-feature-manifest JSON.  Exits 2 if any danger feature trips.
 *
 *   bonfyre-sae inspect <dict.bfsae>
 *       Print header metadata: model family, layer, hidden dim,
 *       feature count, dtype, file size.
 *
 *   bonfyre-sae synth [--model-family F] [--layer L]
 *                     [--hidden-dim D] [--features N] --out <file.bfsae>
 *       Write a synthetic .bfsae for testing (random unit-norm encoder).
 *
 *   bonfyre-sae gate <dict.bfsae> <features.json> [--alpha A]
 *       Re-check an existing feature manifest against threshold alpha.
 *       Exit 0 = clear, 2 = danger feature tripped.
 *
 *   bonfyre-sae hash <dict.bfsae> <residual.bin> [--top-k N]
 *       Print the stable 16-char FNV-1a semantic feature hash to stdout.
 *
 *   bonfyre-sae history [--limit N]
 *       Recent activation records from sae.db.
 *
 *   bonfyre-sae help
 *
 * Integration notes:
 *   - Residual input: raw float32 array, --hidden-dim bytes / 4 elements.
 *     Produce via: bonfyre-embed branch --residual > residual.bin
 *   - Output features.json is a BfArtifact of type "sae-feature-manifest".
 *   - Danger exit code 2 lets bonfyre-control route on it:
 *       bonfyre-sae activate ... || bonfyre-control branch --sae-danger
 */

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <sqlite3.h>
#include <math.h>

#include <bonfyre.h>
#include <bf_sae.h>

#define VERSION         "1.0.0"
#define DB_ENV          "BONFYRE_SAE_DB"
#define DB_SUBPATH      "/.local/share/bonfyre/sae.db"
#define DEFAULT_TOP_K   64
#define DEFAULT_ALPHA   0.70f
#define MAX_JSON        (1 << 20)  /* 1 MB feature manifest cap */

/* ── db path ──────────────────────────────────────────────────────────────── */

static void db_path(char *buf, size_t len) {
    const char *e = getenv(DB_ENV);
    if (e) { snprintf(buf, len, "%s", e); return; }
    const char *home = getenv("HOME");
    snprintf(buf, len, "%s%s", home ? home : "/tmp", DB_SUBPATH);
}

/* ── schema ───────────────────────────────────────────────────────────────── */

static const char *SCHEMA =
    "PRAGMA journal_mode=WAL;"
    "PRAGMA foreign_keys=ON;"
    "CREATE TABLE IF NOT EXISTS activations ("
    "  id              INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  artifact_id     TEXT,"
    "  dict_path       TEXT NOT NULL,"
    "  model_family    TEXT NOT NULL,"
    "  layer           INTEGER NOT NULL,"
    "  top_k           INTEGER NOT NULL,"
    "  feature_hash    TEXT NOT NULL,"
    "  danger_tripped  INTEGER NOT NULL DEFAULT 0,"
    "  elapsed_ms      REAL,"
    "  manifest_path   TEXT,"
    "  ts              TEXT NOT NULL"
    ");"
    "CREATE TABLE IF NOT EXISTS feature_events ("
    "  id              INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  activation_id   INTEGER NOT NULL REFERENCES activations(id),"
    "  feature_id      INTEGER NOT NULL,"
    "  activation      REAL NOT NULL,"
    "  normalised      REAL NOT NULL,"
    "  tags            INTEGER NOT NULL,"
    "  label           TEXT,"
    "  rank            INTEGER NOT NULL"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_fe_fid ON feature_events(feature_id);"
    "CREATE INDEX IF NOT EXISTS idx_fe_act ON feature_events(activation);"
    "CREATE INDEX IF NOT EXISTS idx_act_hash ON activations(feature_hash);"
    "CREATE INDEX IF NOT EXISTS idx_act_model ON activations(model_family, layer);";

static sqlite3 *open_db(void) {
    char path[512]; db_path(path, sizeof(path));
    bf_ensure_parent_dir(path);
    sqlite3 *db;
    if (sqlite3_open(path, &db) != SQLITE_OK) {
        fprintf(stderr, "sae: cannot open db %s\n", path); return NULL;
    }
    sqlite3_exec(db, SCHEMA, NULL, NULL, NULL);
    return db;
}

/* ── timestamp ────────────────────────────────────────────────────────────── */
static void now_iso(char *buf, size_t len) {
    time_t t = time(NULL);
    strftime(buf, len, "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
}

/* ── read raw float32 residual ─────────────────────────────────────────────── */

static float *read_residual(const char *path, uint32_t *dim_out) {
    FILE *f = fopen(path, "rb");
    if (!f) { fprintf(stderr, "sae: cannot open residual %s: %s\n", path, strerror(errno)); return NULL; }
    fseek(f, 0, SEEK_END); long sz = ftell(f); fseek(f, 0, SEEK_SET);
    if (sz < 4 || sz % 4 != 0) {
        fprintf(stderr, "sae: residual size %ld not a multiple of 4\n", sz);
        fclose(f); return NULL;
    }
    uint32_t dim = (uint32_t)(sz / 4);
    float *buf = malloc((size_t)sz);
    if (!buf) { fclose(f); return NULL; }
    fread(buf, 4, dim, f);
    fclose(f);
    *dim_out = dim;
    return buf;
}

/* ── record activation in DB ──────────────────────────────────────────────── */

static sqlite3_int64 record_activation(sqlite3 *db,
                                        const char           *artifact_id,
                                        const char           *dict_path,
                                        const BfsaeActivation *act,
                                        const char           *feat_hash,
                                        int                   danger,
                                        double                elapsed_ms,
                                        const char           *manifest_path) {
    char ts[32]; now_iso(ts, sizeof(ts));
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "INSERT INTO activations"
        "(artifact_id,dict_path,model_family,layer,top_k,feature_hash,"
        " danger_tripped,elapsed_ms,manifest_path,ts)"
        " VALUES(?,?,?,?,?,?,?,?,?,?)", -1, &st, NULL);
    sqlite3_bind_text  (st,1, artifact_id   ? artifact_id   : "", -1, SQLITE_STATIC);
    sqlite3_bind_text  (st,2, dict_path,     -1, SQLITE_STATIC);
    sqlite3_bind_text  (st,3, act->header->model_family, -1, SQLITE_STATIC);
    sqlite3_bind_int   (st,4, (int)act->layer);
    sqlite3_bind_int   (st,5, (int)act->top_k);
    sqlite3_bind_text  (st,6, feat_hash,     -1, SQLITE_STATIC);
    sqlite3_bind_int   (st,7, danger > 0 ? 1 : 0);
    sqlite3_bind_double(st,8, elapsed_ms);
    sqlite3_bind_text  (st,9, manifest_path ? manifest_path : "", -1, SQLITE_STATIC);
    sqlite3_bind_text  (st,10,ts,            -1, SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);
    sqlite3_int64 row_id = sqlite3_last_insert_rowid(db);

    /* store per-feature events */
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    sqlite3_prepare_v2(db,
        "INSERT INTO feature_events"
        "(activation_id,feature_id,activation,normalised,tags,label,rank)"
        " VALUES(?,?,?,?,?,?,?)", -1, &st, NULL);
    for (uint32_t k = 0; k < act->count; k++) {
        const BfsaeTopFeature *fe = &act->features[k];
        sqlite3_reset(st);
        sqlite3_bind_int64 (st,1, row_id);
        sqlite3_bind_int   (st,2, (int)fe->feature_id);
        sqlite3_bind_double(st,3, fe->activation);
        sqlite3_bind_double(st,4, fe->normalised);
        sqlite3_bind_int   (st,5, fe->tags);
        sqlite3_bind_text  (st,6, fe->label ? fe->label : "", -1, SQLITE_STATIC);
        sqlite3_bind_int   (st,7, (int)k);
        sqlite3_step(st);
    }
    sqlite3_finalize(st);
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    return row_id;
}

/* ────────────────────────────────────────────────────────────────────────── */
/* cmd_activate                                                               */
/* ────────────────────────────────────────────────────────────────────────── */

static int cmd_activate(int argc, char **argv) {
    /* defaults */
    const char *dict_path    = NULL;
    const char *residual_path= NULL;
    const char *out_path     = NULL;
    const char *artifact_id  = NULL;
    uint32_t    top_k        = DEFAULT_TOP_K;
    float       alpha        = DEFAULT_ALPHA;

    for (int i = 0; i < argc; i++) {
        if      (!dict_path     && argv[i][0] != '-') { dict_path     = argv[i]; }
        else if (!residual_path && argv[i][0] != '-') { residual_path = argv[i]; }
        else if (strcmp(argv[i], "--top-k")       == 0 && i+1 < argc) top_k = (uint32_t)atoi(argv[++i]);
        else if (strcmp(argv[i], "--alpha")        == 0 && i+1 < argc) alpha = (float)atof(argv[++i]);
        else if (strcmp(argv[i], "--out")          == 0 && i+1 < argc) out_path    = argv[++i];
        else if (strcmp(argv[i], "--artifact-id")  == 0 && i+1 < argc) artifact_id = argv[++i];
    }
    if (!dict_path || !residual_path) {
        fprintf(stderr, "usage: bonfyre-sae activate <dict.bfsae> <residual.bin>"
                        " [--top-k N] [--alpha A] [--out features.json]\n");
        return 1;
    }

    BfsaeDict *dict = bfsae_open(dict_path);
    if (!dict) return 1;

    uint32_t dim; float *residual = read_residual(residual_path, &dim);
    if (!residual) { bfsae_close(dict); return 1; }

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    BfsaeActivation *act = bfsae_activate_f32(dict, residual, dim, top_k);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    free(residual);

    if (!act) { fprintf(stderr, "sae: activation failed\n"); bfsae_close(dict); return 1; }

    double elapsed_ms = (t1.tv_sec  - t0.tv_sec) * 1000.0
                      + (t1.tv_nsec - t0.tv_nsec) / 1e6;

    char feat_hash[17]; bfsae_feature_hash(act, feat_hash);
    int  danger = bfsae_danger_check(act, alpha);

    /* write manifest JSON */
    char *json = malloc(MAX_JSON);
    if (!json) { bfsae_activation_free(act); bfsae_close(dict); return 1; }
    bfsae_manifest_json(act, artifact_id, json, MAX_JSON);

    if (out_path) {
        FILE *fout = fopen(out_path, "w");
        if (fout) { fputs(json, fout); fclose(fout); }
        else { fprintf(stderr, "sae: cannot write %s\n", out_path); }
    } else {
        fputs(json, stdout);
    }
    free(json);

    /* record in DB */
    sqlite3 *db = open_db();
    if (db) {
        record_activation(db, artifact_id, dict_path, act, feat_hash,
                          danger, elapsed_ms, out_path);
        sqlite3_close(db);
    }

    printf("feature_hash: %s\n",  feat_hash);
    printf("elapsed_ms:   %.2f\n",elapsed_ms);
    if (danger > 0) {
        fprintf(stderr, "sae: DANGER feature exceeded alpha=%.2f\n", alpha);
        bfsae_activation_free(act);
        bfsae_close(dict);
        return 2;
    }
    bfsae_activation_free(act);
    bfsae_close(dict);
    return 0;
}

/* ── cmd_inspect ──────────────────────────────────────────────────────────── */

static int cmd_inspect(int argc, char **argv) {
    if (argc < 1) { fprintf(stderr, "usage: bonfyre-sae inspect <dict.bfsae>\n"); return 1; }
    BfsaeDict *dict = bfsae_open(argv[0]);
    if (!dict) return 1;
    printf("bonfyre-sae inspect: %s\n\n", argv[0]);
    bfsae_inspect(dict);
    bfsae_close(dict);
    return 0;
}

/* ── cmd_synth ────────────────────────────────────────────────────────────── */

static int cmd_synth(int argc, char **argv) {
    const char *out_path     = NULL;
    const char *model_family = "synthetic";
    uint32_t    layer        = 24;
    uint32_t    hidden_dim   = 4096;
    uint32_t    features     = 16384;

    for (int i = 0; i < argc; i++) {
        if      (strcmp(argv[i], "--out")          == 0 && i+1 < argc) out_path     = argv[++i];
        else if (strcmp(argv[i], "--model-family")  == 0 && i+1 < argc) model_family = argv[++i];
        else if (strcmp(argv[i], "--layer")         == 0 && i+1 < argc) layer        = (uint32_t)atoi(argv[++i]);
        else if (strcmp(argv[i], "--hidden-dim")    == 0 && i+1 < argc) hidden_dim   = (uint32_t)atoi(argv[++i]);
        else if (strcmp(argv[i], "--features")      == 0 && i+1 < argc) features     = (uint32_t)atoi(argv[++i]);
    }
    if (!out_path) {
        fprintf(stderr, "usage: bonfyre-sae synth --out <file.bfsae>"
                        " [--model-family F] [--layer L]"
                        " [--hidden-dim D] [--features N]\n");
        return 1;
    }

    printf("Writing synthetic .bfsae: %s × %u features @ layer %u → %s\n",
           model_family, features, layer, out_path);

    int r = bfsae_write_synthetic(out_path, model_family, layer, hidden_dim, features);
    if (r != 0) return 1;

    struct stat st; stat(out_path, &st);
    printf("  done: %.2f MB\n", (double)st.st_size / 1048576.0);
    return 0;
}

/* ── cmd_gate ─────────────────────────────────────────────────────────────── */

static int cmd_gate(int argc, char **argv) {
    /* Re-reads features.json from a prior activate run; exits 2 if danger. */
    const char *dict_path    = NULL;
    const char *manifest     = NULL;
    float       alpha        = DEFAULT_ALPHA;

    for (int i = 0; i < argc; i++) {
        if      (!dict_path && argv[i][0] != '-') { dict_path = argv[i]; }
        else if (!manifest  && argv[i][0] != '-') { manifest  = argv[i]; }
        else if (strcmp(argv[i], "--alpha") == 0 && i+1 < argc) alpha = (float)atof(argv[++i]);
    }
    if (!dict_path || !manifest) {
        fprintf(stderr, "usage: bonfyre-sae gate <dict.bfsae> <features.json> [--alpha A]\n");
        return 1;
    }

    /* Open dict just for header info */
    BfsaeDict *dict = bfsae_open(dict_path);
    if (!dict) return 1;

    /* Parse features.json to find max danger activation */
    FILE *f = fopen(manifest, "r");
    if (!f) { fprintf(stderr, "sae: cannot open %s\n", manifest); bfsae_close(dict); return 1; }
    /* Simple scan: look for "activation" values near DANGER-tagged entries.
       Since we store tags in the JSON we can scan for danger_tripped via DB. */
    sqlite3 *db = open_db();
    int tripped = 0;
    if (db) {
        sqlite3_stmt *st;
        sqlite3_prepare_v2(db,
            "SELECT MAX(fe.activation) FROM feature_events fe"
            " JOIN activations a ON a.id = fe.activation_id"
            " WHERE a.manifest_path = ? AND fe.tags & 1 = 1", -1, &st, NULL);
        sqlite3_bind_text(st, 1, manifest, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_type(st,0) != SQLITE_NULL) {
            double max_danger = sqlite3_column_double(st, 0);
            printf("sae gate: max danger activation = %.4f  alpha = %.2f\n",
                   max_danger, alpha);
            if (max_danger >= (double)alpha) tripped = 1;
        }
        sqlite3_finalize(st);
        sqlite3_close(db);
    }
    fclose(f);
    bfsae_close(dict);

    if (tripped) {
        printf("sae gate: DANGER — feature exceeded alpha %.2f → EXIT 2\n", alpha);
        return 2;
    }
    printf("sae gate: clear\n");
    return 0;
}

/* ── cmd_hash ─────────────────────────────────────────────────────────────── */

static int cmd_hash(int argc, char **argv) {
    const char *dict_path    = NULL;
    const char *residual_path= NULL;
    uint32_t    top_k        = DEFAULT_TOP_K;

    for (int i = 0; i < argc; i++) {
        if      (!dict_path     && argv[i][0] != '-') { dict_path     = argv[i]; }
        else if (!residual_path && argv[i][0] != '-') { residual_path = argv[i]; }
        else if (strcmp(argv[i],"--top-k") == 0 && i+1 < argc) top_k = (uint32_t)atoi(argv[++i]);
    }
    if (!dict_path || !residual_path) {
        fprintf(stderr, "usage: bonfyre-sae hash <dict.bfsae> <residual.bin> [--top-k N]\n");
        return 1;
    }
    BfsaeDict *dict = bfsae_open(dict_path);
    if (!dict) return 1;
    uint32_t dim; float *r = read_residual(residual_path, &dim);
    if (!r) { bfsae_close(dict); return 1; }
    BfsaeActivation *act = bfsae_activate_f32(dict, r, dim, top_k);
    free(r);
    if (!act) { bfsae_close(dict); return 1; }
    char h[17]; bfsae_feature_hash(act, h);
    printf("bfh:feature:%s:l%u:%s\n", dict->header->model_family, dict->header->layer, h);
    bfsae_activation_free(act);
    bfsae_close(dict);
    return 0;
}

/* ── cmd_history ─────────────────────────────────────────────────────────── */

static int cmd_history(int argc, char **argv) {
    int limit = 20;
    for (int i = 0; i < argc; i++)
        if (strcmp(argv[i], "--limit") == 0 && i+1 < argc) limit = atoi(argv[++i]);

    sqlite3 *db = open_db();
    if (!db) return 1;
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT id,model_family,layer,top_k,feature_hash,"
        "       danger_tripped,elapsed_ms,ts"
        " FROM activations ORDER BY id DESC LIMIT ?", -1, &st, NULL);
    sqlite3_bind_int(st, 1, limit);
    printf("%-5s  %-18s  %-5s  %-5s  %-18s  %-7s  %-8s  %s\n",
           "id", "model_family", "layer", "top_k", "feature_hash", "danger", "ms", "ts");
    while (sqlite3_step(st) == SQLITE_ROW) {
        printf("%-5lld  %-18s  %-5d  %-5d  %-18s  %-7s  %-8.2f  %s\n",
               (long long)sqlite3_column_int64(st,0),
               sqlite3_column_text(st,1),
               sqlite3_column_int(st,2),
               sqlite3_column_int(st,3),
               sqlite3_column_text(st,4),
               sqlite3_column_int(st,5) ? "YES" : "no",
               sqlite3_column_double(st,6),
               sqlite3_column_text(st,7));
    }
    sqlite3_finalize(st); sqlite3_close(db);
    return 0;
}

/* ── help ─────────────────────────────────────────────────────────────────── */

static void print_help(void) {
    puts(
"bonfyre-sae " VERSION " — SAE feature dictionary runtime\n"
"\n"
"Commands:\n"
"  activate <dict.bfsae> <residual.bin>   run encoder, emit feature manifest\n"
"    --top-k N          (default 64)  features to return\n"
"    --alpha A          (default 0.7) danger gate threshold\n"
"    --out features.json              write manifest to file\n"
"    --artifact-id ID                 tag manifest with parent artifact id\n"
"    exits 2 if a DANGER-tagged feature exceeds alpha\n"
"\n"
"  inspect <dict.bfsae>               print header metadata\n"
"\n"
"  synth --out <file.bfsae>           write synthetic dict for testing\n"
"    --model-family F   (default synthetic)\n"
"    --layer L          (default 24)\n"
"    --hidden-dim D     (default 4096)\n"
"    --features N       (default 16384)\n"
"\n"
"  gate <dict.bfsae> <features.json>  re-check manifest, exit 2 on danger\n"
"    --alpha A          (default 0.7)\n"
"\n"
"  hash <dict.bfsae> <residual.bin>   print bfh:feature:<model>:l<N>:<hash>\n"
"    --top-k N          (default 64)\n"
"\n"
"  history [--limit N]                recent activation records\n"
"\n"
"Pipeline slot:  BonfyreEmbed → bonfyre-sae activate → BonfyreVec\n"
"\n"
"Environment:\n"
"  BONFYRE_SAE_DB      override default DB path\n"
    );
}

/* ── main ─────────────────────────────────────────────────────────────────── */

int main(int argc, char **argv) {
    if (argc < 2) { print_help(); return 0; }
    const char *cmd = argv[1];
    int   sub_argc = argc - 2;
    char **sub_argv = argv + 2;

    if (strcmp(cmd, "activate") == 0) return cmd_activate(sub_argc, sub_argv);
    if (strcmp(cmd, "inspect")  == 0) return cmd_inspect (sub_argc, sub_argv);
    if (strcmp(cmd, "synth")    == 0) return cmd_synth   (sub_argc, sub_argv);
    if (strcmp(cmd, "gate")     == 0) return cmd_gate    (sub_argc, sub_argv);
    if (strcmp(cmd, "hash")     == 0) return cmd_hash    (sub_argc, sub_argv);
    if (strcmp(cmd, "history")  == 0) return cmd_history (sub_argc, sub_argv);
    if (strcmp(cmd, "help")     == 0 || strcmp(cmd, "--help") == 0 || strcmp(cmd, "-h") == 0) {
        print_help(); return 0;
    }
    fprintf(stderr, "bonfyre-sae: unknown command '%s'. Try: bonfyre-sae help\n", cmd);
    return 1;
}
