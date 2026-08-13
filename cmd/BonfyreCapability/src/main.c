/*
 * bonfyre-capability - capability discovery and matching layer.
 *
 * A searchable registry of everything Bonfyre can do: which binary handles
 * which task, where it sits in the pipeline, what artifact it emits, and
 * what cost/latency profile it roughly carries.
 *
 * This is the native home for the useful part of the old generator project:
 * semantic capability keywords and intent matching. The stub recipe emission
 * was discarded; the ontology stayed.
 *
 * DB: ~/.local/share/bonfyre/capability.db
 *   If that path is a directory, the registry uses capability.sqlite3 inside it.
 *   Override with $BONFYRE_CAPABILITY_DB (file or directory path).
 *
 * Commands:
 *   bonfyre-capability status              - registry summary
 *   bonfyre-capability list                - all capabilities
 *   bonfyre-capability search <query>      - full-text capability search
 *   bonfyre-capability show <cap-id>       - full capability record
 *   bonfyre-capability match <description> - semantic match for a task
 *   bonfyre-capability index               - rebuild full-text index
 *   bonfyre-capability help                - this message
 */

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <sqlite3.h>
#include <bonfyre.h>

#define VERSION    "1.1.0"
#define DB_ENV     "BONFYRE_CAPABILITY_DB"
#define DB_SUBPATH "/.local/share/bonfyre/capability.db"
#define DB_FALLBACK_FILE "capability.sqlite3"

#define MAX_TOKENS 256
#define MAX_TOKLEN 48

typedef struct {
    const char *id;
    const char *name;
    const char *description;
    const char *binary;
    const char *command;
    const char *model_id;
    const char *hardware_tier;
    const char *latency_tier;
    const char *stage_class;
    const char *artifact_out;
    double cost_estimate;
    const char *keywords[20];
} BuiltinCapability;

typedef struct {
    char word[MAX_TOKLEN];
    int freq;
} MatchToken;

typedef struct {
    int builtin_idx;
    double score;
    int hits;
} MatchScore;

static void db_path_raw(char *buf, size_t len) {
    const char *e = getenv(DB_ENV);
    if (e) {
        snprintf(buf, len, "%s", e);
        return;
    }
    {
        const char *h = getenv("HOME");
        if (!h) h = "/tmp";
        snprintf(buf, len, "%s%s", h, DB_SUBPATH);
    }
}

static int path_is_dir(const char *path) {
    struct stat st;
    return path && stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

static void parent_dir(const char *path, char *buf, size_t len) {
    char *slash;
    if (!path || !path[0]) {
        snprintf(buf, len, ".");
        return;
    }

    snprintf(buf, len, "%s", path);
    slash = strrchr(buf, '/');
    if (!slash) {
        snprintf(buf, len, ".");
        return;
    }
    if (slash == buf) {
        slash[1] = '\0';
        return;
    }
    *slash = '\0';
}

static void resolve_db_path(char *buf, size_t len) {
    char raw[4096];

    db_path_raw(raw, sizeof(raw));
    if (path_is_dir(raw)) {
        snprintf(buf, len, "%s/%s", raw, DB_FALLBACK_FILE);
        return;
    }
    snprintf(buf, len, "%s", raw);
}

static const char *SCHEMA =
    "PRAGMA journal_mode=WAL;"
    "CREATE TABLE IF NOT EXISTS capabilities("
    "  id            TEXT PRIMARY KEY,"
    "  name          TEXT NOT NULL,"
    "  description   TEXT NOT NULL,"
    "  tags          TEXT,"
    "  binary        TEXT NOT NULL,"
    "  command       TEXT,"
    "  model_id      TEXT,"
    "  hardware_tier TEXT NOT NULL DEFAULT 'cpu',"
    "  cost_estimate REAL NOT NULL DEFAULT 0.0,"
    "  latency_tier  TEXT NOT NULL DEFAULT 'fast',"
    "  stage_class   TEXT,"
    "  artifact_out  TEXT,"
    "  keywords      TEXT,"
    "  source        TEXT NOT NULL DEFAULT 'manual',"
    "  updated       INTEGER NOT NULL"
    ");"
    "CREATE VIRTUAL TABLE IF NOT EXISTS cap_fts USING fts5("
    "  id,name,description,tags,binary,"
    "  content='capabilities',content_rowid='rowid'"
    ");";

static const BuiltinCapability BUILTINS[] = {
    {
        "ingest", "Artifact Ingest",
        "Ingest files or media into the Bonfyre artifact pipeline.",
        "bonfyre-ingest", "run", "-", "cpu", "instant", "ingest", "artifact",
        0.001,
        { "ingest", "intake", "load", "import", "read", "input", "file", "media", NULL }
    },
    {
        "mediaprep", "Media Preparation",
        "Prepare and normalize audio or video before downstream processing.",
        "bonfyre-mediaprep", "run", "-", "cpu", "fast", "transform", "media-assets",
        0.003,
        { "media", "prepare", "prep", "encode", "transcode", "convert", "resize", "clip", "audio", "video", NULL }
    },
    {
        "transcribe", "Speech to Text",
        "Transcribe audio into text artifacts for downstream pipeline stages.",
        "bonfyre-transcribe", "run", "whisper-large-v3", "gpu", "batch", "ingest", "transcript",
        0.006,
        { "transcribe", "transcription", "audio", "speech", "whisper", "stt", "voice", "recording", "spoken", "asr", NULL }
    },
    {
        "clean", "Transcript Cleanup",
        "Normalize and clean transcripts before structuring or summarization.",
        "bonfyre-clean", "run", "-", "cpu", "fast", "transform", "clean-transcript",
        0.001,
        { "cleanup", "clean", "fix", "normalize", "correct", "denoise", "filter", "repair", NULL }
    },
    {
        "paragraph", "Paragraph Structuring",
        "Recover paragraph boundaries and structure long-form text artifacts.",
        "bonfyre-paragraph", "run", "-", "cpu", "fast", "transform", "paragraphs",
        0.002,
        { "paragraph", "paragraphs", "structure", "boundary", "format", "layout", "prose", "chunk", NULL }
    },
    {
        "brief", "Brief Extraction",
        "Generate a compact brief, summary, or digest from a transcript or document.",
        "bonfyre-brief", "run", "llama-3-8b-instruct", "cpu", "fast", "transform", "brief",
        0.002,
        { "brief", "summarize", "summary", "tldr", "digest", "abstract", "overview", "outline", "condense", NULL }
    },
    {
        "proof", "Proof and Verification",
        "Score or verify generated artifacts against quality or factual standards.",
        "bonfyre-proof", "run", "-", "cpu", "fast", "score", "proof",
        0.003,
        { "proof", "verify", "verification", "check", "fact", "accuracy", "validate", "confirm", "review", NULL }
    },
    {
        "offer", "Offer Generation",
        "Produce offers, quotes, or proposal artifacts from structured work.",
        "bonfyre-offer", "run", "-", "cpu", "fast", "transform", "offer",
        0.002,
        { "offer", "price", "quote", "proposal", "bid", "estimate", "rfq", NULL }
    },
    {
        "narrate", "Narration",
        "Turn text artifacts into spoken narration or voice output.",
        "bonfyre-narrate", "run", "-", "cpu", "batch", "emit", "narration",
        0.004,
        { "narrate", "tts", "speak", "voice", "read", "readaloud", "speech", "synthesize", NULL }
    },
    {
        "pack", "Artifact Packing",
        "Bundle or compress outputs into a portable Bonfyre package.",
        "bonfyre-pack", "run", "-", "cpu", "fast", "emit", "bundle",
        0.001,
        { "pack", "package", "packaging", "bundle", "zip", "compress", "archive", "combine", "merge", NULL }
    },
    {
        "distribute", "Distribution",
        "Deliver packaged artifacts to downstream channels or destinations.",
        "bonfyre-distribute", "run", "-", "cpu", "batch", "emit", "distribution",
        0.010,
        { "distribute", "deliver", "delivery", "publish", "share", "send", "broadcast", "export", "post", NULL }
    },
    {
        "embed", "Embedding",
        "Embed artifacts into vector space for retrieval or similarity operations.",
        "bonfyre-embed", "run", "nomic-embed-text", "cpu", "fast", "transform", "embeddings",
        0.003,
        { "embed", "embedding", "vector", "semantic", "retrieve", "retrieval", "similarity", "index", NULL }
    },
    {
        "segment", "Segmentation",
        "Split artifacts into segments, speakers, chapters, or scenes.",
        "bonfyre-segment", "run", "pyannote-speaker-segmentation", "gpu", "batch", "transform", "segments",
        0.005,
        { "segment", "split", "chapter", "speaker", "diarize", "diarization", "turn", "scene", NULL }
    },
    {
        "control", "Control and Scoring",
        "Score artifacts, route decisions, and enforce quality gates.",
        "bonfyre-control", "score", "-", "cpu", "instant", "score", "score",
        0.000,
        { "score", "quality", "evaluate", "assess", "grade", "rank", "rate", "gate", "validate", "helsi", NULL }
    },
    {
        "queue", "Batch Queue",
        "Queue artifacts or jobs for deferred background execution.",
        "bonfyre-queue", "add", "-", "cpu", "instant", "infra", "queue-entry",
        0.000,
        { "queue", "enqueue", "schedule", "defer", "batch", "later", "pending", "async", NULL }
    },
    {
        "sync", "Sync and Watch",
        "Watch folders or synchronize manifests into the Bonfyre runtime.",
        "bonfyre-sync", "run", "-", "cpu", "instant", "infra", "sync-manifest",
        0.000,
        { "sync", "synchronize", "watch", "monitor", "folder", "directory", "replicate", NULL }
    },
    {
        "model", "Model Registry",
        "Inspect, route, pull, and manage model records for Bonfyre stages.",
        "bonfyre-model", "route", "-", "cpu", "instant", "registry", "model-ref",
        0.000,
        { "model", "models", "registry", "route", "routing", "pull", "weights", "family", NULL }
    },
    {
        "layer", "Layer Registry",
        "Manage transform layers, lattice-aligned layer specs, and upgraded set bindings.",
        "bonfyre-layer", "list", "-", "cpu", "instant", "registry", "layer-spec",
        0.000,
        { "layer", "layers", "lattice", "e8", "sli", "ising", "energy", "transform", "binding", NULL }
    },
    {
        "sli", "Spectral Lattice Inference",
        "Run SLI routing or inference over lattice-oriented Bonfyre artifacts.",
        "bonfyre-sli", "route", "-", "cpu", "fast", "optimization", "routed-family",
        0.000,
        { "sli", "spectral", "lattice", "e8", "ising", "energy", "inference", "route", "geometry", NULL }
    },
    {
        "fpq", "FPQ Compression",
        "Compress or score model artifacts with Bonfyre FPQ tooling.",
        "bonfyre-fpq", "run", "-", "cpu", "batch", "optimization", "fpq-pack",
        0.000,
        { "fpq", "fpqx", "quantize", "quantization", "compress", "packing", "lattice", "e8", "weights", NULL }
    },
    {
        "recipe", "Recipe Registry",
        "Validate, register, search, and inspect Bonfyre recipe definitions.",
        "bonfyre-recipe", "validate", "-", "cpu", "instant", "registry", "recipe-record",
        0.000,
        { "recipe", "recipes", "pipeline", "validate", "register", "show", "dag", NULL }
    },
    {
        "run", "Recipe Runner",
        "Execute Bonfyre recipes and emit run manifests for stage-level execution.",
        "bonfyre-run", "run", "-", "cpu", "batch", "runtime", "run-manifest",
        0.000,
        { "run", "execute", "runner", "pipeline", "workflow", "dag", "manifest", "stages", NULL }
    },
    {
        "capability", "Capability Discovery",
        "Discover Bonfyre capabilities and match natural-language tasks to native binaries.",
        "bonfyre-capability", "match", "-", "cpu", "instant", "registry", "capability-match",
        0.000,
        { "capability", "capabilities", "discover", "match", "semantic", "intent", "registry", NULL }
    }
};

static const int NBUILTINS = (int)(sizeof(BUILTINS) / sizeof(BUILTINS[0]));

static void rebuild_index(sqlite3 *db) {
    char *err = NULL;
    sqlite3_exec(db, "INSERT INTO cap_fts(cap_fts) VALUES('rebuild')", NULL, NULL, &err);
    if (err) sqlite3_free(err);
}

static void join_keywords(const BuiltinCapability *cap, char *buf, size_t buf_len) {
    int first = 1;
    size_t used = 0;
    buf[0] = '\0';
    for (int i = 0; cap->keywords[i]; i++) {
        int n = snprintf(buf + used, buf_len - used, "%s%s",
                         first ? "" : ",",
                         cap->keywords[i]);
        if (n < 0 || (size_t)n >= buf_len - used) break;
        used += (size_t)n;
        first = 0;
    }
}

static void seed_builtin(sqlite3 *db, const BuiltinCapability *cap, time_t now) {
    char tags[1024];
    sqlite3_stmt *st = NULL;

    join_keywords(cap, tags, sizeof(tags));

    sqlite3_prepare_v2(db,
        "INSERT OR IGNORE INTO capabilities("
        "id,name,description,tags,binary,command,model_id,hardware_tier,cost_estimate,latency_tier,"
        "stage_class,artifact_out,keywords,source,updated)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        -1, &st, NULL);
    sqlite3_bind_text(st,  1, cap->id,            -1, SQLITE_STATIC);
    sqlite3_bind_text(st,  2, cap->name,          -1, SQLITE_STATIC);
    sqlite3_bind_text(st,  3, cap->description,   -1, SQLITE_STATIC);
    sqlite3_bind_text(st,  4, tags,               -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st,  5, cap->binary,        -1, SQLITE_STATIC);
    sqlite3_bind_text(st,  6, cap->command,       -1, SQLITE_STATIC);
    sqlite3_bind_text(st,  7, cap->model_id,      -1, SQLITE_STATIC);
    sqlite3_bind_text(st,  8, cap->hardware_tier, -1, SQLITE_STATIC);
    sqlite3_bind_double(st, 9, cap->cost_estimate);
    sqlite3_bind_text(st, 10, cap->latency_tier,  -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 11, cap->stage_class,   -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 12, cap->artifact_out,  -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 13, tags,               -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 14, "builtin",          -1, SQLITE_STATIC);
    sqlite3_bind_int64(st,15, (sqlite3_int64)now);
    sqlite3_step(st);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,
        "UPDATE capabilities SET "
        "name=?,description=?,tags=?,binary=?,command=?,model_id=?,hardware_tier=?,cost_estimate=?,"
        "latency_tier=?,stage_class=?,artifact_out=?,keywords=?,source='builtin',updated=? "
        "WHERE id=? AND (source='builtin' OR source IS NULL)",
        -1, &st, NULL);
    sqlite3_bind_text(st,  1, cap->name,          -1, SQLITE_STATIC);
    sqlite3_bind_text(st,  2, cap->description,   -1, SQLITE_STATIC);
    sqlite3_bind_text(st,  3, tags,               -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st,  4, cap->binary,        -1, SQLITE_STATIC);
    sqlite3_bind_text(st,  5, cap->command,       -1, SQLITE_STATIC);
    sqlite3_bind_text(st,  6, cap->model_id,      -1, SQLITE_STATIC);
    sqlite3_bind_text(st,  7, cap->hardware_tier, -1, SQLITE_STATIC);
    sqlite3_bind_double(st, 8, cap->cost_estimate);
    sqlite3_bind_text(st,  9, cap->latency_tier,  -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 10, cap->stage_class,   -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 11, cap->artifact_out,  -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 12, tags,               -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(st,13, (sqlite3_int64)now);
    sqlite3_bind_text(st, 14, cap->id,            -1, SQLITE_STATIC);
    sqlite3_step(st);
    sqlite3_finalize(st);
}

static sqlite3 *open_db(void) {
    char path[4096];
    char dir[4096];
    sqlite3 *db = NULL;
    char *err = NULL;
    time_t now = time(NULL);

    resolve_db_path(path, sizeof(path));
    parent_dir(path, dir, sizeof(dir));
    if (bf_ensure_dir(dir) != 0) {
        fprintf(stderr, "bonfyre-capability: failed to create db directory: %s\n", dir);
        exit(1);
    }

    if (bf_sqlite3_open(path, &db) != SQLITE_OK) {
        fprintf(stderr, "bonfyre-capability: db open failed: %s\n", path);
        exit(1);
    }

    sqlite3_exec(db, SCHEMA, NULL, NULL, &err);
    if (err) {
        fprintf(stderr, "%s\n", err);
        sqlite3_free(err);
        exit(1);
    }

    sqlite3_exec(db, "ALTER TABLE capabilities ADD COLUMN stage_class   TEXT;", NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE capabilities ADD COLUMN artifact_out  TEXT;", NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE capabilities ADD COLUMN keywords      TEXT;", NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE capabilities ADD COLUMN source        TEXT NOT NULL DEFAULT 'manual';", NULL, NULL, NULL);

    for (int i = 0; i < NBUILTINS; i++) seed_builtin(db, &BUILTINS[i], now);
    rebuild_index(db);
    {
        char catalog_db[4096];
        bf_catalog_default_db_path(catalog_db, sizeof(catalog_db));
        bf_catalog_sync_default(catalog_db);
    }
    return db;
}

static int open_catalog(sqlite3 **out_db, char *path, size_t path_sz) {
    bf_catalog_default_db_path(path, path_sz);
    if (bf_catalog_sync_default(path) != 0) return 0;
    if (bf_sqlite3_open_ro(path, out_db) != SQLITE_OK) {
        sqlite3_close(*out_db);
        *out_db = NULL;
        return 0;
    }
    return 1;
}

static void sync_catalog(void) {
    char path[4096];
    bf_catalog_default_db_path(path, sizeof(path));
    bf_catalog_sync_default(path);
}

static int catalog_query_prepare(sqlite3 **out_db, sqlite3_stmt **out_st,
                                 const char *sql, char *path, size_t path_sz) {
    if (!open_catalog(out_db, path, path_sz)) return 0;
    if (sqlite3_prepare_v2(*out_db, sql, -1, out_st, NULL) != SQLITE_OK) {
        sqlite3_close(*out_db);
        *out_db = NULL;
        *out_st = NULL;
        return 0;
    }
    return 1;
}

static int print_catalog_related_workflow_steps(const char *cid) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char path[4096];
    char capability_node_id[160];
    int shown = 0;

    if (!open_catalog(&db, path, sizeof(path))) return 0;
    if (sqlite3_prepare_v2(db,
        "SELECT ws.external_id, ws.name, ws.category "
        "FROM catalog_edges e "
        "JOIN catalog_nodes ws ON ws.node_id = e.dst_node_id "
        "WHERE e.src_node_id = ? AND e.rel = 'seen_in_workflow_step' "
        "ORDER BY ws.external_id",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    snprintf(capability_node_id, sizeof(capability_node_id), "capability:%s", cid);
    sqlite3_bind_text(st, 1, capability_node_id, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *external_id = (const char *)sqlite3_column_text(st, 0);
        const char *name = (const char *)sqlite3_column_text(st, 1);
        const char *operator = (const char *)sqlite3_column_text(st, 2);
        const char *colon = external_id ? strchr(external_id, ':') : NULL;
        if (shown == 0) printf("workflow steps: ");
        else printf("                ");
        if (colon) {
            printf("%.*s %s  %s  (%s)\n",
                   (int)(colon - external_id),
                   external_id,
                   colon + 1,
                   name ? name : "(unnamed step)",
                   operator ? operator : "-");
        } else {
            printf("%s  %s\n", external_id ? external_id : "-", name ? name : "(unnamed step)");
        }
        shown++;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return shown;
}

static int print_catalog_related_run_stages(const char *cid) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char path[4096];
    char capability_node_id[160];
    int shown = 0;

    if (!open_catalog(&db, path, sizeof(path))) return 0;
    if (sqlite3_prepare_v2(db,
        "SELECT rs.external_id, rs.name, rs.category "
        "FROM catalog_edges e "
        "JOIN catalog_nodes rs ON rs.node_id = e.dst_node_id "
        "WHERE e.src_node_id = ? AND e.rel = 'seen_in_run_stage' "
        "ORDER BY rs.external_id DESC",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    snprintf(capability_node_id, sizeof(capability_node_id), "capability:%s", cid);
    sqlite3_bind_text(st, 1, capability_node_id, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *external_id = (const char *)sqlite3_column_text(st, 0);
        const char *name = (const char *)sqlite3_column_text(st, 1);
        const char *operator = (const char *)sqlite3_column_text(st, 2);
        if (shown == 0) printf("run stages    : ");
        else printf("                ");
        printf("%s  %s  (%s)\n",
               external_id ? external_id : "-",
               name ? name : "(unnamed stage)",
               operator ? operator : "-");
        shown++;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return shown;
}

static int print_catalog_related_models(const char *cid) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char path[4096];
    char capability_node_id[160];
    int shown = 0;

    if (!open_catalog(&db, path, sizeof(path))) return 0;
    if (sqlite3_prepare_v2(db,
        "SELECT m.external_id, m.name, m.category "
        "FROM catalog_edges e "
        "JOIN catalog_nodes m ON m.node_id = e.dst_node_id "
        "WHERE e.src_node_id = ? AND e.rel = 'uses_model' "
        "ORDER BY m.external_id",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    snprintf(capability_node_id, sizeof(capability_node_id), "capability:%s", cid);
    sqlite3_bind_text(st, 1, capability_node_id, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(st, 0);
        const char *name = (const char *)sqlite3_column_text(st, 1);
        const char *family = (const char *)sqlite3_column_text(st, 2);
        if (shown == 0) printf("models        : ");
        else printf("                ");
        printf("%s  %s", id ? id : "-", name ? name : "(unnamed model)");
        if (family && family[0] && strcmp(family, "model") != 0) printf(" [family %s]", family);
        printf("\n");
        shown++;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return shown;
}

static int print_catalog_related_families(const char *cid) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char path[4096];
    char capability_node_id[160];
    int shown = 0;

    if (!open_catalog(&db, path, sizeof(path))) return 0;
    if (sqlite3_prepare_v2(db,
        "SELECT f.external_id, f.category "
        "FROM catalog_edges e "
        "JOIN catalog_nodes f ON f.node_id = e.dst_node_id "
        "WHERE e.src_node_id = ? AND e.rel = 'related_family' "
        "ORDER BY f.external_id",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    snprintf(capability_node_id, sizeof(capability_node_id), "capability:%s", cid);
    sqlite3_bind_text(st, 1, capability_node_id, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(st, 0);
        const char *category = (const char *)sqlite3_column_text(st, 1);
        if (shown == 0) printf("families      : ");
        else printf("                ");
        printf("%s", id ? id : "-");
        if (category && category[0]) printf("  (%s)", category);
        printf("\n");
        shown++;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return shown;
}

static void cmd_status(sqlite3 *db) {
    sqlite3_stmt *st = NULL;
    int total = 0;
    sqlite3 *catalog = NULL;
    char catalog_path[4096];
    int catalog_total = 0;
    sync_catalog();

    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM capabilities", -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) total = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);

    printf("bonfyre-capability %s\n", VERSION);
    printf("  capabilities: %d\n", total);

    sqlite3_prepare_v2(db,
        "SELECT stage_class, COUNT(*) FROM capabilities "
        "GROUP BY stage_class ORDER BY stage_class",
        -1, &st, NULL);
    printf("  by stage:\n");
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *stage = (const char *)sqlite3_column_text(st, 0);
        printf("    %-12s %d\n", stage ? stage : "-", sqlite3_column_int(st, 1));
    }
    sqlite3_finalize(st);

    if (open_catalog(&catalog, catalog_path, sizeof(catalog_path))) {
        sqlite3_prepare_v2(catalog,
            "SELECT COUNT(*) FROM catalog_nodes WHERE kind='capability'",
            -1, &st, NULL);
        if (sqlite3_step(st) == SQLITE_ROW) catalog_total = sqlite3_column_int(st, 0);
        sqlite3_finalize(st);
        printf("  catalog: %s\n", catalog_path);
        printf("  catalog capabilities: %d\n", catalog_total);
        if (catalog_total != total) {
            printf("  mismatch: registry=%d catalog=%d\n", total, catalog_total);
        }
        sqlite3_close(catalog);
    }
}

static void cmd_list(sqlite3 *db) {
    sqlite3 *catalog = NULL;
    sqlite3_stmt *st = NULL;
    char path[4096];
    sync_catalog();
    if (catalog_query_prepare(&catalog, &st,
        "SELECT external_id,name,category,json_data FROM catalog_nodes "
        "WHERE kind='capability' ORDER BY category, name",
        path, sizeof(path))) {
        printf("%-14s  %-24s  %-22s  %-12s  %-18s  %-8s  %9s\n",
               "ID", "NAME", "BINARY", "STAGE", "ARTIFACT", "LATENCY", "COST");
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *id = (const char *)sqlite3_column_text(st, 0);
            const char *name = (const char *)sqlite3_column_text(st, 1);
            const char *stage = (const char *)sqlite3_column_text(st, 2);
            const char *json = (const char *)sqlite3_column_text(st, 3);
            char binary[128] = "", artifact[128] = "", latency[64] = "";
            double cost = 0.0;
            if (json) {
                bf_json_str(json, "binary", binary, sizeof(binary));
                bf_json_str(json, "artifact_out", artifact, sizeof(artifact));
                bf_json_str(json, "latency_tier", latency, sizeof(latency));
                bf_json_double(json, "cost_estimate", &cost);
            }
            printf("%-14s  %-24s  %-22s  %-12s  %-18s  %-8s  %9.4f\n",
                   id ? id : "",
                   name ? name : "",
                   binary[0] ? binary : "-",
                   stage ? stage : "-",
                   artifact[0] ? artifact : "-",
                   latency[0] ? latency : "-",
                   cost);
        }
        sqlite3_finalize(st);
        sqlite3_close(catalog);
        return;
    }

    st = NULL;
    sqlite3_prepare_v2(db,
        "SELECT id,name,binary,stage_class,artifact_out,latency_tier,cost_estimate "
        "FROM capabilities ORDER BY stage_class, name",
        -1, &st, NULL);

    printf("%-14s  %-24s  %-22s  %-12s  %-18s  %-8s  %9s\n",
           "ID", "NAME", "BINARY", "STAGE", "ARTIFACT", "LATENCY", "COST");
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *stage = (const char *)sqlite3_column_text(st, 3);
        const char *artifact = (const char *)sqlite3_column_text(st, 4);
        printf("%-14s  %-24s  %-22s  %-12s  %-18s  %-8s  %9.4f\n",
               (const char *)sqlite3_column_text(st, 0),
               (const char *)sqlite3_column_text(st, 1),
               (const char *)sqlite3_column_text(st, 2),
               stage ? stage : "-",
               artifact ? artifact : "-",
               (const char *)sqlite3_column_text(st, 5),
               sqlite3_column_double(st, 6));
    }
    sqlite3_finalize(st);
}

static void cmd_search(sqlite3 *db, const char *q) {
    sqlite3 *catalog = NULL;
    sqlite3_stmt *st = NULL;
    char path[4096];
    sync_catalog();
    if (catalog_query_prepare(&catalog, &st,
        "SELECT n.external_id,n.name,n.category,n.summary,n.json_data "
        "FROM catalog_fts f JOIN catalog_nodes n ON n.rowid = f.rowid "
        "WHERE f.catalog_fts MATCH ? AND n.kind='capability' LIMIT 10",
        path, sizeof(path))) {
        sqlite3_bind_text(st, 1, q, -1, SQLITE_STATIC);
        printf("search: '%s'\n", q);
        printf("%-14s  %-22s  %-12s  %s\n", "ID", "BINARY", "STAGE", "DESCRIPTION");
        int shown = 0;
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *json = (const char *)sqlite3_column_text(st, 4);
            char binary[128] = "";
            if (json) bf_json_str(json, "binary", binary, sizeof(binary));
            printf("%-14s  %-22s  %-12s  %.60s\n",
                   (const char *)sqlite3_column_text(st, 0),
                   binary[0] ? binary : "-",
                   (const char *)sqlite3_column_text(st, 2) ? (const char *)sqlite3_column_text(st, 2) : "-",
                   (const char *)sqlite3_column_text(st, 3) ? (const char *)sqlite3_column_text(st, 3) : "");
            shown++;
        }
        sqlite3_finalize(st);
        sqlite3_close(catalog);
        if (shown > 0) return;
    }
    if (st) sqlite3_finalize(st);
    if (catalog) sqlite3_close(catalog);

    st = NULL;
    if (sqlite3_prepare_v2(db,
        "SELECT c.id,c.name,c.binary,c.stage_class,c.description FROM cap_fts f "
        "JOIN capabilities c ON c.id=f.id WHERE cap_fts MATCH ? LIMIT 10",
        -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, q, -1, SQLITE_STATIC);
    } else {
        char pat[256];
        snprintf(pat, sizeof(pat), "%%%s%%", q);
        sqlite3_prepare_v2(db,
            "SELECT id,name,binary,stage_class,description FROM capabilities "
            "WHERE name LIKE ? OR description LIKE ? OR tags LIKE ? LIMIT 10",
            -1, &st, NULL);
        sqlite3_bind_text(st, 1, pat, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, pat, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 3, pat, -1, SQLITE_STATIC);
    }

    printf("search: '%s'\n", q);
    printf("%-14s  %-22s  %-12s  %s\n", "ID", "BINARY", "STAGE", "DESCRIPTION");
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *stage = (const char *)sqlite3_column_text(st, 3);
        printf("%-14s  %-22s  %-12s  %.60s\n",
               (const char *)sqlite3_column_text(st, 0),
               (const char *)sqlite3_column_text(st, 2),
               stage ? stage : "-",
               (const char *)sqlite3_column_text(st, 4));
    }
    sqlite3_finalize(st);
}

static void cmd_show(sqlite3 *db, const char *cid) {
    sqlite3 *catalog = NULL;
    sqlite3_stmt *st = NULL;
    char path[4096];
    sync_catalog();
    if (catalog_query_prepare(&catalog, &st,
        "SELECT external_id,name,summary,category,json_data "
        "FROM catalog_nodes WHERE kind='capability' AND external_id=?",
        path, sizeof(path))) {
        sqlite3_bind_text(st, 1, cid, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const char *json = (const char *)sqlite3_column_text(st, 4);
            char binary[128] = "", command[128] = "", model_id[128] = "", hardware[64] = "";
            char latency[64] = "", artifact[128] = "", keywords[512] = "", source[64] = "";
            double cost = 0.0;
            if (json) {
                bf_json_str(json, "binary", binary, sizeof(binary));
                bf_json_str(json, "command", command, sizeof(command));
                bf_json_str(json, "model_id", model_id, sizeof(model_id));
                bf_json_str(json, "hardware_tier", hardware, sizeof(hardware));
                bf_json_str(json, "latency_tier", latency, sizeof(latency));
                bf_json_str(json, "artifact_out", artifact, sizeof(artifact));
                bf_json_str(json, "keywords", keywords, sizeof(keywords));
                bf_json_str(json, "source", source, sizeof(source));
                bf_json_double(json, "cost_estimate", &cost);
            }
            printf(
                "id            : %s\n"
                "name          : %s\n"
                "description   : %s\n"
                "binary        : %s\n"
                "command       : %s\n"
                "stage         : %s\n"
                "artifact_out  : %s\n"
                "model         : %s\n"
                "hardware      : %s\n"
                "latency tier  : %s\n"
                "cost est      : $%.4f/call\n"
                "keywords      : %s\n"
                "source        : %s\n",
                (const char *)sqlite3_column_text(st, 0),
                (const char *)sqlite3_column_text(st, 1),
                (const char *)sqlite3_column_text(st, 2),
                binary[0] ? binary : "-",
                command[0] ? command : "-",
                (const char *)sqlite3_column_text(st, 3) ? (const char *)sqlite3_column_text(st, 3) : "-",
                artifact[0] ? artifact : "-",
                model_id[0] ? model_id : "-",
                hardware[0] ? hardware : "-",
                latency[0] ? latency : "-",
                cost,
                keywords[0] ? keywords : "-",
                source[0] ? source : "manual");
            sqlite3_finalize(st);
            sqlite3_close(catalog);
            print_catalog_related_models(cid);
            print_catalog_related_workflow_steps(cid);
            print_catalog_related_run_stages(cid);
            print_catalog_related_families(cid);
            return;
        }
        sqlite3_finalize(st);
        sqlite3_close(catalog);
    }

    st = NULL;
    sqlite3_prepare_v2(db,
        "SELECT id,name,description,tags,binary,command,model_id,hardware_tier,cost_estimate,"
        "latency_tier,stage_class,artifact_out,keywords,source "
        "FROM capabilities WHERE id=?",
        -1, &st, NULL);
    sqlite3_bind_text(st, 1, cid, -1, SQLITE_STATIC);
    if (sqlite3_step(st) != SQLITE_ROW) {
        fprintf(stderr, "capability not found: %s\n", cid);
        sqlite3_finalize(st);
        return;
    }

    printf(
        "id            : %s\n"
        "name          : %s\n"
        "description   : %s\n"
        "binary        : %s\n"
        "command       : %s\n"
        "stage         : %s\n"
        "artifact_out  : %s\n"
        "model         : %s\n"
        "hardware      : %s\n"
        "latency tier  : %s\n"
        "cost est      : $%.4f/call\n"
        "keywords      : %s\n"
        "source        : %s\n"
        "tags          : %s\n",
        (const char *)sqlite3_column_text(st, 0),
        (const char *)sqlite3_column_text(st, 1),
        (const char *)sqlite3_column_text(st, 2),
        (const char *)sqlite3_column_text(st, 4),
        (const char *)sqlite3_column_text(st, 5),
        sqlite3_column_text(st, 10) ? (const char *)sqlite3_column_text(st, 10) : "-",
        sqlite3_column_text(st, 11) ? (const char *)sqlite3_column_text(st, 11) : "-",
        (const char *)sqlite3_column_text(st, 6),
        (const char *)sqlite3_column_text(st, 7),
        (const char *)sqlite3_column_text(st, 9),
        sqlite3_column_double(st, 8),
        sqlite3_column_text(st, 12) ? (const char *)sqlite3_column_text(st, 12) : "-",
        sqlite3_column_text(st, 13) ? (const char *)sqlite3_column_text(st, 13) : "manual",
        (const char *)sqlite3_column_text(st, 3));
    sqlite3_finalize(st);

    if (!print_catalog_related_models(cid)) {
        /* no-op */
    }
    print_catalog_related_workflow_steps(cid);
    print_catalog_related_run_stages(cid);
    print_catalog_related_families(cid);
}

static void token_add(MatchToken *tokens, int *ntokens, const char *word) {
    if (!word || !word[0] || *ntokens >= MAX_TOKENS) return;
    for (int i = 0; i < *ntokens; i++) {
        if (strcmp(tokens[i].word, word) == 0) {
            tokens[i].freq++;
            return;
        }
    }
    snprintf(tokens[*ntokens].word, sizeof(tokens[*ntokens].word), "%s", word);
    tokens[*ntokens].freq = 1;
    (*ntokens)++;
}

static int tokenise_text(const char *text, MatchToken *tokens) {
    char buf[MAX_TOKLEN];
    int ntokens = 0;
    int wi = 0;

    for (const char *p = text; ; p++) {
        unsigned char c = (unsigned char)*p;
        if (isalnum(c)) {
            if (wi + 1 < MAX_TOKLEN)
                buf[wi++] = (char)tolower(c);
        } else {
            if (wi > 0) {
                buf[wi] = '\0';
                token_add(tokens, &ntokens, buf);
                wi = 0;
            }
            if (c == '\0') break;
        }
    }
    return ntokens;
}

static int token_freq(const MatchToken *tokens, int ntokens, const char *word) {
    for (int i = 0; i < ntokens; i++) {
        if (strcmp(tokens[i].word, word) == 0) return tokens[i].freq;
    }
    return 0;
}

static double score_builtin(const BuiltinCapability *cap, const MatchToken *tokens,
                            int ntokens, int *hits_out) {
    double score = 0.0;
    int hits = 0;

    if (hits_out) *hits_out = 0;
    if (ntokens <= 0) return 0.0;

    for (int i = 0; cap->keywords[i]; i++) {
        int freq = token_freq(tokens, ntokens, cap->keywords[i]);
        if (freq > 0) {
            hits++;
            score += (double)freq / (double)ntokens;
        }
    }

    if (hits_out) *hits_out = hits;
    return score;
}

static int match_score_cmp(const void *a, const void *b) {
    const MatchScore *ma = (const MatchScore *)a;
    const MatchScore *mb = (const MatchScore *)b;
    if (mb->score > ma->score) return 1;
    if (mb->score < ma->score) return -1;
    if (mb->hits != ma->hits) return mb->hits - ma->hits;
    return strcmp(BUILTINS[ma->builtin_idx].id, BUILTINS[mb->builtin_idx].id);
}

static void cmd_match_fallback(sqlite3 *db, const char *desc) {
    char copy[2048];
    char seen[2048] = "";
    char *tok = NULL;

    printf("match: '%s'\n\n", desc);
    printf("%-14s  %-22s  %s\n", "ID", "BINARY", "DESCRIPTION");

    snprintf(copy, sizeof(copy), "%s", desc);
    tok = strtok(copy, " \t,.");
    while (tok) {
        if (strlen(tok) > 3) {
            char term[256];
            sqlite3_stmt *st = NULL;
            snprintf(term, sizeof(term), "%%%s%%", tok);
            sqlite3_prepare_v2(db,
                "SELECT id,name,binary,cost_estimate FROM capabilities "
                "WHERE (name LIKE ? OR tags LIKE ? OR description LIKE ?) LIMIT 3",
                -1, &st, NULL);
            sqlite3_bind_text(st, 1, term, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, term, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 3, term, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW) {
                const char *id = (const char *)sqlite3_column_text(st, 0);
                if (!id || strstr(seen, id)) continue;
                printf("%-14s  %-22s  $%.4f\n",
                       id,
                       (const char *)sqlite3_column_text(st, 2),
                       sqlite3_column_double(st, 3));
                strncat(seen, "|", sizeof(seen) - strlen(seen) - 1);
                strncat(seen, id, sizeof(seen) - strlen(seen) - 1);
            }
            sqlite3_finalize(st);
        }
        tok = strtok(NULL, " \t,.");
    }
}

static void cmd_match(sqlite3 *db, const char *desc) {
    MatchToken tokens[MAX_TOKENS];
    MatchScore scores[NBUILTINS];
    int ntokens = tokenise_text(desc, tokens);
    int nscores = 0;

    if (ntokens <= 0) {
        fprintf(stderr, "bonfyre-capability: no recognizable words in description\n");
        return;
    }

    for (int i = 0; i < NBUILTINS; i++) {
        int hits = 0;
        double score = score_builtin(&BUILTINS[i], tokens, ntokens, &hits);
        if (score <= 0.0 || hits <= 0) continue;
        scores[nscores].builtin_idx = i;
        scores[nscores].score = score;
        scores[nscores].hits = hits;
        nscores++;
    }

    if (nscores == 0) {
        cmd_match_fallback(db, desc);
        return;
    }

    qsort(scores, (size_t)nscores, sizeof(scores[0]), match_score_cmp);

    printf("match: '%s'\n\n", desc);
    printf("%-7s  %-14s  %-22s  %-12s  %-18s  %-8s  %9s\n",
           "SCORE", "ID", "BINARY", "STAGE", "ARTIFACT", "LATENCY", "COST");
    for (int i = 0; i < nscores && i < 8; i++) {
        const BuiltinCapability *cap = &BUILTINS[scores[i].builtin_idx];
        printf("%0.3f   %-14s  %-22s  %-12s  %-18s  %-8s  %9.4f\n",
               scores[i].score,
               cap->id,
               cap->binary,
               cap->stage_class,
               cap->artifact_out,
               cap->latency_tier,
               cap->cost_estimate);
    }
}

static void cmd_index(sqlite3 *db) {
    rebuild_index(db);
    sync_catalog();
    printf("capability index rebuilt\n");
}

static void cmd_help(void) {
    printf(
"bonfyre-capability %s - capability discovery and matching\n\n"
"USAGE\n"
"  bonfyre-capability <command> [args]\n\n"
"COMMANDS\n"
"  status              registry summary\n"
"  list                all capabilities\n"
"  search <query>      full-text search\n"
"  show <cap-id>       full capability record\n"
"  match <description> semantic match for a natural-language task\n"
"  index               rebuild full-text search index\n"
"  help                this message\n\n"
"CAPABILITY FIELDS\n"
"  id | name | description | binary | command | stage_class | artifact_out\n"
"  model_id | hardware_tier | latency_tier | cost_estimate | keywords | source\n\n"
"NOTES\n"
"  match uses the native semantic registry salvaged from the old generator\n"
"  project, but it stays inside Bonfyre's capability layer instead of emitting\n"
"  stub recipes.\n\n"
"ENVIRONMENT\n"
"  BONFYRE_CAPABILITY_DB   override DB path (file or directory)\n",
    VERSION);
}

int main(int argc, char **argv) {
    sqlite3 *db = NULL;
    int rc = 0;
    const char *cmd = NULL;

    if (argc < 2 || strcmp(argv[1], "help") == 0 || strcmp(argv[1], "--help") == 0) {
        cmd_help();
        return 0;
    }

    db = open_db();
    cmd = argv[1];

    if (strcmp(cmd, "status") == 0) cmd_status(db);
    else if (strcmp(cmd, "list") == 0) cmd_list(db);
    else if (strcmp(cmd, "search") == 0) {
        if (argc < 3) {
            fprintf(stderr, "usage: bonfyre-capability search <query>\n");
            rc = 1;
        } else cmd_search(db, argv[2]);
    } else if (strcmp(cmd, "show") == 0) {
        if (argc < 3) {
            fprintf(stderr, "usage: bonfyre-capability show <cap-id>\n");
            rc = 1;
        } else cmd_show(db, argv[2]);
    } else if (strcmp(cmd, "match") == 0) {
        if (argc < 3) {
            fprintf(stderr, "usage: bonfyre-capability match <description>\n");
            rc = 1;
        } else cmd_match(db, argv[2]);
    } else if (strcmp(cmd, "index") == 0) cmd_index(db);
    else {
        fprintf(stderr, "bonfyre-capability: unknown command: %s\n", cmd);
        rc = 1;
    }

    sqlite3_close(db);
    return rc;
}
