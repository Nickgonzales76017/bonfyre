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
 *   bonfyre-capability repair-admit <replay.json> --claim TEXT
 *                                          - admit a deterministically replayed repair
 *   bonfyre-capability repairs             - governed repair records (JSON)
 *   bonfyre-capability repair-show <id>    - one repair record (JSON)
 *   bonfyre-capability help                - this message
 */

#include <ctype.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <ctype.h>
#include "bf_lex.h"
#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif
#include <unistd.h>
#include <limits.h>
#include <dirent.h>
#include <sys/stat.h>
#include <time.h>
#include <sqlite3.h>
#include <bonfyre.h>
#include "bf_control.h"
#include "bf_jv.h"
#include "bf_jsonw.h"

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


/* A cost below zero means nobody has measured one. It is never shown as a
 * price, and zero is not used for it because zero is a price. */
static const char *cost_label(double cost) {
    static char text[32];
    if (cost < 0) return "unmeasured";
    snprintf(text, sizeof(text), "$%.4f/call", cost);
    return text;
}
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
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "ingest", "intake", "load", "import", "read", "input", "file", "media", NULL }
    },
    {
        "mediaprep", "Media Preparation",
        "Prepare and normalize audio or video before downstream processing.",
        "bonfyre-media-prep", "run", "-", "cpu", "fast", "transform", "media-assets",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "media", "prepare", "prep", "encode", "transcode", "convert", "resize", "clip", "audio", "video", NULL }
    },
    {
        "transcribe", "Speech to Text",
        "Transcribe audio into text artifacts for downstream pipeline stages.",
        "bonfyre-transcribe", "run", "-", "cpu", "batch", "ingest", "transcript",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "transcribe", "transcription", "audio", "speech", "whisper", "stt", "voice", "recording", "spoken", "asr", NULL }
    },
    {
        "clean", "Transcript Cleanup",
        "Normalize and clean transcripts before structuring or summarization.",
        "bonfyre-transcript-clean", "run", "-", "cpu", "fast", "transform", "clean-transcript",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "cleanup", "clean", "fix", "normalize", "correct", "denoise", "filter", "repair", NULL }
    },
    {
        "paragraph", "Paragraph Structuring",
        "Recover paragraph boundaries and structure long-form text artifacts.",
        "bonfyre-paragraph", "run", "-", "cpu", "fast", "transform", "paragraphs",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "paragraph", "paragraphs", "structure", "boundary", "format", "layout", "prose", "chunk", NULL }
    },
    {
        "brief", "Brief Extraction",
        "Generate a compact brief, summary, or digest from a transcript or document.",
        "bonfyre-brief", "run", "-", "cpu", "fast", "transform", "brief",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "brief", "summarize", "summary", "tldr", "digest", "abstract", "overview", "outline", "condense", NULL }
    },
    {
        "proof", "Proof and Verification",
        "Score or verify generated artifacts against quality or factual standards.",
        "bonfyre-proof", "run", "-", "cpu", "fast", "score", "proof",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "proof", "verify", "verification", "check", "fact", "accuracy", "validate", "confirm", "review", NULL }
    },
    {
        "offer", "Offer Generation",
        "Produce offers, quotes, or proposal artifacts from structured work.",
        "bonfyre-offer", "run", "-", "cpu", "fast", "transform", "offer",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "offer", "price", "quote", "proposal", "bid", "estimate", "rfq", NULL }
    },
    {
        "narrate", "Narration",
        "Turn text artifacts into spoken narration or voice output.",
        "bonfyre-narrate", "run", "-", "cpu", "batch", "emit", "narration",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "narrate", "tts", "speak", "voice", "read", "readaloud", "speech", "synthesize", NULL }
    },
    {
        "pack", "Artifact Packing",
        "Bundle or compress outputs into a portable Bonfyre package.",
        "bonfyre-pack", "run", "-", "cpu", "fast", "emit", "bundle",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "pack", "package", "packaging", "bundle", "zip", "compress", "archive", "combine", "merge", NULL }
    },
    {
        "distribute", "Distribution",
        "Deliver packaged artifacts to downstream channels or destinations.",
        "bonfyre-distribute", "run", "-", "cpu", "batch", "emit", "distribution",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "distribute", "deliver", "delivery", "publish", "share", "send", "broadcast", "export", "post", NULL }
    },
    {
        "embed", "Embedding",
        "Embed artifacts into vector space for retrieval or similarity operations.",
        "bonfyre-embed", "run", "nomic-embed-text", "cpu", "fast", "transform", "embeddings",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "embed", "embedding", "vector", "semantic", "retrieve", "retrieval", "similarity", "index", NULL }
    },
    {
        "segment", "Segmentation",
        "Split artifacts into segments, speakers, chapters, or scenes.",
        "bonfyre-segment", "run", "-", "cpu", "batch", "transform", "segments",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "segment", "split", "chapter", "speaker", "diarize", "diarization", "turn", "scene", NULL }
    },
    {
        "control", "Control and Scoring",
        "Score artifacts, route decisions, and enforce quality gates.",
        "bonfyre-control", "score", "-", "cpu", "instant", "score", "score",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "score", "quality", "evaluate", "assess", "grade", "rank", "rate", "gate", "validate", "helsi", NULL }
    },
    {
        "queue", "Batch Queue",
        "Queue artifacts or jobs for deferred background execution.",
        "bonfyre-queue", "add", "-", "cpu", "instant", "infra", "queue-entry",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "queue", "enqueue", "schedule", "defer", "batch", "later", "pending", "async", NULL }
    },
    {
        "sync", "Sync and Watch",
        "Watch folders or synchronize manifests into the Bonfyre runtime.",
        "bonfyre-sync", "run", "-", "cpu", "instant", "infra", "sync-manifest",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "sync", "synchronize", "watch", "monitor", "folder", "directory", "replicate", NULL }
    },
    {
        "model", "Model Registry",
        "Inspect, route, pull, and manage model records for Bonfyre stages.",
        "bonfyre-model", "route", "-", "cpu", "instant", "registry", "model-ref",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "model", "models", "registry", "route", "routing", "pull", "weights", "family", NULL }
    },
    {
        "layer", "Layer Registry",
        "Manage transform layers, lattice-aligned layer specs, and upgraded set bindings.",
        "bonfyre-layer", "list", "-", "cpu", "instant", "registry", "layer-spec",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "layer", "layers", "lattice", "e8", "sli", "ising", "energy", "transform", "binding", NULL }
    },
    {
        "sli", "Spectral Lattice Inference",
        "Run SLI routing or inference over lattice-oriented Bonfyre artifacts.",
        "bonfyre-sli", "route", "-", "cpu", "fast", "optimization", "routed-family",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "sli", "spectral", "lattice", "e8", "ising", "energy", "inference", "route", "geometry", NULL }
    },
    {
        "fpq", "FPQ Compression",
        "Compress or score model artifacts with Bonfyre FPQ tooling.",
        "bonfyre-fpq", "run", "-", "cpu", "batch", "optimization", "fpq-pack",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "fpq", "fpqx", "quantize", "quantization", "compress", "packing", "lattice", "e8", "weights", NULL }
    },
    {
        "recipe", "Recipe Registry",
        "Validate, register, search, and inspect Bonfyre recipe definitions through the native Runtime owner.",
        "bonfyre-runtime", "recipe", "-", "cpu", "instant", "registry", "recipe-record",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "recipe", "recipes", "pipeline", "validate", "register", "show", "dag", NULL }
    },
    {
        "run", "Recipe Runner",
        "Execute Bonfyre recipes through the native Runtime owner and emit run manifests for stage-level execution.",
        "bonfyre-runtime", "run-recipe", "-", "cpu", "batch", "runtime", "run-manifest",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
        { "run", "execute", "runner", "pipeline", "workflow", "dag", "manifest", "stages", NULL }
    },
    {
        "capability", "Capability Discovery",
        "Discover Bonfyre capabilities and match natural-language tasks to native binaries.",
        "bonfyre-capability", "match", "-", "cpu", "instant", "registry", "capability-match",
        -1.0, /* unmeasured: nothing on this estate has timed or priced a call */
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

/* The capability registry is an executable advertisement, not a history of
 * names which once existed.  Keep migrations deliberately narrow: a locally
 * authored record is never rewritten, but the four known stale stock rows
 * cannot continue to send a runnable closure to an absent binary/provider.
 *
 * Translation remains a semantic desire elsewhere in the estate.  It is
 * intentionally absent from this executable registry until a current model
 * passport plus ResourceAdmission/provider invocation contract exists. */
static void migrate_stale_realizations(sqlite3 *db, time_t now) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db,
        "UPDATE capabilities SET name='Audio Ingest',"
        "description='Normalize audio through the native BonfyreIngest owner.',"
        "binary='bonfyre-ingest',command='ingest-audio',model_id='-',"
        "hardware_tier='cpu',latency_tier='instant',stage_class='ingest',"
        "artifact_out='artifact',keywords='audio,ingest,intake,normalize,media',"
        "source='owner-contract-migration',updated=? "
        "WHERE id='audio-ingest' AND binary='bonfyre-intake' "
        "AND (source='manual' OR source='builtin' OR source IS NULL)",
        -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(st, 1, (sqlite3_int64)now);
        sqlite3_step(st);
        sqlite3_finalize(st);
    }
    if (sqlite3_prepare_v2(db,
        "DELETE FROM capabilities WHERE id='translate-text' "
        "AND binary='bonfyre-translate' AND model_id='gpt-4o' "
        "AND (source='manual' OR source='builtin' OR source IS NULL)",
        -1, &st, NULL) == SQLITE_OK) {
        sqlite3_step(st);
        sqlite3_finalize(st);
    }
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

    /* Fast tier, by decision: an index rebuilt by `index`; losing the last commits to a power cut costs a re-run. */
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
    migrate_stale_realizations(db, now);
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
            {
                /* Negative cost means unmeasured, not a credit and not free. */
                char cost_text[16];
                if (cost < 0) snprintf(cost_text, sizeof(cost_text), "%9s", "unmeasured");
                else snprintf(cost_text, sizeof(cost_text), "%9.4f", cost);
                printf("%-14s  %-24s  %-22s  %-12s  %-18s  %-8s  %s\n",
                       id ? id : "",
                       name ? name : "",
                       binary[0] ? binary : "-",
                       stage ? stage : "-",
                       artifact[0] ? artifact : "-",
                       latency[0] ? latency : "-",
                       cost_text);
            }
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
        {
            /* A discovered organ carries cost -1, meaning nobody has measured
             * it. Printing that as a number invites reading it as a price, and
             * printing it as 0 would say the thing is free. It is neither. */
            double cost = sqlite3_column_double(st, 6);
            char cost_text[16];
            if (cost < 0) snprintf(cost_text, sizeof(cost_text), "%9s", "unmeasured");
            else snprintf(cost_text, sizeof(cost_text), "%9.4f", cost);
            printf("%-14s  %-24s  %-22s  %-12s  %-18s  %-8s  %s\n",
                   (const char *)sqlite3_column_text(st, 0),
                   (const char *)sqlite3_column_text(st, 1),
                   (const char *)sqlite3_column_text(st, 2),
                   stage ? stage : "-",
                   artifact ? artifact : "-",
                   (const char *)sqlite3_column_text(st, 5),
                   cost_text);
        }
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

static int cmd_show(sqlite3 *db, const char *cid) {
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
                "cost est      : %s\n"
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
                cost_label(cost),
                keywords[0] ? keywords : "-",
                source[0] ? source : "manual");
            sqlite3_finalize(st);
            sqlite3_close(catalog);
            print_catalog_related_models(cid);
            print_catalog_related_workflow_steps(cid);
            print_catalog_related_run_stages(cid);
            print_catalog_related_families(cid);
            return 1;
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
        return 0;
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
        "cost est      : %s\n"
        "keywords      : %s\n"
        "source        : %s\n"
        "tags          : %s\n",
        (const char *)sqlite3_column_text(st, 0),
        (const char *)sqlite3_column_text(st, 1),
        (const char *)sqlite3_column_text(st, 2),
        (const char *)sqlite3_column_text(st, 4),
        (sqlite3_column_text(st, 5) && sqlite3_column_text(st, 5)[0])
            ? (const char *)sqlite3_column_text(st, 5) : "-",
        sqlite3_column_text(st, 10) ? (const char *)sqlite3_column_text(st, 10) : "-",
        sqlite3_column_text(st, 11) ? (const char *)sqlite3_column_text(st, 11) : "-",
        (const char *)sqlite3_column_text(st, 6),
        (const char *)sqlite3_column_text(st, 7),
        (const char *)sqlite3_column_text(st, 9),
        cost_label(sqlite3_column_double(st, 8)),
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
    return 1;
}

static void token_add(MatchToken *tokens, int *ntokens, const char *word) {
    if (!word || !word[0] || *ntokens >= MAX_TOKENS) return;
    /* Ignore conversational glue when matching capabilities to objectives. */
    static const char *const glue[] = {
        "and","the","for","with","from","into","that","this","these",
        "those","which","when","where","what","have","will","your","their",
        "over","about","then","also","using","there",NULL
    };
    for (int i=0;glue[i];i++) if (!strcmp(word,glue[i])) return;
    /* Only source terms and task intent should contribute to ranking. */
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

/* ----------------------------------------------------- semantic matching --
 *
 * Word overlap cannot reach a capability whose description shares no
 * vocabulary with the question. The estate holds a learned embedding organ and
 * a cached model, so the honest fix is a real metric rather than more clever
 * string matching.
 *
 * Two refusals shape this. The embedding organ refuses to hand back a
 * hashed-token vector when a learned one was asked for, because hashed-token
 * geometry is not a learned metric and distances are not comparable across the
 * two -- so this never falls back to the hash backend, it falls back to word
 * overlap and says so. And a ranking must say which metric produced it: a
 * caller told only "0.71" cannot tell a learned distance from a word count.
 *
 * Vectors live in the capabilities table beside the row they describe. A
 * separate vector store would be a second thing to keep in step with the first,
 * which is the drift this registry already suffered once between list and
 * match. */
#define EMBED_DIMS 768

static const char *embed_model(void) {
    const char *m = getenv("BONFYRE_EMBED_MODEL");
    return m && *m ? m : "nomic-embed-text-onnx";
}

/* Run the embedding organ over one text. Returns dims on success, 0 when no
 * learned representation could be produced -- which is a fallback signal and
 * never an error to paper over. */
static int embed_text(const char *text, float *out, int max_dims) {
    char dir[] = "/tmp/bonfyre-capembed-XXXXXX";
    char in_path[PATH_MAX], out_path[PATH_MAX];
    char *json = NULL;
    FILE *fh;
    pid_t pid;
    int status = 0, dims = 0;

    if (!text || !*text || !mkdtemp(dir)) return 0;
    snprintf(in_path, sizeof(in_path), "%s/q.txt", dir);
    snprintf(out_path, sizeof(out_path), "%s/q.json", dir);
    if (!(fh = fopen(in_path, "w"))) goto done;
    fputs(text, fh);
    fclose(fh);

    pid = fork();
    if (pid < 0) goto done;
    if (pid == 0) {
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) { dup2(devnull, STDOUT_FILENO); dup2(devnull, STDERR_FILENO); close(devnull); }
        execlp("bonfyre-embed", "bonfyre-embed", "--text", in_path, "--out", out_path,
               "--backend", "onnx", "--model", embed_model(), (char *)NULL);
        _exit(127);
    }
    waitpid(pid, &status, 0);
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) goto done;

    {
        long size;
        if (!(fh = fopen(out_path, "rb"))) goto done;
        fseek(fh, 0, SEEK_END); size = ftell(fh); rewind(fh);
        if (size > 0 && (json = malloc((size_t)size + 1))) {
            if (fread(json, 1, (size_t)size, fh) == (size_t)size) {
                const char *p = strstr(json, "\"vector\"");
                json[size] = '\0';
                if (p && (p = strchr(p, '['))) {
                    p++;
                    while (*p && dims < max_dims) {
                        char *end = NULL;
                        double v = strtod(p, &end);
                        if (end == p) break;
                        out[dims++] = (float)v;
                        p = end;
                        while (*p == ' ' || *p == '\n' || *p == '\t' || *p == ',') p++;
                        if (*p == ']') break;
                    }
                }
            }
        }
        fclose(fh);
    }
done:
    free(json);
    unlink(in_path);
    unlink(out_path);
    { char meta[PATH_MAX]; snprintf(meta, sizeof(meta), "%s.json", out_path); unlink(meta); }
    rmdir(dir);
    return dims;
}

static double cosine(const float *a, const float *b, int n) {
    double dot = 0.0, na = 0.0, nb = 0.0;
    for (int i = 0; i < n; i++) { dot += (double)a[i] * b[i]; na += (double)a[i] * a[i]; nb += (double)b[i] * b[i]; }
    if (na <= 0.0 || nb <= 0.0) return 0.0;
    return dot / (sqrt(na) * sqrt(nb));
}

static void cmd_semantic_index(sqlite3 *db) {
    sqlite3_stmt *st = NULL;
    int done = 0, skipped = 0;

    sqlite3_exec(db, "ALTER TABLE capabilities ADD COLUMN vector BLOB;", NULL, NULL, NULL);
    if (sqlite3_prepare_v2(db,
            "SELECT id,name,description,keywords FROM capabilities WHERE vector IS NULL",
            -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "bonfyre-capability: cannot read capabilities\n");
        return;
    }
    while (sqlite3_step(st) == SQLITE_ROW) {
        char text[2048];
        float vec[EMBED_DIMS];
        int dims;
        const unsigned char *id = sqlite3_column_text(st, 0);
        snprintf(text, sizeof(text), "%s. %s. %s",
                 sqlite3_column_text(st, 1) ? (const char *)sqlite3_column_text(st, 1) : "",
                 sqlite3_column_text(st, 2) ? (const char *)sqlite3_column_text(st, 2) : "",
                 sqlite3_column_text(st, 3) ? (const char *)sqlite3_column_text(st, 3) : "");
        dims = embed_text(text, vec, EMBED_DIMS);
        if (dims != EMBED_DIMS) { skipped++; continue; }
        {
            sqlite3_stmt *up = NULL;
            if (sqlite3_prepare_v2(db, "UPDATE capabilities SET vector=? WHERE id=?", -1, &up, NULL) == SQLITE_OK) {
                sqlite3_bind_blob(up, 1, vec, (int)(sizeof(float) * EMBED_DIMS), SQLITE_TRANSIENT);
                sqlite3_bind_text(up, 2, (const char *)id, -1, SQLITE_TRANSIENT);
                sqlite3_step(up);
                sqlite3_finalize(up);
                done++;
            }
        }
    }
    sqlite3_finalize(st);
    printf("semantic index: %d embedded, %d left to word overlap\n", done, skipped);
    if (skipped && !done)
        printf("  no learned representation was available; matching stays lexical\n");
}

/* score_builtin and match_score_cmp were removed with the compiled-table match.
 * They scored BUILTINS only, which is what made a discovered capability
 * listable and unmatchable; keeping them would leave a second scorer that
 * disagrees with the one the registry uses. */

static void cmd_match_fallback(sqlite3 *db, const char *desc) {
    char copy[2048];
    char seen[2048] = "";
    char *tok = NULL;

    /* The fallback is a third ranking path and must name its metric like the
     * other two. A caller cannot be shown an ordering with no way to tell what
     * produced it. */
    printf("match: '%s'  [metric: word overlap, full-text fallback]\n\n", desc);
    printf("%-14s  %-22s  %s\n", "ID", "BINARY", "COST PER CALL");

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
                printf("%-14s  %-22s  %s\n",
                       id,
                       (const char *)sqlite3_column_text(st, 2),
                       cost_label(sqlite3_column_double(st, 3)));
                strncat(seen, "|", sizeof(seen) - strlen(seen) - 1);
                strncat(seen, id, sizeof(seen) - strlen(seen) - 1);
            }
            sqlite3_finalize(st);
        }
        tok = strtok(NULL, " \t,.");
    }
}

/* Score one capability's text against the query. A curated row carries hand
 * written keywords; a discovered one carries its atlas description, family,
 * domain and the artifact it publishes. Both are words, and both are matched
 * the same way. */
static double score_text(const char *text, const MatchToken *tokens, int ntokens, int *hits_out) {
    char copy[2048];
    char *tok;
    double score = 0.0;
    int hits = 0;
    if (hits_out) *hits_out = 0;
    if (!text || !*text || ntokens <= 0) return 0.0;
    /* Split camel case before tokenising. An organ is named BonfyreSign, which
     * is one word to a splitter and therefore never matches "sign" -- the organ
     * whose entire purpose is signing did not rank for the verb. */
    {
        size_t w = 0;
        for (const char *c = text; *c && w + 2 < sizeof(copy); c++) {
            if (c != text && isupper((unsigned char)*c) && !isupper((unsigned char)c[-1]))
                copy[w++] = ' ';
            copy[w++] = *c;
        }
        copy[w] = '\0';
    }
    for (tok = strtok(copy, " \t,.;:/()[]{}<>-_\"'\n"); tok; tok = strtok(NULL, " \t,.;:/()[]{}<>-_\"'\n")) {
        int freq;
        for (char *c = tok; *c; c++) *c = (char)tolower((unsigned char)*c);
        if (strlen(tok) < 3) continue;
        freq = token_freq(tokens, ntokens, tok);
        if (freq > 0) { hits++; score += (double)freq / (double)ntokens; }
    }
    if (hits_out) *hits_out = hits;
    return score;
}

/* Match over the registry, not over the compiled-in table.
 *
 * This scored BUILTINS and printed from BUILTINS, while `list` read the
 * database. That split meant a discovered capability was listable and
 * unmatchable: the registry could show you an organ it had just found and then
 * fail to return it for the need it answers, which made discovery cosmetic.
 *
 * A row's own words are what it is matched on, so a curated row still wins on
 * its curated keywords and a discovered row competes on what the atlas says
 * about it. */
/* Word overlap is the default and the learned metric is opt-in, because that
 * is what the measurement said.
 *
 * Over ten queries with known answers, word overlap alone ranked the right
 * capability first nine times and fusing a learned cosine into it managed
 * seven. Cosine over these descriptions separates weakly -- every row lands
 * between 0.56 and 0.58 -- so its ordering is close to arbitrary here, and
 * fusing an arbitrary ordering with a good one costs accuracy however the two
 * are combined. Scaling was tried and was worse still: "lex extraction" went
 * from returning lex first to not returning it at all.
 *
 * The model is not at fault and neither is the estate; a one-line description
 * is simply not much for a sentence embedding to work with. `--semantic` keeps
 * it reachable, because a query sharing no vocabulary with any description is
 * the case word overlap genuinely cannot serve, and the measurement above does
 * not say the metric is useless -- it says it does not belong in front. */
/* Search text supplied by the command's own bounded source header, not a
 * second skill catalogue. This is *lexical discovery evidence only*: it cannot
 * authorize an executable, infer a cost, admit a model or change the atlas.
 * Use it only where the atlas projects a generic, uninformative description.
 * Never follow a path from an untrusted catalogue row: a valid Bonfyre organ
 * name maps to precisely cmd/<Organ>/src/main.c below this checkout. */
static int native_owner_source_words(const char *name, char *out, size_t cap) {
    char root[PATH_MAX], path[PATH_MAX], line[512], block[2048];
    FILE *fp = NULL;
    size_t used=0;
    int inside=0, identified=0;
    out[0]='\0';
    if (!name || strncmp(name,"Bonfyre",6) || cap<2) return 0;
    for (const char *s=name; *s; s++) if (!isalnum((unsigned char)*s)) return 0;
    if (!bf_catalog_find_repo_root(root,sizeof(root))) return 0;
    if (snprintf(path,sizeof(path),"%s/cmd/%s/src/main.c",root,name)>=(int)sizeof(path)) return 0;
    fp=fopen(path,"rb"); if (!fp) return 0;
    block[0]='\0';
    /* Feature macros can precede an organ's descriptive block. Only use
     * a bounded comment that actually names a Bonfyre owner. */
    for (int n=0; n<90 && fgets(line,sizeof(line),fp); n++) {
        char *s=line, *end;
        size_t length;
        if (!inside) {
            s=strstr(s,"/*");
            if (!s) continue;
            s+=2; inside=1;identified=0;used=0;block[0]='\0';
        }
        end=strstr(s,"*/");
        if(end) *end='\0';
        while (*s==' ' || *s=='\t' || *s=='*') s++;
        if(strstr(s,"Bonfyre") || strstr(s,"bonfyre-")) identified=1;
        length=strcspn(s,"\r\n");
        if(length && used+length+2<sizeof(block)) {
            memcpy(block+used,s,length); used+=length;
            block[used++]=' '; block[used]='\0';
        }
        if(end) {
            if(identified && used) {
                snprintf(out,cap,"%s",block); fclose(fp); return 1;
            }
            inside=0;identified=0;used=0;block[0]='\0';
        }
    }
    fclose(fp); return 0;
}

static int g_lexical_only = 1;

static void cmd_match(sqlite3 *db, const char *desc) {
    MatchToken tokens[MAX_TOKENS];
    int ntokens = tokenise_text(desc, tokens);
    sqlite3_stmt *st = NULL;
    struct Row { double score; double lex; double cos; int hits;
                 char id[128], binary[128], stage[64],
                 artifact[128], latency[32]; double cost; } rows[256];
    int n = 0, have_vectors = 0, qdims = 0;
    float qvec[EMBED_DIMS];

    if (ntokens <= 0) {
        fprintf(stderr, "bonfyre-capability: no recognizable words in description\n");
        return;
    }
    /* A learned ranking is used when one can be produced, and the metric is
     * named in the output. A caller shown only a number cannot tell a learned
     * distance from a word count, and the two are not comparable. */
    {
        sqlite3_stmt *probe = NULL;
        if (sqlite3_prepare_v2(db, "SELECT count(*) FROM capabilities WHERE vector IS NOT NULL",
                               -1, &probe, NULL) == SQLITE_OK) {
            if (sqlite3_step(probe) == SQLITE_ROW && sqlite3_column_int(probe, 0) > 0)
                have_vectors = 1;
            sqlite3_finalize(probe);
        }
    }
    if (have_vectors && !g_lexical_only) qdims = embed_text(desc, qvec, EMBED_DIMS);
    if (sqlite3_prepare_v2(db,
            "SELECT id,binary,stage_class,artifact_out,latency_tier,cost_estimate,"
            "description,keywords,name,vector,source FROM capabilities", -1, &st, NULL) != SQLITE_OK) {
        /* A fresh registry may not have the optional learned vector column.
         * That is not permission to change its lexical scorer into LIKE. */
        if(sqlite3_prepare_v2(db,
             "SELECT id,binary,stage_class,artifact_out,latency_tier,cost_estimate,"
             "description,keywords,name,NULL AS vector,source FROM capabilities",
             -1,&st,NULL)!=SQLITE_OK) {
            cmd_match_fallback(db, desc);
            return;
        }
    }
    while (sqlite3_step(st) == SQLITE_ROW && n < (int)(sizeof(rows)/sizeof(rows[0]))) {
        int h1 = 0, h2 = 0, h3 = 0, h4 = 0;
        double sc = 0.0;
        const unsigned char *txt;
        sc += score_text((const char *)sqlite3_column_text(st, 7), tokens, ntokens, &h1);
        sc += score_text((const char *)sqlite3_column_text(st, 6), tokens, ntokens, &h2);
        sc += score_text((const char *)sqlite3_column_text(st, 8), tokens, ntokens, &h3);
        /* The id is the shortest true name a capability has -- "sign", "lex",
         * "spatial" -- and was the one field not scored. A caller asking for
         * the thing by its own name got nothing back. */
        sc += score_text((const char *)sqlite3_column_text(st, 0), tokens, ntokens, &h4);
        {
            const char *stored=(const char *)sqlite3_column_text(st,6);
            const char *origin=(const char *)sqlite3_column_text(st,10);
            const char *name=(const char *)sqlite3_column_text(st,8);
            char words[2048]; int additional_hits=0;
            if(stored && origin && name &&
               (!strcmp(origin,"discovered") || !strcmp(origin,"atlas-projection")) &&
               (strstr(stored,"Atlas-declared capability.") ||
                strstr(stored,"Installed organ found") ||
                !strncmp(stored,"What does ",10)) &&
               native_owner_source_words(name,words,sizeof(words))) {
                sc+=score_text(words,tokens,ntokens,&additional_hits);
                h2+=additional_hits;
            }
        }
        {
            double c = 0.0;
            const void *blob = sqlite3_column_blob(st, 9);
            int bytes = sqlite3_column_bytes(st, 9);
            if (qdims == EMBED_DIMS && blob && bytes == (int)(sizeof(float) * EMBED_DIMS))
                c = cosine(qvec, (const float *)blob, EMBED_DIMS);
            /* Both signals are kept and neither is added to the other. Adding
             * them was the mistake: cosine over short descriptions separates
             * weakly -- every row lands between 0.56 and 0.58 -- so scaling it
             * to compete with word overlap turned the lexical signal into noise
             * on top of a near-constant. "lex extraction" went from ranking lex
             * first to not returning it at all. */
            if (sc <= 0.0 && c <= 0.0) continue;
            rows[n].lex = sc;
            rows[n].cos = c;
            rows[n].hits = h1 + h2 + h3 + h4;
            rows[n].score = 0.0;
        }
        txt = sqlite3_column_text(st, 0);
        snprintf(rows[n].id, sizeof(rows[n].id), "%s", txt ? (const char *)txt : "");
        txt = sqlite3_column_text(st, 1);
        snprintf(rows[n].binary, sizeof(rows[n].binary), "%s", txt ? (const char *)txt : "-");
        txt = sqlite3_column_text(st, 2);
        snprintf(rows[n].stage, sizeof(rows[n].stage), "%s", txt && *txt ? (const char *)txt : "-");
        txt = sqlite3_column_text(st, 3);
        snprintf(rows[n].artifact, sizeof(rows[n].artifact), "%s", txt && *txt ? (const char *)txt : "-");
        txt = sqlite3_column_text(st, 4);
        snprintf(rows[n].latency, sizeof(rows[n].latency), "%s", txt && *txt ? (const char *)txt : "-");
        rows[n].cost = sqlite3_column_double(st, 5);
        n++;
    }
    sqlite3_finalize(st);

    if (n == 0) { cmd_match_fallback(db, desc); return; }

    /* Reciprocal rank fusion. Each metric votes with its ordering rather than
     * its magnitude, so a metric that separates weakly cannot swamp one that
     * separates well, and neither needs a hand-tuned weight to be comparable
     * with the other. The constant is the usual 60. */
    {
        int *by_lex = malloc(sizeof(int) * (size_t)n);
        int *by_cos = malloc(sizeof(int) * (size_t)n);
        if (!by_lex || !by_cos) { free(by_lex); free(by_cos); return; }
        for (int i = 0; i < n; i++) by_lex[i] = by_cos[i] = i;
        for (int i = 1; i < n; i++) {
            int k = by_lex[i], j = i - 1;
            while (j >= 0 && rows[by_lex[j]].lex < rows[k].lex) { by_lex[j + 1] = by_lex[j]; j--; }
            by_lex[j + 1] = k;
        }
        for (int i = 1; i < n; i++) {
            int k = by_cos[i], j = i - 1;
            while (j >= 0 && rows[by_cos[j]].cos < rows[k].cos) { by_cos[j + 1] = by_cos[j]; j--; }
            by_cos[j + 1] = k;
        }
        for (int r = 0; r < n; r++) {
            if (rows[by_lex[r]].lex > 0.0) rows[by_lex[r]].score += 1.0 / (60.0 + r);
            if (qdims == EMBED_DIMS && rows[by_cos[r]].cos > 0.0)
                rows[by_cos[r]].score += 1.0 / (60.0 + r);
        }
        free(by_lex);
        free(by_cos);
    }

    for (int i = 1; i < n; i++) {
        struct Row key = rows[i];
        int j = i - 1;
        while (j >= 0 && rows[j].score < key.score) { rows[j + 1] = rows[j]; j--; }
        rows[j + 1] = key;
    }

    printf("match: '%s'  [metric: %s]\n\n", desc,
           qdims == EMBED_DIMS ? "word overlap fused with learned cosine (--semantic)"
                              : "word overlap");
    printf("%-7s  %-14s  %-22s  %-12s  %-22s  %-10s  %9s\n",
           "SCORE", "ID", "BINARY", "STAGE", "ARTIFACT", "LATENCY", "COST");
    for (int i = 0; i < n && i < 8; i++) {
        char cost_text[16];
        if (rows[i].cost < 0) snprintf(cost_text, sizeof(cost_text), "%9s", "unmeasured");
        else snprintf(cost_text, sizeof(cost_text), "%9.4f", rows[i].cost);
        printf("%0.4f  %-14s  %-22s  %-12s  %-22s  %-10s  %s\n",
               rows[i].score, rows[i].id, rows[i].binary, rows[i].stage,
               rows[i].artifact, rows[i].latency, cost_text);
    }
}



/* What the atlas already says this organ is.
 *
 * A directory scan yields a name and a path. The atlas has carried a
 * semantic_question, a layer, a family, a domain and a maturity for every organ
 * all along, in the entry whose source_path points into that organ's directory.
 * Reading it is the difference between a capability that can be found by name
 * and one that can be ranked against a curated peer.
 *
 * The projection is read, not the 258KB index: architecture/atlas.organs.json
 * is a generated view the atlas writes on export. If it is absent or an organ
 * is not in it, the row stays a bare name -- an organ nobody has described is
 * recorded as undescribed rather than given a description this tool invented. */
static int organ_facts(const char *organ, char *desc, size_t desc_n,
                       char *layer, size_t layer_n, char *publishes, size_t pub_n,
                       char *keywords, size_t kw_n) {
    char path[PATH_MAX], root[PATH_MAX];
    char *json = NULL;
    const char *entry;
    long size;
    FILE *fh = NULL;
    int ok = 0;

    desc[0] = layer[0] = publishes[0] = keywords[0] = '\0';
    if (!bf_catalog_find_repo_root(root, sizeof(root))) return 0;
    snprintf(path, sizeof(path), "%s/architecture/atlas.organs.json", root);
    if (!(fh = fopen(path, "rb"))) return 0;
    if (fseek(fh, 0, SEEK_END) || (size = ftell(fh)) <= 0) { fclose(fh); return 0; }
    rewind(fh);
    if (!(json = malloc((size_t)size + 1))) { fclose(fh); return 0; }
    if (fread(json, 1, (size_t)size, fh) != (size_t)size) { free(json); fclose(fh); return 0; }
    json[size] = '\0';
    fclose(fh);

    /* The projection is one object keyed by organ name; seek that object and
     * read its fields at their own depth. */
    {
        char needle[160];
        snprintf(needle, sizeof(needle), "\"%s\"", organ);
        entry = strstr(json, needle);
        if (entry) {
            const char *close = strchr(entry, '}');
            size_t span = close ? (size_t)(close - entry) : strlen(entry);
            char *slice = malloc(span + 2);
            if (slice) {
                memcpy(slice, entry, span);
                slice[span] = '}';
                slice[span + 1] = '\0';
                bf_lex_json_string(slice, "description", desc, desc_n);
                bf_lex_json_string(slice, "layer", layer, layer_n);
                bf_lex_json_string(slice, "publishes", publishes, pub_n);
                {
                    char family[64] = "", domain[128] = "", maturity[32] = "";
                    bf_lex_json_string(slice, "family", family, sizeof(family));
                    bf_lex_json_string(slice, "domain", domain, sizeof(domain));
                    bf_lex_json_string(slice, "maturity", maturity, sizeof(maturity));
                    snprintf(keywords, kw_n, "%s,%s,%s", family, domain, maturity);
                }
                free(slice);
                ok = desc[0] != '\0';
            }
        }
    }
    free(json);
    return ok;
}


/* Where this binary lives, which is where the estate installs. Honoured from
 * the environment first so a caller can point discovery at a different tree
 * without moving the binary. */
static int family_manifest_name(const char *dir, const char *binary, char *out, size_t cap) {
    char path[PATH_MAX], *json = NULL, needle[256];
    FILE *fh = NULL;
    long size;
    const char *hit, *cursor;
    size_t written = 0;

    if (!dir || !binary || !out || cap < 2) return 0;
    out[0] = '\0';
    snprintf(path, sizeof(path), "%s/.families.json", dir);
    if (!(fh = fopen(path, "rb"))) return 0;
    if (fseek(fh, 0, SEEK_END) || (size = ftell(fh)) <= 0 || size > 1024 * 1024) { fclose(fh); return 0; }
    rewind(fh);
    if (!(json = malloc((size_t)size + 1))) { fclose(fh); return 0; }
    if (fread(json, 1, (size_t)size, fh) != (size_t)size) { free(json); fclose(fh); return 0; }
    fclose(fh);
    json[size] = '\0';

    snprintf(needle, sizeof(needle), "\"%s\"", binary);
    hit = strstr(json, needle);
    if (!hit) { free(json); return 0; }
    cursor = hit + strlen(needle);
    while (*cursor && isspace((unsigned char)*cursor)) cursor++;
    if (*cursor++ != ':') { free(json); return 0; }
    while (*cursor && isspace((unsigned char)*cursor)) cursor++;
    if (*cursor++ != '"') { free(json); return 0; }
    while (*cursor && *cursor != '"' && written + 1 < cap) {
        unsigned char ch = (unsigned char)*cursor++;
        if (ch == '\\' || !(isalnum(ch) || ch == '_' || ch == '-')) { free(json); out[0] = '\0'; return 0; }
        out[written++] = (char)ch;
    }
    if (*cursor != '"' || written == 0 || written + 1 >= cap) { free(json); out[0] = '\0'; return 0; }
    out[written] = '\0';
    free(json);
    return 1;
}

static int canonical_organ_filename(const char *name) {
    const char *dot = name ? strchr(name, '.') : NULL;
    if (!dot) return 1;
    if (!strncmp(dot, ".bak", 4) || !strncmp(dot, ".old", 4) ||
        !strncmp(dot, ".orig", 5) || !strncmp(dot, ".archive", 8) ||
        !strncmp(dot, ".patched", 8) || !strncmp(dot, ".tmp", 4) ||
        !strncmp(dot, ".disabled", 9)) return 0;
    return 1;
}

static int self_directory(char *out, size_t cap) {
    const char *env = getenv("BONFYRE_BIN_DIR");
    char buf[PATH_MAX];
    char *slash;
    if (env && *env) { snprintf(out, cap, "%s", env); return 0; }
#if defined(__APPLE__)
    {
        uint32_t size = (uint32_t)sizeof(buf);
        if (_NSGetExecutablePath(buf, &size) != 0) return -1;
    }
#else
    {
        ssize_t n = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
        if (n <= 0) return -1;
        buf[n] = '\0';
    }
#endif
    if (!(slash = strrchr(buf, '/'))) return -1;
    *slash = '\0';
    snprintf(out, cap, "%s", buf);
    return 0;
}


/* What is installed that the table does not know about.
 *
 * The capability set was a compiled-in table plus rows seeded once, and `index`
 * called rebuild_index, which rebuilds a full-text index over the rows that
 * already exist. So a command named index re-indexed and did not discover, and
 * nineteen organs installed into the managed binary directory left the registry
 * reporting exactly the same count. The estate could not see a capability it
 * had just gained.
 *
 * Discovery is a scan of the directory this binary itself lives in, which is
 * the directory the estate installs into. A binary already named by the builtin
 * table is left alone: the curated row carries a latency tier and a stage
 * that a scan does not know. Its cost is unmeasured too, like a scanned one.
 *
 * A discovered row is recorded with cost -1, meaning unmeasured, and never 0.
 * Zero is a price and this estate already refuses to let an unpriced capability
 * read as a free one. The source column says `discovered` so a consumer can
 * tell a probe from an estimate. */
static void discover_installed(sqlite3 *db, time_t now) {
    char dir[PATH_MAX];
    DIR *d;
    struct dirent *ent;
    char *slash;

    if (self_directory(dir, sizeof(dir)) != 0) return;
    if (!(d = opendir(dir))) return;
    while ((ent = readdir(d))) {
        char path[PATH_MAX], id[128];
        sqlite3_stmt *st = NULL;
        struct stat sb;
        const char *name = ent->d_name;
        if (strncmp(name, "bonfyre-", 8)) continue;
        if (!canonical_organ_filename(name)) continue;
        snprintf(path, sizeof(path), "%s/%s", dir, name);
        if (stat(path, &sb) || !S_ISREG(sb.st_mode) || !(sb.st_mode & S_IXUSR)) continue;
        /* the capability id is the organ name without the family prefix */
        snprintf(id, sizeof(id), "%s", name + 8);
        if ((slash = strchr(id, '.'))) *slash = '\0';
        if (!id[0]) continue;
        {
            char organ[160], desc[512], layer[64], publishes[128], keywords[256];
            /* Prefer the exact managed-family identity. Acronym-heavy names
             * such as bonfyre-mcp-proof cannot be recovered by CamelCasing. */
            if (!family_manifest_name(dir, name, organ, sizeof(organ))) {
                const char *p = name + 8;
                size_t w = 0;
                int upper = 1;
                w += (size_t)snprintf(organ, sizeof(organ), "Bonfyre");
                for (; *p && w + 1 < sizeof(organ); p++) {
                    if (*p == '-') { upper = 1; continue; }
                    organ[w++] = (char)(upper ? toupper((unsigned char)*p) : *p);
                    upper = 0;
                }
                organ[w] = '\0';
            }
            if (!organ_facts(organ, desc, sizeof(desc), layer, sizeof(layer),
                             publishes, sizeof(publishes), keywords, sizeof(keywords))) {
                snprintf(desc, sizeof(desc), "Installed organ found by scanning the "
                         "managed binary directory; no atlas entry describes it");
                snprintf(layer, sizeof(layer), "-");
                snprintf(publishes, sizeof(publishes), "-");
                keywords[0] = '\0';
            }
            if (!layer[0]) snprintf(layer, sizeof(layer), "-");
            if (!publishes[0]) snprintf(publishes, sizeof(publishes), "-");
            if (sqlite3_prepare_v2(db,
                    "INSERT OR IGNORE INTO capabilities("
                    "id,name,description,tags,binary,command,model_id,hardware_tier,"
                    "cost_estimate,latency_tier,stage_class,artifact_out,keywords,source,updated)"
                    " VALUES(?,?,?,?,?,'','','',-1.0,'unmeasured',?,?,?,"
                    "'discovered',?)", -1, &st, NULL) != SQLITE_OK) continue;
            sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 2, organ, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 3, desc, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 4, keywords, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 5, name, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 6, layer, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 7, publishes, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 8, keywords, -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(st, 9, (sqlite3_int64)now);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
    }
    closedir(d);
}

/* A legacy help probe once stored "Usage:" as an organ's description,
 * "layer" as its command, and a guessed zero-dollar call cost. Reindex the
 * source-backed atlas fields instead. Author-authored and builtin contracts
 * are never overwritten, and unavailable atlas entries remain untouched. */
static int reconcile_probed(sqlite3 *db, time_t now) {
    sqlite3_stmt *read = NULL, *up = NULL;
    int changed = 0;
    if (sqlite3_prepare_v2(db,
        "SELECT id,binary FROM capabilities WHERE source='probed' AND binary LIKE 'bonfyre-%'",
        -1, &read, NULL) != SQLITE_OK) return 0;
    if (sqlite3_prepare_v2(db,
        "UPDATE capabilities SET name=?,description=?,tags=?,keywords=?,"
        "stage_class=?,artifact_out=?,command='',cost_estimate=-1.0,"
        "latency_tier='unmeasured',source='atlas-projection',updated=? "
        "WHERE id=? AND source='probed'",
        -1, &up, NULL) != SQLITE_OK) { sqlite3_finalize(read); return 0; }
    while (sqlite3_step(read) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(read, 0);
        const char *binary = (const char *)sqlite3_column_text(read, 1);
        char organ[160], desc[512], layer[64], publishes[128], keywords[256];
        int n = 0, upper = 1;
        if (!id || !binary || strstr(binary, ".bak")) continue;
        n = snprintf(organ, sizeof(organ), "Bonfyre");
        for (const char *p=binary+8; *p && n+1<(int)sizeof(organ); p++) {
            if (*p == '-') { upper = 1; continue; }
            organ[n++] = (char)(upper ? toupper((unsigned char)*p) : *p);
            upper = 0;
        }
        organ[n] = '\0';
        if (!organ_facts(organ, desc, sizeof(desc), layer, sizeof(layer),
                         publishes, sizeof(publishes), keywords, sizeof(keywords))) continue;
        if (!strncmp(desc, "What does ", 10)) {
            snprintf(desc, sizeof(desc), "%s: %s; %s. Atlas-declared capability.",
                     organ, keywords, publishes[0] ? publishes : "output unspecified");
        }
        sqlite3_reset(up); sqlite3_clear_bindings(up);
        sqlite3_bind_text(up, 1, organ, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(up, 2, desc, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(up, 3, keywords, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(up, 4, keywords, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(up, 5, layer[0] ? layer : "-", -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(up, 6, publishes[0] ? publishes : "-", -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(up, 7, (sqlite3_int64)now);
        sqlite3_bind_text(up, 8, id, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(up) == SQLITE_DONE) changed += sqlite3_changes(db);
    }
    sqlite3_finalize(up);
    sqlite3_finalize(read);
    return changed;
}

static void cmd_index(sqlite3 *db) {
    sqlite3_stmt *st = NULL;
    int before = 0, after = 0;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM capabilities", -1, &st, NULL) == SQLITE_OK) {
        if (sqlite3_step(st) == SQLITE_ROW) before = sqlite3_column_int(st, 0);
        sqlite3_finalize(st);
    }
    discover_installed(db, time(NULL));
    int repaired = reconcile_probed(db, time(NULL));
    rebuild_index(db);
    sync_catalog();
    st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM capabilities", -1, &st, NULL) == SQLITE_OK) {
        if (sqlite3_step(st) == SQLITE_ROW) after = sqlite3_column_int(st, 0);
        sqlite3_finalize(st);
    }
    printf("capability index rebuilt: %d known, %d newly discovered\n",
           after, after - before);
    printf("  %d legacy help probes refreshed from native atlas\n", repaired);
}

static int cmd_maturity(int argc, char **argv) {
    BfMaturity maturity;
    const char *input;
    int resolved = -1;
    if (argc < 3 || bf_maturity_from_name(argv[2], &maturity) != 0) {
        fprintf(stderr, "bonfyre-capability: unknown maturity\n");
        return 1;
    }
    input = argv[2];
    for (int index = 3; index < argc; index++) {
        if (strcmp(argv[index], "--resolved") == 0 && index + 1 < argc) {
            const char *value = argv[++index];
            if (strcmp(value, "true") == 0) resolved = 1;
            else if (strcmp(value, "false") == 0) resolved = 0;
            else return 1;
        } else {
            return 1;
        }
    }
    if (resolved >= 0) maturity = bf_capability_apply_probe(maturity, resolved);
    printf("{\"input_maturity\":\"%s\",\"maturity\":\"%s\","
           "\"callable\":%s,\"proven\":%s}\n",
           input, bf_maturity_name(maturity),
           bf_capability_callable(maturity) ? "true" : "false",
           bf_capability_proven(maturity) ? "true" : "false");
    return 0;
}

/* ── governed repair records ─────────────────────────────────────
 * A repair becomes a record here only from a bonfyre.repair-replay.v1 file
 * (tests/vectors bonfyre-vectors --replay-repair): its vector failed on the
 * before binary and passed on the after binary, identically on every run.
 * Admission re-hashes the vector file and the after binary, so a record never
 * describes bytes that changed since the replay.  A record is not a
 * deployment (installed_matches says whether ~/.bonfyre/bin runs those
 * bytes), not a priced product and not authority to invoke anything. */
static const char *REPAIR_SCHEMA =
    "CREATE TABLE IF NOT EXISTS verified_repairs("
    "  id               TEXT PRIMARY KEY,"
    "  claim            TEXT NOT NULL,"
    "  vector_file      TEXT NOT NULL,"
    "  vector_sha256    TEXT NOT NULL,"
    "  case_text        TEXT NOT NULL,"
    "  runs             INTEGER NOT NULL,"
    "  before_sha256    TEXT NOT NULL,"
    "  before_source    TEXT,"
    "  after_binary     TEXT NOT NULL,"
    "  after_sha256     TEXT NOT NULL,"
    "  installed_sha256 TEXT,"
    "  record           TEXT NOT NULL,"
    "  admitted         INTEGER NOT NULL"
    ");";

static const char *jv_str_at(const bf_jv *o, const char *a, const char *b) {
    const bf_jv *v = bf_jv_get(o, a);
    if (b) v = v ? bf_jv_get(v, b) : NULL;
    return (v && v->type == BF_JV_STR) ? v->s : NULL;
}
static int jv_true_at(const bf_jv *o, const char *a, const char *b) {
    const bf_jv *v = bf_jv_get(o, a);
    v = v ? bf_jv_get(v, b) : NULL;
    return v && v->type == BF_JV_BOOL && v->b;
}
static size_t jv_len_at(const bf_jv *o, const char *a, const char *b) {
    const bf_jv *v = bf_jv_get(o, a);
    v = v ? bf_jv_get(v, b) : NULL;
    return (v && v->type == BF_JV_ARR) ? v->len : 0;
}

static int repair_refuse(const char *why) {
    fprintf(stderr, "bonfyre-capability repair-admit: refused: %s\n", why);
    return 1;
}

static int cmd_repair_admit(sqlite3 *db, const char *path, const char *claim) {
    if (!claim || !*claim) return repair_refuse("--claim names what was repaired");
    size_t len = 0;
    char *text = bf_read_file(path, &len), err[256] = "";
    if (!text) return repair_refuse("replay record unreadable");
    bf_jv *r = bf_jv_parse(text, len, err, sizeof err);
    free(text);
    if (!r) return repair_refuse("replay record is not JSON");
    int rc = 1;
    const char *schema = jv_str_at(r, "schema", NULL), *id = jv_str_at(r, "repair_id", NULL);
    const char *verdict = jv_str_at(r, "verdict", NULL), *vfile = jv_str_at(r, "vector_file", NULL);
    const char *vsha = jv_str_at(r, "vector_sha256", NULL), *cs = jv_str_at(r, "case", NULL);
    const char *bsha = jv_str_at(r, "before", "binary_sha256"), *asha = jv_str_at(r, "after", "binary_sha256");
    const char *abin = jv_str_at(r, "after", "binary"), *bsrc = jv_str_at(r, "before", "source");
    const bf_jv *runs_v = bf_jv_get(r, "runs"), *reasons = bf_jv_get(r, "reasons");
    int runs = (runs_v && runs_v->type == BF_JV_NUM) ? (int)runs_v->n : 0;
    char now_v[65], now_a[65], inst[65] = "", ipath[4096];
    if (!schema || strcmp(schema, "bonfyre.repair-replay.v1")) { repair_refuse("not a bonfyre.repair-replay.v1 record"); goto out; }
    if (!id || !*id || !vfile || !vsha || !cs || !bsha || !asha || !abin) { repair_refuse("record is missing a field"); goto out; }
    if (!verdict || strcmp(verdict, "replayed") || !reasons || reasons->type != BF_JV_ARR || reasons->len) { repair_refuse("the replay did not replay"); goto out; }
    if (runs < 2) { repair_refuse("one run is not a replay"); goto out; }
    if (!jv_true_at(r, "before", "every_run_fails_case") || !jv_true_at(r, "before", "signatures_identical")
        || !jv_len_at(r, "before", "failing_checks")) { repair_refuse("the before binary did not fail the case identically on every run"); goto out; }
    if (!jv_true_at(r, "after", "every_run_passes") || !jv_true_at(r, "after", "signatures_identical")
        || jv_len_at(r, "after", "failing_checks")) { repair_refuse("the after binary did not pass identically on every run"); goto out; }
    if (!strcmp(bsha, asha)) { repair_refuse("before and after are the same bytes"); goto out; }
    if (bf_sha256_file(vfile, now_v) != 0) { repair_refuse("the vector file is not readable from here"); goto out; }
    if (strcmp(now_v, vsha)) { repair_refuse("the vector file changed since the replay"); goto out; }
    if (bf_sha256_file(abin, now_a) != 0) { repair_refuse("the after binary is not readable from here"); goto out; }
    if (strcmp(now_a, asha)) { repair_refuse("the after binary changed since the replay"); goto out; }
    {
        const char *base = strrchr(abin, '/');
        base = base ? base + 1 : abin;
        const char *home = getenv("HOME");
        snprintf(ipath, sizeof ipath, "%s/.bonfyre/bin/%s", home ? home : "", base);
        if (bf_sha256_file(ipath, inst) != 0) inst[0] = 0;
    }
    if (sqlite3_exec(db, REPAIR_SCHEMA, NULL, NULL, NULL) != SQLITE_OK) { repair_refuse("registry schema failed"); goto out; }
    {
        sqlite3_stmt *st = NULL;
        sqlite3_prepare_v2(db, "SELECT vector_sha256, after_sha256, before_sha256, claim FROM verified_repairs WHERE id=?", -1, &st, NULL);
        sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            int same = !strcmp((const char *)sqlite3_column_text(st, 0), vsha)
                    && !strcmp((const char *)sqlite3_column_text(st, 1), asha)
                    && !strcmp((const char *)sqlite3_column_text(st, 2), bsha)
                    && !strcmp((const char *)sqlite3_column_text(st, 3), claim);
            sqlite3_finalize(st);
            if (!same) { repair_refuse("a different record already holds this id"); goto out; }
            printf("{\"id\":\"%s\",\"admitted\":false,\"already\":true}\n", id);
            rc = 0; goto out;
        }
        sqlite3_finalize(st);
    }
    {
        bf_jsonw w; bf_jsonw_init(&w, 0); bf_jv_write(&w, r, 0);
        sqlite3_stmt *st = NULL;
        sqlite3_prepare_v2(db,
            "INSERT INTO verified_repairs(id,claim,vector_file,vector_sha256,case_text,runs,before_sha256,"
            "before_source,after_binary,after_sha256,installed_sha256,record,admitted) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", -1, &st, NULL);
        sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, claim, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, vfile, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 4, vsha, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 5, cs, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 6, runs);
        sqlite3_bind_text(st, 7, bsha, -1, SQLITE_TRANSIENT);
        if (bsrc) sqlite3_bind_text(st, 8, bsrc, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(st, 8);
        sqlite3_bind_text(st, 9, abin, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 10, asha, -1, SQLITE_TRANSIENT);
        if (inst[0]) sqlite3_bind_text(st, 11, inst, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(st, 11);
        sqlite3_bind_text(st, 12, w.buf ? w.buf : "{}", -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 13, (sqlite3_int64)time(NULL));
        int ok = sqlite3_step(st) == SQLITE_DONE;
        sqlite3_finalize(st);
        bf_jsonw_free(&w);
        if (!ok) { repair_refuse(sqlite3_errmsg(db)); goto out; }
    }
    printf("{\"id\":\"%s\",\"admitted\":true,\"installed_matches\":%s}\n", id,
           (inst[0] && !strcmp(inst, asha)) ? "true" : "false");
    rc = 0;
out:
    bf_jv_free(r);
    return rc;
}

static void repair_row_json(bf_jsonw *w, sqlite3_stmt *st) {
    const char *asha = (const char *)sqlite3_column_text(st, 9);
    const char *inst = (const char *)sqlite3_column_text(st, 10);
    bf_jsonw_obj(w);
    static const char *keys[] = { "id", "claim", "vector_file", "vector_sha256", "case", NULL,
                                  "before_sha256", "before_source", "after_binary", "after_sha256", "installed_sha256" };
    for (int i = 0; i < 11; i++) {
        if (i == 5) { bf_jsonw_key(w, "runs"); bf_jsonw_int(w, sqlite3_column_int(st, 5)); continue; }
        bf_jsonw_key(w, keys[i]);
        const char *v = (const char *)sqlite3_column_text(st, i);
        if (v) bf_jsonw_str(w, v); else bf_jsonw_null(w);
    }
    bf_jsonw_key(w, "installed_matches"); bf_jsonw_bool(w, inst && asha && !strcmp(inst, asha));
    bf_jsonw_key(w, "admitted"); bf_jsonw_int(w, sqlite3_column_int64(st, 12));
    bf_jsonw_obj_end(w);
}

static int cmd_repairs(sqlite3 *db, const char *only) {
    sqlite3_exec(db, REPAIR_SCHEMA, NULL, NULL, NULL);
    sqlite3_stmt *st = NULL;
    const char *sql = "SELECT id,claim,vector_file,vector_sha256,case_text,runs,before_sha256,before_source,"
                      "after_binary,after_sha256,installed_sha256,record,admitted FROM verified_repairs "
                      "WHERE (?1 IS NULL OR id=?1) ORDER BY id";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 1;
    if (only) sqlite3_bind_text(st, 1, only, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(st, 1);
    bf_jsonw w; bf_jsonw_init(&w, 0);
    int n = 0;
    if (!only) bf_jsonw_arr(&w);
    while (sqlite3_step(st) == SQLITE_ROW) { repair_row_json(&w, st); n++; }
    if (!only) bf_jsonw_arr_end(&w);
    sqlite3_finalize(st);
    if (only && !n) { bf_jsonw_free(&w); fprintf(stderr, "bonfyre-capability: no repair record %s\n", only); return 1; }
    printf("%s\n", w.buf);
    bf_jsonw_free(&w);
    return 0;
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
"  maturity <rung> [--resolved true|false]  native callability/proof ladder\n"
"  repair-admit <replay.json> --claim TEXT  admit a repair whose vector replayed\n"
"                      (bonfyre-vectors --replay-repair: fails before, passes after)\n"
"  repairs             governed repair records as JSON\n"
"  repair-show <id>    one repair record as JSON\n"
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
    if (bf_help_requested(argc, argv)) { cmd_help(); return 0; }
    sqlite3 *db = NULL;
    int rc = 0;
    const char *cmd = NULL;

    if (argc < 2) {
        cmd_help();
        return 0;
    }

    cmd = argv[1];
    if (strcmp(cmd, "maturity") == 0) {
        return cmd_maturity(argc, argv);
    }
    db = open_db();
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
        } else rc = cmd_show(db, argv[2]) ? 0 : 1;
    } else if (strcmp(cmd, "match") == 0) {
        if (argc < 3) {
            fprintf(stderr, "usage: bonfyre-capability match <description>\n");
            rc = 1;
        } else {
            for (int i = 3; i < argc; i++) {
                if (!strcmp(argv[i], "--lexical")) g_lexical_only = 1;
                else if (!strcmp(argv[i], "--semantic")) g_lexical_only = 0;
            }
            cmd_match(db, argv[2]);
        }
    } else if (strcmp(cmd, "index") == 0) cmd_index(db);
    else if (strcmp(cmd, "semantic-index") == 0) cmd_semantic_index(db);
    else if (strcmp(cmd, "repair-admit") == 0) {
        const char *claim = NULL, *file = NULL;
        for (int i = 2; i < argc; i++) {
            if (!strcmp(argv[i], "--claim") && i + 1 < argc) claim = argv[++i];
            else if (!file) file = argv[i];
        }
        if (!file) { fprintf(stderr, "usage: bonfyre-capability repair-admit <replay.json> --claim TEXT\n"); rc = 2; }
        else rc = cmd_repair_admit(db, file, claim);
    } else if (strcmp(cmd, "repairs") == 0) rc = cmd_repairs(db, NULL);
    else if (strcmp(cmd, "repair-show") == 0) {
        if (argc < 3) { fprintf(stderr, "usage: bonfyre-capability repair-show <id>\n"); rc = 2; }
        else rc = cmd_repairs(db, argv[2]);
    }
    else {
        fprintf(stderr, "bonfyre-capability: unknown command: %s\n", cmd);
        rc = 1;
    }

    sqlite3_close(db);
    return rc;
}
