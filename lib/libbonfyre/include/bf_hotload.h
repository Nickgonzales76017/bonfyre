/*
 * bf_hotload.h — Hot-reload pipeline stages via dlopen
 *
 * Turns BonfyrePipeline from hard-coded procedural stages into
 * swappable shared-object plugins.  Each .dylib/.so exports a
 * standard entry point (bf_stage_entry) that the loader discovers
 * via dlsym.  A file-watcher (kqueue/inotify) detects builds and
 * hot-swaps the stage without restarting the pipeline.
 *
 * Use cases:
 *   - A/B test a new Index algorithm vs. production
 *   - Hot-patch Compress strategy (zstd level tuning)
 *   - Load customer-specific Gate extensions at runtime
 *
 * Build a stage plugin:
 *   cc -shared -fPIC -o stage_index_v2.dylib stage_index_v2.c
 *
 * Plugin must export:
 *   const bf_stage_meta_t bf_stage_meta;      // metadata
 *   int bf_stage_entry(bf_stage_ctx_t *ctx);   // entry point
 *   void bf_stage_cleanup(void);               // optional teardown
 */

#ifndef BF_HOTLOAD_H
#define BF_HOTLOAD_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Stage ABI version ───────────────────────────────────────── */

#define BF_STAGE_ABI_VERSION  1

/* ── Stage names (match pipeline_* functions in BonfyrePipeline) */

#define BF_STAGE_GATE       "gate"
#define BF_STAGE_INGEST     "ingest"
#define BF_STAGE_INDEX      "index"
#define BF_STAGE_COMPRESS   "compress"
#define BF_STAGE_METER      "meter"
#define BF_STAGE_STITCH     "stitch"
#define BF_STAGE_LEDGER     "ledger"

/* Transcript pipeline stages */
#define BF_STAGE_TRANSCRIBE "transcribe"
#define BF_STAGE_CLEAN      "clean"
#define BF_STAGE_BRIEF      "brief"
#define BF_STAGE_PROOF      "proof"
#define BF_STAGE_TAG        "tag"
#define BF_STAGE_OFFER      "offer"
#define BF_STAGE_PACK       "pack"

/* ── Stage metadata (exported from plugin as bf_stage_meta) ──── */

typedef struct {
    int             abi_version;     /* Must == BF_STAGE_ABI_VERSION       */
    const char     *stage_name;      /* e.g. "index", "compress"           */
    const char     *version;         /* Semver string, e.g. "2.1.0"        */
    const char     *author;          /* Optional                           */
    const char     *description;     /* One-liner                          */
    uint32_t        flags;           /* BF_STAGE_FLAG_* below              */
} bf_stage_meta_t;

#define BF_STAGE_FLAG_ASYNC       (1u << 0)  /* Stage forks / runs async   */
#define BF_STAGE_FLAG_IDEMPOTENT  (1u << 1)  /* Safe to re-run             */
#define BF_STAGE_FLAG_AB_TEST     (1u << 2)  /* A/B test candidate         */

/* ── Stage execution context (passed into bf_stage_entry) ────── */

typedef struct {
    /* Input */
    const char     *input_path;      /* Primary input file/dir             */
    const char     *output_dir;      /* Where to write results             */
    const char     *artifact_hash;   /* SHA-256 of input artifact          */
    const char     *api_key;         /* Gate key (if authenticated)        */
    const char     *timestamp;       /* ISO 8601 pipeline start time       */
    const char     *tier;            /* License tier                       */

    /* Pipeline state (read-only) */
    int             stage_index;     /* 0-based position in pipeline       */
    int             total_stages;    /* Total stages in current pipeline   */
    const char     *pipeline_id;     /* Unique run ID                      */

    /* Output (stage fills these) */
    char            result_path[4096];   /* Primary output path            */
    char            error_msg[1024];     /* Error message if rc != 0       */
    uint64_t        bytes_processed;     /* Telemetry: bytes in            */
    uint64_t        bytes_produced;      /* Telemetry: bytes out           */
    double          elapsed_ms;          /* Telemetry: wall time           */

    /* Opaque user data (from bf_hotload_set_user_data) */
    void           *user_data;
} bf_stage_ctx_t;

/* ── Stage function signatures ───────────────────────────────── */

typedef int  (*bf_stage_entry_fn)(bf_stage_ctx_t *ctx);
typedef void (*bf_stage_cleanup_fn)(void);

/* ── Loaded stage handle ─────────────────────────────────────── */

typedef struct bf_loaded_stage bf_loaded_stage_t;

/* ── Hot-load registry ───────────────────────────────────────── */

#define BF_HOTLOAD_MAX_STAGES  32

typedef struct bf_hotload_registry bf_hotload_registry_t;

/*
 * Create a registry that watches a directory for .dylib/.so files.
 *   plugin_dir:  directory containing stage plugins
 *   watch:       if non-zero, start a file-watcher for hot-reload
 */
bf_hotload_registry_t *bf_hotload_create(const char *plugin_dir, int watch);

/*
 * Destroy registry, unload all plugins, stop watcher.
 */
void bf_hotload_destroy(bf_hotload_registry_t *reg);

/*
 * Manually load a specific plugin file.
 * Returns 0 on success, -1 on error (ABI mismatch, missing symbols).
 */
int bf_hotload_load(bf_hotload_registry_t *reg, const char *path);

/*
 * Reload a stage by name.  Unloads old version, loads new .so from
 * the same path (or a new path if provided).
 * Returns 0 on success.
 */
int bf_hotload_reload(bf_hotload_registry_t *reg, const char *stage_name,
                       const char *new_path);

/*
 * Look up a loaded stage by name.
 * Returns NULL if no plugin is loaded for that stage name.
 */
const bf_loaded_stage_t *bf_hotload_get(const bf_hotload_registry_t *reg,
                                         const char *stage_name);

/*
 * Execute a stage.  If a plugin is loaded for the stage, runs the
 * plugin; otherwise returns -1 (caller should fall back to built-in).
 */
int bf_hotload_exec(bf_hotload_registry_t *reg, const char *stage_name,
                     bf_stage_ctx_t *ctx);

/*
 * Set user data pointer passed to all stage contexts.
 */
void bf_hotload_set_user_data(bf_hotload_registry_t *reg, void *data);

/*
 * Get metadata for a loaded stage (NULL if not loaded).
 */
const bf_stage_meta_t *bf_hotload_meta(const bf_hotload_registry_t *reg,
                                        const char *stage_name);

/*
 * List all loaded stages.  Returns count, fills names[] up to max_names.
 */
int bf_hotload_list(const bf_hotload_registry_t *reg,
                     const char **names, int max_names);

/*
 * A/B test helper: register two plugins for the same stage.
 * Execution alternates between them based on the pipeline_id hash.
 * The "b_path" plugin is loaded as "<stage_name>__b".
 */
int bf_hotload_ab_register(bf_hotload_registry_t *reg,
                            const char *stage_name,
                            const char *a_path,
                            const char *b_path);

/*
 * Execute with A/B routing.  If an A/B pair is registered for the
 * stage, picks A or B based on FNV-1a hash of ctx->pipeline_id.
 * Otherwise falls through to bf_hotload_exec().
 */
int bf_hotload_ab_exec(bf_hotload_registry_t *reg, const char *stage_name,
                        bf_stage_ctx_t *ctx);

#ifdef __cplusplus
}
#endif

#endif /* BF_HOTLOAD_H */
