/*
 * BonfyreModel — model dependency manager for bonfyre pipelines.
 *
 * Content-addressed SQLite registry of AI models required by pipeline
 * recipes.  SHA-256 hash is the canonical model identity — source URLs
 * are hints, not truth.
 *
 * DB path: ~/.local/share/bonfyre/models.db  (override: $BONFYRE_MODEL_DB)
 * Cache:   ~/.cache/bonfyre/models/           (override: $BONFYRE_MODEL_CACHE)
 *
 * Commands:
 *   bonfyre-model list                    — list registered models
 *   bonfyre-model show <id>               — print full model record
 *   bonfyre-model pull <id>               — ensure model is present in cache
 *   bonfyre-model pull --recipe <code>    — pull all models required by a recipe
 *   bonfyre-model add <file.json>         — register a model manifest from file
 *   bonfyre-model verify <id>             — re-check SHA-256 of cached file
 *   bonfyre-model path <id>               — print absolute cache path (for scripts)
 *   bonfyre-model rm <id>                 — remove from registry (does not delete cache)
 *   bonfyre-model rm --purge <id>         — remove from registry AND delete cache file
 *   bonfyre-model search <query>          — full-text search over names + descriptions
 *   bonfyre-model sources <id>            — list configured pull sources for a model
 *   bonfyre-model source add <id> <url>   — add a pull source URL for a model
 *   bonfyre-model source rm <id> <url>    — remove a pull source URL
 *   bonfyre-model ls-cache               — list cached files with sizes
 *   bonfyre-model status                  — registry + cache stats
 *   bonfyre-model help                    — this message
 *
 * Source URL schemes (evaluated in priority order per model):
 *   swarm://  — bonfyre-swarm local peer (fastest, $0)
 *   file://   — local path (already have it on disk)
 *   s3://     — S3-compatible object store
 *   hf://     — huggingface.co  (HTTPS GET, no SDK required)
 *   https://  — generic HTTPS download
 *
 * bonfyre-run integration:
 *   bonfyre-run reads model deps from recipe JSON and calls
 *   `bonfyre-model pull --recipe <code>` before DAG execution.
 *   Set BONFYRE_MODEL_SKIP_CHECK=1 to bypass (if models pre-staged).
 */

#include <errno.h>
#include <dirent.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <sqlite3.h>
#include <bonfyre.h>

#define VERSION        "1.1.0"
#define MAX_JSON       131072   /* 128 KB max manifest */
#define HASH_HEX       65
#define DB_ENV         "BONFYRE_MODEL_DB"
#define CACHE_ENV      "BONFYRE_MODEL_CACHE"
#define DB_SUBPATH     "/.local/share/bonfyre/models.db"
#define CACHE_SUBPATH  "/.cache/bonfyre/models"

/* ====================================================================
 * Tiny JSON helpers (emit only — no parser needed for built-ins)
 * ==================================================================== */
static void json_escape(const char *s, char *out, size_t cap) {
    size_t j = 0;
    for(size_t i = 0; s[i] && j+4 < cap; i++) {
        unsigned char ch = (unsigned char)s[i];
        if(ch == '"')      { out[j++]='\\'; out[j++]='"'; }
        else if(ch == '\\') { out[j++]='\\'; out[j++]='\\'; }
        else if(ch == '\n') { out[j++]='\\'; out[j++]='n'; }
        else if(ch == '\r') { out[j++]='\\'; out[j++]='r'; }
        else if(ch == '\t') { out[j++]='\\'; out[j++]='t'; }
        else if(ch < 0x20)  { j += (size_t)snprintf(out+j, cap-j, "\\u%04x", ch); }
        else                 out[j++] = (char)ch;
    }
    out[j] = '\0';
}

/* ====================================================================
 * Path helpers
 * ==================================================================== */
static void get_db_path(char *buf, size_t n) {
    const char *e = getenv(DB_ENV);
    if(e) { snprintf(buf, n, "%s", e); return; }
    const char *h = getenv("HOME");
    if(!h) h = "/tmp";
    snprintf(buf, n, "%s%s", h, DB_SUBPATH);
}
static void get_cache_dir(char *buf, size_t n) {
    const char *e = getenv(CACHE_ENV);
    if(e) { snprintf(buf, n, "%s", e); return; }
    const char *h = getenv("HOME");
    if(!h) h = "/tmp";
    snprintf(buf, n, "%s%s", h, CACHE_SUBPATH);
}

static int mkdirs(const char *path) {
    char tmp[PATH_MAX]; size_t len;
    snprintf(tmp, sizeof(tmp), "%s", path);
    len = strlen(tmp);
    if(tmp[len-1]=='/') tmp[--len]='\0';
    for(char *p = tmp+1; *p; p++) {
        if(*p=='/') { *p='\0'; mkdir(tmp, 0755); *p='/'; }
    }
    return mkdir(tmp, 0755);
}

/* ====================================================================
 * DB bootstrap
 * ==================================================================== */
static sqlite3 *db_open(const char *path) {
    /* ensure parent dir exists */
    char parent[PATH_MAX]; snprintf(parent, sizeof(parent), "%s", path);
    char *sl = strrchr(parent, '/');
    if(sl) { *sl='\0'; mkdirs(parent); }

    sqlite3 *db = NULL;
    if(bf_sqlite3_open(path, &db) != SQLITE_OK) {
        fprintf(stderr, "error: cannot open model DB at %s: %s\n",
                path, sqlite3_errmsg(db));
        sqlite3_close(db); return NULL;
    }
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA foreign_keys=ON;",  NULL, NULL, NULL);

    const char *schema =
        "CREATE TABLE IF NOT EXISTS models ("
        "  id                  TEXT PRIMARY KEY,"
        "  name                TEXT NOT NULL,"
        "  description         TEXT,"
        "  format              TEXT NOT NULL,"  /* gguf|safetensors|onnx|bin|layer_fragment|transform_fragment */
        "  sha256              TEXT UNIQUE,"
        "  size_mb             REAL,"
        "  fpq_sha256          TEXT,"
        "  fpq_size_mb         REAL,"
        "  transform_family    TEXT,"           /* T04 | T15 | T16 | ... */
        "  geometry            TEXT,"           /* global | long-form | short-form */
        "  geometry_condition  TEXT,"           /* e.g. 'avg_doc_len > 500' */
        "  layer_frag_spec     TEXT,"           /* JSON: layer_fragment / transform_fragment metadata */
        "  mean_f1             REAL,"           /* from calibration run */
        "  added_at            INTEGER NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS sources ("
        "  model_id TEXT NOT NULL REFERENCES models(id) ON DELETE CASCADE,"
        "  url      TEXT NOT NULL,"
        "  priority INTEGER NOT NULL DEFAULT 50,"
        "  PRIMARY KEY(model_id, url)"
        ");"
        "CREATE TABLE IF NOT EXISTS recipe_models ("
        "  recipe_code TEXT NOT NULL,"
        "  model_id    TEXT NOT NULL REFERENCES models(id) ON DELETE CASCADE,"
        "  role        TEXT,"                 /* transcribe | embed | infer | score */
        "  PRIMARY KEY(recipe_code, model_id)"
        ");"
        "CREATE VIRTUAL TABLE IF NOT EXISTS models_fts USING fts5("
        "  id, name, description,"
        "  content=models, content_rowid=rowid"
        ");";
    char *err = NULL;
    if(sqlite3_exec(db, schema, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "error: schema: %s\n", err);
        sqlite3_free(err); sqlite3_close(db); return NULL;
    }
    /* Live migrations — ADD COLUMN is idempotent (fails silently if column exists) */
    sqlite3_exec(db, "ALTER TABLE models ADD COLUMN transform_family    TEXT;",   NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE models ADD COLUMN geometry            TEXT;",   NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE models ADD COLUMN geometry_condition  TEXT;",   NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE models ADD COLUMN layer_frag_spec     TEXT;",   NULL, NULL, NULL);
    sqlite3_exec(db, "ALTER TABLE models ADD COLUMN mean_f1             REAL;",   NULL, NULL, NULL);
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

static const char *scheme_label(const char *url);

static void sync_catalog_after_model_change(void) {
    char catalog_path[PATH_MAX];
    bf_catalog_default_db_path(catalog_path, sizeof(catalog_path));
    bf_catalog_sync_default(catalog_path);
}

static int catalog_model_sources(sqlite3 *catalog, const char *model_id) {
    sqlite3_stmt *st = NULL;
    char model_node_id[192];
    int count = 0;

    snprintf(model_node_id, sizeof(model_node_id), "model:%s", model_id);
    if (sqlite3_prepare_v2(catalog,
        "SELECT s.name, s.json_data "
        "FROM catalog_edges e "
        "JOIN catalog_nodes s ON s.node_id = e.dst_node_id "
        "WHERE e.src_node_id=? AND e.rel='has_source' AND s.kind='model_source' "
        "ORDER BY s.external_id",
        -1, &st, NULL) != SQLITE_OK) {
        printf("  \"sources\": []");
        return 0;
    }

    sqlite3_bind_text(st, 1, model_node_id, -1, SQLITE_STATIC);
    printf("  \"sources\": [\n");
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *url = (const char *)sqlite3_column_text(st, 0);
        const char *json = (const char *)sqlite3_column_text(st, 1);
        int priority = 0;
        if (json) bf_json_int(json, "priority", &priority);
        if (count > 0) printf(",\n");
        printf("    { \"url\": ");
        char eurl[1024];
        json_escape(url ? url : "", eurl, sizeof(eurl));
        printf("\"%s\", \"scheme\": \"%s\", \"priority\": %d }",
               eurl, scheme_label(url ? url : ""), priority);
        count++;
    }
    printf("\n  ]");
    sqlite3_finalize(st);
    return count;
}

static void catalog_print_string_array(sqlite3 *catalog,
                                       const char *sql,
                                       const char *node_id,
                                       const char *label) {
    sqlite3_stmt *st = NULL;
    int first = 1;
    if (sqlite3_prepare_v2(catalog, sql, -1, &st, NULL) != SQLITE_OK) {
        printf("  \"%s\": []", label);
        return;
    }
    sqlite3_bind_text(st, 1, node_id, -1, SQLITE_STATIC);
    printf("  \"%s\": [", label);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *value = (const char *)sqlite3_column_text(st, 0);
        char escaped[512];
        json_escape(value ? value : "", escaped, sizeof(escaped));
        if (!first) printf(", ");
        printf("\"%s\"", escaped);
        first = 0;
    }
    printf("]");
    sqlite3_finalize(st);
}

/* ====================================================================
 * Built-in model registry
 * ==================================================================== */
typedef struct {
    const char *id;
    const char *name;
    const char *description;
    const char *format;
    double      size_mb;
    const char *sources[6];  /* NULL-terminated */
    const char *recipes[8];  /* recipe codes, NULL-terminated */
    const char *role;
} BuiltinModel;

static const BuiltinModel BUILTIN_MODELS[] = {
    {
        "whisper-large-v3",
        "Whisper Large v3",
        "OpenAI Whisper large-v3. 99-language ASR, 2.9 GB GGUF. Used by all A/M/V/R series recipes.",
        "gguf", 2941.0,
        { "hf://openai/whisper-large-v3-GGUF/whisper-large-v3-q5_k_m.gguf",
          "https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-large-v3-q5_k_m.bin",
          NULL },
        { "A1","A2","A3","M1" }, "transcribe"
    },
    {
        "whisper-base",
        "Whisper Base",
        "OpenAI Whisper base model. 148 MB GGUF. Fast transcription for quick briefs (A1 default on low-memory systems).",
        "gguf", 148.0,
        { "hf://openai/whisper-base-GGUF/whisper-base-q5_k_m.gguf",
          "https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-base-q5_k_m.bin",
          NULL },
        { "A1", NULL }, "transcribe"
    },
    {
        "llama-3-8b-instruct",
        "Llama 3 8B Instruct",
        "Meta Llama 3 8B Instruct GGUF Q4_K_M. 4.7 GB. Brief extraction, repurpose, and summary stages.",
        "gguf", 4700.0,
        { "hf://meta-llama/Meta-Llama-3-8B-Instruct-GGUF/Meta-Llama-3-8B-Instruct-Q4_K_M.gguf",
          NULL },
        { "A2","A3","M1","P1","P2","V1","R1", NULL }, "infer"
    },
    {
        "llama-3-8b-instruct-fpq",
        "Llama 3 8B Instruct (FPQ compressed)",
        "bonfyre-fpq INT8 compressed Llama 3 8B Instruct. ~1.2 GB. Drop-in replacement; load with BonfyreFPQ proxy.",
        "bin", 1200.0,
        { "swarm://llama-3-8b-instruct-fpq", NULL },
        { "A2","A3","M1" }, "infer"
    },
    {
        "nomic-embed-text",
        "Nomic Embed Text v1.5",
        "Nomic AI embedding model. 274 MB GGUF. Used by BonfyreEmbed for vector indexing in A2/A3/R1.",
        "gguf", 274.0,
        { "hf://nomic-ai/nomic-embed-text-v1.5-GGUF/nomic-embed-text-v1.5.Q4_K_M.gguf",
          NULL },
        { "A2","A3","R1" }, "embed"
    },
    {
        "bge-reranker-base",
        "BGE Reranker Base",
        "BAAI BGE reranker base. 278 MB GGUF. FPQ scoring stage in A3 full pipeline.",
        "gguf", 278.0,
        { "hf://BAAI/bge-reranker-base-GGUF/bge-reranker-base-q5_k_m.gguf",
          NULL },
        { "A3" }, "score"
    },
    {
        "pyannote-speaker-segmentation",
        "Pyannote Speaker Segmentation 3.1",
        "Speaker diarisation model. 17 MB ONNX. BonfyreSegment stage. Required for A3/M1 multi-speaker.",
        "onnx", 17.0,
        { "hf://pyannote/speaker-segmentation-3.1/pytorch_model.bin",
          NULL },
        { "A3","M1" }, "score"
    },
    {
        "silero-vad",
        "Silero VAD",
        "Voice activity detection. 1.8 MB ONNX. BonfyreIngest stage gates silence removal before transcription.",
        "onnx", 1.8,
        { "hf://snakers4/silero-vad/silero_vad.onnx",
          NULL },
        { "A1","A2","A3","M1","V1", NULL }, "score"
    },
    {
        "whisper-medium",
        "Whisper Medium",
        "OpenAI Whisper medium. 769 MB GGUF. Balanced accuracy/speed; A2 archive default.",
        "gguf", 769.0,
        { "hf://openai/whisper-medium-GGUF/whisper-medium-q5_k_m.gguf",
          "https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-medium-q5_k_m.bin",
          NULL },
        { "A2","V1" }, "transcribe"
    },
    {
        "llama-3-70b-instruct-fpq",
        "Llama 3 70B Instruct (FPQ compressed)",
        "bonfyre-fpq INT8 compressed Llama 3 70B Instruct. ~9.2 GB. A3 --tier pro inference stage.",
        "bin", 9200.0,
        { "swarm://llama-3-70b-instruct-fpq",
          "hf://meta-llama/Meta-Llama-3-70B-Instruct-GGUF/Meta-Llama-3-70B-Instruct-Q4_K_M.gguf",
          NULL },
        { "A3" }, "infer"
    },
};
#define N_BUILTIN_MODELS (int)(sizeof(BUILTIN_MODELS)/sizeof(BUILTIN_MODELS[0]))

/* ====================================================================
 * Format helpers
 * ==================================================================== */
static void fmt_size(double mb, char *buf, size_t n) {
    if(mb >= 1024.0) snprintf(buf, n, "%.1f GB", mb/1024.0);
    else             snprintf(buf, n, "%.0f MB", mb);
}
static const char *scheme_label(const char *url) {
    if(!url) return "?";
    if(strncmp(url,"swarm://",8)==0) return "swarm";
    if(strncmp(url,"file://",7)==0)  return "local";
    if(strncmp(url,"s3://",5)==0)    return "s3";
    if(strncmp(url,"hf://",5)==0)    return "huggingface";
    return "https";
}

/* ====================================================================
 * Pull: source resolution
 * ==================================================================== */

/* Convert hf://owner/repo/filename → HTTPS URL */
static void hf_to_https(const char *hf, char *out, size_t n) {
    /* hf://owner/repo/file  →  https://huggingface.co/owner/repo/resolve/main/file */
    const char *p = hf + 5; /* skip "hf://" */
    char owner[128], repo[256], file[512];
    /* parse owner/repo/file — repo may contain slashes */
    const char *sl1 = strchr(p, '/');
    if(!sl1) { snprintf(out, n, ""); return; }
    snprintf(owner, sizeof(owner), "%.*s", (int)(sl1-p), p);
    p = sl1+1;
    const char *sl2 = strchr(p, '/');
    if(!sl2) { snprintf(out, n, ""); return; }
    snprintf(repo, sizeof(repo), "%.*s", (int)(sl2-p), p);
    snprintf(file, sizeof(file), "%s", sl2+1);
    snprintf(out, n, "https://huggingface.co/%s/%s/resolve/main/%s", owner, repo, file);
}

/* Invoke curl to download a URL to dest path.
 * No libcurl — just exec curl which is universally available on macOS/Linux.
 * Falls back to wget if curl not found. */
static int download_url(const char *url, const char *dest) {
    char https_url[2048];
    const char *actual_url = url;

    if(strncmp(url,"hf://",5)==0) {
        hf_to_https(url, https_url, sizeof(https_url));
        if(!https_url[0]) { fprintf(stderr,"error: malformed hf:// URL: %s\n",url); return -1; }
        actual_url = https_url;
    } else if(strncmp(url,"file://",7)==0) {
        /* local copy */
        const char *src = url+7;
        pid_t pid = fork();
        if(pid==0) { execlp("cp","cp",src,dest,(char*)NULL); _exit(127); }
        int st; waitpid(pid,&st,0);
        return WIFEXITED(st)&&WEXITSTATUS(st)==0 ? 0 : -1;
    } else if(strncmp(url,"swarm://",8)==0) {
        /* delegate to bonfyre-swarm */
        const char *hash = url+8;
        pid_t pid = fork();
        if(pid==0) {
            execlp("bonfyre-swarm","bonfyre-swarm","pull", hash, "--out", dest, (char*)NULL);
            _exit(127);
        }
        int st; waitpid(pid,&st,0);
        return WIFEXITED(st)&&WEXITSTATUS(st)==0 ? 0 : -1;
    } else if(strncmp(url,"s3://",5)==0) {
        /* delegate to aws cli or s5cmd */
        pid_t pid = fork();
        if(pid==0) {
            execlp("aws","aws","s3","cp",url,dest,(char*)NULL);
            _exit(127);
        }
        int st; waitpid(pid,&st,0);
        return WIFEXITED(st)&&WEXITSTATUS(st)==0 ? 0 : -1;
    }

    /* HTTPS: try curl, then wget */
    fprintf(stderr, "  → downloading from %s\n", actual_url);

    /* Check for curl */
    if(access("/usr/bin/curl",X_OK)==0 || access("/usr/local/bin/curl",X_OK)==0) {
        pid_t pid = fork();
        if(pid==0) {
            execlp("curl","curl","-fsSL","--progress-bar",
                   "-o", dest, actual_url, (char*)NULL);
            _exit(127);
        }
        int st; waitpid(pid,&st,0);
        if(WIFEXITED(st)&&WEXITSTATUS(st)==0) return 0;
        return -1;
    }
    /* Fallback: wget */
    pid_t pid = fork();
    if(pid==0) {
        execlp("wget","wget","-q","--show-progress","-O",dest,actual_url,(char*)NULL);
        _exit(127);
    }
    int st; waitpid(pid,&st,0);
    return WIFEXITED(st)&&WEXITSTATUS(st)==0 ? 0 : -1;
}

/* ====================================================================
 * Seed built-ins into DB
 * ==================================================================== */
static void seed_builtins(sqlite3 *db) {
    for(int i = 0; i < N_BUILTIN_MODELS; i++) {
        const BuiltinModel *m = &BUILTIN_MODELS[i];
        sqlite3_stmt *st;
        sqlite3_prepare_v2(db,
            "INSERT OR IGNORE INTO models(id,name,description,format,sha256,size_mb,added_at)"
            " VALUES(?,?,?,?,?,?,?)", -1, &st, NULL);
        sqlite3_bind_text(st,1,m->id,-1,SQLITE_STATIC);
        sqlite3_bind_text(st,2,m->name,-1,SQLITE_STATIC);
        sqlite3_bind_text(st,3,m->description,-1,SQLITE_STATIC);
        sqlite3_bind_text(st,4,m->format,-1,SQLITE_STATIC);
        /* sha256 unknown until first pull — store NULL */
        sqlite3_bind_null(st,5);
        sqlite3_bind_double(st,6,m->size_mb);
        sqlite3_bind_int64(st,7,(sqlite3_int64)time(NULL));
        sqlite3_step(st); sqlite3_finalize(st);

        /* sources */
        for(int s = 0; m->sources[s]; s++) {
            sqlite3_prepare_v2(db,
                "INSERT OR IGNORE INTO sources(model_id,url,priority) VALUES(?,?,?)",
                -1, &st, NULL);
            sqlite3_bind_text(st,1,m->id,-1,SQLITE_STATIC);
            sqlite3_bind_text(st,2,m->sources[s],-1,SQLITE_STATIC);
            sqlite3_bind_int(st,3,s);
            sqlite3_step(st); sqlite3_finalize(st);
        }

        /* recipe associations */
        for(int r = 0; m->recipes[r]; r++) {
            sqlite3_prepare_v2(db,
                "INSERT OR IGNORE INTO recipe_models(recipe_code,model_id,role) VALUES(?,?,?)",
                -1, &st, NULL);
            sqlite3_bind_text(st,1,m->recipes[r],-1,SQLITE_STATIC);
            sqlite3_bind_text(st,2,m->id,-1,SQLITE_STATIC);
            sqlite3_bind_text(st,3,m->role,-1,SQLITE_STATIC);
            sqlite3_step(st); sqlite3_finalize(st);
        }

        /* FTS sync */
        sqlite3_prepare_v2(db,
            "INSERT OR REPLACE INTO models_fts(rowid,id,name,description)"
            " SELECT rowid,id,name,description FROM models WHERE id=?",
            -1, &st, NULL);
        sqlite3_bind_text(st,1,m->id,-1,SQLITE_STATIC);
        sqlite3_step(st); sqlite3_finalize(st);
    }
}

/* ====================================================================
 * Pull helpers
 * ==================================================================== */

/* Returns 1 if model file is already in cache and hash matches */
static int cache_hit(const char *model_id, const char *expected_sha256,
                     const char *cache_dir, char *hit_path, size_t hp_len) {
    /* Try <cache_dir>/<id>.gguf, <id>.bin, <id>.onnx, <id> */
    static const char *exts[] = { "gguf", "bin", "onnx", "safetensors", "", NULL };
    for(int i = 0; exts[i]; i++) {
        char path[PATH_MAX];
        if(exts[i][0])
            snprintf(path, sizeof(path), "%s/%s.%s", cache_dir, model_id, exts[i]);
        else
            snprintf(path, sizeof(path), "%s/%s", cache_dir, model_id);

        if(access(path, F_OK) != 0) continue;

        /* File exists. If sha256 is known and not "pending", verify */
        if(expected_sha256 && strcmp(expected_sha256,"pending")!=0) {
            char actual[65];
            if(bf_sha256_file(path, actual) == 0 && strcmp(actual, expected_sha256)==0) {
                if(hit_path) snprintf(hit_path, hp_len, "%s", path);
                return 1;
            }
            /* hash mismatch — treat as miss */
            continue;
        }
        /* hash unknown — accept presence */
        if(hit_path) snprintf(hit_path, hp_len, "%s", path);
        return 1;
    }
    return 0;
}

static int pull_model(sqlite3 *db, const char *model_id,
                      const char *cache_dir, int verbose) {
    /* Look up model */
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT name, format, sha256, size_mb FROM models WHERE id=?",
        -1, &st, NULL);
    sqlite3_bind_text(st,1,model_id,-1,SQLITE_STATIC);
    int rc = sqlite3_step(st);
    if(rc != SQLITE_ROW) {
        fprintf(stderr, "error: model '%s' not in registry. "
                "Run: bonfyre-model add <manifest.json>\n", model_id);
        sqlite3_finalize(st); return 1;
    }
    char name[256], format[32], sha256[65];
    double size_mb;
    snprintf(name,   sizeof(name),   "%s", (const char*)sqlite3_column_text(st,0));
    snprintf(format, sizeof(format), "%s", (const char*)sqlite3_column_text(st,1));
    snprintf(sha256, sizeof(sha256), "%s", sqlite3_column_text(st,2) ?
             (const char*)sqlite3_column_text(st,2) : "pending");
    size_mb = sqlite3_column_double(st,3);
    sqlite3_finalize(st);

    char sz[32]; fmt_size(size_mb, sz, sizeof(sz));

    /* Check cache */
    char hit[PATH_MAX];
    if(cache_hit(model_id, sha256, cache_dir, hit, sizeof(hit))) {
        printf("  ✓ %-30s  already cached  %s\n", model_id, hit);
        return 0;
    }

    printf("  ↓ %-30s  %s (%s)  ...\n", model_id, name, sz);
    fflush(stdout);

    /* Ensure cache dir exists */
    mkdirs(cache_dir);

    /* Get sources ordered by priority */
    sqlite3_prepare_v2(db,
        "SELECT url FROM sources WHERE model_id=? ORDER BY priority ASC",
        -1, &st, NULL);
    sqlite3_bind_text(st,1,model_id,-1,SQLITE_STATIC);

    char dest[PATH_MAX];
    snprintf(dest, sizeof(dest), "%s/%s.%s", cache_dir, model_id, format);

    int pulled = 0;
    while(sqlite3_step(st) == SQLITE_ROW) {
        const char *url = (const char*)sqlite3_column_text(st,0);
        if(verbose) fprintf(stderr,"  trying source [%s]: %s\n", scheme_label(url), url);

        /* Skip swarm/s3/file sources if tool not available —
           check by trying the download and falling through on failure */
        int r = download_url(url, dest);
        if(r == 0) {
            pulled = 1;
            break;
        }
        fprintf(stderr,"  source failed, trying next...\n");
        unlink(dest); /* clean partial file */
    }
    sqlite3_finalize(st);

    if(!pulled) {
        fprintf(stderr,"error: all sources failed for '%s'.\n"
                "  Add a source: bonfyre-model source add %s <url>\n",
                model_id, model_id);
        return 1;
    }

    /* Verify SHA-256 if known */
    if(strcmp(sha256,"pending")!=0) {
        char actual[65];
        if(bf_sha256_file(dest, actual) != 0) {            fprintf(stderr,"error: cannot hash downloaded file %s\n", dest);
            return 1;
        }
        if(strcmp(actual, sha256) != 0) {
            fprintf(stderr,"error: SHA-256 mismatch for '%s'\n"
                    "  expected: %s\n  got:      %s\n"
                    "  Removing corrupt file.\n",
                    model_id, sha256, actual);
            unlink(dest);
            return 1;
        }
        printf("  ✓ %-30s  verified  %s\n", model_id, dest);
    } else {
        /* Record actual hash now that we have the file */
        char actual[65];
        if(bf_sha256_file(dest, actual) == 0) {
            sqlite3_prepare_v2(db,
                "UPDATE models SET sha256=? WHERE id=?", -1, &st, NULL);
            sqlite3_bind_text(st,1,actual,-1,SQLITE_STATIC);
            sqlite3_bind_text(st,2,model_id,-1,SQLITE_STATIC);
            sqlite3_step(st); sqlite3_finalize(st);
        }
        printf("  ✓ %-30s  saved  %s\n", model_id, dest);
    }
    return 0;
}

/* ====================================================================
 * Command implementations
 * ==================================================================== */

static void cmd_help(void) {
    printf(
        "bonfyre-model %s — AI model dependency manager\n\n"
        "COMMANDS\n"
        "  list                       list registered models\n"
        "  show <id>                  print full model record\n"
        "  pull <id>                  ensure model is in local cache\n"
        "  pull --recipe <code>       pull all models required by a recipe\n"
        "  add <manifest.json>        register a model from JSON manifest\n"
        "  verify <id>                re-check SHA-256 of cached file\n"
        "  path <id>                  print absolute cache path\n"
        "  rm <id>                    remove from registry\n"
        "  rm --purge <id>            remove from registry + delete cache\n"
        "  search <query>             full-text search over model names\n"
        "  sources <id>               list pull sources for a model\n"
        "  source add <id> <url>      add a pull source URL\n"
        "  source rm <id> <url>       remove a pull source URL\n"
        "  ls-cache                   list cached files with sizes\n"
        "  status                     registry + cache stats\n"
        "  family [<family>]          list models by transform family (e.g. T04, T15, T16)\n"
        "  route <corpus_stats.json>  select best transform family for given corpus stats\n"
        "  push <id> --repo <hf/repo> upload FPQ artifact to HuggingFace\n"
        "  help                       this message\n\n"
        "SOURCE SCHEMES (evaluated in priority order)\n"
        "  swarm://hash               bonfyre-swarm local peer\n"
        "  file:///abs/path           already on disk\n"
        "  s3://bucket/key            S3-compatible store (requires aws cli)\n"
        "  hf://owner/repo/file       huggingface.co (plain HTTPS, no SDK)\n"
        "  https://host/path          direct download\n\n"
        "ENVIRONMENT\n"
        "  BONFYRE_MODEL_DB           override model DB path\n"
        "  BONFYRE_MODEL_CACHE        override model cache dir\n"
        "  BONFYRE_MODEL_SKIP_CHECK   skip model checks in bonfyre-run (set to 1)\n\n"
        "EXAMPLES\n"
        "  bonfyre-model list\n"
        "  bonfyre-model pull whisper-large-v3\n"
        "  bonfyre-model pull --recipe A3\n"
        "  bonfyre-model source add whisper-large-v3 file:///data/models/whisper.gguf\n"
        "  bonfyre-model push bonfyre-topic-mapper-v1 --repo bonfyre-oss/topic-mapper-v1\n",
        VERSION
    );
}

/* ====================================================================
 * cmd_push — upload FPQ-compressed model to HuggingFace Hub
 *
 * Requires: HF_TOKEN env var (huggingface.co user access token)
 * Strategy: huggingface-cli upload if available, else curl PUT fallback.
 * After success, adds hf://<repo>/model.fpq as a source URL in models.db
 * so the artifact is immediately pullable on other machines.
 * ==================================================================== */
static int cmd_push(sqlite3 *db, const char *model_id, const char *repo) {
    const char *token = getenv("HF_TOKEN");
    if (!token || !token[0]) {
        fprintf(stderr, "error: HF_TOKEN environment variable required\n"
                        "       export HF_TOKEN=hf_...\n");
        return 1;
    }

    /* 1. Look up fpq_sha256 → derive local .fpq cache path */
    char fpq_hash[HASH_HEX] = {0};
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT fpq_sha256 FROM models WHERE id=?", -1, &st, NULL);
    sqlite3_bind_text(st, 1, model_id, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0))
        strncpy(fpq_hash, (const char *)sqlite3_column_text(st, 0), sizeof(fpq_hash)-1);
    sqlite3_finalize(st);

    if (!fpq_hash[0]) {
        fprintf(stderr, "error: no fpq_sha256 for model '%s'\n"
                        "       run: bonfyre-quant compress <model> <out.fpq>\n"
                        "       then: bonfyre-model add <artifact.json>\n",
                model_id);
        return 1;
    }

    char cache_dir[PATH_MAX];
    get_cache_dir(cache_dir, sizeof(cache_dir));

    char fpq_path[PATH_MAX];
    snprintf(fpq_path, sizeof(fpq_path), "%s/%s.fpq", cache_dir, fpq_hash);

    struct stat st2;
    if (stat(fpq_path, &st2) != 0) {
        fprintf(stderr, "error: .fpq not in cache at %s\n"
                        "       run: bonfyre-model pull %s\n",
                fpq_path, model_id);
        return 1;
    }

    /* 2. Build artifact.json path (same stem, .json) */
    char artifact_path[PATH_MAX];
    snprintf(artifact_path, sizeof(artifact_path), "%s/%s.json", cache_dir, fpq_hash);

    /* 3. Upload model.fpq — try huggingface-cli first, fall back to curl */
    printf("Pushing %s → hf://%s ...\n", model_id, repo);
    char cmd[8192];

    /* huggingface-cli upload <repo> <local_path> <path_in_repo> */
    snprintf(cmd, sizeof(cmd),
        "huggingface-cli upload '%s' '%s' model.fpq --token '%s' --repo-type model 2>&1",
        repo, fpq_path, token);
    int rc = system(cmd);

    if (rc != 0) {
        /* Curl fallback — HF Hub LFS upload API */
        fprintf(stderr, "[push] huggingface-cli failed (rc=%d), trying curl...\n", rc);
        snprintf(cmd, sizeof(cmd),
            "curl -s --fail -X PUT "
            "  'https://huggingface.co/api/models/%s/upload/main/model.fpq' "
            "  -H 'Authorization: Bearer %s' "
            "  -H 'Content-Type: application/octet-stream' "
            "  --data-binary '@%s' 2>&1",
            repo, token, fpq_path);
        rc = system(cmd);
    }

    if (rc != 0) {
        fprintf(stderr, "error: push failed — check HF_TOKEN and repo permissions\n");
        return 1;
    }

    /* 4. Upload artifact.json as model card metadata (best-effort) */
    if (stat(artifact_path, &st2) == 0) {
        snprintf(cmd, sizeof(cmd),
            "curl -s -X PUT "
            "  'https://huggingface.co/api/models/%s/upload/main/artifact.json' "
            "  -H 'Authorization: Bearer %s' "
            "  -H 'Content-Type: application/json' "
            "  --data-binary '@%s' 2>&1",
            repo, token, artifact_path);
        system(cmd); /* non-fatal */
    }

    /* 5. Register hf://<repo>/model.fpq as pull source in models.db */
    char hf_url[512];
    snprintf(hf_url, sizeof(hf_url), "hf://%s/model.fpq", repo);
    sqlite3_prepare_v2(db,
        "INSERT OR IGNORE INTO sources(model_id, url, priority) VALUES(?,?,0)",
        -1, &st, NULL);
    sqlite3_bind_text(st, 1, model_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, hf_url,   -1, SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);
    sync_catalog_after_model_change();

    printf("Pushed: hf://%s/model.fpq\n", repo);
    printf("  source registered — pull later with: bonfyre-model pull %s\n", model_id);
    return 0;
}

static int cmd_list(sqlite3 *db) {
    sqlite3 *catalog = NULL;
    sqlite3_stmt *cst = NULL;
    char catalog_path[PATH_MAX];
    if (open_catalog(&catalog, catalog_path, sizeof(catalog_path))) {
        if (sqlite3_prepare_v2(catalog,
            "SELECT external_id, json_data, "
            "(SELECT COUNT(*) FROM catalog_edges e WHERE e.dst_node_id = n.node_id AND e.rel='uses_model') "
            "FROM catalog_nodes n WHERE kind='model' ORDER BY external_id",
            -1, &cst, NULL) == SQLITE_OK) {
            char cache_dir[PATH_MAX]; get_cache_dir(cache_dir, sizeof(cache_dir));
            printf("%-32s  %-8s  %-8s  %-7s  %s\n",
                   "ID", "FORMAT", "SIZE", "RECIPES", "CACHED");
            printf("%-32s  %-8s  %-8s  %-7s  %s\n",
                   "--------------------------------", "--------", "--------", "-------", "------");
            int count = 0;
            while (sqlite3_step(cst) == SQLITE_ROW) {
                const char *id = (const char *)sqlite3_column_text(cst, 0);
                const char *json = (const char *)sqlite3_column_text(cst, 1);
                char fmt[64] = "";
                double sz = 0.0;
                int nrec = sqlite3_column_int(cst, 2);
                if (json) {
                    bf_json_str(json, "format", fmt, sizeof(fmt));
                    bf_json_double(json, "size_mb", &sz);
                }
                char szs[16];
                fmt_size(sz, szs, sizeof(szs));
                int cached = cache_hit(id, NULL, cache_dir, NULL, 0);
                printf("%-32s  %-8s  %-8s  %-7d  %s\n",
                       id ? id : "", fmt[0] ? fmt : "-", szs, nrec, cached ? "yes" : "-");
                count++;
            }
            sqlite3_finalize(cst);
            sqlite3_close(catalog);
            printf("\n%d model(s) indexed.\n", count);
            return 0;
        }
        if (cst) sqlite3_finalize(cst);
        sqlite3_close(catalog);
    }

    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT m.id, m.name, m.format, m.size_mb, "
        "       (SELECT COUNT(*) FROM recipe_models r WHERE r.model_id=m.id) AS recipes "
        "FROM models m ORDER BY m.id",
        -1, &st, NULL);

    char cache_dir[PATH_MAX]; get_cache_dir(cache_dir, sizeof(cache_dir));

    printf("%-32s  %-8s  %-8s  %-7s  %s\n",
           "ID", "FORMAT", "SIZE", "RECIPES", "CACHED");
    printf("%-32s  %-8s  %-8s  %-7s  %s\n",
           "--------------------------------", "--------", "--------", "-------", "------");

    int count = 0;
    while(sqlite3_step(st) == SQLITE_ROW) {
        const char *id     = (const char*)sqlite3_column_text(st,0);
        const char *name   = (const char*)sqlite3_column_text(st,1);
        const char *fmt    = (const char*)sqlite3_column_text(st,2);
        double      sz     = sqlite3_column_double(st,3);
        int         nrec   = sqlite3_column_int(st,4);
        char        szs[16]; fmt_size(sz, szs, sizeof(szs));
        int         cached = cache_hit(id, NULL, cache_dir, NULL, 0);
        printf("%-32s  %-8s  %-8s  %-7d  %s\n",
               id, fmt, szs, nrec, cached ? "yes" : "-");
        (void)name;
        count++;
    }
    sqlite3_finalize(st);
    printf("\n%d model(s) registered.\n", count);
    return 0;
}

static int cmd_show(sqlite3 *db, const char *id) {
    sqlite3 *catalog = NULL;
    sqlite3_stmt *cst = NULL;
    char catalog_path[PATH_MAX];
    if (open_catalog(&catalog, catalog_path, sizeof(catalog_path))) {
        if (sqlite3_prepare_v2(catalog,
            "SELECT name, summary, json_data FROM catalog_nodes WHERE kind='model' AND external_id=?",
            -1, &cst, NULL) == SQLITE_OK) {
            sqlite3_bind_text(cst, 1, id, -1, SQLITE_STATIC);
            if (sqlite3_step(cst) == SQLITE_ROW) {
                const char *name = (const char *)sqlite3_column_text(cst, 0);
                const char *summary = (const char *)sqlite3_column_text(cst, 1);
                const char *json = (const char *)sqlite3_column_text(cst, 2);
                char format[64] = "", family[64] = "", geometry[64] = "", cond[256] = "", layer_frag[512] = "";
                char sha[128] = "", fpq_sha[128] = "";
                double mean_f1 = 0.0, size_mb = 0.0, fpq_size_mb = 0.0;
                if (json) {
                    bf_json_str(json, "format", format, sizeof(format));
                    bf_json_str(json, "sha256", sha, sizeof(sha));
                    bf_json_double(json, "size_mb", &size_mb);
                    bf_json_str(json, "fpq_sha256", fpq_sha, sizeof(fpq_sha));
                    bf_json_double(json, "fpq_size_mb", &fpq_size_mb);
                    bf_json_str(json, "transform_family", family, sizeof(family));
                    bf_json_str(json, "geometry", geometry, sizeof(geometry));
                    bf_json_str(json, "geometry_condition", cond, sizeof(cond));
                    bf_json_str(json, "layer_frag_spec", layer_frag, sizeof(layer_frag));
                    bf_json_double(json, "mean_f1", &mean_f1);
                }
                char model_node_id[192];
                snprintf(model_node_id, sizeof(model_node_id), "model:%s", id);
                printf("{\n");
                printf("  \"id\": \"%s\",\n", id);
                printf("  \"name\": \"%s\",\n", name ? name : "");
                char edesc[1024]; json_escape(summary ? summary : "", edesc, sizeof(edesc));
                printf("  \"description\": \"%s\",\n", edesc);
                printf("  \"format\": \"%s\",\n", format[0] ? format : "-");
                printf("  \"sha256\": \"%s\",\n", sha[0] ? sha : "pending");
                printf("  \"size_mb\": %.3f,\n", size_mb);
                printf("  \"fpq_sha256\": \"%s\",\n", fpq_sha[0] ? fpq_sha : "-");
                printf("  \"fpq_size_mb\": %.3f,\n", fpq_size_mb);
                printf("  \"transform_family\": \"%s\",\n", family[0] ? family : "-");
                printf("  \"geometry\": \"%s\",\n", geometry[0] ? geometry : "-");
                printf("  \"geometry_condition\": \"%s\",\n", cond[0] ? cond : "-");
                printf("  \"layer_frag_spec\": \"%s\",\n", layer_frag[0] ? layer_frag : "-");
                printf("  \"mean_f1\": %.3f,\n", mean_f1);
                catalog_model_sources(catalog, id);
                printf(",\n");
                catalog_print_string_array(
                    catalog,
                    "SELECT n.external_id || '(' || n.kind || ':' || COALESCE(NULLIF(e.meta,''), 'model') || ')' "
                    "FROM catalog_edges e JOIN catalog_nodes n ON n.node_id = e.src_node_id "
                    "WHERE e.dst_node_id=? AND e.rel='uses_model' AND (n.kind='recipe' OR n.kind='workflow') "
                    "ORDER BY n.kind, n.external_id",
                    model_node_id,
                    "used_by");
                printf(",\n");
                catalog_print_string_array(
                    catalog,
                    "SELECT n.external_id FROM catalog_edges e "
                    "JOIN catalog_nodes n ON n.node_id = e.src_node_id "
                    "WHERE e.dst_node_id=? AND e.rel='powers_capability' AND n.kind='capability' "
                    "ORDER BY n.external_id",
                    model_node_id,
                    "powers_capabilities");
                printf(",\n");
                catalog_print_string_array(
                    catalog,
                    "SELECT n.external_id FROM catalog_edges e "
                    "JOIN catalog_nodes n ON n.node_id = e.dst_node_id "
                    "WHERE e.src_node_id=? AND e.rel='has_layer_artifact' AND n.kind='layer' "
                    "ORDER BY n.external_id",
                    model_node_id,
                    "layers");
                printf(",\n");
                printf("  \"catalog\": \"%s\"\n", catalog_path);
                printf("}\n");
                sqlite3_finalize(cst);
                sqlite3_close(catalog);
                return 0;
            }
        }
        if (cst) sqlite3_finalize(cst);
        sqlite3_close(catalog);
    }

    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT id,name,description,format,sha256,size_mb,fpq_sha256,fpq_size_mb,"
        "transform_family,geometry,geometry_condition,layer_frag_spec,mean_f1,added_at"
        " FROM models WHERE id=?", -1, &st, NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
    if(sqlite3_step(st) != SQLITE_ROW) {
        fprintf(stderr,"error: model '%s' not found\n", id);
        sqlite3_finalize(st); return 1;
    }

    char cache_dir[PATH_MAX]; get_cache_dir(cache_dir, sizeof(cache_dir));
    char hit[PATH_MAX]; int cached = cache_hit(id, NULL, cache_dir, hit, sizeof(hit));

    printf("{\n");
    printf("  \"id\": \"%s\",\n",           sqlite3_column_text(st,0));
    printf("  \"name\": \"%s\",\n",         sqlite3_column_text(st,1));
    const char *desc = (const char*)sqlite3_column_text(st,2);
    char edesc[512]; json_escape(desc?desc:"", edesc, sizeof(edesc));
    printf("  \"description\": \"%s\",\n",  edesc);
    printf("  \"format\": \"%s\",\n",       sqlite3_column_text(st,3));
    const char *sha_raw = (const char*)sqlite3_column_text(st,4);
    printf("  \"sha256\": \"%s\",\n", sha_raw ? sha_raw : "pending");
    double sz = sqlite3_column_double(st,5);
    char szs[16]; fmt_size(sz,szs,sizeof(szs));
    printf("  \"size\": \"%s\",\n",         szs);
    const char *fsz = (const char*)sqlite3_column_text(st,7);
    if(fsz) {
        char fszs[16]; fmt_size(atof(fsz),fszs,sizeof(fszs));
        printf("  \"fpq_sha256\": \"%s\",\n",  sqlite3_column_text(st,6));
        printf("  \"fpq_size\": \"%s\",\n",    fszs);
    }
    printf("  \"cached\": %s,\n",           cached ? "true" : "false");
    if(cached) { char ep[PATH_MAX]; json_escape(hit,ep,sizeof(ep));
                 printf("  \"cache_path\": \"%s\",\n", ep); }
    sqlite3_finalize(st);

    /* sources */
    sqlite3_prepare_v2(db,
        "SELECT url, priority FROM sources WHERE model_id=? ORDER BY priority",
        -1, &st, NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
    printf("  \"sources\": [\n");
    int first = 1;
    while(sqlite3_step(st)==SQLITE_ROW) {
        if(!first) printf(",\n"); first=0;
        char eu[1024]; json_escape((const char*)sqlite3_column_text(st,0),eu,sizeof(eu));
        printf("    { \"url\": \"%s\", \"scheme\": \"%s\" }",
               eu, scheme_label((const char*)sqlite3_column_text(st,0)));
    }
    printf("\n  ],\n");
    sqlite3_finalize(st);

    /* recipe associations */
    sqlite3_prepare_v2(db,
        "SELECT recipe_code, role FROM recipe_models WHERE model_id=? ORDER BY recipe_code",
        -1, &st, NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
    printf("  \"used_by\": [");
    first=1;
    while(sqlite3_step(st)==SQLITE_ROW)  {
        if(!first) printf(", "); first=0;
        printf("\"%s(%s)\"", sqlite3_column_text(st,0), sqlite3_column_text(st,1));
    }
    printf("]\n}\n");
    sqlite3_finalize(st);
    return 0;
}

static int cmd_pull(sqlite3 *db, const char *model_id) {
    char cache_dir[PATH_MAX]; get_cache_dir(cache_dir, sizeof(cache_dir));
    return pull_model(db, model_id, cache_dir, 1);
}

static int cmd_pull_recipe(sqlite3 *db, const char *recipe_code) {
    printf("Pulling models for recipe %s...\n", recipe_code);
    fflush(stdout);

    /* First seed builtins so recipe associations are present */
    seed_builtins(db);

    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT model_id, role FROM recipe_models WHERE recipe_code=? ORDER BY model_id",
        -1, &st, NULL);
    sqlite3_bind_text(st,1,recipe_code,-1,SQLITE_STATIC);

    char cache_dir[PATH_MAX]; get_cache_dir(cache_dir, sizeof(cache_dir));
    int errors = 0, count = 0;
    while(sqlite3_step(st)==SQLITE_ROW) {
        const char *mid = (const char*)sqlite3_column_text(st,0);
        errors += pull_model(db, mid, cache_dir, 0);
        count++;
    }
    sqlite3_finalize(st);

    if(count == 0) {
        fprintf(stderr,"warning: no models registered for recipe '%s'.\n"
                "  Run: bonfyre-model list   to see all models.\n", recipe_code);
        return 1;
    }
    printf("\n%d model(s) checked for recipe %s. %d error(s).\n",
           count, recipe_code, errors);
    return errors ? 1 : 0;
}

static int cmd_verify(sqlite3 *db, const char *model_id) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT sha256, format FROM models WHERE id=?", -1, &st, NULL);
    sqlite3_bind_text(st,1,model_id,-1,SQLITE_STATIC);
    if(sqlite3_step(st) != SQLITE_ROW) {
        fprintf(stderr,"error: model '%s' not found\n", model_id);
        sqlite3_finalize(st); return 1;
    }
    char sha256[65], format[32];
    snprintf(sha256, sizeof(sha256), "%s", (const char*)sqlite3_column_text(st,0));
    snprintf(format, sizeof(format), "%s", (const char*)sqlite3_column_text(st,1));
    sqlite3_finalize(st);

    char cache_dir[PATH_MAX]; get_cache_dir(cache_dir, sizeof(cache_dir));
    char hit[PATH_MAX];
    if(!cache_hit(model_id, NULL, cache_dir, hit, sizeof(hit))) {
        fprintf(stderr,"error: '%s' not in cache. Run: bonfyre-model pull %s\n",
                model_id, model_id);
        return 1;
    }

    if(strcmp(sha256,"pending")==0) {
        printf("  pending  no expected hash stored — computing and storing...\n");
        char actual[65];
        if(bf_sha256_file(hit, actual)!=0) {
            fprintf(stderr,"error: cannot hash file %s\n", hit); return 1;
        }
        sqlite3_prepare_v2(db,
            "UPDATE models SET sha256=? WHERE id=?", -1, &st, NULL);
        sqlite3_bind_text(st,1,actual,-1,SQLITE_STATIC);
        sqlite3_bind_text(st,2,model_id,-1,SQLITE_STATIC);
        sqlite3_step(st); sqlite3_finalize(st);
        printf("  stored   %s  %s\n", actual, hit);
        return 0;
    }

    printf("  verifying  %s  ...\n", hit); fflush(stdout);
    char actual[65];
    if(bf_sha256_file(hit, actual)!=0) {
        fprintf(stderr,"error: cannot hash file %s\n", hit); return 1;
    }
    if(strcmp(actual, sha256)==0) {
        printf("  ✓ ok      %s\n", sha256);
        return 0;
    }
    fprintf(stderr,"  ✗ FAIL    expected: %s\n               got:      %s\n",
            sha256, actual);
    return 1;
}

static int cmd_path(sqlite3 *db, const char *model_id) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT format FROM models WHERE id=?", -1, &st, NULL);
    sqlite3_bind_text(st,1,model_id,-1,SQLITE_STATIC);
    if(sqlite3_step(st) != SQLITE_ROW) {
        fprintf(stderr,"error: model '%s' not found\n", model_id);
        sqlite3_finalize(st); return 1;
    }
    sqlite3_finalize(st);

    char cache_dir[PATH_MAX]; get_cache_dir(cache_dir, sizeof(cache_dir));
    char hit[PATH_MAX];
    if(cache_hit(model_id, NULL, cache_dir, hit, sizeof(hit))) {
        printf("%s\n", hit);
        return 0;
    }
    fprintf(stderr,"error: '%s' not in cache. Run: bonfyre-model pull %s\n",
            model_id, model_id);
    return 1;
}

static int cmd_rm(sqlite3 *db, const char *model_id, int purge) {
    if(purge) {
        char cache_dir[PATH_MAX]; get_cache_dir(cache_dir, sizeof(cache_dir));
        char hit[PATH_MAX];
        if(cache_hit(model_id, NULL, cache_dir, hit, sizeof(hit))) {
            if(unlink(hit)==0) printf("  deleted cache file: %s\n", hit);
            else perror("unlink");
        }
    }
    char *err=NULL;
    char sql[512];
    snprintf(sql,sizeof(sql),"DELETE FROM models WHERE id='%s'", model_id);
    /* NOTE: Using parameterized query for safety */
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,"DELETE FROM models WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,model_id,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);
    int changed = sqlite3_changes(db);
    (void)err; (void)sql;
    if(changed==0) { fprintf(stderr,"model '%s' not found\n",model_id); return 1; }
    sync_catalog_after_model_change();
    printf("  removed: %s%s\n", model_id, purge?" (cache purged)":"");
    return 0;
}

static int cmd_search(sqlite3 *db, const char *query) {
    sqlite3 *catalog = NULL;
    sqlite3_stmt *cst = NULL;
    char catalog_path[PATH_MAX];
    if (open_catalog(&catalog, catalog_path, sizeof(catalog_path))) {
        if (sqlite3_prepare_v2(catalog,
            "SELECT n.external_id, n.name, n.json_data "
            "FROM catalog_fts f JOIN catalog_nodes n ON n.rowid = f.rowid "
            "WHERE f.catalog_fts MATCH ? AND n.kind='model' ORDER BY n.external_id",
            -1, &cst, NULL) == SQLITE_OK) {
            sqlite3_bind_text(cst, 1, query, -1, SQLITE_STATIC);
            int count = 0;
            while (sqlite3_step(cst) == SQLITE_ROW) {
                const char *id  = (const char*)sqlite3_column_text(cst,0);
                const char *nm  = (const char*)sqlite3_column_text(cst,1);
                const char *json = (const char*)sqlite3_column_text(cst,2);
                char fmt[64] = "";
                double sz = 0.0;
                if (json) {
                    bf_json_str(json, "format", fmt, sizeof(fmt));
                    bf_json_double(json, "size_mb", &sz);
                }
                char szs[16]; fmt_size(sz,szs,sizeof(szs));
                printf("%-32s  %-8s  %-8s  %s\n", id ? id : "", fmt[0] ? fmt : "-", szs, nm ? nm : "");
                count++;
            }
            sqlite3_finalize(cst);
            sqlite3_close(catalog);
            if(count==0) printf("no results for '%s'\n", query);
            return 0;
        }
        if (cst) sqlite3_finalize(cst);
        sqlite3_close(catalog);
    }

    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT m.id, m.name, m.format, m.size_mb FROM models m "
        "JOIN models_fts f ON m.rowid = f.rowid "
        "WHERE models_fts MATCH ? ORDER BY rank",
        -1, &st, NULL);
    sqlite3_bind_text(st,1,query,-1,SQLITE_STATIC);
    int count=0;
    while(sqlite3_step(st)==SQLITE_ROW) {
        const char *id  = (const char*)sqlite3_column_text(st,0);
        const char *nm  = (const char*)sqlite3_column_text(st,1);
        const char *fmt = (const char*)sqlite3_column_text(st,2);
        double sz       = sqlite3_column_double(st,3);
        char szs[16]; fmt_size(sz,szs,sizeof(szs));
        printf("%-32s  %-8s  %-8s  %s\n", id, fmt, szs, nm);
        count++;
    }
    sqlite3_finalize(st);
    if(count==0) printf("no results for '%s'\n", query);
    return 0;
}

static int cmd_sources(sqlite3 *db, const char *model_id) {
    sqlite3 *catalog = NULL;
    sqlite3_stmt *cst = NULL;
    char catalog_path[PATH_MAX];
    if (open_catalog(&catalog, catalog_path, sizeof(catalog_path))) {
        char model_node_id[192];
        int count=0;
        snprintf(model_node_id, sizeof(model_node_id), "model:%s", model_id);
        if (sqlite3_prepare_v2(catalog,
            "SELECT s.name, s.json_data FROM catalog_edges e "
            "JOIN catalog_nodes s ON s.node_id = e.dst_node_id "
            "WHERE e.src_node_id=? AND e.rel='has_source' AND s.kind='model_source' "
            "ORDER BY s.external_id",
            -1, &cst, NULL) == SQLITE_OK) {
            sqlite3_bind_text(cst,1,model_node_id,-1,SQLITE_STATIC);
            printf("Sources for '%s' (priority order, catalog-backed):\n", model_id);
            while(sqlite3_step(cst)==SQLITE_ROW) {
                const char *url = (const char*)sqlite3_column_text(cst,0);
                const char *json = (const char*)sqlite3_column_text(cst,1);
                int pri = 0;
                if (json) bf_json_int(json, "priority", &pri);
                printf("  [%d] [%-11s] %s\n",
                       pri, scheme_label(url ? url : ""), url ? url : "");
                count++;
            }
            sqlite3_finalize(cst);
            sqlite3_close(catalog);
            if(count==0) printf("  (no sources registered)\n");
            return 0;
        }
        if (cst) sqlite3_finalize(cst);
        sqlite3_close(catalog);
    }

    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT url, priority FROM sources WHERE model_id=? ORDER BY priority",
        -1, &st, NULL);
    sqlite3_bind_text(st,1,model_id,-1,SQLITE_STATIC);
    int count=0;
    printf("Sources for '%s' (priority order):\n", model_id);
    while(sqlite3_step(st)==SQLITE_ROW) {
        printf("  [%d] [%-11s] %s\n",
               sqlite3_column_int(st,1),
               scheme_label((const char*)sqlite3_column_text(st,0)),
               sqlite3_column_text(st,0));
        count++;
    }
    sqlite3_finalize(st);
    if(count==0) printf("  (no sources registered)\n");
    return 0;
}

static int cmd_source_add(sqlite3 *db, const char *model_id, const char *url) {
    /* Get max priority */
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "SELECT COALESCE(MAX(priority),0)+10 FROM sources WHERE model_id=?",
        -1, &st, NULL);
    sqlite3_bind_text(st,1,model_id,-1,SQLITE_STATIC);
    int pri = 50;
    if(sqlite3_step(st)==SQLITE_ROW) pri = sqlite3_column_int(st,0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO sources(model_id,url,priority) VALUES(?,?,?)",
        -1, &st, NULL);
    sqlite3_bind_text(st,1,model_id,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,url,-1,SQLITE_STATIC);
    sqlite3_bind_int(st,3,pri);
    sqlite3_step(st); sqlite3_finalize(st);
    sync_catalog_after_model_change();
    printf("  added [%s] %s\n", scheme_label(url), url);
    return 0;
}

static int cmd_source_rm(sqlite3 *db, const char *model_id, const char *url) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "DELETE FROM sources WHERE model_id=? AND url=?", -1, &st, NULL);
    sqlite3_bind_text(st,1,model_id,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,url,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);
    int changed = sqlite3_changes(db);
    if(changed==0) { fprintf(stderr,"source not found\n"); return 1; }
    sync_catalog_after_model_change();
    printf("  removed: %s\n", url);
    return 0;
}

static int cmd_add(sqlite3 *db, const char *json_path) {
    FILE *f = fopen(json_path, "r");
    if(!f) { perror(json_path); return 1; }
    char buf[MAX_JSON]; size_t n = fread(buf,1,sizeof(buf)-1,f); fclose(f);
    buf[n]='\0';

    /* Minimal JSON field extraction — handles both quoted strings and bare numbers */
    #define JFIELD(key, dest, dsz) do { \
        const char *_p = strstr(buf,"\"" key "\""); \
        dest[0]='\0'; \
        if(_p) { \
            _p = strchr(_p,':'); if(_p) { \
                _p++; while(*_p==' '||*_p=='\t') _p++; \
                if(*_p=='"') { _p++; size_t _i=0; \
                    while(*_p&&*_p!='"'&&_i<(dsz)-1) dest[_i++]=*_p++; \
                    dest[_i]='\0'; \
                } else if(*_p!='\0'&&*_p!='n'&&*_p!=']'&&*_p!='}') { \
                    size_t _i=0; \
                    while(*_p&&*_p!=','&&*_p!='}'&&*_p!=']'&&*_p!='\n'&&_i<(dsz)-1) dest[_i++]=*_p++; \
                    while(_i>0&&(dest[_i-1]==' '||dest[_i-1]=='\t'||dest[_i-1]=='\r')) _i--; \
                    dest[_i]='\0'; } \
            } \
        } \
    } while(0)

    char id[128], name[256], desc[512], fmt[32], sha256[65], size_s[32],
         fpq_sha[65], fpq_sz[32], tf[32], geom[64], geom_cond[256],
         lf_spec[1024], mean_f1_s[32];
    JFIELD("id",                  id,        sizeof(id));
    JFIELD("name",                name,      sizeof(name));
    JFIELD("description",         desc,      sizeof(desc));
    JFIELD("format",              fmt,       sizeof(fmt));
    JFIELD("sha256",              sha256,    sizeof(sha256));
    JFIELD("size_mb",             size_s,    sizeof(size_s));
    JFIELD("fpq_sha256",          fpq_sha,   sizeof(fpq_sha));
    JFIELD("fpq_size_mb",         fpq_sz,    sizeof(fpq_sz));
    JFIELD("transform_family",    tf,        sizeof(tf));
    JFIELD("geometry",            geom,      sizeof(geom));
    JFIELD("geometry_condition",  geom_cond, sizeof(geom_cond));
    JFIELD("layer_frag_spec",     lf_spec,   sizeof(lf_spec));
    JFIELD("mean_f1",             mean_f1_s, sizeof(mean_f1_s));
    #undef JFIELD

    if(!id[0] || !name[0] || !fmt[0]) {
        fprintf(stderr,"error: manifest must have id, name, format fields\n");
        return 1;
    }
    if(!sha256[0]) snprintf(sha256, sizeof(sha256), "pending");
    double size_mb  = size_s[0]  ? atof(size_s)  : 0.0;
    double fpq_size = fpq_sz[0]  ? atof(fpq_sz)  : 0.0;
    double mean_f1  = mean_f1_s[0] ? atof(mean_f1_s) : 0.0;

    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO models(id,name,description,format,sha256,size_mb,"
        "fpq_sha256,fpq_size_mb,transform_family,geometry,geometry_condition,"
        "layer_frag_spec,mean_f1,added_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        -1, &st, NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,name,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,desc[0]?desc:NULL,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,4,fmt,-1,SQLITE_STATIC);
    /* use NULL for unknown sha256 so UNIQUE constraint allows multiple unknowns */
    if(sha256[0] && strcmp(sha256,"pending")!=0)
        sqlite3_bind_text(st,5,sha256,-1,SQLITE_STATIC);
    else
        sqlite3_bind_null(st,5);
    sqlite3_bind_double(st,6,size_mb);
    sqlite3_bind_text(st,7,fpq_sha[0]?fpq_sha:NULL,-1,SQLITE_STATIC);
    fpq_size>0 ? sqlite3_bind_double(st,8,fpq_size) : sqlite3_bind_null(st,8);
    sqlite3_bind_text(st,9, tf[0]        ? tf        : NULL, -1, SQLITE_STATIC);
    sqlite3_bind_text(st,10, geom[0]      ? geom      : NULL, -1, SQLITE_STATIC);
    sqlite3_bind_text(st,11, geom_cond[0] ? geom_cond : NULL, -1, SQLITE_STATIC);
    sqlite3_bind_text(st,12, lf_spec[0]   ? lf_spec   : NULL, -1, SQLITE_STATIC);
    mean_f1>0 ? sqlite3_bind_double(st,13,mean_f1) : sqlite3_bind_null(st,13);
    sqlite3_bind_int64(st,14,(sqlite3_int64)time(NULL));
    int rc = sqlite3_step(st); sqlite3_finalize(st);
    if(rc != SQLITE_DONE){
        fprintf(stderr,"bonfyre-model: db insert failed: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    /* Parse and insert sources array: "sources": ["url1","url2"] */
    const char *src_start = strstr(buf,"\"sources\"");
    if(src_start) {
        src_start = strchr(src_start,'[');
        int pri=0;
        while(src_start && *src_start) {
            src_start = strchr(src_start,'"');
            if(!src_start) break;
            src_start++;
            const char *end = strchr(src_start,'"');
            if(!end) break;
            char url[1024];
            snprintf(url, sizeof(url), "%.*s", (int)(end-src_start), src_start);
            src_start = end+1;
            if(url[0]) {
                sqlite3_prepare_v2(db,
                    "INSERT OR IGNORE INTO sources(model_id,url,priority) VALUES(?,?,?)",
                    -1, &st, NULL);
                sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
                sqlite3_bind_text(st,2,url,-1,SQLITE_STATIC);
                sqlite3_bind_int(st,3,pri++);
                sqlite3_step(st); sqlite3_finalize(st);
            }
            /* stop at ] */
            const char *cl = strchr(src_start,']');
            const char *nx = strchr(src_start,'"');
            if(!nx || (cl && cl<nx)) break;
        }
    }

    /* FTS update */
    sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO models_fts(rowid,id,name,description)"
        " SELECT rowid,id,name,description FROM models WHERE id=?",
        -1, &st, NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);
    sync_catalog_after_model_change();

    printf("  registered: %s  (%s)\n", id, name);
    return 0;
}

static int cmd_ls_cache(void) {
    char cache_dir[PATH_MAX]; get_cache_dir(cache_dir, sizeof(cache_dir));

    /* Use ls -lh via shell — no need to implement recursive walk here */
    char cmd[PATH_MAX+32];
    snprintf(cmd, sizeof(cmd), "ls -lh '%s' 2>/dev/null || echo '(cache empty or not found)'", cache_dir);
    printf("Cache: %s\n\n", cache_dir);
    system(cmd); /* intentionally using system here — read-only ls */
    return 0;
}

/* ====================================================================
 * cmd_family — list all models belonging to a transform family
 * ==================================================================== */
static int cmd_family(sqlite3 *db, const char *family) {
    sqlite3 *catalog = NULL;
    sqlite3_stmt *cst = NULL;
    char catalog_path[PATH_MAX];
    if (open_catalog(&catalog, catalog_path, sizeof(catalog_path))) {
        const char *sql = family
            ? "SELECT external_id, category, json_data "
              "FROM catalog_nodes WHERE kind='model' AND category=? ORDER BY external_id"
            : "SELECT external_id, category, json_data "
              "FROM catalog_nodes WHERE kind='model' AND category != 'model' ORDER BY category, external_id";
        if (sqlite3_prepare_v2(catalog, sql, -1, &cst, NULL) == SQLITE_OK) {
            if (family) sqlite3_bind_text(cst, 1, family, -1, SQLITE_STATIC);
            int count = 0;
            printf("%-36s  %-10s  %-12s  %-28s  %s\n",
                   "id","family","geometry","condition","mean_f1");
            printf("%s\n","--------------------------------------------------------------------------------------------------");
            while (sqlite3_step(cst) == SQLITE_ROW) {
                const char *id = (const char *)sqlite3_column_text(cst, 0);
                const char *fam = (const char *)sqlite3_column_text(cst, 1);
                const char *json = (const char *)sqlite3_column_text(cst, 2);
                char geom[64] = "";
                char cond[256] = "";
                double f1 = 0.0;
                if (json) {
                    bf_json_str(json, "geometry", geom, sizeof(geom));
                    bf_json_str(json, "geometry_condition", cond, sizeof(cond));
                    bf_json_double(json, "mean_f1", &f1);
                }
                printf("%-36s  %-10s  %-12s  %-28s  %.3f\n",
                       id ? id : "",
                       fam ? fam : (family ? family : ""),
                       geom[0] ? geom : "—",
                       cond[0] ? cond : "—",
                       f1);
                count++;
            }
            sqlite3_finalize(cst);
            sqlite3_close(catalog);
            if (count == 0) {
                if (family) printf("no models found for family '%s'\n", family);
                else printf("no transform families indexed\n");
            }
            return 0;
        }
        if (cst) sqlite3_finalize(cst);
        sqlite3_close(catalog);
    }

    sqlite3_stmt *st;
    int rc;
    if(family) {
        rc = sqlite3_prepare_v2(db,
            "SELECT id, name, geometry, geometry_condition, mean_f1, format, transform_family "
            "FROM models WHERE transform_family=? ORDER BY mean_f1 DESC",
            -1, &st, NULL);
        sqlite3_bind_text(st,1,family,-1,SQLITE_STATIC);
    } else {
        rc = sqlite3_prepare_v2(db,
            "SELECT id, name, geometry, geometry_condition, mean_f1, format, transform_family "
            "FROM models WHERE transform_family IS NOT NULL ORDER BY transform_family, mean_f1 DESC",
            -1, &st, NULL);
    }
    if(rc != SQLITE_OK) { fprintf(stderr,"error: %s\n",sqlite3_errmsg(db)); return 1; }
    int count=0;
    printf("%-36s  %-10s  %-12s  %-28s  %s\n",
           "id","family","geometry","condition","mean_f1");
    printf("%s\n","--------------------------------------------------------------------------------------------------");
    while(sqlite3_step(st)==SQLITE_ROW) {
        const char *id   = (const char*)sqlite3_column_text(st,0);
        const char *geom = (const char*)sqlite3_column_text(st,2);
        const char *cond = (const char*)sqlite3_column_text(st,3);
        double f1        = sqlite3_column_double(st,4);
        const char *fam  = (const char*)sqlite3_column_text(st,6);
        printf("%-36s  %-10s  %-12s  %-28s  %.3f\n",
               id ? id : "",
               fam ? fam : (family ? family : ""),
               geom ? geom : "—",
               cond ? cond : "—",
               f1);
        count++;
    }
    sqlite3_finalize(st);
    if(count==0) {
        if(family) printf("no models found for family '%s'\n", family);
        else       printf("no transform families registered\n");
    }
    return 0;
}

/* ====================================================================
 * cmd_route — select best transform family for given corpus stats
 *
 * Reads a JSON file with corpus statistics (avg_doc_len, n_docs, etc.)
 * and returns the model id + family with the highest mean_f1 whose
 * geometry_condition passes.
 *
 * Condition evaluator: supports "avg_doc_len > N", "avg_doc_len < N",
 * "avg_doc_len >= N", "avg_doc_len <= N", NULL (always passes).
 * ==================================================================== */
static double parse_stat(const char *json, const char *key) {
    if(!json || !key) return -1.0;
    /* find "key": <value> */
    char pat[128]; snprintf(pat, sizeof(pat), "\"%s\"", key);
    const char *p = strstr(json, pat);
    if(!p) return -1.0;
    p += strlen(pat);
    while(*p==' '||*p==':') p++;
    return atof(p);
}

static int eval_condition(const char *cond, const char *stats_json) {
    if(!cond || cond[0]=='\0') return 1; /* no condition → always passes */
    /* parse: "avg_doc_len > 500" */
    char field[64]; char op[4]; double threshold;
    if(sscanf(cond, "%63s %3s %lf", field, op, &threshold) != 3) return 1;
    double val = parse_stat(stats_json, field);
    if(val < 0) return 0; /* condition has a concrete stat; missing stat is not eligible */
    if(strcmp(op,">")==0)  return val > threshold;
    if(strcmp(op,">=")==0) return val >= threshold;
    if(strcmp(op,"<")==0)  return val < threshold;
    if(strcmp(op,"<=")==0) return val <= threshold;
    if(strcmp(op,"==")==0) return val == threshold;
    return 1;
}

/* ====================================================================
 * frontier_cosine — look up cosine_mean for a family pair from
 * frontier.json.  Tries (fa→fb) first, then (fb→fa).  Returns -1.0
 * when the pair is not found.
 * ==================================================================== */
static double frontier_cosine(const char *json, const char *fa, const char *fb) {
    if (!json || !fa || !fb) return -1.0;
    for (int swap = 0; swap < 2; swap++) {
        const char *a = swap ? fb : fa;
        const char *b = swap ? fa : fb;
        char pat_a[128], pat_b[128];
        snprintf(pat_a, sizeof(pat_a), "\"family_a\": \"%s\"", a);
        snprintf(pat_b, sizeof(pat_b), "\"family_b\": \"%s\"", b);
        const char *p = json;
        while ((p = strstr(p, pat_a)) != NULL) {
            /* find the enclosing pair object: advance to the first '{' before p,
             * then find the matching '}' — search only within that block */
            const char *obj_start = p;
            while (obj_start > json && *obj_start != '{') obj_start--;
            /* find end of this object: next top-level '}' at depth 1 */
            int depth = 0;
            const char *obj_end = obj_start;
            while (*obj_end) {
                if (*obj_end == '{') depth++;
                else if (*obj_end == '}') { depth--; if (depth == 0) break; }
                obj_end++;
            }
            /* check that pat_b AND "cosine_mean" are within [obj_start, obj_end] */
            size_t blen = (size_t)(obj_end - obj_start);
            if (blen > 0 && blen < 2048) {
                char block[2048];
                memcpy(block, obj_start, blen);
                block[blen] = '\0';
                if (strstr(block, pat_b)) {
                    const char *cm = strstr(block, "\"cosine_mean\":");
                    if (cm) {
                        cm += strlen("\"cosine_mean\":");
                        while (*cm == ' ') cm++;
                        return atof(cm);
                    }
                }
            }
            p++;
        }
    }
    return -1.0;
}

typedef struct {
    char id[128];
    char family[64];
    char geometry[64];
    char condition[256];
    double mean_f1;
} RouteCandidate;

static int has_suffix(const char *s, const char *suffix) {
    if (!s || !suffix) return 0;
    size_t slen = strlen(s);
    size_t xlen = strlen(suffix);
    return slen >= xlen && strcmp(s + slen - xlen, suffix) == 0;
}

static char *read_small_text_file(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return NULL; }
    long sz = ftell(f);
    if (sz < 0 || sz > MAX_JSON) { fclose(f); return NULL; }
    if (fseek(f, 0, SEEK_SET) != 0) { fclose(f); return NULL; }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t n = fread(buf, 1, (size_t)sz, f);
    fclose(f);
    buf[n] = '\0';
    return buf;
}

static int json_extract_text_field(const char *json, const char *key,
                                   char *out, size_t out_sz) {
    if (!json || !key || !out || out_sz == 0) return 0;
    out[0] = '\0';

    char pat[128];
    snprintf(pat, sizeof(pat), "\"%s\"", key);
    const char *p = strstr(json, pat);
    if (!p) return 0;

    p = strchr(p, ':');
    if (!p) return 0;
    p++;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    if (*p != '"') return 0;
    p++;

    size_t i = 0;
    while (*p && i + 1 < out_sz) {
        if (*p == '\\' && p[1]) {
            p++;
            out[i++] = *p++;
            continue;
        }
        if (*p == '"') break;
        out[i++] = *p++;
    }
    out[i] = '\0';
    return out[0] != '\0';
}

static double json_extract_number_field(const char *json, const char *key,
                                        double fallback) {
    if (!json || !key) return fallback;

    char pat[128];
    snprintf(pat, sizeof(pat), "\"%s\"", key);
    const char *p = strstr(json, pat);
    if (!p) return fallback;

    p = strchr(p, ':');
    if (!p) return fallback;
    p++;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;

    char num[64];
    size_t i = 0;
    if (*p == '"') {
        p++;
        while (*p && *p != '"' && i + 1 < sizeof(num)) num[i++] = *p++;
    } else {
        while (*p && *p != ',' && *p != '}' && *p != ']' &&
               *p != '\n' && *p != '\r' && i + 1 < sizeof(num)) {
            num[i++] = *p++;
        }
    }
    num[i] = '\0';
    return i > 0 ? atof(num) : fallback;
}

static int load_route_candidates_from_dir(const char *dir,
                                          RouteCandidate *out,
                                          int max_candidates) {
    DIR *d = opendir(dir);
    if (!d) return 0;

    int count = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL && count < max_candidates) {
        if (de->d_name[0] != 'T' || !has_suffix(de->d_name, ".json")) continue;

        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s", dir, de->d_name);
        char *json = read_small_text_file(path);
        if (!json) continue;

        RouteCandidate c;
        memset(&c, 0, sizeof(c));
        json_extract_text_field(json, "transform_family", c.family, sizeof(c.family));
        if (!c.family[0]) {
            free(json);
            continue;
        }

        c.mean_f1 = json_extract_number_field(json, "mean_f1", 0.0);
        if (c.mean_f1 <= 0.0) {
            free(json);
            continue;
        }

        if (!json_extract_text_field(json, "code", c.id, sizeof(c.id))) {
            if (!json_extract_text_field(json, "recipe_id", c.id, sizeof(c.id))) {
                snprintf(c.id, sizeof(c.id), "%s", c.family);
            }
        }
        json_extract_text_field(json, "geometry", c.geometry, sizeof(c.geometry));
        json_extract_text_field(json, "geometry_condition", c.condition, sizeof(c.condition));

        out[count++] = c;
        free(json);
    }

    closedir(d);
    return count;
}

static int load_route_candidates(RouteCandidate *out, int max_candidates) {
    static const char *dirs[] = { "recipes", "../recipes", "../../recipes", NULL };
    int count = 0;
    for (int i = 0; dirs[i] && count < max_candidates; i++) {
        count += load_route_candidates_from_dir(dirs[i], out + count, max_candidates - count);
        if (count > 0) break;
    }
    return count;
}

static void consider_route_candidate(const char *id, const char *fam,
                                     const char *geom, const char *cond,
                                     double f1, const char *stats_json,
                                     int use_frontier,
                                     const char *frontier_json,
                                     const char *from_family,
                                     double frontier_weight,
                                     char *best_id, size_t best_id_sz,
                                     char *best_family, size_t best_family_sz,
                                     char *best_geometry, size_t best_geometry_sz,
                                     double *best_score,
                                     double *best_f1,
                                     double *best_cos) {
    if (!fam || !fam[0]) return;
    if (!eval_condition(cond, stats_json)) return;

    double cos_val = -1.0;
    double score = f1;
    if (use_frontier) {
        cos_val = frontier_cosine(frontier_json, from_family, fam);
        if (cos_val >= 0.0)
            score = (1.0 - frontier_weight) * f1 + frontier_weight * cos_val;
    }

    if (score > *best_score) {
        *best_score = score;
        *best_f1 = f1;
        *best_cos = cos_val;
        snprintf(best_id, best_id_sz, "%s", id ? id : "");
        snprintf(best_family, best_family_sz, "%s", fam);
        snprintf(best_geometry, best_geometry_sz, "%s", geom ? geom : "");
    }
}

static int consider_catalog_route_candidates(const char *stats_json,
                                             int use_frontier,
                                             const char *frontier_json,
                                             const char *from_family,
                                             double frontier_weight,
                                             char *best_id, size_t best_id_sz,
                                             char *best_family, size_t best_family_sz,
                                             char *best_geometry, size_t best_geometry_sz,
                                             double *best_score,
                                             double *best_f1,
                                             double *best_cos) {
    sqlite3 *catalog = NULL;
    sqlite3_stmt *st = NULL;
    char catalog_path[PATH_MAX];

    if (!open_catalog(&catalog, catalog_path, sizeof(catalog_path))) return 0;
    if (sqlite3_prepare_v2(catalog,
        "SELECT external_id, category, json_data "
        "FROM catalog_nodes WHERE kind='model' ORDER BY external_id",
        -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(catalog);
        return 0;
    }

    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(st, 0);
        const char *family = (const char *)sqlite3_column_text(st, 1);
        const char *json = (const char *)sqlite3_column_text(st, 2);
        char geometry[64] = "";
        char condition[256] = "";
        double f1 = 0.0;
        if (!json) continue;
        bf_json_str(json, "geometry", geometry, sizeof(geometry));
        bf_json_str(json, "geometry_condition", condition, sizeof(condition));
        bf_json_double(json, "mean_f1", &f1);
        consider_route_candidate(id,
                                 family,
                                 geometry,
                                 condition,
                                 f1,
                                 stats_json,
                                 use_frontier,
                                 frontier_json,
                                 from_family,
                                 frontier_weight,
                                 best_id, best_id_sz,
                                 best_family, best_family_sz,
                                 best_geometry, best_geometry_sz,
                                 best_score,
                                 best_f1,
                                 best_cos);
    }

    sqlite3_finalize(st);
    sqlite3_close(catalog);
    return 1;
}

static int cmd_route(sqlite3 *db, const char *stats_path,
                     const char *frontier_path, const char *from_family,
                     const char *weights_path) {
    /* read stats JSON */
    char *stats_json = NULL;
    if(stats_path) {
        FILE *f = fopen(stats_path, "r");
        if(!f) { fprintf(stderr,"error: cannot open %s\n", stats_path); return 1; }
        fseek(f,0,SEEK_END); long sz=ftell(f); fseek(f,0,SEEK_SET);
        stats_json = malloc((size_t)sz+1);
        if(!stats_json) { fclose(f); return 1; }
        fread(stats_json, 1, (size_t)sz, f); fclose(f);
        stats_json[sz]='\0';
    }

    /* optionally load frontier JSON for cosine-weighted scoring */
    char *frontier_json = NULL;
    if (frontier_path) {
        FILE *ff = fopen(frontier_path, "r");
        if (ff) {
            fseek(ff,0,SEEK_END); long fsz=ftell(ff); fseek(ff,0,SEEK_SET);
            frontier_json = malloc((size_t)fsz+1);
            if (frontier_json) {
                fread(frontier_json, 1, (size_t)fsz, ff);
                frontier_json[fsz] = '\0';
            }
            fclose(ff);
        }
    }
    int   use_frontier = (frontier_json && from_family && from_family[0]);
    double FRONTIER_W = 0.30; /* default: 70% f1, 30% cosine_mean */

    /* optionally load routing_weights.json to override FRONTIER_W */
    if (weights_path) {
        FILE *wf = fopen(weights_path, "r");
        if (wf) {
            fseek(wf,0,SEEK_END); long wsz=ftell(wf); fseek(wf,0,SEEK_SET);
            char *wjson = malloc((size_t)wsz+1);
            if (wjson) {
                fread(wjson, 1, (size_t)wsz, wf);
                wjson[wsz] = '\0';
                /* parse "cosine_weight": N.NN */
                const char *cw = strstr(wjson, "\"cosine_weight\":");
                if (cw) {
                    cw += strlen("\"cosine_weight\":");
                    while (*cw == ' ') cw++;
                    double loaded = atof(cw);
                    if (loaded >= 0.0 && loaded <= 1.0)
                        FRONTIER_W = loaded;
                }
                free(wjson);
            }
            fclose(wf);
        }
    }

    char bid[256]={0}, bfam[64]={0}, bgeom[64]={0};
    double best_score = -1.0, best_f1 = -1.0, best_cos = -1.0;

    if (!consider_catalog_route_candidates(stats_json,
                                           use_frontier,
                                           frontier_json,
                                           from_family,
                                           FRONTIER_W,
                                           bid, sizeof(bid),
                                           bfam, sizeof(bfam),
                                           bgeom, sizeof(bgeom),
                                           &best_score,
                                           &best_f1,
                                           &best_cos)) {
        sqlite3_stmt *st;
        sqlite3_prepare_v2(db,
            "SELECT id, transform_family, geometry, geometry_condition, mean_f1 "
            "FROM models WHERE transform_family IS NOT NULL "
            "ORDER BY mean_f1 DESC",
            -1, &st, NULL);

        while(sqlite3_step(st)==SQLITE_ROW) {
            const char *id   = (const char*)sqlite3_column_text(st,0);
            const char *fam  = (const char*)sqlite3_column_text(st,1);
            const char *geom = (const char*)sqlite3_column_text(st,2);
            const char *cond = (const char*)sqlite3_column_text(st,3);
            double f1        = sqlite3_column_double(st,4);
            consider_route_candidate(id, fam, geom, cond, f1, stats_json,
                                     use_frontier, frontier_json, from_family,
                                     FRONTIER_W, bid, sizeof(bid), bfam,
                                     sizeof(bfam), bgeom, sizeof(bgeom),
                                     &best_score, &best_f1, &best_cos);
        }
        sqlite3_finalize(st);
    }

    if (!bid[0]) {
        RouteCandidate candidates[128];
        int n_candidates = load_route_candidates(candidates, 128);
        for (int i = 0; i < n_candidates; i++) {
            char rid[160];
            snprintf(rid, sizeof(rid), "recipe:%s", candidates[i].id);
            consider_route_candidate(rid,
                                     candidates[i].family,
                                     candidates[i].geometry,
                                     candidates[i].condition,
                                     candidates[i].mean_f1,
                                     stats_json,
                                     use_frontier,
                                     frontier_json,
                                     from_family,
                                     FRONTIER_W,
                                     bid, sizeof(bid),
                                     bfam, sizeof(bfam),
                                     bgeom, sizeof(bgeom),
                                     &best_score,
                                     &best_f1,
                                     &best_cos);
        }
    }

    free(stats_json);
    free(frontier_json);

    if (!bid[0]) {
        fprintf(stderr,"route: no eligible transform family found\n"); return 1;
    }
    if (use_frontier && best_cos >= 0.0) {
        printf("model_id=%s family=%s geometry=%s mean_f1=%.3f cosine_bias=%.4f\n",
               bid, bfam, bgeom, best_f1, best_cos);
    } else {
        printf("model_id=%s family=%s geometry=%s mean_f1=%.3f\n",
               bid, bfam, bgeom, best_f1);
    }
    return 0;
}

static int cmd_status(sqlite3 *db) {
    char db_path[PATH_MAX]; get_db_path(db_path, sizeof(db_path));
    char cache_dir[PATH_MAX]; get_cache_dir(cache_dir, sizeof(cache_dir));
    sqlite3 *catalog = NULL;
    sqlite3_stmt *cst = NULL;
    char catalog_path[PATH_MAX];

    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM models",-1,&st,NULL);
    sqlite3_step(st); int nmodel = sqlite3_column_int(st,0); sqlite3_finalize(st);
    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM sources",-1,&st,NULL);
    sqlite3_step(st); int nsrc   = sqlite3_column_int(st,0); sqlite3_finalize(st);
    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM recipe_models",-1,&st,NULL);
    sqlite3_step(st); int nrec   = sqlite3_column_int(st,0); sqlite3_finalize(st);

    /* Count cached files */
    int ncached=0; double total_mb=0;
    char cache_dir2[PATH_MAX]; get_cache_dir(cache_dir2, sizeof(cache_dir2));
    sqlite3_prepare_v2(db,"SELECT id,format,size_mb FROM models",-1,&st,NULL);
    while(sqlite3_step(st)==SQLITE_ROW) {
        const char *id  = (const char*)sqlite3_column_text(st,0);
        double sz = sqlite3_column_double(st,2);
        if(cache_hit(id,NULL,cache_dir2,NULL,0)) { ncached++; total_mb+=sz; }
    }
    sqlite3_finalize(st);

    char total_s[16]; fmt_size(total_mb, total_s, sizeof(total_s));

    printf("bonfyre-model %s\n\n", VERSION);
    printf("  DB:            %s\n", db_path);
    printf("  Cache:         %s\n", cache_dir);
    printf("  Models:        %d registered\n", nmodel);
    printf("  Sources:       %d configured\n", nsrc);
    printf("  Recipe links:  %d\n", nrec);
    printf("  Cached:        %d / %d  (%s on disk)\n",
           ncached, nmodel, total_s);

    if (open_catalog(&catalog, catalog_path, sizeof(catalog_path))) {
        sqlite3_prepare_v2(catalog, "SELECT COUNT(*) FROM catalog_nodes WHERE kind='model'", -1, &cst, NULL);
        if (sqlite3_step(cst) == SQLITE_ROW)
            printf("  Catalog:       %s (%d indexed)\n", catalog_path, sqlite3_column_int(cst, 0));
        sqlite3_finalize(cst);
        sqlite3_close(catalog);
    }
    return 0;
}

/* ====================================================================
 * main
 * ==================================================================== */
int main(int argc, char **argv) {
    if(argc < 2 || strcmp(argv[1],"help")==0 || strcmp(argv[1],"--help")==0) {
        cmd_help(); return 0;
    }

    char db_path[PATH_MAX]; get_db_path(db_path, sizeof(db_path));
    sqlite3 *db = db_open(db_path);
    if(!db) return 1;

    /* Always seed built-ins (INSERT OR IGNORE — idempotent) */
    seed_builtins(db);

    const char *cmd = argv[1];
    int ret = 0;

    if(strcmp(cmd,"list")==0) {
        ret = cmd_list(db);

    } else if(strcmp(cmd,"show")==0) {
        if(argc < 3) { fprintf(stderr,"usage: bonfyre-model show <id>\n"); ret=1; }
        else ret = cmd_show(db, argv[2]);

    } else if(strcmp(cmd,"pull")==0) {
        if(argc < 3) {
            fprintf(stderr,"usage: bonfyre-model pull <id> | --recipe <code>\n"); ret=1;
        } else if(strcmp(argv[2],"--recipe")==0) {
            if(argc < 4) { fprintf(stderr,"usage: bonfyre-model pull --recipe <code>\n"); ret=1; }
            else ret = cmd_pull_recipe(db, argv[3]);
        } else {
            ret = cmd_pull(db, argv[2]);
        }

    } else if(strcmp(cmd,"add")==0) {
        if(argc < 3) { fprintf(stderr,"usage: bonfyre-model add <manifest.json>\n"); ret=1; }
        else ret = cmd_add(db, argv[2]);

    } else if(strcmp(cmd,"verify")==0) {
        if(argc < 3) { fprintf(stderr,"usage: bonfyre-model verify <id>\n"); ret=1; }
        else ret = cmd_verify(db, argv[2]);

    } else if(strcmp(cmd,"path")==0) {
        if(argc < 3) { fprintf(stderr,"usage: bonfyre-model path <id>\n"); ret=1; }
        else ret = cmd_path(db, argv[2]);

    } else if(strcmp(cmd,"rm")==0) {
        int purge = 0; const char *id = NULL;
        for(int i=2;i<argc;i++) {
            if(strcmp(argv[i],"--purge")==0) purge=1;
            else id=argv[i];
        }
        if(!id) { fprintf(stderr,"usage: bonfyre-model rm [--purge] <id>\n"); ret=1; }
        else ret = cmd_rm(db, id, purge);

    } else if(strcmp(cmd,"search")==0) {
        if(argc < 3) { fprintf(stderr,"usage: bonfyre-model search <query>\n"); ret=1; }
        else ret = cmd_search(db, argv[2]);

    } else if(strcmp(cmd,"sources")==0) {
        if(argc < 3) { fprintf(stderr,"usage: bonfyre-model sources <id>\n"); ret=1; }
        else ret = cmd_sources(db, argv[2]);

    } else if(strcmp(cmd,"source")==0) {
        if(argc < 4) { fprintf(stderr,"usage: bonfyre-model source add|rm <id> <url>\n"); ret=1; }
        else if(strcmp(argv[2],"add")==0) {
            if(argc < 5) { fprintf(stderr,"usage: bonfyre-model source add <id> <url>\n"); ret=1; }
            else ret = cmd_source_add(db, argv[3], argv[4]);
        } else if(strcmp(argv[2],"rm")==0) {
            if(argc < 5) { fprintf(stderr,"usage: bonfyre-model source rm <id> <url>\n"); ret=1; }
            else ret = cmd_source_rm(db, argv[3], argv[4]);
        } else {
            fprintf(stderr,"unknown source subcommand: %s\n", argv[2]); ret=1;
        }

    } else if(strcmp(cmd,"family")==0) {
        ret = cmd_family(db, argc >= 3 ? argv[2] : NULL);

    } else if(strcmp(cmd,"route")==0) {
        if(argc < 3) {
            fprintf(stderr,"usage: bonfyre-model route <corpus_stats.json> "
                          "[--frontier <path>] [--from <family>] [--weights <path>]\n"); ret=1;
        } else {
            const char *frontier_path = NULL, *from_family = NULL, *weights_path = NULL;
            for(int i = 3; i < argc; i++) {
                if(strcmp(argv[i],"--frontier")==0 && i+1<argc) frontier_path = argv[++i];
                else if(strcmp(argv[i],"--from")==0 && i+1<argc) from_family = argv[++i];
                else if(strcmp(argv[i],"--weights")==0 && i+1<argc) weights_path = argv[++i];
            }
            ret = cmd_route(db, argv[2], frontier_path, from_family, weights_path);
        }

    } else if(strcmp(cmd,"ls-cache")==0) {
        ret = cmd_ls_cache();

    } else if(strcmp(cmd,"status")==0) {
        ret = cmd_status(db);

    } else if(strcmp(cmd,"push")==0) {
        const char *push_id = NULL, *push_repo = NULL;
        for(int i = 2; i < argc; i++) {
            if(strcmp(argv[i],"--repo")==0 && i+1<argc) push_repo = argv[++i];
            else push_id = argv[i];
        }
        if(!push_id || !push_repo) {
            fprintf(stderr,"usage: bonfyre-model push <id> --repo <hf-org/name>\n"); ret=1;
        } else {
            ret = cmd_push(db, push_id, push_repo);
        }

    } else if(strcmp(cmd,"version")==0 || strcmp(cmd,"--version")==0) {
        printf("bonfyre-model %s\n", VERSION);

    } else {
        fprintf(stderr,"unknown command: %s\n"
                "Run: bonfyre-model help\n", cmd);
        ret = 1;
    }

    sqlite3_close(db);
    return ret;
}
