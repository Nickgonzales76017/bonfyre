/*
 * bf_lmdb.c — LMDB zero-copy artifact cache
 *
 * Wraps LMDB (lmdb.h) with Bonfyre semantics:
 *   - Named sub-databases (artifacts, families, blobs, meta)
 *   - Zero-copy reads via mmap pointer returns
 *   - Batch writer for pipeline bulk inserts
 *
 * LMDB guarantees:
 *   - Readers never block writers, writers never block readers
 *   - Crash-safe (copy-on-write B+ tree)
 *   - Zero-copy: MDB_val.mv_data points into mmap
 */

#ifdef __has_include
#if __has_include(<lmdb.h>)
#define BF_HAS_LMDB 1
#endif
#endif

#ifdef BF_HAS_LMDB
#include <lmdb.h>
#endif

#include "bf_lmdb.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/* ── Internal structures ─────────────────────────────────────── */

struct bf_lmdb {
#ifdef BF_HAS_LMDB
    MDB_env    *env;
    MDB_dbi     dbi_artifacts;
    MDB_dbi     dbi_families;
    MDB_dbi     dbi_blobs;
    MDB_dbi     dbi_meta;
    int         dbi_opened;   /* Bitmask of opened DBIs */
#endif
    char        path[4096];
    int         readonly;
};

struct bf_lmdb_txn {
#ifdef BF_HAS_LMDB
    MDB_txn    *txn;
#endif
    bf_lmdb_t  *db;
};

struct bf_lmdb_cursor {
#ifdef BF_HAS_LMDB
    MDB_cursor *cursor;
    MDB_txn    *txn;
#endif
    int         started;
};

struct bf_lmdb_writer {
#ifdef BF_HAS_LMDB
    MDB_txn    *txn;
#endif
    bf_lmdb_t  *db;
};

#ifdef BF_HAS_LMDB

/* ── DBI resolution ──────────────────────────────────────────── */

static MDB_dbi resolve_dbi(bf_lmdb_t *db, const char *db_name) {
    if (!db_name || strcmp(db_name, BF_LMDB_DB_ARTIFACTS) == 0)
        return db->dbi_artifacts;
    if (strcmp(db_name, BF_LMDB_DB_FAMILIES) == 0) return db->dbi_families;
    if (strcmp(db_name, BF_LMDB_DB_BLOBS) == 0) return db->dbi_blobs;
    if (strcmp(db_name, BF_LMDB_DB_META) == 0) return db->dbi_meta;
    return db->dbi_artifacts;
}

static int open_dbis(bf_lmdb_t *db) {
    if (db->dbi_opened) return 0;

    MDB_txn *txn;
    unsigned int flags = db->readonly ? 0 : MDB_CREATE;
    int rc = mdb_txn_begin(db->env, NULL, 0, &txn);
    if (rc != 0) return -1;

    rc = mdb_dbi_open(txn, BF_LMDB_DB_ARTIFACTS, flags, &db->dbi_artifacts);
    if (rc != 0) { mdb_txn_abort(txn); return -1; }

    rc = mdb_dbi_open(txn, BF_LMDB_DB_FAMILIES, flags, &db->dbi_families);
    if (rc != 0) { mdb_txn_abort(txn); return -1; }

    rc = mdb_dbi_open(txn, BF_LMDB_DB_BLOBS, flags, &db->dbi_blobs);
    if (rc != 0) { mdb_txn_abort(txn); return -1; }

    rc = mdb_dbi_open(txn, BF_LMDB_DB_META, flags, &db->dbi_meta);
    if (rc != 0) { mdb_txn_abort(txn); return -1; }

    mdb_txn_commit(txn);
    db->dbi_opened = 1;
    return 0;
}

/* ── Public API ──────────────────────────────────────────────── */

bf_lmdb_t *bf_lmdb_open(const char *path, size_t map_size, int readonly) {
    bf_lmdb_t *db = calloc(1, sizeof(*db));
    if (!db) return NULL;

    snprintf(db->path, sizeof(db->path), "%s", path);
    db->readonly = readonly;

    /* Ensure directory exists */
    mkdir(path, 0755);

    int rc = mdb_env_create(&db->env);
    if (rc != 0) { free(db); return NULL; }

    mdb_env_set_maxdbs(db->env, BF_LMDB_MAX_DBS);
    mdb_env_set_mapsize(db->env, map_size > 0 ? map_size : BF_LMDB_MAP_SIZE);

    unsigned int env_flags = MDB_NOSUBDIR | MDB_NOSYNC;
    if (readonly) env_flags |= MDB_RDONLY;

    rc = mdb_env_open(db->env, path, env_flags, 0644);
    if (rc != 0) {
        mdb_env_close(db->env);
        free(db);
        return NULL;
    }

    if (open_dbis(db) != 0) {
        mdb_env_close(db->env);
        free(db);
        return NULL;
    }

    return db;
}

void bf_lmdb_close(bf_lmdb_t *db) {
    if (!db) return;
    mdb_env_close(db->env);
    free(db);
}

/* ── Read transactions ───────────────────────────────────────── */

bf_lmdb_txn_t *bf_lmdb_txn_begin(bf_lmdb_t *db) {
    if (!db) return NULL;

    bf_lmdb_txn_t *txn = calloc(1, sizeof(*txn));
    if (!txn) return NULL;
    txn->db = db;

    int rc = mdb_txn_begin(db->env, NULL, MDB_RDONLY, &txn->txn);
    if (rc != 0) { free(txn); return NULL; }

    return txn;
}

void bf_lmdb_txn_end(bf_lmdb_txn_t *txn) {
    if (!txn) return;
    mdb_txn_abort(txn->txn);  /* Read-only: abort == commit */
    free(txn);
}

int bf_lmdb_get(bf_lmdb_txn_t *txn, const char *db_name,
                 const void *key, size_t key_len,
                 const void **val_out, size_t *val_len_out) {
    if (!txn) return -1;

    MDB_dbi dbi = resolve_dbi(txn->db, db_name);
    MDB_val mkey = { .mv_size = key_len, .mv_data = (void *)key };
    MDB_val mval = {0};

    int rc = mdb_get(txn->txn, dbi, &mkey, &mval);
    if (rc != 0) return -1;

    /* Zero-copy: pointer directly into mmap */
    if (val_out) *val_out = mval.mv_data;
    if (val_len_out) *val_len_out = mval.mv_size;

    return 0;
}

/* ── Write ───────────────────────────────────────────────────── */

int bf_lmdb_put(bf_lmdb_t *db, const char *db_name,
                 const void *key, size_t key_len,
                 const void *val, size_t val_len) {
    if (!db || db->readonly) return -1;

    MDB_txn *txn;
    int rc = mdb_txn_begin(db->env, NULL, 0, &txn);
    if (rc != 0) return -1;

    MDB_dbi dbi = resolve_dbi(db, db_name);
    MDB_val mkey = { .mv_size = key_len, .mv_data = (void *)key };
    MDB_val mval = { .mv_size = val_len, .mv_data = (void *)val };

    rc = mdb_put(txn, dbi, &mkey, &mval, 0);
    if (rc != 0) { mdb_txn_abort(txn); return -1; }

    return mdb_txn_commit(txn) == 0 ? 0 : -1;
}

/* ── Batch writer ────────────────────────────────────────────── */

bf_lmdb_writer_t *bf_lmdb_batch_begin(bf_lmdb_t *db) {
    if (!db || db->readonly) return NULL;

    bf_lmdb_writer_t *w = calloc(1, sizeof(*w));
    if (!w) return NULL;
    w->db = db;

    int rc = mdb_txn_begin(db->env, NULL, 0, &w->txn);
    if (rc != 0) { free(w); return NULL; }

    return w;
}

int bf_lmdb_batch_put(bf_lmdb_writer_t *w, const char *db_name,
                       const void *key, size_t key_len,
                       const void *val, size_t val_len) {
    if (!w) return -1;

    MDB_dbi dbi = resolve_dbi(w->db, db_name);
    MDB_val mkey = { .mv_size = key_len, .mv_data = (void *)key };
    MDB_val mval = { .mv_size = val_len, .mv_data = (void *)val };

    return mdb_put(w->txn, dbi, &mkey, &mval, 0) == 0 ? 0 : -1;
}

int bf_lmdb_batch_commit(bf_lmdb_writer_t *w) {
    if (!w) return -1;
    int rc = mdb_txn_commit(w->txn);
    free(w);
    return rc == 0 ? 0 : -1;
}

void bf_lmdb_batch_abort(bf_lmdb_writer_t *w) {
    if (!w) return;
    mdb_txn_abort(w->txn);
    free(w);
}

/* ── Delete ──────────────────────────────────────────────────── */

int bf_lmdb_del(bf_lmdb_t *db, const char *db_name,
                 const void *key, size_t key_len) {
    if (!db || db->readonly) return -1;

    MDB_txn *txn;
    int rc = mdb_txn_begin(db->env, NULL, 0, &txn);
    if (rc != 0) return -1;

    MDB_dbi dbi = resolve_dbi(db, db_name);
    MDB_val mkey = { .mv_size = key_len, .mv_data = (void *)key };

    rc = mdb_del(txn, dbi, &mkey, NULL);
    if (rc != 0) { mdb_txn_abort(txn); return -1; }

    return mdb_txn_commit(txn) == 0 ? 0 : -1;
}

/* ── Cursor ──────────────────────────────────────────────────── */

bf_lmdb_cursor_t *bf_lmdb_cursor_open(bf_lmdb_txn_t *txn, const char *db_name) {
    if (!txn) return NULL;

    bf_lmdb_cursor_t *cur = calloc(1, sizeof(*cur));
    if (!cur) return NULL;

    MDB_dbi dbi = resolve_dbi(txn->db, db_name);
    int rc = mdb_cursor_open(txn->txn, dbi, &cur->cursor);
    if (rc != 0) { free(cur); return NULL; }

    return cur;
}

int bf_lmdb_cursor_next(bf_lmdb_cursor_t *cur,
                          const void **key_out, size_t *key_len_out,
                          const void **val_out, size_t *val_len_out) {
    if (!cur) return -1;

    MDB_val mkey, mval;
    MDB_cursor_op op = cur->started ? MDB_NEXT : MDB_FIRST;
    cur->started = 1;

    int rc = mdb_cursor_get(cur->cursor, &mkey, &mval, op);
    if (rc != 0) return -1;

    if (key_out) *key_out = mkey.mv_data;
    if (key_len_out) *key_len_out = mkey.mv_size;
    if (val_out) *val_out = mval.mv_data;
    if (val_len_out) *val_len_out = mval.mv_size;

    return 0;
}

int bf_lmdb_cursor_seek(bf_lmdb_cursor_t *cur,
                          const void *target, size_t target_len) {
    if (!cur) return -1;

    MDB_val mkey = { .mv_size = target_len, .mv_data = (void *)target };
    MDB_val mval;

    int rc = mdb_cursor_get(cur->cursor, &mkey, &mval, MDB_SET_RANGE);
    cur->started = 1;
    return rc == 0 ? 0 : -1;
}

void bf_lmdb_cursor_close(bf_lmdb_cursor_t *cur) {
    if (!cur) return;
    mdb_cursor_close(cur->cursor);
    free(cur);
}

/* ── Stats ───────────────────────────────────────────────────── */

bf_lmdb_stat_t bf_lmdb_stats(bf_lmdb_t *db) {
    bf_lmdb_stat_t s = {0};
    if (!db) return s;

    MDB_envinfo info;
    MDB_stat stat;
    mdb_env_info(db->env, &info);
    mdb_env_stat(db->env, &stat);

    s.map_size = info.me_mapsize;
    s.pages_used = (size_t)stat.ms_branch_pages + (size_t)stat.ms_leaf_pages +
                    (size_t)stat.ms_overflow_pages;
    s.page_size = stat.ms_psize;
    s.entries = (int)stat.ms_entries;

    return s;
}

#else /* !BF_HAS_LMDB */

/* ── Stub implementations when LMDB is not available ─────────── */

bf_lmdb_t *bf_lmdb_open(const char *path, size_t map_size, int readonly) {
    (void)path; (void)map_size; (void)readonly;
    fprintf(stderr, "[bf_lmdb] LMDB not available (install liblmdb-dev)\n");
    return NULL;
}

void bf_lmdb_close(bf_lmdb_t *db) { (void)db; }

bf_lmdb_txn_t *bf_lmdb_txn_begin(bf_lmdb_t *db) { (void)db; return NULL; }
void bf_lmdb_txn_end(bf_lmdb_txn_t *txn) { (void)txn; }

int bf_lmdb_get(bf_lmdb_txn_t *txn, const char *db_name,
                 const void *key, size_t key_len,
                 const void **val_out, size_t *val_len_out) {
    (void)txn; (void)db_name; (void)key; (void)key_len;
    (void)val_out; (void)val_len_out;
    return -1;
}

int bf_lmdb_put(bf_lmdb_t *db, const char *db_name,
                 const void *key, size_t key_len,
                 const void *val, size_t val_len) {
    (void)db; (void)db_name; (void)key; (void)key_len;
    (void)val; (void)val_len;
    return -1;
}

bf_lmdb_writer_t *bf_lmdb_batch_begin(bf_lmdb_t *db) { (void)db; return NULL; }
int bf_lmdb_batch_put(bf_lmdb_writer_t *w, const char *db_name,
                       const void *key, size_t key_len,
                       const void *val, size_t val_len) {
    (void)w; (void)db_name; (void)key; (void)key_len; (void)val; (void)val_len;
    return -1;
}
int bf_lmdb_batch_commit(bf_lmdb_writer_t *w) { (void)w; return -1; }
void bf_lmdb_batch_abort(bf_lmdb_writer_t *w) { (void)w; }

int bf_lmdb_del(bf_lmdb_t *db, const char *db_name,
                 const void *key, size_t key_len) {
    (void)db; (void)db_name; (void)key; (void)key_len;
    return -1;
}

bf_lmdb_cursor_t *bf_lmdb_cursor_open(bf_lmdb_txn_t *txn, const char *db_name) {
    (void)txn; (void)db_name; return NULL;
}
int bf_lmdb_cursor_next(bf_lmdb_cursor_t *cur,
                          const void **key_out, size_t *key_len_out,
                          const void **val_out, size_t *val_len_out) {
    (void)cur; (void)key_out; (void)key_len_out; (void)val_out; (void)val_len_out;
    return -1;
}
int bf_lmdb_cursor_seek(bf_lmdb_cursor_t *cur,
                          const void *target, size_t target_len) {
    (void)cur; (void)target; (void)target_len; return -1;
}
void bf_lmdb_cursor_close(bf_lmdb_cursor_t *cur) { (void)cur; }

bf_lmdb_stat_t bf_lmdb_stats(bf_lmdb_t *db) {
    (void)db;
    bf_lmdb_stat_t s = {0};
    return s;
}

#endif /* BF_HAS_LMDB */
