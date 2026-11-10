/* Receipts, wherever they are kept.
 *
 * A receipt used to be a file and nothing else. It may now also be a row in
 * the pack beside the files (.receipts.pack, written by `bonfyre-project
 * foreign pack`). These three calls stand where opendir/readdir/closedir and
 * fopen stood at each place this organ reads receipts: a listing is the loose
 * names and the packed names together, each once, and opening a receipt takes
 * the loose file when there is one and the packed copy when there is not. A
 * loose file is always the newer word. Nothing here writes. */
#ifndef BF_RECEIPT_STORE_H
#define BF_RECEIPT_STORE_H
#include <dirent.h>
#include <limits.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#ifdef BONFYRE_HAVE_ZSTD
#include <zstd.h>
#endif

typedef struct {
    char **names;
    size_t count, at;
    struct dirent entry;
} BfReceiptDir;

#ifdef BONFYRE_HAVE_ZSTD
/* One pack held open for the life of the process: a listing of a hundred
 * thousand receipts opens each of them, and must not open the pack each time. */
static struct { char path[PATH_MAX]; sqlite3 *db; sqlite3_stmt *one; ZSTD_DDict *dict; ZSTD_DCtx *dctx; } bf_receipt_pack_held;

static int bf_receipt_pack_use(const char *dir) {
    char path[PATH_MAX];
    const char *named = getenv("BONFYRE_FOREIGN_RECEIPT_PACK");
    sqlite3_stmt *st = NULL;
    if (named && *named) snprintf(path, sizeof(path), "%s", named);
    else if (snprintf(path, sizeof(path), "%s/.receipts.pack", dir) >= (int)sizeof(path)) return 0;
    if (bf_receipt_pack_held.db && !strcmp(bf_receipt_pack_held.path, path)) return 1;
    if (bf_receipt_pack_held.db) {
        sqlite3_finalize(bf_receipt_pack_held.one); sqlite3_close(bf_receipt_pack_held.db);
        if (bf_receipt_pack_held.dict) ZSTD_freeDDict(bf_receipt_pack_held.dict);
        if (bf_receipt_pack_held.dctx) ZSTD_freeDCtx(bf_receipt_pack_held.dctx);
        memset(&bf_receipt_pack_held, 0, sizeof(bf_receipt_pack_held));
    }
    if (access(path, R_OK) != 0) return 0;
    if (sqlite3_open_v2(path, &bf_receipt_pack_held.db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { sqlite3_close(bf_receipt_pack_held.db); bf_receipt_pack_held.db = NULL; return 0; }
    if (sqlite3_prepare_v2(bf_receipt_pack_held.db, "SELECT value FROM meta WHERE key='dictionary'", -1, &st, NULL) == SQLITE_OK &&
        sqlite3_step(st) == SQLITE_ROW && sqlite3_column_bytes(st, 0) > 0)
        bf_receipt_pack_held.dict = ZSTD_createDDict(sqlite3_column_blob(st, 0), (size_t)sqlite3_column_bytes(st, 0));
    if (st) sqlite3_finalize(st);
    bf_receipt_pack_held.dctx = ZSTD_createDCtx();
    if (!bf_receipt_pack_held.dctx ||
        sqlite3_prepare_v2(bf_receipt_pack_held.db, "SELECT bytes, z FROM receipt WHERE name=?1", -1, &bf_receipt_pack_held.one, NULL) != SQLITE_OK) {
        sqlite3_close(bf_receipt_pack_held.db); memset(&bf_receipt_pack_held, 0, sizeof(bf_receipt_pack_held)); return 0;
    }
    snprintf(bf_receipt_pack_held.path, sizeof(bf_receipt_pack_held.path), "%s", path);
    return 1;
}
#endif

static int bf_receipt_name_order(const void *a, const void *b) { return strcmp(*(char *const *)a, *(char *const *)b); }

static BfReceiptDir *bf_receipt_opendir(const char *dir) {
    BfReceiptDir *out = calloc(1, sizeof(*out));
    size_t cap = 0, loose = 0;
    DIR *d = opendir(dir);
    struct dirent *e;
    if (!out) { if (d) closedir(d); return NULL; }
    while (d && (e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        if (out->count == cap) { size_t grow = cap ? cap * 2 : 256; char **bigger = realloc(out->names, grow * sizeof(*bigger)); if (!bigger) break; out->names = bigger; cap = grow; }
        if ((out->names[out->count] = strdup(e->d_name))) out->count++;
    }
    if (d) closedir(d);
    loose = out->count;
    if (loose) qsort(out->names, loose, sizeof(*out->names), bf_receipt_name_order);
#ifdef BONFYRE_HAVE_ZSTD
    if (bf_receipt_pack_use(dir)) {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(bf_receipt_pack_held.db, "SELECT name FROM receipt", -1, &st, NULL) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW) {
                const char *name = (const char *)sqlite3_column_text(st, 0);
                if (!name || (loose && bsearch(&name, out->names, loose, sizeof(*out->names), bf_receipt_name_order))) continue;
                if (out->count == cap) { size_t grow = cap ? cap * 2 : 256; char **bigger = realloc(out->names, grow * sizeof(*bigger)); if (!bigger) break; out->names = bigger; cap = grow; }
                if ((out->names[out->count] = strdup(name))) out->count++;
            }
            sqlite3_finalize(st);
        }
    }
#endif
    /* Neither a directory nor a pack: there is no receipt store here. */
    if (!d && !out->count) { free(out->names); free(out); return NULL; }
    return out;
}

static struct dirent *bf_receipt_readdir(BfReceiptDir *dir) {
    if (!dir || dir->at >= dir->count) return NULL;
    snprintf(dir->entry.d_name, sizeof(dir->entry.d_name), "%s", dir->names[dir->at++]);
    return &dir->entry;
}

static int bf_receipt_closedir(BfReceiptDir *dir) {
    if (!dir) return 0;
    for (size_t i = 0; i < dir->count; i++) free(dir->names[i]);
    free(dir->names); free(dir);
    return 0;
}

/* The receipt at `path`: its loose file, or its packed copy as an unnamed
 * temporary file the caller reads and closes like any other. */
static FILE *bf_receipt_fopen(const char *path, const char *mode) {
    FILE *f = fopen(path, mode);
#ifdef BONFYRE_HAVE_ZSTD
    if (!f) {
        char dir[PATH_MAX];
        const char *slash = strrchr(path, '/');
        size_t dir_len = slash ? (size_t)(slash - path) : 0;
        if (!slash || dir_len >= sizeof(dir)) return NULL;
        memcpy(dir, path, dir_len); dir[dir_len] = '\0';
        if (!bf_receipt_pack_use(dir)) return NULL;
        sqlite3_reset(bf_receipt_pack_held.one);
        sqlite3_bind_text(bf_receipt_pack_held.one, 1, slash + 1, -1, SQLITE_STATIC);
        if (sqlite3_step(bf_receipt_pack_held.one) == SQLITE_ROW) {
            size_t bytes = (size_t)sqlite3_column_int64(bf_receipt_pack_held.one, 0), got;
            char *text = malloc(bytes ? bytes : 1);
            if (text) {
                got = bf_receipt_pack_held.dict
                    ? ZSTD_decompress_usingDDict(bf_receipt_pack_held.dctx, text, bytes, sqlite3_column_blob(bf_receipt_pack_held.one, 1), (size_t)sqlite3_column_bytes(bf_receipt_pack_held.one, 1), bf_receipt_pack_held.dict)
                    : ZSTD_decompressDCtx(bf_receipt_pack_held.dctx, text, bytes, sqlite3_column_blob(bf_receipt_pack_held.one, 1), (size_t)sqlite3_column_bytes(bf_receipt_pack_held.one, 1));
                /* A damaged row is no receipt, rather than some other text. */
                if (!ZSTD_isError(got) && got == bytes && (f = tmpfile())) {
                    if (fwrite(text, 1, bytes, f) != bytes) { fclose(f); f = NULL; } else rewind(f);
                }
                free(text);
            }
        }
        sqlite3_reset(bf_receipt_pack_held.one);
    }
#endif
    return f;
}
#endif
