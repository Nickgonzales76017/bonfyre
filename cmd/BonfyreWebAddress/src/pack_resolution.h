#ifndef BONFYRE_PACK_RESOLUTION_H
#define BONFYRE_PACK_RESOLUTION_H
/* PackGraph resolution shared by its WebAddress reader and CMS write owner.
 * No content fetches, grants, jobs or domain records are created by resolution. */
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define BFPG_MAX 64
#define BFPG_BYTES 262144

typedef struct {
    char pack[96], version[64], root[128], label[192], layer[24], kind[24];
    unsigned visiting;
} BfPackResolved;
typedef struct {
    BfPackResolved nodes[BFPG_MAX];
    size_t count;
    char digest[65], error[64], subject[96];
} BfPackResolution;
static int bfpg_token(const char *s, size_t cap) {
    if (!s || !*s || strlen(s) >= cap) return 0;
    return strspn(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:") == strlen(s);
}
static int bfpg_failure(BfPackResolution *p, const char *why, const char *subject) {
    if (!p->error[0]) {
        snprintf(p->error, sizeof(p->error), "%s", why);
        snprintf(p->subject, sizeof(p->subject), "%s", subject ? subject : "");
    }
    return 0;
}
static int bfpg_find(const BfPackResolution *p, const char *pack) {
    for (size_t i = 0; i < p->count; i++) if (!strcmp(p->nodes[i].pack, pack)) return (int)i;
    return -1;
}
static int bfpg_text(sqlite3_stmt *s, int col, char *out, size_t cap) {
    const char *value = (const char *)sqlite3_column_text(s, col);
    size_t len = value ? strlen(value) : 0;
    if (!value || len >= cap || len != (size_t)sqlite3_column_bytes(s, col)) return 0;
    memcpy(out, value, len + 1); return 1;
}
static int bfpg_walk(sqlite3 *db, BfPackResolution *p, const BfPackResolution *lock,
                     const char *pack, const char *pin, const char *channel, unsigned depth) {
    sqlite3_stmt *s = NULL;
    char version[64] = "";
    int step, index, ok = 0;
    if (!bfpg_token(pack, 96) || (pin && *pin && !bfpg_token(pin, 64)) ||
        !bfpg_token(channel && *channel ? channel : "stable", 64))
        return bfpg_failure(p, "invalid_coordinate", pack);
    if (depth > 16) return bfpg_failure(p, "dependency_depth", pack);
    if (lock) {
        index = bfpg_find(lock, pack);
        if (index < 0) return bfpg_failure(p, "lock_missing_dependency", pack);
        snprintf(version, sizeof(version), "%s", lock->nodes[index].version);
        if (pin && *pin && strcmp(pin, version)) return bfpg_failure(p, "version_conflict", pack);
    } else if (pin && *pin) snprintf(version, sizeof(version), "%s", pin);
    else {
        if (sqlite3_prepare_v2(db, "SELECT version FROM pack_head WHERE namespace='root' AND pack=?1 AND channel=?2 AND status='published' ORDER BY version", -1, &s, NULL) != SQLITE_OK)
            return bfpg_failure(p, "head_unavailable", pack);
        sqlite3_bind_text(s, 1, pack, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(s, 2, channel && *channel ? channel : "stable", -1, SQLITE_TRANSIENT);
        step = sqlite3_step(s);
        if (step != SQLITE_ROW || !bfpg_text(s, 0, version, sizeof(version))) {
            sqlite3_finalize(s); return bfpg_failure(p, "missing_head", pack);
        }
        step = sqlite3_step(s); sqlite3_finalize(s); s = NULL;
        if (step != SQLITE_DONE) return bfpg_failure(p, "ambiguous_head", pack);
    }
    if (!bfpg_token(version, sizeof(version))) return bfpg_failure(p, "invalid_version", pack);
    index = bfpg_find(p, pack);
    if (index >= 0) {
        if (p->nodes[index].visiting) return bfpg_failure(p, "dependency_cycle", pack);
        return strcmp(p->nodes[index].version, version) ? bfpg_failure(p, "version_conflict", pack) : 1;
    }
    if (p->count >= BFPG_MAX) return bfpg_failure(p, "dependency_count", pack);
    index = (int)p->count++;
    BfPackResolved *n = &p->nodes[index];
    snprintf(n->pack, sizeof(n->pack), "%s", pack);
    snprintf(n->version, sizeof(n->version), "%s", version); n->visiting = 1;
    if (sqlite3_prepare_v2(db, "SELECT a.canonical_name,a.layer,a.kind,v.root FROM pack_artifact a JOIN pack_version v ON v.pack=a.pack AND v.namespace='root' AND v.version=?2 AND v.status='published' WHERE a.namespace='root' AND a.pack=?1 AND a.status='published'", -1, &s, NULL) != SQLITE_OK)
        return bfpg_failure(p, "definition_unavailable", pack);
    sqlite3_bind_text(s, 1, pack, -1, SQLITE_TRANSIENT); sqlite3_bind_text(s, 2, version, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(s) != SQLITE_ROW || !bfpg_text(s, 0, n->label, sizeof(n->label)) ||
        !bfpg_text(s, 1, n->layer, sizeof(n->layer)) || !bfpg_text(s, 2, n->kind, sizeof(n->kind)) ||
        !bfpg_text(s, 3, n->root, sizeof(n->root))) {
        sqlite3_finalize(s); return bfpg_failure(p, "missing_published_version", pack);
    }
    step = sqlite3_step(s); sqlite3_finalize(s); s = NULL;
    if (step != SQLITE_DONE) return bfpg_failure(p, "ambiguous_definition", pack);
    if (lock && strcmp(n->root, lock->nodes[bfpg_find(lock, pack)].root))
        return bfpg_failure(p, "component_root_changed", pack);
    for (const unsigned char *c = (const unsigned char *)n->label; *c; c++)
        if (*c < 32 || *c == 127) return bfpg_failure(p, "invalid_definition", pack);
    if (!bfpg_token(n->root, sizeof(n->root)) || !bfpg_token(n->layer, sizeof(n->layer)) || !bfpg_token(n->kind, sizeof(n->kind)) || strpbrk(n->label, "\r\n\t"))
        return bfpg_failure(p, "invalid_definition", pack);
    if (sqlite3_prepare_v2(db, "SELECT depends_on,COALESCE(pinned_version,''),COALESCE(NULLIF(channel,''),'stable') FROM pack_dependency WHERE namespace='root' AND pack=?1 AND status='published' ORDER BY depends_on,pinned_version,channel", -1, &s, NULL) != SQLITE_OK)
        return bfpg_failure(p, "dependencies_unavailable", pack);
    sqlite3_bind_text(s, 1, pack, -1, SQLITE_TRANSIENT);
    unsigned edges = 0;
    while ((step = sqlite3_step(s)) == SQLITE_ROW) {
        char dep[96], ver[64], chan[64];
        if (++edges > BFPG_MAX || !bfpg_text(s, 0, dep, sizeof(dep)) || !bfpg_text(s, 1, ver, sizeof(ver)) || !bfpg_text(s, 2, chan, sizeof(chan)) || !bfpg_walk(db, p, lock, dep, ver, chan, depth + 1)) goto done;
    }
    if (step != SQLITE_DONE) goto done;
    sqlite3_finalize(s); s = NULL;
    /* Overlay additions are dependencies too. They cannot evade the lock or bounds. */
    if (sqlite3_prepare_v2(db, "SELECT verb,target,COALESCE(value,'') FROM pack_overlay WHERE namespace='root' AND overlay=?1 AND status='published' ORDER BY overrides,layer,verb,target,value,why", -1, &s, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(s, 1, pack, -1, SQLITE_TRANSIENT);
    edges = 0;
    while ((step = sqlite3_step(s)) == SQLITE_ROW) {
        char verb[32], target[192], value[2048];
        if (++edges > 256 || !bfpg_text(s, 0, verb, sizeof(verb)) || !bfpg_text(s, 1, target, sizeof(target)) || !*target || !bfpg_text(s, 2, value, sizeof(value))) goto done;
        if (strcmp(verb, "hide") && strcmp(verb, "add") && strcmp(verb, "specialize") && strcmp(verb, "present") && strcmp(verb, "tighten") && strcmp(verb, "default")) {
            bfpg_failure(p, "forbidden_overlay", pack); goto done;
        }
        if (!strcmp(verb, "add") && !bfpg_walk(db, p, lock, target, "", "stable", depth + 1)) goto done;
        if (!strcmp(verb, "tighten") && !strcmp(target, "audience") &&
            strcmp(value, "self") && strcmp(value, "household") && strcmp(value, "group") && strcmp(value, "community") && strcmp(value, "public") && strcmp(value, "market") && strcmp(value, "institution")) {
            bfpg_failure(p, "invalid_audience", pack); goto done;
        }
    }
    if (step != SQLITE_DONE) goto done;
    n->visiting = 0; ok = 1;
 done:
    sqlite3_finalize(s);
    return ok ? 1 : bfpg_failure(p, "definition_read_failed", pack);
}
static int bfpg_order(const void *a, const void *b) {
    return strcmp(((const BfPackResolved *)a)->pack, ((const BfPackResolved *)b)->pack);
}
/* Length-prefix each scalar, preserving null and type. Most row IDs and
 * timestamps are excluded. Overlay IDs are retained because the existing
 * renderer uses them to order operations within a layer. */
static int bfpg_rows(sqlite3 *db, sqlite3_str *body, const char *tag, const char *sql,
                     const char *pack, const char *version) {
    sqlite3_stmt *s = NULL; unsigned rows = 0; int step, ok = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(s, 1, pack, -1, SQLITE_TRANSIENT);
    if (sqlite3_bind_parameter_count(s) > 1) sqlite3_bind_text(s, 2, version, -1, SQLITE_TRANSIENT);
    sqlite3_str_appendf(body, "%u:%s", (unsigned)strlen(tag), tag);
    while ((step = sqlite3_step(s)) == SQLITE_ROW) {
        if (++rows > 256) goto done;
        sqlite3_str_appendall(body, "R");
        for (int col = 0; col < sqlite3_column_count(s); col++) {
            const char *v = (const char *)sqlite3_column_text(s, col);
            int bytes = sqlite3_column_bytes(s, col), type = sqlite3_column_type(s, col);
            if (bytes > 16384 || (v && (size_t)bytes != strlen(v))) goto done;
            sqlite3_str_appendf(body, "%d:%d:", type, bytes);
            if (v) sqlite3_str_append(body, v, bytes);
        }
        if (sqlite3_str_length(body) > BFPG_BYTES || sqlite3_str_errcode(body) != SQLITE_OK) goto done;
    }
    if (step == SQLITE_DONE) ok = 1;
 done:
    sqlite3_finalize(s); return ok;
}
static int bfpg_compile(sqlite3 *db, const char *root, const char *version,
                        const BfPackResolution *lock, BfPackResolution *out,
                        unsigned char **image, size_t *image_bytes) {
    if(image)*image=NULL;if(image_bytes)*image_bytes=0;
    memset(out, 0, sizeof(*out));
    if (!bfpg_walk(db, out, lock, root, version, "stable", 0)) return 0;
    if (lock && out->count != lock->count) return bfpg_failure(out, "lock_changed", root);
    qsort(out->nodes, out->count, sizeof(out->nodes[0]), bfpg_order);
    sqlite3_str *body = sqlite3_str_new(db);
    if (!body) return bfpg_failure(out, "allocation_failed", root);
    sqlite3_str_appendf(body, "pack-closure-v1:%u:%s", (unsigned)strlen(root), root);
    int ok = 1;
    for (size_t i = 0; i < out->count && ok; i++) {
        BfPackResolved *n = &out->nodes[i];
        ok = bfpg_rows(db, body, "artifact", "SELECT pack,canonical_name,publisher,layer,kind,purpose,COALESCE(forks_from,''),governed_by,cannot_infer FROM pack_artifact WHERE namespace='root' AND pack=?1 AND status='published'", n->pack, n->version) &&
             bfpg_rows(db, body, "version", "SELECT pack,version,root,COALESCE(parent_root,''),changed,COALESCE(proof,'') FROM pack_version WHERE namespace='root' AND pack=?1 AND version=?2 AND status='published'", n->pack, n->version) &&
             bfpg_rows(db, body, "dependency", "SELECT depends_on,COALESCE(pinned_version,''),COALESCE(NULLIF(channel,''),'stable'),why FROM pack_dependency WHERE namespace='root' AND pack=?1 AND status='published' ORDER BY depends_on,pinned_version,channel,why", n->pack, n->version) &&
             bfpg_rows(db, body, "overlay", "SELECT id,overrides,layer,verb,target,COALESCE(value,''),why FROM pack_overlay WHERE namespace='root' AND overlay=?1 AND status='published' ORDER BY layer,id", n->pack, n->version) &&
             bfpg_rows(db, body, "demand", "SELECT demand_kind,needs,COALESCE(preferred_sources,''),COALESCE(fields,''),COALESCE(body_regime,''),COALESCE(reheat_when,''),COALESCE(optional,'') FROM pack_demand WHERE namespace='root' AND pack=?1 AND status='published' ORDER BY demand_kind,needs,preferred_sources,fields,body_regime,reheat_when,optional", n->pack, n->version);
    }
    int size = sqlite3_str_length(body), error = sqlite3_str_errcode(body);
    char *bytes = sqlite3_str_finish(body);
    if (!ok || !bytes || error != SQLITE_OK || size > BFPG_BYTES) { sqlite3_free(bytes); return bfpg_failure(out, "definition_bounds", root); }
    bf_sha256_hex((const uint8_t *)bytes, (size_t)size, out->digest);
    if(image&&image_bytes){*image=(unsigned char*)bytes;*image_bytes=(size_t)size;}
    else sqlite3_free(bytes);
    return 1;
}
static int bfpg_resolve(sqlite3 *db, const char *root, const char *version,
                        const BfPackResolution *lock, BfPackResolution *out) {
    return bfpg_compile(db,root,version,lock,out,NULL,NULL);
}
#endif
