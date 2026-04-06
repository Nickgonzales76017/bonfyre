/*
 * BonfyreGraph — Upgrade IX: Provenance & Merkle Proofs
 *
 * Builds a Merkle tree over _ops, provides compact inclusion proofs,
 * and maintains verifiable compaction manifests.
 */
#include "provenance.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

extern uint64_t fnv1a64(const void *data, size_t len);
extern void     iso_timestamp(char *buf, size_t sz);
extern int      db_exec(sqlite3 *db, const char *sql);

/* ------------------------------------------------------------------ */
/*  Bootstrap                                                          */
/* ------------------------------------------------------------------ */
int prov_bootstrap(sqlite3 *db)
{
    const char *ddl =
        "CREATE TABLE IF NOT EXISTS _merkle_tree ("
        "  node_id    INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  ns         TEXT NOT NULL,"
        "  depth      INTEGER NOT NULL,"
        "  left_child INTEGER,"         /* node_id or NULL for leaf */
        "  right_child INTEGER,"
        "  op_id      INTEGER,"         /* non-NULL only for leaves */
        "  hash       INTEGER NOT NULL"  /* uint64 stored as int64 */
        ");"
        "CREATE INDEX IF NOT EXISTS idx_merkle_ns "
        "  ON _merkle_tree(ns, depth);"
        "CREATE INDEX IF NOT EXISTS idx_merkle_op "
        "  ON _merkle_tree(op_id);"
        "CREATE TABLE IF NOT EXISTS _compaction_manifests ("
        "  manifest_id  INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  content_type TEXT NOT NULL,"
        "  merkle_root  INTEGER NOT NULL,"
        "  family_count INTEGER NOT NULL DEFAULT 0,"
        "  gen_hash     INTEGER NOT NULL,"
        "  eigen_hash   INTEGER NOT NULL,"
        "  created_at   TEXT NOT NULL"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_manifest_ct "
        "  ON _compaction_manifests(content_type);";
    return db_exec(db, ddl);
}

/* ------------------------------------------------------------------ */
/*  Internal: combine two hashes into a parent                        */
/* ------------------------------------------------------------------ */
static uint64_t hash_combine(uint64_t left, uint64_t right)
{
    uint8_t buf[16];
    memcpy(buf, &left, 8);
    memcpy(buf + 8, &right, 8);
    return fnv1a64(buf, 16);
}

/* ------------------------------------------------------------------ */
/*  Internal: hash a single op for leaf nodes                         */
/* ------------------------------------------------------------------ */
static uint64_t hash_op(int64_t op_id, const char *op_type,
                        const char *target, int64_t target_id,
                        const char *payload, const char *prev_hash_str)
{
    /* concatenate all fields and hash */
    size_t cap = 256;
    if (payload) cap += strlen(payload);
    if (op_type) cap += strlen(op_type);
    if (target) cap += strlen(target);
    if (prev_hash_str) cap += strlen(prev_hash_str);

    char *buf = malloc(cap);
    if (!buf) return 0;

    int n = snprintf(buf, cap, "%lld:%s:%s:%lld:%s:%s",
                     (long long)op_id,
                     op_type ? op_type : "",
                     target ? target : "",
                     (long long)target_id,
                     payload ? payload : "",
                     prev_hash_str ? prev_hash_str : "");
    uint64_t h = fnv1a64(buf, n > 0 ? (size_t)n : 0);
    free(buf);
    return h;
}

/* ------------------------------------------------------------------ */
/*  prov_build_tree                                                    */
/* ------------------------------------------------------------------ */
int prov_build_tree(sqlite3 *db, const char *ns, uint64_t *out_root)
{
    if (!db || !ns || !out_root) return -1;

    /* clear existing tree for this namespace */
    {
        sqlite3_stmt *st = NULL;
        const char *sql = "DELETE FROM _merkle_tree WHERE ns = ?1";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, ns, -1, SQLITE_STATIC);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
    }

    /* load all ops for this namespace, ordered by op_id */
    typedef struct { int64_t op_id; uint64_t hash; int64_t node_id; } Leaf;
    int leaf_cap = 4096;
    Leaf *leaves = malloc(leaf_cap * sizeof(Leaf));
    if (!leaves) return -1;
    int nleaves = 0;

    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT op_id, op_type, target, target_id, payload, hash "
            "FROM _ops WHERE ns = ?1 ORDER BY op_id";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
            free(leaves);
            return -1;
        }
        sqlite3_bind_text(st, 1, ns, -1, SQLITE_STATIC);

        while (sqlite3_step(st) == SQLITE_ROW) {
            if (nleaves >= leaf_cap) {
                leaf_cap *= 2;
                Leaf *nl = realloc(leaves, leaf_cap * sizeof(Leaf));
                if (!nl) { free(leaves); sqlite3_finalize(st); return -1; }
                leaves = nl;
            }
            int64_t oid = sqlite3_column_int64(st, 0);
            const char *ot = (const char *)sqlite3_column_text(st, 1);
            const char *tg = (const char *)sqlite3_column_text(st, 2);
            int64_t tid = sqlite3_column_int64(st, 3);
            const char *pl = (const char *)sqlite3_column_text(st, 4);
            const char *ph = (const char *)sqlite3_column_text(st, 5);

            uint64_t h = hash_op(oid, ot, tg, tid, pl, ph);
            leaves[nleaves].op_id = oid;
            leaves[nleaves].hash = h;
            leaves[nleaves].node_id = 0;
            nleaves++;
        }
        sqlite3_finalize(st);
    }

    if (nleaves == 0) {
        free(leaves);
        *out_root = 0;
        return 0;
    }

    /* insert leaf nodes */
    {
        sqlite3_stmt *ins = NULL;
        const char *sql =
            "INSERT INTO _merkle_tree (ns, depth, left_child, right_child, op_id, hash) "
            "VALUES (?1, 0, NULL, NULL, ?2, ?3)";
        if (sqlite3_prepare_v2(db, sql, -1, &ins, NULL) != SQLITE_OK) {
            free(leaves);
            return -1;
        }
        for (int i = 0; i < nleaves; i++) {
            sqlite3_reset(ins);
            sqlite3_bind_text(ins, 1, ns, -1, SQLITE_STATIC);
            sqlite3_bind_int64(ins, 2, leaves[i].op_id);
            sqlite3_bind_int64(ins, 3, (int64_t)leaves[i].hash);
            sqlite3_step(ins);
            leaves[i].node_id = sqlite3_last_insert_rowid(db);
        }
        sqlite3_finalize(ins);
    }

    /* build tree bottom-up */
    typedef struct { int64_t node_id; uint64_t hash; } TreeNode;
    int cur_count = nleaves;
    TreeNode *current = malloc(cur_count * sizeof(TreeNode));
    if (!current) { free(leaves); return -1; }
    for (int i = 0; i < cur_count; i++) {
        current[i].node_id = leaves[i].node_id;
        current[i].hash = leaves[i].hash;
    }
    free(leaves);

    int depth = 1;
    while (cur_count > 1) {
        int next_count = (cur_count + 1) / 2;
        TreeNode *next = malloc(next_count * sizeof(TreeNode));
        if (!next) { free(current); return -1; }

        sqlite3_stmt *ins = NULL;
        const char *sql =
            "INSERT INTO _merkle_tree (ns, depth, left_child, right_child, op_id, hash) "
            "VALUES (?1, ?2, ?3, ?4, NULL, ?5)";
        if (sqlite3_prepare_v2(db, sql, -1, &ins, NULL) != SQLITE_OK) {
            free(current); free(next);
            return -1;
        }

        for (int i = 0; i < cur_count; i += 2) {
            uint64_t parent_hash;
            int64_t left_id = current[i].node_id;
            int64_t right_id;

            if (i + 1 < cur_count) {
                right_id = current[i + 1].node_id;
                parent_hash = hash_combine(current[i].hash, current[i + 1].hash);
            } else {
                /* odd node: duplicate */
                right_id = left_id;
                parent_hash = hash_combine(current[i].hash, current[i].hash);
            }

            sqlite3_reset(ins);
            sqlite3_bind_text(ins, 1, ns, -1, SQLITE_STATIC);
            sqlite3_bind_int(ins, 2, depth);
            sqlite3_bind_int64(ins, 3, left_id);
            sqlite3_bind_int64(ins, 4, right_id);
            sqlite3_bind_int64(ins, 5, (int64_t)parent_hash);
            sqlite3_step(ins);

            int idx = i / 2;
            next[idx].node_id = sqlite3_last_insert_rowid(db);
            next[idx].hash = parent_hash;
        }
        sqlite3_finalize(ins);

        free(current);
        current = next;
        cur_count = next_count;
        depth++;
    }

    *out_root = current[0].hash;
    free(current);
    return 0;
}

/* ------------------------------------------------------------------ */
/*  prov_prove — generate inclusion proof                             */
/* ------------------------------------------------------------------ */
int prov_prove(sqlite3 *db, const char *ns, int64_t op_id,
               uint64_t **out_proof, int *out_count)
{
    if (!db || !ns || !out_proof || !out_count) return -1;

    /* find the leaf node for this op */
    int64_t leaf_node = -1;
    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT node_id FROM _merkle_tree WHERE ns = ?1 AND op_id = ?2";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
        sqlite3_bind_text(st, 1, ns, -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 2, op_id);
        if (sqlite3_step(st) == SQLITE_ROW)
            leaf_node = sqlite3_column_int64(st, 0);
        sqlite3_finalize(st);
    }
    if (leaf_node < 0) return -1;

    /* walk up the tree, collecting sibling hashes */
    int proof_cap = 64;
    uint64_t *proof = malloc(proof_cap * sizeof(uint64_t));
    if (!proof) return -1;
    int count = 0;

    int64_t current = leaf_node;
    while (1) {
        /* find parent that has current as left or right child */
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT node_id, left_child, right_child, hash FROM _merkle_tree "
            "WHERE ns = ?1 AND (left_child = ?2 OR right_child = ?2) "
            "ORDER BY depth ASC LIMIT 1";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) break;
        sqlite3_bind_text(st, 1, ns, -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 2, current);

        if (sqlite3_step(st) != SQLITE_ROW) {
            sqlite3_finalize(st);
            break; /* reached root */
        }

        int64_t parent_id = sqlite3_column_int64(st, 0);
        int64_t left_id = sqlite3_column_int64(st, 1);
        int64_t right_id = sqlite3_column_int64(st, 2);
        (void)parent_id;
        sqlite3_finalize(st);

        /* sibling is the other child */
        int64_t sibling = (current == left_id) ? right_id : left_id;

        /* look up sibling hash */
        sqlite3_stmt *sh = NULL;
        const char *shsql =
            "SELECT hash FROM _merkle_tree WHERE node_id = ?1";
        if (sqlite3_prepare_v2(db, shsql, -1, &sh, NULL) == SQLITE_OK) {
            sqlite3_bind_int64(sh, 1, sibling);
            if (sqlite3_step(sh) == SQLITE_ROW) {
                if (count >= proof_cap) {
                    proof_cap *= 2;
                    uint64_t *np = realloc(proof, proof_cap * sizeof(uint64_t));
                    if (!np) { free(proof); sqlite3_finalize(sh); return -1; }
                    proof = np;
                }
                proof[count++] = (uint64_t)sqlite3_column_int64(sh, 0);
            }
            sqlite3_finalize(sh);
        }

        current = parent_id;
    }

    *out_proof = proof;
    *out_count = count;
    return 0;
}

/* ------------------------------------------------------------------ */
/*  prov_verify — verify an inclusion proof                           */
/* ------------------------------------------------------------------ */
int prov_verify(uint64_t leaf_hash, const uint64_t *proof, int proof_len,
                const int *directions, uint64_t expected_root)
{
    uint64_t current = leaf_hash;
    for (int i = 0; i < proof_len; i++) {
        if (directions && directions[i]) {
            current = hash_combine(proof[i], current);
        } else {
            current = hash_combine(current, proof[i]);
        }
    }
    return current == expected_root ? 1 : 0;
}

/* ------------------------------------------------------------------ */
/*  prov_root — current Merkle root                                   */
/* ------------------------------------------------------------------ */
int prov_root(sqlite3 *db, const char *ns, uint64_t *out_root)
{
    if (!db || !ns || !out_root) return -1;

    sqlite3_stmt *st = NULL;
    const char *sql =
        "SELECT hash FROM _merkle_tree WHERE ns = ?1 "
        "ORDER BY depth DESC, node_id DESC LIMIT 1";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, ns, -1, SQLITE_STATIC);
    int rc = -1;
    if (sqlite3_step(st) == SQLITE_ROW) {
        *out_root = (uint64_t)sqlite3_column_int64(st, 0);
        rc = 0;
    }
    sqlite3_finalize(st);
    return rc;
}

/* ------------------------------------------------------------------ */
/*  prov_manifest — snapshot compaction state                         */
/* ------------------------------------------------------------------ */
int prov_manifest(sqlite3 *db, const char *content_type, int64_t *out_manifest_id)
{
    if (!db || !content_type || !out_manifest_id) return -1;

    /* compute Merkle root for default namespace */
    uint64_t root = 0;
    prov_root(db, "default", &root);

    /* hash all generators for this content_type */
    uint64_t gen_hash = 0;
    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT generator FROM _families WHERE content_type = ?1 ORDER BY family_id";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, content_type, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW) {
                const char *g = (const char *)sqlite3_column_text(st, 0);
                if (g) {
                    uint64_t gh = fnv1a64(g, strlen(g));
                    gen_hash = hash_combine(gen_hash, gh);
                }
            }
            sqlite3_finalize(st);
        }
    }

    /* hash eigenvalue profiles */
    uint64_t eigen_hash = 0;
    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT eigenvalue FROM _tensor_eigen "
            "WHERE family_id IN (SELECT family_id FROM _families WHERE content_type = ?1) "
            "ORDER BY family_id, position";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, content_type, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW) {
                double ev = sqlite3_column_double(st, 0);
                eigen_hash = hash_combine(eigen_hash, fnv1a64(&ev, sizeof(ev)));
            }
            sqlite3_finalize(st);
        }
    }

    /* count families */
    int family_count = 0;
    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT COUNT(*) FROM _families WHERE content_type = ?1";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, content_type, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW)
                family_count = sqlite3_column_int(st, 0);
            sqlite3_finalize(st);
        }
    }

    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    sqlite3_stmt *ins = NULL;
    const char *sql =
        "INSERT INTO _compaction_manifests "
        "(content_type, merkle_root, family_count, gen_hash, eigen_hash, created_at) "
        "VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
    if (sqlite3_prepare_v2(db, sql, -1, &ins, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(ins, 1, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_int64(ins, 2, (int64_t)root);
    sqlite3_bind_int(ins, 3, family_count);
    sqlite3_bind_int64(ins, 4, (int64_t)gen_hash);
    sqlite3_bind_int64(ins, 5, (int64_t)eigen_hash);
    sqlite3_bind_text(ins, 6, ts, -1, SQLITE_STATIC);
    if (sqlite3_step(ins) != SQLITE_DONE) {
        sqlite3_finalize(ins);
        return -1;
    }
    sqlite3_finalize(ins);

    *out_manifest_id = sqlite3_last_insert_rowid(db);
    return 0;
}

/* ------------------------------------------------------------------ */
/*  prov_verify_manifest                                               */
/* ------------------------------------------------------------------ */
int prov_verify_manifest(sqlite3 *db, int64_t manifest_id)
{
    if (!db) return 0;

    /* load manifest */
    char content_type[256] = {0};
    uint64_t expected_gen_hash = 0, expected_eigen_hash = 0;

    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT content_type, gen_hash, eigen_hash FROM _compaction_manifests "
            "WHERE manifest_id = ?1";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
        sqlite3_bind_int64(st, 1, manifest_id);
        if (sqlite3_step(st) != SQLITE_ROW) {
            sqlite3_finalize(st);
            return 0;
        }
        const char *ct = (const char *)sqlite3_column_text(st, 0);
        if (ct) strncpy(content_type, ct, sizeof(content_type) - 1);
        expected_gen_hash = (uint64_t)sqlite3_column_int64(st, 1);
        expected_eigen_hash = (uint64_t)sqlite3_column_int64(st, 2);
        sqlite3_finalize(st);
    }

    /* recompute gen_hash */
    uint64_t gen_hash = 0;
    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT generator FROM _families WHERE content_type = ?1 ORDER BY family_id";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, content_type, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW) {
                const char *g = (const char *)sqlite3_column_text(st, 0);
                if (g) gen_hash = hash_combine(gen_hash, fnv1a64(g, strlen(g)));
            }
            sqlite3_finalize(st);
        }
    }

    /* recompute eigen_hash */
    uint64_t eigen_hash = 0;
    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT eigenvalue FROM _tensor_eigen "
            "WHERE family_id IN (SELECT family_id FROM _families WHERE content_type = ?1) "
            "ORDER BY family_id, position";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, content_type, -1, SQLITE_STATIC);
            while (sqlite3_step(st) == SQLITE_ROW) {
                double ev = sqlite3_column_double(st, 0);
                eigen_hash = hash_combine(eigen_hash, fnv1a64(&ev, sizeof(ev)));
            }
            sqlite3_finalize(st);
        }
    }

    return (gen_hash == expected_gen_hash && eigen_hash == expected_eigen_hash) ? 1 : 0;
}

/* ------------------------------------------------------------------ */
/*  prov_chain — JSON provenance chain                                */
/* ------------------------------------------------------------------ */
int prov_chain(sqlite3 *db, const char *ns, char **out_json)
{
    if (!db || !ns || !out_json) return -1;

    size_t cap = 4096;
    char *buf = malloc(cap);
    if (!buf) return -1;
    int off = 0;

    off += snprintf(buf + off, cap - off,
        "{\"ns\":\"%s\",\"manifests\":[", ns);

    sqlite3_stmt *st = NULL;
    const char *sql =
        "SELECT manifest_id, content_type, merkle_root, family_count, "
        "       gen_hash, eigen_hash, created_at "
        "FROM _compaction_manifests ORDER BY manifest_id";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        free(buf);
        return -1;
    }

    int first = 1;
    while (sqlite3_step(st) == SQLITE_ROW) {
        if (!first) buf[off++] = ',';
        first = 0;

        int64_t mid = sqlite3_column_int64(st, 0);
        const char *ct = (const char *)sqlite3_column_text(st, 1);
        int64_t mr = sqlite3_column_int64(st, 2);
        int fc = sqlite3_column_int(st, 3);
        int64_t gh = sqlite3_column_int64(st, 4);
        int64_t eh = sqlite3_column_int64(st, 5);
        const char *ca = (const char *)sqlite3_column_text(st, 6);

        size_t need = 512;
        while (off + need >= cap) {
            cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) { free(buf); sqlite3_finalize(st); return -1; }
            buf = nb;
        }

        off += snprintf(buf + off, cap - off,
            "{\"manifest_id\":%lld,\"content_type\":\"%s\","
            "\"merkle_root\":\"%llx\",\"family_count\":%d,"
            "\"gen_hash\":\"%llx\",\"eigen_hash\":\"%llx\","
            "\"created_at\":\"%s\"}",
            (long long)mid, ct ? ct : "",
            (unsigned long long)mr, fc,
            (unsigned long long)gh, (unsigned long long)eh,
            ca ? ca : "");
    }
    sqlite3_finalize(st);

    /* merkle root */
    uint64_t root = 0;
    prov_root(db, ns, &root);

    while (off + 128 >= (int)cap) {
        cap *= 2;
        char *nb = realloc(buf, cap);
        if (!nb) { free(buf); return -1; }
        buf = nb;
    }
    off += snprintf(buf + off, cap - off,
        "],\"current_merkle_root\":\"%llx\"}",
        (unsigned long long)root);

    *out_json = buf;
    return 0;
}

/* ------------------------------------------------------------------ */
/*  prov_stats — summary statistics                                   */
/* ------------------------------------------------------------------ */
int prov_stats(sqlite3 *db, char **out_json)
{
    if (!db || !out_json) return -1;

    int tree_nodes = 0, max_depth = 0, manifests = 0;

    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT COUNT(*), COALESCE(MAX(depth), 0) FROM _merkle_tree";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            if (sqlite3_step(st) == SQLITE_ROW) {
                tree_nodes = sqlite3_column_int(st, 0);
                max_depth = sqlite3_column_int(st, 1);
            }
            sqlite3_finalize(st);
        }
    }
    {
        sqlite3_stmt *st = NULL;
        const char *sql = "SELECT COUNT(*) FROM _compaction_manifests";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            if (sqlite3_step(st) == SQLITE_ROW)
                manifests = sqlite3_column_int(st, 0);
            sqlite3_finalize(st);
        }
    }

    size_t cap = 256;
    char *buf = malloc(cap);
    if (!buf) return -1;
    snprintf(buf, cap,
        "{\"tree_nodes\":%d,\"max_depth\":%d,\"manifests\":%d}",
        tree_nodes, max_depth, manifests);

    *out_json = buf;
    return 0;
}
