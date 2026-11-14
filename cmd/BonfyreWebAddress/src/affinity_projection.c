#include "affinity_projection.h"
#include "bf_lex.h"
#include "bf_jsonw.h"
#include "../../BonfyreSurfaceScene/src/authoring_native_components.h"
#include "form_tree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int subject_ok(const char *s) {
    size_t n;
    if (!s || !(n = strlen(s)) || n > 191) return 0;
    if (strncmp(s, "person:", 7) && strncmp(s, "user:", 5)) return 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (!(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9') &&
            c != ':' && c != '.' && c != '_' && c != '-') return 0;
    }
    return 1;
}

static int type_exists(sqlite3 *db, const char *name) {
    sqlite3_stmt *st = NULL;
    int found = 0;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
                           -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, name, -1, SQLITE_STATIC);
    found = sqlite3_step(st) == SQLITE_ROW;
    sqlite3_finalize(st);
    return found;
}

static int latest_kind(sqlite3 *db, const char *actor, const char *target,
                       char *out, size_t cap) {
    static const char *sql =
        "SELECT kind, state FROM person_affinity "
        "WHERE actor_subject=?1 AND target_subject=?2 AND status='published' "
        "ORDER BY id DESC LIMIT 1";
    sqlite3_stmt *st = NULL;
    int found = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, actor, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, target, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *kind = (const char *)sqlite3_column_text(st, 0);
        const char *state = (const char *)sqlite3_column_text(st, 1);
        if (kind && state && !strcmp(state, "active") &&
            (!strcmp(kind, "like") || !strcmp(kind, "pass") || !strcmp(kind, "superlike") ||
             !strcmp(kind, "block")) &&
            strlen(kind) < cap) {
            snprintf(out, cap, "%s", kind);
            found = 1;
        }
    }
    sqlite3_finalize(st);
    return found;
}

int bf_affinity_status(sqlite3 *db, const char *actor, const char *target,
                       char *kind_out, size_t kind_cap) {
    if (!db || !subject_ok(actor) || !subject_ok(target) || !kind_out || kind_cap < 10)
        return -1;
    kind_out[0] = '\0';
    if (!strcmp(actor, target)) return 0;
    if (!type_exists(db, "person_affinity")) return 0;
    return latest_kind(db, actor, target, kind_out, kind_cap);
}

static int counts_as_like(const char *kind) {
    return kind && (!strcmp(kind, "like") || !strcmp(kind, "superlike"));
}

int bf_affinity_mutual(sqlite3 *db, const char *a, const char *b) {
    char ka[16], kb[16];
    int ra, rb;
    if (!db || !subject_ok(a) || !subject_ok(b)) return -1;
    if (!strcmp(a, b)) return 0;
    if (!type_exists(db, "person_affinity")) return 0;
    ra = latest_kind(db, a, b, ka, sizeof(ka));
    rb = latest_kind(db, b, a, kb, sizeof(kb));
    if (ra < 0 || rb < 0) return -1;
    if ((ra && !strcmp(ka, "block")) || (rb && !strcmp(kb, "block"))) return 0;
    return ra && rb && counts_as_like(ka) && counts_as_like(kb);
}

static int affinity_blocked(sqlite3 *db, const char *a, const char *b) {
    char ka[16], kb[16];
    int ra = latest_kind(db, a, b, ka, sizeof(ka));
    int rb = latest_kind(db, b, a, kb, sizeof(kb));
    if (ra < 0 || rb < 0) return -1;
    return (ra && !strcmp(ka, "block")) || (rb && !strcmp(kb, "block"));
}

int bf_affinity_blocked(sqlite3 *db, const char *a, const char *b) {
    if (!db || !subject_ok(a) || !subject_ok(b)) return -1;
    if (!strcmp(a, b)) return 0;
    if (!type_exists(db, "person_affinity")) return 0;
    return affinity_blocked(db, a, b);
}

int bf_report_quota_ok(sqlite3 *db, const char *actor) {
    sqlite3_stmt *st = NULL;
    int count = 0, ok = 0;
    if (!db || !subject_ok(actor)) return -1;
    if (!type_exists(db, "person_report")) return 1;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM person_report WHERE actor_subject=?1"
                           " AND status='published' AND date(substr(created_at,1,10))=date('now')",
                           -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, actor, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        count = sqlite3_column_int(st, 0);
        ok = 1;
    }
    sqlite3_finalize(st);
    if (!ok) return -1;
    return count < 5;
}

/* Assembled only from static fragments plus positional binds; no caller
 * bytes enter the SQL text. Missing tables degrade: no affinity table
 * drops pass exclusion, no interest table drops label search. */
static int companions_sql_text(sqlite3 *db, char *out, size_t cap) {
    int w = snprintf(out, cap,
        "SELECT s.subject_address, s.host, s.territory,"
        " (SELECT heading FROM site_page WHERE namespace=s.site_id AND route='/'"
        "  AND status='published' LIMIT 1),"
        " (SELECT substr(lede,1,150) FROM site_page WHERE namespace=s.site_id AND route='/'"
        "  AND status='published' LIMIT 1)"
        " FROM site_presentation s"
        " WHERE s.namespace='root' AND s.profile='member-profile-v1' AND s.status='published'"
        " AND s.subject_address LIKE 'person:%%'"
        " AND (?1='' OR s.territory=?1)"
        " AND (?2='' OR s.subject_address LIKE '%%'||?2||'%%' OR s.host LIKE '%%'||?2||'%%'");
    if (w < 0 || (size_t)w >= cap) return -1;
    if (type_exists(db, "person_interest")) {
        int m = snprintf(out + w, cap - (size_t)w,
            " OR EXISTS (SELECT 1 FROM person_interest"
            "  WHERE actor_subject=s.subject_address AND status='published'"
            "  AND label LIKE '%%'||?2||'%%')");
        if (m < 0 || (size_t)m >= cap - (size_t)w) return -1;
        w += m;
    }
    {
        int m = snprintf(out + w, cap - (size_t)w,
            ")"
            " AND (?3='' OR s.subject_address<>?3)");
        if (m < 0 || (size_t)m >= cap - (size_t)w) return -1;
        w += m;
    }
    if (type_exists(db, "person_affinity")) {
        int m = snprintf(out + w, cap - (size_t)w,
            " AND (?3='' OR NOT EXISTS (SELECT 1 FROM person_affinity a"
            "  WHERE a.actor_subject=?3 AND a.target_subject=s.subject_address"
            "  AND a.status='published' AND a.state='active' AND a.kind='pass'"
            "  AND a.id=(SELECT MAX(id) FROM person_affinity"
            "   WHERE actor_subject=a.actor_subject AND target_subject=a.target_subject"
            "   AND status='published')))"
            " AND (?3='' OR NOT EXISTS (SELECT 1 FROM person_affinity b"
            "  WHERE ((b.actor_subject=?3 AND b.target_subject=s.subject_address)"
            "   OR (b.actor_subject=s.subject_address AND b.target_subject=?3))"
            "  AND b.status='published' AND b.state='active' AND b.kind='block'"
            "  AND b.id=(SELECT MAX(id) FROM person_affinity"
            "   WHERE actor_subject=b.actor_subject AND target_subject=b.target_subject"
            "   AND status='published')))");
        if (m < 0 || (size_t)m >= cap - (size_t)w) return -1;
        w += m;
    }
    if (type_exists(db, "person_report")) {
        int m = snprintf(out + w, cap - (size_t)w,
            " AND (?3='' OR NOT EXISTS (SELECT 1 FROM person_report r"
            "  WHERE r.actor_subject=?3 AND r.target_subject=s.subject_address"
            "  AND r.status='published'))");
        if (m < 0 || (size_t)m >= cap - (size_t)w) return -1;
        w += m;
    }
    if (type_exists(db, "community_membership")) {
        int m = snprintf(out + w, cap - (size_t)w,
            " AND (?6='' OR EXISTS (SELECT 1 FROM community_membership m"
            "  WHERE m.namespace='root' AND m.community=?6 AND m.member_subject=s.subject_address"
            "  AND m.state='active' AND m.status='published'))");
        if (m < 0 || (size_t)m >= cap - (size_t)w) return -1;
        w += m;
    } else {
        int m = snprintf(out + w, cap - (size_t)w, " AND ?6=''");
        if (m < 0 || (size_t)m >= cap - (size_t)w) return -1;
        w += m;
    }
    {
        int m = snprintf(out + w, cap - (size_t)w,
            " ORDER BY s.subject_address LIMIT ?4 OFFSET ?5");
        if (m < 0 || (size_t)m >= cap - (size_t)w) return -1;
    }
    return 0;
}

static int companions_params_ok(const char *viewer, const char *territory,
                                const char *needle, const char *sort, const char *view,
                                const char *club) {
    if (viewer && *viewer && !subject_ok(viewer)) return 0;
    if (territory && *territory &&
        (strcmp(territory, "work") && strcmp(territory, "life") &&
         strcmp(territory, "live") && strcmp(territory, "shop"))) return 0;
    if (needle && strlen(needle) > 64) return 0;
    if (sort && *sort && strcmp(sort, "match")) return 0;
    if (view && *view && strcmp(view, "matches") && strcmp(view, "attention")) return 0;
    if (club && *club &&
        (strlen(club) > 64 || strspn(club, "abcdefghijklmnopqrstuvwxyz0123456789-") != strlen(club)))
        return 0;
    return 1;
}

typedef struct {
    char subject[192], host[256], territory[24], name[161], bio[601];
    char shared[341];
    int score;
} CompanionRow;

#define COMPANION_RANK_POOL 96

static int companions_fetch(sqlite3 *db, const char *viewer, const char *territory,
                            const char *needle, const char *club, unsigned limit, unsigned offset,
                            CompanionRow *rows, unsigned cap) {
    sqlite3_stmt *st = NULL;
    unsigned n = 0;
    char sql[4096];
    if (companions_sql_text(db, sql, sizeof(sql)) != 0) return -1;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, territory ? territory : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, needle ? needle : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, viewer ? viewer : "", -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 4, (int)limit);
    sqlite3_bind_int(st, 5, (int)offset);
    sqlite3_bind_text(st, 6, club ? club : "", -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *s = (const char *)sqlite3_column_text(st, 0);
        const char *h = (const char *)sqlite3_column_text(st, 1);
        const char *t = (const char *)sqlite3_column_text(st, 2);
        const char *name = (const char *)sqlite3_column_text(st, 3);
        const char *bio = (const char *)sqlite3_column_text(st, 4);
        if (!s || !h || !t || n >= cap) continue;
        snprintf(rows[n].subject, sizeof(rows[n].subject), "%s", s);
        snprintf(rows[n].host, sizeof(rows[n].host), "%s", h);
        snprintf(rows[n].territory, sizeof(rows[n].territory), "%s", t);
        if (name && *name) snprintf(rows[n].name, sizeof(rows[n].name), "%s", name);
        else {
            const char *handle = strchr(s, ':');
            snprintf(rows[n].name, sizeof(rows[n].name), "%s",
                     handle && handle[1] ? handle + 1 : s);
        }
        if (bio) snprintf(rows[n].bio, sizeof(rows[n].bio), "%s", bio);
        rows[n].shared[0] = '\0';
        rows[n].score = 0;
        n++;
    }
    sqlite3_finalize(st);
    return (int)n;
}

/* Ranked discovery scores a bounded candidate pool and slices one page.
 * Anonymous or scoreless views keep subject order; ties keep subject order.
 * matches_only keeps mutuals before scoring and slicing. */
static int companions_query(sqlite3 *db, const char *viewer, const char *territory,
                            const char *needle, unsigned offset, unsigned page_size,
                            int ranked, int matches_only, const char *club,
                            CompanionRow *rows, unsigned cap, int *has_more) {
    CompanionRow pool[COMPANION_RANK_POOL + 1];
    int n, shown = 0;
    unsigned i, j;
    *has_more = 0;
    if ((!ranked && !matches_only) || !viewer || !*viewer) {
        n = companions_fetch(db, viewer, territory, needle, club, page_size + 1, offset,
                             rows, cap);
        if (n < 0) return -1;
        if (n > (int)page_size) {
            *has_more = 1;
            n = (int)page_size;
        }
        return n;
    }
    n = companions_fetch(db, viewer, territory, needle, club, COMPANION_RANK_POOL + 1, 0,
                         pool, COMPANION_RANK_POOL + 1);
    if (n < 0) return -1;
    if (n > COMPANION_RANK_POOL) {
        *has_more = 1;
        n = COMPANION_RANK_POOL;
    }
    if (matches_only) {
        unsigned w = 0;
        for (i = 0; i < (unsigned)n; i++) {
            int mutual = bf_affinity_mutual(db, viewer, pool[i].subject);
            if (mutual < 0) return -1;
            if (mutual && w < COMPANION_RANK_POOL + 1) pool[w++] = pool[i];
        }
        n = (int)w;
    }
    for (i = 0; i < (unsigned)n; i++) {
        if (bf_match_score(db, viewer, pool[i].subject, &pool[i].score,
                           pool[i].shared, sizeof(pool[i].shared)) != 0) return -1;
    }
    if (ranked) {
        for (i = 1; i < (unsigned)n; i++) {
            CompanionRow key = pool[i];
            j = i;
            while (j > 0 && (pool[j - 1].score < key.score ||
                             (pool[j - 1].score == key.score &&
                              strcmp(pool[j - 1].subject, key.subject) > 0))) {
                pool[j] = pool[j - 1];
                j--;
            }
            pool[j] = key;
        }
    }
    for (i = offset; i < (unsigned)n && shown < (int)page_size && (unsigned)shown < cap; i++)
        rows[shown++] = pool[i];
    if ((unsigned)n > offset + (unsigned)shown) *has_more = 1;
    return shown;
}

int bf_companions_page(FILE *out, sqlite3 *db, const char *viewer,
                       const char *territory, const char *needle,
                       unsigned offset, unsigned page_size, const char *sort,
                       const char *view, const char *club) {
    CompanionRow rows[25];
    int has_more = 0, n, ranked;
    if (!out || !db || !companions_params_ok(viewer, territory, needle, sort, view, club)) return -1;
    if (!page_size || page_size > 24) page_size = 24;
    if (offset > 1000000U) return -1;
    if (view && !strcmp(view, "attention")) {
        if (!viewer || !*viewer) return -1;
        return bf_attention_page(out, db, viewer, page_size);
    }
    ranked = sort && !strcmp(sort, "match") && viewer && *viewer;
    if (view && !strcmp(view, "matches") && (!viewer || !*viewer)) return -1;
    n = companions_query(db, viewer, territory, needle, offset, page_size, ranked,
                         view && !strcmp(view, "matches"), club,
                         rows, page_size + 1, &has_more);
    if (n < 0) return -1;
    {
        BfAuthoringTree *tree = calloc(1, sizeof(*tree));
        char *rendered = NULL;
        size_t rendered_len = 0;
        FILE *memory;
        int ok = 0;
        if (!tree) return -1;
        for (int i = 0; i < n; i++) {
            char id[32], child[40], home[300], report[300], label[420];
            char kind[16] = "";
            int status = 0, mutual = 0;
            bf_jsonw w;
            snprintf(id, sizeof(id), "cp-%d", i);
            if (viewer && *viewer) {
                status = bf_affinity_status(db, viewer, rows[i].subject, kind, sizeof(kind));
                if (status < 0) goto tree_done;
                mutual = bf_affinity_mutual(db, viewer, rows[i].subject);
                if (mutual < 0) goto tree_done;
            }
            snprintf(home, sizeof(home), "https://%s/", rows[i].host);
            snprintf(report, sizeof(report), "/companions/report?target=%s", rows[i].subject);
            bf_jsonw_init(&w, 0);
            bf_jsonw_obj(&w);
            bf_jsonw_key(&w, "title");
            bf_jsonw_str(&w, rows[i].name);
            bf_jsonw_key(&w, "body");
            bf_jsonw_str(&w, rows[i].bio);
            bf_jsonw_key(&w, "actions");
            bf_jsonw_arr(&w);
            bf_jsonw_obj(&w);
            bf_jsonw_key(&w, "label");
            bf_jsonw_str(&w, "Open home");
            bf_jsonw_key(&w, "href");
            bf_jsonw_str(&w, home);
            bf_jsonw_obj_end(&w);
            if (viewer && *viewer) {
                bf_jsonw_obj(&w);
                bf_jsonw_key(&w, "label");
                bf_jsonw_str(&w, "Report");
                bf_jsonw_key(&w, "href");
                bf_jsonw_str(&w, report);
                bf_jsonw_obj_end(&w);
            }
            bf_jsonw_arr_end(&w);
            bf_jsonw_obj_end(&w);
            ok = !w.oom && w.buf &&
                 bf_authoring_tree_add(tree, id, "ui:card", NULL, i) &&
                 bf_authoring_tree_data(tree, id, "payload", w.buf);
            bf_jsonw_free(&w);
            if (!ok) goto tree_done;
            if (ranked && rows[i].score > 0) {
                snprintf(label, sizeof(label), "Match %d", rows[i].score);
                if (rows[i].shared[0]) {
                    size_t used = strlen(label);
                    snprintf(label + used, sizeof(label) - used, " \u00B7 %s", rows[i].shared);
                }
                snprintf(child, sizeof(child), "cp-%d-score", i);
                bf_jsonw_init(&w, 0);
                bf_jsonw_obj(&w);
                bf_jsonw_key(&w, "label");
                bf_jsonw_str(&w, label);
                bf_jsonw_obj_end(&w);
                ok = !w.oom && w.buf && bf_authoring_tree_add(tree, child, "ui:badge", id, -1) &&
                     bf_authoring_tree_data(tree, child, "payload", w.buf);
                bf_jsonw_free(&w);
                if (!ok) goto tree_done;
            }
            if (mutual || (status && *kind)) {
                snprintf(child, sizeof(child), "cp-%d-state", i);
                bf_jsonw_init(&w, 0);
                bf_jsonw_obj(&w);
                bf_jsonw_key(&w, "label");
                bf_jsonw_str(&w, mutual ? "Mutual" : kind);
                bf_jsonw_obj_end(&w);
                ok = !w.oom && w.buf && bf_authoring_tree_add(tree, child, "ui:badge", id, -1) &&
                     bf_authoring_tree_data(tree, child, "payload", w.buf);
                bf_jsonw_free(&w);
                if (!ok) goto tree_done;
            }
            if (viewer && *viewer) {
                static const char *const verbs[] = {"like", "pass", "block"};
                for (size_t v = 0; v < 3; v++) {
                    snprintf(child, sizeof(child), "cp-%d-%s", i, verbs[v]);
                    bf_jsonw_init(&w, 0);
                    bf_jsonw_obj(&w);
                    bf_jsonw_key(&w, "label");
                    bf_jsonw_str(&w, verbs[v][0] == 'l' ? "Like" : verbs[v][0] == 'p' ? "Pass" : "Block");
                    bf_jsonw_key(&w, "action");
                    bf_jsonw_str(&w, "/companions/affinity");
                    bf_jsonw_key(&w, "method");
                    bf_jsonw_str(&w, "post");
                    bf_jsonw_key(&w, "fields");
                    bf_jsonw_arr(&w);
                    bf_jsonw_obj(&w);
                    bf_jsonw_key(&w, "name");
                    bf_jsonw_str(&w, "target");
                    bf_jsonw_key(&w, "value");
                    bf_jsonw_str(&w, rows[i].subject);
                    bf_jsonw_obj_end(&w);
                    bf_jsonw_obj(&w);
                    bf_jsonw_key(&w, "name");
                    bf_jsonw_str(&w, "kind");
                    bf_jsonw_key(&w, "value");
                    bf_jsonw_str(&w, verbs[v]);
                    bf_jsonw_obj_end(&w);
                    bf_jsonw_arr_end(&w);
                    bf_jsonw_obj_end(&w);
                    ok = !w.oom && w.buf &&
                         bf_authoring_tree_add(tree, child, "ui:action", id, -1) &&
                         bf_authoring_tree_data(tree, child, "payload", w.buf);
                    bf_jsonw_free(&w);
                    if (!ok) goto tree_done;
                }
            }
        }
        memory = open_memstream(&rendered, &rendered_len);
        ok = memory && bf_authoring_native_render(memory, tree, 1048576);
        if (memory) fclose(memory);
        if (!ok || !rendered) {
            free(rendered);
            goto tree_done;
        }
        fputs(rendered, out);
        free(rendered);
        if (has_more) fputs("<p class=\"bf-native-empty\" data-more>More companions beyond this page.</p>", out);
        if (!n) fputs("<p class=\"bf-native-empty\" data-empty>No companions match this view.</p>", out);
        bf_authoring_tree_clear(tree);
        free(tree);
        return n;
    tree_done:
        bf_authoring_tree_clear(tree);
        free(tree);
        return -1;
    }
}

int bf_companions_json(FILE *out, sqlite3 *db, const char *viewer,
                       const char *territory, const char *needle,
                       unsigned offset, unsigned page_size, const char *sort,
                       const char *view, const char *club) {
    CompanionRow rows[25];
    int has_more = 0, n, ranked;
    if (!out || !db || !companions_params_ok(viewer, territory, needle, sort, view, club)) return -1;
    if (!page_size || page_size > 24) page_size = 24;
    if (offset > 1000000U) return -1;
    if (view && !strcmp(view, "attention")) {
        if (!viewer || !*viewer) return -1;
        return bf_attention_json(out, db, viewer, page_size);
    }
    ranked = sort && !strcmp(sort, "match") && viewer && *viewer;
    if (view && !strcmp(view, "matches") && (!viewer || !*viewer)) return -1;
    n = companions_query(db, viewer, territory, needle, offset, page_size, ranked,
                         view && !strcmp(view, "matches"), club,
                         rows, page_size + 1, &has_more);
    if (n < 0) return -1;
    {
        bf_jsonw w;
        bf_jsonw_init(&w, 0);
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "people");
        bf_jsonw_arr(&w);
        for (int i = 0; i < n; i++) {
            char kind[16] = "";
            int status = 0, mutual = 0;
            if (viewer && *viewer) {
                status = bf_affinity_status(db, viewer, rows[i].subject, kind, sizeof(kind));
                if (status < 0) { bf_jsonw_free(&w); return -1; }
                mutual = bf_affinity_mutual(db, viewer, rows[i].subject);
                if (mutual < 0) { bf_jsonw_free(&w); return -1; }
            }
            bf_jsonw_obj(&w);
            bf_jsonw_key(&w, "subject"); bf_jsonw_str(&w, rows[i].subject);
            bf_jsonw_key(&w, "host"); bf_jsonw_str(&w, rows[i].host);
            bf_jsonw_key(&w, "territory"); bf_jsonw_str(&w, rows[i].territory);
            bf_jsonw_key(&w, "name"); bf_jsonw_str(&w, rows[i].name);
            bf_jsonw_key(&w, "bio"); bf_jsonw_str(&w, rows[i].bio);
            bf_jsonw_key(&w, "affinity");
            if (status && *kind) bf_jsonw_str(&w, kind);
            else bf_jsonw_null(&w);
            bf_jsonw_key(&w, "mutual"); bf_jsonw_bool(&w, mutual);
            if (ranked) {
                const char *p = rows[i].shared;
                bf_jsonw_key(&w, "score"); bf_jsonw_int(&w, rows[i].score);
                bf_jsonw_key(&w, "shared"); bf_jsonw_arr(&w);
                while (p && *p) {
                    const char *comma = strchr(p, ',');
                    size_t len = comma ? (size_t)(comma - p) : strlen(p);
                    if (len > 40) len = 40;
                    bf_jsonw_strn(&w, p, len);
                    p = comma ? comma + 1 : NULL;
                }
                bf_jsonw_arr_end(&w);
            }
            bf_jsonw_obj_end(&w);
        }
        bf_jsonw_arr_end(&w);
        bf_jsonw_key(&w, "has_more"); bf_jsonw_bool(&w, has_more);
        bf_jsonw_obj_end(&w);
        if (w.oom || !w.buf) { bf_jsonw_free(&w); return -1; }
        fputs(w.buf, out);
        fputc('\n', out);
        bf_jsonw_free(&w);
    }
    return n;
}

/* Original compatibility bank (clean-room behavior; no donor content).
 * Keys are stable; prompts may be reworded without breaking agreement. */
const BfQuizQuestion bf_quiz_bank[] = {
    {"q-weekend", "Your ideal weekend looks like…",
     {"a", "b", "c", "d"},
     {"Outdoors adventure", "Quiet recharge at home", "A gathering of friends", "Making or building something"},
     4},
    {"q-morning", "When do you feel sharpest?",
     {"a", "b", "c", NULL},
     {"Early morning", "Late at night", "Whenever the day allows", NULL},
     3},
    {"q-plans", "How do you like to plan?",
     {"a", "b", "c", NULL},
     {"Decide ahead", "Stay spontaneous", "A loose outline", NULL},
     3},
    {"q-social", "What restores your social battery?",
     {"a", "b", "c", NULL},
     {"Big groups", "A small circle", "Time alone", NULL},
     3},
    {"q-food", "Food is…",
     {"a", "b", "c", NULL},
     {"Cooking at home", "Eating out and exploring", "Fuel, whatever is near", NULL},
     3},
    {"q-travel", "How do you feel about travel?",
     {"a", "b", "c", NULL},
     {"Far and often", "An occasional trip", "Happiest near home", NULL},
     3},
    {"q-conflict", "When you disagree with someone close…",
     {"a", "b", "c", NULL},
     {"Talk it out right away", "Cool off first", "Write it down", NULL},
     3},
    {"q-money", "Money is best spent on…",
     {"a", "b", "c", NULL},
     {"Saving for later", "A balanced mix", "Experiences over things", NULL},
     3},
};
const unsigned bf_quiz_bank_n = sizeof(bf_quiz_bank) / sizeof(bf_quiz_bank[0]);

static const BfQuizQuestion *quiz_find(const char *key) {
    unsigned i;
    if (!key) return NULL;
    for (i = 0; i < bf_quiz_bank_n; i++)
        if (!strcmp(bf_quiz_bank[i].key, key)) return &bf_quiz_bank[i];
    return NULL;
}

static int quiz_choice_ok(const BfQuizQuestion *q, const char *choice) {
    unsigned c;
    if (!q || !choice) return 0;
    for (c = 0; c < q->choices; c++)
        if (q->choice_keys[c] && !strcmp(q->choice_keys[c], choice)) return 1;
    return 0;
}

/* Quiz agreement: latest published answer per actor per bank question.
 * An agreed question contributes the lower of the two stated importances
 * (1..3); anything invalid reads as never-existed. Capped at +6 so stated
 * interests stay the primary signal. */
#define BF_QUIZ_CAP 6

static int quiz_latest(sqlite3 *db, const char *actor, const char *question,
                       char *choice_out, size_t choice_cap, int *weight_out) {
    static const char *sql =
        "SELECT choice, weight FROM person_quiz_answer WHERE actor_subject=?1"
        " AND question=?2 AND status='published'"
        " AND id=(SELECT MAX(id) FROM person_quiz_answer WHERE actor_subject=?1"
        "  AND question=?2 AND status='published')";
    sqlite3_stmt *st = NULL;
    const char *choice;
    const char *weight;
    long w;
    char *end;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, actor, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, question, -1, SQLITE_STATIC);
    if (sqlite3_step(st) != SQLITE_ROW) {
        sqlite3_finalize(st);
        return 0;
    }
    choice = (const char *)sqlite3_column_text(st, 0);
    weight = (const char *)sqlite3_column_text(st, 1);
    if (!choice || !quiz_choice_ok(quiz_find(question), choice) || !weight ||
        (w = strtol(weight, &end, 10), end == weight || *end || w < 1 || w > 3)) {
        sqlite3_finalize(st);
        return 0;
    }
    snprintf(choice_out, choice_cap, "%s", choice);
    *weight_out = (int)w;
    sqlite3_finalize(st);
    return 1;
}

static int quiz_points(sqlite3 *db, const char *viewer, const char *target,
                       int *points_out) {
    unsigned i;
    int total = 0;
    *points_out = 0;
    for (i = 0; i < bf_quiz_bank_n; i++) {
        char mine[8] = "", theirs[8] = "";
        int mine_w = 0, theirs_w = 0, mine_has, theirs_has;
        mine_has = quiz_latest(db, viewer, bf_quiz_bank[i].key,
                               mine, sizeof(mine), &mine_w);
        if (mine_has < 0) return -1;
        theirs_has = quiz_latest(db, target, bf_quiz_bank[i].key,
                                 theirs, sizeof(theirs), &theirs_w);
        if (theirs_has < 0) return -1;
        if (mine_has && theirs_has && !strcmp(mine, theirs))
            total += mine_w < theirs_w ? mine_w : theirs_w;
        if (total >= BF_QUIZ_CAP) {
            total = BF_QUIZ_CAP;
            break;
        }
    }
    *points_out = total;
    return 0;
}

int bf_match_score(sqlite3 *db, const char *viewer, const char *target,
                   int *score, char *shared_out, size_t shared_cap) {
    static const char *overlap_sql =
        "SELECT DISTINCT label FROM person_interest WHERE actor_subject=?1 AND status='published'"
        " AND label IN (SELECT label FROM person_interest WHERE actor_subject=?2"
        " AND status='published') ORDER BY label LIMIT 8";
    static const char *recent_sql =
        "SELECT 1 FROM community_post WHERE author_subject=?1 AND status='published'"
        " AND visibility='public' AND moderation_state='accepted'"
        " AND date(substr(published_at,1,10))>=date('now','-30 days') LIMIT 1";
    sqlite3_stmt *st = NULL;
    int points = 0, shared = 0;
    size_t used = 0;
    if (!db || !score || !shared_out || shared_cap < 2) return -1;
    *score = 0;
    shared_out[0] = '\0';
    if (!subject_ok(viewer) || !subject_ok(target) || !strcmp(viewer, target)) return 0;
    {
        int blocked = bf_affinity_blocked(db, viewer, target);
        if (blocked < 0) return -1;
        if (blocked) return 0;
    }
    if (!type_exists(db, "person_interest")) return 0;
    if (sqlite3_prepare_v2(db, overlap_sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, viewer, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, target, -1, SQLITE_STATIC);
    while (sqlite3_step(st) == SQLITE_ROW && shared < 8) {
        const char *label = (const char *)sqlite3_column_text(st, 0);
        size_t len;
        if (!label || !(len = strlen(label)) || len > 40) continue;
        if (used + len + 2 >= shared_cap) break;
        if (used) shared_out[used++] = ',';
        memcpy(shared_out + used, label, len + 1);
        used += len;
        shared++;
    }
    sqlite3_finalize(st);
    points = shared * 2 > 10 ? 10 : shared * 2;
    if (type_exists(db, "community_post") &&
        sqlite3_prepare_v2(db, recent_sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, target, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) points += 1;
        sqlite3_finalize(st);
    }
    if (type_exists(db, "person_quiz_answer")) {
        int quiz = 0;
        char token[16];
        if (quiz_points(db, viewer, target, &quiz) != 0) return -1;
        if (quiz > 0) {
            int m = snprintf(token, sizeof(token), "quiz+%d", quiz);
            if (m > 0 && used + (size_t)m + 2 < shared_cap) {
                if (used) shared_out[used++] = ',';
                memcpy(shared_out + used, token, (size_t)m + 1);
                used += (size_t)m;
            }
            points += quiz;
        }
    }
    *score = points;
    return 0;
}

static const char *attention_sql =
    "SELECT m.actor_subject, m.kind, substr(m.created_at,1,10),"
    " (SELECT s.host FROM site_presentation s WHERE s.subject_address=m.actor_subject"
    "  AND s.namespace='root' AND s.profile='member-profile-v1' AND s.status='published'"
    "  ORDER BY s.host LIMIT 1),"
    " (SELECT p.heading FROM site_page p WHERE p.namespace=("
    "   SELECT s2.site_id FROM site_presentation s2 WHERE s2.subject_address=m.actor_subject"
    "   AND s2.namespace='root' AND s2.profile='member-profile-v1' AND s2.status='published'"
    "   ORDER BY s2.host LIMIT 1) AND p.route='/' AND p.status='published' LIMIT 1)"
    " FROM person_affinity m"
    " WHERE m.target_subject=?1 AND m.status='published' AND m.state='active'"
    " AND (m.kind='like' OR m.kind='superlike')"
    " AND m.id=(SELECT MAX(id) FROM person_affinity"
    "  WHERE actor_subject=m.actor_subject AND target_subject=m.target_subject"
    "  AND status='published')"
    " AND NOT EXISTS (SELECT 1 FROM person_affinity b"
    "  WHERE ((b.actor_subject=m.actor_subject AND b.target_subject=?1)"
    "   OR (b.actor_subject=?1 AND b.target_subject=m.actor_subject))"
    "  AND b.status='published' AND b.state='active' AND b.kind='block'"
    "  AND b.id=(SELECT MAX(id) FROM person_affinity"
    "   WHERE actor_subject=b.actor_subject AND target_subject=b.target_subject"
    "   AND status='published'))"
    " ORDER BY m.id DESC LIMIT ?2";

typedef struct {
    char subject[192], kind[16], day[11], host[256], name[161];
} AttentionRow;

static int attention_rows(sqlite3 *db, const char *viewer, AttentionRow *rows, unsigned cap) {
    sqlite3_stmt *st = NULL;
    unsigned n = 0;
    if (sqlite3_prepare_v2(db, attention_sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, viewer, -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 2, (int)cap);
    while (n < cap && sqlite3_step(st) == SQLITE_ROW) {
        const char *s = (const char *)sqlite3_column_text(st, 0);
        const char *k = (const char *)sqlite3_column_text(st, 1);
        const char *d = (const char *)sqlite3_column_text(st, 2);
        const char *h = (const char *)sqlite3_column_text(st, 3);
        const char *name = (const char *)sqlite3_column_text(st, 4);
        if (!s || !k) continue;
        snprintf(rows[n].subject, sizeof(rows[n].subject), "%s", s);
        snprintf(rows[n].kind, sizeof(rows[n].kind), "%s", k);
        snprintf(rows[n].day, sizeof(rows[n].day), "%s", d ? d : "");
        snprintf(rows[n].host, sizeof(rows[n].host), "%s", h ? h : "");
        if (name && *name) snprintf(rows[n].name, sizeof(rows[n].name), "%s", name);
        else {
            const char *handle = strchr(s, ':');
            snprintf(rows[n].name, sizeof(rows[n].name), "%s",
                     handle && handle[1] ? handle + 1 : s);
        }
        n++;
    }
    sqlite3_finalize(st);
    return (int)n;
}

int bf_attention_page(FILE *out, sqlite3 *db, const char *viewer, unsigned limit) {
    AttentionRow rows[24];
    BfAuthoringTree *tree = NULL;
    int n, ok = 0;
    if (!out || !db || !subject_ok(viewer)) return -1;
    if (!limit || limit > 24) limit = 24;
    if (!type_exists(db, "person_affinity")) return 0;
    tree = calloc(1, sizeof(*tree));
    if (!tree) return -1;
    n = attention_rows(db, viewer, rows, limit);
    if (n < 0) goto done;
    for (int i = 0; i < n; i++) {
        char id[32], child[48], badge[64], home[300];
        FtLink links[1];
        unsigned nlinks = 0;
        snprintf(id, sizeof(id), "attn-%d", i);
        snprintf(badge, sizeof(badge), "%s%s%s", rows[i].kind,
                 rows[i].day[0] ? " \u00B7 " : "", rows[i].day);
        if (rows[i].host[0]) {
            snprintf(home, sizeof(home), "https://%.200s/", rows[i].host);
            links[0].label = "Open home";
            links[0].href = home;
            nlinks = 1;
        }
        snprintf(child, sizeof(child), "attn-%d-kind", i);
        if (!ft_card(tree, id, NULL, i, rows[i].name, "", links, nlinks) ||
            !ft_badge(tree, child, id, 0, badge)) goto done;
    }
    ok = 1;
done:
    if (!ok) {
        if (tree) {
            bf_authoring_tree_clear(tree);
            free(tree);
        }
        return -1;
    }
    if (!ft_render(tree, out)) return -1;
    if (!n) fputs("<p class=\"bf-native-empty\" data-empty>No recent attention.</p>", out);
    return n;
}

int bf_attention_json(FILE *out, sqlite3 *db, const char *viewer, unsigned limit) {
    AttentionRow rows[24];
    int n;
    if (!out || !db || !subject_ok(viewer)) return -1;
    if (!limit || limit > 24) limit = 24;
    if (!type_exists(db, "person_affinity")) {
        fputs("{\"attention\":[]}\n", out);
        return 0;
    }
    n = attention_rows(db, viewer, rows, limit);
    if (n < 0) return -1;
    {
        bf_jsonw w;
        bf_jsonw_init(&w, 0);
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "attention");
        bf_jsonw_arr(&w);
        for (int i = 0; i < n; i++) {
            bf_jsonw_obj(&w);
            bf_jsonw_key(&w, "subject"); bf_jsonw_str(&w, rows[i].subject);
            bf_jsonw_key(&w, "kind"); bf_jsonw_str(&w, rows[i].kind);
            bf_jsonw_key(&w, "day"); bf_jsonw_str(&w, rows[i].day);
            bf_jsonw_key(&w, "host"); bf_jsonw_str(&w, rows[i].host);
            bf_jsonw_key(&w, "name"); bf_jsonw_str(&w, rows[i].name);
            bf_jsonw_obj_end(&w);
        }
        bf_jsonw_arr_end(&w);
        bf_jsonw_obj_end(&w);
        if (w.oom || !w.buf) { bf_jsonw_free(&w); return -1; }
        fputs(w.buf, out);
        fputc('\n', out);
        bf_jsonw_free(&w);
    }
    return n;
}
