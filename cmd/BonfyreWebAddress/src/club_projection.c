#include "club_projection.h"
#include "bf_lex.h"
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

static int table_exists(sqlite3 *db, const char *name) {
    sqlite3_stmt *st = NULL;
    int found = 0;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
                           -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, name, -1, SQLITE_STATIC);
    found = sqlite3_step(st) == SQLITE_ROW;
    sqlite3_finalize(st);
    return found;
}

static int column_exists(sqlite3 *db, const char *table, const char *column) {
    sqlite3_stmt *st = NULL;
    int found = 0;
    char sql[256];
    snprintf(sql, sizeof(sql), "SELECT 1 FROM pragma_table_info('%s') WHERE name='%s' LIMIT 1",
             table, column);
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    found = sqlite3_step(st) == SQLITE_ROW;
    sqlite3_finalize(st);
    return found;
}

int bf_gold_active(sqlite3 *db, const char *actor) {
    sqlite3_stmt *st = NULL;
    int active = 0;
    if (!db || !subject_ok(actor)) return -1;
    if (!table_exists(db, "community_membership")) return 0;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM community_membership WHERE namespace='root'"
                           " AND community='gold' AND member_subject=?1 AND state='active'"
                           " AND status='published' AND expires_at IS NOT NULL"
                           " AND datetime(expires_at)>datetime('now') LIMIT 1",
                           -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, actor, -1, SQLITE_STATIC);
    active = sqlite3_step(st) == SQLITE_ROW;
    sqlite3_finalize(st);
    return active;
}

int bf_quota_superlike(sqlite3 *db, const char *actor, int *left) {
    sqlite3_stmt *st = NULL;
    int gold, used = 0;
    if (!db || !subject_ok(actor) || !left) return -1;
    gold = bf_gold_active(db, actor);
    if (gold < 0) return -1;
    if (gold) {
        *left = -1;
        return 0;
    }
    *left = 1;
    if (!table_exists(db, "person_affinity")) return 0;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM person_affinity WHERE actor_subject=?1"
                           " AND kind='superlike' AND state='active' AND status='published'"
                           " AND date(substr(created_at,1,10))=date('now')",
                           -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, actor, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) used = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    *left = used >= 1 ? 0 : 1;
    return 0;
}

int bf_quota_plans(sqlite3 *db, const char *actor, int *left) {
    sqlite3_stmt *st = NULL;
    int gold, used = 0;
    if (!db || !subject_ok(actor) || !left) return -1;
    gold = bf_gold_active(db, actor);
    if (gold < 0) return -1;
    if (gold) {
        *left = -1;
        return 0;
    }
    *left = 3;
    if (!table_exists(db, "date_plan")) return 0;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM date_plan WHERE actor_subject=?1"
                           " AND status='published'"
                           " AND date(substr(created_at,1,10))>=date('now','-7 days')",
                           -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, actor, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) used = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    *left = used >= 3 ? 0 : 3 - used;
    return 0;
}

int bf_clubs_page(FILE *out, sqlite3 *db, const char *viewer) {
    static const char *sql =
        "SELECT c.community, c.title, c.interest_label,"
        " (SELECT count(*) FROM community_membership m WHERE m.namespace='root'"
        "  AND m.community=c.community AND m.state='active' AND m.status='published')"
        " FROM community_body c WHERE c.namespace='root' AND c.status='published'"
        " AND c.interest_label IS NOT NULL AND c.interest_label<>''"
        " ORDER BY c.title LIMIT 25";
    sqlite3_stmt *st = NULL;
    BfAuthoringTree *tree = NULL;
    int n = 0, ok = 0;
    if (!out || !db || (viewer && *viewer && !subject_ok(viewer))) return -1;
    if (!table_exists(db, "community_body") || !column_exists(db, "community_body", "interest_label"))
        return 0;
    tree = calloc(1, sizeof(*tree));
    if (!tree) return -1;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) goto done;
    while (n < 24 && sqlite3_step(st) == SQLITE_ROW) {
        const char *community = (const char *)sqlite3_column_text(st, 0);
        const char *title = (const char *)sqlite3_column_text(st, 1);
        const char *label = (const char *)sqlite3_column_text(st, 2);
        int members = sqlite3_column_int(st, 3);
        sqlite3_stmt *mine = NULL;
        char id[40], body[420], child[48];
        int joined = 0;
        if (!community || !title) continue;
        if (viewer && *viewer && table_exists(db, "community_membership") &&
            sqlite3_prepare_v2(db, "SELECT 1 FROM community_membership WHERE namespace='root'"
                               " AND community=?1 AND member_subject=?2 AND state='active'"
                               " AND status='published' LIMIT 1", -1, &mine, NULL) == SQLITE_OK) {
            sqlite3_bind_text(mine, 1, community, -1, SQLITE_STATIC);
            sqlite3_bind_text(mine, 2, viewer, -1, SQLITE_STATIC);
            joined = sqlite3_step(mine) == SQLITE_ROW;
        }
        sqlite3_finalize(mine);
        FtLink links[1];
        char href[200];
        snprintf(id, sizeof(id), "club-%d", n);
        snprintf(body, sizeof(body), "%s \u00B7 %d members", label ? label : "", members);
        snprintf(href, sizeof(href), "/companions?club=%s", community);
        links[0].label = "View members";
        links[0].href = href;
        if (!ft_card(tree, id, NULL, n, title, body, links, 1)) goto done;
        if (joined) {
            snprintf(child, sizeof(child), "club-%d-joined", n);
            if (!ft_badge(tree, child, id, 0, "Joined")) goto done;
        } else if (viewer && *viewer) {
            FtField fields[1];
            snprintf(child, sizeof(child), "club-%d-join", n);
            fields[0].name = "club";
            fields[0].value = community;
            if (!ft_action(tree, child, id, 0, "Join", "post", "/companions/clubs/join", fields, 1)) goto done;
        }
        n++;
    }
    ok = 1;
done:
    sqlite3_finalize(st);
    if (!ok || n < 0) {
        if (tree) {
            bf_authoring_tree_clear(tree);
            free(tree);
        }
        return -1;
    }
    if (!ft_render(tree, out)) return -1;
    if (!n) fputs("<p class=\"bf-native-empty\" data-empty>No interest clubs yet.</p>", out);
    return n;
}

static const char *notices_sql =
    "SELECT kind, subject, at, extra FROM ("
    " SELECT 'mutual' AS kind,"
    "  CASE WHEN l1.actor_subject=?1 THEN l1.target_subject ELSE l1.actor_subject END AS subject,"
    "  MAX(l1.created_at, l2.created_at) AS at, '' AS extra"
    "  FROM person_affinity l1 JOIN person_affinity l2"
    "   ON l1.target_subject=l2.actor_subject AND l2.target_subject=l1.actor_subject"
    "  WHERE (l1.actor_subject=?1 OR l1.target_subject=?1)"
    "   AND l1.status='published' AND l1.state='active'"
    "   AND (l1.kind='like' OR l1.kind='superlike')"
    "   AND l2.status='published' AND l2.state='active'"
    "   AND (l2.kind='like' OR l2.kind='superlike')"
    "   AND l1.id=(SELECT MAX(id) FROM person_affinity WHERE actor_subject=l1.actor_subject"
    "    AND target_subject=l1.target_subject AND status='published')"
    "   AND l2.id=(SELECT MAX(id) FROM person_affinity WHERE actor_subject=l2.actor_subject"
    "    AND target_subject=l2.target_subject AND status='published')"
    "   AND l1.created_at GLOB '____-__-__*' AND l2.created_at GLOB '____-__-__*'"
    " UNION ALL"
    " SELECT 'superlike', m.actor_subject, m.created_at, ''"
    "  FROM person_affinity m WHERE m.target_subject=?1 AND m.status='published'"
    "  AND m.state='active' AND m.kind='superlike'"
    "  AND m.id=(SELECT MAX(id) FROM person_affinity WHERE actor_subject=m.actor_subject"
    "   AND target_subject=m.target_subject AND status='published')"
    "  AND m.created_at GLOB '____-__-__*'"
    " UNION ALL"
    " SELECT 'rsvp', r.actor_subject, r.created_at,"
    "  (SELECT p.title FROM community_post p WHERE p.post=r.post LIMIT 1)"
    "  FROM community_reaction r JOIN community_post p ON p.post=r.post"
    "  WHERE p.author_subject=?1 AND r.actor_subject<>?1 AND r.kind='rsvp'"
    "  AND r.state='active' AND r.status='published'"
    "  AND r.id=(SELECT MAX(id) FROM community_reaction WHERE post=r.post"
    "   AND actor_subject=r.actor_subject AND status='published')"
    "  AND r.created_at GLOB '____-__-__*'"
    ") ORDER BY at DESC LIMIT 24";

typedef struct {
    char kind[16], subject[192], at[20], extra[221];
} NoticeRow;

static int notices_query(sqlite3 *db, const char *viewer, NoticeRow *rows, unsigned cap) {
    sqlite3_stmt *st = NULL;
    unsigned n = 0;
    if (sqlite3_prepare_v2(db, notices_sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, viewer, -1, SQLITE_STATIC);
    while (n < cap && sqlite3_step(st) == SQLITE_ROW) {
        const char *k = (const char *)sqlite3_column_text(st, 0);
        const char *s = (const char *)sqlite3_column_text(st, 1);
        const char *t = (const char *)sqlite3_column_text(st, 2);
        const char *e = (const char *)sqlite3_column_text(st, 3);
        if (!k || !s) continue;
        snprintf(rows[n].kind, sizeof(rows[n].kind), "%s", k);
        snprintf(rows[n].subject, sizeof(rows[n].subject), "%s", s);
        snprintf(rows[n].at, sizeof(rows[n].at), "%.10s", t ? t : "");
        snprintf(rows[n].extra, sizeof(rows[n].extra), "%s", e ? e : "");
        n++;
    }
    sqlite3_finalize(st);
    return (int)n;
}

static void notice_name(sqlite3 *db, const char *subject, char *out, size_t cap) {
    sqlite3_stmt *st = NULL;
    const char *handle = strchr(subject, ':');
    snprintf(out, cap, "%s", handle && handle[1] ? handle + 1 : subject);
    if (sqlite3_prepare_v2(db, "SELECT heading FROM site_page WHERE namespace=("
                           " SELECT site_id FROM site_presentation WHERE subject_address=?1"
                           " AND namespace='root' AND profile='member-profile-v1'"
                           " AND status='published' ORDER BY host LIMIT 1)"
                           " AND route='/' AND status='published' LIMIT 1",
                           -1, &st, NULL) != SQLITE_OK) return;
    sqlite3_bind_text(st, 1, subject, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(st, 0);
        if (name && *name) snprintf(out, cap, "%s", name);
    }
    sqlite3_finalize(st);
}

int bf_notices_page(FILE *out, sqlite3 *db, const char *viewer) {
    NoticeRow rows[24];
    BfAuthoringTree *tree = NULL;
    int n, ok = 0;
    if (!out || !db || !subject_ok(viewer)) return -1;
    if (!table_exists(db, "person_affinity")) return 0;
    tree = calloc(1, sizeof(*tree));
    if (!tree) return -1;
    n = notices_query(db, viewer, rows, 24);
    if (n < 0) goto done;
    for (int i = 0; i < n; i++) {
        char name[161], id[32], child[40], badge[48], text[260];
        notice_name(db, rows[i].subject, name, sizeof(name));
        if (!strcmp(rows[i].kind, "mutual")) snprintf(text, sizeof(text), "New match.");
        else if (!strcmp(rows[i].kind, "superlike")) snprintf(text, sizeof(text), "Superliked you.");
        else if (rows[i].extra[0]) snprintf(text, sizeof(text), "RSVPed to %s.", rows[i].extra);
        else snprintf(text, sizeof(text), "RSVPed.");
        snprintf(badge, sizeof(badge), "%s%s%s", rows[i].kind,
                 rows[i].at[0] ? " \u00B7 " : "", rows[i].at);
        snprintf(id, sizeof(id), "notice-%d", i);
        snprintf(child, sizeof(child), "notice-%d-kind", i);
        if (!ft_card(tree, id, NULL, i, name, text, NULL, 0) ||
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
    if (!n) fputs("<p class=\"bf-native-empty\" data-empty>Nothing new.</p>", out);
    return n;
}

int bf_notices_json(FILE *out, sqlite3 *db, const char *viewer) {
    NoticeRow rows[24];
    int n;
    if (!out || !db || !subject_ok(viewer)) return -1;
    if (!table_exists(db, "person_affinity")) {
        fputs("{\"notices\":[]}\n", out);
        return 0;
    }
    n = notices_query(db, viewer, rows, 24);
    if (n < 0) return -1;
    {
        bf_jsonw w;
        bf_jsonw_init(&w, 0);
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "notices");
        bf_jsonw_arr(&w);
        for (int i = 0; i < n; i++) {
            bf_jsonw_obj(&w);
            bf_jsonw_key(&w, "kind"); bf_jsonw_str(&w, rows[i].kind);
            bf_jsonw_key(&w, "subject"); bf_jsonw_str(&w, rows[i].subject);
            bf_jsonw_key(&w, "at"); bf_jsonw_str(&w, rows[i].at);
            bf_jsonw_key(&w, "extra"); bf_jsonw_str(&w, rows[i].extra);
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

static int viewer_rsvp(sqlite3 *db, const char *viewer, const char *post) {
    sqlite3_stmt *st = NULL;
    int yes = 0;
    if (!viewer || !*viewer || !table_exists(db, "community_reaction")) return 0;
    if (sqlite3_prepare_v2(db, "SELECT state FROM community_reaction"
                           " WHERE post=?1 AND actor_subject=?2 AND kind='rsvp'"
                           " AND status='published' ORDER BY id DESC LIMIT 1",
                           -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, post, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, viewer, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *state = (const char *)sqlite3_column_text(st, 0);
        yes = state && !strcmp(state, "active");
    }
    sqlite3_finalize(st);
    return yes;
}

int bf_events_page(FILE *out, sqlite3 *db, const char *viewer, const char *club) {
    static const char *sql =
        "SELECT p.post, p.title, p.event_at, p.venue, p.community,"
        " (SELECT count(*) FROM community_reaction r WHERE r.post=p.post AND r.kind='rsvp'"
        "  AND r.state='active' AND r.status='published'"
        "  AND r.id=(SELECT MAX(id) FROM community_reaction"
        "   WHERE post=r.post AND actor_subject=r.actor_subject AND status='published'))"
        " FROM community_post p WHERE p.status='published' AND p.visibility='public'"
        " AND p.moderation_state='accepted' AND p.event_at IS NOT NULL AND p.event_at<>''"
        " AND datetime(p.event_at)>=datetime('now') AND (?1='' OR p.community=?1)"
        " ORDER BY datetime(p.event_at) LIMIT 25";
    sqlite3_stmt *st = NULL;
    BfAuthoringTree *tree = NULL;
    int n = 0, ok = 0;
    if (!out || !db || (viewer && *viewer && !subject_ok(viewer))) return -1;
    if (club && strlen(club) > 120) return -1;
    if (!table_exists(db, "community_post") || !column_exists(db, "community_post", "event_at")) return 0;
    tree = calloc(1, sizeof(*tree));
    if (!tree) return -1;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(st, 1, club ? club : "", -1, SQLITE_STATIC);
    while (n < 24 && sqlite3_step(st) == SQLITE_ROW) {
        const char *post = (const char *)sqlite3_column_text(st, 0);
        const char *title = (const char *)sqlite3_column_text(st, 1);
        const char *at = (const char *)sqlite3_column_text(st, 2);
        const char *venue = (const char *)sqlite3_column_text(st, 3);
        const char *community = (const char *)sqlite3_column_text(st, 4);
        int count = sqlite3_column_int(st, 5);
        char id[32], child[48], body[420], going[48];
        int pos = 0;
        if (!post) continue;
        snprintf(id, sizeof(id), "event-%d", n);
        if (venue && *venue && community && *community)
            snprintf(body, sizeof(body), "%s \u00B7 %s", venue, community);
        else if (venue && *venue)
            snprintf(body, sizeof(body), "%s", venue);
        else if (community && *community)
            snprintf(body, sizeof(body), "%s", community);
        else
            body[0] = '\0';
        if (!ft_card(tree, id, NULL, n, title && *title ? title : post, body, NULL, 0)) goto done;
        if (at && *at) {
            snprintf(child, sizeof(child), "event-%d-when", n);
            if (!ft_badge(tree, child, id, pos++, at)) goto done;
        }
        snprintf(going, sizeof(going), "%d going", count);
        snprintf(child, sizeof(child), "event-%d-going", n);
        if (!ft_badge(tree, child, id, pos++, going)) goto done;
        if (viewer && *viewer) {
            if (viewer_rsvp(db, viewer, post)) {
                snprintf(child, sizeof(child), "event-%d-rsvp", n);
                if (!ft_badge(tree, child, id, pos++, "Going")) goto done;
            } else {
                FtField fields[1];
                snprintf(child, sizeof(child), "event-%d-rsvp", n);
                fields[0].name = "post";
                fields[0].value = post;
                if (!ft_action(tree, child, id, pos++, "RSVP", "post", "/companions/events/rsvp", fields, 1)) goto done;
            }
        }
        n++;
    }
    ok = 1;
done:
    sqlite3_finalize(st);
    if (!ok) {
        if (tree) {
            bf_authoring_tree_clear(tree);
            free(tree);
        }
        return -1;
    }
    if (!ft_render(tree, out)) return -1;
    if (!n) fputs("<p class=\"bf-native-empty\" data-empty>No upcoming events.</p>", out);
    return n;
}
