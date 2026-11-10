#define _POSIX_C_SOURCE 200809L
#include "community_projection.h"

#include <ctype.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf_lex.h"

static void reply(int code, const char *body) {
    const char *reason = code == 200 ? "OK" : code == 400 ? "Bad Request" :
                         code == 404 ? "Not Found" : code == 405 ? "Method Not Allowed" :
                         "Service Unavailable";
    printf("HTTP/1.1 %d %s\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n%s",
           code, reason, body ? body : "{}\n");
}

static void json(const char *s) {
    bf_lex_json_emit(stdout, s);
}

static int slug_ok(const char *s) {
    size_t n = 0;
    if (!s || !*s) return 0;
    for (; *s; ++s, ++n)
        if (n >= 96 || !(isalnum((unsigned char)*s) || *s == '-' || *s == '_' || *s == '.')) return 0;
    return 1;
}

static int query_community(const char *query, char *out, size_t cap) {
    const char *p = query ? strstr(query, "community=") : NULL;
    size_t n;
    if (!p) return 0;
    p += 10;
    n = strcspn(p, "&");
    if (!n || n >= cap) return 0;
    memcpy(out, p, n); out[n] = '\0';
    return slug_ok(out);
}

static int type_exists(sqlite3 *db, const char *type) {
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1", -1, &st, NULL) != SQLITE_OK)
        return 0;
    sqlite3_bind_text(st, 1, type, -1, SQLITE_TRANSIENT);
    ok = sqlite3_step(st) == SQLITE_ROW;
    sqlite3_finalize(st);
    return ok;
}

/* Membership and presence are temporal relations.  A record retained for
 * evidence after its stated end must not remain socially current merely
 * because it still has `state=active`.  julianday accepts the RFC3339 offsets
 * emitted by foreign and native writers, unlike a lexical string comparison. */
static const char *active_membership_sql =
    "status='published' AND state='active' AND "
    "(COALESCE(expires_at,'')='' OR julianday(expires_at)>julianday('now'))";

static int reaction_count(sqlite3 *db, const char *post) {
    sqlite3_stmt *st = NULL;
    int count = 0;
    if (!type_exists(db, "community_reaction") ||
        sqlite3_prepare_v2(db,
          "SELECT COUNT(*) FROM community_reaction WHERE post=?1 AND status='published' AND state='active'",
          -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, post ? post : "", -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) count = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    return count;
}

int bf_serve_human_community(const char *method, const char *target, const char *query) {
    const char *path = getenv("BONFYRE_CMS_DB");
    char community[128];
    sqlite3 *db = NULL;
    sqlite3_stmt *body = NULL, *members = NULL, *posts = NULL, *presence = NULL;
    int member_count = 0, presence_count = 0, first = 1;
    if (!target || strcmp(target, "/_surface/community-body")) return 0;
    if (!method || strcmp(method, "GET")) { reply(405, "{\"error\":\"GET required\"}\n"); return 1; }
    if (!query_community(query, community, sizeof(community))) { reply(400, "{\"error\":\"community_required\"}\n"); return 1; }
    if (!path || !*path) path = "/var/lib/bonfyre/cms/bonfyre_cms.db";
    if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) goto unavailable;
    if (!type_exists(db, "community_body") || !type_exists(db, "community_membership") || !type_exists(db, "community_post")) goto unavailable;
    if (sqlite3_prepare_v2(db,
        "SELECT subject_address,title,COALESCE(description,''),visibility,COALESCE(governance_scope,''),COALESCE(moderation_policy,''),cannot_infer "
        "FROM community_body WHERE community=?1 AND status='published' AND visibility='public' ORDER BY id DESC LIMIT 1",
        -1, &body, NULL) != SQLITE_OK) goto unavailable;
    sqlite3_bind_text(body, 1, community, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(body) != SQLITE_ROW) { sqlite3_finalize(body); sqlite3_close(db); reply(404, "{\"error\":\"community_not_found\"}\n"); return 1; }
    printf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n");
    fputs("{\"schema_version\":\"bonfyre.human_community.v1\",\"community\":", stdout); json(community);
    fputs(",\"subject_address\":", stdout); json((const char *)sqlite3_column_text(body, 0));
    fputs(",\"title\":", stdout); json((const char *)sqlite3_column_text(body, 1));
    fputs(",\"description\":", stdout); json((const char *)sqlite3_column_text(body, 2));
    fputs(",\"visibility\":", stdout); json((const char *)sqlite3_column_text(body, 3));
    fputs(",\"governance_scope\":", stdout); json((const char *)sqlite3_column_text(body, 4));
    fputs(",\"moderation_policy\":", stdout); json((const char *)sqlite3_column_text(body, 5));
    fputs(",\"cannot_infer\":", stdout); json((const char *)sqlite3_column_text(body, 6));
    sqlite3_finalize(body); body = NULL;
    {
        char sql[512];
        snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM community_membership WHERE community=?1 AND %s", active_membership_sql);
        if (sqlite3_prepare_v2(db, sql, -1, &members, NULL) == SQLITE_OK) {
        sqlite3_bind_text(members, 1, community, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(members) == SQLITE_ROW) member_count = sqlite3_column_int(members, 0);
        }
    }
    if (members) sqlite3_finalize(members);
    if (type_exists(db, "community_presence") && sqlite3_prepare_v2(db,
        "SELECT COUNT(*) FROM community_presence p WHERE p.community=?1 AND p.surface='community' "
        "AND p.status='published' AND p.disclosure='public' "
        "AND julianday(p.observed_at)<=julianday('now') AND julianday(p.expires_at)>julianday('now') "
        "AND EXISTS (SELECT 1 FROM community_membership m WHERE m.community=p.community "
        "AND m.member_subject=p.actor_subject AND m.status='published' AND m.state='active' "
        "AND (COALESCE(m.expires_at,'')='' OR julianday(m.expires_at)>julianday('now')))",
        -1, &presence, NULL) == SQLITE_OK) {
        sqlite3_bind_text(presence, 1, community, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(presence) == SQLITE_ROW) presence_count = sqlite3_column_int(presence, 0);
        sqlite3_finalize(presence); presence = NULL;
    }
    printf(",\"member_count\":%d,\"public_presence_count\":%d,\"records\":[", member_count, presence_count);
    if (sqlite3_prepare_v2(db,
        "SELECT post,author_subject,COALESCE(subject_address,''),COALESCE(title,''),body,COALESCE(media_program,''),COALESCE(reply_to,''),published_at,moderation_state,cannot_infer "
        "FROM community_post WHERE community=?1 AND status='published' AND visibility='public' AND moderation_state IN ('accepted','visible','published') ORDER BY published_at DESC,id DESC LIMIT 100",
        -1, &posts, NULL) != SQLITE_OK) { sqlite3_close(db); fputs("],\"error\":\"post_query_failed\"}\n", stdout); return 1; }
    sqlite3_bind_text(posts, 1, community, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(posts) == SQLITE_ROW) {
        if (!first) putchar(',');
        first = 0;
        const char *post = (const char *)sqlite3_column_text(posts, 0);
        fputs("{\"post\":", stdout); json(post);
        fputs(",\"author_subject\":", stdout); json((const char *)sqlite3_column_text(posts, 1));
        fputs(",\"subject_address\":", stdout); json((const char *)sqlite3_column_text(posts, 2));
        fputs(",\"title\":", stdout); json((const char *)sqlite3_column_text(posts, 3));
        fputs(",\"body\":", stdout); json((const char *)sqlite3_column_text(posts, 4));
        fputs(",\"media_program\":", stdout); json((const char *)sqlite3_column_text(posts, 5));
        fputs(",\"reply_to\":", stdout); json((const char *)sqlite3_column_text(posts, 6));
        fputs(",\"published_at\":", stdout); json((const char *)sqlite3_column_text(posts, 7));
        fputs(",\"moderation_state\":", stdout); json((const char *)sqlite3_column_text(posts, 8));
        printf(",\"reaction_count\":%d", reaction_count(db, post));
        fputs(",\"cannot_infer\":", stdout); json((const char *)sqlite3_column_text(posts, 9)); putchar('}');
    }
    sqlite3_finalize(posts); sqlite3_close(db);
    fputs("],\"federation\":\"projected elsewhere\",\"truth_level\":\"authored_human_social_body\"}\n", stdout);
    return 1;
unavailable:
    if (body) sqlite3_finalize(body);
    if (members) sqlite3_finalize(members);
    if (presence) sqlite3_finalize(presence);
    if (posts) sqlite3_finalize(posts);
    if (db) sqlite3_close(db);
    reply(503, "{\"error\":\"community_store_unavailable\"}\n"); return 1;
}
