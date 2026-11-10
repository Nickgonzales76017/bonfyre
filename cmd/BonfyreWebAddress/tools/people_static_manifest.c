#include <sqlite3.h>
#include <bonfyre.h>
#include <bf_static_query.h>
#include <bf_sqlite_path.h>
#include "../src/credit_profile_host.h"
#include "../src/people_name_filter.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int emit_coordinate(const unsigned char *who, size_t who_len,
                           const unsigned char *source, size_t source_len, int nul) {
    static const char path_prefix[] = "/people?";
    static const char key_prefix[] = "people/__q/h/";
    size_t input_len, capacity, query_len;
    char *query, *end, hex[65];
    if (who_len > SIZE_MAX - source_len) return 0;
    input_len = who_len + source_len;
    if (input_len > (SIZE_MAX - 32) / 3) return 0;
    capacity = input_len * 3 + 32;
    query = malloc(capacity);
    if (!query) return 0;
    end = query;
    memcpy(end, "who=", 4); end += 4;
    end = bf_static_query_encode(end, who, who_len);
    if (source) {
        memcpy(end, "&source=", 8); end += 8;
        end = bf_static_query_encode(end, source, source_len);
    }
    query_len = (size_t)(end - query);
    bf_sha256_hex((const uint8_t *)query, query_len, hex);
    if (fwrite(path_prefix, 1, sizeof(path_prefix) - 1, stdout) != sizeof(path_prefix) - 1 ||
        fwrite(query, 1, query_len, stdout) != query_len ||
        fputc(nul ? '\0' : '\t', stdout) == EOF ||
        fwrite(key_prefix, 1, sizeof(key_prefix) - 1, stdout) != sizeof(key_prefix) - 1 ||
        fwrite(hex, 1, 64, stdout) != 64 ||
        fwrite(".html", 1, 5, stdout) != 5 ||
        fputc(nul ? '\0' : '\n', stdout) == EOF) {
        free(query);
        return 0;
    }
    free(query);
    return 1;
}

static int emit_query(sqlite3 *db, const char *sql, int with_source, int nul) {
    sqlite3_stmt *statement = NULL;
    int rc;
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) != SQLITE_OK) return 0;
    while ((rc = sqlite3_step(statement)) == SQLITE_ROW) {
        int who_col = with_source ? 1 : 0;
        const unsigned char *who = sqlite3_column_text(statement, who_col);
        const unsigned char *source = with_source ? sqlite3_column_text(statement, 0) : NULL;
        int who_len = sqlite3_column_bytes(statement, who_col);
        int source_len = with_source ? sqlite3_column_bytes(statement, 0) : 0;
        if (!who || who_len < 0 || (with_source && (!source || source_len < 0)) ||
            !emit_coordinate(who, (size_t)who_len, source, (size_t)source_len, nul)) {
            sqlite3_finalize(statement);
            return 0;
        }
    }
    sqlite3_finalize(statement);
    return rc == SQLITE_DONE;
}

/* Each observed source credit gets a read-only public address. The leaf points
 * to existing /people?who&source material and therefore existing /o detail
 * pages. This is not a verified member profile or a copied publication. */
static int emit_profile_coordinate(const unsigned char *who, int who_len,
                                   const unsigned char *source, int source_len, int nul) {
    char host[128], *query, *end;
    char key[192];
    size_t input_len, capacity, length;
    if (!who || !source || who_len < 1 || source_len < 1 ||
        (size_t)who_len != strlen((const char *)who) ||
        (size_t)source_len != strlen((const char *)source) ||
        !people_name_plausible((const char *)who) ||
        !bf_credit_profile_host((const char *)who, (const char *)source, host))
        return 1; /* not an admitted maker credit */
    input_len = (size_t)who_len + (size_t)source_len;
    if (input_len > (SIZE_MAX - 160) / 3) return 0;
    capacity = input_len * 3 + 160;
    query = malloc(capacity);
    if (!query) return 0;
    end = query;
    const char prefix[] = "/people?who=";
    memcpy(end, prefix, sizeof(prefix)-1); end += sizeof(prefix)-1;
    end = bf_static_query_encode(end, who, (size_t)who_len);
    memcpy(end, "&source=", 8); end += 8;
    end = bf_static_query_encode(end, source, (size_t)source_len);
    memcpy(end, "&profile=1", 10); end += 10;
    length = (size_t)(end - query);
    int n = snprintf(key, sizeof(key), "hosts/%s/index.html", host);
    int ok = n > 0 && (size_t)n < sizeof(key) &&
        fwrite(query, 1, length, stdout) == length &&
        fputc(nul ? '\0' : '\t', stdout) != EOF &&
        fputs(key, stdout) != EOF &&
        fputc(nul ? '\0' : '\n', stdout) != EOF;
    free(query);
    return ok;
}
static int emit_profile_index(sqlite3 *db, int nul, int cms_attached) {
    sqlite3_stmt *statement = NULL;
    const char *base =
        "SELECT source,by FROM item WHERE COALESCE(by,'')<>''"
        " AND COALESCE(source,'')<>''"
        " AND by NOT IN (SELECT publisher FROM item WHERE COALESCE(publisher,'')<>'')"
        " AND COALESCE(at,0)<=CAST(strftime('%s','now') AS INTEGER)+86400"
        " AND family IN('news','today','local-news','podcasts','books','culture','music',"
        "'entertainment','sports','gaming','food','home','style','shopping','health','travel',"
        "'small-business','commons','nature','local-nature','vulnerabilities')"
        ;
    const char *const cte = "WITH source_credits AS MATERIALIZED ("
        "SELECT source,lower(COALESCE(label,'')) AS name FROM cms_source.internet_source WHERE status='published'"
        " UNION ALL SELECT source,lower(COALESCE(publisher_name,'')) FROM cms_source.internet_source WHERE status='published') ";
    const char *const reject =
        " AND NOT EXISTS(SELECT 1 FROM source_credits c WHERE c.source=item.source AND c.name=lower(item.by))";
    char sql[3000];
    int len=snprintf(sql,sizeof(sql),"%s%s%s GROUP BY source,by ORDER BY by,source",
        cms_attached?cte:"",base,cms_attached?reject:"");
    if(len<0||(size_t)len>=sizeof(sql))return 0;
    int rc = SQLITE_ERROR;
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) != SQLITE_OK) return 0;
    while ((rc = sqlite3_step(statement)) == SQLITE_ROW) {
        if (!emit_profile_coordinate(sqlite3_column_text(statement, 1),
           sqlite3_column_bytes(statement, 1), sqlite3_column_text(statement, 0),
           sqlite3_column_bytes(statement, 0), nul)) break;
    }
    sqlite3_finalize(statement);
    return rc == SQLITE_DONE;
}

int main(int argc, char **argv) {
    const char *archive = NULL;
    const char *cms_path = NULL;
    sqlite3 *db = NULL;
    int nul = 0, profiles = 0, attached = 0, ok;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--archive") && i + 1 < argc) archive = argv[++i];
        else if (!strcmp(argv[i], "--cms") && i + 1 < argc) cms_path = argv[++i];
        else if (!strcmp(argv[i], "--null")) nul = 1;
        else if (!strcmp(argv[i], "--profile-hosts")) profiles = 1;
        else {
            fputs("usage: people-static-manifest --archive items.db [--null] [--profile-hosts]\n", stderr);
            return 2;
        }
    }
    if (!archive || sqlite3_open_v2(archive, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        fputs("people-static-manifest: archive is not readable\n", stderr);
        if (db) sqlite3_close(db);
        return 1;
    }
    if(profiles && cms_path && *cms_path){
        char uri[PATH_MAX*3+32];
        sqlite3_stmt *probe=NULL;
        int ready=0;
        if(bf_sqlite_readonly_uri(uri,sizeof(uri),cms_path)<(int)sizeof(uri)){
            char *attach=sqlite3_mprintf("ATTACH DATABASE %Q AS cms_source",uri);
            if(attach && sqlite3_exec(db,attach,NULL,NULL,NULL)==SQLITE_OK){
                if(sqlite3_prepare_v2(db,
                   "SELECT 1 FROM cms_source.sqlite_master WHERE type='table' AND name='internet_source'",
                   -1,&probe,NULL)==SQLITE_OK && sqlite3_step(probe)==SQLITE_ROW)ready=1;
            }
            sqlite3_free(attach);
        }
        sqlite3_finalize(probe);
        if(!ready){fputs("people-static-manifest: exact source-credit CMS unavailable\n",stderr);sqlite3_close(db);return 1;}
        attached=1;
    }
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    ok = profiles ? emit_profile_index(db, nul, attached) :
         (emit_query(db, "SELECT DISTINCT by FROM item WHERE COALESCE(by,'')<>'' ORDER BY by", 0, nul) &&
         emit_query(db, "SELECT source,by FROM item WHERE COALESCE(source,'')<>'' AND COALESCE(by,'')<>'' "
                        "GROUP BY source,by ORDER BY source,by", 1, nul));
    sqlite3_close(db);
    if (!ok || fflush(stdout) != 0) {
        fputs("people-static-manifest: archive query or output failed\n", stderr);
        return 1;
    }
    return 0;
}
