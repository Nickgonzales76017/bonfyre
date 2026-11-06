#define _POSIX_C_SOURCE 200809L
#include "owner_resolution.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>

/* Only declarations are loaded. This reader neither opens owner databases nor
 * invokes a command, provider or model. Search weights rank candidates only. */
typedef struct { char words[64][65]; int count; } Terms;

static int terms_read(Terms *terms, const char *text) {
    static const char *stop = " a an and are as at be by can do for from how i in into is it me my of on or our that the their this to us want we with would ";
    size_t n = strlen(text);
    if (!n || n > 4096) return 0;
    while (*text) {
        char word[65], padded[68]; size_t used = 0; int duplicate = 0;
        while (*text && !isalnum((unsigned char)*text)) text++;
        while (*text && isalnum((unsigned char)*text)) {
            if (used == sizeof(word) - 1) return 0;
            word[used++] = (char)tolower((unsigned char)*text++);
        }
        if (!used) break;
        word[used] = 0;
        snprintf(padded, sizeof(padded), " %s ", word);
        if (strstr(stop, padded)) continue;
        for (int i = 0; i < terms->count; i++) if (!strcmp(terms->words[i], word)) duplicate = 1;
        if (duplicate) continue;
        if (terms->count == 64) return 0;
        strcpy(terms->words[terms->count++], word);
    }
    return terms->count > 0;
}

static int word_present(const char *text, const char *word) {
    size_t n = strlen(word);
    while (*text) {
        const char *start; size_t used;
        while (*text && !isalnum((unsigned char)*text)) text++;
        start = text;
        while (*text && isalnum((unsigned char)*text)) text++;
        used = (size_t)(text - start);
        if (used != n) continue;
        size_t i;
        for (i = 0; i < n && tolower((unsigned char)start[i]) == (unsigned char)word[i]; i++);
        if (i == n) return 1;
    }
    return 0;
}

static void score(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    Terms *terms = sqlite3_user_data(ctx); int result = 0;
    (void)argc;
    const char *value = (const char *)sqlite3_value_text(argv[0]);
    if (value) for (int i = 0; i < terms->count; i++) result += word_present(value, terms->words[i]);
    sqlite3_result_int(ctx, result);
}

static int nul_escape(const char *text) {
    for (const char *p = text; *p; p++) {
        if (*p != '\\') continue;
        if (!strncmp(p + 1, "u0000", 5)) return 1;
        if (p[1]) p++;
    }
    return 0;
}

static int statement(sqlite3 *db, const char *sql, const char *value) {
    sqlite3_stmt *st = NULL; int ok = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK &&
        (!value || sqlite3_bind_text(st, 1, value, -1, SQLITE_STATIC) == SQLITE_OK))
        ok = sqlite3_step(st) == SQLITE_DONE;
    sqlite3_finalize(st); return ok;
}

int experience_resolve(const char *root, const char *objective, const char *anchor_limit) {
    Terms terms = {0}; char path[1200], *data = NULL, *end = NULL;
    FILE *fp = NULL; sqlite3 *db = NULL; sqlite3_stmt *st = NULL;
    long limit = anchor_limit ? strtol(anchor_limit, &end, 10) : 5;
    int rc = 2, length; long bytes;
    if (!terms_read(&terms, objective) || limit < 1 || limit > 10 || (end && *end)) {
        fprintf(stderr, "experience-map: objective needs 1-64 distinct search words (4096 bytes maximum); anchors must be 1-10\n");
        return 2;
    }
    length = snprintf(path, sizeof(path), "%s/owner_graph.json", root);
    if (length < 0 || (size_t)length >= sizeof(path) || !(fp = fopen(path, "rb"))) goto done;
    if (fseek(fp, 0, SEEK_END) || (bytes = ftell(fp)) < 1 || bytes > 8 * 1024 * 1024 || fseek(fp, 0, SEEK_SET)) goto done;
    data = malloc((size_t)bytes + 1);
    if (!data || fread(data, 1, (size_t)bytes, fp) != (size_t)bytes || memchr(data, 0, (size_t)bytes)) goto done;
    data[bytes] = 0;
    if (nul_escape(data)) goto done;
    if (sqlite3_open(":memory:", &db) != SQLITE_OK ||
        sqlite3_create_function(db, "term_score", 1, SQLITE_UTF8, &terms, score, NULL, NULL) != SQLITE_OK) goto done;
    /* A primary key rejects duplicate identities before any result is printed. */
    if (sqlite3_exec(db,
        "CREATE TABLE graph(body TEXT NOT NULL);"
        "CREATE TABLE owners(id TEXT PRIMARY KEY NOT NULL CHECK(length(id)>0),body TEXT NOT NULL);"
        "CREATE TABLE edges(id TEXT PRIMARY KEY NOT NULL,source TEXT NOT NULL,destination TEXT NOT NULL,body TEXT NOT NULL);"
        "CREATE TABLE workflows(id TEXT PRIMARY KEY NOT NULL,body TEXT NOT NULL);",
        NULL, NULL, NULL) != SQLITE_OK ||
        !statement(db, "INSERT INTO graph VALUES(?1)", data)) goto done;
    if (sqlite3_prepare_v2(db,
        "SELECT json_extract(body,'$.schema')='bonfyre.experience-owner-graph.v1' "
        "AND json_type(body,'$.owners')='array' AND json_type(body,'$.interactions')='array' "
        "AND json_type(body,'$.workflows')='array' "
        "AND length(json_extract(body,'$.atlas_declaration_digest'))=64 "
        "AND NOT EXISTS(SELECT 1 FROM json_tree(body) WHERE parent IS NOT NULL GROUP BY parent,key HAVING count(*)>1) "
        "AND NOT EXISTS(SELECT 1 FROM json_each(body,'$.owners') WHERE json_type(value,'$.id') IS NOT 'text' OR json_type(value,'$.experience_chains') IS NOT 'array') "
        "AND NOT EXISTS(SELECT 1 FROM json_each(body,'$.interactions') WHERE json_type(value,'$.id') IS NOT 'text' OR json_type(value,'$.source') IS NOT 'text' OR json_type(value,'$.destination') IS NOT 'text') "
        "AND NOT EXISTS(SELECT 1 FROM json_each(body,'$.workflows') WHERE json_type(value,'$.id') IS NOT 'text') FROM graph", -1, &st, NULL) != SQLITE_OK ||
        sqlite3_step(st) != SQLITE_ROW || sqlite3_column_int(st, 0) != 1) goto done;
    sqlite3_finalize(st); st = NULL;
    if (!statement(db, "INSERT INTO owners SELECT json_extract(value,'$.id'),value FROM graph,json_each(body,'$.owners')", NULL) ||
        !statement(db, "INSERT INTO edges SELECT json_extract(value,'$.id'),json_extract(value,'$.source'),json_extract(value,'$.destination'),value FROM graph,json_each(body,'$.interactions')", NULL) ||
        !statement(db, "INSERT INTO workflows SELECT json_extract(value,'$.id'),value FROM graph,json_each(body,'$.workflows')", NULL)) goto done;
    if (sqlite3_prepare_v2(db,
        "SELECT (SELECT count(*) FROM owners)>0 AND NOT EXISTS(SELECT 1 FROM edges WHERE source NOT IN (SELECT id FROM owners) OR destination NOT IN (SELECT id FROM owners)) "
        "AND NOT EXISTS(SELECT 1 FROM owners,json_each(body,'$.experience_chains') c WHERE c.value NOT IN (SELECT id FROM workflows))",
        -1, &st, NULL) != SQLITE_OK || sqlite3_step(st) != SQLITE_ROW || sqlite3_column_int(st, 0) != 1) goto done;
    sqlite3_finalize(st); st = NULL;
    /* Bind the objective and limit. Expansion follows actual edges once in
     * either direction and retains each edge's original direction/contract. */
    const char *sql =
        "WITH ranked AS (SELECT id,body,CASE WHEN id=?1 THEN 100000 ELSE "
        "8*term_score(id)+3*term_score(json_extract(body,'$.semantic_question'))+term_score(json_extract(body,'$.domain')) END AS score FROM owners),"
        "anchors AS (SELECT * FROM ranked WHERE score>0 ORDER BY score DESC,id LIMIT ?2),"
        "selected_edges AS (SELECT * FROM edges WHERE source IN (SELECT id FROM anchors) OR destination IN (SELECT id FROM anchors)),"
        "selected_ids AS (SELECT id FROM anchors UNION SELECT source FROM selected_edges UNION SELECT destination FROM selected_edges),"
        "selected_owners AS (SELECT * FROM owners WHERE id IN (SELECT id FROM selected_ids) ORDER BY id),"
        "selected_workflows AS (SELECT * FROM workflows WHERE id IN (SELECT c.value FROM selected_owners,json_each(body,'$.experience_chains') c) ORDER BY id) "
        "SELECT (SELECT count(*) FROM anchors),json_object("
        "'schema','bonfyre.experience-resolution.v1','status',CASE WHEN EXISTS(SELECT 1 FROM anchors) THEN 'candidates' ELSE 'no_match' END,"
        "'objective',?1,'atlas_declaration_digest',json_extract((SELECT body FROM graph),'$.atlas_declaration_digest'),"
        "'matching_owner_count',(SELECT count(*) FROM ranked WHERE score>0),'anchor_limit',?2,"
        "'expansion','one_hop_undirected','ranking','lexical_terms_not_semantic_proof',"
        "'anchors',json((SELECT json_group_array(json_object('id',id,'score',score)) FROM anchors)),"
        "'owners',json((SELECT json_group_array(json(body)) FROM selected_owners)),"
        "'interactions',json((SELECT json_group_array(json(body)) FROM (SELECT body FROM selected_edges ORDER BY id))),"
        "'workflows',json((SELECT json_group_array(json(body)) FROM selected_workflows)),"
        "'execution_authorized',json('false'),'executable_chain_proven',json('false'),"
        "'unresolved',json('[\"current owner binary and callable contract\",\"actor and effect authority\",\"evidence references and source freshness at execution\",\"provider and resource admission\",\"economic commitment and paid entitlement\",\"complete ordered workflow and fact flow\"]'))";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK ||
        sqlite3_bind_text(st, 1, objective, -1, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_bind_int(st, 2, (int)limit) != SQLITE_OK || sqlite3_step(st) != SQLITE_ROW) goto done;
    rc = sqlite3_column_int(st, 0) ? 0 : 1;
    puts((const char *)sqlite3_column_text(st, 1));
done:
    if (rc == 2) fprintf(stderr, "experience-map: missing or invalid owner graph%s%s\n", db ? ": " : "", db ? sqlite3_errmsg(db) : "");
    sqlite3_finalize(st); sqlite3_close(db); if (fp) fclose(fp); free(data); return rc;
}
