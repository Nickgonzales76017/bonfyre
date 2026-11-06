/* A reference opens only where the estate answers for it: lookalike and
 * smuggled hosts are not the estate's, an over-long address is refused rather
 * than cut, parent-domain fallback stops above a public suffix, and two hosts
 * that share a slug do not share a publisher unless its subject reads that
 * very root. */
#ifndef _DARWIN_C_SOURCE
#define _DARWIN_C_SOURCE
#endif
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <sqlite3.h>
#include "bf_sqlite_path.h"
#include "../src/estate_reference.h"

static int failures;
static void check(int ok, const char *what) { if (!ok) { fprintf(stderr, "estate reference: %s\n", what); failures++; } }

static void exec(const char *path, const char *sql) {
    sqlite3 *db; char *err = NULL;
    if (sqlite3_open(path, &db) != SQLITE_OK || sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) { fprintf(stderr, "fixture: %s\n", err ? err : "open"); exit(1); }
    sqlite3_close(db);
}

int main(void) {
    char dir[] = "/tmp/estate-reference.XXXXXX", frontier[256], matrix[256], fabric[256];
    if (!mkdtemp(dir)) return 1;
    snprintf(frontier, sizeof frontier, "%s/frontier.db", dir);
    snprintf(matrix, sizeof matrix, "%s/matrix.db", dir);
    snprintf(fabric, sizeof fabric, "%s/fabric.db", dir);
    exec(frontier,
        "CREATE TABLE root(root_id TEXT PRIMARY KEY, host TEXT, name TEXT, identity TEXT);"
        "CREATE TABLE channel(channel_id TEXT, root_id TEXT, title TEXT, state TEXT, identity TEXT, census_source TEXT);"
        "INSERT INTO root VALUES('r-sib','sib.swiss','SIB','Q1'),('r-other','sib-swiss.example','Other','Q2'),('r-uk','gov.co.uk','','');");
    exec(matrix,
        "CREATE TABLE matrix_domains(hostname TEXT, institution_id TEXT, default_scope TEXT, canonical_hostname TEXT);"
        "INSERT INTO matrix_domains VALUES('sib-swiss.aurekai.life','inst:sib-swiss','public','');");
    exec(fabric,
        "CREATE TABLE subject_reader(subject TEXT, scope TEXT, census_source TEXT, root_id TEXT);"
        "INSERT INTO subject_reader VALUES('institution:publisher:sib-swiss','public','feed-sib','r-sib');");
    setenv("BONFYRE_SOURCE_FRONTIER_DB", frontier, 1);
    setenv("BONFYRE_MATRIX_DB", matrix, 1);
    setenv("BONFYRE_SUBJECT_FABRIC_DB", fabric, 1);

    check(estate_address_is_ours("https://sib-swiss.aurekai.life/"), "an estate host is the estate's");
    check(estate_address_is_ours("/about/Q1"), "a same-origin path is the estate's");
    check(!estate_address_is_ours("https://evil.example\\.aurekai.life/"), "a backslash-smuggled host is not the estate's");
    check(!estate_address_is_ours("https://evil.example@x.aurekai.life/"), "userinfo does not make a host the estate's");
    check(!estate_address_is_ours("https://notaurekai.life/"), "a lookalike apex is not the estate's");
    check(!estate_address_is_ours("https://aurekai.life.evil.example/"), "an estate name inside another host is not the estate's");
    check(!estate_address_is_ours("//aurekai.life/"), "a scheme-relative address is not a same-origin path");

    EstateReferenceStores stores = {0}; EstateReference r;
    estate_reference_open(&stores);
    check(estate_reference_resolve(&stores, "https://www.sib.swiss/news/x", &r) && !strcmp(r.url, "https://sib-swiss.aurekai.life/"), "the publisher's own link opens its subject host");
    check(estate_reference_resolve(&stores, "https://sib-swiss.example/page", &r) == 0 || strcmp(r.url, "https://sib-swiss.aurekai.life/"), "a host sharing the slug does not open another publisher's address");
    {
        char long_ref[700]; memset(long_ref, 'a', sizeof long_ref - 1); long_ref[sizeof long_ref - 1] = '\0'; memcpy(long_ref, "https://sib.swiss/", 18);
        check(!estate_reference_resolve(&stores, long_ref, &r), "an address longer than a reference holds is refused, not cut");
    }
    check(!estate_reference_resolve(&stores, "https://deep.gov.co.uk/x", &r) || strcmp(r.host, "co.uk"), "fallback never reaches a public suffix");
    estate_reference_resolve(&stores, "https://deep.gov.co.uk/x", &r);
    check(!strcmp(r.host, "gov.co.uk") && !r.estate, "fallback finds the registrable name and, holding no address for it, opens nothing");
    estate_reference_close(&stores);
    char cmd[300]; snprintf(cmd, sizeof cmd, "rm -rf %s", dir); if (system(cmd)) {}
    if (failures) return 1;
    puts("estate reference: PASS");
    return 0;
}
