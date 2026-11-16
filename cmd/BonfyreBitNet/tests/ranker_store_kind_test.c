/* Witness: the ranker store kind builds a store from the registry and reads
 * it back. schemas/storage/stores declares store rank (kind ranker) over the
 * bitnet_rank_run fragment; the compiler emits its attach statement and the
 * frontier order query. This builds a rank store from the fragment, writes one
 * run with its candidates out of order, attaches it read-only the way
 * evaluate-retention does, and checks the generated query returns every key
 * at its rank and that the attached store refuses a write. */
#include "bf_store_durability.h"
#include "../generated/store_schema.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int main(void) {
    char path[] = "/tmp/bitnet-ranker-XXXXXX";
    int fd = mkstemp(path), failures = 0;
    sqlite3 *db = NULL, *mem = NULL; sqlite3_stmt *st = NULL;
    const char *keys[] = {"cand:a", "cand:b", "cand:c"};
    const int ranks[] = {2, 0, 1};
    if (fd < 0) return 2;
    close(fd);
    if (strcmp(BF_STORE_BITNET_RANK_KIND, "ranker")) { fputs("FAIL: store rank is not a ranker\n", stderr); return 1; }
    if (sqlite3_open(path, &db) != SQLITE_OK || sqlite3_exec(db, BF_STORE_BIT_NET_BITNET_RANK_RUN, NULL, NULL, NULL) != SQLITE_OK ||
        sqlite3_exec(db, "INSERT INTO bitnet_rank_run VALUES('run-1','rep','scope','digest','SHADOW',4,3,0)", NULL, NULL, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "INSERT INTO " BF_STORE_BITNET_RANK_RANKER_FRONTIER_TABLE " VALUES('run-1',?1,?2,'kind','owner','verifier','lowerer')", -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "FAIL: build: %s\n", sqlite3_errmsg(db)); return 1;
    }
    for (int i = 0; i < 3; i++) {
        sqlite3_bind_int(st, 1, ranks[i]); sqlite3_bind_text(st, 2, keys[i], -1, SQLITE_STATIC);
        if (sqlite3_step(st) != SQLITE_DONE) { fprintf(stderr, "FAIL: insert: %s\n", sqlite3_errmsg(db)); return 1; }
        sqlite3_reset(st);
    }
    sqlite3_finalize(st); st = NULL;
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) return 1;
    sqlite3_close(db);

    if (sqlite3_open_v2(":memory:", &mem, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL) != SQLITE_OK) return 2;
    if (!bf_store_attach(mem, path, BF_STORE_BITNET_RANK_ATTACH, BF_STORE_BITNET_RANK_ATTACH_READONLY)) {
        fprintf(stderr, "FAIL: attach: %s\n", sqlite3_errmsg(mem)); return 1;
    }
    if (sqlite3_prepare_v2(mem, BF_STORE_BITNET_RANK_RANKER_FRONTIER, -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "FAIL: frontier query: %s\n", sqlite3_errmsg(mem)); return 1;
    }
    sqlite3_bind_text(st, 1, "run-1", -1, SQLITE_STATIC);
    const char *order[] = {"cand:b", "cand:c", "cand:a"};
    for (int i = 0; i < 3; i++) {
        if (sqlite3_step(st) != SQLITE_ROW || sqlite3_column_int(st, 0) != i ||
            strcmp((const char *)sqlite3_column_text(st, 1), order[i])) {
            fprintf(stderr, "FAIL: rank %d is not %s\n", i, order[i]); failures++; break;
        }
    }
    if (sqlite3_step(st) != SQLITE_DONE) { fputs("FAIL: frontier holds more than the run\n", stderr); failures++; }
    sqlite3_finalize(st);
    if (sqlite3_exec(mem, "DELETE FROM rank_source.ranked_candidate_envelope", NULL, NULL, NULL) == SQLITE_OK) {
        fputs("FAIL: the attached rank store accepted a write\n", stderr); failures++;
    }
    sqlite3_close(mem);
    unlink(path);
    if (failures) return 1;
    puts("ranker store kind: rank store built from the registry; frontier read back in rank order through the declared read-only attach");
    return 0;
}
