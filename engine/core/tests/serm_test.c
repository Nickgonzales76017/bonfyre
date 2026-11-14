/* SERM: sufficiency needs witness-backed coverage; a witness-less claim is laundering. */
#include "bf_serm.h"
#include "bf_proof.h"

#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;
#define CHECK(c, m) do { if (!(c)) { fprintf(stderr, "FAIL: %s (line %d)\n", m, __LINE__); failures++; } } while (0)

int main(void) {
    sqlite3 *db; sqlite3_open(":memory:", &db);
    BfProof *p; bf_proof_open_database(db, &p, NULL);
    bf_proof_record_layer(p, "model:fpq", "", 1, "decode", "proven", "witness:decode.tsv");
    bf_proof_record_layer(p, "model:fpq", "", 2, "quality", "untested", "");
    bf_proof_record_invariant(p, "inv:ppl", "model:fpq", "", "perplexity", "ppl within 2%", "measured", "cooled", NULL);
    sqlite3_exec(db, "UPDATE solved_invariants SET proof_refs='[\"run:ppl-1\"]' WHERE invariant_id='inv:ppl'", 0, 0, 0);
    bf_proof_close(p);
    BfSermResult r; const char *need[] = {"decode", "perplexity"}, *need2[] = {"decode", "quality"};
    CHECK(bf_serm_reduce(db, "model:fpq", "", need, 2, &r) == 0 && r.sufficient && r.honest, "witness-backed coverage is sufficient and honest");
    CHECK(!strncmp(r.verdict, "sufficient", 10), "verdict says sufficient");
    CHECK(bf_serm_record_sufficiency(db, "c1", "compress", "model:fpq", "", &r) == 1, "sufficiency earned and cooled");
    sqlite3_stmt *q; char refs[128] = "", plane[16] = "";
    sqlite3_prepare_v2(db, "SELECT proof_refs,truth_plane FROM solved_invariants WHERE invariant_id='serm.sufficiency.c1'", -1, &q, NULL);
    if (sqlite3_step(q) == SQLITE_ROW) { snprintf(refs, sizeof refs, "%s", sqlite3_column_text(q, 0)); snprintf(plane, sizeof plane, "%s", sqlite3_column_text(q, 1)); }
    sqlite3_finalize(q);
    CHECK(strstr(refs, "serm:covered:decode") && !strcmp(plane, "measured"), "the invariant cites coverage and is only measured");
    CHECK(bf_serm_reduce(db, "model:fpq", "", need2, 2, &r) == 0 && !r.sufficient && r.missing_n == 1 && !strcmp(r.missing[0], "quality"), "untested layer is missing");
    CHECK(bf_serm_record_sufficiency(db, "c2", "x", "model:fpq", "", &r) == 0, "insufficient earns nothing");
    /* laundering: a measured claim with no witness */
    bf_proof_open_database(db, &p, NULL);
    bf_proof_record_invariant(p, "inv:bare", "model:fpq", "", "decode", "fast", "proven", "cooled", NULL);
    bf_proof_close(p);
    CHECK(bf_serm_reduce(db, "model:fpq", "", need, 2, &r) == 0 && !r.honest && r.laundered_n >= 1, "a witness-less plane is laundering");
    CHECK(!strncmp(r.verdict, "dishonest", 9), "verdict says dishonest");
    CHECK(bf_serm_record_sufficiency(db, "c3", "x", "model:fpq", "", &r) == 0, "dishonest earns nothing even when covered");
    sqlite3_close(db);
    if (failures) return 1;
    puts("serm sufficiency and honesty reduction: PASS");
    return 0;
}
