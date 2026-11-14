/* SERM reduction over the proof frontier.  See bf_serm.h. */
#include "bf_serm.h"
#include "bf_proof.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int present(const char *s) { return s && *s; }
static const char *nz(const char *s) { return s ? s : ""; }

static int cmpstr(const void *a, const void *b) { return strcmp((const char *)a, (const char *)b); }

int bf_serm_reduce(void *vdb, const char *res, const char *prof, const char *const *req, size_t nreq, BfSermResult *out) {
    sqlite3 *db = vdb; sqlite3_stmt *st; BfProof *p = NULL;
    if (!db || !present(res) || !out || nreq > BF_SERM_MAX) return -1;
    memset(out, 0, sizeof *out);
    if (bf_proof_open_database(db, &p, NULL) != 0) return -1; /* ensures the frontier schema */
    bf_proof_close(p);
    char backed[2 * BF_SERM_MAX][128]; size_t nb = 0;
    /* proven frontier layers */
    if (sqlite3_prepare_v2(db, "SELECT layer,witness_ref FROM frontier_layers WHERE subject_resource=?1 AND subject_profile=?2 AND status='proven'",
                           -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, res, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, nz(prof), -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *layer = (const char *)sqlite3_column_text(st, 0), *w = (const char *)sqlite3_column_text(st, 1);
        if (present(w)) { if (nb < 2 * BF_SERM_MAX) snprintf(backed[nb++], 128, "%s", layer); }
        else if (out->laundered_n < BF_SERM_MAX) snprintf(out->laundered[out->laundered_n++], 192, "frontier:%s:%s", layer, layer);
    }
    sqlite3_finalize(st);
    /* cooled measured/proven invariants */
    if (sqlite3_prepare_v2(db, "SELECT invariant_id,layer,json_array_length(COALESCE(proof_refs,'[]')) FROM solved_invariants "
                           "WHERE subject_resource=?1 AND status='cooled' AND truth_plane IN ('measured','proven')", -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, res, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(st, 0), *layer = (const char *)sqlite3_column_text(st, 1);
        if (sqlite3_column_int(st, 2) > 0) { if (nb < 2 * BF_SERM_MAX) snprintf(backed[nb++], 128, "%s", layer); }
        else if (out->laundered_n < BF_SERM_MAX) snprintf(out->laundered[out->laundered_n++], 192, "%s:%s", id, layer);
    }
    sqlite3_finalize(st);
    for (size_t i = 0; i < nreq; i++) {
        int hit = 0; for (size_t j = 0; j < nb; j++) if (!strcmp(backed[j], nz(req[i]))) hit = 1;
        if (hit) snprintf(out->covered[out->covered_n++], 128, "%s", req[i]);
        else snprintf(out->missing[out->missing_n++], 128, "%s", nz(req[i]));
    }
    qsort(out->covered, out->covered_n, 128, cmpstr); qsort(out->missing, out->missing_n, 128, cmpstr);
    qsort(out->laundered, out->laundered_n, 192, cmpstr);
    out->honest = out->laundered_n == 0; out->sufficient = out->missing_n == 0;
    if (out->sufficient && out->honest) snprintf(out->verdict, sizeof out->verdict, "sufficient: all required layers covered by witness-backed measurements");
    else if (!out->honest) snprintf(out->verdict, sizeof out->verdict, "dishonest: %zu claim(s) assert a plane with no witness", out->laundered_n);
    else {
        size_t w = (size_t)snprintf(out->verdict, sizeof out->verdict, "insufficient: %zu required layer(s) unproven: ", out->missing_n);
        for (size_t i = 0; i < out->missing_n && w < sizeof out->verdict; i++) w += (size_t)snprintf(out->verdict + w, sizeof out->verdict - w, "%s%s", i ? ", " : "", out->missing[i]);
    }
    return 0;
}

int bf_serm_record_sufficiency(void *vdb, const char *contract, const char *demand, const char *res, const char *prof, const BfSermResult *r) {
    sqlite3 *db = vdb; BfProof *p = NULL; sqlite3_stmt *st;
    if (!db || !present(contract) || !present(res) || !r) return -1;
    if (!(r->sufficient && r->honest)) return 0;
    char id[300], statement[1024]; size_t w;
    snprintf(id, sizeof id, "serm.sufficiency.%s", contract);
    w = (size_t)snprintf(statement, sizeof statement, "capability demand '%s' is sufficiently evidenced: ", nz(demand));
    for (size_t i = 0; i < r->covered_n && w < sizeof statement; i++) w += (size_t)snprintf(statement + w, sizeof statement - w, "%s%s", i ? ", " : "", r->covered[i]);
    if (w < sizeof statement) snprintf(statement + w, sizeof statement - w, " covered by witnesses");
    if (bf_proof_open_database(db, &p, NULL) != 0) return -1;
    int rc = bf_proof_record_invariant(p, id, res, nz(prof), "sufficiency", statement, "measured", "cooled", NULL);
    bf_proof_close(p);
    if (rc != 0) return -1;
    /* proof refs and reheat conditions, as the reference recorded them */
    if (sqlite3_prepare_v2(db, "UPDATE solved_invariants SET proof_refs=?2, reheat_conditions="
            "json_array('a required layer is reheated','the measurement contract changes') WHERE invariant_id=?1", -1, &st, NULL) != SQLITE_OK) return -1;
    char refs[4096]; w = (size_t)snprintf(refs, sizeof refs, "[");
    for (size_t i = 0; i < r->covered_n && w < sizeof refs; i++) w += (size_t)snprintf(refs + w, sizeof refs - w, "%s\"serm:covered:%s\"", i ? "," : "", r->covered[i]);
    if (w < sizeof refs) snprintf(refs + w, sizeof refs - w, "]");
    sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, refs, -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(st); sqlite3_finalize(st);
    return rc == SQLITE_DONE ? 1 : -1;
}
