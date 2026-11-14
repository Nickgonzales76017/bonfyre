/* FormGraph compile and AtomicForm readiness: the cases the archived Python
 * reference asserted (conditional requiredness, verified-only autofill, exact
 * blockers, never a false green). */
#include "bf_form.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;
#define CHECK(c, m) do { if (!(c)) { fprintf(stderr, "FAIL: %s (line %d)\n", m, __LINE__); failures++; } } while (0)

static bf_jv *J(const char *t) { bf_jv *v = bf_jv_parse(t, strlen(t), NULL, 0); if (!v) { fprintf(stderr, "bad json: %s\n", t); exit(2); } return v; }
static int ready(const bf_jv *r) { char *d = bf_jv_dump(bf_jv_get(r, "ready"), 0, 0, NULL); int t = d && !strcmp(d, "true"); free(d); return t; }
static size_t nblock(const bf_jv *r) { size_t n = 0; while (bf_jv_at(bf_jv_get(r, "blockers"), n)) n++; return n; }
static int blocks(const bf_jv *r, const char *want) {
    for (size_t i = 0; bf_jv_at(bf_jv_get(r, "blockers"), i); i++) if (strstr(bf_jv_s(bf_jv_at(bf_jv_get(r, "blockers"), i)), want)) return 1;
    return 0;
}

int main(void) {
    bf_jv *fg = J("{\"form_id\":\"grant\",\"title\":\"Equipment grant\",\"fields\":["
        "{\"name\":\"applicant\",\"autofill_actor\":\"actor:org\"},"
        "{\"name\":\"requests_equipment\"},"
        "{\"name\":\"equipment_justification\",\"required_when\":{\"field\":\"requests_equipment\",\"equals\":\"yes\"}},"
        "{\"name\":\"notes\",\"base_required\":false}],"
        "\"evidence\":[{\"name\":\"budget\",\"match_kind\":\"spreadsheet\"},{\"name\":\"letters\",\"required\":false}],"
        "\"authority_required\":[\"signatory\"],\"review_gate\":\"board\"}");

    /* conditional requiredness */
    bf_jv *no = J("{\"requests_equipment\":\"no\"}"), *yes = J("{\"requests_equipment\":\"yes\"}");
    bf_jv *a = bf_form_compile(fg, no, NULL), *r = bf_form_submit_ready(a);
    CHECK(!blocks(r, "equipment_justification"), "conditional field not required when its condition is false");
    CHECK(blocks(r, "'applicant' is empty (autofill actor actor:org not verified)"), "empty autofill field names its actor");
    CHECK(blocks(r, "evidence slot 'budget'") && !blocks(r, "'letters'"), "required evidence blocks, optional does not");
    CHECK(blocks(r, "authority 'signatory'") && blocks(r, "review gate 'board'"), "authority and review gate block");
    CHECK(!blocks(r, "'notes'"), "base_required false never blocks");
    CHECK(!ready(r) && nblock(r) == 4, "exactly four blockers, not ready");
    bf_jv_free(r); bf_jv_free(a);
    a = bf_form_compile(fg, yes, NULL); r = bf_form_submit_ready(a);
    CHECK(blocks(r, "'equipment_justification' is empty"), "conditional field required when its condition holds");
    bf_jv_free(r); bf_jv_free(a);

    /* verified-only autofill */
    sqlite3 *db; sqlite3_open(":memory:", &db);
    sqlite3_exec(db, "CREATE TABLE actor_nodes(actor_id TEXT PRIMARY KEY, display_name TEXT, confidence TEXT);"
                     "INSERT INTO actor_nodes VALUES('actor:org','Deep Ellum Arts Co','verified'),('actor:guess','Maybe Inc','asserted');", 0, 0, 0);
    a = bf_form_compile(fg, no, NULL);
    CHECK(bf_form_autofill(a, db) == 1, "verified actor fills");
    CHECK(!strcmp(bf_jv_s(bf_jv_get(bf_jv_at(bf_jv_get(a, "fields"), 0), "value")), "Deep Ellum Arts Co"), "filled with display name");
    bf_jv_free(a);
    bf_jv *fg2 = J("{\"form_id\":\"g2\",\"fields\":[{\"name\":\"applicant\",\"autofill_actor\":\"actor:guess\"}]}");
    a = bf_form_compile(fg2, NULL, NULL);
    CHECK(bf_form_autofill(a, db) == 0, "asserted actor never fills");
    bf_jv_free(a); bf_jv_free(fg2);

    /* everything satisfied -> ready; nothing ready without it */
    bf_jv *vals = J("{\"requests_equipment\":\"no\"}");
    bf_jv *st = J("{\"authorities_present\":[\"signatory\"],\"review_passed\":true,\"evidence\":{\"budget\":{\"artifact_digest\":\"sha256:ab\"}}}");
    a = bf_form_compile(fg, vals, st); bf_form_autofill(a, db); r = bf_form_submit_ready(a);
    CHECK(ready(r) && nblock(r) == 0, "all satisfied is ready");
    bf_jv_free(r); bf_jv_free(a);

    /* evidence binding: semantic, never positional; unique; most constrained first */
    bf_jv *bg = J("{\"form_id\":\"b\",\"fields\":[],\"evidence\":["
        "{\"name\":\"loose\",\"match_kind\":\"pdf\"},"
        "{\"name\":\"budget\",\"match_kind\":\"pdf\",\"match_name\":[\"Budget\"],\"match_tags\":[\"fy26\"]},"
        "{\"name\":\"nomatcher\"},"
        "{\"name\":\"photo\",\"match_kind\":\"image\"}]}");
    bf_jv *cands = J("[{\"digest\":\"sha256:aa\",\"kind\":\"pdf\",\"name\":\"letter.pdf\"},"
        "{\"digest\":\"sha256:bb\",\"kind\":\"pdf\",\"name\":\"fy26-budget.pdf\",\"tags\":[\"fy26\"]},"
        "{\"digest\":\"sha256:cc\",\"kind\":\"sheet\",\"name\":\"budget.xlsx\"}]");
    a = bf_form_compile(bg, NULL, NULL);
    bf_jv *res = bf_form_bind_evidence(a, cands);
    CHECK(!strcmp(bf_jv_s(bf_jv_path(res, "bound.budget.digest")), "sha256:bb"), "most constrained slot takes its exact match");
    CHECK(!strcmp(bf_jv_s(bf_jv_path(res, "bound.loose.digest")), "sha256:aa"), "loose slot is not starved and gets what is left");
    CHECK(strstr(bf_jv_s(bf_jv_path(res, "bound.budget.witness")), "tag=fy26") != NULL, "binding carries its witness");
    CHECK(!bf_jv_get(bf_jv_get(res, "bound"), "nomatcher"), "a slot with no matcher never binds by position");
    CHECK(!bf_jv_get(bf_jv_get(res, "bound"), "photo"), "no candidate of the kind, no binding");
    r = bf_form_submit_ready(a);
    CHECK(blocks(r, "'photo'") && blocks(r, "'nomatcher'") && !blocks(r, "'budget'"), "bound slots stop blocking, unbound still block");
    bf_jv_free(r); bf_jv_free(res); bf_jv_free(a); bf_jv_free(cands); bf_jv_free(bg);

    /* malformed graph refused */
    bf_jv *bad = J("{\"title\":\"no id\"}"), *bad2 = J("{\"form_id\":\"x\",\"fields\":[{\"kind\":\"text\"}]}");
    CHECK(bf_form_compile(bad, NULL, NULL) == NULL && bf_form_compile(bad2, NULL, NULL) == NULL, "malformed graph refused");

    sqlite3_close(db); bf_jv_free(bad); bf_jv_free(bad2); bf_jv_free(vals); bf_jv_free(st); bf_jv_free(no); bf_jv_free(yes); bf_jv_free(fg);
    if (failures) return 1;
    puts("form graph compile and atomic form readiness: PASS");
    return 0;
}
