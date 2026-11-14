/* The four evidence-estate relations beside evidence support: transformation
 * lineage, provenance paths, the eight time planes and the claim graph.  Each
 * case is the behaviour the archived Python reference asserted. */
#include "bf_evidence.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int failures = 0;
#define CHECK(c, m) do { if (!(c)) { fprintf(stderr, "FAIL: %s (%s:%d)\n", m, __FILE__, __LINE__); failures++; } } while (0)

typedef struct { char seen[16][64]; int n; } Seen;
static int collect(const char *a, const char *b, void *ctx) {
    Seen *s = ctx; if (s->n < 16) snprintf(s->seen[s->n++], 64, "%s|%s", a, b); return 0;
}
static int has(const Seen *s, const char *v) { for (int i = 0; i < s->n; i++) if (!strcmp(s->seen[i], v)) return 1; return 0; }

int main(void) {
    char path[] = "/tmp/bf-evidence-estate-XXXXXX";
    int fd = mkstemp(path); if (fd < 0) return 2; close(fd);
    BfEvidence *g = NULL;
    if (bf_evidence_open(path, &g, stderr) != 0) return 2;

    /* ArtifactGraph: transitive, each ancestor once, a cycle terminates. */
    CHECK(bf_evidence_record_transform(g, "raw.wav", "transcribe", "t.txt") == BF_EVIDENCE_OK, "transform");
    CHECK(bf_evidence_record_transform(g, "t.txt", "summarize", "brief.md") == BF_EVIDENCE_OK, "transform 2");
    CHECK(bf_evidence_record_transform(g, "brief.md", "loop", "t.txt") == BF_EVIDENCE_OK, "cycle edge");
    Seen s = {0};
    int64_t n = bf_evidence_lineage(g, "brief.md", collect, &s);
    CHECK(n == 2 && has(&s, "t.txt|") && has(&s, "raw.wav|"), "lineage is transitive and finite under a cycle");
    CHECK(bf_evidence_record_transform(g, "", "x", "y") == BF_EVIDENCE_INVALID, "empty source refused");

    /* ProvenanceGraph: path check rejects an untrusted node; provenance is not support. */
    bf_evidence_record_provenance(g, "brief.md", "observer", "fabric");
    bf_evidence_record_provenance(g, "brief.md", "transport", "mirror-7");
    const char *bad[] = {"mirror-7"}, *good[] = {"mirror-9"};
    CHECK(bf_evidence_path_admits(g, "brief.md", bad, 1) == 0, "untrusted node on path refuses");
    CHECK(bf_evidence_path_admits(g, "brief.md", good, 1) == 1, "clean path admits");
    CHECK(bf_evidence_supports(g, "brief.md", "claim:x") == 0, "a clean path is not support");
    memset(&s, 0, sizeof s);
    CHECK(bf_evidence_provenance_path(g, "brief.md", collect, &s) == 2 && !strcmp(s.seen[0], "observer|fabric"), "path ordered by role");

    /* TemporalEvidenceGraph: planes kept apart, undeclared refused, stale never deletes. */
    char v[64];
    CHECK(bf_evidence_record_time(g, "fact:1", "publication_time", "2026-10-01T00:00:00Z") == BF_EVIDENCE_OK, "plane");
    CHECK(bf_evidence_record_time(g, "fact:1", "expiration_time", "2026-11-01T00:00:00Z") == BF_EVIDENCE_OK, "second plane");
    CHECK(bf_evidence_record_time(g, "fact:1", "updated_at", "2026") == BF_EVIDENCE_INVALID_KIND, "undeclared plane refused");
    CHECK(bf_evidence_time_of(g, "fact:1", "publication_time", v, sizeof v) == 1 && !strcmp(v, "2026-10-01T00:00:00Z"),
          "a second plane did not overwrite the first");
    CHECK(bf_evidence_time_of(g, "fact:1", "event_time", v, sizeof v) == 0, "unset plane is unset, not collapsed");
    CHECK(bf_evidence_is_stale(g, "fact:1", "2026-11-08T00:00:00Z") == 1, "past expiration is stale");
    CHECK(bf_evidence_is_stale(g, "fact:1", "2026-10-15T00:00:00Z") == 0, "before expiration is not");
    CHECK(bf_evidence_time_of(g, "fact:1", "publication_time", v, sizeof v) == 1, "staleness keeps the row");
    CHECK(bf_evidence_is_stale(g, "fact:none", "2026-11-08T00:00:00Z") == 0, "no expiration is never stale");

    /* ClaimGraph: dependencies, counters, proof state default and update. */
    const char *deps[] = {"claim:a", "claim:b"}, *ctr[] = {"claim:not-x"};
    CHECK(bf_evidence_add_claim(g, "claim:x", deps, 2, ctr, 1, NULL) == BF_EVIDENCE_OK, "add claim");
    CHECK(bf_evidence_proof_state(g, "claim:x", v, sizeof v) == 1 && !strcmp(v, "open"), "default proof state open");
    memset(&s, 0, sizeof s);
    CHECK(bf_evidence_claim_dependencies(g, "claim:x", collect, &s) == 3 && has(&s, "counter|claim:not-x") &&
          has(&s, "depends_on|claim:a"), "dependencies and counters kept apart");
    CHECK(bf_evidence_add_claim(g, "claim:x", NULL, 0, NULL, 0, "proven") == BF_EVIDENCE_OK, "state update");
    CHECK(bf_evidence_proof_state(g, "claim:x", v, sizeof v) == 1 && !strcmp(v, "proven"), "state updated");
    CHECK(bf_evidence_proof_state(g, "claim:unknown", v, sizeof v) == 0, "unknown claim has no state");
    const char *empty[] = {""};
    CHECK(bf_evidence_add_claim(g, "claim:y", empty, 1, NULL, 0, NULL) == BF_EVIDENCE_INVALID, "empty dependency refused");
    CHECK(bf_evidence_proof_state(g, "claim:y", v, sizeof v) == 0, "a refused claim writes nothing");

    bf_evidence_close(g); unlink(path);
    if (failures) return 1;
    puts("evidence estate (lineage, provenance, time planes, claims): PASS");
    return 0;
}
