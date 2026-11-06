#include "bf_reservation.h"
#include "bf_target_obligation.h"
#include "bf_authority.h"
#include "bf_evidence.h"
#include "bf_foreign_twin.h"
#include "bf_proof.h"
#include "inverse_programs.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "live_inverse_test:%d: %s\n", __LINE__, #x); return 1; } } while (0)

static int run(const char *binary, const char *db, const char *target) {
    char command[2048];
    snprintf(command, sizeof(command), "%s synthesize --db %s --target %s --actor actor:planner --at 2026-09-01T00:00:00Z", binary, db, target);
    return system(command) == 0;
}
static int admit(const char *binary, const char *db, const char *target) {
    char command[2048];
    snprintf(command, sizeof(command), "%s admit --db %s --target %s --predicate communication_capacity --transformation reserve_capacity --actor actor:planner --at 2026-09-01T00:00:00Z", binary, db, target);
    return system(command) == 0;
}
static int reconcile(const char *binary, const char *db, const char *target) {
    char command[2048];
    snprintf(command, sizeof(command), "%s reconcile --db %s --target %s --actor actor:planner --at 2026-09-01T00:00:00Z", binary, db, target);
    return system(command) == 0;
}
static int drain_inverse(const char *binary, const char *db, const char *directory) {
    char command[2048];
    snprintf(command, sizeof(command), "%s inverse-drain --db %s --lowerer-dir %s --actor actor:planner --at 2026-09-01T00:00:00Z", binary, db, directory);
    return system(command) == 0;
}
static int field(sqlite3 *db, const char *sql, const char *value) {
    sqlite3_stmt *s=NULL; int ok=0;
    if (sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(s, 1, value, -1, SQLITE_TRANSIENT);
    ok=sqlite3_step(s)==SQLITE_ROW; sqlite3_finalize(s); return ok;
}
static int scalar(sqlite3 *db, const char *sql) {
    sqlite3_stmt *s=NULL; int value=-1;
    if (sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK) return -1;
    if (sqlite3_step(s)==SQLITE_ROW) value=sqlite3_column_int(s,0);
    sqlite3_finalize(s); return value;
}

int main(int argc, char **argv) {
    char database[]="/tmp/bf_generic_inverse_XXXXXX", inbox[512], inverse_dir[640], handoff[768], handled[832]; int fd=mkstemp(database); sqlite3 *db=NULL;
    BfTargetObligation *target=NULL; BfReservation *reservation=NULL; BfAuthority *authority=NULL;
    BfForeignTwin *foreign=NULL; BfEvidence *evidence=NULL; BfProof *proof=NULL;
    BfTargetObligationSpec spec={.target_id="target:storm-closure-742",.target="world:regional-storm/communication-capacity",.semantic_contract="regional-storm-ready",.source_frontier="proof-frontier:storm-742",.preference_policy="resilient"};
    BfTargetPredicateSpec predicate={.predicate_id="communication_capacity",.owner="ResourceReservation",.subject="resource:storm-mesh-west",.predicate="capacity",.expected_state="reserved",.valid_start="2026-09-18T18:00:00Z",.valid_end="2026-09-19T02:00:00Z",.evidence_requirement="",.authority_requirement="reserve:storm-mesh",.adapter=BF_INVERSE_ADAPTER_RESERVATION,.native_state=BF_INVERSE_STATE_RESERVED,.owner_query=1};
    BfReservationSpec hold={.reservation_id="hold:storm-mesh-west",.resource_id="resource:storm-mesh-west",.holder="world:regional-storm",.purpose="emergency-connectivity",.starts_at="2026-09-18T18:00:00Z",.ends_at="2026-09-19T02:00:00Z",.authority_ref="native-test",.evidence_ref="native-test"};
    BfTargetObligationRecord record;
    BfTargetObligationSpec collision_spec={.target_id="target:storm-closure-743",.target="world:regional-storm/communication-capacity-east",.semantic_contract="regional-storm-ready",.source_frontier="proof-frontier:storm-743",.preference_policy="resilient"};
    BfTargetObligationSpec observed_spec={.target_id="target:roundtrip-closure-742",.target="world:regional-storm/communication-capacity-observed",.semantic_contract="regional-storm-roundtrip-ready",.source_frontier="proof:storm-roundtrip-742",.preference_policy="resilient"};
    BfTargetPredicateSpec observed_predicates[]={
        {.predicate_id="foreign_result",.owner="ForeignTwin",.subject="result:storm-mesh-west",.predicate="result",.expected_state="observed",.adapter=BF_INVERSE_ADAPTER_FOREIGN,.native_state=BF_INVERSE_STATE_OBSERVED_RESULT,.owner_query=1},
        {.predicate_id="independent_evidence",.owner="EvidenceGraph",.subject="claim:storm-mesh-west-capacity",.predicate="claim",.expected_state="valid",.adapter=BF_INVERSE_ADAPTER_EVIDENCE,.native_state=BF_INVERSE_STATE_STANDS,.owner_query=1},
        {.predicate_id="proof_frontier",.owner="ProofFrontier",.subject="proof:storm-roundtrip-742",.predicate="invariant",.expected_state="proven",.adapter=BF_INVERSE_ADAPTER_PROOF,.native_state=BF_INVERSE_STATE_PROVEN,.owner_query=1},
    };
    BfForeignTwinObservation observation={.receipt_id="foreign-receipt:storm-mesh-west-1",.subject="result:storm-mesh-west",.predicate="result",.source_url="https://independent.example/storm-mesh-west",.body_sha256="91aa",.observed_at="2026-09-01T00:01:00Z",.observer="foreign-twin:independent"};
    const char *binary=argc>1?argv[1]:"./bonfyre-semantic-optimizer";
    CHECK(fd>=0); close(fd); unlink(database);
    CHECK(bf_target_obligation_open(database,&target,stderr)==0);
    CHECK(bf_authority_open(database,&authority,stderr)==0);
    CHECK(bf_authority_grant(authority,&(BfAuthorityEdge){.edge_id="grant:storm-mesh",.actor="actor:planner",.permission="reserve:storm-mesh",.subject="resource:storm-mesh-west"},"2026-09-01T00:00:00Z")==0);
    CHECK(bf_target_obligation_require(target,&spec,&predicate,1)==0);
    /* AddressPlane writes this exact lowerer envelope after a target cut. The
     * inverse consumer must synthesize only the residual, leave the target
     * OPEN, and atomically retain the completed handoff for replay evidence. */
    snprintf(inbox,sizeof(inbox),"/tmp/bf-inverse-inbox-%ld",(long)getpid());
    snprintf(inverse_dir,sizeof(inverse_dir),"%s/InverseCompiler-closure_program",inbox);
    snprintf(handoff,sizeof(handoff),"%s/target.json",inverse_dir);
    snprintf(handled,sizeof(handled),"%s.handled",handoff);
    CHECK(mkdir(inbox,0700)==0); CHECK(mkdir(inverse_dir,0700)==0);
    {FILE *f=fopen(handoff,"wb");CHECK(f!=NULL);CHECK(fputs("{\"schema\":\"bonfyre-lowering-handoff.v1\",\"delta_id\":\"delta:storm-target\",\"identity\":\"target:target:storm-closure-742\",\"candidate_kind\":\"target_obligation\",\"owner_ref\":\"TargetObligation:target:storm-closure-742\",\"lowerer_ref\":\"InverseCompiler:closure_program\",\"truth\":\"dependency_cut_only\"}\n",f)>0);CHECK(fclose(f)==0);}
    CHECK(drain_inverse(binary,database,inbox));
    CHECK(access(handled,F_OK)==0);
    CHECK(sqlite3_open(database,&db)==SQLITE_OK);
    CHECK(field(db,"SELECT 1 FROM inverse_target_receipts WHERE target_id=? AND state='OPEN'",spec.target_id));
    CHECK(field(db,"SELECT 1 FROM inverse_cross_owner_residuals WHERE target_id=? AND protocol_contract='resource-reservation' AND effect_kind='reservation'",spec.target_id));
    sqlite3_close(db); db=NULL;
    /* IDs with equal byte length must not share a WorkGraph mission merely
     * because their old lowering used strlen(target). */
    CHECK(bf_target_obligation_require(target,&collision_spec,&predicate,1)==0);
    CHECK(run(binary,database,collision_spec.target_id));
    CHECK(sqlite3_open(database,&db)==SQLITE_OK);
    CHECK(scalar(db,"SELECT COUNT(DISTINCT mission_id) FROM workgraph_nodes WHERE input_uri IN ('target:storm-closure-742','target:storm-closure-743')")==2);
    sqlite3_close(db); db=NULL;
    CHECK(bf_target_obligation_get(target,spec.target_id,&record)==0);
    CHECK(record.state==BF_TARGET_OPEN); /* a compiler cannot manufacture proof */
    CHECK(bf_reservation_open(database,&reservation,stderr)==0);
    CHECK(bf_reservation_hold(reservation,&hold,"2026-09-01T00:00:00Z")==1);
    bf_reservation_close(reservation); reservation=NULL;
    CHECK(run(binary,database,spec.target_id));
    CHECK(sqlite3_open(database,&db)==SQLITE_OK);
    CHECK(field(db,"SELECT 1 FROM inverse_target_receipts WHERE target_id=? AND state='FRONTIER_SATISFIED'",spec.target_id));
    sqlite3_close(db); db=NULL;
    CHECK(bf_target_obligation_get(target,spec.target_id,&record)==0);
    CHECK(record.state==BF_TARGET_OPEN);
    /* A green synthesis frontier is not proof, and a target without an
     * explicitly declared ProofFrontier predicate cannot close by reconcile. */
    CHECK(reconcile(binary,database,spec.target_id));
    CHECK(bf_target_obligation_get(target,spec.target_id,&record)==0);
    CHECK(record.state==BF_TARGET_OPEN);
    CHECK(admit(binary,database,spec.target_id));
    CHECK(sqlite3_open(database,&db)==SQLITE_OK);
    CHECK(field(db,"SELECT 1 FROM inverse_effect_admissions WHERE target_id=? AND state='ADMISSIBLE_APERTURE'",spec.target_id));
    sqlite3_close(db); db=NULL;
    CHECK(bf_authority_revoke(authority,"grant:storm-mesh")==1);
    CHECK(admit(binary,database,spec.target_id));
    CHECK(sqlite3_open(database,&db)==SQLITE_OK);
    CHECK(field(db,"SELECT 1 FROM inverse_effect_admissions WHERE target_id=? AND state='REFUSED' AND reason='effect_authority_missing_or_expired'",spec.target_id));
    sqlite3_close(db); db=NULL;
    /* This second arbitrary target closes the whole observed return seam:
     * ForeignTwin receipt -> directional Evidence -> ProofFrontier -> target.
     * The optimizer only reconciles owner state; it never writes PROVEN. */
    CHECK(bf_target_obligation_require(target,&observed_spec,observed_predicates,3)==0);
    CHECK(bf_foreign_twin_open(database,&foreign,stderr)==0);
    CHECK(bf_evidence_open(database,&evidence,stderr)==0);
    CHECK(bf_proof_open(database,&proof,stderr)==0);
    CHECK(bf_foreign_twin_record(foreign,&observation)==0);
    CHECK(reconcile(binary,database,observed_spec.target_id));
    CHECK(bf_target_obligation_get(target,observed_spec.target_id,&record)==0);
    CHECK(record.state==BF_TARGET_OPEN);
    CHECK(bf_proof_record_independent_invariant(proof,foreign,evidence,
        "proof:storm-roundtrip-742","result:storm-mesh-west","","roundtrip",
        "independent capacity observation","result:storm-mesh-west","result",
        "claim:storm-mesh-west-capacity","2026-09-01T00:02:00Z")==0);
    CHECK(bf_proof_invariant_is_proven(proof,"proof:storm-roundtrip-742")==0);
    CHECK(bf_evidence_relate(evidence,"foreign-receipt:storm-mesh-west-1","supports","claim:storm-mesh-west-capacity")==BF_EVIDENCE_OK);
    CHECK(bf_proof_record_independent_invariant(proof,foreign,evidence,
        "proof:storm-roundtrip-742","result:storm-mesh-west","","roundtrip",
        "independent capacity observation","result:storm-mesh-west","result",
        "claim:storm-mesh-west-capacity","2026-09-01T00:03:00Z")==0);
    CHECK(bf_proof_invariant_is_proven(proof,"proof:storm-roundtrip-742")==1);
    /* A stale plan cannot close through a challenged proof, even while the
     * ForeignTwin and EvidenceGraph rows still stand. */
    CHECK(bf_proof_challenge_invariant(proof,"proof:storm-roundtrip-742","2026-09-01T00:03:30Z")==1);
    CHECK(reconcile(binary,database,observed_spec.target_id));
    CHECK(bf_target_obligation_get(target,observed_spec.target_id,&record)==0);
    CHECK(record.state==BF_TARGET_OPEN);
    CHECK(bf_proof_record_independent_invariant(proof,foreign,evidence,
        "proof:storm-roundtrip-742","result:storm-mesh-west","","roundtrip",
        "independent capacity observation","result:storm-mesh-west","result",
        "claim:storm-mesh-west-capacity","2026-09-01T00:03:45Z")==0);
    CHECK(reconcile(binary,database,observed_spec.target_id));
    CHECK(sqlite3_open(database,&db)==SQLITE_OK);
    CHECK(field(db,"SELECT 1 FROM inverse_target_reconciliations WHERE target_id=? AND state='TARGET_PROVEN'",observed_spec.target_id));
    sqlite3_close(db); db=NULL;
    CHECK(bf_target_obligation_get(target,observed_spec.target_id,&record)==0);
    CHECK(record.state==BF_TARGET_PROVEN);
    bf_proof_close(proof); bf_evidence_close(evidence); bf_foreign_twin_close(foreign);
    bf_authority_close(authority); bf_target_obligation_close(target); unlink(handled); rmdir(inverse_dir); rmdir(inbox); unlink(database);
    puts("live_inverse_test: PASS"); return 0;
}
