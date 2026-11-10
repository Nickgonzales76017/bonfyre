#include "bf_protocol_contract.h"

#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

#define CHECK(condition) do { if (!(condition)) { fprintf(stderr, "protocol_contract_test:%d: %s\n", __LINE__, #condition); return 1; } } while (0)

static BfProtocolRoleSpec role(const char *name, const char *authority, const char *const *requires, size_t requires_count, const char *const *produces, size_t produces_count) { BfProtocolRoleSpec spec = {0}; spec.name = name; spec.authority = authority; spec.requires = requires; spec.requires_count = requires_count; spec.produces = produces; spec.produces_count = produces_count; return spec; }

int main(void) {
    BfProtocolContract contract, leaky, racy; BfProtocolRoleSpec spec; BfProtocolCommitmentSpec commitment; BfProtocolConformance conformance; BfProtocolWaves waves; BfProtocolEnactment enactment, outstanding; BfProtocolClosure closure; BfProtocolAuthorityRefusal refusal; BfInformationPolicy policy; BfAuthority *authority = NULL; BfEvidence *evidence = NULL; sqlite3 *db = NULL;
    const char *buyer_requires[] = {"capability", "buyer_identity"}; const char *buyer_produces[] = {"request"}; const char *seller_requires[] = {"request"}; const char *seller_produces[] = {"quote"}; const char *settle_requires[] = {"quote"}; const char *settle_produces[] = {"acceptance"}; const char *deliver_requires[] = {"acceptance"}; const char *deliver_produces[] = {"delivery"};
    BfProtocolActor actors[] = {{"buyer", "agent:acme"}, {"seller", "aurekai.life"}, {"settlement", "aurekai.life"}, {"deliverer", "aurekai.life"}};
    bf_protocol_contract_init(&contract, "quote-accept");
    CHECK(bf_protocol_contract_given(&contract, "capability") == BF_PROTOCOL_OK && bf_protocol_contract_given(&contract, "buyer_identity") == BF_PROTOCOL_OK && bf_protocol_contract_key(&contract, "exchange_id") == BF_PROTOCOL_OK);
    spec = role("buyer", "request:capability", buyer_requires, 2, buyer_produces, 1); CHECK(bf_protocol_contract_role(&contract, &spec) == BF_PROTOCOL_OK);
    spec = role("seller", "quote:capability", seller_requires, 1, seller_produces, 1); CHECK(bf_protocol_contract_role(&contract, &spec) == BF_PROTOCOL_OK);
    spec = role("settlement", "accept:quote", settle_requires, 1, settle_produces, 1); CHECK(bf_protocol_contract_role(&contract, &spec) == BF_PROTOCOL_OK);
    spec = role("deliverer", "deliver:capability", deliver_requires, 1, deliver_produces, 1); CHECK(bf_protocol_contract_role(&contract, &spec) == BF_PROTOCOL_OK);
    commitment = (BfProtocolCommitmentSpec){"delivery", "deliverer", "buyer", "the quoted capability", "delivery"}; CHECK(bf_protocol_contract_commit(&contract, &commitment) == BF_PROTOCOL_OK);
    commitment = (BfProtocolCommitmentSpec){"payment", "buyer", "seller", "the quoted amount", "acceptance"}; CHECK(bf_protocol_contract_commit(&contract, &commitment) == BF_PROTOCOL_OK);
    CHECK(bf_protocol_contract_evidence_required(&contract, "claim:quoted-capability") == BF_PROTOCOL_OK);
    CHECK(bf_protocol_contract_conformance(&contract, NULL, BF_AUDIENCE_PUBLIC, &conformance) == BF_PROTOCOL_OK && !conformance.liveness_roles && !conformance.safety_parameters && !conformance.integrity_roles);
    CHECK(bf_protocol_contract_waves(&contract, &waves) == BF_PROTOCOL_OK && waves.wave_count == 4 && waves.counts[0] == 1 && !strcmp(contract.roles[waves.role_indices[0][0]].name, "buyer"));
    CHECK(sqlite3_open(":memory:", &db) == SQLITE_OK);
    CHECK(bf_protocol_contract_authorize(&contract, db, actors, 4, "subject:quote", "2026-09-06T00:00:00Z", NULL, &refusal) == BF_PROTOCOL_NONCONFORMANT && !strcmp(refusal.role, "buyer"));
    CHECK(bf_protocol_contract_outstanding(&contract, db, "ex1", &outstanding) == BF_PROTOCOL_OK && outstanding.opened_count == 0);
    CHECK(bf_authority_open_database(db, &authority, stderr) == 0);
    for (size_t index = 0; index < contract.role_count; index++) { BfAuthorityEdge grant = {0}; grant.edge_id = contract.roles[index].name; grant.actor = actors[index].actor; grant.permission = contract.roles[index].authority; grant.subject = "subject:quote"; CHECK(bf_authority_grant(authority, &grant, NULL) == 0); }
    bf_authority_close(authority);
    CHECK(bf_protocol_contract_enact_authorized(&contract, db, db, "ex1", actors, 4, "subject:quote", "2026-09-06T00:00:00Z", NULL, "2026-09-06T00:00:00Z", &enactment) == BF_PROTOCOL_OK && enactment.opened_count == 2);
    CHECK(bf_protocol_contract_outstanding(&contract, db, "ex1", &outstanding) == BF_PROTOCOL_OK && outstanding.opened_count == 2);
    CHECK(!strcmp(outstanding.commitment_ids[0], "ex1:delivery") && !strcmp(outstanding.commitment_ids[1], "ex1:payment"));
    CHECK(bf_protocol_contract_close(&contract, db, db, "ex1", &closure) == BF_PROTOCOL_OK && !closure.obligations_terminal && !closure.evidence_closed && closure.unevidenced_count == 1 && closure.evidence_verdicts[0] == BF_EVIDENCE_UNEVIDENCED);
    CHECK(bf_evidence_open_database(db, &evidence, stderr) == 0);
    CHECK(bf_evidence_relate(evidence, "receipt:quote", "supports", "claim:quoted-capability") == BF_EVIDENCE_OK);
    CHECK(bf_evidence_relate(evidence, "receipt:appeal", "contradicts", "claim:quoted-capability") == BF_EVIDENCE_OK);
    bf_evidence_close(evidence); evidence = NULL;
    CHECK(bf_protocol_contract_close(&contract, db, db, "ex1", &closure) == BF_PROTOCOL_OK && !closure.evidence_closed && closure.evidence_verdicts[0] == BF_EVIDENCE_CONTESTED);
    /* Disclosure integrity and producer safety are separate compiler failures,
     * neither of which a well-formed carrier message would reveal. */
    bf_information_policy_init(&policy);
    CHECK(bf_information_policy_disclose(&policy, "account", bf_information_policy_audience_mask(BF_AUDIENCE_PUBLIC), "") == BF_INFORMATION_POLICY_OK);
    CHECK(bf_information_policy_disclose(&policy, "owner_mailing_address", bf_information_policy_audience_mask(BF_AUDIENCE_INSTITUTION), "restricted") == BF_INFORMATION_POLICY_OK);
    bf_protocol_contract_init(&leaky, "leaky");
    CHECK(bf_protocol_contract_given(&leaky, "account") == BF_PROTOCOL_OK && bf_protocol_contract_given(&leaky, "owner_mailing_address") == BF_PROTOCOL_OK);
    { const char *requires[] = {"account", "owner_mailing_address"}; const char *produces[] = {"score"}; spec = role("scorer", "", requires, 2, produces, 1); spec.reads = requires; spec.reads_count = 2; CHECK(bf_protocol_contract_role(&leaky, &spec) == BF_PROTOCOL_OK); }
    CHECK(bf_protocol_contract_conformance(&leaky, &policy, BF_AUDIENCE_PUBLIC, &conformance) == BF_PROTOCOL_OK && conformance.integrity_roles == 1);
    bf_protocol_contract_init(&racy, "racy");
    CHECK(bf_protocol_contract_given(&racy, "request") == BF_PROTOCOL_OK);
    { const char *requires[] = {"request"}; const char *produces[] = {"price"}; spec = role("seller", "", requires, 1, produces, 1); CHECK(bf_protocol_contract_role(&racy, &spec) == BF_PROTOCOL_OK); spec = role("repricer", "", requires, 1, produces, 1); CHECK(bf_protocol_contract_role(&racy, &spec) == BF_PROTOCOL_OK); }
    CHECK(bf_protocol_contract_conformance(&racy, NULL, BF_AUDIENCE_PUBLIC, &conformance) == BF_PROTOCOL_OK && conformance.safety_parameters != 0);
    sqlite3_close(db); puts("protocol_contract_test: PASS"); return 0;
}
