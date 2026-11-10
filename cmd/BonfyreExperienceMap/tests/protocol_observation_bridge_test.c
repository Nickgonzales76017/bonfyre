#include "protocol_derivation_bridge.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

/* This is deliberately one real occurrence, not a generic “protocol mapped”
 * count. OGC API Common is the retained and DomainProfile-bound external
 * occurrence. The contract only compiles its read/projection relationship; it
 * does not enact an external request or claim conformance by OGC. */
int main(void)
{
    BfProtocolContract contract;
    BfSemanticDerivationGraph *graph = malloc(sizeof(*graph));
    const char *cataloguer_requires[] = {"domain_profile", "source_url"};
    const char *cataloguer_produces[] = {"carrier_description"};
    const char *compiler_requires[] = {"carrier_description"};
    const char *compiler_produces[] = {"projection_contract"};
    const char *reader_requires[] = {"projection_contract"};
    const char *reader_produces[] = {"semantic_projection"};
    BfProtocolRoleSpec cataloguer = {
        .name = "cataloguer", .requires = cataloguer_requires,
        .requires_count = 2, .produces = cataloguer_produces, .produces_count = 1,
    };
    BfProtocolRoleSpec compiler = {
        .name = "compiler", .requires = compiler_requires,
        .requires_count = 1, .produces = compiler_produces, .produces_count = 1,
    };
    BfProtocolRoleSpec reader = {
        .name = "reader", .requires = reader_requires,
        .requires_count = 1, .produces = reader_produces, .produces_count = 1,
    };
    size_t added = 0;

    assert(graph != NULL);
    bf_protocol_contract_init(&contract, "ogc-api-common-observation");
    assert(bf_protocol_contract_given(&contract, "domain_profile") == BF_PROTOCOL_OK);
    assert(bf_protocol_contract_given(&contract, "source_url") == BF_PROTOCOL_OK);
    assert(bf_protocol_contract_role(&contract, &cataloguer) == BF_PROTOCOL_OK);
    assert(bf_protocol_contract_role(&contract, &compiler) == BF_PROTOCOL_OK);
    assert(bf_protocol_contract_role(&contract, &reader) == BF_PROTOCOL_OK);
    bf_semantic_derivation_init(graph);
    assert(bf_protocol_derivation_register(
        &contract, "external:protocol-0304", graph, &added) ==
        BF_PROTOCOL_DERIVATION_BRIDGE_OK);
    assert(added == 3);
    assert(graph->program_count == 3);
    printf("{\"schema\":\"bonfyre-protocol-observation-bridge.v1\","
           "\"registered\":true,\"occurrence_id\":\"external:protocol-0304\","
           "\"contract\":\"ogc-api-common-observation\",\"programs\":3,"
           "\"scoped\":true,\"state\":\"compiled\","
           "\"cannot_infer\":[\"compiled contract -> OGC conformance\","
           "\"compiled contract -> enacted request\",\"compiled contract -> effect\"]}\n");
    free(graph);
    return 0;
}
