#include "protocol_derivation_bridge.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static BfProtocolContract quote_contract(void)
{
    BfProtocolContract contract;
    const char *buyer_requires[] = {"capability", "buyer_identity"};
    const char *buyer_produces[] = {"request"};
    const char *seller_requires[] = {"request"};
    const char *seller_produces[] = {"quote"};
    const char *settlement_requires[] = {"quote"};
    const char *settlement_produces[] = {"acceptance"};
    const char *deliverer_requires[] = {"acceptance"};
    const char *deliverer_produces[] = {"delivery"};
    BfProtocolRoleSpec buyer = {
        .name = "buyer",
        .requires = buyer_requires,
        .requires_count = 2,
        .produces = buyer_produces,
        .produces_count = 1,
    };
    BfProtocolRoleSpec seller = {
        .name = "seller",
        .requires = seller_requires,
        .requires_count = 1,
        .produces = seller_produces,
        .produces_count = 1,
    };
    BfProtocolRoleSpec settlement = {
        .name = "settlement",
        .requires = settlement_requires,
        .requires_count = 1,
        .produces = settlement_produces,
        .produces_count = 1,
    };
    BfProtocolRoleSpec deliverer = {
        .name = "deliverer",
        .requires = deliverer_requires,
        .requires_count = 1,
        .produces = deliverer_produces,
        .produces_count = 1,
    };

    bf_protocol_contract_init(&contract, "quote-accept-fixture");
    assert(bf_protocol_contract_given(&contract, "capability") == BF_PROTOCOL_OK);
    assert(bf_protocol_contract_given(&contract, "buyer_identity") == BF_PROTOCOL_OK);
    assert(bf_protocol_contract_role(&contract, &buyer) == BF_PROTOCOL_OK);
    assert(bf_protocol_contract_role(&contract, &seller) == BF_PROTOCOL_OK);
    assert(bf_protocol_contract_role(&contract, &settlement) == BF_PROTOCOL_OK);
    assert(bf_protocol_contract_role(&contract, &deliverer) == BF_PROTOCOL_OK);
    return contract;
}

static int affected_contains(const BfSemanticSlice *slice, const char *program_id)
{
    for (size_t index = 0; index < slice->affected_count; index++) {
        if (strcmp(slice->affected_programs[index], program_id) == 0) return 1;
    }
    return 0;
}

static BfSemanticSlice slice_one(
    const BfSemanticDerivationGraph *graph,
    const char *delta_id,
    const char *address)
{
    const char *writes[] = {address};
    BfFactDelta delta = {
        .delta_id = delta_id,
        .writes = writes,
        .write_count = 1,
        .causal_frontier = "bridge-test",
        .hlc = {.physical_ms = 1, .logical = 0, .node = "test"},
    };
    BfSemanticSlice slice;
    assert(bf_semantic_derivation_slice(graph, &delta, &slice) == BF_DERIVATION_OK);
    return slice;
}

int main(void)
{
    BfProtocolContract contract = quote_contract();
    BfSemanticDerivationGraph *graph = malloc(sizeof(*graph));
    BfSemanticSlice slice;
    size_t added = 0;
    size_t before;

    assert(graph != NULL);
    bf_semantic_derivation_init(graph);

    assert(bf_protocol_derivation_register(
        &contract, "ex1", graph, &added) == BF_PROTOCOL_DERIVATION_BRIDGE_OK);
    assert(added == 4);
    assert(graph->program_count == 4);

    added = 0;
    assert(bf_protocol_derivation_register(
        &contract, "ex2", graph, &added) == BF_PROTOCOL_DERIVATION_BRIDGE_OK);
    assert(added == 4);
    assert(graph->program_count == 8);

    slice = slice_one(graph, "d-head", "ex1/capability");
    assert(slice.affected_count == 4);
    assert(affected_contains(&slice, "ex1:buyer"));
    assert(affected_contains(&slice, "ex1:seller"));
    assert(affected_contains(&slice, "ex1:settlement"));
    assert(affected_contains(&slice, "ex1:deliverer"));
    assert(!affected_contains(&slice, "ex2:buyer"));

    slice = slice_one(graph, "d-middle", "ex1/quote");
    assert(slice.affected_count == 2);
    assert(affected_contains(&slice, "ex1:settlement"));
    assert(affected_contains(&slice, "ex1:deliverer"));
    assert(!affected_contains(&slice, "ex1:buyer"));
    assert(!affected_contains(&slice, "ex1:seller"));

    slice = slice_one(graph, "d-tail", "ex1/acceptance");
    assert(slice.affected_count == 1);
    assert(affected_contains(&slice, "ex1:deliverer"));

    slice = slice_one(graph, "d-unrelated", "ex1/weather");
    assert(slice.affected_count == 0);

    before = graph->program_count;
    added = 99;
    assert(bf_protocol_derivation_register(
        &contract, "ex1", graph, &added) == BF_PROTOCOL_DERIVATION_BRIDGE_DERIVATION);
    assert(added == 0);
    assert(graph->program_count == before);

    {
        BfProtocolContract broken;
        const char *requires[] = {"missing"};
        const char *produces[] = {"output"};
        BfProtocolRoleSpec stuck = {
            .name = "stuck",
            .requires = requires,
            .requires_count = 1,
            .produces = produces,
            .produces_count = 1,
        };
        bf_protocol_contract_init(&broken, "broken");
        assert(bf_protocol_contract_role(&broken, &stuck) == BF_PROTOCOL_OK);
        before = graph->program_count;
        assert(bf_protocol_derivation_register(
            &broken, "broken:1", graph, &added) ==
            BF_PROTOCOL_DERIVATION_BRIDGE_NONCONFORMANT);
        assert(graph->program_count == before);
    }

    printf("{\"schema\":\"bonfyre-protocol-derivation-bridge.v1\","
           "\"registered\":true,\"contract\":\"quote-accept-fixture\","
           "\"enactments\":2,\"programs\":8,\"scoped\":true,"
           "\"targeted_invalidation\":true,\"atomic\":true}\n");
    free(graph);
    return 0;
}
