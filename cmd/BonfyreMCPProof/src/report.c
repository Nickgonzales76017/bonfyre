#include "proof.h"

#include <stdio.h>

static void emit_check(const char *name, int passed, const char *detail, int *first) {
    if (!*first) putchar(',');
    *first = 0;
    bf_json_string(name);
    fputs(":{\"passed\":", stdout);
    fputs(passed ? "true" : "false", stdout);
    fputs(",\"detail\":", stdout);
    bf_json_string(detail ? detail : "");
    putchar('}');
}

static int control_ok(const BfProofChecks *c) {
    return c->receipt_schema && c->receipt_status;
}
static int cms_ok(const BfProofChecks *c) {
    return c->cms_entry && c->cms_hash && c->cms_declared && c->cms_schema;
}
static int graph_ok(const BfProofChecks *c) {
    return c->graph_atom && c->graph_hash && c->graph_operator;
}
static int flow_ok(const BfProofChecks *c) {
    return c->flow_record && c->flow_definition;
}

void bf_emit_report(const BfProofContext *ctx, const BfProofChecks *c) {
    int passed = control_ok(c) && c->manifest && cms_ok(c) &&
                 graph_ok(c) && flow_ok(c) && c->discipl_contract;
    int verified = control_ok(c) + cms_ok(c) + graph_ok(c) +
                   flow_ok(c) + c->discipl_contract;
    int first = 1;

    fputs("{\"schema\":\"bonfyre-mcp-generation-proof.v1\",\"receipt_path\":", stdout);
    bf_json_string(ctx->receipt_path);
    fputs(",\"receipt_id\":", stdout); bf_json_string(ctx->receipt_id);
    fputs(",\"passed\":", stdout); fputs(passed ? "true" : "false", stdout);
    printf(",\"verified_owner_count\":%d,\"verified_owners\":[", verified);
    first = 1;
    if (control_ok(c)) { if (!first) putchar(','); first = 0; bf_json_string("MCPControlReceipt"); }
    if (cms_ok(c)) { if (!first) putchar(','); first = 0; bf_json_string("CMSLambdaTensor"); }
    if (graph_ok(c)) { if (!first) putchar(','); first = 0; bf_json_string("ArtifactGraph"); }
    if (flow_ok(c)) { if (!first) putchar(','); first = 0; bf_json_string("WorkGraphFlow"); }
    if (c->discipl_contract) { if (!first) putchar(','); bf_json_string("DisCIPLActorGraph"); }

    fputs("],\"checks\":{", stdout);
    first = 1;
    emit_check("receipt_schema", c->receipt_schema, "generation + control-plane schema", &first);
    emit_check("receipt_status", c->receipt_status, "passed", &first);
    emit_check("control_plane_manifest_persisted", c->manifest, c->manifest ? "persisted" : "missing", &first);
    emit_check("cms_entry", c->cms_entry, ctx->cms_external_id, &first);
    emit_check("cms_output_content_hash", c->cms_hash, ctx->expected_hash, &first);
    emit_check("cms_declared_output_hash", c->cms_declared, ctx->expected_hash, &first);
    emit_check("cms_schema_contract", c->cms_schema, "output+digest+external_id+source_schema_version", &first);
    emit_check("artifact_output_atom", c->graph_atom, ctx->graph_output_atom, &first);
    emit_check("artifact_output_hash", c->graph_hash, ctx->expected_hash, &first);
    emit_check("artifact_operator", c->graph_operator, ctx->graph_operator, &first);
    emit_check("flow_record", c->flow_record, ctx->flow_id, &first);
    emit_check("flow_definition", c->flow_definition, ctx->flow_definition, &first);
    emit_check("discipl_contract", c->discipl_contract, "T_MOE_ROUTER->T_MOE_EXPERT", &first);

    fputs("},\"failures\":[", stdout);
    first = 1;
#define FAIL(name, ok) do { if (!(ok)) { if (!first) putchar(','); first = 0; bf_json_string(name); } } while (0)
    FAIL("receipt_schema", c->receipt_schema);
    FAIL("receipt_status", c->receipt_status);
    FAIL("control_plane_manifest_persisted", c->manifest);
    FAIL("cms_projection", cms_ok(c));
    FAIL("artifact_graph", graph_ok(c));
    FAIL("workgraph", flow_ok(c));
    FAIL("discipl_contract", c->discipl_contract);
#undef FAIL
    fputs("],\"truth\":\"control_plane_projections_verified_independently_from_the_mcp_success_response\","
          "\"cannot_infer\":[\"a recorded projection -> semantic correctness of generated text\","
          "\"a defined flow -> an executed flow\",\"a DisCIPL contract -> enactment or authority\","
          "\"a CMS row -> publication\",\"an ArtifactGraph atom -> an external effect\"]}\n", stdout);
}
