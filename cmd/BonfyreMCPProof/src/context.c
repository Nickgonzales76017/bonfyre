#include "proof.h"

#include <stdio.h>
#include <string.h>

static void fallback(char *out, size_t cap, const char *base, const char *leaf) {
    snprintf(out, cap, "%s/%s", base, leaf);
}

int bf_load_context(sqlite3 *db, const char *receipt, const char *receipt_path,
                    const char *repo_root, BfProofContext *ctx,
                    BfProofChecks *checks) {
    char receipt_dir[PATH_MAX];
    char schema[128] = "", status[64] = "", cp_type[32] = "", cp_schema[128] = "";

    memset(ctx, 0, sizeof(*ctx));
    memset(checks, 0, sizeof(*checks));
    snprintf(ctx->receipt_path, sizeof(ctx->receipt_path), "%s", receipt_path);
    snprintf(ctx->repo_root, sizeof(ctx->repo_root), "%s", repo_root ? repo_root : "");
    bf_dirname_copy(receipt_path, receipt_dir, sizeof(receipt_dir));
    bf_dirname_copy(receipt_dir, ctx->control_home, sizeof(ctx->control_home));

    bf_jtext(db, receipt, "$.receipt_id", ctx->receipt_id, sizeof(ctx->receipt_id));
    bf_jtext(db, receipt, "$.schema_version", schema, sizeof(schema));
    bf_jtext(db, receipt, "$.status", status, sizeof(status));
    bf_jtype(db, receipt, "$.control_plane", cp_type, sizeof(cp_type));
    bf_jtext(db, receipt, "$.control_plane.schema_version", cp_schema, sizeof(cp_schema));
    bf_jtext(db, receipt, "$.output_sha256", ctx->expected_hash, sizeof(ctx->expected_hash));
    bf_jtext(db, receipt, "$.model", ctx->model, sizeof(ctx->model));

    checks->manifest = !strcmp(cp_type, "object");
    checks->receipt_schema =
        !strcmp(schema, "bonfyre.moe.generation.receipt.v1") &&
        (!checks->manifest || !strcmp(cp_schema, "bonfyre.moe.control-plane.receipt.v1"));
    checks->receipt_status = !strcmp(status, "passed");

    if (!bf_jtext(db, receipt, "$.control_plane.cms_lambda_tensor.database",
                  ctx->cms_db, sizeof(ctx->cms_db)))
        fallback(ctx->cms_db, sizeof(ctx->cms_db), ctx->control_home, "cms.db");
    if (!bf_jtext(db, receipt, "$.control_plane.cms_lambda_tensor.schemas",
                  ctx->cms_schemas, sizeof(ctx->cms_schemas)))
        fallback(ctx->cms_schemas, sizeof(ctx->cms_schemas), ctx->control_home, "content-types");
    if (!bf_jtext(db, receipt, "$.control_plane.cms_lambda_tensor.namespace",
                  ctx->cms_namespace, sizeof(ctx->cms_namespace)))
        snprintf(ctx->cms_namespace, sizeof(ctx->cms_namespace), "bonfyre-generation");
    bf_jint(db, receipt, "$.control_plane.cms_lambda_tensor.entry_id", &ctx->cms_entry_id);
    if (!bf_jtext(db, receipt, "$.control_plane.cms_lambda_tensor.external_id",
                  ctx->cms_external_id, sizeof(ctx->cms_external_id)))
        snprintf(ctx->cms_external_id, sizeof(ctx->cms_external_id), "%s:%s",
                 ctx->model, ctx->expected_hash);

    if (!bf_jtext(db, receipt, "$.control_plane.artifact_graph.database",
                  ctx->graph_db, sizeof(ctx->graph_db)))
        fallback(ctx->graph_db, sizeof(ctx->graph_db), ctx->control_home, "generation-graph.db");
    if (!bf_jtext(db, receipt, "$.control_plane.artifact_graph.operator",
                  ctx->graph_operator, sizeof(ctx->graph_operator)))
        snprintf(ctx->graph_operator, sizeof(ctx->graph_operator), "moe.generate-%s", ctx->receipt_id);
    if (!bf_jtext(db, receipt, "$.control_plane.artifact_graph.output_atom",
                  ctx->graph_output_atom, sizeof(ctx->graph_output_atom)))
        snprintf(ctx->graph_output_atom, sizeof(ctx->graph_output_atom),
                 "generation-output-%s", ctx->receipt_id);

    if (!bf_jtext(db, receipt, "$.control_plane.workgraph.database",
                  ctx->flow_db, sizeof(ctx->flow_db)))
        fallback(ctx->flow_db, sizeof(ctx->flow_db), ctx->control_home, "workgraphs.db");
    if (!bf_jtext(db, receipt, "$.control_plane.workgraph.definition",
                  ctx->flow_definition, sizeof(ctx->flow_definition)))
        snprintf(ctx->flow_definition, sizeof(ctx->flow_definition),
                 "%s/workgraphs/%s.json", ctx->control_home, ctx->receipt_id);
    if (!bf_jtext(db, receipt, "$.control_plane.workgraph.flow_id",
                  ctx->flow_id, sizeof(ctx->flow_id)))
        snprintf(ctx->flow_id, sizeof(ctx->flow_id), "generation-%s", ctx->receipt_id);

    if (!bf_jtext(db, receipt, "$.control_plane.actor_graph.database",
                  ctx->actor_db, sizeof(ctx->actor_db)))
        snprintf(ctx->actor_db, sizeof(ctx->actor_db), "%s/discipl.db",
                 ctx->repo_root[0] ? ctx->repo_root : ".");
    bf_dirname_copy(ctx->actor_db, ctx->actor_root, sizeof(ctx->actor_root));
    return 0;
}
