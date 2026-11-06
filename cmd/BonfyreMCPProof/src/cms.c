#include "proof.h"
#include "bonfyre.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int bf_verify_cms(sqlite3 *db, const char *receipt, BfProofContext *ctx,
                  BfProofChecks *checks) {
    char binbuf[PATH_MAX], out[262144], err[32768];
    const char *cms = bf_owner_bin("BONFYRE_CMS_BINARY", "bonfyre-cms",
                                   binbuf, sizeof(binbuf));
    char *entry = NULL;
    (void)receipt;

    if (ctx->cms_entry_id > 0) {
        char idbuf[32];
        snprintf(idbuf, sizeof(idbuf), "%lld", ctx->cms_entry_id);
        char *const av[] = {
            (char *)cms, "entry", "get", "cms_generation", idbuf,
            "--ns", ctx->cms_namespace, "--db", ctx->cms_db,
            "--schemas", ctx->cms_schemas, NULL
        };
        if (bf_run_capture(av, NULL, NULL, out, sizeof(out), err, sizeof(err)) == 0)
            entry = bf_jraw(db, out, "$.data");
    } else {
        char *const av[] = {
            (char *)cms, "entry", "list", "cms_generation", "--limit", "100",
            "--ns", ctx->cms_namespace, "--db", ctx->cms_db,
            "--schemas", ctx->cms_schemas, NULL
        };
        if (bf_run_capture(av, NULL, NULL, out, sizeof(out), err, sizeof(err)) == 0)
            entry = bf_array_object_by(db, out, "$.data",
                                       "$.attributes.external_id",
                                       ctx->cms_external_id);
    }

    if (entry) {
        char output[65536] = "", declared[80] = "", actual[65] = "";
        char type[32] = "";
        checks->cms_entry = 1;
        bf_jtext(db, entry, "$.attributes.output", output, sizeof(output));
        bf_jtext(db, entry, "$.attributes.output_sha256", declared, sizeof(declared));
        bf_sha256_hex((const uint8_t *)output, strlen(output), actual);
        checks->cms_hash = ctx->expected_hash[0] &&
                           !strcmp(actual, ctx->expected_hash);
        checks->cms_declared = ctx->expected_hash[0] &&
                               !strcmp(declared, ctx->expected_hash);
        checks->cms_schema =
            bf_jtype(db, entry, "$.attributes.output", type, sizeof(type)) &&
            strcmp(type, "null") &&
            bf_jtype(db, entry, "$.attributes.output_sha256", type, sizeof(type)) &&
            strcmp(type, "null") &&
            bf_jtype(db, entry, "$.attributes.external_id", type, sizeof(type)) &&
            strcmp(type, "null") &&
            bf_jtype(db, entry, "$.attributes.source_schema_version", type, sizeof(type)) &&
            strcmp(type, "null");
    }

    free(entry);
    return checks->cms_entry && checks->cms_hash &&
           checks->cms_declared && checks->cms_schema;
}
