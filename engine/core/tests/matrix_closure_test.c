#define _POSIX_C_SOURCE 200809L
#include "bf_matrix.h"
#include "bf_matrix_closure.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fail(const char *message) { fprintf(stderr, "FAIL: %s\n", message); return 1; }

static int contains(FILE *file, const char *needle) {
    char body[16384]; size_t n;
    if (fflush(file) != 0 || fseek(file, 0, SEEK_SET) != 0) return 0;
    n = fread(body, 1, sizeof(body) - 1, file); body[n] = '\0';
    return strstr(body, needle) != NULL;
}

int main(void) {
    char path[] = "/tmp/bf_matrix_closure_XXXXXX";
    BfMatrix *matrix = NULL; BfMatrixAddressResolution resolution;
    BfMatrixSubjectSpec subject = {
        "institution:closure-test", "Institution",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "receipt:closure-test"
    };
    BfMatrixPresenceSpec presence = {
        "presence:closure-test", "institution:closure-test", NULL,
        "live", BF_MATRIX_PUBLIC, BF_MATRIX_LIVE, "HTML", "receipt:presence"
    };
    BfMatrixClosureContributionSpec material = {
        "institution:closure-test", "presence:closure-test", "material",
        "ObjectFabric", "bonfyre://objectfabric/closure-test/body",
        "Zarr", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "receipt:objectfabric:closure-test"
    };
    BfMatrixClosureContributionSpec invalid_presence = {
        "institution:closure-test", "presence:missing", "material",
        "ObjectFabric", "bonfyre://objectfabric/closure-test/missing",
        "Zarr", "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        "receipt:objectfabric:missing"
    };
    FILE *crate = NULL, *zarr = NULL; int fd = mkstemp(path);
    if (fd >= 0) close(fd); unlink(path);
    if (bf_matrix_open(path, &matrix, stderr) != 0) return fail("open");
    if (bf_matrix_subject_bind(matrix, &subject) != BF_MATRIX_OK ||
        bf_matrix_address_bind(matrix, subject.subject_id, "web_url", "https://closure.aurekai.live/", "receipt:address") != BF_MATRIX_OK ||
        bf_matrix_presence_bind(matrix, &presence) != BF_MATRIX_OK ||
        bf_matrix_closure_contribution_bind(matrix, &material) != BF_MATRIX_OK ||
        bf_matrix_closure_contribution_bind(matrix, &invalid_presence) != BF_MATRIX_UNKNOWN_PRESENCE ||
        bf_matrix_address_resolve(matrix, "https://closure.aurekai.live/", "live", BF_MATRIX_LIVE,
                                  BF_MATRIX_PUBLIC, "HTML", &resolution) != 1) return fail("bind and resolve");
    crate = tmpfile(); zarr = tmpfile();
    if (!crate || !zarr ||
        bf_matrix_closure_emit_ro_crate(crate, matrix, &resolution,
                                        "https://closure.aurekai.live/ro-crate/ro-crate-metadata.json",
                                        "https://closure.aurekai.live/zarr/zarr.json") != 0 ||
        !contains(crate, "BonfyreMatrixClosure") ||
        !contains(crate, "\"name\":\"exact-semantic\",\"state\":\"resolved\"") ||
        !contains(crate, "\"name\":\"material\",\"state\":\"referenced\"") ||
        !contains(crate, "bonfyre://objectfabric/closure-test/body") ||
        !contains(crate, "\"name\":\"economic\",\"state\":\"unresolved\"") ||
        bf_matrix_closure_emit_zarr_group(zarr, matrix, &resolution,
                                          "https://closure.aurekai.live/ro-crate/ro-crate-metadata.json") != 0 ||
        !contains(zarr, "\"zarr_format\":3") || !contains(zarr, "\"bonfyre:virtual\":true") ||
        !contains(zarr, "\"material\":\"referenced\"")) return fail("sparse closure emission");
    fclose(crate); fclose(zarr); bf_matrix_close(matrix); unlink(path);
    puts("matrix_closure_test: PASS"); return 0;
}
