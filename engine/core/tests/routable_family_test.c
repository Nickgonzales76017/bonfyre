/* Native conformance: the WorkGraph rejects an unroutable node family, matching
 * the Python reference. Closes the gap where BfWorkgraphNodeSpec.family was an
 * unchecked free string that accepted targets like "coordinator". */

#include "bf_workgraph.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fail(const char *msg) {
    fprintf(stderr, "FAIL: %s\n", msg);
    return 1;
}

static BfWorkgraphNodeSpec node(const char *node_id, const char *family) {
    BfWorkgraphNodeSpec spec;
    memset(&spec, 0, sizeof(spec));
    spec.mission_id = "m";
    spec.node_id = node_id;
    spec.operator_id = "op";
    spec.family = family;
    return spec;
}

int main(void) {
    char db_path[] = "/tmp/bf_routable_XXXXXX";
    int fd = mkstemp(db_path);
    if (fd >= 0) {
        close(fd);
    }
    unlink(db_path); /* let open create the schema fresh */

    BfWorkgraph *graph = NULL;
    if (bf_workgraph_open(db_path, &graph, stderr) != 0) {
        return fail("open");
    }
    if (bf_workgraph_create_mission(graph, "m").status != BF_WORKGRAPH_OK) {
        return fail("create_mission");
    }

    /* before any family is registered, behavior is unchanged (permissive) */
    if (bf_workgraph_add_node(graph, &(BfWorkgraphNodeSpec){0}).status != BF_WORKGRAPH_INVALID) {
        return fail("empty spec should be invalid");
    }
    BfWorkgraphNodeSpec s0 = node("n0", "coordinator");
    if (bf_workgraph_add_node(graph, &s0).status != BF_WORKGRAPH_OK) {
        return fail("permissive before registry");
    }

    /* register the routable families */
    bf_workgraph_register_family(graph, "T_FPQ");
    bf_workgraph_register_family(graph, "T_KVCACHE");

    /* a registered family is accepted */
    BfWorkgraphNodeSpec s1 = node("n1", "T_FPQ");
    if (bf_workgraph_add_node(graph, &s1).status != BF_WORKGRAPH_OK) {
        return fail("registered family rejected");
    }
    /* "default" is always accepted */
    BfWorkgraphNodeSpec s2 = node("n2", "default");
    if (bf_workgraph_add_node(graph, &s2).status != BF_WORKGRAPH_OK) {
        return fail("default rejected");
    }
    /* an unroutable family is now REJECTED (the bug) */
    BfWorkgraphNodeSpec s3 = node("n3", "coordinator");
    BfWorkgraphResult r3 = bf_workgraph_add_node(graph, &s3);
    if (r3.status != BF_WORKGRAPH_INVALID) {
        return fail("unroutable family accepted");
    }
    if (strcmp(r3.error_code, "unroutable_family") != 0) {
        return fail("wrong error_code for unroutable family");
    }

    bf_workgraph_close(graph);
    unlink(db_path);
    printf("routable_family_test: PASS\n");
    return 0;
}
