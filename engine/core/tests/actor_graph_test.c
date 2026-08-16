/* Native conformance for the ActorGraph: shared actor_nodes/actor_edges store,
 * declared node/edge kinds, mandatory provenance, the non-collapsing confidence
 * tier (verified vs asserted vs inferred), edge endpoint existence, and
 * neighbour/unverified counting. The db path is printed so a companion harness
 * can reopen it from Python and confirm the shared store. */

#include "bf_actor.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fail(const char *msg) {
    fprintf(stderr, "FAIL: %s\n", msg);
    return 1;
}

int main(int argc, char **argv) {
    char default_path[] = "/tmp/bf_actor_XXXXXX";
    const char *db_path = default_path;
    if (argc > 1) {
        db_path = argv[1]; /* harness supplies a path it will reopen from Python */
    } else {
        int fd = mkstemp(default_path);
        if (fd >= 0) {
            close(fd);
        }
        unlink(default_path);
    }

    BfActorGraph *graph = NULL;
    if (bf_actor_open(db_path, &graph, stderr) != 0) {
        return fail("open");
    }

    BfActorNodeSpec org;
    memset(&org, 0, sizeof(org));
    org.actor_id = "org:githubfund";
    org.node_kind = "organization";
    org.display_name = "GitHub Secure OSS Fund";
    org.confidence = "verified";
    org.provenance = "public program page";
    if (bf_actor_upsert(graph, &org, NULL) != BF_ACTOR_OK) {
        return fail("upsert org");
    }

    BfActorNodeSpec person;
    memset(&person, 0, sizeof(person));
    person.actor_id = "person:alex";
    person.node_kind = "person";
    person.display_name = "Alex";
    person.role = "maintainer";
    person.org_id = "org:githubfund";
    person.confidence = "asserted"; /* someone said it; not yet confirmed */
    person.provenance = "email signature";
    if (bf_actor_upsert(graph, &person, NULL) != BF_ACTOR_OK) {
        return fail("upsert person");
    }

    /* provenance is mandatory */
    BfActorNodeSpec noprov = person;
    noprov.actor_id = "person:ghost";
    noprov.provenance = "";
    if (bf_actor_upsert(graph, &noprov, NULL) != BF_ACTOR_MISSING_PROVENANCE) {
        return fail("missing provenance should be rejected");
    }
    /* undeclared node kind */
    BfActorNodeSpec badkind = person;
    badkind.actor_id = "x:1";
    badkind.node_kind = "robot";
    if (bf_actor_upsert(graph, &badkind, NULL) != BF_ACTOR_INVALID_KIND) {
        return fail("undeclared node kind should be rejected");
    }
    /* undeclared confidence tier -- cannot invent an epistemic level */
    BfActorNodeSpec badconf = person;
    badconf.actor_id = "x:2";
    badconf.confidence = "probably";
    if (bf_actor_upsert(graph, &badconf, NULL) != BF_ACTOR_INVALID_CONFIDENCE) {
        return fail("undeclared confidence should be rejected");
    }

    /* two nodes recorded, one verified -> one unverified */
    if (bf_actor_unverified_count(graph) != 1) {
        return fail("unverified count should be 1");
    }

    /* a typed edge between existing endpoints */
    BfActorEdgeSpec employs;
    memset(&employs, 0, sizeof(employs));
    employs.from_id = "org:githubfund";
    employs.edge_kind = "employs";
    employs.to_id = "person:alex";
    employs.provenance = "org chart";
    if (bf_actor_add_edge(graph, &employs, NULL) != BF_ACTOR_OK) {
        return fail("add employs edge");
    }
    /* an edge to an actor we never recorded is refused */
    BfActorEdgeSpec dangling = employs;
    dangling.to_id = "person:nobody";
    if (bf_actor_add_edge(graph, &dangling, NULL) != BF_ACTOR_UNKNOWN_ENDPOINT) {
        return fail("edge to unknown endpoint should be refused");
    }
    /* undeclared edge kind */
    BfActorEdgeSpec badedge = employs;
    badedge.edge_kind = "vibes_with";
    if (bf_actor_add_edge(graph, &badedge, NULL) != BF_ACTOR_INVALID_KIND) {
        return fail("undeclared edge kind should be rejected");
    }

    if (bf_actor_neighbour_count(graph, "person:alex", NULL) != 1) {
        return fail("alex should have 1 neighbour edge");
    }
    if (bf_actor_neighbour_count(graph, "person:alex", "funds") != 0) {
        return fail("alex should have 0 funds edges");
    }

    /* confirming a person promotes asserted -> verified (upsert), not a new row */
    person.confidence = "verified";
    person.provenance = "confirmed on call";
    if (bf_actor_upsert(graph, &person, NULL) != BF_ACTOR_OK) {
        return fail("re-upsert to verify");
    }
    if (bf_actor_unverified_count(graph) != 0) {
        return fail("unverified count should be 0 after verifying");
    }

    bf_actor_close(graph);
    printf("DB=%s\n", db_path);
    printf("actor_graph_test: PASS\n");
    return 0;
}
