/* ActorGraph, as something the estate can actually run.
 *
 * The archive credits a maker on nearly every thing it kept -- thousands of
 * names a publisher printed beside an article -- and the estate had no model
 * of who any of them are. ActorGraph is that model, and nothing called it: no
 * file outside its own source, header and test named a symbol it exports.
 *
 * The load-bearing invariant is why it matters here. Every node and edge
 * carries a mandatory provenance and a confidence tier that is never
 * collapsed: verified is a human confirming it, asserted is someone saying it,
 * inferred is a model concluding it unchecked. A name printed in a byline is
 * asserted and nothing more. Two publishers printing the same name is not one
 * person, and this refuses to say it is.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf_actor.h"
#include "bf_corroboration.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static void iso_now(char *out, size_t cap) {
    time_t now = time(NULL);
    struct tm tm;
    gmtime_r(&now, &tm);
    strftime(out, cap, "%Y-%m-%dT%H:%M:%SZ", &tm);
}

static void json_text(const char *s) {
    putchar('"');
    for (; s && *s; s++) {
        unsigned char c = (unsigned char)*s;
        if (c == '"' || c == '\\') { putchar('\\'); putchar(c); }
        else if (c == '\n') fputs("\\n", stdout);
        else if (c < 0x20) printf("\\u%04x", c);
        else putchar(c);
    }
    putchar('"');
}

static int usage(void) {
    fputs("BonfyreActors -- who, and how well this estate knows it\n\n"
          "  bonfyre-actors upsert --db F --id A --kind K --name N --provenance P\n"
          "                        [--role R] [--org O] [--confidence verified|asserted|inferred]\n"
          "  bonfyre-actors relate --db F --from A --kind E --to B --provenance P [--confidence C]\n"
          "  bonfyre-actors corroborate --db F --id A --source S [--evidence E] [--detail D]\n"
          "  bonfyre-actors standing --db F [--id A]\n\n"
          "Confidence defaults to asserted. A byline is asserted; only a human\n"
          "confirming it makes it verified, and this will not do that for you.\n", stderr);
    return 2;
}

int main(int argc, char *argv[]) {
    const char *db = getenv("BONFYRE_ACTOR_DB");
    const char *id = NULL, *kind = NULL, *name = NULL, *role = "", *org = NULL;
    const char *confidence = NULL, *provenance = NULL, *detail = "";
    const char *from = NULL, *to = NULL, *source = NULL, *evidence = "";
    char now[64];
    int i;

    if (argc < 2) return usage();
    for (i = 2; i < argc - 1; i++) {
        if (!strcmp(argv[i], "--db")) db = argv[i + 1];
        else if (!strcmp(argv[i], "--id")) id = argv[i + 1];
        else if (!strcmp(argv[i], "--kind")) kind = argv[i + 1];
        else if (!strcmp(argv[i], "--name")) name = argv[i + 1];
        else if (!strcmp(argv[i], "--role")) role = argv[i + 1];
        else if (!strcmp(argv[i], "--org")) org = argv[i + 1];
        else if (!strcmp(argv[i], "--confidence")) confidence = argv[i + 1];
        else if (!strcmp(argv[i], "--provenance")) provenance = argv[i + 1];
        else if (!strcmp(argv[i], "--detail")) detail = argv[i + 1];
        else if (!strcmp(argv[i], "--from")) from = argv[i + 1];
        else if (!strcmp(argv[i], "--to")) to = argv[i + 1];
        else if (!strcmp(argv[i], "--source")) source = argv[i + 1];
        else if (!strcmp(argv[i], "--evidence")) evidence = argv[i + 1];
    }
    if (!db) {
        fputs("no actor store: pass --db or set BONFYRE_ACTOR_DB. Refusing to create one "
              "in the current directory.\n", stderr);
        return 2;
    }
    iso_now(now, sizeof(now));

    enum {
        AC_CORROBORATE, AC_RELATE, AC_STANDING, AC_UPSERT,
    };
    static const BfCommandEntry actor_commands[] = {
        {"corroborate", AC_CORROBORATE, 0, NULL},
        {"relate", AC_RELATE, 0, NULL},
        {"standing", AC_STANDING, 0, NULL},
        {"upsert", AC_UPSERT, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(actor_commands,
        sizeof(actor_commands) / sizeof(actor_commands[0]), argv[1]);

    if (command && command->id == AC_UPSERT) {
        BfActorGraph *graph = NULL;
        BfActorNodeSpec spec;
        BfActorStatus status;
        if (!id || !kind || !name || !provenance) return usage();
        if (bf_actor_open(db, &graph, stderr) != 0) return 1;
        memset(&spec, 0, sizeof(spec));
        spec.actor_id = id; spec.node_kind = kind; spec.display_name = name;
        spec.role = role; spec.org_id = org; spec.confidence = confidence;
        spec.provenance = provenance; spec.detail = detail;
        status = bf_actor_upsert(graph, &spec, now);
        bf_actor_close(graph);
        if (status != BF_ACTOR_OK) {
            /* The organ refuses a node it cannot place honestly: an undeclared
             * kind, an undeclared confidence tier, or no provenance at all.
             * Each refusal is the invariant working. */
            fputs("{\"refused\":", stdout);
            json_text(status == BF_ACTOR_INVALID_KIND ? "node_kind not declared"
                    : status == BF_ACTOR_INVALID_CONFIDENCE ? "confidence tier not declared"
                    : status == BF_ACTOR_MISSING_PROVENANCE ? "provenance is mandatory"
                    : status == BF_ACTOR_INVALID ? "a required id field is empty"
                    : "storage error");
            fputs("}\n", stdout);
            return 3;
        }
        fputs("{\"actor\":", stdout); json_text(id);
        fputs(",\"confidence\":", stdout); json_text(confidence ? confidence : "asserted");
        fputs(",\"cannot_infer\":", stdout);
        json_text("a recorded actor is someone this estate has a name for at a stated "
                  "confidence; inferred is never verified, and a name is not an identity");
        fputs("}\n", stdout);
        return 0;
    }

    if (command && command->id == AC_RELATE) {
        BfActorGraph *graph = NULL;
        BfActorEdgeSpec spec;
        BfActorStatus status;
        if (!from || !kind || !to || !provenance) return usage();
        if (bf_actor_open(db, &graph, stderr) != 0) return 1;
        memset(&spec, 0, sizeof(spec));
        spec.from_id = from; spec.edge_kind = kind; spec.to_id = to;
        spec.confidence = confidence; spec.provenance = provenance;
        status = bf_actor_add_edge(graph, &spec, now);
        bf_actor_close(graph);
        if (status != BF_ACTOR_OK) {
            fputs("{\"refused\":", stdout);
            json_text(status == BF_ACTOR_UNKNOWN_ENDPOINT
                      ? "an edge cannot reference an actor that does not exist"
                      : status == BF_ACTOR_INVALID_KIND ? "edge_kind not declared"
                      : status == BF_ACTOR_MISSING_PROVENANCE ? "provenance is mandatory"
                      : "refused");
            fputs("}\n", stdout);
            return 3;
        }
        fputs("{\"related\":true}\n", stdout);
        return 0;
    }

    if (command && command->id == AC_CORROBORATE) {
        BfCorroboration *ledger = NULL;
        int ok;
        if (!id || !source) return usage();
        if (bf_corroboration_open(db, &ledger, stderr) != 0) return 1;
        /* The contract distinguishes a new independent source from a repeat:
         * 1 is newly independent, 0 is this source again, -1 is a refusal.
         * Reading 0 as failure counted only the repeats and threw away every
         * genuinely independent corroboration. */
        ok = bf_corroboration_record(ledger, id, source, evidence, detail, now);
        if (ok >= 0) {
            int64_t independent = bf_corroboration_independent_sources(ledger, id);
            fputs("{\"actor\":", stdout); json_text(id);
            printf(",\"newly_independent\":%s", ok == 1 ? "true" : "false");
            printf(",\"independent_sources\":%lld", (long long)independent);
            fputs(",\"cannot_infer\":", stdout);
            json_text("independent sources naming the same actor is corroboration, not "
                      "verification, and two publishers printing one name is not one person");
            fputs("}\n", stdout);
        } else {
            fputs("{\"refused\":\"corroboration not recorded\"}\n", stdout);
        }
        bf_corroboration_close(ledger);
        return ok >= 0 ? 0 : 3;
    }

    if (command && command->id == AC_STANDING) {
        BfActorGraph *graph = NULL;
        if (bf_actor_open(db, &graph, stderr) != 0) return 1;
        fputs("{\"unverified\":", stdout);
        printf("%lld", (long long)bf_actor_unverified_count(graph));
        if (id) {
            BfCorroboration *ledger = NULL;
            fputs(",\"actor\":", stdout); json_text(id);
            if (bf_corroboration_open(db, &ledger, stderr) == 0) {
                printf(",\"independent_sources\":%lld",
                       (long long)bf_corroboration_independent_sources(ledger, id));
                bf_corroboration_close(ledger);
            }
        }
        fputs(",\"cannot_infer\":", stdout);
        json_text("unverified is what outreach reads before acting on a relationship; it is a "
                  "count of what no human has confirmed, not a count of what is false");
        fputs("}\n", stdout);
        bf_actor_close(graph);
        return 0;
    }

    return usage();
}
