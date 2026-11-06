/* The commerce chain, as something the estate can actually run.
 *
 * Fifteen non-interchangeable stages, each requiring exactly the one before
 * it, each naming the owner entitled to establish it and what it still does
 * not establish. The organ was measured against all two hundred and twenty-five
 * stage pairs and then called by nothing: no caller for bf_commerce_record
 * existed anywhere in the estate, so the chain was a proof with no subject.
 *
 * This is the caller. It exists so a shopping feed can say the one honest
 * thing an observation entitles it to say -- that a product is listed -- and
 * so a surface can show the thirteen stages that remain, each with the owner
 * who alone could establish it. That list is the truthful answer to "what can
 * be done here", and it comes from the organ rather than from a page.
 *
 * It records stages. It never performs one: nothing here pays, commits,
 * fulfils or takes ownership of anything.
 */
#include <stdio.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include <stdlib.h>
#include <string.h>
#include "bf_commerce.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static BfCommerceStage stage_by_name(const char *name, int *ok) {
    BfCommerceStage stage;
    *ok = 0;
    for (stage = BF_COMMERCE_PRODUCT; stage < BF_COMMERCE_STAGE_COUNT; stage++) {
        const char *known = bf_commerce_stage_name(stage);
        if (known && name && !strcmp(known, name)) { *ok = 1; return stage; }
    }
    return BF_COMMERCE_PRODUCT;
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
    fputs("BonfyreCommerce -- the fifteen-stage chain, recorded and read\n\n"
          "  bonfyre-commerce record --db F --exchange E --stage S --owner O [--detail D] [--at T]\n"
          "  bonfyre-commerce progress --db F --exchange E\n"
          "  bonfyre-commerce stages\n\n"
          "A stage requires exactly the one before it. Recording a stage establishes\n"
          "that stage and nothing later; nothing here performs a commercial act.\n", stderr);
    return 2;
}

static int cmd_stages(void) {
    BfCommerceStage stage;
    fputs("{\"stages\":[", stdout);
    for (stage = BF_COMMERCE_PRODUCT; stage < BF_COMMERCE_STAGE_COUNT; stage++) {
        if (stage) putchar(',');
        fputs("{\"stage\":", stdout); json_text(bf_commerce_stage_name(stage));
        fputs(",\"owner\":", stdout); json_text(bf_commerce_stage_owner(stage));
        fputs(",\"does_not_establish\":", stdout); json_text(bf_commerce_stage_does_not_establish(stage));
        putchar('}');
    }
    fputs("]}\n", stdout);
    return 0;
}

static int cmd_progress(const char *db, const char *exchange) {
    BfCommerce *commerce = NULL;
    BfCommerceProgress outstanding;
    BfCommerceStage furthest = BF_COMMERCE_PRODUCT, stage;
    int found = 0, first = 1;
    size_t i;
    if (bf_commerce_open(db, &commerce, stderr) != BF_COMMERCE_OK) {
        fputs("{\"error\":\"commerce store unavailable\"}\n", stdout);
        return 1;
    }
    memset(&outstanding, 0, sizeof(outstanding));
    bf_commerce_furthest(commerce, exchange, &furthest, &found);
    bf_commerce_outstanding(commerce, exchange, &outstanding);

    fputs("{\"exchange\":", stdout); json_text(exchange);
    fputs(",\"established\":[", stdout);
    for (stage = BF_COMMERCE_PRODUCT; stage < BF_COMMERCE_STAGE_COUNT; stage++) {
        int reached = 0;
        bf_commerce_reached(commerce, exchange, stage, &reached);
        if (!reached) continue;
        if (!first) putchar(',');
        first = 0;
        fputs("{\"stage\":", stdout); json_text(bf_commerce_stage_name(stage));
        fputs(",\"does_not_establish\":", stdout);
        json_text(bf_commerce_stage_does_not_establish(stage));
        putchar('}');
    }
    fputs("],\"furthest\":", stdout);
    if (found) json_text(bf_commerce_stage_name(furthest)); else fputs("null", stdout);
    /* What has not happened, and who alone could make it happen. This is the
     * honest form of "what can be done here": not a button this estate owns,
     * but the next fact and the party entitled to establish it. */
    fputs(",\"outstanding\":[", stdout);
    for (i = 0; i < outstanding.count; i++) {
        if (i) putchar(',');
        fputs("{\"stage\":", stdout); json_text(bf_commerce_stage_name(outstanding.stages[i]));
        fputs(",\"owner\":", stdout); json_text(bf_commerce_stage_owner(outstanding.stages[i]));
        putchar('}');
    }
    fputs("],\"cannot_infer\":", stdout);
    json_text("an established stage establishes only itself; nothing here says a later stage "
              "happened, and this estate performs none of them");
    fputs("}\n", stdout);
    bf_commerce_close(commerce);
    return 0;
}

int main(int argc, char *argv[]) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    const char *db = getenv("BONFYRE_COMMERCE_DB");
    const char *exchange = NULL, *owner = NULL, *detail = "", *at = "", *stage_name = NULL;
    int i, ok = 0;
    if (argc < 2) return usage();
    enum { CM_PROGRESS, CM_RECORD, CM_STAGES };
    static const BfCommandEntry commerce_commands[] = {
        {"progress", CM_PROGRESS, 0, NULL},
        {"record", CM_RECORD, 0, NULL},
        {"stages", CM_STAGES, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(commerce_commands,
        sizeof(commerce_commands) / sizeof(commerce_commands[0]), argv[1]);
    for (i = 2; i < argc - 1; i++) {
        if (!strcmp(argv[i], "--db")) db = argv[i + 1];
        else if (!strcmp(argv[i], "--exchange")) exchange = argv[i + 1];
        else if (!strcmp(argv[i], "--stage")) stage_name = argv[i + 1];
        else if (!strcmp(argv[i], "--owner")) owner = argv[i + 1];
        else if (!strcmp(argv[i], "--detail")) detail = argv[i + 1];
        else if (!strcmp(argv[i], "--at")) at = argv[i + 1];
    }
    if (command && command->id == CM_STAGES) return cmd_stages();
    if (!db) {
        fputs("no commerce store: pass --db or set BONFYRE_COMMERCE_DB. Refusing to create "
              "one in the current directory.\n", stderr);
        return 2;
    }
    if (command && command->id == CM_PROGRESS) {
        if (!exchange) return usage();
        return cmd_progress(db, exchange);
    }
    if (command && command->id == CM_RECORD) {
        BfCommerce *commerce = NULL;
        BfCommerceStage stage;
        BfCommerceStatus status;
        if (!exchange || !stage_name || !owner) return usage();
        stage = stage_by_name(stage_name, &ok);
        if (!ok) { fprintf(stderr, "no such stage: %s\n", stage_name); return 2; }
        if (bf_commerce_open(db, &commerce, stderr) != BF_COMMERCE_OK) return 1;
        status = bf_commerce_record(commerce, exchange, stage, owner, detail, at);
        if (status == BF_COMMERCE_NOT_FOUND) {
            /* The chain refused because the prior fact is not established. That
             * refusal is the organ working, so it is reported as such. */
            fprintf(stderr, "refused: %s requires %s to be established first\n",
                    bf_commerce_stage_name(stage),
                    bf_commerce_stage_name((BfCommerceStage)(stage - 1)));
            bf_commerce_close(commerce);
            return 3;
        }
        bf_commerce_close(commerce);
        if (status != BF_COMMERCE_OK) {
            fprintf(stderr, "refused: %s\n", bf_commerce_status_name(status));
            return 1;
        }
        printf("{\"exchange\":");
        json_text(exchange);
        printf(",\"established\":");
        json_text(bf_commerce_stage_name(stage));
        printf(",\"owner\":");
        json_text(owner);
        printf(",\"does_not_establish\":");
        json_text(bf_commerce_stage_does_not_establish(stage));
        printf("}\n");
        return 0;
    }
    return usage();
}
