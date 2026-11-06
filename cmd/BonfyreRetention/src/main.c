/* RetentionGC, as something the estate can actually run.
 *
 * Bodies are removable material; occurrences are institutional history. The
 * organ keeps them apart, refuses to remove a body while any hold stands, and
 * always leaves a tombstone so the occurrence stays answerable after the body
 * is gone. It was written, tested, and then called by nothing: no caller for
 * bf_retention_retain existed anywhere in the estate.
 *
 * This is the caller. It records what is retained, asks whether a record may
 * be removed, and sweeps what may. It supplies no tier: a kind carries its own
 * closed retention tier, which is the whole point -- a caller that could name
 * the tier could delete anything by calling it short-lived.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf_retention.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static int64_t now_ms(void) {
    return (int64_t)time(NULL) * 1000;
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
    fputs("BonfyreRetention -- what may be removed, and what may never be\n\n"
          "  bonfyre-retention retain --db F --record R --occurrence O --kind K [--body B]\n"
          "  bonfyre-retention removable --db F --record R\n"
          "  bonfyre-retention collect --db F [--occurrence O] [--apply]\n"
          "  bonfyre-retention tombstone --db F --record R\n\n"
          "A kind carries its own retention tier; no caller may supply one.\n"
          "An unknown kind is refused rather than given a default lifetime.\n", stderr);
    return 2;
}

int main(int argc, char *argv[]) {
    const char *db = getenv("BONFYRE_RETENTION_DB");
    const char *record = NULL, *occurrence = NULL, *kind = NULL, *body = "";
    int apply = 0, i;
    BfRetention *retention = NULL;
    BfRetentionStatus status;

    if (argc < 2) return usage();
    for (i = 2; i < argc; i++) {
        if (!strcmp(argv[i], "--apply")) { apply = 1; continue; }
        if (i + 1 >= argc) break;
        if (!strcmp(argv[i], "--db")) db = argv[i + 1];
        else if (!strcmp(argv[i], "--record")) record = argv[i + 1];
        else if (!strcmp(argv[i], "--occurrence")) occurrence = argv[i + 1];
        else if (!strcmp(argv[i], "--kind")) kind = argv[i + 1];
        else if (!strcmp(argv[i], "--body")) body = argv[i + 1];
    }
    if (!db) {
        fputs("no retention store: pass --db or set BONFYRE_RETENTION_DB. Refusing to create "
              "one in the current directory.\n", stderr);
        return 2;
    }
    if (bf_retention_open(db, &retention, stderr) != BF_RETENTION_OK) {
        fputs("{\"error\":\"retention store unavailable\"}\n", stdout);
        return 1;
    }

    enum {
        RT_COLLECT, RT_REMOVABLE, RT_RETAIN, RT_TOMBSTONE,
    };
    static const BfCommandEntry retention_commands[] = {
        {"collect", RT_COLLECT, 0, NULL},
        {"removable", RT_REMOVABLE, 0, NULL},
        {"retain", RT_RETAIN, 0, NULL},
        {"tombstone", RT_TOMBSTONE, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(retention_commands,
        sizeof(retention_commands) / sizeof(retention_commands[0]), argv[1]);

    if (command && command->id == RT_RETAIN) {
        if (!record || !occurrence || !kind) { bf_retention_close(retention); return usage(); }
        status = bf_retention_retain(retention, record, occurrence, kind, now_ms(), body);
        if (status != BF_RETENTION_OK) {
            /* The organ refuses a kind it has no tier for, rather than giving
             * it a default lifetime. A default would be a deletion policy
             * nobody wrote. */
            fputs("{\"refused\":", stdout); json_text(bf_retention_status_name(status));
            fputs(",\"kind\":", stdout); json_text(kind);
            fputs(",\"why\":", stdout);
            json_text("this kind carries no retention tier, so the organ will not retain it "
                      "under a lifetime nobody declared");
            fputs("}\n", stdout);
            bf_retention_close(retention);
            return 3;
        }
        fputs("{\"retained\":", stdout); json_text(record);
        fputs(",\"kind\":", stdout); json_text(kind); fputs("}\n", stdout);
        bf_retention_close(retention);
        return 0;
    }

    if (command && command->id == RT_REMOVABLE) {
        BfRetentionDecision decision;
        if (!record) { bf_retention_close(retention); return usage(); }
        memset(&decision, 0, sizeof(decision));
        status = bf_retention_removable(retention, record, now_ms(), &decision);
        if (status != BF_RETENTION_OK) {
            fputs("{\"error\":", stdout); json_text(bf_retention_status_name(status)); fputs("}\n", stdout);
            bf_retention_close(retention); return 1;
        }
        fputs("{\"record\":", stdout); json_text(record);
        printf(",\"may_remove\":%s", decision.may_remove ? "true" : "false");
        if (!decision.may_remove) {
            fputs(",\"refusal\":", stdout); json_text(decision.refusal.reason);
            fputs(",\"holder\":", stdout); json_text(decision.refusal.holder);
        }
        fputs("}\n", stdout);
        bf_retention_close(retention);
        return 0;
    }

    if (command && command->id == RT_COLLECT) {
        BfRetentionSweep sweep;
        size_t n;
        memset(&sweep, 0, sizeof(sweep));
        if (!apply) {
            /* Without --apply nothing is swept: this organ removes bodies, and
             * a sweep is not something to discover by running a command to see
             * what it does. */
            fputs("{\"swept\":false,\"why\":", stdout);
            json_text("collect removes bodies; pass --apply to sweep");
            fputs("}\n", stdout);
            bf_retention_close(retention);
            return 0;
        }
        status = bf_retention_collect(retention, now_ms(), occurrence, &sweep);
        if (status != BF_RETENTION_OK) {
            fputs("{\"error\":", stdout); json_text(bf_retention_status_name(status)); fputs("}\n", stdout);
            bf_retention_close(retention); return 1;
        }
        printf("{\"removed\":%zu,\"refused\":%zu,\"removed_records\":[",
               sweep.removed_count, sweep.refused_count);
        for (n = 0; n < sweep.removed_count; n++) {
            if (n) putchar(',');
            json_text(sweep.removed[n]);
        }
        fputs("],\"refusals\":[", stdout);
        for (n = 0; n < sweep.refused_count; n++) {
            if (n) putchar(',');
            fputs("{\"record\":", stdout); json_text(sweep.refused[n].record_id);
            fputs(",\"reason\":", stdout); json_text(sweep.refused[n].reason);
            fputs(",\"holder\":", stdout); json_text(sweep.refused[n].holder);
            putchar('}');
        }
        fputs("],\"cannot_infer\":", stdout);
        json_text("a removed body is absent; the occurrence remains answerable through its "
                  "tombstone, and removal says nothing about whether the thing was true");
        fputs("}\n", stdout);
        bf_retention_close(retention);
        return 0;
    }

    if (command && command->id == RT_TOMBSTONE) {
        BfRetentionTombstone stone;
        int present = 0;
        if (!record) { bf_retention_close(retention); return usage(); }
        memset(&stone, 0, sizeof(stone));
        status = bf_retention_tombstone(retention, record, &stone, &present);
        if (status != BF_RETENTION_OK) {
            fputs("{\"error\":", stdout); json_text(bf_retention_status_name(status)); fputs("}\n", stdout);
            bf_retention_close(retention); return 1;
        }
        if (!present) {
            fputs("{\"tombstone\":null,\"happened\":", stdout);
            fputs(bf_retention_happened(retention, record) ? "true" : "false", stdout);
            fputs("}\n", stdout);
        } else {
            fputs("{\"record\":", stdout); json_text(stone.record_id);
            fputs(",\"occurrence\":", stdout); json_text(stone.occurrence);
            fputs(",\"kind\":", stdout); json_text(stone.kind);
            fputs(",\"tier\":", stdout); json_text(stone.tier);
            printf(",\"written_at_ms\":%lld,\"removed_at_ms\":%lld}\n",
                   (long long)stone.written_at_ms, (long long)stone.removed_at_ms);
        }
        bf_retention_close(retention);
        return 0;
    }

    bf_retention_close(retention);
    return usage();
}
