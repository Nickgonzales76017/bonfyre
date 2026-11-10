/* StandingAddress, as something the estate can actually run.
 *
 * A recompiled ranking of addressed opportunities, never a stored feed. The
 * invariant is the whole point: attention may reorder candidates only. It
 * cannot invent an institutional action, and it cannot lift something the
 * world does not currently offer above something it does. A feed that learns
 * what you linger on and then shows you an unavailable thing first has broken
 * exactly this rule.
 *
 * The organ had no caller anywhere in the estate.
 *
 * Candidates arrive one per line, as:
 *   subject|action|territory|relevance|available|reason
 * Attention arrives the same way through --attention:
 *   subject|signal|weight
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf_standing_address.h"

static void json_text(const char *s) {
    putchar('"');
    for (; s && *s; s++) {
        unsigned char c = (unsigned char)*s;
        if (c == '"' || c == '\\') { putchar('\\'); putchar(c); }
        else if (c < 0x20) printf("\\u%04x", c);
        else putchar(c);
    }
    putchar('"');
}

static int action_by_name(const char *name, BfAction *out) {
    BfAction a;
    for (a = BF_ACTION_WATCH; a <= BF_ACTION_ESCALATE; a++) {
        const char *known = bf_standing_action_name(a);
        if (known && name && !strcmp(known, name)) { *out = a; return 1; }
    }
    return 0;
}

static int territory_by_name(const char *name, BfTerritory *out) {
    BfTerritory t;
    for (t = BF_TERRITORY_LIFE; t <= BF_TERRITORY_WORK; t++) {
        const char *known = bf_standing_territory_name(t);
        if (known && name && !strcmp(known, name)) { *out = t; return 1; }
    }
    return 0;
}

static int signal_by_name(const char *name, BfAttentionSignal *out) {
    BfAttentionSignal s;
    for (s = BF_ATTENTION_WATCHED; s <= BF_ATTENTION_MUTED; s++) {
        const char *known = bf_standing_attention_signal_name(s);
        if (known && name && !strcmp(known, name)) { *out = s; return 1; }
    }
    return 0;
}

static char *field(char **cursor) {
    char *start = *cursor, *bar;
    if (!start) return NULL;
    bar = strchr(start, '|');
    if (bar) { *bar = '\0'; *cursor = bar + 1; } else { *cursor = NULL; }
    return start;
}

int main(int argc, char *argv[]) {
    static BfOpportunity candidates[BF_STANDING_MAX_OPPORTUNITIES];
    static char storage[BF_STANDING_MAX_OPPORTUNITIES][512];
    BfStandingAddress address;
    BfStandingRanking ranking;
    const char *actor = "anonymous", *territory_name = "life", *attention_path = NULL;
    BfTerritory territory;
    size_t count = 0, i;
    char line[600];
    int argi;
    FILE *in = stdin;

    for (argi = 1; argi < argc - 1; argi++) {
        if (!strcmp(argv[argi], "--actor")) actor = argv[argi + 1];
        else if (!strcmp(argv[argi], "--territory")) territory_name = argv[argi + 1];
        else if (!strcmp(argv[argi], "--attention")) attention_path = argv[argi + 1];
        else if (!strcmp(argv[argi], "--candidates")) in = fopen(argv[argi + 1], "r");
    }
    if (!in) { fputs("cannot read candidates\n", stderr); return 2; }
    if (!territory_by_name(territory_name, &territory)) {
        fprintf(stderr, "no such territory: %s\n", territory_name);
        return 2;
    }
    if (bf_standing_address_init(&address, actor, territory, NULL) != BF_STANDING_OK) {
        fputs("{\"error\":\"standing address refused\"}\n", stdout);
        return 1;
    }

    while (count < BF_STANDING_MAX_OPPORTUNITIES && fgets(line, sizeof(line), in)) {
        char *cursor = line, *subject, *action_name, *terr, *relevance, *available, *reason;
        BfAction action;
        BfTerritory here;
        line[strcspn(line, "\r\n")] = '\0';
        if (!line[0] || line[0] == '#') continue;
        snprintf(storage[count], sizeof(storage[count]), "%s", line);
        cursor = storage[count];
        subject = field(&cursor); action_name = field(&cursor); terr = field(&cursor);
        relevance = field(&cursor); available = field(&cursor); reason = field(&cursor);
        if (!subject || !action_name || !terr || !relevance || !available) continue;
        if (!action_by_name(action_name, &action) || !territory_by_name(terr, &here)) {
            /* An undeclared action is not a ranking problem, it is an invented
             * institutional act. It is dropped rather than ranked. */
            fprintf(stderr, "dropped: %s names an action or territory this estate has not declared\n",
                    subject);
            continue;
        }
        candidates[count].subject = subject;
        candidates[count].action = action;
        candidates[count].territory = here;
        candidates[count].relevance = atof(relevance);
        candidates[count].available = atoi(available);
        candidates[count].reason = reason ? reason : "";
        if (bf_standing_opportunity_validate(&candidates[count]) != BF_STANDING_OK) {
            fprintf(stderr, "dropped: %s is not a valid opportunity\n", subject);
            continue;
        }
        count++;
    }
    if (in != stdin) fclose(in);

    if (attention_path) {
        FILE *att = fopen(attention_path, "r");
        while (att && fgets(line, sizeof(line), att)) {
            char *cursor = line, *subject, *signal_name, *weight;
            BfAttentionSignal signal;
            line[strcspn(line, "\r\n")] = '\0';
            if (!line[0] || line[0] == '#') continue;
            subject = field(&cursor); signal_name = field(&cursor); weight = field(&cursor);
            if (!subject || !signal_name || !signal_by_name(signal_name, &signal)) continue;
            bf_standing_observe_attention(&address, subject, signal, weight ? atof(weight) : 1.0);
        }
        if (att) fclose(att);
    }

    memset(&ranking, 0, sizeof(ranking));
    if (bf_standing_compile(&address, candidates, count, &ranking) != BF_STANDING_OK) {
        fputs("{\"error\":\"ranking refused\"}\n", stdout);
        return 1;
    }

    fputs("{\"actor\":", stdout); json_text(actor);
    fputs(",\"territory\":", stdout); json_text(bf_standing_territory_name(territory));
    printf(",\"candidates\":%zu,\"ranked\":[", count);
    for (i = 0; i < ranking.count; i++) {
        const BfOpportunity *o = &candidates[ranking.candidate_indices[i]];
        if (i) putchar(',');
        fputs("{\"subject\":", stdout); json_text(o->subject);
        fputs(",\"action\":", stdout); json_text(bf_standing_action_name(o->action));
        printf(",\"available\":%s", o->available ? "true" : "false");
        fputs(",\"reason\":", stdout); json_text(o->reason);
        putchar('}');
    }
    fputs("],\"cannot_infer\":", stdout);
    json_text("attention reorders candidates and nothing more: it cannot create an "
              "institutional action, and it never lifts an unavailable thing above an "
              "available one. A ranking is recompiled, never a stored feed");
    fputs("}\n", stdout);
    return 0;
}
