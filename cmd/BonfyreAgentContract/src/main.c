/* The agent session contract, as something the estate can actually run.
 *
 * A receipt that says an agent did work has to cover the exact event frontier
 * it claims, with evidence from the same sequence and the same origin. A model
 * turn additionally needs the exact provider and model identity plus an
 * explicit verifier observation, because repeated self-attribution is not
 * producer proof: a model saying which model it is, many times, is still the
 * same unverified claim.
 *
 * The organ enforced all of that and had no caller anywhere in the estate.
 *
 * A session arrives as bounded pipe-delimited lines:
 *   session|session_id|agent_profile_ref|root_work_node
 *   event|sequence|kind|origin|work_node|observed_at|provider|model|verifier|provider_session|context_before|context_after|strategy_before|strategy_after|reason|artifact_identity|transport_identity
 * and the receipt frontier as:
 *   receipt|identity|subject|occurrence|status|first_sequence|last_sequence
 *
 * Empty fields are meaningful for the event kinds that do not require them.
 * A model turn requires provider/model/verifier, an external event also needs
 * transport_identity, and compaction/strategy transitions carry their own
 * changed before/after pair plus a reason.  This is intentionally a carrier
 * adapter: it never fabricates an event from a CLI exit code.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf_agent_contract.h"

#define MAX_EVENTS 256
#define MAX_LINE 4096

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

static BfAgentEventKind kind_by_name(const char *name) {
    if (name && !strcmp(name, "model_turn")) return BF_AGENT_EVENT_MODEL_TURN;
    if (name && !strcmp(name, "context_compaction")) return BF_AGENT_EVENT_CONTEXT_COMPACTION;
    if (name && !strcmp(name, "strategy_change")) return BF_AGENT_EVENT_STRATEGY_CHANGE;
    return BF_AGENT_EVENT_UNKNOWN;
}

static BfEvidenceOrigin origin_by_name(const char *name) {
    if (name && !strcmp(name, "subject")) return BF_EVIDENCE_ORIGIN_SUBJECT;
    if (name && !strcmp(name, "host")) return BF_EVIDENCE_ORIGIN_HOST;
    if (name && !strcmp(name, "external")) return BF_EVIDENCE_ORIGIN_EXTERNAL;
    return BF_EVIDENCE_ORIGIN_UNKNOWN;
}

static char *field(char **cursor) {
    char *start = *cursor, *bar;
    if (!start) return NULL;
    bar = strchr(start, '|');
    if (bar) { *bar = '\0'; *cursor = bar + 1; } else { *cursor = NULL; }
    return start;
}

int main(int argc, char *argv[]) {
    static BfAgentEvent events[MAX_EVENTS];
    static BfReceiptEvidence evidence[MAX_EVENTS];
    /* Two copies of every line. The first pass splits `lines` in place, which
     * writes a terminator over each separator, so a second pass reading the
     * same memory would find only the first field. `raw` is never split and is
     * what the events are parsed from. */
    static char lines[MAX_EVENTS * 2][MAX_LINE];
    static char raw[MAX_EVENTS * 2][MAX_LINE];
    BfAgentSession session;
    BfReceiptEnvelope receipt;
    size_t event_count = 0, evidence_count = 0, stored = 0;
    const char *session_id = "", *profile = "", *root = "";
    char line[MAX_LINE + 32];
    FILE *in = stdin;
    int i;
    BfAgentContractStatus session_status, receipt_status, cover_status;

    memset(&session, 0, sizeof(session));
    memset(&receipt, 0, sizeof(receipt));

    for (i = 1; i < argc - 1; i++)
        if (!strcmp(argv[i], "--file")) in = fopen(argv[i + 1], "r");
    if (!in) { fputs("cannot read the session\n", stderr); return 2; }

    while (stored < MAX_EVENTS * 2 && fgets(line, sizeof(line), in)) {
        char *cursor, *what;
        if (strchr(line, '\n') == NULL && !feof(in)) {
            fputs("agent-contract: input line exceeds bounded carrier record\n", stderr);
            if (in != stdin) fclose(in);
            return 2;
        }
        line[strcspn(line, "\r\n")] = '\0';
        if (!line[0] || line[0] == '#') continue;
        snprintf(lines[stored], sizeof(lines[stored]), "%s", line);
        snprintf(raw[stored], sizeof(raw[stored]), "%s", line);
        cursor = lines[stored];
        stored++;
        what = field(&cursor);
        if (!what) continue;
        if (!strcmp(what, "session")) {
            session_id = field(&cursor); profile = field(&cursor); root = field(&cursor);
        } else if (!strcmp(what, "receipt")) {
            receipt.identity = field(&cursor); receipt.subject = field(&cursor);
            receipt.occurrence_ref = field(&cursor); receipt.status = field(&cursor);
            receipt.session_id = session_id;
            { const char *a = field(&cursor), *b = field(&cursor);
              receipt.first_event_sequence = a ? strtoull(a, NULL, 10) : 0;
              receipt.last_event_sequence = b ? strtoull(b, NULL, 10) : 0; }
        }
    }
    if (in != stdin) fclose(in);

    /* Events are read in a second pass, once the session they belong to is
     * known: an event carries its session id, and a first pass that guessed it
     * would be inventing the very binding this contract exists to check. */
    for (i = 0; i < (int)stored; i++) {
        /* Each event is split in its own buffer that outlives the loop: the
         * specs hold pointers into it, and splitting in a reused stack buffer
         * left every one of them dangling at the next iteration. */
        static char parsed[MAX_EVENTS][MAX_LINE];
        char *cursor, *what;
        if (event_count >= MAX_EVENTS) break;
        snprintf(parsed[event_count], sizeof(parsed[event_count]), "%s", raw[i]);
        cursor = parsed[event_count];
        what = field(&cursor);
        if (!what || strcmp(what, "event")) continue;
        if (event_count >= MAX_EVENTS) break;
        {
            BfAgentEvent *e = &events[event_count];
            BfReceiptEvidence *ev = &evidence[evidence_count];
            const char *seq = field(&cursor), *kind = field(&cursor), *origin = field(&cursor);
            const char *node = field(&cursor), *at = field(&cursor);
            const char *provider = field(&cursor), *model = field(&cursor), *verifier = field(&cursor);
            const char *provider_session = field(&cursor);
            const char *context_before = field(&cursor), *context_after = field(&cursor);
            const char *strategy_before = field(&cursor), *strategy_after = field(&cursor);
            const char *reason = field(&cursor), *artifact = field(&cursor), *transport = field(&cursor);
            memset(e, 0, sizeof(*e));
            e->session_id = session_id;
            e->sequence = seq ? strtoull(seq, NULL, 10) : 0;
            e->kind = kind_by_name(kind);
            e->evidence_origin = origin_by_name(origin);
            e->work_node = node ? node : "";
            e->observed_at = at ? at : "";
            e->provider_identity = provider ? provider : "";
            e->model_identity = model ? model : "";
            e->provider_session_ref = provider_session ? provider_session : "";
            e->context_before_ref = context_before ? context_before : "";
            e->context_after_ref = context_after ? context_after : "";
            e->strategy_before_ref = strategy_before ? strategy_before : "";
            e->strategy_after_ref = strategy_after ? strategy_after : "";
            e->reason_ref = reason ? reason : "";
            memset(ev, 0, sizeof(*ev));
            ev->event_sequence = e->sequence;
            ev->evidence_ref = e->work_node;
            ev->origin = e->evidence_origin;
            ev->artifact_identity = artifact && *artifact ? artifact : e->work_node;
            ev->producer_identity = e->provider_identity;
            ev->model_identity = e->model_identity;
            ev->producer_verifier_ref = verifier ? verifier : "";
            ev->transport_identity = transport ? transport : "";
            if (getenv("BONFYRE_AGENT_CONTRACT_TRACE"))
                fprintf(stderr, "[parsed] session=%s seq=%llu kind=%s origin=%s node=%s at=%s\n",
                        e->session_id ? e->session_id : "(null)",
                        (unsigned long long)e->sequence,
                        bf_agent_event_kind_name(e->kind),
                        bf_evidence_origin_name(e->evidence_origin),
                        e->work_node, e->observed_at);
            event_count++; evidence_count++;
        }
    }

    session.session_id = session_id;
    session.agent_profile_ref = profile;
    session.root_work_node = root;
    session.events = events;
    session.event_count = event_count;
    receipt.evidence = evidence;
    receipt.evidence_count = evidence_count;

    session_status = bf_agent_session_validate(&session);
    receipt_status = bf_receipt_envelope_validate(&receipt);
    cover_status = bf_receipt_envelope_covers_session(&receipt, &session);

    fputs("{\"session\":", stdout); json_text(session_id);
    printf(",\"events\":%zu", event_count);
    fputs(",\"session_valid\":", stdout); json_text(bf_agent_contract_status_name(session_status));
    fputs(",\"receipt_valid\":", stdout); json_text(bf_agent_contract_status_name(receipt_status));
    fputs(",\"receipt_covers_session\":", stdout); json_text(bf_agent_contract_status_name(cover_status));
    fputs(",\"cannot_infer\":", stdout);
    json_text("a valid receipt covers the event frontier it claims with same-sequence, "
              "same-origin evidence; it is not proof that the work was good, and a model "
              "naming itself repeatedly is not proof of which producer ran");
    fputs("}\n", stdout);
    return (session_status == BF_AGENT_CONTRACT_OK && receipt_status == BF_AGENT_CONTRACT_OK
            && cover_status == BF_AGENT_CONTRACT_OK) ? 0 : 3;
}
