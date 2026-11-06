/* Generated from schemas/control/missing-cells.cue. Do not edit. */
#ifndef BONFYRE_NATIVE_MISSING_CELLS_H
#define BONFYRE_NATIVE_MISSING_CELLS_H
#include <stddef.h>
#include <string.h>

/* `refuses` is as load-bearing as `does`: each of these is the smallest
 * honest version of a cell, not a clone of the product that named it. */
typedef struct { const char *cell, *label, *record, *does, *refuses, *cannot_infer; } BfCellContract;

typedef struct { const char *change, *label, *means; } BfPathChange;

/* `transmitted` is false for every state and the compiler refuses a table
 * where it is not. An outbox with no transport cannot have a state that
 * claims one. */
typedef struct { const char *state, *label; unsigned char transmitted; const char *means; } BfMessageState;

static const BfCellContract bf_cell_contracts[] = {
  {"version", "A snapshot of a namespace", "estate_version", "records the digest of every file under a namespace at one moment, and diffs two such snapshots into added, removed and changed paths", "branching and merging. A snapshot is a reading of what was there, and a merge would have to reconstruct derived material this estate cannot rebuild", "a snapshot is not a backup and not a restore point: the bytes it names may be gone, and nothing here can put them back"},
  {"message", "Something composed for somebody", "outbound_message", "records a message, its intended recipient, and the act somebody would have to perform to send it", "sending. There is no transport here, on purpose: the estate computes what is reachable and stops before anything leaves it", "a held message is not a sent one, not a delivered one, and not a claim the recipient knows it exists"},
  {"signature", "Assent to exact bytes", "assent_signature", "binds one person's recorded assent to one digest, and reports the assent as broken the moment the bytes hash differently", "identity verification and legal effect. It says who this estate recorded as assenting, not who they are or what it obliges", "a signature here is not a legal signature, not notarised, and not evidence the signer read what they assented to"},
  {"stream", "Something happening now", "live_session", "records a session, the heartbeats that keep it live, and what it points at for replay once it ends", "carrying media. No bytes flow through this estate; a session names where something is happening and what was kept", "a live session is not an audience, not a viewer count, and not a claim anybody is watching"},
};
#define BF_CELL_CONTRACT_COUNT (sizeof(bf_cell_contracts) / sizeof(bf_cell_contracts[0]))

static const BfPathChange bf_path_changes[] = {
  {"added", "New here", "the path exists in the later snapshot and not the earlier one"},
  {"removed", "Gone", "the path existed in the earlier snapshot and not the later one"},
  {"changed", "Different bytes", "the path exists in both and its digest differs"},
  {"unchanged", "Same bytes", "the path exists in both with the same digest"},
};
#define BF_PATH_CHANGE_COUNT (sizeof(bf_path_changes) / sizeof(bf_path_changes[0]))

static const BfMessageState bf_message_states[] = {
  {"composed", "Written and held", 0, "the text exists and the recipient is named; nothing has left this estate"},
  {"withheld", "Held back on purpose", 0, "somebody decided it should not go, and the reason is recorded"},
  {"handed_over", "Given to a person to send", 0, "a named person took it to send themselves; the estate still transmitted nothing"},
  {"abandoned", "Dropped", 0, "it will not be sent and is kept only as a record that it was written"},
};
#define BF_MESSAGE_STATE_COUNT (sizeof(bf_message_states) / sizeof(bf_message_states[0]))

static const BfCellContract *bf_cell_contract(const char *name) {
    size_t i;
    if (!name) return NULL;
    for (i = 0; i < BF_CELL_CONTRACT_COUNT; i++)
        if (!strcmp(bf_cell_contracts[i].cell, name)) return &bf_cell_contracts[i];
    return NULL;
}

/* A message state this estate declares, or nothing. A caller inventing a
 * state is a caller inventing a transport. */
static const BfMessageState *bf_message_state(const char *name) {
    size_t i;
    if (!name) return NULL;
    for (i = 0; i < BF_MESSAGE_STATE_COUNT; i++)
        if (!strcmp(bf_message_states[i].state, name)) return &bf_message_states[i];
    return NULL;
}

/* Did anything leave this estate? Always no, read from the table rather
 * than assumed, so the day somebody adds a transport this stops agreeing
 * with them by accident. */
static int bf_message_transmitted(const char *state) {
    const BfMessageState *s = bf_message_state(state);
    return s ? (int)s->transmitted : 0;
}
#endif
