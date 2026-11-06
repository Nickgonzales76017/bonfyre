#ifndef BF_RESERVATION_H
#define BF_RESERVATION_H

/*
 * ResourceReservation owns a temporal hold.  It intentionally does not fold
 * into ResourceActivation: an activated resource can be unavailable during a
 * requested window, and a held resource is not delivered merely because its
 * hold exists.  The state machine is small because it is an institutional
 * boundary, not a scheduler.
 */

#include <stdio.h>

typedef struct BfReservation BfReservation;

typedef enum BfReservationState {
    BF_RESERVATION_HELD,
    BF_RESERVATION_CONFIRMED,
    BF_RESERVATION_RELEASED,
    BF_RESERVATION_CANCELLED,
    BF_RESERVATION_EXPIRED
} BfReservationState;

typedef struct BfReservationSpec {
    const char *reservation_id;
    const char *resource_id;
    const char *holder;
    const char *purpose;
    const char *starts_at;
    const char *ends_at;
    const char *authority_ref;
    const char *evidence_ref;
} BfReservationSpec;

int bf_reservation_open(const char *database_path, BfReservation **out, FILE *err);
int bf_reservation_open_database(void *sqlite_database, BfReservation **out, FILE *err);
void bf_reservation_close(BfReservation *reservations);

/* A hold is refused when a live hold/confirmation already overlaps the same
 * resource window. The caller still needs AuthorityGraph to decide whether it
 * may request this transition. */
int bf_reservation_hold(BfReservation *reservations, const BfReservationSpec *spec,
                        const char *at);
int bf_reservation_transition(BfReservation *reservations, const char *reservation_id,
                              BfReservationState state, const char *at);

/* Exact predicate query used by inverse compilation.  It never infers a hold
 * from activation or a booking from a quote.  Returns 1/0/-1. */
int bf_reservation_covers(BfReservation *reservations, const char *resource_id,
                          const char *starts_at, const char *ends_at);
const char *bf_reservation_state_name(BfReservationState state);

#endif
