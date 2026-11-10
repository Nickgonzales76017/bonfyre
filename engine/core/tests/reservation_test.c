#include "bf_reservation.h"

#include <stdio.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "reservation_test:%d: %s\n", __LINE__, #x); return 1; } } while (0)

static BfReservationSpec spec(const char *id, const char *start, const char *end) {
    BfReservationSpec value = {0};
    value.reservation_id = id; value.resource_id = "equipment:pa";
    value.holder = "event:93"; value.purpose = "event-ready";
    value.starts_at = start; value.ends_at = end;
    value.authority_ref = "grant:reserve"; value.evidence_ref = "receipt:hold";
    return value;
}

int main(void) {
    BfReservation *reservations = NULL;
    BfReservationSpec first = spec("hold:one", "2026-09-18T18:00:00Z", "2026-09-19T02:00:00Z");
    BfReservationSpec overlap = spec("hold:two", "2026-09-18T20:00:00Z", "2026-09-19T03:00:00Z");
    BfReservationSpec adjacent = spec("hold:three", "2026-09-19T02:00:00Z", "2026-09-19T03:00:00Z");
    CHECK(bf_reservation_open(":memory:", &reservations, stderr) == 0);
    CHECK(bf_reservation_hold(reservations, &first, "2026-09-01T00:00:00Z") == 1);
    CHECK(bf_reservation_covers(reservations, "equipment:pa", first.starts_at, first.ends_at) == 1);
    CHECK(bf_reservation_hold(reservations, &overlap, "2026-09-01T00:00:00Z") == 0);
    CHECK(bf_reservation_hold(reservations, &adjacent, "2026-09-01T00:00:00Z") == 1);
    CHECK(bf_reservation_transition(reservations, "hold:one", BF_RESERVATION_CANCELLED, "2026-09-02T00:00:00Z") == 1);
    CHECK(bf_reservation_covers(reservations, "equipment:pa", first.starts_at, first.ends_at) == 0);
    bf_reservation_close(reservations);
    puts("reservation_test: PASS");
    return 0;
}
