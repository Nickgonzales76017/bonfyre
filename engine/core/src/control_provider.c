/*
 * Provider health as a fold over append-only observations.
 *
 * Ported from 10-Code/BonfyreControlPlane/provider_state.py. Conformance
 * vectors: tests/conformance/control/vectors/provider.
 */

#include "bf_control.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MS_PER_MINUTE 60000LL
#define MS_PER_HOUR (60LL * MS_PER_MINUTE)
#define MS_PER_DAY (24LL * MS_PER_HOUR)

/* Consecutive-failure backoff, in minutes. */
static const int64_t kTransientBackoffMinutes[] = { 1, 2, 5, 15, 30 };
static const size_t kTransientBackoffCount =
    sizeof(kTransientBackoffMinutes) / sizeof(kTransientBackoffMinutes[0]);

static int compare_observations(const void *left, const void *right)
{
    const BfProviderObservation *a = (const BfProviderObservation *)left;
    const BfProviderObservation *b = (const BfProviderObservation *)right;
    if (a->observed_at_ms < b->observed_at_ms) return -1;
    if (a->observed_at_ms > b->observed_at_ms) return 1;
    if ((int)a->kind < (int)b->kind) return -1;
    if ((int)a->kind > (int)b->kind) return 1;
    return 0;
}

BfProviderState bf_provider_fold(const BfProviderObservation *observations,
                                 size_t count, int64_t now_ms)
{
    BfProviderState state;
    memset(&state, 0, sizeof(state));
    state.status = BF_PROVIDER_READY;

    BfProviderObservation stack_copy[64];
    BfProviderObservation *ordered = stack_copy;
    BfProviderObservation *heap_copy = NULL;

    if (count > sizeof(stack_copy) / sizeof(stack_copy[0])) {
        heap_copy = (BfProviderObservation *)malloc(count * sizeof(*heap_copy));
        if (heap_copy == NULL) {
            /* Refusing to guess is safer than folding a partial history: an
             * unknown provider is treated as paused rather than ready. */
            state.status = BF_PROVIDER_PAUSED;
            return state;
        }
        ordered = heap_copy;
    }
    if (count > 0) {
        memcpy(ordered, observations, count * sizeof(*ordered));
        qsort(ordered, count, sizeof(*ordered), compare_observations);
    }

    int64_t transient_until = 0;
    int64_t hard_until = 0;
    int paused = 0;

    for (size_t index = 0; index < count; index++) {
        const BfProviderObservation *observation = &ordered[index];
        switch (observation->kind) {
        case BF_PROVIDER_SUCCESS:
            state.consecutive_failures = 0;
            state.last_success_at_ms = observation->observed_at_ms;
            transient_until = 0;
            /* Deliberately does not clear hard_until: one served response is
             * not evidence that capacity returned. */
            break;

        case BF_PROVIDER_TRANSIENT_FAILURE: {
            state.consecutive_failures++;
            size_t slot = (size_t)state.consecutive_failures;
            if (slot > kTransientBackoffCount) slot = kTransientBackoffCount;
            transient_until = observation->observed_at_ms +
                              kTransientBackoffMinutes[slot - 1] * MS_PER_MINUTE;
            break;
        }

        case BF_PROVIDER_HARD_CAPACITY: {
            state.consecutive_failures++;
            state.hard_capacity_hits++;
            int64_t candidate = observation->reset_at_ms > 0
                                    ? observation->reset_at_ms
                                    : observation->observed_at_ms + MS_PER_DAY;
            /* Extends, never shortens. */
            if (candidate > hard_until) hard_until = candidate;
            break;
        }

        case BF_PROVIDER_MANUAL_PAUSE:
            paused = 1;
            break;

        case BF_PROVIDER_MANUAL_RESUME:
            paused = 0;
            hard_until = 0;
            transient_until = 0;
            state.consecutive_failures = 0;
            break;
        }
    }

    free(heap_copy);

    if (paused) {
        state.status = BF_PROVIDER_PAUSED;
        state.circuit_until_ms = 0;
        return state;
    }
    if (hard_until > 0 && now_ms < hard_until) {
        state.status = BF_PROVIDER_CAPACITY_EXHAUSTED;
        state.circuit_until_ms = hard_until;
        return state;
    }
    if (transient_until > 0 && now_ms < transient_until) {
        state.status = BF_PROVIDER_COOLING;
        state.circuit_until_ms = transient_until;
        return state;
    }
    state.status = BF_PROVIDER_READY;
    state.circuit_until_ms = 0;
    return state;
}

int bf_provider_available(const BfProviderState *state, int64_t now_ms)
{
    if (state == NULL) return 0;
    if (state->status == BF_PROVIDER_PAUSED) return 0;
    if (state->circuit_until_ms == 0) return 1;
    return now_ms >= state->circuit_until_ms;
}

const char *bf_provider_status_name(BfProviderStatus status)
{
    switch (status) {
    case BF_PROVIDER_READY: return "ready";
    case BF_PROVIDER_COOLING: return "cooling";
    case BF_PROVIDER_CAPACITY_EXHAUSTED: return "capacity_exhausted";
    case BF_PROVIDER_PAUSED: return "manual_pause";
    }
    return "unknown";
}

/* ------------------------------------------------------------- classification */

static int contains_fold(const char *haystack, const char *needle)
{
    size_t needle_length = strlen(needle);
    if (needle_length == 0) return 1;
    for (const char *cursor = haystack; *cursor != '\0'; cursor++) {
        size_t index = 0;
        while (index < needle_length &&
               tolower((unsigned char)cursor[index]) ==
                   tolower((unsigned char)needle[index])) {
            index++;
        }
        if (index == needle_length) return 1;
    }
    return 0;
}

int bf_provider_is_hard_capacity(const char *text)
{
    if (text == NULL) return 0;
    /* "usage limit" rather than "usage limit reached": Run 6's matcher looked
     * for the latter while Codex actually says "You've hit your usage limit". */
    static const char *const markers[] = {
        "usage limit",     "out of credits",  "no credits remaining",
        "purchase more credits", "quota exhausted", "insufficient_quota",
        "session limit",   "weekly limit",    "capacity limit"
    };
    for (size_t index = 0; index < sizeof(markers) / sizeof(markers[0]); index++) {
        if (contains_fold(text, markers[index])) return 1;
    }
    return 0;
}

static const char *const kMonthNames[] = {
    "january", "february", "march",     "april",   "may",      "june",
    "july",    "august",   "september", "october", "november", "december"
};

static int month_from_prefix(const char *text, size_t length)
{
    if (length < 3) return 0;
    for (int index = 0; index < 12; index++) {
        size_t position = 0;
        while (position < length && kMonthNames[index][position] != '\0' &&
               tolower((unsigned char)text[position]) == kMonthNames[index][position]) {
            position++;
        }
        if (position == length) return index + 1;
    }
    return 0;
}

/* Days from the civil epoch. Howard Hinnant's algorithm. */
static int64_t days_from_civil(int64_t year, int64_t month, int64_t day)
{
    year -= month <= 2;
    const int64_t era = (year >= 0 ? year : year - 399) / 400;
    const int64_t year_of_era = year - era * 400;
    const int64_t day_of_year =
        (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
    const int64_t day_of_era =
        year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    return era * 146097 + day_of_era - 719468;
}

static int64_t civil_to_ms(int64_t year, int64_t month, int64_t day, int64_t hour,
                           int64_t minute)
{
    return ((days_from_civil(year, month, day) * 24 + hour) * 60 + minute) * 60000LL;
}

int64_t bf_provider_parse_reset(const char *text)
{
    if (text == NULL) return 0;

    /* ISO first: 2026-08-19T22:53 or 2026-08-19 22:53 */
    for (const char *cursor = text; *cursor != '\0'; cursor++) {
        if (!isdigit((unsigned char)cursor[0])) continue;
        int year, month, day, hour, minute;
        char separator;
        if (sscanf(cursor, "%4d-%2d-%2d%c%2d:%2d", &year, &month, &day, &separator,
                   &hour, &minute) == 6 &&
            (separator == 'T' || separator == ' ')) {
            return civil_to_ms(year, month, day, hour, minute);
        }
        break;
    }

    /* Absolute with an optional ordinal suffix: "Aug 19th, 2026 10:53 PM".
     * The suffix is what broke a naive month/day regex during the port. */
    for (const char *cursor = text; *cursor != '\0'; cursor++) {
        if (!isalpha((unsigned char)*cursor)) continue;
        if (cursor != text && isalpha((unsigned char)cursor[-1])) continue;

        size_t name_length = 0;
        while (isalpha((unsigned char)cursor[name_length])) name_length++;
        int month = month_from_prefix(cursor, name_length);
        if (month == 0) continue;

        const char *rest = cursor + name_length;
        while (*rest == '.' || *rest == ' ') rest++;
        if (!isdigit((unsigned char)*rest)) continue;

        int day = (int)strtol(rest, (char **)&rest, 10);
        while (isalpha((unsigned char)*rest)) rest++; /* st nd rd th */
        while (*rest == ',' || *rest == ' ') rest++;
        if (!isdigit((unsigned char)*rest)) continue;

        int year = (int)strtol(rest, (char **)&rest, 10);
        while (*rest == ',' || *rest == ' ') rest++;

        int hour = 0, minute = 0;
        if (isdigit((unsigned char)*rest)) {
            hour = (int)strtol(rest, (char **)&rest, 10);
            if (*rest == ':') {
                rest++;
                minute = (int)strtol(rest, (char **)&rest, 10);
            }
            while (*rest == ' ') rest++;
            if (tolower((unsigned char)rest[0]) == 'p' &&
                tolower((unsigned char)rest[1]) == 'm' && hour != 12) {
                hour += 12;
            } else if (tolower((unsigned char)rest[0]) == 'a' &&
                       tolower((unsigned char)rest[1]) == 'm' && hour == 12) {
                hour = 0;
            }
        }
        if (day < 1 || day > 31 || year < 1970) continue;
        return civil_to_ms(year, month, day, hour, minute);
    }

    return 0;
}
