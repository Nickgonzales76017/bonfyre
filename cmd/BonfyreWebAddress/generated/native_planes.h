/* Generated from schemas/control/institution-planes.cue. Do not edit. */
#ifndef BONFYRE_NATIVE_PLANES_H
#define BONFYRE_NATIVE_PLANES_H
#include <stddef.h>

/* `granted` means the plane answers only to a relationship the
 * institution itself established. It is never reachable by wanting it,
 * and must not be shown beside a public plane as though it were. */
typedef struct { const char *plane; const char *label; const char *reach;
                 const char *carries; const char *cannot_infer; } BfInstitutionPlane;

static const BfInstitutionPlane bf_institution_planes[] = {
  {"calendar-ics", "Published calendar", "public", "when things are scheduled to happen, as the institution published them", "a calendar entry says an event was published, never that it happened, that it still stands, or that any particular person is expected at it"},
  {"feed", "News or notices feed", "public", "what the institution chose to announce, in the order it announced it", "an announcement is what was published, not what was decided, and a silent feed is not an absence of news"},
  {"web-page", "Public page", "public", "whatever the institution put on its own site", "a page read once is not a page that still says that, and a staff listing is not a directory anyone consented to be in"},
  {"document", "Published document", "public", "agendas, minutes, menus, handbooks and notices as filed", "a document being public is not permission to republish it whole, and an agenda item is not a decision"},
  {"open-data", "Open data catalogue", "public", "tabular public records the body publishes deliberately", "an open dataset describes a population, never an individual in it, however few rows it has"},
  {"roster", "Roster exchange", "granted", "enrolment, courses and sections, where a relationship has been established", "a roster names who is enrolled; it is never evidence of attendance, performance, or anything about a person beyond enrolment"},
  {"student-record", "Student information system", "granted", "attendance, assignments and notices for a person whose guardian connected it", "connecting a record is permission for one household to read its own, never for this estate to hold, compare or publish it"},
  {"messaging", "Family messaging", "granted", "what the institution sent to a family that connected it", "a message delivered here is a copy shown to its recipient, never correspondence this estate is party to"},
  {"payment", "Fees and payments", "granted", "what is owed and what was paid, where a household connected it", "a balance shown is what the institution stated; this estate holds no money and settles nothing"},
};
#define BF_INSTITUTION_PLANE_COUNT (sizeof(bf_institution_planes) / sizeof(bf_institution_planes[0]))
#endif
