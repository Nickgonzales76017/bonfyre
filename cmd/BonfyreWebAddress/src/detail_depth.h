#ifndef BONFYRE_DETAIL_DEPTH_H
#define BONFYRE_DETAIL_DEPTH_H

#include <stdio.h>

/* Detail depth: where a visitor goes next from any detail page.
 *
 * Every detail page (a piece, a credited maker, a feed, a publisher, the
 * reader a feed is read with, an application's record shape, a topic, a
 * printed name) closes with the other kinds of page the same material
 * already reaches, read from the stores that hold it:
 *
 *   item archive   BONFYRE_ITEM_ARCHIVE_DB    pieces, bylines, kinds
 *   source ledger  BONFYRE_SOURCE_FRONTIER_DB feeds, publishers, readers
 *   app shapes     BONFYRE_FRAPPE_SURFACE_DB  record kinds that exist
 *   detail depth   BONFYRE_DETAIL_DEPTH_DB    facets derived from the
 *                                             retained feed bodies
 *
 * Nothing here fetches, ranks people, or asserts that two equal strings are
 * one person or one subject. */

typedef enum {
    BF_DETAIL_ITEM,      /* a = source, b = key */
    BF_DETAIL_PERSON,    /* a = printed name, b = source */
    BF_DETAIL_CHANNEL,   /* a = exact channel address */
    BF_DETAIL_ROOT,      /* a = publisher host */
    BF_DETAIL_READER,    /* a = protocol the ledger read it as */
    BF_DETAIL_TOPIC,     /* a = topic as printed */
    BF_DETAIL_NAMED,     /* a = name as printed */
    BF_DETAIL_RECORD,    /* a = record-kind slug */
    BF_DETAIL_APP        /* a = application name */
} BfDetailKind;

typedef struct {
    BfDetailKind kind;
    const char *a, *b;
    int desk;            /* 1: the subject-room desk-row dress; 0: world .more */
} BfDetailFocus;

/* The continuation region for one detail page.  Returns rows written. */
int bf_detail_continue(FILE *out, const BfDetailFocus *focus);

/* A derived feed: everything kept under one topic or printed name, with its
 * continuations.  facet is "topic" or "mention".  Returns rows written. */
int bf_detail_facet_page(FILE *out, const char *facet, const char *value, const char *route);

/* A maker named only in a feed body's credit fields, at one source. */
int bf_detail_person_work(FILE *out, const char *who, const char *source);

/* The reader a protocol names, in words a person reads. */
const char *bf_detail_reader_words(const char *protocol);

/* Offline: derive facets from the archive and the retained bodies into out_db.
 * Writes a one-line JSON summary to report. Returns 0 on success. */
int bf_detail_depth_derive(const char *archive, const char *bodies_dir, const char *out_db, FILE *report);

/* Offline: per-source yield before and after the derived facets, as TSV. */
int bf_detail_depth_measure(const char *archive, const char *frontier, const char *depth_db, FILE *out);

#endif
