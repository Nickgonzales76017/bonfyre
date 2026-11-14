#ifndef BONFYRE_PEER_FACETS_H
#define BONFYRE_PEER_FACETS_H
#include <stddef.h>
#include <sqlite3.h>
/* One subject, many independently chosen contexts. These are private
 * interactions, not a second identity, friendship inference or any grant.
 * Dating has a separate adult+moderation owner and is NEVER auto-enrolled. */
typedef enum {
 BF_PEER_NONE=0, BF_PEER_PASS=1, BF_PEER_INTEREST=2,
 BF_PEER_ACCEPT=3, BF_PEER_BLOCK=4
} BfPeerChoice;
typedef struct {
 char id[32],label[80],description[240],territory[12];
} BfPeerFacet;
typedef struct {
 char subject[192];
 unsigned my_choice_revision;
 BfPeerChoice mine;
 int incoming, outgoing, matched;
 char shared_topics[265];
} BfPeerCandidate;
int bf_peer_store_init(sqlite3 *db);
int bf_peer_facets(sqlite3 *db,BfPeerFacet *out,size_t cap,size_t *count);
/* 0 absent, 1 active, 2 withdrawn, -1 error. */
int bf_peer_status(sqlite3 *db,const char *subject,const char *facet,unsigned *revision);
int bf_peer_join(sqlite3 *db,const char *subject,const char *facet,int enable,unsigned expected);
int bf_peer_choice(sqlite3 *db,const char *subject,const char *facet,
                   const char *target,BfPeerChoice choice,unsigned expected);
int bf_peer_candidates(sqlite3 *db,const char *subject,const char *facet,
                       BfPeerCandidate *out,size_t cap,size_t *count);
/* Optional interests are disclosed only inside the independently joined
 * context; stale participation epochs do not restore old selections. */
int bf_peer_interests_read(sqlite3 *db,const char *subject,const char *facet,
                           char topics[265],unsigned *revision);
int bf_peer_interests_set(sqlite3 *db,const char *subject,const char *facet,
                          const char *topics,unsigned participation_revision,
                          unsigned expected_revision);
typedef struct {long long id,at;char sender[192],body[1001];} BfPeerMessage;
int bf_peer_send(sqlite3 *db,const char *actor,const char *facet,const char *target,
                 const char *nonce,const char *body);
int bf_peer_messages(sqlite3 *db,const char *actor,const char *facet,const char *target,
                     BfPeerMessage *out,size_t cap,size_t *count);
int bf_peer_mutual(sqlite3 *db,const char *left,const char *right,const char *facet);
#endif
