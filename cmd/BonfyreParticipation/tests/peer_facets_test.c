#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "../src/peer_facets.h"
static unsigned rev(sqlite3 *db,const char *who,const char *facet,int expected){
 unsigned n=0;assert(bf_peer_status(db,who,facet,&n)==expected);return n;
}
static void crowded_feed(void){
 sqlite3 *db=NULL;assert(sqlite3_open(":memory:",&db)==SQLITE_OK);assert(bf_peer_store_init(db));
 assert(bf_peer_join(db,"person:owner","art",1,0));
 for(int i=0;i<300;i++){char name[40];snprintf(name,sizeof(name),"person:member-%03d",i);assert(bf_peer_join(db,name,"art",1,0));}
 assert(bf_peer_join(db,"person:z-inviter","art",1,0));
 assert(bf_peer_choice(db,"person:z-inviter","art","person:owner",BF_PEER_INTEREST,0));
 BfPeerCandidate rows[24];size_t n=0;assert(bf_peer_candidates(db,"person:owner","art",rows,24,&n));
 assert(n==24&&!strcmp(rows[0].subject,"person:z-inviter")&&rows[0].incoming);
 assert(bf_peer_choice(db,"person:owner","art","person:z-inviter",BF_PEER_BLOCK,0));
 assert(bf_peer_candidates(db,"person:owner","art",rows,24,&n)&&n==24);
 for(size_t i=0;i<n;i++)assert(strcmp(rows[i].subject,"person:z-inviter"));
 sqlite3_close(db);
}
static void interests(void){
 sqlite3 *db=NULL;assert(sqlite3_open(":memory:",&db)==SQLITE_OK);assert(bf_peer_store_init(db));
 assert(bf_peer_join(db,"person:alice","personal",1,0));assert(bf_peer_join(db,"person:bob","personal",1,0));
 assert(bf_peer_interests_set(db,"person:alice","personal","Hiking, Board Games",1,0));
 assert(bf_peer_interests_set(db,"person:bob","personal","hiking, music",1,0));
 char topics[265];unsigned rev=0;assert(bf_peer_interests_read(db,"person:alice","personal",topics,&rev));
 assert(rev==1&&!strcmp(topics,"hiking,board-games"));
 assert(!bf_peer_interests_set(db,"person:alice","personal","hiking,hiking",1,1));
 assert(!bf_peer_interests_set(db,"person:alice","personal","hiking,<script>",1,1));
 assert(!bf_peer_interests_set(db,"person:alice","personal","music",1,0));
 assert(!bf_peer_interests_set(db,"person:alice","art","music",1,0));
 BfPeerCandidate rows[4];size_t n=0;assert(bf_peer_candidates(db,"person:alice","personal",rows,4,&n));
 assert(n==1&&!strcmp(rows[0].shared_topics,"hiking"));
 assert(bf_peer_join(db,"person:bob","personal",0,1));assert(bf_peer_join(db,"person:bob","personal",1,2));
 assert(bf_peer_interests_read(db,"person:bob","personal",topics,&rev)&&rev==1&&!topics[0]);
 assert(bf_peer_candidates(db,"person:alice","personal",rows,4,&n)&&n==1&&!rows[0].shared_topics[0]);
 assert(!bf_peer_interests_set(db,"person:bob","personal","hiking",1,1));
 assert(bf_peer_interests_set(db,"person:bob","personal","hiking",3,1));
 assert(bf_peer_candidates(db,"person:alice","personal",rows,4,&n)&&n==1&&!strcmp(rows[0].shared_topics,"hiking"));
 sqlite3_close(db);
}
static void conversations(void){
 sqlite3 *db=NULL;assert(sqlite3_open(":memory:",&db)==SQLITE_OK);assert(bf_peer_store_init(db));
 assert(bf_peer_join(db,"person:alice","personal",1,0));assert(bf_peer_join(db,"person:bob","personal",1,0));
 const char *nonce="0123456789abcdef0123456789abcdef";
 assert(!bf_peer_send(db,"person:alice","personal","person:bob",nonce,"Hello"));
 assert(bf_peer_choice(db,"person:alice","personal","person:bob",BF_PEER_INTEREST,0));
 assert(bf_peer_choice(db,"person:bob","personal","person:alice",BF_PEER_ACCEPT,0));
 assert(!bf_peer_send(db,"person:alice","personal","person:bob",nonce,"Hello"));
 assert(bf_peer_choice(db,"person:alice","personal","person:bob",BF_PEER_ACCEPT,1));
 assert(bf_peer_send(db,"person:alice","personal","person:bob",nonce,"Hello"));
 assert(bf_peer_send(db,"person:alice","personal","person:bob",nonce,"Hello"));
 assert(!bf_peer_send(db,"person:alice","personal","person:bob",nonce,"Changed"));
 BfPeerMessage messages[10];size_t count=0;
 assert(bf_peer_messages(db,"person:bob","personal","person:alice",messages,10,&count)&&count==1&&!strcmp(messages[0].body,"Hello"));
 assert(!bf_peer_messages(db,"person:bob","art","person:alice",messages,10,&count));
 assert(bf_peer_choice(db,"person:bob","personal","person:alice",BF_PEER_NONE,1));
 assert(!bf_peer_messages(db,"person:alice","personal","person:bob",messages,10,&count));
 assert(bf_peer_choice(db,"person:bob","personal","person:alice",BF_PEER_ACCEPT,2));
 assert(bf_peer_messages(db,"person:bob","personal","person:alice",messages,10,&count)&&count==0);
 assert(bf_peer_choice(db,"person:bob","personal","person:alice",BF_PEER_BLOCK,3));
 assert(!bf_peer_messages(db,"person:alice","personal","person:bob",messages,10,&count));
 assert(!bf_peer_send(db,"person:alice","personal","person:bob","1123456789abcdef0123456789abcdef","Blocked"));
 sqlite3_close(db);
}
int main(void){
 crowded_feed();interests();conversations();
 sqlite3 *db=NULL;assert(sqlite3_open(":memory:",&db)==SQLITE_OK);
 assert(bf_peer_store_init(db));
 BfPeerFacet facets[8];size_t count=0;
 assert(bf_peer_facets(db,facets,8,&count)&&count==4);
 assert(!strcmp(facets[1].id,"art")&&!strcmp(facets[2].id,"civic"));
 assert(rev(db,"person:alice","personal",0)==0);
 assert(!bf_peer_join(db,"person:alice","fictional",1,0));
 assert(!bf_peer_join(db,"person:alice","dating",1,0));
 assert(!bf_peer_join(db,"organization:alice","art",1,0));
 assert(bf_peer_join(db,"person:alice","art",1,0));
 assert(!bf_peer_join(db,"person:alice","art",1,0));
 assert(bf_peer_join(db,"person:bob","art",1,0));
 assert(bf_peer_join(db,"person:alice","civic",1,0));
 assert(bf_peer_join(db,"person:bob","civic",1,0));
 assert(!bf_peer_mutual(db,"person:alice","person:bob","art"));
 assert(!bf_peer_choice(db,"person:alice","art","person:alice",BF_PEER_INTEREST,0));
 assert(!bf_peer_choice(db,"person:alice","art","person:bob",BF_PEER_ACCEPT,0));
 assert(bf_peer_choice(db,"person:alice","art","person:bob",BF_PEER_INTEREST,0));
 BfPeerCandidate row[4];size_t n=0;
 assert(bf_peer_candidates(db,"person:bob","art",row,4,&n));
 assert(n==1&&row[0].incoming&&row[0].my_choice_revision==0);
 assert(bf_peer_choice(db,"person:bob","art","person:alice",BF_PEER_ACCEPT,0));
 assert(!bf_peer_mutual(db,"person:alice","person:bob","art"));
 assert(!bf_peer_mutual(db,"person:alice","person:bob","civic"));
 assert(bf_peer_choice(db,"person:alice","art","person:bob",BF_PEER_ACCEPT,1));
 assert(bf_peer_mutual(db,"person:alice","person:bob","art"));
 assert(!bf_peer_mutual(db,"person:alice","person:bob","civic"));
 assert(bf_peer_join(db,"person:alice","art",0,1));
 assert(!bf_peer_mutual(db,"person:alice","person:bob","art"));
 assert(!bf_peer_candidates(db,"person:alice","art",row,4,&n));
 assert(bf_peer_join(db,"person:alice","art",1,2));
 assert(!bf_peer_mutual(db,"person:alice","person:bob","art"));
 assert(bf_peer_choice(db,"person:alice","art","person:bob",BF_PEER_INTEREST,2));
 assert(!bf_peer_choice(db,"person:bob","art","person:alice",BF_PEER_ACCEPT,0));
 assert(bf_peer_choice(db,"person:bob","art","person:alice",BF_PEER_ACCEPT,1));
 assert(bf_peer_choice(db,"person:alice","art","person:bob",BF_PEER_ACCEPT,3));
 assert(bf_peer_mutual(db,"person:alice","person:bob","art"));
 assert(bf_peer_choice(db,"person:bob","civic","person:alice",BF_PEER_BLOCK,0));
 assert(!bf_peer_mutual(db,"person:alice","person:bob","art"));
 assert(!bf_peer_candidates(db,"person:bob","art",row,4,&n)||n==0);
 assert(!bf_peer_choice(db,"person:alice","art","person:bob",BF_PEER_INTEREST,4));
 assert(bf_peer_join(db,"person:bob","civic",0,1));
 assert(!bf_peer_mutual(db,"person:alice","person:bob","civic"));
 assert(bf_peer_join(db,"person:bob","civic",1,2));
 assert(!bf_peer_mutual(db,"person:alice","person:bob","art")); /* block survives epoch */
 assert(sqlite3_exec(db,"SELECT * FROM peer_facet",NULL,NULL,NULL)==SQLITE_OK);
 sqlite3_close(db);
 puts("peer facets PASS: separate contexts, reciprocal consent, CAS, withdrawal, global block, no dating inference");
 return 0;
}
