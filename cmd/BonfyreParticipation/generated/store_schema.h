/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREPARTICIPATION_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREPARTICIPATION_H
/* cmd/BonfyreParticipation/src/dating.c */
#define BF_STORE_PARTICIPATION_DATING_REQUEST \
    "CREATE TABLE IF NOT EXISTS dating_request(subject TEXT PRIMARY KEY,revision INTEGER NOT NULL,state TEXT NOT NULL,updated_at INTEGER NOT NULL,CHECK(state IN ('requested','withdrawn')))"
/* cmd/BonfyreParticipation/src/dating.c */
#define BF_STORE_PARTICIPATION_DATING_VERIFICATION \
    "CREATE TABLE IF NOT EXISTS dating_verification(subject TEXT PRIMARY KEY,age INTEGER NOT NULL,eligibility_witness TEXT NOT NULL,moderation_witness TEXT NOT NULL,issued_by TEXT NOT NULL,state TEXT NOT NULL,valid_until INTEGER NOT NULL)"
/* cmd/BonfyreParticipation/src/dating.c */
#define BF_STORE_PARTICIPATION_DATING_PERSON \
    "CREATE TABLE IF NOT EXISTS dating_person(subject TEXT PRIMARY KEY,revision INTEGER NOT NULL,age INTEGER NOT NULL,min_age INTEGER NOT NULL,max_age INTEGER NOT NULL,active INTEGER NOT NULL,intro INTEGER NOT NULL,eligibility_witness TEXT NOT NULL,moderation_witness TEXT NOT NULL)"
/* cmd/BonfyreParticipation/src/dating.c */
#define BF_STORE_PARTICIPATION_DATING_CHOICE \
    "CREATE TABLE IF NOT EXISTS dating_choice(actor TEXT NOT NULL,target TEXT NOT NULL,choice INTEGER NOT NULL,revision INTEGER NOT NULL,actor_rev INTEGER NOT NULL,target_rev INTEGER NOT NULL,PRIMARY KEY(actor,target),CHECK(actor<>target),CHECK(choice BETWEEN 0 AND 4))"
/* cmd/BonfyreParticipation/src/dating.c */
#define BF_STORE_PARTICIPATION_DATING_ANSWER \
    "CREATE TABLE IF NOT EXISTS dating_answer(subject TEXT NOT NULL,question INTEGER NOT NULL,answer INTEGER NOT NULL,importance INTEGER NOT NULL,participant_rev INTEGER NOT NULL,PRIMARY KEY(subject,question),CHECK(question BETWEEN 1 AND 4),CHECK(answer IN (-1,1)),CHECK(importance BETWEEN 1 AND 5))"
/* cmd/BonfyreParticipation/src/dating.c */
#define BF_STORE_PARTICIPATION_DATING_MESSAGE \
    "CREATE TABLE IF NOT EXISTS dating_message(id INTEGER PRIMARY KEY AUTOINCREMENT,sender TEXT NOT NULL,recipient TEXT NOT NULL,nonce TEXT NOT NULL,body TEXT NOT NULL,sent_at INTEGER NOT NULL,sender_rev INTEGER NOT NULL,recipient_rev INTEGER NOT NULL,UNIQUE(sender,nonce),CHECK(sender<>recipient))"
/* cmd/BonfyreParticipation/src/dating.c */
#define BF_STORE_PARTICIPATION_DATING_MESSAGE_LOOKUP \
    "CREATE INDEX IF NOT EXISTS dating_message_lookup ON dating_message(recipient,sender,id)"
/* cmd/BonfyreParticipation/src/dating.c */
#define BF_STORE_PARTICIPATION_DATING_PLAN_DETAIL \
    "CREATE TABLE IF NOT EXISTS dating_plan_detail(plan_key TEXT PRIMARY KEY,activity TEXT NOT NULL,meeting_context TEXT NOT NULL)"
/* cmd/BonfyreParticipation/src/dating.c */
#define BF_STORE_PARTICIPATION_DATING_PLAN \
    "CREATE TABLE IF NOT EXISTS dating_plan(plan_key TEXT PRIMARY KEY,proposer TEXT NOT NULL,partner TEXT NOT NULL,revision INTEGER NOT NULL,total_cents INTEGER NOT NULL,proposer_rev INTEGER NOT NULL,partner_rev INTEGER NOT NULL,yes_proposer INTEGER NOT NULL DEFAULT 0,yes_partner INTEGER NOT NULL DEFAULT 0)"
/* cmd/BonfyreParticipation/src/dating.c */
#define BF_STORE_PARTICIPATION_PEER_BLOCK \
    "CREATE TABLE IF NOT EXISTS peer_block(subject TEXT NOT NULL,target TEXT NOT NULL,PRIMARY KEY(subject,target),CHECK(subject<>target))"
/* cmd/BonfyreParticipation/src/peer_facets.c */
#define BF_STORE_PARTICIPATION_PEER_FACET \
    "CREATE TABLE IF NOT EXISTS peer_facet(id TEXT PRIMARY KEY,label TEXT NOT NULL,description TEXT NOT NULL,territory TEXT NOT NULL,owner TEXT NOT NULL,scope TEXT NOT NULL,active INTEGER NOT NULL CHECK(active IN (0,1)))"
/* cmd/BonfyreParticipation/src/peer_facets.c */
#define BF_STORE_PARTICIPATION_PEER_PARTICIPATION \
    "CREATE TABLE IF NOT EXISTS peer_participation(subject TEXT NOT NULL,facet TEXT NOT NULL,revision INTEGER NOT NULL,state TEXT NOT NULL CHECK(state IN ('active','withdrawn')),PRIMARY KEY(subject,facet))"
/* cmd/BonfyreParticipation/src/peer_facets.c */
#define BF_STORE_PARTICIPATION_PEER_PARTICIPATION_FEED \
    "CREATE INDEX IF NOT EXISTS peer_participation_feed ON peer_participation(facet,state,subject)"
/* cmd/BonfyreParticipation/src/peer_facets.c */
#define BF_STORE_PARTICIPATION_PEER_INTERESTS \
    "CREATE TABLE IF NOT EXISTS peer_interests(subject TEXT NOT NULL,facet TEXT NOT NULL,participation_revision INTEGER NOT NULL,revision INTEGER NOT NULL,topics TEXT NOT NULL,PRIMARY KEY(subject,facet))"
/* cmd/BonfyreParticipation/src/peer_facets.c */
#define BF_STORE_PARTICIPATION_PEER_CHOICE \
    "CREATE TABLE IF NOT EXISTS peer_choice(subject TEXT NOT NULL,target TEXT NOT NULL,facet TEXT NOT NULL,choice INTEGER NOT NULL,revision INTEGER NOT NULL,subject_rev INTEGER NOT NULL,target_rev INTEGER NOT NULL,PRIMARY KEY(subject,target,facet),CHECK(subject<>target),CHECK(choice BETWEEN 0 AND 4))"
/* cmd/BonfyreParticipation/src/peer_facets.c */
#define BF_STORE_PARTICIPATION_PEER_MESSAGE \
    "CREATE TABLE IF NOT EXISTS peer_message(id INTEGER PRIMARY KEY,facet TEXT NOT NULL,sender TEXT NOT NULL,recipient TEXT NOT NULL,nonce TEXT NOT NULL,body TEXT NOT NULL,sent_at INTEGER NOT NULL,sender_epoch INTEGER NOT NULL,recipient_epoch INTEGER NOT NULL,sender_choice_revision INTEGER NOT NULL,recipient_choice_revision INTEGER NOT NULL,UNIQUE(sender,nonce),CHECK(sender<>recipient))"
/* cmd/BonfyreParticipation/src/peer_facets.c */
#define BF_STORE_PARTICIPATION_PEER_MESSAGE_THREAD \
    "CREATE INDEX IF NOT EXISTS peer_message_thread ON peer_message(facet,sender,recipient,id)"
/* cmd/BonfyreParticipation/src/peer_facets.c */
#define BF_STORE_PARTICIPATION_PEER_BLOCK_2 \
    "CREATE TABLE IF NOT EXISTS peer_block(subject TEXT NOT NULL,target TEXT NOT NULL,PRIMARY KEY(subject,target),CHECK(subject<>target))"
/* cmd/BonfyreParticipation/src/evidence.c */
#define BF_STORE_PARTICIPATION_PARTICIPATION_FACT \
    "CREATE TABLE participation_fact(occurrence TEXT NOT NULL,axis TEXT NOT NULL,owner TEXT NOT NULL,state TEXT NOT NULL);" \
    "CREATE INDEX participation_fact_occurrence ON participation_fact(occurrence,axis);" \
    "CREATE TABLE participation_edge(occurrence TEXT NOT NULL,source TEXT NOT NULL,target TEXT NOT NULL,UNIQUE(occurrence,source,target));"
#endif
