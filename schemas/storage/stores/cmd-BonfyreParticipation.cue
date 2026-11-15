package stores

// cmd/BonfyreParticipation: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreParticipation/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreParticipation": {
	fragments: [
		{
			macro:  "BF_STORE_PARTICIPATION_DATING_REQUEST"
			source: "cmd/BonfyreParticipation/src/dating.c"
			statements: [
				{kind: "table", name: "dating_request", sql: "CREATE TABLE IF NOT EXISTS dating_request(subject TEXT PRIMARY KEY,revision INTEGER NOT NULL,state TEXT NOT NULL,updated_at INTEGER NOT NULL,CHECK(state IN ('requested','withdrawn')))"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_DATING_VERIFICATION"
			source: "cmd/BonfyreParticipation/src/dating.c"
			statements: [
				{kind: "table", name: "dating_verification", sql: "CREATE TABLE IF NOT EXISTS dating_verification(subject TEXT PRIMARY KEY,age INTEGER NOT NULL,eligibility_witness TEXT NOT NULL,moderation_witness TEXT NOT NULL,issued_by TEXT NOT NULL,state TEXT NOT NULL,valid_until INTEGER NOT NULL)"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_DATING_PERSON"
			source: "cmd/BonfyreParticipation/src/dating.c"
			statements: [
				{kind: "table", name: "dating_person", sql: "CREATE TABLE IF NOT EXISTS dating_person(subject TEXT PRIMARY KEY,revision INTEGER NOT NULL,age INTEGER NOT NULL,min_age INTEGER NOT NULL,max_age INTEGER NOT NULL,active INTEGER NOT NULL,intro INTEGER NOT NULL,eligibility_witness TEXT NOT NULL,moderation_witness TEXT NOT NULL)"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_DATING_CHOICE"
			source: "cmd/BonfyreParticipation/src/dating.c"
			statements: [
				{kind: "table", name: "dating_choice", sql: "CREATE TABLE IF NOT EXISTS dating_choice(actor TEXT NOT NULL,target TEXT NOT NULL,choice INTEGER NOT NULL,revision INTEGER NOT NULL,actor_rev INTEGER NOT NULL,target_rev INTEGER NOT NULL,PRIMARY KEY(actor,target),CHECK(actor<>target),CHECK(choice BETWEEN 0 AND 4))"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_DATING_ANSWER"
			source: "cmd/BonfyreParticipation/src/dating.c"
			statements: [
				{kind: "table", name: "dating_answer", sql: "CREATE TABLE IF NOT EXISTS dating_answer(subject TEXT NOT NULL,question INTEGER NOT NULL,answer INTEGER NOT NULL,importance INTEGER NOT NULL,participant_rev INTEGER NOT NULL,PRIMARY KEY(subject,question),CHECK(question BETWEEN 1 AND 4),CHECK(answer IN (-1,1)),CHECK(importance BETWEEN 1 AND 5))"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_DATING_MESSAGE"
			source: "cmd/BonfyreParticipation/src/dating.c"
			statements: [
				{kind: "table", name: "dating_message", sql: "CREATE TABLE IF NOT EXISTS dating_message(id INTEGER PRIMARY KEY AUTOINCREMENT,sender TEXT NOT NULL,recipient TEXT NOT NULL,nonce TEXT NOT NULL,body TEXT NOT NULL,sent_at INTEGER NOT NULL,sender_rev INTEGER NOT NULL,recipient_rev INTEGER NOT NULL,UNIQUE(sender,nonce),CHECK(sender<>recipient))"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_DATING_MESSAGE_LOOKUP"
			source: "cmd/BonfyreParticipation/src/dating.c"
			statements: [
				{kind: "index", name: "dating_message_lookup", sql: "CREATE INDEX IF NOT EXISTS dating_message_lookup ON dating_message(recipient,sender,id)"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_DATING_PLAN_DETAIL"
			source: "cmd/BonfyreParticipation/src/dating.c"
			statements: [
				{kind: "table", name: "dating_plan_detail", sql: "CREATE TABLE IF NOT EXISTS dating_plan_detail(plan_key TEXT PRIMARY KEY,activity TEXT NOT NULL,meeting_context TEXT NOT NULL)"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_DATING_PLAN"
			source: "cmd/BonfyreParticipation/src/dating.c"
			statements: [
				{kind: "table", name: "dating_plan", sql: "CREATE TABLE IF NOT EXISTS dating_plan(plan_key TEXT PRIMARY KEY,proposer TEXT NOT NULL,partner TEXT NOT NULL,revision INTEGER NOT NULL,total_cents INTEGER NOT NULL,proposer_rev INTEGER NOT NULL,partner_rev INTEGER NOT NULL,yes_proposer INTEGER NOT NULL DEFAULT 0,yes_partner INTEGER NOT NULL DEFAULT 0)"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_PEER_BLOCK"
			source: "cmd/BonfyreParticipation/src/dating.c"
			statements: [
				{kind: "table", name: "peer_block", sql: "CREATE TABLE IF NOT EXISTS peer_block(subject TEXT NOT NULL,target TEXT NOT NULL,PRIMARY KEY(subject,target),CHECK(subject<>target))"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_PEER_FACET"
			source: "cmd/BonfyreParticipation/src/peer_facets.c"
			statements: [
				{kind: "table", name: "peer_facet", sql: "CREATE TABLE IF NOT EXISTS peer_facet(id TEXT PRIMARY KEY,label TEXT NOT NULL,description TEXT NOT NULL,territory TEXT NOT NULL,owner TEXT NOT NULL,scope TEXT NOT NULL,active INTEGER NOT NULL CHECK(active IN (0,1)))"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_PEER_PARTICIPATION"
			source: "cmd/BonfyreParticipation/src/peer_facets.c"
			statements: [
				{kind: "table", name: "peer_participation", sql: "CREATE TABLE IF NOT EXISTS peer_participation(subject TEXT NOT NULL,facet TEXT NOT NULL,revision INTEGER NOT NULL,state TEXT NOT NULL CHECK(state IN ('active','withdrawn')),PRIMARY KEY(subject,facet))"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_PEER_PARTICIPATION_FEED"
			source: "cmd/BonfyreParticipation/src/peer_facets.c"
			statements: [
				{kind: "index", name: "peer_participation_feed", sql: "CREATE INDEX IF NOT EXISTS peer_participation_feed ON peer_participation(facet,state,subject)"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_PEER_INTERESTS"
			source: "cmd/BonfyreParticipation/src/peer_facets.c"
			statements: [
				{kind: "table", name: "peer_interests", sql: "CREATE TABLE IF NOT EXISTS peer_interests(subject TEXT NOT NULL,facet TEXT NOT NULL,participation_revision INTEGER NOT NULL,revision INTEGER NOT NULL,topics TEXT NOT NULL,PRIMARY KEY(subject,facet))"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_PEER_CHOICE"
			source: "cmd/BonfyreParticipation/src/peer_facets.c"
			statements: [
				{kind: "table", name: "peer_choice", sql: "CREATE TABLE IF NOT EXISTS peer_choice(subject TEXT NOT NULL,target TEXT NOT NULL,facet TEXT NOT NULL,choice INTEGER NOT NULL,revision INTEGER NOT NULL,subject_rev INTEGER NOT NULL,target_rev INTEGER NOT NULL,PRIMARY KEY(subject,target,facet),CHECK(subject<>target),CHECK(choice BETWEEN 0 AND 4))"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_PEER_MESSAGE"
			source: "cmd/BonfyreParticipation/src/peer_facets.c"
			statements: [
				{kind: "table", name: "peer_message", sql: "CREATE TABLE IF NOT EXISTS peer_message(id INTEGER PRIMARY KEY,facet TEXT NOT NULL,sender TEXT NOT NULL,recipient TEXT NOT NULL,nonce TEXT NOT NULL,body TEXT NOT NULL,sent_at INTEGER NOT NULL,sender_epoch INTEGER NOT NULL,recipient_epoch INTEGER NOT NULL,sender_choice_revision INTEGER NOT NULL,recipient_choice_revision INTEGER NOT NULL,UNIQUE(sender,nonce),CHECK(sender<>recipient))"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_PEER_MESSAGE_THREAD"
			source: "cmd/BonfyreParticipation/src/peer_facets.c"
			statements: [
				{kind: "index", name: "peer_message_thread", sql: "CREATE INDEX IF NOT EXISTS peer_message_thread ON peer_message(facet,sender,recipient,id)"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_PEER_BLOCK_2"
			source: "cmd/BonfyreParticipation/src/peer_facets.c"
			statements: [
				{kind: "table", name: "peer_block", sql: "CREATE TABLE IF NOT EXISTS peer_block(subject TEXT NOT NULL,target TEXT NOT NULL,PRIMARY KEY(subject,target),CHECK(subject<>target))"},
			]
		},
		{
			macro:  "BF_STORE_PARTICIPATION_PARTICIPATION_FACT"
			source: "cmd/BonfyreParticipation/src/evidence.c"
			statements: [
				{kind: "table", name: "participation_fact", sql: "CREATE TABLE participation_fact(occurrence TEXT NOT NULL,axis TEXT NOT NULL,owner TEXT NOT NULL,state TEXT NOT NULL);"},
				{kind: "index", name: "participation_fact_occurrence", sql: "CREATE INDEX participation_fact_occurrence ON participation_fact(occurrence,axis);"},
				{kind: "table", name: "participation_edge", sql: "CREATE TABLE participation_edge(occurrence TEXT NOT NULL,source TEXT NOT NULL,target TEXT NOT NULL,UNIQUE(occurrence,source,target));"},
			]
		},
	]
}
