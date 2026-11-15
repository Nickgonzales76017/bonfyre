/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREMEDIALIBRARY_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREMEDIALIBRARY_H
/* cmd/BonfyreMediaLibrary/src/main.c */
#define BF_STORE_MEDIA_LIBRARY_MEDIA_LIBRARY \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS media_library(library_id TEXT PRIMARY KEY,mode TEXT NOT NULL CHECK(mode IN('personal','community')),owner_subject TEXT NOT NULL,title TEXT NOT NULL,visibility TEXT NOT NULL CHECK(visibility IN('private','community','public')),state TEXT NOT NULL CHECK(state IN('active','retired')),updated_at INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS media_library_access(library_id TEXT NOT NULL REFERENCES media_library(library_id) ON DELETE CASCADE,actor_subject TEXT NOT NULL,role TEXT NOT NULL CHECK(role IN('viewer','curator','admin')),state TEXT NOT NULL CHECK(state IN('active','revoked')),updated_at INTEGER NOT NULL,PRIMARY KEY(library_id,actor_subject));" \
    "CREATE TABLE IF NOT EXISTS media_library_membership(library_id TEXT NOT NULL REFERENCES media_library(library_id) ON DELETE CASCADE,subject_ref TEXT NOT NULL,media_program_ref TEXT NOT NULL,collection_ref TEXT NOT NULL DEFAULT '',state TEXT NOT NULL CHECK(state IN('active','removed')),added_at INTEGER NOT NULL,PRIMARY KEY(library_id,subject_ref,media_program_ref));" \
    "CREATE TABLE IF NOT EXISTS media_playback_session(session_id TEXT PRIMARY KEY,library_id TEXT NOT NULL,actor_subject TEXT NOT NULL,subject_ref TEXT NOT NULL,media_program_ref TEXT NOT NULL,representation_id TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('prepared','playing','paused','ended')),position_ms INTEGER NOT NULL CHECK(position_ms>=0),opened_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS media_continue_by_actor ON media_playback_session(actor_subject,library_id,updated_at DESC);"
#endif
