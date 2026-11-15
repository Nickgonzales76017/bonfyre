/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRESOURCEFRONTIER_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRESOURCEFRONTIER_H
/* cmd/BonfyreSourceFrontier/src/main.c */
#define BF_STORE_SOURCE_FRONTIER_INVENTORY \
    "PRAGMA journal_mode=DELETE;" \
    "CREATE TABLE IF NOT EXISTS inventory(label TEXT PRIMARY KEY, first_seen TEXT, last_seen TEXT, rows INTEGER DEFAULT 0, refused INTEGER DEFAULT 0);" \
    "CREATE TABLE IF NOT EXISTS root(root_id TEXT PRIMARY KEY, host TEXT NOT NULL, name TEXT DEFAULT '', identity TEXT DEFAULT '', kind TEXT DEFAULT '', robots TEXT DEFAULT 'unknown', economic TEXT DEFAULT '', first_seen TEXT, last_seen TEXT);" \
    "CREATE TABLE IF NOT EXISTS root_origin(root_id TEXT, label TEXT, listings INTEGER DEFAULT 0, PRIMARY KEY(root_id,label));" \
    "CREATE TABLE IF NOT EXISTS family(family_id TEXT PRIMARY KEY, root_id TEXT NOT NULL, protocol TEXT NOT NULL, endpoint TEXT DEFAULT '', discovered_from TEXT DEFAULT '', first_seen TEXT);" \
    "CREATE TABLE IF NOT EXISTS channel(channel_id TEXT PRIMARY KEY, family_id TEXT NOT NULL, root_id TEXT NOT NULL, address TEXT NOT NULL, exact_address TEXT NOT NULL, title TEXT DEFAULT '', material TEXT DEFAULT '', protocol TEXT DEFAULT 'unknown', state TEXT NOT NULL DEFAULT 'candidate', truth_level TEXT DEFAULT 'listed', refusal TEXT DEFAULT '', virtual INTEGER DEFAULT 0, census_source TEXT DEFAULT '', credential TEXT DEFAULT '', rights TEXT DEFAULT '', cadence TEXT DEFAULT '', discovered_from TEXT DEFAULT '', body_sha256 TEXT DEFAULT '', identity TEXT DEFAULT '', first_seen TEXT, last_seen TEXT, last_contributed TEXT DEFAULT '', contributions INTEGER DEFAULT 0);" \
    "CREATE TABLE IF NOT EXISTS channel_origin(channel_id TEXT, label TEXT, PRIMARY KEY(channel_id,label));" \
    "CREATE TABLE IF NOT EXISTS channel_census(channel_id TEXT, census_source TEXT, material TEXT DEFAULT '', PRIMARY KEY(channel_id,census_source));" \
    "CREATE TABLE IF NOT EXISTS contribution(route TEXT, channel_id TEXT, day TEXT, items INTEGER DEFAULT 0, PRIMARY KEY(route,channel_id,day));" \
    "CREATE TABLE IF NOT EXISTS gap(gap_id TEXT PRIMARY KEY, root_id TEXT NOT NULL, channel_id TEXT DEFAULT '', kind TEXT NOT NULL, state TEXT NOT NULL, resolutions TEXT NOT NULL, dependents INTEGER DEFAULT 0, measured TEXT DEFAULT '', status TEXT DEFAULT 'open', first_seen TEXT, last_seen TEXT);" \
    "CREATE TABLE IF NOT EXISTS attention(root_id TEXT PRIMARY KEY, level TEXT NOT NULL, reason TEXT DEFAULT '', updated TEXT);" \
    "CREATE INDEX IF NOT EXISTS channel_root ON channel(root_id);" \
    "CREATE INDEX IF NOT EXISTS channel_family ON channel(family_id);" \
    "CREATE INDEX IF NOT EXISTS family_root ON family(root_id);" \
    "CREATE INDEX IF NOT EXISTS channel_exact_address ON channel(exact_address);" \
    "CREATE INDEX IF NOT EXISTS root_host ON root(host);" \
    "CREATE INDEX IF NOT EXISTS channel_state ON channel(state);" \
    "CREATE INDEX IF NOT EXISTS channel_by_census ON channel(census_source);" \
    "CREATE INDEX IF NOT EXISTS contribution_channel ON contribution(channel_id);"
/* cmd/BonfyreSourceFrontier/src/main.c */
#define BF_STORE_SOURCE_FRONTIER_CHANNEL_PUBLIC_SELECTION \
    "CREATE INDEX IF NOT EXISTS channel_public_selection ON channel(state,material,carries,census_source,root_id,protocol,last_contributed,contributions,first_seen)"
#endif
