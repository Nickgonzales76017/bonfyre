/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREEXPERIENCEMAP_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREEXPERIENCEMAP_H
/* cmd/BonfyreExperienceMap/src/owner_resolution.c */
#define BF_STORE_EXPERIENCE_MAP_GRAPH \
    "CREATE TABLE graph(body TEXT NOT NULL);" \
    "CREATE TABLE owners(id TEXT PRIMARY KEY NOT NULL CHECK(length(id)>0),body TEXT NOT NULL);" \
    "CREATE TABLE edges(id TEXT PRIMARY KEY NOT NULL,source TEXT NOT NULL,destination TEXT NOT NULL,body TEXT NOT NULL);" \
    "CREATE TABLE workflows(id TEXT PRIMARY KEY NOT NULL,body TEXT NOT NULL);"
#endif
