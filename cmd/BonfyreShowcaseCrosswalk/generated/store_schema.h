/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRESHOWCASECROSSWALK_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRESHOWCASECROSSWALK_H
/* cmd/BonfyreShowcaseCrosswalk/src/main.c */
#define BF_STORE_SHOWCASE_CROSSWALK_ESTATE \
    "CREATE TABLE estate(id TEXT PRIMARY KEY,name TEXT,url TEXT,host TEXT,kind TEXT);" \
    "CREATE INDEX estate_url ON estate(url);" \
    "CREATE INDEX estate_host ON estate(host);" \
    "CREATE INDEX estate_name ON estate(name);" \
    "CREATE TABLE showcase(id TEXT PRIMARY KEY,label TEXT,authority TEXT,url TEXT,host TEXT,carrier TEXT,auth TEXT,protocols TEXT,worlds INTEGER);"
#endif
