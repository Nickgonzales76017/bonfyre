#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include "source_constellations.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static int scalar(sqlite3 *db, const char *sql, int *out) {
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, bf_source_constellations_json, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) { *out = sqlite3_column_int(st,0); ok = 1; }
    sqlite3_finalize(st);
    return ok;
}

static int audit(sqlite3 *db) {
    int worlds=0, featured=0, source_occurrences=0, unique_sources=0, bad_truth=0, shallow=0, entry_projection=0;
    if (!scalar(db,"SELECT count(*) FROM json_each(?1,'$.worlds')",&worlds) ||
        !scalar(db,"SELECT count(*) FROM json_each(?1,'$.worlds') WHERE json_extract(value,'$.featured')=1",&featured) ||
        !scalar(db,"SELECT count(*) FROM json_each(?1,'$.worlds') w,json_each(w.value,'$.sources') s",&source_occurrences) ||
        !scalar(db,"SELECT count(DISTINCT json_extract(s.value,'$.id')) FROM json_each(?1,'$.worlds') w,json_each(w.value,'$.sources') s",&unique_sources) ||
        !scalar(db,"SELECT count(*) FROM json_each(?1,'$.worlds') w,json_each(w.value,'$.sources') s WHERE json_extract(s.value,'$.truth')!='candidate_unobserved_external'",&bad_truth) ||
        !scalar(db,"SELECT count(*) FROM json_each(?1,'$.worlds') w,json_each(w.value,'$.sources') s WHERE json_array_length(json_extract(s.value,'$.surfaces'))<4",&shallow) ||
        !scalar(db,"SELECT json_array_length(json_extract(value,'$.surface_projection')) FROM json_each(?1,'$.worlds') WHERE json_extract(value,'$.id')=json_extract(?1,'$.entry_world')",&entry_projection))
        return 0;
    printf("{\"schema\":\"bonfyre-showcase-estate-audit.v1\",\"worlds\":%d,\"featured_worlds\":%d,\"source_occurrences\":%d,\"unique_sources\":%d,\"bad_truth_rows\":%d,\"shallow_sources\":%d,\"entry_surface_projection\":%d,\"truth\":\"candidate_constellations_not_observed_state\"}\n",
           worlds,featured,source_occurrences,unique_sources,bad_truth,shallow,entry_projection);
    return worlds==5 && featured==1 && source_occurrences==25 && unique_sources==17 &&
           bad_truth==0 && shallow==0 && entry_projection==22;
}

static int show(sqlite3 *db, const char *world) {
    sqlite3_stmt *st=NULL;
    const char *sql=
        "SELECT json_object('schema','bonfyre-showcase-world.v1','entry_world',json_extract(?1,'$.entry_world'),"
        "'world',json(value),'truth','source_candidates_require_foreign_twin_observation_and_owner_admission') "
        "FROM json_each(?1,'$.worlds') WHERE json_extract(value,'$.id')=?2 LIMIT 1";
    if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(st,1,bf_source_constellations_json,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,world,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)!=SQLITE_ROW||!sqlite3_column_text(st,0)){sqlite3_finalize(st);return 0;}
    puts((const char*)sqlite3_column_text(st,0));
    sqlite3_finalize(st);
    return 1;
}

static int impact(sqlite3 *db, const char *surface) {
    sqlite3_stmt *st=NULL;
    const char *sql=
        "SELECT json_object('schema','bonfyre-source-surface-impact.v1','surface',?2,'candidates',"
        "json_group_array(json_object('world',json_extract(w.value,'$.id'),'source',json_extract(s.value,'$.id'),"
        "'label',json_extract(s.value,'$.label'),'authority',json_extract(s.value,'$.authority'),"
        "'url',json_extract(s.value,'$.url'),'auth',json_extract(s.value,'$.auth'),'truth',json_extract(s.value,'$.truth'))),"
        "'truth','candidate_projection_impact_only') "
        "FROM json_each(?1,'$.worlds') w,json_each(w.value,'$.sources') s,json_each(s.value,'$.surfaces') p "
        "WHERE p.value=?2";
    if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(st,1,bf_source_constellations_json,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,surface,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)!=SQLITE_ROW||!sqlite3_column_text(st,0)){sqlite3_finalize(st);return 0;}
    puts((const char*)sqlite3_column_text(st,0));
    sqlite3_finalize(st);
    return 1;
}

static int shared(sqlite3 *db) {
    sqlite3_stmt *st=NULL;
    const char *sql=
        "SELECT json_extract(s.value,'$.id'),count(DISTINCT json_extract(w.value,'$.id')) n "
        "FROM json_each(?1,'$.worlds') w,json_each(w.value,'$.sources') s "
        "GROUP BY 1 HAVING n>1 ORDER BY n DESC,1";
    if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(st,1,bf_source_constellations_json,-1,SQLITE_STATIC);
    while(sqlite3_step(st)==SQLITE_ROW)
        printf("%s\t%d\n",sqlite3_column_text(st,0),sqlite3_column_int(st,1));
    sqlite3_finalize(st);
    return 1;
}

int main(int argc,char**argv){
    sqlite3*db=NULL;int ok=0;
    if(argc<2||sqlite3_open(":memory:",&db)!=SQLITE_OK)return 2;
    enum { SE_AUDIT, SE_IMPACT, SE_SHARED, SE_SHOW };
    static const BfCommandEntry showcase_commands[] = {
        {"audit", SE_AUDIT, 0, NULL},
        {"impact", SE_IMPACT, 0, NULL},
        {"shared", SE_SHARED, 0, NULL},
        {"show", SE_SHOW, 0, NULL},
    };
    const BfCommandEntry *command = bf_command_lookup(showcase_commands,
        sizeof(showcase_commands) / sizeof(showcase_commands[0]), argv[1]);
    if(command&&command->id==SE_AUDIT)ok=audit(db);
    else if(command&&command->id==SE_SHOW&&argc==3)ok=show(db,argv[2]);
    else if(command&&command->id==SE_IMPACT&&argc==3)ok=impact(db,argv[2]);
    else if(command&&command->id==SE_SHARED)ok=shared(db);
    else fprintf(stderr,"usage: bonfyre-showcase-estate audit | show WORLD | impact SURFACE | shared\n");
    sqlite3_close(db);return ok?0:1;
}
