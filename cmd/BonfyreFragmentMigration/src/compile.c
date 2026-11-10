#define _POSIX_C_SOURCE 200809L
#include "compile.h"
#include "util.h"
#include <bonfyre.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int jt(sqlite3 *db,const char *json,const char *path,char *out,size_t cap){
    sqlite3_stmt *st=NULL;int ok=0;
    if(sqlite3_prepare_v2(db,"SELECT json_extract(?1,?2)",-1,&st,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(st,1,json,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,path,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)==SQLITE_ROW&&sqlite3_column_type(st,0)==SQLITE_TEXT){snprintf(out,cap,"%s",sqlite3_column_text(st,0));ok=1;}
    sqlite3_finalize(st);return ok;
}
static int ji(sqlite3 *db,const char *json,const char *path,int def){
    sqlite3_stmt *st=NULL;int v=def;
    if(sqlite3_prepare_v2(db,"SELECT json_extract(?1,?2)",-1,&st,NULL)!=SQLITE_OK)return def;
    sqlite3_bind_text(st,1,json,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,path,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)==SQLITE_ROW&&sqlite3_column_type(st,0)==SQLITE_INTEGER)v=sqlite3_column_int(st,0);
    sqlite3_finalize(st);return v;
}
static int mapping(sqlite3 *db,const char *map,const char *fn,char *page,size_t pc,char *aud,size_t ac,char *slot,size_t sc,int *review){
    sqlite3_stmt *st=NULL;int found=0;*review=0;
    if(sqlite3_prepare_v2(db,"SELECT json_extract(value,'$.page'),json_extract(value,'$.audience'),coalesce(json_extract(value,'$.slot'),'legacy-copy') FROM json_each(?1,'$.mappings') WHERE key=?2",-1,&st,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(st,1,map,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,fn,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)==SQLITE_ROW&&sqlite3_column_text(st,0)&&sqlite3_column_text(st,1)){snprintf(page,pc,"%s",sqlite3_column_text(st,0));snprintf(aud,ac,"%s",sqlite3_column_text(st,1));snprintf(slot,sc,"%s",sqlite3_column_text(st,2));found=1;}
    sqlite3_finalize(st);
    if(sqlite3_prepare_v2(db,"SELECT 1 FROM json_each(?1,'$.review_required') WHERE value=?2 LIMIT 1",-1,&st,NULL)==SQLITE_OK){sqlite3_bind_text(st,1,map,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,fn,-1,SQLITE_TRANSIENT);*review=sqlite3_step(st)==SQLITE_ROW;sqlite3_finalize(st);}
    return found;
}
static void review_row(FILE *f,const char *fn,const char *text,int idx,const char *ref,const char *dig){
    fputs("{\"function\":",f);bf_fm_json_string(f,fn);fprintf(f,",\"node_index\":%d,\"reason\":\"function requires exact page/slot/audience review\",\"source_digest\":",idx);bf_fm_json_string(f,dig);fputs(",\"source_ref\":",f);bf_fm_json_string(f,ref);fputs(",\"text\":",f);bf_fm_json_string(f,text);fputs("}\n",f);
}
static void draft_row(FILE *f,const char *coord,const char *page,const char *slot,const char *aud,int pos,const char *body,const char *fn,const char *role,const char *eq,const char *ref,const char *dig,const char *evidence){
    char tag[640];
    fputs("{\"audience\":",f);bf_fm_json_string(f,aud);fputs(",\"body\":",f);bf_fm_json_string(f,body);fputs(",\"coordinate\":",f);bf_fm_json_string(f,coord);
    fputs(",\"evidence_ref\":",f);if(evidence&&*evidence)bf_fm_json_string(f,evidence);else fputs("null",f);
    fputs(",\"kind\":\"copy\",\"notes\":\"migration candidate only; compiled fallback remains authoritative until exact projection and publication witnesses pass\",\"page\":",f);bf_fm_json_string(f,page);fprintf(f,",\"position\":%d",pos);
    fputs(",\"projection_contract\":{\"carriers\":[\"html\",\"htmx\"],\"fallback\":\"compiled\"},\"semantic_tags\":[\"migration\",\"compiled-literal\",",f);
    snprintf(tag,sizeof(tag),"function:%s",fn);bf_fm_json_string(f,tag);fputc(',',f);snprintf(tag,sizeof(tag),"role:%s",role);bf_fm_json_string(f,tag);fputc(',',f);snprintf(tag,sizeof(tag),"equivalence:%.16s",eq);bf_fm_json_string(f,tag);
    fputs("],\"slot\":",f);bf_fm_json_string(f,slot);fputs(",\"source_digest\":",f);bf_fm_json_string(f,dig);fputs(",\"source_ref\":",f);bf_fm_json_string(f,ref);fputs(",\"status\":\"draft\",\"variant_key\":\"legacy-compiled\"}\n",f);
}
static int eq_add(sqlite3 *db,const char *hash){
    sqlite3_stmt *st=NULL;int ok=0;
    if(sqlite3_prepare_v2(db,"INSERT INTO equivalence(hash,n) VALUES(?1,1) ON CONFLICT(hash) DO UPDATE SET n=n+1",-1,&st,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(st,1,hash,-1,SQLITE_TRANSIENT);ok=sqlite3_step(st)==SQLITE_DONE;sqlite3_finalize(st);return ok;
}
static int eq_stats(sqlite3 *db,int *groups,int *dup_groups,int *dup_occ){
    sqlite3_stmt *st=NULL;
    if(sqlite3_prepare_v2(db,"SELECT count(*),coalesce(sum(n>1),0),coalesce(sum(CASE WHEN n>1 THEN n ELSE 0 END),0) FROM equivalence",-1,&st,NULL)!=SQLITE_OK)return 0;
    if(sqlite3_step(st)!=SQLITE_ROW){sqlite3_finalize(st);return 0;}
    *groups=sqlite3_column_int(st,0);*dup_groups=sqlite3_column_int(st,1);*dup_occ=sqlite3_column_int(st,2);sqlite3_finalize(st);return 1;
}
int bf_fm_compile(const char *literal_manifest,const char *source_map,const char *out_dir,const char *evidence_ref){
    sqlite3 *db=NULL;sqlite3_stmt *candidates=NULL;char *manifest=NULL,*map=NULL,*draft_path=NULL,*review_path=NULL,*manifest_path=NULL;
    FILE *drafts=NULL,*reviews=NULL,*summary=NULL;char mschema[128]="",sschema[128]="",msha[65],ssha[65];
    int dc=0,rcount=0,groups=0,dup_groups=0,dup_occ=0,rc=2;
    if(!literal_manifest||!source_map||!out_dir)return 2;
    manifest=bf_fm_read_file(literal_manifest);map=bf_fm_read_file(source_map);
    if(!manifest||!map||bf_sha256_file(literal_manifest,msha)!=0||bf_sha256_file(source_map,ssha)!=0||sqlite3_open(":memory:",&db)!=SQLITE_OK)goto done;
    if(!jt(db,manifest,"$.schema_version",mschema,sizeof(mschema))||strcmp(mschema,"bonfyre.surface.literal-candidates.v2")||!jt(db,map,"$.schema_version",sschema,sizeof(sschema))||strcmp(sschema,"bonfyre.surface.source-map.v1")){fprintf(stderr,"fragment-migration: unsupported manifest or source-map schema\n");goto done;}
    if(bf_fm_ensure_dir(out_dir)!=0||sqlite3_exec(db,"CREATE TABLE equivalence(hash TEXT PRIMARY KEY,n INTEGER NOT NULL)",NULL,NULL,NULL)!=SQLITE_OK)goto done;
    draft_path=bf_fm_path_join(out_dir,"drafts.jsonl");review_path=bf_fm_path_join(out_dir,"review.jsonl");manifest_path=bf_fm_path_join(out_dir,"manifest.json");
    if(!draft_path||!review_path||!manifest_path||!(drafts=fopen(draft_path,"w"))||!(reviews=fopen(review_path,"w")))goto done;
    if(sqlite3_prepare_v2(db,"SELECT value FROM json_each(?1,'$.candidates')",-1,&candidates,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(candidates,1,manifest,-1,SQLITE_TRANSIENT);
    while(sqlite3_step(candidates)==SQLITE_ROW){
        const char *candidate=(const char *)sqlite3_column_text(candidates,0);sqlite3_stmt *nodes=NULL;
        char fn[512]="global",role[128]="",cid[128]="candidate",sref[4096]="",sdig[128]="";
        char pr[256]="",ar[256]="",sr[256]="legacy-copy",page[128],aud[128],slot[128],catom[64];int review=0;
        int line=ji(db,candidate,"$.line",0);jt(db,candidate,"$.function",fn,sizeof(fn));jt(db,candidate,"$.role",role,sizeof(role));jt(db,candidate,"$.candidate_id",cid,sizeof(cid));jt(db,candidate,"$.source_ref",sref,sizeof(sref));jt(db,candidate,"$.source_digest",sdig,sizeof(sdig));
        int mapped=mapping(db,map,fn,pr,sizeof(pr),ar,sizeof(ar),sr,sizeof(sr),&review);bf_fm_atom(pr,page,sizeof(page),96);bf_fm_atom(ar,aud,sizeof(aud),96);bf_fm_atom(sr,slot,sizeof(slot),96);bf_fm_atom(cid,catom,sizeof(catom),32);
        if(sqlite3_prepare_v2(db,"SELECT CAST(key AS INTEGER),value FROM json_each(?1,'$.text_nodes')",-1,&nodes,NULL)!=SQLITE_OK)goto done;
        sqlite3_bind_text(nodes,1,candidate,-1,SQLITE_TRANSIENT);
        while(sqlite3_step(nodes)==SQLITE_ROW){
            int idx=sqlite3_column_int(nodes,0);const char *text=(const char *)sqlite3_column_text(nodes,1);char *norm;
            if(!text||!(norm=bf_fm_normalize_space(text))){sqlite3_finalize(nodes);goto done;}if(!*norm){free(norm);continue;}
            if(!mapped||review){review_row(reviews,fn,norm,idx,sref,sdig);rcount++;}
            else{
                char eq[65],derived[65],coord[512],nref[4608];const char *ep[]={"surface-copy-exact-v2",norm};const char *sp[]={sdig,norm};
                bf_fm_digest_parts(ep,2,eq);bf_fm_digest_parts(sp,2,derived);snprintf(nref,sizeof(nref),"%s:text:%d",sref,idx);snprintf(coord,sizeof(coord),"%s.%s.%s.legacy.%s.%d",page,slot,aud,catom,idx);
                draft_row(drafts,coord,page,slot,aud,line*10+idx,norm,fn,role,eq,nref,derived,evidence_ref);if(!eq_add(db,eq)){free(norm);sqlite3_finalize(nodes);goto done;}dc++;
            }
            free(norm);
        }
        sqlite3_finalize(nodes);
    }
    sqlite3_finalize(candidates);candidates=NULL;if(!eq_stats(db,&groups,&dup_groups,&dup_occ))goto done;fclose(drafts);drafts=NULL;fclose(reviews);reviews=NULL;
    if(!(summary=fopen(manifest_path,"w")))goto done;
    fputs("{\n  \"schema_version\": \"bonfyre.surface.fragment-migration.v2\",\n  \"literal_manifest\": ",summary);bf_fm_json_string(summary,literal_manifest);fputs(",\n  \"literal_manifest_sha256\": ",summary);bf_fm_json_string(summary,msha);fputs(",\n  \"source_map\": ",summary);bf_fm_json_string(summary,source_map);fputs(",\n  \"source_map_sha256\": ",summary);bf_fm_json_string(summary,ssha);
    fprintf(summary,",\n  \"draft_count\": %d,\n  \"review_count\": %d,\n  \"equivalence_groups\": %d,\n  \"duplicate_equivalence_groups\": %d,\n  \"duplicate_occurrences\": %d,\n",dc,rcount,groups,dup_groups,dup_occ);
    fputs("  \"equivalence_law\": \"normalized UTF-8 byte identity only; case or semantic similarity is candidate geometry, not exact equivalence\",\n  \"publication_effect\": false,\n  \"promotion_law\": [\n    \"draft admission is not publication\",\n    \"compiled fallback remains until exact SurfaceIR projection passes\",\n    \"Recall/ANN may propose reuse but cannot satisfy an exact coordinate\",\n    \"removing a compiled fallback requires a separate verified source patch\"\n  ]\n}\n",summary);
    fclose(summary);summary=NULL;printf("{\"schema\":\"bonfyre.surface.fragment-migration-compile.v2\",\"drafts\":%d,\"review\":%d,\"equivalence_groups\":%d,\"duplicate_groups\":%d}\n",dc,rcount,groups,dup_groups);rc=0;
done:
    if(candidates)sqlite3_finalize(candidates);if(summary)fclose(summary);if(drafts)fclose(drafts);if(reviews)fclose(reviews);if(db)sqlite3_close(db);
    free(manifest_path);free(review_path);free(draft_path);free(map);free(manifest);return rc;
}
