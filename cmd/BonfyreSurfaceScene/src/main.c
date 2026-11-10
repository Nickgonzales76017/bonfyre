#define _POSIX_C_SOURCE 200809L
#include <dirent.h>
#include <errno.h>
#include <sqlite3.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include "bonfyre.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"
#include "authoring_markdown.h"

typedef struct { char *p; size_t n, cap; } Buf;

static int grow(Buf *b, size_t add) {
    size_t need = b->n + add + 1;
    if (need <= b->cap) return 1;
    size_t cap = b->cap ? b->cap : 4096;
    while (cap < need) cap *= 2;
    char *p = realloc(b->p, cap);
    if (!p) return 0;
    b->p = p; b->cap = cap; return 1;
}

static int putn(Buf *b, const char *s, size_t n) {
    if (!grow(b, n)) return 0;
    memcpy(b->p + b->n, s, n); b->n += n; b->p[b->n] = 0; return 1;
}

static int putsb(Buf *b, const char *s) { return putn(b, s ? s : "", s ? strlen(s) : 0); }

static int printfb(Buf *b, const char *fmt, ...) {
    va_list ap, cp; va_start(ap, fmt); va_copy(cp, ap);
    int n = vsnprintf(NULL, 0, fmt, cp); va_end(cp);
    if (n < 0 || !grow(b, (size_t)n)) { va_end(ap); return 0; }
    vsnprintf(b->p + b->n, b->cap - b->n, fmt, ap); va_end(ap); b->n += (size_t)n; return 1;
}

static int jsonq(Buf *b, const char *s) {
    if (!putsb(b, "\"")) return 0;
    for (const unsigned char *p = (const unsigned char *)(s ? s : ""); *p; p++) {
        char esc[8];
        if (*p == '"' || *p == '\\') { esc[0]='\\'; esc[1]=(char)*p; esc[2]=0; if(!putsb(b,esc))return 0; }
        else if (*p == '\n') { if(!putsb(b,"\\n"))return 0; }
        else if (*p == '\r') { if(!putsb(b,"\\r"))return 0; }
        else if (*p == '\t') { if(!putsb(b,"\\t"))return 0; }
        else if (*p < 0x20) { snprintf(esc,sizeof(esc),"\\u%04x",*p); if(!putsb(b,esc))return 0; }
        else if(!putn(b,(const char *)p,1)) return 0;
    }
    return putsb(b, "\"");
}

static int table_exists(sqlite3 *db, const char *name) {
    sqlite3_stmt *st = NULL; int ok = 0;
    if (sqlite3_prepare_v2(db,"SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",-1,&st,NULL)!=SQLITE_OK) return 0;
    sqlite3_bind_text(st,1,name,-1,SQLITE_STATIC); ok = sqlite3_step(st)==SQLITE_ROW; sqlite3_finalize(st); return ok;
}

static char *query_text(sqlite3 *db, const char *sql, const char **binds, int nbind, const char *fallback) {
    sqlite3_stmt *st = NULL; char *out = NULL;
    if (sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK) return strdup(fallback ? fallback : "");
    for (int i=0;i<nbind;i++) sqlite3_bind_text(st,i+1,binds[i] ? binds[i] : "",-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st)==SQLITE_ROW && sqlite3_column_text(st,0)) out = strdup((const char *)sqlite3_column_text(st,0));
    sqlite3_finalize(st); return out ? out : strdup(fallback ? fallback : "");
}

static int query_row3(sqlite3 *db, const char *sql, const char **binds, int nbind,
                      char *a,size_t an,char *b,size_t bn,char *c,size_t cn) {
    sqlite3_stmt *st=NULL; int found=0;
    if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)return 0;
    for(int i=0;i<nbind;i++)sqlite3_bind_text(st,i+1,binds[i]?binds[i]:"",-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)==SQLITE_ROW){found=1;
        snprintf(a,an,"%s",sqlite3_column_text(st,0)?(const char*)sqlite3_column_text(st,0):"");
        snprintf(b,bn,"%s",sqlite3_column_text(st,1)?(const char*)sqlite3_column_text(st,1):"");
        snprintf(c,cn,"%s",sqlite3_column_text(st,2)?(const char*)sqlite3_column_text(st,2):"");
    }
    sqlite3_finalize(st); return found;
}

static void digest_text(const char *s, char out[65]) {
    BfSha256 ctx; unsigned char d[32];
    bf_sha256_init(&ctx); bf_sha256_update(&ctx,(const unsigned char *)(s?s:""),s?strlen(s):0);
    bf_sha256_final(&ctx,d); bf_sha256_digest_hex(d,out);
}

static const char *opt(int argc,char **argv,const char *name,const char *def) {
    for(int i=1;i+1<argc;i++) if(!strcmp(argv[i],name)) return argv[i+1];
    return def;
}

static int run_capture(char *const argv[], char **out) {
    int fds[2]; if(pipe(fds)!=0)return -1; pid_t pid=fork();
    if(pid<0){close(fds[0]);close(fds[1]);return -1;}
    if(pid==0){close(fds[0]);dup2(fds[1],STDOUT_FILENO);dup2(fds[1],STDERR_FILENO);close(fds[1]);
        if(strchr(argv[0],'/'))execv(argv[0],argv);else execvp(argv[0],argv);_exit(127);}
    close(fds[1]); Buf b={0}; char chunk[4096]; ssize_t n;
    while((n=read(fds[0],chunk,sizeof(chunk)))>0) if(!putn(&b,chunk,(size_t)n)){close(fds[0]);return -1;}
    close(fds[0]); int status=0; waitpid(pid,&status,0); if(!b.p)b.p=strdup(""); *out=b.p;
    return WIFEXITED(status)?WEXITSTATUS(status):-1;
}

typedef struct {
    char site[160], subject[384], world[192], profile[192], territory[96];
    char world_title[256], world_character[128], world_template[192];
} Root;

static int resolve_root(sqlite3 *db,const char *ns,const char *site_arg,const char *world_arg,const char *subject_arg,Root *r) {
    memset(r,0,sizeof(*r));
    snprintf(r->site,sizeof(r->site),"%s",site_arg?site_arg:"");
    snprintf(r->world,sizeof(r->world),"%s",world_arg?world_arg:"");
    snprintf(r->subject,sizeof(r->subject),"%s",subject_arg?subject_arg:"");
    if(r->site[0] && table_exists(db,"site_presentation")){
        const char *bs[]={ns,r->site}; char subject[384],profile[192],territory[96];
        if(query_row3(db,"SELECT subject_address,profile,territory FROM site_presentation WHERE namespace=?1 AND site_id=?2 AND status='published' ORDER BY id DESC LIMIT 1",bs,2,subject,sizeof(subject),profile,sizeof(profile),territory,sizeof(territory))){
            if(!r->subject[0])snprintf(r->subject,sizeof(r->subject),"%s",subject);
            snprintf(r->profile,sizeof(r->profile),"%s",profile); snprintf(r->territory,sizeof(r->territory),"%s",territory);
        }
    }
    if(!r->site[0] && r->subject[0] && table_exists(db,"site_presentation")){
        const char *bs[]={ns,r->subject}; char site[160],profile[192],territory[96];
        if(query_row3(db,"SELECT site_id,profile,territory FROM site_presentation WHERE namespace=?1 AND subject_address=?2 AND status='published' ORDER BY id DESC LIMIT 1",bs,2,site,sizeof(site),profile,sizeof(profile),territory,sizeof(territory))){
            snprintf(r->site,sizeof(r->site),"%s",site); snprintf(r->profile,sizeof(r->profile),"%s",profile); snprintf(r->territory,sizeof(r->territory),"%s",territory);
        }
    }
    if(r->world[0] && table_exists(db,"world")){
        const char *bs[]={ns,r->world}; sqlite3_stmt *st=NULL;
        if(sqlite3_prepare_v2(db,"SELECT COALESCE(subject,''),title,character,COALESCE(template,'') FROM world WHERE namespace=?1 AND world=?2 AND status='published' ORDER BY id DESC LIMIT 1",-1,&st,NULL)==SQLITE_OK){
            sqlite3_bind_text(st,1,bs[0],-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,bs[1],-1,SQLITE_TRANSIENT);
            if(sqlite3_step(st)==SQLITE_ROW){if(!r->subject[0])snprintf(r->subject,sizeof(r->subject),"%s",(const char*)sqlite3_column_text(st,0));
                snprintf(r->world_title,sizeof(r->world_title),"%s",(const char*)sqlite3_column_text(st,1));
                snprintf(r->world_character,sizeof(r->world_character),"%s",(const char*)sqlite3_column_text(st,2));
                snprintf(r->world_template,sizeof(r->world_template),"%s",(const char*)sqlite3_column_text(st,3));}
            sqlite3_finalize(st);
        }
    }
    if(!r->world[0] && r->subject[0] && table_exists(db,"world")){
        const char *bs[]={ns,r->subject}; sqlite3_stmt *st=NULL;
        if(sqlite3_prepare_v2(db,"SELECT world,title,character,COALESCE(template,'') FROM world WHERE namespace=?1 AND subject=?2 AND status='published' ORDER BY id DESC LIMIT 1",-1,&st,NULL)==SQLITE_OK){
            sqlite3_bind_text(st,1,bs[0],-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,bs[1],-1,SQLITE_TRANSIENT);
            if(sqlite3_step(st)==SQLITE_ROW){snprintf(r->world,sizeof(r->world),"%s",(const char*)sqlite3_column_text(st,0));
                snprintf(r->world_title,sizeof(r->world_title),"%s",(const char*)sqlite3_column_text(st,1));
                snprintf(r->world_character,sizeof(r->world_character),"%s",(const char*)sqlite3_column_text(st,2));
                snprintf(r->world_template,sizeof(r->world_template),"%s",(const char*)sqlite3_column_text(st,3));}
            sqlite3_finalize(st);
        }
    }
    if(!r->site[0] && r->subject[0] && table_exists(db,"site_presentation")){
        const char *bs[]={ns,r->subject}; char site[160],profile[192],territory[96];
        if(query_row3(db,"SELECT site_id,profile,territory FROM site_presentation WHERE namespace=?1 AND subject_address=?2 AND status='published' ORDER BY id DESC LIMIT 1",bs,2,site,sizeof(site),profile,sizeof(profile),territory,sizeof(territory))){
            snprintf(r->site,sizeof(r->site),"%s",site); snprintf(r->profile,sizeof(r->profile),"%s",profile); snprintf(r->territory,sizeof(r->territory),"%s",territory);
        }
    }
    return r->subject[0] || r->world[0] || r->site[0];
}

static char *arr(sqlite3 *db,const char *sql,const char **b,int n){return query_text(db,sql,b,n,"[]");}
static char *obj(sqlite3 *db,const char *sql,const char **b,int n){return query_text(db,sql,b,n,"{}");}

static char *json_union_arrays(const char *a,const char *b){
    sqlite3 *db=NULL;sqlite3_stmt *st=NULL;char *out=NULL;
    if(sqlite3_open(":memory:",&db)!=SQLITE_OK)return strdup(a?a:"[]");
    if(sqlite3_prepare_v2(db,"SELECT COALESCE(json_group_array(value),'[]') FROM (SELECT value FROM json_each(?1) UNION SELECT value FROM json_each(?2) ORDER BY value)",-1,&st,NULL)==SQLITE_OK){
        sqlite3_bind_text(st,1,a?a:"[]",-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,b?b:"[]",-1,SQLITE_TRANSIENT);
        if(sqlite3_step(st)==SQLITE_ROW&&sqlite3_column_text(st,0))out=strdup((const char*)sqlite3_column_text(st,0));
        sqlite3_finalize(st);
    }
    sqlite3_close(db);return out?out:strdup(a?a:"[]");
}

static char *compile_scene(sqlite3 *db,const char *ns,const Root *r,const char *scope,const char *frontier,const char *intent,char digest[65]) {
    const char *bsw[]={ns,r->world}, *bss[]={ns,r->site}, *bsi[]={ns,r->site,r->world};
    char *world = table_exists(db,"world") ? obj(db,
      "SELECT COALESCE((SELECT json_object('id',id,'world',world,'title',title,'character',character,'template',COALESCE(template,''),'lede',lede,'subject',COALESCE(subject,''),'sources',sources,'filter',COALESCE(filter,''),'grammars',COALESCE(grammars,''),'updated_at',updated_at) FROM world WHERE namespace=?1 AND world=?2 AND status='published' ORDER BY id DESC LIMIT 1),'{}')",bsw,2) : strdup("{}");
    char *site = table_exists(db,"site_presentation") ? obj(db,
      "SELECT COALESCE((SELECT json_object('id',id,'site_id',site_id,'host',host,'subject_address',subject_address,'profile',profile,'territory',territory,'navigation',CASE WHEN json_valid(navigation) THEN json(navigation) ELSE json('[]') END,'updated_at',updated_at) FROM site_presentation WHERE namespace=?1 AND site_id=?2 AND status='published' ORDER BY id DESC LIMIT 1),'{}')",bss,2) : strdup("{}");
    char *program = table_exists(db,"site_program") ? obj(db,
      "SELECT COALESCE((SELECT json_object('id',id,'program_id',program_id,'site_id',site_id,'experience_programs',json(experience_programs),'application_bindings',json(application_bindings),'renderer_preferences',json(renderer_preferences),'source_collections',json(source_collections),'capability_programs',json(capability_programs),'actor_presentations',json(actor_presentations),'updated_at',updated_at) FROM site_program WHERE namespace=?1 AND site_id=?2 AND status='published' ORDER BY id DESC LIMIT 1),'{}')",bss,2) : strdup("{}");
    char *experiences = table_exists(db,"site_program") ? query_text(db,
      "SELECT COALESCE(experience_programs,'[]') FROM site_program WHERE namespace=?1 AND site_id=?2 AND status='published' ORDER BY id DESC LIMIT 1",bss,2,"[]") : strdup("[]");
    char *fragments = table_exists(db,"world_fragment") ? arr(db,
      "SELECT COALESCE(json_group_array(json(j)),'[]') FROM (SELECT json_object('id',id,'fragment_id',fragment_id,'ordinal',ordinal,'subject_address',subject_address,'surface_grammar',surface_grammar,'renderer_family',renderer_family,'renderer_variant',COALESCE(renderer_variant,''),'extent',extent,'template',COALESCE(template,''),'source_projection',COALESCE(source_projection,''),'media_posture',COALESCE(media_posture,''),'temporal_posture',COALESCE(temporal_posture,''),'continuity_group',COALESCE(continuity_group,''),'visible_sections',COALESCE(visible_sections,''),'action_aperture',COALESCE(action_aperture,''),'updated_at',updated_at) j FROM world_fragment WHERE namespace=?1 AND world=?2 AND status='published' ORDER BY ordinal,id)",bsw,2) : strdup("[]");
    char *slots = table_exists(db,"semantic_slot") ? arr(db,
      "SELECT COALESCE(json_group_array(json(j)),'[]') FROM (SELECT json_object('id',id,'slot_id',slot_id,'slot_name',slot_name,'ordinal',ordinal,'extent',extent,'accepts',json(accepts),'min_material_quality',min_material_quality,'max_items',max_items,'htmx_boundary',COALESCE(htmx_boundary,''),'updated_at',updated_at) j FROM semantic_slot WHERE namespace=?1 AND site_id=?2 AND status='published' ORDER BY ordinal,id)",bss,2) : strdup("[]");
    char *bindings = table_exists(db,"application_binding") ? arr(db,
      "SELECT COALESCE(json_group_array(json(j)),'[]') FROM (SELECT json_object('id',id,'binding_id',binding_id,'slot_id',slot_id,'application_kind',application_kind,'material_query',json(material_query),'source_collection',COALESCE(source_collection,''),'action_aperture',COALESCE(action_aperture,''),'continuity_key',COALESCE(continuity_key,''),'updated_at',updated_at) j FROM application_binding WHERE namespace=?1 AND site_id=?2 AND status='published' ORDER BY id)",bss,2) : strdup("[]");
    char *stages = table_exists(db,"experience_stage") ? arr(db,
      "SELECT COALESCE(json_group_array(json(j)),'[]') FROM (SELECT json_object('id',s.id,'stage_id',s.stage_id,'experience',s.experience,'application_kind',s.application_kind,'consumes',json(s.consumes),'produces',json(s.produces),'material_queries',json(s.material_queries),'renderer',s.renderer,'extent',s.extent,'actions',json(s.actions),'next_stage_rules',json(s.next_stage_rules),'empty_projection',s.empty_projection,'refused_projection',s.refused_projection,'updated_at',s.updated_at) j FROM experience_stage s WHERE s.namespace=?1 AND s.status='published' AND EXISTS(SELECT 1 FROM json_each(?2) e WHERE e.value=s.experience) ORDER BY s.stage_id)",(const char*[]){ns,experiences},2) : strdup("[]");
    char *renderers = table_exists(db,"surface_renderer") ? arr(db,
      "WITH wanted(name) AS (SELECT renderer_family FROM world_fragment WHERE namespace=?1 AND world=?2 AND status='published' UNION SELECT renderer FROM experience_stage WHERE namespace=?1 AND status='published' AND EXISTS(SELECT 1 FROM json_each(?3) e WHERE e.value=experience_stage.experience)) SELECT COALESCE(json_group_array(json(j)),'[]') FROM (SELECT json_object('id',r.id,'renderer',r.renderer,'canonical_name',r.canonical_name,'donor',COALESCE(r.donor,''),'grammar',r.grammar,'seam',r.seam,'renders',r.renders,'extents',r.extents,'updated_at',r.updated_at) j FROM surface_renderer r JOIN wanted w ON w.name=r.renderer WHERE r.namespace=?1 AND r.status='published' ORDER BY r.renderer)",(const char*[]){ns,r->world,experiences},3) : strdup("[]");
    char *patterns = (table_exists(db,"surface_pattern") && table_exists(db,"world_fragment")) ? arr(db,
      "WITH wanted(pattern) AS (SELECT DISTINCT substr(source_projection,11,CASE WHEN instr(substr(source_projection,11),'?')>0 THEN instr(substr(source_projection,11),'?')-1 ELSE length(substr(source_projection,11)) END) FROM world_fragment WHERE namespace=?1 AND world=?2 AND status='published' AND source_projection LIKE '/_surface/%') SELECT COALESCE(json_group_array(json(j)),'[]') FROM (SELECT json_object('id',p.id,'pattern',p.pattern,'reads_type',COALESCE(p.reads_type,''),'record_namespace',COALESCE(p.record_namespace,''),'selector_field',COALESCE(p.selector_field,''),'order_by',p.order_by,'item_fields',p.item_fields,'shape',p.shape,'effect_class',p.effect_class,'requires_authority',COALESCE(p.requires_authority,''),'time_field',COALESCE(p.time_field,''),'time_mode',p.time_mode,'reads_organ',COALESCE(p.reads_organ,''),'organ_command',COALESCE(p.organ_command,''),'extent',p.extent,'updated_at',p.updated_at) j FROM surface_pattern p JOIN wanted w ON w.pattern=p.pattern WHERE p.namespace=?1 AND p.status='published' ORDER BY p.pattern)",bsw,2) : strdup("[]");
    char *presentation = table_exists(db,"presentation_template") ? arr(db,
      "SELECT COALESCE(json_group_array(json(j)),'[]') FROM (SELECT json_object('id',id,'template',template,'family',family,'renderer_mix',renderer_mix,'density',density,'hero_mode',hero_mode,'navigation_mode',navigation_mode,'media_mode',media_mode,'typography_mode',typography_mode,'card_mode',card_mode,'chrome_mode',chrome_mode,'semantic_roles',json(semantic_roles),'layout_tokens',json(layout_tokens),'updated_at',updated_at) j FROM presentation_template WHERE namespace=?1 AND status='published' AND (template=?2 OR template=?3) ORDER BY id)",(const char*[]){ns,r->profile,r->world_template},3) : strdup("[]");
    char *interaction = table_exists(db,"surface_interaction_profile") ? obj(db,
      "SELECT COALESCE((SELECT json_object('id',id,'profile_id',profile_id,'site_id',COALESCE(site_id,''),'world',COALESCE(world,''),'default_lens',COALESCE(default_lens,''),'lens_order',json(lens_order),'semantic_zoom',json(semantic_zoom),'interaction_depths',json(interaction_depths),'context_rail',json(context_rail),'relation_halo',json(relation_halo),'command_aperture',json(command_aperture),'transition_policy',json(transition_policy),'mobile_posture',json(mobile_posture),'desktop_posture',json(desktop_posture),'live_update_policy',json(live_update_policy),'updated_at',updated_at) FROM surface_interaction_profile WHERE namespace=?1 AND status='published' AND ((site_id=?2 AND ?2!='') OR (world=?3 AND ?3!='')) ORDER BY CASE WHEN site_id=?2 THEN 0 ELSE 1 END,id DESC LIMIT 1),'{}')",bsi,3) : strdup("{}");
    char kind[128]={0}; const char *colon=strchr(r->subject,':'); size_t kn=colon?(size_t)(colon-r->subject):strlen(r->subject); if(kn>=sizeof(kind))kn=sizeof(kind)-1;memcpy(kind,r->subject,kn);kind[kn]=0;
    char *contracts = table_exists(db,"object_contract") ? arr(db,
      "SELECT COALESCE(json_group_array(json(j)),'[]') FROM (SELECT json_object('id',id,'object_name',object_name,'birth_operation',birth_operation,'birth_mode',birth_mode,'requires_authority',COALESCE(requires_authority,''),'collection_lens',collection_lens,'human_fields',COALESCE(human_fields,''),'closed_fields',COALESCE(closed_fields,''),'submit_to',COALESCE(submit_to,''),'submit_method',submit_method,'action_label',COALESCE(action_label,''),'updated_at',updated_at) j FROM object_contract WHERE namespace=?1 AND applies_to_kind=?2 AND status='published' ORDER BY id)",(const char*[]){ns,kind},2) : strdup("[]");
    char *proposals = (table_exists(db,"proposal_contract") && table_exists(db,"object_contract")) ? arr(db,
      "SELECT COALESCE(json_group_array(json(j)),'[]') FROM (SELECT json_object('id',p.id,'operation',p.operation,'semantic_contract',p.semantic_contract,'preference_policy',p.preference_policy,'predicate_id',p.predicate_id,'owner',p.owner,'predicate',p.predicate,'expected_state',p.expected_state,'evidence_requirement',COALESCE(p.evidence_requirement,''),'authority_requirement',COALESCE(p.authority_requirement,''),'updated_at',p.updated_at) j FROM proposal_contract p WHERE p.namespace=?1 AND p.status='published' AND EXISTS(SELECT 1 FROM object_contract o WHERE o.namespace=?1 AND o.status='published' AND o.applies_to_kind=?2 AND o.birth_mode='PROPOSE' AND o.birth_operation=p.operation) ORDER BY p.operation)",(const char*[]){ns,kind},2) : strdup("[]");
    char *apertures = query_text(db,
      "WITH a(v,ord) AS (SELECT trim(value),ordinal*10+CAST(key AS INTEGER) FROM world_fragment,json_each('[\"'||replace(COALESCE(action_aperture,''),',','\",\"')||'\"]') WHERE namespace=?1 AND world=?2 AND status='published' AND COALESCE(action_aperture,'')<>'' UNION ALL SELECT trim(value),10000+application_binding.id*10+CAST(key AS INTEGER) FROM application_binding,json_each('[\"'||replace(COALESCE(action_aperture,''),',','\",\"')||'\"]') WHERE namespace=?1 AND site_id=?3 AND status='published' AND COALESCE(action_aperture,'')<>'' UNION ALL SELECT CAST(value AS TEXT),20000+experience_stage.id*10+CAST(key AS INTEGER) FROM experience_stage,json_each(experience_stage.actions) WHERE namespace=?1 AND status='published' AND EXISTS(SELECT 1 FROM json_each(?4)e WHERE e.value=experience_stage.experience)) SELECT COALESCE(json_group_array(v),'[]') FROM (SELECT v,min(ord) ord FROM a WHERE v<>'' GROUP BY lower(v) ORDER BY ord,v)",(const char*[]){ns,r->world,r->site,experiences},4,"[]");
    char *cap_apertures = table_exists(db,"capability_program") ? query_text(db,
      "SELECT COALESCE(json_group_array(action),'[]') FROM capability_program c WHERE c.namespace=?1 AND c.status='published' AND COALESCE(c.action,'')<>'' AND EXISTS(SELECT 1 FROM site_program sp,json_each(sp.capability_programs)e WHERE sp.namespace=?1 AND sp.site_id=?2 AND sp.status='published' AND e.value=c.program_id) ORDER BY c.program_id",
      (const char*[]){ns,r->site},2,"[]") : strdup("[]");
    char *joined_apertures=json_union_arrays(apertures,cap_apertures);
    free(apertures);free(cap_apertures);apertures=joined_apertures;
    char *lenses = query_text(db,
      "WITH lens(name,source,ord) AS (SELECT value,'navigation',CAST(key AS INTEGER) FROM site_presentation,json_each(site_presentation.navigation) WHERE site_presentation.namespace=?1 AND site_presentation.site_id=?2 AND site_presentation.status='published' UNION ALL SELECT surface_grammar,'world_fragment',1000+ordinal FROM world_fragment WHERE namespace=?1 AND world=?3 AND status='published' UNION ALL SELECT application_kind,'application_binding',2000+id FROM application_binding WHERE namespace=?1 AND site_id=?2 AND status='published' UNION ALL SELECT application_kind,'experience_stage',3000+id FROM experience_stage WHERE namespace=?1 AND status='published' AND EXISTS(SELECT 1 FROM site_program sp,json_each(sp.experience_programs) e WHERE sp.namespace=?1 AND sp.site_id=?2 AND sp.status='published' AND e.value=experience_stage.experience)) SELECT COALESCE(json_group_array(name),'[]') FROM (SELECT name,min(ord) ord FROM lens WHERE name<>'' GROUP BY lower(name) ORDER BY ord,name)",(const char*[]){ns,r->site,r->world},3,"[]");
    char *deps = query_text(db,
      "WITH dep(v) AS (SELECT 'cms:site_presentation:'||site_id FROM site_presentation WHERE namespace=?1 AND site_id=?2 AND status='published' UNION ALL SELECT 'cms:'||site_id FROM site_presentation WHERE namespace=?1 AND site_id=?2 AND status='published' UNION ALL SELECT 'cms:site_program:'||program_id FROM site_program WHERE namespace=?1 AND site_id=?2 AND status='published' UNION ALL SELECT 'cms:world:'||world FROM world WHERE namespace=?1 AND world=?3 AND status='published' UNION ALL SELECT 'cms:world_fragment:'||fragment_id FROM world_fragment WHERE namespace=?1 AND world=?3 AND status='published' UNION ALL SELECT 'cms:semantic_slot:'||slot_id FROM semantic_slot WHERE namespace=?1 AND site_id=?2 AND status='published' UNION ALL SELECT 'cms:application_binding:'||binding_id FROM application_binding WHERE namespace=?1 AND site_id=?2 AND status='published' UNION ALL SELECT 'cms:experience_stage:'||stage_id FROM experience_stage WHERE namespace=?1 AND status='published' AND EXISTS(SELECT 1 FROM site_program sp,json_each(sp.experience_programs)e WHERE sp.namespace=?1 AND sp.site_id=?2 AND sp.status='published' AND e.value=experience_stage.experience) UNION ALL SELECT 'cms:surface_renderer:'||renderer FROM surface_renderer WHERE namespace=?1 AND status='published' AND (renderer IN (SELECT renderer_family FROM world_fragment WHERE namespace=?1 AND world=?3 AND status='published') OR renderer IN (SELECT es.renderer FROM experience_stage es WHERE es.namespace=?1 AND es.status='published' AND EXISTS(SELECT 1 FROM site_program sp,json_each(sp.experience_programs)e WHERE sp.namespace=?1 AND sp.site_id=?2 AND sp.status='published' AND e.value=es.experience))) UNION ALL SELECT 'cms:presentation_template:'||template FROM presentation_template WHERE namespace=?1 AND status='published' AND (template=?4 OR template=?5) UNION ALL SELECT 'cms:surface_interaction_profile:'||profile_id FROM surface_interaction_profile WHERE namespace=?1 AND status='published' AND ((site_id=?2 AND ?2!='') OR (world=?3 AND ?3!='')) UNION ALL SELECT 'cms:object_contract:'||birth_operation FROM object_contract WHERE namespace=?1 AND applies_to_kind=?6 AND status='published') SELECT COALESCE(json_group_array(v),'[]') FROM (SELECT DISTINCT v FROM dep ORDER BY v)",(const char*[]){ns,r->site,r->world,r->profile,r->world_template,kind},6,"[]");
    char *embedded_deps=query_text(db,"WITH dep(v) AS (SELECT 'cms:surface_pattern:'||p.pattern FROM surface_pattern p WHERE p.namespace=?1 AND p.status='published' AND p.pattern IN (SELECT DISTINCT substr(source_projection,11,CASE WHEN instr(substr(source_projection,11),'?')>0 THEN instr(substr(source_projection,11),'?')-1 ELSE length(substr(source_projection,11)) END) FROM world_fragment WHERE namespace=?1 AND world=?2 AND status='published' AND source_projection LIKE '/_surface/%') UNION ALL SELECT 'cms:proposal_contract:'||p.operation FROM proposal_contract p WHERE p.namespace=?1 AND p.status='published' AND EXISTS(SELECT 1 FROM object_contract o WHERE o.namespace=?1 AND o.status='published' AND o.applies_to_kind=?3 AND o.birth_mode='PROPOSE' AND o.birth_operation=p.operation)) SELECT COALESCE(json_group_array(v),'[]') FROM (SELECT DISTINCT v FROM dep ORDER BY v)",(const char*[]){ns,r->world,kind},3,"[]");
    char *all_deps=json_union_arrays(deps,embedded_deps);
    free(deps);free(embedded_deps);deps=all_deps;
    Buf core={0};
    putsb(&core,"{\"schema\":\"bonfyre-surface-scene.v1\",\"focus\":{\"subject_address\":");jsonq(&core,r->subject);
    putsb(&core,",\"world\":");jsonq(&core,r->world);putsb(&core,",\"site_id\":");jsonq(&core,r->site);putsb(&core,",\"actor_scope\":");jsonq(&core,scope);
    putsb(&core,",\"frontier\":");jsonq(&core,frontier);putsb(&core,",\"intent\":");jsonq(&core,intent);putsb(&core,"},\"lenses\":");putsb(&core,lenses);
    putsb(&core,",\"world_material\":{\"world\":");putsb(&core,world);putsb(&core,",\"site_presentation\":");putsb(&core,site);putsb(&core,",\"site_program\":");putsb(&core,program);putsb(&core,"}");
    putsb(&core,",\"fragments\":");putsb(&core,fragments);putsb(&core,",\"slots\":");putsb(&core,slots);putsb(&core,",\"bindings\":");putsb(&core,bindings);
    putsb(&core,",\"stages\":");putsb(&core,stages);putsb(&core,",\"renderers\":");putsb(&core,renderers);putsb(&core,",\"patterns\":");putsb(&core,patterns);
    putsb(&core,",\"presentation\":");putsb(&core,presentation);putsb(&core,",\"operations\":{\"apertures\":");putsb(&core,apertures);putsb(&core,",\"object_contracts\":");putsb(&core,contracts);putsb(&core,",\"proposal_contracts\":");putsb(&core,proposals);putsb(&core,"}");
    putsb(&core,",\"interaction\":");putsb(&core,interaction);putsb(&core,",\"runtime_queries\":{\"source_collections\":");
    char *sources = table_exists(db,"site_program") ? query_text(db,"SELECT COALESCE(source_collections,'[]') FROM site_program WHERE namespace=?1 AND site_id=?2 AND status='published' ORDER BY id DESC LIMIT 1",bss,2,"[]") : strdup("[]");
    char *caps = table_exists(db,"site_program") ? query_text(db,"SELECT COALESCE(capability_programs,'[]') FROM site_program WHERE namespace=?1 AND site_id=?2 AND status='published' ORDER BY id DESC LIMIT 1",bss,2,"[]") : strdup("[]");
    char *actors = table_exists(db,"site_program") ? query_text(db,"SELECT COALESCE(actor_presentations,'{}') FROM site_program WHERE namespace=?1 AND site_id=?2 AND status='published' ORDER BY id DESC LIMIT 1",bss,2,"{}") : strdup("{}");
    putsb(&core,sources);putsb(&core,",\"capability_programs\":");putsb(&core,caps);putsb(&core,",\"actor_presentations\":");putsb(&core,actors);putsb(&core,",\"surface_patterns\":");putsb(&core,patterns);putsb(&core,"},\"dependencies\":");putsb(&core,deps);
    putsb(&core,",\"cannot_infer\":\"a SurfaceScene is a derived presentation closure; it does not grant identity, visibility, authority, availability, price, proof, commitment, presence, target completion or effect state\"}");
    digest_text(core.p,digest);
    free(world);free(site);free(program);free(experiences);free(fragments);free(slots);free(bindings);free(stages);free(renderers);free(patterns);free(presentation);free(interaction);free(contracts);free(proposals);free(apertures);free(lenses);free(deps);free(sources);free(caps);free(actors);
    return core.p;
}

static char *scene_record(const Root *r,const char *scope,const char *digest,const char *scene) {
    Buf b={0}; char key[1024]; snprintf(key,sizeof(key),"site:%s|world:%s|subject:%s|scope:%s",r->site,r->world,r->subject,scope);
    sqlite3 *jdb=NULL; sqlite3_stmt *st=NULL; char *fields[13]={0};
    if(sqlite3_open(":memory:",&jdb)!=SQLITE_OK)return NULL;
    const char *sql="SELECT json_extract(?1,'$.focus'),json_extract(?1,'$.lenses'),json_extract(?1,'$.world_material'),json_extract(?1,'$.fragments'),json_extract(?1,'$.slots'),json_extract(?1,'$.bindings'),json_extract(?1,'$.stages'),json_extract(?1,'$.renderers'),json_extract(?1,'$.patterns'),json_extract(?1,'$.presentation'),json_extract(?1,'$.operations'),json_extract(?1,'$.interaction'),json_extract(?1,'$.runtime_queries')";
    if(sqlite3_prepare_v2(jdb,sql,-1,&st,NULL)!=SQLITE_OK){sqlite3_close(jdb);return NULL;}sqlite3_bind_text(st,1,scene,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)==SQLITE_ROW)for(int i=0;i<13;i++)fields[i]=strdup(sqlite3_column_text(st,i)?(const char*)sqlite3_column_text(st,i):(i==1||i>=3?"[]":"{}"));
    sqlite3_finalize(st);
    const char *dbinds[]={scene}; char *deps=query_text(jdb,"SELECT COALESCE(json_extract(?1,'$.dependencies'),'[]')",dbinds,1,"[]"); sqlite3_close(jdb);
    putsb(&b,"{\"scene_key\":");jsonq(&b,key);putsb(&b,",\"subject_address\":");jsonq(&b,r->subject);putsb(&b,",\"world\":");jsonq(&b,r->world);putsb(&b,",\"site_id\":");jsonq(&b,r->site);putsb(&b,",\"actor_scope\":");jsonq(&b,scope);putsb(&b,",\"source_digest\":");jsonq(&b,digest);
    const char *names[]={"focus","lenses","world_material","fragments","slots","bindings","stages","renderers","patterns","presentation","operations","interaction","runtime_queries"};
    for(int i=0;i<13;i++){const char *fallback=(i==1||(i>=3&&i<=9))?"[]":"{}";printfb(&b,",\"%s\":",names[i]);putsb(&b,fields[i]?fields[i]:fallback);free(fields[i]);}
    putsb(&b,",\"dependencies\":");putsb(&b,deps);free(deps);
    putsb(&b,",\"cannot_infer\":\"a SurfaceScene is a derived presentation closure; request-time owners still decide semantic truth, actor closure, authority, availability, price, proof, commitment, presence, target state and effects\",\"status\":\"published\"}");
    return b.p;
}

static const char *scene_schema_arg=NULL,*schemas_dir_arg=NULL;
static const char *cms_bin(void){const char *e=getenv("BONFYRE_CMS_BINARY");if(e&&*e)return e;if(access("../BonfyreCMS/bonfyre-cms",X_OK)==0)return "../BonfyreCMS/bonfyre-cms";return "bonfyre-cms";}
static const char *scene_schema(void){const char *e=getenv("BONFYRE_SURFACE_SCENE_SCHEMA");if(scene_schema_arg&&*scene_schema_arg)return scene_schema_arg;return e&&*e?e:"../BonfyreCMS/content-types/surface_scene.json";}
static const char *schemas_dir(void){const char *e=getenv("BONFYRE_CMS_SCHEMAS");if(schemas_dir_arg&&*schemas_dir_arg)return schemas_dir_arg;return e&&*e?e:"../BonfyreCMS/content-types";}

static int persist_scene(sqlite3 *db,const char *dbpath,const char *ns,const char *record,const Root *r,const char *scope,int *id_out) {
    char key[1024];snprintf(key,sizeof(key),"site:%s|world:%s|subject:%s|scope:%s",r->site,r->world,r->subject,scope);
    char *out=NULL; char *applyv[]={(char*)cms_bin(),"schema","apply",(char*)scene_schema(),"--db",(char*)dbpath,"--schemas",(char*)schemas_dir(),NULL};
    if(run_capture(applyv,&out)!=0){fprintf(stderr,"%s",out?out:"schema apply failed\n");free(out);return 0;}free(out);
    int id=0; if(table_exists(db,"surface_scene")){sqlite3_stmt *st=NULL;if(sqlite3_prepare_v2(db,"SELECT id FROM surface_scene WHERE namespace=?1 AND scene_key=?2 ORDER BY id DESC LIMIT 1",-1,&st,NULL)==SQLITE_OK){sqlite3_bind_text(st,1,ns,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,key,-1,SQLITE_TRANSIENT);if(sqlite3_step(st)==SQLITE_ROW)id=sqlite3_column_int(st,0);sqlite3_finalize(st);}}
    char idbuf[32];snprintf(idbuf,sizeof(idbuf),"%d",id);
    char *argv_create[]={(char*)cms_bin(),"entry","create","surface_scene",(char*)record,"--ns",(char*)ns,"--db",(char*)dbpath,"--schemas",(char*)schemas_dir(),NULL};
    char *argv_update[]={(char*)cms_bin(),"entry","update","surface_scene",idbuf,(char*)record,"--ns",(char*)ns,"--db",(char*)dbpath,"--schemas",(char*)schemas_dir(),NULL};
    int rc=run_capture(id?argv_update:argv_create,&out); if(rc!=0){fprintf(stderr,"%s",out?out:"scene persist failed\n");free(out);return 0;}
    if(!id){
        sqlite3_stmt *st=NULL;
        if(sqlite3_prepare_v2(db,"SELECT id FROM surface_scene WHERE namespace=?1 AND scene_key=?2 ORDER BY id DESC LIMIT 1",-1,&st,NULL)==SQLITE_OK){
            sqlite3_bind_text(st,1,ns,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(st,2,key,-1,SQLITE_TRANSIENT);
            if(sqlite3_step(st)==SQLITE_ROW)id=sqlite3_column_int(st,0);
            sqlite3_finalize(st);
        }
    }
    free(out); if(id_out)*id_out=id; return 1;
}

static int do_one(const char *dbpath,const char *ns,const char *site,const char *world,const char *subject,const char *scope,const char *frontier,const char *intent,int sync) {
    sqlite3 *db=NULL; if(sqlite3_open_v2(dbpath,&db,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE,NULL)!=SQLITE_OK){fprintf(stderr,"surface-scene: cannot open db\n");return 1;}
    Root r; if(!resolve_root(db,ns,site,world,subject,&r)){fprintf(stderr,"surface-scene: no addressed site/world/subject\n");sqlite3_close(db);return 2;}
    char digest[65]; char *scene=compile_scene(db,ns,&r,scope,frontier,intent,digest); if(!scene){sqlite3_close(db);return 3;}
    if(!sync){
        /* The compiler already owns this digest. Retain it in the read-only
         * projection as well as the persisted record, so runtime bindings and
         * deltas do not have to invent or recompute a source revision. */
        size_t len=strlen(scene);
        if(!len||scene[len-1]!='}'){free(scene);sqlite3_close(db);return 3;}
        scene[len-1]='\0';printf("%s,\"source_digest\":\"%s\"}\n",scene,digest);
        free(scene);sqlite3_close(db);return 0;
    }
    char *record=scene_record(&r,scope,digest,scene); if(!record){free(scene);sqlite3_close(db);return 4;}
    int id=0,ok=persist_scene(db,dbpath,ns,record,&r,scope,&id);
    if(ok){Buf b={0};putsb(&b,"{\"schema\":\"bonfyre-surface-scene-sync.v1\",\"id\":");printfb(&b,"%d",id);putsb(&b,",\"site_id\":");jsonq(&b,r.site);putsb(&b,",\"world\":");jsonq(&b,r.world);putsb(&b,",\"subject_address\":");jsonq(&b,r.subject);putsb(&b,",\"source_digest\":");jsonq(&b,digest);putsb(&b,",\"state\":\"materialized\",\"truth\":\"derived presentation closure only\"}");puts(b.p);free(b.p);}
    free(record);free(scene);sqlite3_close(db);return ok?0:5;
}

static int sync_all(const char *dbpath,const char *ns,const char *scope) {
    sqlite3 *db=NULL;if(sqlite3_open_v2(dbpath,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)return 1;
    char **sites=NULL;int count=0,cap=0;
    if(table_exists(db,"site_presentation")){
        sqlite3_stmt *st=NULL;
        if(sqlite3_prepare_v2(db,"SELECT DISTINCT site_id FROM site_presentation WHERE namespace=?1 AND status='published' AND COALESCE(subject_address,'')<>'' ORDER BY site_id",-1,&st,NULL)==SQLITE_OK){
            sqlite3_bind_text(st,1,ns,-1,SQLITE_TRANSIENT);
            while(sqlite3_step(st)==SQLITE_ROW){
                if(count==cap){cap=cap?cap*2:16;char **tmp=realloc(sites,(size_t)cap*sizeof(*sites));if(!tmp){sqlite3_finalize(st);sqlite3_close(db);return 2;}sites=tmp;}
                sites[count++]=strdup((const char*)sqlite3_column_text(st,0));
            }
            sqlite3_finalize(st);
        }
    }
    sqlite3_close(db);int synced=0;
    for(int i=0;i<count;i++){if(do_one(dbpath,ns,sites[i],"","",scope,"","",1)==0)synced++;free(sites[i]);}
    free(sites);
    printf("{\"schema\":\"bonfyre-surface-scene-sync-all.v1\",\"synced\":%d,\"actor_scope\":\"%s\"}\n",synced,scope);
    return synced==count?0:3;
}

typedef struct { char site[160],world[192],subject[384],scope[64]; } SceneRef;

static int lower_handoff(const char *dbpath,const char *ns,const char *scope,const char *path) {
    FILE *f=fopen(path,"rb");if(!f){perror("handoff");return 1;}fseek(f,0,SEEK_END);long sz=ftell(f);rewind(f);if(sz<=0||sz>1048576){fclose(f);return 2;}
    char *body=malloc((size_t)sz+1);if(!body){fclose(f);return 2;}if(fread(body,1,(size_t)sz,f)!=(size_t)sz){free(body);fclose(f);return 2;}body[sz]=0;fclose(f);
    sqlite3 *db=NULL;if(sqlite3_open_v2(dbpath,&db,SQLITE_OPEN_READWRITE,NULL)!=SQLITE_OK){free(body);return 3;}
    sqlite3 *jdb=NULL;sqlite3_stmt *js=NULL;char identity[768]={0},lowerer[256]={0},schema[128]={0};
    if(sqlite3_open(":memory:",&jdb)!=SQLITE_OK||sqlite3_prepare_v2(jdb,"SELECT json_extract(?1,'$.schema'),json_extract(?1,'$.identity'),json_extract(?1,'$.lowerer_ref') WHERE json_valid(?1)",-1,&js,NULL)!=SQLITE_OK){sqlite3_close(db);free(body);return 3;}
    sqlite3_bind_text(js,1,body,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(js)==SQLITE_ROW){snprintf(schema,sizeof(schema),"%s",sqlite3_column_text(js,0)?(const char*)sqlite3_column_text(js,0):"");snprintf(identity,sizeof(identity),"%s",sqlite3_column_text(js,1)?(const char*)sqlite3_column_text(js,1):"");snprintf(lowerer,sizeof(lowerer),"%s",sqlite3_column_text(js,2)?(const char*)sqlite3_column_text(js,2):"");}
    sqlite3_finalize(js);sqlite3_close(jdb);free(body);
    if(strcmp(schema,"bonfyre-lowering-handoff.v1")||strncmp(lowerer,"SurfaceIR:",10)||!identity[0]){fprintf(stderr,"surface-scene: not a SurfaceIR handoff\n");sqlite3_close(db);return 4;}
    if(!table_exists(db,"surface_scene")){sqlite3_close(db);return sync_all(dbpath,ns,scope);}
    SceneRef *refs=NULL;int n=0,cap=0;sqlite3_stmt *st=NULL;
    if(sqlite3_prepare_v2(db,"SELECT DISTINCT site_id,world,subject_address,actor_scope FROM surface_scene,json_each(surface_scene.dependencies) WHERE namespace=?1 AND status='published' AND json_each.value=?2",-1,&st,NULL)==SQLITE_OK){
        sqlite3_bind_text(st,1,ns,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,identity,-1,SQLITE_TRANSIENT);
        while(sqlite3_step(st)==SQLITE_ROW){
            if(n==cap){cap=cap?cap*2:8;SceneRef *tmp=realloc(refs,(size_t)cap*sizeof(*refs));if(!tmp){sqlite3_finalize(st);sqlite3_close(db);free(refs);return 5;}refs=tmp;}
            snprintf(refs[n].site,sizeof(refs[n].site),"%s",sqlite3_column_text(st,0)?(const char*)sqlite3_column_text(st,0):"");
            snprintf(refs[n].world,sizeof(refs[n].world),"%s",sqlite3_column_text(st,1)?(const char*)sqlite3_column_text(st,1):"");
            snprintf(refs[n].subject,sizeof(refs[n].subject),"%s",sqlite3_column_text(st,2)?(const char*)sqlite3_column_text(st,2):"");
            snprintf(refs[n].scope,sizeof(refs[n].scope),"%s",sqlite3_column_text(st,3)?(const char*)sqlite3_column_text(st,3):scope);n++;
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(db);int synced=0;
    for(int i=0;i<n;i++)if(do_one(dbpath,ns,refs[i].site,refs[i].world,refs[i].subject,refs[i].scope[0]?refs[i].scope:scope,"","",1)==0)synced++;
    free(refs);
    if(!synced && !strncmp(identity,"cms:world:",10)) synced += do_one(dbpath,ns,"",identity+10,"",scope,"","",1)==0;
    if(!synced && !strcmp(lowerer,"SurfaceIR:site_program") && !strncmp(identity,"cms:",4)) synced += do_one(dbpath,ns,identity+4,"","",scope,"","",1)==0;
    printf("{\"schema\":\"bonfyre-surface-scene-lower.v1\",\"identity\":\"%s\",\"lowerer\":\"%s\",\"resynced\":%d,\"truth\":\"dependency-scoped scene rematerialization only\"}\n",identity,lowerer,synced);
    return synced?0:6;
}

/* SurfaceIR is a separate lowerer family, so it drains only its own durable
 * handoff partitions. A successful rematerialization is acknowledged by an
 * atomic rename; a stale/invalid handoff remains visible for retry or audit. */
static int drain_handoffs(const char *dbpath,const char *ns,const char *scope,const char *root,int limit) {
    DIR *directory=NULL,*partition=NULL;struct dirent *entry,*file;int seen=0,handled=0,failed=0;
    if(!root||!*root||limit<1||!(directory=opendir(root)))return 2;
    while(seen<limit&&(entry=readdir(directory))){char path[1024];
        if(strncmp(entry->d_name,"SurfaceIR-",10)||snprintf(path,sizeof(path),"%s/%s",root,entry->d_name)>=(int)sizeof(path)||!(partition=opendir(path)))continue;
        while(seen<limit&&(file=readdir(partition))){char handoff[1280],done[1340];size_t n=strlen(file->d_name);
            /* PublicProjection owns the first SurfaceIR acknowledgement. Its
             * .json.handled envelope is the explicit downstream handoff to
             * SurfaceScene, so these workers never race for a raw file. */
            if(file->d_name[0]=='.'||n<14||strcmp(file->d_name+n-13,".json.handled")||snprintf(handoff,sizeof(handoff),"%s/%s",path,file->d_name)>=(int)sizeof(handoff))continue;
            seen++;
            if(lower_handoff(dbpath,ns,scope,handoff)==0&&snprintf(done,sizeof(done),"%s.surface-scene.handled",handoff)<(int)sizeof(done)&&rename(handoff,done)==0)handled++;else failed++;
        }
        closedir(partition);partition=NULL;
    }
    closedir(directory);
    printf("{\"schema\":\"bonfyre-surface-scene-drain.v1\",\"seen\":%d,\"handled\":%d,\"failed_or_deferred\":%d,\"truth\":\"SurfaceIR dependency-scoped scene rematerialization only\"}\n",seen,handled,failed);
    return failed?6:0;
}

static void usage(void){fprintf(stderr,"usage: bonfyre-surface-scene <compile|sync|sync-all|lower|drain> --db FILE [--ns NS] [--site ID] [--world ID] [--subject ADDRESS] [--actor-scope public] [--frontier REF] [--intent TEXT] [--handoff FILE|--lowerer-dir DIRECTORY] [--limit N]\n       bonfyre-surface-scene render-md [--max-html N] < input.md > output.html\n");}

static int render_md(int argc,char **argv){
    long cap=262144;const char *m=opt(argc,argv,"--max-html",NULL);
    if(m){cap=atol(m);if(cap<1024||cap>16777216)return 2;}
    size_t n=0,len=0;char *text=NULL;const size_t CH=65536;
    for(;;){char *g=realloc(text,len+CH+1);if(!g){free(text);return 1;}text=g;
        n=fread(text+len,1,CH,stdin);len+=n;if(n<CH)break;
        if(len>16777216){free(text);return 2;}}
    text[len]=0;
    int ok=bf_authoring_markdown_render(stdout,text,(size_t)cap);
    free(text);return ok?0:1;
}

int main(int argc,char **argv){
    if(argc<2){usage();return 2;}
    if(!strcmp(argv[1],"render-md"))return render_md(argc,argv);
    const char *db=opt(argc,argv,"--db",NULL);if(!db){usage();return 2;}
    schemas_dir_arg=opt(argc,argv,"--schemas",NULL);
    scene_schema_arg=opt(argc,argv,"--scene-schema",NULL);
    const char *ns=opt(argc,argv,"--ns","root"),*site=opt(argc,argv,"--site",""),*world=opt(argc,argv,"--world",""),*subject=opt(argc,argv,"--subject",""),*scope=opt(argc,argv,"--actor-scope","public"),*frontier=opt(argc,argv,"--frontier",""),*intent=opt(argc,argv,"--intent","");
    enum{SS_COMPILE,SS_DRAIN,SS_LOWER,SS_SYNC,SS_SYNC_ALL};
    static const BfCommandEntry scene_commands[]={{"compile",SS_COMPILE,0,NULL},{"drain",SS_DRAIN,0,NULL},{"lower",SS_LOWER,0,NULL},{"sync",SS_SYNC,0,NULL},{"sync-all",SS_SYNC_ALL,0,NULL}};
    const BfCommandEntry *command=bf_command_lookup(scene_commands,sizeof(scene_commands)/sizeof(scene_commands[0]),argv[1]);
    if(command&&command->id==SS_COMPILE)return do_one(db,ns,site,world,subject,scope,frontier,intent,0);
    if(command&&command->id==SS_SYNC)return do_one(db,ns,site,world,subject,scope,frontier,intent,1);
    if(command&&command->id==SS_SYNC_ALL)return sync_all(db,ns,scope);
    if(command&&command->id==SS_LOWER){const char *h=opt(argc,argv,"--handoff",NULL);if(!h){usage();return 2;}return lower_handoff(db,ns,scope,h);}
    if(command&&command->id==SS_DRAIN){const char *root=opt(argc,argv,"--lowerer-dir",NULL);int limit=atoi(opt(argc,argv,"--limit","128"));if(!root||limit<1||limit>4096){usage();return 2;}return drain_handoffs(db,ns,scope,root,limit);}
    usage();return 2;
}
