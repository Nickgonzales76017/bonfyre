#ifndef BONFYRE_PACK_SETUP_H
#define BONFYRE_PACK_SETUP_H
#include "pack_resolution.h"
#include "../../BonfyreCMS/src/household_access.h"
static int session_self_authoring(const char *, const char *);
static int session_institution_authorized(const char *, const char *, const char *, const char *);
static int workspace_package_available(const char *, const char *);
static int workspace_plan_available(const BfPackResolution *);
static void workspace_editor_links(long long);
typedef struct {
    char pack[96], context[96], version[64], digest[65], revision[24], operation[24], shared[65], repr[16], design[208], q[97], kind[16], order[16], view[8];
} PackSetupInput;
typedef struct {
    char state[24], digest[65], pinned[65], version[64], reason[96], subject[96];
    long long revision, installation;
    int committed;
    BfPackResolution resolution, pins;
} PackSetupResult;
static int psetup_hex(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}
static int psetup_decode(const char *restrict s, size_t len, char *restrict out, size_t cap) {
    size_t n = 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c == '%') {
            if (i + 2 >= len || psetup_hex(s[i+1]) < 0 || psetup_hex(s[i+2]) < 0) return 0;
            c = (unsigned char)(psetup_hex(s[i+1])*16+psetup_hex(s[i+2])); i += 2;
        } else if (c == '+') c = ' ';
        if (c < 33 || c > 126 || n + 1 >= cap) return 0;
        out[n++] = (char)c;
    }
    out[n] = 0; return 1;
}
static int psetup_input(const char *text, PackSetupInput *out) {
    memset(out, 0, sizeof(*out));
    if (text && strlen(text) > 4096) return 0;
    unsigned seen = 0;
    for (const char *p = text ? text : ""; *p;) {
        const char *end = strchr(p, '&'); if (!end) end = p + strlen(p);
        const char *eq = memchr(p, '=', (size_t)(end-p)); char key[24];
        if (!eq || !psetup_decode(p, (size_t)(eq-p), key, sizeof(key))) return 0;
        char *dest = NULL; size_t cap = 0; unsigned bit = 0;
#define PSET_FIELD(k,f,b) if (!strcmp(key,k)) { dest=out->f; cap=sizeof(out->f); bit=b; }
        PSET_FIELD("pack",pack,1) else PSET_FIELD("context",context,2) else PSET_FIELD("version",version,4)
        else PSET_FIELD("digest",digest,8) else PSET_FIELD("revision",revision,16)
        else PSET_FIELD("operation",operation,32) else PSET_FIELD("shared",shared,64) else PSET_FIELD("repr",repr,128) else PSET_FIELD("design",design,256)
        else PSET_FIELD("q",q,512) else PSET_FIELD("kind",kind,1024) else PSET_FIELD("order",order,2048) else PSET_FIELD("view",view,4096)
#undef PSET_FIELD
        if (!dest || (seen & bit)) return 0;
        if (bit == 512) { /* a search phrase may carry spaces; nothing else may */
            if (!query_value(p, "q", dest, cap)) dest[0] = 0;
            for (char *c = dest; *c; c++) if ((unsigned char)*c < 32) return 0;
        } else if (!psetup_decode(eq+1, (size_t)(end-eq-1), dest, cap)) return 0;
        seen |= bit; p = *end ? end+1 : end;
    }
    if (!out->context[0]) snprintf(out->context, sizeof(out->context), "self");
    if (out->pack[0] && !bfpg_token(out->pack, sizeof(out->pack))) return 0;
    if (out->version[0] && !bfpg_token(out->version, sizeof(out->version))) return 0;
    if (!bfpg_token(out->context, sizeof(out->context))) return 0;
    if (out->repr[0] && strcmp(out->repr,"API") && strcmp(out->repr,"MCP") && strcmp(out->repr,"A2A") && strcmp(out->repr,"BonfyreFS")) return 0;
    if ((out->digest[0] && (strlen(out->digest)!=64 || strspn(out->digest,"abcdef0123456789")!=64)) ||
        (out->shared[0] && (strlen(out->shared)!=64 || strspn(out->shared,"abcdef0123456789")!=64))) return 0;
    if (out->revision[0] && (strspn(out->revision,"0123456789")!=strlen(out->revision) || strlen(out->revision)>10 || strtoll(out->revision,NULL,10)>=INT_MAX)) return 0;
    if ((out->kind[0] && strcmp(out->kind,"application") && strcmp(out->kind,"configuration")) ||
        (out->order[0] && strcmp(out->order,"published") && strcmp(out->order,"name") && strcmp(out->order,"recent")) ||
        (out->view[0] && strcmp(out->view,"gallery") && strcmp(out->view,"list"))) return 0;
    if(out->design[0]){char ns[128],slug[72];if(strcmp(out->pack,"experience.connected-instruments.workspace")||!production_reference(out->design,ns,slug))return 0;}
    return 1;
}
static int psetup_context(const char *bearer, const char *auth_db, const char *context, int write,
                           char *actor, size_t actor_cap, char *subject, size_t subject_cap,
                           const char **scope) {
    StudioViewer who;
    if (!session_identity(bearer, auth_db, &who) || who.id < 1) return 0;
    snprintf(actor, actor_cap, "user:%ld", who.id);
    if (!strcmp(context, "self")) {
        if (write && !session_self_authoring(bearer, auth_db)) return -1;
        snprintf(subject, subject_cap, "%s", actor); *scope = "self"; return 1;
    }
    if (!strncmp(context, "household:", 10)) {
        sqlite3 *db=cms_open_ro();BfHouseholdAccess access;
        int permitted=bhh_context(db,context+10,actor,&access) && bhh_rank(access.role)>=(write==2?2:write?3:1);
        if(db)sqlite3_close(db);if(!permitted || (write && !session_self_authoring(bearer,auth_db)))return -1;
        snprintf(subject,subject_cap,"%s",context);*scope="household";return 1;
    }
    const char *id = !strncmp(context, "institution:", 12) ? context + 12 : NULL;
    if (!id || !*id || strlen(id)>63 || strspn(id,"abcdefghijklmnopqrstuvwxyz0123456789-")!=strlen(id) ||
        !session_institution_authorized(bearer, auth_db, id, write==2 ? "write" : write ? "admin" : "read")) return -1;
    snprintf(subject, subject_cap, "%s", context); *scope = "institution"; return 1;
}
static int psetup_parse(char *body, PackSetupResult *out) {
    memset(out, 0, sizeof(*out)); int schema = 0; char *save = NULL;
    for (char *line = strtok_r(body, "\n", &save); line; line = strtok_r(NULL,"\n",&save)) {
        char *tab = strchr(line, '\t'); if (!tab) continue; *tab++ = 0;
        if (!strcmp(line,"schema")) { if (schema++ || strcmp(tab,"bonfyre.pack.lifecycle.v1")) return 0; }
        else if (!strcmp(line,"state")) { if (strlen(tab)>=sizeof(out->state)) return 0; snprintf(out->state,sizeof(out->state),"%s",tab); }
        else if (!strcmp(line,"digest")) { if (strlen(tab)>=sizeof(out->digest)) return 0; snprintf(out->digest,sizeof(out->digest),"%s",tab); }
        else if (!strcmp(line,"pinned_digest")) { if (strlen(tab)>=sizeof(out->pinned)) return 0; snprintf(out->pinned,sizeof(out->pinned),"%s",tab); }
        else if (!strcmp(line,"version")) { if (strlen(tab)>=sizeof(out->version)) return 0; snprintf(out->version,sizeof(out->version),"%s",tab); }
        else if (!strcmp(line,"reason") || !strcmp(line,"error")) { if (strlen(tab)>=sizeof(out->reason)) return 0; snprintf(out->reason,sizeof(out->reason),"%s",tab); }
        else if (!strcmp(line,"subject")) { if (strlen(tab)>=sizeof(out->subject)) return 0; snprintf(out->subject,sizeof(out->subject),"%s",tab); }
        else if (!strcmp(line,"revision")) out->revision = strtoll(tab,NULL,10);
        else if (!strcmp(line,"installation")) out->installation = strtoll(tab,NULL,10);
        else if (!strcmp(line,"committed")) out->committed = !strcmp(tab,"1");
        else if (!strcmp(line,"pin")) {
            if (out->pins.count >= BFPG_MAX) return 0;
            BfPackResolved *n=&out->pins.nodes[out->pins.count++];char *version=strchr(tab,'\t');if(!version)return 0;*version++=0;char *root=strchr(version,'\t');if(!root)return 0;*root++=0;
            if(!bfpg_token(tab,sizeof(n->pack))||!bfpg_token(version,sizeof(n->version))||!bfpg_token(root,sizeof(n->root)))return 0;
            snprintf(n->pack,sizeof(n->pack),"%s",tab);snprintf(n->version,sizeof(n->version),"%s",version);snprintf(n->root,sizeof(n->root),"%s",root);
        }
        else if (!strcmp(line,"node")) {
            if (out->resolution.count >= BFPG_MAX) return 0;
            BfPackResolved *n=&out->resolution.nodes[out->resolution.count++];
            char *fields[6]={tab};
            for (int i=1;i<6;i++) { fields[i]=strchr(fields[i-1],'\t'); if (!fields[i]) return 0; *fields[i]++=0; }
#define PSET_COPY(f,i) if (strlen(fields[i])>=sizeof(n->f)) return 0; snprintf(n->f,sizeof(n->f),"%s",fields[i])
            PSET_COPY(pack,0); PSET_COPY(version,1); PSET_COPY(root,2); PSET_COPY(layer,3); PSET_COPY(kind,4); PSET_COPY(label,5);
#undef PSET_COPY
        }
    }
    return schema==1 && *out->state && out->revision>=0 && out->revision<INT_MAX;
}
static int psetup_call(const char *operation, const PackSetupInput *in, const char *actor,
                       const char *subject, const char *scope, PackSetupResult *out) {
    char output[65536];
    const char *bin=page_cms_env("BONFYRE_CMS_BINARY","/usr/local/bin/bonfyre-cms");
    const char *db=page_cms_env("BONFYRE_CMS_DB","/var/lib/bonfyre/cms/bonfyre_cms.db");
    char *const argv[]={(char *)bin,"pack-lifecycle","--operation",(char *)operation,"--pack",(char *)in->pack,
        "--version",(char *)in->version,"--subject",(char *)subject,"--scope",(char *)scope,"--actor",(char *)actor,
        "--expected-digest",(char *)in->digest,"--expected-revision",(char *)(in->revision[0]?in->revision:"0"),"--db",(char *)db,NULL};
    int rc=auth_call(argv,NULL,output,sizeof(output));
    return psetup_parse(output,out) ? rc : -1;
}
#include "pack_presentation.h"
/* Request-local pointers are refreshed before each synchronous dispatch.
 * No account page inherits a previous host, path or query. */
static const char *psetup_presentation_host;
static const char *psetup_presentation_route;
static const char *psetup_presentation_query;
/* Every account page here opens on the one shell: the estate bar with the
 * session filled in, a single <main>, the one footer and player. The route's
 * presentation record decorates head and body; nothing else frames the page. */
static void psetup_bar(void){
    const char *bar=BF_ESTATE_BAR,*a=strstr(bar,"<!--BF_SESSION_BEGIN-->"),*b=a?strstr(a,"<!--BF_SESSION_END-->"):NULL;
    if(!a||!b){fputs(bar,stdout);return;}
    fwrite(bar,1,(size_t)(a-bar),stdout);fputs("<!--BF_SESSION_BEGIN-->",stdout);
    account_navigation_emit(page_request_bearer,page_request_auth_db);fputs(b,stdout);
}
static void psetup_open_with_style(int code,const char *title,const char *main_class,const char *extra_style){
    printf("HTTP/1.1 %d %s\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: private, no-store\r\nReferrer-Policy: same-origin\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",code,code==200?"OK":code==403?"Forbidden":code==409?"Conflict":"Unavailable");
    const char *pattern=bf_route_presentation_pattern(psetup_presentation_host,psetup_presentation_route,psetup_presentation_query);
    char *head_extra=NULL,*body_extra=NULL;size_t head_n=0,body_n=0;
    if(pattern){
        FILE *head=open_memstream(&head_extra,&head_n),*attrs=open_memstream(&body_extra,&body_n);
        if(head&&attrs){
            const char *territory=bf_route_presentation_territory(psetup_presentation_host);
            bf_route_presentation_emit_head_for(head,territory,psetup_presentation_route,psetup_presentation_query,pattern);
            bf_route_presentation_emit_body_attribute_for(attrs,territory,psetup_presentation_route,psetup_presentation_query,pattern);
        }
        if(head)fclose(head);if(attrs)fclose(attrs);
    }
    fputs("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><meta name=\"theme-color\" content=\"#0f1511\"><meta name=\"robots\" content=\"noindex\"><title>",stdout);
    html(title);fputs(" \xc2\xb7 Aurekai</title><link rel=\"stylesheet\" href=\"/assets/authoring-reader.css\">",stdout);
    if(head_extra&&*head_extra)fputs(head_extra,stdout);else fputs("<link rel=\"stylesheet\" href=\"/assets/composition.css\">",stdout);
    if(!strcmp(main_class,"pack-workspace")) fputs("<link rel=\"stylesheet\" href=\"/assets/pack-setup.css\"><link rel=\"stylesheet\" href=\"/assets/pack-presentation.css\">",stdout);
    if(extra_style) { fputs("<link rel=\"stylesheet\" href=\"",stdout); html(extra_style); fputs("\">",stdout); }
    fputs("</head><body class=\"ak-app\"",stdout);
    if(body_extra&&*body_extra)fputs(body_extra,stdout);else fputs(" data-aurekai-pattern=\"workbench\"",stdout);
    fputs(">",stdout);free(head_extra);free(body_extra);
    psetup_bar();fputs("<main class=\"",stdout);fputs(main_class,stdout);fputs("\">",stdout);
}
static void psetup_open(int code,const char *title,const char *main_class){
    psetup_open_with_style(code,title,main_class,NULL);
}
static void psetup_start(int code, const char *title) {
    psetup_open(code,title,"pack-workspace");
}
/* The Stitch workbench layout (composition.css .ak-wb) on the same shell. */
static void psetup_start_wb(int code,const char *title){psetup_open(code,title,"ak-wb");}
static void psetup_end(void) { fputs("</main>" BF_ESTATE_FOOT "</body></html>\n",stdout); }
static void psetup_hidden(const char *name,const char *value) { fputs("<input type=\"hidden\" name=\"",stdout);html(name);fputs("\" value=\"",stdout);html(value);fputs("\">",stdout); }
static void psetup_link(const PackSetupInput *in,int share,const char *digest) {
    fputs("/packs/setup?pack=",stdout); url_query_value(in->pack);
    if (!share) { fputs("&amp;context=",stdout);url_query_value(in->context); }
    if (*in->version) { fputs("&amp;version=",stdout);url_query_value(in->version); }
    if (share && digest && *digest) { fputs("&amp;shared=",stdout);url_query_value(digest); }
    if(!share&&*in->design){fputs("&amp;design=",stdout);url_query_value(in->design);}
}
static void psetup_form(const char *op,const char *label,const PackSetupInput *in,const PackSetupResult *state,const char *digest) {
    char rev[24];snprintf(rev,sizeof(rev),"%lld",state->revision);
    fputs("<form method=\"post\" action=\"/packs/setup\">",stdout);psetup_hidden("pack",in->pack);psetup_hidden("context",in->context);psetup_hidden("version",in->version);
    psetup_hidden("operation",op);psetup_hidden("revision",rev);psetup_hidden("digest",digest);
    if(*in->design)psetup_hidden("design",in->design);
    fputs(!strcmp(op,"install")||!strcmp(op,"activate")?"<button class=\"ak-btn is-primary\" type=\"submit\" data-pack-action=\"":"<button class=\"ak-btn\" type=\"submit\" data-pack-action=\"",stdout);html(op);fputs("\">",stdout);html(label);fputs("</button></form>",stdout);
}
/* HTML and machine discovery share one bounded read of current destination
 * owners. These choices do not reserve access; each operation checks again. */
#define PSETUP_DESTINATIONS_MAX 201
typedef struct {char context[96],label[320];int can_write;} PackSetupDestination;
static int psetup_table(sqlite3 *db,const char *name){
    sqlite3_stmt *s=NULL;int found=-1;
    if(sqlite3_prepare_v2(db,"SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?1",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,name,-1,SQLITE_STATIC);
        if(sqlite3_step(s)==SQLITE_ROW){int n=sqlite3_column_int(s,0);if((n==0||n==1)&&sqlite3_step(s)==SQLITE_DONE)found=n;}
    }
    sqlite3_finalize(s);return found;
}
static int psetup_destination_row(PackSetupDestination *rows,size_t *count,sqlite3_stmt *s,const char *prefix,const char *label_prefix){
    char id[64],label[256];if(*count>=PSETUP_DESTINATIONS_MAX||!bfpg_text(s,0,id,sizeof(id))||!bhh_slug(id)||!bfpg_text(s,1,label,sizeof(label))||!*label)return 0;
    PackSetupDestination *row=&rows[*count];
    snprintf(row->context,sizeof(row->context),"%s%s",prefix,id);snprintf(row->label,sizeof(row->label),"%s%s",label_prefix,label);
    for(size_t i=0;i<*count;i++)if(!strcmp(rows[i].context,row->context))return 0;
    (*count)++;return 1;
}
/* 1 complete, 0 unauthenticated, -1 unavailable or over the collection bound. */
static int psetup_destinations(const char *bearer,const char *auth_db,PackSetupDestination *rows,size_t *count){
    StudioViewer who;sqlite3 *db=NULL;sqlite3_stmt *s=NULL;int ok=-1,step=SQLITE_DONE;size_t total=1,seen=0;
    *count=0;if(!session_identity(bearer,auth_db,&who)||who.id<1)return 0;
    memset(rows,0,sizeof(*rows)*PSETUP_DESTINATIONS_MAX);snprintf(rows[0].context,sizeof(rows[0].context),"self");snprintf(rows[0].label,sizeof(rows[0].label),"My personal workspace");
    if(sqlite3_open_v2(auth_db,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto done;
    int origins=psetup_table(db,"institution_origins");if(origins<0||psetup_table(db,"institution_grants")!=1)goto done;
    const char *institutions=origins?
        "SELECT g.institution,COALESCE(NULLIF(o.label,''),g.institution) FROM institution_grants g LEFT JOIN institution_origins o ON o.institution=g.institution WHERE g.user_id=?1 AND g.active=1 AND (g.expires_at IS NULL OR g.expires_at='' OR g.expires_at>=strftime('%Y-%m-%dT%H:%M:%SZ','now')) ORDER BY 2,g.institution LIMIT 101":
        "SELECT institution,institution FROM institution_grants WHERE user_id=?1 AND active=1 AND (expires_at IS NULL OR expires_at='' OR expires_at>=strftime('%Y-%m-%dT%H:%M:%SZ','now')) ORDER BY institution LIMIT 101";
    if(sqlite3_prepare_v2(db,institutions,-1,&s,NULL)!=SQLITE_OK)goto done;sqlite3_bind_int64(s,1,who.id);
    while((step=sqlite3_step(s))==SQLITE_ROW){if(++seen>100||!psetup_destination_row(rows,&total,s,"institution:",""))goto done;}
    if(step!=SQLITE_DONE)goto done;sqlite3_finalize(s);s=NULL;sqlite3_close(db);db=cms_open_ro();if(!db)goto done;
    int households=psetup_table(db,"household"),members=psetup_table(db,"_household_membership"),authority=psetup_table(db,"_household_authority");
    if(households<0||members<0||authority<0||members!=authority)goto done;
    if(households){
        const char *sql=members?
            "SELECT h.household_id,h.label FROM household h WHERE h.namespace='root' AND h.status='published' AND (h.steward=?1 OR EXISTS(SELECT 1 FROM _household_membership m JOIN _household_authority a ON a.household_row=m.household_row AND a.epoch=m.authority_epoch WHERE m.household_row=h.id AND m.member=?1 AND m.role IN ('read','write','admin') AND (m.expires_at=0 OR m.expires_at>CAST(strftime('%s','now') AS INTEGER)))) ORDER BY h.label,h.household_id LIMIT 101":
            "SELECT household_id,label FROM household WHERE namespace='root' AND steward=?1 AND status='published' ORDER BY label,household_id LIMIT 101";
        if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)goto done;
        char actor[160];snprintf(actor,sizeof(actor),"user:%ld",who.id);sqlite3_bind_text(s,1,actor,-1,SQLITE_STATIC);seen=0;
        while((step=sqlite3_step(s))==SQLITE_ROW){if(++seen>100||!psetup_destination_row(rows,&total,s,"household:","Household: "))goto done;}
        if(step!=SQLITE_DONE)goto done;
    }
    sqlite3_finalize(s);s=NULL;sqlite3_close(db);db=NULL;
    for(size_t i=0;i<total;i++){
        char actor[160],subject[160];const char *scope=NULL;
        int read=psetup_context(bearer,auth_db,rows[i].context,0,actor,sizeof(actor),subject,sizeof(subject),&scope);
        if(!read){ok=0;goto done;}if(read!=1)continue;
        rows[i].can_write=psetup_context(bearer,auth_db,rows[i].context,2,actor,sizeof(actor),subject,sizeof(subject),&scope)==1;
        rows[(*count)++]=rows[i];
    }
    ok=1;
 done:sqlite3_finalize(s);if(db)sqlite3_close(db);if(ok!=1)*count=0;return ok;
}
static void psetup_destination_picker_with(const PackSetupInput *in,const char *bearer,const char *auth_db,const char *target,const char *label,const char *extra_name,const char *extra_value) {
    PackSetupDestination rows[PSETUP_DESTINATIONS_MAX];size_t count=0;int result=psetup_destinations(bearer,auth_db,rows,&count);
    if(result<0)fputs("<p role=\"alert\">The complete destination list is unavailable or exceeds 100 organizations or households. Refresh after the owner is available.</p>",stdout);
    else {
        fputs("<form method=\"get\" action=\"",stdout);html(target);fputs("\"><label for=\"pack-context\">",stdout);html(label);
        if(extra_name&&extra_value)psetup_hidden(extra_name,extra_value);
        fputs("</label><br><select id=\"pack-context\" name=\"context\">",stdout);
        if(!result)fputs("<option value=\"self\">My personal workspace</option>",stdout);
        for(size_t i=0;i<count;i++){fputs("<option value=\"",stdout);html(rows[i].context);fputs(!strcmp(rows[i].context,in->context)?"\" selected>":"\">",stdout);html(rows[i].label);fputs("</option>",stdout);}
        fputs("</select>",stdout);psetup_hidden("pack",in->pack);psetup_hidden("version",in->version);fputs(" <button>Change workspace</button></form>",stdout);
    }
    fputs("<p><a data-organizations href=\"/organizations",stdout);if(*in->pack){fputs("?pack=",stdout);url_query_value(in->pack);}fputs("\">Create an organization or manage your people</a></p>",stdout);
    if(!strncmp(in->context,"household:",10)){fputs("<p><a href=\"/households/access?household=",stdout);url_query_value(in->context+10);fputs("\">Household access</a></p>",stdout);}
}
static void psetup_destination_picker(const PackSetupInput *in,const char *bearer,const char *auth_db,const char *target,const char *label) {
    psetup_destination_picker_with(in,bearer,auth_db,target,label,NULL,NULL);
}
static void psetup_context_picker(const PackSetupInput *in,const char *bearer,const char *auth_db) {
    psetup_destination_picker_with(in,bearer,auth_db,"/packs/setup","Use this configuration in",*in->design?"design":NULL,in->design);
}
/* Published previews are separate from configuration ownership. Never append
 * actor, session, installation, draft or permission coordinates to these links. */
static void psetup_preview(const char *pack) {
    sqlite3 *db=cms_open_ro();sqlite3_stmt *s=NULL;
    if(db&&sqlite3_prepare_v2(db,
        "SELECT needs,COALESCE(preferred_sources,''),COALESCE(fields,'') FROM pack_demand "
        "WHERE namespace='root' AND pack=?1 AND demand_kind='public-preview' AND status='published' ORDER BY id LIMIT 8",
        -1,&s,NULL)==SQLITE_OK) {
        sqlite3_bind_text(s,1,pack,-1,SQLITE_TRANSIENT);
        while(sqlite3_step(s)==SQLITE_ROW) {
            const char *url=(const char *)sqlite3_column_text(s,0),*label=(const char *)sqlite3_column_text(s,1),*kind=(const char *)sqlite3_column_text(s,2);
            if(!url||strncmp(url,"https://",8)||!public_http_url(url)||strchr(url,'@')||strchr(url,'?')||strchr(url,'#')||strchr(url,'\\'))continue;
            if(!kind||(strcmp(kind,"demo")&&strcmp(kind,"guide")))continue;
            fputs("<section class=\"ak-panel ak-sec-panel\" data-pack-preview><h2>",stdout);
            fputs(!strcmp(kind,"guide")?"Application guide":"Published demo",stdout);
            fputs("</h2><p>",stdout);html(label);
            fputs("</p><a class=\"pack-link\" data-pack-preview-link target=\"_blank\" rel=\"noopener noreferrer\" href=\"",stdout);html(url);
            fputs("\">",stdout);fputs(!strcmp(kind,"guide")?"Read the application guide":"Open the published demo",stdout);fputs("</a><p>",stdout);
            fputs(!strcmp(kind,"guide")?"This page explains the application; it is not an installed customer workspace.":"The demo keeps its existing storage and review behavior. Installing this configuration does not transfer its drafts or connect shared editing.",stdout);
            fputs("</p></section>",stdout);
        }
    }
    sqlite3_finalize(s);if(db)sqlite3_close(db);
}
/* The tool directory (Stitch: tool packs workspace setup). One bounded read of
 * published PackGraph artifacts, filtered and ordered on the server; each card
 * links to the reviewed setup, which is the only path that installs anything. */
static void psetup_listing_link(const PackSetupInput *in,const char *kind,const char *order,const char *view){
    fputs("/packs/setup?context=",stdout);url_query_value(in->context);
    if(*in->q){fputs("&amp;q=",stdout);url_query_value(in->q);}
    if(kind&&*kind){fputs("&amp;kind=",stdout);url_query_value(kind);}
    if(order&&*order&&strcmp(order,"published")){fputs("&amp;order=",stdout);url_query_value(order);}
    if(view&&*view&&strcmp(view,"gallery")){fputs("&amp;view=",stdout);url_query_value(view);}
}
static void psetup_option(const char *value,const char *current,const char *label){
    fputs("<option value=\"",stdout);html(value);fputs(!strcmp(value,current)?"\" selected>":"\">",stdout);html(label);fputs("</option>",stdout);
}
static void psetup_chain(sqlite3 *db,const char *pack){
    sqlite3_stmt *s=NULL;int n=0;
    if(sqlite3_prepare_v2(db,"SELECT COALESCE(a.canonical_name,d.depends_on) FROM pack_dependency d LEFT JOIN pack_artifact a ON a.namespace='root' AND a.status='published' AND a.pack=d.depends_on WHERE d.namespace='root' AND d.status='published' AND d.pack=?1 ORDER BY d.id LIMIT 7",-1,&s,NULL)==SQLITE_OK){
        sqlite3_bind_text(s,1,pack,-1,SQLITE_TRANSIENT);
        while(sqlite3_step(s)==SQLITE_ROW){const char *t=(const char*)sqlite3_column_text(s,0);if(!t)continue;
            fputs(n++?" <i>\xe2\x86\x92</i> ":"<p class=\"ak-pack-chain\"><span>Builds on</span> ",stdout);html(t);}
    }
    sqlite3_finalize(s);if(n)fputs("</p>",stdout);
}
static void psetup_directory(const PackSetupInput *in,const char *bearer,const char *auth_db,int identity,const char *actor,const char *subject,const char *scope){
    const char *kind=in->kind,*order=*in->order?in->order:"published",*view=*in->view?in->view:"gallery";
    sqlite3 *db=cms_open_ro();sqlite3_stmt *s=NULL;
    const char *sql=
        "SELECT a.pack,a.canonical_name,COALESCE(a.purpose,''),a.kind='application',COALESCE(h.version,''),a.kind "
        "FROM pack_artifact a LEFT JOIN pack_head h ON h.namespace='root' AND h.status='published' AND h.pack=a.pack AND h.channel='stable' "
        "WHERE a.namespace='root' AND a.status='published' "
        "AND (?1='' OR instr(lower(a.canonical_name||' '||COALESCE(a.purpose,'')||' '||a.pack),lower(?1))>0) "
        "AND (?2='' OR (?2='application')=(a.kind='application')) "
        "ORDER BY 4 DESC,CASE ?3 WHEN 'recent' THEN a.updated_at END DESC,CASE ?3 WHEN 'name' THEN a.canonical_name END,a.id,a.pack LIMIT 101";
    if(!db||sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK){
        sqlite3_finalize(s);if(db)sqlite3_close(db);
        psetup_start_wb(503,"Tools unavailable");fputs("<header class=\"ak-wb-hero is-solo\"><div><p class=\"ak-kicker\">Workspace setup / Tools</p><h1>The configuration owner could not be read</h1><p class=\"ak-lede\">Try again before choosing a tool. Nothing you already set up has changed.</p></div></header>",stdout);psetup_end();return;
    }
    sqlite3_bind_text(s,1,in->q,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,kind,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,order,-1,SQLITE_TRANSIENT);
    long long total=0;{sqlite3_stmt *c=NULL;if(sqlite3_prepare_v2(db,"SELECT count(*) FROM pack_artifact WHERE namespace='root' AND status='published'",-1,&c,NULL)==SQLITE_OK&&sqlite3_step(c)==SQLITE_ROW)total=sqlite3_column_int64(c,0);sqlite3_finalize(c);}
    psetup_start_wb(200,"Choose tools for your work");
    fputs("<div class=\"ak-packs\" data-pack-view=\"",stdout);html(view);fputs("\"><header class=\"ak-wb-hero ak-packs-hero\"><div><p class=\"ak-kicker\">Workspace setup <span>/</span> Tools</p>"
          "<h1>Choose tools for your work</h1><p class=\"ak-lede\">Applications open an editor once you have reviewed and switched them on. Workflow configurations keep a plan and what it needs.</p></div>"
          "<p class=\"ak-packs-count\"><span class=\"ak-live\">",stdout);printf("%lld",total);fputs(" published</span> <small>Each one is checked again before it changes anything</small></p></header>",stdout);
    /* Where the tools land: the destination picker is the same owner read the setup review uses. */
    fputs("<section class=\"ak-packs-rail\" aria-label=\"Workspace\"><div class=\"ak-packs-where\">",stdout);
    psetup_destination_picker_with(in,bearer,auth_db,"/packs/setup","Use these tools in",NULL,NULL);
    fputs("</div><nav class=\"ak-packs-links\"><a data-setup-existing href=\"/apps?context=",stdout);url_query_value(in->context);fputs("\">Open your installed tools and saved work</a></nav></section>",stdout);
    /* Filters are a plain GET form; the server does the filtering. */
    fputs("<form class=\"ak-packs-filter\" method=\"get\" action=\"/packs/setup\" role=\"search\"><input type=\"hidden\" name=\"context\" value=\"",stdout);html(in->context);
    fputs("\"><label class=\"ak-packs-find\"><span class=\"ak-visually-hidden\">Find a tool</span><input type=\"search\" name=\"q\" maxlength=\"96\" placeholder=\"Find a tool\xe2\x80\xa6\" value=\"",stdout);html(in->q);
    fputs("\"></label><label><span>Type</span><select name=\"kind\">",stdout);psetup_option("",kind,"All types");psetup_option("application",kind,"Applications");psetup_option("configuration",kind,"Workflow configurations");
    fputs("</select></label><label><span>Order</span><select name=\"order\">",stdout);psetup_option("published",order,"Published order");psetup_option("name",order,"Alphabetical");psetup_option("recent",order,"Recently updated");
    fputs("</select></label><input type=\"hidden\" name=\"view\" value=\"",stdout);html(view);fputs("\"><button class=\"ak-btn\" type=\"submit\">Apply</button><span class=\"ak-packs-views\" role=\"group\" aria-label=\"View\"><a href=\"",stdout);
    psetup_listing_link(in,kind,order,"gallery");fputs(strcmp(view,"list")?"\" aria-current=\"true\">Gallery</a><a href=\"":"\">Gallery</a><a href=\"",stdout);
    psetup_listing_link(in,kind,order,"list");fputs(!strcmp(view,"list")?"\" aria-current=\"true\">List</a></span></form>":"\">List</a></span></form>",stdout);
    int group=-1,step=SQLITE_DONE,section=0;size_t shown=0;
    while((step=sqlite3_step(s))==SQLITE_ROW&&shown<100){
        const char *pack=(const char *)sqlite3_column_text(s,0);if(!bfpg_token(pack,96))continue;
        int application=sqlite3_column_int(s,3);PackSetupInput choice=*in;choice.q[0]=choice.kind[0]=choice.order[0]=choice.view[0]=0;
        snprintf(choice.pack,sizeof(choice.pack),"%s",pack);
        if(group!=application){
            if(group!=-1)fputs("</div></section>",stdout);
            printf("<section class=\"ak-sec ak-packs-sec\" data-setup-kind=\"%s\"><header><div><span class=\"ak-kicker\">%02d</span><h2>%s</h2></div><p>%s</p></header><div class=\"ak-grid ak-packs-grid\">",
                application?"application":"configuration",++section,application?"Applications":"Workflow configurations",
                application?"Review the editor release and choose where its records belong.":"Reusable behaviour. Installing a plan does not provision an application or connect an outside account.");
            group=application;
        }
        const char *label=(const char*)sqlite3_column_text(s,1),*purpose=(const char*)sqlite3_column_text(s,2),*version=(const char*)sqlite3_column_text(s,4),*raw_kind=(const char*)sqlite3_column_text(s,5);
        fputs("<article class=\"ak-panel ak-pack\" data-pack=\"",stdout);html(pack);fputs("\"><header><span class=\"ak-tag",stdout);fputs(application?" is-on\">Application":"\">Workflow configuration",stdout);
        fputs("</span><span class=\"ak-pack-glyph\" aria-hidden=\"true\">",stdout);page_glyph(bf_pack_icon(pack,raw_kind?raw_kind:""));fputs("</span></header><h3>",stdout);html(label?label:pack);fputs("</h3>",stdout);
        if(!application)psetup_chain(db,pack);
        if(purpose&&*purpose){fputs("<p>",stdout);html(purpose);fputs("</p>",stdout);}
        if(application&&identity==1){PackSetupResult existing={0};if(!psetup_call("current",&choice,actor,subject,scope,&existing)&&!strcmp(existing.state,"active")){fputs("<div class=\"ak-pack-open\">",stdout);workspace_editor_links(existing.installation);fputs("</div>",stdout);}}
        fputs("<footer><small>",stdout);if(version&&*version){fputs("Release ",stdout);char v[9];snprintf(v,sizeof(v),"%s",version);html(v);}else fputs("No stable release",stdout);
        fputs("</small><a class=\"ak-btn",stdout);fputs(application?" is-primary":"",stdout);fputs("\" data-setup-review href=\"",stdout);psetup_link(&choice,0,NULL);
        fputs(application?"\">Review application setup</a></footer></article>":"\">Review workflow configuration</a></footer></article>",stdout);
        shown++;
    }
    if(group!=-1)fputs("</div></section>",stdout);
    if(step!=SQLITE_DONE&&step!=SQLITE_ROW)fputs("<p class=\"ak-empty\" role=\"alert\">The tool list was interrupted. Reload before choosing.</p>",stdout);
    else if(!shown)fputs(*in->q||*kind?"<p class=\"ak-empty\">No published tool matches this search.</p>":"<p class=\"ak-empty\">No tools are published here yet.</p>",stdout);
    sqlite3_finalize(s);sqlite3_close(db);
    fputs("<p class=\"ak-packs-foot\"><span class=\"ak-live\">Showing up to 100 published tools. Setup checks the chosen release and your current access before anything changes.</span></p></div>",stdout);
    psetup_end();
}
static int serve_pack_setup(const char *method,const char *target,const char *query,const char *body,
                            const char *raw,const char *host,const char *bearer,const char *auth_db) {
    if(strcmp(target,"/packs/setup"))return 0;
    int post=!strcmp(method,"POST");
    if(!post&&strcmp(method,"GET")){protocol_reply(405,"text/plain","Use GET to review or POST to change setup.\n");return 1;}
    PackSetupInput in;PackSetupResult plan={0},current={0};char actor[160]="public",subject[160]="public";const char *scope="self";
    if(!psetup_input(post?body:query,&in)){protocol_reply(400,"text/plain","The setup request is malformed or contains duplicate fields.\n");return 1;}
    int identity=psetup_context(bearer,auth_db,in.context,post,actor,sizeof(actor),subject,sizeof(subject),&scope);
    if(identity<0){protocol_reply(403,"text/plain","This account does not have the required workspace permission.\n");return 1;}
    if(post){
        char origin[512]={0},https[512],http[512];const char *p=raw?strstr(raw,"\nOrigin:"):NULL;if(!p&&raw)p=strstr(raw,"\norigin:");
        if(p)sscanf(p+8," %511[^\r\n]",origin);snprintf(https,sizeof(https),"https://%s",host?host:"");snprintf(http,sizeof(http),"http://%s",host?host:"");
        if(!identity||!p||(strcmp(origin,https)&&strcmp(origin,http))||!in.pack[0]||!in.revision[0]||!in.digest[0]||
           (strcmp(in.operation,"install")&&strcmp(in.operation,"activate")&&strcmp(in.operation,"deactivate")&&strcmp(in.operation,"remove"))){protocol_reply(403,"text/plain","Sign in and submit the reviewed form from this workspace.\n");return 1;}
        char checked_actor[160],checked_subject[160];const char *checked_scope;
        if(psetup_context(bearer,auth_db,in.context,1,checked_actor,sizeof(checked_actor),checked_subject,sizeof(checked_subject),&checked_scope)!=1||strcmp(actor,checked_actor)||strcmp(subject,checked_subject)){protocol_reply(403,"text/plain","Your workspace permission changed.\n");return 1;}
        if(!strcmp(in.operation,"install")){
            PackSetupResult checked;if(psetup_call("plan",&in,actor,subject,scope,&checked)||!workspace_plan_available(&checked.resolution)){
                protocol_reply(409,"text/plain","The reviewed editor release is not available. No installation or draft was changed.\n");return 1;}
        }
        int rc=psetup_call(in.operation,&in,actor,subject,scope,&plan);
        if(rc==0&&plan.committed){
            if(*in.design&&!strcmp(in.operation,"activate")){fputs("HTTP/1.1 303 See Other\r\nLocation: /cms/productions?context=",stdout);url_query_value(in.context);fputs("&design=",stdout);url_query_value(in.design);}
            else{fputs("HTTP/1.1 303 See Other\r\nLocation: /packs/setup?pack=",stdout);url_query_value(in.pack);fputs("&context=",stdout);url_query_value(in.context);if(*in.version){fputs("&version=",stdout);url_query_value(in.version);}if(*in.design){fputs("&design=",stdout);url_query_value(in.design);}}
            fputs("\r\nCache-Control: no-store\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",stdout);return 1;}
        psetup_start_wb(rc==2&&plan.committed?200:409,"Review setup again");fputs("<header class=\"ak-wb-hero is-solo\"><div><p class=\"ak-kicker\">Workspace setup</p><h1>",stdout);fputs(plan.committed?"Saved; a downstream refresh needs attention":"This setup was not changed",stdout);fputs("</h1><p class=\"ak-lede\">",stdout);html(plan.reason[0]?plan.reason:"The content owner could not confirm the operation.");fputs("</p><p class=\"ak-actions\"><a class=\"ak-btn is-primary\" href=\"",stdout);psetup_link(&in,0,NULL);fputs("\">Review the current configuration</a></p></div></header>",stdout);psetup_end();return 1;
    }
    if(!in.pack[0]){psetup_directory(&in,bearer,auth_db,identity,actor,subject,scope);return 1;}
    int result=psetup_call("plan",&in,actor,subject,scope,&plan);
    int installed=plan.installation>0,current_ok=0;
    if(identity==1&&installed)current_ok=psetup_call("current",&in,actor,subject,scope,&current)==0;
    const char *state=installed?plan.state:"absent";
    int index=bfpg_find(&plan.resolution,in.pack);const char *label=index>=0?plan.resolution.nodes[index].label:in.pack;
    const char *state_words=!strcmp(state,"absent")?"Not installed":!strcmp(state,"active")?"Switched on":!strcmp(state,"stale")?"Source changed; review again":!strcmp(state,"removed")?"Removed":"Installed; not yet switched on";
    /* Stitch: the setup workbench. Strip, hero with the reviewed actions,
     * instrument row, the included-behaviour ledger beside the configuration,
     * and the fingerprint band. Every action is the existing reviewed POST. */
    psetup_start_wb(result<0?503:200,label);
    fputs("<div class=\"ak-wb-strip\"><a href=\"/packs/setup?context=",stdout);url_query_value(in.context);fputs("\">Workspace setup</a><span>/</span><a href=\"/apps?context=",stdout);url_query_value(in.context);
    fputs("\">Your work</a><span class=\"ak-live\">",stdout);html(state_words);fputs("</span></div>",stdout);
    fputs("<header class=\"ak-wb-hero\"><div><p class=\"ak-kicker\">Set up a tool</p><h1>",stdout);html(label);fputs("</h1><p class=\"ak-lede\">Review exactly what this release includes, choose where its records belong, then install and switch it on. Nothing changes until you press a button below.</p>",stdout);
    if(*in.version){PackSetupInput latest=in;latest.version[0]='\0';fputs("<p class=\"ak-actions\"><a class=\"ak-btn is-quiet\" data-pack-latest href=\"",stdout);psetup_link(&latest,0,NULL);fputs("\">Review the latest release</a></p>",stdout);}
    fputs("</div><aside class=\"ak-panel is-raised\" data-pack-config><h2>Your configuration</h2><p class=\"pack-status\" data-pack-state=\"",stdout);html(state);fputs("\"><span class=\"ak-dot",stdout);fputs(strcmp(state,"active")?" is-off":"",stdout);fputs("\">",stdout);html(state_words);fputs("</span></p>",stdout);
    if(!identity){fputs("<p>Sign in to keep this configuration in your workspace.</p><p class=\"ak-actions\"><a class=\"ak-btn is-primary\" data-pack-signin href=\"/login?next=",stdout);char back[1024];snprintf(back,sizeof(back),"/packs/setup?pack=%s&version=%s&shared=%s&context=%s%s%s",in.pack,in.version,in.shared,in.context,*in.design?"&design=":"",in.design);url_query_value(back);fputs("\">Sign in or create an account</a></p>",stdout);}
    else{
        char a[160],sb[160];const char *sc;int canwrite=psetup_context(bearer,auth_db,in.context,1,a,sizeof(a),sb,sizeof(sb),&sc)==1;
        if(!canwrite)fputs("<p>You can review this workspace. An administrator must change its configuration.</p>",stdout);
        else{
            fputs("<div class=\"ak-actions ak-pack-ops\">",stdout);
            if(result==0&&(!installed||!strcmp(state,"removed")||strcmp(plan.digest,plan.pinned)))psetup_form("install",installed?"Install reviewed configuration":"Install for this workspace",&in,&plan,plan.digest);
            if(current_ok&&!strcmp(current.state,"installed"))psetup_form("activate","Switch on installed behaviour",&in,&current,current.pinned);
            if(installed&&strcmp(state,"removed")){psetup_form("deactivate","Switch off",&in,&plan,plan.pinned);psetup_form("remove","Remove installation",&in,&plan,plan.pinned);}
            fputs("</div>",stdout);
        }
        if(current_ok&&!strcmp(current.state,"active")){workspace_editor_links(current.installation);fputs("<p><a data-pack-current href=\"/packs/closure?pack=",stdout);url_query_value(in.pack);fputs("&amp;context=",stdout);url_query_value(in.context);fputs("\">Inspect the active behaviour</a></p>",stdout);}
    }
    fputs("</aside></header>",stdout);
    /* Instrument row: counts read from the plan the owner just resolved. */
    size_t changed=0,added=0;
    if(!result){for(size_t i=0;i<plan.resolution.count;i++){int old=bfpg_find(&plan.pins,plan.resolution.nodes[i].pack);if(old<0)added++;else if(strcmp(plan.pins.nodes[old].version,plan.resolution.nodes[i].version))changed++;}}
    fputs("<dl class=\"ak-stats is-row\"><div><dt>Included behaviour</dt><dd>",stdout);if(result)fputs("\xe2\x80\x94",stdout);else printf("%zu",plan.resolution.count);
    fputs("</dd><small>packages in this release</small></div><div><dt>New here</dt><dd>",stdout);if(result)fputs("\xe2\x80\x94",stdout);else printf("%zu",added);
    fputs("</dd><small>not yet in this workspace</small></div><div><dt>Changed</dt><dd>",stdout);if(result)fputs("\xe2\x80\x94",stdout);else printf("%zu",changed);
    fputs("</dd><small>installed at another version</small></div><div><dt>Status</dt><dd class=\"",stdout);fputs(!strcmp(state,"active")?"is-on":"",stdout);fputs("\">",stdout);html(!strcmp(state,"active")?"On":installed&&strcmp(state,"removed")?"Installed":"Off");fputs("</dd><small>",stdout);html(state_words);fputs("</small></div></dl>",stdout);
    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">Where it lands</span><h2>Workspace</h2></div></header><div class=\"ak-panel ak-packs-where\">",stdout);psetup_context_picker(&in,bearer,auth_db);fputs("</div></section>",stdout);
    fputs("<section class=\"ak-sec\"><header><div><span class=\"ak-kicker\">Review</span><h2>What is included</h2></div><p>Installing keeps these version references and creates any editor this release declares. Published source material stays shared; your drafts stay in your workspace. It never connects an outside account, books a service or publishes a merchant change.</p></header><div class=\"ak-panel ak-scroll\">",stdout);
    if(result){fputs("<p class=\"pack-warning\" role=\"alert\">This configuration is incomplete: ",stdout);html(plan.reason[0]?plan.reason:"The content owner is unavailable.");if(*plan.subject){fputs(" (",stdout);html(plan.subject);fputs(")",stdout);}fputs(". Nothing can be installed from this review.</p>",stdout);}
    else{
        if(*in.shared&&strcmp(in.shared,plan.digest))fputs("<p class=\"pack-warning\" role=\"alert\">The shared configuration has changed since the link was made. Review the current version below before installing it.</p>",stdout);
        fputs("<table class=\"ak-table\"><thead><tr><th>Included behaviour</th><th>Installed</th><th>Reviewed</th></tr></thead><tbody>",stdout);
        for(size_t i=0;i<plan.resolution.count;i++){BfPackResolved *n=&plan.resolution.nodes[i];int old=bfpg_find(&plan.pins,n->pack);fputs("<tr><td>",stdout);html(n->label);fputs("<br><code>",stdout);html(n->pack);fputs("</code></td><td>",stdout);html(old>=0?plan.pins.nodes[old].version:"Not installed");fputs("</td><td>",stdout);html(n->version);fputs("</td></tr>",stdout);}
        for(size_t i=0;i<plan.pins.count;i++)if(bfpg_find(&plan.resolution,plan.pins.nodes[i].pack)<0){fputs("<tr><td>",stdout);html(plan.pins.nodes[i].pack);fputs("</td><td>",stdout);html(plan.pins.nodes[i].version);fputs("</td><td>Removed in this review</td></tr>",stdout);}
        fputs("</tbody></table>",stdout);
    }
    fputs("</div></section>",stdout);
    psetup_preview(in.pack);
    fputs("<section class=\"ak-band ak-pack-proof\"><div><p class=\"ak-kicker\">Review fingerprint</p><h2>",stdout);
    if(result)fputs("No fingerprint: this review is incomplete",stdout);else{fputs("<code>",stdout);html(plan.digest);fputs("</code>",stdout);}
    fputs("</h2><p>A shared link carries the published version and this fingerprint. Whoever opens it chooses their own workspace.</p></div>",stdout);
    if(result==0){PackSetupInput shared=in;snprintf(shared.version,sizeof(shared.version),"%s",plan.version);fputs("<a class=\"ak-btn pack-link\" data-pack-share href=\"",stdout);psetup_link(&shared,1,plan.digest);fputs("\">Open reusable link</a>",stdout);}
    fputs("</section>",stdout);
    psetup_end();return 1;
}
/* Supply the existing PackGraph renderer with exactly the installed closure.
 * The outer CMS read transaction also pins overlay reads to this verification. */
static int psetup_scoped_closure(sqlite3 *db,const char *query,const char *bearer,const char *auth_db,
                                 PackNode *nodes,int *count,char *actor,size_t actor_cap) {
    char probe[128];
    if(!query_value(query,"context",probe,sizeof(probe))&&!query_value(query,"pack",probe,sizeof(probe)))return 0;
    PackSetupInput in;PackSetupResult current;char subject[160];const char *scope;
    if(!psetup_input(query,&in)||!in.pack[0]||in.operation[0]||in.digest[0]||in.revision[0]||in.shared[0]){protocol_reply(400,"text/plain","Choose an installed configuration and workspace.\n");return -1;}
    int admitted=psetup_context(bearer,auth_db,in.context,0,actor,actor_cap,subject,sizeof(subject),&scope);
    if(admitted!=1){protocol_reply(403,"text/plain","Sign in with access to this workspace.\n");return -1;}
    int rc=psetup_call("current",&in,actor,subject,scope,&current);
    if(rc!=0||strcmp(current.state,"active")){protocol_reply(409,"text/plain","This configuration is inactive or its installed sources changed. Review setup before use.\n");return -1;}
    BfPackResolution verified;
    if(!bfpg_resolve(db,in.pack,current.version,&current.resolution,&verified)||strcmp(verified.digest,current.pinned)){
        protocol_reply(409,"text/plain","The configuration changed during resolution. Review setup again.\n");return -1;
    }
    *count=(int)verified.count;
    for(int i=0;i<*count;i++){
        BfPackResolved *n=&verified.nodes[i];PackNode *p=&nodes[i];memset(p,0,sizeof(*p));
        if(strlen(n->root)>=sizeof(p->root)||strlen(n->version)>=sizeof(p->version)){protocol_reply(409,"text/plain","The renderer cannot retain this version coordinate.\n");return -1;}
        snprintf(p->pack,sizeof(p->pack),"%s",n->pack);snprintf(p->root,sizeof(p->root),"%s",n->root);snprintf(p->version,sizeof(p->version),"%s",n->version);
        snprintf(p->kind,sizeof(p->kind),"%s",n->kind);snprintf(p->layer,sizeof(p->layer),"%s",n->layer);p->rank=pack_layer_rank(p->layer);p->active=!strcmp(n->pack,in.pack);
    }
    return 1;
}
#endif
