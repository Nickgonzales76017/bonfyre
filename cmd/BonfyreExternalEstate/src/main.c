#define _POSIX_C_SOURCE 200809L
#include <curl/curl.h>
#include <sqlite3.h>
#include <stdint.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <bonfyre.h>
#include <bf_admission.h>
#include <bf_command_lookup.h>

#include "bf_lex.h"
#include "address_resolve.h"
/* The native candidate reservoir is deliberately narrow: it imports observed
 * source occurrences and CUE-compiled presentation demand, performs only
 * deterministic candidate selection, and emits a fragment. It cannot fetch a
 * source, admit a provider, alter a Pack, or cross an authority boundary. */

static const char *arg(int argc, char **argv, const char *name) {
    int i; for (i=2; i+1<argc; i++) if (!strcmp(argv[i], name)) return argv[i+1];
    return NULL;
}
static void esc(const char *s) {
    for (; s && *s; s++) {
        if (*s=='&') fputs("&amp;", stdout); else if (*s=='<') fputs("&lt;", stdout);
        else if (*s=='>') fputs("&gt;", stdout); else if (*s=='\"') fputs("&quot;", stdout);
        else putchar(*s);
    }
}
/* Delegates to the one implementation. Nineteen copies of this shape existed
 * with sixteen different opinions about escaping.
 * This copy was one of the four defects: it escaped only the quote, the backslash and the two newline bytes, and
 * passed every other control character through raw -- so a tab or a \x01 in an
 * advertisement term produced a document no JSON parser would accept.
 */
static void json_text(const char *s) {
    bf_lex_json_emit(stdout, s);
}
static void query_esc(const char *s) {
    static const char hex[]="0123456789ABCDEF";
    for (; s && *s; s++) { unsigned char c=(unsigned char)*s;
      if ((c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='-'||c=='_'||c=='.'||c=='~') putchar(c);
      else { putchar('%'); putchar(hex[c>>4]); putchar(hex[c&15]); }
    }
}
static int exec_sql(sqlite3 *db, const char *sql) {
    char *error=NULL; int rc=sqlite3_exec(db,sql,NULL,NULL,&error);
    if (rc!=SQLITE_OK) { fprintf(stderr,"sqlite: %s\n", error?error:"error"); sqlite3_free(error); }
    return rc==SQLITE_OK;
}
static char *read_all(const char *path, size_t *size) {
    FILE *f=fopen(path,"rb"); long n; char *buf;
    if (!f) return NULL;
    if (fseek(f,0,SEEK_END) || (n=ftell(f))<0 || fseek(f,0,SEEK_SET)) { fclose(f); return NULL; }
    buf=malloc((size_t)n+1); if (!buf) { fclose(f); return NULL; }
    if ((size_t)n != fread(buf,1,(size_t)n,f)) { free(buf); fclose(f); return NULL; }
    buf[n]='\0'; fclose(f); if(size)*size=(size_t)n; return buf;
}

/* The OwnerContract image is a compiled freshness boundary, not a source of
 * authority. AddressPlane may use it only to reject an unresolved or
 * unrealized composition before it replaces derived advertisements. Exact
 * owner facts are always read later through the declared verifier. */
static int owner_contract_image_valid(const char *path) {
    char *body = NULL;
    size_t size = 0;
    int ok = 0;
    if (!path || !*path || !(body = read_all(path, &size)) || !size) goto done;
    ok = strstr(body, "\"schema\":\"bonfyre-owner-contract-image.v1\"") &&
         strstr(body, "\"state\":\"compiled_reference_image\"") &&
         strstr(body, "\"unresolved\":0") &&
         strstr(body, "\"realized_unmapped\":0");
done:
    free(body);
    return ok;
}

typedef struct { char *body; size_t used,cap; long status; } McpHttp;
static size_t mcp_collect(void *ptr,size_t size,size_t nmemb,void *opaque) {
    McpHttp *out=opaque;size_t n=size*nmemb,want;
    if(n>8*1024*1024-out->used)return 0;
    want=out->used+n+1;if(want>out->cap){size_t cap=out->cap?out->cap:4096;char *next;while(cap<want)cap*=2;next=realloc(out->body,cap);if(!next)return 0;out->body=next;out->cap=cap;}
    memcpy(out->body+out->used,ptr,n);out->used+=n;out->body[out->used]='\0';return n;
}
static char *read_secret_file(const char *path) {
    char *value;size_t n=0;
    if(!(value=read_all(path,&n)))return NULL;
    while(n&&isspace((unsigned char)value[n-1]))value[--n]='\0';
    if(!n){free(value);return NULL;}return value;
}
static int mcp_list_call(const char *url,const char *token,const char *method,int id,char **result) {
    CURL *curl=NULL;struct curl_slist *headers=NULL;McpHttp out={0};char request[512],auth[4096],method_header[384],name_header[384];CURLcode rc;int ok=0;
    /* MCP 2026-07-28 is stateless: this refresh carries its protocol version
     * and client identity on every request.  It builds a disposable list
     * snapshot only; a later owner/effect gate still authorizes each call. */
    *result=NULL;if(!url||!token||!method||snprintf(request,sizeof(request),"{\"jsonrpc\":\"2.0\",\"id\":%d,\"method\":\"%s\",\"params\":{\"_meta\":{\"io.modelcontextprotocol/clientInfo\":{\"name\":\"bonfyre-external-estate\",\"version\":\"1\"}}}}",id,method)>=(int)sizeof(request)||snprintf(auth,sizeof(auth),"Authorization: Bearer %s",token)>=(int)sizeof(auth)||snprintf(method_header,sizeof(method_header),"Mcp-Method: %s",method)>=(int)sizeof(method_header)||snprintf(name_header,sizeof(name_header),"Mcp-Name: %s",method)>=(int)sizeof(name_header))return 0;
    if(!(curl=curl_easy_init()))return 0;
    headers=curl_slist_append(headers,"Content-Type: application/json");headers=curl_slist_append(headers,"Accept: application/json");headers=curl_slist_append(headers,"MCP-Protocol-Version: 2026-07-28");headers=curl_slist_append(headers,method_header);headers=curl_slist_append(headers,name_header);headers=curl_slist_append(headers,auth);
    curl_easy_setopt(curl,CURLOPT_URL,url);curl_easy_setopt(curl,CURLOPT_POST,1L);curl_easy_setopt(curl,CURLOPT_POSTFIELDS,request);curl_easy_setopt(curl,CURLOPT_POSTFIELDSIZE,(long)strlen(request));curl_easy_setopt(curl,CURLOPT_HTTPHEADER,headers);curl_easy_setopt(curl,CURLOPT_TIMEOUT,20L);curl_easy_setopt(curl,CURLOPT_CONNECTTIMEOUT,5L);curl_easy_setopt(curl,CURLOPT_PROTOCOLS_STR,"http,https");curl_easy_setopt(curl,CURLOPT_WRITEFUNCTION,mcp_collect);curl_easy_setopt(curl,CURLOPT_WRITEDATA,&out);
    rc=curl_easy_perform(curl);curl_easy_getinfo(curl,CURLINFO_RESPONSE_CODE,&out.status);if(rc==CURLE_OK&&out.status>=200&&out.status<300&&out.body){*result=out.body;out.body=NULL;ok=1;}
    free(out.body);curl_slist_free_all(headers);curl_easy_cleanup(curl);return ok;
}
static int mcp_array(sqlite3 *db,const char *response,const char *path,char **out) {
    sqlite3_stmt *st=NULL;int ok=0;*out=NULL;
    if(sqlite3_prepare_v2(db,"SELECT json_extract(?1,?2) WHERE json_valid(?1) AND json_type(?1,?2)='array'",-1,&st,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(st,1,response,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,path,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)==SQLITE_ROW&&sqlite3_column_text(st,0)){*out=strdup((const char*)sqlite3_column_text(st,0));ok=*out!=NULL;}sqlite3_finalize(st);return ok;
}
/* Refresh an authenticated machine aperture through its public protocol, not
 * gateway source inspection.  The resulting snapshot is a disposable derived
 * artifact; authorization still occurs when MCP later receives a call. */
static int refresh_mcp_closure(const char *url,const char *token_path,const char *output) {
    char *token=NULL,*tools=NULL,*resources=NULL,*templates=NULL,*prompts=NULL,*a=NULL,*b=NULL,*c=NULL,*d=NULL;sqlite3 *db=NULL;char tmp[4096]={0};int fd=-1,ok=0;FILE *f=NULL;
    if(!url||!token_path||!output||!(token=read_secret_file(token_path)))goto done;
    if(curl_global_init(CURL_GLOBAL_DEFAULT)!=CURLE_OK)goto done;
    if(!mcp_list_call(url,token,"tools/list",1,&a)||!mcp_list_call(url,token,"resources/list",2,&b)||!mcp_list_call(url,token,"resources/templates/list",3,&c)||!mcp_list_call(url,token,"prompts/list",4,&d)||sqlite3_open(":memory:",&db)!=SQLITE_OK)goto done;
    if(!mcp_array(db,a,"$.result.tools",&tools)||!mcp_array(db,b,"$.result.resources",&resources)||!mcp_array(db,c,"$.result.resourceTemplates",&templates)||!mcp_array(db,d,"$.result.prompts",&prompts))goto done;
    if(snprintf(tmp,sizeof(tmp),"%s.tmp.%ld",output,(long)getpid())>=(int)sizeof(tmp))goto done;
    fd=open(tmp,O_WRONLY|O_CREAT|O_TRUNC,0640);if(fd<0)goto done;if(!(f=fdopen(fd,"wb"))){close(fd);fd=-1;goto done;}fd=-1;
    fprintf(f,"{\"tools\":%s,\"resources\":%s,\"resourceTemplates\":%s,\"prompts\":%s}\n",tools,resources,templates,prompts);
    if(fflush(f)||fsync(fileno(f))||fclose(f)){f=NULL;unlink(tmp);goto done;}f=NULL;
    if(rename(tmp,output)){unlink(tmp);goto done;}printf("{\"mcp_closure\":\"refreshed\",\"truth\":\"advertisement_snapshot_only\"}\n");ok=1;
done:if(f)fclose(f);else if(fd>=0)close(fd);if(!ok&&tmp[0])unlink(tmp);if(db)sqlite3_close(db);curl_global_cleanup();free(token);free(tools);free(resources);free(templates);free(prompts);free(a);free(b);free(c);free(d);return ok;
}
static int table_has_column(sqlite3 *db, const char *table, const char *column);

static int init_schema(sqlite3 *db) {
    int ok = exec_sql(db,
      "PRAGMA journal_mode=WAL;"
      "CREATE TABLE IF NOT EXISTS occurrence("
      " id TEXT PRIMARY KEY,observed_name TEXT NOT NULL,source_ref TEXT NOT NULL,"
      " primary_url TEXT NOT NULL,source_kind TEXT NOT NULL,section TEXT NOT NULL,raw_json TEXT NOT NULL);"
      "CREATE TABLE IF NOT EXISTS occurrence_temporal("
      " occurrence_id TEXT PRIMARY KEY,state TEXT NOT NULL CHECK(state IN ('bearing','not_evident')),basis TEXT NOT NULL) WITHOUT ROWID;"
      "CREATE TABLE IF NOT EXISTS occurrence_temporal_mode("
      " occurrence_id TEXT NOT NULL,mode TEXT NOT NULL CHECK(mode IN ('feed','live_update','history','forecast','schedule','telemetry')),"
      " PRIMARY KEY(occurrence_id,mode)) WITHOUT ROWID;"
      "CREATE INDEX IF NOT EXISTS occurrence_temporal_mode_lookup ON occurrence_temporal_mode(mode,occurrence_id);"
      "CREATE TABLE IF NOT EXISTS occurrence_term(occurrence_id TEXT NOT NULL,axis TEXT NOT NULL,term TEXT NOT NULL,"
      " PRIMARY KEY(occurrence_id,axis,term)) WITHOUT ROWID;"
      "CREATE INDEX IF NOT EXISTS occurrence_term_lookup ON occurrence_term(axis,term,occurrence_id);"
      /* AddressPlane's derived forwarding index.  It is rebuilt from the
       * canonical occurrence terms, so it owns no source fact.  Terms are
       * assigned deterministically into 62-bit SQLite-safe words; a caller
       * may cheaply reject impossible candidates before any exact owner read. */
      "CREATE TABLE IF NOT EXISTS address_bit(axis TEXT NOT NULL,term TEXT NOT NULL,word INTEGER NOT NULL,bit INTEGER NOT NULL,"
      " PRIMARY KEY(axis,term)) WITHOUT ROWID;"
      "CREATE TABLE IF NOT EXISTS candidate_mask(occurrence_id TEXT NOT NULL,word INTEGER NOT NULL,provides INTEGER NOT NULL,"
      " PRIMARY KEY(occurrence_id,word)) WITHOUT ROWID;"
      "CREATE INDEX IF NOT EXISTS candidate_mask_word ON candidate_mask(word);"
      /* CandidateAdvertisement is the non-owning common forwarding record.
       * It carries coordinates and immutable owner references only.  Exact
       * state remains in the named owner and must be checked after this cut. */
      "CREATE TABLE IF NOT EXISTS candidate_advertisement("
      " identity TEXT PRIMARY KEY,candidate_kind TEXT NOT NULL,owner_ref TEXT NOT NULL,"
      " exact_verifier TEXT NOT NULL,lowerer_ref TEXT NOT NULL,validity_frontier TEXT NOT NULL DEFAULT '',"
      " canonical_json TEXT NOT NULL) WITHOUT ROWID;"
      "CREATE TABLE IF NOT EXISTS candidate_advertisement_term("
      " identity TEXT NOT NULL,axis TEXT NOT NULL,term TEXT NOT NULL,"
      " PRIMARY KEY(identity,axis,term),"
      " FOREIGN KEY(identity) REFERENCES candidate_advertisement(identity)) WITHOUT ROWID;"
      "CREATE INDEX IF NOT EXISTS candidate_advertisement_term_lookup ON candidate_advertisement_term(axis,term,identity);"
      "CREATE TABLE IF NOT EXISTS candidate_advertisement_mask("
      " identity TEXT NOT NULL,word INTEGER NOT NULL,provides INTEGER NOT NULL,"
      " PRIMARY KEY(identity,word),"
      " FOREIGN KEY(identity) REFERENCES candidate_advertisement(identity)) WITHOUT ROWID;"
      "CREATE INDEX IF NOT EXISTS candidate_advertisement_mask_word ON candidate_advertisement_mask(word);"
      /* Dependencies are declared addresses, not materialized owner facts.
       * FactDelta later intersects these rows to find a cut. */
      "CREATE TABLE IF NOT EXISTS candidate_advertisement_dependency("
      " identity TEXT NOT NULL,dependency_ref TEXT NOT NULL,"
      " PRIMARY KEY(identity,dependency_ref),"
      " FOREIGN KEY(identity) REFERENCES candidate_advertisement(identity)) WITHOUT ROWID;"
      "CREATE INDEX IF NOT EXISTS candidate_advertisement_dependency_lookup ON candidate_advertisement_dependency(dependency_ref,identity);"
      /* Durable hand-off from a FactDelta cut to the candidate's declared
       * lowerer.  This queue owns neither semantic execution nor effects. */
      "CREATE TABLE IF NOT EXISTS candidate_lowering_queue("
      " delta_id TEXT NOT NULL,identity TEXT NOT NULL,candidate_kind TEXT NOT NULL,"
      " owner_ref TEXT NOT NULL,lowerer_ref TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'queued',"
      " causal_writes_json TEXT NOT NULL DEFAULT '[]',"
      " queued_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),"
      " PRIMARY KEY(delta_id,identity)) WITHOUT ROWID;"
      "CREATE TABLE IF NOT EXISTS address_delta_receipt("
      " delta_id TEXT PRIMARY KEY,source_path TEXT NOT NULL,processed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),"
      " affected_count INTEGER NOT NULL) WITHOUT ROWID;"
      /* ExperienceMap is a compiled, read-only projection of this exact
       * occurrence reservoir.  Keeping it adjacent to the occurrence—not in
       * a browser catalogue—lets a human surface use the deep chains without
       * pretending that a chain admits, authorizes, or invokes a source. */
      "CREATE TABLE IF NOT EXISTS occurrence_experience(occurrence_id TEXT PRIMARY KEY,experience_json TEXT NOT NULL,stage_json TEXT NOT NULL DEFAULT '{}') WITHOUT ROWID;"
      "CREATE TABLE IF NOT EXISTS experience_chain(chain_id TEXT PRIMARY KEY,chain_json TEXT NOT NULL) WITHOUT ROWID;"
      "CREATE TABLE IF NOT EXISTS presentation_cell(grammar TEXT NOT NULL,presentation_class TEXT NOT NULL,cell_json TEXT NOT NULL,"
      " PRIMARY KEY(grammar,presentation_class)) WITHOUT ROWID;"
      "CREATE TABLE IF NOT EXISTS presentation_demand(grammar TEXT NOT NULL,presentation_class TEXT NOT NULL,axis TEXT NOT NULL,term TEXT NOT NULL,"
      " PRIMARY KEY(grammar,presentation_class,axis,term)) WITHOUT ROWID;"
      "CREATE INDEX IF NOT EXISTS presentation_demand_lookup ON presentation_demand(grammar,presentation_class,axis,term);");
    /* Additive queue migration: old address indexes retain their completed
     * cuts, while all newly published handoffs preserve their causal writes. */
    if (ok && !table_has_column(db, "candidate_lowering_queue", "causal_writes_json"))
        ok = exec_sql(db, "ALTER TABLE candidate_lowering_queue ADD COLUMN causal_writes_json TEXT NOT NULL DEFAULT '[]';");
    return ok;
}

/* Reproducible, non-owning physical index over all addressed populations.
 * `address_bit` is intentionally the one vocabulary shared by external
 * occurrences and every CandidateAdvertisement.  A bit is never a fact,
 * permission, proof, or cross-kind coercion. */
static int rebuild_forwarding_index(sqlite3 *db) {
    return exec_sql(db,
      "DELETE FROM candidate_mask;DELETE FROM candidate_advertisement_mask;DELETE FROM address_bit;"
      "INSERT INTO address_bit(axis,term,word,bit) "
      "SELECT axis,term,(ordinal-1)/62,(ordinal-1)%62 FROM ("
      " SELECT axis,term,row_number() OVER (ORDER BY axis,term) AS ordinal FROM ("
      "  SELECT axis,term FROM occurrence_term UNION SELECT axis,term FROM candidate_advertisement_term));"
      "INSERT INTO candidate_mask(occurrence_id,word,provides) "
      "SELECT t.occurrence_id,b.word,sum(1 << b.bit) FROM occurrence_term t "
      "JOIN address_bit b ON b.axis=t.axis AND b.term=t.term "
      "GROUP BY t.occurrence_id,b.word;"
      "INSERT INTO candidate_advertisement_mask(identity,word,provides) "
      "SELECT t.identity,b.word,sum(1 << b.bit) FROM candidate_advertisement_term t "
      "JOIN address_bit b ON b.axis=t.axis AND b.term=t.term "
      "GROUP BY t.identity,b.word;");
}
static int rebuild_temporal_classification(sqlite3 *db) {
    return exec_sql(db,
      "DELETE FROM occurrence_temporal_mode;DELETE FROM occurrence_temporal;"
      "DROP VIEW IF EXISTS temp.temporal_source_text;"
      "CREATE TEMP VIEW temporal_source_text AS SELECT id,source_kind,lower(observed_name||' '||section||' '||coalesce(json_extract(raw_json,'$.raw_source_text'),'')) AS text FROM occurrence;"
      "INSERT INTO occurrence_temporal_mode SELECT id,'feed' FROM temporal_source_text WHERE source_kind='rss';"
      "INSERT OR IGNORE INTO occurrence_temporal_mode SELECT id,'live_update' FROM temporal_source_text WHERE text LIKE '%real-time%' OR text LIKE '%realtime%' OR text LIKE '%real time%' OR text LIKE '%websocket%' OR text LIKE '%web socket%' OR text LIKE '%websub%' OR text LIKE '%mqtt%' OR text LIKE '%server-sent events%' OR text LIKE '%eventsource%' OR text LIKE '%event stream%' OR text LIKE '%event-stream%' OR text LIKE '%notification%' OR text LIKE '%alert%' OR text LIKE '%updated every%' OR text LIKE '%updates every%' OR text LIKE '%refresh every%';"
      "INSERT OR IGNORE INTO occurrence_temporal_mode SELECT id,'history' FROM temporal_source_text WHERE text LIKE '%historical%' OR text LIKE '%time series%' OR text LIKE '%time-series%' OR text LIKE '%timeseries%';"
      "INSERT OR IGNORE INTO occurrence_temporal_mode SELECT id,'forecast' FROM temporal_source_text WHERE text LIKE '%forecast%' OR text LIKE '%nowcast%';"
      "INSERT OR IGNORE INTO occurrence_temporal_mode SELECT id,'schedule' FROM temporal_source_text WHERE text LIKE '%schedule%' OR text LIKE '%calendar%';"
      "INSERT OR IGNORE INTO occurrence_temporal_mode SELECT id,'telemetry' FROM temporal_source_text WHERE text LIKE '%telemetry%';"
      "INSERT INTO occurrence_temporal(occurrence_id,state,basis) SELECT id,CASE WHEN EXISTS(SELECT 1 FROM occurrence_temporal_mode m WHERE m.occurrence_id=temporal_source_text.id) THEN 'bearing' ELSE 'not_evident' END,CASE WHEN EXISTS(SELECT 1 FROM occurrence_temporal_mode m WHERE m.occurrence_id=temporal_source_text.id) THEN 'source_description_or_kind' ELSE 'listing_no_temporal_evidence' END FROM temporal_source_text;"
      "DROP VIEW temporal_source_text;");
}
static int import_occurrences(sqlite3 *db, const char *path) {
    FILE *f=fopen(path,"r"); char *line=NULL; size_t cap=0; ssize_t n; sqlite3_stmt *base=NULL,*terms=NULL; int rows=0;
    if (!f) { perror(path); return 0; }
    if (!exec_sql(db,"BEGIN IMMEDIATE;DELETE FROM occurrence_term;DELETE FROM occurrence;")) { fclose(f); return 0; }
    if (sqlite3_prepare_v2(db,
      "INSERT INTO occurrence(id,observed_name,source_ref,primary_url,source_kind,section,raw_json) "
      "SELECT json_extract(?1,'$.id'),json_extract(?1,'$.observed_name'),json_extract(?1,'$.source_ref'),"
      "json_extract(?1,'$.primary_url'),json_extract(?1,'$.source_kind'),json_extract(?1,'$.section'),?1",-1,&base,NULL)!=SQLITE_OK) goto fail;
    if (sqlite3_prepare_v2(db,
      "INSERT OR IGNORE INTO occurrence_term(occurrence_id,axis,term) "
      "SELECT json_extract(?1,'$.id'),?2,lower(value) FROM json_each(?1,?3)",-1,&terms,NULL)!=SQLITE_OK) goto fail;
    while ((n=getline(&line,&cap,f))>=0) {
      if (n && line[n-1]=='\n') line[--n]='\0';
      if (!n) continue;
      sqlite3_bind_text(base,1,line,n,SQLITE_TRANSIENT);
      if (sqlite3_step(base)!=SQLITE_DONE) goto fail;
      sqlite3_reset(base); sqlite3_clear_bindings(base);
      /* Human narrowing uses direct topical material, never broad reuse or a
       * carrier default. Those wider fields remain in raw provenance for a
       * later Pack/World compiler, but cannot make a protocol/toolchain row
       * masquerade as a weather, care, or travel result. */
      const char *axes[][2]={{"domain","$.projection.topic_domains"},{"affordance","$.projection.candidate_affordances"},{"world","$.projection.candidate_worlds"},{"interaction","$.projection.interaction_fits"},{"role","$.projection.estate_roles"}};
      for (size_t i=0;i<sizeof(axes)/sizeof(*axes);i++) {
        sqlite3_bind_text(terms,1,line,n,SQLITE_TRANSIENT); sqlite3_bind_text(terms,2,axes[i][0],-1,SQLITE_STATIC); sqlite3_bind_text(terms,3,axes[i][1],-1,SQLITE_STATIC);
        if (sqlite3_step(terms)!=SQLITE_DONE) goto fail;
        sqlite3_reset(terms); sqlite3_clear_bindings(terms);
      }
      /* source kind is a declared SurfaceDemand axis too.  It was previously
       * present in the row but absent from the ranking index, which made the
       * CUE demand table only partially executable. */
      sqlite3_bind_text(terms,1,line,n,SQLITE_TRANSIENT); sqlite3_bind_text(terms,2,"source_kind",-1,SQLITE_STATIC); sqlite3_bind_text(terms,3,"$.source_kind",-1,SQLITE_STATIC);
      if (sqlite3_step(terms)!=SQLITE_DONE) goto fail;
      sqlite3_reset(terms); sqlite3_clear_bindings(terms);
      /* Whether a source publishes position at all.  This was a literal
       * "unknown" on every occurrence and is now classified from what each
       * source says about itself, so the plane can ask which of the estate is
       * even worth pointing a spatial intake at.  `bearing` is evidence in a
       * description and not an observation: it names no position, admits no
       * source, and is not a claim that the source is reachable. */
      sqlite3_bind_text(terms,1,line,n,SQLITE_TRANSIENT); sqlite3_bind_text(terms,2,"spatial",-1,SQLITE_STATIC); sqlite3_bind_text(terms,3,"$.projection.spatial_candidate",-1,SQLITE_STATIC);
      if (sqlite3_step(terms)!=SQLITE_DONE) goto fail;
      sqlite3_reset(terms); sqlite3_clear_bindings(terms);
      rows++;
    }
    sqlite3_finalize(base); sqlite3_finalize(terms); free(line); fclose(f);
    /* Replace the generator's historical literal temporal `unknown` with an
     * owner-local classification.  This is not forwarded as an AddressPlane
     * axis until that vocabulary is declared in CUE. */
    if (!rebuild_temporal_classification(db)) { exec_sql(db,"ROLLBACK;"); return 0; }
    /* Only bearing temporal modes receive a forwarding coordinate.
     * `not_evident` remains epistemic absence rather than temporal:false. */
    if (!exec_sql(db,
      "INSERT OR IGNORE INTO occurrence_term(occurrence_id,axis,term) "
      "SELECT occurrence_id,'temporal',mode FROM occurrence_temporal_mode;")) {
        exec_sql(db,"ROLLBACK;"); return 0;
    }
    /* ExternalOccurrence remains the canonical reservoir.  These are only
     * forwarding descriptors derived from it, so the full 4,891-source
     * population also participates in the common typed API without copying a
     * body or manufacturing provider/evidence/authority state. */
    if (!exec_sql(db,
      "DELETE FROM candidate_advertisement_dependency WHERE identity IN (SELECT identity FROM candidate_advertisement WHERE owner_ref LIKE 'ExternalEstate:%');"
      "DELETE FROM candidate_advertisement_term WHERE identity IN (SELECT identity FROM candidate_advertisement WHERE owner_ref LIKE 'ExternalEstate:%');"
      "DELETE FROM candidate_advertisement WHERE owner_ref LIKE 'ExternalEstate:%';"
      "INSERT INTO candidate_advertisement(identity,candidate_kind,owner_ref,exact_verifier,lowerer_ref,validity_frontier,canonical_json) "
      "SELECT id,'external_occurrence','ExternalEstate:'||id,'ForeignTwin:admission','MaterialCompiler:external','',json_object('identity',id,'candidate_kind','external_occurrence','owner_ref','ExternalEstate:'||id) FROM occurrence;"
      "INSERT INTO candidate_advertisement_term(identity,axis,term) "
      "SELECT o.id,t.axis,t.term FROM occurrence o JOIN occurrence_term t ON t.occurrence_id=o.id;"
      "INSERT INTO candidate_advertisement_dependency(identity,dependency_ref) "
      "SELECT id,'ExternalEstate:'||id FROM occurrence;"
      /* A ForeignTwin receipt names its observed topic, not an occurrence id.
       * Retain both declared dependencies so its later FactDelta can wake the
       * exact source candidates without turning a successful observation into
       * a fact about any semantic owner. */
      "INSERT OR IGNORE INTO candidate_advertisement_dependency(identity,dependency_ref) "
      "SELECT id,'ForeignTwin:'||primary_url FROM occurrence;")) return 0;
    /* The registry is lexical only for stable reproducibility.  It has no
     * semantic interpretation: exact owner verification still happens after
     * this forwarding cut.  62 bits avoids SQLite's signed top-bit behavior. */
    if (!rebuild_forwarding_index(db)) return 0;
    if (!exec_sql(db,"COMMIT;")) return 0;
    printf("{\"imported_occurrences\":%d}\n",rows); return 1;
fail:
    fprintf(stderr,"import occurrence: %s\n",sqlite3_errmsg(db)); sqlite3_finalize(base); sqlite3_finalize(terms); free(line); fclose(f); exec_sql(db,"ROLLBACK;"); return 0;
}

/* Import the deep ExperienceMap projection by exact ExternalOccurrence id.
 * These JSONL rows are compiler output, not a second source of facts: each
 * names the source's specific human/protocol programs, public homes and
 * reusable role.  They make the existing work visible without allowing it to
 * manufacture a provider, private closure, payment, or effect. */
static int import_experience(sqlite3 *db, const char *path, const char *stage_path, const char *chain_path) {
    FILE *f=NULL; char *line=NULL; size_t cap=0; ssize_t n; sqlite3_stmt *base=NULL,*terms=NULL; int rows=0;
    if (!path) return 1; /* compatibility import for focused native tests */
    if (!(f=fopen(path,"r"))) { perror(path); return 0; }
    if (!exec_sql(db,"BEGIN IMMEDIATE;DELETE FROM occurrence_experience;DELETE FROM experience_chain;")) { fclose(f); return 0; }
    if (sqlite3_prepare_v2(db,"INSERT INTO occurrence_experience(occurrence_id,experience_json) SELECT json_extract(?1,'$.id'),?1",-1,&base,NULL)!=SQLITE_OK) goto fail;
    if (sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO occurrence_term(occurrence_id,axis,term) SELECT json_extract(?1,'$.id'),?2,lower(value) FROM json_each(?1,?3)",-1,&terms,NULL)!=SQLITE_OK) goto fail;
    while ((n=getline(&line,&cap,f))>=0) {
      const char *axes[][2]={{"chain","$.human_chains"},{"protocol","$.protocol_chains"},{"territory","$.territories"},{"home","$.home_blueprints"},{"mode","$.presentation_modes"},{"role","$.reagent_roles"},{"domain","$.direct_domains"},{"world","$.candidate_worlds"}};
      if (n && line[n-1]=='\n') line[--n]='\0';
      if (!n) continue;
      sqlite3_bind_text(base,1,line,n,SQLITE_TRANSIENT);
      if (sqlite3_step(base)!=SQLITE_DONE) goto fail;
      sqlite3_reset(base); sqlite3_clear_bindings(base);
      for (size_t i=0;i<sizeof(axes)/sizeof(*axes);i++) {
        sqlite3_bind_text(terms,1,line,n,SQLITE_TRANSIENT); sqlite3_bind_text(terms,2,axes[i][0],-1,SQLITE_STATIC); sqlite3_bind_text(terms,3,axes[i][1],-1,SQLITE_STATIC);
        if (sqlite3_step(terms)!=SQLITE_DONE) goto fail;
        sqlite3_reset(terms); sqlite3_clear_bindings(terms);
      }
      rows++;
    }
    sqlite3_finalize(base); base=NULL; sqlite3_finalize(terms); terms=NULL; free(line); line=NULL; fclose(f); f=NULL;
    if (stage_path && *stage_path) {
      sqlite3_stmt *stage=NULL;
      if (!(f=fopen(stage_path,"r"))) { perror(stage_path); goto fail; }
      if (sqlite3_prepare_v2(db,"UPDATE occurrence_experience SET stage_json=?1 WHERE occurrence_id=json_extract(?1,'$.id')",-1,&stage,NULL)!=SQLITE_OK) goto fail;
      while ((n=getline(&line,&cap,f))>=0) {
        if (n && line[n-1]=='\n') line[--n]='\0';
        if (!n) continue;
        sqlite3_bind_text(stage,1,line,n,SQLITE_TRANSIENT);
        if (sqlite3_step(stage)!=SQLITE_DONE) { sqlite3_finalize(stage); goto fail; }
        sqlite3_reset(stage); sqlite3_clear_bindings(stage);
      }
      sqlite3_finalize(stage); free(line); line=NULL; fclose(f); f=NULL;
    }
    if (chain_path && *chain_path) {
      sqlite3_stmt *chain_stmt=NULL;
      if (!(f=fopen(chain_path,"r"))) { perror(chain_path); goto fail; }
      if (sqlite3_prepare_v2(db,"INSERT INTO experience_chain(chain_id,chain_json) SELECT json_extract(?1,'$.id'),?1",-1,&chain_stmt,NULL)!=SQLITE_OK) goto fail;
      while ((n=getline(&line,&cap,f))>=0) {
        if (n && line[n-1]=='\n') line[--n]='\0';
        if (!n) continue;
        sqlite3_bind_text(chain_stmt,1,line,n,SQLITE_TRANSIENT);
        if (sqlite3_step(chain_stmt)!=SQLITE_DONE) { sqlite3_finalize(chain_stmt); goto fail; }
        sqlite3_reset(chain_stmt); sqlite3_clear_bindings(chain_stmt);
      }
      sqlite3_finalize(chain_stmt); free(line); line=NULL; fclose(f); f=NULL;
    }
    if (!exec_sql(db,"COMMIT;")) return 0;
    printf("{\"imported_experience_bindings\":%d}\n",rows); return rows>0;
fail:
    fprintf(stderr,"import experience: %s\n",sqlite3_errmsg(db));
    sqlite3_finalize(base); sqlite3_finalize(terms); free(line); if(f)fclose(f); exec_sql(db,"ROLLBACK;"); return 0;
}
static int import_cells(sqlite3 *db, const char *path) {
    size_t n=0; char *json=read_all(path,&n); sqlite3_stmt *cell=NULL,*demand=NULL; int rows=0;
    if (!json) { perror(path); return 0; }
    if (!exec_sql(db,"BEGIN IMMEDIATE;DELETE FROM presentation_demand;DELETE FROM presentation_cell;")) { free(json); return 0; }
    if (sqlite3_prepare_v2(db,"INSERT INTO presentation_cell(grammar,presentation_class,cell_json) "
      "SELECT json_extract(value,'$.grammar'),json_extract(value,'$.presentation_class'),value FROM json_each(?1)",-1,&cell,NULL)!=SQLITE_OK) goto fail;
    sqlite3_bind_text(cell,1,json,(int)n,SQLITE_TRANSIENT); if(sqlite3_step(cell)!=SQLITE_DONE) goto fail;
    sqlite3_finalize(cell); cell=NULL;
    if (sqlite3_prepare_v2(db,"INSERT INTO presentation_demand(grammar,presentation_class,axis,term) "
      "SELECT json_extract(c.value,'$.grammar'),json_extract(c.value,'$.presentation_class'),"
      "substr(d.value,1,instr(d.value,':')-1),lower(substr(d.value,instr(d.value,':')+1)) "
      "FROM json_each(?1) c,json_each(c.value,'$.external_demand_roles') d WHERE instr(d.value,':')>1",-1,&demand,NULL)!=SQLITE_OK) goto fail;
    sqlite3_bind_text(demand,1,json,(int)n,SQLITE_TRANSIENT); if(sqlite3_step(demand)!=SQLITE_DONE) goto fail;
    sqlite3_finalize(demand); demand=NULL; free(json);
    if (!exec_sql(db,"COMMIT;")) return 0;
    if (sqlite3_prepare_v2(db,"SELECT count(*) FROM presentation_cell",-1,&cell,NULL)==SQLITE_OK && sqlite3_step(cell)==SQLITE_ROW) rows=sqlite3_column_int(cell,0);
    /* The caller supplies a cell population; sixty was one historical pack,
     * not the contract for importing a valid smaller or larger population. */
    sqlite3_finalize(cell); printf("{\"imported_presentation_cells\":%d}\n",rows); return rows>0;
fail:
    fprintf(stderr,"import cell: %s\n",sqlite3_errmsg(db)); sqlite3_finalize(cell); sqlite3_finalize(demand); free(json); exec_sql(db,"ROLLBACK;"); return 0;
}
static int valid_token(const char *s) { const char *p=s; if(!p||!*p)return 0; for (;*p;p++) if (!((*p>='a'&&*p<='z')||(*p>='0'&&*p<='9')||*p=='_'||*p=='-')) return 0; return 1; }

typedef struct { int word; sqlite3_int64 mask; } AddressNeedWord;
typedef struct { char axis[48]; char term[160]; } AddressTerm;

static int address_axis_valid(const char *axis);
static int address_term(const char *raw, AddressTerm *out) {
    const char *colon; size_t a, t;
    if (!raw || !(colon = strchr(raw, ':'))) return 0;
    a = (size_t)(colon - raw); t = strlen(colon + 1);
    if (!a || a >= sizeof(out->axis) || !t || t >= sizeof(out->term)) return 0;
    memcpy(out->axis, raw, a); out->axis[a] = '\0';
    memcpy(out->term, colon + 1, t + 1);
    for (char *p = out->axis; *p; ++p) *p = (char)tolower((unsigned char)*p);
    for (char *p = out->term; *p; ++p) *p = (char)tolower((unsigned char)*p);
    return valid_token(out->axis) && valid_token(out->term) && address_axis_valid(out->axis);
}

/* The kind and axis vocabularies are declared in
 * schemas/control/address-advertisement.cue and mirrored here.  Two
 * independently maintained copies is how a plane starts answering with terms
 * no owner meant, so `kinds` prints both sets and the estate witness proves
 * they are identical to the CUE declaration. */
static const char *const bf_candidate_kinds[] = {
    "surface_renderer", "site_presentation", "site_program", "semantic_slot", "application_binding",
    "world_publication", "source_collection", "external_source", "capability_program", "experience_stage",
    "pack", "pack_demand", "relation", "social_presence", "information_policy", "spatial_observation", "coverage_volume",
    "mcp_projection", "protocol_pattern", "protocol_chain",
    "carrier", "provider", "compute", "transformation", "target_obligation",
    "external_occurrence", "media_library", "creator_channel", "interaction_grammar", "projection"
};

/* An axis says which question a value answers.  It never says the answer is
 * available, permitted, affordable or true: `authority` is a requirement and
 * not a grant, `audience` is a declared disclosure surface and not a
 * disclosure decision, `carrier` is a candidate and not a running protocol. */
static const char *const bf_address_axes[] = {
    "cms", "subject", "subject_kind", "institution", "world", "site", "library", "territory", "domain", "source_kind", "affordance", "interaction",
    "presentation", "profile", "renderer", "grammar", "extent", "slot", "density", "projection", "experience", "application", "binding",
    "source", "source_collection", "material", "refresh", "temporal",
    "operation", "consumes", "produces", "transition", "condition",
    "carrier", "protocol", "requirement", "authority", "scope", "purpose", "target", "admission",
    "audience", "community", "member", "role", "state", "surface", "disclosure", "field", "evidence",
    "pack", "capability_program", "demand", "regime", "reheat",
    "spatial", "mobility", "scale", "region", "cell", "geometry_kind", "mode", "derivation",
    "provider", "compute", "representation", "transform", "geometry", "version", "exactness", "measurement", "mcp",
    /* what a public estate's stated terms allow or require (source-passport.cue) */
    "reuse"
};

static int candidate_kind_valid(const char *kind) {
    if (!valid_token(kind)) return 0;
    for (size_t i=0;i<sizeof(bf_candidate_kinds)/sizeof(*bf_candidate_kinds);i++) if (!strcmp(kind,bf_candidate_kinds[i])) return 1;
    return 0;
}

static int address_axis_valid(const char *axis) {
    if (!valid_token(axis)) return 0;
    for (size_t i=0;i<sizeof(bf_address_axes)/sizeof(*bf_address_axes);i++) if (!strcmp(axis,bf_address_axes[i])) return 1;
    return 0;
}

/* Print the declared vocabulary so a witness can compare it with the CUE
 * declaration rather than scraping this source file. */
static void print_vocabulary(void) {
    for (size_t i=0;i<sizeof(bf_candidate_kinds)/sizeof(*bf_candidate_kinds);i++) printf("kind %s\n",bf_candidate_kinds[i]);
    for (size_t i=0;i<sizeof(bf_address_axes)/sizeof(*bf_address_axes);i++) printf("axis %s\n",bf_address_axes[i]);
}

/* A generic compiler intake.  Each line is a reproducible projection emitted
 * by a canonical owner/compiler, never a state copy.  The required shape is:
 * {identity,candidate_kind,owner_ref,exact_verifier,lowerer_ref,
 *  validity_frontier?,terms:["axis:term"],dependencies:["address",...]}
 * The compiler rejects an unknown candidate class and malformed coordinates
 * before an address bit can be minted. */
static int import_advertisements(sqlite3 *db, const char *path, const char *replace_owner, const char *replace_owner_prefix) {
    FILE *f=NULL; char *line=NULL; size_t cap=0; ssize_t n; int rows=0;
    sqlite3_stmt *fields=NULL,*insert=NULL,*term_insert=NULL,*dep_insert=NULL;
    if (!path || !(f=fopen(path,"r"))) { if(path)perror(path); return 0; }
    if (!exec_sql(db,"BEGIN IMMEDIATE;")) { fclose(f); return 0; }
    if (replace_owner && *replace_owner) {
        if (sqlite3_prepare_v2(db,
            "DELETE FROM candidate_advertisement_dependency WHERE identity IN (SELECT identity FROM candidate_advertisement WHERE owner_ref=?)",-1,&fields,NULL)!=SQLITE_OK) goto fail;
        sqlite3_bind_text(fields,1,replace_owner,-1,SQLITE_TRANSIENT);
        if(sqlite3_step(fields)!=SQLITE_DONE) goto fail;
        sqlite3_finalize(fields); fields=NULL;
        if (sqlite3_prepare_v2(db,"DELETE FROM candidate_advertisement_term WHERE identity IN (SELECT identity FROM candidate_advertisement WHERE owner_ref=?)",-1,&fields,NULL)!=SQLITE_OK) goto fail;
        sqlite3_bind_text(fields,1,replace_owner,-1,SQLITE_TRANSIENT);
        if(sqlite3_step(fields)!=SQLITE_DONE) goto fail;
        sqlite3_finalize(fields); fields=NULL;
        if (sqlite3_prepare_v2(db,"DELETE FROM candidate_advertisement WHERE owner_ref=?",-1,&fields,NULL)!=SQLITE_OK) goto fail;
        sqlite3_bind_text(fields,1,replace_owner,-1,SQLITE_TRANSIENT);
        if(sqlite3_step(fields)!=SQLITE_DONE) goto fail;
        sqlite3_finalize(fields); fields=NULL;
    }
    if (replace_owner_prefix && *replace_owner_prefix) {
        char like[640];
        if(strlen(replace_owner_prefix)+2>=sizeof(like))goto fail;
        snprintf(like,sizeof(like),"%s%%",replace_owner_prefix);
        if (sqlite3_prepare_v2(db,"DELETE FROM candidate_advertisement_dependency WHERE identity IN (SELECT identity FROM candidate_advertisement WHERE owner_ref LIKE ? ESCAPE '\\')",-1,&fields,NULL)!=SQLITE_OK) goto fail;
        sqlite3_bind_text(fields,1,like,-1,SQLITE_TRANSIENT);if(sqlite3_step(fields)!=SQLITE_DONE)goto fail;sqlite3_finalize(fields);fields=NULL;
        if (sqlite3_prepare_v2(db,"DELETE FROM candidate_advertisement_term WHERE identity IN (SELECT identity FROM candidate_advertisement WHERE owner_ref LIKE ? ESCAPE '\\')",-1,&fields,NULL)!=SQLITE_OK) goto fail;
        sqlite3_bind_text(fields,1,like,-1,SQLITE_TRANSIENT);if(sqlite3_step(fields)!=SQLITE_DONE)goto fail;sqlite3_finalize(fields);fields=NULL;
        if (sqlite3_prepare_v2(db,"DELETE FROM candidate_advertisement WHERE owner_ref LIKE ? ESCAPE '\\'",-1,&fields,NULL)!=SQLITE_OK) goto fail;
        sqlite3_bind_text(fields,1,like,-1,SQLITE_TRANSIENT);if(sqlite3_step(fields)!=SQLITE_DONE)goto fail;sqlite3_finalize(fields);fields=NULL;
    }
    if (sqlite3_prepare_v2(db,
        "SELECT json_extract(?1,'$.identity'),json_extract(?1,'$.candidate_kind'),"
        "json_extract(?1,'$.owner_ref'),json_extract(?1,'$.exact_verifier'),"
        "json_extract(?1,'$.lowerer_ref'),COALESCE(json_extract(?1,'$.validity_frontier'),'')",-1,&fields,NULL)!=SQLITE_OK) goto fail;
    if (sqlite3_prepare_v2(db,
        "INSERT INTO candidate_advertisement(identity,candidate_kind,owner_ref,exact_verifier,lowerer_ref,validity_frontier,canonical_json) "
        "VALUES(?,?,?,?,?,?,?) ON CONFLICT(identity) DO UPDATE SET candidate_kind=excluded.candidate_kind,owner_ref=excluded.owner_ref,exact_verifier=excluded.exact_verifier,lowerer_ref=excluded.lowerer_ref,validity_frontier=excluded.validity_frontier,canonical_json=excluded.canonical_json",-1,&insert,NULL)!=SQLITE_OK) goto fail;
    if (sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO candidate_advertisement_term(identity,axis,term) VALUES(?,?,?)",-1,&term_insert,NULL)!=SQLITE_OK) goto fail;
    if (sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO candidate_advertisement_dependency(identity,dependency_ref) VALUES(?,?)",-1,&dep_insert,NULL)!=SQLITE_OK) goto fail;
    while ((n=getline(&line,&cap,f))>=0) {
        const char *identity,*kind,*owner,*verifier,*lowerer,*frontier;
        char identity_copy[512],kind_copy[64],owner_copy[512],verifier_copy[512],lowerer_copy[512],frontier_copy[512];
        sqlite3_stmt *items=NULL;
        if(n && line[n-1]=='\n')line[--n]='\0'; if(!n)continue;
        sqlite3_bind_text(fields,1,line,n,SQLITE_TRANSIENT);
        if(sqlite3_step(fields)!=SQLITE_ROW) goto fail;
        identity=(const char*)sqlite3_column_text(fields,0); kind=(const char*)sqlite3_column_text(fields,1);
        owner=(const char*)sqlite3_column_text(fields,2); verifier=(const char*)sqlite3_column_text(fields,3);
        lowerer=(const char*)sqlite3_column_text(fields,4); frontier=(const char*)sqlite3_column_text(fields,5);
        if(!identity||!kind||!owner||!verifier||!lowerer||!valid_token(kind)||!candidate_kind_valid(kind)||!*identity||!*owner||!*verifier||!*lowerer) goto fail;
        if(strlen(identity)>=sizeof(identity_copy)||strlen(kind)>=sizeof(kind_copy)||strlen(owner)>=sizeof(owner_copy)||strlen(verifier)>=sizeof(verifier_copy)||strlen(lowerer)>=sizeof(lowerer_copy)||strlen(frontier?frontier:"")>=sizeof(frontier_copy))goto fail;
        snprintf(identity_copy,sizeof(identity_copy),"%s",identity);snprintf(kind_copy,sizeof(kind_copy),"%s",kind);snprintf(owner_copy,sizeof(owner_copy),"%s",owner);snprintf(verifier_copy,sizeof(verifier_copy),"%s",verifier);snprintf(lowerer_copy,sizeof(lowerer_copy),"%s",lowerer);snprintf(frontier_copy,sizeof(frontier_copy),"%s",frontier?frontier:"");
        identity=identity_copy;kind=kind_copy;owner=owner_copy;verifier=verifier_copy;lowerer=lowerer_copy;frontier=frontier_copy;
        sqlite3_reset(fields); sqlite3_clear_bindings(fields);
        /* Replacing a record also replaces its derived coordinates. */
        if(sqlite3_prepare_v2(db,"DELETE FROM candidate_advertisement_term WHERE identity=?",-1,&items,NULL)!=SQLITE_OK)goto fail;
        sqlite3_bind_text(items,1,identity,-1,SQLITE_TRANSIENT);
        if(sqlite3_step(items)!=SQLITE_DONE){sqlite3_finalize(items);goto fail;}sqlite3_finalize(items);items=NULL;
        if(sqlite3_prepare_v2(db,"DELETE FROM candidate_advertisement_dependency WHERE identity=?",-1,&items,NULL)!=SQLITE_OK)goto fail;
        sqlite3_bind_text(items,1,identity,-1,SQLITE_TRANSIENT);
        if(sqlite3_step(items)!=SQLITE_DONE){sqlite3_finalize(items);goto fail;}sqlite3_finalize(items);items=NULL;
        sqlite3_bind_text(insert,1,identity,-1,SQLITE_TRANSIENT);sqlite3_bind_text(insert,2,kind,-1,SQLITE_TRANSIENT);sqlite3_bind_text(insert,3,owner,-1,SQLITE_TRANSIENT);sqlite3_bind_text(insert,4,verifier,-1,SQLITE_TRANSIENT);sqlite3_bind_text(insert,5,lowerer,-1,SQLITE_TRANSIENT);sqlite3_bind_text(insert,6,frontier?frontier:"",-1,SQLITE_TRANSIENT);sqlite3_bind_text(insert,7,line,n,SQLITE_TRANSIENT);
        if(sqlite3_step(insert)!=SQLITE_DONE)goto fail;sqlite3_reset(insert);sqlite3_clear_bindings(insert);
        if(sqlite3_prepare_v2(db,"SELECT value FROM json_each(?1,'$.terms')",-1,&items,NULL)!=SQLITE_OK)goto fail;
        sqlite3_bind_text(items,1,line,n,SQLITE_TRANSIENT);
        while(sqlite3_step(items)==SQLITE_ROW){AddressTerm term;const char*v=(const char*)sqlite3_column_text(items,0);if(!address_term(v,&term)){sqlite3_finalize(items);goto fail;}sqlite3_bind_text(term_insert,1,identity,-1,SQLITE_TRANSIENT);sqlite3_bind_text(term_insert,2,term.axis,-1,SQLITE_TRANSIENT);sqlite3_bind_text(term_insert,3,term.term,-1,SQLITE_TRANSIENT);if(sqlite3_step(term_insert)!=SQLITE_DONE){sqlite3_finalize(items);goto fail;}sqlite3_reset(term_insert);sqlite3_clear_bindings(term_insert);}sqlite3_finalize(items);items=NULL;
        if(sqlite3_prepare_v2(db,"SELECT value FROM json_each(?1,'$.dependencies')",-1,&items,NULL)!=SQLITE_OK)goto fail;
        sqlite3_bind_text(items,1,line,n,SQLITE_TRANSIENT);
        while(sqlite3_step(items)==SQLITE_ROW){const char*v=(const char*)sqlite3_column_text(items,0);if(!v||!*v){sqlite3_finalize(items);goto fail;}sqlite3_bind_text(dep_insert,1,identity,-1,SQLITE_TRANSIENT);sqlite3_bind_text(dep_insert,2,v,-1,SQLITE_TRANSIENT);if(sqlite3_step(dep_insert)!=SQLITE_DONE){sqlite3_finalize(items);goto fail;}sqlite3_reset(dep_insert);sqlite3_clear_bindings(dep_insert);}sqlite3_finalize(items);
        rows++;
    }
    sqlite3_finalize(fields);sqlite3_finalize(insert);sqlite3_finalize(term_insert);sqlite3_finalize(dep_insert);free(line);fclose(f);
    if(!rebuild_forwarding_index(db)||!exec_sql(db,"COMMIT;"))return 0;
    printf("{\"imported_candidate_advertisements\":%d,\"truth\":\"eligible_for_further_consideration\"}\n",rows);return 1;
fail:
    fprintf(stderr,"candidate advertisement import: %s\n",sqlite3_errmsg(db));sqlite3_finalize(fields);sqlite3_finalize(insert);sqlite3_finalize(term_insert);sqlite3_finalize(dep_insert);free(line);if(f)fclose(f);exec_sql(db,"ROLLBACK;");return 0;
}

/* A generic native AddressPlane forwarding cut over every imported
 * ExternalOccurrence. `--need axis:term` is an intersection; `--forbid` is a
 * prohibition.  Returned rows are candidates for exact verification only. */
static int address_candidates(sqlite3 *db, int argc, char **argv) {
    AddressNeedWord words[32]; AddressTerm forbidden[32]; int word_count=0, forbidden_count=0;
    int total=0, returned=0, limit=32; sqlite3_stmt *lookup=NULL,*st=NULL; char sql[16384]; size_t w=0;
    for (int i=2;i<argc;i++) {
        AddressTerm term; int is_need=0;
        if (!strcmp(argv[i],"--need") && i+1<argc) { is_need=1; i++; }
        else if (!strcmp(argv[i],"--forbid") && i+1<argc) { i++; }
        else if (!strcmp(argv[i],"--limit") && i+1<argc) { limit=atoi(argv[++i]); continue; }
        else continue;
        if (!address_term(argv[i],&term)) { fprintf(stderr,"invalid address term: %s\n",argv[i]); return 0; }
        if (!is_need) { if(forbidden_count>=32)return 0; forbidden[forbidden_count++]=term; continue; }
        if (sqlite3_prepare_v2(db,"SELECT word,bit FROM address_bit WHERE axis=? AND term=?",-1,&lookup,NULL)!=SQLITE_OK) return 0;
        sqlite3_bind_text(lookup,1,term.axis,-1,SQLITE_TRANSIENT); sqlite3_bind_text(lookup,2,term.term,-1,SQLITE_TRANSIENT);
        if (sqlite3_step(lookup)!=SQLITE_ROW) { sqlite3_finalize(lookup); printf("{\"considered\":0,\"survivors\":[],\"reason\":\"unknown_coordinate\"}\n"); return 1; }
        int word=sqlite3_column_int(lookup,0), bit=sqlite3_column_int(lookup,1), seen=0;
        sqlite3_finalize(lookup); lookup=NULL;
        for(int j=0;j<word_count;j++) if(words[j].word==word){words[j].mask|=((sqlite3_int64)1<<bit);seen=1;break;}
        if(!seen){if(word_count>=32)return 0;words[word_count].word=word;words[word_count].mask=((sqlite3_int64)1<<bit);word_count++;}
    }
    if (limit<1) limit=1; if(limit>256) limit=256;
    if(sqlite3_prepare_v2(db,"SELECT count(*) FROM occurrence",-1,&lookup,NULL)==SQLITE_OK && sqlite3_step(lookup)==SQLITE_ROW)total=sqlite3_column_int(lookup,0);
    if(lookup)sqlite3_finalize(lookup);
    w+=(size_t)snprintf(sql+w,sizeof(sql)-w,"SELECT o.id,o.observed_name,o.source_kind FROM occurrence o WHERE 1=1");
    for(int i=0;i<word_count;i++) w+=(size_t)snprintf(sql+w,sizeof(sql)-w," AND EXISTS(SELECT 1 FROM candidate_mask m WHERE m.occurrence_id=o.id AND m.word=? AND (m.provides & ?) = ?)");
    if(forbidden_count){w+=(size_t)snprintf(sql+w,sizeof(sql)-w," AND NOT EXISTS(SELECT 1 FROM occurrence_term f WHERE f.occurrence_id=o.id AND (");for(int i=0;i<forbidden_count;i++)w+=(size_t)snprintf(sql+w,sizeof(sql)-w,"%s(f.axis=? AND f.term=?)",i?" OR ":"");w+=(size_t)snprintf(sql+w,sizeof(sql)-w,"))");}
    if(w+32>=sizeof(sql))return 0; snprintf(sql+w,sizeof(sql)-w," ORDER BY o.id LIMIT ?");
    if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK){fprintf(stderr,"address query: %s\n",sqlite3_errmsg(db));return 0;}
    int bind=1;for(int i=0;i<word_count;i++){sqlite3_bind_int(st,bind++,words[i].word);sqlite3_bind_int64(st,bind++,words[i].mask);sqlite3_bind_int64(st,bind++,words[i].mask);}for(int i=0;i<forbidden_count;i++){sqlite3_bind_text(st,bind++,forbidden[i].axis,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,bind++,forbidden[i].term,-1,SQLITE_TRANSIENT);}sqlite3_bind_int(st,bind,limit);
    printf("{\"considered\":%d,\"mask_words\":%d,\"survivors\":[",total,word_count);
    while(sqlite3_step(st)==SQLITE_ROW){if(returned++)putchar(',');printf("{\"id\":");json_text((const char*)sqlite3_column_text(st,0));printf(",\"name\":");json_text((const char*)sqlite3_column_text(st,1));printf(",\"kind\":");json_text((const char*)sqlite3_column_text(st,2));putchar('}');}
    printf("],\"returned\":%d,\"rejected_before_exact\":%d,\"truth\":\"eligible_for_further_consideration\"}\n",returned,total-returned);
    sqlite3_finalize(st); return 1;
}

/* Same mask calculus, but over typed non-owning advertisements.  `--kind` is
 * compulsory: a surface candidate can never be returned from a provider or
 * carrier request merely because their coordinates overlap. */
typedef struct { FILE *out; int count; } ForwardOutput;
static int forward_row(void *context,const BfAddressCandidate *c){
    ForwardOutput *o=context;FILE *f=o->out;
    if(o->count++)fputc(',',f);
    const char *keys[]={"identity","owner_ref","exact_verifier","lowerer_ref","validity_frontier","name","url"};
    const char *values[]={c->identity,c->owner,c->verifier,c->lowerer,c->frontier,c->name,c->url};
    fputc('{',f);for(int i=0;i<7;i++){if(i)fputc(',',f);bf_lex_json_emit(f,keys[i]);fputc(':',f);bf_lex_json_emit(f,values[i]);}
    fputs(",\"dependency_refs\":[",f);for(size_t i=0;i<c->dependency_count;i++){if(i)fputc(',',f);bf_lex_json_emit(f,c->dependencies[i]);}
    fputs("]}",f);
    return ferror(f)?1:0;
}
static int forward_advertisements(sqlite3 *db,int argc,char **argv){
    BfAddressQuery q={.limit=32};BfAddressResolution r;char *body=NULL;size_t size=0;int ok=0;
    for(int i=2;i<argc;i++){
        if(!strcmp(argv[i],"--db")&&i+1<argc){i++;continue;}
        if(!strcmp(argv[i],"--kind")&&i+1<argc){q.kind=argv[++i];continue;}
        if(!strcmp(argv[i],"--limit")&&i+1<argc){char *end;long n=strtol(argv[++i],&end,10);if(*end||n<1||n>256)return 0;q.limit=(int)n;continue;}
        int need=!strcmp(argv[i],"--need"),forbid=!strcmp(argv[i],"--forbid");AddressTerm parsed;
        if(!(need||forbid)||i+1>=argc||!address_term(argv[i+1],&parsed))return 0;
        const char *value=argv[++i];
        if(need){if(q.need_count==BF_ADDRESS_TERMS)return 0;q.need[q.need_count++]=value;}
        else {if(q.forbid_count==BF_ADDRESS_TERMS)return 0;q.forbid[q.forbid_count++]=value;}
    }
    if(!q.kind||!candidate_kind_valid(q.kind))return 0;
    FILE *f=open_memstream(&body,&size);if(!f)return 0;ForwardOutput out={f,0};
    int failed=bf_address_resolve(db,&q,forward_row,&out,&r,stderr);if(fclose(f))failed=1;
    if(!failed){printf("{\"candidate_kind\":");json_text(q.kind);
        printf(",\"considered\":%lld,\"eligible\":%lld,\"mask_words\":%d,\"survivors\":[",(long long)r.considered,(long long)r.eligible,r.mask_words);
        if(size)fwrite(body,1,size,stdout);
        printf("],\"returned\":%d,\"rejected_before_exact\":%lld,\"has_more\":%s,\"reason\":\"%s\",\"truth\":\"eligible_for_further_consideration\"}\n",r.returned,(long long)(r.considered-r.eligible),r.has_more?"true":"false",r.unknown_coordinate?"unknown_coordinate":"owner_index");ok=!ferror(stdout);
    }
    free(body);return ok;
}

/* McpApertureIR is the machine-facing projection of an already-addressed
 * closure.  It deliberately consumes the same typed CandidateAdvertisement
 * masks as every other AddressPlane caller; it is not a second MCP catalogue
 * or an authorization cache.  The four closure digests are supplied by the
 * gateway after it has authenticated the caller, but no bearer, actor fact,
 * authority result, or target proof is retained here. */
typedef struct {
    char *identity, *kind, *owner, *verifier, *lowerer, *frontier;
} McpApertureCandidate;

static void aperture_candidate_free(McpApertureCandidate *candidate) {
    if (!candidate) return;
    free(candidate->identity); free(candidate->kind); free(candidate->owner); free(candidate->verifier);
    free(candidate->lowerer); free(candidate->frontier);
}

static void aperture_hash_text(BfSha256 *hash, const char *text) {
    const unsigned char separator = 0xff;
    bf_sha256_update(hash, (const uint8_t *)(text ? text : ""), strlen(text ? text : ""));
    bf_sha256_update(hash, &separator, 1);
}

static int mcp_aperture(sqlite3 *db, int argc, char **argv) {
    AddressNeedWord words[32]; AddressTerm forbidden[32];
    McpApertureCandidate candidates[256];
    const char *actor = NULL, *address = NULL, *target = NULL, *policy = NULL;
    int word_count = 0, forbidden_count = 0, total = 0, count = 0, limit = 64;
    sqlite3_stmt *lookup = NULL, *st = NULL, *dependencies = NULL;
    char sql[16384], digest[65]; size_t w = 0;
    memset(candidates, 0, sizeof(candidates));
    for (int i = 2; i < argc; i++) {
        AddressTerm term; int is_need = 0;
        if (!strcmp(argv[i], "--actor-closure") && i + 1 < argc) { actor = argv[++i]; continue; }
        if (!strcmp(argv[i], "--address-closure") && i + 1 < argc) { address = argv[++i]; continue; }
        if (!strcmp(argv[i], "--target-frontier") && i + 1 < argc) { target = argv[++i]; continue; }
        if (!strcmp(argv[i], "--policy-generation") && i + 1 < argc) { policy = argv[++i]; continue; }
        if (!strcmp(argv[i], "--need") && i + 1 < argc) { is_need = 1; i++; }
        else if (!strcmp(argv[i], "--forbid") && i + 1 < argc) { i++; }
        else if (!strcmp(argv[i], "--limit") && i + 1 < argc) { limit = atoi(argv[++i]); continue; }
        else continue;
        if (!address_term(argv[i], &term)) { fprintf(stderr, "invalid address term: %s\n", argv[i]); goto fail; }
        if (!is_need) { if (forbidden_count >= 32) goto fail; forbidden[forbidden_count++] = term; continue; }
        if (sqlite3_prepare_v2(db, "SELECT word,bit FROM address_bit WHERE axis=? AND term=?", -1, &lookup, NULL) != SQLITE_OK) goto fail;
        sqlite3_bind_text(lookup, 1, term.axis, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(lookup, 2, term.term, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(lookup) != SQLITE_ROW) {
            sqlite3_finalize(lookup); lookup = NULL;
            printf("{\"schema\":\"bonfyre-mcp-aperture.v1\",\"state\":\"empty\",\"reason\":\"unknown_coordinate\",\"truth\":\"advertisement_only\"}\n");
            return 1;
        }
        { int word = sqlite3_column_int(lookup, 0), bit = sqlite3_column_int(lookup, 1), seen = 0;
          sqlite3_finalize(lookup); lookup = NULL;
          for (int j = 0; j < word_count; j++) if (words[j].word == word) { words[j].mask |= ((sqlite3_int64)1 << bit); seen = 1; break; }
          if (!seen) { if (word_count >= 32) goto fail; words[word_count].word = word; words[word_count].mask = ((sqlite3_int64)1 << bit); word_count++; }
        }
    }
    if (!actor || !*actor || !address || !*address || !target || !*target || !policy || !*policy) {
        fprintf(stderr, "mcp-aperture requires actor/address/target/policy closure digests\n"); goto fail;
    }
    if (limit < 1) limit = 1; if (limit > 256) limit = 256;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM candidate_advertisement WHERE candidate_kind IN ('mcp_projection','media_library','creator_channel','interaction_grammar','compute','provider','protocol_pattern','protocol_chain','carrier','target_obligation','transformation')", -1, &lookup, NULL) != SQLITE_OK) goto fail;
    if (sqlite3_step(lookup) == SQLITE_ROW) total = sqlite3_column_int(lookup, 0);
    sqlite3_finalize(lookup); lookup = NULL;
    w += (size_t)snprintf(sql + w, sizeof(sql) - w,
        "SELECT a.identity,a.candidate_kind,a.owner_ref,a.exact_verifier,a.lowerer_ref,a.validity_frontier FROM candidate_advertisement a WHERE a.candidate_kind IN ('mcp_projection','media_library','creator_channel','interaction_grammar','compute','provider','protocol_pattern','protocol_chain','carrier','target_obligation','transformation')");
    for (int i = 0; i < word_count; i++) w += (size_t)snprintf(sql + w, sizeof(sql) - w,
        " AND EXISTS(SELECT 1 FROM candidate_advertisement_mask m WHERE m.identity=a.identity AND m.word=? AND (m.provides & ?) = ?)");
    if (forbidden_count) {
        w += (size_t)snprintf(sql + w, sizeof(sql) - w, " AND NOT EXISTS(SELECT 1 FROM candidate_advertisement_term f WHERE f.identity=a.identity AND (");
        for (int i = 0; i < forbidden_count; i++) w += (size_t)snprintf(sql + w, sizeof(sql) - w, "%s(f.axis=? AND f.term=?)", i ? " OR " : "");
        w += (size_t)snprintf(sql + w, sizeof(sql) - w, "))");
    }
    if (w + 40 >= sizeof(sql)) goto fail;
    snprintf(sql + w, sizeof(sql) - w, " ORDER BY a.identity LIMIT ?");
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) goto fail;
    { int bind = 1;
      for (int i = 0; i < word_count; i++) { sqlite3_bind_int(st, bind++, words[i].word); sqlite3_bind_int64(st, bind++, words[i].mask); sqlite3_bind_int64(st, bind++, words[i].mask); }
      for (int i = 0; i < forbidden_count; i++) { sqlite3_bind_text(st, bind++, forbidden[i].axis, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, bind++, forbidden[i].term, -1, SQLITE_TRANSIENT); }
      sqlite3_bind_int(st, bind, limit);
    }
    while (sqlite3_step(st) == SQLITE_ROW && count < limit) {
        McpApertureCandidate *candidate = &candidates[count];
        candidate->identity = strdup((const char *)sqlite3_column_text(st, 0));
        candidate->kind = strdup((const char *)sqlite3_column_text(st, 1));
        candidate->owner = strdup((const char *)sqlite3_column_text(st, 2));
        candidate->verifier = strdup((const char *)sqlite3_column_text(st, 3));
        candidate->lowerer = strdup((const char *)sqlite3_column_text(st, 4));
        candidate->frontier = strdup((const char *)sqlite3_column_text(st, 5));
        if (!candidate->identity || !candidate->kind || !candidate->owner || !candidate->verifier || !candidate->lowerer || !candidate->frontier) goto fail;
        count++;
    }
    sqlite3_finalize(st); st = NULL;
    { BfSha256 hash; uint8_t raw[32];
      bf_sha256_init(&hash);
      aperture_hash_text(&hash, "bonfyre-mcp-aperture.v1"); aperture_hash_text(&hash, "2026-07-28");
      aperture_hash_text(&hash, actor); aperture_hash_text(&hash, address); aperture_hash_text(&hash, target); aperture_hash_text(&hash, policy);
      for (int i = 0; i < count; i++) { aperture_hash_text(&hash, candidates[i].identity); aperture_hash_text(&hash, candidates[i].owner); aperture_hash_text(&hash, candidates[i].verifier); aperture_hash_text(&hash, candidates[i].lowerer); aperture_hash_text(&hash, candidates[i].frontier); }
      bf_sha256_final(&hash, raw); bf_sha256_digest_hex(raw, digest);
    }
    printf("{\"schema\":\"bonfyre-mcp-aperture.v1\",\"mcp_protocol_version\":\"2026-07-28\",\"aperture_digest\":\"sha256:"); fputs(digest, stdout); fputs("\",\"actor_closure\":", stdout); json_text(actor);
    fputs(",\"address_closure\":", stdout); json_text(address); fputs(",\"target_frontier\":", stdout); json_text(target); fputs(",\"policy_generation\":", stdout); json_text(policy);
    printf(",\"candidate_population\":%d,\"mask_words\":%d,\"rejected_before_exact\":%d,\"cache\":{\"scope\":\"private\",\"ttl_ms\":1000,\"deterministic\":true},\"resources\":[", total, word_count, total - count);
    { int emitted = 0; for (int i = 0; i < count; i++) if (!strncmp(candidates[i].identity, "mcp:resource:", 13)) { if (emitted++) putchar(','); printf("{\"identity\":"); json_text(candidates[i].identity); printf(",\"exact_verifier\":"); json_text(candidates[i].verifier); printf("}"); } }
    fputs("],\"resource_templates\":[", stdout);
    { int emitted = 0, media = 0, channels = 0, grammars = 0; for (int i = 0; i < count; i++) { if (!strcmp(candidates[i].kind, "media_library")) media = 1; if (!strcmp(candidates[i].kind, "creator_channel")) channels = 1; if (!strcmp(candidates[i].kind, "interaction_grammar")) grammars = 1; if (!strncmp(candidates[i].identity, "mcp:resource_template:", 22)) { if (emitted++) putchar(','); printf("{\"identity\":"); json_text(candidates[i].identity); printf(",\"exact_verifier\":"); json_text(candidates[i].verifier); printf("}"); } } if (media) { if (emitted++) putchar(','); fputs("{\"identity\":\"bonfyre://media/library/{library}/catalog\",\"exact_verifier\":\"MediaLibrary:access\"}", stdout); } if (channels) { if (emitted++) putchar(','); fputs("{\"identity\":\"bonfyre://channel/{channel}/surface\",\"exact_verifier\":\"CreatorChannel:visibility\"}", stdout); } if (grammars) { if (emitted++) putchar(','); fputs("{\"identity\":\"bonfyre://interaction/{binding}/surface\",\"exact_verifier\":\"owner_specific\"}", stdout); } }
    fputs("],\"operations\":[", stdout);
    { int emitted = 0, media = 0, channels = 0, grammars = 0; for (int i = 0; i < count; i++) { if (!strcmp(candidates[i].kind, "media_library")) media = 1; if (!strcmp(candidates[i].kind, "creator_channel")) channels = 1; if (!strcmp(candidates[i].kind, "interaction_grammar")) grammars = 1; if (!strncmp(candidates[i].identity, "mcp:tool:", 9)) { if (emitted++) putchar(','); printf("{\"identity\":"); json_text(candidates[i].identity); printf(",\"exact_verifier\":"); json_text(candidates[i].verifier); printf("}"); } } if (media) { if (emitted++) putchar(','); fputs("{\"identity\":\"media_library_catalog\",\"exact_verifier\":\"MediaLibrary:access\"}", stdout); putchar(','); emitted++; fputs("{\"identity\":\"media_playback_plan\",\"exact_verifier\":\"MediaLibrary:access\"}", stdout); putchar(','); emitted++; fputs("{\"identity\":\"media_transport_lease\",\"exact_verifier\":\"MediaDelivery:right\"}", stdout); } if (channels) { if (emitted++) putchar(','); fputs("{\"identity\":\"creator_channel_surface\",\"exact_verifier\":\"CreatorChannel:visibility\"}", stdout); } if (grammars) { if (emitted++) putchar(','); fputs("{\"identity\":\"interaction_surface_plan\",\"exact_verifier\":\"owner_specific\"}", stdout); } }
    fputs("],\"interaction_programs\":[", stdout);
    { int emitted = 0; for (int i = 0; i < count; i++) if (!strncmp(candidates[i].identity, "mcp:prompt:", 11)) { if (emitted++) putchar(','); printf("{\"identity\":"); json_text(candidates[i].identity); printf(",\"exact_verifier\":"); json_text(candidates[i].verifier); printf("}"); } }
    fputs("],\"dependency_frontier\":[", stdout);
    if (sqlite3_prepare_v2(db, "SELECT dependency_ref FROM candidate_advertisement_dependency WHERE identity=?1 ORDER BY dependency_ref", -1, &dependencies, NULL) == SQLITE_OK) {
        int emitted = 0;
        for (int i = 0; i < count; i++) {
            sqlite3_bind_text(dependencies, 1, candidates[i].identity, -1, SQLITE_TRANSIENT);
            while (sqlite3_step(dependencies) == SQLITE_ROW) { if (emitted++) putchar(','); json_text((const char *)sqlite3_column_text(dependencies, 0)); }
            sqlite3_reset(dependencies); sqlite3_clear_bindings(dependencies);
        }
    }
    sqlite3_finalize(dependencies); dependencies = NULL;
    fputs("],\"forbidden_inferences\":[\"aperture advertisement is not authority\",\"aperture advertisement is not proof\",\"aperture advertisement does not execute an effect\"],\"truth\":\"advertisement_only_exact_owner_recheck_required\"}\n", stdout);
    for (int i = 0; i < count; i++) aperture_candidate_free(&candidates[i]);
    return 1;
fail:
    if (lookup) sqlite3_finalize(lookup); if (st) sqlite3_finalize(st); if (dependencies) sqlite3_finalize(dependencies);
    for (int i = 0; i < 256; i++) aperture_candidate_free(&candidates[i]);
    return 0;
}

/* FactDelta forwarding consumes only declared dependency registrations.  A
 * dependency hit means recomputation is relevant; it never asserts the delta
 * is true, selects a candidate, or grants an effect. */
static int affected_advertisements(sqlite3 *db, int argc, char **argv) {
    sqlite3_stmt *st=NULL; int first=1,count=0; const char *write=NULL;
    for(int i=2;i+1<argc;i++)if(!strcmp(argv[i],"--write")){write=argv[i+1];break;}
    if(!write||!*write){fprintf(stderr,"affected requires --write ADDRESS\n");return 0;}
    if(sqlite3_prepare_v2(db,"SELECT a.identity,a.candidate_kind,a.owner_ref FROM candidate_advertisement_dependency d JOIN candidate_advertisement a ON a.identity=d.identity WHERE d.dependency_ref=? ORDER BY a.candidate_kind,a.identity",-1,&st,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(st,1,write,-1,SQLITE_TRANSIENT);printf("{\"write\":");json_text(write);printf(",\"affected\":[");
    while(sqlite3_step(st)==SQLITE_ROW){if(!first)putchar(',');first=0;count++;printf("{\"identity\":");json_text((const char*)sqlite3_column_text(st,0));printf(",\"candidate_kind\":");json_text((const char*)sqlite3_column_text(st,1));printf(",\"owner_ref\":");json_text((const char*)sqlite3_column_text(st,2));putchar('}');}
    printf("],\"affected_count\":%d,\"truth\":\"dependency_cut_only\"}\n",count);sqlite3_finalize(st);return 1;
}

/* Native FactDelta consumer for the shared forwarding registry.  It consumes
 * the write set emitted by an owner transaction and returns the union of
 * explicitly declared dependent candidates.  It does not execute their
 * lowerers: this is the affected cut handed to SemanticDerivation/Feldera. */
static int consume_fact_delta(sqlite3 *db, const char *path) {
    char *body=NULL,*delta=NULL;size_t n=0;sqlite3 *json=NULL;sqlite3_stmt *writes=NULL,*insert=NULL,*out=NULL;int first=1,count=0;
    if(!(body=read_all(path,&n))||sqlite3_open(":memory:",&json)!=SQLITE_OK){free(body);if(json)sqlite3_close(json);return 0;}
    if(sqlite3_prepare_v2(json,"SELECT json_extract(?1,'$.delta_id')",-1,&writes,NULL)!=SQLITE_OK)goto fail;
    sqlite3_bind_text(writes,1,body,n,SQLITE_TRANSIENT);if(sqlite3_step(writes)==SQLITE_ROW&&sqlite3_column_text(writes,0))delta=strdup((const char*)sqlite3_column_text(writes,0));sqlite3_finalize(writes);writes=NULL;if(!delta||!*delta)goto fail;
    /* A receipt and all of its lowering residues are one durable event.  A
     * crash may leave the delta file available to retry, but never a receipt
     * claiming a cut that was only partly handed off. */
    if(!exec_sql(db,"BEGIN IMMEDIATE;"))goto fail;
    if(sqlite3_prepare_v2(db,"SELECT 1 FROM address_delta_receipt WHERE delta_id=?",-1,&out,NULL)!=SQLITE_OK)goto fail_tx;
    sqlite3_bind_text(out,1,delta,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(out)==SQLITE_ROW){sqlite3_finalize(out);exec_sql(db,"ROLLBACK;");sqlite3_close(json);free(body);printf("{\"delta_id\":");json_text(delta);puts(",\"state\":\"already_processed\",\"truth\":\"dependency_cut_only\"}");free(delta);return 1;}sqlite3_finalize(out);out=NULL;
    if(!exec_sql(db,"CREATE TEMP TABLE IF NOT EXISTS address_delta_cut(identity TEXT PRIMARY KEY,candidate_kind TEXT NOT NULL,owner_ref TEXT NOT NULL) WITHOUT ROWID;DELETE FROM address_delta_cut;"))goto fail_tx;
    if(sqlite3_prepare_v2(json,"SELECT value FROM json_each(?1,'$.writes')",-1,&writes,NULL)!=SQLITE_OK)goto fail_tx;
    if(sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO address_delta_cut(identity,candidate_kind,owner_ref) SELECT a.identity,a.candidate_kind,a.owner_ref FROM candidate_advertisement_dependency d JOIN candidate_advertisement a ON a.identity=d.identity WHERE d.dependency_ref=?",-1,&insert,NULL)!=SQLITE_OK)goto fail_tx;
    sqlite3_bind_text(writes,1,body,n,SQLITE_TRANSIENT);
    while(sqlite3_step(writes)==SQLITE_ROW){const char *write=(const char*)sqlite3_column_text(writes,0);if(!write||!*write)continue;sqlite3_bind_text(insert,1,write,-1,SQLITE_TRANSIENT);if(sqlite3_step(insert)!=SQLITE_DONE)goto fail_tx;sqlite3_reset(insert);sqlite3_clear_bindings(insert);}sqlite3_finalize(writes);writes=NULL;sqlite3_finalize(insert);insert=NULL;
    if(sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO candidate_lowering_queue(delta_id,identity,candidate_kind,owner_ref,lowerer_ref,causal_writes_json) SELECT ?,a.identity,a.candidate_kind,a.owner_ref,a.lowerer_ref,json_extract(?,'$.writes') FROM address_delta_cut c JOIN candidate_advertisement a ON a.identity=c.identity",-1,&out,NULL)!=SQLITE_OK)goto fail_tx;
    sqlite3_bind_text(out,1,delta,-1,SQLITE_TRANSIENT);sqlite3_bind_text(out,2,body,(int)n,SQLITE_TRANSIENT);if(sqlite3_step(out)!=SQLITE_DONE)goto fail_tx;sqlite3_finalize(out);out=NULL;
    if(sqlite3_prepare_v2(db,"SELECT count(*) FROM address_delta_cut",-1,&out,NULL)!=SQLITE_OK)goto fail_tx;if(sqlite3_step(out)==SQLITE_ROW)count=sqlite3_column_int(out,0);sqlite3_finalize(out);out=NULL;
    if(sqlite3_prepare_v2(db,"INSERT INTO address_delta_receipt(delta_id,source_path,affected_count) VALUES(?,?,?)",-1,&out,NULL)!=SQLITE_OK)goto fail_tx;sqlite3_bind_text(out,1,delta,-1,SQLITE_TRANSIENT);sqlite3_bind_text(out,2,path,-1,SQLITE_TRANSIENT);sqlite3_bind_int(out,3,count);if(sqlite3_step(out)!=SQLITE_DONE)goto fail_tx;sqlite3_finalize(out);out=NULL;
    if(sqlite3_prepare_v2(db,"SELECT identity,candidate_kind,owner_ref FROM address_delta_cut ORDER BY candidate_kind,identity",-1,&out,NULL)!=SQLITE_OK)goto fail_tx;
    printf("{\"delta_id\":");json_text(delta);printf(",\"affected\":[");while(sqlite3_step(out)==SQLITE_ROW){if(!first)putchar(',');first=0;printf("{\"identity\":");json_text((const char*)sqlite3_column_text(out,0));printf(",\"candidate_kind\":");json_text((const char*)sqlite3_column_text(out,1));printf(",\"owner_ref\":");json_text((const char*)sqlite3_column_text(out,2));putchar('}');}printf("],\"affected_count\":%d,\"lowering_state\":\"queued\",\"truth\":\"dependency_cut_only\"}\n",count);sqlite3_finalize(out);out=NULL;if(!exec_sql(db,"COMMIT;"))goto fail;sqlite3_close(json);free(body);free(delta);return 1;
fail_tx:exec_sql(db,"ROLLBACK;");
fail:fprintf(stderr,"fact delta: invalid or unreadable delta\n");sqlite3_finalize(writes);sqlite3_finalize(insert);sqlite3_finalize(out);if(json)sqlite3_close(json);free(body);free(delta);return 0;
}

static int drain_fact_deltas(sqlite3 *db, const char *directory, int limit) {
    DIR *dir;struct dirent *entry;int seen=0,processed=0;char path[1024];
    if(!directory||!(dir=opendir(directory))){perror(directory?directory:"fact-delta directory");return 0;}if(limit<1)limit=1;if(limit>4096)limit=4096;
    while((entry=readdir(dir))!=NULL&&seen<limit){size_t n=strlen(entry->d_name);if(n<6||strcmp(entry->d_name+n-5,".json"))continue;if(snprintf(path,sizeof(path),"%s/%s",directory,entry->d_name)>=(int)sizeof(path))continue;seen++;if(consume_fact_delta(db,path))processed++;}
    closedir(dir);printf("{\"fact_delta_files_seen\":%d,\"processed_or_deduplicated\":%d,\"truth\":\"dependency_cut_only\"}\n",seen,processed);return 1;
}

/* Durable forwarding-to-lowerer outbox. AddressPlane does not execute the
 * lowerer: it publishes one immutable handoff envelope into the configured
 * lowerer inbox and marks the queue row dispatched only after fsync+rename. */
static uint64_t handoff_hash(const char *a,const char *b){uint64_t h=1469598103934665603ULL;const char *v[2]={a,b};for(int x=0;x<2;x++){for(const unsigned char*p=(const unsigned char*)(v[x]?v[x]:"");*p;p++){h^=*p;h*=1099511628211ULL;}h^=0xff;h*=1099511628211ULL;}return h;}
static void path_atom(const char *in,char *out,size_t cap){size_t j=0;for(size_t i=0;in&&in[i]&&j+1<cap;i++){unsigned char c=(unsigned char)in[i];out[j++]=(isalnum(c)||c=='-'||c=='_')?(char)c:'-';}if(!j&&cap>1)out[j++]='x';out[j]='\0';}
static int write_all_fd(int fd,const char *buf,size_t n){while(n){ssize_t w=write(fd,buf,n);if(w<0){if(errno==EINTR)continue;return 0;}buf+=w;n-=(size_t)w;}return 1;}
static int publish_lowerer_handoff(const char *root,const char *delta,const char *identity,const char *kind,const char *owner,const char *lowerer,const char *causal_writes){
    char lower_atom[256],dir[1024],dest[1280],tmp[1280];int fd=-1,dfd=-1;
    if(!root||!*root||!delta||!identity||!lowerer||!*lowerer)return 0;path_atom(lowerer,lower_atom,sizeof(lower_atom));
    if(snprintf(dir,sizeof(dir),"%s/%s",root,lower_atom)>=(int)sizeof(dir))return 0;
    if(mkdir(root,0750)&&errno!=EEXIST)return 0;if(mkdir(dir,0750)&&errno!=EEXIST)return 0;
    if(snprintf(dest,sizeof(dest),"%s/%016llx.json",dir,(unsigned long long)handoff_hash(delta,identity))>=(int)sizeof(dest))return 0;
    if(snprintf(tmp,sizeof(tmp),"%s/.handoff-XXXXXX",dir)>=(int)sizeof(tmp))return 0;if((fd=mkstemp(tmp))<0)return 0;
    {sqlite3 *j=NULL;sqlite3_stmt *st=NULL;char *encoded=NULL;if(sqlite3_open(":memory:",&j)!=SQLITE_OK){close(fd);unlink(tmp);return 0;}
     if(sqlite3_prepare_v2(j,"SELECT json_object('schema','bonfyre-lowering-handoff.v1','delta_id',?1,'identity',?2,'candidate_kind',?3,'owner_ref',?4,'lowerer_ref',?5,'causal_writes',json(?6),'truth','dependency_cut_only')",-1,&st,NULL)!=SQLITE_OK){sqlite3_close(j);close(fd);unlink(tmp);return 0;}
     sqlite3_bind_text(st,1,delta,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,identity,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,3,kind?kind:"",-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,4,owner?owner:"",-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,5,lowerer,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,6,causal_writes&&*causal_writes?causal_writes:"[]",-1,SQLITE_TRANSIENT);
     if(sqlite3_step(st)==SQLITE_ROW&&sqlite3_column_text(st,0))encoded=strdup((const char*)sqlite3_column_text(st,0));sqlite3_finalize(st);sqlite3_close(j);if(!encoded){close(fd);unlink(tmp);return 0;}
     if(!write_all_fd(fd,encoded,strlen(encoded))||!write_all_fd(fd,"\n",1)||fsync(fd)){free(encoded);close(fd);unlink(tmp);return 0;}free(encoded);}
    if(close(fd)){unlink(tmp);return 0;}if(rename(tmp,dest)){unlink(tmp);return 0;}
    dfd=open(dir,O_RDONLY);if(dfd>=0){if(fsync(dfd)){close(dfd);return 0;}close(dfd);}return 1;
}
static int drain_lowering_queue(sqlite3 *db,const char *directory,int limit){
    sqlite3_stmt *st=NULL,*up=NULL;int dispatched=0;if(!directory||!*directory)return 1;if(limit<1)limit=1;if(limit>4096)limit=4096;
    while(dispatched<limit){char *delta=NULL,*identity=NULL,*kind=NULL,*owner=NULL,*lowerer=NULL,*causal_writes=NULL;
        if(sqlite3_prepare_v2(db,"SELECT delta_id,identity,candidate_kind,owner_ref,lowerer_ref,causal_writes_json FROM candidate_lowering_queue WHERE state='queued' ORDER BY queued_at,delta_id,identity LIMIT 1",-1,&st,NULL)!=SQLITE_OK)return 0;
        if(sqlite3_step(st)==SQLITE_ROW){delta=strdup((const char*)sqlite3_column_text(st,0));identity=strdup((const char*)sqlite3_column_text(st,1));kind=strdup((const char*)sqlite3_column_text(st,2));owner=strdup((const char*)sqlite3_column_text(st,3));lowerer=strdup((const char*)sqlite3_column_text(st,4));causal_writes=strdup((const char*)sqlite3_column_text(st,5));}sqlite3_finalize(st);st=NULL;
        if(!delta)break;if(!publish_lowerer_handoff(directory,delta,identity,kind,owner,lowerer,causal_writes)){free(delta);free(identity);free(kind);free(owner);free(lowerer);free(causal_writes);return 0;}
        if(sqlite3_prepare_v2(db,"UPDATE candidate_lowering_queue SET state='dispatched' WHERE delta_id=? AND identity=? AND state='queued'",-1,&up,NULL)!=SQLITE_OK){free(delta);free(identity);free(kind);free(owner);free(lowerer);free(causal_writes);return 0;}
        sqlite3_bind_text(up,1,delta,-1,SQLITE_TRANSIENT);sqlite3_bind_text(up,2,identity,-1,SQLITE_TRANSIENT);if(sqlite3_step(up)!=SQLITE_DONE||sqlite3_changes(db)!=1){sqlite3_finalize(up);free(delta);free(identity);free(kind);free(owner);free(lowerer);free(causal_writes);return 0;}sqlite3_finalize(up);up=NULL;dispatched++;free(delta);free(identity);free(kind);free(owner);free(lowerer);free(causal_writes);
    }
    printf("{\"lowering_handoffs_dispatched\":%d,\"truth\":\"handoff_only\"}\n",dispatched);return 1;
}

static int table_exists(sqlite3 *db, const char *name) {
    sqlite3_stmt *st=NULL; int yes=0;
    if(sqlite3_prepare_v2(db,"SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",-1,&st,NULL)!=SQLITE_OK)return 0;
    sqlite3_bind_text(st,1,name,-1,SQLITE_TRANSIENT);yes=sqlite3_step(st)==SQLITE_ROW;sqlite3_finalize(st);return yes;
}
/* Owner schemas evolve independently.  A declared optional dependency may be
 * emitted only when this owner version actually carries the field; absence is
 * not permission to infer a relationship from a name or a route. */
static int table_has_column(sqlite3 *db, const char *table, const char *column) {
    sqlite3_stmt *st = NULL;
    int yes = 0;
    char sql[512];
    if (!db || !table || !column ||
        snprintf(sql, sizeof(sql), "PRAGMA table_info(%s)", table) >= (int)sizeof(sql) ||
        sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(st, 1);
        if (name && !strcmp(name, column)) { yes = 1; break; }
    }
    sqlite3_finalize(st);
    return yes;
}

/* A published row keeps every field its owner declared.  The rich CMS
 * emitters below read fields that older stores do not carry; a missing
 * column reads as NULL rather than failing the whole estate build, and
 * NULL is absence of a declaration, never an empty declaration. */
static const char *cms_col(sqlite3 *db, const char *table, const char *column) {
    return table_has_column(db, table, column) ? column : "NULL";
}

static int source_topic(sqlite3 *db, const char *source, char *out, size_t cap) {
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (!db || !source || !*source || !out || !cap || !table_exists(db, "internet_source") ||
        !table_has_column(db, "internet_source", "source") || !table_has_column(db, "internet_source", "url") ||
        sqlite3_prepare_v2(db, "SELECT url FROM internet_source WHERE source=?1 AND status='published' ORDER BY id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, source, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) {
        snprintf(out, cap, "%s", (const char *)sqlite3_column_text(st, 0));
        ok = *out != '\0';
    }
    sqlite3_finalize(st);
    return ok;
}
static void address_atom(const char *in,char *out,size_t cap) {
    size_t j=0;int dash=0;
    for(size_t i=0;in&&in[i]&&j+1<cap;i++){unsigned char c=(unsigned char)tolower((unsigned char)in[i]);if(isalnum(c)){out[j++]=(char)c;dash=0;}else if(!dash&&j){out[j++]='-';dash=1;}}
    while(j&&out[j-1]=='-')j--;out[j]='\0';if(!j&&cap>1){out[0]='x';out[1]='\0';}
}
static void emit_terms_begin(void){fputs("\"terms\":[",stdout);}
/* JSON term output cannot call json_text for the value after its opening quote.
 * Keep this narrow helper so owner emitters never hand-assemble unsafe JSON. */
static void emit_term_atom(int *first,const char *axis,const char *value){char atom[256];address_atom(value,atom,sizeof(atom));if(!*first)putchar(',');*first=0;putchar('"');fputs(axis,stdout);putchar(':');fputs(atom,stdout);putchar('"');}
static void emit_record(const char *identity,const char *kind,const char *owner,const char *verifier,const char *lowerer,const char *frontier,const char *const *axes,const char *const *values,size_t term_count,const char *const *dependencies,size_t dep_count){
    int first=1;printf("{\"identity\":");json_text(identity);printf(",\"candidate_kind\":");json_text(kind);printf(",\"owner_ref\":");json_text(owner);printf(",\"exact_verifier\":");json_text(verifier);printf(",\"lowerer_ref\":");json_text(lowerer);printf(",\"validity_frontier\":");json_text(frontier?frontier:"");putchar(',');emit_terms_begin();for(size_t i=0;i<term_count;i++)emit_term_atom(&first,axes[i],values[i]);fputs("],\"dependencies\":[",stdout);for(size_t i=0;i<dep_count;i++){if(i)putchar(',');json_text(dependencies[i]);}puts("]}");
}

static int emit_site_programs(sqlite3 *cms);
static int emit_packs(sqlite3 *cms);
static int emit_relations(sqlite3 *cms);
static int emit_social_presence(sqlite3 *cms);
static int emit_spatial(sqlite3 *cms);
static int emit_source_collections(sqlite3 *cms);
static int emit_capability_programs(sqlite3 *cms);

/* CMS rows are source code.  This emitter reads published rows only and
 * preserves their owner coordinate; it does not select a profile, material,
 * renderer, or action.  A downstream import is the derived-index publish. */
static int emit_cms(sqlite3 *cms) {
    struct CmsShape { const char *table,*kind,*id,*a,*b,*c,*d,*lowerer; } shapes[] = {
      {"site_presentation","site_presentation","site_id","subject_address","territory","profile",NULL,"SurfaceIR:site_program"},
      {"world","world_publication","world","subject","character","grammars","sources","SurfaceIR:world"},
      {"semantic_slot","semantic_slot","slot_id","site_id","slot_name","extent",NULL,"SurfaceIR:slot"},
      {"application_binding","application_binding","binding_id","site_id","slot_id","application_kind","source_collection","SurfaceIR:application"},
      {"internet_source","external_source","source","family","kind","tier",NULL,"MaterialCompiler:source"},
      {"experience_stage","experience_stage","stage_id","experience","application_kind","extent",NULL,"SurfaceIR:experience"},
      {"surface_renderer","surface_renderer","renderer","canonical_name","grammar","extents",NULL,"SurfaceIR:renderer"},
      {"presentation_template","projection","template","family","renderer_mix","territory_affinity",NULL,"SurfaceIR:presentation"},
      {"design_fingerprint","projection","fingerprint_id","template","renderer_affinity","density",NULL,"SurfaceIR:design"},
      {"world_fragment","projection","fragment_id","world","surface_grammar","extent",NULL,"SurfaceIR:world_fragment"}
    }; int emitted=0;
    for(size_t x=0;x<sizeof(shapes)/sizeof(*shapes);x++){
      struct CmsShape *s=&shapes[x];sqlite3_stmt *st=NULL;char sql[512];
      if(!table_exists(cms,s->table))continue;
      snprintf(sql,sizeof(sql),"SELECT %s,%s,%s,%s,%s FROM %s WHERE status='published' ORDER BY %s",s->id,s->a?s->a:"''",s->b?s->b:"''",s->c?s->c:"''",s->d&&table_has_column(cms,s->table,s->d)?s->d:"''",s->table,s->id);
      if(sqlite3_prepare_v2(cms,sql,-1,&st,NULL)!=SQLITE_OK)return -1;
      while(sqlite3_step(st)==SQLITE_ROW){const char *id=(const char*)sqlite3_column_text(st,0),*a=(const char*)sqlite3_column_text(st,1),*b=(const char*)sqlite3_column_text(st,2),*c=(const char*)sqlite3_column_text(st,3),*d=(const char*)sqlite3_column_text(st,4);char identity[640],owner[640],frontier[640],publish[256],related[12][640];const char*axes[4]={"cms","subject","territory","extent"};const char*vals[4]={id?id:"",a?a:"",b?b:"","page"};const char*deps[16];size_t terms=1,depc=2;
        if(!id||!*id)continue;snprintf(identity,sizeof(identity),"cms:%s:%s",s->table,id);snprintf(owner,sizeof(owner),"CMS:%s:%s",s->table,id);snprintf(frontier,sizeof(frontier),"cms:%s:%s",s->table,id);snprintf(publish,sizeof(publish),"cms:%s",s->table);deps[0]=frontier;deps[1]=publish;
        if(!strcmp(s->table,"application_binding") && d && *d && snprintf(related[0],sizeof(related[0]),"cms:source_collection:%s",d)<(int)sizeof(related[0])){deps[depc++]=related[0];}
        if(!strcmp(s->table,"world") && d && *d){char sources[2048],*part;size_t related_count=0;snprintf(sources,sizeof(sources),"%s",d);for(part=strtok(sources,",");part&&depc<sizeof(deps)/sizeof(*deps)&&related_count<sizeof(related)/sizeof(*related);part=strtok(NULL,",")){char topic[640],*end;while(*part&&isspace((unsigned char)*part))part++;end=part+strlen(part);while(end>part&&isspace((unsigned char)end[-1]))*--end='\0';if(!*part)continue;if(snprintf(related[related_count],sizeof(related[related_count]),"cms:internet_source:%s",part)>=(int)sizeof(related[related_count]))continue;deps[depc++]=related[related_count++];if(depc<sizeof(deps)/sizeof(*deps)&&related_count<sizeof(related)/sizeof(*related)&&snprintf(related[related_count],sizeof(related[related_count]),"cms:publication_source:%s",part)<(int)sizeof(related[related_count]))deps[depc++]=related[related_count++];if(depc<sizeof(deps)/sizeof(*deps)&&related_count<sizeof(related)/sizeof(*related)&&source_topic(cms,part,topic,sizeof(topic))&&snprintf(related[related_count],sizeof(related[related_count]),"ForeignTwin:%s",topic)<(int)sizeof(related[related_count]))deps[depc++]=related[related_count++];}}
        if(a&&*a)terms=2;if(b&&*b)terms=3;if(c&&*c){axes[terms]="application";vals[terms++]=c;}if(!strcmp(s->table,"site_presentation")){axes[1]="subject";axes[2]="territory";axes[3]="profile";vals[3]=c?c:"";terms=4;}
        else if(!strcmp(s->table,"semantic_slot")){axes[1]="site";axes[2]="slot";axes[3]="extent";terms=4;}
        else if(!strcmp(s->table,"application_binding")){axes[1]="site";axes[2]="slot";axes[3]="application";terms=4;}
        else if(!strcmp(s->table,"experience_stage")){axes[1]="experience";axes[2]="application";axes[3]="extent";terms=4;}
        else if(!strcmp(s->table,"surface_renderer")){axes[1]="renderer";axes[2]="grammar";axes[3]="extent";terms=4;}
        else if(!strcmp(s->table,"presentation_template")){axes[1]="presentation";axes[2]="renderer";axes[3]="territory";terms=4;}
        else if(!strcmp(s->table,"design_fingerprint")){axes[1]="presentation";axes[2]="renderer";axes[3]="density";terms=4;}
        else if(!strcmp(s->table,"world_fragment")){axes[1]="world";axes[2]="grammar";axes[3]="extent";terms=4;}
        emit_record(identity,s->kind,owner,"CMS:publication",s->lowerer,frontier,axes,vals,terms,deps,depc);emitted++;
      }sqlite3_finalize(st);
    }
    /* Three CMS tables carry more than an identity and a label.  A
     * SourceCollection names domains, sources, material kinds and a refresh
     * policy; a CapabilityProgram names what it consumes and produces, which
     * carriers may carry it, what it requires and what authority it demands;
     * a SiteProgram names the experiences, bindings, collections and
     * capability programs it composes.  Flattening those to id-plus-label
     * made the forwarding plane unable to answer the questions they exist to
     * answer, so they are emitted whole.  Cannot infer: a preserved term is a
     * published declaration, never a runnable carrier, an admitted resource,
     * a granted authority or a satisfied requirement. */
    { int rich=emit_source_collections(cms); if(rich<0)return -1; emitted+=rich;
      rich=emit_capability_programs(cms); if(rich<0)return -1; emitted+=rich;
      rich=emit_site_programs(cms); if(rich<0)return -1; emitted+=rich;
      rich=emit_packs(cms); if(rich<0)return -1; emitted+=rich;
      rich=emit_relations(cms); if(rich<0)return -1; emitted+=rich;
      rich=emit_social_presence(cms); if(rich<0)return -1; emitted+=rich;
      rich=emit_spatial(cms); if(rich<0)return -1; emitted+=rich; }
    return emitted;
}

static int emit_targets(sqlite3 *owner) {
    sqlite3_stmt *st=NULL;int emitted=0;
    if(!table_exists(owner,"target_obligations"))return 0;
    if(sqlite3_prepare_v2(owner,"SELECT target_id,source_frontier,semantic_contract FROM target_obligations WHERE state IN ('OPEN','WAITING_EXTERNAL_PREDICATE') ORDER BY target_id",-1,&st,NULL)!=SQLITE_OK)return -1;
    while(sqlite3_step(st)==SQLITE_ROW){const char *id=(const char*)sqlite3_column_text(st,0),*frontier=(const char*)sqlite3_column_text(st,1),*contract=(const char*)sqlite3_column_text(st,2);char identity[640],ref[640];const char*axes[]={"operation","extent","target"};const char*vals[]={"desired-state","workspace",id?id:""};const char*deps[2];if(!id||!*id)continue;snprintf(identity,sizeof(identity),"target:%s",id);snprintf(ref,sizeof(ref),"TargetObligation:%s",id);deps[0]=ref;deps[1]=frontier?frontier:"";emit_record(identity,"target_obligation",ref,"TargetObligation:predicate_recheck","InverseCompiler:closure_program",frontier?frontier:"",axes,vals,3,deps,frontier&&*frontier?2:1);(void)contract;emitted++;}sqlite3_finalize(st);return emitted;
}

static int emit_providers(sqlite3 *provider) {
    sqlite3_stmt *st=NULL;int emitted=0;
    if(table_exists(provider,"provider_materializations")){
        if(sqlite3_prepare_v2(provider,"SELECT id,provider,kind,purpose,scope,COALESCE(expires_at,'') FROM provider_materializations WHERE released_at IS NULL ORDER BY id",-1,&st,NULL)!=SQLITE_OK)return -1;
        while(sqlite3_step(st)==SQLITE_ROW){char identity[128],ref[256];const char *id=(const char*)sqlite3_column_text(st,0),*provider_name=(const char*)sqlite3_column_text(st,1),*kind=(const char*)sqlite3_column_text(st,2),*purpose=(const char*)sqlite3_column_text(st,3),*scope=(const char*)sqlite3_column_text(st,4),*expiry=(const char*)sqlite3_column_text(st,5);const char*axes[]={"provider","material","purpose","scope"};const char*vals[]={provider_name?provider_name:"",kind?kind:"",purpose?purpose:"",scope?scope:""};const char*deps[1];if(!id||!*id)continue;snprintf(identity,sizeof(identity),"provider-materialization:%s",id);snprintf(ref,sizeof(ref),"ProviderMaterialization:%s",id);deps[0]=ref;emit_record(identity,"provider",ref,"ProviderState:availability","ResourceAdmission:provider",expiry?expiry:"",axes,vals,4,deps,1);emitted++;}sqlite3_finalize(st);st=NULL;
    }
    /* A completed provider embedding run is a real, typed representation
     * capability. It may advertise an embedding candidate, but it is neither
     * a semantic result nor an admitted provider execution: ResourceAdmission
     * and the exact ProviderState run check remain downstream. */
    if(!table_exists(provider,"provider_embedding_run"))return emitted;
    if(sqlite3_prepare_v2(provider,"SELECT run_id,provider,model,representation_id,input_digest,dimensions,completed_at FROM provider_embedding_run WHERE status='complete' AND requested_count>0 AND stored_count=requested_count AND dimensions>0 ORDER BY completed_at,run_id",-1,&st,NULL)!=SQLITE_OK)return -1;
    while(sqlite3_step(st)==SQLITE_ROW){char identity[320],ref[320],input_ref[320];const char *run=(const char*)sqlite3_column_text(st,0),*provider_name=(const char*)sqlite3_column_text(st,1),*model=(const char*)sqlite3_column_text(st,2),*representation=(const char*)sqlite3_column_text(st,3),*input=(const char*)sqlite3_column_text(st,4),*completed=(const char*)sqlite3_column_text(st,6);const char*axes[]={"provider","compute","representation","operation","exactness","measurement"};const char*vals[]={provider_name?provider_name:"",model?model:"",representation?representation:"","embedding","latent","measured"};const char*deps[2];if(!run||!*run||!provider_name||!*provider_name||!model||!*model||!representation||!*representation||!input||!*input)continue;snprintf(identity,sizeof(identity),"provider-embedding:%s",run);snprintf(ref,sizeof(ref),"ProviderEmbeddingRun:%s",run);snprintf(input_ref,sizeof(input_ref),"ProviderEmbeddingInput:%s",input);deps[0]=ref;deps[1]=input_ref;emit_record(identity,"provider",ref,"ProviderState:embedding_run","ProviderCompiler:embedding",completed?completed:"",axes,vals,6,deps,2);emitted++;}
    sqlite3_finalize(st);return emitted;
}

/* Model residency is a compute candidate population, not a model identity
 * owner.  The exact catalog/replay passport remains the verifier. */
static int emit_compute(sqlite3 *models) {
    sqlite3_stmt *st=NULL;int emitted=0;
    if(!table_exists(models,"models"))return 0;
    if(sqlite3_prepare_v2(models,"SELECT id,format,COALESCE(geometry,''),COALESCE(transform_family,''),COALESCE(fpq_sha256,'') FROM models ORDER BY id",-1,&st,NULL)!=SQLITE_OK)return -1;
    while(sqlite3_step(st)==SQLITE_ROW){const char *id=(const char*)sqlite3_column_text(st,0),*format=(const char*)sqlite3_column_text(st,1),*geometry=(const char*)sqlite3_column_text(st,2),*family=(const char*)sqlite3_column_text(st,3),*fpq=(const char*)sqlite3_column_text(st,4);char identity[512],ref[512];const char*axes[]={"compute","representation","geometry","transform","operation","admission"};const char*vals[]={id?id:"",format?format:"",geometry?geometry:"",family?family:"","catalog","unknown"};const char*deps[2];if(!id||!*id)continue;snprintf(identity,sizeof(identity),"compute:%s",id);snprintf(ref,sizeof(ref),"ModelCatalog:%s",id);deps[0]=ref;deps[1]=fpq?fpq:"";/* A bare catalog declaration is not an executable representation.  Only a ModelRepresentationPassport can advertise generation, embedding, or rerank operations. */emit_record(identity,"compute",ref,"ModelCatalog:representation_passport","ResourceAdmission:compute",fpq?fpq:"",axes,vals,6,deps,fpq&&*fpq?2:1);emitted++;}sqlite3_finalize(st);st=NULL;
    /* A generic model row says only that a model declaration exists.  A
     * representation row carries the independent runtime/passport/admission
     * frontier.  It is still merely a forwarding candidate: ADMIT/DEFER/
     * REJECT are owner facts rechecked by ModelCatalog and ResourceAdmission,
     * never AddressPlane selection. */
    if(!table_exists(models,"model_representation_passport"))return emitted;
    if(sqlite3_prepare_v2(models,"SELECT representation_id,model_id,representation_family,representation_version,exactness_class,measurement_state,admission_state,admission_scope,receipt_digest FROM model_representation_passport ORDER BY representation_id",-1,&st,NULL)!=SQLITE_OK)return -1;
    while(sqlite3_step(st)==SQLITE_ROW){const char *rid=(const char*)sqlite3_column_text(st,0),*model=(const char*)sqlite3_column_text(st,1),*family=(const char*)sqlite3_column_text(st,2),*version=(const char*)sqlite3_column_text(st,3),*exact=(const char*)sqlite3_column_text(st,4),*measurement=(const char*)sqlite3_column_text(st,5),*admission=(const char*)sqlite3_column_text(st,6),*scope=(const char*)sqlite3_column_text(st,7),*receipt=(const char*)sqlite3_column_text(st,8);char identity[640],ref[640],receipt_ref[640];const char*axes[]={"compute","representation","operation","admission","exactness","measurement","version"};const char*vals[]={model?model:"",family?family:"",scope?scope:"",admission?admission:"",exact?exact:"",measurement?measurement:"",version?version:""};const char*deps[2];if(!rid||!*rid||!model||!*model||!receipt||!*receipt)continue;snprintf(identity,sizeof(identity),"compute-representation:%s",rid);snprintf(ref,sizeof(ref),"ModelCatalog:representation:%s",rid);snprintf(receipt_ref,sizeof(receipt_ref),"ModelRepresentationReceipt:%s",receipt);deps[0]=ref;deps[1]=receipt_ref;emit_record(identity,"compute",ref,"ModelCatalog:representation_passport","ResourceAdmission:compute",receipt,axes,vals,7,deps,2);emitted++;}
    sqlite3_finalize(st);return emitted;
}

/* MediaLibrary is its own owner population.  It publishes only the compact
 * membership coordinates that AddressPlane can use to find a possible media
 * surface; exact library access and representation selection remain with the
 * MediaLibrary owner at request time. */
static int emit_media_library(sqlite3 *media) {
    sqlite3_stmt *st=NULL; int emitted=0;
    if(!table_exists(media,"media_library")||!table_exists(media,"media_library_membership"))return 0;
    if(sqlite3_prepare_v2(media,"SELECT l.library_id,l.mode,l.visibility,m.subject_ref,m.media_program_ref FROM media_library l JOIN media_library_membership m ON m.library_id=l.library_id WHERE l.state='active' AND m.state='active' ORDER BY l.library_id,m.subject_ref,m.media_program_ref",-1,&st,NULL)!=SQLITE_OK)return -1;
    while(sqlite3_step(st)==SQLITE_ROW){
        const char *library=(const char*)sqlite3_column_text(st,0),*mode=(const char*)sqlite3_column_text(st,1),*visibility=(const char*)sqlite3_column_text(st,2),*subject=(const char*)sqlite3_column_text(st,3),*program=(const char*)sqlite3_column_text(st,4);
        char identity[1024],owner[512],program_dep[768];
        const char *axes[]={"subject","library","material","operation","representation","audience"};
        const char *vals[]={subject?subject:"",library?library:"",mode?mode:"","playback","media-program",visibility?visibility:""};
        const char *deps[2];
        if(!library||!*library||!subject||!*subject||!program||!*program)continue;
        if(snprintf(identity,sizeof(identity),"media:library:%s:%s",library,program)>=(int)sizeof(identity)||snprintf(owner,sizeof(owner),"MediaLibrary:%s",library)>=(int)sizeof(owner)||snprintf(program_dep,sizeof(program_dep),"CMS:media_program:%s",program)>=(int)sizeof(program_dep))continue;
        deps[0]=owner;deps[1]=program_dep;
        emit_record(identity,"media_library",owner,"MediaLibrary:access","MediaLibraryCompiler:surface",owner,axes,vals,6,deps,2);
        emitted++;
    }
    sqlite3_finalize(st); return emitted;
}

/* CreatorChannel contributes a surface population, not a second media store.
 * Its current membership references CMS MediaProgram; exact visibility stays
 * with CreatorChannel and playback/delivery remain separate owners. */
static int emit_creator_channel(sqlite3 *channel) {
    sqlite3_stmt *st=NULL;int emitted=0;
    if(!table_exists(channel,"creator_channel")||!table_exists(channel,"channel_publication"))return 0;
    if(sqlite3_prepare_v2(channel,"SELECT c.channel_id,c.visibility,p.subject_ref,p.media_program_ref,p.program_kind FROM creator_channel c JOIN channel_publication p ON p.channel_id=c.channel_id WHERE c.state='active' AND p.state='published' ORDER BY c.channel_id,p.published_at DESC",-1,&st,NULL)!=SQLITE_OK)return -1;
    while(sqlite3_step(st)==SQLITE_ROW){const char *id=(const char*)sqlite3_column_text(st,0),*visibility=(const char*)sqlite3_column_text(st,1),*subject=(const char*)sqlite3_column_text(st,2),*program=(const char*)sqlite3_column_text(st,3),*kind=(const char*)sqlite3_column_text(st,4);char identity[1024],owner[512],program_dep[768];const char *axes[]={"subject","material","operation","extent","audience","application"};const char *vals[]={subject?subject:"","media","watch","page",visibility?visibility:"",kind?kind:""};const char *deps[2];if(!id||!*id||!subject||!*subject||!program||!*program)continue;if(snprintf(identity,sizeof(identity),"channel:%s:%s",id,program)>=(int)sizeof(identity)||snprintf(owner,sizeof(owner),"CreatorChannel:%s",id)>=(int)sizeof(owner)||snprintf(program_dep,sizeof(program_dep),"CMS:media_program:%s",program)>=(int)sizeof(program_dep))continue;deps[0]=owner;deps[1]=program_dep;emit_record(identity,"creator_channel",owner,"CreatorChannel:visibility","ChannelSurfaceCompiler:surface",owner,axes,vals,6,deps,2);emitted++;}
    sqlite3_finalize(st);return emitted;
}

/* InteractionGrammar is a bounded donor-informed compiler vocabulary. A row
 * is emitted only after a native binding names its real subject and owner.
 * The grammar never carries owner facts; its exact verifier is invoked by the
 * native InteractionGrammarCompiler after forwarding.  The compiler then
 * emits a pending-exact-owner composition artifact; it does not render or
 * enact the grammar. */
static int emit_interaction_grammar(sqlite3 *grammar) {
    sqlite3_stmt *st=NULL; int emitted=0;
    if(!table_exists(grammar,"interaction_grammar_binding")||!table_exists(grammar,"interaction_grammar_catalog"))return 0;
    if(sqlite3_prepare_v2(grammar,"SELECT b.binding_id,b.grammar_id,b.subject_ref,b.institution_ref,b.owner_ref,b.visibility,c.application,c.operation,c.extent,c.exact_verifier,c.lowerer_ref FROM interaction_grammar_binding b JOIN interaction_grammar_catalog c ON c.grammar_id=b.grammar_id WHERE b.state='active' ORDER BY b.binding_id",-1,&st,NULL)!=SQLITE_OK)return -1;
    while(sqlite3_step(st)==SQLITE_ROW){
        const char *id=(const char*)sqlite3_column_text(st,0),*gid=(const char*)sqlite3_column_text(st,1),*subject=(const char*)sqlite3_column_text(st,2),*institution=(const char*)sqlite3_column_text(st,3),*owner=(const char*)sqlite3_column_text(st,4),*visibility=(const char*)sqlite3_column_text(st,5),*application=(const char*)sqlite3_column_text(st,6),*operation=(const char*)sqlite3_column_text(st,7),*extent=(const char*)sqlite3_column_text(st,8),*verifier=(const char*)sqlite3_column_text(st,9),*lowerer=(const char*)sqlite3_column_text(st,10);
        char identity[1024],frontier[768]; const char *axes[7],*vals[7],*deps[3]; size_t terms=0,depc=0;
        if(!id||!*id||!gid||!*gid||!subject||!*subject||!owner||!*owner||!visibility||!*visibility||!application||!*application||!operation||!*operation||!extent||!*extent||!verifier||!*verifier||!lowerer||!*lowerer)continue;
        if(snprintf(identity,sizeof(identity),"interaction-grammar:%s",id)>=(int)sizeof(identity)||snprintf(frontier,sizeof(frontier),"interaction-grammar:%s",id)>=(int)sizeof(frontier))continue;
        axes[terms]="subject";vals[terms++]=subject; axes[terms]="grammar";vals[terms++]=gid; axes[terms]="application";vals[terms++]=application; axes[terms]="operation";vals[terms++]=operation; axes[terms]="extent";vals[terms++]=extent; axes[terms]="audience";vals[terms++]=visibility;
        if(institution&&*institution&&terms<7){axes[terms]="institution";vals[terms++]=institution;}
        deps[depc++]=frontier;deps[depc++]=owner;
        emit_record(identity,"interaction_grammar",owner,verifier,"InteractionGrammarCompiler:surface",frontier,axes,vals,terms,deps,depc);emitted++;
    }
    sqlite3_finalize(st);return emitted;
}

/* MCP is a dynamic machine projection.  This accepts an authenticated gateway
 * closure snapshot (the JSON-RPC tools/resources/prompts lists), rather than
 * scraping a gateway's source or turning its global catalogue into authority.
 * The caller's ActorClosure remains with MCP; this records only a candidate
 * aperture and requires the gateway to verify scope again when used. */
static int emit_mcp(const char *path) {
    sqlite3 *db=NULL;sqlite3_stmt *st=NULL,*valid=NULL;char *body=NULL;size_t n=0;int emitted=0,ok=0;
    if(!(body=read_all(path,&n)))return -1;if(sqlite3_open(":memory:",&db)!=SQLITE_OK){free(body);return -1;}
    /* A closure is a complete authenticated MCP aperture.  Treat a malformed
     * or partial snapshot as a failed generation, never as an empty closure
     * that could atomically retire valid advertisements. */
    if(sqlite3_prepare_v2(db,"SELECT json_valid(?1) AND json_type(?1,'$.tools')='array' AND json_type(?1,'$.resources')='array' AND json_type(?1,'$.resourceTemplates')='array' AND json_type(?1,'$.prompts')='array'",-1,&valid,NULL)!=SQLITE_OK){sqlite3_close(db);free(body);return -1;}
    sqlite3_bind_text(valid,1,body,n,SQLITE_TRANSIENT);if(sqlite3_step(valid)==SQLITE_ROW)ok=sqlite3_column_int(valid,0);sqlite3_finalize(valid);valid=NULL;if(!ok){sqlite3_close(db);free(body);return -1;}
    if(sqlite3_prepare_v2(db,
        "SELECT 'tool',json_extract(value,'$.name') FROM json_each(?1,'$.tools') UNION ALL "
        "SELECT 'resource',json_extract(value,'$.uri') FROM json_each(?1,'$.resources') UNION ALL "
        "SELECT 'resource_template',json_extract(value,'$.uriTemplate') FROM json_each(?1,'$.resourceTemplates') UNION ALL "
        "SELECT 'prompt',json_extract(value,'$.name') FROM json_each(?1,'$.prompts')",-1,&st,NULL)!=SQLITE_OK){sqlite3_close(db);free(body);return -1;}
    sqlite3_bind_text(st,1,body,n,SQLITE_TRANSIENT);
    {int rc;while((rc=sqlite3_step(st))==SQLITE_ROW){const char *type=(const char*)sqlite3_column_text(st,0),*name=(const char*)sqlite3_column_text(st,1);char identity[640],ref[640],lowerer[640];const char*axes[]={"mcp","projection","operation"};const char*vals[]={type?type:"",type?type:"",name?name:""};const char*deps[1];if(!type||!name||!*name)continue;snprintf(identity,sizeof(identity),"mcp:%s:%s",type,name);snprintf(ref,sizeof(ref),"MCPClosure:%s:%s",type,name);snprintf(lowerer,sizeof(lowerer),"MCP:%s",type);deps[0]="mcp:closure";emit_record(identity,"mcp_projection",ref,"MCPClosure:scope_recheck",lowerer,"",axes,vals,3,deps,1);emitted++;}ok=rc==SQLITE_DONE;}
    sqlite3_finalize(st);sqlite3_close(db);free(body);return ok?emitted:-1;
}

/* PhysicalCompiler consumes a compact passport declaration rather than a
 * duplicate candidate graph.  This exporter is intentionally generic over
 * candidate_kind: a newly compiled owner population receives the same dense
 * forwarding representation without an if/else branch here.  The resulting
 * record is declared (not a performance measurement) and never carries owner
 * facts, authority, evidence, or a selected lowerer. */
static int emit_representation_passports(sqlite3 *db) {
    sqlite3_stmt *st=NULL; int emitted=0;
    const char *q =
      "SELECT identity,candidate_kind,owner_ref,canonical_json FROM candidate_advertisement ORDER BY identity";
    if(sqlite3_prepare_v2(db,q,-1,&st,NULL)!=SQLITE_OK)return -1;
    while(sqlite3_step(st)==SQLITE_ROW) {
        const char *id=(const char*)sqlite3_column_text(st,0),*kind=(const char*)sqlite3_column_text(st,1),*owner=(const char*)sqlite3_column_text(st,2),*canonical=(const char*)sqlite3_column_text(st,3);
        char passport[1024],semantic[65],binding[65],body[1024];
        int canonical_n=sqlite3_column_bytes(st,3);
        if(!id||!*id||!kind||!*kind||!owner||!canonical||canonical_n<1||!candidate_kind_valid(kind))continue;
        if(snprintf(passport,sizeof(passport),"addressplane:advertisement:%s:sqlite-v1",id)>=(int)sizeof(passport)||snprintf(body,sizeof(body),"sqlite:address-plane/candidate_advertisement/%s",id)>=(int)sizeof(body))continue;
        bf_sha256_hex((const uint8_t*)canonical,(size_t)canonical_n,semantic);
        bf_sha256_hex((const uint8_t*)owner,strlen(owner),binding);
        fputs("{\"identity\":",stdout);json_text(passport);fputs(",\"subject_family\":",stdout);json_text(kind);fputs(",\"semantic_digest\":\"sha256:",stdout);fputs(semantic,stdout);fputs("\",\"representation_family\":\"sqlite-address-index\",\"representation_version\":\"1\",\"exactness_class\":\"exact\",\"quality_loss_budget\":0,\"measurement_state\":\"declared\",\"shared_basis\":\"addressplane:candidate-advertisement:v1\",\"binding_digest\":\"sha256:",stdout);fputs(binding,stdout);fputs("\",\"exception_digest\":\"\",\"physical_body_ref\":",stdout);json_text(body);fputs(",\"physical_digest\":\"sha256:",stdout);fputs(semantic,stdout);fputs("\",\"supports\":[\"filtering\",\"ordering\",\"partial_decode\",\"random_access\",\"delta_application\"],\"lift_ref\":\"AddressPlane:CandidateAdvertisement\",\"bytes\":0}\n",stdout);
        emitted++;
    }
    sqlite3_finalize(st); return emitted;
}

/* InformationPolicy decides disclosure per field and per audience, never per
 * record, and an undeclared field is withheld by default.  The plane cannot
 * make that decision, so what is compiled here is only the declaration's
 * shape: one candidate per (subject kind, audience) carrying the fields that
 * audience is declared to be able to see.  A field withheld from an audience
 * is absent rather than marked, so the forwarding cut is default-deny in the
 * same direction as the evaluator.  Cannot infer: a field term is a declared
 * disclosure surface, never a disclosure -- the exact evaluator still decides,
 * still returns withheld fields with their reason, and still owns aggregation
 * floors and evidence release, none of which are forwardable. */
static int emit_policy(const char *path) {
    sqlite3 *db=NULL;sqlite3_stmt *pairs=NULL,*fields=NULL,*evidence=NULL,*valid=NULL;char *body=NULL;size_t n=0;int emitted=0,ok=0;
    if(!(body=read_all(path,&n)))return -1;
    if(sqlite3_open(":memory:",&db)!=SQLITE_OK){free(body);return -1;}
    /* A profile set is a complete declaration.  A malformed file is a failed
     * generation, never an empty policy that could retire valid records. */
    if(sqlite3_prepare_v2(db,"SELECT json_valid(?1) AND json_type(?1)='array'",-1,&valid,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(valid,1,body,(int)n,SQLITE_TRANSIENT);
    if(sqlite3_step(valid)==SQLITE_ROW)ok=sqlite3_column_int(valid,0);
    sqlite3_finalize(valid);valid=NULL;
    if(!ok)goto done;
    ok=0;
    if(sqlite3_prepare_v2(db,
        "SELECT DISTINCT json_extract(p.value,'$.name'),a.value FROM json_each(?1) p, json_each(p.value,'$.fields') f, json_each(f.value,'$.audiences') a "
        "WHERE json_extract(p.value,'$.name') IS NOT NULL AND a.type='text' ORDER BY 1,2",-1,&pairs,NULL)!=SQLITE_OK)goto done;
    if(sqlite3_prepare_v2(db,
        "SELECT json_extract(f.value,'$.name') FROM json_each(?1) p, json_each(p.value,'$.fields') f, json_each(f.value,'$.audiences') a "
        "WHERE json_extract(p.value,'$.name')=?2 AND a.value=?3 AND json_extract(f.value,'$.name') IS NOT NULL ORDER BY 1",-1,&fields,NULL)!=SQLITE_OK)goto done;
    if(sqlite3_prepare_v2(db,
        "SELECT e.value FROM json_each(?1) p, json_each(p.value,'$.evidence_requirements') e "
        "WHERE json_extract(p.value,'$.name')=?2 AND e.type='text' ORDER BY 1",-1,&evidence,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(pairs,1,body,(int)n,SQLITE_TRANSIENT);
    while(sqlite3_step(pairs)==SQLITE_ROW){
      const char*profile=(const char*)sqlite3_column_text(pairs,0),*audience=(const char*)sqlite3_column_text(pairs,1);
      char identity[640],owner[640],frontier[640],publish[256],term_storage[64][256];const char*axes[64],*vals[64],*deps[4];size_t terms=0,depc=0;
      if(!profile||!*profile||!audience||!*audience)continue;
      if(snprintf(identity,sizeof(identity),"policy:%s:%s",profile,audience)>=(int)sizeof(identity))continue;
      snprintf(owner,sizeof(owner),"InformationPolicy:%s",profile);snprintf(frontier,sizeof(frontier),"policy:%s",profile);snprintf(publish,sizeof(publish),"policy:declaration");
      axes[terms]="subject_kind";vals[terms++]=profile;axes[terms]="audience";vals[terms++]=audience;
      sqlite3_bind_text(fields,1,body,(int)n,SQLITE_TRANSIENT);sqlite3_bind_text(fields,2,profile,-1,SQLITE_TRANSIENT);sqlite3_bind_text(fields,3,audience,-1,SQLITE_TRANSIENT);
      while(terms<64&&sqlite3_step(fields)==SQLITE_ROW){const char*f=(const char*)sqlite3_column_text(fields,0);if(!f||!*f)continue;snprintf(term_storage[terms],sizeof(term_storage[terms]),"%s",f);axes[terms]="field";vals[terms]=term_storage[terms];terms++;}
      sqlite3_reset(fields);sqlite3_clear_bindings(fields);
      sqlite3_bind_text(evidence,1,body,(int)n,SQLITE_TRANSIENT);sqlite3_bind_text(evidence,2,profile,-1,SQLITE_TRANSIENT);
      while(terms<64&&sqlite3_step(evidence)==SQLITE_ROW){const char*e=(const char*)sqlite3_column_text(evidence,0);if(!e||!*e)continue;snprintf(term_storage[terms],sizeof(term_storage[terms]),"%s",e);axes[terms]="evidence";vals[terms]=term_storage[terms];terms++;}
      sqlite3_reset(evidence);sqlite3_clear_bindings(evidence);
      deps[depc++]=frontier;deps[depc++]=publish;
      emit_record(identity,"information_policy",owner,"InformationPolicy:evaluate","InformationPolicy:disclosure",frontier,axes,vals,terms,deps,depc);emitted++;
    }
    ok=1;
done:
    if(valid)sqlite3_finalize(valid);if(pairs)sqlite3_finalize(pairs);if(fields)sqlite3_finalize(fields);if(evidence)sqlite3_finalize(evidence);
    sqlite3_close(db);free(body);return ok?emitted:-1;
}

typedef enum AdvertisementEmitter { EMIT_CMS, EMIT_TARGETS, EMIT_PROVIDERS, EMIT_COMPUTE, EMIT_PROTOCOLS, EMIT_MCP, EMIT_POLICY, EMIT_MEDIA_LIBRARY, EMIT_CREATOR_CHANNEL, EMIT_INTERACTION_GRAMMAR } AdvertisementEmitter;
static int emit_protocols(const char *path);
static const char *emitter_prefix(AdvertisementEmitter kind) { static const char *const prefixes[]={"CMS:","TargetObligation:","Provider","ModelCatalog:","ProtocolCommons:","MCPClosure:","InformationPolicy:","MediaLibrary:","CreatorChannel:","InteractionGrammar:"};return prefixes[(int)kind]; }
static int capture_emitter(int out_fd, AdvertisementEmitter kind, const char *path) {
    pid_t pid;int status=0;if(!path||!*path)return 1;
    /* Never let a buffered parent receipt cross the fork into the private
     * generation stream.  In particular an MCP refresh receipt belongs to the
     * caller, not to the JSONL CandidateAdvertisement snapshot. */
    if(fflush(NULL))return 0;
    pid=fork();if(pid<0)return 0;
    if(pid==0){sqlite3 *source=NULL;int emitted=-1;if(dup2(out_fd,STDOUT_FILENO)<0)_exit(127);close(out_fd);
        if(kind==EMIT_PROTOCOLS)emitted=emit_protocols(path);else if(kind==EMIT_MCP)emitted=emit_mcp(path);else if(kind==EMIT_POLICY)emitted=emit_policy(path);else if(sqlite3_open_v2(path,&source,SQLITE_OPEN_READONLY,NULL)==SQLITE_OK){emitted=kind==EMIT_CMS?emit_cms(source):kind==EMIT_TARGETS?emit_targets(source):kind==EMIT_PROVIDERS?emit_providers(source):kind==EMIT_COMPUTE?emit_compute(source):kind==EMIT_MEDIA_LIBRARY?emit_media_library(source):kind==EMIT_CREATOR_CHANNEL?emit_creator_channel(source):emit_interaction_grammar(source);sqlite3_close(source);} fflush(stdout);_exit(emitted<0?1:0);}
    if(waitpid(pid,&status,0)<0)return 0;return WIFEXITED(status)&&WEXITSTATUS(status)==0;
}
static int replace_advertisement_generation(sqlite3 *db, const char *stage_path, unsigned selected) {
    sqlite3_stmt *attach=NULL;char stage_uri[1024];
    if(!exec_sql(db,"BEGIN IMMEDIATE;"))return 0;
    if(sqlite3_prepare_v2(db,"ATTACH DATABASE ? AS advertisement_stage",-1,&attach,NULL)!=SQLITE_OK)goto fail;
    sqlite3_bind_text(attach,1,stage_path,-1,SQLITE_TRANSIENT);if(sqlite3_step(attach)!=SQLITE_DONE){sqlite3_finalize(attach);goto fail;}sqlite3_finalize(attach);attach=NULL;
    for(int i=0;i<=EMIT_INTERACTION_GRAMMAR;i++)if(selected&(1u<<i)){
        if(i==EMIT_INTERACTION_GRAMMAR){
            if(sqlite3_prepare_v2(db,"DELETE FROM candidate_advertisement_dependency WHERE identity IN (SELECT identity FROM candidate_advertisement WHERE candidate_kind='interaction_grammar')",-1,&attach,NULL)!=SQLITE_OK)goto fail;if(sqlite3_step(attach)!=SQLITE_DONE){sqlite3_finalize(attach);goto fail;}sqlite3_finalize(attach);attach=NULL;
            if(sqlite3_prepare_v2(db,"DELETE FROM candidate_advertisement_term WHERE identity IN (SELECT identity FROM candidate_advertisement WHERE candidate_kind='interaction_grammar')",-1,&attach,NULL)!=SQLITE_OK)goto fail;if(sqlite3_step(attach)!=SQLITE_DONE){sqlite3_finalize(attach);goto fail;}sqlite3_finalize(attach);attach=NULL;
            if(sqlite3_prepare_v2(db,"DELETE FROM candidate_advertisement WHERE candidate_kind='interaction_grammar'",-1,&attach,NULL)!=SQLITE_OK)goto fail;if(sqlite3_step(attach)!=SQLITE_DONE){sqlite3_finalize(attach);goto fail;}sqlite3_finalize(attach);attach=NULL;continue;
        }
        snprintf(stage_uri,sizeof(stage_uri),"%s%%",emitter_prefix((AdvertisementEmitter)i));
        if(sqlite3_prepare_v2(db,"DELETE FROM candidate_advertisement_dependency WHERE identity IN (SELECT identity FROM candidate_advertisement WHERE owner_ref LIKE ?)",-1,&attach,NULL)!=SQLITE_OK)goto fail;sqlite3_bind_text(attach,1,stage_uri,-1,SQLITE_TRANSIENT);if(sqlite3_step(attach)!=SQLITE_DONE){sqlite3_finalize(attach);goto fail;}sqlite3_finalize(attach);attach=NULL;
        if(sqlite3_prepare_v2(db,"DELETE FROM candidate_advertisement_term WHERE identity IN (SELECT identity FROM candidate_advertisement WHERE owner_ref LIKE ?)",-1,&attach,NULL)!=SQLITE_OK)goto fail;sqlite3_bind_text(attach,1,stage_uri,-1,SQLITE_TRANSIENT);if(sqlite3_step(attach)!=SQLITE_DONE){sqlite3_finalize(attach);goto fail;}sqlite3_finalize(attach);attach=NULL;
        if(sqlite3_prepare_v2(db,"DELETE FROM candidate_advertisement WHERE owner_ref LIKE ?",-1,&attach,NULL)!=SQLITE_OK)goto fail;sqlite3_bind_text(attach,1,stage_uri,-1,SQLITE_TRANSIENT);if(sqlite3_step(attach)!=SQLITE_DONE){sqlite3_finalize(attach);goto fail;}sqlite3_finalize(attach);attach=NULL;}
    if(!exec_sql(db,"INSERT INTO candidate_advertisement SELECT * FROM advertisement_stage.candidate_advertisement;INSERT INTO candidate_advertisement_term SELECT * FROM advertisement_stage.candidate_advertisement_term;INSERT INTO candidate_advertisement_dependency SELECT * FROM advertisement_stage.candidate_advertisement_dependency;"))goto fail;
    if(!rebuild_forwarding_index(db)||!exec_sql(db,"COMMIT;"))goto fail_no_detach;
    /* The generation is already atomically visible after COMMIT.  Detach is
     * resource cleanup, not another mutation boundary. */
    (void)exec_sql(db,"DETACH DATABASE advertisement_stage;");return 1;
fail:sqlite3_finalize(attach);exec_sql(db,"DETACH DATABASE advertisement_stage;");
fail_no_detach:exec_sql(db,"ROLLBACK;");return 0;
}

/* The durable compilation event.  All selected owner snapshots are emitted to
 * a private file, fully validated by a disposable index, then atomically
 * substituted into the live derived index.  A failed emitter or malformed
 * descriptor leaves the live generation untouched. */
static int compile_advertisement_generation(const char *index_path,const char *cms,const char *targets,const char *providers,const char *models,const char *protocols,const char *mcp,const char *policy,const char *media,const char *channels,const char *grammars) {
    const char *paths[]={cms,targets,providers,models,protocols,mcp,policy,media,channels,grammars};char snapshot[]="/tmp/bonfyre-address-generation-XXXXXX",stage[]="/tmp/bonfyre-address-stage-XXXXXX";int fd=-1,sfd=-1;unsigned selected=0;sqlite3 *preflight=NULL,*live=NULL;int ok=0;
    fd=mkstemp(snapshot);if(fd<0)return 0;for(int i=0;i<=EMIT_INTERACTION_GRAMMAR;i++)if(paths[i]&&*paths[i]){selected|=1u<<i;if(!capture_emitter(fd,(AdvertisementEmitter)i,paths[i]))goto done;}if(!selected)goto done;close(fd);fd=-1;
    sfd=mkstemp(stage);if(sfd<0)goto done;close(sfd);sfd=-1;
    if(sqlite3_open(stage,&preflight)!=SQLITE_OK||!init_schema(preflight)||!import_advertisements(preflight,snapshot,NULL,NULL))goto done;
    sqlite3_close(preflight);preflight=NULL;
    if(sqlite3_open(index_path,&live)!=SQLITE_OK||!init_schema(live)||!replace_advertisement_generation(live,stage,selected))goto done;
    printf("{\"compiled_owner_populations\":%u,\"state\":\"published_derived_generation\",\"truth\":\"eligible_for_further_consideration\"}\n",(unsigned)__builtin_popcount(selected));ok=1;
done:if(fd>=0)close(fd);if(sfd>=0)close(sfd);if(preflight)sqlite3_close(preflight);if(live)sqlite3_close(live);unlink(snapshot);unlink(stage);return ok;
}

static int compile_owner_sources(const char *index_path,const char *cms,const char *targets,const char *providers,const char *models,const char *protocols,const char *mcp,const char *mcp_url,const char *mcp_token,const char *policy,const char *media,const char *channels,const char *grammars) {
    char refreshed[]="/tmp/bonfyre-mcp-closure-XXXXXX";int fd=-1,ok;
    if(mcp_url&&*mcp_url){
        if((mcp&&*mcp)||!mcp_token||!*mcp_token)return 0;
        if((fd=mkstemp(refreshed))<0)return 0;close(fd);fd=-1;
        if(!refresh_mcp_closure(mcp_url,mcp_token,refreshed)){unlink(refreshed);return 0;}
        ok=compile_advertisement_generation(index_path,cms,targets,providers,models,protocols,refreshed,policy,media,channels,grammars);unlink(refreshed);return ok;
    }
    return compile_advertisement_generation(index_path,cms,targets,providers,models,protocols,mcp,policy,media,channels,grammars);
}

/* A derived physical registry is rebuilt from an immutable snapshot of the
 * current AddressPlane generation.  The snapshot is fsync'd before the native
 * PhysicalCompiler sees it; that compiler preflights and atomically replaces
 * its own handle registry.  Neither side acquires owner state or runs an
 * effect. */
static int capture_representation_passports(int out_fd,const char *index_path) {
    pid_t pid; int status=0;
    /* compile_owner_sources reports its completed derived generation before
     * this child redirects stdout to the immutable passport snapshot.  Flush
     * those parent receipts first: otherwise stdio's inherited buffer becomes
     * a non-passport first JSONL line in the physical compiler input. */
    if(fflush(NULL))return 0;
    pid=fork();
    if(pid<0)return 0;
    if(pid==0){sqlite3 *source=NULL;int emitted=-1;if(dup2(out_fd,STDOUT_FILENO)<0)_exit(127);close(out_fd);if(sqlite3_open_v2(index_path,&source,SQLITE_OPEN_READONLY,NULL)==SQLITE_OK){emitted=emit_representation_passports(source);sqlite3_close(source);}fflush(stdout);_exit(emitted<0?1:0);}
    if(waitpid(pid,&status,0)<0)return 0;return WIFEXITED(status)&&WEXITSTATUS(status)==0;
}
typedef enum {
    BF_PHYSICAL_COMPILE_ERROR = 0,
    BF_PHYSICAL_COMPILE_OK,
    BF_PHYSICAL_COMPILE_DEFER,
    BF_PHYSICAL_COMPILE_REJECT
} BfPhysicalCompileState;

/* The registry compiler is derived work, but its scratch snapshot and atomic
 * replacement are still physical work.  This gate deliberately reserves only
 * the measured snapshot aperture; it does not claim to account for persistent
 * representation ownership or select a representation. */
static BfPhysicalCompileState compile_physical_passports(const char *index_path,const char *physical_db,const char *compiler,const char *admission_db,const char *admission_volume,const char *admission_plane,const char *admission_free_bytes,const char *protected_floor_bytes,const char *per_plane_quota_bytes,const char *max_grant_bytes) {
    char snapshot[]="/tmp/bonfyre-physical-passports-XXXXXX";int fd=-1,status=0;pid_t pid;int ok=0;struct stat snapshot_stat;BfAdmissionGrants *grants=NULL;BfAdmissionDecision decision;BfAdmissionPolicy policy={0};BfResourceRequest request;int64_t grant_id=0,free_bytes=0;BfAdmissionVerdict verdict;
    if(!index_path||!*index_path||!physical_db||!*physical_db||!compiler||!*compiler)return 0;
    if((fd=mkstemp(snapshot))<0)return 0;
    if(!capture_representation_passports(fd,index_path)){close(fd);unlink(snapshot);return 0;}
    if(fsync(fd)||fstat(fd,&snapshot_stat)||close(fd)){fd=-1;unlink(snapshot);return 0;}fd=-1;
    if(admission_db&&*admission_db){
        if(bf_admission_open(admission_db,&grants,stderr)!=0)goto done;
        bf_admission_policy_defaults(&policy);
        if(protected_floor_bytes&&*protected_floor_bytes)policy.protected_floor_bytes=strtoll(protected_floor_bytes,NULL,10);
        if(per_plane_quota_bytes&&*per_plane_quota_bytes)policy.per_plane_quota_bytes=strtoll(per_plane_quota_bytes,NULL,10);
        if(max_grant_bytes&&*max_grant_bytes)policy.max_grant_bytes=strtoll(max_grant_bytes,NULL,10);
        free_bytes=admission_free_bytes&&*admission_free_bytes?strtoll(admission_free_bytes,NULL,10):bf_admission_disk_free(physical_db);
        if(free_bytes<0)goto done;
        request.plane=admission_plane&&*admission_plane?admission_plane:"physical-compiler";
        request.kind="physical-passport-snapshot";
        request.estimated_bytes=(int64_t)snapshot_stat.st_size;
        request.volume=admission_volume&&*admission_volume?admission_volume:physical_db;
        request.detail="AddressPlane derived RepresentationPassport snapshot";
        verdict=bf_admission_request_grant(grants,&request,&policy,free_bytes,NULL,&decision,&grant_id);
        if(verdict==BF_ADMISSION_DEFER){bf_admission_close(grants);unlink(snapshot);return BF_PHYSICAL_COMPILE_DEFER;}
        if(verdict==BF_ADMISSION_REJECT){bf_admission_close(grants);unlink(snapshot);return BF_PHYSICAL_COMPILE_REJECT;}
    }
    pid=fork();if(pid<0)goto done;
    if(pid==0){execl(compiler,compiler,"compile-passports","--db",physical_db,"--input",snapshot,(char*)NULL);_exit(127);}
    if(waitpid(pid,&status,0)<0)goto done;ok=WIFEXITED(status)&&WEXITSTATUS(status)==0;
done:if(grants){if(grant_id>0)bf_admission_release_grant(grants,grant_id,NULL);bf_admission_close(grants);}if(fd>=0)close(fd);unlink(snapshot);return ok?BF_PHYSICAL_COMPILE_OK:BF_PHYSICAL_COMPILE_ERROR;
}

/* A bounded durable worker turn.  Generation replacement happens before delta
 * consumption, so a cut always sees one coherent forwarding generation.  The
 * queue is the handoff to a declared lowerer; this forwarding organ never
 * invokes lowerers or turns a dependency hit into a semantic transition. */
static int run_address_worker(const char *index_path,const char *cms,const char *targets,const char *providers,const char *models,const char *protocols,const char *mcp,const char *mcp_url,const char *mcp_token,const char *policy,const char *media,const char *channels,const char *grammars,const char *delta_dir,const char *lowerer_dir,const char *physical_db,const char *physical_compiler,const char *admission_db,const char *admission_volume,const char *admission_plane,const char *admission_free_bytes,const char *protected_floor_bytes,const char *per_plane_quota_bytes,const char *max_grant_bytes,const char *owner_image,int limit) {
    int have_sources=(cms&&*cms)||(targets&&*targets)||(providers&&*providers)||(models&&*models)||(protocols&&*protocols)||(mcp&&*mcp)||(mcp_url&&*mcp_url)||(policy&&*policy)||(media&&*media)||(channels&&*channels)||(grammars&&*grammars);
    int have_physical=(physical_db&&*physical_db)||(physical_compiler&&*physical_compiler);
    int have_admission_options=(admission_volume&&*admission_volume)||(admission_plane&&*admission_plane)||(admission_free_bytes&&*admission_free_bytes)||(protected_floor_bytes&&*protected_floor_bytes)||(per_plane_quota_bytes&&*per_plane_quota_bytes)||(max_grant_bytes&&*max_grant_bytes);
    BfPhysicalCompileState physical_state=BF_PHYSICAL_COMPILE_ERROR;
    sqlite3 *db=NULL;
    if(!index_path||!*index_path||(!have_sources&&(!delta_dir||!*delta_dir)&&(!lowerer_dir||!*lowerer_dir)&&!have_physical))return 0;
    if(owner_image&&*owner_image&&!owner_contract_image_valid(owner_image))return 0;
    if(have_physical&&(!physical_db||!*physical_db||!physical_compiler||!*physical_compiler))return 0;
    if((admission_db&&*admission_db)&&!have_physical)return 0;
    if(have_admission_options&&(!admission_db||!*admission_db))return 0;
    if(have_sources&&!compile_owner_sources(index_path,cms,targets,providers,models,protocols,mcp,mcp_url,mcp_token,policy,media,channels,grammars))return 0;
    if(have_physical){physical_state=compile_physical_passports(index_path,physical_db,physical_compiler,admission_db,admission_volume,admission_plane,admission_free_bytes,protected_floor_bytes,per_plane_quota_bytes,max_grant_bytes);if(physical_state==BF_PHYSICAL_COMPILE_ERROR)return 0;}
    if(delta_dir&&*delta_dir){
        if(sqlite3_open(index_path,&db)!=SQLITE_OK)return 0;
        if(!init_schema(db)||!drain_fact_deltas(db,delta_dir,limit)){sqlite3_close(db);return 0;}
    }
    if(lowerer_dir&&*lowerer_dir){if(!db&&sqlite3_open(index_path,&db)!=SQLITE_OK)return 0;if(!init_schema(db)||!drain_lowering_queue(db,lowerer_dir,limit)){if(db)sqlite3_close(db);return 0;}}
    if(db)sqlite3_close(db);
    printf("{\"worker\":\"address-plane\",\"owner_contract_image\":%s,\"published_generation\":%s,\"physical_registry\":%s,\"physical_admission\":\"%s\",\"drained_fact_deltas\":%s,\"lowerer_handoff\":%s,\"truth\":\"forwarding_only\"}\n",owner_image&&*owner_image?"true":"false",have_sources?"true":"false",physical_state==BF_PHYSICAL_COMPILE_OK?"true":"false",!have_physical?"DISABLED":!admission_db||!*admission_db?"UNMANAGED":physical_state==BF_PHYSICAL_COMPILE_OK?"ADMIT":physical_state==BF_PHYSICAL_COMPILE_DEFER?"DEFER":"REJECT",delta_dir&&*delta_dir?"true":"false",lowerer_dir&&*lowerer_dir?"true":"false");
    return 1;
}

static int emit_protocols(const char *path) {
    sqlite3 *db=NULL;sqlite3_stmt *st=NULL;char *body=NULL;size_t n=0;int emitted=0;
    if(!(body=read_all(path,&n)))return -1;if(sqlite3_open(":memory:",&db)!=SQLITE_OK){free(body);return -1;}
    if(sqlite3_exec(db,"CREATE TABLE p(j TEXT)",NULL,NULL,NULL)!=SQLITE_OK){sqlite3_close(db);free(body);return -1;}
    {char *line=body,*next;while(line&&*line){next=strchr(line,'\n');if(next)*next++='\0';if(*line){sqlite3_stmt *ins=NULL;if(sqlite3_prepare_v2(db,"INSERT INTO p VALUES(?)",-1,&ins,NULL)!=SQLITE_OK){sqlite3_close(db);free(body);return -1;}sqlite3_bind_text(ins,1,line,-1,SQLITE_TRANSIENT);if(sqlite3_step(ins)!=SQLITE_DONE){sqlite3_finalize(ins);sqlite3_close(db);free(body);return -1;}sqlite3_finalize(ins);}line=next;}}
    if(sqlite3_prepare_v2(db,"SELECT json_extract(j,'$.id'),j FROM p ORDER BY json_extract(j,'$.id')",-1,&st,NULL)!=SQLITE_OK){sqlite3_close(db);free(body);return -1;}
    while(sqlite3_step(st)==SQLITE_ROW){const char *id=(const char*)sqlite3_column_text(st,0);char identity[320],ref[320];const char*axes[]={"protocol","operation"};const char*vals[]={id?id:"","interaction"};const char*deps[1];if(!id||!*id)continue;snprintf(identity,sizeof(identity),"protocol:%s",id);snprintf(ref,sizeof(ref),"ProtocolCommons:%s",id);deps[0]=ref;emit_record(identity,"protocol_chain",ref,"ProtocolContract:conformance","ProtocolCompiler:late_carrier","",axes,vals,2,deps,1);emitted++;}sqlite3_finalize(st);sqlite3_close(db);free(body);return emitted;
}
static void terms_html(sqlite3 *db, const char *id, const char *axis, const char *label, const char *klass) {
    sqlite3_stmt *st=NULL; int any=0;
    if (sqlite3_prepare_v2(db,"SELECT term FROM occurrence_term WHERE occurrence_id=?1 AND axis=?2 ORDER BY term LIMIT 12",-1,&st,NULL)!=SQLITE_OK) return;
    sqlite3_bind_text(st,1,id,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,2,axis,-1,SQLITE_TRANSIENT);
    printf("<p class=\"%s\"><strong>",klass); esc(label); printf("</strong> ");
    while(sqlite3_step(st)==SQLITE_ROW) { const char *term=(const char*)sqlite3_column_text(st,0); if(any++)printf(" <span aria-hidden=\"true\">·</span> "); esc(term); }
    if (!any) printf("<span>unresolved</span>");
    printf("</p>"); sqlite3_finalize(st);
}
/* The same terms the card renders, as an array.
 *
 * Beside terms_html rather than instead of it: the human and the machine read
 * one query and differ only in how the answer is written down. A second SELECT
 * here would be a second corpus, which is the failure the surface test names
 * in its own docstring. */
static void terms_json(sqlite3 *db, const char *id, const char *axis, const char *label) {
    sqlite3_stmt *st=NULL; int any=0;
    char buf[512];
    if (sqlite3_prepare_v2(db,"SELECT term FROM occurrence_term WHERE occurrence_id=?1 AND axis=?2 ORDER BY term LIMIT 12",-1,&st,NULL)!=SQLITE_OK) { printf(",\"%s\":[]",label); return; }
    sqlite3_bind_text(st,1,id,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,2,axis,-1,SQLITE_TRANSIENT);
    printf(",\"%s\":[",label);
    while(sqlite3_step(st)==SQLITE_ROW) {
        const char *term=(const char*)sqlite3_column_text(st,0);
        if(!term) continue;
        bf_lex_json_escape(term, buf, sizeof(buf));
        printf("%s\"%s\"", any++ ? "," : "", buf);
    }
    printf("]"); sqlite3_finalize(st);
}

static void json_field(const char *key, const char *value) {
    char buf[4096];
    bf_lex_json_escape(value ? value : "", buf, sizeof(buf));
    printf("\"%s\":\"%s\"", key, buf);
}

static int route(sqlite3 *db, const char *id, const char *expected_url) {
    sqlite3_stmt *st=NULL;
    const char *name,*kind,*ref,*section,*url;
    if (!id || !*id || !expected_url || !*expected_url) return 0;
    if (sqlite3_prepare_v2(db,
        "SELECT observed_name,source_kind,source_ref,section,primary_url FROM occurrence WHERE id=?1 AND primary_url=?2",
        -1,&st,NULL)!=SQLITE_OK) return 0;
    sqlite3_bind_text(st,1,id,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,2,expected_url,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(st)!=SQLITE_ROW) { sqlite3_finalize(st); return 0; }
    name=(const char*)sqlite3_column_text(st,0); kind=(const char*)sqlite3_column_text(st,1);
    ref=(const char*)sqlite3_column_text(st,2); section=(const char*)sqlite3_column_text(st,3);
    url=(const char*)sqlite3_column_text(st,4);
    printf("<section class=\"bf-route-frame\" data-occurrence=\"");esc(id);printf("\">");
    printf("<header><p class=\"bf-route-kicker\">ExternalEstate candidate route</p><h2>");esc(name);printf("</h2>");
    printf("<p>");esc(kind);printf(" · ");esc(ref);printf("</p></header><p>");esc(section);printf("</p>");
    if (url&&*url) { printf("<p><a href=\"");esc(url);printf("\" rel=\"noreferrer\" target=\"_blank\">Observed public address</a></p>"); }
    terms_html(db,id,"domain","Candidate domains","bf-route-domains");
    terms_html(db,id,"world","Candidate worlds","bf-route-worlds");
    terms_html(db,id,"chain","Human paths","bf-route-paths");
    terms_html(db,id,"protocol","Protocol programs","bf-route-protocols");
    terms_html(db,id,"home","Composes into","bf-route-homes");
    terms_html(db,id,"role","Reagent roles","bf-route-roles");
    printf("<footer>Compiled candidate context only: no source admission, evidence, provider admission, authority, payment, settlement, or effect.</footer></section>");
    sqlite3_finalize(st);
    return 1;
}
/* One query, one corpus, two projections.
 *
 * `/_surface/external-estate` answered 200 text/html to a client that sent
 * `Accept: application/json`. It was not unserved -- it was served, and it
 * handed the machine the human's page. The surface test that has been failing
 * says exactly this in its own first line, and it had been read as "nobody is
 * running the endpoint".
 *
 * The fix is not a JSON endpoint. A second endpoint is a second corpus and
 * they drift; the test's whole point is that the same address answers both
 * readers from the same rows. So the SELECT, the binds, the ordering, the
 * limit and the refusal sentence are shared, and `as_json` changes only how
 * the answer is written down. */
static int fragment(sqlite3 *db, const char *grammar, const char *klass, const char *query, const char *domain, const char *world, const char *affordance, const char *chain, int limit, int as_json) {
    sqlite3_stmt *cell=NULL,*st=NULL; const char *question=NULL,*character=NULL,*empty=NULL,*refusal=NULL; int count=0;
    const char *sql =
      "SELECT o.id,o.observed_name,o.source_kind,o.source_ref,o.section,o.primary_url,"
      "(SELECT json_extract(j.value,'$[0]') FROM json_each(e.stage_json,'$.chain_bindings') j LIMIT 1),"
      "COALESCE(sum(CASE WHEN pd.term IS NOT NULL THEN 1 ELSE 0 END),0) score "
      "FROM occurrence o JOIN occurrence_experience e ON e.occurrence_id=o.id LEFT JOIN occurrence_term t ON t.occurrence_id=o.id "
      "LEFT JOIN presentation_demand pd ON pd.grammar=?1 AND pd.presentation_class=?2 AND pd.axis=t.axis AND pd.term=t.term "
      "WHERE (?3='' OR lower(o.observed_name||' '||o.section||' '||o.source_ref) LIKE '%'||lower(?3)||'%' OR EXISTS(SELECT 1 FROM occurrence_term q WHERE q.occurrence_id=o.id AND q.term LIKE '%'||lower(?3)||'%')) "
      "AND (?4='' OR EXISTS(SELECT 1 FROM occurrence_term d WHERE d.occurrence_id=o.id AND d.axis='domain' AND d.term=lower(?4))) "
      "AND (?5='' OR EXISTS(SELECT 1 FROM occurrence_term w WHERE w.occurrence_id=o.id AND w.axis='world' AND w.term=lower(?5))) "
      "AND (?6='' OR EXISTS(SELECT 1 FROM occurrence_term a WHERE a.occurrence_id=o.id AND a.axis='affordance' AND a.term=lower(?6))) "
      "AND (?7='' OR EXISTS(SELECT 1 FROM occurrence_term c WHERE c.occurrence_id=o.id AND c.axis='chain' AND c.term=lower(?7))) "
      "GROUP BY o.id ORDER BY score DESC,o.source_ref LIMIT ?8";
    if (!valid_token(grammar)||!valid_token(klass)) return 0;
    /* An ExperienceMap chain is an execution route.  It can select material
     * for a World compiler, but it is never a post, event, person, offer or
     * community artifact.  Discover and Stage therefore cannot lower this
     * reservoir directly; their human bodies come from their real owners. */
    if (!strcmp(grammar,"discover") || !strcmp(grammar,"stage")) {
      if (as_json) {
        printf("{\"schema\":\"bonfyre-surface-fragment.v1\",");
        json_field("grammar", grammar); printf(",");
        json_field("presentation_class", klass);
        printf(",\"records\":[],\"count\":0,\"refused\":true,\"reason\":\"external_execution_material_is_not_public_feed_or_stage_content\"}\n");
        return 1;
      }
      printf("<section class=\"bf-surface-fragment bf-material-boundary\" data-grammar=\""); esc(grammar); printf("\" data-presentation=\""); esc(klass); printf("\"><p>External execution material is available only to a World or Pack composition. It is not public feed or Stage content.</p></section>");
      return 1;
    }
    if (sqlite3_prepare_v2(db,"SELECT json_extract(cell_json,'$.question'),json_extract(cell_json,'$.experience_character'),json_extract(cell_json,'$.empty_character'),json_extract(cell_json,'$.refusal_character') FROM presentation_cell WHERE grammar=?1 AND presentation_class=?2",-1,&cell,NULL)!=SQLITE_OK) return 0;
    sqlite3_bind_text(cell,1,grammar,-1,SQLITE_TRANSIENT); sqlite3_bind_text(cell,2,klass,-1,SQLITE_TRANSIENT);
    if (sqlite3_step(cell)!=SQLITE_ROW) { sqlite3_finalize(cell); return 0; }
    question=(const char*)sqlite3_column_text(cell,0); character=(const char*)sqlite3_column_text(cell,1); empty=(const char*)sqlite3_column_text(cell,2); refusal=(const char*)sqlite3_column_text(cell,3);
    if (as_json) {
      printf("{\"schema\":\"bonfyre-surface-fragment.v1\",");
      json_field("grammar", grammar); printf(",");
      json_field("presentation_class", klass); printf(",");
      json_field("question", question); printf(",");
      json_field("character", character);
      printf(",\"records\":[");
    } else {
      printf("<section class=\"bf-surface-fragment bf-material-stream bf-"); esc(grammar); printf("\" data-grammar=\""); esc(grammar); printf("\" data-presentation=\"");esc(klass);printf("\"><header class=\"bf-stream-head\"><p class=\"bf-stream-kicker\">compiled public material</p><h1>");esc(question);printf("</h1><p>");esc(character);printf("</p></header><div class=\"bf-material-list\">");
    }
    if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK) { fprintf(stderr,"fragment query: %s\n",sqlite3_errmsg(db)); sqlite3_finalize(cell); return 0; }
    sqlite3_bind_text(st,1,grammar,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,2,klass,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,3,query?query:"",-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,4,domain?domain:"",-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,5,world?world:"",-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,6,affordance?affordance:"",-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,7,chain?chain:"",-1,SQLITE_TRANSIENT); sqlite3_bind_int(st,8,limit);
    while(sqlite3_step(st)==SQLITE_ROW) {
      const char *id=(const char*)sqlite3_column_text(st,0),*name=(const char*)sqlite3_column_text(st,1),*kind=(const char*)sqlite3_column_text(st,2),*ref=(const char*)sqlite3_column_text(st,3),*section=(const char*)sqlite3_column_text(st,4),*url=(const char*)sqlite3_column_text(st,5),*stage=(const char*)sqlite3_column_text(st,6);
      if (as_json) {
        printf("%s{", count ? "," : "");
        json_field("occurrence_id", id); printf(",");
        json_field("name", name); printf(",");
        json_field("source_kind", kind); printf(",");
        json_field("source_ref", ref); printf(",");
        json_field("summary", section); printf(",");
        json_field("url", url);
        if (!strcmp(grammar,"stage")) { printf(","); json_field("program_window", stage && *stage ? stage : "entry"); }
        terms_json(db,id,"chain","human_paths");
        terms_json(db,id,"protocol","protocol_programs");
        terms_json(db,id,"home","composes_into");
        terms_json(db,id,"role","reagent_roles");
        printf(",\"truth\":\"observed_candidate\"}");
        count++;
        continue;
      }
      printf("<article class=\"bf-material-card\"><div class=\"bf-material-topline\"><span class=\"bf-material-kind\">"); esc(kind); printf("</span><span class=\"bf-material-truth\">observed candidate</span></div><h2>");
      if(url&&*url) { printf("<a href=\"");esc(url);printf("\" rel=\"noreferrer\" target=\"_blank\">"); } esc(name); if(url&&*url)printf("</a>"); printf("</h2><p class=\"bf-material-summary\">");esc(section);printf("</p>");
      terms_html(db,id,"chain","Human paths","bf-material-paths"); terms_html(db,id,"protocol","Protocol program","bf-material-protocol");
      if (!strcmp(grammar,"stage")) { printf("<p class=\"bf-material-stage\"><strong>Current program window</strong> "); esc(stage&&*stage?stage:"entry"); printf("</p>"); }
      terms_html(db,id,"home","Composes into","bf-material-homes"); terms_html(db,id,"role","Reagent role","bf-material-roles");
      printf("<footer><span>");esc(ref);printf("</span>"); if(url&&*url){printf("<a class=\"surface-act\" href=\"/_surface/intake?occurrence=");query_esc(id);printf("&url=");query_esc(url);printf("\">Prepare route</a>");} printf("<a href=\""); if(!strcmp(grammar,"stage"))printf("/discover?chain="); else printf("/stage?chain=");
      { sqlite3_stmt *first=NULL; const char *first_chain="discover"; if(sqlite3_prepare_v2(db,"SELECT term FROM occurrence_term WHERE occurrence_id=?1 AND axis='chain' ORDER BY term LIMIT 1",-1,&first,NULL)==SQLITE_OK){sqlite3_bind_text(first,1,id,-1,SQLITE_TRANSIENT);if(sqlite3_step(first)==SQLITE_ROW)first_chain=(const char*)sqlite3_column_text(first,0);esc(first_chain);sqlite3_finalize(first);} }
      printf("\">See the adjacent program</a></footer></article>"); count++;
    }
    sqlite3_finalize(st);
    if (as_json) {
      printf("],\"count\":%d,", count);
      json_field("empty_character", empty); printf(",");
      json_field("boundary", refusal);
      printf(",\"cannot_infer\":[\"candidate material -> an admitted provider, evidence, authority, payment, settlement, or effect\"]}\n");
      sqlite3_finalize(cell);
      return 1;
    }
    if(!count) { printf("<article class=\"bf-material-empty\"><p>");esc(empty);printf("</p></article>"); }
    printf("</div><footer class=\"bf-stream-boundary\">");esc(refusal);printf(" Candidate material is not an admitted provider, evidence, authority, payment, settlement, or effect.</footer></section>");
    sqlite3_finalize(cell);
    return 1;
}
/* ------------------------------------------------ owner participation -- */

/* What this plane can say it did for each occurrence.
 *
 * The participation report declares seven axes and twenty-six rungs, and three
 * of those axes -- capability, economic, round_trip -- had no evidence schema
 * at all, so no owner could report into them and they read as zero. They were
 * not zero because nothing had happened. They were zero because the address
 * plane, which indexes every occurrence and decides what can be reached, had no
 * way to say so.
 *
 * Three rungs are earned here and they are all the lowest rung of their axis:
 *
 *   discovery/routable    the plane holds a reachable address and at least one
 *                         term a caller could name to arrive at it
 *   capability/qualified  the occurrence carries a role that makes it a
 *                         candidate for capability work at all
 *   round_trip/projected  a round trip can be described for it
 *
 * Every higher rung is deliberately not emitted. `eligible` needs a lowerer
 * that survives an exact cut, `authorized` needs an authority, `activated`
 * needs an effect, `observed` needs the network, and nothing on the economic
 * axis is emitted at all because a quote without a price from a real carrier
 * would be a number this plane invented.
 *
 * Cannot infer: routable -> reachable. This says the plane can form an address
 * and name a need that arrives at it, not that the host answers, permits the
 * request, or exists today. */
static int occurrence_has_role(sqlite3 *db, const char *id, const char *const *roles, int n) {
    sqlite3_stmt *st = NULL;
    int found = 0;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM occurrence_term WHERE occurrence_id=?1 AND axis='role' AND term=?2 LIMIT 1", -1, &st, NULL) != SQLITE_OK)
        return 0;
    for (int i = 0; i < n && !found; i++) {
        sqlite3_reset(st);
        sqlite3_bind_text(st, 1, id, -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 2, roles[i], -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) found = 1;
    }
    sqlite3_finalize(st);
    return found;
}

static void participation_row(FILE *fh, const char *id, const char *axis, const char *state,
                              const char *basis, const char *evidence, const char *stamp,
                              const char *cannot) {
    fprintf(fh,
        "{\"schema\":\"bonfyre-owner-participation.v1\",\"occurrence_id\":\"%s\""
        ",\"axis\":\"%s\",\"state\":\"%s\",\"owner\":\"AddressPlane\""
        ",\"from_owner\":\"ExternalEstate\",\"to_owner\":\"AddressPlane\""
        ",\"basis_sha256\":\"%s\",\"evidence_sha256\":\"%s\""
        ",\"produced_by\":\"bonfyre-external-estate\",\"recorded_at\":\"%s\""
        ",\"cannot_infer\":[\"%s\"]}\n",
        id, axis, state, basis, evidence, stamp, cannot);
}

static int emit_participation(sqlite3 *db, const char *out_path) {
    static const char *const CAPABILITY_ROLES[] = {
        "action_adapter", "authoring_adapter", "compiler_toolchain",
        "external_service_candidate", "verification_tool", "media_service" };
    static const char *const ROUNDTRIP_ROLES[] = {
        "action_adapter", "authoring_adapter", "publication_carrier", "verification_tool" };
    FILE *fh = out_path ? fopen(out_path, "w") : stdout;
    sqlite3_stmt *st = NULL;
    long long routable = 0, qualified = 0, projected = 0, occurrences = 0;
    char stamp[32];
    time_t now = time(NULL);
    struct tm tm;

    if (!fh) { fprintf(stderr, "cannot write %s\n", out_path); return 0; }
    gmtime_r(&now, &tm);
    strftime(stamp, sizeof(stamp), "%Y-%m-%dT%H:%M:%SZ", &tm);

    if (sqlite3_prepare_v2(db,
            "SELECT o.id, o.primary_url, o.source_kind,"
            " (SELECT count(*) FROM occurrence_term t WHERE t.occurrence_id=o.id AND t.axis IN ('domain','protocol')),"
            " (SELECT group_concat(t.axis||':'||t.term) FROM occurrence_term t WHERE t.occurrence_id=o.id)"
            " FROM occurrence o ORDER BY o.id", -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "participation query failed: %s\n", sqlite3_errmsg(db));
        if (out_path) fclose(fh);
        return 0;
    }
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(st, 0);
        const char *url = (const char *)sqlite3_column_text(st, 1);
        const char *kind = (const char *)sqlite3_column_text(st, 2);
        int named = sqlite3_column_int(st, 3);
        const char *terms = (const char *)sqlite3_column_text(st, 4);
        char basis[65], evidence[65], scratch[4096];
        int n;
        if (!id) continue;
        occurrences++;
        /* What the plane read about this occurrence: its address and the terms
         * a caller could name. Digesting the terms rather than a row id means a
         * re-indexed estate produces the same digest for the same knowledge. */
        n = snprintf(scratch, sizeof(scratch), "%s\x1f%s\x1f%s\x1f%s",
                     id, url ? url : "", kind ? kind : "", terms ? terms : "");
        if (n < 0) n = 0;
        if ((size_t)n > sizeof(scratch) - 1) n = (int)sizeof(scratch) - 1;
        bf_sha256_hex((const uint8_t *)scratch, (size_t)n, basis);

        if (url && *url && named > 0) {
            n = snprintf(scratch, sizeof(scratch), "%s\x1froutable\x1f%d", id, named);
            if (n < 0) n = 0;
            bf_sha256_hex((const uint8_t *)scratch, (size_t)n, evidence);
            participation_row(fh, id, "discovery", "routable", basis, evidence, stamp,
                "routable -> reachable; the plane can form the address, the host still decides");
            routable++;
        }
        if (occurrence_has_role(db, id, CAPABILITY_ROLES, 6)) {
            n = snprintf(scratch, sizeof(scratch), "%s\x1fqualified", id);
            if (n < 0) n = 0;
            bf_sha256_hex((const uint8_t *)scratch, (size_t)n, evidence);
            participation_row(fh, id, "capability", "qualified", basis, evidence, stamp,
                "qualified -> eligible, authorized or activated; those need a lowerer, an authority and an effect");
            qualified++;
        }
        if (occurrence_has_role(db, id, ROUNDTRIP_ROLES, 4)) {
            n = snprintf(scratch, sizeof(scratch), "%s\x1fprojected", id);
            if (n < 0) n = 0;
            bf_sha256_hex((const uint8_t *)scratch, (size_t)n, evidence);
            participation_row(fh, id, "round_trip", "projected", basis, evidence, stamp,
                "projected -> observed; a projection is what would happen, not what did");
            projected++;
        }
    }
    sqlite3_finalize(st);
    if (out_path) fclose(fh);
    fprintf(stderr,
        "{\"schema\":\"bonfyre-address-plane-participation.v1\",\"occurrences\":%lld"
        ",\"discovery_routable\":%lld,\"capability_qualified\":%lld,\"round_trip_projected\":%lld"
        ",\"economic\":0,\"truth\":\"lowest_rung_of_each_axis_earned_from_the_index\""
        ",\"cannot_infer\":[\"a rung emitted here -> any rung above it\""
        ",\"no economic row -> no economic participation; a quote needs a price from a carrier and this plane has none\"]}\n",
        occurrences, routable, qualified, projected);
    return 1;
}

/* ---------------------------------------------------------- cost routing -- */

/* Which admissible candidate costs least, and what routing to it avoids.
 *
 * The estate holds 211 metered x402 sources and 2,291 sources it has proven it
 * can read for nothing under a published permission. Many of both serve the
 * same need: 137 metered sources answer affordance:search and 248 free and
 * proven ones answer it too. A router that does not know this pays for an
 * answer it could have had for free, and the difference is not a saving anyone
 * negotiated -- it is a price that was never incurred.
 *
 * Free here is a strong word and is earned: the occurrence has a permitted
 * read, the fetch returned 200, and the retained bytes still hash to what the
 * receipt recorded. A source nobody has read is not free, it is unknown, and it
 * is reported that way.
 *
 * Cannot infer: cheapest -> best, or even adequate. This orders admissible
 * candidates by what they cost. Whether a free source answers the question as
 * well as a metered one is a judgement no price contains, and the exact
 * verifier still has to admit whichever one is tried. */
struct CostRow { char id[64]; double price; int have_price; int free_proven; };

static int cost_load_jsonl(const char *path, const char *key, struct CostRow **rows,
                           size_t *count, size_t *cap, int price_field) {
    FILE *fh = path ? fopen(path, "rb") : NULL;
    char *line = NULL;
    size_t linecap = 0;
    ssize_t len;
    if (!fh) return 0;
    while ((len = getline(&line, &linecap, fh)) > 0) {
        char id[64], value[64], state[64];
        if (!bf_lex_json_string(line, "occurrence_id", id, sizeof(id))) continue;
        /* Read the field, do not grep for it. A substring match for
         * "state":"quoted" misses "state": "quoted" and silently returned an
         * empty price book, which read as a frontier with nothing metered on
         * it rather than as a filter that never matched. */
        if (key) {
            if (!bf_lex_json_string(line, "state", state, sizeof(state))) continue;
            if (strcmp(state, key)) continue;
        }
        if (*count == *cap) {
            size_t next = *cap ? *cap * 2 : 256;
            struct CostRow *grown = realloc(*rows, next * sizeof(**rows));
            if (!grown) break;
            *rows = grown;
            *cap = next;
        }
        memset(&(*rows)[*count], 0, sizeof((*rows)[0]));
        snprintf((*rows)[*count].id, sizeof((*rows)[0].id), "%s", id);
        if (price_field && bf_lex_json_scalar(line, "min_price_usd", value, sizeof(value))) {
            (*rows)[*count].price = atof(value);
            (*rows)[*count].have_price = 1;
        } else if (!price_field) {
            (*rows)[*count].free_proven = 1;
        }
        (*count)++;
    }
    free(line);
    fclose(fh);
    return 1;
}

static const struct CostRow *cost_find(const struct CostRow *rows, size_t n, const char *id) {
    for (size_t i = 0; i < n; i++) if (!strcmp(rows[i].id, id)) return &rows[i];
    return NULL;
}

static int route_cost(sqlite3 *db, int argc, char **argv) {
    const char *book = arg(argc, argv, "--price-book");
    const char *proof = arg(argc, argv, "--free-proof");
    struct CostRow *prices = NULL, *frees = NULL;
    size_t nprice = 0, cprice = 0, nfree = 0, cfree = 0;
    sqlite3_stmt *st = NULL;
    char sql[4096];
    int needs = 0, first = 1;
    long long frontier = 0, free_count = 0, metered = 0, unknown = 0;
    double cheapest_metered = -1, dearest_metered = -1, metered_total = 0;
    double *metered_prices = NULL;
    size_t metered_cap = 0;
    char cheapest_free[64] = {0};

    cost_load_jsonl(book, "quoted", &prices, &nprice, &cprice, 1);
    cost_load_jsonl(proof, "obligations_closed", &frees, &nfree, &cfree, 0);

    /* The needs are ANDed: a candidate must carry every coordinate asked for,
     * which is the same deterministic cut forwarding uses. */
    snprintf(sql, sizeof(sql), "SELECT o.id, o.observed_name, o.source_kind FROM occurrence o");
    for (int i = 2; i + 1 < argc; i++) {
        if (strcmp(argv[i], "--need")) continue;
        {
            char clause[256];
            const char *spec = argv[i + 1];
            const char *colon = strchr(spec, ':');
            if (!colon) continue;
            snprintf(clause, sizeof(clause),
                     " JOIN occurrence_term t%d ON t%d.occurrence_id=o.id AND t%d.axis='%.*s' AND t%d.term='%s'",
                     needs, needs, needs, (int)(colon - spec), spec, needs, colon + 1);
            if (strlen(sql) + strlen(clause) + 1 >= sizeof(sql)) break;
            strcat(sql, clause);
            needs++;
        }
    }
    if (!needs) { fprintf(stderr, "route-cost requires at least one --need\n"); free(prices); free(frees); return 0; }
    strcat(sql, " ORDER BY o.id");

    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "route query failed: %s\n", sqlite3_errmsg(db));
        free(prices); free(frees);
        return 0;
    }
    fputs("{\"schema\":\"bonfyre-cost-route.v1\",\"candidates\":[", stdout);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(st, 0);
        const char *name = (const char *)sqlite3_column_text(st, 1);
        const char *kind = (const char *)sqlite3_column_text(st, 2);
        const struct CostRow *p = cost_find(prices, nprice, id);
        const struct CostRow *f = cost_find(frees, nfree, id);
        frontier++;
        /* A published price wins over a free read of the same origin. The read
         * that was enacted was of the service describing itself, which it
         * serves for nothing; the endpoint it describes is the metered thing.
         * Treating the landing page as evidence that the service is free is the
         * mistake this ordering exists to prevent -- it made every priced
         * source disappear from the metered side of the route. */
        if (p && p->have_price) {
            metered++;
            if (cheapest_metered < 0 || p->price < cheapest_metered) cheapest_metered = p->price;
            if (p->price > dearest_metered) dearest_metered = p->price;
            metered_total += p->price;
            /* metered has already been incremented, so the slot wanted is
             * metered-1 and the capacity must exceed it. Growing on equality
             * against a capacity that starts at zero never grows at all, and
             * the median silently stayed zero while the mean looked fine. */
            if ((size_t)metered > metered_cap) {
                size_t next = metered_cap ? metered_cap * 2 : 64;
                double *grown = realloc(metered_prices, next * sizeof(*grown));
                if (grown) { metered_prices = grown; metered_cap = next; }
            }
            if ((size_t)metered <= metered_cap) metered_prices[metered - 1] = p->price;
        } else if (f) {
            free_count++;
            if (!cheapest_free[0]) snprintf(cheapest_free, sizeof(cheapest_free), "%s", id);
        } else {
            unknown++;
            continue;   /* neither proven free nor priced: not routable on cost */
        }
        if (!first) fputc(',', stdout);
        first = 0;
        fputs("{\"identity\":", stdout); json_text(id);
        fputs(",\"name\":", stdout); json_text(name ? name : "");
        fputs(",\"source_kind\":", stdout); json_text(kind ? kind : "");
        printf(",\"cost_usd\":%.6f,\"basis\":\"%s\"}",
               (p && p->have_price) ? p->price : 0.0,
               (p && p->have_price) ? "published_price" : "proven_permitted_read");
    }
    sqlite3_finalize(st);
    printf("],\"frontier\":%lld,\"free_and_proven\":%lld,\"metered\":%lld,\"unpriced_and_unread\":%lld",
           frontier, free_count, metered, unknown);
    /* What routing avoids depends on what the alternative would have been. A
     * router that does not know the prices does not pick the cheapest metered
     * source; it picks one. So the expected avoidance is the mean of the
     * metered options, the floor is the cheapest and the ceiling is the
     * dearest, and reporting only the floor understates it by the width of the
     * span. None of the three is revenue. */
    if (free_count && metered) {
        /* A mean over a thin market is not a market price. Two sellers where
         * one asks 149 dollars and the other asks 40 cents average to 74, and
         * no agent would ever be quoted 74. The median is reported beside the
         * mean and the market is marked thin below four sellers, so a caller
         * can see that the number rests on almost nothing rather than having to
         * infer it from a count printed elsewhere. */
        double median = 0;
        if (metered_prices && metered > 0) {
            for (int i = 1; i < metered; i++) {
                double key = metered_prices[i];
                int j = i - 1;
                while (j >= 0 && metered_prices[j] > key) { metered_prices[j + 1] = metered_prices[j]; j--; }
                metered_prices[j + 1] = key;
            }
            median = (metered % 2)
                   ? metered_prices[metered / 2]
                   : (metered_prices[metered / 2 - 1] + metered_prices[metered / 2]) / 2.0;
        }
        printf(",\"avoided_cost_usd\":{\"floor\":%.6f,\"median\":%.6f,\"mean\":%.6f,\"ceiling\":%.6f}"
               ",\"thin_market\":%s,\"route\":\"%s\"",
               cheapest_metered, median, metered_total / (double)metered, dearest_metered,
               metered < 4 ? "true" : "false", cheapest_free);
    }
    else
        fputs(",\"avoided_cost_usd\":{\"floor\":0,\"median\":0,\"mean\":0,\"ceiling\":0}"
              ",\"thin_market\":true", stdout);
    if (metered) printf(",\"metered_span_usd\":[%.6f,%.6f]", cheapest_metered, dearest_metered);
    fputs(",\"truth\":\"admissible_candidates_ordered_by_what_they_cost\""
          ",\"cannot_infer\":[\"cheapest -> best or adequate\""
          ",\"free_and_proven -> an answer to this need; the exact verifier still decides\""
          ",\"avoided_cost -> revenue; it is a price that was never incurred\"]}\n", stdout);
    free(metered_prices);
    free(prices);
    free(frees);
    return 1;
}

static void usage(void) { fputs("usage: bonfyre-external-estate import --db FILE --occurrences JSONL --cells JSON [--bindings JSONL --stages JSONL --chains JSONL]\n       bonfyre-external-estate import-advertisements --db FILE --advertisements JSONL [--replace-owner OWNER|--replace-owner-prefix PREFIX]\n       bonfyre-external-estate compile-advertisements --db INDEX [--cms-db FILE --target-db FILE --provider-db FILE --model-db FILE --media-library-db FILE --creator-channel-db FILE --interaction-grammar-db FILE --protocols JSONL --mcp-closure JSON]\n       bonfyre-external-estate address-worker --db INDEX [owner sources] [--interaction-grammar-db FILE --delta-dir DIR --lowerer-dir DIR] [--physical-db FILE --physical-compiler PATH] [--admission-db FILE --admission-volume VOLUME --admission-plane PLANE --admission-free-bytes N --admission-protected-floor-bytes N --admission-per-plane-quota-bytes N --admission-max-grant-bytes N]\n       bonfyre-external-estate emit-cms --cms-db FILE\n       bonfyre-external-estate emit-targets --target-db FILE\n       bonfyre-external-estate emit-providers --provider-db FILE\n       bonfyre-external-estate emit-compute --model-db FILE\n       bonfyre-external-estate emit-media-library --media-library-db FILE\n       bonfyre-external-estate emit-creator-channel --creator-channel-db FILE\n       bonfyre-external-estate emit-interaction-grammar --interaction-grammar-db FILE\n       bonfyre-external-estate emit-protocols --protocols JSONL\n       bonfyre-external-estate emit-mcp --mcp-closure JSON\n       bonfyre-external-estate emit-representation-passports --db FILE\n       bonfyre-external-estate address --db FILE --need axis:term [--need axis:term] [--forbid axis:term] [--limit N]\n       bonfyre-external-estate forward --db FILE --kind KIND --need axis:term [--need axis:term] [--forbid axis:term] [--limit N]\n       bonfyre-external-estate mcp-aperture --db FILE --actor-closure DIGEST --address-closure DIGEST --target-frontier DIGEST --policy-generation DIGEST [--need axis:term] [--forbid axis:term] [--limit N]\n       bonfyre-external-estate affected --db FILE --write FACT_ADDRESS\n       bonfyre-external-estate fact-delta --db FILE --delta JSON\n       bonfyre-external-estate drain-fact-deltas --db FILE --dir DIRECTORY [--limit N]\n       bonfyre-external-estate drain-lowering-queue --db FILE --dir DIRECTORY [--limit N]\n       bonfyre-external-estate fragment --db FILE --grammar G --class C [--q Q --domain D --world W --affordance A --chain C --limit N --as html|json]\n       bonfyre-external-estate route --db FILE --occurrence ID --url URL\n",stderr); }
#define EXTERNAL_COMMANDS(X) \
    X("address", EC_ADDRESS, EC_WRITE, "--db") \
    X("address-worker", EC_ADDRESS_WORKER, EC_DIRECT, "--db") \
    X("affected", EC_AFFECTED, EC_WRITE, "--db") \
    X("compile-advertisements", EC_COMPILE_ADVERTISEMENTS, EC_DIRECT, "--db") \
    X("drain-fact-deltas", EC_DRAIN_FACT_DELTAS, EC_WRITE, "--db") \
    X("drain-lowering-queue", EC_DRAIN_LOWERING_QUEUE, EC_WRITE, "--db") \
    X("emit-cms", EC_EMIT_CMS, EC_READ, "--cms-db") \
    X("emit-compute", EC_EMIT_COMPUTE, EC_READ, "--model-db") \
    X("emit-creator-channel", EC_EMIT_CREATOR_CHANNEL, EC_READ, "--creator-channel-db") \
    X("emit-interaction-grammar", EC_EMIT_INTERACTION_GRAMMAR, EC_READ, "--interaction-grammar-db") \
    X("emit-mcp", EC_EMIT_MCP, EC_DIRECT, NULL) \
    X("emit-media-library", EC_EMIT_MEDIA_LIBRARY, EC_READ, "--media-library-db") \
    X("emit-policy", EC_EMIT_POLICY, EC_DIRECT, NULL) \
    X("emit-protocols", EC_EMIT_PROTOCOLS, EC_DIRECT, NULL) \
    X("emit-providers", EC_EMIT_PROVIDERS, EC_READ, "--provider-db") \
    X("emit-representation-passports", EC_EMIT_REPRESENTATION_PASSPORTS, EC_WRITE, "--db") \
    X("emit-targets", EC_EMIT_TARGETS, EC_READ, "--target-db") \
    X("fact-delta", EC_FACT_DELTA, EC_WRITE, "--db") \
    X("forward", EC_FORWARD, EC_READ, "--db") \
    X("fragment", EC_FRAGMENT, EC_WRITE, "--db") \
    X("import", EC_IMPORT, EC_WRITE, "--db") \
    X("import-advertisements", EC_IMPORT_ADVERTISEMENTS, EC_WRITE, "--db") \
    X("kinds", EC_KINDS, EC_DIRECT, NULL) \
    X("mcp-aperture", EC_MCP_APERTURE, EC_WRITE, "--db") \
    X("participation", EC_PARTICIPATION, EC_READ, "--db") \
    X("refresh-mcp-closure", EC_REFRESH_MCP_CLOSURE, EC_DIRECT, NULL) \
    X("route", EC_ROUTE, EC_WRITE, "--db") \
    X("route-cost", EC_ROUTE_COST, EC_READ, "--db")

enum { EC_DIRECT, EC_READ, EC_WRITE };
enum {
#define EC_ENUM(name, id, mode, path) id,
    EXTERNAL_COMMANDS(EC_ENUM)
#undef EC_ENUM
};
static const BfCommandEntry external_commands[] = {
#define EC_ENTRY(name, id, mode, path) {name, id, mode, path},
    EXTERNAL_COMMANDS(EC_ENTRY)
#undef EC_ENTRY
};

static int external_direct(int id, int argc, char **argv, const char *dbpath) {
    const char *limit = arg(argc, argv, "--limit");
    switch (id) {
    case EC_REFRESH_MCP_CLOSURE:
        return refresh_mcp_closure(arg(argc, argv, "--mcp-url"),
            arg(argc, argv, "--mcp-token-file"), arg(argc, argv, "--out")) ? 0 : 1;
    case EC_COMPILE_ADVERTISEMENTS:
        return compile_owner_sources(dbpath, arg(argc, argv, "--cms-db"),
            arg(argc, argv, "--target-db"), arg(argc, argv, "--provider-db"),
            arg(argc, argv, "--model-db"), arg(argc, argv, "--protocols"),
            arg(argc, argv, "--mcp-closure"), arg(argc, argv, "--mcp-url"),
            arg(argc, argv, "--mcp-token-file"), arg(argc, argv, "--policy-json"),
            arg(argc, argv, "--media-library-db"), arg(argc, argv, "--creator-channel-db"),
            arg(argc, argv, "--interaction-grammar-db")) ? 0 : 1;
    case EC_ADDRESS_WORKER:
        return run_address_worker(dbpath, arg(argc, argv, "--cms-db"),
            arg(argc, argv, "--target-db"), arg(argc, argv, "--provider-db"),
            arg(argc, argv, "--model-db"), arg(argc, argv, "--protocols"),
            arg(argc, argv, "--mcp-closure"), arg(argc, argv, "--mcp-url"),
            arg(argc, argv, "--mcp-token-file"), arg(argc, argv, "--policy-json"),
            arg(argc, argv, "--media-library-db"), arg(argc, argv, "--creator-channel-db"),
            arg(argc, argv, "--interaction-grammar-db"), arg(argc, argv, "--delta-dir"),
            arg(argc, argv, "--lowerer-dir"), arg(argc, argv, "--physical-db"),
            arg(argc, argv, "--physical-compiler"), arg(argc, argv, "--admission-db"),
            arg(argc, argv, "--admission-volume"), arg(argc, argv, "--admission-plane"),
            arg(argc, argv, "--admission-free-bytes"),
            arg(argc, argv, "--admission-protected-floor-bytes"),
            arg(argc, argv, "--admission-per-plane-quota-bytes"),
            arg(argc, argv, "--admission-max-grant-bytes"),
            arg(argc, argv, "--owner-contract-image"), limit ? atoi(limit) : 64) ? 0 : 1;
    case EC_KINDS: print_vocabulary(); return 0;
    case EC_EMIT_PROTOCOLS: return emit_protocols(arg(argc, argv, "--protocols")) < 0 ? 1 : 0;
    case EC_EMIT_MCP: return emit_mcp(arg(argc, argv, "--mcp-closure")) < 0 ? 1 : 0;
    case EC_EMIT_POLICY: return emit_policy(arg(argc, argv, "--policy-json")) < 0 ? 1 : 0;
    default: return 2;
    }
}

static int external_read(int id, sqlite3 *db, int argc, char **argv) {
    switch (id) {
    case EC_FORWARD: return forward_advertisements(db, argc, argv) ? 0 : 1;
    case EC_ROUTE_COST: return route_cost(db, argc, argv) ? 0 : 1;
    case EC_PARTICIPATION: return emit_participation(db, arg(argc, argv, "--out")) ? 0 : 1;
    case EC_EMIT_CMS: return emit_cms(db) < 0 ? 1 : 0;
    case EC_EMIT_TARGETS: return emit_targets(db) < 0 ? 1 : 0;
    case EC_EMIT_PROVIDERS: return emit_providers(db) < 0 ? 1 : 0;
    case EC_EMIT_COMPUTE: return emit_compute(db) < 0 ? 1 : 0;
    case EC_EMIT_MEDIA_LIBRARY: return emit_media_library(db) < 0 ? 1 : 0;
    case EC_EMIT_CREATOR_CHANNEL: return emit_creator_channel(db) < 0 ? 1 : 0;
    case EC_EMIT_INTERACTION_GRAMMAR: return emit_interaction_grammar(db) < 0 ? 1 : 0;
    default: return 2;
    }
}

static int external_write(int id, sqlite3 *db, int argc, char **argv) {
    const char *limit_arg = arg(argc, argv, "--limit");
    int limit = limit_arg ? atoi(limit_arg) : 64;
    switch (id) {
    case EC_IMPORT:
        return arg(argc, argv, "--occurrences") && arg(argc, argv, "--cells") &&
            import_occurrences(db, arg(argc, argv, "--occurrences")) &&
            import_cells(db, arg(argc, argv, "--cells")) &&
            import_experience(db, arg(argc, argv, "--bindings"),
                arg(argc, argv, "--stages"), arg(argc, argv, "--chains"));
    case EC_IMPORT_ADVERTISEMENTS:
        return import_advertisements(db, arg(argc, argv, "--advertisements"),
            arg(argc, argv, "--replace-owner"), arg(argc, argv, "--replace-owner-prefix"));
    case EC_EMIT_REPRESENTATION_PASSPORTS: return emit_representation_passports(db) >= 0;
    case EC_ADDRESS: return address_candidates(db, argc, argv);
    case EC_MCP_APERTURE: return mcp_aperture(db, argc, argv);
    case EC_AFFECTED: return affected_advertisements(db, argc, argv);
    case EC_FACT_DELTA: return consume_fact_delta(db, arg(argc, argv, "--delta"));
    case EC_DRAIN_FACT_DELTAS: return drain_fact_deltas(db, arg(argc, argv, "--dir"), limit);
    case EC_DRAIN_LOWERING_QUEUE: return drain_lowering_queue(db, arg(argc, argv, "--dir"), limit);
    case EC_FRAGMENT: {
        const char *as = arg(argc, argv, "--as");
        int fragment_limit = limit_arg ? atoi(limit_arg) : 8;
        if (fragment_limit < 1) fragment_limit = 1;
        if (fragment_limit > 48) fragment_limit = 48;
        return fragment(db, arg(argc, argv, "--grammar"), arg(argc, argv, "--class"),
            arg(argc, argv, "--q"), arg(argc, argv, "--domain"),
            arg(argc, argv, "--world"), arg(argc, argv, "--affordance"),
            arg(argc, argv, "--chain"), fragment_limit, as && !strcmp(as, "json"));
    }
    case EC_ROUTE: return route(db, arg(argc, argv, "--occurrence"), arg(argc, argv, "--url"));
    default: return 0;
    }
}

int main(int argc, char **argv) {
    const BfCommandEntry *command;
    const char *path;
    sqlite3 *db = NULL;
    int result;
    if (argc < 2 || !(command = bf_command_lookup(external_commands,
            sizeof(external_commands) / sizeof(external_commands[0]), argv[1]))) {
        usage();
        return 2;
    }
    path = command->path_option ? arg(argc, argv, command->path_option) : NULL;
    if (command->path_option && !path) {
        if (command->mode == EC_READ && command->id != EC_ROUTE_COST && command->id != EC_PARTICIPATION)
            fputs("open owner database failed\n", stderr);
        else usage();
        return 2;
    }
    if (command->mode == EC_DIRECT) return external_direct(command->id, argc, argv, path);
    if (command->mode == EC_READ) {
        if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
            fputs(command->id == EC_ROUTE_COST || command->id == EC_PARTICIPATION
                ? "open index failed\n" : "open owner database failed\n", stderr);
            if (db) sqlite3_close(db);
            return 2;
        }
        result = external_read(command->id, db, argc, argv);
        sqlite3_close(db);
        return result;
    }
    if (sqlite3_open(path, &db) != SQLITE_OK) {
        fprintf(stderr, "open: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 2;
    }
    if (!init_schema(db)) { sqlite3_close(db); return 1; }
    result = external_write(command->id, db, argc, argv);
    sqlite3_close(db);
    return result ? 0 : 1;
}
static size_t collect_json_terms(sqlite3 *db,const char *raw,const char *path,const char *axis,
                                 char storage[][256],const char **axes,const char **vals,
                                 size_t count,size_t cap){
    sqlite3_stmt *st=NULL;
    if(!db||!raw||!axis||count>=cap||sqlite3_prepare_v2(db,
      "SELECT value FROM json_each(CASE WHEN json_valid(?1) THEN ?1 ELSE '[]' END,?2) WHERE type='text' ORDER BY key",
      -1,&st,NULL)!=SQLITE_OK)return count;
    sqlite3_bind_text(st,1,raw,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,path?path:"$",-1,SQLITE_TRANSIENT);
    while(count<cap&&sqlite3_step(st)==SQLITE_ROW){const char*v=(const char*)sqlite3_column_text(st,0);if(!v||!*v)continue;snprintf(storage[count],sizeof(storage[count]),"%s",v);axes[count]=axis;vals[count]=storage[count];count++;}
    sqlite3_finalize(st);return count;
}

static size_t collect_json_dependencies(sqlite3 *db,const char *raw,const char *path,const char *prefix,
                                        char storage[][640],const char **deps,size_t count,size_t cap){
    sqlite3_stmt *st=NULL;
    if(!db||!raw||count>=cap||sqlite3_prepare_v2(db,
      "SELECT value FROM json_each(CASE WHEN json_valid(?1) THEN ?1 ELSE '[]' END,?2) WHERE type='text' ORDER BY key",
      -1,&st,NULL)!=SQLITE_OK)return count;
    sqlite3_bind_text(st,1,raw,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,path?path:"$",-1,SQLITE_TRANSIENT);
    while(count<cap&&sqlite3_step(st)==SQLITE_ROW){const char*v=(const char*)sqlite3_column_text(st,0);int duplicate=0;if(!v||!*v)continue;
      if(prefix&&*prefix){if(snprintf(storage[count],sizeof(storage[count]),"%s%s",prefix,v)>=(int)sizeof(storage[count]))continue;}
      else {if(strlen(v)>=sizeof(storage[count]))continue;snprintf(storage[count],sizeof(storage[count]),"%s",v);}
      for(size_t i=0;i<count;i++)if(!strcmp(deps[i],storage[count])){duplicate=1;break;}if(duplicate)continue;deps[count]=storage[count];count++;}
    sqlite3_finalize(st);return count;
}

typedef struct {char **items;size_t count,capacity;} SourceDependencies;
static void source_dependencies_free(SourceDependencies *refs){
    for(size_t i=0;i<refs->count;i++)free(refs->items[i]);free(refs->items);memset(refs,0,sizeof(*refs));
}
static int source_dependency_add(SourceDependencies *refs,const char *value){
    if(!value||!*value||strlen(value)>8192||refs->count>=32768)return 0;
    if(refs->count==refs->capacity){size_t n=refs->capacity?refs->capacity*2:32;
        char **grown=realloc(refs->items,n*sizeof(*grown));if(!grown)return 0;refs->items=grown;refs->capacity=n;}
    refs->items[refs->count]=strdup(value);if(!refs->items[refs->count])return 0;refs->count++;return 1;
}
static int source_dependency_compare(const void *a,const void *b){return strcmp(*(char *const*)a,*(char *const*)b);}
/* Dependency references are compiled once with the collection, not rediscovered
 * by its frontend. Do not silently discard everything after the old 32 slots. */
static int source_dependencies(sqlite3 *cms,const char *selector,const char *frontier,const char *refresh,SourceDependencies *refs){
    sqlite3_stmt *st=NULL;int step,ok=0;memset(refs,0,sizeof(*refs));
    if(!source_dependency_add(refs,frontier)||!source_dependency_add(refs,"cms:source_collection"))goto done;
    if(refresh&&strchr(refresh,':')&&!source_dependency_add(refs,refresh))goto done;
    if(!selector||!*selector)selector="{}";
    if(sqlite3_prepare_v2(cms,"SELECT json_type(?1,'$.source_refs'),json_type(?1,'$.resolved_dependency_refs'),json_extract(?1,'$.template_ref')",-1,&st,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(st,1,selector,-1,SQLITE_STATIC);if(sqlite3_step(st)!=SQLITE_ROW)goto done;
    for(int c=0;c<2;c++){const char *type=(const char*)sqlite3_column_text(st,c);if(type&&strcmp(type,"array"))goto done;}
    if(sqlite3_column_type(st,2)!=SQLITE_NULL){const char *ref=(const char*)sqlite3_column_text(st,2);char key[1024];
        if(sqlite3_column_type(st,2)!=SQLITE_TEXT||!ref||!*ref||snprintf(key,sizeof(key),"cms:program_template:%s",ref)>=(int)sizeof(key)||!source_dependency_add(refs,key))goto done;}
    sqlite3_finalize(st);st=NULL;
    if(sqlite3_prepare_v2(cms,"SELECT value,type FROM json_each(?1,'$.resolved_dependency_refs') ORDER BY key",-1,&st,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(st,1,selector,-1,SQLITE_STATIC);
    while((step=sqlite3_step(st))==SQLITE_ROW){const char *v=(const char*)sqlite3_column_text(st,0),*t=(const char*)sqlite3_column_text(st,1);
        if(!t||strcmp(t,"text")||!v||strlen(v)!=(size_t)sqlite3_column_bytes(st,0)||!source_dependency_add(refs,v))goto done;}
    if(step!=SQLITE_DONE)goto done;sqlite3_finalize(st);st=NULL;
    if(sqlite3_prepare_v2(cms,"SELECT value,type FROM json_each(?1,'$.source_refs') ORDER BY key",-1,&st,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(st,1,selector,-1,SQLITE_STATIC);
    while((step=sqlite3_step(st))==SQLITE_ROW){const char *v=(const char*)sqlite3_column_text(st,0),*t=(const char*)sqlite3_column_text(st,1);char ref[8193],topic[640];
        if(!t||strcmp(t,"text")||!v||!*v||strlen(v)!=(size_t)sqlite3_column_bytes(st,0))goto done;
        if(snprintf(ref,sizeof(ref),"cms:internet_source:%s",v)>=(int)sizeof(ref)||!source_dependency_add(refs,ref))goto done;
        if(snprintf(ref,sizeof(ref),"cms:publication_source:%s",v)>=(int)sizeof(ref)||!source_dependency_add(refs,ref))goto done;
        if(source_topic(cms,v,topic,sizeof(topic))){if(snprintf(ref,sizeof(ref),"ForeignTwin:%s",topic)>=(int)sizeof(ref)||!source_dependency_add(refs,ref))goto done;}
    }
    if(step!=SQLITE_DONE)goto done;
    qsort(refs->items,refs->count,sizeof(*refs->items),source_dependency_compare);size_t n=0;
    for(size_t i=0;i<refs->count;i++){if(n&&!strcmp(refs->items[n-1],refs->items[i]))free(refs->items[i]);else refs->items[n++]=refs->items[i];}refs->count=n;ok=1;
 done:sqlite3_finalize(st);if(!ok)source_dependencies_free(refs);return ok;
}

static int emit_source_collections(sqlite3 *cms){
    sqlite3_stmt *st=NULL;int emitted=0;if(!table_exists(cms,"source_collection"))return 0;
    char sql[640];
    snprintf(sql,sizeof(sql),"SELECT collection_id,%s,%s,%s,%s FROM source_collection WHERE status='published' ORDER BY collection_id",cms_col(cms,"source_collection","subject_address"),cms_col(cms,"source_collection","source_selector"),cms_col(cms,"source_collection","material_kind"),cms_col(cms,"source_collection","refresh_policy"));
    if(sqlite3_prepare_v2(cms,sql,-1,&st,NULL)!=SQLITE_OK)return -1;
    while(sqlite3_step(st)==SQLITE_ROW){
      const char*id=(const char*)sqlite3_column_text(st,0),*subject=(const char*)sqlite3_column_text(st,1),*selector=(const char*)sqlite3_column_text(st,2),*materials=(const char*)sqlite3_column_text(st,3),*refresh=(const char*)sqlite3_column_text(st,4);char identity[640],owner[640],frontier[640],publish[256],term_storage[32][256];const char*axes[32],*vals[32];size_t terms=0;SourceDependencies dependencies;
      if(!id||!*id)continue;snprintf(identity,sizeof(identity),"cms:source_collection:%s",id);snprintf(owner,sizeof(owner),"CMS:source_collection:%s",id);snprintf(frontier,sizeof(frontier),"cms:source_collection:%s",id);snprintf(publish,sizeof(publish),"cms:source_collection");axes[terms]="cms";vals[terms++]=id;if(subject&&*subject){axes[terms]="subject";vals[terms++]=subject;}
      terms=collect_json_terms(cms,selector,"$.domains","domain",term_storage,axes,vals,terms,32);terms=collect_json_terms(cms,selector,"$.source_refs","source",term_storage,axes,vals,terms,32);terms=collect_json_terms(cms,materials,"$","material",term_storage,axes,vals,terms,32);if(refresh&&*refresh&&terms<32){axes[terms]="refresh";vals[terms++]=refresh;}
      if(!source_dependencies(cms,selector,frontier,refresh,&dependencies)){sqlite3_finalize(st);fprintf(stderr,"source collection dependency projection failed\n");return -1;}
      emit_record(identity,"source_collection",owner,"CMS:publication","MaterialCompiler:source_collection",frontier,axes,vals,terms,(const char**)dependencies.items,dependencies.count);source_dependencies_free(&dependencies);emitted++;
    }
    sqlite3_finalize(st);return emitted;
}

static int emit_capability_programs(sqlite3 *cms){
    sqlite3_stmt *st=NULL;int emitted=0;if(!table_exists(cms,"capability_program"))return 0;
    char sql[1024];
    snprintf(sql,sizeof(sql),"SELECT program_id,%s,%s,%s,%s,%s,%s,%s,%s FROM capability_program WHERE status='published' ORDER BY program_id",cms_col(cms,"capability_program","application_kind"),cms_col(cms,"capability_program","action"),cms_col(cms,"capability_program","consumes"),cms_col(cms,"capability_program","produces"),cms_col(cms,"capability_program","carrier_candidates"),cms_col(cms,"capability_program","capability_requirements"),cms_col(cms,"capability_program","authority_requirement"),cms_col(cms,"capability_program","transition_conditions"));
    if(sqlite3_prepare_v2(cms,sql,-1,&st,NULL)!=SQLITE_OK)return -1;
    while(sqlite3_step(st)==SQLITE_ROW){
      const char*id=(const char*)sqlite3_column_text(st,0),*app=(const char*)sqlite3_column_text(st,1),*action=(const char*)sqlite3_column_text(st,2),*consumes=(const char*)sqlite3_column_text(st,3),*produces=(const char*)sqlite3_column_text(st,4),*carriers=(const char*)sqlite3_column_text(st,5),*requirements=(const char*)sqlite3_column_text(st,6),*authority=(const char*)sqlite3_column_text(st,7),*transitions=(const char*)sqlite3_column_text(st,8);char identity[640],owner[640],frontier[640],publish[256],term_storage[64][256],dep_storage[64][640];const char*axes[64],*vals[64],*deps[64];size_t terms=0,depc=0;
      if(!id||!*id)continue;snprintf(identity,sizeof(identity),"cms:capability_program:%s",id);snprintf(owner,sizeof(owner),"CMS:capability_program:%s",id);snprintf(frontier,sizeof(frontier),"cms:capability_program:%s",id);snprintf(publish,sizeof(publish),"cms:capability_program");axes[terms]="cms";vals[terms++]=id;if(app&&*app){axes[terms]="application";vals[terms++]=app;}if(action&&*action){axes[terms]="operation";vals[terms++]=action;}
      terms=collect_json_terms(cms,consumes,"$","consumes",term_storage,axes,vals,terms,64);terms=collect_json_terms(cms,produces,"$","produces",term_storage,axes,vals,terms,64);terms=collect_json_terms(cms,carriers,"$","carrier",term_storage,axes,vals,terms,64);terms=collect_json_terms(cms,requirements,"$","requirement",term_storage,axes,vals,terms,64);terms=collect_json_terms(cms,transitions,"$","transition",term_storage,axes,vals,terms,64);if(authority&&*authority&&terms<64){axes[terms]="authority";vals[terms++]=authority;}
      deps[depc++]=frontier;deps[depc++]=publish;depc=collect_json_dependencies(cms,consumes,"$",NULL,dep_storage,deps,depc,64);depc=collect_json_dependencies(cms,requirements,"$",NULL,dep_storage,deps,depc,64);depc=collect_json_dependencies(cms,transitions,"$",NULL,dep_storage,deps,depc,64);
      emit_record(identity,"capability_program",owner,"CMS:publication","ProtocolContract:capability",frontier,axes,vals,terms,deps,depc);emitted++;
    }
    sqlite3_finalize(st);return emitted;
}

static int emit_site_programs(sqlite3 *cms){
    sqlite3_stmt *st=NULL;int emitted=0;if(!table_exists(cms,"site_program"))return 0;
    char sql[896];
    snprintf(sql,sizeof(sql),"SELECT program_id,%s,%s,%s,%s,%s FROM site_program WHERE status='published' ORDER BY program_id",cms_col(cms,"site_program","site_id"),cms_col(cms,"site_program","experience_programs"),cms_col(cms,"site_program","application_bindings"),cms_col(cms,"site_program","source_collections"),cms_col(cms,"site_program","capability_programs"));
    if(sqlite3_prepare_v2(cms,sql,-1,&st,NULL)!=SQLITE_OK)return -1;
    while(sqlite3_step(st)==SQLITE_ROW){
      const char*id=(const char*)sqlite3_column_text(st,0),*site=(const char*)sqlite3_column_text(st,1),*experiences=(const char*)sqlite3_column_text(st,2),*bindings=(const char*)sqlite3_column_text(st,3),*collections=(const char*)sqlite3_column_text(st,4),*capabilities=(const char*)sqlite3_column_text(st,5);char identity[640],owner[640],frontier[640],publish[256],term_storage[64][256],dep_storage[64][640];const char*axes[64],*vals[64],*deps[64];size_t terms=0,depc=0;
      if(!id||!*id)continue;snprintf(identity,sizeof(identity),"cms:site_program:%s",id);snprintf(owner,sizeof(owner),"CMS:site_program:%s",id);snprintf(frontier,sizeof(frontier),"cms:site_program:%s",id);snprintf(publish,sizeof(publish),"cms:site_program");axes[terms]="cms";vals[terms++]=id;if(site&&*site){axes[terms]="site";vals[terms++]=site;}
      terms=collect_json_terms(cms,experiences,"$","experience",term_storage,axes,vals,terms,64);terms=collect_json_terms(cms,bindings,"$","binding",term_storage,axes,vals,terms,64);terms=collect_json_terms(cms,collections,"$","source_collection",term_storage,axes,vals,terms,64);terms=collect_json_terms(cms,capabilities,"$","capability_program",term_storage,axes,vals,terms,64);
      deps[depc++]=frontier;deps[depc++]=publish;if(site&&*site&&depc<64&&snprintf(dep_storage[depc],sizeof(dep_storage[depc]),"cms:site_presentation:%s",site)<(int)sizeof(dep_storage[depc]))deps[depc]=dep_storage[depc],depc++;
      depc=collect_json_dependencies(cms,bindings,"$","cms:application_binding:",dep_storage,deps,depc,64);depc=collect_json_dependencies(cms,collections,"$","cms:source_collection:",dep_storage,deps,depc,64);depc=collect_json_dependencies(cms,capabilities,"$","cms:capability_program:",dep_storage,deps,depc,64);
      emit_record(identity,"site_program",owner,"CMS:publication","SurfaceIR:site_program",frontier,axes,vals,terms,deps,depc);emitted++;
    }
    sqlite3_finalize(st);return emitted;
}

/* PackGraph publishes behaviour, and only an activation composes a closure:
 * forking is not installing, installing is not activating, and activating is
 * not authorizing.  This emits activations, so the plane can ask which packs
 * are live for a scope before any overlay is composed, and carries the pack's
 * declared dependencies as edges so a dependency publish retires the closure
 * that stood on it.  Cannot infer: an activation term is a declared
 * composition input, never an authority grant, an installed body, or a
 * resolved overlay; a pack is not an addressable subject. */
static int emit_packs(sqlite3 *cms){
    sqlite3_stmt *st=NULL,*deps_st=NULL;int emitted=0;
    if(table_exists(cms,"pack_activation")){
      char sql[640];
      snprintf(sql,sizeof(sql),"SELECT pack,activated_for,scope,%s FROM pack_activation WHERE status='published' ORDER BY pack,activated_for,scope",cms_col(cms,"pack_activation","conditions"));
      if(sqlite3_prepare_v2(cms,sql,-1,&st,NULL)!=SQLITE_OK)return -1;
      while(sqlite3_step(st)==SQLITE_ROW){
        const char*pack=(const char*)sqlite3_column_text(st,0),*forwhom=(const char*)sqlite3_column_text(st,1),*scope=(const char*)sqlite3_column_text(st,2),*conditions=(const char*)sqlite3_column_text(st,3);
        /* Private installed closures require their retained dependency lock and
         * current session scope. A public head-based advertisement cannot
         * represent that decision or expose its account coordinates. */
        if(conditions&&!strncmp(conditions,"pack-lock:",10))continue;
        char identity[640],owner[640],frontier[640],publish[256],head[640],term_storage[32][256],dep_storage[32][640];const char*axes[32],*vals[32],*deps[32];size_t terms=0,depc=0;
        if(!pack||!*pack||!forwhom||!*forwhom||!scope||!*scope)continue;
        if(snprintf(identity,sizeof(identity),"cms:pack_activation:%s:%s:%s",pack,forwhom,scope)>=(int)sizeof(identity))continue;
        snprintf(owner,sizeof(owner),"CMS:pack_activation:%s",pack);snprintf(frontier,sizeof(frontier),"%s",identity);snprintf(publish,sizeof(publish),"cms:pack_activation");
        axes[terms]="pack";vals[terms++]=pack;axes[terms]="subject";vals[terms++]=forwhom;axes[terms]="scope";vals[terms++]=scope;
        terms=collect_json_terms(cms,conditions,"$","condition",term_storage,axes,vals,terms,32);
        deps[depc++]=frontier;deps[depc++]=publish;
        if(snprintf(head,sizeof(head),"cms:pack_head:%s",pack)<(int)sizeof(head)&&depc<32){deps[depc]=head;depc++;}
        if(table_exists(cms,"pack_dependency")&&sqlite3_prepare_v2(cms,"SELECT depends_on FROM pack_dependency WHERE pack=? AND status='published' ORDER BY depends_on",-1,&deps_st,NULL)==SQLITE_OK){
          sqlite3_bind_text(deps_st,1,pack,-1,SQLITE_TRANSIENT);
          while(depc<32&&sqlite3_step(deps_st)==SQLITE_ROW){const char*on=(const char*)sqlite3_column_text(deps_st,0);if(!on||!*on)continue;
            if(snprintf(dep_storage[depc],sizeof(dep_storage[depc]),"cms:pack_head:%s",on)<(int)sizeof(dep_storage[depc])){deps[depc]=dep_storage[depc];depc++;}}
          sqlite3_finalize(deps_st);deps_st=NULL;}
        emit_record(identity,"pack",owner,"CMS:publication","PackGraph:activation",frontier,axes,vals,terms,deps,depc);emitted++;
      }
      sqlite3_finalize(st);st=NULL;
    }
    /* A pack declares demand and does not fetch.  Emitting the declaration is
     * what lets a hydration compiler be woken by it; it is not a schedule, a
     * reservation, or permission to retrieve a body. */
    if(table_exists(cms,"pack_demand")){
      char sql[896];
      snprintf(sql,sizeof(sql),"SELECT pack,demand_kind,needs,%s,%s,%s,%s FROM pack_demand WHERE status='published' ORDER BY pack,demand_kind,needs",
        cms_col(cms,"pack_demand","preferred_sources"),cms_col(cms,"pack_demand","fields"),
        cms_col(cms,"pack_demand","body_regime"),cms_col(cms,"pack_demand","reheat_when"));
      if(sqlite3_prepare_v2(cms,sql,-1,&st,NULL)!=SQLITE_OK)return -1;
      while(sqlite3_step(st)==SQLITE_ROW){
        const char*pack=(const char*)sqlite3_column_text(st,0),*kind=(const char*)sqlite3_column_text(st,1),*needs=(const char*)sqlite3_column_text(st,2),*sources=(const char*)sqlite3_column_text(st,3),*fields=(const char*)sqlite3_column_text(st,4),*regime=(const char*)sqlite3_column_text(st,5),*reheat=(const char*)sqlite3_column_text(st,6);
        char identity[640],owner[640],frontier[640],publish[256],head[640],term_storage[48][256];const char*axes[48],*vals[48],*deps[8];size_t terms=0,depc=0;
        if(!pack||!*pack||!kind||!*kind||!needs||!*needs)continue;
        if(snprintf(identity,sizeof(identity),"cms:pack_demand:%s:%s:%s",pack,kind,needs)>=(int)sizeof(identity))continue;
        snprintf(owner,sizeof(owner),"CMS:pack_demand:%s",pack);snprintf(frontier,sizeof(frontier),"%s",identity);snprintf(publish,sizeof(publish),"cms:pack_demand");
        axes[terms]="pack";vals[terms++]=pack;axes[terms]="demand";vals[terms++]=kind;axes[terms]="requirement";vals[terms++]=needs;
        terms=collect_json_terms(cms,sources,"$","source",term_storage,axes,vals,terms,48);
        terms=collect_json_terms(cms,fields,"$","field",term_storage,axes,vals,terms,48);
        if(regime&&*regime&&terms<48){axes[terms]="regime";vals[terms++]=regime;}
        if(reheat&&*reheat&&terms<48){axes[terms]="reheat";vals[terms++]=reheat;}
        deps[depc++]=frontier;deps[depc++]=publish;
        if(snprintf(head,sizeof(head),"cms:pack_head:%s",pack)<(int)sizeof(head)){deps[depc]=head;depc++;}
        emit_record(identity,"pack_demand",owner,"CMS:publication","MaterialCompiler:pack_demand",frontier,axes,vals,terms,deps,depc);emitted++;
      }
      sqlite3_finalize(st);
    }
    return emitted;
}

/* RelationGraph answers which relationship applies, and that is a separate
 * fact from what may be disclosed under it and from what the member may do.
 * A membership's authority reference is carried as a dependency so an
 * authority publish retires this cut, and deliberately not as a term: a term
 * would let the plane read a relation as a grant.  Cannot infer: a membership
 * term is a declared relation, never disclosure, authority or entitlement. */
static int emit_relations(sqlite3 *cms){
    sqlite3_stmt *st=NULL;int emitted=0;char sql[768];
    if(!table_exists(cms,"community_membership"))return 0;
    snprintf(sql,sizeof(sql),"SELECT membership,community,member_subject,role,%s,%s FROM community_membership WHERE status='published' ORDER BY membership",
      cms_col(cms,"community_membership","state"),cms_col(cms,"community_membership","authority_ref"));
    if(sqlite3_prepare_v2(cms,sql,-1,&st,NULL)!=SQLITE_OK)return -1;
    while(sqlite3_step(st)==SQLITE_ROW){
      const char*id=(const char*)sqlite3_column_text(st,0),*community=(const char*)sqlite3_column_text(st,1),*member=(const char*)sqlite3_column_text(st,2),*role=(const char*)sqlite3_column_text(st,3),*state=(const char*)sqlite3_column_text(st,4),*authority=(const char*)sqlite3_column_text(st,5);
      char identity[640],owner[640],frontier[640],publish[256],body[640],auth[640];const char*axes[8],*vals[8],*deps[8];size_t terms=0,depc=0;
      if(!id||!*id||!community||!*community||!member||!*member)continue;
      snprintf(identity,sizeof(identity),"cms:community_membership:%s",id);snprintf(owner,sizeof(owner),"CMS:community_membership:%s",id);snprintf(frontier,sizeof(frontier),"%s",identity);snprintf(publish,sizeof(publish),"cms:community_membership");
      axes[terms]="community";vals[terms++]=community;axes[terms]="member";vals[terms++]=member;
      if(role&&*role){axes[terms]="role";vals[terms++]=role;}
      if(state&&*state){axes[terms]="state";vals[terms++]=state;}
      deps[depc++]=frontier;deps[depc++]=publish;
      if(snprintf(body,sizeof(body),"cms:community_body:%s",community)<(int)sizeof(body)){deps[depc]=body;depc++;}
      if(authority&&*authority&&snprintf(auth,sizeof(auth),"%s",authority)<(int)sizeof(auth)){deps[depc]=auth;depc++;}
      emit_record(identity,"relation",owner,"CMS:publication","RelationGraph:membership",frontier,axes,vals,terms,deps,depc);emitted++;
    }
    sqlite3_finalize(st);return emitted;
}

/* Social presence is a bounded projection over a membership, not a second
 * relationship graph and never an attendance, location or connection claim.
 * The forwarding index receives the named community/surface and disclosure
 * posture so a lawful community/program closure can find it, while the actor,
 * attention and availability stay with the canonical owner.  The exact
 * membership reference is a dependency, making withdrawal/expiry change this
 * candidate without turning membership into authority. */
static int emit_social_presence(sqlite3 *cms){
    sqlite3_stmt *st=NULL; int emitted=0; char sql[1024];
    if(!table_exists(cms,"community_presence") || !table_exists(cms,"community_membership")) return 0;
    snprintf(sql,sizeof(sql),
      "SELECT p.presence,p.community,p.surface,%s,"
      "(SELECT m.membership FROM community_membership m WHERE m.community=p.community AND m.member_subject=p.actor_subject "
      "AND m.status='published' ORDER BY m.rowid DESC LIMIT 1) "
      "FROM community_presence p WHERE p.status='published' ORDER BY p.presence",
      cms_col(cms,"community_presence","disclosure"));
    if(sqlite3_prepare_v2(cms,sql,-1,&st,NULL)!=SQLITE_OK) return -1;
    while(sqlite3_step(st)==SQLITE_ROW){
      const char *id=(const char*)sqlite3_column_text(st,0), *community=(const char*)sqlite3_column_text(st,1),
                 *surface=(const char*)sqlite3_column_text(st,2), *disclosure=(const char*)sqlite3_column_text(st,3),
                 *membership=(const char*)sqlite3_column_text(st,4);
      char identity[640],owner[640],frontier[640],publish[256],body[640],member_ref[640];
      const char *axes[6],*vals[6],*deps[5]; size_t terms=0,depc=0;
      if(!id||!*id||!community||!*community||!surface||!*surface) continue;
      if(snprintf(identity,sizeof(identity),"cms:community_presence:%s",id)>=(int)sizeof(identity)) continue;
      snprintf(owner,sizeof(owner),"CMS:community_presence:%s",id);
      snprintf(frontier,sizeof(frontier),"%s",identity);
      snprintf(publish,sizeof(publish),"cms:community_presence");
      axes[terms]="community"; vals[terms++]=community;
      axes[terms]="surface"; vals[terms++]=surface;
      if(disclosure&&*disclosure){axes[terms]="disclosure";vals[terms++]=disclosure;}
      deps[depc++]=frontier; deps[depc++]=publish;
      if(snprintf(body,sizeof(body),"cms:community_body:%s",community)<(int)sizeof(body)){deps[depc]=body;depc++;}
      if(membership&&*membership&&snprintf(member_ref,sizeof(member_ref),"cms:community_membership:%s",membership)<(int)sizeof(member_ref)){deps[depc]=member_ref;depc++;}
      emit_record(identity,"social_presence",owner,"CMS:publication","SocialPresenceGraph:community",frontier,axes,vals,terms,deps,depc);
      emitted++;
    }
    sqlite3_finalize(st); return emitted;
}

/* Space is another exact coordinate on an existing subject, not a new identity
 * for it, so this lowers only what narrows a population and stops there. The
 * geometry, the CRS and the datum are never emitted as terms: an exact spatial
 * question -- intersects, within-radius, along-route, visible-from -- is
 * answered by the spatial owner on whatever survives this cut, and a plane that
 * carried geometry would start answering it badly and cheaply.
 *
 * `mode` is the term that earns this record's existence. A display position
 * computed between two fixes is legitimate and is not a third fix, so asking
 * the plane for what a source observed can never return what this estate
 * interpolated. Same for a CoverageVolume's `derivation`: a published
 * specification and a measured footprint are different claims.
 *
 * Cannot infer: a coarse cell is not a location; a subject sharing a cell with
 * another is not near it; a coverage volume reaching a subject is not
 * permission to observe, disclose or serve it; and a valid position is not a
 * disclosable one -- InformationPolicy still decides. */
static int emit_spatial(sqlite3 *cms){
    sqlite3_stmt *st=NULL;int emitted=0;
    if(table_exists(cms,"spatial_observation")){
      char sql[1024];
      snprintf(sql,sizeof(sql),"SELECT observation_id,subject_address,geometry_kind,observation_mode,mobility,scale,region,cell,%s,%s FROM spatial_observation WHERE status='published' ORDER BY observation_id",
        cms_col(cms,"spatial_observation","source_occurrence"),cms_col(cms,"spatial_observation","derived_from"));
      if(sqlite3_prepare_v2(cms,sql,-1,&st,NULL)!=SQLITE_OK)return -1;
      while(sqlite3_step(st)==SQLITE_ROW){
        const char*id=(const char*)sqlite3_column_text(st,0),*subject=(const char*)sqlite3_column_text(st,1),*kind=(const char*)sqlite3_column_text(st,2),*mode=(const char*)sqlite3_column_text(st,3),*mobility=(const char*)sqlite3_column_text(st,4),*scale=(const char*)sqlite3_column_text(st,5),*region=(const char*)sqlite3_column_text(st,6),*cell=(const char*)sqlite3_column_text(st,7),*occurrence=(const char*)sqlite3_column_text(st,8),*derived=(const char*)sqlite3_column_text(st,9);
        char identity[640],owner[640],frontier[640],publish[256],dep_storage[32][640];const char*axes[32],*vals[32],*deps[32];size_t terms=0,depc=0;
        if(!id||!*id||!subject||!*subject)continue;
        snprintf(identity,sizeof(identity),"cms:spatial_observation:%s",id);snprintf(owner,sizeof(owner),"CMS:spatial_observation:%s",id);snprintf(frontier,sizeof(frontier),"%s",identity);snprintf(publish,sizeof(publish),"cms:spatial_observation");
        axes[terms]="spatial";vals[terms++]="true";axes[terms]="subject";vals[terms++]=subject;
        if(kind&&*kind){axes[terms]="geometry_kind";vals[terms++]=kind;}
        if(mode&&*mode){axes[terms]="mode";vals[terms++]=mode;}
        if(mobility&&*mobility){axes[terms]="mobility";vals[terms++]=mobility;}
        if(scale&&*scale){axes[terms]="scale";vals[terms++]=scale;}
        if(region&&*region){axes[terms]="region";vals[terms++]=region;}
        if(cell&&*cell){axes[terms]="cell";vals[terms++]=cell;}
        deps[depc++]=frontier;deps[depc++]=publish;
        /* The observation retires behind the occurrence that produced it, and a
         * derived position retires behind every fix it was computed from. */
        if(occurrence&&*occurrence&&depc<32&&strlen(occurrence)<sizeof(dep_storage[depc])){snprintf(dep_storage[depc],sizeof(dep_storage[depc]),"%s",occurrence);deps[depc]=dep_storage[depc];depc++;}
        depc=collect_json_dependencies(cms,derived,"$","cms:spatial_observation:",dep_storage,deps,depc,32);
        emit_record(identity,"spatial_observation",owner,"SpatialOwner:verify","SpatialOwner:observation",frontier,axes,vals,terms,deps,depc);emitted++;
      }
      sqlite3_finalize(st);st=NULL;
    }
    if(table_exists(cms,"coverage_volume")){
      char sql[896];
      snprintf(sql,sizeof(sql),"SELECT coverage_id,owner_subject,derivation,scale,region,cell,%s,%s FROM coverage_volume WHERE status='published' ORDER BY coverage_id",
        cms_col(cms,"coverage_volume","quality"),cms_col(cms,"coverage_volume","valid_frontier"));
      if(sqlite3_prepare_v2(cms,sql,-1,&st,NULL)!=SQLITE_OK)return -1;
      while(sqlite3_step(st)==SQLITE_ROW){
        const char*id=(const char*)sqlite3_column_text(st,0),*subject=(const char*)sqlite3_column_text(st,1),*derivation=(const char*)sqlite3_column_text(st,2),*scale=(const char*)sqlite3_column_text(st,3),*region=(const char*)sqlite3_column_text(st,4),*cell=(const char*)sqlite3_column_text(st,5),*quality=(const char*)sqlite3_column_text(st,6),*valid=(const char*)sqlite3_column_text(st,7);
        char identity[640],owner[640],frontier[640],publish[256];const char*axes[16],*vals[16],*deps[8];size_t terms=0,depc=0;
        if(!id||!*id||!subject||!*subject)continue;
        snprintf(identity,sizeof(identity),"cms:coverage_volume:%s",id);snprintf(owner,sizeof(owner),"CMS:coverage_volume:%s",id);snprintf(publish,sizeof(publish),"cms:coverage_volume");
        snprintf(frontier,sizeof(frontier),"%s",(valid&&*valid)?valid:identity);
        axes[terms]="spatial";vals[terms++]="true";axes[terms]="subject";vals[terms++]=subject;axes[terms]="geometry_kind";vals[terms++]="volume";
        if(derivation&&*derivation){axes[terms]="derivation";vals[terms++]=derivation;}
        if(scale&&*scale){axes[terms]="scale";vals[terms++]=scale;}
        if(region&&*region){axes[terms]="region";vals[terms++]=region;}
        if(cell&&*cell){axes[terms]="cell";vals[terms++]=cell;}
        if(quality&&*quality){axes[terms]="state";vals[terms++]=quality;}
        deps[depc++]=frontier;deps[depc++]=publish;
        emit_record(identity,"coverage_volume",owner,"SpatialOwner:verify","SpatialOwner:coverage",frontier,axes,vals,terms,deps,depc);emitted++;
      }
      sqlite3_finalize(st);
    }
    return emitted;
}
