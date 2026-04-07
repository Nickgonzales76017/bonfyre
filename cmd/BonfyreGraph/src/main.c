/*
 * BonfyreGraph — Merkle-DAG artifact graph engine.
 *
 * Replaces: ArtifactManifest (validate_artifact.py) + SQLiteGraphStore (sqlite_graph.py)
 *
 * Manages a content-addressed DAG of atoms, operators, and realizations.
 * Each operator's node_hash is computed from its op, params, input hashes,
 * and version — enabling tamper detection and lineage tracking.
 *
 * Storage: SQLite (atoms, operators, realizations, meta tables).
 * Hashing: SHA-256 via hand-rolled implementation (zero deps beyond libc).
 *
 * Usage:
 *   bonfyre-graph init <db>
 *   bonfyre-graph add-atom <db> --id ID --hash HASH --type TYPE [--path PATH]
 *   bonfyre-graph add-op <db> --id ID --op OP --inputs A,B --output OUT [--params '{}']
 *   bonfyre-graph validate <manifest.json>
 *   bonfyre-graph merkle <manifest.json>
 *   bonfyre-graph lineage <db> --id ID
 *   bonfyre-graph export <db> [--out artifact.json]
 *   bonfyre-graph status <db>
 */
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <sqlite3.h>
#include <bonfyre.h>

#define MAX_PATH  2048
#define MAX_IDS   512
#define MAX_LINE  65536
#define FILE_CHUNK 65536

/* ── SHA-256 (FIPS 180-4, self-contained) ─────────────────────────────── */

static const unsigned sha256_k[64] = {
    0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2,
};

#define RR(x,n) (((x)>>(n))|((x)<<(32-(n))))
#define CH(x,y,z) (((x)&(y))^((~(x))&(z)))
#define MAJ(x,y,z) (((x)&(y))^((x)&(z))^((y)&(z)))
#define EP0(x) (RR(x,2)^RR(x,13)^RR(x,22))
#define EP1(x) (RR(x,6)^RR(x,11)^RR(x,25))
#define SIG0(x) (RR(x,7)^RR(x,18)^((x)>>3))
#define SIG1(x) (RR(x,17)^RR(x,19)^((x)>>10))

typedef struct {
    unsigned state[8];
    unsigned char buf[64];
    unsigned long long bitlen;
    unsigned buflen;
} Sha256;

static void sha256_init(Sha256 *ctx) {
    ctx->state[0]=0x6a09e667; ctx->state[1]=0xbb67ae85;
    ctx->state[2]=0x3c6ef372; ctx->state[3]=0xa54ff53a;
    ctx->state[4]=0x510e527f; ctx->state[5]=0x9b05688c;
    ctx->state[6]=0x1f83d9ab; ctx->state[7]=0x5be0cd19;
    ctx->bitlen=0; ctx->buflen=0;
}

static void sha256_transform(Sha256 *ctx, const unsigned char *data) {
    unsigned w[64], a,b,c,d,e,f,g,h,t1,t2;
    for (int i=0;i<16;i++)
        w[i]=(unsigned)data[i*4]<<24|(unsigned)data[i*4+1]<<16|
             (unsigned)data[i*4+2]<<8|(unsigned)data[i*4+3];
    for (int i=16;i<64;i++)
        w[i]=SIG1(w[i-2])+w[i-7]+SIG0(w[i-15])+w[i-16];
    a=ctx->state[0];b=ctx->state[1];c=ctx->state[2];d=ctx->state[3];
    e=ctx->state[4];f=ctx->state[5];g=ctx->state[6];h=ctx->state[7];
    for (int i=0;i<64;i++){
        t1=h+EP1(e)+CH(e,f,g)+sha256_k[i]+w[i];
        t2=EP0(a)+MAJ(a,b,c);
        h=g;g=f;f=e;e=d+t1;d=c;c=b;b=a;a=t1+t2;
    }
    ctx->state[0]+=a;ctx->state[1]+=b;ctx->state[2]+=c;ctx->state[3]+=d;
    ctx->state[4]+=e;ctx->state[5]+=f;ctx->state[6]+=g;ctx->state[7]+=h;
}

static void sha256_update(Sha256 *ctx, const unsigned char *data, size_t len) {
    for (size_t i=0;i<len;i++){
        ctx->buf[ctx->buflen++]=data[i];
        if (ctx->buflen==64){ sha256_transform(ctx,ctx->buf); ctx->bitlen+=512; ctx->buflen=0; }
    }
}

static void sha256_final(Sha256 *ctx, unsigned char hash[32]) {
    unsigned i=ctx->buflen;
    ctx->buf[i++]=0x80;
    if (i>56){ while(i<64) ctx->buf[i++]=0; sha256_transform(ctx,ctx->buf); i=0; }
    while(i<56) ctx->buf[i++]=0;
    ctx->bitlen+=ctx->buflen*8;
    for (int j=7;j>=0;j--) ctx->buf[56+(7-j)]=(unsigned char)(ctx->bitlen>>(j*8));
    sha256_transform(ctx,ctx->buf);
    for (int j=0;j<8;j++){
        hash[j*4]=(ctx->state[j]>>24)&0xff; hash[j*4+1]=(ctx->state[j]>>16)&0xff;
        hash[j*4+2]=(ctx->state[j]>>8)&0xff; hash[j*4+3]=ctx->state[j]&0xff;
    }
}

static const char g_hex_lut[16] = "0123456789abcdef";

static void sha256_hex(const unsigned char *data, size_t len, char out[65]) {
    Sha256 ctx; sha256_init(&ctx);
    sha256_update(&ctx, data, len);
    unsigned char hash[32]; sha256_final(&ctx, hash);
    for (int i=0;i<32;i++){
        out[i*2]  =g_hex_lut[hash[i]>>4];
        out[i*2+1]=g_hex_lut[hash[i]&0x0f];
    }
    out[64]='\0';
}

static void sha256_file_hex(const char *path, char out[65]) {
    FILE *fp=fopen(path,"rb");
    if (!fp){ out[0]='\0'; return; }
    Sha256 ctx; sha256_init(&ctx);
    unsigned char buf[FILE_CHUNK]; size_t n;
    while ((n=fread(buf,1,sizeof(buf),fp))>0) sha256_update(&ctx,buf,n);
    fclose(fp);
    unsigned char hash[32]; sha256_final(&ctx,hash);
    for (int i=0;i<32;i++) sprintf(out+i*2,"%02x",hash[i]);
    out[64]='\0';
}

/* ── Canonical JSON (deterministic, sorted keys) ──────────────────────── */

/* We only need to canonicalize simple structs for hashing.
 * Format: sorted keys, no whitespace, ascii-escaped. */

/* ── Utility ──────────────────────────────────────────────────────────── */

static char *read_file_full(const char *path) {
    FILE *fp=fopen(path,"rb");
    if (!fp) return NULL;
    fseek(fp,0,SEEK_END); long sz=ftell(fp); fseek(fp,0,SEEK_SET);
    if (sz<0){ fclose(fp); return NULL; }
    char *buf=malloc((size_t)sz+1);
    if (!buf){ fclose(fp); return NULL; }
    fread(buf,1,(size_t)sz,fp); buf[sz]='\0';
    fclose(fp); return buf;
}

static void iso_timestamp(char *buf, size_t sz) {
    time_t now=time(NULL); struct tm t; gmtime_r(&now,&t);
    strftime(buf,sz,"%Y-%m-%dT%H:%M:%SZ",&t);
}

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }
/* ── Tiny JSON parser (read-only, no deps) ────────────────────────────── */

static const char *skip_ws(const char *p) {
    while (*p && isspace((unsigned char)*p)) p++;
    return p;
}

static const char *skip_value(const char *p);

static const char *skip_string(const char *p) {
    if (*p!='"') return p;
    p++;
    while (*p && *p!='"') { if (*p=='\\') p++; p++; }
    if (*p=='"') p++;
    return p;
}

static const char *skip_value(const char *p) {
    p=skip_ws(p);
    if (*p=='"') return skip_string(p);
    if (*p=='{'){ int d=1; p++; while(*p&&d){ if(*p=='{')d++; else if(*p=='}')d--; else if(*p=='"') p=skip_string(p)-1; p++; } return p; }
    if (*p=='['){ int d=1; p++; while(*p&&d){ if(*p=='[')d++; else if(*p==']')d--; else if(*p=='"') p=skip_string(p)-1; p++; } return p; }
    while (*p && *p!=',' && *p!='}' && *p!=']' && !isspace((unsigned char)*p)) p++;
    return p;
}

/* Extract a string value for key from a JSON object. Returns 0 if not found. */
static int json_get_str(const char *json, const char *key, char *out, size_t outsz) {
    char needle[256];
    snprintf(needle,sizeof(needle),"\"%s\"",key);
    const char *pos=strstr(json,needle);
    if (!pos) return 0;
    pos+=strlen(needle);
    pos=skip_ws(pos);
    if (*pos!=':') return 0;
    pos=skip_ws(pos+1);
    if (*pos!='"') return 0;
    pos++;
    size_t i=0;
    while (*pos && *pos!='"' && i<outsz-1) {
        if (*pos=='\\' && *(pos+1)) { pos++; }
        out[i++]=*pos++;
    }
    out[i]='\0';
    return 1;
}

/* Find start of array for a key. Returns pointer to '[' or NULL. */
static const char *json_get_array(const char *json, const char *key) {
    char needle[256];
    snprintf(needle,sizeof(needle),"\"%s\"",key);
    const char *pos=strstr(json,needle);
    if (!pos) return NULL;
    pos+=strlen(needle);
    pos=skip_ws(pos);
    if (*pos!=':') return NULL;
    pos=skip_ws(pos+1);
    if (*pos!='[') return NULL;
    return pos;
}

/* Iterate array of objects. Call with *cursor pointing to '['.
 * Each call fills objbuf with one object and advances cursor.
 * Returns 1 on success, 0 when array ends. */
static int json_next_object(const char **cursor, char *objbuf, size_t objsz) {
    const char *p=skip_ws(*cursor);
    if (*p=='[') p=skip_ws(p+1);
    if (*p==']' || !*p) return 0;
    if (*p==',') p=skip_ws(p+1);
    if (*p!='{') return 0;
    int depth=1; const char *start=p; p++;
    while (*p && depth) {
        if (*p=='{') depth++;
        else if (*p=='}') depth--;
        else if (*p=='"') p=skip_string(p)-1;
        p++;
    }
    size_t len=(size_t)(p-start);
    if (len>=objsz) len=objsz-1;
    memcpy(objbuf,start,len); objbuf[len]='\0';
    *cursor=p;
    return 1;
}

/* Count elements in a JSON array */
static int json_array_count(const char *arr) {
    if (!arr || *arr!='[') return 0;
    const char *p=skip_ws(arr+1);
    if (*p==']') return 0;
    int count=0;
    while (*p && *p!=']') {
        p=skip_value(p);
        count++;
        p=skip_ws(p);
        if (*p==',') p=skip_ws(p+1);
    }
    return count;
}

/* Extract array of strings from JSON array "inputs":["a","b"] into id_buf.
 * Returns count. */
static int json_get_str_array(const char *json, const char *key,
                              char ids[][256], int max_ids) {
    const char *arr=json_get_array(json,key);
    if (!arr) return 0;
    const char *p=skip_ws(arr+1);
    int count=0;
    while (*p && *p!=']' && count<max_ids) {
        p=skip_ws(p);
        if (*p=='"') {
            p++;
            size_t i=0;
            while (*p && *p!='"' && i<255) {
                if (*p=='\\' && *(p+1)) p++;
                ids[count][i++]=*p++;
            }
            ids[count][i]='\0';
            if (*p=='"') p++;
            count++;
        }
        p=skip_ws(p);
        if (*p==',') p++;
    }
    return count;
}

/* ── SQLite schema ────────────────────────────────────────────────────── */

static const char *SCHEMA_SQL =
    "CREATE TABLE IF NOT EXISTS atoms ("
    "  atom_id TEXT PRIMARY KEY,"
    "  content_hash TEXT NOT NULL,"
    "  media_type TEXT NOT NULL,"
    "  path TEXT,"
    "  byte_size INTEGER,"
    "  label TEXT"
    ");"
    "CREATE TABLE IF NOT EXISTS operators ("
    "  operator_id TEXT PRIMARY KEY,"
    "  op TEXT NOT NULL,"
    "  inputs TEXT NOT NULL,"
    "  output TEXT NOT NULL,"
    "  params TEXT DEFAULT '{}',"
    "  node_hash TEXT,"
    "  version TEXT DEFAULT '1.0.0',"
    "  deterministic INTEGER DEFAULT 1"
    ");"
    "CREATE TABLE IF NOT EXISTS realizations ("
    "  realization_id TEXT PRIMARY KEY,"
    "  media_type TEXT NOT NULL,"
    "  path TEXT,"
    "  content_hash TEXT,"
    "  byte_size INTEGER,"
    "  pinned INTEGER DEFAULT 0,"
    "  produced_by TEXT,"
    "  label TEXT"
    ");"
    "CREATE TABLE IF NOT EXISTS meta ("
    "  key TEXT PRIMARY KEY,"
    "  value TEXT"
    ");";

/* ── Node hash computation ────────────────────────────────────────────── */

/* Compute node_hash: SHA-256 of canonical {"inputs_hashes":[...],"op":"...","params":{...},"version":"..."} */
static void compute_node_hash(const char *op, const char *params,
                               const char *input_hashes[], int ninputs,
                               const char *version, char out[65]) {
    /* Sort input hashes */
    const char *sorted[MAX_IDS];
    for (int i=0;i<ninputs;i++) sorted[i]=input_hashes[i];
    for (int i=0;i<ninputs-1;i++)
        for (int j=i+1;j<ninputs;j++)
            if (strcmp(sorted[i],sorted[j])>0){ const char *t=sorted[i]; sorted[i]=sorted[j]; sorted[j]=t; }

    /* Build canonical JSON via memcpy — no format parsing overhead */
    char canonical[MAX_LINE];
    char *p=canonical;
    #define CPLIT(s) do{ memcpy(p,s,sizeof(s)-1); p+=sizeof(s)-1; }while(0)
    CPLIT("{\"inputs_hashes\":[");
    for (int i=0;i<ninputs;i++){
        if (i) *p++=',';
        *p++='"';
        size_t hlen=strlen(sorted[i]);
        memcpy(p,sorted[i],hlen); p+=hlen;
        *p++='"';
    }
    CPLIT("],\"op\":\"");
    { size_t ol=strlen(op); memcpy(p,op,ol); p+=ol; }
    CPLIT("\",\"params\":");
    { const char *pp=(params&&*params)?params:"{}"; size_t pl=strlen(pp); memcpy(p,pp,pl); p+=pl; }
    CPLIT(",\"version\":\"");
    { const char *vv=version?version:""; size_t vl=strlen(vv); memcpy(p,vv,vl); p+=vl; }
    CPLIT("\"}");
    #undef CPLIT
    *p='\0';

    sha256_hex((const unsigned char *)canonical,(size_t)(p-canonical),out);
}

/* ── Commands ─────────────────────────────────────────────────────────── */

static int cmd_init(const char *db_path) {
    sqlite3 *db;
    if (sqlite3_open(db_path,&db)!=SQLITE_OK){
        fprintf(stderr,"Cannot open %s: %s\n",db_path,sqlite3_errmsg(db));
        return 1;
    }
    char *err=NULL;
    if (sqlite3_exec(db,SCHEMA_SQL,NULL,NULL,&err)!=SQLITE_OK){
        fprintf(stderr,"Schema error: %s\n",err);
        sqlite3_free(err); sqlite3_close(db); return 1;
    }
    sqlite3_exec(db,"INSERT OR IGNORE INTO meta VALUES ('schema_version','1.0.0')",NULL,NULL,NULL);
    sqlite3_close(db);
    printf("Initialized: %s\n",db_path);
    return 0;
}

static int cmd_add_atom(const char *db_path, const char *id,
                        const char *hash, const char *type, const char *path) {
    sqlite3 *db;
    if (sqlite3_open(db_path,&db)!=SQLITE_OK){
        fprintf(stderr,"Cannot open %s\n",db_path); return 1;
    }
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO atoms (atom_id,content_hash,media_type,path) VALUES (?,?,?,?)",-1,&st,NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,hash,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,type,-1,SQLITE_STATIC);
    if (path) sqlite3_bind_text(st,4,path,-1,SQLITE_STATIC);
    else sqlite3_bind_null(st,4);
    sqlite3_step(st); sqlite3_finalize(st);
    sqlite3_close(db);
    printf("Added atom: %s\n",id);
    return 0;
}

static int cmd_add_op(const char *db_path, const char *id, const char *op,
                      const char *inputs_csv, const char *output,
                      const char *params, const char *version) {
    sqlite3 *db;
    if (sqlite3_open(db_path,&db)!=SQLITE_OK){
        fprintf(stderr,"Cannot open %s\n",db_path); return 1;
    }
    if (!version) version="1.0.0";
    if (!params) params="{}";

    /* Parse inputs CSV */
    char inputs_json[MAX_LINE];
    int ij_off=0;
    ij_off+=snprintf(inputs_json,sizeof(inputs_json),"[");
    char inputs_copy[MAX_LINE];
    snprintf(inputs_copy,sizeof(inputs_copy),"%s",inputs_csv);
    char *tok=strtok(inputs_copy,",");
    int first=1;
    char input_ids[MAX_IDS][256];
    int ninputs=0;
    while (tok && ninputs<MAX_IDS) {
        while (*tok==' ') tok++;
        char *end=tok+strlen(tok)-1;
        while (end>tok && *end==' ') *end--='\0';
        snprintf(input_ids[ninputs],256,"%s",tok);
        if (!first) ij_off+=snprintf(inputs_json+ij_off,sizeof(inputs_json)-ij_off,",");
        ij_off+=snprintf(inputs_json+ij_off,sizeof(inputs_json)-ij_off,"\"%s\"",tok);
        first=0; ninputs++;
        tok=strtok(NULL,",");
    }
    ij_off+=snprintf(inputs_json+ij_off,sizeof(inputs_json)-ij_off,"]");

    /* Collect input hashes from DB */
    const char *hash_ptrs[MAX_IDS];
    char hash_bufs[MAX_IDS][65];
    for (int i=0;i<ninputs;i++) {
        hash_bufs[i][0]='\0';
        hash_ptrs[i]=hash_bufs[i];
        sqlite3_stmt *st;
        /* Check atoms first */
        sqlite3_prepare_v2(db,
            "SELECT content_hash FROM atoms WHERE atom_id=?",-1,&st,NULL);
        sqlite3_bind_text(st,1,input_ids[i],-1,SQLITE_STATIC);
        if (sqlite3_step(st)==SQLITE_ROW){
            snprintf(hash_bufs[i],65,"%s",(const char*)sqlite3_column_text(st,0));
        }
        sqlite3_finalize(st);
        if (!hash_bufs[i][0]){
            /* Check operators */
            sqlite3_prepare_v2(db,
                "SELECT node_hash FROM operators WHERE operator_id=? AND node_hash IS NOT NULL",
                -1,&st,NULL);
            sqlite3_bind_text(st,1,input_ids[i],-1,SQLITE_STATIC);
            if (sqlite3_step(st)==SQLITE_ROW){
                snprintf(hash_bufs[i],65,"%s",(const char*)sqlite3_column_text(st,0));
            }
            sqlite3_finalize(st);
        }
    }

    /* Compute node hash */
    char node_hash[65];
    compute_node_hash(op,params,(const char**)hash_ptrs,ninputs,version,node_hash);

    /* Insert */
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO operators "
        "(operator_id,op,inputs,output,params,node_hash,version) "
        "VALUES (?,?,?,?,?,?,?)",-1,&st,NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,op,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,inputs_json,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,4,output,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,5,params,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,6,node_hash,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,7,version,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);
    sqlite3_close(db);
    printf("Added operator: %s (hash: %.16s...)\n",id,node_hash);
    return 0;
}

static int cmd_lineage(const char *db_path, const char *node_id) {
    sqlite3 *db;
    if (sqlite3_open(db_path,&db)!=SQLITE_OK){
        fprintf(stderr,"Cannot open %s\n",db_path); return 1;
    }

    /* BFS backward from node_id */
    char queue[MAX_IDS][256];
    int visited_count=0;
    char visited[MAX_IDS][256];
    int head=0, tail=0;
    snprintf(queue[tail++],256,"%s",node_id);

    while (head<tail && head<MAX_IDS) {
        char *nid=queue[head++];
        /* Check if already visited */
        int seen=0;
        for (int i=0;i<visited_count;i++)
            if (strcmp(visited[i],nid)==0){ seen=1; break; }
        if (seen) continue;
        snprintf(visited[visited_count++],256,"%s",nid);

        /* Check operators */
        sqlite3_stmt *st;
        sqlite3_prepare_v2(db,
            "SELECT operator_id,op,inputs,output FROM operators WHERE operator_id=? OR output=?",
            -1,&st,NULL);
        sqlite3_bind_text(st,1,nid,-1,SQLITE_STATIC);
        sqlite3_bind_text(st,2,nid,-1,SQLITE_STATIC);
        if (sqlite3_step(st)==SQLITE_ROW) {
            const char *oid=(const char*)sqlite3_column_text(st,0);
            const char *op=(const char*)sqlite3_column_text(st,1);
            const char *inputs=(const char*)sqlite3_column_text(st,2);
            const char *output=(const char*)sqlite3_column_text(st,3);
            printf("  op: %s  inputs: %s  output: %s\n",op,inputs,output);

            /* Parse inputs JSON array and enqueue */
            if (inputs && *inputs=='[') {
                const char *p=inputs+1;
                while (*p && *p!=']') {
                    while (*p && (*p==','||isspace((unsigned char)*p))) p++;
                    if (*p=='"') {
                        p++;
                        char inp[256]; size_t j=0;
                        while (*p && *p!='"' && j<255) inp[j++]=*p++;
                        inp[j]='\0';
                        if (*p=='"') p++;
                        if (tail<MAX_IDS) snprintf(queue[tail++],256,"%s",inp);
                    } else break;
                }
            }
            (void)oid; (void)output;
        }
        sqlite3_finalize(st);

        /* Check atoms */
        sqlite3_prepare_v2(db,
            "SELECT atom_id,media_type,path FROM atoms WHERE atom_id=?",-1,&st,NULL);
        sqlite3_bind_text(st,1,nid,-1,SQLITE_STATIC);
        if (sqlite3_step(st)==SQLITE_ROW) {
            const char *a_id=(const char*)sqlite3_column_text(st,0);
            const char *a_type=(const char*)sqlite3_column_text(st,1);
            const char *a_path=(const char*)sqlite3_column_text(st,2);
            printf("  atom: %s  type: %s  path: %s\n",
                a_id?a_id:"", a_type?a_type:"", a_path?a_path:"(none)");
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(db);
    return 0;
}

static int cmd_export(const char *db_path, const char *out_path) {
    sqlite3 *db;
    if (sqlite3_open(db_path,&db)!=SQLITE_OK){
        fprintf(stderr,"Cannot open %s\n",db_path); return 1;
    }
    FILE *fp=fopen(out_path,"w");
    if (!fp){ fprintf(stderr,"Cannot write %s\n",out_path); sqlite3_close(db); return 1; }

    /* Extract db stem as artifact_id */
    const char *slash=strrchr(db_path,'/');
    const char *name=slash?slash+1:db_path;
    char artifact_id[256];
    snprintf(artifact_id,sizeof(artifact_id),"%s",name);
    char *dot=strrchr(artifact_id,'.');
    if (dot) *dot='\0';

    fprintf(fp,"{\n  \"schema_version\": \"1.0.0\",\n");
    fprintf(fp,"  \"artifact_id\": \"%s\",\n",artifact_id);
    fprintf(fp,"  \"artifact_type\": \"custom\",\n");

    /* Atoms */
    fprintf(fp,"  \"atoms\": [\n");
    sqlite3_stmt *st;
    sqlite3_prepare_v2(db,"SELECT atom_id,content_hash,media_type,path,byte_size,label FROM atoms",-1,&st,NULL);
    int first=1;
    while (sqlite3_step(st)==SQLITE_ROW) {
        if (!first) fprintf(fp,",\n");
        first=0;
        fprintf(fp,"    {\"atom_id\":\"%s\",\"content_hash\":\"%s\",\"media_type\":\"%s\"",
            (const char*)sqlite3_column_text(st,0),
            (const char*)sqlite3_column_text(st,1),
            (const char*)sqlite3_column_text(st,2));
        if (sqlite3_column_type(st,3)!=SQLITE_NULL)
            fprintf(fp,",\"path\":\"%s\"",(const char*)sqlite3_column_text(st,3));
        if (sqlite3_column_type(st,4)!=SQLITE_NULL)
            fprintf(fp,",\"byte_size\":%d",sqlite3_column_int(st,4));
        if (sqlite3_column_type(st,5)!=SQLITE_NULL)
            fprintf(fp,",\"label\":\"%s\"",(const char*)sqlite3_column_text(st,5));
        fprintf(fp,"}");
    }
    sqlite3_finalize(st);
    fprintf(fp,"\n  ],\n");

    /* Operators */
    fprintf(fp,"  \"operators\": [\n");
    sqlite3_prepare_v2(db,"SELECT operator_id,op,inputs,output,params,node_hash,version FROM operators",-1,&st,NULL);
    first=1;
    while (sqlite3_step(st)==SQLITE_ROW) {
        if (!first) fprintf(fp,",\n");
        first=0;
        fprintf(fp,"    {\"operator_id\":\"%s\",\"op\":\"%s\",\"inputs\":%s,\"output\":\"%s\"",
            (const char*)sqlite3_column_text(st,0),
            (const char*)sqlite3_column_text(st,1),
            (const char*)sqlite3_column_text(st,2),
            (const char*)sqlite3_column_text(st,3));
        const char *params=(const char*)sqlite3_column_text(st,4);
        if (params && strcmp(params,"{}")!=0)
            fprintf(fp,",\"params\":%s",params);
        if (sqlite3_column_type(st,5)!=SQLITE_NULL)
            fprintf(fp,",\"node_hash\":\"%s\"",(const char*)sqlite3_column_text(st,5));
        if (sqlite3_column_type(st,6)!=SQLITE_NULL)
            fprintf(fp,",\"version\":\"%s\"",(const char*)sqlite3_column_text(st,6));
        fprintf(fp,"}");
    }
    sqlite3_finalize(st);
    fprintf(fp,"\n  ],\n");

    /* Realizations */
    fprintf(fp,"  \"realizations\": [\n");
    sqlite3_prepare_v2(db,
        "SELECT realization_id,media_type,path,content_hash,byte_size,pinned,produced_by,label FROM realizations",
        -1,&st,NULL);
    first=1;
    while (sqlite3_step(st)==SQLITE_ROW) {
        if (!first) fprintf(fp,",\n");
        first=0;
        fprintf(fp,"    {\"realization_id\":\"%s\",\"media_type\":\"%s\"",
            (const char*)sqlite3_column_text(st,0),
            (const char*)sqlite3_column_text(st,1));
        if (sqlite3_column_type(st,2)!=SQLITE_NULL)
            fprintf(fp,",\"path\":\"%s\"",(const char*)sqlite3_column_text(st,2));
        if (sqlite3_column_type(st,3)!=SQLITE_NULL)
            fprintf(fp,",\"content_hash\":\"%s\"",(const char*)sqlite3_column_text(st,3));
        if (sqlite3_column_type(st,4)!=SQLITE_NULL)
            fprintf(fp,",\"byte_size\":%d",sqlite3_column_int(st,4));
        if (sqlite3_column_type(st,5)!=SQLITE_NULL)
            fprintf(fp,",\"pinned\":%d",sqlite3_column_int(st,5));
        if (sqlite3_column_type(st,6)!=SQLITE_NULL)
            fprintf(fp,",\"produced_by\":\"%s\"",(const char*)sqlite3_column_text(st,6));
        if (sqlite3_column_type(st,7)!=SQLITE_NULL)
            fprintf(fp,",\"label\":\"%s\"",(const char*)sqlite3_column_text(st,7));
        fprintf(fp,"}");
    }
    sqlite3_finalize(st);
    fprintf(fp,"\n  ]\n}\n");

    sqlite3_close(db);
    fclose(fp);
    printf("Exported: %s\n",out_path);
    return 0;
}

/* ── Manifest validation (standalone JSON file, no DB needed) ─────────── */

static int cmd_validate(const char *manifest_path) {
    char *json=read_file_full(manifest_path);
    if (!json){ fprintf(stderr,"Cannot read %s\n",manifest_path); return 2; }

    int errors=0;

    /* Check schema_version */
    char sv[64];
    if (json_get_str(json,"schema_version",sv,sizeof(sv))) {
        if (strcmp(sv,"1.0.0")!=0){
            fprintf(stderr,"  schema_version must be '1.0.0', got '%s'\n",sv);
            errors++;
        }
    }

    /* Check atoms array exists */
    const char *atoms_arr=json_get_array(json,"atoms");
    if (!atoms_arr){ fprintf(stderr,"  Missing 'atoms' array\n"); errors++; }

    /* Check operators array exists */
    const char *ops_arr=json_get_array(json,"operators");
    if (!ops_arr){ fprintf(stderr,"  Missing 'operators' array\n"); errors++; }

    /* Count atoms, check for duplicates */
    int atom_count=0;
    char atom_ids[MAX_IDS][256];
    if (atoms_arr) {
        const char *cursor=atoms_arr;
        char objbuf[MAX_LINE];
        while (json_next_object(&cursor,objbuf,sizeof(objbuf))) {
            char aid[256];
            if (json_get_str(objbuf,"atom_id",aid,sizeof(aid))) {
                /* Check duplicate */
                for (int j=0;j<atom_count;j++){
                    if (strcmp(atom_ids[j],aid)==0){
                        fprintf(stderr,"  Duplicate atom_id: %s\n",aid);
                        errors++;
                    }
                }
                if (atom_count<MAX_IDS)
                    snprintf(atom_ids[atom_count++],256,"%s",aid);
            } else {
                fprintf(stderr,"  atoms[%d]: missing atom_id\n",atom_count);
                errors++;
            }
            /* Check required fields */
            char tmp[256];
            if (!json_get_str(objbuf,"content_hash",tmp,sizeof(tmp))){
                fprintf(stderr,"  atoms[%d]: missing content_hash\n",atom_count-1);
                errors++;
            }
            if (!json_get_str(objbuf,"media_type",tmp,sizeof(tmp))){
                fprintf(stderr,"  atoms[%d]: missing media_type\n",atom_count-1);
                errors++;
            }
        }
    }

    /* Count operators, check duplicates and references */
    int op_count=0;
    char op_ids[MAX_IDS][256];
    if (ops_arr) {
        const char *cursor=ops_arr;
        char objbuf[MAX_LINE];
        while (json_next_object(&cursor,objbuf,sizeof(objbuf))) {
            char oid[256];
            if (json_get_str(objbuf,"operator_id",oid,sizeof(oid))) {
                for (int j=0;j<op_count;j++){
                    if (strcmp(op_ids[j],oid)==0){
                        fprintf(stderr,"  Duplicate operator_id: %s\n",oid);
                        errors++;
                    }
                }
                if (op_count<MAX_IDS)
                    snprintf(op_ids[op_count++],256,"%s",oid);
            } else {
                fprintf(stderr,"  operators[%d]: missing operator_id\n",op_count);
                errors++;
            }
            char tmp[256];
            if (!json_get_str(objbuf,"op",tmp,sizeof(tmp))){
                fprintf(stderr,"  operators[%d]: missing op\n",op_count-1);
                errors++;
            }
        }
    }

    if (errors) printf("INVALID — %d error(s)\n",errors);
    else printf("VALID\n");

    free(json);
    return errors?1:0;
}

/* ── Merkle hash computation over JSON manifest ──────────────────────── */

static int cmd_merkle(const char *manifest_path) {
    char *json=read_file_full(manifest_path);
    if (!json){ fprintf(stderr,"Cannot read %s\n",manifest_path); return 2; }

    /* Build hash map: id -> hash */
    char ids[MAX_IDS][256];
    char hashes[MAX_IDS][65];
    int nids=0;

    /* Seed with atom content_hashes */
    const char *atoms_arr=json_get_array(json,"atoms");
    if (atoms_arr) {
        const char *cursor=atoms_arr;
        char objbuf[MAX_LINE];
        while (json_next_object(&cursor,objbuf,sizeof(objbuf)) && nids<MAX_IDS) {
            char aid[256], ahash[65];
            if (json_get_str(objbuf,"atom_id",aid,sizeof(aid)) &&
                json_get_str(objbuf,"content_hash",ahash,sizeof(ahash))) {
                snprintf(ids[nids],256,"%s",aid);
                snprintf(hashes[nids],65,"%s",ahash);
                nids++;
            }
        }
    }

    /* Parse operators */
    typedef struct { char id[256]; char op[256]; char params[MAX_LINE]; char version[64]; char inputs[MAX_IDS][256]; int ninputs; int computed; char hash[65]; } OpInfo;
    const char *ops_arr=json_get_array(json,"operators");
    OpInfo *ops=NULL;
    int nops=0;
    if (ops_arr) {
        /* Count first */
        int cnt=json_array_count(ops_arr);
        ops=calloc(cnt,sizeof(OpInfo));
        const char *cursor=ops_arr;
        char objbuf[MAX_LINE];
        while (json_next_object(&cursor,objbuf,sizeof(objbuf)) && nops<cnt) {
            OpInfo *o=&ops[nops];
            json_get_str(objbuf,"operator_id",o->id,sizeof(o->id));
            json_get_str(objbuf,"op",o->op,sizeof(o->op));
            if (!json_get_str(objbuf,"version",o->version,sizeof(o->version)))
                snprintf(o->version,sizeof(o->version),"");
            /* Get params as raw JSON */
            const char *pstart=strstr(objbuf,"\"params\"");
            if (pstart) {
                pstart=strchr(pstart,':');
                if (pstart) {
                    pstart=skip_ws(pstart+1);
                    const char *pend=skip_value(pstart);
                    size_t plen=(size_t)(pend-pstart);
                    if (plen>=sizeof(o->params)) plen=sizeof(o->params)-1;
                    memcpy(o->params,pstart,plen); o->params[plen]='\0';
                }
            }
            if (!o->params[0]) snprintf(o->params,sizeof(o->params),"{}");
            /* Parse inputs */
            const char *iarr=json_get_array(objbuf,"inputs");
            if (iarr) {
                const char *ip=skip_ws(iarr+1);
                while (*ip && *ip!=']' && o->ninputs<MAX_IDS) {
                    if (*ip=='"') {
                        ip++;
                        size_t k=0;
                        while (*ip && *ip!='"' && k<255) o->inputs[o->ninputs][k++]=*ip++;
                        o->inputs[o->ninputs][k]='\0';
                        if (*ip=='"') ip++;
                        o->ninputs++;
                    }
                    if (*ip==',') ip++;
                    ip=skip_ws(ip);
                }
            }
            nops++;
        }
    }

    /* Topological sort: iterate until stable */
    int max_iter=nops+1;
    for (int iter=0;iter<max_iter;iter++) {
        int progress=0;
        for (int i=0;i<nops;i++) {
            if (ops[i].computed) continue;
            /* Check all inputs resolved */
            int ready=1;
            const char *input_hashes[MAX_IDS];
            for (int j=0;j<ops[i].ninputs;j++) {
                int found=0;
                for (int k=0;k<nids;k++) {
                    if (strcmp(ids[k],ops[i].inputs[j])==0){
                        input_hashes[j]=hashes[k];
                        found=1; break;
                    }
                }
                if (!found){ ready=0; break; }
            }
            if (!ready) continue;
            compute_node_hash(ops[i].op,ops[i].params,input_hashes,ops[i].ninputs,ops[i].version,ops[i].hash);
            ops[i].computed=1;
            /* Add to id→hash map */
            if (nids<MAX_IDS){
                snprintf(ids[nids],256,"%s",ops[i].id);
                snprintf(hashes[nids],65,"%s",ops[i].hash);
                nids++;
            }
            progress=1;
        }
        if (!progress) break;
    }

    /* Compute root hash = SHA-256 of sorted operator hashes */
    /* Collect computed hashes */
    char *op_hashes[MAX_IDS];
    int ncomp=0;
    for (int i=0;i<nops;i++)
        if (ops[i].computed) op_hashes[ncomp++]=ops[i].hash;
    /* Sort */
    for (int i=0;i<ncomp-1;i++)
        for (int j=i+1;j<ncomp;j++)
            if (strcmp(op_hashes[i],op_hashes[j])>0){ char *t=op_hashes[i]; op_hashes[i]=op_hashes[j]; op_hashes[j]=t; }
    /* Build canonical array and hash */
    char root_input[MAX_LINE];
    int ri_off=0;
    ri_off+=snprintf(root_input,sizeof(root_input),"[");
    for (int i=0;i<ncomp;i++){
        if (i) ri_off+=snprintf(root_input+ri_off,sizeof(root_input)-ri_off,",");
        ri_off+=snprintf(root_input+ri_off,sizeof(root_input)-ri_off,"\"%s\"",op_hashes[i]);
    }
    ri_off+=snprintf(root_input+ri_off,sizeof(root_input)-ri_off,"]");
    char root_hash[65];
    if (ncomp>0) sha256_hex((const unsigned char*)root_input,(size_t)ri_off,root_hash);
    else root_hash[0]='\0';

    printf("Merkle root: %s\n",root_hash[0]?root_hash:"(none)");
    for (int i=0;i<nops;i++){
        if (ops[i].computed)
            printf("  %s: %s\n",ops[i].id,ops[i].hash);
        else
            printf("  %s: UNRESOLVED (missing inputs)\n",ops[i].id);
    }

    free(ops);
    free(json);
    return 0;
}

static int cmd_status(const char *db_path) {
    sqlite3 *db;
    if (sqlite3_open(db_path,&db)!=SQLITE_OK){
        fprintf(stderr,"Cannot open %s\n",db_path); return 1;
    }
    sqlite3_stmt *st;
    int atoms=0,ops=0,reals=0;
    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM atoms",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) atoms=sqlite3_column_int(st,0);
    sqlite3_finalize(st);
    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM operators",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) ops=sqlite3_column_int(st,0);
    sqlite3_finalize(st);
    sqlite3_prepare_v2(db,"SELECT COUNT(*) FROM realizations",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) reals=sqlite3_column_int(st,0);
    sqlite3_finalize(st);
    sqlite3_close(db);
    printf("Graph: %s\n",db_path);
    printf("  Atoms:        %d\n",atoms);
    printf("  Operators:    %d\n",ops);
    printf("  Realizations: %d\n",reals);
    printf("  Total nodes:  %d\n",atoms+ops+reals);
    return 0;
}

static void usage(void) {
    fprintf(stderr,
        "BonfyreGraph — Merkle-DAG artifact graph engine\n\n"
        "Usage:\n"
        "  bonfyre-graph init <db>\n"
        "  bonfyre-graph add-atom <db> --id ID --hash HASH --type TYPE [--path PATH]\n"
        "  bonfyre-graph add-op <db> --id ID --op OP --inputs A,B --output OUT [--params '{}'] [--version V]\n"
        "  bonfyre-graph validate <manifest.json>\n"
        "  bonfyre-graph merkle <manifest.json>\n"
        "  bonfyre-graph lineage <db> --id ID\n"
        "  bonfyre-graph export <db> [--out artifact.json]\n"
        "  bonfyre-graph status <db>\n");
}

/* Simple arg helper: find --flag value */
static const char *arg_get(int argc, char **argv, const char *flag) {
    for (int i=0;i<argc-1;i++)
        if (strcmp(argv[i],flag)==0) return argv[i+1];
    return NULL;
}

int main(int argc, char **argv) {
    if (argc<2){ usage(); return 1; }
    const char *cmd=argv[1];

    if (strcmp(cmd,"init")==0) {
        if (argc<3){ fprintf(stderr,"Usage: bonfyre-graph init <db>\n"); return 1; }
        return cmd_init(argv[2]);
    }
    if (strcmp(cmd,"add-atom")==0) {
        if (argc<3){ fprintf(stderr,"Usage: bonfyre-graph add-atom <db> --id .. --hash .. --type ..\n"); return 1; }
        const char *id=arg_get(argc,argv,"--id");
        const char *hash=arg_get(argc,argv,"--hash");
        const char *type=arg_get(argc,argv,"--type");
        const char *path=arg_get(argc,argv,"--path");
        if (!id||!hash||!type){ fprintf(stderr,"Missing required --id, --hash, --type\n"); return 1; }
        return cmd_add_atom(argv[2],id,hash,type,path);
    }
    if (strcmp(cmd,"add-op")==0) {
        if (argc<3){ fprintf(stderr,"Usage: bonfyre-graph add-op <db> --id .. --op .. --inputs .. --output ..\n"); return 1; }
        const char *id=arg_get(argc,argv,"--id");
        const char *op=arg_get(argc,argv,"--op");
        const char *inputs=arg_get(argc,argv,"--inputs");
        const char *output=arg_get(argc,argv,"--output");
        const char *params=arg_get(argc,argv,"--params");
        const char *version=arg_get(argc,argv,"--version");
        if (!id||!op||!inputs||!output){ fprintf(stderr,"Missing required --id, --op, --inputs, --output\n"); return 1; }
        return cmd_add_op(argv[2],id,op,inputs,output,params,version);
    }
    if (strcmp(cmd,"validate")==0) {
        if (argc<3){ fprintf(stderr,"Usage: bonfyre-graph validate <manifest.json>\n"); return 1; }
        return cmd_validate(argv[2]);
    }
    if (strcmp(cmd,"merkle")==0) {
        if (argc<3){ fprintf(stderr,"Usage: bonfyre-graph merkle <manifest.json>\n"); return 1; }
        return cmd_merkle(argv[2]);
    }
    if (strcmp(cmd,"lineage")==0) {
        if (argc<3){ fprintf(stderr,"Usage: bonfyre-graph lineage <db> --id ID\n"); return 1; }
        const char *id=arg_get(argc,argv,"--id");
        if (!id){ fprintf(stderr,"Missing --id\n"); return 1; }
        return cmd_lineage(argv[2],id);
    }
    if (strcmp(cmd,"export")==0) {
        if (argc<3){ fprintf(stderr,"Usage: bonfyre-graph export <db> [--out file.json]\n"); return 1; }
        const char *out=arg_get(argc,argv,"--out");
        if (!out) out="artifact.json";
        return cmd_export(argv[2],out);
    }
    if (strcmp(cmd,"status")==0) {
        if (argc<3){ fprintf(stderr,"Usage: bonfyre-graph status <db>\n"); return 1; }
        return cmd_status(argv[2]);
    }

    fprintf(stderr,"Unknown command: %s\n",cmd);
    usage();
    return 1;
}
