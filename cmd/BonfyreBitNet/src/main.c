/*
 * BonfyreBitNet — bounded learned reordering for AddressPlane survivors.
 *
 * The input is the already mask-filtered output of AddressPlane. This process
 * never queries a broader candidate population, changes eligibility, invokes a
 * lowerer, or reads owner facts into the CandidateAdvertisement. It projects
 * each surviving descriptor into a model-passported latent vector, then emits
 * every input handle in a new order with its original verifier/lowerer intact.
 */
#define _POSIX_C_SOURCE 200809L
#include <curl/curl.h>
#include <math.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <bonfyre.h>
#include <bf_admission.h>

typedef struct { char *data; size_t len, cap; } Buffer;
typedef struct {
    char *identity, *kind, *owner, *verifier, *lowerer, *descriptor;
    float *vector, score;
} Candidate;

static void usage(void) {
    fputs("usage: bonfyre-bitnet reorder --address-db INDEX --forward-result JSON --model-db MODELS --representation ID --scope S (--endpoint URL | --bitnet-cpp BIN --bitnet-model GGUF) --query TEXT --out DB --admission-db DB --admission-volume V --admission-plane P --admission-estimated-bytes N [--admission-free-bytes N] [--admission-protected-floor-bytes N] [--admission-per-plane-quota-bytes N] [--admission-max-grant-bytes N] [--threads N] [--candidate-batch N] [--top-k N] [--offset N] [--handoff-dir DIR] [--diagnostic]\n       bonfyre-bitnet expand --rank-db DB --run-id ID --model-db MODELS --representation ID --scope S --handoff-dir DIR [--top-k N] [--offset N]\n", stderr);
    fputs("       bonfyre-bitnet evidence --rank-db DB --run-id ID [--out JSONL]\n", stderr);
    fputs("       bonfyre-bitnet evaluate-retention --rank-db DB --run-id ID --exact-owner-evidence JSONL --out JSON\n", stderr);
}
static void candidate_free(Candidate *c) {
    if (!c) return;
    free(c->identity); free(c->kind); free(c->owner); free(c->verifier);
    free(c->lowerer); free(c->descriptor); free(c->vector); memset(c, 0, sizeof(*c));
}
static size_t write_cb(char *ptr, size_t size, size_t nmemb, void *opaque) {
    Buffer *b = opaque; size_t n = size * nmemb, need = b->len + n + 1;
    if (need > b->cap) { size_t cap = b->cap ? b->cap : 4096; while (cap < need) cap *= 2; char *next = realloc(b->data, cap); if (!next) return 0; b->data = next; b->cap = cap; }
    memcpy(b->data + b->len, ptr, n); b->len += n; b->data[b->len] = '\0'; return n;
}
static int json_append(Buffer *b, const char *s) {
    const unsigned char *p = (const unsigned char *)(s ? s : "");
    if (write_cb("\"",1,1,b)!=1) return 0;
    for (; *p; p++) {
        char esc[7]; const char *out = NULL; size_t n = 0;
        if (*p == '"') out = "\\\""; else if (*p == '\\') out = "\\\\"; else if (*p == '\n') out = "\\n"; else if (*p == '\r') out = "\\r"; else if (*p == '\t') out = "\\t";
        if (out) { n = strlen(out); } else if (*p < 0x20) { snprintf(esc, sizeof(esc), "\\u%04x", *p); out = esc; n = 6; } else { esc[0] = (char)*p; esc[1] = 0; out = esc; n = 1; }
        if (write_cb((char *)out,1,n,b) != n) return 0;
    }
    return write_cb("\"",1,1,b)==1;
}
static void json_text(const char *s) {
    Buffer b = {0};
    if (json_append(&b, s ? s : "")) fputs(b.data, stdout);
    free(b.data);
}
static int json_file_text(FILE *out, const char *s) {
    Buffer b = {0};
    int ok = json_append(&b, s ? s : "") && fputs(b.data, out) >= 0;
    free(b.data);
    return ok;
}
/* All embedding transports converge here.  In particular, reject NaN and
 * infinity rather than allowing a broken numerical kernel to turn into a
 * plausible-looking score. */
static int parse_vector(const char *body, float **out, int *dims) {
    char *end = NULL, *p = (char *)body;
    int n = 0, cap = 1024;
    float *v;
    if (!body || !out || !dims) return 0;
    v = malloc((size_t)cap * sizeof(*v));
    if (!v) return 0;
    while (*p) {
        float x;
        while (*p && !((*p >= '0' && *p <= '9') || *p == '-' || *p == '+')) p++;
        if (!*p) break;
        x = strtof(p, &end);
        if (end == p || !isfinite(x)) { free(v); return 0; }
        if (n == cap) {
            float *next;
            cap *= 2;
            next = realloc(v, (size_t)cap * sizeof(*v));
            if (!next) { free(v); return 0; }
            v = next;
        }
        v[n++] = x;
        p = end;
    }
    if (n < 1 || (*dims && n != *dims)) { free(v); return 0; }
    *dims = n;
    *out = v;
    return 1;
}
static int parse_array_embeddings(const char *json, int count, float ***vectors, int *dims) {
    sqlite3 *mem = NULL;
    sqlite3_stmt *rows = NULL;
    float **out = NULL;
    int got = 0, ok = 0;
    if (!json || count < 1 || !vectors || !dims) return 0;
    if (sqlite3_open(":memory:", &mem) != SQLITE_OK ||
        sqlite3_prepare_v2(mem, "SELECT CAST(key AS INTEGER),value FROM json_each(?1) ORDER BY CAST(key AS INTEGER)", -1, &rows, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(rows, 1, json, -1, SQLITE_TRANSIENT);
    out = calloc((size_t)count, sizeof(*out));
    if (!out) goto done;
    while (sqlite3_step(rows) == SQLITE_ROW) {
        int idx = sqlite3_column_int(rows, 0);
        const char *body = (const char *)sqlite3_column_text(rows, 1);
        if (idx < 0 || idx >= count || out[idx] || !parse_vector(body, &out[idx], dims)) goto done;
        got++;
    }
    if (got != count) goto done;
    *vectors = out;
    out = NULL;
    ok = 1;
done:
    if (rows) sqlite3_finalize(rows);
    if (mem) sqlite3_close(mem);
    if (out) { for (int i = 0; i < count; i++) free(out[i]); free(out); }
    return ok;
}
static int http_embeddings(const char *endpoint, char **texts, int count, float ***vectors, int *dims) {
    CURL *curl = NULL; struct curl_slist *headers = NULL; Buffer req={0}, response={0}; char *url = NULL; CURLcode rc; int ok = 0;
    sqlite3 *mem = NULL; sqlite3_stmt *rows = NULL; float **out = NULL; int got = 0;
    if (!endpoint || !texts || count < 1 || !vectors || !dims) return 0;
    if (write_cb("{\"input\":[",1,10,&req)!=10) goto done;
    for (int i=0;i<count;i++) { if (i && write_cb(",",1,1,&req)!=1) goto done; if (!json_append(&req,texts[i])) goto done; }
    if (write_cb("]}",1,2,&req)!=2) goto done;
    url = malloc(strlen(endpoint)+16); if (!url) goto done; snprintf(url, strlen(endpoint)+16, "%s/v1/embeddings", endpoint);
    curl = curl_easy_init(); if (!curl) goto done;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_URL, url); curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, req.data); curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)req.len);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb); curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 5000L); curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 120000L);
    rc = curl_easy_perform(curl); if (rc != CURLE_OK) goto done;
    { long code=0; curl_easy_getinfo(curl,CURLINFO_RESPONSE_CODE,&code); if (code != 200) goto done; }
    if (sqlite3_open(":memory:",&mem)!=SQLITE_OK) goto done;
    if (sqlite3_prepare_v2(mem,"SELECT json_extract(value,'$.index'),json_extract(value,'$.embedding') FROM json_each(?1,'$.data') ORDER BY json_extract(value,'$.index')",-1,&rows,NULL)!=SQLITE_OK) goto done;
    sqlite3_bind_text(rows,1,response.data,-1,SQLITE_TRANSIENT); out=calloc((size_t)count,sizeof(*out)); if(!out)goto done;
    while (sqlite3_step(rows)==SQLITE_ROW) {
        const char *body=(const char *)sqlite3_column_text(rows,1); int idx=sqlite3_column_int(rows,0);
        if (!body || idx < 0 || idx >= count || out[idx] || !parse_vector(body, &out[idx], dims)) goto done;
        got++;
    }
    if (got != count) goto done;
    *vectors=out; out=NULL; ok=1;
done:
    if(rows)sqlite3_finalize(rows); if(mem)sqlite3_close(mem); if(curl)curl_easy_cleanup(curl); if(headers)curl_slist_free_all(headers);
    if(out){for(int i=0;i<count;i++)free(out[i]);free(out);} free(req.data);free(response.data);free(url);return ok;
}
/* This is deliberately not an HTTP compatibility facade.  The direct path
 * invokes the configured bitnet.cpp llama-embedding binary without a shell,
 * feeds it only the already AddressPlane-bounded descriptor text, and accepts
 * only finite array output.  It is a cold CLI phenotype for now; callers must
 * name the binary and GGUF explicitly so a passport can bind the run. */
/* The embedding thread count.
 *
 * This was pinned at 1, which is the safe number to write when nobody has
 * measured. Measured on this host over a real 92-candidate AddressPlane
 * frontier: -t 1 is 1.596s per candidate, -t 4 is 0.641s, -t 8 is 0.736s. Four
 * is 2.5x faster than one and eight is slower than four, because the machine
 * has four performance cores and oversubscribing them costs more than it buys.
 *
 * The default is the measured optimum rather than a guess in either direction,
 * and it is overridable because the next host will have a different answer.
 * Cannot infer: a faster embedding -> a better ranking. This changes how long
 * the model takes to say the same thing. */
#define BITNET_DEFAULT_THREADS "4"

static int bitnet_cpp_embeddings(const char *binary, const char *model,
                                 char **texts, int count,
                                 float ***vectors, int *dims, const char *threads) {
    static const char separator[] = "<#bonfyre-candidate-separator#>";
    Buffer prompt = {0}, response = {0};
    int pipefd[2] = {-1, -1}, status = 0, ok = 0;
    pid_t pid;
    if (!binary || !model || !texts || count < 1 || !vectors || !dims ||
        access(binary, X_OK) || access(model, R_OK)) return 0;
    for (int i = 0; i < count; i++) {
        size_t len;
        if (!texts[i] || strstr(texts[i], separator)) goto done;
        len = strlen(texts[i]);
        if (prompt.len + len + sizeof(separator) > 65536U ||
            write_cb(texts[i], 1, len, &prompt) != len ||
            (i + 1 < count && write_cb((char *)separator, 1, sizeof(separator) - 1, &prompt) != sizeof(separator) - 1)) goto done;
    }
    if (pipe(pipefd)) goto done;
    pid = fork();
    if (pid < 0) goto done;
    if (pid == 0) {
        char *args[] = {
            (char *)binary, "-ngl", "0", "-dev", "none", "--no-op-offload",
            "-m", (char *)model, "-p", prompt.data,
            "--embd-separator", (char *)separator,
            "--pooling", "last", "--attention", "causal",
            "--embd-normalize", "2", "--embd-output-format", "array",
            "-t", (char *)(threads && *threads ? threads : BITNET_DEFAULT_THREADS), NULL
        };
        /* A locally-built bitnet.cpp tree keeps its shared llama runtime beside
         * llama-embedding.  Preserve an explicit caller-provided library path,
         * but make that direct, content-addressed execution reproducible when
         * the binary and its sibling dylibs have not been installed globally.
         * This is physical loading only: it cannot alter the model passport,
         * AddressPlane frontier, gate, or lowerer law. */
#ifdef __APPLE__
        if (!getenv("DYLD_LIBRARY_PATH")) {
            const char *slash = strrchr(binary, '/');
            if (slash) {
                char sibling_libs[PATH_MAX];
                size_t dir_len = (size_t)(slash - binary);
                if (dir_len < sizeof(sibling_libs)) {
                    memcpy(sibling_libs, binary, dir_len);
                    sibling_libs[dir_len] = '\0';
                    (void)setenv("DYLD_LIBRARY_PATH", sibling_libs, 0);
                }
            }
        }
#endif
        close(pipefd[0]);
        if (dup2(pipefd[1], STDOUT_FILENO) < 0) _exit(127);
        close(pipefd[1]);
        execv(binary, args);
        _exit(127);
    }
    close(pipefd[1]); pipefd[1] = -1;
    for (;;) {
        char block[8192];
        ssize_t n = read(pipefd[0], block, sizeof(block));
        if (n == 0) break;
        if (n < 0 || write_cb(block, 1, (size_t)n, &response) != (size_t)n) goto done;
    }
    close(pipefd[0]); pipefd[0] = -1;
    if (waitpid(pid, &status, 0) != pid || !WIFEXITED(status) || WEXITSTATUS(status) != 0) goto done;
    ok = parse_array_embeddings(response.data, count, vectors, dims);
done:
    if (pipefd[0] >= 0) close(pipefd[0]);
    if (pipefd[1] >= 0) close(pipefd[1]);
    free(prompt.data); free(response.data);
    return ok;
}
static int gate(sqlite3 *db,const char *id,const char *scope,int diagnostic,char state[16]) {
    sqlite3_stmt *s=NULL; const char *measurement,*admission,*declared; int ok=0;
    if(!db||!id||!scope)return 0;
    if(sqlite3_prepare_v2(db,"SELECT measurement_state,admission_state,admission_scope FROM model_representation_passport WHERE representation_id=?",-1,&s,NULL)!=SQLITE_OK)goto out;
    sqlite3_bind_text(s,1,id,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(s)!=SQLITE_ROW)goto out; measurement=(const char*)sqlite3_column_text(s,0);admission=(const char*)sqlite3_column_text(s,1);declared=(const char*)sqlite3_column_text(s,2);
    if(!measurement||!admission||!declared||strcmp(measurement,"measured")||strcmp(declared,scope))goto out;
    if(!strcmp(admission,"ADMIT")){snprintf(state,16,"ADMIT");ok=1;goto out;}
    if(diagnostic&&!strcmp(admission,"DEFER")){snprintf(state,16,"SHADOW");ok=1;}
out: if(s)sqlite3_finalize(s);return ok;
}
/* The passport says which representation has been measured for this operation.
 * It says nothing about whether this machine may spend memory/disk pressure on
 * the run now.  Keep that ephemeral physical question in ResourceAdmission,
 * release the grant after the derived frontier has been persisted, and never
 * turn either answer into selection or an owner fact. */
static int acquire_admission(const char *db_path, const char *volume,
                             const char *plane, const char *estimate_text,
                             const char *free_text, const char *floor_text,
                             const char *quota_text, const char *max_text,
                             const char *representation, BfAdmissionGrants **out,
                             int64_t *grant_id) {
    BfAdmissionPolicy policy;
    BfResourceRequest request;
    BfAdmissionDecision decision;
    BfAdmissionVerdict verdict;
    char *end = NULL;
    int64_t estimate, free_bytes;
    if (!db_path || !*db_path || !volume || !*volume || !plane || !*plane ||
        !estimate_text || !*estimate_text || !out || !grant_id) return 0;
    estimate = strtoll(estimate_text, &end, 10);
    if (!end || *end || estimate <= 0 || bf_admission_open(db_path, out, stderr) != 0) return 0;
    bf_admission_policy_defaults(&policy);
    if (floor_text && *floor_text) policy.protected_floor_bytes = strtoll(floor_text, NULL, 10);
    if (quota_text && *quota_text) policy.per_plane_quota_bytes = strtoll(quota_text, NULL, 10);
    if (max_text && *max_text) policy.max_grant_bytes = strtoll(max_text, NULL, 10);
    free_bytes = free_text && *free_text ? strtoll(free_text, &end, 10) : bf_admission_disk_free(db_path);
    if ((!free_text || !*free_text) ? free_bytes < 0 : (!end || *end || free_bytes < 0)) {
        bf_admission_close(*out); *out = NULL; return 0;
    }
    memset(&request, 0, sizeof(request));
    request.plane = plane;
    request.kind = "bitnet_embedding_rerank";
    request.estimated_bytes = estimate;
    request.volume = volume;
    request.detail = representation;
    verdict = bf_admission_request_grant(*out, &request, &policy, free_bytes,
                                         NULL, &decision, grant_id);
    if (verdict != BF_ADMISSION_ADMIT) {
        fprintf(stderr, "BitNet ResourceAdmission %s: %s\n",
                bf_admission_verdict_name(verdict), decision.reason);
        bf_admission_close(*out); *out = NULL; *grant_id = 0; return 0;
    }
    return 1;
}
static int descriptor(sqlite3 *db, Candidate *c) {
    sqlite3_stmt *s=NULL,*terms=NULL; Buffer b={0}; int ok=0;
    if(sqlite3_prepare_v2(db,"SELECT candidate_kind,owner_ref,exact_verifier,lowerer_ref FROM candidate_advertisement WHERE identity=?",-1,&s,NULL)!=SQLITE_OK)goto out;
    sqlite3_bind_text(s,1,c->identity,-1,SQLITE_TRANSIENT);if(sqlite3_step(s)!=SQLITE_ROW)goto out;
    c->kind=strdup((const char*)sqlite3_column_text(s,0));c->owner=strdup((const char*)sqlite3_column_text(s,1));c->verifier=strdup((const char*)sqlite3_column_text(s,2));c->lowerer=strdup((const char*)sqlite3_column_text(s,3));
    if(!c->kind||!c->owner||!c->verifier||!c->lowerer)goto out;
    /* Keep the query-side task instruction and the candidate-side canonical
     * document envelope separate.  The latter still contains only forwarding
     * coordinates and declared refs; it never reads owner facts or invents a
     * semantic match.  In particular, the Gemma-derived 270M BitNet GGUF can
     * produce non-finite last-token vectors for a bare leading `external:`
     * coordinate, while this stable document envelope yields a finite vector.
     * parse_vector remains the fail-closed guard for every runtime/model pair. */
    if(write_cb("document: ",1,10,&b)!=10||
       write_cb(c->identity,1,strlen(c->identity),&b)!=strlen(c->identity)||
       write_cb(" ",1,1,&b)!=1||write_cb(c->kind,1,strlen(c->kind),&b)!=strlen(c->kind))goto out;
    if(sqlite3_prepare_v2(db,"SELECT axis,term FROM candidate_advertisement_term WHERE identity=? ORDER BY axis,term",-1,&terms,NULL)!=SQLITE_OK)goto out;
    sqlite3_bind_text(terms,1,c->identity,-1,SQLITE_TRANSIENT);while(sqlite3_step(terms)==SQLITE_ROW){const char*a=(const char*)sqlite3_column_text(terms,0),*t=(const char*)sqlite3_column_text(terms,1);if(write_cb(" ",1,1,&b)!=1||write_cb((char *)a,1,strlen(a),&b)!=strlen(a)||write_cb(":",1,1,&b)!=1||write_cb((char *)t,1,strlen(t),&b)!=strlen(t))goto out;}
    c->descriptor=b.data;b.data=NULL;ok=1;
out:if(s)sqlite3_finalize(s);if(terms)sqlite3_finalize(terms);free(b.data);return ok;
}
static int load_survivors(sqlite3 *address,const char *path,Candidate **out,int *count) {
    FILE *f=NULL;long n;char *body=NULL;sqlite3 *mem=NULL;sqlite3_stmt*s=NULL;Candidate*cs=NULL;int cap=0,used=0,ok=0;
    if(!(f=fopen(path,"rb")))goto done;if(fseek(f,0,SEEK_END)||(n=ftell(f))<0||fseek(f,0,SEEK_SET))goto done;body=malloc((size_t)n+1);if(!body||fread(body,1,(size_t)n,f)!=(size_t)n)goto done;body[n]=0;
    if(sqlite3_open(":memory:",&mem)!=SQLITE_OK||sqlite3_prepare_v2(mem,"SELECT json_extract(value,'$.identity') FROM json_each(?1,'$.survivors')",-1,&s,NULL)!=SQLITE_OK)goto done;sqlite3_bind_text(s,1,body,n,SQLITE_TRANSIENT);
    while(sqlite3_step(s)==SQLITE_ROW){const char*id=(const char*)sqlite3_column_text(s,0);if(!id||!*id)goto done;if(used==cap){int next=cap?cap*2:32;Candidate*x=realloc(cs,(size_t)next*sizeof(*x));if(!x)goto done;cs=x;memset(cs+cap,0,(size_t)(next-cap)*sizeof(*cs));cap=next;}cs[used].identity=strdup(id);if(!cs[used].identity||!descriptor(address,&cs[used]))goto done;used++;}
    if(!used)goto done;*out=cs;*count=used;cs=NULL;ok=1;
done:if(s)sqlite3_finalize(s);if(mem)sqlite3_close(mem);if(f)fclose(f);free(body);if(cs){for(int i=0;i<cap;i++)candidate_free(&cs[i]);free(cs);}return ok;
}
/* Where embeddings come from.
 *
 * The usage string has always offered two sources -- an HTTP endpoint or the
 * bitnet.cpp binary directly -- and only the first was reachable. The direct
 * path was written, compiled and never called, which -Wunused-function turns
 * into a build failure the moment anything else in the file moves. So the organ
 * could only rank against a running server, and the documented offline path was
 * documentation.
 *
 * Cannot infer: an embedding source -> an admitted representation. This chooses
 * how vectors are produced; the ModelRepresentationPassport still decides
 * whether they may be used for anything but shadow. */
static const char *g_cpp_binary = NULL, *g_cpp_model = NULL, *g_threads = NULL;

static int embed(const char *endpoint, char **texts, int count, float ***vectors, int *dims) {
    if (endpoint) return http_embeddings(endpoint, texts, count, vectors, dims);
    return bitnet_cpp_embeddings(g_cpp_binary, g_cpp_model, texts, count, vectors, dims, g_threads);
}

static int cmp_candidate(const void *a,const void*b){const Candidate*x=a,*y=b;return x->score<y->score?1:x->score>y->score?-1:strcmp(x->identity,y->identity);}
/* A rank handoff has exactly the AddressPlane lowerer envelope shape.  The
 * extra rank fields are trace metadata only; existing exact consumers can
 * ignore them.  The full ordered frontier stays in the derived run table, so
 * a refusal can request the next bounded slice instead of claiming a top-k
 * omission proved a candidate impossible. */
static int write_handoff(const char *root, const char *run, const Candidate *c, int rank, int total) {
    char lane[128],dir[1024],tmp[1200],path[1200]; Buffer b={0}; int fd=-1,ok=0;
    if(!root||!*root||!c||!c->lowerer)return 0;
    for(size_t i=0;i+1<sizeof(lane)&&c->lowerer[i];i++){unsigned char x=(unsigned char)c->lowerer[i];lane[i]=(x==':'||x=='/'||x==' ') ? '-' : ((x>='a'&&x<='z')||(x>='A'&&x<='Z')||(x>='0'&&x<='9')||x=='-'||x=='_'?(char)x:'_');lane[i+1]=0;}
    if(mkdir(root,0750)&&access(root,F_OK))return 0;if(snprintf(dir,sizeof(dir),"%s/%s",root,lane)>=(int)sizeof(dir)||(mkdir(dir,0750)&&access(dir,F_OK)))return 0;
    if(write_cb("{\"schema\":",1,10,&b)!=10||!json_append(&b,"bonfyre-lowering-handoff.v1"))goto out;
    { const char *x=",\"delta_id\":"; if(write_cb((char *)x,1,strlen(x),&b)!=strlen(x)||!json_append(&b,run))goto out;
      x=",\"identity\":"; if(write_cb((char *)x,1,strlen(x),&b)!=strlen(x)||!json_append(&b,c->identity))goto out;
      x=",\"candidate_kind\":"; if(write_cb((char *)x,1,strlen(x),&b)!=strlen(x)||!json_append(&b,c->kind))goto out;
      x=",\"owner_ref\":"; if(write_cb((char *)x,1,strlen(x),&b)!=strlen(x)||!json_append(&b,c->owner))goto out;
      x=",\"lowerer_ref\":"; if(write_cb((char *)x,1,strlen(x),&b)!=strlen(x)||!json_append(&b,c->lowerer))goto out;
      x=",\"truth\":\"dependency_cut_only\",\"rank\":"; if(write_cb((char *)x,1,strlen(x),&b)!=strlen(x))goto out; }
    {char num[64];int n=snprintf(num,sizeof(num),"%d,\"frontier_size\":%d}",rank,total);if(n<0||write_cb(num,1,(size_t)n,&b)!=(size_t)n)goto out;}
    if(snprintf(tmp,sizeof(tmp),"%s/.bitnet-XXXXXX",dir)>=(int)sizeof(tmp)||snprintf(path,sizeof(path),"%s/%04d-%s.json",dir,rank,c->identity)>=(int)sizeof(path))goto out;
    for(char *p=path+strlen(dir)+1;*p;p++)if(!((*p>='a'&&*p<='z')||(*p>='A'&&*p<='Z')||(*p>='0'&&*p<='9')||*p=='-'||*p=='_'||*p=='.'))*p='_';
    fd=mkstemp(tmp);if(fd<0||write(fd,b.data,b.len)!=(ssize_t)b.len||fsync(fd)||close(fd)){if(fd>=0)close(fd);unlink(tmp);goto out;}fd=-1;if(rename(tmp,path))goto out;ok=1;
out:if(fd>=0)close(fd);free(b.data);return ok;
}
static int persist(sqlite3 *db,const char*run,const char*representation,const char*scope,const char*query,const char*gate_state,Candidate*cs,int n,int dims){sqlite3_stmt*s=NULL,*envelope=NULL;int ok=0;char digest[65];
    if(sqlite3_exec(db,"PRAGMA journal_mode=WAL;CREATE TABLE IF NOT EXISTS bitnet_rank_run(run_id TEXT PRIMARY KEY,representation_id TEXT NOT NULL,scope TEXT NOT NULL,query_digest TEXT NOT NULL,gate_state TEXT NOT NULL,dimensions INTEGER NOT NULL,candidate_count INTEGER NOT NULL,created_at INTEGER NOT NULL);CREATE TABLE IF NOT EXISTS embedding_projection(run_id TEXT NOT NULL,candidate_identity TEXT NOT NULL,representation_id TEXT NOT NULL,source_digest TEXT NOT NULL,vector_digest TEXT NOT NULL,vector BLOB NOT NULL,score REAL NOT NULL,exact_verifier TEXT NOT NULL,lowerer_ref TEXT NOT NULL,causal_frontier TEXT NOT NULL,PRIMARY KEY(run_id,candidate_identity));CREATE TABLE IF NOT EXISTS ranked_candidate_envelope(run_id TEXT NOT NULL,rank INTEGER NOT NULL,candidate_identity TEXT NOT NULL,candidate_kind TEXT NOT NULL,owner_ref TEXT NOT NULL,exact_verifier TEXT NOT NULL,lowerer_ref TEXT NOT NULL,PRIMARY KEY(run_id,rank),UNIQUE(run_id,candidate_identity));BEGIN IMMEDIATE;",NULL,NULL,NULL)!=SQLITE_OK)return 0;
    bf_sha256_hex((const uint8_t*)query,strlen(query),digest);if(sqlite3_prepare_v2(db,"INSERT INTO bitnet_rank_run VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",-1,&s,NULL)!=SQLITE_OK)goto out;sqlite3_bind_text(s,1,run,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,representation,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,scope,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,4,digest,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,5,gate_state,-1,SQLITE_TRANSIENT);sqlite3_bind_int(s,6,dims);sqlite3_bind_int(s,7,n);sqlite3_bind_int64(s,8,(sqlite3_int64)time(NULL));if(sqlite3_step(s)!=SQLITE_DONE)goto out;sqlite3_finalize(s);s=NULL;
    if(sqlite3_prepare_v2(db,"INSERT INTO embedding_projection VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",-1,&s,NULL)!=SQLITE_OK)goto out;
    if(sqlite3_prepare_v2(db,"INSERT INTO ranked_candidate_envelope VALUES(?1,?2,?3,?4,?5,?6,?7)",-1,&envelope,NULL)!=SQLITE_OK)goto out;
    for(int i=0;i<n;i++){char source[65],vector[65];bf_sha256_hex((const uint8_t*)cs[i].descriptor,strlen(cs[i].descriptor),source);bf_sha256_hex((const uint8_t*)cs[i].vector,(size_t)dims*sizeof(float),vector);sqlite3_bind_text(s,1,run,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,2,cs[i].identity,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,3,representation,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,4,source,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,5,vector,-1,SQLITE_TRANSIENT);sqlite3_bind_blob(s,6,cs[i].vector,dims*(int)sizeof(float),SQLITE_TRANSIENT);sqlite3_bind_double(s,7,cs[i].score);sqlite3_bind_text(s,8,cs[i].verifier,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,9,cs[i].lowerer,-1,SQLITE_TRANSIENT);sqlite3_bind_text(s,10,"AddressPlane:forward-result",-1,SQLITE_STATIC);if(sqlite3_step(s)!=SQLITE_DONE)goto out;sqlite3_reset(s);sqlite3_clear_bindings(s);sqlite3_bind_text(envelope,1,run,-1,SQLITE_TRANSIENT);sqlite3_bind_int(envelope,2,i);sqlite3_bind_text(envelope,3,cs[i].identity,-1,SQLITE_TRANSIENT);sqlite3_bind_text(envelope,4,cs[i].kind,-1,SQLITE_TRANSIENT);sqlite3_bind_text(envelope,5,cs[i].owner,-1,SQLITE_TRANSIENT);sqlite3_bind_text(envelope,6,cs[i].verifier,-1,SQLITE_TRANSIENT);sqlite3_bind_text(envelope,7,cs[i].lowerer,-1,SQLITE_TRANSIENT);if(sqlite3_step(envelope)!=SQLITE_DONE)goto out;sqlite3_reset(envelope);sqlite3_clear_bindings(envelope);}
    sqlite3_finalize(envelope);envelope=NULL;sqlite3_finalize(s);s=NULL;ok=sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)==SQLITE_OK;return ok;
out:if(envelope)sqlite3_finalize(envelope);if(s)sqlite3_finalize(s);sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);return 0;}
/* Export only what a persisted rank run proves: one representation really
 * produced a vector for this already-bounded candidate.  Carrying an exact
 * verifier reference is not an owner query, and this receipt says so. */
static int emit_representation_evidence(const char *rank_path, const char *run, const char *out_path) {
    sqlite3 *db = NULL; sqlite3_stmt *rows = NULL; FILE *out = stdout;
    int emitted = 0, ok = 0, fd = -1;
    char tmp[1024] = {0};
    if (out_path) {
        if (snprintf(tmp, sizeof(tmp), "%s.tmp.XXXXXX", out_path) >= (int)sizeof(tmp)) goto done;
        fd = mkstemp(tmp);
        if (fd < 0 || !(out = fdopen(fd, "w"))) goto done;
        fd = -1; /* fclose owns the descriptor after fdopen. */
    }
    if (!rank_path || !run ||
        sqlite3_open_v2(rank_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) goto done;
    if (sqlite3_prepare_v2(db,
        "SELECT r.representation_id,r.scope,r.gate_state,e.candidate_identity,"
        "e.source_digest,e.vector_digest,e.exact_verifier,e.lowerer_ref,e.causal_frontier "
        "FROM bitnet_rank_run r JOIN embedding_projection e ON e.run_id=r.run_id "
        "WHERE r.run_id=?1 ORDER BY e.candidate_identity", -1, &rows, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_text(rows, 1, run, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(rows) == SQLITE_ROW) {
        const char *representation=(const char*)sqlite3_column_text(rows,0), *scope=(const char*)sqlite3_column_text(rows,1),
                   *gate=(const char*)sqlite3_column_text(rows,2), *identity=(const char*)sqlite3_column_text(rows,3),
                   *source=(const char*)sqlite3_column_text(rows,4), *vector=(const char*)sqlite3_column_text(rows,5),
                   *verifier=(const char*)sqlite3_column_text(rows,6), *lowerer=(const char*)sqlite3_column_text(rows,7),
                   *frontier=(const char*)sqlite3_column_text(rows,8);
        if (!representation || !*representation || !identity || !*identity || !source || !*source || !vector || !*vector) goto done;
        if (fputs("{\"schema\":\"bonfyre-representation-use-evidence.v1\",\"occurrence_id\":", out) < 0 ||
            !json_file_text(out, identity) ||
            fputs(",\"state\":\"used\",\"representation_id\":", out) < 0 || !json_file_text(out, representation) ||
            fputs(",\"operation_scope\":", out) < 0 || !json_file_text(out, scope ? scope : "") ||
            fputs(",\"gate_state\":", out) < 0 || !json_file_text(out, gate ? gate : "") ||
            fputs(",\"source_digest\":", out) < 0 || !json_file_text(out, source) ||
            fputs(",\"vector_digest\":", out) < 0 || !json_file_text(out, vector) ||
            fputs(",\"exact_verifier_ref\":", out) < 0 || !json_file_text(out, verifier ? verifier : "") ||
            fputs(",\"lowerer_ref\":", out) < 0 || !json_file_text(out, lowerer ? lowerer : "") ||
            fputs(",\"causal_frontier\":", out) < 0 || !json_file_text(out, frontier ? frontier : "") ||
            fputs(",\"exact_owner_queried\":false,\"truth\":\"learned_reordering_representation_executed_only\"}\n", out) < 0) goto done;
        emitted++;
    }
    if (!emitted || fflush(out) || (out_path && fsync(fileno(out)))) goto done;
    if (out_path) {
        if (fclose(out) || rename(tmp, out_path)) { out = NULL; goto done; }
        out = NULL;
        printf("{\"schema\":\"bonfyre-representation-evidence-output.v1\",\"run_id\":");
        json_text(run);
        printf(",\"records\":%d,\"output\":", emitted); json_text(out_path);
        puts(",\"truth\":\"atomically_materialized_owner_evidence_only\"}");
    }
    ok = 1;
done:
    if (rows) sqlite3_finalize(rows);
    if (db) sqlite3_close(db);
    if (out_path && out) fclose(out);
    if (!ok && tmp[0]) unlink(tmp);
    return ok;
}
/* A retention evaluation is deliberately an owner-result measurement, not a
 * gate mutator.  It requires one complete, matching ForeignTwin query ledger
 * for the stored frontier and reports how the learned order concentrates the
 * exact retained-body predicate.  "Retained" is useful for material/context
 * work, but it is neither relevance nor an authorization to lower. */
static int evaluate_retention(const char *rank_path, const char *run,
                              const char *evidence_path, const char *out_path) {
    sqlite3 *rank=NULL,*mem=NULL; sqlite3_stmt *meta=NULL,*parse=NULL,*insert=NULL,*rows=NULL,*count=NULL;
    FILE *evidence=NULL,*out=NULL; char *line=NULL,tmp[1024]={0},rep[256]={0},gate[32]={0},digest[65]={0}; const char *stage="arguments";
    size_t cap=0; ssize_t len; int fd=-1,n=0,seen=0,observed=0,row_count=0,query_count=0,emitted_cuts=0,ok=0;
    const int cuts[]={1,5,10,16,20,50,92,128}; int cut_observed[8]={0};
    if(!rank_path||!run||!evidence_path||!out_path)goto done;
    stage="rank_metadata";if(sqlite3_open_v2(rank_path,&rank,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK||
       sqlite3_prepare_v2(rank,"SELECT representation_id,gate_state,candidate_count FROM bitnet_rank_run WHERE run_id=?1",-1,&meta,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(meta,1,run,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(meta)!=SQLITE_ROW||!sqlite3_column_text(meta,0)||(n=sqlite3_column_int(meta,2))<1)goto done;
    snprintf(rep,sizeof(rep),"%s",sqlite3_column_text(meta,0));snprintf(gate,sizeof(gate),"%s",sqlite3_column_text(meta,1)?(const char*)sqlite3_column_text(meta,1):"");
    sqlite3_finalize(meta);meta=NULL;
    stage="exact_owner_ledger";if(sqlite3_open(":memory:",&mem)!=SQLITE_OK||sqlite3_exec(mem,"CREATE TABLE q(identity TEXT PRIMARY KEY,observed INTEGER NOT NULL)",NULL,NULL,NULL)!=SQLITE_OK||
       sqlite3_prepare_v2(mem,"SELECT json_extract(?1,'$.run_id'),json_extract(?1,'$.representation_id'),json_extract(?1,'$.gate_state'),json_extract(?1,'$.candidate_identity'),json_extract(?1,'$.exact_verifier_ref'),json_extract(?1,'$.query_state'),json_extract(?1,'$.outcome') WHERE json_valid(?1) AND json_extract(?1,'$.schema')='bonfyre-exact-owner-query-evidence.v1'",-1,&parse,NULL)!=SQLITE_OK||
       sqlite3_prepare_v2(mem,"INSERT INTO q VALUES(?1,?2)",-1,&insert,NULL)||!(evidence=fopen(evidence_path,"r")))goto done;
    while((len=getline(&line,&cap,evidence))>0){const char *r,*p,*g,*id,*v,*state,*outcome;if(len&&line[len-1]=='\n')line[--len]=0;sqlite3_bind_text(parse,1,line,(int)len,SQLITE_TRANSIENT);if(sqlite3_step(parse)!=SQLITE_ROW){sqlite3_reset(parse);sqlite3_clear_bindings(parse);continue;}r=(const char*)sqlite3_column_text(parse,0);p=(const char*)sqlite3_column_text(parse,1);g=(const char*)sqlite3_column_text(parse,2);id=(const char*)sqlite3_column_text(parse,3);v=(const char*)sqlite3_column_text(parse,4);state=(const char*)sqlite3_column_text(parse,5);outcome=(const char*)sqlite3_column_text(parse,6);
        if(!r||!p||!g||!id||!v||!state||!outcome||strcmp(r,run)||strcmp(p,rep)||strcmp(g,gate)||strcmp(v,"ForeignTwin:admission")||strcmp(state,"queried")||(strcmp(outcome,"observed")&&strcmp(outcome,"unobserved")))goto done;
        /* sqlite3_column_text pointers belong to parse; copy identity into the
         * insert binding before resetting that statement. */
        sqlite3_bind_text(insert,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_int(insert,2,!strcmp(outcome,"observed"));
        sqlite3_reset(parse);sqlite3_clear_bindings(parse);
        if(sqlite3_step(insert)!=SQLITE_DONE)goto done;if(sqlite3_changes(mem)!=1)goto done;sqlite3_reset(insert);sqlite3_clear_bindings(insert);query_count++;
    }
    stage="complete_owner_frontier";if(ferror(evidence)||query_count!=n||sqlite3_prepare_v2(mem,"SELECT count(*),coalesce(sum(observed),0) FROM q",-1,&count,NULL)!=SQLITE_OK||sqlite3_step(count)!=SQLITE_ROW||sqlite3_column_int(count,0)!=n)goto done;
    seen=sqlite3_column_int(count,0);observed=sqlite3_column_int(count,1);sqlite3_finalize(count);count=NULL;
    /* q is a private validation table. Attach the immutable run into that
     * connection so the join cannot accidentally read an unrelated writable
     * table from the rank database. */
    stage="rank_join";if(sqlite3_prepare_v2(mem,"ATTACH DATABASE ?1 AS rank_source",-1,&meta,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(meta,1,rank_path,-1,SQLITE_TRANSIENT);if(sqlite3_step(meta)!=SQLITE_DONE)goto done;sqlite3_finalize(meta);meta=NULL;
    if(sqlite3_prepare_v2(mem,"SELECT e.rank,q.observed FROM rank_source.ranked_candidate_envelope e JOIN q ON q.identity=e.candidate_identity WHERE e.run_id=?1 ORDER BY e.rank",-1,&rows,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(rows,1,run,-1,SQLITE_TRANSIENT);
    while(sqlite3_step(rows)==SQLITE_ROW){int pos=sqlite3_column_int(rows,0),hit=sqlite3_column_int(rows,1);if(pos!=row_count++)goto done;for(int i=0;i<8;i++)if(pos<cuts[i])cut_observed[i]+=hit;}
    stage="rank_coverage";if(row_count!=n||bf_sha256_file(evidence_path,digest)!=0)goto done;
    stage="atomic_output";if(snprintf(tmp,sizeof(tmp),"%s.tmp.XXXXXX",out_path)>=(int)sizeof(tmp)||(fd=mkstemp(tmp))<0||!(out=fdopen(fd,"w")))goto done;fd=-1;
    fprintf(out,"{\"schema\":\"bonfyre-bitnet-retention-evaluation.v1\",\"run_id\":");if(!json_file_text(out,run))goto done;
    fputs(",\"representation_id\":",out);if(!json_file_text(out,rep))goto done;
    fputs(",\"gate_state\":",out);if(!json_file_text(out,gate))goto done;
    fprintf(out,",\"exact_owner_predicate\":\"ForeignTwin:retained_body\",\"frontier_candidates\":%d,\"exact_owner_queries\":%d,\"retained_observations\":%d,\"baseline_retained_precision\":%.8f,\"evidence_sha256\":\"sha256:%s\",\"top_k\":[",n,seen,observed,(double)observed/(double)n,digest);
    for(int i=0;i<8;i++){int k=cuts[i];if(i>0&&cuts[i-1]>=n)break;if(k>n)k=n;if(emitted_cuts++)fputc(',',out);fprintf(out,"{\"k\":%d,\"retained_observations\":%d,\"retained_precision\":%.8f,\"lift_over_frontier\":%.8f}",k,cut_observed[i],(double)cut_observed[i]/(double)k,observed?((double)cut_observed[i]/(double)k)/((double)observed/(double)n):0.0);}
    fputs("],\"measurement_state\":\"single_owner_predicate_measured\",\"admission_decision\":\"unchanged\",\"truth\":\"exact_retained_observation_coverage_measurement_only\",\"forbidden_inference\":[\"retained body -> relevance, semantic truth, authority, proof, or effect\",\"one owner predicate or one query -> preservation envelope or ADMIT\",\"measured ranking coverage -> declared lowerer handoff\"]}\n",out);
    if(fflush(out)||fsync(fileno(out))||fclose(out)||rename(tmp,out_path)){out=NULL;goto done;}out=NULL;printf("{\"schema\":\"bonfyre-bitnet-retention-evaluation-output.v1\",\"run_id\":");json_text(run);printf(",\"frontier_candidates\":%d,\"retained_observations\":%d,\"output\":",n,observed);json_text(out_path);puts(",\"truth\":\"owner_coverage_measurement_only\"}");ok=1;
done:if(meta)sqlite3_finalize(meta);if(parse)sqlite3_finalize(parse);if(insert)sqlite3_finalize(insert);if(rows)sqlite3_finalize(rows);if(count)sqlite3_finalize(count);if(rank)sqlite3_close(rank);if(mem)sqlite3_close(mem);if(evidence)fclose(evidence);free(line);if(out)fclose(out);if(fd>=0)close(fd);if(!ok&&tmp[0])unlink(tmp);if(!ok)fprintf(stderr,"BitNet retention evaluation refused at %s\n",stage);return ok;
}
/* Exact rejection never makes an untried rank impossible.  This path reads
 * the content-addressed, fully retained frontier and publishes another
 * bounded declared-lowerer slice without rerunning embeddings.  It rechecks
 * that the stored run was ADMIT and that the representation has not since
 * lost its ModelCatalog admission. */
static int expand_frontier(const char *rank_path, const char *run, const char *model_path,
                           const char *representation, const char *scope,
                           const char *handoff, int offset, int top) {
    sqlite3 *rank_db=NULL,*model=NULL;sqlite3_stmt *state=NULL,*rows=NULL;char gate_state[16]={0};int total=0,written=0,ok=0;
    if(!rank_path||!run||!model_path||!representation||!scope||!handoff||top<1||offset<0)goto done;
    if(sqlite3_open_v2(rank_path,&rank_db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK||sqlite3_open_v2(model_path,&model,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto done;
    if(!gate(model,representation,scope,0,gate_state)||strcmp(gate_state,"ADMIT")){fprintf(stderr,"BitNet representation gate refused frontier expansion\n");goto done;}
    if(sqlite3_prepare_v2(rank_db,"SELECT candidate_count FROM bitnet_rank_run WHERE run_id=?1 AND representation_id=?2 AND scope=?3 AND gate_state='ADMIT'",-1,&state,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(state,1,run,-1,SQLITE_TRANSIENT);sqlite3_bind_text(state,2,representation,-1,SQLITE_TRANSIENT);sqlite3_bind_text(state,3,scope,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(state)!=SQLITE_ROW||(total=sqlite3_column_int(state,0))<1)goto done;sqlite3_finalize(state);state=NULL;
    if(sqlite3_prepare_v2(rank_db,"SELECT rank,candidate_identity,candidate_kind,owner_ref,exact_verifier,lowerer_ref FROM ranked_candidate_envelope WHERE run_id=?1 AND rank>=?2 ORDER BY rank LIMIT ?3",-1,&rows,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(rows,1,run,-1,SQLITE_TRANSIENT);sqlite3_bind_int(rows,2,offset);sqlite3_bind_int(rows,3,top);
    while(sqlite3_step(rows)==SQLITE_ROW){Candidate c={0};c.identity=strdup((const char*)sqlite3_column_text(rows,1));c.kind=strdup((const char*)sqlite3_column_text(rows,2));c.owner=strdup((const char*)sqlite3_column_text(rows,3));c.verifier=strdup((const char*)sqlite3_column_text(rows,4));c.lowerer=strdup((const char*)sqlite3_column_text(rows,5));if(!c.identity||!c.kind||!c.owner||!c.verifier||!c.lowerer||!write_handoff(handoff,run,&c,sqlite3_column_int(rows,0),total)){candidate_free(&c);goto done;}candidate_free(&c);written++;}
    if(!written)goto done;
    printf("{\"schema\":\"bonfyre-bitnet-frontier-expansion.v1\",\"run_id\":");json_text(run);printf(",\"offset\":%d,\"written\":%d,\"frontier_size\":%d,\"truth\":\"next_bounded_attempt_slice_only\"}\n",offset,written,total);ok=1;
done:if(rows)sqlite3_finalize(rows);if(state)sqlite3_finalize(state);if(rank_db)sqlite3_close(rank_db);if(model)sqlite3_close(model);return ok;}
int main(int ac,char**av){const char *address_path=NULL,*forward=NULL,*model_path=NULL,*representation=NULL,*scope=NULL,*endpoint=NULL,*query=NULL,*out_path=NULL,*handoff=NULL,*admission_db=NULL,*admission_volume=NULL,*admission_plane=NULL,*admission_estimate=NULL,*admission_free=NULL,*admission_floor=NULL,*admission_quota=NULL,*admission_max=NULL;int top=16,offset=0,diagnostic=0,candidate_batch=16;sqlite3 *address=NULL,*model=NULL,*out=NULL;BfAdmissionGrants *grants=NULL;int64_t grant_id=0;Candidate*cs=NULL;int n=0,dims=0,ok=0;char gate_state[16]={0},run[80];float **qv=NULL,**cv=NULL;char *qtexts[1];
    if(ac<2){usage();return 2;}
    if(!strcmp(av[1],"evidence")){
        const char *rank_db=NULL,*run_id=NULL,*evidence_out=NULL;
        for(int i=2;i<ac;i++){if(!strcmp(av[i],"--rank-db")&&i+1<ac)rank_db=av[++i];else if(!strcmp(av[i],"--run-id")&&i+1<ac)run_id=av[++i];else if(!strcmp(av[i],"--out")&&i+1<ac)evidence_out=av[++i];else{usage();return 2;}}
        if(!rank_db||!run_id){usage();return 2;}
        return emit_representation_evidence(rank_db,run_id,evidence_out)?0:1;
    }
    if(!strcmp(av[1],"evaluate-retention")){
        const char *rank_db=NULL,*run_id=NULL,*queries=NULL,*evaluation_out=NULL;
        for(int i=2;i<ac;i++){const char **p=NULL;if(!strcmp(av[i],"--rank-db"))p=&rank_db;else if(!strcmp(av[i],"--run-id"))p=&run_id;else if(!strcmp(av[i],"--exact-owner-evidence"))p=&queries;else if(!strcmp(av[i],"--out"))p=&evaluation_out;else{usage();return 2;}if(++i>=ac){usage();return 2;}*p=av[i];}
        return evaluate_retention(rank_db,run_id,queries,evaluation_out)?0:1;
    }
    if(!strcmp(av[1],"expand")){
        const char *rank_db=NULL,*run_id=NULL;
        for(int i=2;i<ac;i++){const char **p=NULL;if(!strcmp(av[i],"--rank-db"))p=&rank_db;else if(!strcmp(av[i],"--run-id"))p=&run_id;else if(!strcmp(av[i],"--model-db"))p=&model_path;else if(!strcmp(av[i],"--representation"))p=&representation;else if(!strcmp(av[i],"--scope"))p=&scope;else if(!strcmp(av[i],"--handoff-dir"))p=&handoff;else if(!strcmp(av[i],"--top-k")&&i+1<ac){top=atoi(av[++i]);continue;}else if(!strcmp(av[i],"--offset")&&i+1<ac){offset=atoi(av[++i]);continue;}else{usage();return 2;}if(++i>=ac){usage();return 2;}*p=av[i];}
        return expand_frontier(rank_db,run_id,model_path,representation,scope,handoff,offset,top)?0:1;
    }
    if(strcmp(av[1],"reorder")){usage();return 2;}for(int i=2;i<ac;i++){const char**p=NULL;if(!strcmp(av[i],"--address-db"))p=&address_path;else if(!strcmp(av[i],"--forward-result"))p=&forward;else if(!strcmp(av[i],"--model-db"))p=&model_path;else if(!strcmp(av[i],"--representation"))p=&representation;else if(!strcmp(av[i],"--scope"))p=&scope;else if(!strcmp(av[i],"--endpoint"))p=&endpoint;else if(!strcmp(av[i],"--bitnet-cpp"))p=&g_cpp_binary;else if(!strcmp(av[i],"--bitnet-model"))p=&g_cpp_model;else if(!strcmp(av[i],"--threads"))p=&g_threads;else if(!strcmp(av[i],"--query"))p=&query;else if(!strcmp(av[i],"--out"))p=&out_path;else if(!strcmp(av[i],"--handoff-dir"))p=&handoff;else if(!strcmp(av[i],"--admission-db"))p=&admission_db;else if(!strcmp(av[i],"--admission-volume"))p=&admission_volume;else if(!strcmp(av[i],"--admission-plane"))p=&admission_plane;else if(!strcmp(av[i],"--admission-estimated-bytes"))p=&admission_estimate;else if(!strcmp(av[i],"--admission-free-bytes"))p=&admission_free;else if(!strcmp(av[i],"--admission-protected-floor-bytes"))p=&admission_floor;else if(!strcmp(av[i],"--admission-per-plane-quota-bytes"))p=&admission_quota;else if(!strcmp(av[i],"--admission-max-grant-bytes"))p=&admission_max;else if(!strcmp(av[i],"--candidate-batch")&&i+1<ac){candidate_batch=atoi(av[++i]);continue;}else if(!strcmp(av[i],"--top-k")&&i+1<ac){top=atoi(av[++i]);continue;}else if(!strcmp(av[i],"--offset")&&i+1<ac){offset=atoi(av[++i]);continue;}else if(!strcmp(av[i],"--diagnostic")){diagnostic=1;continue;}else{usage();return 2;}if(++i>=ac){usage();return 2;}*p=av[i];}
    if(!address_path||!forward||!model_path||!representation||!scope||!query||!out_path||top<1||candidate_batch<1||
       ((g_cpp_binary==NULL)!=(g_cpp_model==NULL))||
       (endpoint&&(g_cpp_binary||g_cpp_model))||
       (!endpoint&&!(g_cpp_binary&&g_cpp_model))){usage();return 2;}
    if(endpoint&&curl_global_init(CURL_GLOBAL_DEFAULT)!=CURLE_OK){fprintf(stderr,"BitNet HTTP initialization failed\n");goto done;}
    if(sqlite3_open_v2(model_path,&model,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK||!gate(model,representation,scope,diagnostic,gate_state)){fprintf(stderr,"BitNet representation gate refused this operation\n");goto done;}
    if(!acquire_admission(admission_db, admission_volume, admission_plane,
                          admission_estimate, admission_free, admission_floor,
                          admission_quota, admission_max, representation,
                          &grants, &grant_id)) {
        fprintf(stderr, "BitNet requires an admitted ResourceAdmission grant\n"); goto done;
    }
    if(sqlite3_open_v2(address_path,&address,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK||!load_survivors(address,forward,&cs,&n))goto done;
    qtexts[0]=(char*)query;if(!embed(endpoint,qtexts,1,&qv,&dims)){fprintf(stderr,"BitNet query embedding failed\n");goto done;}for(int start=0;start<n;start+=candidate_batch){int cnt=n-start<candidate_batch?n-start:candidate_batch;char**texts=calloc((size_t)cnt,sizeof(*texts));float**batch=NULL;if(!texts)goto done;for(int i=0;i<cnt;i++)texts[i]=cs[start+i].descriptor;if(!embed(endpoint,texts,cnt,&batch,&dims)){fprintf(stderr,"BitNet candidate embedding failed at %d/%d with batch=%d\n",start,n,cnt);free(texts);goto done;}for(int i=0;i<cnt;i++)cs[start+i].vector=batch[i];free(batch);free(texts);}for(int i=0;i<n;i++){double score=0;for(int j=0;j<dims;j++)score+=(double)qv[0][j]*cs[i].vector[j];cs[i].score=(float)score;}qsort(cs,(size_t)n,sizeof(*cs),cmp_candidate);snprintf(run,sizeof(run),"bitnet:%ld",(long)time(NULL));if(sqlite3_open(out_path,&out)!=SQLITE_OK||!persist(out,run,representation,scope,query,gate_state,cs,n,dims)){fprintf(stderr,"BitNet projection persistence failed\n");goto done;}
    if(handoff&&strcmp(gate_state,"ADMIT")){fprintf(stderr,"shadow rank may not hand candidates to lowerers\n");goto done;}if(handoff)for(int i=offset;i<n&&i<offset+top;i++)if(!write_handoff(handoff,run,&cs[i],i,n))goto done;
    printf("{\"schema\":\"bonfyre-bitnet-ranked-candidates.v1\",\"run_id\":\"");fputs(run,stdout);printf("\",\"representation_id\":");Buffer js={0};json_append(&js,representation);fputs(js.data,stdout);free(js.data);printf(",\"gate_state\":\"");fputs(gate_state,stdout);printf("\",\"candidate_count\":%d,\"dimensions\":%d,\"ranked\":[",n,dims);for(int i=0;i<n;i++){if(i)putchar(',');printf("{\"identity\":");json_text(cs[i].identity);printf(",\"score\":%.8f,\"exact_verifier\":",cs[i].score);json_text(cs[i].verifier);printf(",\"lowerer_ref\":");json_text(cs[i].lowerer);printf(",\"in_initial_slice\":%s}",i>=offset&&i<offset+top?"true":"false");}printf("],\"truth\":\"learned_reordering_only_exact_owner_and_declared_lowerer_remain_required\"}\n");ok=1;
done:if(qv){free(qv[0]);free(qv);}if(cv)free(cv);if(address)sqlite3_close(address);if(model)sqlite3_close(model);if(out)sqlite3_close(out);if(cs){for(int i=0;i<n;i++)candidate_free(&cs[i]);free(cs);}if(grants){if(grant_id>0)bf_admission_release_grant(grants,grant_id,NULL);bf_admission_close(grants);}if(endpoint)curl_global_cleanup();return ok?0:1;}
