/*
 * BonfyreAPI v2 — Async HTTP gateway for the Bonfyre binary family.
 *
 * Shares a single SQLite WAL database with BonfyreQueue.  Jobs are enqueued
 * (INSERT) and return immediately; the queue worker daemon picks them up.
 *
 * New in v2:
 *   - GET  /api/events          Server-Sent Events stream (real-time job updates)
 *   - GET  /api/search?q=term   FTS5 full-text search over job results
 *   - POST /api/webhooks        Register webhook callback URLs
 *   - GET  /api/webhooks        List registered webhooks
 *   - API-key auth middleware    (api_keys table; header Authorization: Bearer <key>)
 *   - In-memory rate limiting    (token bucket per API key, 120 req/min)
 *
 * Retained from v1:
 *   - POST /api/upload           File upload
 *   - POST /api/jobs             Submit job (now async)
 *   - GET  /api/jobs[/:id]       Job status/list
 *   - *    /api/binaries/:name   Proxy to any binary
 *   - GET  /api/health           Health check
 *   - GET  /api/status           Dashboard
 *   - GET  /                     Static files / SPA fallback
 *
 * Usage:
 *   bonfyre-api serve [--port 8080] [--db FILE] [--static DIR] [--uploads DIR]
 */
#define _GNU_SOURCE
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <sqlite3.h>
#include <bonfyre.h>
#include <bf_json.h>
#include <bf_jsonw.h>
#include "../../../lib/libbonfyre/include/bf_help.h"

#define VERSION       "2.0.0"
#define MAX_BODY      (4*1024*1024)
#define MAX_PATH_SEGS 16
#define MAX_PATH_LEN  2048
#define MAX_RESULT    (1024*1024)
#define MAX_THREADS   64
#define THREAD_STACK  (256*1024)

/* Rate limiting: token bucket */
#define RL_CAPACITY   120      /* tokens (requests) */
#define RL_REFILL     2.0      /* tokens per second */
#define RL_MAX_KEYS   256

static volatile int g_running = 1;
static atomic_int g_thread_count = 0;
static sqlite3 *g_db = NULL;
static pthread_mutex_t g_db_mutex = PTHREAD_MUTEX_INITIALIZER;
static char g_static_dir[MAX_PATH_LEN] = "";
static char g_upload_dir[MAX_PATH_LEN] = "";

static const char *arg_get(int argc, char **argv, const char *flag);
static int api_scope_exact(const char *key, const char *wanted);
static int api_public_command(const char *cli, const char *name);

static int delegate_layer_api(int argc, char **argv) {
    if (argc < 4) return 1;
    const char *root = arg_get(argc, argv, "--root");
    if (strcmp(argv[2], "get") == 0) {
        char *json = NULL;
        if (bf_layer_load_json(root, argv[3], &json) != 0) return 1;
        puts(json);
        free(json);
        return 0;
    } else if (strcmp(argv[2], "lineage") == 0) {
        char *json = NULL;
        if (bf_layer_graph_edges_json(root, argv[3], &json) != 0) return 1;
        puts(json);
        free(json);
        return 0;
    } else if (strcmp(argv[2], "compat") == 0 && argc >= 5) {
        char *json = NULL;
        if (bf_layer_compat_json(root, argv[3], argv[4], &json) != 0) return 1;
        puts(json);
        free(json);
        return 0;
    } else if (strcmp(argv[2], "compose") == 0 && argc >= 5) {
        char *json = NULL;
        int dry_run = 0;
        for (int i = 5; i < argc; i++) {
            if (strcmp(argv[i], "--dry-run") == 0) dry_run = 1;
        }
        if (bf_layer_compose_json(root, argv[3], argv[4], dry_run, &json) != 0) return 1;
        puts(json);
        free(json);
        return 0;
    } else {
        return 1;
    }
    return 1;
}

/* ── Rate limiter (in-memory token bucket per API key) ────────────── */

typedef struct {
    char key[256];
    double tokens;
    double last_refill;  /* monotonic seconds */
} RateBucket;

static RateBucket g_buckets[RL_MAX_KEYS];
static int g_nbuckets = 0;
static pthread_mutex_t g_rl_mutex = PTHREAD_MUTEX_INITIALIZER;

static double mono_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

static int rate_limit_check(const char *key) {
    if (!key || !key[0]) return 1;  /* No key = anonymous = allow */

    pthread_mutex_lock(&g_rl_mutex);
    double now = mono_sec();
    RateBucket *b = NULL;

    for (int i = 0; i < g_nbuckets; i++) {
        if (strcmp(g_buckets[i].key, key) == 0) { b = &g_buckets[i]; break; }
    }

    if (!b) {
        if (g_nbuckets >= RL_MAX_KEYS) {
            /* Evict oldest (simplistic) */
            b = &g_buckets[0];
        } else {
            b = &g_buckets[g_nbuckets++];
        }
        strncpy(b->key, key, sizeof(b->key) - 1);
        b->tokens = RL_CAPACITY;
        b->last_refill = now;
    }

    /* Refill tokens */
    double elapsed = now - b->last_refill;
    b->tokens += elapsed * RL_REFILL;
    if (b->tokens > RL_CAPACITY) b->tokens = RL_CAPACITY;
    b->last_refill = now;

    int allowed = 0;
    if (b->tokens >= 1.0) {
        b->tokens -= 1.0;
        allowed = 1;
    }

    pthread_mutex_unlock(&g_rl_mutex);
    return allowed;
}

/* ── HTTP primitives ──────────────────────────────────────────────── */

typedef struct {
    char method[16];
    char path[MAX_PATH_LEN];
    char query[MAX_PATH_LEN];
    char *body;
    int body_len;
    char auth_token[256];
    char session_token[128];
    int content_length;
    char content_type[256];
} HttpRequest;

typedef struct {
    int fd;
    char *buf;
    size_t buf_len;
    int status;
    char content_type[128];
} HttpResponse;

static void http_resp_init(HttpResponse *r, int fd) {
    memset(r, 0, sizeof(*r));
    r->fd = fd; r->status = 200;
    strcpy(r->content_type, "application/json");
}
static void http_resp_free(HttpResponse *r) { free(r->buf); r->buf = NULL; }

static void http_resp_send(HttpResponse *r) {
    const char *st = "OK";
    if (r->status == 201) st = "Created";
    else if (r->status == 204) st = "No Content";
    else if (r->status == 400) st = "Bad Request";
    else if (r->status == 401) st = "Unauthorized";
    else if (r->status == 403) st = "Forbidden";
    else if (r->status == 404) st = "Not Found";
    else if (r->status == 405) st = "Method Not Allowed";
    else if (r->status == 413) st = "Payload Too Large";
    else if (r->status == 429) st = "Too Many Requests";
    else if (r->status == 500) st = "Internal Server Error";
    else if (r->status == 503) st = "Service Unavailable";

    char hdr[1024];
    int hlen = snprintf(hdr, sizeof(hdr),
        "HTTP/1.1 %d %s\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n"
        "Access-Control-Allow-Headers: Authorization, Content-Type\r\n"
        "Connection: close\r\n\r\n",
        r->status, st, r->content_type, r->buf_len);
    write(r->fd, hdr, (size_t)hlen);
    if (r->buf_len > 0 && r->buf) write(r->fd, r->buf, r->buf_len);
}

static void http_resp_json(HttpResponse *r, int status, const char *fmt, ...) {
    r->status = status;
    free(r->buf);
    va_list ap; va_start(ap, fmt);
    int n = vasprintf(&r->buf, fmt, ap);
    va_end(ap);
    r->buf_len = (size_t)(n > 0 ? n : 0);
}

/* Share the libbonfyre JSON writer across account read responses. Stored
 * names, file paths, snippets and callback URLs must never become raw JSON. */
static void api_finish_json(HttpResponse *resp, bf_jsonw *out) {
    if (out->oom || !out->buf) {
        bf_jsonw_free(out);
        http_resp_json(resp, 503, "{\"error\":\"JSON response unavailable\"}");
        return;
    }
    free(resp->buf);
    resp->buf = out->buf;
    resp->buf_len = out->len;
    resp->status = 200;
    out->buf = NULL;
    bf_jsonw_free(out);
}

static const char *api_sqlite_text(sqlite3_stmt *st, int col) {
    const unsigned char *value = sqlite3_column_text(st, col);
    return value ? (const char *)value : "";
}

static int parse_http_request(int fd, HttpRequest *req) {
    memset(req, 0, sizeof(*req));
    req->body = NULL;

    char hdr[8192];
    ssize_t total = 0;
    while (total < (ssize_t)sizeof(hdr) - 1) {
        ssize_t n = read(fd, hdr + total, sizeof(hdr) - (size_t)total - 1);
        if (n <= 0) break;
        total += n; hdr[total] = '\0';
        if (strstr(hdr, "\r\n\r\n")) break;
    }
    if (total <= 0) return -1;
    hdr[total] = '\0';

    char *body_marker = strstr(hdr, "\r\n\r\n");
    if (!body_marker) return -1; /* incomplete header */
    size_t header_end = (size_t)(body_marker - hdr);

    char *line_end = strstr(hdr, "\r\n");
    if (!line_end) return -1;
    *line_end = '\0';
    sscanf(hdr, "%15s %2047s", req->method, req->path);

    char *qm = strchr(req->path, '?');
    if (qm) { *qm = '\0'; strncpy(req->query, qm + 1, sizeof(req->query) - 1); }

    char *hp = line_end + 2;
    int seen_length = 0;
    while (hp < hdr + header_end) {
        char *he = strstr(hp, "\r\n");
        if (!he || he == hp) break;
        *he = '\0';
        if (strncasecmp(hp, "Content-Length:", 15) == 0) {
            if (++seen_length != 1) return -1;
            const char *v = hp + 15;
            while (*v == ' ' || *v == '\t') v++;
            if (!isdigit((unsigned char)*v)) return -1;
            unsigned long length = 0;
            while (isdigit((unsigned char)*v)) {
                length = length * 10 + (unsigned long)(*v++ - '0');
                if (length > MAX_BODY) return -2; /* no truncated body */
            }
            while (*v == ' ' || *v == '\t') v++;
            if (*v) return -1;
            req->content_length = (int)length;
        }
        if (strncasecmp(hp, "Transfer-Encoding:", 18) == 0)
            return -1; /* no chunked framing support or CL/TE ambiguity */
        if (strncasecmp(hp, "Content-Type:", 13) == 0) {
            const char *v = hp + 13; while (*v == ' ') v++;
            strncpy(req->content_type, v, sizeof(req->content_type) - 1);
        }
        if (strncasecmp(hp, "Authorization:", 14) == 0) {
            const char *av = hp + 14; while (*av == ' ') av++;
            if (strncasecmp(av, "Bearer ", 7) == 0) av += 7;
            strncpy(req->auth_token, av, sizeof(req->auth_token) - 1);
        }
        if (strncasecmp(hp, "X-Bonfyre-Session:", 18) == 0) {
            const char *v = hp + 18;
            while (*v == ' ' || *v == '\t') v++;
            if (req->session_token[0] || !*v || strlen(v) >= sizeof(req->session_token) ||
                strspn(v, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.") != strlen(v)) return -1;
            memcpy(req->session_token, v, strlen(v) + 1);
        }
        hp = he + 2;
    }

    if (req->content_length > 0) {
        int cap = req->content_length;
        req->body = malloc((size_t)cap + 1);
        if (!req->body) return -1;
        char *body_start = body_marker + 4;
        size_t in_hdr = (size_t)(total - (body_start - hdr));
        if (in_hdr > 0) {
            size_t cp = in_hdr < (size_t)cap ? in_hdr : (size_t)cap;
            memcpy(req->body, body_start, cp);
            req->body_len = (int)cp;
        }
        while (req->body_len < cap) {
            ssize_t n = read(fd, req->body + req->body_len, (size_t)(cap - req->body_len));
            if (n <= 0) break;
            req->body_len += (int)n;
        }
        req->body[req->body_len] = '\0';
        if (req->body_len != cap) return -1; /* no partial body is a valid write */
    }
    return 0;
}

static void free_request(HttpRequest *req) { free(req->body); }

/* ── Utilities ────────────────────────────────────────────────────── */

static void iso_now(char *buf, size_t sz) { bf_iso_timestamp(buf, sz); }

static int path_segments(const char *path, char segs[][256], int max) {
    int c = 0; const char *p = path;
    while (*p == '/') p++;
    while (*p && c < max) {
        size_t i = 0;
        while (*p && *p != '/' && i < 255) segs[c][i++] = *p++;
        segs[c][i] = '\0'; c++;
        while (*p == '/') p++;
    }
    return c;
}

static const char *query_param(const char *query, const char *key, char *buf, size_t sz) {
    size_t klen = strlen(key);
    const char *p = query;
    while (p && *p) {
        if (strncmp(p, key, klen) == 0 && p[klen] == '=') {
            const char *v = p + klen + 1;
            size_t i = 0;
            while (*v && *v != '&' && i < sz - 1) buf[i++] = *v++;
            buf[i] = '\0';
            return buf;
        }
        p = strchr(p, '&');
        if (p) p++;
    }
    buf[0] = '\0';
    return NULL;
}

static int run_binary(const char *bin, char *const argv[], char *out, size_t out_sz) {
    int pipefd[2];
    if (pipe(pipefd) < 0) return -1;
    pid_t pid = fork();
    if (pid < 0) { close(pipefd[0]); close(pipefd[1]); return -1; }
    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[1]);
        execvp(bin, argv);
        _exit(127);
    }
    close(pipefd[1]);
    size_t total = 0;
    while (total < out_sz - 1) {
        ssize_t n = read(pipefd[0], out + total, out_sz - total - 1);
        if (n <= 0) break;
        total += (size_t)n;
    }
    out[total] = '\0';
    close(pipefd[0]);
    int status; waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

/* Native Auth receives session secrets on stdin, never argv or logs.
 * An effect still needs current PackGraph and review authority afterwards. */
static int api_owner_stdin(const char *bin, char *const args[], const char *secret,
                            char *output, size_t cap) {
    int in[2] = {-1,-1}, out[2] = {-1,-1};
    if (!bin || !*bin || !secret || !*secret || cap < 2 ||
        pipe(in) || pipe(out)) {
        if (in[0]>=0) close(in[0]); if (in[1]>=0) close(in[1]);
        if (out[0]>=0) close(out[0]); if (out[1]>=0) close(out[1]);
        return -1;
    }
    pid_t pid=fork();
    if(pid<0){close(in[0]);close(in[1]);close(out[0]);close(out[1]);return -1;}
    if(pid==0){
        close(in[1]);close(out[0]);
        if(dup2(in[0],STDIN_FILENO)<0 || dup2(out[1],STDOUT_FILENO)<0)_exit(127);
        close(in[0]);close(out[1]);
        (void)freopen("/dev/null","w",stderr);
        execvp(bin,args);_exit(127);
    }
    close(in[0]);close(out[1]);
    size_t off=0,len=strlen(secret);
    while(off<len){
        ssize_t n=write(in[1],secret+off,len-off);
        if(n<0&&errno==EINTR)continue;
        if(n<=0)break;
        off+=(size_t)n;
    }
    close(in[1]);
    size_t used=0;int complete=0;
    while(used+1<cap){
        ssize_t n=read(out[0],output+used,cap-1-used);
        if(n<0&&errno==EINTR)continue;
        if(n<0)break;
        if(!n){complete=1;break;}
        used+=(size_t)n;
    }
    output[used]='\0';close(out[0]);
    int status=0;
    if(waitpid(pid,&status,0)!=pid||!WIFEXITED(status)||WEXITSTATUS(status)||
       off!=len||!complete)return -1;
    return 0;
}

static int api_native_actor(const char *session, int *user, char *actor, size_t capacity) {
    const char *bin=getenv("BONFYRE_AUTH_BINARY"),*db=getenv("BONFYRE_AUTH_DB");
    if(!bin||!*bin||!db||!*db||!session||!*session||strlen(session)>120)return 0;
    char *args[]={(char*)bin,"--db",(char*)db,"actor-closure-stdin",NULL};
    char output[4096]="",err[96]="";
    if(api_owner_stdin(bin,args,session,output,sizeof(output)))return 0;
    bf_json_doc_t *doc=bf_json_parse(output,strlen(output),err,sizeof(err));
    if(!doc)return 0;
    const bf_json_node_t *root=bf_json_root(doc);
    const bf_json_node_t *yes=bf_json_obj_get(doc,root,"authenticated");
    const bf_json_node_t *principal=bf_json_obj_get(doc,root,"auth_principal");
    const bf_json_node_t *id=principal?bf_json_obj_get(doc,principal,"user_id"):NULL;
    const bf_json_node_t *bound=bf_json_obj_get(doc,root,"actor_subject");
    int ok=yes&&yes->type==BF_JSON_BOOL&&bf_json_get_bool(yes)&&
        principal&&principal->type==BF_JSON_OBJECT&&id&&id->type==BF_JSON_INT&&
        bf_json_get_int(id)>0&&bf_json_get_int(id)<2147483647&&
        bound&&bf_json_get_str_copy(bound,actor,capacity);
    if(ok)*user=(int)bf_json_get_int(id);
    bf_json_free(doc);return ok;
}
static int api_native_institution(const char *session,const char *institution) {
    const char *bin=getenv("BONFYRE_AUTH_BINARY"),*db=getenv("BONFYRE_AUTH_DB");
    if(!bin||!*bin||!db||!*db)return 0;
    char *args[]={(char*)bin,"--db",(char*)db,"institution-authorize-stdin",
        "--institution",(char*)institution,"--role","write",NULL};
    char output[1024]="",err[96]="";
    if(api_owner_stdin(bin,args,session,output,sizeof(output)))return 0;
    bf_json_doc_t *doc=bf_json_parse(output,strlen(output),err,sizeof(err));
    if(!doc)return 0;
    const bf_json_node_t *yes=bf_json_obj_get(doc,bf_json_root(doc),"authorized");
    int ok=yes&&yes->type==BF_JSON_BOOL&&bf_json_get_bool(yes);
    bf_json_free(doc);return ok;
}
static int api_authority_name(const char *name,size_t capacity) {
    if(!name||!*name||strlen(name)>=capacity)return 0;
    for(const unsigned char *p=(const unsigned char*)name;*p;p++)
        if(!((*p>='a'&&*p<='z')||(*p>='0'&&*p<='9')||*p=='-'||*p=='.'||*p=='_'))return 0;
    return 1;
}

static int api_native_pack(const char *pack,const char *subject,const char *scope,
                           const char *actor,char *digest,size_t capacity) {
    const char *bin=getenv("BONFYRE_CMS_BINARY"),*db=getenv("BONFYRE_CMS_DB");
    if(!bin||!*bin||!db||!*db||!api_authority_name(pack,96)||capacity<65)return 0;
    char *args[]={(char*)bin,"pack-lifecycle","--operation","current","--pack",(char*)pack,
        "--version","","--subject",(char*)subject,"--scope",(char*)scope,"--actor",(char*)actor,
        "--expected-digest","","--expected-revision","0","--db",(char*)db,NULL,NULL,NULL};
    const char *schemas=getenv("BONFYRE_CMS_SCHEMAS");
    if(schemas&&*schemas){
        size_t i=0;while(args[i])i++;
        args[i++]="--schemas";args[i++]=(char*)schemas;args[i]=NULL;
    }
    char output[32768]="";
    if(run_binary(bin,args,output,sizeof(output)))return 0;
    int schema=0,active=0;
    char resolved[65]="",pinned[65]="";
    char *save=NULL;
    for(char *line=strtok_r(output,"\n",&save);line;line=strtok_r(NULL,"\n",&save)){
        char *value=strchr(line,'\t');if(!value)continue;*value++='\0';
        if(!strcmp(line,"schema"))schema=!strcmp(value,"bonfyre.pack.lifecycle.v1");
        else if(!strcmp(line,"state"))active=!strcmp(value,"active");
        else if(!strcmp(line,"digest")&&strlen(value)==64)memcpy(resolved,value,65);
        else if(!strcmp(line,"pinned_digest")&&strlen(value)==64)memcpy(pinned,value,65);
    }
    if(!schema||!active||!*pinned||strcmp(resolved,pinned)||
       strspn(pinned,"abcdef0123456789")!=64)return 0;
    memcpy(digest,pinned,65);
    return 1;
}
/* Do not compose an arbitrary valid API key with another user's session.
 * Only an explicitly bound API-key to Auth-user reference may carry closure. */
static int api_key_actor_bound(const char *key,int user) {
    if(!key||!*key||user<1)return 0;
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st=NULL;int ok=0;
    if(sqlite3_prepare_v2(g_db,
        "SELECT 1 FROM api_keys WHERE key=? AND active=1 AND auth_user_id=?",
        -1,&st,NULL)==SQLITE_OK){
        sqlite3_bind_text(st,1,key,-1,SQLITE_TRANSIENT);
        sqlite3_bind_int(st,2,user);
        ok=sqlite3_step(st)==SQLITE_ROW;
    }
    sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);
    return ok;
}
/* Auth + PackGraph are necessary, never sufficient, for an effect. */
static int api_native_preflight(const HttpRequest *req,const char *context,const char *pack,
                                char *actor,size_t acap,char *subject,size_t scap,
                                char *digest,size_t dcap) {
    int user=0;char bound[192]="";
    if(!req->session_token[0]||
       !api_native_actor(req->session_token,&user,bound,sizeof(bound))||
       !api_key_actor_bound(req->auth_token,user)||
       !api_authority_name(pack,96))return 0;
    char work_actor[64];
    snprintf(work_actor,sizeof(work_actor),"user:%d",user);
    if(!strcmp(context,"self")){
        snprintf(subject,scap,"%s",work_actor);
        if(!api_native_pack(pack,subject,"self",work_actor,digest,dcap))return 0;
    }else if(!strncmp(context,"institution:",12)){
        const char *institution=context+12;
        if(!api_authority_name(institution,64)||
           !api_native_institution(req->session_token,institution))return 0;
        snprintf(subject,scap,"%s",context);
        if(!api_native_pack(pack,subject,"institution",work_actor,digest,dcap))return 0;
    }else return 0; /* household needs its distinct membership owner */
    snprintf(actor,acap,"%s",bound);
    return 1;
}

/* ── Database ─────────────────────────────────────────────────────── */

/*
 * Shared schema: the API creates queue tables if the queue hasn't yet,
 * and adds its own tables (uploads, api_keys, request_log).  When both
 * API and Queue point at the same DB file, they share the jobs/events
 * tables via WAL mode.
 */
static const char *SCHEMA_SQL =
    "PRAGMA journal_mode=WAL;"
    "PRAGMA busy_timeout=5000;"
    "PRAGMA synchronous=NORMAL;"

    /* Queue tables (shared with BonfyreQueue) */
    "CREATE TABLE IF NOT EXISTS jobs ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  type TEXT NOT NULL,"
    "  status TEXT NOT NULL DEFAULT 'queued',"
    "  priority INTEGER NOT NULL DEFAULT 100,"
    "  input_path TEXT,"
    "  output_path TEXT,"
    "  source TEXT DEFAULT 'api',"
    "  api_key TEXT,"
    "  worker TEXT,"
    "  attempts INTEGER DEFAULT 0,"
    "  max_retries INTEGER DEFAULT 3,"
    "  created_at TEXT NOT NULL,"
    "  claimed_at TEXT,"
    "  completed_at TEXT,"
    "  error TEXT,"
    "  result_json TEXT,"
    "  wall_ms REAL,"
    "  exit_code INTEGER"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);"
    "CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority, id);"
    "CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs(type);"

    "CREATE TABLE IF NOT EXISTS events ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  job_id INTEGER NOT NULL,"
    "  event TEXT NOT NULL,"
    "  data TEXT,"
    "  created_at TEXT NOT NULL,"
    "  FOREIGN KEY(job_id) REFERENCES jobs(id)"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id);"

    "CREATE TABLE IF NOT EXISTS webhooks ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  url TEXT NOT NULL,"
    "  events TEXT DEFAULT 'completed,failed',"
    "  active INTEGER DEFAULT 1,"
    "  created_at TEXT NOT NULL,"
    "  api_key TEXT"
    ");"

    "CREATE VIRTUAL TABLE IF NOT EXISTS jobs_fts USING fts5("
    "  type, source, result_json, error,"
    "  content=jobs, content_rowid=id"
    ");"

    /* API-only tables */
    "CREATE TABLE IF NOT EXISTS uploads ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  filename TEXT NOT NULL,"
    "  path TEXT NOT NULL,"
    "  size_bytes INTEGER,"
    "  content_type TEXT,"
    "  uploaded_at TEXT NOT NULL,"
    "  api_key TEXT"
    ");"

    "CREATE TABLE IF NOT EXISTS api_keys ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  key TEXT NOT NULL UNIQUE,"
    "  label TEXT,"
    "  scopes TEXT DEFAULT '*',"
    "  auth_user_id INTEGER,"
    "  active INTEGER DEFAULT 1,"
    "  created_at TEXT NOT NULL"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_api_keys_key ON api_keys(key);"

    "CREATE TABLE IF NOT EXISTS request_log ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  method TEXT,"
    "  path TEXT,"
    "  api_key TEXT,"
    "  status INTEGER,"
    "  wall_ms REAL,"
    "  created_at TEXT NOT NULL"
    ");";

static sqlite3 *open_db(const char *path) {
    sqlite3 *db;
    if (sqlite3_open(path, &db) != SQLITE_OK) {
        fprintf(stderr, "[api] cannot open %s: %s\n", path, sqlite3_errmsg(db));
        return NULL;
    }
    char *err = NULL;
    if (sqlite3_exec(db, SCHEMA_SQL, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "[api] schema error: %s\n", err);
        sqlite3_free(err); sqlite3_close(db); return NULL;
    }
    /* Existing deployments created webhooks without owner provenance.
     * Add the column without rewriting or assigning legacy callbacks. */
    sqlite3_stmt *columns = NULL;
    int has_owner = 0;
    if (sqlite3_prepare_v2(db, "PRAGMA table_info(webhooks)", -1, &columns, NULL) != SQLITE_OK) {
        sqlite3_close(db); return NULL;
    }
    while (sqlite3_step(columns) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(columns, 1);
        if (name && !strcmp(name, "api_key")) has_owner = 1;
    }
    sqlite3_finalize(columns);
    if (!has_owner && sqlite3_exec(db, "ALTER TABLE webhooks ADD COLUMN api_key TEXT", NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "[api] webhook owner migration failed: %s\n", err ? err : sqlite3_errmsg(db));
        sqlite3_free(err); sqlite3_close(db); return NULL;
    }
    /* Legacy keys remain usable for discovery but cannot claim an Actor.
     * A session may be paired only with its explicitly bound Auth user ID. */
    columns = NULL; has_owner = 0;
    if (sqlite3_prepare_v2(db, "PRAGMA table_info(api_keys)", -1, &columns, NULL) != SQLITE_OK) {
        sqlite3_close(db); return NULL;
    }
    while (sqlite3_step(columns) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(columns, 1);
        if (name && !strcmp(name, "auth_user_id")) has_owner = 1;
    }
    sqlite3_finalize(columns);
    if (!has_owner && sqlite3_exec(db, "ALTER TABLE api_keys ADD COLUMN auth_user_id INTEGER", NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "[api] API key actor-binding migration failed: %s\n", err ? err : sqlite3_errmsg(db));
        sqlite3_free(err); sqlite3_close(db); return NULL;
    }
    return db;
}

static const char *default_db(void) {
    static char p[MAX_PATH_LEN];
    const char *h = getenv("HOME");
    snprintf(p, sizeof(p), "%s/.local/share/bonfyre/queue.db", h ? h : ".");
    char d[MAX_PATH_LEN];
    snprintf(d, sizeof(d), "%s/.local/share/bonfyre", h ? h : ".");
    mkdir(d, 0755);
    return p;
}

/* ── Auth middleware ──────────────────────────────────────────────── */

static int validate_api_key(const char *key) {
    if (!key || !key[0]) return 0;  /* Protected routes require an actual key. */

    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db,
        "SELECT active FROM api_keys WHERE key=?", -1, &st, NULL);
    sqlite3_bind_text(st, 1, key, -1, SQLITE_STATIC);
    int valid = 0;
    if (sqlite3_step(st) == SQLITE_ROW)
        valid = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);
    return valid;
}

/* ── Request logging ──────────────────────────────────────────────── */

static void log_request(const char *method, const char *path,
                        const char *key, int status, double wall_ms) {
    char ts[64]; iso_now(ts, sizeof(ts));
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db,
        "INSERT INTO request_log (method,path,api_key,status,wall_ms,created_at) "
        "VALUES (?,?,?,?,?,?)", -1, &st, NULL);
    sqlite3_bind_text(st, 1, method, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, key && key[0] ? key : "anon", -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 4, status);
    sqlite3_bind_double(st, 5, wall_ms);
    sqlite3_bind_text(st, 6, ts, -1, SQLITE_STATIC);
    sqlite3_step(st);
    sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);
}

/* ── Event emission (shared with queue) ───────────────────────────── */

static void emit_event(int job_id, const char *event, const char *data) {
    char ts[64]; iso_now(ts, sizeof(ts));
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db,
        "INSERT INTO events (job_id, event, data, created_at) VALUES (?,?,?,?)",
        -1, &st, NULL);
    sqlite3_bind_int(st, 1, job_id);
    sqlite3_bind_text(st, 2, event, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, data ? data : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 4, ts, -1, SQLITE_STATIC);
    sqlite3_step(st);
    sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);
}

/* ── Static file serving ──────────────────────────────────────────── */

static const char *mime_for_ext(const char *path) {
    const char *dot = strrchr(path, '.');
    if (!dot) return "application/octet-stream";
    if (strcasecmp(dot, ".html") == 0 || strcasecmp(dot, ".htm") == 0) return "text/html";
    if (strcasecmp(dot, ".css") == 0) return "text/css";
    if (strcasecmp(dot, ".js") == 0) return "application/javascript";
    if (strcasecmp(dot, ".json") == 0) return "application/json";
    if (strcasecmp(dot, ".png") == 0) return "image/png";
    if (strcasecmp(dot, ".jpg") == 0 || strcasecmp(dot, ".jpeg") == 0) return "image/jpeg";
    if (strcasecmp(dot, ".gif") == 0) return "image/gif";
    if (strcasecmp(dot, ".svg") == 0) return "image/svg+xml";
    if (strcasecmp(dot, ".ico") == 0) return "image/x-icon";
    if (strcasecmp(dot, ".woff2") == 0) return "font/woff2";
    if (strcasecmp(dot, ".woff") == 0) return "font/woff";
    return "application/octet-stream";
}

static int serve_static(HttpResponse *resp, const char *url_path) {
    if (!g_static_dir[0]) return 0;
    if (strstr(url_path, "..")) return 0;

    char fpath[MAX_PATH_LEN];
    if (strcmp(url_path, "/") == 0 || strcmp(url_path, "") == 0)
        snprintf(fpath, sizeof(fpath), "%s/index.html", g_static_dir);
    else
        snprintf(fpath, sizeof(fpath), "%s%s", g_static_dir, url_path);

    char resolved[MAX_PATH_LEN];
    if (!realpath(fpath, resolved)) return 0;
    char resolved_root[MAX_PATH_LEN];
    if (!realpath(g_static_dir, resolved_root)) return 0;
    if (strncmp(resolved, resolved_root, strlen(resolved_root)) != 0) return 0;

    FILE *f = fopen(resolved, "rb");
    if (!f) {
        snprintf(fpath, sizeof(fpath), "%s/index.html", g_static_dir);
        f = fopen(fpath, "rb");
        if (!f) return 0;
    }
    fseek(f, 0, SEEK_END); long sz = ftell(f); fseek(f, 0, SEEK_SET);
    if (sz <= 0 || sz > 10 * 1024 * 1024) { fclose(f); return 0; }

    free(resp->buf);
    resp->buf = malloc((size_t)sz);
    resp->buf_len = fread(resp->buf, 1, (size_t)sz, f);
    fclose(f);
    resp->status = 200;
    strncpy(resp->content_type, mime_for_ext(resolved), sizeof(resp->content_type) - 1);
    return 1;
}

/* ── API routes ───────────────────────────────────────────────────── */

/* GET /api/health */
static void handle_health(HttpResponse *resp) {
    http_resp_json(resp, 200,
        "{\"status\":\"ok\",\"version\":\"%s\",\"service\":\"bonfyre-api\"}", VERSION);
}

/* GET /api/status — system dashboard */
static void handle_status(HttpResponse *resp) {
    pthread_mutex_lock(&g_db_mutex);

    int total_jobs = 0, completed = 0, failed = 0, queued = 0, running = 0;
    int total_uploads = 0;
    long total_bytes = 0;
    double avg_wall = 0;
    int event_count = 0, webhook_count = 0, key_count = 0;
    int req_count = 0;

    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db, "SELECT status, COUNT(*) FROM jobs GROUP BY status",
        -1, &st, NULL);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *s = (const char *)sqlite3_column_text(st, 0);
        int c = sqlite3_column_int(st, 1);
        total_jobs += c;
        if (s && strcmp(s, "completed") == 0) completed = c;
        else if (s && strcmp(s, "failed") == 0) failed = c;
        else if (s && strcmp(s, "queued") == 0) queued = c;
        else if (s && strcmp(s, "running") == 0) running = c;
    }
    sqlite3_finalize(st);

    sqlite3_prepare_v2(g_db, "SELECT AVG(wall_ms) FROM jobs WHERE wall_ms>0",
        -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) avg_wall = sqlite3_column_double(st, 0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(g_db, "SELECT COUNT(*), COALESCE(SUM(size_bytes),0) FROM uploads",
        -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) {
        total_uploads = sqlite3_column_int(st, 0);
        total_bytes = (long)sqlite3_column_int64(st, 1);
    }
    sqlite3_finalize(st);

    sqlite3_prepare_v2(g_db, "SELECT COUNT(*) FROM events", -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) event_count = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(g_db, "SELECT COUNT(*) FROM webhooks WHERE active=1",
        -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) webhook_count = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(g_db, "SELECT COUNT(*) FROM api_keys WHERE active=1",
        -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) key_count = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);

    sqlite3_prepare_v2(g_db, "SELECT COUNT(*) FROM request_log", -1, &st, NULL);
    if (sqlite3_step(st) == SQLITE_ROW) req_count = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);

    pthread_mutex_unlock(&g_db_mutex);

    http_resp_json(resp, 200,
        "{\"status\":\"ok\",\"version\":\"%s\","
        "\"jobs\":{\"total\":%d,\"queued\":%d,\"running\":%d,\"completed\":%d,\"failed\":%d,\"avg_wall_ms\":%.1f},"
        "\"uploads\":{\"total\":%d,\"bytes\":%ld},"
        "\"events\":%d,\"webhooks\":%d,\"api_keys\":%d,\"requests_logged\":%d,"
        "\"active_threads\":%d}",
        VERSION, total_jobs, queued, running, completed, failed, avg_wall,
        total_uploads, total_bytes,
        event_count, webhook_count, key_count, req_count,
        atomic_load(&g_thread_count));
}

/* POST /api/upload */
static void handle_upload(HttpRequest *req, HttpResponse *resp) {
    if (!req->body || req->body_len <= 0) {
        http_resp_json(resp, 400, "{\"error\":\"empty body\"}"); return;
    }

    char ts[64]; iso_now(ts, sizeof(ts));
    char fname[256];
    snprintf(fname, sizeof(fname), "upload_%ld_%d", (long)time(NULL), getpid());

    char *fn_param = strstr(req->query, "filename=");
    if (fn_param) {
        fn_param += 9;
        char safe[128]; int si = 0;
        while (*fn_param && *fn_param != '&' && si < (int)sizeof(safe) - 1) {
            char c = *fn_param++;
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.')
                safe[si++] = c;
        }
        safe[si] = '\0';
        if (si > 0) snprintf(fname, sizeof(fname), "%ld_%s", (long)time(NULL), safe);
    }

    char fpath[MAX_PATH_LEN];
    snprintf(fpath, sizeof(fpath), "%s/%s", g_upload_dir, fname);

    FILE *f = fopen(fpath, "wb");
    if (!f) { http_resp_json(resp, 500, "{\"error\":\"cannot write file\"}"); return; }
    fwrite(req->body, 1, (size_t)req->body_len, f);
    fclose(f);

    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db,
        "INSERT INTO uploads (filename,path,size_bytes,content_type,uploaded_at,api_key) "
        "VALUES (?,?,?,?,?,?)", -1, &st, NULL);
    sqlite3_bind_text(st, 1, fname, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, fpath, -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 3, req->body_len);
    sqlite3_bind_text(st, 4, req->content_type, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 5, ts, -1, SQLITE_STATIC);
    if (req->auth_token[0]) sqlite3_bind_text(st, 6, req->auth_token, -1, SQLITE_STATIC);
    else sqlite3_bind_null(st, 6);
    sqlite3_step(st); sqlite3_finalize(st);
    int upload_id = (int)sqlite3_last_insert_rowid(g_db);
    pthread_mutex_unlock(&g_db_mutex);

    http_resp_json(resp, 201,
        "{\"data\":{\"id\":%d,\"filename\":\"%s\",\"size\":%d,\"path\":\"%s\"},"
        "\"meta\":{\"uploaded\":true}}",
        upload_id, fname, req->body_len, fpath);
}

/* POST /api/jobs — async: enqueue and return immediately */
static void handle_job_submit(HttpRequest *req, HttpResponse *resp) {
    if (!req->body || req->body_len <= 0) {
        http_resp_json(resp, 400, "{\"error\":\"empty body\"}"); return;
    }

    /* Jobs are effectful. Parse a typed request before authorizing an exact
     * command-specific enqueue scope; substring extraction is not a grammar. */
    char type[64] = "", input[MAX_PATH_LEN] = "";
    int priority = 100;
    char err[128] = "";
    bf_json_doc_t *doc = bf_json_parse(req->body, (size_t)req->body_len, err, sizeof(err));
    const bf_json_node_t *root = doc ? bf_json_root(doc) : NULL;
    if (!root || root->type != BF_JSON_OBJECT) {
        bf_json_free(doc);
        http_resp_json(resp, 400, "{\"error\":\"job requires a JSON object\"}"); return;
    }
    const bf_json_node_t *t = bf_json_obj_get(doc, root, "type");
    const bf_json_node_t *in = bf_json_obj_get(doc, root, "input");
    const bf_json_node_t *pr = bf_json_obj_get(doc, root, "priority");
    int tlen = 0, ilen = 0;
    if (!bf_json_get_str(t, &tlen) || tlen <= 0 || tlen >= (int)sizeof(type)) {
        bf_json_free(doc);
        http_resp_json(resp, 400, "{\"error\":\"invalid job type\"}"); return;
    }
    bf_json_get_str_copy(t, type, sizeof(type));
    for (int i = 0; i < tlen; i++) {
        char c = type[i];
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-')) {
            bf_json_free(doc);
            http_resp_json(resp, 400, "{\"error\":\"invalid command identity\"}"); return;
        }
    }
    if (in) {
        if (!bf_json_get_str(in, &ilen) || ilen >= (int)sizeof(input)) {
            bf_json_free(doc);
            http_resp_json(resp, 400, "{\"error\":\"invalid job input\"}"); return;
        }
        bf_json_get_str_copy(in, input, sizeof(input));
    }
    if (pr) {
        if (pr->type != BF_JSON_INT || bf_json_get_int(pr) < 1 || bf_json_get_int(pr) > 1000) {
            bf_json_free(doc);
            http_resp_json(resp, 400, "{\"error\":\"priority must be 1..1000\"}"); return;
        }
        priority = (int)bf_json_get_int(pr);
    }
    bf_json_free(doc);
    char scope[128];
    int n = snprintf(scope, sizeof(scope), "job:%s:enqueue", type);
    if (n <= 0 || (size_t)n >= sizeof(scope) || !api_scope_exact(req->auth_token, scope)) {
        http_resp_json(resp, 403, "{\"error\":\"exact job enqueue scope required\"}"); return;
    }
    const char *cli = getenv("BONFYRE_CLI_BINARY");
    if (!cli || !*cli) cli = "bonfyre";
    int known = api_public_command(cli, type);
    if (known <= 0) {
        http_resp_json(resp, known ? 503 : 404,
            known ? "{\"error\":\"native job catalog unavailable\"}" :
                    "{\"error\":\"job type not in public command registry\"}");
        return;
    }

    /* Catalogue presence + API key scope is not permission to enqueue.
     * Refuse until the selected native destination has an active pack and
     * an effect-specific BonfyreReview/WorkGraph decision. The Review owner
     * enqueues approved work itself; generic HTTP must not duplicate it. */
    char context[128]="",pack[96]="",actor[192]="",subject[192]="",digest[65]="";
    if(!query_param(req->query,"context",context,sizeof(context))||
       !query_param(req->query,"pack",pack,sizeof(pack))){
        http_resp_json(resp,400,"{\"error\":\"Select an exact account and installed pack\"}");
        return;
    }
    if(!api_native_preflight(req,context,pack,actor,sizeof(actor),
                             subject,sizeof(subject),digest,sizeof(digest))){
        http_resp_json(resp,403,"{\"schema\":\"bonfyre.command-authority.v1\",\"admitted\":false,\"reason\":\"current_actor_destination_or_pack_refused\"}");
        return;
    }
    http_resp_json(resp,409,
        "{\"schema\":\"bonfyre.command-authority.v1\","
        "\"state\":\"review_required\",\"admitted\":false,"
        "\"command\":\"%s\",\"destination\":\"%s\","
        "\"pack_digest\":\"%s\","
        "\"next_owner\":\"BonfyreReview/BonfyreQueue\"}",
        type,subject,digest);
}

/* GET /api/jobs[/:id] */
static void handle_job_get(HttpResponse *resp, int job_id, const char *api_key) {
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    if (job_id > 0) {
        sqlite3_prepare_v2(g_db,
            "SELECT id,type,status,priority,input_path,output_path,source,worker,"
            "attempts,created_at,claimed_at,completed_at,wall_ms,exit_code,error "
            "FROM jobs WHERE id=? AND api_key=?", -1, &st, NULL);
        sqlite3_bind_int(st, 1, job_id);
        sqlite3_bind_text(st, 2, api_key, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            bf_jsonw out;
            bf_jsonw_init(&out, 0);
            bf_jsonw_obj(&out);
            bf_jsonw_key(&out, "data"); bf_jsonw_obj(&out);
            bf_jsonw_key(&out, "id"); bf_jsonw_int(&out, sqlite3_column_int(st, 0));
            bf_jsonw_key(&out, "type"); bf_jsonw_str(&out, api_sqlite_text(st, 1));
            bf_jsonw_key(&out, "status"); bf_jsonw_str(&out, api_sqlite_text(st, 2));
            bf_jsonw_key(&out, "priority"); bf_jsonw_int(&out, sqlite3_column_int(st, 3));
            bf_jsonw_key(&out, "input"); bf_jsonw_str(&out, api_sqlite_text(st, 4));
            bf_jsonw_key(&out, "output"); bf_jsonw_str(&out, api_sqlite_text(st, 5));
            bf_jsonw_key(&out, "source"); bf_jsonw_str(&out, api_sqlite_text(st, 6));
            bf_jsonw_key(&out, "worker"); bf_jsonw_str(&out, api_sqlite_text(st, 7));
            bf_jsonw_key(&out, "attempts"); bf_jsonw_int(&out, sqlite3_column_int(st, 8));
            bf_jsonw_key(&out, "created_at"); bf_jsonw_str(&out, api_sqlite_text(st, 9));
            bf_jsonw_key(&out, "claimed_at"); bf_jsonw_str(&out, api_sqlite_text(st, 10));
            bf_jsonw_key(&out, "completed_at"); bf_jsonw_str(&out, api_sqlite_text(st, 11));
            bf_jsonw_key(&out, "wall_ms"); bf_jsonw_num(&out, sqlite3_column_double(st, 12));
            bf_jsonw_key(&out, "exit_code"); bf_jsonw_int(&out, sqlite3_column_int(st, 13));
            if (api_sqlite_text(st, 14)[0]) {
                bf_jsonw_key(&out, "error"); bf_jsonw_str(&out, api_sqlite_text(st, 14));
            }
            bf_jsonw_obj_end(&out);
            bf_jsonw_obj_end(&out);
            api_finish_json(resp, &out);
        } else {
            http_resp_json(resp, 404, "{\"error\":\"job not found\"}");
        }
        sqlite3_finalize(st);
    } else {
        sqlite3_prepare_v2(g_db,
            "SELECT id,type,status,priority,source,worker,attempts,"
            "created_at,completed_at,wall_ms,exit_code FROM jobs "
            "WHERE api_key=? ORDER BY id DESC LIMIT 50",
            -1, &st, NULL);
        sqlite3_bind_text(st, 1, api_key, -1, SQLITE_TRANSIENT);
        bf_jsonw out;
        bf_jsonw_init(&out, 0);
        bf_jsonw_obj(&out);
        bf_jsonw_key(&out, "data"); bf_jsonw_arr(&out);
        int i = 0;
        while (sqlite3_step(st) == SQLITE_ROW) {
            bf_jsonw_obj(&out);
            bf_jsonw_key(&out, "id"); bf_jsonw_int(&out, sqlite3_column_int(st, 0));
            bf_jsonw_key(&out, "type"); bf_jsonw_str(&out, api_sqlite_text(st, 1));
            bf_jsonw_key(&out, "status"); bf_jsonw_str(&out, api_sqlite_text(st, 2));
            bf_jsonw_key(&out, "priority"); bf_jsonw_int(&out, sqlite3_column_int(st, 3));
            bf_jsonw_key(&out, "source"); bf_jsonw_str(&out, api_sqlite_text(st, 4));
            bf_jsonw_key(&out, "worker"); bf_jsonw_str(&out, api_sqlite_text(st, 5));
            bf_jsonw_key(&out, "attempts"); bf_jsonw_int(&out, sqlite3_column_int(st, 6));
            bf_jsonw_key(&out, "created_at"); bf_jsonw_str(&out, api_sqlite_text(st, 7));
            bf_jsonw_key(&out, "completed_at"); bf_jsonw_str(&out, api_sqlite_text(st, 8));
            bf_jsonw_key(&out, "wall_ms"); bf_jsonw_num(&out, sqlite3_column_double(st, 9));
            bf_jsonw_key(&out, "exit_code"); bf_jsonw_int(&out, sqlite3_column_int(st, 10));
            bf_jsonw_obj_end(&out);
            i++;
        }
        bf_jsonw_arr_end(&out);
        bf_jsonw_key(&out, "meta"); bf_jsonw_obj(&out);
        bf_jsonw_key(&out, "total"); bf_jsonw_int(&out, i);
        bf_jsonw_obj_end(&out);
        bf_jsonw_obj_end(&out);
        api_finish_json(resp, &out);
        sqlite3_finalize(st);
    }
    pthread_mutex_unlock(&g_db_mutex);
}

/* ── SSE: GET /api/events ─────────────────────────────────────────── */

static void handle_events_sse(HttpRequest *req, int fd) {
    /* Send SSE headers directly — this is a streaming response */
    const char *sse_hdr =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/event-stream\r\n"
        "Cache-Control: no-cache\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "Connection: keep-alive\r\n\r\n";
    write(fd, sse_hdr, strlen(sse_hdr));

    /* Optional filter by job_id */
    char jid_buf[32]; int filter_job = 0;
    if (query_param(req->query, "job_id", jid_buf, sizeof(jid_buf)))
        filter_job = atoi(jid_buf);

    /* Start from last_event_id or latest */
    char since_buf[32]; int since = 0;
    if (query_param(req->query, "since", since_buf, sizeof(since_buf)))
        since = atoi(since_buf);

    while (g_running) {
        /* Revocation must also close an already-open event stream. */
        if (!validate_api_key(req->auth_token)) return;
        pthread_mutex_lock(&g_db_mutex);
        sqlite3_stmt *st;

        if (filter_job > 0) {
            sqlite3_prepare_v2(g_db,
                "SELECT e.id, e.job_id, e.event, e.data, e.created_at FROM events e "
                "JOIN jobs j ON j.id=e.job_id "
                "WHERE e.id > ? AND e.job_id = ? AND j.api_key = ? ORDER BY e.id ASC LIMIT 50",
                -1, &st, NULL);
            sqlite3_bind_int(st, 1, since);
            sqlite3_bind_int(st, 2, filter_job);
            sqlite3_bind_text(st, 3, req->auth_token, -1, SQLITE_TRANSIENT);
        } else {
            sqlite3_prepare_v2(g_db,
                "SELECT e.id, e.job_id, e.event, e.data, e.created_at FROM events e "
                "JOIN jobs j ON j.id=e.job_id "
                "WHERE e.id > ? AND j.api_key = ? ORDER BY e.id ASC LIMIT 50",
                -1, &st, NULL);
            sqlite3_bind_int(st, 1, since);
            sqlite3_bind_text(st, 2, req->auth_token, -1, SQLITE_TRANSIENT);
        }

        int sent = 0;
        while (sqlite3_step(st) == SQLITE_ROW) {
            int eid = sqlite3_column_int(st, 0);
            int jid = sqlite3_column_int(st, 1);
            const char *evt = sqlite3_column_text(st, 2) ?
                (const char *)sqlite3_column_text(st, 2) : "";
            const char *data = sqlite3_column_text(st, 3) ?
                (const char *)sqlite3_column_text(st, 3) : "{}";
            const char *ts = sqlite3_column_text(st, 4) ?
                (const char *)sqlite3_column_text(st, 4) : "";

            char msg[4096];
            int mlen = snprintf(msg, sizeof(msg),
                "id: %d\n"
                "event: %s\n"
                "data: {\"job_id\":%d,\"event\":\"%s\",\"data\":%s,\"ts\":\"%s\"}\n\n",
                eid, evt, jid, evt, data, ts);

            ssize_t wr = write(fd, msg, (size_t)mlen);
            if (wr <= 0) {
                /* Client disconnected */
                sqlite3_finalize(st);
                pthread_mutex_unlock(&g_db_mutex);
                return;
            }
            since = eid;
            sent++;
        }
        sqlite3_finalize(st);
        pthread_mutex_unlock(&g_db_mutex);

        if (sent == 0) usleep(500000);  /* 500ms poll */
    }
}

/* ── FTS5 search: GET /api/search?q=... ───────────────────────────── */

static void handle_search(HttpRequest *req, HttpResponse *resp) {
    char q[512];
    if (!query_param(req->query, "q", q, sizeof(q)) || !q[0]) {
        http_resp_json(resp, 400, "{\"error\":\"missing ?q=term\"}"); return;
    }

    int limit = 20;
    char lbuf[16];
    if (query_param(req->query, "limit", lbuf, sizeof(lbuf)))
        limit = atoi(lbuf);
    if (limit < 1) limit = 1;
    if (limit > 100) limit = 100;

    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db,
        "SELECT j.id, j.type, j.status, j.source, j.created_at, j.wall_ms, "
        "snippet(jobs_fts, 2, '<b>', '</b>', '...', 32) AS snippet, "
        "rank "
        "FROM jobs_fts f "
        "JOIN jobs j ON j.id = f.rowid "
        "WHERE jobs_fts MATCH ? AND j.api_key = ? "
        "ORDER BY rank "
        "LIMIT ?",
        -1, &st, NULL);
    sqlite3_bind_text(st, 1, q, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, req->auth_token, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 3, limit);

    bf_jsonw out;
    bf_jsonw_init(&out, 0);
    bf_jsonw_obj(&out);
    bf_jsonw_key(&out, "query"); bf_jsonw_str(&out, q);
    bf_jsonw_key(&out, "results");
    bf_jsonw_arr(&out);
    int i = 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        bf_jsonw_obj(&out);
        bf_jsonw_key(&out, "id"); bf_jsonw_int(&out, sqlite3_column_int(st, 0));
        bf_jsonw_key(&out, "type"); bf_jsonw_str(&out, api_sqlite_text(st, 1));
        bf_jsonw_key(&out, "status"); bf_jsonw_str(&out, api_sqlite_text(st, 2));
        bf_jsonw_key(&out, "source"); bf_jsonw_str(&out, api_sqlite_text(st, 3));
        bf_jsonw_key(&out, "created_at"); bf_jsonw_str(&out, api_sqlite_text(st, 4));
        bf_jsonw_key(&out, "wall_ms"); bf_jsonw_num(&out, sqlite3_column_double(st, 5));
        bf_jsonw_key(&out, "snippet"); bf_jsonw_str(&out, api_sqlite_text(st, 6));
        bf_jsonw_key(&out, "rank"); bf_jsonw_num(&out, sqlite3_column_double(st, 7));
        bf_jsonw_obj_end(&out);
        i++;
    }
    bf_jsonw_arr_end(&out);
    bf_jsonw_key(&out, "total"); bf_jsonw_int(&out, i);
    bf_jsonw_obj_end(&out);
    api_finish_json(resp, &out);
    sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);
}

/* ── Webhooks: POST/GET /api/webhooks ─────────────────────────────── */

static void handle_webhook_list(HttpResponse *resp, const char *api_key) {
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db,
        "SELECT id, url, events, active, created_at FROM webhooks WHERE api_key=? ORDER BY id",
        -1, &st, NULL);
    sqlite3_bind_text(st, 1, api_key, -1, SQLITE_TRANSIENT);

    bf_jsonw out;
    bf_jsonw_init(&out, 0);
    bf_jsonw_obj(&out);
    bf_jsonw_key(&out, "data");
    bf_jsonw_arr(&out);
    int i = 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        bf_jsonw_obj(&out);
        bf_jsonw_key(&out, "id"); bf_jsonw_int(&out, sqlite3_column_int(st, 0));
        bf_jsonw_key(&out, "url"); bf_jsonw_str(&out, (const char *)sqlite3_column_text(st, 1) ?: "");
        bf_jsonw_key(&out, "events"); bf_jsonw_str(&out, (const char *)sqlite3_column_text(st, 2) ?: "");
        bf_jsonw_key(&out, "active"); bf_jsonw_int(&out, sqlite3_column_int(st, 3));
        bf_jsonw_key(&out, "created_at"); bf_jsonw_str(&out, (const char *)sqlite3_column_text(st, 4) ?: "");
        bf_jsonw_obj_end(&out);
        i++;
    }
    bf_jsonw_arr_end(&out);
    bf_jsonw_key(&out, "total"); bf_jsonw_int(&out, i);
    bf_jsonw_obj_end(&out);
    api_finish_json(resp, &out);
    sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);
}

static void handle_webhook_create(HttpRequest *req, HttpResponse *resp) {
    if (!req->body || req->body_len <= 0) {
        http_resp_json(resp, 400, "{\"error\":\"empty body\"}"); return;
    }

    char url[1024] = "", events[256] = "completed,failed";
    char err[128] = "";
    bf_json_doc_t *doc = bf_json_parse(req->body, (size_t)req->body_len, err, sizeof(err));
    const bf_json_node_t *root = doc ? bf_json_root(doc) : NULL;
    const bf_json_node_t *un = root && root->type == BF_JSON_OBJECT
        ? bf_json_obj_get(doc, root, "url") : NULL;
    const bf_json_node_t *en = root && root->type == BF_JSON_OBJECT
        ? bf_json_obj_get(doc, root, "events") : NULL;
    if (!un || un->type != BF_JSON_STRING || (en && en->type != BF_JSON_STRING)) {
        bf_json_free(doc);
        http_resp_json(resp, 400, "{\"error\":\"webhook requires a URL string and optional events string\"}"); return;
    }
    size_t url_len = 0, events_len = 0;
    char *decoded_url = bf_json_unescape(un->val.str.ptr, un->val.str.len, &url_len);
    char *decoded_events = en ? bf_json_unescape(en->val.str.ptr, en->val.str.len, &events_len) : NULL;
    int invalid = !decoded_url || url_len == 0 || url_len >= sizeof(url) ||
        memchr(decoded_url, 0, url_len) || strpbrk(decoded_url, "\r\n") ||
        (strncmp(decoded_url, "https://", 8) && strncmp(decoded_url, "http://", 7)) ||
        (en && (!decoded_events || events_len == 0 || events_len >= sizeof(events) ||
                memchr(decoded_events, 0, events_len) || strpbrk(decoded_events, "\r\n")));
    if (!invalid) {
        memcpy(url, decoded_url, url_len + 1);
        if (en) memcpy(events, decoded_events, events_len + 1);
    }
    free(decoded_url);
    free(decoded_events);
    bf_json_free(doc);
    if (invalid) {
        http_resp_json(resp, 400, "{\"error\":\"invalid webhook URL or events\"}"); return;
    }

    char ts[64]; iso_now(ts, sizeof(ts));
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(g_db,
        "INSERT INTO webhooks (url, events, active, created_at, api_key) VALUES (?,?,1,?,?)",
        -1, &st, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&g_db_mutex);
        http_resp_json(resp, 503, "{\"error\":\"webhook store unavailable\"}"); return;
    }
    sqlite3_bind_text(st, 1, url, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, events, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 3, ts, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 4, req->auth_token, -1, SQLITE_TRANSIENT);
    int written = sqlite3_step(st) == SQLITE_DONE;
    sqlite3_finalize(st);
    int wid = written ? (int)sqlite3_last_insert_rowid(g_db) : 0;
    pthread_mutex_unlock(&g_db_mutex);
    if (!written) {
        http_resp_json(resp, 503, "{\"error\":\"webhook write failed\"}"); return;
    }
    bf_jsonw out;
    bf_jsonw_init(&out, 0);
    bf_jsonw_obj(&out);
    bf_jsonw_key(&out, "data"); bf_jsonw_obj(&out);
    bf_jsonw_key(&out, "id"); bf_jsonw_int(&out, wid);
    bf_jsonw_key(&out, "url"); bf_jsonw_str(&out, url);
    bf_jsonw_key(&out, "events"); bf_jsonw_str(&out, events);
    bf_jsonw_key(&out, "active"); bf_jsonw_bool(&out, 1);
    bf_jsonw_obj_end(&out);
    bf_jsonw_obj_end(&out);
    api_finish_json(resp, &out);
    if (resp->status == 200) resp->status = 201;
}

/* Proxy: run a Bonfyre command and return its output as JSON. */
static void handle_proxy(HttpResponse *resp, const char *binary, int argc, char **argv) {
    char *run_argv[64];
    run_argv[0] = (char *)binary;
    for (int i = 0; i < argc && i < 62; i++) run_argv[i + 1] = argv[i];
    run_argv[argc + 1] = NULL;

    /* A connection has a 256 KiB stack. Keep the bounded binary result on
     * the heap; the former 1 MiB stack buffer crashed this route on entry. */
    char *result = malloc(MAX_RESULT);
    if (!result) { http_resp_json(resp, 503, "{\"error\":\"command buffer unavailable\"}"); return; }
    int rc = run_binary(binary, run_argv, result, MAX_RESULT);
    if (rc == 0) {
        const char *p = result; while (*p == ' ' || *p == '\n' || *p == '\t') p++;
        if (*p == '{' || *p == '[') {
            free(resp->buf);
            resp->buf_len = strlen(result);
            resp->buf = result;
            result = NULL;
            resp->status = 200;
        } else {
            bf_jsonw out;
            bf_jsonw_init(&out, 0);
            bf_jsonw_obj(&out);
            bf_jsonw_key(&out, "data");
            bf_jsonw_str(&out, result);
            bf_jsonw_obj_end(&out);
            if (out.oom) {
                bf_jsonw_free(&out);
                http_resp_json(resp, 503, "{\"error\":\"command JSON encoding unavailable\"}");
            } else {
                free(resp->buf);
                resp->buf = out.buf;
                resp->buf_len = out.len;
                resp->status = 200;
            }
        }
    } else {
        http_resp_json(resp, 500, "{\"error\":\"binary failed\",\"code\":%d}", rc);
    }
    free(result);
}

/* Discovery delegates to the CLI's sole route catalog. It does not
 * delegate effects: commands must still pass their owning API/MCP policy. */
static void handle_commands(HttpRequest *req, HttpResponse *resp) {
    const char *cli = getenv("BONFYRE_CLI_BINARY");
    if (!cli || !*cli) cli = "bonfyre";
    char query[256], all[16];
    const char *filter = query_param(req->query, "q", query, sizeof(query));
    const char *labs = query_param(req->query, "all", all, sizeof(all));
    int with_labs = labs && !strcmp(labs, "1");
    if (labs && !with_labs && strcmp(labs, "0")) {
        http_resp_json(resp, 400, "{\"error\":\"all must be 0 or 1\"}"); return;
    }
    if (with_labs && !api_scope_exact(req->auth_token, "catalog:all")) {
        http_resp_json(resp, 403, "{\"error\":\"lab catalog scope required\"}"); return;
    }
    char *argv[6] = {(char *)cli, "list", "--json", NULL};
    int ac = 3;
    if (with_labs) argv[ac++] = "--all";
    if (filter && *filter) argv[ac++] = query;
    argv[ac] = NULL;
    char *result = malloc(MAX_RESULT);
    if (!result) { http_resp_json(resp, 503, "{\"error\":\"command catalog buffer unavailable\"}"); return; }
    result[0] = '\0';
    int rc = run_binary(cli, argv, result, MAX_RESULT);
    if (rc != 0) {
        free(result);
        http_resp_json(resp, 503, "{\"error\":\"native command catalog unavailable\"}");
        return;
    }
    size_t n = strnlen(result, MAX_RESULT);
    char err[128] = "";
    bf_json_doc_t *doc = n < MAX_RESULT ? bf_json_parse(result, n, err, sizeof(err)) : NULL;
    const bf_json_node_t *root = doc ? bf_json_root(doc) : NULL;
    const bf_json_node_t *sections = root ? bf_json_obj_get(doc, root, "sections") : NULL;
    const bf_json_node_t *summary = root ? bf_json_obj_get(doc, root, "summary") : NULL;
    int valid = root && root->type == BF_JSON_OBJECT &&
                sections && sections->type == BF_JSON_ARRAY &&
                summary && summary->type == BF_JSON_OBJECT;
    bf_json_free(doc);
    if (!valid) {
        free(result);
        http_resp_json(resp, 503, "{\"error\":\"native command catalog malformed\"}");
        return;
    }
    free(resp->buf);
    resp->buf = result;
    resp->buf_len = n;
    resp->status = 200;
}

/* An exact authority atom in the current API-key scope list. The
 * historical '*' is never an effect grant. Native owners still verify
 * object, actor, provider and workflow admission independently. */
static int api_scope_exact(const char *key, const char *wanted) {
    if (!key || !*key || !wanted || !*wanted) return 0;
    size_t n = strlen(wanted);
    if (n > 511) return 0;
    int permitted = 0;
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(g_db,
            "SELECT scopes FROM api_keys WHERE key=? AND active=1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, key, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const char *scopes = (const char *)sqlite3_column_text(st, 0);
            for (const char *p = scopes; p && *p;) {
                while (*p == ' ' || *p == '\t' || *p == ',') p++;
                const char *start = p;
                while (*p && *p != ',') p++;
                const char *end = p;
                while (end > start && (end[-1] == ' ' || end[-1] == '\t')) end--;
                if ((size_t)(end - start) == (size_t)n && !memcmp(start, wanted, (size_t)n)) {
                    permitted = 1;
                    break;
                }
            }
        }
    }
    sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);
    return permitted;
}

static int api_command_scope(const char *key, const char *command, const char *verb) {
    char wanted[512];
    int n = snprintf(wanted, sizeof(wanted), "command:%s:%s", command, verb);
    return n > 0 && (size_t)n < sizeof(wanted) && api_scope_exact(key, wanted);
}

/* Verify one public CLI identity against the CLI catalog itself: aliases,
 * labs and unregistered local files never become API verbs by convention. */
static int api_public_command(const char *cli, const char *name) {
    char *output = malloc(MAX_RESULT);
    if (!output) return -1;
    char *argv[] = {(char *)cli, "list", "--json", (char *)name, NULL};
    int rc = run_binary(cli, argv, output, MAX_RESULT);
    if (rc != 0) { free(output); return -1; }
    char err[128] = "";
    bf_json_doc_t *doc = bf_json_parse(output, strlen(output), err, sizeof(err));
    if (!doc) { free(output); return -1; }
    const bf_json_node_t *sections = bf_json_obj_get(doc, bf_json_root(doc), "sections");
    int found = 0;
    if (sections && sections->type == BF_JSON_ARRAY) {
        for (const bf_json_node_t *section = bf_json_child_first(doc, sections); section;
             section = bf_json_child_next(doc, section)) {
            const bf_json_node_t *rows = bf_json_obj_get(doc, section, "commands");
            if (!rows || rows->type != BF_JSON_ARRAY) continue;
            for (const bf_json_node_t *row = bf_json_child_first(doc, rows); row;
                 row = bf_json_child_next(doc, row)) {
                const bf_json_node_t *id = bf_json_obj_get(doc, row, "command");
                const bf_json_node_t *tier = bf_json_obj_get(doc, row, "tier");
                const bf_json_node_t *ready = bf_json_obj_get(doc, row, "ready");
                char idbuf[128], tierbuf[32];
                if (!bf_json_get_str_copy(id, idbuf, sizeof(idbuf)) ||
                    !bf_json_get_str_copy(tier, tierbuf, sizeof(tierbuf))) continue;
                if (!strcmp(idbuf, name) && strcmp(tierbuf, "lab") &&
                    ready && ready->type == BF_JSON_BOOL && bf_json_get_bool(ready)) {
                    found = 1;
                    break;
                }
            }
            if (found) break;
        }
    }
    bf_json_free(doc);
    free(output);
    return found;
}

/* GET /api/binaries/:name/... — proxy to any bonfyre binary */
static void handle_binary_proxy(HttpRequest *req, HttpResponse *resp,
                                const char *binary_name, int nseg,
                                char segs[][256]) {
    /* No generic HTTP mutation or credential-bearing body-to-argv bridge.
     * Those effects require their native account / ActorClosure owner. */
    if (strcmp(req->method, "GET") || req->body_len > 0 || nseg < 4) {
        http_resp_json(resp, 403, "{\"error\":\"command execution needs a scoped read route\"}"); return;
    }
    const char *name = !strncmp(binary_name, "bonfyre-", 8) ? binary_name + 8 : binary_name;
    size_t len = strlen(name);
    if (!len || len > 60 || name[0] < 'a' || name[0] > 'z') {
        http_resp_json(resp, 404, "{\"error\":\"unknown command\"}"); return;
    }
    for (size_t i = 0; i < len; i++) {
        char c = name[i];
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-')) {
            http_resp_json(resp, 404, "{\"error\":\"unknown command\"}"); return;
        }
    }
    const char *verb = segs[3];
    /* A per-key route grant cannot classify an arbitrary native effect.
     * Only the common help ABI is proven no-effect across the CLI surface.
     * Other verbs need an explicit native owner effect/read contract. */
    if (strcmp(verb, "--help") && strcmp(verb, "-h") && strcmp(verb, "help")) {
        http_resp_json(resp, 403, "{\"error\":\"native effect classification required\"}"); return;
    }
    if (!api_command_scope(req->auth_token, name, verb)) {
        http_resp_json(resp, 403, "{\"error\":\"exact command scope required\"}"); return;
    }
    const char *cli = getenv("BONFYRE_CLI_BINARY");
    if (!cli || !*cli) cli = "bonfyre";
    int known = api_public_command(cli, name);
    if (known <= 0) {
        http_resp_json(resp, known ? 503 : 404,
            known ? "{\"error\":\"native command registry unavailable\"}" :
                    "{\"error\":\"command not in the public native registry\"}");
        return;
    }
    char *args[32];
    int ac = 0;
    args[ac++] = (char *)name;
    for (int i = 3; i < nseg && ac < 30; i++) args[ac++] = segs[i];
    handle_proxy(resp, cli, ac, args);
}

/* One native read-only closure. Actor and PackGraph are checked by their
 * owners; no reviewed effect, WorkGraph admission or effect capability follows. */
static void api_authority_preflight(HttpRequest *req,HttpResponse *resp) {
    char context[128]="",pack[128]="",actor[192]="",subject[192]="",digest[65]="";
    if(!query_param(req->query,"context",context,sizeof(context))||
       !query_param(req->query,"pack",pack,sizeof(pack))){
        http_resp_json(resp,400,"{\"error\":\"Select an exact account context and installed pack\"}");return;
    }
    if(!api_native_preflight(req,context,pack,actor,sizeof(actor),
                             subject,sizeof(subject),digest,sizeof(digest))){
        http_resp_json(resp,403,"{\"schema\":\"bonfyre.command-authority.v1\",\"admitted\":false,\"reason\":\"current_actor_destination_or_pack_refused\"}");return;
    }
    bf_jsonw out;bf_jsonw_init(&out,0);bf_jsonw_obj(&out);
    bf_jsonw_key(&out,"schema");bf_jsonw_str(&out,"bonfyre.command-authority.v1");
    bf_jsonw_key(&out,"actor");bf_jsonw_str(&out,actor);
    bf_jsonw_key(&out,"destination");bf_jsonw_str(&out,subject);
    bf_jsonw_key(&out,"pack_digest");bf_jsonw_str(&out,digest);
    bf_jsonw_key(&out,"actor_and_pack_current");bf_jsonw_bool(&out,1);
    bf_jsonw_key(&out,"admitted");bf_jsonw_bool(&out,0);
    bf_jsonw_key(&out,"next_required");
    bf_jsonw_str(&out,"Native reviewed effect and WorkGraph admission for the bound destination");
    bf_jsonw_obj_end(&out);api_finish_json(resp,&out);
}

/* ── Main router ──────────────────────────────────────────────────── */

static void route_request(HttpRequest *req, HttpResponse *resp) {
    char segs[MAX_PATH_SEGS][256];
    memset(segs, 0, sizeof(segs));
    int nseg = path_segments(req->path, segs, MAX_PATH_SEGS);

    /* CORS preflight */
    if (strcmp(req->method, "OPTIONS") == 0) {
        http_resp_json(resp, 204, ""); return;
    }

    /* /api/ routes */
    if (nseg >= 2 && strcmp(segs[0], "api") == 0) {
        const char *ep = segs[1];

        /* Auth check for non-health endpoints */
        if (strcmp(ep, "health") != 0) {
            if (!validate_api_key(req->auth_token)) {
                http_resp_json(resp, 401, "{\"error\":\"invalid API key\"}"); return;
            }
            if (!rate_limit_check(req->auth_token)) {
                http_resp_json(resp, 429,
                    "{\"error\":\"rate limit exceeded\",\"limit\":%d,\"window\":\"per minute\"}",
                    RL_CAPACITY); return;
            }
        }

        if (strcmp(ep, "health") == 0)  { handle_health(resp); return; }
        if (strcmp(ep, "status") == 0)  { handle_status(resp); return; }
        if (strcmp(ep, "commands") == 0 && strcmp(req->method, "GET") == 0) {
            handle_commands(req, resp); return;
        }

        if (strcmp(ep, "authority") == 0 && strcmp(req->method, "GET") == 0) {
            api_authority_preflight(req, resp); return;
        }

        if (strcmp(ep, "upload") == 0 && strcmp(req->method, "POST") == 0) {
            if (!api_scope_exact(req->auth_token, "api:upload:create")) {
                http_resp_json(resp, 403, "{\"error\":\"upload creation scope required\"}"); return;
            }
            handle_upload(req, resp); return;
        }

        if (strcmp(ep, "jobs") == 0) {
            if (strcmp(req->method, "POST") == 0) {
                if (!api_scope_exact(req->auth_token, "api:jobs:create")) {
                    http_resp_json(resp, 403, "{\"error\":\"job creation scope required\"}"); return;
                }
                handle_job_submit(req, resp); return;
            }
            int jid = nseg >= 3 ? atoi(segs[2]) : 0;
            handle_job_get(resp, jid, req->auth_token); return;
        }

        /* SSE stream — handled specially (does not use HttpResponse) */
        if (strcmp(ep, "events") == 0 && strcmp(req->method, "GET") == 0) {
            handle_events_sse(req, resp->fd);
            /* SSE writes directly to fd, set status to -1 to skip normal send */
            resp->status = -1;
            return;
        }

        if (strcmp(ep, "search") == 0 && strcmp(req->method, "GET") == 0) {
            handle_search(req, resp); return;
        }

        if (strcmp(ep, "webhooks") == 0) {
            if (strcmp(req->method, "POST") == 0) {
                if (!api_scope_exact(req->auth_token, "api:webhooks:create")) {
                    http_resp_json(resp, 403, "{\"error\":\"webhook creation scope required\"}"); return;
                }
                handle_webhook_create(req, resp); return;
            }
            if (!api_scope_exact(req->auth_token, "api:webhooks:read")) {
                http_resp_json(resp, 403, "{\"error\":\"webhook read scope required\"}"); return;
            }
            handle_webhook_list(resp, req->auth_token); return;
        }

        if (strcmp(ep, "binaries") == 0 && nseg >= 3) {
            handle_binary_proxy(req, resp, segs[2], nseg, segs); return;
        }

        http_resp_json(resp, 404, "{\"error\":\"unknown endpoint\"}");
        return;
    }

    if (serve_static(resp, req->path)) return;
    http_resp_json(resp, 404, "{\"error\":\"not found\"}");
}

/* ── Server ───────────────────────────────────────────────────────── */

static void *connection_handler(void *arg) {
    int fd = *(int *)arg; free(arg);
    double t0 = mono_sec();

    HttpRequest *req = malloc(sizeof(HttpRequest));
    if (!req) { close(fd); atomic_fetch_sub(&g_thread_count, 1); return NULL; }
    int parsed = parse_http_request(fd, req);
    if (parsed != 0) {
        HttpResponse reject;
        http_resp_init(&reject, fd);
        http_resp_json(&reject, parsed == -2 ? 413 : 400,
            parsed == -2 ? "{\"error\":\"request body exceeds 4 MiB\"}" :
                            "{\"error\":\"incomplete or invalid HTTP request\"}");
        http_resp_send(&reject);
        http_resp_free(&reject);
        free_request(req);
        free(req); close(fd); atomic_fetch_sub(&g_thread_count, 1); return NULL;
    }

    HttpResponse resp;
    http_resp_init(&resp, fd);
    route_request(req, &resp);

    if (resp.status >= 0) http_resp_send(&resp);

    double wall_ms = (mono_sec() - t0) * 1000.0;
    log_request(req->method, req->path, req->auth_token, resp.status, wall_ms);

    http_resp_free(&resp);
    free_request(req);
    free(req);
    close(fd);
    atomic_fetch_sub(&g_thread_count, 1);
    return NULL;
}

static int start_server(int port) {
    signal(SIGPIPE, SIG_IGN);

    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sfd < 0) { perror("socket"); return -1; }
    int opt = 1; setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons((uint16_t)port);

    if (bind(sfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(sfd); return -1;
    }
    if (listen(sfd, 128) < 0) { perror("listen"); close(sfd); return -1; }

    fprintf(stderr,
        "[bonfyre-api] v%s listening on http://0.0.0.0:%d\n"
        "[bonfyre-api] static: %s\n"
        "[bonfyre-api] uploads: %s\n"
        "[bonfyre-api] endpoints:\n"
        "  GET  /api/health              Health check\n"
        "  GET  /api/status              System dashboard\n"
        "  POST /api/upload              File upload\n"
        "  POST /api/jobs                Submit async job\n"
        "  GET  /api/jobs[/:id]          Job status/list\n"
        "  GET  /api/events              SSE stream (real-time)\n"
        "  GET  /api/search?q=term       FTS5 full-text search\n"
        "  POST /api/webhooks            Register webhook\n"
        "  GET  /api/webhooks            List webhooks\n"
        "  *    /api/binaries/:name/...  Proxy to any binary\n"
        "  GET  /*                       Static files / SPA\n",
        VERSION, port,
        g_static_dir[0] ? g_static_dir : "(none)",
        g_upload_dir);

    while (g_running) {
        struct sockaddr_in ca; socklen_t cl = sizeof(ca);
        int cfd = accept(sfd, (struct sockaddr *)&ca, &cl);
        if (cfd < 0) { if (!g_running) break; continue; }

        int one = 1;
        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        if (atomic_load(&g_thread_count) >= MAX_THREADS) {
            const char *busy = "HTTP/1.1 503 Service Unavailable\r\n"
                "Content-Length: 0\r\nConnection: close\r\n\r\n";
            write(cfd, busy, strlen(busy));
            close(cfd);
            continue;
        }
        atomic_fetch_add(&g_thread_count, 1);

        int *pfd = malloc(sizeof(int)); *pfd = cfd;
        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_attr_setstacksize(&attr, THREAD_STACK);
        pthread_create(&tid, &attr, connection_handler, pfd);
        pthread_attr_destroy(&attr);
    }
    close(sfd);
    return 0;
}

static void handle_signal(int sig) { (void)sig; g_running = 0; }

/* ── CLI ──────────────────────────────────────────────────────────── */

static const char *arg_get(int argc, char **argv, const char *flag) {
    for (int i = 0; i < argc - 1; i++)
        if (strcmp(argv[i], flag) == 0) return argv[i + 1];
    return NULL;
}

static void usage(void) {
    fprintf(stderr,
        "BonfyreAPI v%s — Async HTTP gateway with SSE, search, webhooks\n\n"
        "Usage:\n"
        "  bonfyre-api serve [--port 8080] [--db FILE] [--static DIR] [--uploads DIR]\n"
        "  bonfyre-api status\n"
        "  bonfyre-api layer get <artifact_id> [--root DIR]\n"
        "  bonfyre-api layer lineage <artifact_id> [--root DIR]\n"
        "  bonfyre-api layer compat <artifact_a> <artifact_b> [--root DIR]\n"
        "  bonfyre-api layer compose <artifact_a> <artifact_b> [--dry-run] [--root DIR]\n\n"
        "Shares SQLite WAL database with BonfyreQueue (default: ~/.local/share/bonfyre/queue.db).\n"
        "Jobs are enqueued asynchronously — run `bonfyre-queue work` to process them.\n",
        VERSION);
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    if (argc < 2) { usage(); return 1; }

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    const char *cmd = NULL;
    for (int i = 1; i < argc; i++) {
        if (argv[i][0] != '-') { cmd = argv[i]; break; }
        if (strcmp(argv[i], "--db") == 0 || strcmp(argv[i], "--port") == 0 ||
            strcmp(argv[i], "--static") == 0 || strcmp(argv[i], "--uploads") == 0) { i++; continue; }
    }
    if (!cmd) { usage(); return 1; }

    if (strcmp(cmd, "layer") == 0) {
        return delegate_layer_api(argc, argv);
    }

    if (strcmp(cmd, "serve") == 0) {
        int port = 8080;
        const char *p = arg_get(argc, argv, "--port");
        if (p) port = atoi(p);

        const char *db_path = arg_get(argc, argv, "--db");
        if (!db_path) db_path = default_db();

        const char *sd = arg_get(argc, argv, "--static");
        if (sd) strncpy(g_static_dir, sd, sizeof(g_static_dir) - 1);

        const char *ud = arg_get(argc, argv, "--uploads");
        if (!ud) {
            const char *h = getenv("HOME");
            snprintf(g_upload_dir, sizeof(g_upload_dir), "%s/.local/share/bonfyre/uploads", h ? h : ".");
        } else {
            strncpy(g_upload_dir, ud, sizeof(g_upload_dir) - 1);
        }
        mkdir(g_upload_dir, 0755);

        g_db = open_db(db_path);
        if (!g_db) return 1;

        int rc = start_server(port);
        sqlite3_close(g_db);
        return rc;

    } else if (strcmp(cmd, "status") == 0) {
        printf("BonfyreAPI v%s\n", VERSION);
        printf("Use 'bonfyre-api serve' to start, then GET /api/status for live dashboard.\n");
        return 0;

    } else {
        usage(); return 1;
    }
}
