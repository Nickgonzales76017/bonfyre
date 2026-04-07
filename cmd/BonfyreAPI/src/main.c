/*
 * BonfyreAPI — HTTP gateway for the Bonfyre binary family.
 *
 * Provides a unified REST API over all Bonfyre binaries:
 *   - File upload (multipart → temp file → pipeline)
 *   - Job management (submit, status, results)
 *   - Proxy to subsystems (gate, meter, finance, outreach, graph, ledger)
 *   - Static file serving (frontend)
 *   - CORS, auth forwarding, JSON responses
 *
 * Usage:
 *   bonfyre-api serve [--port 8080] [--db FILE] [--static DIR]
 *   bonfyre-api status
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

#define VERSION       "1.0.0"
#define MAX_BODY      (4*1024*1024)  /* 4MB uploads */
#define MAX_PATH_SEGS 16
#define MAX_PATH_LEN  2048
#define MAX_RESULT    (1024*1024)    /* 1MB result buffer */
#define MAX_THREADS   64             /* P4: connection limit */
#define THREAD_STACK  (256*1024)     /* P4: 256KB vs 8MB */

static volatile int g_running = 1;
static atomic_int g_thread_count = 0;
static sqlite3 *g_db = NULL;
static pthread_mutex_t g_db_mutex = PTHREAD_MUTEX_INITIALIZER;
static char g_static_dir[MAX_PATH_LEN] = "";
static char g_upload_dir[MAX_PATH_LEN] = "";

/* ── HTTP primitives (matches BonfyreCMS pattern) ─────────────────── */

typedef struct {
    char method[16];
    char path[MAX_PATH_LEN];
    char query[MAX_PATH_LEN];
    char *body;
    int body_len;
    char auth_token[256];
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
    memset(r,0,sizeof(*r));
    r->fd=fd; r->status=200; r->buf=NULL; r->buf_len=0;
    strcpy(r->content_type,"application/json");
}
static void http_resp_free(HttpResponse *r) { free(r->buf); r->buf=NULL; }

static void http_resp_send(HttpResponse *r) {
    const char *st="OK";
    if (r->status==201) st="Created";
    else if (r->status==204) st="No Content";
    else if (r->status==400) st="Bad Request";
    else if (r->status==401) st="Unauthorized";
    else if (r->status==404) st="Not Found";
    else if (r->status==405) st="Method Not Allowed";
    else if (r->status==500) st="Internal Server Error";

    char hdr[1024];
    int hlen=snprintf(hdr,sizeof(hdr),
        "HTTP/1.1 %d %s\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n"
        "Access-Control-Allow-Headers: Authorization, Content-Type\r\n"
        "Connection: close\r\n\r\n",
        r->status,st,r->content_type,r->buf_len);
    write(r->fd,hdr,(size_t)hlen);
    if (r->buf_len>0 && r->buf) write(r->fd,r->buf,r->buf_len);
}

static void http_resp_json(HttpResponse *r, int status, const char *fmt, ...) {
    r->status=status;
    free(r->buf);
    /* P4: vasprintf avoids 8KB stack buffer + copy */
    va_list ap; va_start(ap,fmt);
    int n=vasprintf(&r->buf,fmt,ap);
    va_end(ap);
    r->buf_len=(size_t)(n>0?n:0);
}

static int parse_http_request(int fd, HttpRequest *req) {
    memset(req,0,sizeof(*req));
    req->body=NULL;

    char hdr[8192];
    ssize_t total=0;
    while (total<(ssize_t)sizeof(hdr)-1) {
        ssize_t n=read(fd,hdr+total,sizeof(hdr)-(size_t)total-1);
        if (n<=0) break;
        total+=n; hdr[total]='\0';
        if (strstr(hdr,"\r\n\r\n")) break;
    }
    if (total<=0) return -1;
    hdr[total]='\0';

    char *body_marker=strstr(hdr,"\r\n\r\n");
    size_t header_end=body_marker?(size_t)(body_marker-hdr):(size_t)total;

    char *line_end=strstr(hdr,"\r\n");
    if (!line_end) return -1;
    *line_end='\0';
    sscanf(hdr,"%15s %2047s",req->method,req->path);

    char *qm=strchr(req->path,'?');
    if (qm) { *qm='\0'; strncpy(req->query,qm+1,sizeof(req->query)-1); }

    char *hp=line_end+2;
    while (hp<hdr+header_end) {
        char *he=strstr(hp,"\r\n");
        if (!he||he==hp) break;
        *he='\0';
        if (strncasecmp(hp,"Content-Length:",15)==0)
            req->content_length=atoi(hp+15);
        if (strncasecmp(hp,"Content-Type:",13)==0) {
            const char *v=hp+13; while(*v==' ')v++;
            strncpy(req->content_type,v,sizeof(req->content_type)-1);
        }
        if (strncasecmp(hp,"Authorization:",14)==0) {
            const char *av=hp+14; while(*av==' ')av++;
            if (strncasecmp(av,"Bearer ",7)==0) av+=7;
            strncpy(req->auth_token,av,sizeof(req->auth_token)-1);
        }
        hp=he+2;
    }

    /* Body */
    if (body_marker && req->content_length>0) {
        int cap=req->content_length<MAX_BODY?req->content_length:MAX_BODY;
        req->body=malloc((size_t)cap+1);
        char *body_start=body_marker+4;
        size_t in_hdr=(size_t)(total-(body_start-hdr));
        if (in_hdr>0) {
            size_t cp=in_hdr<(size_t)cap?in_hdr:(size_t)cap;
            memcpy(req->body,body_start,cp);
            req->body_len=(int)cp;
        }
        while (req->body_len<cap) {
            ssize_t n=read(fd,req->body+req->body_len,(size_t)(cap-req->body_len));
            if (n<=0) break;
            req->body_len+=(int)n;
        }
        req->body[req->body_len]='\0';
    }
    return 0;
}

static void free_request(HttpRequest *req) { free(req->body); }

/* ── Utilities ────────────────────────────────────────────────────── */

static void iso_now(char *buf, size_t sz) {
    time_t t=time(NULL); struct tm tm; gmtime_r(&t,&tm);
    strftime(buf,sz,"%Y-%m-%dT%H:%M:%SZ",&tm);
}

static int path_segments(const char *path, char segs[][256], int max) {
    int c=0; const char *p=path;
    while (*p=='/') p++;
    while (*p && c<max) {
        size_t i=0;
        while (*p && *p!='/' && i<255) segs[c][i++]=*p++;
        segs[c][i]='\0'; c++;
        while (*p=='/') p++;
    }
    return c;
}

/* Run a binary and capture stdout as JSON result */
static int run_binary(const char *bin, char *const argv[], char *out, size_t out_sz) {
    int pipefd[2];
    if (pipe(pipefd)<0) return -1;

    pid_t pid=fork();
    if (pid<0) { close(pipefd[0]); close(pipefd[1]); return -1; }
    if (pid==0) {
        close(pipefd[0]);
        dup2(pipefd[1],STDOUT_FILENO);
        dup2(pipefd[1],STDERR_FILENO);
        close(pipefd[1]);
        execvp(bin,argv);
        _exit(127);
    }
    close(pipefd[1]);
    size_t total=0;
    while (total<out_sz-1) {
        ssize_t n=read(pipefd[0],out+total,out_sz-total-1);
        if (n<=0) break;
        total+=(size_t)n;
    }
    out[total]='\0';
    close(pipefd[0]);
    int status; waitpid(pid,&status,0);
    return WIFEXITED(status)?WEXITSTATUS(status):-1;
}

/* ── Database (job tracking) ──────────────────────────────────────── */

static const char *SCHEMA_SQL =
    "CREATE TABLE IF NOT EXISTS jobs ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  type TEXT NOT NULL,"
    "  status TEXT NOT NULL DEFAULT 'queued',"
    "  input_path TEXT,"
    "  output_path TEXT,"
    "  api_key TEXT,"
    "  created_at TEXT NOT NULL,"
    "  started_at TEXT,"
    "  completed_at TEXT,"
    "  error TEXT,"
    "  result_json TEXT"
    ");"
    "CREATE TABLE IF NOT EXISTS uploads ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  filename TEXT NOT NULL,"
    "  path TEXT NOT NULL,"
    "  size_bytes INTEGER,"
    "  content_type TEXT,"
    "  uploaded_at TEXT NOT NULL,"
    "  api_key TEXT"
    ");";

static sqlite3 *open_db(const char *path) {
    sqlite3 *db;
    if (sqlite3_open(path,&db)!=SQLITE_OK) {
        fprintf(stderr,"Cannot open %s: %s\n",path,sqlite3_errmsg(db));
        return NULL;
    }
    char *err=NULL;
    if (sqlite3_exec(db,SCHEMA_SQL,NULL,NULL,&err)!=SQLITE_OK) {
        fprintf(stderr,"Schema error: %s\n",err);
        sqlite3_free(err); sqlite3_close(db); return NULL;
    }
    return db;
}

static const char *default_db(void) {
    static char p[MAX_PATH_LEN];
    const char *h=getenv("HOME");
    snprintf(p,sizeof(p),"%s/.local/share/bonfyre/api.db",h?h:".");
    char d[MAX_PATH_LEN];
    snprintf(d,sizeof(d),"%s/.local/share/bonfyre",h?h:".");
    mkdir(d,0755);
    return p;
}

/* ── Static file serving ──────────────────────────────────────────── */

static const char *mime_for_ext(const char *path) {
    const char *dot=strrchr(path,'.');
    if (!dot) return "application/octet-stream";
    if (strcasecmp(dot,".html")==0||strcasecmp(dot,".htm")==0) return "text/html";
    if (strcasecmp(dot,".css")==0) return "text/css";
    if (strcasecmp(dot,".js")==0) return "application/javascript";
    if (strcasecmp(dot,".json")==0) return "application/json";
    if (strcasecmp(dot,".png")==0) return "image/png";
    if (strcasecmp(dot,".jpg")==0||strcasecmp(dot,".jpeg")==0) return "image/jpeg";
    if (strcasecmp(dot,".gif")==0) return "image/gif";
    if (strcasecmp(dot,".svg")==0) return "image/svg+xml";
    if (strcasecmp(dot,".ico")==0) return "image/x-icon";
    if (strcasecmp(dot,".woff2")==0) return "font/woff2";
    if (strcasecmp(dot,".woff")==0) return "font/woff";
    return "application/octet-stream";
}

static int serve_static(HttpResponse *resp, const char *url_path) {
    if (!g_static_dir[0]) return 0;

    /* Security: reject path traversal */
    if (strstr(url_path,"..")) return 0;

    char fpath[MAX_PATH_LEN];
    if (strcmp(url_path,"/")==0 || strcmp(url_path,"")==0)
        snprintf(fpath,sizeof(fpath),"%s/index.html",g_static_dir);
    else
        snprintf(fpath,sizeof(fpath),"%s%s",g_static_dir,url_path);

    /* Resolve real path and verify it's under static dir */
    char resolved[MAX_PATH_LEN];
    if (!realpath(fpath, resolved)) return 0;
    char resolved_root[MAX_PATH_LEN];
    if (!realpath(g_static_dir, resolved_root)) return 0;
    if (strncmp(resolved, resolved_root, strlen(resolved_root)) != 0) return 0;

    FILE *f=fopen(resolved,"rb");
    if (!f) {
        /* SPA fallback: serve index.html for non-API, non-file paths */
        snprintf(fpath,sizeof(fpath),"%s/index.html",g_static_dir);
        f=fopen(fpath,"rb");
        if (!f) return 0;
    }
    fseek(f,0,SEEK_END); long sz=ftell(f); fseek(f,0,SEEK_SET);
    if (sz<=0 || sz>10*1024*1024) { fclose(f); return 0; }

    free(resp->buf);
    resp->buf=malloc((size_t)sz);
    resp->buf_len=fread(resp->buf,1,(size_t)sz,f);
    fclose(f);
    resp->status=200;
    strncpy(resp->content_type,mime_for_ext(resolved),sizeof(resp->content_type)-1);
    return 1;
}

/* ── API routes ───────────────────────────────────────────────────── */

/* POST /api/upload — accept file upload */
static void handle_upload(HttpRequest *req, HttpResponse *resp) {
    if (!req->body || req->body_len<=0) {
        http_resp_json(resp,400,"{\"error\":\"empty body\"}"); return;
    }

    /* Generate unique filename */
    char ts[64]; iso_now(ts,sizeof(ts));
    char fname[256];
    snprintf(fname,sizeof(fname),"upload_%ld_%d",(long)time(NULL),getpid());

    /* Extract original filename from query or content-type */
    char *fn_param=strstr(req->query,"filename=");
    if (fn_param) {
        fn_param+=9;
        /* Sanitize: only allow alnum, dash, underscore, dot */
        char safe[128]; int si=0;
        while (*fn_param && *fn_param!='&' && si<(int)sizeof(safe)-1) {
            char c=*fn_param++;
            if ((c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='-'||c=='_'||c=='.')
                safe[si++]=c;
        }
        safe[si]='\0';
        if (si>0) snprintf(fname,sizeof(fname),"%ld_%s",(long)time(NULL),safe);
    }

    char fpath[MAX_PATH_LEN];
    snprintf(fpath,sizeof(fpath),"%s/%s",g_upload_dir,fname);

    FILE *f=fopen(fpath,"wb");
    if (!f) { http_resp_json(resp,500,"{\"error\":\"cannot write file\"}"); return; }
    fwrite(req->body,1,(size_t)req->body_len,f);
    fclose(f);

    /* Record in DB */
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db,
        "INSERT INTO uploads (filename,path,size_bytes,content_type,uploaded_at,api_key) VALUES (?,?,?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,fname,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,fpath,-1,SQLITE_STATIC);
    sqlite3_bind_int(st,3,req->body_len);
    sqlite3_bind_text(st,4,req->content_type,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,5,ts,-1,SQLITE_STATIC);
    if (req->auth_token[0]) sqlite3_bind_text(st,6,req->auth_token,-1,SQLITE_STATIC);
    else sqlite3_bind_null(st,6);
    sqlite3_step(st); sqlite3_finalize(st);
    int upload_id=(int)sqlite3_last_insert_rowid(g_db);
    pthread_mutex_unlock(&g_db_mutex);

    http_resp_json(resp,201,
        "{\"data\":{\"id\":%d,\"filename\":\"%s\",\"size\":%d},\"meta\":{\"uploaded\":true}}",
        upload_id,fname,req->body_len);
}

/* POST /api/jobs — submit a pipeline job */
static void handle_job_submit(HttpRequest *req, HttpResponse *resp) {
    if (!req->body || req->body_len<=0) {
        http_resp_json(resp,400,"{\"error\":\"empty body\"}"); return;
    }

    /* Parse minimal JSON: {"type":"transcribe","input":"/path/to/file"} */
    char type[64]="", input[MAX_PATH_LEN]="";
    const char *p=req->body;
    /* Simple JSON field extraction */
    const char *tf=strstr(p,"\"type\"");
    if (tf) {
        tf=strchr(tf+6,'"'); if (tf) { tf++;
            int i=0; while (*tf && *tf!='"' && i<63) type[i++]=*tf++;
            type[i]='\0';
        }
    }
    const char *inf=strstr(p,"\"input\"");
    if (inf) {
        inf=strchr(inf+7,'"'); if (inf) { inf++;
            int i=0; while (*inf && *inf!='"' && i<(int)sizeof(input)-1) input[i++]=*inf++;
            input[i]='\0';
        }
    }

    if (!type[0]) { http_resp_json(resp,400,"{\"error\":\"missing type\"}"); return; }

    char ts[64]; iso_now(ts,sizeof(ts));

    /* Create output directory */
    char outdir[MAX_PATH_LEN];
    snprintf(outdir,sizeof(outdir),"%s/job_%ld_%d",g_upload_dir,(long)time(NULL),getpid());
    mkdir(outdir,0755);

    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db,
        "INSERT INTO jobs (type,status,input_path,output_path,api_key,created_at) VALUES (?,?,?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,type,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,"queued",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,input,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,4,outdir,-1,SQLITE_STATIC);
    if (req->auth_token[0]) sqlite3_bind_text(st,5,req->auth_token,-1,SQLITE_STATIC);
    else sqlite3_bind_null(st,5);
    sqlite3_bind_text(st,6,ts,-1,SQLITE_STATIC);
    sqlite3_step(st); sqlite3_finalize(st);
    int job_id=(int)sqlite3_last_insert_rowid(g_db);
    pthread_mutex_unlock(&g_db_mutex);

    /* Execute the binary synchronously for now */
    char job_id_str[32]; snprintf(job_id_str,sizeof(job_id_str),"%d",job_id);

    /* Mark running */
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_prepare_v2(g_db,
        "UPDATE jobs SET status='running',started_at=? WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,ts,-1,SQLITE_STATIC);
    sqlite3_bind_int(st,2,job_id);
    sqlite3_step(st); sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);

    /* Route to appropriate binary */
    /* P4: heap-alloc result buffer instead of 1MB on stack */
    char *result=malloc(MAX_RESULT);
    if (!result) {
        http_resp_json(resp,500,"{\"error\":\"out of memory\"}");
        return;
    }
    char bin_name[64];
    snprintf(bin_name,sizeof(bin_name),"bonfyre-%s",type);

    char *argv_run[]={bin_name,input,"--out",outdir,NULL};
    int rc=run_binary(bin_name,argv_run,result,MAX_RESULT);

    char done_ts[64]; iso_now(done_ts,sizeof(done_ts));

    pthread_mutex_lock(&g_db_mutex);
    if (rc==0) {
        sqlite3_prepare_v2(g_db,
            "UPDATE jobs SET status='completed',completed_at=?,result_json=? WHERE id=?",-1,&st,NULL);
        sqlite3_bind_text(st,1,done_ts,-1,SQLITE_STATIC);
        sqlite3_bind_text(st,2,result,-1,SQLITE_STATIC);
        sqlite3_bind_int(st,3,job_id);
    } else {
        sqlite3_prepare_v2(g_db,
            "UPDATE jobs SET status='failed',completed_at=?,error=? WHERE id=?",-1,&st,NULL);
        sqlite3_bind_text(st,1,done_ts,-1,SQLITE_STATIC);
        sqlite3_bind_text(st,2,result,-1,SQLITE_STATIC);
        sqlite3_bind_int(st,3,job_id);
    }
    sqlite3_step(st); sqlite3_finalize(st);
    pthread_mutex_unlock(&g_db_mutex);

    free(result);  /* P4: heap-allocated result buffer */

    http_resp_json(resp,201,
        "{\"data\":{\"id\":%d,\"type\":\"%s\",\"status\":\"%s\"},\"meta\":{\"created\":true}}",
        job_id,type,rc==0?"completed":"failed");
}

/* GET /api/jobs[/:id] */
static void handle_job_get(HttpResponse *resp, int job_id) {
    pthread_mutex_lock(&g_db_mutex);
    sqlite3_stmt *st;
    if (job_id>0) {
        sqlite3_prepare_v2(g_db,
            "SELECT id,type,status,input_path,output_path,created_at,started_at,completed_at,error FROM jobs WHERE id=?",
            -1,&st,NULL);
        sqlite3_bind_int(st,1,job_id);
        if (sqlite3_step(st)==SQLITE_ROW) {
            const char *type_v=sqlite3_column_text(st,1)?(const char*)sqlite3_column_text(st,1):"";
            const char *stat_v=sqlite3_column_text(st,2)?(const char*)sqlite3_column_text(st,2):"";
            const char *inp_v=sqlite3_column_text(st,3)?(const char*)sqlite3_column_text(st,3):"";
            const char *out_v=sqlite3_column_text(st,4)?(const char*)sqlite3_column_text(st,4):"";
            const char *cre_v=sqlite3_column_text(st,5)?(const char*)sqlite3_column_text(st,5):"";
            const char *sta_v=sqlite3_column_text(st,6)?(const char*)sqlite3_column_text(st,6):"";
            const char *com_v=sqlite3_column_text(st,7)?(const char*)sqlite3_column_text(st,7):"";
            const char *err_v=sqlite3_column_text(st,8)?(const char*)sqlite3_column_text(st,8):"";
            http_resp_json(resp,200,
                "{\"data\":{\"id\":%d,\"type\":\"%s\",\"status\":\"%s\","
                "\"input\":\"%s\",\"output\":\"%s\","
                "\"created_at\":\"%s\",\"started_at\":\"%s\",\"completed_at\":\"%s\""
                "%s%s}}",
                job_id,type_v,stat_v,inp_v,out_v,cre_v,sta_v,com_v,
                err_v[0]?",\"error\":\"":"",err_v[0]?err_v:"");
            /* Close error quote if present */
            if (err_v[0]) {
                /* Rebuild with proper quoting */
                http_resp_json(resp,200,
                    "{\"data\":{\"id\":%d,\"type\":\"%s\",\"status\":\"%s\","
                    "\"input\":\"%s\",\"output\":\"%s\","
                    "\"created_at\":\"%s\",\"started_at\":\"%s\",\"completed_at\":\"%s\","
                    "\"error\":\"%s\"}}",
                    job_id,type_v,stat_v,inp_v,out_v,cre_v,sta_v,com_v,err_v);
            }
        } else {
            http_resp_json(resp,404,"{\"error\":\"job not found\"}");
        }
        sqlite3_finalize(st);
    } else {
        /* List all jobs */
        sqlite3_prepare_v2(g_db,
            "SELECT id,type,status,created_at,completed_at FROM jobs ORDER BY id DESC LIMIT 50",
            -1,&st,NULL);
        free(resp->buf); resp->buf=NULL; resp->buf_len=0;
        FILE *mem=open_memstream(&resp->buf,&resp->buf_len);
        fprintf(mem,"{\"data\":[");
        int i=0;
        while (sqlite3_step(st)==SQLITE_ROW) {
            if (i>0) fprintf(mem,",");
            const char *type_v=sqlite3_column_text(st,1)?(const char*)sqlite3_column_text(st,1):"";
            const char *stat_v=sqlite3_column_text(st,2)?(const char*)sqlite3_column_text(st,2):"";
            const char *cre_v=sqlite3_column_text(st,3)?(const char*)sqlite3_column_text(st,3):"";
            const char *com_v=sqlite3_column_text(st,4)?(const char*)sqlite3_column_text(st,4):"";
            fprintf(mem,"{\"id\":%d,\"type\":\"%s\",\"status\":\"%s\",\"created_at\":\"%s\",\"completed_at\":\"%s\"}",
                sqlite3_column_int(st,0),type_v,stat_v,cre_v,com_v);
            i++;
        }
        fprintf(mem,"],\"meta\":{\"total\":%d}}",i);
        fclose(mem);
        resp->status=200;
        sqlite3_finalize(st);
    }
    pthread_mutex_unlock(&g_db_mutex);
}

/* Proxy: run a bonfyre-* binary and return output as JSON */
static void handle_proxy(HttpResponse *resp, const char *binary, int argc, char **argv) {
    /* Build argv for the binary */
    char *run_argv[64];
    run_argv[0]=(char*)binary;
    for (int i=0;i<argc && i<62;i++) run_argv[i+1]=argv[i];
    run_argv[argc+1]=NULL;

    char result[MAX_RESULT];
    int rc=run_binary(binary,run_argv,result,sizeof(result));

    if (rc==0) {
        /* Check if result looks like JSON */
        const char *p=result; while(*p==' '||*p=='\n'||*p=='\t') p++;
        if (*p=='{'||*p=='[') {
            /* Already JSON — pass through */
            free(resp->buf);
            resp->buf_len=strlen(result);
            resp->buf=malloc(resp->buf_len+1);
            memcpy(resp->buf,result,resp->buf_len+1);
            resp->status=200;
        } else {
            /* Wrap plain text in JSON */
            http_resp_json(resp,200,"{\"data\":\"%.*s\"}",
                (int)(strlen(result)>4096?4096:strlen(result)),result);
        }
    } else {
        http_resp_json(resp,500,"{\"error\":\"binary failed\",\"code\":%d,\"output\":\"%.*s\"}",
            rc,(int)(strlen(result)>1024?1024:strlen(result)),result);
    }
}

/* GET /api/status — system-wide dashboard */
static void handle_status(HttpResponse *resp) {
    pthread_mutex_lock(&g_db_mutex);

    int total_jobs=0, completed=0, failed=0, queued=0;
    int total_uploads=0;
    long total_bytes=0;

    sqlite3_stmt *st;
    sqlite3_prepare_v2(g_db,"SELECT status,COUNT(*) FROM jobs GROUP BY status",-1,&st,NULL);
    while (sqlite3_step(st)==SQLITE_ROW) {
        const char *s=sqlite3_column_text(st,0)?(const char*)sqlite3_column_text(st,0):"";
        int c=sqlite3_column_int(st,1);
        total_jobs+=c;
        if (strcmp(s,"completed")==0) completed=c;
        else if (strcmp(s,"failed")==0) failed=c;
        else if (strcmp(s,"queued")==0) queued=c;
    }
    sqlite3_finalize(st);

    sqlite3_prepare_v2(g_db,"SELECT COUNT(*),COALESCE(SUM(size_bytes),0) FROM uploads",-1,&st,NULL);
    if (sqlite3_step(st)==SQLITE_ROW) {
        total_uploads=sqlite3_column_int(st,0);
        total_bytes=(long)sqlite3_column_int64(st,1);
    }
    sqlite3_finalize(st);

    pthread_mutex_unlock(&g_db_mutex);

    /* Check which binaries are available */
    char *bins[]={"bonfyre","bonfyre-pipeline","bonfyre-gate","bonfyre-meter",
                  "bonfyre-finance","bonfyre-outreach","bonfyre-graph","bonfyre-ledger",
                  "bonfyre-auth","bonfyre-pay",NULL};
    char avail[2048]=""; int alen=0;
    for (int i=0;bins[i];i++) {
        char which[256];
        char *test_argv[]={"which",bins[i],NULL};
        if (run_binary("which",test_argv,which,sizeof(which))==0) {
            if (alen>0) alen+=snprintf(avail+alen,sizeof(avail)-(size_t)alen,",");
            alen+=snprintf(avail+alen,sizeof(avail)-(size_t)alen,"\"%s\"",bins[i]);
        }
    }

    http_resp_json(resp,200,
        "{\"status\":\"ok\",\"version\":\"%s\","
        "\"jobs\":{\"total\":%d,\"completed\":%d,\"failed\":%d,\"queued\":%d},"
        "\"uploads\":{\"total\":%d,\"bytes\":%ld},"
        "\"binaries\":[%s]}",
        VERSION,total_jobs,completed,failed,queued,total_uploads,total_bytes,avail);
}

/* GET /api/health */
static void handle_health(HttpResponse *resp) {
    http_resp_json(resp,200,
        "{\"status\":\"ok\",\"version\":\"%s\",\"service\":\"bonfyre-api\"}",VERSION);
}

/* GET /api/binaries/:name/* — proxy to any bonfyre binary */
static void handle_binary_proxy(HttpRequest *req, HttpResponse *resp,
                                const char *binary_name, int nseg,
                                char segs[][256]) {
    /* Build args from remaining path segments + query */
    char bin[64]; snprintf(bin,sizeof(bin),"bonfyre-%s",binary_name);
    char *args[32]; int ac=0;

    /* Path segments after /api/binaries/{name}/ become positional args */
    for (int i=3;i<nseg && ac<30;i++)
        args[ac++]=segs[i];

    /* If POST with body, pass as last arg */
    if (req->body && req->body_len>0 && ac<30)
        args[ac++]=req->body;

    handle_proxy(resp,bin,ac,args);
}

/* ── Main router ──────────────────────────────────────────────────── */

static void route_request(HttpRequest *req, HttpResponse *resp) {
    char segs[MAX_PATH_SEGS][256];
    memset(segs,0,sizeof(segs));
    int nseg=path_segments(req->path,segs,MAX_PATH_SEGS);

    /* CORS preflight */
    if (strcmp(req->method,"OPTIONS")==0) {
        http_resp_json(resp,204,""); return;
    }

    /* /api/* routes */
    if (nseg>=2 && strcmp(segs[0],"api")==0) {
        const char *ep=segs[1];

        /* GET /api/health */
        if (strcmp(ep,"health")==0) { handle_health(resp); return; }

        /* GET /api/status */
        if (strcmp(ep,"status")==0 && strcmp(req->method,"GET")==0) {
            handle_status(resp); return;
        }

        /* POST /api/upload */
        if (strcmp(ep,"upload")==0 && strcmp(req->method,"POST")==0) {
            handle_upload(req,resp); return;
        }

        /* /api/jobs[/:id] */
        if (strcmp(ep,"jobs")==0) {
            if (strcmp(req->method,"POST")==0) {
                handle_job_submit(req,resp); return;
            }
            int jid=nseg>=3?atoi(segs[2]):0;
            handle_job_get(resp,jid); return;
        }

        /* /api/binaries/:name/... — proxy to any binary */
        if (strcmp(ep,"binaries")==0 && nseg>=3) {
            handle_binary_proxy(req,resp,segs[2],nseg,segs); return;
        }

        http_resp_json(resp,404,"{\"error\":\"unknown endpoint\"}");
        return;
    }

    /* Static files (frontend) */
    if (serve_static(resp,req->path)) return;

    http_resp_json(resp,404,"{\"error\":\"not found\"}");
}

/* ── Server ───────────────────────────────────────────────────────── */

static void *connection_handler(void *arg) {
    int fd=*(int*)arg; free(arg);

    HttpRequest *req=malloc(sizeof(HttpRequest));
    if (!req) { close(fd); return NULL; }
    if (parse_http_request(fd,req)!=0) { free(req); close(fd); return NULL; }

    HttpResponse resp;
    http_resp_init(&resp,fd);
    route_request(req,&resp);
    http_resp_send(&resp);
    http_resp_free(&resp);
    free_request(req);
    free(req);
    close(fd);
    atomic_fetch_sub(&g_thread_count, 1);  /* P4: track active threads */
    return NULL;
}

static int start_server(int port) {
    /* P4: ignore SIGPIPE — write() to closed socket kills process otherwise */
    signal(SIGPIPE, SIG_IGN);

    int sfd=socket(AF_INET,SOCK_STREAM,0);
    if (sfd<0) { perror("socket"); return -1; }
    int opt=1; setsockopt(sfd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr,0,sizeof(addr));
    addr.sin_family=AF_INET;
    addr.sin_addr.s_addr=INADDR_ANY;
    addr.sin_port=htons((uint16_t)port);

    if (bind(sfd,(struct sockaddr*)&addr,sizeof(addr))<0) {
        perror("bind"); close(sfd); return -1;
    }
    if (listen(sfd,64)<0) { perror("listen"); close(sfd); return -1; }

    fprintf(stderr,"[bonfyre-api] v%s listening on http://0.0.0.0:%d\n",VERSION,port);
    fprintf(stderr,"[bonfyre-api] static: %s\n",g_static_dir[0]?g_static_dir:"(none)");
    fprintf(stderr,"[bonfyre-api] uploads: %s\n",g_upload_dir);
    fprintf(stderr,"[bonfyre-api] endpoints:\n");
    fprintf(stderr,"  GET  /api/health\n");
    fprintf(stderr,"  GET  /api/status\n");
    fprintf(stderr,"  POST /api/upload\n");
    fprintf(stderr,"  POST /api/jobs\n");
    fprintf(stderr,"  GET  /api/jobs[/:id]\n");
    fprintf(stderr,"  *    /api/binaries/:name/...\n");
    fprintf(stderr,"  GET  /* (static files)\n");

    while (g_running) {
        struct sockaddr_in ca; socklen_t cl=sizeof(ca);
        int cfd=accept(sfd,(struct sockaddr*)&ca,&cl);
        if (cfd<0) { if (!g_running) break; continue; }

        /* P4: TCP_NODELAY eliminates 40ms Nagle delay on responses */
        int one=1;
        setsockopt(cfd,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one));

        /* P4: enforce connection limit — reject with 503 if overloaded */
        if (atomic_load(&g_thread_count) >= MAX_THREADS) {
            const char *busy="HTTP/1.1 503 Service Unavailable\r\n"
                "Content-Length: 0\r\nConnection: close\r\n\r\n";
            write(cfd,busy,strlen(busy));
            close(cfd);
            continue;
        }
        atomic_fetch_add(&g_thread_count, 1);

        int *pfd=malloc(sizeof(int)); *pfd=cfd;
        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_DETACHED);
        pthread_attr_setstacksize(&attr,THREAD_STACK);
        pthread_create(&tid,&attr,connection_handler,pfd);
        pthread_attr_destroy(&attr);
    }
    close(sfd);
    return 0;
}

static void handle_signal(int sig) { (void)sig; g_running=0; }

/* ── CLI ──────────────────────────────────────────────────────────── */

static const char *arg_get(int argc, char **argv, const char *flag) {
    for (int i=0;i<argc-1;i++)
        if (strcmp(argv[i],flag)==0) return argv[i+1];
    return NULL;
}

static void usage(void) {
    fprintf(stderr,
        "BonfyreAPI v%s — HTTP gateway\n\n"
        "Usage:\n"
        "  bonfyre-api serve [--port 8080] [--db FILE] [--static DIR] [--uploads DIR]\n"
        "  bonfyre-api status\n\n"
        "API Endpoints:\n"
        "  GET  /api/health           Health check\n"
        "  GET  /api/status           System dashboard\n"
        "  POST /api/upload           File upload\n"
        "  POST /api/jobs             Submit pipeline job\n"
        "  GET  /api/jobs[/:id]       Job status/list\n"
        "  *    /api/binaries/:name/  Proxy to any bonfyre-* binary\n"
        "  GET  /*                    Static files (frontend)\n",
        VERSION);
}

int main(int argc, char **argv) {
    if (argc<2) { usage(); return 1; }

    signal(SIGINT,handle_signal);
    signal(SIGTERM,handle_signal);

    /* Find command (skip --flags) */
    const char *cmd=NULL;
    for (int i=1;i<argc;i++) {
        if (argv[i][0]!='-') { cmd=argv[i]; break; }
        if (strcmp(argv[i],"--db")==0||strcmp(argv[i],"--port")==0||
            strcmp(argv[i],"--static")==0||strcmp(argv[i],"--uploads")==0) { i++; continue; }
    }
    if (!cmd) { usage(); return 1; }

    if (strcmp(cmd,"serve")==0) {
        int port=8080;
        const char *p=arg_get(argc,argv,"--port");
        if (p) port=atoi(p);

        const char *db_path=arg_get(argc,argv,"--db");
        if (!db_path) db_path=default_db();

        const char *sd=arg_get(argc,argv,"--static");
        if (sd) strncpy(g_static_dir,sd,sizeof(g_static_dir)-1);

        const char *ud=arg_get(argc,argv,"--uploads");
        if (!ud) {
            const char *h=getenv("HOME");
            snprintf(g_upload_dir,sizeof(g_upload_dir),"%s/.local/share/bonfyre/uploads",h?h:".");
        } else {
            strncpy(g_upload_dir,ud,sizeof(g_upload_dir)-1);
        }
        mkdir(g_upload_dir,0755);

        g_db=open_db(db_path);
        if (!g_db) return 1;

        int rc=start_server(port);
        sqlite3_close(g_db);
        return rc;

    } else if (strcmp(cmd,"status")==0) {
        printf("BonfyreAPI v%s\n",VERSION);
        printf("Use 'bonfyre-api serve' to start, then GET /api/status for live dashboard.\n");
        return 0;

    } else {
        usage(); return 1;
    }
}
