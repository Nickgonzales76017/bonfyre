#ifndef BONFYRE_QUEUE_ACCOUNT_INBOX_H
#define BONFYRE_QUEUE_ACCOUNT_INBOX_H

/* Recipient presentation only. Task evidence remains in WorkGraph/WorkerRun;
 * a dismissal cannot acknowledge, cancel, retry or otherwise mutate that work. */
#include "account_work.h"
#include <errno.h>
#define QBOX_REV_MAX INT64_C(9007199254740991)
#define QBOX_INPUT_MAX 4096

typedef struct { QactNotice task; int64_t revision; int read, dismissed; } QboxNotice;
typedef struct { char id[128], version[65], action[9], request[97]; int64_t revision; } QboxChange;
typedef struct { char id[128], version[65], action[9]; int64_t expected, applied; } QboxReceipt;

static inline int qbox_hex(const char *s) {
    return s && strlen(s)==64 && strspn(s,"0123456789abcdef")==64;
}
static inline int qbox_recipient(const char *subject,const char *scope,const char *actor) {
    return qact_scope_valid(subject,scope) && qact_scope_valid(actor,"self") &&
        (strcmp(scope,"self") || !strcmp(subject,actor));
}
static inline int qbox_action(const char *s) {
    return s && (!strcmp(s,"read") || !strcmp(s,"unread") || !strcmp(s,"dismiss") || !strcmp(s,"restore"));
}
static inline int qbox_schema(sqlite3 *db) {
    return sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS queue_inbox_states("
        "subject TEXT NOT NULL,scope TEXT NOT NULL,actor TEXT NOT NULL,mission_id TEXT NOT NULL,"
        "source_version TEXT NOT NULL,read INTEGER NOT NULL CHECK(read IN(0,1)),"
        "dismissed INTEGER NOT NULL CHECK(dismissed IN(0,1)),revision INTEGER NOT NULL CHECK(revision>0),"
        "PRIMARY KEY(subject,scope,actor,mission_id));"
        "CREATE TABLE IF NOT EXISTS queue_inbox_operations("
        "subject TEXT NOT NULL,scope TEXT NOT NULL,actor TEXT NOT NULL,request_id TEXT NOT NULL,"
        "mission_id TEXT NOT NULL,source_version TEXT NOT NULL,expected_revision INTEGER NOT NULL,"
        "action TEXT NOT NULL,result_revision INTEGER NOT NULL,created_at_ms INTEGER NOT NULL,"
        "PRIMARY KEY(subject,scope,actor,request_id));",NULL,NULL,NULL)==SQLITE_OK;
}
static inline void qbox_bind_audience(sqlite3_stmt *q,const char *subject,const char *scope,const char *actor) {
    sqlite3_bind_text(q,1,subject,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(q,2,scope,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(q,3,actor,-1,SQLITE_TRANSIENT);
}
/* Call within the caller's transaction, so version and recipient state describe
 * one snapshot even when a worker completes or another tab marks a notice. */
static inline int qbox_notice(sqlite3 *db,const char *subject,const char *scope,const char *actor,
                              const char *id,QboxNotice *out) {
    memset(out,0,sizeof(*out));
    if(!qbox_recipient(subject,scope,actor) || !qact_atom(id,127))return -1;
    int rc=awk_source_notice(db,subject,scope,id,&out->task);
    if(rc!=1)return rc;
    int exists=qact_table(db,"queue_inbox_states");
    if(exists<=0)return exists<0?-1:1;
    sqlite3_stmt *q=NULL; char version[65]; int answer=-1;
    if(sqlite3_prepare_v2(db,"SELECT source_version,read,dismissed,revision FROM queue_inbox_states "
        "WHERE subject=? AND scope=? AND actor=? AND mission_id=?",-1,&q,NULL)!=SQLITE_OK)goto done;
    qbox_bind_audience(q,subject,scope,actor);sqlite3_bind_text(q,4,id,-1,SQLITE_TRANSIENT);
    rc=sqlite3_step(q);
    if(rc==SQLITE_DONE){answer=1;goto done;}
    if(rc!=SQLITE_ROW || !qact_cell(q,0,version,sizeof(version),0) || !qbox_hex(version) ||
       sqlite3_column_type(q,1)!=SQLITE_INTEGER || sqlite3_column_type(q,2)!=SQLITE_INTEGER ||
       sqlite3_column_type(q,3)!=SQLITE_INTEGER)goto done;
    int r=sqlite3_column_int(q,1),d=sqlite3_column_int(q,2);
    out->revision=sqlite3_column_int64(q,3);
    if((r!=0&&r!=1)||(d!=0&&d!=1)||out->revision<1||out->revision>QBOX_REV_MAX)goto done;
    if(!strcmp(version,out->task.version)){out->read=r;out->dismissed=d;}
    answer=sqlite3_step(q)==SQLITE_DONE?1:-1;
done:sqlite3_finalize(q);return answer;
}
static inline int qbox_receipt(sqlite3 *db,const char *subject,const char *scope,const char *actor,
                               const char *request,QboxReceipt *out) {
    memset(out,0,sizeof(*out));int exists=qact_table(db,"queue_inbox_operations");
    if(exists<=0)return exists;
    sqlite3_stmt *q=NULL;int answer=-1;
    if(sqlite3_prepare_v2(db,"SELECT mission_id,source_version,expected_revision,action,result_revision "
        "FROM queue_inbox_operations WHERE subject=? AND scope=? AND actor=? AND request_id=?",-1,&q,NULL)!=SQLITE_OK)goto done;
    qbox_bind_audience(q,subject,scope,actor);sqlite3_bind_text(q,4,request,-1,SQLITE_TRANSIENT);
    int rc=sqlite3_step(q);if(rc==SQLITE_DONE){answer=0;goto done;}
    if(rc!=SQLITE_ROW || !qact_cell(q,0,out->id,sizeof(out->id),0) || !qact_atom(out->id,127) ||
       !qact_cell(q,1,out->version,sizeof(out->version),0) || !qbox_hex(out->version) ||
       !qact_cell(q,3,out->action,sizeof(out->action),0) || !qbox_action(out->action) ||
       sqlite3_column_type(q,2)!=SQLITE_INTEGER || sqlite3_column_type(q,4)!=SQLITE_INTEGER)goto done;
    out->expected=sqlite3_column_int64(q,2);out->applied=sqlite3_column_int64(q,4);
    if(out->expected<0||out->expected>=QBOX_REV_MAX||out->applied!=out->expected+1)goto done;
    answer=sqlite3_step(q)==SQLITE_DONE?1:-1;
done:sqlite3_finalize(q);return answer;
}
#define QBOX_MP(call) do { if((call)!=0)return 0; } while(0)
static inline int qbox_pack_notice(bf_mp_writer_t *w,const QboxNotice *n) {
    const char *phase=n->task.phase;
    const char *body=*n->task.attention?n->task.attention:!strcmp(phase,"attention")?"This work needs attention.":!strcmp(phase,"active")?
        "Work is in progress.":!strcmp(phase,"completed")?"Work has completed.":
        !strcmp(phase,"cancelled")?"Work was cancelled.":"Work is waiting to continue.";
    QBOX_MP(bf_mp_pack_map(w,11));
    QBOX_MP(bf_mp_pack_kv_str(w,"id",n->task.id));QBOX_MP(bf_mp_pack_kv_str(w,"taskId",n->task.id));
    QBOX_MP(bf_mp_pack_kv_str(w,"version",n->task.version));QBOX_MP(bf_mp_pack_kv_int(w,"revision",n->revision));
    QBOX_MP(bf_mp_pack_kv_str(w,"title",n->task.title));QBOX_MP(bf_mp_pack_kv_str(w,"body",body));
    QBOX_MP(bf_mp_pack_kv_int(w,"updated",n->task.updated));QBOX_MP(bf_mp_pack_kv_str(w,"href",n->task.continuation));
    QBOX_MP(bf_mp_pack_kv_bool(w,"read",n->read));QBOX_MP(bf_mp_pack_kv_bool(w,"dismissed",n->dismissed));
    QBOX_MP(bf_mp_pack_kv_str(w,"phase",phase));return 1;
}
static inline int qbox_begin_packet(bf_mp_writer_t *w,int okay) {
    return !bf_mp_pack_array(w,3)&&!bf_mp_pack_int(w,1)&&!bf_mp_pack_bool(w,okay);
}
static inline int qbox_pack_ack(bf_mp_writer_t *w,const char *request,int replayed,
                                const QboxReceipt *receipt,const QboxNotice *notice) {
    if(!qbox_begin_packet(w,1))return 0;
    QBOX_MP(bf_mp_pack_map(w,5));QBOX_MP(bf_mp_pack_kv_str(w,"requestId",request));
    QBOX_MP(bf_mp_pack_kv_bool(w,"replayed",replayed));QBOX_MP(bf_mp_pack_kv_str(w,"appliedVersion",receipt->version));
    QBOX_MP(bf_mp_pack_kv_int(w,"appliedRevision",receipt->applied));
    QBOX_MP(bf_mp_pack_str_cstr(w,"notice"));return qbox_pack_notice(w,notice);
}
static inline int qbox_emit(bf_mp_writer_t *w,FILE *stream) {
    size_t bytes=bf_mp_writer_size(w);
    return bytes>0&&bytes<=QACT_PACKET_LIMIT&&fwrite(bf_mp_writer_data(w),1,bytes,stream)==bytes?0:1;
}
static inline int qbox_error(FILE *stream,const char *code,const char *message) {
    bf_mp_writer_t w;bf_mp_writer_init(&w,128);
    int good=qbox_begin_packet(&w,0)&&!bf_mp_pack_map(&w,2)&&
        !bf_mp_pack_kv_str(&w,"code",code)&&!bf_mp_pack_kv_str(&w,"message",message);
    int rc=good?qbox_emit(&w,stream):1;bf_mp_writer_free(&w);return rc;
}
static inline int qbox_number(const char *s,int64_t *out) {
    if(!s||!*s||strlen(s)>16||strspn(s,"0123456789")!=strlen(s)||(s[0]=='0'&&s[1]))return 0;
    errno=0;unsigned long long n=strtoull(s,NULL,10);
    if(errno||n>(unsigned long long)QBOX_REV_MAX)return 0;
    *out=(int64_t)n;return 1;
}
static inline int qbox_parse_change(const unsigned char *data,size_t size,QboxChange *change) {
    bf_mp_reader_t r;bf_mp_value_t v;memset(change,0,sizeof(*change));
    if(!data||!size||size>QBOX_INPUT_MAX)return 0;
    bf_mp_reader_init(&r,data,size);
    if(bf_mp_read_next(&r,&v)!=BF_MP_ARRAY||v.v.count!=2)return 0;
    bf_mp_type_t t=bf_mp_read_next(&r,&v);
    if(!((t==BF_MP_UINT&&v.v.u==1)||(t==BF_MP_INT&&v.v.i==1)))return 0;
    if(bf_mp_read_next(&r,&v)!=BF_MP_MAP||v.v.count!=5)return 0;
    unsigned seen=0;
    for(int i=0;i<5;i++){
        if(bf_mp_read_next(&r,&v)!=BF_MP_STR||v.v.raw.len>16||!v.v.raw.len)return 0;
        char key[17];memcpy(key,v.v.raw.ptr,v.v.raw.len);key[v.v.raw.len]=0;
        if(strlen(key)!=v.v.raw.len)return 0;
        unsigned bit=0;char *dest=NULL;size_t cap=0;
        if(!strcmp(key,"id")){bit=1;dest=change->id;cap=sizeof(change->id);}
        else if(!strcmp(key,"version")){bit=2;dest=change->version;cap=sizeof(change->version);}
        else if(!strcmp(key,"revision"))bit=4;
        else if(!strcmp(key,"action")){bit=8;dest=change->action;cap=sizeof(change->action);}
        else if(!strcmp(key,"requestId")){bit=16;dest=change->request;cap=sizeof(change->request);}
        else return 0;
        if(seen&bit)return 0;
        seen|=bit;t=bf_mp_read_next(&r,&v);
        if(bit==4){
            if(t==BF_MP_UINT&&v.v.u<=(uint64_t)QBOX_REV_MAX)change->revision=(int64_t)v.v.u;
            else if(t==BF_MP_INT&&v.v.i>=0&&v.v.i<=QBOX_REV_MAX)change->revision=v.v.i;else return 0;
        }else{
            if(t!=BF_MP_STR||v.v.raw.len>=cap||!v.v.raw.len)return 0;
            memcpy(dest,v.v.raw.ptr,v.v.raw.len);dest[v.v.raw.len]=0;
            if(strlen(dest)!=v.v.raw.len)return 0;
        }
    }
    return seen==31&&r.pos==r.len&&qact_atom(change->id,127)&&qbox_hex(change->version)&&
        qbox_action(change->action)&&strlen(change->request)>=8&&qact_atom(change->request,96);
}
static inline int qbox_list(const char *path,const char *subject,const char *scope,const char *actor,
                            const char *mission,int64_t cursor,FILE *stream) {
    sqlite3 *db=NULL;bf_mp_writer_t w;bf_mp_writer_init(&w,4096);
    char ids[51][128];int64_t sequences[51];size_t count=0;int okay=0,budget=30000;
    if(!qbox_recipient(subject,scope,actor)||cursor<0||cursor>QACT_MAX_TIME)goto done;
    if(sqlite3_open_v2(path,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto done;
    sqlite3_busy_timeout(db,2500);sqlite3_progress_handler(db,1000,qact_budget,&budget);
    if(sqlite3_exec(db,"BEGIN",NULL,NULL,NULL)!=SQLITE_OK)goto done;
    if(mission&&*mission){if(!qact_atom(mission,127)||cursor)goto done;snprintf(ids[0],sizeof(ids[0]),"%s",mission);count=1;}
    else if(awk_source_ids(db,subject,scope,cursor,ids,sequences,&count)!=1)goto done;
    size_t length=count>50?50:count;char next[24]="";
    if(count>50)snprintf(next,sizeof(next),"%lld",(long long)sequences[49]);
    if(!qbox_begin_packet(&w,1)||bf_mp_pack_map(&w,3)||bf_mp_pack_str_cstr(&w,"notices")||bf_mp_pack_array(&w,(uint32_t)length))goto done;
    for(size_t i=0;i<length;i++){
        QboxNotice notice;if(qbox_notice(db,subject,scope,actor,ids[i],&notice)!=1||!qbox_pack_notice(&w,&notice))goto done;
    }
    if(bf_mp_pack_kv_bool(&w,"more",count>50)||bf_mp_pack_kv_str(&w,"cursor",next)||
       sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK)goto done;
    okay=1;
done:
    if(db){sqlite3_progress_handler(db,0,NULL,NULL);sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);sqlite3_close(db);}
    int rc=okay?qbox_emit(&w,stream):qbox_error(stream,"unavailable","The current Inbox could not be read.");
    bf_mp_writer_free(&w);return rc;
}
static inline int qbox_update(const char *path,const char *subject,const char *scope,const char *actor,
                              const QboxChange *change,const char *request,FILE *stream) {
    sqlite3 *db=NULL;sqlite3_stmt *q=NULL;bf_mp_writer_t w;bf_mp_writer_init(&w,4096);
    QboxNotice notice;QboxReceipt receipt;int okay=0,replayed=0,budget=30000;
    const char *code="unavailable",*message="The Inbox change is not confirmed. Check this request or retry the same change.";
    if(!qbox_recipient(subject,scope,actor)||!request||strlen(request)<8||!qact_atom(request,96))goto done;
    if(sqlite3_open_v2(path,&db,change?SQLITE_OPEN_READWRITE:SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK)goto done;
    sqlite3_busy_timeout(db,2500);sqlite3_progress_handler(db,1000,qact_budget,&budget);
    if(sqlite3_exec(db,change?"BEGIN IMMEDIATE":"BEGIN",NULL,NULL,NULL)!=SQLITE_OK)goto done;
    int found=qbox_receipt(db,subject,scope,actor,request,&receipt);
    if(found<0)goto done;
    if(found){
        if(change&&(strcmp(receipt.id,change->id)||strcmp(receipt.version,change->version)||
           receipt.expected!=change->revision||strcmp(receipt.action,change->action))){code="request_conflict";message="This request belongs to a different Inbox change.";goto done;}
        if(qbox_notice(db,subject,scope,actor,receipt.id,&notice)!=1)goto done;
        replayed=1;
    }else{
        if(!change){code="receipt_pending";message="No committed receipt is visible yet. Retry only the same change.";goto done;}
        if(qbox_notice(db,subject,scope,actor,change->id,&notice)!=1){code="not_found";message="This task is not available in the selected account.";goto done;}
        if(strcmp(notice.task.version,change->version)||notice.revision!=change->revision){code="version_conflict";message="This notice changed. Read its current state before choosing another action.";goto done;}
        if(notice.revision>=QBOX_REV_MAX||!qbox_schema(db))goto done;
        if(!strcmp(change->action,"read"))notice.read=1;else if(!strcmp(change->action,"unread"))notice.read=0;
        else if(!strcmp(change->action,"dismiss"))notice.dismissed=1;else if(!strcmp(change->action,"restore"))notice.dismissed=0;else goto done;
        notice.revision++;
        if(sqlite3_prepare_v2(db,"INSERT INTO queue_inbox_states VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(subject,scope,actor,mission_id) "
            "DO UPDATE SET source_version=excluded.source_version,read=excluded.read,dismissed=excluded.dismissed,revision=excluded.revision",-1,&q,NULL)!=SQLITE_OK)goto done;
        qbox_bind_audience(q,subject,scope,actor);sqlite3_bind_text(q,4,change->id,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(q,5,change->version,-1,SQLITE_TRANSIENT);sqlite3_bind_int(q,6,notice.read);
        sqlite3_bind_int(q,7,notice.dismissed);sqlite3_bind_int64(q,8,notice.revision);
        if(sqlite3_step(q)!=SQLITE_DONE)goto done;
        sqlite3_finalize(q);q=NULL;
        if(sqlite3_prepare_v2(db,"INSERT INTO queue_inbox_operations VALUES(?,?,?,?,?,?,?,?,?,?)",-1,&q,NULL)!=SQLITE_OK)goto done;
        qbox_bind_audience(q,subject,scope,actor);sqlite3_bind_text(q,4,request,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(q,5,change->id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(q,6,change->version,-1,SQLITE_TRANSIENT);
        sqlite3_bind_int64(q,7,change->revision);sqlite3_bind_text(q,8,change->action,-1,SQLITE_TRANSIENT);
        sqlite3_bind_int64(q,9,notice.revision);sqlite3_bind_int64(q,10,qact_now());
        if(sqlite3_step(q)!=SQLITE_DONE)goto done;
        sqlite3_finalize(q);q=NULL;
        snprintf(receipt.version,sizeof(receipt.version),"%s",change->version);receipt.applied=notice.revision;
    }
    if(!qbox_pack_ack(&w,request,replayed,&receipt,&notice)||sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK)goto done;
    okay=1;
done:
    sqlite3_finalize(q);if(db){sqlite3_progress_handler(db,0,NULL,NULL);sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);sqlite3_close(db);}
    int rc=okay?qbox_emit(&w,stream):qbox_error(stream,code,message);bf_mp_writer_free(&w);return rc;
}
static inline int qbox_cli(int argc,char **argv) {
    const char *path=NULL,*subject=NULL,*scope=NULL,*actor=NULL,*mission=NULL,*cursor=NULL,*request=NULL;
    unsigned seen=0;if(argc<3)goto bad;
    for(int i=3;i<argc;i+=2){
        if(i+1>=argc||!argv[i+1][0])goto bad;
        const char **dest=NULL;unsigned bit=0;
        if(!strcmp(argv[i],"--db")){dest=&path;bit=1;}else if(!strcmp(argv[i],"--subject")){dest=&subject;bit=2;}
        else if(!strcmp(argv[i],"--scope")){dest=&scope;bit=4;}else if(!strcmp(argv[i],"--actor")){dest=&actor;bit=8;}
        else if(!strcmp(argv[i],"--mission")){dest=&mission;bit=16;}else if(!strcmp(argv[i],"--cursor")){dest=&cursor;bit=32;}
        else if(!strcmp(argv[i],"--request")){dest=&request;bit=64;}else goto bad;
        if(seen&bit)goto bad;
        seen|=bit;*dest=argv[i+1];
    }
    if((seen&15)!=15||strlen(path)>4095||!qbox_recipient(subject,scope,actor))goto bad;
    if(!strcmp(argv[2],"list")){
        int64_t n=0;if(request||(mission&&cursor)||(mission&&!qact_atom(mission,127))||(cursor&&!qbox_number(cursor,&n)))goto bad;
        return qbox_list(path,subject,scope,actor,mission,n,stdout);
    }
    if(!strcmp(argv[2],"receipt")){
        if(seen!=79||strlen(request)<8||!qact_atom(request,96))goto bad;
        return qbox_update(path,subject,scope,actor,NULL,request,stdout);
    }
    if(!strcmp(argv[2],"set")){
        if(seen!=15)goto bad;
        unsigned char input[QBOX_INPUT_MAX+1];size_t bytes=fread(input,1,sizeof(input),stdin);QboxChange change;
        if(ferror(stdin)||!qbox_parse_change(input,bytes,&change))goto bad;
        return qbox_update(path,subject,scope,actor,&change,change.request,stdout);
    }
bad:return qbox_error(stdout,"invalid_request","Use one declared Inbox operation and its exact coordinates.");
}
#undef QBOX_MP
#endif
