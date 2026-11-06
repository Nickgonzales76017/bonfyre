#include "account_world_literature.h"
#include "account_world.h"
#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

/* Writing additions to the account-world owner. See account_world_literature.h. */
static int key_ok(const char *s, size_t max) {
    if (!s || !*s || strlen(s) > max) return 0;
    return strspn(s,"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._:-/")==strlen(s);
}
static int slug_ok(const char *s, size_t max) {
    if (!s || !*s || strlen(s) > max || !((*s>='a'&&*s<='z')||(*s>='0'&&*s<='9'))) return 0;
    return strspn(s,"abcdefghijklmnopqrstuvwxyz0123456789-")==strlen(s);
}
static int line_ok(const char *s, size_t max, int required) {
    if (!s || strlen(s) > max || (required && (!*s || strspn(s," \t")==strlen(s)))) return 0;
    for (const unsigned char *p=(const unsigned char *)s; *p; p++) if (*p<32 || *p==127) return 0;
    return 1;
}
static int text_ok(const char *s, size_t max) { /* exact text: newlines and tabs only */
    if (!s || !*s || strlen(s) > max || strspn(s," \t\n")==strlen(s)) return 0;
    for (const unsigned char *p=(const unsigned char *)s; *p; p++) if ((*p<32 && *p!='\n' && *p!='\t') || *p==127) return 0;
    return 1;
}
static int piece_ok(const char *ref) {
    const char *slash = ref ? strchr(ref,'/') : NULL;
    if (!slash || slash==ref || (size_t)(slash-ref)>95 || strlen(slash+1)!=24 || strspn(slash+1,"0123456789abcdef")!=24) return 0;
    return strspn(ref,"abcdefghijklmnopqrstuvwxyz0123456789-_.")==(size_t)(slash-ref);
}
static int https_ok(const char *u) {
    if (!u || strncmp(u,"https://",8) || strlen(u) > 512 || !u[8] || u[8]=='/') return 0;
    for (const unsigned char *p=(const unsigned char *)u; *p; p++) if (*p<=32 || *p>=127 || strchr("\"'<>\\`",*p)) return 0;
    return 1;
}
static int date_ok(const char *d) {
    if (!d || strlen(d)!=10 || d[4]!='-' || d[7]!='-') return 0;
    for (int i=0;i<10;i++) if (i!=4 && i!=7 && !isdigit((unsigned char)d[i])) return 0;
    int y=atoi(d), m=atoi(d+5), day=atoi(d+8);
    static const int len[]={31,29,31,30,31,30,31,31,30,31,30,31};
    return y>=1900 && y<=2999 && m>=1 && m<=12 && day>=1 && day<=len[m-1] && !(m==2 && day==29 && (y%4 || (y%100==0 && y%400)));
}
static int language_ok(const char *s) {
    return s && strlen(s)>=2 && strlen(s)<=16 && strspn(s,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-")==strlen(s);
}
static int kind_index(const char *k) {
    static const char *kinds[]={"poem","story","essay","memoir","serial","interactive"};
    for (int i=0;k&&i<6;i++) if (!strcmp(k,kinds[i])) return i;
    return -1;
}
static int common(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,int revision) {
    return db && key_ok(subject,256) && key_ok(ns,127) && id>0 && key_ok(space,64) && revision>0 && revision<INT_MAX;
}

static const char *GUARD="SELECT 1 FROM account_world_binding b JOIN world w ON w.id=b.world_id WHERE b.world_id=?1 AND b.subject=?2 AND b.namespace=?3 "
                         "AND w.subject=b.subject AND w.namespace=b.namespace AND w.status='draft'";
static int guarded(sqlite3 *db,sqlite3_int64 id,const char *subject,const char *ns) {
    sqlite3_stmt *s=NULL; if (sqlite3_prepare_v2(db,GUARD,-1,&s,NULL)!=SQLITE_OK) return -1;
    sqlite3_bind_int64(s,1,id); sqlite3_bind_text(s,2,subject,-1,SQLITE_STATIC); sqlite3_bind_text(s,3,ns,-1,SQLITE_STATIC);
    int step=sqlite3_step(s); sqlite3_finalize(s); return step==SQLITE_ROW?1:step==SQLITE_DONE?0:-1;
}
/* Opens the transaction, checks the binding and advances the exact revision. */
static int begin(sqlite3 *db,sqlite3_int64 id,const char *subject,const char *ns,const char *space,int revision) {
    if (sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL)!=SQLITE_OK) return -1;
    int r=guarded(db,id,subject,ns);
    if (r==1) {
        sqlite3_stmt *s=NULL;
        if (sqlite3_prepare_v2(db,"UPDATE account_world_space SET revision=revision+1 WHERE world_id=?1 AND space_id=?2 AND revision=?3",-1,&s,NULL)!=SQLITE_OK) r=-1;
        else { sqlite3_bind_int64(s,1,id); sqlite3_bind_text(s,2,space,-1,SQLITE_STATIC); sqlite3_bind_int(s,3,revision);
               r=sqlite3_step(s)==SQLITE_DONE ? sqlite3_changes(db)==1 : -1; }
        sqlite3_finalize(s);
    }
    if (r!=1) sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);
    return r;
}
static int finish(sqlite3 *db,int ret) {
    if (ret==1 && sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)==SQLITE_OK) return 1;
    sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL); return ret==1?-1:ret;
}
/* Bind world/space as ?1/?2, then texts from ?3 and integers from ?20. */
static int exec_rows(sqlite3 *db,const char *sql,sqlite3_int64 id,const char *space,const char **text,int ntext,const long long *num,int nnum,int *changes) {
    sqlite3_stmt *s=NULL; if (sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK) return -1;
    sqlite3_bind_int64(s,1,id); sqlite3_bind_text(s,2,space,-1,SQLITE_STATIC);
    for (int i=0;i<ntext;i++) sqlite3_bind_text(s,3+i,text[i],-1,SQLITE_STATIC);
    for (int i=0;i<nnum;i++) sqlite3_bind_int64(s,20+i,num[i]);
    int step=sqlite3_step(s); sqlite3_finalize(s);
    if (step!=SQLITE_DONE && step!=SQLITE_ROW) return -1;
    if (changes) *changes=sqlite3_changes(db);
    return step==SQLITE_ROW;
}
/* The draft kind of a work in this space into out[16]; 1 found, 0 not, -1 error. */
static int work_kind(sqlite3 *db,sqlite3_int64 id,const char *space,const char *work,char out[16]) {
    sqlite3_stmt *s=NULL; out[0]=0;
    if (sqlite3_prepare_v2(db,"SELECT kind FROM account_world_work WHERE world_id=?1 AND space_id=?2 AND released=0 AND work=?3",-1,&s,NULL)!=SQLITE_OK) return -1;
    sqlite3_bind_int64(s,1,id); sqlite3_bind_text(s,2,space,-1,SQLITE_STATIC); sqlite3_bind_text(s,3,work,-1,SQLITE_STATIC);
    int step=sqlite3_step(s); if (step==SQLITE_ROW) snprintf(out,16,"%s",(const char*)sqlite3_column_text(s,0));
    sqlite3_finalize(s); return step==SQLITE_ROW?1:step==SQLITE_DONE?0:-1;
}

int awl_units(const char *kind, const char *body) {
    if (!body) return 0;
    int poem=kind&&!strcmp(kind,"poem"), n=0, in=0;
    if (poem) { if (!*body) return 0; n=1; for (const char *p=body;*p;p++) if (*p=='\n' && p[1]) n++; return n; }
    for (const char *p=body; *p; ) {
        const char *e=strchr(p,'\n'); size_t len=e?(size_t)(e-p):strlen(p);
        int blank=strspn(p," \t")>=len;
        if (!blank && !in) n++;
        in=!blank; p=e?e+1:p+len;
    }
    return n;
}

#define AWL_COPY(table, cols) \
    "DELETE FROM " table " WHERE world_id=NEW.world_id AND space_id=NEW.space_id AND released=1;" \
    "INSERT INTO " table "(world_id,space_id,released," cols ") SELECT world_id,space_id,1," cols " FROM " table \
    " WHERE world_id=NEW.world_id AND space_id=NEW.space_id AND released=0 AND NEW.state='public';"
#define AWL_COPY_ALL \
    AWL_COPY("account_world_work","work,key,kind,title,author,translator,language,original,collection,rights,licence,provenance,provenance_label,recitation,position") \
    AWL_COPY("account_world_passage","work,passage,number,title,body,release_on,start") \
    AWL_COPY("account_world_choice","work,passage,number,label,target") \
    AWL_COPY("account_world_gloss","work,passage,unit,kind,body")

int awl_initialize(sqlite3 *db) {
    if (!db || aw_initialize(db)!=1) return -1;
    const char *sql =
      "CREATE TABLE IF NOT EXISTS account_world_work("
      "world_id INTEGER NOT NULL,space_id TEXT NOT NULL,released INTEGER NOT NULL CHECK(released IN(0,1)),work TEXT NOT NULL,"
      "key TEXT NOT NULL CHECK(length(key)=24),kind TEXT NOT NULL CHECK(kind IN('poem','story','essay','memoir','serial','interactive')),"
      "title TEXT NOT NULL,author TEXT NOT NULL,translator TEXT NOT NULL,language TEXT NOT NULL,original TEXT NOT NULL,collection TEXT NOT NULL,"
      "rights TEXT NOT NULL CHECK(rights IN('own','public-domain')),licence TEXT NOT NULL,provenance TEXT NOT NULL,provenance_label TEXT NOT NULL,"
      "recitation TEXT NOT NULL,position INTEGER NOT NULL CHECK(position BETWEEN 1 AND 999),"
      "CHECK(rights='own' OR (provenance LIKE 'https://%' AND lower(licence) LIKE 'public domain%')),"
      "PRIMARY KEY(world_id,space_id,released,work),UNIQUE(world_id,released,key));"
      "CREATE TABLE IF NOT EXISTS account_world_passage("
      "world_id INTEGER NOT NULL,space_id TEXT NOT NULL,released INTEGER NOT NULL CHECK(released IN(0,1)),work TEXT NOT NULL,passage TEXT NOT NULL,"
      "number INTEGER NOT NULL CHECK(number BETWEEN 1 AND 9999),title TEXT NOT NULL,body TEXT NOT NULL,release_on TEXT NOT NULL,"
      "start INTEGER NOT NULL CHECK(start IN(0,1)),PRIMARY KEY(world_id,space_id,released,work,passage));"
      "CREATE TABLE IF NOT EXISTS account_world_choice("
      "world_id INTEGER NOT NULL,space_id TEXT NOT NULL,released INTEGER NOT NULL CHECK(released IN(0,1)),work TEXT NOT NULL,passage TEXT NOT NULL,"
      "number INTEGER NOT NULL CHECK(number BETWEEN 1 AND 9),label TEXT NOT NULL,target TEXT NOT NULL,"
      "PRIMARY KEY(world_id,space_id,released,work,passage,number));"
      "CREATE TABLE IF NOT EXISTS account_world_gloss("
      "id INTEGER PRIMARY KEY,world_id INTEGER NOT NULL,space_id TEXT NOT NULL,released INTEGER NOT NULL CHECK(released IN(0,1)),"
      "work TEXT NOT NULL,passage TEXT NOT NULL,unit INTEGER NOT NULL CHECK(unit>=1),kind TEXT NOT NULL CHECK(kind IN('note','footnote')),"
      "body TEXT NOT NULL,UNIQUE(world_id,space_id,released,work,passage,unit,kind,body));"
      "CREATE TABLE IF NOT EXISTS account_world_passage_version("
      "world_id INTEGER NOT NULL,space_id TEXT NOT NULL,work TEXT NOT NULL,passage TEXT NOT NULL,saved_revision INTEGER NOT NULL,body TEXT NOT NULL,"
      "PRIMARY KEY(world_id,space_id,work,passage,saved_revision));"
      "CREATE TABLE IF NOT EXISTS account_world_place("
      "reader TEXT NOT NULL,namespace TEXT NOT NULL,space_id TEXT NOT NULL,work TEXT NOT NULL,passage TEXT NOT NULL,"
      "unit INTEGER NOT NULL CHECK(unit>=0),path TEXT NOT NULL,updated TEXT NOT NULL,PRIMARY KEY(reader,namespace,space_id,work));"
      "CREATE TRIGGER IF NOT EXISTS account_world_writing_release AFTER INSERT ON account_world_release BEGIN " AWL_COPY_ALL " END;"
      "CREATE TRIGGER IF NOT EXISTS account_world_writing_rerelease AFTER UPDATE ON account_world_release BEGIN " AWL_COPY_ALL " END;";
    return sqlite3_exec(db,sql,NULL,NULL,NULL)==SQLITE_OK ? 1 : -1;
}

int awl_work_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *work,const char *kind,
                 const char *title,const char *author,const char *translator,const char *language,const char *original,const char *collection,
                 const char *rights,const char *licence,const char *provenance,const char *provenance_label,const char *recitation,int position,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !slug_ok(work,64) || kind_index(kind)<0 || !line_ok(title,256,1) || !line_ok(author,160,1) ||
        !line_ok(translator,160,0) || !language_ok(language) || !original || (*original && (!slug_ok(original,64) || !strcmp(original,work))) ||
        !line_ok(collection,80,0) || !rights || (strcmp(rights,"own") && strcmp(rights,"public-domain")) || !line_ok(licence,128,1) ||
        !provenance || (*provenance && !https_ok(provenance)) || !line_ok(provenance_label,160,0) || !recitation || (*recitation && !piece_ok(recitation)) ||
        position<1 || position>999) return 0;
    if (!strcmp(rights,"public-domain") && (!*provenance || strncasecmp(licence,"public domain",13))) return 0;
    int r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
    if (*original) { char k[16]; r=work_kind(db,id,space,original,k); }
    long long n[]={position,AWL_WORK_LIMIT};
    if (r==1) { const char *v[]={work};
        int over=exec_rows(db,"SELECT 1 WHERE NOT EXISTS(SELECT 1 FROM account_world_work WHERE world_id=?1 AND space_id=?2 AND released=0 AND work=?3) "
                              "AND (SELECT count(*) FROM account_world_work WHERE world_id=?1 AND space_id=?2 AND released=0)>=?21",id,space,v,1,n,2,NULL);
        r=over<0?-1:!over; }
    if (r==1) { const char *v[]={work,kind,title,author,translator,language,original,collection,rights,licence,provenance,provenance_label,recitation};
        r=exec_rows(db,"INSERT INTO account_world_work(world_id,space_id,released,work,key,kind,title,author,translator,language,original,collection,rights,"
                       "licence,provenance,provenance_label,recitation,position) VALUES(?1,?2,0,?3,lower(hex(randomblob(12))),?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?20) "
                       "ON CONFLICT(world_id,space_id,released,work) DO UPDATE SET kind=excluded.kind,title=excluded.title,author=excluded.author,"
                       "translator=excluded.translator,language=excluded.language,original=excluded.original,collection=excluded.collection,rights=excluded.rights,"
                       "licence=excluded.licence,provenance=excluded.provenance,provenance_label=excluded.provenance_label,recitation=excluded.recitation,"
                       "position=excluded.position",id,space,v,13,n,1,NULL)<0?-1:1; }
    /* a work that has choices cannot stop being interactive */
    if (r==1 && strcmp(kind,"interactive")) { const char *v[]={work};
        int has=exec_rows(db,"SELECT 1 FROM account_world_choice WHERE world_id=?1 AND space_id=?2 AND released=0 AND work=?3 LIMIT 1",id,space,v,1,NULL,0,NULL);
        r=has<0?-1:!has; }
    return finish(db,r);
}
int awl_work_remove(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *work,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !slug_ok(work,64)) return 0;
    int r=begin(db,id,subject,ns,space,revision),changes=0; if (r!=1) return r;
    const char *v[]={work};
    r=exec_rows(db,"DELETE FROM account_world_work WHERE world_id=?1 AND space_id=?2 AND released=0 AND work=?3",id,space,v,1,NULL,0,&changes);
    if (r>=0 && changes==1) {
        const char *tables[]={"account_world_passage","account_world_choice","account_world_gloss"};
        char sql[200];
        for (int i=0;i<3 && r>=0;i++) { snprintf(sql,sizeof(sql),"DELETE FROM %s WHERE world_id=?1 AND space_id=?2 AND released=0 AND work=?3",tables[i]); r=exec_rows(db,sql,id,space,v,1,NULL,0,NULL); }
        if (r>=0) r=exec_rows(db,"DELETE FROM account_world_passage_version WHERE world_id=?1 AND space_id=?2 AND work=?3",id,space,v,1,NULL,0,NULL);
        if (r>=0) r=exec_rows(db,"UPDATE account_world_work SET original='' WHERE world_id=?1 AND space_id=?2 AND released=0 AND original=?3",id,space,v,1,NULL,0,NULL);
        r=r<0?-1:1;
    } else r=r<0?-1:0;
    return finish(db,r);
}
int awl_passage_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *work,const char *passage,int number,
                    const char *title,const char *body,const char *release_on,int start,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !slug_ok(work,64) || !slug_ok(passage,64) || number<1 || number>9999 || !line_ok(title,256,0) ||
        !text_ok(body,AWL_BODY_LIMIT) || !release_on || (*release_on && !date_ok(release_on)) || (start!=0 && start!=1)) return 0;
    int r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
    char kind[16]; r=work_kind(db,id,space,work,kind);
    if (r==1 && start && strcmp(kind,"interactive")) r=0;
    const char *v[]={work,passage,title,body,release_on};
    long long n[]={number,start,revision,AWL_PASSAGE_LIMIT,AWL_VERSION_KEEP};
    if (r==1) {
        int over=exec_rows(db,"SELECT 1 WHERE NOT EXISTS(SELECT 1 FROM account_world_passage WHERE world_id=?1 AND space_id=?2 AND released=0 AND work=?3 AND passage=?4) "
                              "AND (SELECT count(*) FROM account_world_passage WHERE world_id=?1 AND space_id=?2 AND released=0 AND work=?3)>=?23",id,space,v,2,n,4,NULL);
        r=over<0?-1:!over;
    }
    /* keep the text this edit replaces */
    if (r==1) r=exec_rows(db,"INSERT OR REPLACE INTO account_world_passage_version(world_id,space_id,work,passage,saved_revision,body) "
                             "SELECT world_id,space_id,work,passage,?22,body FROM account_world_passage WHERE world_id=?1 AND space_id=?2 AND released=0 "
                             "AND work=?3 AND passage=?4 AND body<>?6",id,space,v,4,n,3,NULL)<0?-1:1;
    if (r==1) r=exec_rows(db,"DELETE FROM account_world_passage_version WHERE world_id=?1 AND space_id=?2 AND work=?3 AND passage=?4 AND saved_revision NOT IN("
                             "SELECT saved_revision FROM account_world_passage_version WHERE world_id=?1 AND space_id=?2 AND work=?3 AND passage=?4 "
                             "ORDER BY saved_revision DESC LIMIT ?24)",id,space,v,2,n,5,NULL)<0?-1:1;
    if (r==1 && start) r=exec_rows(db,"UPDATE account_world_passage SET start=0 WHERE world_id=?1 AND space_id=?2 AND released=0 AND work=?3",id,space,v,1,NULL,0,NULL)<0?-1:1;
    if (r==1) r=exec_rows(db,"INSERT INTO account_world_passage(world_id,space_id,released,work,passage,number,title,body,release_on,start) "
                             "VALUES(?1,?2,0,?3,?4,?20,?5,?6,?7,?21) ON CONFLICT(world_id,space_id,released,work,passage) DO UPDATE SET number=excluded.number,"
                             "title=excluded.title,body=excluded.body,release_on=excluded.release_on,start=excluded.start",id,space,v,5,n,2,NULL)<0?-1:1;
    /* glosses beyond the new end of the text would point nowhere */
    if (r==1) { long long u[]={awl_units(kind,body)};
        int dangling=exec_rows(db,"SELECT 1 FROM account_world_gloss WHERE world_id=?1 AND space_id=?2 AND released=0 AND work=?3 AND passage=?4 AND unit>?20 LIMIT 1",id,space,v,2,u,1,NULL);
        r=dangling<0?-1:!dangling; }
    return finish(db,r);
}
int awl_passage_remove(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *work,const char *passage,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !slug_ok(work,64) || !slug_ok(passage,64)) return 0;
    int r=begin(db,id,subject,ns,space,revision),changes=0; if (r!=1) return r;
    const char *v[]={work,passage};
    int targeted=exec_rows(db,"SELECT 1 FROM account_world_choice WHERE world_id=?1 AND space_id=?2 AND released=0 AND work=?3 AND target=?4 AND passage<>?4 LIMIT 1",id,space,v,2,NULL,0,NULL);
    r=targeted<0?-1:!targeted;
    if (r==1) r=exec_rows(db,"DELETE FROM account_world_passage WHERE world_id=?1 AND space_id=?2 AND released=0 AND work=?3 AND passage=?4",id,space,v,2,NULL,0,&changes)<0?-1:changes==1;
    if (r==1) {
        const char *sql[]={"DELETE FROM account_world_choice WHERE world_id=?1 AND space_id=?2 AND released=0 AND work=?3 AND passage=?4",
                           "DELETE FROM account_world_gloss WHERE world_id=?1 AND space_id=?2 AND released=0 AND work=?3 AND passage=?4",
                           "DELETE FROM account_world_passage_version WHERE world_id=?1 AND space_id=?2 AND work=?3 AND passage=?4"};
        for (int i=0;i<3 && r==1;i++) r=exec_rows(db,sql[i],id,space,v,2,NULL,0,NULL)<0?-1:1;
    }
    return finish(db,r);
}
static int passage_exists(sqlite3 *db,sqlite3_int64 id,const char *space,const char *work,const char *passage) {
    const char *v[]={work,passage};
    return exec_rows(db,"SELECT 1 FROM account_world_passage WHERE world_id=?1 AND space_id=?2 AND released=0 AND work=?3 AND passage=?4",id,space,v,2,NULL,0,NULL);
}
int awl_choice_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *work,const char *passage,int number,
                   const char *label,const char *target,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !slug_ok(work,64) || !slug_ok(passage,64) || !slug_ok(target,64) || number<1 || number>AWL_CHOICE_LIMIT ||
        !line_ok(label,200,1)) return 0;
    int r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
    char kind[16]; r=work_kind(db,id,space,work,kind);
    if (r==1 && strcmp(kind,"interactive")) r=0;
    if (r==1) r=passage_exists(db,id,space,work,passage);
    if (r==1) r=passage_exists(db,id,space,work,target);
    if (r==1) { const char *v[]={work,passage,label,target}; long long n[]={number};
        r=exec_rows(db,"INSERT INTO account_world_choice(world_id,space_id,released,work,passage,number,label,target) VALUES(?1,?2,0,?3,?4,?20,?5,?6) "
                       "ON CONFLICT(world_id,space_id,released,work,passage,number) DO UPDATE SET label=excluded.label,target=excluded.target",id,space,v,4,n,1,NULL)<0?-1:1; }
    return finish(db,r);
}
int awl_choice_remove(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *work,const char *passage,int number,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !slug_ok(work,64) || !slug_ok(passage,64) || number<1 || number>AWL_CHOICE_LIMIT) return 0;
    int r=begin(db,id,subject,ns,space,revision),changes=0; if (r!=1) return r;
    const char *v[]={work,passage}; long long n[]={number};
    r=exec_rows(db,"DELETE FROM account_world_choice WHERE world_id=?1 AND space_id=?2 AND released=0 AND work=?3 AND passage=?4 AND number=?20",id,space,v,2,n,1,&changes);
    return finish(db,r<0?-1:changes==1);
}
int awl_gloss_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *work,const char *passage,int unit,
                  const char *kind,const char *body,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !slug_ok(work,64) || !slug_ok(passage,64) || unit<1 || !kind ||
        (strcmp(kind,"note") && strcmp(kind,"footnote")) || !line_ok(body,1000,1)) return 0;
    int r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
    char wkind[16]; r=work_kind(db,id,space,work,wkind);
    if (r==1) {
        sqlite3_stmt *s=NULL; r=-1;
        if (sqlite3_prepare_v2(db,"SELECT body FROM account_world_passage WHERE world_id=?1 AND space_id=?2 AND released=0 AND work=?3 AND passage=?4",-1,&s,NULL)==SQLITE_OK) {
            sqlite3_bind_int64(s,1,id); sqlite3_bind_text(s,2,space,-1,SQLITE_STATIC); sqlite3_bind_text(s,3,work,-1,SQLITE_STATIC); sqlite3_bind_text(s,4,passage,-1,SQLITE_STATIC);
            int step=sqlite3_step(s);
            r=step==SQLITE_ROW ? unit<=awl_units(wkind,(const char*)sqlite3_column_text(s,0)) : step==SQLITE_DONE ? 0 : -1;
        }
        sqlite3_finalize(s);
    }
    long long n[]={unit,AWL_GLOSS_LIMIT};
    if (r==1) { const char *v[]={work};
        int over=exec_rows(db,"SELECT 1 WHERE (SELECT count(*) FROM account_world_gloss WHERE world_id=?1 AND space_id=?2 AND released=0 AND work=?3)>=?21",id,space,v,1,n,2,NULL);
        r=over<0?-1:!over; }
    if (r==1) { const char *v[]={work,passage,kind,body};
        r=exec_rows(db,"INSERT OR IGNORE INTO account_world_gloss(world_id,space_id,released,work,passage,unit,kind,body) VALUES(?1,?2,0,?3,?4,?20,?5,?6)",id,space,v,4,n,1,NULL)<0?-1:1; }
    return finish(db,r);
}
int awl_gloss_remove(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,sqlite3_int64 gloss,int revision) {
    if (!common(db,subject,ns,id,space,revision) || gloss<1) return 0;
    int r=begin(db,id,subject,ns,space,revision),changes=0; if (r!=1) return r;
    long long n[]={gloss};
    r=exec_rows(db,"DELETE FROM account_world_gloss WHERE id=?20 AND world_id=?1 AND space_id=?2 AND released=0",id,space,NULL,0,n,1,&changes);
    return finish(db,r<0?-1:changes==1);
}

#define AWL_PUBLIC_JOIN " JOIN account_world_release r ON r.world_id=t.world_id AND r.space_id=t.space_id AND r.state='public' AND r.namespace=?4 " \
    "JOIN account_world_binding b ON b.world_id=r.world_id AND b.namespace=r.namespace " \
    "JOIN world w ON w.id=b.world_id AND w.namespace=b.namespace AND w.subject=b.subject AND w.status IN('draft','published') "
int awl_rows(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,int table,const char *work,sqlite3_stmt **rows) {
    if (rows) *rows=NULL;
    if (!db || !rows || !key_ok(ns,127) || id<=0 || !key_ok(space,64) || table<AWL_WORKS || table>AWL_GLOSSES || (subject && !key_ok(subject,256)) || (work && !slug_ok(work,64))) return 0;
    static const char *cols[]={
        "t.work,t.key,t.kind,t.title,t.author,t.translator,t.language,t.original,t.collection,t.rights,t.licence,t.provenance,t.provenance_label,t.recitation,t.position FROM account_world_work t",
        "t.work,t.passage,t.number,t.title,t.body,t.release_on,t.start FROM account_world_passage t",
        "t.work,t.passage,t.number,t.label,t.target FROM account_world_choice t",
        "t.id,t.work,t.passage,t.unit,t.kind,t.body FROM account_world_gloss t"};
    static const char *order[]={" ORDER BY t.collection,t.position,t.title"," ORDER BY t.work,t.number,t.passage"," ORDER BY t.work,t.passage,t.number",
                                " ORDER BY t.work,t.passage,t.unit,t.kind,t.id"};
    char sql[1400];
    if (subject) {
        int g=guarded(db,id,subject,ns); if (g!=1) return g;
        snprintf(sql,sizeof(sql),"SELECT %s WHERE t.world_id=?1 AND t.space_id=?2 AND t.released=0%s%s LIMIT 5000",cols[table],work?" AND t.work=?3":"",order[table]);
    } else
        snprintf(sql,sizeof(sql),"SELECT %s" AWL_PUBLIC_JOIN "WHERE t.world_id=?1 AND t.space_id=?2 AND t.released=1%s%s LIMIT 5000",cols[table],work?" AND t.work=?3":"",order[table]);
    if (sqlite3_prepare_v2(db,sql,-1,rows,NULL)!=SQLITE_OK) { *rows=NULL; return -1; }
    sqlite3_bind_int64(*rows,1,id); sqlite3_bind_text(*rows,2,space,-1,SQLITE_TRANSIENT);
    if (work) sqlite3_bind_text(*rows,3,work,-1,SQLITE_TRANSIENT);
    if (!subject) sqlite3_bind_text(*rows,4,ns,-1,SQLITE_TRANSIENT);
    return 1;
}
int awl_versions(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *work,const char *passage,sqlite3_stmt **rows) {
    if (rows) *rows=NULL;
    if (!db || !rows || !key_ok(subject,256) || !key_ok(ns,127) || id<=0 || !key_ok(space,64) || !slug_ok(work,64) || !slug_ok(passage,64)) return 0;
    int g=guarded(db,id,subject,ns); if (g!=1) return g;
    if (sqlite3_prepare_v2(db,"SELECT saved_revision,body FROM account_world_passage_version WHERE world_id=?1 AND space_id=?2 AND work=?3 AND passage=?4 "
                              "ORDER BY saved_revision DESC",-1,rows,NULL)!=SQLITE_OK) { *rows=NULL; return -1; }
    sqlite3_bind_int64(*rows,1,id); sqlite3_bind_text(*rows,2,space,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(*rows,3,work,-1,SQLITE_TRANSIENT); sqlite3_bind_text(*rows,4,passage,-1,SQLITE_TRANSIENT);
    return 1;
}
int awl_public_key(sqlite3 *db,const char *ns,sqlite3_int64 id,const char *key,char space_out[65],char work_out[65]) {
    space_out[0]=work_out[0]=0;
    if (!db || !key_ok(ns,127) || id<=0 || !key || strlen(key)!=24 || strspn(key,"0123456789abcdef")!=24) return 0;
    sqlite3_stmt *s=NULL;
    if (sqlite3_prepare_v2(db,"SELECT t.space_id,t.work FROM account_world_work t" AWL_PUBLIC_JOIN "WHERE t.world_id=?1 AND t.key=?2 AND t.released=1 LIMIT 1",-1,&s,NULL)!=SQLITE_OK) return -1;
    sqlite3_bind_int64(s,1,id); sqlite3_bind_text(s,2,key,-1,SQLITE_STATIC); sqlite3_bind_text(s,4,ns,-1,SQLITE_STATIC);
    int step=sqlite3_step(s);
    if (step==SQLITE_ROW) { snprintf(space_out,65,"%s",(const char*)sqlite3_column_text(s,0)); snprintf(work_out,65,"%s",(const char*)sqlite3_column_text(s,1)); }
    sqlite3_finalize(s); return step==SQLITE_ROW?1:step==SQLITE_DONE?0:-1;
}

/* ---------------------------------------------------------------- places */
/* The public World holding a space on this namespace; 0 when none. */
static sqlite3_int64 public_world(sqlite3 *db,const char *ns,const char *space) {
    sqlite3_stmt *s=NULL; sqlite3_int64 id=0;
    if (aw_public_space(db,ns,space,&s)==1 && !strcmp((const char*)sqlite3_column_text(s,5),"public")) id=sqlite3_column_int64(s,6);
    sqlite3_finalize(s); return id;
}
/* A released passage readers may open today; kind into k. 1, 0 or -1. */
static int open_passage(sqlite3 *db,sqlite3_int64 id,const char *space,const char *work,const char *passage,char k[16],int *units) {
    sqlite3_stmt *s=NULL;
    if (sqlite3_prepare_v2(db,"SELECT w.kind,p.body FROM account_world_passage p JOIN account_world_work w ON w.world_id=p.world_id AND w.space_id=p.space_id "
                              "AND w.released=1 AND w.work=p.work WHERE p.world_id=?1 AND p.space_id=?2 AND p.released=1 AND p.work=?3 AND p.passage=?4 "
                              "AND (p.release_on='' OR p.release_on<=date('now'))",-1,&s,NULL)!=SQLITE_OK) return -1;
    sqlite3_bind_int64(s,1,id); sqlite3_bind_text(s,2,space,-1,SQLITE_STATIC); sqlite3_bind_text(s,3,work,-1,SQLITE_STATIC); sqlite3_bind_text(s,4,passage,-1,SQLITE_STATIC);
    int step=sqlite3_step(s);
    if (step==SQLITE_ROW) { snprintf(k,16,"%s",(const char*)sqlite3_column_text(s,0)); if (units) *units=awl_units(k,(const char*)sqlite3_column_text(s,1)); }
    sqlite3_finalize(s); return step==SQLITE_ROW?1:step==SQLITE_DONE?0:-1;
}
/* Walk "a.b.c": a is the first passage, every step a released choice. */
static int path_ok(sqlite3 *db,sqlite3_int64 id,const char *space,const char *work,const char *passage,const char *path) {
    if (!path || !*path || strlen(path)>AWL_PATH_STEPS*65) return 0;
    sqlite3_stmt *s=NULL; char first[65]="";
    if (sqlite3_prepare_v2(db,"SELECT passage FROM account_world_passage WHERE world_id=?1 AND space_id=?2 AND released=1 AND work=?3 ORDER BY start DESC,number,passage LIMIT 1",-1,&s,NULL)!=SQLITE_OK) return -1;
    sqlite3_bind_int64(s,1,id); sqlite3_bind_text(s,2,space,-1,SQLITE_STATIC); sqlite3_bind_text(s,3,work,-1,SQLITE_STATIC);
    if (sqlite3_step(s)==SQLITE_ROW) snprintf(first,sizeof(first),"%s",(const char*)sqlite3_column_text(s,0));
    sqlite3_finalize(s); if (!*first) return 0;
    char prev[65]="",step_name[65]; int steps=0;
    for (const char *p=path; ; ) {
        const char *dot=strchr(p,'.'); size_t len=dot?(size_t)(dot-p):strlen(p);
        if (!len || len>64 || ++steps>AWL_PATH_STEPS) return 0;
        memcpy(step_name,p,len); step_name[len]=0; if (!slug_ok(step_name,64)) return 0;
        if (!*prev) { if (strcmp(step_name,first)) return 0; }
        else {
            const char *v[]={work,prev,step_name};
            int edge=exec_rows(db,"SELECT 1 FROM account_world_choice WHERE world_id=?1 AND space_id=?2 AND released=1 AND work=?3 AND passage=?4 AND target=?5",id,space,v,3,NULL,0,NULL);
            if (edge!=1) return edge<0?-1:0;
        }
        snprintf(prev,sizeof(prev),"%s",step_name);
        if (!dot) break; p=dot+1;
    }
    return !strcmp(prev,passage);
}
int awl_place_put(sqlite3 *db,const char *reader,const char *ns,const char *space,const char *work,const char *passage,int unit,const char *path) {
    if (!db || !key_ok(reader,256) || !key_ok(ns,127) || !key_ok(space,64) || !slug_ok(work,64) || !slug_ok(passage,64) || unit<0 || !path) return 0;
    if (sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL)!=SQLITE_OK) return -1;
    sqlite3_int64 id=public_world(db,ns,space); char kind[16]=""; int units=0,r=id>0;
    if (r==1) r=open_passage(db,id,space,work,passage,kind,&units);
    if (r==1 && unit>units) r=0;
    if (r==1) r=!strcmp(kind,"interactive") ? path_ok(db,id,space,work,passage,path) : !*path;
    if (r==1) {
        sqlite3_stmt *s=NULL;
        if (sqlite3_prepare_v2(db,"INSERT INTO account_world_place(reader,namespace,space_id,work,passage,unit,path,updated) VALUES(?1,?2,?3,?4,?5,?6,?7,strftime('%Y-%m-%dT%H:%M:%SZ','now')) "
                                  "ON CONFLICT(reader,namespace,space_id,work) DO UPDATE SET passage=excluded.passage,unit=excluded.unit,path=excluded.path,updated=excluded.updated",-1,&s,NULL)!=SQLITE_OK) r=-1;
        else { const char *v[]={reader,ns,space,work,passage}; for (int i=0;i<5;i++) sqlite3_bind_text(s,i+1,v[i],-1,SQLITE_STATIC);
               sqlite3_bind_int(s,6,unit); sqlite3_bind_text(s,7,path,-1,SQLITE_STATIC); r=sqlite3_step(s)==SQLITE_DONE?1:-1; }
        sqlite3_finalize(s);
    }
    return finish(db,r);
}
int awl_place_clear(sqlite3 *db,const char *reader,const char *ns,const char *space,const char *work) {
    if (!db || !key_ok(reader,256) || !key_ok(ns,127) || !key_ok(space,64) || !slug_ok(work,64)) return 0;
    sqlite3_stmt *s=NULL;
    if (sqlite3_prepare_v2(db,"DELETE FROM account_world_place WHERE reader=?1 AND namespace=?2 AND space_id=?3 AND work=?4",-1,&s,NULL)!=SQLITE_OK) return -1;
    const char *v[]={reader,ns,space,work}; for (int i=0;i<4;i++) sqlite3_bind_text(s,i+1,v[i],-1,SQLITE_STATIC);
    int step=sqlite3_step(s); sqlite3_finalize(s);
    return step==SQLITE_DONE ? sqlite3_changes(db)==1 : -1;
}
int awl_places(sqlite3 *db,const char *reader,const char *ns,const char *space,sqlite3_stmt **rows) {
    if (rows) *rows=NULL;
    if (!db || !rows || !key_ok(reader,256) || !key_ok(ns,127) || !key_ok(space,64)) return 0;
    sqlite3_int64 id=public_world(db,ns,space); if (id<=0) return 0;
    if (sqlite3_prepare_v2(db,"SELECT p.work,p.passage,p.unit,p.path,p.updated FROM account_world_place p JOIN account_world_work w ON w.world_id=?4 AND w.space_id=p.space_id "
                              "AND w.released=1 AND w.work=p.work WHERE p.reader=?1 AND p.namespace=?2 AND p.space_id=?3 ORDER BY p.updated DESC,p.work LIMIT 64",-1,rows,NULL)!=SQLITE_OK) { *rows=NULL; return -1; }
    sqlite3_bind_text(*rows,1,reader,-1,SQLITE_TRANSIENT); sqlite3_bind_text(*rows,2,ns,-1,SQLITE_TRANSIENT); sqlite3_bind_text(*rows,3,space,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int64(*rows,4,id);
    return 1;
}
