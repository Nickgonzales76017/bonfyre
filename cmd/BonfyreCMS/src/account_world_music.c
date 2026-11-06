#include "account_world_music.h"
#include "account_world_audio.h"
#include "account_world.h"
#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Music additions to the account-world owner. See account_world_music.h. */
static int key_ok(const char *s, size_t max) {
    if (!s || !*s || strlen(s) > max) return 0;
    return strspn(s,"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._:-/")==strlen(s);
}
static int id_ok(const char *s) { /* a short lowercase address used in links */
    return s && *s && strlen(s)<=64 && strspn(s,"abcdefghijklmnopqrstuvwxyz0123456789-")==strlen(s) && *s!='-';
}
static int line_ok(const char *s, size_t max, int required) {
    if (!s || strlen(s) > max || (required && (!*s || strspn(s," \t")==strlen(s)))) return 0;
    for (const unsigned char *p=(const unsigned char *)s; *p; p++) if (*p<32 || *p==127) return 0;
    return 1;
}
static int prose_ok(const char *s, size_t max) { /* paragraphs allowed */
    if (!s || strlen(s) > max) return 0;
    for (const unsigned char *p=(const unsigned char *)s; *p; p++) if ((*p<32 && *p!='\n' && *p!='\r') || *p==127) return 0;
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
static int space_link_ok(const char *s) { return s && !strncmp(s,"space:",6) && id_ok(s+6); }
static int link_ok(const char *s) { return s && (!*s || https_ok(s) || space_link_ok(s)); }
static int mbid_ok(const char *s) {
    if (!s) return 0; if (!*s) return 1; if (strlen(s)!=36) return 0;
    for (int i=0;i<36;i++) { if (i==8||i==13||i==18||i==23) { if (s[i]!='-') return 0; } else if (!strchr("0123456789abcdef",s[i])) return 0; }
    return 1;
}
static int year_ok(const char *s) { return s && (!*s || (strlen(s)==4 && strspn(s,"0123456789")==4 && atoi(s)>=1000 && atoi(s)<=2999)); }
static int day_ok(const char *d) {
    if (!d || strlen(d)!=10 || d[4]!='-' || d[7]!='-') return 0;
    for (int i=0;i<10;i++) if (i!=4 && i!=7 && !isdigit((unsigned char)d[i])) return 0;
    int y=atoi(d), m=atoi(d+5), day=atoi(d+8);
    static const int len[]={31,29,31,30,31,30,31,31,30,31,30,31};
    return y>=1000 && y<=2999 && m>=1 && m<=12 && day>=1 && day<=len[m-1] && !(m==2 && day==29 && (y%4 || (y%100==0 && y%400)));
}
static int partial_date_ok(const char *d) { /* "" | YYYY | YYYY-MM | YYYY-MM-DD */
    if (!d) return 0; size_t n=strlen(d); if (!n) return 1;
    if (n==4) return year_ok(d);
    if (n==7) { char y[5]; memcpy(y,d,4); y[4]=0; return year_ok(y) && d[4]=='-' && isdigit((unsigned char)d[5]) && isdigit((unsigned char)d[6]) && atoi(d+5)>=1 && atoi(d+5)<=12; }
    return day_ok(d);
}
static int minute_ok(const char *s) {
    if (!s || strlen(s)!=16 || s[10]!='T' || s[13]!=':') return 0;
    char day[11]; memcpy(day,s,10); day[10]=0;
    if (!day_ok(day) || !isdigit((unsigned char)s[11]) || !isdigit((unsigned char)s[12]) || !isdigit((unsigned char)s[14]) || !isdigit((unsigned char)s[15])) return 0;
    return atoi(s+11)<24 && atoi(s+14)<60;
}
static int iswc_ok(const char *s) { /* T-123.456.789-0 with optional separators */
    if (!s) return 0; if (!*s) return 1; if (s[0]!='T' || strlen(s)>15) return 0;
    int digits=0; for (const char *p=s+1; *p; p++) { if (isdigit((unsigned char)*p)) digits++; else if (*p!='-' && *p!='.') return 0; }
    return digits==10;
}
static int isrc_ok(const char *s) {
    if (!s) return 0; if (!*s) return 1; if (strlen(s)!=12) return 0;
    for (int i=0;i<12;i++) { int c=(unsigned char)s[i];
        if (i<2 ? !isupper(c) : i<5 ? !(isupper(c)||isdigit(c)) : !isdigit(c)) return 0; }
    return 1;
}
static int one_of(const char *s, const char *const *list) { for (; s && *list; list++) if (!strcmp(s,*list)) return 1; return 0; }
static const char *const RELEASE_TYPES[]={"album","ep","single","compilation","live","soundtrack","other",NULL};
static const char *const WORK_ROLES[]={"composer","lyricist","songwriter","arranger","translator","publisher",NULL};
static const char *const RECORDING_ROLES[]={"performer","vocals","instrument","producer","engineer","mixing","remixer","conductor","featured",NULL};
static const char *const RELEASE_ROLES[]={"artwork","design","photography","liner-notes","mastering","executive-producer",NULL};
static const char *const RELATION_KINDS[]={"member-of","founded","signed-to","collaborated-with","split-from","also-known-as","influenced-by",NULL};

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
/* world/space bind as ?1/?2, texts from ?3, integers from ?20. Returns 1 when
 * a row came back, 0 when done, -1 on error; *changes gets the change count. */
static int q(sqlite3 *db,const char *sql,sqlite3_int64 id,const char *space,const char **text,int ntext,const long long *num,int nnum,int *changes) {
    sqlite3_stmt *s=NULL; if (sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK) return -1;
    sqlite3_bind_int64(s,1,id); sqlite3_bind_text(s,2,space,-1,SQLITE_STATIC);
    for (int i=0;i<ntext;i++) sqlite3_bind_text(s,3+i,text[i],-1,SQLITE_STATIC);
    for (int i=0;i<nnum;i++) sqlite3_bind_int64(s,20+i,num[i]);
    int step=sqlite3_step(s); sqlite3_finalize(s);
    if (step!=SQLITE_DONE && step!=SQLITE_ROW) return -1;
    if (changes) *changes=sqlite3_changes(db);
    return step==SQLITE_ROW;
}
static int exists(sqlite3 *db,sqlite3_int64 id,const char *space,const char *table,const char *col,const char *value) {
    char sql[200]; snprintf(sql,sizeof(sql),"SELECT 1 FROM %s WHERE world_id=?1 AND space_id=?2 AND released=0 AND %s=?3",table,col);
    const char *v[]={value}; return q(db,sql,id,space,v,1,NULL,0,NULL);
}
static int under(sqlite3 *db,sqlite3_int64 id,const char *space,const char *table,long long limit) {
    char sql[200]; snprintf(sql,sizeof(sql),"SELECT 1 WHERE (SELECT count(*) FROM %s WHERE world_id=?1 AND space_id=?2 AND released=0)>?20",table);
    long long n[]={limit}; int over=q(db,sql,id,space,NULL,0,n,1,NULL); return over<0?-1:!over;
}
static int unready(sqlite3 *db,sqlite3_int64 id,const char *space,const char *release) {
    const char *v[]={release};
    return q(db,"UPDATE account_world_music_release SET ready=0 WHERE world_id=?1 AND space_id=?2 AND released=0 AND release_id=?3",id,space,v,1,NULL,0,NULL)<0?-1:1;
}

#define AWM_COPY(table, cols) \
    "DELETE FROM " table " WHERE world_id=NEW.world_id AND space_id=NEW.space_id AND released=1;" \
    "INSERT INTO " table "(world_id,space_id,released," cols ") SELECT world_id,space_id,1," cols " FROM " table \
    " WHERE world_id=NEW.world_id AND space_id=NEW.space_id AND released=0 AND NEW.state='public';"
#define AWM_COPY_ALL \
    AWM_COPY("account_world_music_party","kind,name,mbid,area,begun") \
    AWM_COPY("account_world_music_work","work_id,title,iswc,mbid") \
    AWM_COPY("account_world_music_recording","recording_id,work_id,title,ref,isrc,mbid") \
    AWM_COPY("account_world_music_release","release_id,title,type,released_on,label,catalog,cover_ref,notes,edition_of,mbid,ready") \
    AWM_COPY("account_world_music_track","release_id,disc,position,recording_id") \
    AWM_COPY("account_world_music_credit","target,target_id,role,name,detail,href") \
    AWM_COPY("account_world_music_relation","kind,name,href,begun,ended") \
    AWM_COPY("account_world_music_room","room_id,starts,target,target_id,note,ticket,state")
#define AWM_SCOPE "world_id INTEGER NOT NULL,space_id TEXT NOT NULL,released INTEGER NOT NULL CHECK(released IN(0,1)),"

int awm_initialize(sqlite3 *db) {
    if (!db || awa_initialize(db)!=1) return -1;
    const char *sql =
      "CREATE TABLE IF NOT EXISTS account_world_music_party(" AWM_SCOPE
      "kind TEXT NOT NULL CHECK(kind IN('artist','label')),name TEXT NOT NULL,mbid TEXT NOT NULL,area TEXT NOT NULL,begun TEXT NOT NULL,"
      "PRIMARY KEY(world_id,space_id,released));"
      "CREATE TABLE IF NOT EXISTS account_world_music_work(" AWM_SCOPE
      "work_id TEXT NOT NULL,title TEXT NOT NULL,iswc TEXT NOT NULL,mbid TEXT NOT NULL,PRIMARY KEY(world_id,space_id,released,work_id));"
      "CREATE TABLE IF NOT EXISTS account_world_music_recording(" AWM_SCOPE
      "recording_id TEXT NOT NULL,work_id TEXT NOT NULL,title TEXT NOT NULL,ref TEXT NOT NULL,isrc TEXT NOT NULL,mbid TEXT NOT NULL,"
      "PRIMARY KEY(world_id,space_id,released,recording_id));"
      "CREATE TABLE IF NOT EXISTS account_world_music_release(" AWM_SCOPE
      "release_id TEXT NOT NULL,title TEXT NOT NULL,type TEXT NOT NULL,released_on TEXT NOT NULL,label TEXT NOT NULL,catalog TEXT NOT NULL,"
      "cover_ref TEXT NOT NULL,notes TEXT NOT NULL,edition_of TEXT NOT NULL,mbid TEXT NOT NULL,ready INTEGER NOT NULL CHECK(ready IN(0,1)),"
      "PRIMARY KEY(world_id,space_id,released,release_id));"
      "CREATE TABLE IF NOT EXISTS account_world_music_track(" AWM_SCOPE
      "release_id TEXT NOT NULL,disc INTEGER NOT NULL CHECK(disc BETWEEN 1 AND 9),position INTEGER NOT NULL CHECK(position BETWEEN 1 AND 99),"
      "recording_id TEXT NOT NULL,PRIMARY KEY(world_id,space_id,released,release_id,disc,position));"
      "CREATE TABLE IF NOT EXISTS account_world_music_credit(id INTEGER PRIMARY KEY," AWM_SCOPE
      "target TEXT NOT NULL CHECK(target IN('work','recording','release')),target_id TEXT NOT NULL,role TEXT NOT NULL,name TEXT NOT NULL,"
      "detail TEXT NOT NULL,href TEXT NOT NULL,UNIQUE(world_id,space_id,released,target,target_id,role,name,detail));"
      "CREATE TABLE IF NOT EXISTS account_world_music_relation(id INTEGER PRIMARY KEY," AWM_SCOPE
      "kind TEXT NOT NULL,name TEXT NOT NULL,href TEXT NOT NULL,begun TEXT NOT NULL,ended TEXT NOT NULL,UNIQUE(world_id,space_id,released,kind,name));"
      "CREATE TABLE IF NOT EXISTS account_world_music_room(" AWM_SCOPE
      "room_id TEXT NOT NULL,starts TEXT NOT NULL,target TEXT NOT NULL CHECK(target IN('release','recording')),target_id TEXT NOT NULL,"
      "note TEXT NOT NULL,ticket TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('draft','announced','cancelled')),"
      "PRIMARY KEY(world_id,space_id,released,room_id));"
      "CREATE TABLE IF NOT EXISTS account_world_music_collection(member TEXT NOT NULL,namespace TEXT NOT NULL,world_id INTEGER NOT NULL,"
      "space_id TEXT NOT NULL,release_id TEXT NOT NULL,collected TEXT NOT NULL,PRIMARY KEY(member,namespace,space_id,release_id));"
      "CREATE INDEX IF NOT EXISTS account_world_music_collectors ON account_world_music_collection(world_id,space_id,release_id);"
      "CREATE TRIGGER IF NOT EXISTS account_world_music_release_copy AFTER INSERT ON account_world_release BEGIN " AWM_COPY_ALL " END;"
      "CREATE TRIGGER IF NOT EXISTS account_world_music_rerelease_copy AFTER UPDATE ON account_world_release BEGIN " AWM_COPY_ALL " END;";
    return sqlite3_exec(db,sql,NULL,NULL,NULL)==SQLITE_OK ? 1 : -1;
}

int awm_party_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *kind,const char *name,
                  const char *mbid,const char *area,const char *begun,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !kind || (strcmp(kind,"artist") && strcmp(kind,"label")) || !line_ok(name,160,1) ||
        !mbid_ok(mbid) || !line_ok(area,120,0) || !year_ok(begun)) return 0;
    int r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
    const char *v[]={kind,name,mbid,area,begun};
    r=q(db,"INSERT INTO account_world_music_party(world_id,space_id,released,kind,name,mbid,area,begun) VALUES(?1,?2,0,?3,?4,?5,?6,?7) "
           "ON CONFLICT(world_id,space_id,released) DO UPDATE SET kind=excluded.kind,name=excluded.name,mbid=excluded.mbid,area=excluded.area,begun=excluded.begun",
           id,space,v,5,NULL,0,NULL)<0?-1:1;
    return finish(db,r);
}
int awm_work_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *work,const char *title,
                 const char *iswc,const char *mbid,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !id_ok(work) || !line_ok(title,256,1) || !iswc_ok(iswc) || !mbid_ok(mbid)) return 0;
    int r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
    const char *v[]={work,title,iswc,mbid};
    r=q(db,"INSERT INTO account_world_music_work(world_id,space_id,released,work_id,title,iswc,mbid) VALUES(?1,?2,0,?3,?4,?5,?6) "
           "ON CONFLICT(world_id,space_id,released,work_id) DO UPDATE SET title=excluded.title,iswc=excluded.iswc,mbid=excluded.mbid",id,space,v,4,NULL,0,NULL)<0?-1:1;
    if (r==1) r=under(db,id,space,"account_world_music_work",AWM_WORK_LIMIT);
    return finish(db,r);
}
int awm_recording_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *rec,const char *work,
                      const char *title,const char *ref,const char *isrc,const char *mbid,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !id_ok(rec) || !work || (*work && !id_ok(work)) || !line_ok(title,256,1) ||
        !piece_ok(ref) || !isrc_ok(isrc) || !mbid_ok(mbid)) return 0;
    int r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
    if (*work) r=exists(db,id,space,"account_world_music_work","work_id",work);
    if (r==1) { const char *v[]={rec,work,title,ref,isrc,mbid};
        r=q(db,"INSERT INTO account_world_music_recording(world_id,space_id,released,recording_id,work_id,title,ref,isrc,mbid) VALUES(?1,?2,0,?3,?4,?5,?6,?7,?8) "
               "ON CONFLICT(world_id,space_id,released,recording_id) DO UPDATE SET work_id=excluded.work_id,title=excluded.title,ref=excluded.ref,isrc=excluded.isrc,mbid=excluded.mbid",
               id,space,v,6,NULL,0,NULL)<0?-1:1; }
    /* a changed recording changes every release it is on */
    if (r==1) { const char *v[]={rec};
        r=q(db,"UPDATE account_world_music_release SET ready=0 WHERE world_id=?1 AND space_id=?2 AND released=0 AND release_id IN("
               "SELECT release_id FROM account_world_music_track WHERE world_id=?1 AND space_id=?2 AND released=0 AND recording_id=?3)",id,space,v,1,NULL,0,NULL)<0?-1:1; }
    if (r==1) r=under(db,id,space,"account_world_music_recording",AWM_RECORDING_LIMIT);
    return finish(db,r);
}
int awm_release_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *rel,const char *title,
                    const char *type,const char *on,const char *label,const char *catalog,const char *cover,const char *notes,
                    const char *edition_of,const char *mbid,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !id_ok(rel) || !line_ok(title,256,1) || !one_of(type,RELEASE_TYPES) || !partial_date_ok(on) ||
        !line_ok(label,160,0) || (!strncmp(label,"space:",6) && !space_link_ok(label)) || !line_ok(catalog,64,0) ||
        !cover || (*cover && !piece_ok(cover)) || !prose_ok(notes,AWM_NOTES_LIMIT) || !edition_of || (*edition_of && (!id_ok(edition_of) || !strcmp(edition_of,rel))) ||
        !mbid_ok(mbid)) return 0;
    int r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
    if (*edition_of) r=exists(db,id,space,"account_world_music_release","release_id",edition_of);
    if (r==1) { /* an edition of an edition is not allowed: editions point at the original */
        const char *v[]={edition_of,rel};
        if (*edition_of && q(db,"SELECT 1 FROM account_world_music_release WHERE world_id=?1 AND space_id=?2 AND released=0 AND release_id=?3 AND edition_of<>''",id,space,v,1,NULL,0,NULL)!=0) r=0;
        if (r==1 && *edition_of && q(db,"SELECT 1 FROM account_world_music_release WHERE world_id=?1 AND space_id=?2 AND released=0 AND edition_of=?4",id,space,v,2,NULL,0,NULL)!=0) r=0;
    }
    if (r==1) { const char *v[]={rel,title,type,on,label,catalog,cover,notes,edition_of,mbid};
        r=q(db,"INSERT INTO account_world_music_release(world_id,space_id,released,release_id,title,type,released_on,label,catalog,cover_ref,notes,edition_of,mbid,ready) "
               "VALUES(?1,?2,0,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,0) ON CONFLICT(world_id,space_id,released,release_id) DO UPDATE SET title=excluded.title,type=excluded.type,"
               "released_on=excluded.released_on,label=excluded.label,catalog=excluded.catalog,cover_ref=excluded.cover_ref,notes=excluded.notes,"
               "edition_of=excluded.edition_of,mbid=excluded.mbid,ready=0",id,space,v,10,NULL,0,NULL)<0?-1:1; }
    if (r==1) r=under(db,id,space,"account_world_music_release",AWM_RELEASE_LIMIT);
    return finish(db,r);
}
int awm_release_ready(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *rel,int ready,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !id_ok(rel) || (ready!=0 && ready!=1)) return 0;
    int r=begin(db,id,subject,ns,space,revision),changes=0; if (r!=1) return r;
    const char *v[]={rel}; long long n[]={ready};
    if (ready && exists(db,id,space,"account_world_music_track","release_id",rel)!=1) r=0;
    if (r==1) r=q(db,"UPDATE account_world_music_release SET ready=?20 WHERE world_id=?1 AND space_id=?2 AND released=0 AND release_id=?3 AND ready<>?20",
                  id,space,v,1,n,1,&changes)<0?-1:changes==1;
    return finish(db,r);
}
int awm_track_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *rel,int disc,int position,
                  const char *rec,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !id_ok(rel) || disc<1 || disc>9 || position<1 || position>99 || !id_ok(rec)) return 0;
    int r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
    r=exists(db,id,space,"account_world_music_release","release_id",rel);
    if (r==1) r=exists(db,id,space,"account_world_music_recording","recording_id",rec);
    if (r==1) { const char *v[]={rel,rec}; long long n[]={disc,position};
        r=q(db,"INSERT INTO account_world_music_track(world_id,space_id,released,release_id,disc,position,recording_id) VALUES(?1,?2,0,?3,?20,?21,?4) "
               "ON CONFLICT(world_id,space_id,released,release_id,disc,position) DO UPDATE SET recording_id=excluded.recording_id",id,space,v,2,n,2,NULL)<0?-1:1; }
    if (r==1) r=unready(db,id,space,rel);
    if (r==1) { const char *v[]={rel}; long long n[]={AWM_TRACK_LIMIT};
        int over=q(db,"SELECT 1 WHERE (SELECT count(*) FROM account_world_music_track WHERE world_id=?1 AND space_id=?2 AND released=0 AND release_id=?3)>?20",id,space,v,1,n,1,NULL);
        r=over<0?-1:!over; }
    return finish(db,r);
}
int awm_track_remove(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *rel,int disc,int position,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !id_ok(rel) || disc<1 || disc>9 || position<1 || position>99) return 0;
    int r=begin(db,id,subject,ns,space,revision),changes=0; if (r!=1) return r;
    const char *v[]={rel}; long long n[]={disc,position};
    r=q(db,"DELETE FROM account_world_music_track WHERE world_id=?1 AND space_id=?2 AND released=0 AND release_id=?3 AND disc=?20 AND position=?21",id,space,v,1,n,2,&changes)<0?-1:changes==1;
    if (r==1) r=unready(db,id,space,rel);
    return finish(db,r);
}
int awm_credit_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *target,const char *target_id,
                   const char *role,const char *name,const char *detail,const char *href,int revision) {
    const char *table=NULL,*col=NULL;const char *const *roles=NULL;
    if (target && !strcmp(target,"work")) { table="account_world_music_work"; col="work_id"; roles=WORK_ROLES; }
    else if (target && !strcmp(target,"recording")) { table="account_world_music_recording"; col="recording_id"; roles=RECORDING_ROLES; }
    else if (target && !strcmp(target,"release")) { table="account_world_music_release"; col="release_id"; roles=RELEASE_ROLES; }
    if (!table || !common(db,subject,ns,id,space,revision) || !id_ok(target_id) || !one_of(role,roles) || !line_ok(name,160,1) ||
        !line_ok(detail,64,0) || !link_ok(href)) return 0;
    int r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
    r=exists(db,id,space,table,col,target_id);
    if (r==1) { const char *v[]={target,target_id,role,name,detail,href};
        r=q(db,"INSERT INTO account_world_music_credit(world_id,space_id,released,target,target_id,role,name,detail,href) VALUES(?1,?2,0,?3,?4,?5,?6,?7,?8) "
               "ON CONFLICT(world_id,space_id,released,target,target_id,role,name,detail) DO UPDATE SET href=excluded.href",id,space,v,6,NULL,0,NULL)<0?-1:1; }
    if (r==1 && !strcmp(target,"release")) r=unready(db,id,space,target_id);
    if (r==1 && strcmp(target,"release")) { /* a credit change on a recording or work reopens the releases that carry it */
        const char *v[]={target_id};
        r=q(db,!strcmp(target,"recording") ?
              "UPDATE account_world_music_release SET ready=0 WHERE world_id=?1 AND space_id=?2 AND released=0 AND release_id IN(SELECT release_id FROM account_world_music_track "
              "WHERE world_id=?1 AND space_id=?2 AND released=0 AND recording_id=?3)" :
              "UPDATE account_world_music_release SET ready=0 WHERE world_id=?1 AND space_id=?2 AND released=0 AND release_id IN(SELECT t.release_id FROM account_world_music_track t "
              "JOIN account_world_music_recording c ON c.world_id=t.world_id AND c.space_id=t.space_id AND c.released=0 AND c.recording_id=t.recording_id "
              "WHERE t.world_id=?1 AND t.space_id=?2 AND t.released=0 AND c.work_id=?3)",id,space,v,1,NULL,0,NULL)<0?-1:1; }
    if (r==1) r=under(db,id,space,"account_world_music_credit",AWM_CREDIT_LIMIT);
    return finish(db,r);
}
int awm_credit_remove(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,sqlite3_int64 credit,int revision) {
    if (!common(db,subject,ns,id,space,revision) || credit<1) return 0;
    int r=begin(db,id,subject,ns,space,revision),changes=0; if (r!=1) return r;
    long long n[]={credit};
    r=q(db,"UPDATE account_world_music_release SET ready=0 WHERE world_id=?1 AND space_id=?2 AND released=0 AND release_id IN("
           "SELECT target_id FROM account_world_music_credit WHERE id=?20 AND target='release' AND released=0)",id,space,NULL,0,n,1,NULL)<0?-1:1;
    if (r==1) r=q(db,"DELETE FROM account_world_music_credit WHERE id=?20 AND world_id=?1 AND space_id=?2 AND released=0",id,space,NULL,0,n,1,&changes)<0?-1:changes==1;
    return finish(db,r);
}
int awm_relation_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *kind,const char *name,
                     const char *href,const char *begun,const char *ended,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !one_of(kind,RELATION_KINDS) || !line_ok(name,160,1) || !link_ok(href) ||
        !year_ok(begun) || !year_ok(ended) || (*begun && *ended && strcmp(ended,begun)<0)) return 0;
    int r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
    const char *v[]={kind,name,href,begun,ended};
    r=q(db,"INSERT INTO account_world_music_relation(world_id,space_id,released,kind,name,href,begun,ended) VALUES(?1,?2,0,?3,?4,?5,?6,?7) "
           "ON CONFLICT(world_id,space_id,released,kind,name) DO UPDATE SET href=excluded.href,begun=excluded.begun,ended=excluded.ended",id,space,v,5,NULL,0,NULL)<0?-1:1;
    if (r==1) r=under(db,id,space,"account_world_music_relation",AWM_RELATION_LIMIT);
    return finish(db,r);
}
int awm_relation_remove(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,sqlite3_int64 relation,int revision) {
    if (!common(db,subject,ns,id,space,revision) || relation<1) return 0;
    int r=begin(db,id,subject,ns,space,revision),changes=0; if (r!=1) return r;
    long long n[]={relation};
    r=q(db,"DELETE FROM account_world_music_relation WHERE id=?20 AND world_id=?1 AND space_id=?2 AND released=0",id,space,NULL,0,n,1,&changes)<0?-1:changes==1;
    return finish(db,r);
}
int awm_room_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *room,const char *starts,
                 const char *target,const char *target_id,const char *note,const char *ticket,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !id_ok(room) || !minute_ok(starts) || !target || (strcmp(target,"release") && strcmp(target,"recording")) ||
        !id_ok(target_id) || !line_ok(note,280,0) || !ticket || (*ticket && !https_ok(ticket))) return 0;
    int r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
    r=!strcmp(target,"release") ? exists(db,id,space,"account_world_music_release","release_id",target_id)
                                : exists(db,id,space,"account_world_music_recording","recording_id",target_id);
    if (r==1) { const char *v[]={room}; /* a cancelled room stays as it was */
        if (q(db,"SELECT 1 FROM account_world_music_room WHERE world_id=?1 AND space_id=?2 AND released=0 AND room_id=?3 AND state='cancelled'",id,space,v,1,NULL,0,NULL)!=0) r=0; }
    if (r==1) { const char *v[]={room,starts,target,target_id,note,ticket};
        r=q(db,"INSERT INTO account_world_music_room(world_id,space_id,released,room_id,starts,target,target_id,note,ticket,state) VALUES(?1,?2,0,?3,?4,?5,?6,?7,?8,'draft') "
               "ON CONFLICT(world_id,space_id,released,room_id) DO UPDATE SET starts=excluded.starts,target=excluded.target,target_id=excluded.target_id,"
               "note=excluded.note,ticket=excluded.ticket,state='draft'",id,space,v,6,NULL,0,NULL)<0?-1:1; }
    if (r==1) r=under(db,id,space,"account_world_music_room",AWM_ROOM_LIMIT);
    return finish(db,r);
}
int awm_room_state(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *room,const char *state,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !id_ok(room) || !state || (strcmp(state,"announced") && strcmp(state,"cancelled") && strcmp(state,"draft"))) return 0;
    int r=begin(db,id,subject,ns,space,revision),changes=0; if (r!=1) return r;
    const char *v[]={room,state};
    /* draft -> announced; announced -> cancelled or back to draft; a cancelled draft is deleted */
    if (!strcmp(state,"cancelled") && q(db,"SELECT 1 FROM account_world_music_room WHERE world_id=?1 AND space_id=?2 AND released=0 AND room_id=?3 AND state='draft'",id,space,v,1,NULL,0,NULL)==1)
        r=q(db,"DELETE FROM account_world_music_room WHERE world_id=?1 AND space_id=?2 AND released=0 AND room_id=?3",id,space,v,1,NULL,0,&changes)<0?-1:changes==1;
    else
        r=q(db,"UPDATE account_world_music_room SET state=?4 WHERE world_id=?1 AND space_id=?2 AND released=0 AND room_id=?3 AND "
               "((state='draft' AND ?4='announced') OR (state='announced' AND ?4 IN('cancelled','draft')))",id,space,v,2,NULL,0,&changes)<0?-1:changes==1;
    return finish(db,r);
}
int awm_remove(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *what,const char *key,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !id_ok(key) || !what) return 0;
    const char *v[]={key};int r,changes=0;
    if (!strcmp(what,"work")) {
        r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
        if (exists(db,id,space,"account_world_music_recording","work_id",key)!=0) r=0;
        if (r==1) r=q(db,"DELETE FROM account_world_music_credit WHERE world_id=?1 AND space_id=?2 AND released=0 AND target='work' AND target_id=?3",id,space,v,1,NULL,0,NULL)<0?-1:1;
        if (r==1) r=q(db,"DELETE FROM account_world_music_work WHERE world_id=?1 AND space_id=?2 AND released=0 AND work_id=?3",id,space,v,1,NULL,0,&changes)<0?-1:changes==1;
    } else if (!strcmp(what,"recording")) {
        r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
        if (exists(db,id,space,"account_world_music_track","recording_id",key)!=0) r=0;
        if (r==1 && q(db,"SELECT 1 FROM account_world_music_room WHERE world_id=?1 AND space_id=?2 AND released=0 AND target='recording' AND target_id=?3",id,space,v,1,NULL,0,NULL)!=0) r=0;
        if (r==1) r=q(db,"DELETE FROM account_world_music_credit WHERE world_id=?1 AND space_id=?2 AND released=0 AND target='recording' AND target_id=?3",id,space,v,1,NULL,0,NULL)<0?-1:1;
        if (r==1) r=q(db,"DELETE FROM account_world_music_recording WHERE world_id=?1 AND space_id=?2 AND released=0 AND recording_id=?3",id,space,v,1,NULL,0,&changes)<0?-1:changes==1;
    } else if (!strcmp(what,"release")) {
        r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
        if (exists(db,id,space,"account_world_music_release","edition_of",key)!=0) r=0;
        if (r==1 && q(db,"SELECT 1 FROM account_world_music_room WHERE world_id=?1 AND space_id=?2 AND released=0 AND target='release' AND target_id=?3",id,space,v,1,NULL,0,NULL)!=0) r=0;
        if (r==1) r=q(db,"DELETE FROM account_world_music_credit WHERE world_id=?1 AND space_id=?2 AND released=0 AND target='release' AND target_id=?3",id,space,v,1,NULL,0,NULL)<0?-1:1;
        if (r==1) r=q(db,"DELETE FROM account_world_music_track WHERE world_id=?1 AND space_id=?2 AND released=0 AND release_id=?3",id,space,v,1,NULL,0,NULL)<0?-1:1;
        if (r==1) r=q(db,"DELETE FROM account_world_music_release WHERE world_id=?1 AND space_id=?2 AND released=0 AND release_id=?3",id,space,v,1,NULL,0,&changes)<0?-1:changes==1;
    } else if (!strcmp(what,"room")) {
        r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
        r=q(db,"DELETE FROM account_world_music_room WHERE world_id=?1 AND space_id=?2 AND released=0 AND room_id=?3",id,space,v,1,NULL,0,&changes)<0?-1:changes==1;
    } else return 0;
    return finish(db,r);
}

/* Public reach: what a released copy shows. Aliases: t is the row. */
#define PUB_REL(alias) "EXISTS(SELECT 1 FROM account_world_music_release pr WHERE pr.world_id=" alias ".world_id AND pr.space_id=" alias ".space_id AND pr.released=1 AND pr.ready=1 AND pr.release_id="
#define PUB_REC_ON_READY(idexpr) "EXISTS(SELECT 1 FROM account_world_music_track pt JOIN account_world_music_release pr ON pr.world_id=pt.world_id AND pr.space_id=pt.space_id " \
    "AND pr.released=1 AND pr.ready=1 AND pr.release_id=pt.release_id WHERE pt.world_id=t.world_id AND pt.space_id=t.space_id AND pt.released=1 AND pt.recording_id=" idexpr ")"
#define PUB_WORK(idexpr) "EXISTS(SELECT 1 FROM account_world_music_recording pc WHERE pc.world_id=t.world_id AND pc.space_id=t.space_id AND pc.released=1 AND pc.work_id=" idexpr \
    " AND EXISTS(SELECT 1 FROM account_world_music_track pt JOIN account_world_music_release pr ON pr.world_id=pt.world_id AND pr.space_id=pt.space_id AND pr.released=1 " \
    "AND pr.ready=1 AND pr.release_id=pt.release_id WHERE pt.world_id=pc.world_id AND pt.space_id=pc.space_id AND pt.released=1 AND pt.recording_id=pc.recording_id))"
#define PUBLIC_SPACE "JOIN account_world_release r ON r.world_id=t.world_id AND r.space_id=t.space_id AND r.state='public' AND r.namespace=?4 " \
    "JOIN account_world_binding b ON b.world_id=r.world_id AND b.namespace=r.namespace " \
    "JOIN world w ON w.id=b.world_id AND w.namespace=b.namespace AND w.subject=b.subject AND w.status IN('draft','published') "

int awm_rows(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,int table,const char *key,sqlite3_stmt **rows) {
    if (rows) *rows=NULL;
    if (!db || !rows || !key_ok(ns,127) || id<=0 || !key_ok(space,64) || table<AWM_PARTY || table>AWM_ROOMS || (subject && !key_ok(subject,256)) ||
        (key && !key_ok(key,80))) return 0;
    static const char *cols[]={
        "t.kind,t.name,t.mbid,t.area,t.begun FROM account_world_music_party t",
        "t.work_id,t.title,t.iswc,t.mbid FROM account_world_music_work t",
        "t.recording_id,t.work_id,t.title,t.ref,t.isrc,t.mbid FROM account_world_music_recording t",
        "t.release_id,t.title,t.type,t.released_on,t.label,t.catalog,t.cover_ref,t.notes,t.edition_of,t.mbid,t.ready FROM account_world_music_release t",
        "t.release_id,t.disc,t.position,t.recording_id FROM account_world_music_track t",
        "t.id,t.target,t.target_id,t.role,t.name,t.detail,t.href FROM account_world_music_credit t",
        "t.id,t.kind,t.name,t.href,t.begun,t.ended FROM account_world_music_relation t",
        "t.room_id,t.starts,t.target,t.target_id,t.note,t.ticket,t.state FROM account_world_music_room t"};
    static const char *keyed[]={"",
        " AND t.work_id=?3"," AND t.recording_id=?3"," AND t.release_id=?3"," AND t.release_id=?3"," AND t.target||':'||t.target_id=?3",""," AND t.room_id=?3"};
    static const char *reach[]={"",
        " AND " PUB_WORK("t.work_id"),
        " AND " PUB_REC_ON_READY("t.recording_id"),
        " AND t.ready=1",
        " AND " PUB_REL("t") "t.release_id)",
        " AND ((t.target='release' AND " PUB_REL("t") "t.target_id)) OR (t.target='recording' AND " PUB_REC_ON_READY("t.target_id") ") OR (t.target='work' AND " PUB_WORK("t.target_id") "))",
        "",
        " AND t.state IN('announced','cancelled') AND ((t.target='release' AND " PUB_REL("t") "t.target_id)) OR (t.target='recording' AND " PUB_REC_ON_READY("t.target_id") "))"};
    static const char *order[]={"",
        " ORDER BY t.title,t.work_id"," ORDER BY t.title,t.recording_id",
        " ORDER BY CASE WHEN t.released_on='' THEN 1 ELSE 0 END,t.released_on DESC,t.title"," ORDER BY t.release_id,t.disc,t.position",
        " ORDER BY t.target,t.target_id,t.role,t.name"," ORDER BY CASE WHEN t.begun='' THEN 1 ELSE 0 END,t.begun,t.name"," ORDER BY t.starts,t.room_id"};
    char sql[4096];
    if (subject) {
        int g=guarded(db,id,subject,ns); if (g!=1) return g;
        snprintf(sql,sizeof(sql),"SELECT %s WHERE t.world_id=?1 AND t.space_id=?2 AND t.released=0%s%s LIMIT 5000",cols[table],key?keyed[table]:"",order[table]);
    } else
        snprintf(sql,sizeof(sql),"SELECT %s " PUBLIC_SPACE "WHERE t.world_id=?1 AND t.space_id=?2 AND t.released=1%s%s%s LIMIT 5000",cols[table],reach[table],key?keyed[table]:"",order[table]);
    if (sqlite3_prepare_v2(db,sql,-1,rows,NULL)!=SQLITE_OK) { *rows=NULL; return -1; }
    sqlite3_bind_int64(*rows,1,id); sqlite3_bind_text(*rows,2,space,-1,SQLITE_TRANSIENT);
    if (key && *keyed[table]) sqlite3_bind_text(*rows,3,key,-1,SQLITE_TRANSIENT);
    if (!subject) sqlite3_bind_text(*rows,4,ns,-1,SQLITE_TRANSIENT);
    return 1;
}

int awm_label_releases(sqlite3 *db,const char *ns,sqlite3_int64 id,const char *label_space,sqlite3_stmt **rows) {
    if (rows) *rows=NULL;
    if (!db || !rows || !key_ok(ns,127) || id<=0 || !id_ok(label_space)) return 0;
    char label[80]; snprintf(label,sizeof(label),"space:%s",label_space);
    const char *sql="SELECT t.space_id,t.release_id,t.title,t.type,t.released_on,t.cover_ref FROM account_world_music_release t " PUBLIC_SPACE
                    "WHERE t.world_id=?1 AND t.released=1 AND t.ready=1 AND t.label=?2 AND t.space_id<>?3 "
                    "ORDER BY CASE WHEN t.released_on='' THEN 1 ELSE 0 END,t.released_on DESC,t.title LIMIT 500";
    if (sqlite3_prepare_v2(db,sql,-1,rows,NULL)!=SQLITE_OK) { *rows=NULL; return -1; }
    sqlite3_bind_int64(*rows,1,id); sqlite3_bind_text(*rows,2,label,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(*rows,3,label_space,-1,SQLITE_TRANSIENT); sqlite3_bind_text(*rows,4,ns,-1,SQLITE_TRANSIENT);
    return 1;
}
int awm_heard_on(sqlite3 *db,const char *ns,sqlite3_int64 id,const char *ref,sqlite3_stmt **rows) {
    if (rows) *rows=NULL;
    if (!db || !rows || !key_ok(ns,127) || id<=0 || !piece_ok(ref)) return 0;
    const char *sql=
      "SELECT t.space_id,r.title,r.medium,'played',t.start_ms,t.ref FROM account_world_mark t " PUBLIC_SPACE
      "WHERE t.world_id=?1 AND t.released=1 AND t.kind='track' AND t.href=?2 "
      "UNION ALL SELECT t.space_id,r.title,r.medium,'holds',0,'' FROM account_world_release_item t " PUBLIC_SPACE
      "WHERE t.world_id=?1 AND t.ref=?2 ORDER BY 1,4,5 LIMIT 200";
    if (sqlite3_prepare_v2(db,sql,-1,rows,NULL)!=SQLITE_OK) { *rows=NULL; return -1; }
    sqlite3_bind_int64(*rows,1,id); sqlite3_bind_text(*rows,2,ref,-1,SQLITE_TRANSIENT); sqlite3_bind_text(*rows,4,ns,-1,SQLITE_TRANSIENT);
    return 1;
}

/* ------------------------------------------------------------ collections */
static const char *PUBLIC_RELEASE=
    "SELECT t.world_id FROM account_world_music_release t " PUBLIC_SPACE
    "WHERE t.space_id=?2 AND t.released=1 AND t.ready=1 AND t.release_id=?3";
int awm_collect(sqlite3 *db,const char *member,const char *ns,const char *space,const char *rel) {
    if (!db || !key_ok(member,256) || !key_ok(ns,127) || !id_ok(space) || !id_ok(rel)) return 0;
    if (sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL)!=SQLITE_OK) return -1;
    sqlite3_stmt *s=NULL; int r=-1; sqlite3_int64 world=0;
    if (sqlite3_prepare_v2(db,PUBLIC_RELEASE,-1,&s,NULL)==SQLITE_OK) {
        sqlite3_bind_text(s,2,space,-1,SQLITE_STATIC); sqlite3_bind_text(s,3,rel,-1,SQLITE_STATIC); sqlite3_bind_text(s,4,ns,-1,SQLITE_STATIC);
        int step=sqlite3_step(s); if (step==SQLITE_ROW) { world=sqlite3_column_int64(s,0); r=1; } else r=step==SQLITE_DONE?0:-1;
    }
    sqlite3_finalize(s); s=NULL;
    if (r==1 && sqlite3_prepare_v2(db,"SELECT count(*) FROM account_world_music_collection WHERE member=?1",-1,&s,NULL)==SQLITE_OK) {
        sqlite3_bind_text(s,1,member,-1,SQLITE_STATIC); r=sqlite3_step(s)==SQLITE_ROW ? sqlite3_column_int(s,0)<AWM_COLLECTION_LIMIT : -1;
    } else if (r==1) r=-1;
    sqlite3_finalize(s); s=NULL;
    if (r==1) {
        char day[11]; time_t now=time(NULL); struct tm g; gmtime_r(&now,&g); strftime(day,sizeof(day),"%Y-%m-%d",&g);
        if (sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO account_world_music_collection(member,namespace,world_id,space_id,release_id,collected) VALUES(?1,?2,?3,?4,?5,?6)",-1,&s,NULL)!=SQLITE_OK) r=-1;
        else { sqlite3_bind_text(s,1,member,-1,SQLITE_STATIC); sqlite3_bind_text(s,2,ns,-1,SQLITE_STATIC); sqlite3_bind_int64(s,3,world);
               sqlite3_bind_text(s,4,space,-1,SQLITE_STATIC); sqlite3_bind_text(s,5,rel,-1,SQLITE_STATIC); sqlite3_bind_text(s,6,day,-1,SQLITE_STATIC);
               r=sqlite3_step(s)==SQLITE_DONE ? sqlite3_changes(db)==1 : -1; }
        sqlite3_finalize(s);
    }
    return finish(db,r);
}
int awm_uncollect(sqlite3 *db,const char *member,const char *ns,const char *space,const char *rel) {
    if (!db || !key_ok(member,256) || !key_ok(ns,127) || !id_ok(space) || !id_ok(rel)) return 0;
    sqlite3_stmt *s=NULL;
    if (sqlite3_prepare_v2(db,"DELETE FROM account_world_music_collection WHERE member=?1 AND namespace=?2 AND space_id=?3 AND release_id=?4",-1,&s,NULL)!=SQLITE_OK) return -1;
    sqlite3_bind_text(s,1,member,-1,SQLITE_STATIC); sqlite3_bind_text(s,2,ns,-1,SQLITE_STATIC); sqlite3_bind_text(s,3,space,-1,SQLITE_STATIC); sqlite3_bind_text(s,4,rel,-1,SQLITE_STATIC);
    int r=sqlite3_step(s)==SQLITE_DONE ? sqlite3_changes(db)==1 : -1; sqlite3_finalize(s); return r;
}
int awm_collected(sqlite3 *db,const char *member,const char *ns,const char *space,const char *rel) {
    if (!db || !key_ok(member,256) || !key_ok(ns,127) || !id_ok(space) || !id_ok(rel)) return 0;
    sqlite3_stmt *s=NULL;
    if (sqlite3_prepare_v2(db,"SELECT 1 FROM account_world_music_collection WHERE member=?1 AND namespace=?2 AND space_id=?3 AND release_id=?4",-1,&s,NULL)!=SQLITE_OK) return 0;
    sqlite3_bind_text(s,1,member,-1,SQLITE_STATIC); sqlite3_bind_text(s,2,ns,-1,SQLITE_STATIC); sqlite3_bind_text(s,3,space,-1,SQLITE_STATIC); sqlite3_bind_text(s,4,rel,-1,SQLITE_STATIC);
    int r=sqlite3_step(s)==SQLITE_ROW; sqlite3_finalize(s); return r;
}
int awm_collection(sqlite3 *db,const char *member,sqlite3_stmt **rows) {
    if (rows) *rows=NULL;
    if (!db || !rows || !key_ok(member,256)) return 0;
    const char *sql=
      "SELECT c.namespace,c.space_id,c.release_id,coalesce(t.title,''),coalesce(t.type,''),coalesce(t.released_on,''),c.collected,"
      "(SELECT count(*) FROM account_world_music_release t2 JOIN account_world_release r ON r.world_id=t2.world_id AND r.space_id=t2.space_id AND r.state='public' AND r.namespace=c.namespace "
      " JOIN account_world_binding b ON b.world_id=r.world_id AND b.namespace=r.namespace JOIN world w ON w.id=b.world_id AND w.namespace=b.namespace AND w.subject=b.subject AND w.status IN('draft','published') "
      " WHERE t2.world_id=c.world_id AND t2.space_id=c.space_id AND t2.released=1 AND t2.ready=1 AND t2.release_id=c.release_id)>0 "
      "FROM account_world_music_collection c LEFT JOIN account_world_music_release t ON t.world_id=c.world_id AND t.space_id=c.space_id AND t.released=1 AND t.release_id=c.release_id "
      "WHERE c.member=?1 ORDER BY c.collected DESC,c.rowid DESC LIMIT 2000";
    if (sqlite3_prepare_v2(db,sql,-1,rows,NULL)!=SQLITE_OK) { *rows=NULL; return -1; }
    sqlite3_bind_text(*rows,1,member,-1,SQLITE_TRANSIENT); return 1;
}
int awm_collectors(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,sqlite3_stmt **rows) {
    if (rows) *rows=NULL;
    if (!db || !rows || !key_ok(subject,256) || !key_ok(ns,127) || id<=0 || !key_ok(space,64)) return 0;
    int g=guarded(db,id,subject,ns); if (g!=1) return g;
    if (sqlite3_prepare_v2(db,"SELECT release_id,count(*) FROM account_world_music_collection WHERE world_id=?1 AND space_id=?2 AND namespace=?3 GROUP BY release_id ORDER BY 2 DESC,1",-1,rows,NULL)!=SQLITE_OK) { *rows=NULL; return -1; }
    sqlite3_bind_int64(*rows,1,id); sqlite3_bind_text(*rows,2,space,-1,SQLITE_TRANSIENT); sqlite3_bind_text(*rows,3,ns,-1,SQLITE_TRANSIENT); return 1;
}
