#include "account_world_audio.h"
#include "account_world.h"
#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Sound additions to the account-world owner. See account_world_audio.h. */
static int key_ok(const char *s, size_t max) {
    if (!s || !*s || strlen(s) > max) return 0;
    return strspn(s,"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._:-/")==strlen(s);
}
static int line_ok(const char *s, size_t max, int required) { /* one line of prose */
    if (!s || strlen(s) > max || (required && (!*s || strspn(s," \t")==strlen(s)))) return 0;
    for (const unsigned char *p=(const unsigned char *)s; *p; p++) if (*p<32 || *p==127) return 0;
    return 1;
}
static int piece_ok(const char *ref) { /* "<source>/<24 hex key>" only */
    const char *slash = ref ? strchr(ref,'/') : NULL;
    if (!slash || slash==ref || (size_t)(slash-ref)>95 || strlen(slash+1)!=24 || strspn(slash+1,"0123456789abcdef")!=24) return 0;
    return strspn(ref,"abcdefghijklmnopqrstuvwxyz0123456789-_.")==(size_t)(slash-ref);
}
static int space_ref_ok(const char *ref) { return ref && !strncmp(ref,"space:",6) && key_ok(ref+6,64); }
static int https_ok(const char *u) {
    if (!u || strncmp(u,"https://",8) || strlen(u) > 512 || !u[8] || u[8]=='/') return 0;
    for (const unsigned char *p=(const unsigned char *)u; *p; p++) if (*p<=32 || *p>=127 || strchr("\"'<>\\`",*p)) return 0;
    return 1;
}
static int zone_ok(const char *z) {
    if (!z || !strcmp(z,"UTC")) return z!=NULL;
    if (!key_ok(z,64) || !strchr(z,'/') || strstr(z,"..") || *z=='/' || strchr(z,':')) return 0;
    return strspn(z,"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_+-/")==strlen(z);
}
static int date_ok(const char *d) {
    if (!d || strlen(d)!=10 || d[4]!='-' || d[7]!='-') return 0;
    for (int i=0;i<10;i++) if (i!=4 && i!=7 && !isdigit((unsigned char)d[i])) return 0;
    int y=atoi(d), m=atoi(d+5), day=atoi(d+8);
    static const int len[]={31,29,31,30,31,30,31,31,30,31,30,31};
    return y>=1900 && y<=2999 && m>=1 && m<=12 && day>=1 && day<=len[m-1] && !(m==2 && day==29 && (y%4 || (y%100==0 && y%400)));
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
/* Bind world/space as ?1/?2, then texts from ?3 and integers from ?10. */
static int exec_rows(sqlite3 *db,const char *sql,sqlite3_int64 id,const char *space,const char **text,int ntext,const long long *num,int nnum,int *changes) {
    sqlite3_stmt *s=NULL; if (sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK) return -1;
    sqlite3_bind_int64(s,1,id); sqlite3_bind_text(s,2,space,-1,SQLITE_STATIC);
    for (int i=0;i<ntext;i++) sqlite3_bind_text(s,3+i,text[i],-1,SQLITE_STATIC);
    for (int i=0;i<nnum;i++) sqlite3_bind_int64(s,10+i,num[i]);
    int step=sqlite3_step(s); sqlite3_finalize(s);
    if (step!=SQLITE_DONE && step!=SQLITE_ROW) return -1;
    if (changes) *changes=sqlite3_changes(db);
    return step==SQLITE_ROW;
}
static int is_item(sqlite3 *db,sqlite3_int64 id,const char *space,const char *ref) {
    const char *v[]={ref};
    return exec_rows(db,"SELECT 1 FROM account_world_item WHERE world_id=?1 AND space_id=?2 AND ref=?3",id,space,v,1,NULL,0,NULL);
}

#define AWA_COPY(table, cols) \
    "DELETE FROM " table " WHERE world_id=NEW.world_id AND space_id=NEW.space_id AND released=1;" \
    "INSERT INTO " table "(world_id,space_id,released," cols ") SELECT world_id,space_id,1," cols " FROM " table \
    " WHERE world_id=NEW.world_id AND space_id=NEW.space_id AND released=0 AND NEW.state='public';"
#define AWA_COPY_ALL \
    AWA_COPY("account_world_sound","kind,timezone,language,author,category,explicit,stream_ref") \
    AWA_COPY("account_world_slot","slot,weekday,start_minute,minutes,programme,host,ref") \
    AWA_COPY("account_world_episode","ref,season,number,published") \
    AWA_COPY("account_world_mark","ref,kind,start_ms,end_ms,title,credit,href,checked")

int awa_initialize(sqlite3 *db) {
    if (!db || aw_initialize(db)!=1) return -1;
    const char *sql =
      "CREATE TABLE IF NOT EXISTS account_world_sound("
      "world_id INTEGER NOT NULL,space_id TEXT NOT NULL,released INTEGER NOT NULL CHECK(released IN(0,1)),"
      "kind TEXT NOT NULL CHECK(kind IN('station','show')),timezone TEXT NOT NULL,language TEXT NOT NULL,author TEXT NOT NULL,"
      "category TEXT NOT NULL,explicit INTEGER NOT NULL CHECK(explicit IN(0,1)),stream_ref TEXT NOT NULL,"
      "PRIMARY KEY(world_id,space_id,released));"
      "CREATE TABLE IF NOT EXISTS account_world_slot("
      "world_id INTEGER NOT NULL,space_id TEXT NOT NULL,released INTEGER NOT NULL CHECK(released IN(0,1)),"
      "slot INTEGER NOT NULL CHECK(slot BETWEEN 1 AND 48),weekday INTEGER NOT NULL CHECK(weekday BETWEEN 1 AND 7),"
      "start_minute INTEGER NOT NULL CHECK(start_minute BETWEEN 0 AND 1439),minutes INTEGER NOT NULL CHECK(minutes BETWEEN 1 AND 1440),"
      "programme TEXT NOT NULL,host TEXT NOT NULL,ref TEXT NOT NULL,PRIMARY KEY(world_id,space_id,released,slot));"
      "CREATE TABLE IF NOT EXISTS account_world_episode("
      "world_id INTEGER NOT NULL,space_id TEXT NOT NULL,released INTEGER NOT NULL CHECK(released IN(0,1)),ref TEXT NOT NULL,"
      "season INTEGER NOT NULL CHECK(season BETWEEN 0 AND 999),number INTEGER NOT NULL CHECK(number BETWEEN 0 AND 9999),"
      "published TEXT NOT NULL,PRIMARY KEY(world_id,space_id,released,ref));"
      "CREATE TABLE IF NOT EXISTS account_world_mark("
      "id INTEGER PRIMARY KEY,world_id INTEGER NOT NULL,space_id TEXT NOT NULL,released INTEGER NOT NULL CHECK(released IN(0,1)),"
      "ref TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN('track','chapter','cue','clip','person','source')),"
      "start_ms INTEGER NOT NULL CHECK(start_ms BETWEEN 0 AND 86400000),end_ms INTEGER NOT NULL CHECK(end_ms=0 OR end_ms>start_ms),"
      "title TEXT NOT NULL,credit TEXT NOT NULL,href TEXT NOT NULL,checked INTEGER NOT NULL CHECK(checked IN(0,1)),"
      "UNIQUE(world_id,space_id,released,ref,kind,start_ms,title));"
      "CREATE TRIGGER IF NOT EXISTS account_world_sound_release AFTER INSERT ON account_world_release BEGIN " AWA_COPY_ALL " END;"
      "CREATE TRIGGER IF NOT EXISTS account_world_sound_rerelease AFTER UPDATE ON account_world_release BEGIN " AWA_COPY_ALL " END;";
    return sqlite3_exec(db,sql,NULL,NULL,NULL)==SQLITE_OK ? 1 : -1;
}

int awa_sound_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *kind,const char *timezone,
                  const char *language,const char *author,const char *category,int explicit_content,const char *stream_ref,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !kind || (strcmp(kind,"station") && strcmp(kind,"show")) || !zone_ok(timezone) ||
        !language || strlen(language)<2 || strlen(language)>16 || strspn(language,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-")!=strlen(language) ||
        !line_ok(author,160,1) || !line_ok(category,64,1) || (explicit_content!=0 && explicit_content!=1) ||
        !stream_ref || (*stream_ref && !piece_ok(stream_ref))) return 0;
    int r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
    const char *v[]={kind,timezone,language,author,category,stream_ref}; long long n[]={explicit_content};
    r=exec_rows(db,"INSERT INTO account_world_sound(world_id,space_id,released,kind,timezone,language,author,category,explicit,stream_ref) "
                   "VALUES(?1,?2,0,?3,?4,?5,?6,?7,?10,?8) ON CONFLICT(world_id,space_id,released) DO UPDATE SET kind=excluded.kind,"
                   "timezone=excluded.timezone,language=excluded.language,author=excluded.author,category=excluded.category,"
                   "explicit=excluded.explicit,stream_ref=excluded.stream_ref",id,space,v,6,n,1,NULL)<0?-1:1;
    return finish(db,r);
}
int awa_slot_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,int slot,int weekday,int start,int minutes,
                 const char *programme,const char *host,const char *ref,int revision) {
    if (!common(db,subject,ns,id,space,revision) || slot<1 || slot>AWA_SLOT_LIMIT || weekday<1 || weekday>7 || start<0 || start>1439 ||
        minutes<1 || minutes>1440 || !line_ok(programme,160,1) || !line_ok(host,160,0) || !ref || (*ref && !piece_ok(ref) && !space_ref_ok(ref))) return 0;
    int r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
    const char *v[]={programme,host,ref}; long long n[]={slot,weekday,start,minutes};
    r=exec_rows(db,"INSERT INTO account_world_slot(world_id,space_id,released,slot,weekday,start_minute,minutes,programme,host,ref) "
                   "VALUES(?1,?2,0,?10,?11,?12,?13,?3,?4,?5) ON CONFLICT(world_id,space_id,released,slot) DO UPDATE SET weekday=excluded.weekday,"
                   "start_minute=excluded.start_minute,minutes=excluded.minutes,programme=excluded.programme,host=excluded.host,ref=excluded.ref",
                   id,space,v,3,n,4,NULL)<0?-1:1;
    return finish(db,r);
}
int awa_slot_remove(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,int slot,int revision) {
    if (!common(db,subject,ns,id,space,revision) || slot<1 || slot>AWA_SLOT_LIMIT) return 0;
    int r=begin(db,id,subject,ns,space,revision),changes=0; if (r!=1) return r;
    long long n[]={slot};
    r=exec_rows(db,"DELETE FROM account_world_slot WHERE world_id=?1 AND space_id=?2 AND released=0 AND slot=?10",id,space,NULL,0,n,1,&changes);
    return finish(db,r<0?-1:changes==1);
}
int awa_episode_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *ref,int season,int number,
                    const char *published,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !piece_ok(ref) || season<0 || season>999 || number<0 || number>9999 || !date_ok(published)) return 0;
    int r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
    r=is_item(db,id,space,ref);
    if (r==1) { const char *v[]={ref,published}; long long n[]={season,number};
        r=exec_rows(db,"INSERT INTO account_world_episode(world_id,space_id,released,ref,season,number,published) VALUES(?1,?2,0,?3,?10,?11,?4) "
                       "ON CONFLICT(world_id,space_id,released,ref) DO UPDATE SET season=excluded.season,number=excluded.number,published=excluded.published",
                       id,space,v,2,n,2,NULL)<0?-1:1; }
    return finish(db,r);
}
static int mark_count_ok(sqlite3 *db,sqlite3_int64 id,const char *space,const char *ref) {
    const char *v[]={ref}; long long n[]={AWA_MARK_LIMIT};
    int over=exec_rows(db,"SELECT 1 WHERE (SELECT count(*) FROM account_world_mark WHERE world_id=?1 AND space_id=?2 AND released=0 AND ref=?3)>?10",
                          id,space,v,1,n,1,NULL);
    return over<0?-1:!over;
}
static int mark_fields_ok(const char *kind,long long start,long long end,const char *title,const char *credit,const char *href,int checked) {
    static const char *kinds[]={"track","chapter","cue","clip","person","source"}; int k=-1;
    for (int i=0;kind&&i<6;i++) if (!strcmp(kind,kinds[i])) k=i;
    if (k<0 || start<0 || start>86400000LL || end<0 || end>86400000LL || (end && end<=start) || ((k==2||k==3) && !end) ||
        !line_ok(title,600,1) || !line_ok(credit,160,0) || !href || (*href && !https_ok(href) && !piece_ok(href)) || (checked!=0 && checked!=1)) return 0;
    return 1;
}
int awa_mark_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *ref,const char *kind,
                 long long start,long long end,const char *title,const char *credit,const char *href,int checked,int revision) {
    if (!common(db,subject,ns,id,space,revision) || !piece_ok(ref) || !mark_fields_ok(kind,start,end,title,credit,href,checked)) return 0;
    int r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
    r=is_item(db,id,space,ref);
    if (r==1) { const char *v[]={ref,kind,title,credit,href}; long long n[]={start,end,checked};
        r=exec_rows(db,"INSERT INTO account_world_mark(world_id,space_id,released,ref,kind,start_ms,end_ms,title,credit,href,checked) "
                       "VALUES(?1,?2,0,?3,?4,?10,?11,?5,?6,?7,?12) ON CONFLICT(world_id,space_id,released,ref,kind,start_ms,title) DO UPDATE SET "
                       "end_ms=excluded.end_ms,credit=excluded.credit,href=excluded.href,checked=excluded.checked",id,space,v,5,n,3,NULL)<0?-1:1; }
    if (r==1) r=mark_count_ok(db,id,space,ref);
    return finish(db,r);
}
int awa_mark_check(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,sqlite3_int64 mark,int checked,int revision) {
    if (!common(db,subject,ns,id,space,revision) || mark<1 || (checked!=0 && checked!=1)) return 0;
    int r=begin(db,id,subject,ns,space,revision),changes=0; if (r!=1) return r;
    long long n[]={mark,checked};
    r=exec_rows(db,"UPDATE account_world_mark SET checked=?11 WHERE id=?10 AND world_id=?1 AND space_id=?2 AND released=0",id,space,NULL,0,n,2,&changes);
    return finish(db,r<0?-1:changes==1);
}
int awa_mark_remove(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,sqlite3_int64 mark,int revision) {
    if (!common(db,subject,ns,id,space,revision) || mark<1) return 0;
    int r=begin(db,id,subject,ns,space,revision),changes=0; if (r!=1) return r;
    long long n[]={mark};
    r=exec_rows(db,"DELETE FROM account_world_mark WHERE id=?10 AND world_id=?1 AND space_id=?2 AND released=0",id,space,NULL,0,n,1,&changes);
    return finish(db,r<0?-1:changes==1);
}

/* "hh:mm:ss.mmm" or "mm:ss.mmm" -> ms; -1 when malformed. */
static long long vtt_time(const char *s,const char **end) {
    long long part[3]={0,0,0}; int n=0;
    for (;;) {
        if (!isdigit((unsigned char)*s)) return -1;
        long long v=0; int digits=0; while (isdigit((unsigned char)*s) && digits<4) { v=v*10+(*s++-'0'); digits++; }
        if (n>=3) return -1; part[n++]=v;
        if (*s==':') { s++; continue; }
        break;
    }
    if (n<2 || *s!='.' || !isdigit((unsigned char)s[1]) || !isdigit((unsigned char)s[2]) || !isdigit((unsigned char)s[3])) return -1;
    long long ms=(s[1]-'0')*100+(s[2]-'0')*10+(s[3]-'0'); s+=4; if (end) *end=s;
    long long h=n==3?part[0]:0, m=n==3?part[1]:part[0], sec=n==3?part[2]:part[1];
    if (m>59 || sec>59) return -1;
    return ((h*60+m)*60+sec)*1000+ms;
}
int awa_cues_import(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *ref,const char *vtt,int revision,int *imported) {
    if (imported) *imported=0;
    if (!common(db,subject,ns,id,space,revision) || !piece_ok(ref) || !vtt || strlen(vtt)>AWA_VTT_LIMIT || strncmp(vtt,"WEBVTT",6)) return 0;
    int r=begin(db,id,subject,ns,space,revision); if (r!=1) return r;
    r=is_item(db,id,space,ref);
    if (r==1) { const char *v[]={ref};
        r=exec_rows(db,"DELETE FROM account_world_mark WHERE world_id=?1 AND space_id=?2 AND released=0 AND ref=?3 AND kind='cue' AND checked=0",id,space,v,1,NULL,0,NULL)<0?-1:1; }
    int count=0;
    for (const char *p=vtt; r==1 && *p; ) {
        const char *eol=strchr(p,'\n'); size_t len=eol?(size_t)(eol-p):strlen(p);
        const char *arrow=memchr(p,'-',len);
        if (arrow && len>=10 && strstr(p,"-->") && (size_t)(strstr(p,"-->")-p)<len) {
            const char *after=NULL; long long start=vtt_time(p,&after), end=-1;
            const char *a=strstr(p,"-->")+3; while (*a==' '||*a=='\t') a++;
            end=vtt_time(a,NULL);
            char text[601]="", speaker[161]=""; size_t tn=0;
            p=eol?eol+1:p+len;
            /* cue payload: lines until a blank line */
            while (*p && *p!='\n' && !(*p=='\r' && p[1]=='\n')) {
                const char *le=strchr(p,'\n'); size_t ll=le?(size_t)(le-p):strlen(p);
                for (size_t i=0;i<ll;i++) {
                    char c=p[i];
                    if (c=='<') { /* voice tag gives the speaker; other tags are dropped */
                        const char *close=memchr(p+i,'>',ll-i); if (!close) break;
                        if (p[i+1]=='v' && (p[i+2]==' '||p[i+2]=='.') && !speaker[0]) {
                            const char *nm=p+i+3; while (nm<close && *nm!=' ' && p[i+2]=='.') nm++; while (nm<close && *nm==' ') nm++;
                            size_t sl=(size_t)(close-nm); if (sl>160) sl=160; memcpy(speaker,nm,sl); speaker[sl]=0;
                        }
                        i=(size_t)(close-p); continue;
                    }
                    if (c=='\r') continue;
                    if ((unsigned char)c<32) c=' ';
                    if (tn+1<sizeof(text)) text[tn++]=c;
                }
                if (tn && tn+1<sizeof(text) && text[tn-1]!=' ') text[tn++]=' ';
                p=le?le+1:p+ll;
            }
            while (tn && text[tn-1]==' ') tn--; text[tn]=0;
            for (char *q=speaker; *q; q++) if ((unsigned char)*q<32) *q=' ';
            if (start<0 || end<=start || !text[0]) { r=0; break; }
            const char *v[]={ref,text,speaker}; long long n[]={start,end};
            if (exec_rows(db,"INSERT OR IGNORE INTO account_world_mark(world_id,space_id,released,ref,kind,start_ms,end_ms,title,credit,href,checked) "
                             "VALUES(?1,?2,0,?3,'cue',?10,?11,?4,?5,'',0)",id,space,v,3,n,2,NULL)<0) { r=-1; break; }
            count++; (void)after; (void)arrow;
            continue;
        }
        p=eol?eol+1:p+len;
    }
    if (r==1 && !count) r=0;
    if (r==1) r=mark_count_ok(db,id,space,ref);
    r=finish(db,r);
    if (r==1 && imported) *imported=count;
    return r;
}

int awa_rows(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,int table,const char *ref,sqlite3_stmt **rows) {
    if (rows) *rows=NULL;
    if (!db || !rows || !key_ok(ns,127) || id<=0 || !key_ok(space,64) || table<AWA_SOUND || table>AWA_MARKS || (subject && !key_ok(subject,256)) || (ref && !piece_ok(ref))) return 0;
    static const char *cols[]={
        "t.kind,t.timezone,t.language,t.author,t.category,t.explicit,t.stream_ref FROM account_world_sound t",
        "t.slot,t.weekday,t.start_minute,t.minutes,t.programme,t.host,t.ref FROM account_world_slot t",
        "t.ref,t.season,t.number,t.published FROM account_world_episode t",
        "t.id,t.ref,t.kind,t.start_ms,t.end_ms,t.title,t.credit,t.href,t.checked FROM account_world_mark t"};
    static const char *order[]={""," ORDER BY t.weekday,t.start_minute,t.slot"," ORDER BY t.published DESC,t.season DESC,t.number DESC",
                                " ORDER BY t.ref,t.kind,t.start_ms,t.title"};
    char sql[1400];
    if (subject) {
        int g=guarded(db,id,subject,ns); if (g!=1) return g;
        snprintf(sql,sizeof(sql),"SELECT %s WHERE t.world_id=?1 AND t.space_id=?2 AND t.released=0%s%s LIMIT 2000",cols[table],ref&&table>=AWA_EPISODES?" AND t.ref=?3":"",order[table]);
    } else
        snprintf(sql,sizeof(sql),"SELECT %s JOIN account_world_release r ON r.world_id=t.world_id AND r.space_id=t.space_id AND r.state='public' AND r.namespace=?4 "
                 "JOIN account_world_binding b ON b.world_id=r.world_id AND b.namespace=r.namespace "
                 "JOIN world w ON w.id=b.world_id AND w.namespace=b.namespace AND w.subject=b.subject AND w.status IN('draft','published') "
                 "WHERE t.world_id=?1 AND t.space_id=?2 AND t.released=1%s%s LIMIT 2000",cols[table],ref&&table>=AWA_EPISODES?" AND t.ref=?3":"",order[table]);
    if (sqlite3_prepare_v2(db,sql,-1,rows,NULL)!=SQLITE_OK) { *rows=NULL; return -1; }
    sqlite3_bind_int64(*rows,1,id); sqlite3_bind_text(*rows,2,space,-1,SQLITE_TRANSIENT);
    if (ref && table>=AWA_EPISODES) sqlite3_bind_text(*rows,3,ref,-1,SQLITE_TRANSIENT);
    if (!subject) sqlite3_bind_text(*rows,4,ns,-1,SQLITE_TRANSIENT);
    return 1;
}
