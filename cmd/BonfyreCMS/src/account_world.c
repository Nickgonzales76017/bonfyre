#include "account_world.h"
#include <string.h>
#include <limits.h>

/* 1 = success, 0 = absent/refused/conflict, -1 = storage error.
 * Every mutation runs under an immediate transaction. */
static int key(const char *s, size_t max) {
    if (!s || !*s || strlen(s) > max) return 0;
    return strspn(s,"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._:-/")==strlen(s);
}
static int text_ok(const char *s, size_t max) {
    if (!s || !*s || strlen(s)>max || strspn(s," \t\r\n")==strlen(s)) return 0;
    for (const unsigned char *p=(const unsigned char *)s; *p; p++) if (*p<32 || *p==127) return 0;
    return 1;
}
static int step_done(sqlite3_stmt *s) { return sqlite3_step(s)==SQLITE_DONE; }
int aw_initialize(sqlite3 *db) {
    const char *sql =
      "CREATE TABLE IF NOT EXISTS account_world_binding("
      "world_id INTEGER PRIMARY KEY,subject TEXT NOT NULL,namespace TEXT NOT NULL);"
      "CREATE TABLE IF NOT EXISTS account_world_space("
      "world_id INTEGER NOT NULL,space_id TEXT NOT NULL,title TEXT NOT NULL,renderer TEXT NOT NULL,"
      "template_ref TEXT NOT NULL,layout_ref TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>0),"
      "PRIMARY KEY(world_id,space_id));"
      "CREATE TABLE IF NOT EXISTS account_world_space_style("
      "world_id INTEGER NOT NULL,space_id TEXT NOT NULL,medium TEXT NOT NULL,character TEXT NOT NULL,lede TEXT NOT NULL,"
      "PRIMARY KEY(world_id,space_id));"
      "CREATE TABLE IF NOT EXISTS account_world_item("
      "world_id INTEGER NOT NULL,space_id TEXT NOT NULL,position INTEGER NOT NULL CHECK(position BETWEEN 1 AND 24),"
      "ref TEXT NOT NULL,role TEXT NOT NULL,note TEXT NOT NULL,PRIMARY KEY(world_id,space_id,position));"
      "CREATE TABLE IF NOT EXISTS account_world_release("
      "world_id INTEGER NOT NULL,space_id TEXT NOT NULL,namespace TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('public','withdrawn')),"
      "revision INTEGER NOT NULL CHECK(revision>0),title TEXT NOT NULL,renderer TEXT NOT NULL,medium TEXT NOT NULL,"
      "character TEXT NOT NULL,lede TEXT NOT NULL,PRIMARY KEY(world_id,space_id));"
      "CREATE INDEX IF NOT EXISTS account_world_release_ns ON account_world_release(namespace,space_id,state);"
      "CREATE TABLE IF NOT EXISTS account_world_release_item("
      "world_id INTEGER NOT NULL,space_id TEXT NOT NULL,position INTEGER NOT NULL,ref TEXT NOT NULL,role TEXT NOT NULL,"
      "note TEXT NOT NULL,PRIMARY KEY(world_id,space_id,position));"
      "CREATE TRIGGER IF NOT EXISTS account_world_bound_delete BEFORE DELETE ON world "
      "WHEN EXISTS(SELECT 1 FROM account_world_binding WHERE world_id=OLD.id) "
      "BEGIN SELECT RAISE(ABORT,'bound world cannot be deleted'); END;"
      "CREATE TRIGGER IF NOT EXISTS account_world_bound_identity BEFORE UPDATE OF id,subject,namespace,world ON world "
      "WHEN EXISTS(SELECT 1 FROM account_world_binding WHERE world_id=OLD.id) AND "
      "(NEW.id IS NOT OLD.id OR NEW.subject IS NOT OLD.subject OR NEW.namespace IS NOT OLD.namespace OR NEW.world IS NOT OLD.world) "
      "BEGIN SELECT RAISE(ABORT,'bound world identity cannot change'); END;";
    return db && sqlite3_exec(db,sql,NULL,NULL,NULL)==SQLITE_OK && aw_cine_initialize(db)==1 ? 1 : -1;
}
int aw_bind(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id) {
    sqlite3_stmt *s=NULL; int ret=-1,tx=0;
    if(!db||!key(subject,256)||!key(ns,127)||id<=0)return 0;
    if(sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL)!=SQLITE_OK)return -1;tx=1;
    const char *sql="INSERT INTO account_world_binding(world_id,subject,namespace) "
      "SELECT id,?1,?2 FROM world WHERE id=?3 AND namespace=?2 AND subject=?1 AND status='draft' "
      "ON CONFLICT(world_id) DO NOTHING";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,subject,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,ns,-1,SQLITE_STATIC);sqlite3_bind_int64(s,3,id);
    if(!step_done(s))goto done;sqlite3_finalize(s);s=NULL;
    if(sqlite3_prepare_v2(db,"SELECT 1 FROM account_world_binding b JOIN world w ON w.id=b.world_id WHERE b.world_id=?3 AND b.subject=?1 AND b.namespace=?2 AND w.subject=b.subject AND w.namespace=b.namespace AND w.status='draft'",-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_text(s,1,subject,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,ns,-1,SQLITE_STATIC);sqlite3_bind_int64(s,3,id);
    int step=sqlite3_step(s); if(step!=SQLITE_ROW && step!=SQLITE_DONE)goto done;ret=step==SQLITE_ROW;
    sqlite3_finalize(s);s=NULL;if(sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK){ret=-1;goto done;}tx=0;
 done: sqlite3_finalize(s);if(tx)sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);return ret;
}
int aw_space_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *title,const char *renderer,const char *template_ref,const char *layout_ref,int revision) {
    sqlite3_stmt *s=NULL;int ret=-1,tx=0;
    if(!db||!key(subject,256)||!key(ns,127)||id<=0||!key(space,64)||!text_ok(title,256)||
       !renderer|| (strcmp(renderer,"2d") && strcmp(renderer,"3d")) || !key(template_ref,256)||!key(layout_ref,256)||revision<0||revision==INT_MAX)return 0;
    if(sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL)!=SQLITE_OK)return -1;tx=1;
    const char *guard="SELECT 1 FROM account_world_binding b JOIN world w ON w.id=b.world_id WHERE b.world_id=?1 AND b.subject=?2 AND b.namespace=?3 AND w.subject=b.subject AND w.namespace=b.namespace AND w.status='draft'";
    if(sqlite3_prepare_v2(db,guard,-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_int64(s,1,id);sqlite3_bind_text(s,2,subject,-1,SQLITE_STATIC);sqlite3_bind_text(s,3,ns,-1,SQLITE_STATIC);
    int step=sqlite3_step(s);if(step!=SQLITE_ROW&&step!=SQLITE_DONE)goto done;sqlite3_finalize(s);s=NULL;if(step==SQLITE_DONE){ret=0;goto done;}
    const char *sql= revision==0 ?
      "INSERT INTO account_world_space SELECT ?1,?2,?3,?4,?5,?6,1 WHERE (SELECT count(*) FROM account_world_space WHERE world_id=?1)<32 ON CONFLICT(world_id,space_id) DO NOTHING" :
      "UPDATE account_world_space SET title=?3,renderer=?4,template_ref=?5,layout_ref=?6,revision=revision+1 WHERE world_id=?1 AND space_id=?2 AND revision=?7";
    if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)goto done;
    sqlite3_bind_int64(s,1,id);const char *v[]={space,title,renderer,template_ref,layout_ref};for(int i=0;i<5;i++)sqlite3_bind_text(s,i+2,v[i],-1,SQLITE_STATIC);
    if(revision)sqlite3_bind_int(s,7,revision);if(!step_done(s))goto done;ret=sqlite3_changes(db)==1;
    sqlite3_finalize(s);s=NULL;if(sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK){ret=-1;goto done;}tx=0;
 done:sqlite3_finalize(s);if(tx)sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);return ret;
}
int aw_space_get(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,sqlite3_stmt **row) {
    if(row)*row=NULL;if(!db||!row||!key(subject,256)||!key(ns,127)||!key(space,64)||id<=0)return 0;
    const char *sql="SELECT s.title,s.renderer,s.template_ref,s.layout_ref,s.revision FROM account_world_space s JOIN account_world_binding b ON b.world_id=s.world_id JOIN world w ON w.id=b.world_id WHERE s.world_id=?1 AND s.space_id=?2 AND b.subject=?3 AND b.namespace=?4 AND w.subject=b.subject AND w.namespace=b.namespace AND w.status='draft'";
    if(sqlite3_prepare_v2(db,sql,-1,row,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_int64(*row,1,id);sqlite3_bind_text(*row,2,space,-1,SQLITE_TRANSIENT);sqlite3_bind_text(*row,3,subject,-1,SQLITE_TRANSIENT);sqlite3_bind_text(*row,4,ns,-1,SQLITE_TRANSIENT);
    int step=sqlite3_step(*row);if(step==SQLITE_ROW)return 1;sqlite3_finalize(*row);*row=NULL;return step==SQLITE_DONE?0:-1;
}
/* Bounded, owner-scoped draft index; no publication or writes. */
int aw_space_list(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,sqlite3_stmt **rows) {
    if(rows)*rows=NULL;if(!db||!rows||!key(subject,256)||!key(ns,127)||id<=0)return 0;
    sqlite3_stmt *g=NULL;const char *guard="SELECT 1 FROM account_world_binding b JOIN world w ON w.id=b.world_id WHERE b.world_id=?1 AND b.subject=?2 AND b.namespace=?3 AND w.subject=b.subject AND w.namespace=b.namespace AND w.status='draft'";
    if(sqlite3_prepare_v2(db,guard,-1,&g,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_int64(g,1,id);sqlite3_bind_text(g,2,subject,-1,SQLITE_TRANSIENT);sqlite3_bind_text(g,3,ns,-1,SQLITE_TRANSIENT);
    int step=sqlite3_step(g);sqlite3_finalize(g);if(step!=SQLITE_ROW)return step==SQLITE_DONE?0:-1;
    const char *sql="SELECT s.space_id,s.title,s.renderer,s.template_ref,s.layout_ref,s.revision FROM account_world_space s JOIN account_world_binding b ON b.world_id=s.world_id JOIN world w ON w.id=b.world_id WHERE s.world_id=?1 AND b.subject=?2 AND b.namespace=?3 AND w.subject=b.subject AND w.namespace=b.namespace AND w.status='draft' ORDER BY s.space_id LIMIT 33";
    if(sqlite3_prepare_v2(db,sql,-1,rows,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_int64(*rows,1,id);sqlite3_bind_text(*rows,2,subject,-1,SQLITE_TRANSIENT);sqlite3_bind_text(*rows,3,ns,-1,SQLITE_TRANSIENT);
    return 1;
}

/* ---------------------------------------------------------- account-world-2
 * Style, items and release. Every mutation holds one immediate transaction,
 * rechecks the binding and draft World, and advances the space revision only
 * when the caller names the exact current one. */
static int slug(const char *s, size_t max) {
    if (!s || !*s || strlen(s) > max || !((*s>='a'&&*s<='z')||(*s>='0'&&*s<='9'))) return 0;
    return strspn(s,"abcdefghijklmnopqrstuvwxyz0123456789-")==strlen(s);
}
static int prose_ok(const char *s, size_t max) {
    if (!s || strlen(s) > max) return 0;
    for (const unsigned char *p=(const unsigned char *)s; *p; p++) if ((*p<32 && *p!='\n') || *p==127) return 0;
    return 1;
}
static int ref_ok(const char *ref) {
    if (!ref) return 0;
    if (!strncmp(ref,"space:",6)) return key(ref+6,64);
    const char *slash=strchr(ref,'/');
    if (!slash || slash==ref || (size_t)(slash-ref)>95 || strlen(slash+1)!=24 || strspn(slash+1,"0123456789abcdef")!=24) return 0;
    return strspn(ref,"abcdefghijklmnopqrstuvwxyz0123456789-_.")==(size_t)(slash-ref);
}
static const char *GUARD="SELECT 1 FROM account_world_binding b JOIN world w ON w.id=b.world_id WHERE b.world_id=?1 AND b.subject=?2 AND b.namespace=?3 AND w.subject=b.subject AND w.namespace=b.namespace AND w.status='draft'";
static int guarded(sqlite3 *db,sqlite3_int64 id,const char *subject,const char *ns) {
    sqlite3_stmt *s=NULL; if(sqlite3_prepare_v2(db,GUARD,-1,&s,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_int64(s,1,id);sqlite3_bind_text(s,2,subject,-1,SQLITE_STATIC);sqlite3_bind_text(s,3,ns,-1,SQLITE_STATIC);
    int step=sqlite3_step(s);sqlite3_finalize(s);return step==SQLITE_ROW?1:step==SQLITE_DONE?0:-1;
}
static int run(sqlite3 *db,const char *sql,sqlite3_int64 id,const char *space,const char **text,int ntext,int a,int b,int *changes) {
    sqlite3_stmt *s=NULL; if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_int64(s,1,id);sqlite3_bind_text(s,2,space,-1,SQLITE_STATIC);
    for(int i=0;i<ntext;i++)sqlite3_bind_text(s,3+i,text[i],-1,SQLITE_STATIC);
    if(sqlite3_bind_parameter_index(s,"?8"))sqlite3_bind_int(s,8,a);
    if(sqlite3_bind_parameter_index(s,"?9"))sqlite3_bind_int(s,9,b);
    int step=sqlite3_step(s);sqlite3_finalize(s);if(step!=SQLITE_DONE&&step!=SQLITE_ROW)return -1;
    if(changes)*changes=sqlite3_changes(db);return step==SQLITE_ROW?1:0;
}
/* Opens the transaction and checks the binding; 1 = open, 0 = refused, -1 = storage error. */
static int begin(sqlite3 *db,sqlite3_int64 id,const char *subject,const char *ns) {
    if(sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL)!=SQLITE_OK)return -1;
    int g=guarded(db,id,subject,ns);if(g!=1)sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);return g;
}
static int finish(sqlite3 *db,int ret) {
    if(ret==1&&sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)==SQLITE_OK)return 1;
    sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);return ret==1?-1:ret;
}
static int bump(sqlite3 *db,sqlite3_int64 id,const char *space,int revision) {
    int changes=0;if(run(db,"UPDATE account_world_space SET revision=revision+1 WHERE world_id=?1 AND space_id=?2 AND revision=?8",id,space,NULL,0,revision,0,&changes)<0)return -1;
    return changes==1;
}
static int common(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,int revision) {
    return db&&key(subject,256)&&key(ns,127)&&id>0&&key(space,64)&&revision>0&&revision<INT_MAX;
}
int aw_space_style(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *medium,const char *character,const char *lede,int revision) {
    if(!common(db,subject,ns,id,space,revision)||!slug(medium,32)||!slug(character,32)||!prose_ok(lede,600))return 0;
    int r=begin(db,id,subject,ns);if(r!=1)return r;
    r=bump(db,id,space,revision);
    if(r==1){const char *v[]={medium,character,lede};r=run(db,"INSERT INTO account_world_space_style VALUES(?1,?2,?3,?4,?5) ON CONFLICT(world_id,space_id) DO UPDATE SET medium=excluded.medium,character=excluded.character,lede=excluded.lede",id,space,v,3,0,0,NULL)<0?-1:1;}
    return finish(db,r);
}
int aw_item_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,int position,const char *ref,const char *role,const char *note,int revision) {
    if(!common(db,subject,ns,id,space,revision)||position<1||position>AW_ITEM_LIMIT||!ref_ok(ref)||!slug(role,32)||!prose_ok(note?note:"",AW_NOTE_LIMIT))return 0;
    if(!strncmp(ref,"space:",6)&&!strcmp(ref+6,space))return 0;
    int r=begin(db,id,subject,ns);if(r!=1)return r;
    if(!strncmp(ref,"space:",6)){const char *v[]={ref+6};r=run(db,"SELECT 1 FROM account_world_space WHERE world_id=?1 AND space_id=?3 AND ?2 IS NOT NULL",id,space,v,1,0,0,NULL);}
    if(r==1)r=bump(db,id,space,revision);
    if(r==1){const char *v[]={ref,role,note?note:""};r=run(db,"INSERT INTO account_world_item VALUES(?1,?2,?8,?3,?4,?5) ON CONFLICT(world_id,space_id,position) DO UPDATE SET ref=excluded.ref,role=excluded.role,note=excluded.note",id,space,v,3,position,0,NULL)<0?-1:1;}
    return finish(db,r);
}
int aw_item_remove(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,int position,int revision) {
    if(!common(db,subject,ns,id,space,revision)||position<1||position>AW_ITEM_LIMIT)return 0;
    int r=begin(db,id,subject,ns),changes=0;if(r!=1)return r;
    r=bump(db,id,space,revision);
    if(r==1){r=run(db,"DELETE FROM account_world_item WHERE world_id=?1 AND space_id=?2 AND position=?8",id,space,NULL,0,position,0,&changes);r=r<0?-1:changes==1;}
    return finish(db,r);
}
static int owned_rows(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *sql,sqlite3_stmt **rows,int need_row) {
    if(rows)*rows=NULL;if(!db||!rows||!key(subject,256)||!key(ns,127)||!key(space,64)||id<=0)return 0;
    int g=guarded(db,id,subject,ns);if(g!=1)return g;
    if(sqlite3_prepare_v2(db,sql,-1,rows,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_int64(*rows,1,id);sqlite3_bind_text(*rows,2,space,-1,SQLITE_TRANSIENT);
    if(!need_row)return 1;
    int step=sqlite3_step(*rows);if(step==SQLITE_ROW)return 1;sqlite3_finalize(*rows);*rows=NULL;return step==SQLITE_DONE?0:-1;
}
int aw_space_detail(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,sqlite3_stmt **row) {
    return owned_rows(db,subject,ns,id,space,"SELECT COALESCE(st.medium,''),COALESCE(st.character,''),COALESCE(st.lede,''),COALESCE(r.state,''),COALESCE(r.revision,0) "
      "FROM account_world_space s LEFT JOIN account_world_space_style st ON st.world_id=s.world_id AND st.space_id=s.space_id "
      "LEFT JOIN account_world_release r ON r.world_id=s.world_id AND r.space_id=s.space_id WHERE s.world_id=?1 AND s.space_id=?2",row,1);
}
int aw_items(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,sqlite3_stmt **rows) {
    return owned_rows(db,subject,ns,id,space,"SELECT position,ref,role,note FROM account_world_item WHERE world_id=?1 AND space_id=?2 ORDER BY position LIMIT 24",rows,0);
}
int aw_space_release(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,int revision) {
    if(!common(db,subject,ns,id,space,revision))return 0;
    int r=begin(db,id,subject,ns);if(r!=1)return r;
    /* exact revision, a chosen look, at least one item, and no other public
     * space under the same address on this site */
    const char *v[]={ns};
    r=run(db,"SELECT 1 FROM account_world_space s JOIN account_world_space_style st ON st.world_id=s.world_id AND st.space_id=s.space_id "
             "WHERE s.world_id=?1 AND s.space_id=?2 AND s.revision=?8 AND EXISTS(SELECT 1 FROM account_world_item i WHERE i.world_id=s.world_id AND i.space_id=s.space_id) "
             "AND NOT EXISTS(SELECT 1 FROM account_world_release o WHERE o.namespace=?3 AND o.space_id=?2 AND o.world_id<>?1 AND o.state='public')",id,space,v,1,revision,0,NULL);
    if(r==1)r=run(db,"INSERT INTO account_world_release SELECT s.world_id,s.space_id,?3,'public',s.revision,s.title,s.renderer,st.medium,st.character,st.lede "
                     "FROM account_world_space s JOIN account_world_space_style st ON st.world_id=s.world_id AND st.space_id=s.space_id WHERE s.world_id=?1 AND s.space_id=?2 "
                     "ON CONFLICT(world_id,space_id) DO UPDATE SET namespace=excluded.namespace,state='public',revision=excluded.revision,title=excluded.title,"
                     "renderer=excluded.renderer,medium=excluded.medium,character=excluded.character,lede=excluded.lede",id,space,v,1,0,0,NULL)<0?-1:1;
    if(r==1)r=run(db,"DELETE FROM account_world_release_item WHERE world_id=?1 AND space_id=?2",id,space,NULL,0,0,0,NULL)<0?-1:1;
    if(r==1)r=run(db,"INSERT INTO account_world_release_item SELECT world_id,space_id,position,ref,role,note FROM account_world_item WHERE world_id=?1 AND space_id=?2",id,space,NULL,0,0,0,NULL)<0?-1:1;
    return finish(db,r);
}
int aw_space_withdraw(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space) {
    if(!common(db,subject,ns,id,space,1))return 0;
    int r=begin(db,id,subject,ns),changes=0;if(r!=1)return r;
    r=run(db,"UPDATE account_world_release SET state='withdrawn' WHERE world_id=?1 AND space_id=?2 AND state='public'",id,space,NULL,0,0,0,&changes);
    r=r<0?-1:changes==1;
    if(r==1)r=run(db,"DELETE FROM account_world_release_item WHERE world_id=?1 AND space_id=?2",id,space,NULL,0,0,0,NULL)<0?-1:1;
    return finish(db,r);
}
#define PUBLIC_GUARD "JOIN account_world_binding b ON b.world_id=r.world_id AND b.namespace=r.namespace " \
    "JOIN world w ON w.id=b.world_id AND w.namespace=b.namespace AND w.subject=b.subject AND w.status IN('draft','published') "
int aw_public_space(sqlite3 *db,const char *ns,const char *space,sqlite3_stmt **row) {
    if(row)*row=NULL;if(!db||!row||!key(ns,127)||!key(space,64))return 0;
    if(sqlite3_prepare_v2(db,"SELECT r.title,r.renderer,r.medium,r.character,r.lede,r.state,r.world_id,r.revision FROM account_world_release r " PUBLIC_GUARD
                          "WHERE r.namespace=?1 AND r.space_id=?2 ORDER BY r.state='public' DESC,r.world_id LIMIT 1",-1,row,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_text(*row,1,ns,-1,SQLITE_TRANSIENT);sqlite3_bind_text(*row,2,space,-1,SQLITE_TRANSIENT);
    int step=sqlite3_step(*row);if(step==SQLITE_ROW)return 1;sqlite3_finalize(*row);*row=NULL;return step==SQLITE_DONE?0:-1;
}
int aw_public_items(sqlite3 *db,const char *ns,sqlite3_int64 id,const char *space,sqlite3_stmt **rows) {
    if(rows)*rows=NULL;if(!db||!rows||!key(ns,127)||!key(space,64)||id<=0)return 0;
    if(sqlite3_prepare_v2(db,"SELECT i.position,i.ref,i.role,i.note FROM account_world_release_item i JOIN account_world_release r ON r.world_id=i.world_id AND r.space_id=i.space_id AND r.state='public' "
                          PUBLIC_GUARD "WHERE r.namespace=?1 AND r.world_id=?2 AND r.space_id=?3 ORDER BY i.position LIMIT 24",-1,rows,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_text(*rows,1,ns,-1,SQLITE_TRANSIENT);sqlite3_bind_int64(*rows,2,id);sqlite3_bind_text(*rows,3,space,-1,SQLITE_TRANSIENT);return 1;
}
int aw_public_spaces(sqlite3 *db,const char *ns,sqlite3_stmt **rows) {
    if(rows)*rows=NULL;if(!db||!rows||!key(ns,127))return 0;
    if(sqlite3_prepare_v2(db,"SELECT r.space_id,r.title,r.medium,r.character,r.lede FROM account_world_release r " PUBLIC_GUARD
                          "WHERE r.namespace=?1 AND r.state='public' ORDER BY r.space_id LIMIT 64",-1,rows,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_text(*rows,1,ns,-1,SQLITE_TRANSIENT);return 1;
}

/* ------------------------------------------------------- account-world-cine
 * Screenings, a club and a watch diary. A film is always a PageComposition
 * reference; nothing here stores a media URL, a rights statement or an
 * availability claim. The reader re-reads each source on every request. */
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
static int cine_prepare(sqlite3 *db,const char *sql,sqlite3_stmt **s,const char *fmt,va_list ap) {
    if(sqlite3_prepare_v2(db,sql,-1,s,NULL)!=SQLITE_OK)return -1;
    for(int i=0;fmt[i];i++){
        if(fmt[i]=='i')sqlite3_bind_int64(*s,i+1,va_arg(ap,sqlite3_int64));
        else sqlite3_bind_text(*s,i+1,va_arg(ap,const char *),-1,SQLITE_TRANSIENT);
    }
    return 1;
}
/* 1 = a row, 0 = done, -1 = error; changes after DML when asked. */
static int cine_do(sqlite3 *db,int *changes,const char *sql,const char *fmt,...) {
    sqlite3_stmt *s=NULL;va_list ap;va_start(ap,fmt);int r=cine_prepare(db,sql,&s,fmt,ap);va_end(ap);
    if(r<0){sqlite3_finalize(s);return -1;}
    int step=sqlite3_step(s);sqlite3_finalize(s);if(step!=SQLITE_ROW&&step!=SQLITE_DONE)return -1;
    if(changes)*changes=sqlite3_changes(db);return step==SQLITE_ROW;
}
static int cine_rows(sqlite3 *db,sqlite3_stmt **rows,int need_row,const char *sql,const char *fmt,...) {
    va_list ap;va_start(ap,fmt);int r=cine_prepare(db,sql,rows,fmt,ap);va_end(ap);
    if(r<0){sqlite3_finalize(*rows);*rows=NULL;return -1;}
    if(!need_row)return 1;
    int step=sqlite3_step(*rows);if(step==SQLITE_ROW)return 1;sqlite3_finalize(*rows);*rows=NULL;return step==SQLITE_DONE?0:-1;
}
static int shape(const char *s,const char *pattern) {
    size_t n=strlen(pattern);if(!s||strlen(s)!=n)return 0;
    for(size_t i=0;i<n;i++)if(pattern[i]=='d'?(s[i]<'0'||s[i]>'9'):s[i]!=pattern[i])return 0;
    return 1;
}
static int iso_day(const char *s) {
    static const int days[]={31,29,31,30,31,30,31,31,30,31,30,31};
    if(!shape(s,"dddd-dd-dd"))return 0;
    int y=atoi(s),m=atoi(s+5),d=atoi(s+8);
    if(y<1870||y>2200||m<1||m>12||d<1||d>days[m-1])return 0;
    return !(m==2&&d==29&&!((y%4==0&&y%100!=0)||y%400==0));
}
static int iso_minute(const char *s) {
    char day[11];if(!s||strlen(s)!=16||s[10]!='T')return 0;memcpy(day,s,10);day[10]=0;
    return iso_day(day)&&shape(s+11,"dd:dd")&&atoi(s+11)<24&&atoi(s+14)<60;
}
static int film_ref(const char *ref){return ref&&strncmp(ref,"space:",6)&&ref_ok(ref);}
static int words_ok(const char *s,size_t max){return prose_ok(s,max)&&s&&*s&&strspn(s," \t\r\n")!=strlen(s);}
int aw_cine_initialize(sqlite3 *db) {
    const char *sql=
      "CREATE TABLE IF NOT EXISTS account_world_screening(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,screening_id TEXT NOT NULL,"
      "starts TEXT NOT NULL,venue TEXT NOT NULL,ref TEXT NOT NULL,note TEXT NOT NULL,"
      "state TEXT NOT NULL CHECK(state IN('draft','announced','cancelled')),PRIMARY KEY(world_id,space_id,screening_id));"
      "CREATE TABLE IF NOT EXISTS account_world_club(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,"
      "state TEXT NOT NULL CHECK(state IN('open','closed')),PRIMARY KEY(world_id,space_id));"
      "CREATE TABLE IF NOT EXISTS account_world_club_member(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,member TEXT NOT NULL,"
      "state TEXT NOT NULL CHECK(state IN('member','left','removed')),PRIMARY KEY(world_id,space_id,member));"
      "CREATE TABLE IF NOT EXISTS account_world_club_pick(pick_id INTEGER PRIMARY KEY,world_id INTEGER NOT NULL,space_id TEXT NOT NULL,"
      "ref TEXT NOT NULL,note TEXT NOT NULL,proposed_by TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('open','hidden')));"
      "CREATE INDEX IF NOT EXISTS account_world_club_pick_space ON account_world_club_pick(world_id,space_id,state);"
      "CREATE TABLE IF NOT EXISTS account_world_club_vote(pick_id INTEGER NOT NULL,member TEXT NOT NULL,PRIMARY KEY(pick_id,member));"
      "CREATE TABLE IF NOT EXISTS account_world_club_post(post_id INTEGER PRIMARY KEY,world_id INTEGER NOT NULL,space_id TEXT NOT NULL,"
      "author TEXT NOT NULL,ref TEXT NOT NULL,body TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('shown','hidden','retracted')));"
      "CREATE INDEX IF NOT EXISTS account_world_club_post_space ON account_world_club_post(world_id,space_id,post_id);"
      "CREATE TABLE IF NOT EXISTS account_world_note(note_id INTEGER PRIMARY KEY,world_id INTEGER NOT NULL,ref TEXT NOT NULL,"
      "watched TEXT NOT NULL,rating INTEGER NOT NULL CHECK(rating BETWEEN 0 AND 10),rewatch INTEGER NOT NULL CHECK(rewatch IN(0,1)),"
      "body TEXT NOT NULL,visibility TEXT NOT NULL CHECK(visibility IN('private','public')));"
      "CREATE INDEX IF NOT EXISTS account_world_note_day ON account_world_note(world_id,watched,note_id);";
    return db&&sqlite3_exec(db,sql,NULL,NULL,NULL)==SQLITE_OK?1:-1;
}

/* programme ---------------------------------------------------------------- */
int aw_screening_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *sid,
                     const char *starts,const char *venue,const char *ref,const char *note) {
    if(!common(db,subject,ns,id,space,1)||!slug(sid,64)||!iso_minute(starts)||!text_ok(venue,120)||!film_ref(ref)||!prose_ok(note?note:"",AW_NOTE_LIMIT))return 0;
    int r=begin(db,id,subject,ns),changes=0;if(r!=1)return r;
    r=cine_do(db,NULL,"SELECT 1 FROM account_world_space WHERE world_id=?1 AND space_id=?2 AND "
                      "(SELECT count(*) FROM account_world_screening WHERE world_id=?1 AND space_id=?2 AND screening_id<>?3)<48","iss",id,space,sid);
    /* an edit takes the screening back to draft; a cancelled one stays cancelled */
    if(r==1){r=cine_do(db,&changes,"INSERT INTO account_world_screening VALUES(?1,?2,?3,?4,?5,?6,?7,'draft') ON CONFLICT(world_id,space_id,screening_id) "
                                  "DO UPDATE SET starts=excluded.starts,venue=excluded.venue,ref=excluded.ref,note=excluded.note,state='draft' WHERE state<>'cancelled'",
                                  "issssss",id,space,sid,starts,venue,ref,note?note:"");r=r<0?-1:changes==1;}
    return finish(db,r);
}
int aw_screening_announce(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *sid) {
    if(!common(db,subject,ns,id,space,1)||!slug(sid,64))return 0;
    int r=begin(db,id,subject,ns),changes=0;if(r!=1)return r;
    r=cine_do(db,&changes,"UPDATE account_world_screening SET state='announced' WHERE world_id=?1 AND space_id=?2 AND screening_id=?3 AND state='draft'","iss",id,space,sid);
    return finish(db,r<0?-1:changes==1);
}
int aw_screening_cancel(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *sid) {
    if(!common(db,subject,ns,id,space,1)||!slug(sid,64))return 0;
    int r=begin(db,id,subject,ns),changes=0;if(r!=1)return r;
    r=cine_do(db,&changes,"DELETE FROM account_world_screening WHERE world_id=?1 AND space_id=?2 AND screening_id=?3 AND state='draft'","iss",id,space,sid);
    if(r==0&&changes==0)r=cine_do(db,&changes,"UPDATE account_world_screening SET state='cancelled' WHERE world_id=?1 AND space_id=?2 AND screening_id=?3 AND state='announced'","iss",id,space,sid);
    return finish(db,r<0?-1:changes==1);
}
int aw_screenings(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,sqlite3_stmt **rows) {
    return owned_rows(db,subject,ns,id,space,"SELECT screening_id,starts,venue,ref,note,state FROM account_world_screening "
                      "WHERE world_id=?1 AND space_id=?2 ORDER BY starts,screening_id LIMIT 48",rows,0);
}
int aw_public_screenings(sqlite3 *db,const char *ns,sqlite3_int64 id,const char *space,sqlite3_stmt **rows) {
    if(rows)*rows=NULL;if(!db||!rows||!key(ns,127)||!key(space,64)||id<=0)return 0;
    return cine_rows(db,rows,0,"SELECT s.screening_id,s.starts,s.venue,s.ref,s.note,s.state FROM account_world_screening s "
                     "JOIN account_world_release r ON r.world_id=s.world_id AND r.space_id=s.space_id AND r.state='public' " PUBLIC_GUARD
                     "WHERE r.namespace=?1 AND r.world_id=?2 AND r.space_id=?3 AND s.state IN('announced','cancelled') ORDER BY s.starts,s.screening_id LIMIT 48",
                     "sis",ns,id,space);
}

/* club --------------------------------------------------------------------- */
/* The World and host behind a public space whose club is open. */
static int club_find(sqlite3 *db,const char *ns,const char *space,sqlite3_int64 *id,char host[257]) {
    sqlite3_stmt *s=NULL;
    int r=cine_rows(db,&s,1,"SELECT r.world_id,b.subject FROM account_world_release r " PUBLIC_GUARD
                    "JOIN account_world_club c ON c.world_id=r.world_id AND c.space_id=r.space_id AND c.state='open' "
                    "WHERE r.namespace=?1 AND r.space_id=?2 AND r.state='public' ORDER BY r.world_id LIMIT 1","ss",ns,space);
    if(r==1){*id=sqlite3_column_int64(s,0);snprintf(host,257,"%s",(const char *)sqlite3_column_text(s,1));}
    sqlite3_finalize(s);return r;
}
/* 2 = host, 1 = member, 0 = neither, -1 = error */
static int club_role(sqlite3 *db,sqlite3_int64 id,const char *space,const char *host,const char *who) {
    if(!strcmp(host,who))return 2;
    return cine_do(db,NULL,"SELECT 1 FROM account_world_club_member WHERE world_id=?1 AND space_id=?2 AND member=?3 AND state='member'","iss",id,space,who);
}
static int club_begin(sqlite3 *db,const char *who,const char *ns,const char *space,sqlite3_int64 *id,char host[257],int *role) {
    if(!db||!key(who,256)||!key(ns,127)||!key(space,64))return 0;
    if(sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL)!=SQLITE_OK)return -1;
    int r=club_find(db,ns,space,id,host);
    if(r==1){*role=club_role(db,*id,space,host,who);if(*role<0)r=-1;}
    if(r!=1)sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);return r;
}
int aw_club_set(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,int open) {
    if(!common(db,subject,ns,id,space,1))return 0;
    int r=begin(db,id,subject,ns),changes=0;if(r!=1)return r;
    r=cine_do(db,&changes,"INSERT INTO account_world_club SELECT ?1,?2,?3 WHERE EXISTS(SELECT 1 FROM account_world_space WHERE world_id=?1 AND space_id=?2) "
                          "ON CONFLICT(world_id,space_id) DO UPDATE SET state=excluded.state WHERE state<>excluded.state","iss",id,space,open?"open":"closed");
    return finish(db,r<0?-1:changes==1);
}
int aw_club_state(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,char out[8]) {
    out[0]=0;if(!db||!key(subject,256)||!key(ns,127)||!key(space,64)||id<=0)return 0;
    int g=guarded(db,id,subject,ns);if(g!=1)return g;
    sqlite3_stmt *s=NULL;int r=cine_rows(db,&s,1,"SELECT state FROM account_world_club WHERE world_id=?1 AND space_id=?2","is",id,space);
    if(r==1)snprintf(out,8,"%s",(const char *)sqlite3_column_text(s,0));sqlite3_finalize(s);return r<0?-1:1;
}
int aw_club_join(sqlite3 *db,const char *member,const char *ns,const char *space) {
    sqlite3_int64 id=0;char host[257];int role=0,changes=0;
    int r=club_begin(db,member,ns,space,&id,host,&role);if(r!=1)return r;
    if(role)return finish(db,0);
    r=cine_do(db,&changes,"INSERT INTO account_world_club_member SELECT ?1,?2,?3,'member' WHERE "
                          "(SELECT count(*) FROM account_world_club_member WHERE world_id=?1 AND space_id=?2 AND state='member')<5000 "
                          "ON CONFLICT(world_id,space_id,member) DO UPDATE SET state='member' WHERE state='left'","iss",id,space,member);
    return finish(db,r<0?-1:changes==1);
}
int aw_club_leave(sqlite3 *db,const char *member,const char *ns,const char *space) {
    sqlite3_int64 id=0;char host[257];int role=0,changes=0;
    int r=club_begin(db,member,ns,space,&id,host,&role);if(r!=1)return r;
    if(role!=1)return finish(db,0);
    r=cine_do(db,&changes,"UPDATE account_world_club_member SET state='left' WHERE world_id=?1 AND space_id=?2 AND member=?3 AND state='member'","iss",id,space,member);
    return finish(db,r<0?-1:changes==1);
}
int aw_club_remove(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *member) {
    if(!common(db,subject,ns,id,space,1)||!key(member,256)||!strcmp(member,subject))return 0;
    int r=begin(db,id,subject,ns),changes=0;if(r!=1)return r;
    r=cine_do(db,&changes,"UPDATE account_world_club_member SET state='removed' WHERE world_id=?1 AND space_id=?2 AND member=?3 AND state<>'removed'","iss",id,space,member);
    return finish(db,r<0?-1:changes==1);
}
int aw_club_pick(sqlite3 *db,const char *member,const char *ns,const char *space,const char *ref,const char *note) {
    if(!film_ref(ref)||!prose_ok(note?note:"",AW_NOTE_LIMIT))return 0;
    sqlite3_int64 id=0;char host[257];int role=0,changes=0;
    int r=club_begin(db,member,ns,space,&id,host,&role);if(r!=1)return r;
    if(!role)return finish(db,0);
    r=cine_do(db,&changes,"INSERT INTO account_world_club_pick(world_id,space_id,ref,note,proposed_by,state) SELECT ?1,?2,?3,?4,?5,'open' WHERE "
                          "(SELECT count(*) FROM account_world_club_pick WHERE world_id=?1 AND space_id=?2 AND state='open')<24 AND "
                          "NOT EXISTS(SELECT 1 FROM account_world_club_pick WHERE world_id=?1 AND space_id=?2 AND ref=?3 AND state='open')",
                          "issss",id,space,ref,note?note:"",member);
    return finish(db,r<0?-1:changes==1);
}
int aw_club_vote(sqlite3 *db,const char *member,const char *ns,const char *space,sqlite3_int64 pick) {
    sqlite3_int64 id=0;char host[257];int role=0,changes=0;if(pick<=0)return 0;
    int r=club_begin(db,member,ns,space,&id,host,&role);if(r!=1)return r;
    if(!role)return finish(db,0);
    r=cine_do(db,&changes,"INSERT INTO account_world_club_vote SELECT ?1,?2 WHERE EXISTS(SELECT 1 FROM account_world_club_pick "
                          "WHERE pick_id=?1 AND world_id=?3 AND space_id=?4 AND state='open') ON CONFLICT(pick_id,member) DO NOTHING","isis",pick,member,id,space);
    return finish(db,r<0?-1:changes==1);
}
int aw_club_say(sqlite3 *db,const char *member,const char *ns,const char *space,const char *ref,const char *body) {
    if((ref&&*ref&&!film_ref(ref))||!words_ok(body,1000))return 0;
    sqlite3_int64 id=0;char host[257];int role=0,changes=0;
    int r=club_begin(db,member,ns,space,&id,host,&role);if(r!=1)return r;
    if(!role)return finish(db,0);
    r=cine_do(db,&changes,"INSERT INTO account_world_club_post(world_id,space_id,author,ref,body,state) VALUES(?1,?2,?3,?4,?5,'shown')",
              "issss",id,space,member,ref?ref:"",body);
    return finish(db,r<0?-1:changes==1);
}
int aw_club_retract(sqlite3 *db,const char *member,const char *ns,const char *space,sqlite3_int64 post) {
    sqlite3_int64 id=0;char host[257];int role=0,changes=0;if(post<=0)return 0;
    int r=club_begin(db,member,ns,space,&id,host,&role);if(r!=1)return r;
    r=cine_do(db,&changes,"UPDATE account_world_club_post SET state='retracted',body='',ref='' WHERE post_id=?1 AND world_id=?2 AND space_id=?3 AND author=?4 AND state<>'retracted'",
              "iiss",post,id,space,member);
    return finish(db,r<0?-1:changes==1);
}
int aw_club_hide(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *space,const char *kind,sqlite3_int64 item) {
    if(!common(db,subject,ns,id,space,1)||item<=0||!kind||(strcmp(kind,"post")&&strcmp(kind,"pick")))return 0;
    int r=begin(db,id,subject,ns),changes=0;if(r!=1)return r;
    r=cine_do(db,&changes,!strcmp(kind,"post")?
              "UPDATE account_world_club_post SET state='hidden' WHERE post_id=?1 AND world_id=?2 AND space_id=?3 AND state='shown'":
              "UPDATE account_world_club_pick SET state='hidden' WHERE pick_id=?1 AND world_id=?2 AND space_id=?3 AND state='open'","iis",item,id,space);
    return finish(db,r<0?-1:changes==1);
}
int aw_club_view(sqlite3 *db,const char *viewer,const char *ns,const char *space,sqlite3_stmt **row) {
    if(row)*row=NULL;if(!db||!row||!key(viewer,256)||!key(ns,127)||!key(space,64))return 0;
    return cine_rows(db,row,1,"SELECT r.world_id,r.title,r.character,r.lede,CASE WHEN b.subject=?3 THEN 'host' ELSE COALESCE((SELECT m.state FROM account_world_club_member m "
                     "WHERE m.world_id=r.world_id AND m.space_id=r.space_id AND m.member=?3),'') END FROM account_world_release r " PUBLIC_GUARD
                     "JOIN account_world_club c ON c.world_id=r.world_id AND c.space_id=r.space_id AND c.state='open' "
                     "WHERE r.namespace=?1 AND r.space_id=?2 AND r.state='public' ORDER BY r.world_id LIMIT 1","sss",ns,space,viewer);
}
static int club_reader(sqlite3 *db,const char *viewer,const char *ns,const char *space,sqlite3_int64 *id,char host[257],int *role) {
    if(!db||!key(viewer,256)||!key(ns,127)||!key(space,64))return 0;
    int r=club_find(db,ns,space,id,host);if(r!=1)return r;
    *role=club_role(db,*id,space,host,viewer);return *role<0?-1:*role>0;
}
int aw_club_picks(sqlite3 *db,const char *viewer,const char *ns,const char *space,sqlite3_stmt **rows) {
    sqlite3_int64 id=0;char host[257];int role=0;if(rows)*rows=NULL;if(!rows)return 0;
    int r=club_reader(db,viewer,ns,space,&id,host,&role);if(r!=1)return r;
    /* only votes from the host and current members count */
    return cine_rows(db,rows,0,"SELECT p.pick_id,p.ref,p.note,p.proposed_by,(SELECT count(*) FROM account_world_club_vote v WHERE v.pick_id=p.pick_id AND "
                     "(v.member=?4 OR EXISTS(SELECT 1 FROM account_world_club_member m WHERE m.world_id=p.world_id AND m.space_id=p.space_id AND m.member=v.member AND m.state='member'))) AS votes,"
                     "p.state,EXISTS(SELECT 1 FROM account_world_club_vote v WHERE v.pick_id=p.pick_id AND v.member=?3) FROM account_world_club_pick p "
                     "WHERE p.world_id=?1 AND p.space_id=?2 AND (p.state='open' OR ?5=2) ORDER BY p.state='hidden',votes DESC,p.pick_id LIMIT 48",
                     "isssi",id,space,viewer,host,(sqlite3_int64)role);
}
int aw_club_posts(sqlite3 *db,const char *viewer,const char *ns,const char *space,sqlite3_int64 before,sqlite3_stmt **rows) {
    sqlite3_int64 id=0;char host[257];int role=0;if(rows)*rows=NULL;if(!rows||before<0)return 0;
    int r=club_reader(db,viewer,ns,space,&id,host,&role);if(r!=1)return r;
    return cine_rows(db,rows,0,"SELECT post_id,author,ref,body,state FROM account_world_club_post WHERE world_id=?1 AND space_id=?2 AND (state<>'hidden' OR ?3=2) "
                     "AND (?4=0 OR post_id<?4) ORDER BY post_id DESC LIMIT 21","isii",id,space,(sqlite3_int64)role,before);
}

/* diary -------------------------------------------------------------------- */
int aw_note_put(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,sqlite3_int64 note,const char *ref,const char *watched,
                int rating,int rewatch,const char *body,const char *visibility) {
    if(!db||!key(subject,256)||!key(ns,127)||id<=0||note<0||!film_ref(ref)||!iso_day(watched)||rating<0||rating>10||(rewatch!=0&&rewatch!=1)||
       !prose_ok(body?body:"",2000)||!visibility||(strcmp(visibility,"private")&&strcmp(visibility,"public")))return 0;
    int r=begin(db,id,subject,ns),changes=0;if(r!=1)return r;
    if(!note)r=cine_do(db,&changes,"INSERT INTO account_world_note(world_id,ref,watched,rating,rewatch,body,visibility) SELECT ?1,?2,?3,?4,?5,?6,?7 "
                                   "WHERE (SELECT count(*) FROM account_world_note WHERE world_id=?1)<5000","issiiss",id,ref,watched,(sqlite3_int64)rating,(sqlite3_int64)rewatch,body?body:"",visibility);
    else r=cine_do(db,&changes,"UPDATE account_world_note SET ref=?2,watched=?3,rating=?4,rewatch=?5,body=?6,visibility=?7 WHERE world_id=?1 AND note_id=?8",
                   "issiissi",id,ref,watched,(sqlite3_int64)rating,(sqlite3_int64)rewatch,body?body:"",visibility,note);
    return finish(db,r<0?-1:changes==1);
}
int aw_note_remove(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,sqlite3_int64 note) {
    if(!db||!key(subject,256)||!key(ns,127)||id<=0||note<=0)return 0;
    int r=begin(db,id,subject,ns),changes=0;if(r!=1)return r;
    r=cine_do(db,&changes,"DELETE FROM account_world_note WHERE world_id=?1 AND note_id=?2","ii",id,note);
    return finish(db,r<0?-1:changes==1);
}
int aw_notes(sqlite3 *db,const char *subject,const char *ns,sqlite3_int64 id,const char *before_day,sqlite3_int64 before_id,sqlite3_stmt **rows) {
    if(rows)*rows=NULL;if(!db||!rows||!key(subject,256)||!key(ns,127)||id<=0||before_id<0||(before_id&&!iso_day(before_day)))return 0;
    int g=guarded(db,id,subject,ns);if(g!=1)return g;
    return cine_rows(db,rows,0,"SELECT note_id,ref,watched,rating,rewatch,body,visibility FROM account_world_note WHERE world_id=?1 AND "
                     "(?3=0 OR watched<?2 OR (watched=?2 AND note_id<?3)) ORDER BY watched DESC,note_id DESC LIMIT 21","isi",id,before_id?before_day:"",before_id);
}
int aw_public_notes(sqlite3 *db,const char *ns,sqlite3_int64 id,const char *space,sqlite3_stmt **rows) {
    if(rows)*rows=NULL;if(!db||!rows||!key(ns,127)||!key(space,64)||id<=0)return 0;
    return cine_rows(db,rows,0,"SELECT n.ref,n.watched,n.rating,n.rewatch,n.body FROM account_world_note n "
                     "JOIN account_world_release r ON r.world_id=n.world_id AND r.state='public' " PUBLIC_GUARD
                     "WHERE r.namespace=?1 AND r.world_id=?2 AND r.space_id=?3 AND n.visibility='public' AND EXISTS(SELECT 1 FROM account_world_release_item i "
                     "WHERE i.world_id=r.world_id AND i.space_id=r.space_id AND i.ref=n.ref) ORDER BY n.watched DESC,n.note_id DESC LIMIT 24","sis",ns,id,space);
}
int aw_public_club(sqlite3 *db,const char *ns,const char *space) {
    sqlite3_int64 id=0;char host[257];if(!db||!key(ns,127)||!key(space,64))return 0;
    return club_find(db,ns,space,&id,host);
}
