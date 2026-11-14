/* Test public CMS projection without a logged-in account or production store. */
#define _GNU_SOURCE
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../src/profile_story.h"
static void ck(int ok,const char *why){if(!ok){fprintf(stderr,"FAIL: %s\n",why);exit(1);}}
static void sql(sqlite3 *db,const char *q){ck(sqlite3_exec(db,q,NULL,NULL,NULL)==SQLITE_OK,"SQLite setup");}
static char *render(sqlite3 *db,const char *site,int expected){
    char *buf=NULL;size_t n=0;FILE *out=open_memstream(&buf,&n);ck(out!=NULL,"memstream");
    ck(bf_profile_story_emit(out,db,site)==expected,"published block count");
    ck(fclose(out)==0,"flush");return buf;
}
int main(void){
    sqlite3 *db=NULL;ck(sqlite3_open(":memory:",&db)==SQLITE_OK,"db");
    sql(db,"CREATE TABLE site_fragment(id INTEGER PRIMARY KEY,namespace TEXT,page TEXT,slot TEXT,audience TEXT,status TEXT,position INTEGER,kind TEXT,eyebrow TEXT,heading TEXT,body TEXT,label TEXT,href TEXT,variant_key TEXT,projection_contract TEXT)");
    sql(db,"INSERT INTO site_fragment(namespace,page,slot,audience,status,position,kind,eyebrow,heading,body,label,href,variant_key) VALUES"
      "('site-a','/','showcase','public','published',5,'profile-hero','About','A <good> site','Real & clear','Open','https://example.org/','forest'),"
      "('site-a','/','showcase','self','published',6,'profile-card','','Hidden owner','Secret','','','paper'),"
      "('site-a','/','showcase','public','draft',7,'profile-card','','Hidden draft','Secret','','','paper'),"
      "('site-b','/','showcase','public','published',8,'profile-card','','Other person','Secret','','','paper'),"
      "('site-a','/','showcase','public','published',10,'profile-section','Work','Work & writing','Sources and software','','','rust'),"
      "('site-a','/','showcase','public','published',11,'profile-card','Source','Research','A linked publication','Read','https://example.org/post?a=1&b=2','wide'),"
      "('site-a','/','showcase','public','published',12,'profile-quote','','Safety','No execution','Bad','javascript:alert(1)','paper'),"
      "('site-a','/','cover','public','published',13,'profile-card','','Wrong slot','','','','paper')");
    char *a=render(db,"site-a",4);
    ck(strstr(a,"data-profile-storyboard")!=NULL,"semantic feature");
    ck(strstr(a,"class=\"ps-group\"")!=NULL,"profile section uses the shared component");
    ck(strstr(a,"class=\"ps-card\"")!=NULL,"profile card uses the shared component");
    ck(strstr(a,"<style")==NULL,"profile story does not emit inline styling");
    ck(strstr(a,"A &lt;good&gt; site")!=NULL,"escape HTML");
    ck(strstr(a,"Real &amp; clear")!=NULL,"escape copy");
    ck(strstr(a,"post?a=1&amp;b=2")!=NULL,"escape URL");
    ck(strstr(a,"Hidden owner")==NULL&&strstr(a,"Hidden draft")==NULL,"no private material");
    ck(strstr(a,"Other person")==NULL&&strstr(a,"Wrong slot")==NULL,"namespace and slot isolation");
    ck(strstr(a,"javascript:")==NULL,"unsafe URL refused");
    ck(strstr(a,"A &lt;good&gt; site")<strstr(a,"Work &amp; writing")&&strstr(a,"Work &amp; writing")<strstr(a,"Research"),"stable order");
    free(a);
    char *b=render(db,"site-b",1);ck(strstr(b,"Other person")!=NULL&&strstr(b,"A &lt;good&gt; site")==NULL,"second profile independent");free(b);
    char *c=render(db,"site-empty",0);ck(*c=='\0',"empty renders no placeholder");free(c);
    sqlite3_close(db);puts("profile-story PASS");return 0;
}
