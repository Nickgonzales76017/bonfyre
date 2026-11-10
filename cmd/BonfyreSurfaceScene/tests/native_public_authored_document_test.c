/* A native publish-state test, distinct from a browser-authored draft test.
 * Public C output is only admitted after exact site_page + site_fragment state,
 * representation, audience, nesting, material and media provenance gates. */
#include "../../BonfyreWebAddress/src/site_authored_document.h"
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static void ck(int yes,const char *why){
 if(!yes){fprintf(stderr,"native public authored page FAIL: %s\n",why);exit(1);}
}
static void execute(sqlite3 *db,const char *sql){
 char *error=NULL;
 if(sqlite3_exec(db,sql,NULL,NULL,&error)!=SQLITE_OK){
  fprintf(stderr,"SQL: %s\n",error?error:"unknown");sqlite3_free(error);exit(1);
 }
}
static const char *bootstrap=
 "CREATE TABLE site_page (id INTEGER PRIMARY KEY,namespace TEXT,route TEXT,status TEXT);"
 "CREATE TABLE site_fragment("
 "id INTEGER PRIMARY KEY,namespace TEXT,page TEXT,slot TEXT,audience TEXT,"
 "status TEXT,position INTEGER,coordinate TEXT,kind TEXT,heading TEXT,body TEXT,"
 "semantic_tags TEXT,projection_contract TEXT,source_digest TEXT,evidence_ref TEXT);"
 "INSERT INTO site_page VALUES"
 "(1,'person-alpha-work','/','published'),"
 "(2,'person-beta-work','/','published'),"
 "(3,'person-alpha-work','/draft/','draft');"
 "INSERT INTO site_fragment VALUES"
 "(1,'person-alpha-work','/','showcase','public','published',0,'docs',"
 "'ui:documentation','Documented <proof>','',"
 "'{\"authoring_v1\":{\"version\":1,\"parent\":null,\"payload\":{\"title\":\"Documentation & examples\","
 "\"lede\":\"Native C composition\","
 "\"toc\":[{\"label\":\"Overview\",\"href\":\"#overview\"}]}}}',"
 "'{\"html\":true}',NULL,NULL),"
 "(2,'person-alpha-work','/','showcase','public','published',1,'tabs',"
 "'ui:nav-tabs','Interactive tabs','',"
 "'{\"authoring_v1\":{\"version\":1,\"parent\":\"docs\",\"payload\":{\"tabs\":["
 "{\"label\":\"Summary\",\"body\":\"First\"},{\"label\":\"Detail\",\"body\":\"Second\"}]}}}',"
 "'{\"html\":true}',NULL,NULL),"
 "(3,'person-alpha-work','/','showcase','self','published',2,'selfonly',"
 "'ui:alert','PRIVATE OWNER',NULL,NULL,'{\"html\":true}',NULL,NULL),"
 "(4,'person-alpha-work','/','showcase','public','draft',3,'draftonly',"
 "'ui:modal','SECRET DRAFT',NULL,NULL,'{\"html\":true}',NULL,NULL),"
 "(5,'person-beta-work','/','showcase','public','published',0,'other',"
 "'ui:modal','FOREIGN PAGE',NULL,NULL,'{\"html\":true}',NULL,NULL),"
 "(6,'person-alpha-work','/','showcase','public','published',4,'nohtml',"
 "'ui:badge','DENIED REPRESENTATION',NULL,NULL,'{\"html\":false}',NULL,NULL);";
static int project(sqlite3 *db,const char *ns,const char *route,
 char *out,size_t cap,int *rows){
 memset(out,'?',cap);
 return bf_site_authored_document_project(db,ns,route,out,cap,rows);
}
int main(void){
 sqlite3 *db=NULL;char out[32768];int rows=-1;
 ck(sqlite3_open(":memory:",&db)==SQLITE_OK,"db open");
 execute(db,bootstrap);
 ck(project(db,"person-alpha-work","/",out,sizeof(out),&rows)==1,"public admitted");
 ck(rows==2,"two typed components");
 ck(strstr(out,"data-authored-native=\"c\"")!=NULL,"C renderer in actual CMS projection");
 ck(strstr(out,"Documentation &amp; examples")!=NULL,"escaped page data");
 ck(strstr(out,"role=\"tablist\"")!=NULL,"interactive tabs");
 ck(strstr(out,"data-ak-tab=\"1\"")!=NULL,"second tab indexed");
 ck(strstr(out,"PRIVATE OWNER")==NULL&&strstr(out,"SECRET DRAFT")==NULL&&
    strstr(out,"FOREIGN PAGE")==NULL&&strstr(out,"DENIED REPRESENTATION")==NULL,
    "exact namespace, carrier and audience isolation");
 execute(db,"INSERT INTO site_fragment VALUES (12,'person-alpha-work','/','showcase','public','published',2,'native-story','profile-story','Published editorial section','Legacy summary','{\"authoring_v1\":{\"version\":1,\"parent\":\"docs\",\"payload\":{},\"editorial\":[{\"type\":\"paragraph\",\"data\":{\"text\":\"Written through the real Editor.js interface & retained\"}},{\"type\":\"header\",\"data\":{\"level\":3,\"text\":\"A human-authored chapter\"}},{\"type\":\"quote\",\"data\":{\"text\":\"A documented claim\",\"caption\":\"On the record\"}},{\"type\":\"code\",\"data\":{\"code\":\"value < 10\"}},{\"type\":\"delimiter\",\"data\":{}},{\"type\":\"list\",\"data\":{\"style\":\"ordered\",\"items\":[\"First\",\"Second\"]}},{\"type\":\"checklist\",\"data\":{\"items\":[{\"text\":\"Approved\",\"checked\":true}]}},{\"type\":\"table\",\"data\":{\"content\":[[\"Column\",\"Data\"],[\"Source\",\"Local CMS\"]]}}]}}','{\"html\":true}',NULL,NULL);");
 ck(project(db,"person-alpha-work","/",out,sizeof(out),&rows)==1,
   "a published editorial block is compiled from its retained typed CMS record");
 ck(rows==3,"native public composition includes the article");
 ck(strstr(out,"Written through the real Editor.js interface &amp; retained")!=NULL,
    "public C projection retains and escapes Editor.js paragraphs");
 ck(strstr(out,"<h3>A human-authored chapter</h3>")!=NULL,
    "native CMS header level retained");
 ck(strstr(out,"<ol class=\"ak-editorial-list\">")!=NULL,
    "ordered editorial list retained through CMS");
 ck(strstr(out,"<td>Local CMS</td>")!=NULL,"editorial table retained");
 ck(strstr(out,"Legacy summary")==NULL,
    "editorial body has one authoritative rendering, not a duplicate summary");
 execute(db,"INSERT INTO site_fragment VALUES (13,'person-alpha-work','/','showcase','public','published',3,'bad-editorial','profile-story','Bad editorial','','{\"authoring_v1\":{\"version\":1,\"parent\":\"docs\",\"payload\":{},\"editorial\":[{\"type\":\"arbitrary-script\",\"data\":{\"text\":\"disallowed\"}}]}}','{\"html\":true}',NULL,NULL);");
 ck(project(db,"person-alpha-work","/",out,sizeof(out),&rows)==-1,
    "unknown Editor.js tools never silently disappear from a public release");
 ck(*out=='\0'&&rows==0,"invalid editorial composition is atomic");
 execute(db,"DELETE FROM site_fragment WHERE id IN (12,13);");
 execute(db,"UPDATE site_fragment SET semantic_tags=json_set(semantic_tags,'$.authoring_v1.responsive',json('{\"desktop\":{\"layout\":\"grid\",\"columns\":3},\"phone\":{\"layout\":\"stack\"}}')) WHERE id=1;");
 ck(project(db,"person-alpha-work","/",out,sizeof(out),&rows)==1,
    "native C honors the editor's retained responsive data");
 ck(strstr(out,"data-desktop-layout=\"grid\" data-desktop-columns=\"3\"")!=NULL,
    "generated desktop responsive attributes");
 ck(strstr(out,"data-phone-layout=\"stack\"")!=NULL,
    "generated mobile responsive attributes");
 execute(db,"UPDATE site_fragment SET semantic_tags=json_set(semantic_tags,'$.authoring_v1.responsive',json('{\"desktop\":{\"layout\":\"grid\",\"columns\":3,\"gap\":17}}')) WHERE id=1;");
 ck(project(db,"person-alpha-work","/",out,sizeof(out),&rows)==-1,
    "unsupported style token cannot silently vanish in a public release");
 ck(*out=='\0'&&rows==0,"unsupported responsive data is atomic");
 execute(db,"UPDATE site_fragment SET semantic_tags=json_remove(semantic_tags,'$.authoring_v1.responsive') WHERE id=1;");
 ck(project(db,"person-alpha-work","/draft/",out,sizeof(out),&rows)==0,
    "draft-only page never publicly projected");
 ck(rows==0&&*out=='\0',"no accidental fallback bytes");
 ck(project(db,"person-absent","/",out,sizeof(out),&rows)==0,
    "unknown owner is empty");
 ck(project(db,"person-alpha-work","/",out,96,&rows)==-1,
    "bounded output refuses truncation");
 ck(*out=='\0'&&rows==0,"overflow output atomic");
 execute(db,
 "INSERT INTO site_fragment VALUES"
 "(7,'person-alpha-work','/','showcase','public','published',5,'orphan',"
 "'ui:card','Orphan',NULL,"
 "'{\"authoring_v1\":{\"version\":1,\"parent\":\"missing-id\",\"payload\":{\"title\":\"orphan\"}}}',"
 "'{\"html\":true}',NULL,NULL);");
 ck(project(db,"person-alpha-work","/",out,sizeof(out),&rows)==-1,"orphan child denied");
 execute(db,"DELETE FROM site_fragment WHERE id=7;");
 execute(db,
 "INSERT INTO site_fragment VALUES"
 "(8,'person-alpha-work','/','showcase','public','published',5,'unsafe',"
 "'ui:buttons','Unsafe',NULL,"
 "'{\"authoring_v1\":{\"version\":1,\"parent\":\"docs\",\"payload\":{\"items\":[{\"label\":\"Attack\","
 "\"href\":\"javascript:alert(1)\"}]}}}',"
 "'{\"html\":true}',NULL,NULL);");
 ck(project(db,"person-alpha-work","/",out,sizeof(out),&rows)==-1,"malicious destination refused");
 ck(*out=='\0',"malicious output atomic");
 execute(db,"DELETE FROM site_fragment WHERE id=8;");
 execute(db,
 "INSERT INTO site_fragment VALUES"
 "(9,'person-alpha-work','/','showcase','public','published',5,'media',"
 "'ui:card','Unreviewed source',NULL,"
 "'{\"authoring_v1\":{\"version\":1,\"parent\":\"docs\",\"payload\":{\"image\":{\"src\":"
 "\"https://example.org/unreviewed.webp\",\"alt\":\"Camera\"}}}}',"
 "'{\"html\":true}',NULL,NULL);");
 ck(project(db,"person-alpha-work","/",out,sizeof(out),&rows)==-1,
   "public media without evidence is withheld");
 ck(*out=='\0',"media failure atomic");
 execute(db,"DELETE FROM site_fragment WHERE id=9;");
 execute(db,
  "INSERT INTO site_fragment VALUES"
  "(10,'person-alpha-work','/','showcase','public','published',5,'unsupported',"
  "'service:future-component','Unexpected',NULL,"
  "'{\"authoring_v1\":{\"version\":1,\"parent\":\"docs\",\"payload\":{\"title\":\"incomplete\"}}}',"
  "'{\"html\":true}',NULL,NULL);");
 ck(project(db,"person-alpha-work","/",out,sizeof(out),&rows)==-1,
    "unsupported service component fails closed instead of disappearing");
 ck(*out=='\0',"unknown component output atomic");
 execute(db,"DELETE FROM site_fragment WHERE id=10;");
 execute(db,
  "INSERT INTO site_fragment VALUES"
  "(11,'person-alpha-work','/','showcase','public','published',5,'legacy',"
  "'profile-hero','Older editorial module',NULL,NULL,'{\"html\":true}',NULL,NULL);");
 ck(project(db,"person-alpha-work","/",out,sizeof(out),&rows)==-1,
    "older profile block cannot impersonate a typed authored release");
 ck(*out=='\0',"legacy fallback remains separate");
 execute(db,"DELETE FROM site_fragment WHERE id=11;");
 /* Large user-authored guides used to hit the hard-coded 256 KiB call-site
  * buffer even though the native renderer already supports a full MiB. */
 char *huge=(char *)malloc(BF_SITE_AUTHORED_BUFFER_CAP);
 char *longbody=(char *)malloc(12001);
 ck(huge&&longbody,"allocate documented public output bound");
 memset(longbody,'&',12000);longbody[12000]='\0';
 sqlite3_stmt *batch=NULL;
 ck(sqlite3_prepare_v2(db,"INSERT INTO site_fragment(namespace,page,slot,audience,status,position,coordinate,kind,heading,body,semantic_tags,projection_contract) VALUES('person-alpha-work','/','showcase','public','published',?1,?2,'profile-story','Long documentation',?3,'{\"authoring_v1\":{\"version\":1,\"parent\":\"docs\",\"payload\":{}}}','{\"html\":true}')",-1,&batch,NULL)==SQLITE_OK,
    "prepare long documentation fixtures");
 for(int i=0;i<9;i++){
  char coord[32];snprintf(coord,sizeof(coord),"long-section-%d",i);
  sqlite3_bind_int(batch,1,100+i);
  sqlite3_bind_text(batch,2,coord,-1,SQLITE_TRANSIENT);
  sqlite3_bind_text(batch,3,longbody,12000,SQLITE_STATIC);
  ck(sqlite3_step(batch)==SQLITE_DONE,"write long authored chapter");
  sqlite3_reset(batch);sqlite3_clear_bindings(batch);
 }
 sqlite3_finalize(batch);
 ck(project(db,"person-alpha-work","/",huge,BF_SITE_AUTHORED_BUFFER_CAP,&rows)==1,
    "large authored page compiles natively within documented 1 MiB");
 ck(rows==11&&strlen(huge)>262144,
    "large source-backed profile has all sections and exceeds old 256 KiB cap");
 ck(project(db,"person-alpha-work","/",out,sizeof(out),&rows)==-1,
    "small callers are still refused without partial output");
 ck(*out=='\0'&&rows==0,"oversize response is atomic");
 execute(db,"DELETE FROM site_fragment WHERE coordinate LIKE 'long-section-%';");
 free(huge);free(longbody);
 sqlite3_close(db);
 puts("native public authored document PASS");
 return 0;
}
