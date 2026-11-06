#ifndef BONFYRE_ACCOUNT_CHOICES_H
#define BONFYRE_ACCOUNT_CHOICES_H
/* Public institution discovery reads the existing admitted SourceFrontier.
 * The selected host continues through the existing institution owner, which
 * rereads admission. This catalogue never grants or stores a relationship. */
typedef struct {char query[161],after[254];} AccountChoiceQuery;
static int aq_host(const char *s){return s&&*s&&strlen(s)<=253&&strspn(s,"abcdefghijklmnopqrstuvwxyz0123456789.-")==strlen(s)&&strchr(s,'.')&&!strstr(s,"..")&&*s!='.'&&s[strlen(s)-1]!='.';}
static int aq_query(const char *input,AccountChoiceQuery *out){memset(out,0,sizeof(*out));unsigned seen=0;if(input&&strlen(input)>1400)return 0;
 for(const char *p=input?input:"";*p;){const char *end=strchr(p,'&');if(!end)end=p+strlen(p);const char *eq=memchr(p,'=',(size_t)(end-p));char key[20];if(!eq||!og_human(p,(size_t)(eq-p),key,sizeof(key)))return 0;
  char *dest;size_t cap;unsigned bit;if(!strcmp(key,"q")){dest=out->query;cap=sizeof(out->query);bit=1;}else if(!strcmp(key,"after")){dest=out->after;cap=sizeof(out->after);bit=2;}else return 0;
  if((seen&bit)||!og_human(eq+1,(size_t)(end-eq-1),dest,cap))return 0;seen|=bit;p=*end?end+1:end;if(!*p&&*end)return 0;
 }return !*out->after||aq_host(out->after);
}
static int serve_account_choices(const char *method,const char *target,const char *query){
 if(strcmp(target,"/account/choices/institutions"))return 0;if(strcmp(method,"GET")){aw_error(405,"Use a read to choose a public institution.\n");return 1;}
 AccountChoiceQuery input;if(!aq_query(query,&input)){aw_error(400,"Search by a name using the current catalogue controls.\n");return 1;}
 const char *path=getenv("BONFYRE_SOURCE_FRONTIER_DB");sqlite3 *db=NULL;sqlite3_stmt *s=NULL;
 if(!path||!*path||sqlite3_open_v2(path,&db,SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){if(db)sqlite3_close(db);aw_error(503,"The public institution directory is unavailable. Try again; no relationship has changed.\n");return 1;}
 const char *sql="SELECT r.host,COALESCE(NULLIF(trim(r.name),''),r.host),count(c.channel_id) FROM root r JOIN channel c ON c.root_id=r.root_id WHERE c.state IN ('admitted','active') AND r.host>?1 AND (instr(lower(COALESCE(r.name,'')),lower(?2))>0 OR instr(lower(r.host),lower(?2))>0) GROUP BY r.host HAVING count(DISTINCT r.root_id)=1 ORDER BY r.host LIMIT 21";
 if(sqlite3_prepare_v2(db,sql,-1,&s,NULL)!=SQLITE_OK){sqlite3_close(db);aw_error(503,"The public institution directory could not be read.\n");return 1;}sqlite3_bind_text(s,1,input.after,-1,SQLITE_STATIC);sqlite3_bind_text(s,2,input.query,-1,SQLITE_STATIC);
 struct {char host[254],label[501];long long channels;} rows[20];unsigned count=0;int more=0,step;char last[254]="";
 while((step=sqlite3_step(s))==SQLITE_ROW){const char *host=(const char *)sqlite3_column_text(s,0),*label=(const char *)sqlite3_column_text(s,1);if(!aq_host(host))continue;if(count==20){more=1;break;}snprintf(rows[count].host,sizeof(rows[count].host),"%s",host);size_t length=strlen(label);if(length>=sizeof(rows[count].label)){length=sizeof(rows[count].label)-1;while(length&&(((unsigned char)label[length]&0xc0)==0x80))length--;}memcpy(rows[count].label,label,length);rows[count].label[length]=0;rows[count].channels=sqlite3_column_int64(s,2);snprintf(last,sizeof(last),"%s",host);count++;}
 sqlite3_finalize(s);sqlite3_close(db);if(!more&&step!=SQLITE_DONE){aw_error(503,"The public institution directory stopped before its page was complete.\n");return 1;}
 bf_mp_writer_t w;bf_mp_writer_init(&w,16384);int bad=bf_mp_pack_array(&w,3)||bf_mp_pack_uint(&w,1)||bf_mp_pack_array(&w,count);
 for(unsigned n=0;n<count&&!bad;n++)bad=bf_mp_pack_array(&w,3)||bf_mp_pack_str_cstr(&w,rows[n].host)||bf_mp_pack_str_cstr(&w,rows[n].label)||bf_mp_pack_uint(&w,(uint64_t)rows[n].channels);
 if(!bad)bad=bf_mp_pack_str_cstr(&w,more?last:"");if(bad)aw_error(503,"The public institution directory could not be prepared.\n");else aw_response(200,"application/msgpack",w.buf,w.len,0);bf_mp_writer_free(&w);return 1;
}
#endif
