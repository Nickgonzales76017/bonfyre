#ifndef BONFYRE_PROFILE_PUBLICATION_PLACEMENT_H
#define BONFYRE_PROFILE_PUBLICATION_PLACEMENT_H

/* A destination may display a publisher-owned composed article only when two
 * independently retained CMS scopes agree on its exact source, destination,
 * version and review reference. A record is never a membership or editor grant.
 * An incomplete/missing schema, source, contract, or approval is a closed door. */
typedef struct {
 char origin_site[128],origin_route[128],origin_host[256];
 char author_credit[161],content_sha256[65];
} PhPublicationPlacement;

static int ph_placement_route(const char *route){
 if(!route||strncmp(route,"/blog/",6))return 0;
 size_t n=strlen(route);if(n<9||n>=128||route[n-1]!='/')return 0;
 for(size_t i=6;i<n-1;i++){
  unsigned char c=(unsigned char)route[i];
  if(!((c>='a'&&c<='z')||(c>='0'&&c<='9')||c=='-'))return 0;
 }
 return 1;
}
static int ph_placement_resolve(sqlite3 *db,const PhSite *site,const char *route,
                                 PhPublicationPlacement *out,char **rendered){
 if(rendered)*rendered=NULL;
 if(!db||!site||!site->actor[0]||!out||!ph_placement_route(route))return 0;
 memset(out,0,sizeof(*out));
 /* Both approvals are scoped to their own namespace. No source byline is
  * treated as the destination's actor, and no draft can be read here. */
 const char *sql=
 "SELECT d.origin_site,d.origin_route,s.host,COALESCE(p.author_credit,''),d.origin_page_digest "
 "FROM site_publication_placement d "
 "JOIN site_publication_placement o ON o.namespace=d.origin_site "
 " AND o.placement_id=d.placement_id AND o.status='published' "
 " AND o.origin_site=d.origin_site AND o.origin_route=d.origin_route "
 " AND o.origin_page_digest=d.origin_page_digest "
 " AND o.destination_site=d.destination_site AND o.destination_route=d.destination_route "
 " AND o.publisher_subject=d.publisher_subject AND o.destination_subject=d.destination_subject "
 " AND o.review_request_ref=d.review_request_ref AND o.review_request_ref<>'' "
 "JOIN site_presentation s ON s.namespace='root' AND s.site_id=d.origin_site "
 " AND s.status='published' AND s.subject_address=d.publisher_subject "
 "JOIN site_page p ON p.namespace=d.origin_site AND p.route=d.origin_route "
 " AND p.status='published' AND p.editorial_kind='article' AND p.machine_view='authored' "
 "WHERE d.namespace=?1 AND d.destination_site=?1 AND d.destination_subject=?2 "
 " AND d.destination_route=?3 AND d.status='published' "
 " AND d.origin_site<>d.destination_site "
 " AND d.publisher_subject<>'' AND d.origin_page_digest<>'' "
 " AND NOT EXISTS(SELECT 1 FROM site_page own WHERE own.namespace=?1 AND own.route=?3) "
 " AND NOT EXISTS(SELECT 1 FROM site_presentation conflict WHERE conflict.namespace='root' "
 " AND conflict.status='published' AND conflict.host=s.host AND conflict.subject_address<>s.subject_address) "
 " ORDER BY d.id LIMIT 2";
 sqlite3_stmt *st=NULL;int valid=0,step;
 if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(st,1,site->id,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(st,2,site->actor,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(st,3,route,-1,SQLITE_TRANSIENT);
 step=sqlite3_step(st);
 if(step==SQLITE_ROW){
  char *dest[]={out->origin_site,out->origin_route,out->origin_host,out->author_credit,out->content_sha256};
  const size_t caps[]={sizeof(out->origin_site),sizeof(out->origin_route),sizeof(out->origin_host),sizeof(out->author_credit),sizeof(out->content_sha256)};
  valid=1;
  for(int j=0;j<5;j++){
   const char *value=(const char*)sqlite3_column_text(st,j);
   if(!value||!*value||strlen(value)>=caps[j]){valid=0;break;}
   snprintf(dest[j],caps[j],"%s",value);
  }
  if(valid && sqlite3_step(st)==SQLITE_ROW)valid=0;
 }else valid=0;
 sqlite3_finalize(st);
 if(!valid||!studio_safe_slug_arg(out->origin_site)||!ph_placement_route(out->origin_route)
    ||strlen(out->content_sha256)!=64
    ||strspn(out->content_sha256,"0123456789abcdef")!=64
    ||!lower_host(out->origin_host,(char[256]){0})){
  memset(out,0,sizeof(*out));return 0;
 }
 char normal_host[256];if(!lower_host(out->origin_host,normal_host)||strcmp(out->origin_host,normal_host)){
  memset(out,0,sizeof(*out));return 0;
 }
 char *composition=malloc(BF_SITE_AUTHORED_BUFFER_CAP);
 if(!composition){memset(out,0,sizeof(*out));return 0;}
 int blocks=0;
 int built=bf_site_authored_document_project(db,out->origin_site,out->origin_route,
                                              composition,BF_SITE_AUTHORED_BUFFER_CAP,&blocks);
 char actual[65]={0};
 if(built==1&&blocks>0)
  bf_sha256_hex((const unsigned char*)composition,strlen(composition),actual);
 if(strcmp(actual,out->content_sha256)){free(composition);memset(out,0,sizeof(*out));return 0;}
 if(rendered)*rendered=composition;
 else free(composition);
 return 1;
}
#endif
