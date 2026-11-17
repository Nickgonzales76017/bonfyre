#ifndef BF_PROFILE_AUTHORING_OVERVIEW_H
#define BF_PROFILE_AUTHORING_OVERVIEW_H
#include "ui_tree.h"
/* Public, exact CMS-owned C composition. When a site has not undergone a
 * reviewed authoring publication, preserve its existing editorial projection. */
#include "profile_story.h"
#include "site_authored_document.h"
#include <stdlib.h>
static int bf_profile_authoring_overview_emit(FILE *out,sqlite3 *db,const char *site_id){
 if(!out||!db||!site_id||!*site_id)return -1;
 const size_t cap=BF_SITE_AUTHORED_BUFFER_CAP;
 char *content=(char *)malloc(cap);
 if(!content)return bf_profile_story_emit(out,db,site_id);
 int blocks=0;
 int ready=bf_site_authored_document_project(db,site_id,"/",content,cap,&blocks);
 if(ready==1&&blocks>0){
  ui_start(out,"link");ui_attr(out,"rel","stylesheet");ui_attr(out,"href","/assets/authoring-reader.css");ui_gt(out);
  ui_start(out,"script");ui_flag(out,"defer");ui_attr(out,"src","/assets/authoring-reader.js");ui_gt(out);ui_close(out,"script");
  ui_start(out,"div");ui_attr(out,"data-profile-authoring","typed-native-c");ui_attr(out,"role","region");ui_attr(out,"aria-label","Published profile composition");ui_gt(out);
  fputs(content,out);
  ui_close(out,"div");
  free(content);return blocks;
 }
 free(content);
 return bf_profile_story_emit(out,db,site_id);
}
#endif
