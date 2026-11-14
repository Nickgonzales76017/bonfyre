#ifndef BF_PROFILE_AUTHORING_OVERVIEW_H
#define BF_PROFILE_AUTHORING_OVERVIEW_H
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
  fputs("<link rel=\"stylesheet\" href=\"/assets/authoring-reader.css\">",out);
  fputs("<script defer src=\"/assets/authoring-reader.js\"></script>",out);
  fputs("<div data-profile-authoring=\"typed-native-c\" role=\"region\""
        " aria-label=\"Published profile composition\">",out);
  fputs(content,out);
  fputs("</div>",out);
  free(content);return blocks;
 }
 free(content);
 return bf_profile_story_emit(out,db,site_id);
}
#endif
