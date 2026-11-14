#ifndef BONFYRE_PROFILE_DATING_COMPOSITION_H
#define BONFYRE_PROFILE_DATING_COMPOSITION_H
#include "profile_dating_stitch.h"
/* Stitch reference images inform presentation; neither sample people nor
 * sample conversations from those files are generated as CMS members. */
typedef struct {
 char eyebrow[101],heading[201],lede[1201];
} PhDatingComposition;
static PhDatingComposition ph_dating_composition(sqlite3 *cms,const char *view){
 static const char *const modes[]={"discover","matches","chat","studio"};
 static const char *const eyebrows[]={
  "AUREKAI LIFE · DISCOVERY","AUREKAI LIFE · CONNECTIONS",
  "AUREKAI LIFE · PRIVATE CONVERSATION","AUREKAI LIFE · PROFILE STUDIO"
 };
 static const char *const headings[]={
  "Meet beyond the profile","Connections worth continuing",
  "A conversation at your pace","Your story, on your terms"
 };
 static const char *const ledes[]={
  "Discover people through stories they have chosen to publish. Every introduction is private, mutual and revocable.",
  "People you have mutually chosen, and invitations still waiting for a decision. Nothing becomes a conversation by inference.",
  "The best conversations have room to breathe. Every message stays private and depends on two current permissions.",
  "Compose your published Life identity with the shared visual editors. Private compatibility choices stay inside Participation."
 };
 int index=0;
 for(int i=0;i<4;i++)if(!strcmp(view,modes[i]))index=i;
 PhDatingComposition c={0};
 snprintf(c.eyebrow,sizeof(c.eyebrow),"%s",eyebrows[index]);
 snprintf(c.heading,sizeof(c.heading),"%s",headings[index]);
 snprintf(c.lede,sizeof(c.lede),"%s",ledes[index]);
 if(!cms)return c;
 sqlite3_stmt *s=NULL;
 if(sqlite3_prepare_v2(cms,"SELECT COALESCE(eyebrow,''),COALESCE(heading,''),"
  "COALESCE(body,'') FROM site_fragment WHERE namespace='root' "
  "AND page='/profile/dating' AND slot=?1 AND kind='dating-view' "
  "AND audience='self' AND status='published' ORDER BY position,id LIMIT 2",
  -1,&s,NULL)!=SQLITE_OK)return c;
 sqlite3_bind_text(s,1,view,-1,SQLITE_STATIC);
 PhDatingComposition selected={0};int rows=0;
 while(sqlite3_step(s)==SQLITE_ROW){
  if(++rows!=1)break;
  char *fields[]={selected.eyebrow,selected.heading,selected.lede};
  const size_t sizes[]={sizeof(selected.eyebrow),sizeof(selected.heading),sizeof(selected.lede)};
  for(int i=0;i<3;i++){
   const char *value=(const char*)sqlite3_column_text(s,i);
   if(value&&strlen(value)<sizes[i])snprintf(fields[i],sizes[i],"%s",value);
  }
 }
 sqlite3_finalize(s);
 /* A duplicated editorial coordinate is not a last-write-wins authority. */
 if(rows==1&&selected.heading[0]&&selected.lede[0]){
  if(selected.eyebrow[0])snprintf(c.eyebrow,sizeof(c.eyebrow),"%s",selected.eyebrow);
  snprintf(c.heading,sizeof(c.heading),"%s",selected.heading);
  snprintf(c.lede,sizeof(c.lede),"%s",selected.lede);
 }
 return c;
}
static void ph_dating_view_nav(const char *view){
 static const char *const modes[]={"discover","matches","chat","studio"};
 static const char *const labels[]={"Discover","Matches & messages","Conversation","Profile studio"};
 fputs("<nav class=\"ph-date-switcher\" aria-label=\"Dating experience views\">",stdout);
 for(int i=0;i<4;i++){
  fputs("<a href=\"/profile?tab=dating&amp;view=",stdout);
  fputs(modes[i],stdout);
  fputs("\"",stdout);
  if(!strcmp(modes[i],view))fputs(" aria-current=\"page\"",stdout);
  fputs(">",stdout);html(labels[i]);fputs("</a>",stdout);
 }
 fputs("</nav>",stdout);
}
static void ph_dating_public_prompt(sqlite3 *cms,const PhSite *site){
 sqlite3_stmt *s=NULL;
 if(!cms||!site)return;
 if(sqlite3_prepare_v2(cms,"SELECT COALESCE(body,'') FROM site_fragment "
  "WHERE namespace=?1 AND page='/' AND kind='profile-story' "
  "AND slot='showcase' AND audience='public' AND status='published' "
  "AND CASE WHEN json_valid(semantic_tags) THEN "
  "json_extract(semantic_tags,'$.authoring_v1.metadata.excerpt_context')='life-introduction' "
  "ELSE 0 END "
  "ORDER BY position,id LIMIT 1",-1,&s,NULL)!=SQLITE_OK)return;
 sqlite3_bind_text(s,1,site->id,-1,SQLITE_STATIC);
 if(sqlite3_step(s)==SQLITE_ROW){
  const char *value=(const char *)sqlite3_column_text(s,0);
  if(value&&*value&&strlen(value)<=500){
   fputs("<blockquote class=\"ph-date-prompt\">",stdout);html(value);fputs("</blockquote>",stdout);
  }
 }
 sqlite3_finalize(s);
}
static void ph_dating_public_image(sqlite3 *cms,const PhSite *site){
 sqlite3_stmt *s=NULL;int id=0;
 fputs("<figure class=\"ph-date-media\">",stdout);
 if(cms&&site&&sqlite3_prepare_v2(cms,"SELECT id FROM site_fragment "
    "WHERE namespace=?1 AND kind='profile-gallery' AND slot='gallery' "
    "AND page='/' AND status='published' AND audience='public' "
    "ORDER BY position,id LIMIT 1",-1,&s,NULL)==SQLITE_OK){
  sqlite3_bind_text(s,1,site->id,-1,SQLITE_STATIC);
  if(sqlite3_step(s)==SQLITE_ROW)id=sqlite3_column_int(s,0);
 }
 sqlite3_finalize(s);
 if(id>0){
  PhArtRow row;PhArtRef ref;PhArt art;
  if(ph_art_row(cms,site->id,"profile-gallery",NULL,id,&row)==1&&
     ph_art_ref(row.ref,&ref)&&ph_art_resolve(cms,&ref,PH_GALLERY_LABEL,50,50,&art)&&
     !strcmp(row.status,"published")&&!strcmp(row.audience,"public")&&
     !strcmp(art.digest,row.evidence)&&page_image_url_safe(art.image)){
   fputs("<img loading=\"lazy\" decoding=\"async\" referrerpolicy=\"no-referrer\" src=\"",stdout);
   html(art.image);fputs("\" alt=\"",stdout);html(art.title);fputs("\">",stdout);
  }
 }
 fputs("<figcaption>",stdout);html(site?site->name:"Life member");
 fputs(" · Published Life material</figcaption></figure>",stdout);
}
/* Shared CMS token consumers, not a separate dating theme or imported Stitch page. */
static void ph_dating_shared_interaction_styles(void){
 fputs("\n",stdout);
}
#endif
