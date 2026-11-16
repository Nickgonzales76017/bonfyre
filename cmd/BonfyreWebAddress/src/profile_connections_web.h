#ifndef BONFYRE_PROFILE_CONNECTIONS_WEB_H
#define BONFYRE_PROFILE_CONNECTIONS_WEB_H
#include "ui_tree.h"
#include "form_tree.h"
#include "../../BonfyreParticipation/src/peer_facets.h"
/* One profile. Each context is a distinct private participation record.
 * Public posts/art/community records are linked from their original site,
 * not imported into a matching store or made into inferred relationships. */
static int ph_peer_site(sqlite3 *cms,const char *subject,const char *territory,PhSite *site){
 sqlite3_stmt *s=NULL;char id[128]="";int count=0;
 if(!cms||!bf_dating_subject(subject)||!territory||!site)return 0;
 if(sqlite3_prepare_v2(cms,"SELECT site_id FROM site_presentation "
  "WHERE namespace='root' AND status='published' AND profile='member-profile-v1' "
  "AND subject_address=?1 AND territory=?2 LIMIT 2",-1,&s,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(s,1,subject,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,2,territory,-1,SQLITE_TRANSIENT);
 while(sqlite3_step(s)==SQLITE_ROW){
  const char *value=(const char*)sqlite3_column_text(s,0);
  if(++count!=1||!value||strlen(value)>=sizeof(id)){count=0;break;}
  snprintf(id,sizeof(id),"%s",value);
 }
 sqlite3_finalize(s);
 return count==1&&ph_site(cms,NULL,id,site)&&!strcmp(site->actor,subject);
}
static void ph_peer_post_teaser(sqlite3 *cms,const PhSite *site){
 sqlite3_stmt *s=NULL;
 if(!cms||!site||sqlite3_prepare_v2(cms,
   "SELECT title,published_at FROM community_post WHERE namespace=?1 AND "
   "community=?1 AND author_subject=?2 AND visibility='public' AND "
   "moderation_state='accepted' AND status='published' ORDER BY id DESC LIMIT 1",
   -1,&s,NULL)!=SQLITE_OK)return;
 sqlite3_bind_text(s,1,site->id,-1,SQLITE_TRANSIENT);
 sqlite3_bind_text(s,2,site->actor,-1,SQLITE_TRANSIENT);
 if(sqlite3_step(s)==SQLITE_ROW){
  const char *title=(const char*)sqlite3_column_text(s,0);
  const char *time=(const char*)sqlite3_column_text(s,1);
  if(title&&*title){
   ui_open(stdout,"div","ph-peer-source");ui_open(stdout,"small",NULL);ui_copy(stdout,"From their published site");ui_close(stdout,"small");ui_open(stdout,"p",NULL);
   html(title);ui_close(stdout,"p");ui_open(stdout,"time",NULL);html(time?time:"");
   ui_close(stdout,"time");ui_copy(stdout," ");ui_start(stdout,"a");ui_attr(stdout,"rel","noopener noreferrer");ui_attr_open(stdout,"href");ui_copy(stdout,"https://");
   html(site->host);fputs("/?tab=posts\">Read on their site</a></div>",stdout);
  }
 }
 sqlite3_finalize(s);
}
static void ph_peer_action(const char *facet,const char *target,const char *choice,
                           unsigned rev,const char *label){
 char r[16];snprintf(r,sizeof(r),"%u",rev);
 const FtField hidden[]={{"facet",facet},{"other",target},{"choice",choice},{"rev",r}};
 const FtForm f={.id="ph-peer-action",.action="/profile/connections/choice",.label=label,.hook="peer-action",.hidden=hidden,.nhidden=4};
 ft_form(stdout,&f);
}
static void ph_peer_card(sqlite3 *cms,sqlite3 *owner,const BfPeerFacet *facet,const BfPeerCandidate *row,
                         const char *actor){
 PhSite site;if(!ph_peer_site(cms,row->subject,facet->territory,&site))return;
 ui_start(stdout,"article");ui_attr(stdout,"class","ph-peer-item");ui_flag(stdout,"data-peer-candidate");ui_gt(stdout);ui_open(stdout,"div","ph-peer-item-head");ui_start(stdout,"span");ui_attr(stdout,"class","ph-peer-mark");ui_attr(stdout,"aria-hidden","true");ui_gt(stdout);
 html(site.name[0]?"":"A");if(site.name[0]){char initial[2]={site.name[0],0};html(initial);}
 ui_close(stdout,"span");ui_open(stdout,"div",NULL);ui_open(stdout,"p","ph-kicker");ui_copy(stdout,"Published ");html(facet->label);
 fputs(" profile</p><h3>",stdout);html(site.name);
 ui_close(stdout,"h3");ui_open(stdout,"p","ph-peer-dek");html(site.bio);
 ui_close(stdout,"p");ui_close(stdout,"div");ui_start(stdout,"a");ui_attr(stdout,"rel","noopener noreferrer");ui_attr_open(stdout,"href");ui_copy(stdout,"https://");
 html(site.host);fputs("/\">View profile</a></div>",stdout);
 ui_start(stdout,"nav");ui_attr(stdout,"class","ph-peer-context-links");ui_attr(stdout,"aria-label","Source-owned publications");ui_gt(stdout);ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"https://");
 html(site.host);
 fputs(!strcmp(facet->id,"art")?"/?tab=gallery\">Browse published art</a>":
       !strcmp(facet->id,"civic")?"/?tab=communities\">View public communities</a>":
       !strcmp(facet->id,"projects")?"/?tab=posts\">See published work</a>":
       "/?tab=about\">Read their introduction</a>",stdout);
 ui_close(stdout,"nav");
 ph_peer_post_teaser(cms,&site);
 if(row->matched){
  BfPeerMessage messages[8];size_t count=0;
  ui_start(stdout,"section");ui_attr(stdout,"class","ph-peer-thread");ui_attr(stdout,"aria-label","Private conversation");ui_gt(stdout);ui_open(stdout,"h4",NULL);ui_copy(stdout,"Conversation");ui_close(stdout,"h4");
  if(bf_peer_messages(owner,actor,facet->id,row->subject,messages,8,&count)){
   ui_open(stdout,"ol",NULL);for(size_t i=count;i>0;i--){const BfPeerMessage *m=&messages[i-1];
    ui_open(stdout,"li",NULL);ui_open(stdout,"strong",NULL);html(!strcmp(m->sender,actor)?"You":"Them");ui_close(stdout,"strong");ui_open(stdout,"p",NULL);html(m->body);ui_close(stdout,"p");ui_close(stdout,"li");}
   if(!count)ui_text(stdout, "li", NULL, "Start a conversation about this shared context.");ui_close(stdout,"ol");
   char nonce[33];ph_nonce(nonce);
   {const FtField hidden[]={{"facet",facet->id},{"other",row->subject},{"nonce",nonce}};
    static const FtInput msg[]={{.label="Message",.name="body",.maxlength="1000",.required=1,.rows=3}};
    const FtForm f={.id="ph-message",.action="/profile/connections/message",.label="Send privately",.hidden=hidden,.nhidden=3,.inputs=msg,.ninputs=1};
    ft_form(stdout,&f);}
  }else ui_text(stdout, "p", NULL, "This conversation is no longer available.");
  ui_close(stdout,"section");
 }
 if(row->shared_topics[0]){ui_open(stdout,"p","ph-peer-common");ui_copy(stdout,"Shared interests: ");html(row->shared_topics);ui_close(stdout,"p");}
 ui_open(stdout,"div","ph-peer-actions");
 if(row->matched){
  ui_open(stdout,"p","ph-peer-status");ui_copy(stdout,"You both agreed to connect in this context. This does not grant content rights, membership, or a vote.");ui_close(stdout,"p");
  ph_peer_action(facet->id,row->subject,"none",row->my_choice_revision,"Withdraw connection");
 }else if(row->incoming){
  ui_open(stdout,"p","ph-peer-status");ui_copy(stdout,"They invited you to connect about ");
  html(facet->label);fputs(". You choose whether to accept.</p>",stdout);
  ph_peer_action(facet->id,row->subject,"accept",row->my_choice_revision,"Accept introduction");
  ph_peer_action(facet->id,row->subject,"pass",row->my_choice_revision,"Pass");
 }else if(row->mine==BF_PEER_INTEREST){
  ui_text(stdout, "p", "ph-peer-status", "Introduction sent. Conversation is not authorized yet.");
  ph_peer_action(facet->id,row->subject,"accept",row->my_choice_revision,"Confirm mutual connection");
  ph_peer_action(facet->id,row->subject,"none",row->my_choice_revision,"Withdraw interest");
 }else if(row->mine==BF_PEER_ACCEPT){
  ui_text(stdout, "p", "ph-peer-status", "Your permission is recorded. Their agreement is still required.");
  ph_peer_action(facet->id,row->subject,"none",row->my_choice_revision,"Withdraw permission");
 }else{
  ui_text(stdout, "p", "ph-peer-status", "Your introduction is private and does not create a conversation.");
  ph_peer_action(facet->id,row->subject,"interest",row->my_choice_revision,"Introduce yourself");
  ph_peer_action(facet->id,row->subject,"pass",row->my_choice_revision,"Not now");
 }
 ph_peer_action(facet->id,row->subject,"block",row->my_choice_revision,"Block across contexts");
 ui_close(stdout,"div");ui_close(stdout,"article");(void)actor;
}
static void ph_connections_view(sqlite3 *cms,const char *actor,const char *selected){
 ui_start(stdout,"section");ui_attr(stdout,"class","ph-connections");ui_flag(stdout,"data-native-peer-context");ui_gt(stdout);ui_open(stdout,"header","ph-connect-hero");ui_open(stdout,"div",NULL);ui_open(stdout,"p","ph-kicker");ui_copy(stdout,"Aurekai · Contextual participation");ui_close(stdout,"p");ui_open(stdout,"h2",NULL);ui_copy(stdout,"Meet through what matters");ui_close(stdout,"h2");ui_open(stdout,"p",NULL);ui_copy(stdout,"Your relationships change with context: people, art, communities, projects and dating. Each has its own explicit choices. Public writing, collections, and governance remain with their original owners.");ui_close(stdout,"p");ui_close(stdout,"div");ui_start(stdout,"span");ui_attr(stdout,"class","ph-connect-hero-mark");ui_attr(stdout,"aria-hidden","true");ui_gt(stdout);ui_copy(stdout,"Together");ui_close(stdout,"span");ui_close(stdout,"header");
 if(!bf_dating_subject(actor)){
  ui_start(stdout,"p");ui_attr(stdout,"class","ph-privacy");ui_attr(stdout,"role","alert");ui_gt(stdout);ui_copy(stdout,"Connect an exact person identity to your account first.");ui_close(stdout,"p");ui_close(stdout,"section");return;
 }
 sqlite3 *owner=ph_dating_open(0);
 BfPeerFacet facets[16];size_t count=0;
 int available=owner&&bf_peer_facets(owner,facets,16,&count);
 const BfPeerFacet *facet=NULL;
 for(size_t i=0;i<count&&available;i++)if(!strcmp(facets[i].id,selected))facet=&facets[i];
 ui_start(stdout,"nav");ui_attr(stdout,"class","ph-connect-contexts");ui_attr(stdout,"aria-label","Connection contexts");ui_gt(stdout);
 if(available)for(size_t i=0;i<count;i++){
  ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/profile?tab=connections&amp;facet=");url_query_value(facets[i].id);
  fputs("\"",stdout);if(!strcmp(selected,facets[i].id))fputs(" aria-current=\"page\"",stdout);
  fputs(">",stdout);html(facets[i].label);ui_close(stdout,"a");
 }
 /* Dating is its own profile app tab with its own adult and moderation owner;
  * this context list links to it and never embeds or enrolls it. */
 fputs("<a href=\"/profile?tab=dating\">Dating</a></nav>",stdout);
 if(!owner||!available){
  ui_start(stdout,"p");ui_attr(stdout,"role","status");ui_attr(stdout,"class","ph-privacy");ui_gt(stdout);ui_copy(stdout,"Private Participation is not installed here. Your profile, posts and communities remain available; no connections were inferred.");ui_close(stdout,"p");ui_close(stdout,"section");
  if(owner)sqlite3_close(owner);return;
 }
 if(!facet){
  ui_start(stdout,"p");ui_attr(stdout,"role","alert");ui_gt(stdout);ui_copy(stdout,"That context is not admitted for this account.");ui_close(stdout,"p");ui_close(stdout,"section");
  sqlite3_close(owner);return;
 }
 ui_open(stdout,"section","ph-connect-intro");ui_open(stdout,"div",NULL);ui_open(stdout,"p","ph-kicker");ui_copy(stdout,"Your selected context");ui_close(stdout,"p");ui_open(stdout,"h3",NULL);
 html(facet->label);ui_close(stdout,"h3");ui_open(stdout,"p",NULL);html(facet->description);
 ui_close(stdout,"p");ui_open(stdout,"p","ph-privacy");ui_copy(stdout,"Joining this context never enrolls you in dating, creates a public post, gives a content license, or represents a civic decision.");ui_close(stdout,"p");ui_close(stdout,"div");
 PhSite mysite;
 if(ph_peer_site(cms,actor,facet->territory,&mysite)){
  ui_start(stdout,"nav");ui_attr(stdout,"class","ph-connect-source-links");ui_attr(stdout,"aria-label","Your existing authored pages");ui_gt(stdout);ui_start(stdout,"a");ui_attr_open(stdout,"href");ui_copy(stdout,"/cms/compose/advanced?site=");url_query_value(mysite.id);
  fputs("\">Edit your published profile</a><a href=\"/cms/compose/advanced?site=",stdout);
  url_query_value(mysite.id);fputs("&amp;new=article\">Write for this site</a><a href=\"",stdout);
  ph_url(&mysite,"gallery",1);fputs("\">Review gallery</a><a href=\"",stdout);
  ph_url(&mysite,"communities",1);fputs("\">Your communities</a></nav>",stdout);
 }else{
  ui_open(stdout,"div","ph-connect-source-links");ui_open(stdout,"p",NULL);ui_copy(stdout,"Publish a profile in this context before your public work can appear in others' feeds.");ui_close(stdout,"p");ui_start(stdout,"a");ui_attr(stdout,"href","/account#presence");ui_gt(stdout);ui_copy(stdout,"Set up a profile site");ui_close(stdout,"a");ui_close(stdout,"div");
 }
 ui_close(stdout,"section");
 unsigned revision=0;int state=bf_peer_status(owner,actor,facet->id,&revision);
 if(state<0){
  ui_start(stdout,"p");ui_attr(stdout,"role","alert");ui_attr(stdout,"class","ph-privacy");ui_gt(stdout);ui_copy(stdout,"The native participation record could not be checked. No feed or actions were shown.");ui_close(stdout,"p");ui_close(stdout,"section");
  sqlite3_close(owner);return;
 }
 ui_open(stdout,"section","ph-connect-join");ui_open(stdout,"p","ph-kicker");ui_copy(stdout,"Private permission");ui_close(stdout,"p");
 if(state!=1){
  ui_open(stdout,"h3",NULL);ui_copy(stdout,"Not participating in this context");ui_close(stdout,"h3");ui_open(stdout,"p",NULL);ui_copy(stdout,"Your public profile is still public. This is a separate permission to receive introductions from other opted-in members.");ui_close(stdout,"p");
  {char r[16];snprintf(r,sizeof(r),"%u",revision);
   const FtField hidden[]={{"facet",facet->id},{"enable","1"},{"rev",r}};
   static const FtInput ack[]={{.label="Allow introductions for this context only",.type="checkbox",.name="ack",.value="1",.required=1}};
   const FtForm f={.id="ph-join",.action="/profile/connections/join",.label="Join this context",.hidden=hidden,.nhidden=3,.inputs=ack,.ninputs=1};
   ft_form(stdout,&f);}
 }else{
  ui_open(stdout,"h3",NULL);ui_copy(stdout,"You chose to participate");ui_close(stdout,"h3");ui_open(stdout,"p",NULL);ui_copy(stdout,"Introductions are private. Both people must independently agree before any connection is described as mutual.");ui_close(stdout,"p");
  {char r[16];snprintf(r,sizeof(r),"%u",revision);
   const FtField hidden[]={{"facet",facet->id},{"enable","0"},{"rev",r}};
   const FtForm f={.id="ph-leave",.action="/profile/connections/join",.label="Leave this context",.hidden=hidden,.nhidden=3};
   ft_form(stdout,&f);}
 }
 ui_close(stdout,"section");
 if(state==1){
  char topics[265];unsigned topics_revision=0;
  if(bf_peer_interests_read(owner,actor,facet->id,topics,&topics_revision)){
   fputs("<section class=\"ph-connect-interests\"><h3>Your interests in this context</h3>"
    "<p>Add up to eight interests, separated by commas. People who also joined this context see only interests you share.</p>"
    "<form method=\"post\" action=\"/profile/connections/interests\">",stdout);
   ph_hidden("facet",facet->id);printf("<input type=\"hidden\" name=\"epoch\" value=\"%u\"><input type=\"hidden\" name=\"rev\" value=\"%u\">",revision,topics_revision);
   fputs("<label>Interests <input name=\"topics\" maxlength=\"264\" value=\"",stdout);html(topics);
   fputs("\" placeholder=\"hiking, board games\"></label><button type=\"submit\">Save interests</button></form></section>",stdout);
  }
  BfPeerCandidate rows[24];size_t n=0;
  if(!bf_peer_candidates(owner,actor,facet->id,rows,24,&n))
   {ui_start(stdout,"p");ui_attr(stdout,"class","ph-privacy");ui_attr(stdout,"role","alert");ui_gt(stdout);ui_copy(stdout,"The owner cannot verify current introductions.");ui_close(stdout,"p");}
  else{
   ui_start(stdout,"section");ui_attr(stdout,"class","ph-peer-stream");ui_flag(stdout,"data-peer-feed");ui_gt(stdout);ui_open(stdout,"header",NULL);ui_open(stdout,"p","ph-kicker");ui_copy(stdout,"Source-backed member feed");ui_close(stdout,"p");ui_open(stdout,"h3",NULL);ui_copy(stdout,"People and their published work");ui_close(stdout,"h3");ui_open(stdout,"p",NULL);ui_copy(stdout,"Only members who explicitly participate in this context appear. Source titles come from published profile owners, not copied posts.");ui_close(stdout,"p");ui_close(stdout,"header");
   size_t shown=0;
   for(size_t i=0;i<n;i++){
    PhSite site;
    if(!ph_peer_site(cms,rows[i].subject,facet->territory,&site))continue;
    ph_peer_card(cms,owner,facet,&rows[i],actor);shown++;
   }
   if(!shown){
    ui_open(stdout,"div","ph-peer-empty");ui_open(stdout,"h3",NULL);ui_copy(stdout,"No introductions in this context yet");ui_close(stdout,"h3");ui_open(stdout,"p",NULL);ui_copy(stdout,"There are no other opted-in, published profiles available here. You can still explore existing public work and communities.");ui_close(stdout,"p");ui_start(stdout,"a");ui_attr(stdout,"href","/discover");ui_gt(stdout);ui_copy(stdout,"Discover published work");ui_close(stdout,"a");ui_copy(stdout," · ");ui_start(stdout,"a");ui_attr(stdout,"href","/profile?tab=communities");ui_gt(stdout);ui_copy(stdout,"Your communities");ui_close(stdout,"a");ui_close(stdout,"div");
   }
   ui_close(stdout,"section");
  }
 }
 sqlite3_close(owner);ui_close(stdout,"section");
}
static int ph_connections_mutate(const char *input,
                                 const char *raw,const char *host,const char *actor,const BfRequest *route){
 if(!bf_request_is(route,"connections_join")&&!bf_request_is(route,"connections_choice")&&!bf_request_is(route,"connections_interests")&&!bf_request_is(route,"connections_message"))return 0;
 char facet[32]="",other[192]="",choice[16]="",rev[16]="",enable[4]="",ack[4]="",topics[513]="",epoch[16]="",nonce[40]="",message[1001]="";
 PhField fields[]={{"facet",facet,sizeof(facet),0},{"other",other,sizeof(other),0},
   {"choice",choice,sizeof(choice),0},{"rev",rev,sizeof(rev),0},
   {"enable",enable,sizeof(enable),0},{"ack",ack,sizeof(ack),0},
   {"topics",topics,sizeof(topics),0},{"epoch",epoch,sizeof(epoch),0},
   {"nonce",nonce,sizeof(nonce),0},{"body",message,sizeof(message),0}};
 if(!bf_dating_subject(actor)||!ph_origin(raw,host)||!ph_fields(input,fields,10,2048)){
  ph_error(403,"A current personal session and same-origin profile form are required.\n");return 1;
 }
 sqlite3 *owner=ph_dating_open(1);
 if(!owner){ph_error(503,"Private Participation is unavailable.\n");return 1;}
 unsigned revision=0;int ok=0;
 if(!bf_request_is(route,"connections_message")&&!ph_dating_unsigned(rev,&revision))goto invalid;
 if(bf_request_is(route,"connections_join")){
  if(*other||*choice||*topics||*epoch||*nonce||*message||(strcmp(enable,"1")&&strcmp(enable,"0"))||
     (!strcmp(enable,"1")&&strcmp(ack,"1"))||
     (!strcmp(enable,"0")&&*ack))goto invalid;
  ok=bf_peer_join(owner,actor,facet,!strcmp(enable,"1"),revision);
 }else if(bf_request_is(route,"connections_interests")){
  unsigned participation=0;if(*other||*choice||*enable||*ack||*nonce||*message||!ph_dating_unsigned(epoch,&participation))goto invalid;
  ok=bf_peer_interests_set(owner,actor,facet,topics,participation,revision);
 }else if(bf_request_is(route,"connections_message")){
  if(!bf_dating_subject(other)||*choice||*enable||*ack||*topics||*epoch||*rev||strlen(nonce)!=32||strspn(nonce,"0123456789abcdef")!=32)goto invalid;
  ok=bf_peer_send(owner,actor,facet,other,nonce,message);
 }else{
  if(!bf_dating_subject(other)||*enable||*ack||*topics||*epoch||*nonce||*message)goto invalid;
  BfPeerChoice action=BF_PEER_NONE;
  if(!strcmp(choice,"interest"))action=BF_PEER_INTEREST;
  else if(!strcmp(choice,"accept"))action=BF_PEER_ACCEPT;
  else if(!strcmp(choice,"pass"))action=BF_PEER_PASS;
  else if(!strcmp(choice,"block"))action=BF_PEER_BLOCK;
  else if(strcmp(choice,"none"))goto invalid;
  ok=bf_peer_choice(owner,actor,facet,other,action,revision);
 }
 sqlite3_close(owner);
 if(!ok){ph_error(409,"That context or its permission has changed. Reload before taking another action.\n");return 1;}
 fputs("HTTP/1.1 303 See Other\r\nLocation: /profile?tab=connections&facet=",stdout);
 html(facet);fputs("\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",stdout);
 return 1;
invalid:
 sqlite3_close(owner);ph_error(400,"Choose one valid current context action.\n");return 1;
}
#endif
