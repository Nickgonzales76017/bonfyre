#ifndef BONFYRE_PROFILE_CONNECTIONS_WEB_H
#define BONFYRE_PROFILE_CONNECTIONS_WEB_H
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
   fputs("<div class=\"ph-peer-source\"><small>From their published site</small><p>",stdout);
   html(title);fputs("</p><time>",stdout);html(time?time:"");
   fputs("</time> <a rel=\"noopener noreferrer\" href=\"https://",stdout);
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
 fputs("<article class=\"ph-peer-item\" data-peer-candidate><div class=\"ph-peer-item-head\">"
 "<span class=\"ph-peer-mark\" aria-hidden=\"true\">",stdout);
 html(site.name[0]?"":"A");if(site.name[0]){char initial[2]={site.name[0],0};html(initial);}
 fputs("</span><div><p class=\"ph-kicker\">Published ",stdout);html(facet->label);
 fputs(" profile</p><h3>",stdout);html(site.name);
 fputs("</h3><p class=\"ph-peer-dek\">",stdout);html(site.bio);
 fputs("</p></div><a rel=\"noopener noreferrer\" href=\"https://",stdout);
 html(site.host);fputs("/\">View profile</a></div>",stdout);
 fputs("<nav class=\"ph-peer-context-links\" aria-label=\"Source-owned publications\"><a href=\"https://",stdout);
 html(site.host);
 fputs(!strcmp(facet->id,"art")?"/?tab=gallery\">Browse published art</a>":
       !strcmp(facet->id,"civic")?"/?tab=communities\">View public communities</a>":
       !strcmp(facet->id,"projects")?"/?tab=posts\">See published work</a>":
       "/?tab=about\">Read their introduction</a>",stdout);
 fputs("</nav>",stdout);
 ph_peer_post_teaser(cms,&site);
 if(row->matched){
  BfPeerMessage messages[8];size_t count=0;
  fputs("<section class=\"ph-peer-thread\" aria-label=\"Private conversation\"><h4>Conversation</h4>",stdout);
  if(bf_peer_messages(owner,actor,facet->id,row->subject,messages,8,&count)){
   fputs("<ol>",stdout);for(size_t i=count;i>0;i--){const BfPeerMessage *m=&messages[i-1];
    fputs("<li><strong>",stdout);html(!strcmp(m->sender,actor)?"You":"Them");fputs("</strong><p>",stdout);html(m->body);fputs("</p></li>",stdout);}
   if(!count)fputs("<li>Start a conversation about this shared context.</li>",stdout);fputs("</ol>",stdout);
   char nonce[33];ph_nonce(nonce);
   {const FtField hidden[]={{"facet",facet->id},{"other",row->subject},{"nonce",nonce}};
    static const FtInput msg[]={{.label="Message",.name="body",.maxlength="1000",.required=1,.rows=3}};
    const FtForm f={.id="ph-message",.action="/profile/connections/message",.label="Send privately",.hidden=hidden,.nhidden=3,.inputs=msg,.ninputs=1};
    ft_form(stdout,&f);}
  }else fputs("<p>This conversation is no longer available.</p>",stdout);
  fputs("</section>",stdout);
 }
 if(row->shared_topics[0]){fputs("<p class=\"ph-peer-common\">Shared interests: ",stdout);html(row->shared_topics);fputs("</p>",stdout);}
 fputs("<div class=\"ph-peer-actions\">",stdout);
 if(row->matched){
  fputs("<p class=\"ph-peer-status\">You both agreed to connect in this context. "
        "This does not grant content rights, membership, or a vote.</p>",stdout);
  ph_peer_action(facet->id,row->subject,"none",row->my_choice_revision,"Withdraw connection");
 }else if(row->incoming){
  fputs("<p class=\"ph-peer-status\">They invited you to connect about ",stdout);
  html(facet->label);fputs(". You choose whether to accept.</p>",stdout);
  ph_peer_action(facet->id,row->subject,"accept",row->my_choice_revision,"Accept introduction");
  ph_peer_action(facet->id,row->subject,"pass",row->my_choice_revision,"Pass");
 }else if(row->mine==BF_PEER_INTEREST){
  fputs("<p class=\"ph-peer-status\">Introduction sent. Conversation is not authorized yet.</p>",stdout);
  ph_peer_action(facet->id,row->subject,"accept",row->my_choice_revision,"Confirm mutual connection");
  ph_peer_action(facet->id,row->subject,"none",row->my_choice_revision,"Withdraw interest");
 }else if(row->mine==BF_PEER_ACCEPT){
  fputs("<p class=\"ph-peer-status\">Your permission is recorded. Their agreement is still required.</p>",stdout);
  ph_peer_action(facet->id,row->subject,"none",row->my_choice_revision,"Withdraw permission");
 }else{
  fputs("<p class=\"ph-peer-status\">Your introduction is private and does not create a conversation.</p>",stdout);
  ph_peer_action(facet->id,row->subject,"interest",row->my_choice_revision,"Introduce yourself");
  ph_peer_action(facet->id,row->subject,"pass",row->my_choice_revision,"Not now");
 }
 ph_peer_action(facet->id,row->subject,"block",row->my_choice_revision,"Block across contexts");
 fputs("</div></article>",stdout);(void)actor;
}
static void ph_connections_view(sqlite3 *cms,const char *actor,const char *selected){
 fputs("<section class=\"ph-connections\" data-native-peer-context><header class=\"ph-connect-hero\">"
 "<div><p class=\"ph-kicker\">Aurekai · Contextual participation</p>"
 "<h2>Meet through what matters</h2><p>Your relationships change with context: people, art, communities, projects and dating. "
 "Each has its own explicit choices. Public writing, collections, and governance remain with their original owners.</p></div>"
 "<span class=\"ph-connect-hero-mark\" aria-hidden=\"true\">Together</span></header>",stdout);
 if(!bf_dating_subject(actor)){
  fputs("<p class=\"ph-privacy\" role=\"alert\">Connect an exact person identity to your account first.</p></section>",stdout);return;
 }
 sqlite3 *owner=ph_dating_open(0);
 BfPeerFacet facets[16];size_t count=0;
 int available=owner&&bf_peer_facets(owner,facets,16,&count);
 const BfPeerFacet *facet=NULL;
 for(size_t i=0;i<count&&available;i++)if(!strcmp(facets[i].id,selected))facet=&facets[i];
 fputs("<nav class=\"ph-connect-contexts\" aria-label=\"Connection contexts\">",stdout);
 if(available)for(size_t i=0;i<count;i++){
  fputs("<a href=\"/profile?tab=connections&amp;facet=",stdout);url_query_value(facets[i].id);
  fputs("\"",stdout);if(!strcmp(selected,facets[i].id))fputs(" aria-current=\"page\"",stdout);
  fputs(">",stdout);html(facets[i].label);fputs("</a>",stdout);
 }
 /* Dating is its own profile app tab with its own adult and moderation owner;
  * this context list links to it and never embeds or enrolls it. */
 fputs("<a href=\"/profile?tab=dating\">Dating</a></nav>",stdout);
 if(!owner||!available){
  fputs("<p role=\"status\" class=\"ph-privacy\">Private Participation is not installed here. "
   "Your profile, posts and communities remain available; no connections were inferred.</p></section>",stdout);
  if(owner)sqlite3_close(owner);return;
 }
 if(!facet){
  fputs("<p role=\"alert\">That context is not admitted for this account.</p></section>",stdout);
  sqlite3_close(owner);return;
 }
 fputs("<section class=\"ph-connect-intro\"><div><p class=\"ph-kicker\">Your selected context</p><h3>",stdout);
 html(facet->label);fputs("</h3><p>",stdout);html(facet->description);
 fputs("</p><p class=\"ph-privacy\">Joining this context never enrolls you in dating, "
  "creates a public post, gives a content license, or represents a civic decision.</p></div>",stdout);
 PhSite mysite;
 if(ph_peer_site(cms,actor,facet->territory,&mysite)){
  fputs("<nav class=\"ph-connect-source-links\" aria-label=\"Your existing authored pages\">"
  "<a href=\"/cms/compose/advanced?site=",stdout);url_query_value(mysite.id);
  fputs("\">Edit your published profile</a><a href=\"/cms/compose/advanced?site=",stdout);
  url_query_value(mysite.id);fputs("&amp;new=article\">Write for this site</a><a href=\"",stdout);
  ph_url(&mysite,"gallery",1);fputs("\">Review gallery</a><a href=\"",stdout);
  ph_url(&mysite,"communities",1);fputs("\">Your communities</a></nav>",stdout);
 }else{
  fputs("<div class=\"ph-connect-source-links\"><p>Publish a profile in this context before "
   "your public work can appear in others' feeds.</p><a href=\"/account#presence\">Set up a profile site</a></div>",stdout);
 }
 fputs("</section>",stdout);
 unsigned revision=0;int state=bf_peer_status(owner,actor,facet->id,&revision);
 if(state<0){
  fputs("<p role=\"alert\" class=\"ph-privacy\">The native participation record could not be checked. "
        "No feed or actions were shown.</p></section>",stdout);
  sqlite3_close(owner);return;
 }
 fputs("<section class=\"ph-connect-join\"><p class=\"ph-kicker\">Private permission</p>",stdout);
 if(state!=1){
  fputs("<h3>Not participating in this context</h3>"
   "<p>Your public profile is still public. This is a separate permission to receive introductions "
   "from other opted-in members.</p>",stdout);
  {char r[16];snprintf(r,sizeof(r),"%u",revision);
   const FtField hidden[]={{"facet",facet->id},{"enable","1"},{"rev",r}};
   static const FtInput ack[]={{.label="Allow introductions for this context only",.type="checkbox",.name="ack",.value="1",.required=1}};
   const FtForm f={.id="ph-join",.action="/profile/connections/join",.label="Join this context",.hidden=hidden,.nhidden=3,.inputs=ack,.ninputs=1};
   ft_form(stdout,&f);}
 }else{
  fputs("<h3>You chose to participate</h3><p>Introductions are private. Both people "
   "must independently agree before any connection is described as mutual.</p>",stdout);
  {char r[16];snprintf(r,sizeof(r),"%u",revision);
   const FtField hidden[]={{"facet",facet->id},{"enable","0"},{"rev",r}};
   const FtForm f={.id="ph-leave",.action="/profile/connections/join",.label="Leave this context",.hidden=hidden,.nhidden=3};
   ft_form(stdout,&f);}
 }
 fputs("</section>",stdout);
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
   fputs("<p class=\"ph-privacy\" role=\"alert\">The owner cannot verify current introductions.</p>",stdout);
  else{
   fputs("<section class=\"ph-peer-stream\" data-peer-feed><header><p class=\"ph-kicker\">Source-backed member feed</p>"
    "<h3>People and their published work</h3><p>Only members who explicitly participate "
    "in this context appear. Source titles come from published profile owners, not copied posts.</p></header>",stdout);
   size_t shown=0;
   for(size_t i=0;i<n;i++){
    PhSite site;
    if(!ph_peer_site(cms,rows[i].subject,facet->territory,&site))continue;
    ph_peer_card(cms,owner,facet,&rows[i],actor);shown++;
   }
   if(!shown){
    fputs("<div class=\"ph-peer-empty\"><h3>No introductions in this context yet</h3>"
     "<p>There are no other opted-in, published profiles available here. "
     "You can still explore existing public work and communities.</p>"
     "<a href=\"/discover\">Discover published work</a> · "
     "<a href=\"/profile?tab=communities\">Your communities</a></div>",stdout);
   }
   fputs("</section>",stdout);
  }
 }
 sqlite3_close(owner);fputs("</section>",stdout);
}
static int ph_connections_mutate(const char *target,const char *input,
                                 const char *raw,const char *host,const char *actor){
 if(strcmp(target,"/profile/connections/join")&&strcmp(target,"/profile/connections/choice")&&strcmp(target,"/profile/connections/interests")&&strcmp(target,"/profile/connections/message"))return 0;
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
 if(strcmp(target,"/profile/connections/message")&&!ph_dating_unsigned(rev,&revision))goto invalid;
 if(!strcmp(target,"/profile/connections/join")){
  if(*other||*choice||*topics||*epoch||*nonce||*message||(strcmp(enable,"1")&&strcmp(enable,"0"))||
     (!strcmp(enable,"1")&&strcmp(ack,"1"))||
     (!strcmp(enable,"0")&&*ack))goto invalid;
  ok=bf_peer_join(owner,actor,facet,!strcmp(enable,"1"),revision);
 }else if(!strcmp(target,"/profile/connections/interests")){
  unsigned participation=0;if(*other||*choice||*enable||*ack||*nonce||*message||!ph_dating_unsigned(epoch,&participation))goto invalid;
  ok=bf_peer_interests_set(owner,actor,facet,topics,participation,revision);
 }else if(!strcmp(target,"/profile/connections/message")){
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
