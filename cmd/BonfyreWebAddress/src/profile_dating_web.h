#ifndef BONFYRE_PROFILE_DATING_WEB_H
#define BONFYRE_PROFILE_DATING_WEB_H
#include "../../BonfyreParticipation/src/dating.h"
#include "profile_dating_composition.h"
/* The Life dating mode belongs to the SAME account profile, authored Life
 * site, and native Participation owner. No public profile implies opt-in.
 * Age/approval witnesses cannot be minted through these routes. */
static sqlite3 *ph_dating_open(int write){
 const char *path=getenv("BONFYRE_PARTICIPATION_DB");sqlite3 *db=NULL;
 if(!path||!*path)path=getenv("BONFYRE_DATING_DB");
 if(!path||*path!='/'||strlen(path)>4096)return NULL;
 if(sqlite3_open_v2(path,&db,write?SQLITE_OPEN_READWRITE:SQLITE_OPEN_READONLY,NULL)!=SQLITE_OK){
  if(db)sqlite3_close(db);return NULL;
 }
 sqlite3_busy_timeout(db,1200);return db;
}
static int ph_dating_unsigned(const char *s,unsigned *out){
 if(!s||!*s||strlen(s)>10||strspn(s,"0123456789")!=strlen(s)||!out)return 0;
 errno=0;char *end=NULL;unsigned long n=strtoul(s,&end,10);
 if(errno||*end||n>UINT_MAX)return 0;*out=(unsigned)n;return 1;
}
static int ph_dating_life_site(sqlite3 *cms,const char *actor,PhSite *site){
 sqlite3_stmt *s=NULL;char id[128]="";int rows=0;
 if(!cms||!site||!bf_dating_subject(actor))return 0;
 if(sqlite3_prepare_v2(cms,
   "SELECT site_id FROM site_presentation WHERE namespace='root' "
   "AND status='published' AND profile='member-profile-v1' "
   "AND territory='life' AND subject_address=?1 LIMIT 2",
   -1,&s,NULL)!=SQLITE_OK)return 0;
 sqlite3_bind_text(s,1,actor,-1,SQLITE_TRANSIENT);
 while(sqlite3_step(s)==SQLITE_ROW){
  const char *v=(const char*)sqlite3_column_text(s,0);
  if(++rows!=1||!v||strlen(v)>=sizeof(id)){rows=0;break;}
  snprintf(id,sizeof(id),"%s",v);
 }
 sqlite3_finalize(s);
 return rows==1&&ph_site(cms,NULL,id,site)&&!strcmp(site->actor,actor);
}
static void ph_dating_action(const char *other,const char *choice,unsigned revision,const char *label){
 fputs("<form class=\"ph-dating-action\" method=\"post\" action=\"/profile/dating/choice\">",stdout);
 ph_hidden("other",other);ph_hidden("choice",choice);
 printf("<input type=\"hidden\" name=\"rev\" value=\"%u\">",revision);
 fputs("<button type=\"submit\">",stdout);html(label);fputs("</button></form>",stdout);
}
static void ph_dating_own_site(sqlite3 *cms,const char *actor){
 PhSite life;
 if(!ph_dating_life_site(cms,actor,&life)){
  fputs("<p class=\"ph-privacy\">Your Life profile needs a published, exact identity binding before it can appear in introductions. <a href=\"/account#presence\">Manage your Life address</a>.</p>",stdout);
  return;
 }
 fputs("<div class=\"ph-dating-editorial\"><div><p class=\"ph-kicker\">Your authored Life profile</p><h3>",stdout);
 html(life.name);fputs("</h3><p>",stdout);html(life.bio);fputs("</p></div><nav aria-label=\"Your Life story tools\"><a href=\"/cms/compose/advanced?site=",stdout);
 url_query_value(life.id);fputs("\">Compose your profile</a><a href=\"/cms/compose/advanced?site=",stdout);
 url_query_value(life.id);fputs("&amp;new=article\">Write your story</a><a href=\"",stdout);
 ph_url(&life,"gallery",1);fputs("\">Manage your gallery</a><a href=\"https://",stdout);
 html(life.host);fputs("/\">View published Life site</a></nav></div>",stdout);
}
static void ph_dating_messages(sqlite3 *date,const char *actor,const BfDatingCandidate *c,long long before_id){
 BfDatingMessage rows[25];size_t count=0;char nonce[33];
 fputs("<section class=\"ph-dating-thread\" aria-label=\"Private conversation\"><h4>Conversation</h4>",stdout);
 if(bf_dating_messages(date,actor,c->subject,before_id,rows,25,&count)){
  if(before_id){fputs("<p class=\"ph-dating-page-link\"><a href=\"/profile?tab=dating&amp;view=chat&amp;peer=",stdout);
   url_query_value(c->subject);fputs("\">Back to latest messages</a></p>",stdout);}
  fputs("<ol class=\"ph-dating-lines\">",stdout);
  size_t visible=count>24?24:count;for(size_t i=visible;i>0;i--){
   const BfDatingMessage *m=&rows[i-1];
   fputs("<li",stdout);if(!strcmp(m->sender,actor))fputs(" class=\"ph-dating-mine\"",stdout);
   fputs("><span>",stdout);html(!strcmp(m->sender,actor)?"You":"Them");fputs("</span><p>",stdout);
   html(m->body);fputs("</p></li>",stdout);
  }
  if(!count)fputs("<li>Both of you have agreed to talk. Start with a thoughtful introduction.</li>",stdout);
  fputs("</ol>",stdout);
  if(count>24&&rows[visible-1].id>0){
   fputs("<p class=\"ph-dating-page-link\"><a href=\"/profile?tab=dating&amp;view=chat&amp;peer=",stdout);
   url_query_value(c->subject);printf("&amp;before=%lld\">Read earlier messages</a></p>",rows[visible-1].id);
  }
 }else{
  fputs("<p>The conversation is no longer authorized. No private messages were displayed.</p></section>",stdout);
  return;
 }
 ph_nonce(nonce);
 fputs("<form method=\"post\" action=\"/profile/dating/message\" class=\"ph-dating-reply\">",stdout);
 ph_hidden("other",c->subject);ph_hidden("nonce",nonce);
 fputs("<label>Message<textarea name=\"body\" maxlength=\"1000\" required rows=\"3\" placeholder=\"Say hello thoughtfully\"></textarea></label><button type=\"submit\">Send privately</button></form></section>",stdout);
}


static int ph_dating_money(const char *s,uint64_t *out){
 /* Strict USD decimal; values stay estimated and are never a money effect. */
 if(!s||!*s||strlen(s)>8||!out)return 0;
 uint64_t whole=0,fraction=0;int dot=0,digits=0,decimals=0;
 for(const char *p=s;*p;p++){
  if(*p=='.'&&!dot){dot=1;continue;}
  if(*p<'0'||*p>'9')return 0;
  if(!dot){whole=whole*10+(unsigned)(*p-'0');digits++;}
  else{if(++decimals>2)return 0;fraction=fraction*10+(unsigned)(*p-'0');}
 }
 if(!digits||whole>5000)return 0;
 if(decimals==1)fraction*=10;
 *out=whole*100+fraction;return *out<=500000;
}
static void ph_dating_plans(sqlite3 *date,const char *actor,const char *partner){
 BfDatingPlan rows[6];size_t count=0;
 fputs("<section class=\"ph-dating-plans\" aria-label=\"Plans shared with this match\">"
 "<div><p class=\"ph-kicker\">The next chapter</p><h4>Make a plan together</h4>"
 "<p>Propose an activity and cost estimate. Both people approve independently; "
 "nothing is booked, purchased, or sent to a venue.</p></div>",stdout);
 if(!bf_dating_plans(date,actor,partner,rows,6,&count)){
  fputs("<p class=\"ph-dating-state\">Current plan permissions could not be verified.</p></section>",stdout);return;
 }
 if(count)fputs("<ol class=\"ph-dating-plan-list\">",stdout);
 for(size_t i=0;i<count;i++){
  const BfDatingPlan *p=&rows[i];
  fputs("<li><div><strong>",stdout);html(*p->activity?p->activity:"Date idea");
  fputs("</strong><p>",stdout);html(p->meeting_context);
  printf("</p><small>Estimated USD $%llu.%02llu · ",
    (unsigned long long)(p->estimated_cents/100),
    (unsigned long long)(p->estimated_cents%100));
  if(p->approved_by_me&&p->approved_by_other)fputs("Both approved",stdout);
  else if(p->approved_by_me)fputs("Awaiting their approval",stdout);
  else fputs("Awaiting your approval",stdout);
  fputs("</small></div>",stdout);
  if(!p->approved_by_me){
   fputs("<form method=\"post\" action=\"/profile/dating/plan-approve\">",stdout);
   ph_hidden("plan",p->key);
   printf("<input type=\"hidden\" name=\"rev\" value=\"%u\">",p->revision);
   fputs("<button type=\"submit\">Approve this proposal</button></form>",stdout);
  }
  fputs("</li>",stdout);
 }
 if(count)fputs("</ol>",stdout);
 char id[33];ph_nonce(id);
 fputs("<form class=\"ph-dating-plan-create\" method=\"post\" action=\"/profile/dating/plan\">",stdout);
 ph_hidden("other",partner);ph_hidden("nonce",id);ph_hidden("confirm","1");
 fputs("<label>Activity or shared idea<input name=\"activity\" maxlength=\"160\" required placeholder=\"An art museum, then coffee\"></label>"
 "<label>Public meeting context (optional)<input name=\"context\" maxlength=\"120\" placeholder=\"Choose a venue together\"></label>"
 "<div class=\"ph-dating-plan-money\"><label>Estimated total (USD)<input name=\"estimate\" inputmode=\"decimal\" required placeholder=\"35.00\" maxlength=\"8\"></label>"
 "<label>Maximum budget (USD)<input name=\"budget\" inputmode=\"decimal\" required placeholder=\"60.00\" maxlength=\"8\"></label></div>"
 "<button type=\"submit\">Propose for their review</button></form></section>",stdout);
}
static void ph_dating_preferences_editor(sqlite3 *db,const char *actor){
 BfDatingAnswer prefs[4];unsigned revision=0;
 if(!bf_dating_preferences_read(db,actor,prefs,&revision))return;
 const char *questions[]={
  "An afternoon together","How to plan","Conversation rhythm","A first date"
 };
 const char *minus[]={
  "A gallery or a bookshop","Plan ahead","Short check-ins","One unhurried activity"
 };
 const char *plus[]={
  "A walk or outdoor adventure","Keep plans flexible","Longer conversations","Several things to explore"
 };
 fputs("<section class=\"ph-dating-preferences\" aria-labelledby=\"dating-preferences-heading\">"
 "<div><p class=\"ph-kicker\">Private preferences</p><h3 id=\"dating-preferences-heading\">How you like to spend time</h3>"
 "<p>Choose your own preferences. Only aggregate agreement across at least three mutually answered "
 "questions may inform your suggestions. No diagnosis, sensitive attribute, or hidden inference is created.</p></div>"
 "<form method=\"post\" action=\"/profile/dating/preferences\" class=\"ph-dating-preference-form\">",stdout);
 printf("<input type=\"hidden\" name=\"rev\" value=\"%u\">",revision);
 for(unsigned i=0;i<4;i++){
  printf("<fieldset class=\"ph-dating-preference\"><legend>%u. ",i+1);html(questions[i]);
  fputs("</legend><label>Your choice<select name=\"p",stdout);printf("%u\">",i+1);
  fputs("<option value=\"0\"",stdout);if(!prefs[i].answer)fputs(" selected",stdout);
  fputs(">Prefer not to answer</option>",stdout);
  fputs("<option value=\"-1\"",stdout);if(prefs[i].answer==-1)fputs(" selected",stdout);
  fputs(">",stdout);html(minus[i]);fputs("</option><option value=\"1\"",stdout);
  if(prefs[i].answer==1)fputs(" selected",stdout);
  fputs(">",stdout);html(plus[i]);fputs("</option></select></label>",stdout);
  printf("<label>Importance<select name=\"i%u\">",i+1);
  for(unsigned weight=1;weight<=5;weight++){
   printf("<option value=\"%u\"%s>%s</option>",weight,prefs[i].importance==weight?" selected":"",
     weight==1?"1 · Low":weight==3?"3 · Medium":weight==5?"5 · High":weight==2?"2":"4");
  }
  fputs("</select></label></fieldset>",stdout);
 }
 fputs("<button type=\"submit\">Save private preferences</button></form></section>",stdout);
}
static void ph_dating_candidate_card(sqlite3 *cms,sqlite3 *date,
                                     const char *actor,const BfDatingCandidate *candidate,const char *view,long long before_id){
 PhSite life;if(!ph_dating_life_site(cms,candidate->subject,&life))return;
 fputs("<article class=\"ph-dating-person\" data-dating-candidate",stdout);
 if(!strcmp(view,"chat"))fputs(" data-selected-peer",stdout);
 fputs(">",stdout);
 if(strcmp(view,"chat"))ph_dating_public_image(cms,&life);
 fputs("<div class=\"ph-dating-person-head\"><div>",stdout);
 fputs("<p class=\"ph-kicker\">Life profile · Reviewed participation</p><h3>",stdout);
 html(life.name);fputs("</h3><p class=\"ph-dating-dek\">",stdout);
 html(life.bio);fputs("</p>",stdout);
 if(strcmp(view,"chat"))ph_dating_public_prompt(cms,&life);
 if(candidate->shared_answers>=3){
  printf("<p class=\"ph-dating-similarity\"><strong>%u%% shared preference agreement</strong> across %u opted-in answers · Not a personality assessment</p>",
    candidate->agreement_percent,candidate->shared_answers);
 }
 fputs("</div><a href=\"https://",stdout);
 html(life.host);fputs("/\" rel=\"noopener noreferrer\">Read their Life profile</a></div>",stdout);
 if(candidate->matched){
  fputs("<p class=\"ph-dating-state\">You have both explicitly accepted the conversation.</p>",stdout);
  fputs("<p><a href=\"/profile?tab=dating&amp;view=chat&amp;peer=",stdout);url_query_value(candidate->subject);fputs("\">Open private conversation</a></p>",stdout);
  if(!strcmp(view,"matches")){
   BfDatingMessage latest[1];size_t latest_count=0;
   if(bf_dating_messages(date,actor,candidate->subject,0,latest,1,&latest_count)&&latest_count){
    fputs("<a class=\"ph-date-message-preview\" data-dating-message-preview href=\"/profile?tab=dating&amp;view=chat&amp;peer=",stdout);
    url_query_value(candidate->subject);fputs("\"><span>",stdout);
    fputs(!strcmp(latest[0].sender,actor)?"Your latest message":"Their latest message",stdout);
    fputs("</span><p>",stdout);html(latest[0].body);fputs("</p><strong>Continue conversation</strong></a>",stdout);
   }
  }
  if(!strcmp(view,"chat")){
   ph_dating_messages(date,actor,candidate,before_id);
   ph_dating_plans(date,actor,candidate->subject);
  }
 }else if(candidate->introduction_for_me){
  fputs("<p class=\"ph-dating-state\">They sent an introduction. No conversation opens until you explicitly accept.</p><div class=\"ph-dating-actions\">",stdout);
  ph_dating_action(candidate->subject,"accept",candidate->my_choice_revision,"Accept introduction");
  ph_dating_action(candidate->subject,"pass",candidate->my_choice_revision,"Pass");
  fputs("</div>",stdout);
 }else if(candidate->mine==BF_DATING_INTEREST){
  fputs("<p class=\"ph-dating-state\">Introduction sent. You can explicitly authorize a conversation if they respond.</p><div class=\"ph-dating-actions\">",stdout);
  ph_dating_action(candidate->subject,"accept",candidate->my_choice_revision,"Confirm conversation consent");
  ph_dating_action(candidate->subject,"none",candidate->my_choice_revision,"Withdraw interest");
  fputs("</div>",stdout);
 }else if(candidate->mine==BF_DATING_ACCEPT){
  fputs("<p class=\"ph-dating-state\">Your permission is recorded; they have not independently agreed.</p>",stdout);
  ph_dating_action(candidate->subject,"none",candidate->my_choice_revision,"Withdraw permission");
 }else{
  fputs("<p class=\"ph-dating-state\">An introduction asks for their attention. It cannot send an unsolicited private message.</p><div class=\"ph-dating-actions\">",stdout);
  ph_dating_action(candidate->subject,"interest",candidate->my_choice_revision,"Introduce yourself");
  ph_dating_action(candidate->subject,"pass",candidate->my_choice_revision,"Not for me");
  fputs("</div>",stdout);
 }
 fputs("<div class=\"ph-dating-safety\">",stdout);
 ph_dating_action(candidate->subject,"block",candidate->my_choice_revision,"Block this person");
 fputs("</div></article>",stdout);
}
static int ph_dating_view_allowed(const char *view){
 return view&&(!strcmp(view,"discover")||!strcmp(view,"matches")||
               !strcmp(view,"chat")||!strcmp(view,"studio"));
}
static void ph_dating_view(sqlite3 *cms,const char *actor,const char *view,const char *peer,long long before_id){
 if(!ph_dating_view_allowed(view))return;
 /* skin: aurekai-native.css (.ph-dating) */
 if(cms)bf_site_presentation_emit_scoped(stdout,cms,"aurekai-dating-editorial",".ph-dating");
 ph_dating_shared_interaction_styles();
 PhDatingComposition copy=ph_dating_composition(cms,view);
 fputs("<section class=\"ph-dating\" data-native-dating data-bf-stitch=\"dating-editorial\" data-dating-view=\"",stdout);
 html(view);fputs("\"><header class=\"ph-dating-mast\"><div><p class=\"ph-kicker\">",stdout);
 html(copy.eyebrow);fputs("</p><h2>",stdout);html(copy.heading);
 fputs("</h2><p>",stdout);html(copy.lede);
 fputs("</p></div><div class=\"ph-dating-count\" aria-label=\"Aurekai Life participation\">Life</div></header>",stdout);
 ph_dating_view_nav(view);
 if(!bf_dating_subject(actor)){
  fputs("<p class=\"ph-privacy\" role=\"alert\">Your account needs an exact person identity to open introductions. <a href=\"/account\">Check your identity</a>.</p></section>",stdout);
  return;
 }
 if(!strcmp(view,"studio")){
  ph_dating_own_site(cms,actor);
  fputs("<p class=\"ph-privacy\">The published story uses the same GrapesJS, Puck and Editor.js composer as your other Life pages. Private questions and decisions are stored separately.</p>",stdout);
 }
 sqlite3 *date=ph_dating_open(0);
 if(!date){
  fputs("<p class=\"ph-privacy\" role=\"status\">The private Participation owner is not installed for this account destination. Nothing has been inferred from your public profile.</p></section>",stdout);
  return;
 }
 unsigned request_rev=0;int requested=bf_dating_request_status(date,actor,&request_rev);
 if(requested<0){
  fputs("<p class=\"ph-privacy\" role=\"alert\">The private participation record could not be verified. No introductions were shown.</p></section>",stdout);
  sqlite3_close(date);return;
 }
 int eligible=bf_dating_eligible(date,actor);
 fputs("<section class=\"ph-dating-join\" aria-label=\"Dating participation\"><p class=\"ph-kicker\">Your dating participation</p>",stdout);
 if(requested==0||requested==2){
  fputs("<h3>Participation is off</h3><p>A request begins a private review, not approval. Adult eligibility and moderation require independent verification before discovery.</p><form method=\"post\" action=\"/profile/dating/request\">",stdout);
  printf("<input type=\"hidden\" name=\"rev\" value=\"%u\">",request_rev);
  ph_hidden("enable","1");
  fputs("<label><input type=\"checkbox\" name=\"ack\" value=\"1\" required> I request adult-only dating review. This does not publish or contact anyone.</label><button type=\"submit\">Request eligibility review</button></form>",stdout);
 }else{
  fputs(eligible?"<h3>Participation approved</h3><p>Only explicitly permitted introductions are shown.</p>":
     "<h3>Review requested</h3><p>The request is private. You cannot self-certify eligibility or moderation.</p>",stdout);
  fputs("<form method=\"post\" action=\"/profile/dating/request\">",stdout);
  printf("<input type=\"hidden\" name=\"rev\" value=\"%u\">",request_rev);
  ph_hidden("enable","0");fputs("<button type=\"submit\">Withdraw dating participation</button></form>",stdout);
 }
 fputs("</section>",stdout);
 if(eligible){
  if(!strcmp(view,"studio"))ph_dating_preferences_editor(date,actor);
  else if(!strcmp(view,"discover"))
   fputs("<p class=\"ph-privacy\">Your <a href=\"/profile?tab=dating&amp;view=studio\">private preferences</a> are compared only when both members have answered enough questions. Skipped answers are never inferred.</p>",stdout);
  if(strcmp(view,"studio")){
   BfDatingCandidate candidates[24];size_t count=0,shown=0;
   if(bf_dating_candidates(date,actor,candidates,24,&count)){
    fputs("<header class=\"ph-date-screen-head\"><div><p class=\"ph-date-eyebrow\">Current, mutual permissions</p><h3>",stdout);
    fputs(!strcmp(view,"discover")?"Meet the members":!strcmp(view,"matches")?"Connections and introductions":"Private conversation",stdout);
    fputs("</h3></div><p>Published profile content comes from each member's own CMS. All contact requires current, reciprocal consent.</p></header><div class=\"ph-dating-collection\" data-dating-collection>",stdout);
    for(size_t i=0;i<count;i++){
     const BfDatingCandidate *c=&candidates[i];
     if(!strcmp(view,"matches")&&!(c->matched||c->introduction_for_me))continue;
     if(!strcmp(view,"chat")&&(!c->matched||(*peer&&strcmp(c->subject,peer))||(!*peer&&shown)))continue;
     PhSite life;if(!ph_dating_life_site(cms,c->subject,&life))continue;
     shown++;ph_dating_candidate_card(cms,date,actor,c,view,before_id);
    }
    if(!shown){
     fputs("<div class=\"ph-empty\"><h3>",stdout);
     fputs(!strcmp(view,"chat")?"No authorized conversation":
       !strcmp(view,"matches")?"No mutual introductions yet":"No eligible published profiles at the moment",stdout);
     fputs("</h3><p>Nothing is fabricated. You can still explore members' public work independently.</p><a href=\"/discover\">Explore publications</a></div>",stdout);
    }
    fputs("</div>",stdout);
   }else fputs("<p class=\"ph-privacy\" role=\"alert\">Current eligibility could not be confirmed. No candidates were displayed.</p>",stdout);
  }
 }
 sqlite3_close(date);
 fputs("<p class=\"ph-date-privacy-note\"><strong>Consent:</strong> Public stories and gallery media stay CMS-owned. Private preferences, messages and approvals stay in Participation. No plan is a booking.</p></section>",stdout);
}
static int ph_dating_mutate(const char *target,const char *input,const char *raw,
                            const char *host,const char *actor){
 if(strncmp(target,"/profile/dating/",16))return 0;
 char other[192]="",choice[20]="",revision[16]="",nonce[40]="",body[1001]="",enable[4]="",ack[4]="";
 char p[4][4]={{0}},importance[4][4]={{0}};
 char activity[161]="",context[121]="",estimate[10]="",budget[10]="",plan[85]="",confirm[4]="";
 PhField fields[]={{"other",other,sizeof(other),0},{"choice",choice,sizeof(choice),0},
   {"rev",revision,sizeof(revision),0},{"nonce",nonce,sizeof(nonce),0},
   {"body",body,sizeof(body),1},{"enable",enable,sizeof(enable),0},{"ack",ack,sizeof(ack),0},
   {"p1",p[0],sizeof(p[0]),0},{"p2",p[1],sizeof(p[1]),0},
   {"p3",p[2],sizeof(p[2]),0},{"p4",p[3],sizeof(p[3]),0},
   {"i1",importance[0],sizeof(importance[0]),0},{"i2",importance[1],sizeof(importance[1]),0},
   {"i3",importance[2],sizeof(importance[2]),0},{"i4",importance[3],sizeof(importance[3]),0},
   {"activity",activity,sizeof(activity),0},{"context",context,sizeof(context),0},
   {"estimate",estimate,sizeof(estimate),0},{"budget",budget,sizeof(budget),0},
   {"plan",plan,sizeof(plan),0},{"confirm",confirm,sizeof(confirm),0}};
 if(!bf_dating_subject(actor)){ph_error(403,"A verified person identity is required.\n");return 1;}
 if(!ph_origin(raw,host)||!ph_fields(input,fields,21,3000)){
  ph_error(403,"Submit the current private profile form from its own site.\n");return 1;
 }
 unsigned expected=0;int ok=0;
 sqlite3 *date=ph_dating_open(1);
 if(!date){ph_error(503,"Private participation is unavailable. No change was made.\n");return 1;}
 int has_preferences=0;for(int i=0;i<4;i++)has_preferences|=*p[i]||*importance[i];
 int has_plan=*activity||*context||*estimate||*budget||*plan||*confirm;
 if(!strcmp(target,"/profile/dating/request")){
  if(has_preferences||has_plan||*other||*choice||*nonce||*body||
    (!strcmp(enable,"1")&&strcmp(ack,"1"))||
    (strcmp(enable,"0")&&strcmp(enable,"1"))||
    !ph_dating_unsigned(revision,&expected))goto invalid;
  ok=bf_dating_request(date,actor,!strcmp(enable,"1"),expected);
 }else if(!strcmp(target,"/profile/dating/choice")){
  if(has_preferences||has_plan||!bf_dating_subject(other)||*nonce||*body||*enable||*ack||
     !ph_dating_unsigned(revision,&expected))goto invalid;
  BfDatingChoice decision=BF_DATING_NONE;
  if(!strcmp(choice,"interest"))decision=BF_DATING_INTEREST;
  else if(!strcmp(choice,"accept"))decision=BF_DATING_ACCEPT;
  else if(!strcmp(choice,"pass"))decision=BF_DATING_PASS;
  else if(!strcmp(choice,"block"))decision=BF_DATING_BLOCK;
  else if(strcmp(choice,"none"))goto invalid;
  ok=bf_dating_choice(date,actor,other,decision,expected);
 }else if(!strcmp(target,"/profile/dating/message")){
  if(has_preferences||has_plan||!bf_dating_subject(other)||*choice||*revision||*enable||*ack||
    strlen(nonce)!=32||strspn(nonce,"0123456789abcdef")!=32)goto invalid;
  ok=bf_dating_send(date,actor,other,nonce,body);
 }else if(!strcmp(target,"/profile/dating/preferences")){
  if(has_plan||*other||*choice||*nonce||*body||*enable||*ack||
     !ph_dating_unsigned(revision,&expected)||!expected)goto invalid;
  BfDatingAnswer answers[4];
  for(unsigned i=0;i<4;i++){
   if(strcmp(p[i],"0")&&strcmp(p[i],"-1")&&strcmp(p[i],"1"))goto invalid;
   if(strlen(importance[i])!=1||importance[i][0]<'1'||importance[i][0]>'5')goto invalid;
   answers[i]=(BfDatingAnswer){i+1,strcmp(p[i],"0")?(*p[i]=='-'?-1:1):0,
                                (unsigned)(importance[i][0]-'0')};
  }
  ok=bf_dating_preferences(date,actor,answers,expected);
 }else if(!strcmp(target,"/profile/dating/plan")){
  uint64_t cents=0,max=0;
  if(has_preferences||!bf_dating_subject(other)||*choice||*revision||*body||
    *enable||*ack||*plan||strcmp(confirm,"1")||
    strlen(nonce)!=32||strspn(nonce,"0123456789abcdef")!=32||
    !ph_dating_money(estimate,&cents)||!ph_dating_money(budget,&max))goto invalid;
  ok=bf_dating_propose_detail(date,actor,other,nonce,activity,context,cents,max);
 }else if(!strcmp(target,"/profile/dating/plan-approve")){
  int mutual=0;
  if(has_preferences||*other||*choice||*nonce||*body||*enable||*ack||
    *activity||*context||*estimate||*budget||*confirm||
    !ph_dating_unsigned(revision,&expected)||!expected)goto invalid;
  ok=bf_dating_approve(date,actor,plan,expected,&mutual);
 }else goto invalid;
 sqlite3_close(date);
 if(!ok){ph_error(409,"Private participation changed or is no longer permitted. Reload your current profile before trying again.\n");return 1;}
 if((!strcmp(target,"/profile/dating/message")||!strcmp(target,"/profile/dating/plan"))&&bf_dating_subject(other)){
  char next[330];snprintf(next,sizeof(next),"/profile?tab=dating&view=chat&peer=%s",other);
  page_redirect(next);return 1;
 }
 if(!strcmp(target,"/profile/dating/preferences"))page_redirect("/profile?tab=dating&view=studio");
 else if(!strcmp(target,"/profile/dating/choice")||!strcmp(target,"/profile/dating/plan-approve"))page_redirect("/profile?tab=dating&view=matches");
 else page_redirect("/profile?tab=dating");return 1;
invalid:
 sqlite3_close(date);ph_error(400,"Choose one valid current dating action.\n");return 1;
}
#endif
