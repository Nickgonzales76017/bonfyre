/* Verification market candidate planning inside BonfyreEconomy.
 * Exact relational constraints, integer cents and bounded set cover.
 * This is NOT a vendor dispatcher, evidence owner, title verifier or payment rail.
 * There are no serialized business blobs and no second database.
 */
#define _POSIX_C_SOURCE 200809L
#include "verification_planner.h"
#include <ctype.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

enum { BF_VERIFY_MAX_NEEDS=16, BF_VERIFY_MAX_ROUTES=15 };
typedef struct {
    char id[81], kind[32], source[513];
    int weight;
} Need;
typedef struct {
    char id[81], provider[161], method[32], group[81];
    int64_t base, labor, travel;
    int documented;
    unsigned mask;
} Offer;

static int ident(const char *s, size_t cap) {
    size_t n=s?strlen(s):0;
    if(!n||n>=cap)return 0;
    for(size_t i=0;i<n;i++)if(!isalnum((unsigned char)s[i])&&
        s[i]!='-'&&s[i]!='_'&&s[i]!=':'&&s[i]!='.')return 0;
    return 1;
}
static int ref(const char *s,size_t cap) {
    size_t n=s?strlen(s):0;
    if(!n||n>=cap)return 0;
    for(size_t i=0;i<n;i++)if((unsigned char)s[i]<32||(unsigned char)s[i]>126)return 0;
    return 1;
}
static int64_t cents(const char *s) {
    if(!s||!*s)return -1;
    int64_t n=0;
    for(;*s;s++){
        if(*s<'0'||*s>'9'||n>1000000000LL)return -1;
        n=n*10+(*s-'0');
    }
    return n>1000000000LL?-1:n;
}
static int has(const char *value,const char *choices) {
    size_t len=strlen(value);
    const char *part=choices;
    while(*part) {
        const char *end=strchr(part,'|');
        size_t n=end?(size_t)(end-part):strlen(part);
        if(n==len&&!memcmp(part,value,n))return 1;
        if(!end)break;
        part=end+1;
    }
    return 0;
}
static const char *arg(int argc,char **argv,const char *key) {
    for(int i=2;i+1<argc;i++)if(!strcmp(argv[i],key))return argv[i+1];
    return NULL;
}
static int fail(const char *msg) {
    fprintf(stderr,"verification refused: %s\n",msg);
    return 2;
}
static int stmt(sqlite3 *db,sqlite3_stmt **s,const char *sql) {
    if(sqlite3_prepare_v2(db,sql,-1,s,NULL)!=SQLITE_OK)return fail(sqlite3_errmsg(db));
    return 0;
}
static int done(sqlite3 *db,sqlite3_stmt *s) {
    int ok=sqlite3_step(s)==SQLITE_DONE;
    sqlite3_finalize(s);
    return ok?0:fail(sqlite3_errmsg(db));
}
static int schema(sqlite3 *db) {
    char *e=NULL;
    const char *sql=
      "CREATE TABLE IF NOT EXISTS verification_cases("
      "case_id TEXT PRIMARY KEY,domain TEXT NOT NULL,subject_ref TEXT NOT NULL,"
      "budget_cents INTEGER NOT NULL CHECK(budget_cents>=0),"
      "state TEXT NOT NULL DEFAULT 'proposal' CHECK(state='proposal'),"
      "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP) STRICT;"
      "CREATE TABLE IF NOT EXISTS verification_needs("
      "case_id TEXT NOT NULL REFERENCES verification_cases(case_id),"
      "need_id TEXT NOT NULL,kind TEXT NOT NULL,weight INTEGER NOT NULL CHECK(weight IN(1,3,5)),"
      "source_ref TEXT NOT NULL,PRIMARY KEY(case_id,need_id)) WITHOUT ROWID,STRICT;"
      "CREATE TABLE IF NOT EXISTS verification_offers("
      "case_id TEXT NOT NULL REFERENCES verification_cases(case_id),route_id TEXT NOT NULL,"
      "provider_ref TEXT NOT NULL,method TEXT NOT NULL,"
      "base_cents INTEGER NOT NULL CHECK(base_cents>=0),"
      "labor_cents INTEGER NOT NULL CHECK(labor_cents>=0),"
      "travel_cents INTEGER NOT NULL CHECK(travel_cents>=0),"
      "visit_group TEXT NOT NULL,quote_state TEXT NOT NULL CHECK(quote_state IN('estimate','quoted')),"
      "availability TEXT NOT NULL CHECK(availability IN('unknown','conditional','ready')),"
      "quote_ref TEXT NOT NULL,availability_ref TEXT NOT NULL,"
      "PRIMARY KEY(case_id,route_id)) WITHOUT ROWID,STRICT;"
      "CREATE TABLE IF NOT EXISTS verification_coverage("
      "case_id TEXT NOT NULL,route_id TEXT NOT NULL,need_id TEXT NOT NULL,"
      "PRIMARY KEY(case_id,route_id,need_id),"
      "FOREIGN KEY(case_id,route_id) REFERENCES verification_offers(case_id,route_id),"
      "FOREIGN KEY(case_id,need_id) REFERENCES verification_needs(case_id,need_id)"
      ") WITHOUT ROWID,STRICT;";
    int rc=sqlite3_exec(db,sql,NULL,NULL,&e);
    if(rc!=SQLITE_OK){fprintf(stderr,"verification schema refused: %s\n",e?e:"invalid");sqlite3_free(e);return 2;}
    return 0;
}
static int kind_allowed(const char *k){
    return has(k,"model|serial|condition|ownership|title|lien|tax|location|availability|rights|safety|pricing|freight|identity|capacity|geometry|delivery|consent|provenance|hardware-boot");
}
static int method_eligible(const char *method,const char *kind){
    /* A seller's assertion can start a title question; it cannot answer one. */
    if(!strcmp(kind,"title"))return !strcmp(method,"licensed");
    if(!strcmp(kind,"rights")||!strcmp(kind,"consent"))return has(method,"licensed|owner");
    if(!strcmp(kind,"safety"))return has(method,"licensed|lab");
    if(!strcmp(kind,"lien"))return has(method,"licensed|desk");
    if(!strcmp(kind,"hardware-boot")||!strcmp(kind,"condition"))
        return has(method,"field|lab|remote-live");
    if(!strcmp(kind,"serial")||!strcmp(kind,"model"))
        return has(method,"owner|field|lab|remote-live");
    if(!strcmp(kind,"ownership"))return has(method,"licensed|desk|owner");
    if(!strcmp(kind,"tax"))return has(method,"licensed|desk");
    if(!strcmp(kind,"geometry"))return has(method,"desk|field|lab");
    if(!strcmp(kind,"provenance"))return has(method,"desk|owner|licensed");
    if(!strcmp(kind,"capacity"))return has(method,"partner|field|lab|remote-live");
    if(!strcmp(kind,"delivery"))return has(method,"partner|field|remote-live");
    return has(method,"desk|owner|remote-live|partner|field|licensed|lab");
}
static int case_create(sqlite3 *db,int argc,char **argv) {
    if(argc!=8||!ident(argv[4],81)||!has(argv[5],"itad|property|disaster|supply|research|service|civic|media|other")||
        !ref(argv[6],513)||cents(argv[7])<0)
        return fail("case create CASE DOMAIN SUBJECT_REF BUDGET_CENTS; typed domain and cents required");
    sqlite3_stmt *s=NULL;
    if(stmt(db,&s,"INSERT INTO verification_cases(case_id,domain,subject_ref,budget_cents) VALUES(?1,?2,?3,?4)"))return 2;
    sqlite3_bind_text(s,1,argv[4],-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(s,2,argv[5],-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(s,3,argv[6],-1,SQLITE_TRANSIENT);
    sqlite3_bind_int64(s,4,cents(argv[7]));
    if(done(db,s))return 2;
    printf("CASE\t%s\t%s\t%s\tbudget_cents=%s\tstate=proposal\n",argv[4],argv[5],argv[6],argv[7]);
    return 0;
}
static int need_add(sqlite3 *db,int argc,char **argv) {
    if(argc!=9||!ident(argv[4],81)||!ident(argv[5],81)||!kind_allowed(argv[6])||
       !has(argv[7],"1|3|5")||!ref(argv[8],513))
       return fail("need add CASE NEED KIND WEIGHT(1|3|5) SOURCE_REF");
    sqlite3_stmt *s=NULL;
    if(stmt(db,&s,"INSERT INTO verification_needs VALUES(?1,?2,?3,?4,?5)"))return 2;
    sqlite3_bind_text(s,1,argv[4],-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(s,2,argv[5],-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(s,3,argv[6],-1,SQLITE_TRANSIENT);
    sqlite3_bind_int(s,4,atoi(argv[7]));
    sqlite3_bind_text(s,5,argv[8],-1,SQLITE_TRANSIENT);
    if(done(db,s))return 2;
    printf("NEED\t%s\t%s\t%s\tweight=%s\tsource=%s\n",argv[4],argv[5],argv[6],argv[7],argv[8]);
    return 0;
}
static int offer_add(sqlite3 *db,int argc,char **argv) {
    if(argc!=16||!ident(argv[4],81)||!ident(argv[5],81)||!ref(argv[6],161)||
       !has(argv[7],"desk|owner|remote-live|partner|field|licensed|lab")||
       cents(argv[8])<0||cents(argv[9])<0||cents(argv[10])<0||
       !ident(argv[11],81)||!has(argv[12],"estimate|quoted")||
       !has(argv[13],"unknown|conditional|ready")||
       !ref(argv[14],513)||!ref(argv[15],513))
       return fail("offer add CASE ROUTE PROVIDER METHOD BASE_CENTS LABOR_CENTS TRAVEL_CENTS VISIT_GROUP QUOTE_STATE AVAIL_STATE QUOTE_REF AVAIL_REF");
    if((!strcmp(argv[12],"quoted")&&!strcmp(argv[14],"-"))||
       (!strcmp(argv[13],"ready")&&!strcmp(argv[15],"-")))
       return fail("quoted and ready states require explicit foreign quote and availability references");
    if(strcmp(argv[11],"-")){
        sqlite3_stmt *chk=NULL;
        if(stmt(db,&chk,"SELECT count(*) FROM verification_offers WHERE case_id=?1 AND visit_group=?2 AND provider_ref<>?3"))return 2;
        sqlite3_bind_text(chk,1,argv[4],-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(chk,2,argv[11],-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(chk,3,argv[6],-1,SQLITE_TRANSIENT);
        int conflicts=sqlite3_step(chk)==SQLITE_ROW?sqlite3_column_int(chk,0):1;
        sqlite3_finalize(chk);
        if(conflicts)return fail("a shared visit must have one exact provider identity");
    }
    sqlite3_stmt *s=NULL;
    if(stmt(db,&s,"INSERT INTO verification_offers VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)"))return 2;
    for(int i=0;i<4;i++)sqlite3_bind_text(s,i+1,argv[i+4],-1,SQLITE_TRANSIENT);
    for(int i=0;i<3;i++)sqlite3_bind_int64(s,i+5,cents(argv[i+8]));
    for(int i=0;i<5;i++)sqlite3_bind_text(s,i+8,argv[i+11],-1,SQLITE_TRANSIENT);
    if(done(db,s))return 2;
    printf("OFFER\t%s\t%s\t%s\t%s\tquote=%s\tavailability=%s\n",
        argv[4],argv[5],argv[6],argv[7],argv[12],argv[13]);
    return 0;
}
static int covers_add(sqlite3 *db,int argc,char **argv) {
    if(argc!=7||!ident(argv[4],81)||!ident(argv[5],81)||!ident(argv[6],81))
        return fail("covers add CASE ROUTE NEED");
    sqlite3_stmt *s=NULL;
    if(stmt(db,&s,"SELECT v.method,n.kind FROM verification_offers v JOIN verification_needs n ON n.case_id=v.case_id WHERE v.case_id=?1 AND v.route_id=?2 AND n.need_id=?3"))return 2;
    sqlite3_bind_text(s,1,argv[4],-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(s,2,argv[5],-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(s,3,argv[6],-1,SQLITE_TRANSIENT);
    int ok=0;
    if(sqlite3_step(s)==SQLITE_ROW) {
        const char *method=(const char*)sqlite3_column_text(s,0);
        const char *kind=(const char*)sqlite3_column_text(s,1);
        ok=method&&kind&&method_eligible(method,kind);
    }
    sqlite3_finalize(s);
    if(!ok)return fail("route cannot establish that proof kind; exact method or referenced requirement missing");
    if(stmt(db,&s,"INSERT INTO verification_coverage VALUES(?1,?2,?3)"))return 2;
    sqlite3_bind_text(s,1,argv[4],-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(s,2,argv[5],-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(s,3,argv[6],-1,SQLITE_TRANSIENT);
    if(done(db,s))return 2;
    printf("COVERS\t%s\t%s\t%s\tstate=proposed_not_proof\n",argv[4],argv[5],argv[6]);
    return 0;
}
static int64_t subset_cents(const Offer *v,int nr,unsigned selected) {
    int64_t cost=0;
    for(int i=0;i<nr;i++)if(selected&(1u<<i)){
        cost+=v[i].base+v[i].labor;
        if(!strcmp(v[i].group,"-"))cost+=v[i].travel;
        else {
            int earlier=0; int64_t max_travel=v[i].travel;
            for(int j=0;j<nr;j++)if(selected&(1u<<j)) {
                if(strcmp(v[j].group,v[i].group))continue;
                if(j<i)earlier=1;
                if(v[j].travel>max_travel)max_travel=v[j].travel;
            }
            if(!earlier)cost+=max_travel;
        }
    }
    return cost;
}
static int verify_plan(sqlite3 *db,int argc,char **argv) {
    if(argc<4||!ident(argv[3],81))return fail("plan CASE [--mode exploratory|documented] [--budget-cents INTEGER]");
    const char *mode=arg(argc,argv,"--mode");
    if(!mode)mode="exploratory";
    if(!has(mode,"exploratory|documented"))return fail("plan mode must be exploratory or documented");
    sqlite3_stmt *st=NULL; Need needs[BF_VERIFY_MAX_NEEDS]={0};
    Offer offers[BF_VERIFY_MAX_ROUTES]={0};int nn=0,nr=0;int64_t budget=0;
    char domain[40]={0},subject[513]={0};
    if(stmt(db,&st,"SELECT domain,subject_ref,budget_cents FROM verification_cases WHERE case_id=?1"))return 2;
    sqlite3_bind_text(st,1,argv[3],-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)!=SQLITE_ROW){sqlite3_finalize(st);return fail("case not found");}
    snprintf(domain,sizeof(domain),"%s",sqlite3_column_text(st,0));
    snprintf(subject,sizeof(subject),"%s",sqlite3_column_text(st,1));
    budget=sqlite3_column_int64(st,2);sqlite3_finalize(st);st=NULL;
    const char *override=arg(argc,argv,"--budget-cents");
    if(override){budget=cents(override);if(budget<0)return fail("invalid nonnegative budget in integer cents");}
    if(stmt(db,&st,"SELECT need_id,kind,weight,source_ref FROM verification_needs WHERE case_id=?1 ORDER BY need_id"))return 2;
    sqlite3_bind_text(st,1,argv[3],-1,SQLITE_TRANSIENT);
    while(sqlite3_step(st)==SQLITE_ROW){
        if(nn>=BF_VERIFY_MAX_NEEDS){sqlite3_finalize(st);return fail("case exceeds 16 requirements; decompose into linked bounded cases");}
        snprintf(needs[nn].id,sizeof(needs[nn].id),"%s",sqlite3_column_text(st,0));
        snprintf(needs[nn].kind,sizeof(needs[nn].kind),"%s",sqlite3_column_text(st,1));
        needs[nn].weight=sqlite3_column_int(st,2);
        snprintf(needs[nn].source,sizeof(needs[nn].source),"%s",sqlite3_column_text(st,3));
        nn++;
    }
    sqlite3_finalize(st);st=NULL;
    if(!nn)return fail("case has no typed evidence requirements");
    if(stmt(db,&st,"SELECT route_id,provider_ref,method,base_cents,labor_cents,travel_cents,visit_group,"
                 "quote_state,availability,quote_ref,availability_ref FROM verification_offers WHERE case_id=?1 ORDER BY route_id"))return 2;
    sqlite3_bind_text(st,1,argv[3],-1,SQLITE_TRANSIENT);
    while(sqlite3_step(st)==SQLITE_ROW){
        if(nr>=BF_VERIFY_MAX_ROUTES){sqlite3_finalize(st);return fail("case exceeds 15 offers; decompose a location/provider frontier");}
        Offer *o=&offers[nr++];
        snprintf(o->id,sizeof(o->id),"%s",sqlite3_column_text(st,0));
        snprintf(o->provider,sizeof(o->provider),"%s",sqlite3_column_text(st,1));
        snprintf(o->method,sizeof(o->method),"%s",sqlite3_column_text(st,2));
        o->base=sqlite3_column_int64(st,3);
        o->labor=sqlite3_column_int64(st,4);
        o->travel=sqlite3_column_int64(st,5);
        snprintf(o->group,sizeof(o->group),"%s",sqlite3_column_text(st,6));
        const char *quote=(const char*)sqlite3_column_text(st,7);
        const char *ready=(const char*)sqlite3_column_text(st,8);
        const char *quote_ref=(const char*)sqlite3_column_text(st,9);
        const char *availability_ref=(const char*)sqlite3_column_text(st,10);
        o->documented=quote&&ready&&quote_ref&&availability_ref&&!strcmp(quote,"quoted")&&
          !strcmp(ready,"ready")&&strcmp(quote_ref,"-")&&strcmp(availability_ref,"-");
    }
    sqlite3_finalize(st);st=NULL;
    if(stmt(db,&st,"SELECT route_id,need_id FROM verification_coverage WHERE case_id=?1"))return 2;
    sqlite3_bind_text(st,1,argv[3],-1,SQLITE_TRANSIENT);
    while(sqlite3_step(st)==SQLITE_ROW){
        const char *rid=(const char*)sqlite3_column_text(st,0);
        const char *nid=(const char*)sqlite3_column_text(st,1);
        for(int i=0;i<nr;i++)if(!strcmp(offers[i].id,rid))
            for(int j=0;j<nn;j++)if(!strcmp(needs[j].id,nid))offers[i].mask|=1u<<j;
    }
    sqlite3_finalize(st);
    unsigned full=(1u<<nn)-1u,best_plan=0,best_partial=0;
    int64_t cheapest_full=INT64_MAX,best_partial_cost=INT64_MAX;
    int64_t best_cost=INT64_MAX;int best_weight=-1;
    unsigned best_coverage=0; int cheapest_full_found=0,within_budget_found=0;
    for(unsigned selected=0;selected<(1u<<nr);selected++){
        unsigned coverage=0;int count=0,permitted=1;
        for(int i=0;i<nr;i++)if(selected&(1u<<i)){
            if(!strcmp(mode,"documented")&&!offers[i].documented){permitted=0;break;}
            coverage|=offers[i].mask;count++;
        }
        if(!permitted)continue;
        int64_t cost=subset_cents(offers,nr,selected);
        if(coverage==full){
            if(cost<cheapest_full){cheapest_full=cost;cheapest_full_found=1;}
            if(cost<=budget&&(cost<best_cost||(cost==best_cost&&
                __builtin_popcount(selected)<__builtin_popcount(best_plan)))){
                best_cost=cost;best_plan=selected;within_budget_found=1;
            }
        }
        if(cost>budget)continue;
        int weight=0;
        for(int i=0;i<nn;i++)if(coverage&(1u<<i))weight+=needs[i].weight;
        if(weight>best_weight||(weight==best_weight&&cost<best_partial_cost)){
            best_weight=weight;best_partial_cost=cost;best_partial=selected;best_coverage=coverage;
        }
        (void)count;
    }
    unsigned chosen=within_budget_found?best_plan:best_partial;
    unsigned covered=within_budget_found?full:best_coverage;
    int64_t cost=within_budget_found?best_cost:(best_weight>=0?best_partial_cost:0);
    const char *verdict=within_budget_found?"candidate_full":
        cheapest_full_found?"over_budget":best_weight>=0?"incomplete":"no_candidate";
    printf("VERIFICATION_CASE\t%s\tdomain=%s\tsubject=%s\n",argv[3],domain,subject);
    printf("PLAN\tmode=%s\tstatus=%s\tbudget_cents=%lld\tplanned_cents=%lld\n",
        mode,verdict,(long long)budget,(long long)cost);
    int documented_count=0;
    for(int k=0;k<nr;k++)if(offers[k].documented)documented_count++;
    printf("COVERAGE\t%d/%d\tquoted_ready_routes=%d\n",__builtin_popcount(covered),
        nn,documented_count);
    if(cheapest_full_found)printf("LOWEST_FULL_COST_CENTS\t%lld\n",(long long)cheapest_full);
    for(int i=0;i<nr;i++)if(chosen&(1u<<i))
        printf("ROUTE\t%s\tprovider=%s\tmethod=%s\tbasis=%s\tvisit_group=%s\n",
            offers[i].id,offers[i].provider,offers[i].method,
            offers[i].documented?"documented_quote_reference":"exploratory_quote_or_availability",
            offers[i].group);
    for(int j=0;j<nn;j++)if(!(covered&(1u<<j)))
        printf("UNRESOLVED\t%s\tkind=%s\tweight=%d\tsource=%s\n",
            needs[j].id,needs[j].kind,needs[j].weight,needs[j].source);
    puts("AUTHORITY\tnone; a proposal does not verify evidence, accept a quote, dispatch a worker, certify title, settle funds or establish a provider relationship");
    return within_budget_found?0:3;
}
/* Atomic, idempotent source -> native verification requirements.
 * argv: verify intake CASE DOMAIN SUBJECT BUDGET_CENTS [NEED KIND WEIGHT SOURCE]...
 * No quotation is invented. All values and the replay set are independently
 * checked, and every requirement is committed in one native owner transaction.
 */
static int verify_intake(sqlite3 *db,int argc,char **argv){
    const int count=(argc-7)/4;
    sqlite3_stmt *st=NULL;
    const char *case_id=argc>3?argv[3]:NULL;
    const char *domain=argc>4?argv[4]:NULL;
    const char *subject=argc>5?argv[5]:NULL;
    const char *budget=argc>6?argv[6]:NULL;
    int prior=0,ok=0,commit=0;
    if(argc<11||(argc-7)%4!=0||count<1||count>BF_VERIFY_MAX_NEEDS||
        !ident(case_id,81)||!domain||
        !has(domain,"itad|property|disaster|supply|research|service|civic|media|other")||
        !ref(subject,513)||cents(budget)<0)
        return fail("verify intake CASE DOMAIN SUBJECT BUDGET_CENTS [NEED KIND WEIGHT SOURCE]... (1..16 needs)");
    for(int i=0;i<count;i++){
        int pos=7+4*i;
        if(!ident(argv[pos],81)||!kind_allowed(argv[pos+1])||
           !has(argv[pos+2],"1|3|5")||!ref(argv[pos+3],513))
            return fail("intake requires bounded exact need ID, proof kind, weight, source owner");
        for(int j=0;j<i;j++)if(!strcmp(argv[pos],argv[7+4*j]))
            return fail("duplicate requirement identity");
    }
    if(sqlite3_exec(db,"BEGIN IMMEDIATE",NULL,NULL,NULL)!=SQLITE_OK)
        return fail("verification owner transaction unavailable");
    if(sqlite3_prepare_v2(db,
       "SELECT domain,subject_ref,budget_cents FROM verification_cases WHERE case_id=?1",
       -1,&st,NULL)!=SQLITE_OK)goto fail_tx;
    sqlite3_bind_text(st,1,case_id,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)==SQLITE_ROW){
        const char *actual_domain=(const char*)sqlite3_column_text(st,0);
        const char *actual_subject=(const char*)sqlite3_column_text(st,1);
        if(!actual_domain||!actual_subject||strcmp(actual_domain,domain)||
           strcmp(actual_subject,subject)||sqlite3_column_int64(st,2)!=cents(budget))
            goto fail_tx;
        prior=1;
    }
    sqlite3_finalize(st);st=NULL;
    if(prior){
        if(sqlite3_prepare_v2(db,
          "SELECT COUNT(*) FROM verification_needs WHERE case_id=?1",
          -1,&st,NULL)!=SQLITE_OK)goto fail_tx;
        sqlite3_bind_text(st,1,case_id,-1,SQLITE_TRANSIENT);
        if(sqlite3_step(st)!=SQLITE_ROW||sqlite3_column_int(st,0)!=count)goto fail_tx;
        sqlite3_finalize(st);st=NULL;
        if(sqlite3_prepare_v2(db,
          "SELECT kind,weight,source_ref FROM verification_needs WHERE case_id=?1 AND need_id=?2",
          -1,&st,NULL)!=SQLITE_OK)goto fail_tx;
        for(int i=0;i<count;i++){
            int pos=7+4*i;
            sqlite3_bind_text(st,1,case_id,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(st,2,argv[pos],-1,SQLITE_TRANSIENT);
            if(sqlite3_step(st)!=SQLITE_ROW||
               !sqlite3_column_text(st,0)||!sqlite3_column_text(st,2)||
               strcmp((const char*)sqlite3_column_text(st,0),argv[pos+1])||
               sqlite3_column_int(st,1)!=atoi(argv[pos+2])||
               strcmp((const char*)sqlite3_column_text(st,2),argv[pos+3]))
                goto fail_tx;
            sqlite3_reset(st);sqlite3_clear_bindings(st);
        }
        sqlite3_finalize(st);st=NULL;
    }else{
        if(sqlite3_prepare_v2(db,
          "INSERT INTO verification_cases(case_id,domain,subject_ref,budget_cents) VALUES(?1,?2,?3,?4)",
          -1,&st,NULL)!=SQLITE_OK)goto fail_tx;
        sqlite3_bind_text(st,1,case_id,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,2,domain,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(st,3,subject,-1,SQLITE_TRANSIENT);
        sqlite3_bind_int64(st,4,cents(budget));
        if(sqlite3_step(st)!=SQLITE_DONE)goto fail_tx;
        sqlite3_finalize(st);st=NULL;
        if(sqlite3_prepare_v2(db,
          "INSERT INTO verification_needs(case_id,need_id,kind,weight,source_ref) VALUES(?1,?2,?3,?4,?5)",
          -1,&st,NULL)!=SQLITE_OK)goto fail_tx;
        for(int i=0;i<count;i++){
            int pos=7+4*i;
            sqlite3_bind_text(st,1,case_id,-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(st,2,argv[pos],-1,SQLITE_TRANSIENT);
            sqlite3_bind_text(st,3,argv[pos+1],-1,SQLITE_TRANSIENT);
            sqlite3_bind_int(st,4,atoi(argv[pos+2]));
            sqlite3_bind_text(st,5,argv[pos+3],-1,SQLITE_TRANSIENT);
            if(sqlite3_step(st)!=SQLITE_DONE)goto fail_tx;
            sqlite3_reset(st);sqlite3_clear_bindings(st);
        }
        sqlite3_finalize(st);st=NULL;
    }
    if(sqlite3_exec(db,"COMMIT",NULL,NULL,NULL)!=SQLITE_OK)goto fail_tx;
    commit=1;
    if(sqlite3_prepare_v2(db,
      "SELECT COUNT(*) FROM verification_needs WHERE case_id=?1",
      -1,&st,NULL)!=SQLITE_OK)goto fail_tx;
    sqlite3_bind_text(st,1,case_id,-1,SQLITE_TRANSIENT);
    ok=sqlite3_step(st)==SQLITE_ROW&&sqlite3_column_int(st,0)==count;
    sqlite3_finalize(st);st=NULL;
    if(!ok)return fail("owner readback differs from admitted source requirements");
    printf("CASE\\t%s\\tdomain=%s\\tsubject=%s\\n",case_id,domain,subject);
    printf("INTAKE\\t%s\\tcount=%d\\tbudget_cents=%s\\towner_readback=exact\\n",
      prior?"unchanged":"created",count,budget);
    puts("AUTHORITY\\tnone; material review is not quote, proof, payment or authorized dispatch");
    return 0;
fail_tx:
    if(st)sqlite3_finalize(st);
    if(!commit)sqlite3_exec(db,"ROLLBACK",NULL,NULL,NULL);
    return fail("source requirements conflict with existing native case or could not commit atomically");
}
int bf_verification_command(sqlite3 *db,int argc,char **argv){
    enum { VF_VERIFY };
    static const BfCommandEntry verify_commands[] = {
        {"verify", VF_VERIFY, 0, NULL},
    };
    const BfCommandEntry *command = (!db||argc<3) ? NULL : bf_command_lookup(verify_commands,
        sizeof(verify_commands) / sizeof(verify_commands[0]), argv[1]);
    if(!command||command->id!=VF_VERIFY)return fail("verify case|need|offer|covers|plan");
    if(schema(db))return 2;
    if(!strcmp(argv[2],"case")&&argc>=4&&!strcmp(argv[3],"create"))return case_create(db,argc,argv);
    if(!strcmp(argv[2],"need")&&argc>=4&&!strcmp(argv[3],"add"))return need_add(db,argc,argv);
    if(!strcmp(argv[2],"offer")&&argc>=4&&!strcmp(argv[3],"add"))return offer_add(db,argc,argv);
    if(!strcmp(argv[2],"covers")&&argc>=4&&!strcmp(argv[3],"add"))return covers_add(db,argc,argv);
    if(!strcmp(argv[2],"plan"))return verify_plan(db,argc,argv);
    if(!strcmp(argv[2],"intake"))return verify_intake(db,argc,argv);
    return fail("verify case create|need add|offer add|covers add|plan");
}
