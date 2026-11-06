#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "../src/page_style_token.h"
static unsigned checks;
#define CHECK(x) do { assert(x); checks++; } while(0)
static void value(sqlite3 *db,const char *body,const char *key,const char *expected) {
    char out[512]="stale";
    int accepted=page_style_value(db,body,key,out,sizeof(out));
    if(accepted!=(expected!=NULL))fprintf(stderr,"case %u, field %s: expected %s; returned %d for %s\n",checks,key,expected?expected:"refusal",accepted,body);
    CHECK(accepted==(expected!=NULL));
    CHECK(expected?strcmp(out,expected)==0:out[0]==0);
}
int main(void) {
    sqlite3 *a=NULL,*b=NULL;
    CHECK(sqlite3_open(":memory:",&a)==SQLITE_OK);CHECK(sqlite3_open(":memory:",&b)==SQLITE_OK);
    value(a,"{\"nested\":{\"ground\":\"red\"},\"ground\":\"#123456\"}","ground","#123456");
    value(a,"{\"nested\":{\"ground\":\"red\"}}","ground",NULL);
    value(a,"{\"face_body\":\"\\\"DM Sans\\\",sans-serif\"}","face_body","\"DM Sans\",sans-serif");
    value(a,"{\"display_weight\":600}","display_weight","600");
    value(a,"{\"display_weight\":true}","display_weight",NULL);
    value(a,"{\"ground\":\"#111\",\"gro\\u0075nd\":\"#222\"}","ground",NULL);
    value(a,"{\"ground\":null}","ground",NULL);value(a,"{\"ground\":[]}","ground",NULL);
    value(a,"not-json","ground",NULL);value(a,"{}","ground",NULL);
    value(a,"{\"ground\":\"red;position:fixed\"}","ground",NULL);
    value(a,"{\"ground\":\"URL(https://example.test)\"}","ground",NULL);
    value(a,"{\"ground\":\"Expression(alert(1))\"}","ground",NULL);
    value(a,"{\"ground\":\"</style>\"}","ground",NULL);
    value(a,"{\"ground\":\"red\\u0000blue\"}","ground",NULL);
    value(a,"{\"ground\":\"red\\nblue\"}","ground",NULL);
    value(a,"{\"font_href\":\"https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;600&display=swap\"}","font_href","https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;600&display=swap");
    value(a,"{\"font_href\":\"https://example.test/font\"}","font_href",NULL);
    char small[4]="old";CHECK(!page_style_value(a,"{\"ground\":\"1234\"}","ground",small,sizeof(small)));CHECK(!small[0]);
    CHECK(sqlite3_close(a)==SQLITE_OK);a=NULL;
    value(NULL,"{\"ground\":\"red\"}","ground",NULL);
    value(b,"{\"ground\":\"blue\"}","ground","blue");
    CHECK(sqlite3_close(b)==SQLITE_OK);
    printf("page style tokens: PASS (%u checks)\n",checks);
    return 0;
}
