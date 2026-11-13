#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include "../src/auth_continuation.h"
int main(void) {
    char out[BF_AUTH_NEXT_MAX], small[12]; size_t i, length=0; char *rendered=NULL; FILE *view;
    const char *good[]={"/", "/saved", "/o/source/012345678901234567890123?mode=compare&term=a%20b#review", "/catalogue/item%20one", "/café"};
    const char *bad[]={"//evil.test", "/\\evil.test", "https://evil.test/", "javascript:alert(1)", "/%2fevil.test", "/%5cevil", "/%252fevil", "/a/../b", "/a/%2e%2e/b", "/a/%2E", "/a/.", "/a\tb", "/a%0db", "/a%00b", "/a%zz", "/a%", "/a%2", "/a\"b", "/a?x=%0a", "https://jrss.aurekai.work:443/", "https://u@jrss.aurekai.work/", "https://jrss.aurekai.work.evil.test/"};
    for(i=0;i<sizeof(good)/sizeof(good[0]);i++){assert(bf_auth_return_address(good[i],"aurekai.life",out,sizeof(out)));assert(!strcmp(out,good[i]));}
    for(i=0;i<sizeof(bad)/sizeof(bad[0]);i++){assert(!bf_auth_return_address(bad[i],"aurekai.work",out,sizeof(out)));assert(!strcmp(out,"/account"));}
    assert(bf_auth_return_address("https://jrss.aurekai.work/report?x=1#review","aurekai.work",out,sizeof(out)));
    assert(!bf_auth_return_address("https://jrss.aurekai.work/report","aurekai.life",out,sizeof(out)));
    assert(!bf_auth_return_address("https://jrss.aurekai.work/report","aurekai.work.evil.test",out,sizeof(out)));
    assert(!bf_auth_return_address("/a-destination-that-does-not-fit","aurekai.work",small,sizeof(small)));
    assert(!strcmp(small,"/account"));
    assert(bf_auth_next_field("name=Person&next=%2Fo%2Fa%3Fx%3D1%26y%3D2%23review",out,sizeof(out))==1);
    assert(!strcmp(out,"/o/a?x=1&y=2#review"));
    assert(bf_auth_next_field("name=Person",out,sizeof(out))==0);
    const char *invalid[]={"next=/one&next=/two","next","next=%00","next=%GG","next=%","next=12345678901234567890"};
    for(i=0;i<sizeof(invalid)/sizeof(invalid[0]);i++)assert(bf_auth_next_field(invalid[i],small,sizeof(small))==-1);
    view=open_memstream(&rendered,&length);assert(view);
    assert(bf_auth_form_document(view,"<body><style>form{display:grid}</style><form id=\"f\" action=\"/signup\" method=\"post\" novalidate><input name=\"name\" required><p id=\"msg\" class=\"msg\" hidden></p></form><a href=\"/login\">Sign in</a><script>oldRedirect()</script></body>","/saved?a=1&b=2#review","Account created.","<nav>Territories</nav>"));
    assert(!fclose(view));assert(rendered&&length==strlen(rendered));
    assert(strstr(rendered,"<style>form{display:grid}</style>"));assert(strstr(rendered,"<nav>Territories</nav>"));
    assert(strstr(rendered,"name=\"next\" value=\"/saved?a=1&amp;b=2#review\""));
    assert(strstr(rendered,"href=\"/login?next=%2Fsaved%3Fa%3D1%26b%3D2%23review\""));
    assert(strstr(rendered,"role=\"status\">Account created.</p>"));assert(!strstr(rendered,"novalidate"));assert(!strstr(rendered,"oldRedirect"));free(rendered);
    /* A different institution must never inherit the old Aurekai client
     * redirect allowlist or accept a login response from an unrelated host. */
    char tenant_map[]="/tmp/bf-auth-tenant-XXXXXX";
    int fd=mkstemp(tenant_map);
    assert(fd>=0);
    FILE *map=fdopen(fd,"w");
    assert(map);
    assert(fputs("work demo.example.net login.demo.example.net\n"
                 "life people.example.net auth.people.example.net\n",map)>=0);
    assert(!fclose(map));
    assert(bf_tenant_hosts_load(tenant_map)==1);
    assert(bf_auth_return_address("/account?tab=profile","demo.example.net",out,sizeof(out)));
    assert(!strcmp(out,"/account?tab=profile"));
    assert(bf_auth_return_address("/settings","login.demo.example.net",out,sizeof(out)));
    assert(!bf_auth_return_address("/account","aurekai.work",out,sizeof(out)));
    assert(!bf_auth_return_address("/account","malicious.demo.example.net",out,sizeof(out)));
    assert(!bf_auth_return_address("https://jrss.aurekai.work/report","demo.example.net",out,sizeof(out)));
    assert(!bf_auth_return_address("https://login.demo.example.net/account","demo.example.net",out,sizeof(out)));
    assert(!bf_auth_return_address("//evil.example.net","demo.example.net",out,sizeof(out)));
    assert(bf_tenant_hosts_load("/no/such/bonfyre-tenant-map")==-1);
    /* The preceding missing-file result is -1, not fallback to zero. */
    assert(bf_tenant_hosts_status()==-1);
    assert(!bf_auth_return_address("/account","demo.example.net",out,sizeof(out)));
    unlink(tenant_map);
    puts("auth continuation native parser and form lowering: PASS");return 0;
}
