#define _POSIX_C_SOURCE 200809L
#include "campaign.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static unsigned checks;
#define CHECK(expr) do { checks++; if (!(expr)) { fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #expr); exit(1); } } while (0)
static bf_jsonw answer;
static void field(bf_jsonw *w, const char *key, const char *value) { bf_jsonw_key(w,key); bf_jsonw_str(w,value); }
static int raw(const char *s) { return bf_campaign_request(s, strlen(s), &answer); }
static int request(const char *operation, const char *url, const char *source) {
    bf_jsonw w; bf_jsonw_init(&w, 0); bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "version"); bf_jsonw_int(&w, 1);
    field(&w, "operation", operation); field(&w, "url", url);
    if (source) {
        bf_jsonw_key(&w, "utm"); bf_jsonw_obj(&w);
        field(&w, "utm_source", source); field(&w, "utm_medium", "email"); field(&w, "utm_campaign", "Autumn / 2026");
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_obj_end(&w);
    int result = bf_campaign_request(w.buf, w.len, &answer);
    bf_jsonw_free(&w); return result;
}
static void value(const char *path, char *dest, size_t cap) {
    bf_json_doc_t *doc = bf_json_parse(answer.buf, answer.len, NULL, 0);
    CHECK(doc != NULL);
    const bf_json_node_t *n = bf_json_dotpath(doc, path);
    CHECK(n && n->type == BF_JSON_STRING);
    size_t length; char *s = bf_json_unescape(n->val.str.ptr, n->val.str.len, &length);
    CHECK(s && length < cap); memcpy(dest, s, length); dest[length] = 0;
    free(s); bf_json_free(doc);
}
static void equals(const char *path, const char *expected) {
    char got[BF_CAMPAIGN_URL_MAX + 1]; value(path, got, sizeof(got)); CHECK(!strcmp(got, expected));
}
static void error(const char *code) { equals("error", code); CHECK(!strstr(answer.buf, "\"url\"")); }
static void roundtrip(void) {
    const char *build = "{\"version\":1,\"operation\":\"build\",\"url\":\"https://Example.com:443/路径?a=%2F&b=one+two&a=2#章?utm_source=fragment\",\"utm\":{\"utm_content\":\"top & bottom\",\"utm_term\":\"雪 + café\",\"utm_id\":\"C-2026\",\"utm_campaign\":\"Autumn / 2026\",\"utm_medium\":\"email\",\"utm_source\":\"News Letter\"}}";
    CHECK(raw(build) == 0);
    equals("tagging", "complete"); equals("provenance", "authored_coordinates");
    char url[BF_CAMPAIGN_URL_MAX + 1]; value("url", url, sizeof(url));
    CHECK(!strcmp(url, "https://Example.com:443/路径?a=%2F&b=one+two&a=2&utm_source=News%20Letter&utm_medium=email&utm_campaign=Autumn%20%2F%202026&utm_id=C-2026&utm_term=%E9%9B%AA%20%2B%20caf%C3%A9&utm_content=top%20%26%20bottom#章?utm_source=fragment"));
    CHECK(request("parse", url, NULL) == 0);
    equals("url", url); equals("tagging", "complete"); equals("provenance", "untrusted_url_coordinates");
    equals("utm.utm_source", "News Letter"); equals("utm.utm_medium", "email"); equals("utm.utm_campaign", "Autumn / 2026");
    equals("utm.utm_id", "C-2026"); equals("utm.utm_term", "雪 + café"); equals("utm.utm_content", "top & bottom");
    CHECK(!strstr(answer.buf, "actor")); CHECK(strstr(answer.buf, "\"external_effect_executed\":false"));
    CHECK(raw(build) == 0); char *first = strdup(answer.buf); CHECK(first);
    CHECK(raw(build) == 0); CHECK(!strcmp(first, answer.buf)); free(first);
}
static void parsing(void) {
    CHECK(request("parse", "https://example.com/page#utm_source=fragment", NULL) == 0); equals("tagging", "absent");
    CHECK(request("validate", "http://example.com?utm_id=id&x=1#keep", NULL) == 0); equals("tagging", "partial"); equals("utm.utm_id", "id");
    CHECK(request("parse", "https://example.com/?utm%5Fsource=News+Letter&utm_medium=email&utm_campaign=a%26b", NULL) == 0);
    equals("utm.utm_source", "News Letter"); equals("utm.utm_campaign", "a&b");
    CHECK(request("parse", "https://example.com/?utm_source=%2526%2B&utm_id=%F0%9F%94%A5", NULL) == 0);
    equals("utm.utm_source", "%26+"); equals("utm.utm_id", "🔥");
    CHECK(request("parse", "https://example.com/?utm_source=Google", NULL) == 0); equals("utm.utm_source", "Google");
    CHECK(request("parse", "https://example.com/?utm_source=a&utm%5fsource=a", NULL) == 2); error("duplicate_utm");
    CHECK(request("parse", "https://example.com/?utm_source=a&utm_source=", NULL) == 2); error("duplicate_utm");
    CHECK(request("parse", "https://example.com/?UTM_Source=a", NULL) == 2); error("noncanonical_utm_key");
    CHECK(request("parse", "https://example.com/?utm_source", NULL) == 2); error("invalid_utm_value");
    CHECK(request("parse", "https://example.com/?utm_source=", NULL) == 2); error("invalid_utm_value");
    const char *base = "https://example.com/?utm_source_platform=manual&gclid=opaque&x=1&x=2&#part";
    CHECK(request("build", base, "newsletter") == 0);
    char url[BF_CAMPAIGN_URL_MAX + 1]; value("url", url, sizeof(url));
    CHECK(strstr(url,"?utm_source_platform=manual&gclid=opaque&x=1&x=2&utm_source=newsletter")); CHECK(strstr(url,"#part"));
    CHECK(request("build", "https://example.com/?#frag", "newsletter") == 0);
    value("url", url, sizeof(url)); CHECK(strstr(url, "/?utm_source=newsletter")); CHECK(strstr(url, "#frag"));
    CHECK(request("build", "https://example.com/?utm_id=old", "newsletter") == 2); error("destination_already_tagged");
    CHECK(raw("{\"version\":1,\"operation\":\"build\",\"url\":\"https://example.com\",\"utm\":{\"utm_source\":\"a\"}}") == 2); error("required_utm_missing");
}
static void hostile(void) {
    const char *urls[] = {
        "javascript:alert(1)", "file:///etc/passwd", "//example.com/x", "https://user:secret@example.com/", "https://user%40example.com/",
        "https://example.com\\@localhost/", "https://example.com/%5csecret", "https://localhost/", "https://x.localhost/", "https://x.local/",
        "https://x.internal/", "https://x.home.arpa/", "https://127.0.0.1/", "https://127.1/", "https://2130706433/", "https://0177.0.0.1/",
        "https://0x7f000001/", "https://[::1]/", "https://[2001:4860:4860::8888]/", "https://8.8.8.8/", "https://example.com./",
        "https://example..com/", "https://-example.com/", "https://example-.com/", "https://example.com:0/", "https://example.com:65536/",
        "https://example.com:443:80/", "https://example.com/%", "https://example.com/%G0", "https://example.com/%0", "https://example.com/%00",
        "https://example.com/%0d%0aX:evil", "https://example.com/?utm_source=%7F", "https://example.com/?x=%C0%AF", "https://example.com/?x=%ED%A0%80",
        "https://example.com/?x=%F4%90%80%80", "https://example.com/ white", "https://example.com/\twhite", "https://example.com/<script>",
        "https://example.com/?utm_source=ok#%GG", "https://example.com/\xFF"
    };
    for (size_t i = 0; i < sizeof(urls)/sizeof(*urls); i++) CHECK(request("parse", urls[i], NULL) == 2);
    CHECK(request("validate", "https://xn--bcher-kva.example.com:8443/a", NULL) == 0);
    const char *bad[] = {
        "{}", "[]", "{,}", "{\"version\":1,}", "{\"version\":1,,\"url\":\"x\"}", "{\"version\":1} {}",
        "{\"version\":1.0}", "{\"version\":01}", "{\"version\":1e0}", "{\"version\":true}",
        "{\"version\":1,\"operation\":\"parse\",\"url\":\"https://example.com\",\"actor\":\"invented\"}",
        "{\"version\":1,\"operation\":\"parse\",\"url\":\"https://example.com\",\"utm\":{}}",
        "{\"version\":1,\"operation\":\"parse\",\"url\":\"https://example.com\",\"u\\u0072l\":\"https://other.example.com\"}",
        "{\"version\":1,\"operation\":\"parse\",\"url\":\"https://example.com/\\q\"}",
        "{\"version\":1,\"operation\":\"parse\",\"url\":\"https://example.com/\\uD800\"}",
        "{\"version\":1,\"operation\":\"parse\",\"url\":\"https://example.com/\\u0000\"}",
        "{\"version\":1,\"operation\":\"parse\",\"url\":\"https://example.com/\n\"}",
        "{\"version\":1,\"operation\":\"build\",\"url\":\"https://example.com\",\"utm\":{\"utm_source\":\"a\",\"utm_source\":\"b\"}}",
        "{\"version\":1,\"operation\":\"build\",\"url\":\"https://example.com\",\"utm\":{\"utm_source_platform\":\"x\"}}"
    };
    for (size_t i = 0; i < sizeof(bad)/sizeof(*bad); i++) { CHECK(raw(bad[i]) == 2); error("invalid_request"); }
    CHECK(raw("{\"version\":1,\"operation\":\"parse\",\"url\":\"https://example.com/?utm_source=\\uD83D\\uDD25\"}") == 0); equals("utm.utm_source", "🔥");
}
static void bounds(void) {
    char source[BF_CAMPAIGN_VALUE_MAX + 2]; memset(source, 's', sizeof(source)); source[BF_CAMPAIGN_VALUE_MAX] = 0;
    CHECK(request("build", "https://example.com/", source) == 0);
    char url[BF_CAMPAIGN_URL_MAX + 2]; value("url", url, sizeof(url)); CHECK(request("parse", url, NULL) == 0);
    source[BF_CAMPAIGN_VALUE_MAX] = 's'; source[BF_CAMPAIGN_VALUE_MAX + 1] = 0;
    CHECK(request("build", "https://example.com/", source) == 2);
    snprintf(url,sizeof(url),"https://example.com/?utm_source=%s",source); CHECK(request("parse",url,NULL)==2); error("invalid_utm_value");
    memset(url,'a',sizeof(url)); memcpy(url,"https://example.com/",20); url[BF_CAMPAIGN_URL_MAX] = 0;
    CHECK(request("parse",url,NULL)==0); CHECK(request("build",url,"news")==2); error("generated_url_too_large");
    url[BF_CAMPAIGN_URL_MAX]='a';url[BF_CAMPAIGN_URL_MAX+1]=0;CHECK(request("parse",url,NULL)==2);
    char transport[BF_CAMPAIGN_REQUEST_MAX+1];memset(transport,' ',sizeof(transport));
    CHECK(bf_campaign_request(transport,sizeof(transport),&answer)==2);
    CHECK(bf_campaign_request("{}\0{}",5,&answer)==2);
}
static void cli(const char *binary) {
    int in[2], out[2]; CHECK(pipe(in)==0); CHECK(pipe(out)==0);
    pid_t pid = fork(); CHECK(pid>=0);
    if (!pid) {
        if (dup2(in[0],STDIN_FILENO)<0||dup2(out[1],STDOUT_FILENO)<0) _exit(126);
        close(in[0]);close(in[1]);close(out[0]);close(out[1]);execl(binary,binary,"request",(char *)NULL);_exit(127);
    }
    close(in[0]);close(out[1]);
    const char *request_text="{\"version\":1,\"operation\":\"parse\",\"url\":\"https://example.com/?utm_id=cli\"}";
    CHECK(write(in[1],request_text,strlen(request_text))==(ssize_t)strlen(request_text));close(in[1]);
    char buffer[2048];size_t n=0;ssize_t got;
    while ((got=read(out[0],buffer+n,sizeof(buffer)-n-1))>0)n+=(size_t)got;
    CHECK(got==0);close(out[0]);buffer[n]=0;
    int status;CHECK(waitpid(pid,&status,0)==pid);CHECK(WIFEXITED(status)&&WEXITSTATUS(status)==0);
    CHECK(strstr(buffer,"\"utm_id\":\"cli\""));CHECK(n&&buffer[n-1]=='\n');
}
int main(int argc,char **argv) {
    CHECK(argc==2);bf_jsonw_init(&answer,0);
    roundtrip();parsing();hostile();bounds();cli(argv[1]);bf_jsonw_free(&answer);
    printf("campaign native witness: %u checks passed\n",checks);return 0;
}
