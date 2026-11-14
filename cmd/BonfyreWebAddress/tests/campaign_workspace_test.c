#define _POSIX_C_SOURCE 200809L
#ifdef __APPLE__
#define _DARWIN_C_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

/* Standalone helper witness: capture the existing response boundary. This
 * does not exercise HTTP framing, Auth, workspace grants or ac_verified. */
static int response_status, response_public;
static char response_type[128], *response_body;
static size_t response_length;
static unsigned checks;
#define CHECK(value) do { checks++; if (!(value)) { fprintf(stderr,"FAIL line %d: %s\n",__LINE__,#value); exit(1); } } while (0)
static void aw_response(int code,const char *mime,const void *body,size_t size,int public_material) {
    response_status=code;response_public=public_material;snprintf(response_type,sizeof(response_type),"%s",mime);
    free(response_body);response_body=malloc(size+1);CHECK(response_body);memcpy(response_body,body,size);response_body[size]=0;response_length=size;
}
static void aw_error(int code,const char *text) { aw_response(code,"text/plain; charset=utf-8",text,strlen(text),0); }
static void aw_response_with_referrer(int code,const char *mime,const void *body,size_t size,int public_material,const char *referrer) {
    CHECK(!strcmp(referrer,"same-origin"));CHECK(!strcmp(mime,"text/html; charset=utf-8"));
    aw_response(code,mime,body,size,public_material);
}
/* Same header-reader contract as application_workspace.h. cwl_headers first
 * bounds the block, rejects malformed lines and excludes the body. */
static int aw_header(const char *raw,const char *name,char *out,size_t cap) {
    out[0]=0;size_t length=strlen(name);int found=0;
    for(const char *p=raw?strchr(raw,'\n'):NULL;p&&*p;p=strchr(p,'\n')) {
        p++;if(!strncasecmp(p,name,length)&&p[length]==':') {
            if(found++)return 0;const char *value=p+length+1;while(*value==' '||*value=='\t')value++;
            const char *end=value+strcspn(value,"\r\n");while(end>value&&(end[-1]==' '||end[-1]=='\t'))end--;
            if((size_t)(end-value)>=cap)return 0;memcpy(out,value,(size_t)(end-value));out[end-value]=0;
        }
    }
    return found==1;
}
/* The shell's bar and footer are main.c's; this witness covers the form inside <main>. */
#define BF_ESTATE_BAR "<header class=\"ak-bar\"></header>"
#define BF_ESTATE_FOOT "<footer class=\"ak-foot\"></footer>"
#include "../src/campaign_workspace.h"

static const char *back="/apps/ws-0123456789abcdef0123456789abcdef01234567/campaigns/";
static const char *action="/apps/ws-0123456789abcdef0123456789abcdef01234567/_campaign-link";
static const char *host="aurekai.work";
static const char *good="url=https%3A%2F%2Fexample.com%2Fwork%3Fview%3Dcards%23start&utm_source=News+Letter&utm_medium=email&utm_campaign=Autumn+2026&utm_id=C-42&utm_term=caf%C3%A9&utm_content=top";
static void call(const char *method,const char *query,const char *body,const char *headers,const char *hostname,const char *return_path) {
    response_status=0;CHECK(cwl_serve(method,query,body,headers,hostname,return_path)==1);
    CHECK(response_status!=0);CHECK(response_public==0);CHECK(response_length==strlen(response_body));
}
static void post(const char *body,const char *type,const char *origin,const char *extra) {
    char raw[BF_CAMPAIGN_REQUEST_MAX+2048];
    int n=snprintf(raw,sizeof(raw),"POST %s HTTP/1.1\r\nHost: aurekai.work\r\nOrigin: %s\r\nContent-Type: %s\r\nContent-Length: %zu\r\n%s\r\n%s",action,origin,type,strlen(body),extra?extra:"",body);
    CHECK(n>0&&(size_t)n<sizeof(raw));
    /* Match main.c's HTTP reader: the body remains in the buffer, while the
     * header block ends at the last value with no trailing CRLF. */
    char *separator=strstr(raw,"\r\n\r\n");CHECK(separator);char *bodyp=separator+4;bodyp[-4]=0;
    call("POST","",bodyp,raw,host,back);
}
static void form(const char *body) { post(body,"application/x-www-form-urlencoded","https://aurekai.work",NULL); }
static void status(int code,const char *mime) { CHECK(response_status==code);CHECK(strstr(response_type,mime)!=NULL); }
static void get_and_build(void) {
    call("GET","",NULL,NULL,host,back);status(200,"text/html");
    CHECK(strstr(response_body,"<html lang=\"en\""));CHECK(strstr(response_body,"name=\"viewport\""));
    CHECK(strstr(response_body,"Back to campaign workspace"));CHECK(strstr(response_body,back));CHECK(strstr(response_body,action));
    CHECK(strstr(response_body,"method=\"post\""));CHECK(strstr(response_body,"accept-charset=\"UTF-8\""));
    for(int i=0;i<7;i++){char label[80];snprintf(label,sizeof(label),"for=\"campaign-%s\"",cwl_names[i]);CHECK(strstr(response_body,label));}
    CHECK(!strstr(response_body,"<script"));CHECK(!strstr(response_body,"<iframe"));CHECK(!strstr(response_body,"https://"));
    form(good);status(200,"text/html");
    CHECK(strstr(response_body,"<textarea id=\"campaign-result\" rows=\"5\" readonly"));
    CHECK(strstr(response_body,"https://example.com/work?view=cards&amp;utm_source=News%20Letter&amp;utm_medium=email&amp;utm_campaign=Autumn%202026&amp;utm_id=C-42&amp;utm_term=caf%C3%A9&amp;utm_content=top#start"));
    CHECK(strstr(response_body,"value=\"News Letter\""));CHECK(strstr(response_body,"value=\"café\""));
    CHECK(!strstr(response_body,"href=\"https://example.com"));CHECK(!strstr(response_body,"action=\"https://"));CHECK(!strstr(response_body,"<script"));
    form("url=https%3A%2F%2Fexample.com%2F&utm_source=source&utm_medium=email&utm_campaign=autumn&utm_id=&utm_term=&utm_content=");status(200,"text/html");
    CHECK(!strstr(response_body,"&amp;utm_id="));
    form("url=https%3A%2F%2Fexample.com%2F&utm_source=%22%20autofocus%20onfocus%3D%22alert(1)%3C%2Ftextarea%3E%3Cscript%3Ex%3C%2Fscript%3E&utm_medium=email&utm_campaign=autumn");
    status(200,"text/html");CHECK(strstr(response_body,"&quot; autofocus onfocus=&quot;alert(1)&lt;/textarea&gt;&lt;script&gt;x&lt;/script&gt;"));
    CHECK(!strstr(response_body,"<script>"));CHECK(!strstr(response_body,"value=\"\" autofocus"));
    CHECK(strstr(response_body,"%22%20autofocus%20onfocus%3D%22alert%281%29%3C%2Ftextarea%3E%3Cscript%3Ex%3C%2Fscript%3E"));
}
static void form_refusal(void) {
    const char *bad[]={
        "url=https%3A%2F%2Fexample.com&utm_source=a&utm_medium=email&utm_campaign=x&utm_source=b",
        "url=https%3A%2F%2Fexample.com&utm_source=a&utm_medium=email&utm_campaign=x&utm%5Fsource=b",
        "url=https%3A%2F%2Fexample.com&utm_source=a&utm_medium=email&utm_campaign=x&actor=forged",
        "url=https%3A%2F%2Fexample.com&utm_source=a&utm_medium=email&utm_campaign=x&operation=publish",
        "url=https%3A%2F%2Fexample.com&utm_source=a&utm_medium=email&utm_campaign=x&",
        "url=https%3A%2F%2Fexample.com&utm_source=a&utm_medium=email&utm_campaign=x&&utm_id=id",
        "url=https%3A%2F%2Fexample.com&utm_source=%GG&utm_medium=email&utm_campaign=x",
        "url=https%3A%2F%2Fexample.com&utm_source=%00&utm_medium=email&utm_campaign=x",
        "url=https%3A%2F%2Fexample.com&utm_source=%0A&utm_medium=email&utm_campaign=x",
        "url=https%3A%2F%2Fexample.com&utm_source=%ED%A0%80&utm_medium=email&utm_campaign=x",
        "url=https%3A%2F%2Fexample.com&utm_source=a&utm_medium=email",
        "url=https%3A%2F%2Fexample.com&utm_source=a&utm_medium=email&utm_campaign"
    };
    for(size_t i=0;i<sizeof(bad)/sizeof(*bad);i++){form(bad[i]);status(400,"text/html");CHECK(strstr(response_body,"role=\"alert\""));CHECK(!strstr(response_body,"id=\"campaign-result\""));CHECK(!strstr(response_body,"forged"));}
    form("url=https%3A%2F%2Fuser%3Aprivate-pass%40example.com&utm_source=Keep+this&utm_medium=email&utm_campaign=autumn");
    status(400,"text/html");CHECK(!strstr(response_body,"private-pass"));CHECK(strstr(response_body,"value=\"Keep this\""));CHECK(strstr(response_body,"invalid destination was cleared"));
    form("url=https%3A%2F%2Fuser%3Aprivate-pass%40example.com&utm_source=invalid%5Cvalue&utm_medium=email&utm_campaign=autumn");
    status(400,"text/html");CHECK(!strstr(response_body,"private-pass"));CHECK(strstr(response_body,"value=\"invalid\\value\""));
    form("url=javascript%3Aalert(1)&utm_source=Keep+this&utm_medium=email&utm_campaign=autumn");status(400,"text/html");CHECK(!strstr(response_body,"javascript:"));CHECK(strstr(response_body,"value=\"Keep this\""));
    form("url=https%3A%2F%2Fexample.com%2F%3Futm_source%3Dold%23keep&utm_source=new&utm_medium=email&utm_campaign=autumn");
    status(400,"text/html");CHECK(strstr(response_body,"already has campaign tags"));CHECK(strstr(response_body,"value=\"https://example.com/?utm_source=old#keep\""));
    form("url=https%3A%2F%2Fexample.com&utm_source=&utm_medium=email&utm_campaign=autumn");status(400,"text/html");CHECK(strstr(response_body,"Enter a source, medium and campaign name."));
    char large[900];memset(large,'a',sizeof(large));snprintf(large,sizeof(large),"url=https%%3A%%2F%%2Fexample.com&utm_source=%0257d&utm_medium=email&utm_campaign=x",1);
    form(large);status(400,"text/html");CHECK(strstr(response_body,"oversized field"));
}
static void json_api(void) {
    const char *json="{\"version\":1,\"operation\":\"build\",\"url\":\"https://example.com/?x=1#keep\",\"utm\":{\"utm_source\":\"news\",\"utm_medium\":\"email\",\"utm_campaign\":\"fall\"}}";
    post(json,"application/json","https://aurekai.work",NULL);status(200,"application/json");CHECK(strstr(response_body,"\"ok\":true"));CHECK(strstr(response_body,"x=1&utm_source=news&utm_medium=email&utm_campaign=fall#keep"));
    CHECK(strstr(response_body,"\"external_effect_executed\":false"));CHECK(!strstr(response_body,"<html"));
    post("{\"version\":1,\"operation\":\"parse\",\"url\":\"https://example.com/?utm_id=partial\"}","Application/JSON; charset=\"UTF-8\"","https://aurekai.work",NULL);
    status(200,"application/json");CHECK(strstr(response_body,"\"tagging\":\"partial\""));
    post("{\"version\":1,\"operation\":\"validate\",\"url\":\"https://example.com\"}","application/json; charset=utf-8","https://aurekai.work",NULL);
    status(200,"application/json");CHECK(strstr(response_body,"\"tagging\":\"absent\""));
    post("{}","application/json","https://aurekai.work",NULL);status(400,"application/json");CHECK(strstr(response_body,"\"error\":\"invalid_request\""));
    post("{\"version\":1,\"operation\":\"parse\",\"url\":\"https://user:private-pass@example.com\"}","application/json","https://aurekai.work",NULL);
    status(400,"application/json");CHECK(!strstr(response_body,"private-pass"));
    /* A body line that resembles a header is never read as an Origin. */
    post("{\"version\":1,\"operation\":\"parse\",\"url\":\"https://example.com\"}\nOrigin: https://attacker.example","application/json","https://aurekai.work",NULL);
    status(400,"application/json");CHECK(strstr(response_body,"invalid_request"));
}
static void transport(void) {
    post(good,"application/x-www-form-urlencoded","https://attacker.example",NULL);status(403,"text/plain");
    post(good,"application/x-www-form-urlencoded","null",NULL);status(403,"text/plain");
    post(good,"application/x-www-form-urlencoded","https://aurekai.work", "Origin: https://aurekai.work\r\n");status(403,"text/plain");
    post(good,"application/x-www-form-urlencoded","https://aurekai.work", "Content-Length: 3\r\n");status(400,"text/plain");
    post(good,"application/x-www-form-urlencoded","https://aurekai.work", "Content-Type: application/json\r\n");status(415,"text/plain");
    post(good,"application/x-www-form-urlencoded","https://aurekai.work", "Transfer-Encoding: chunked\r\n");status(400,"text/plain");
    post(good,"application/x-www-form-urlencoded","https://aurekai.work", "Content-Encoding: gzip\r\n");status(400,"text/plain");
    post(good,"application/x-www-form-urlencoded","https://aurekai.work", " folded: bad\r\n");status(400,"text/plain");
    const char *types[]={"text/plain","multipart/form-data; boundary=x","application/jsonp","application/json; charset=latin1","application/json; charset=utf-8; charset=utf-8","application/x-www-form-urlencoded; unexpected=yes"};
    for(size_t i=0;i<sizeof(types)/sizeof(*types);i++){post(good,types[i],"https://aurekai.work",NULL);status(415,"text/plain");}
    post(good,"application/x-www-form-urlencoded; charset=UTF-8","https://aurekai.work",NULL);status(200,"text/html");
    call("GET","utm_source=wrong",NULL,NULL,host,back);status(400,"text/plain");
    call("POST","utm_source=wrong",good,NULL,host,back);status(400,"text/plain");
    call("GET","","body",NULL,host,back);status(400,"text/plain");
    call("PUT","",good,NULL,host,back);status(405,"text/plain");
    call("GET","",NULL,NULL,host,"https://attacker.example/");status(409,"text/plain");
    call("GET","",NULL,NULL,host,"/apps/ws-0123456789abcdef0123456789abcdef01234567/../other/");status(409,"text/plain");
    call("GET","",NULL,NULL,host,"/apps/ws-0123456789abcdef0123456789abcdef01234567/campaigns/?next=evil");status(409,"text/plain");
    const char *header_prefix="POST / HTTP/1.1\r\nOrigin: https://aurekai.work\r\nContent-Type: application/json\r\n";
    char headers[1024];const char *lengths[]={"-1","+2","2x","000002","","3","1"};
    for(size_t i=0;i<sizeof(lengths)/sizeof(*lengths);i++){
        snprintf(headers,sizeof(headers),"%sContent-Length: %s\r\n\r\n",header_prefix,lengths[i]);call("POST","","{}",headers,host,back);status(400,"text/plain");
    }
    snprintf(headers,sizeof(headers),"%sContent-Length: 16385\r\n\r\n",header_prefix);call("POST","","{}",headers,host,back);status(413,"text/plain");
    snprintf(headers,sizeof(headers),"%s\r\n",header_prefix);call("POST","","{}",headers,host,back);status(400,"text/plain");
    call("POST","","{}","POST / HTTP/1.1\nOrigin: https://aurekai.work\nContent-Type: application/json\nContent-Length: 2\n\n",host,back);status(400,"application/json");
    call("POST","","{}","POST / HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n",host,back);status(403,"text/plain");
    const char *valid="{\"version\":1,\"operation\":\"parse\",\"url\":\"https://example.com\"}";
    snprintf(headers,sizeof(headers),"POST / HTTP/1.1\r\nOrigin: http://127.0.0.1:9000\r\nContent-Type: application/json\r\nContent-Length: %zu\r\n\r\n",strlen(valid));
    call("POST","",valid,headers,"127.0.0.1:9000",back);status(200,"application/json");
    char oversized[BF_CAMPAIGN_REQUEST_MAX+2];memset(oversized,'x',sizeof(oversized)-1);oversized[sizeof(oversized)-1]=0;
    snprintf(headers,sizeof(headers),"%sContent-Length: 2\r\n\r\n",header_prefix);call("POST","",oversized,headers,host,back);status(413,"text/plain");
    char nul[]={'{','}',0,'x',0};snprintf(headers,sizeof(headers),"%sContent-Length: 4\r\n\r\n",header_prefix);call("POST","",nul,headers,host,back);status(400,"text/plain");
}
static void reader_contract(void) {
    const char *json="{\"version\":1,\"operation\":\"build\",\"url\":\"https://example.com/?x=1#keep\",\"utm\":{\"utm_source\":\"news\",\"utm_medium\":\"email\",\"utm_campaign\":\"fall\"}}";
    char raw[2048];
    snprintf(raw,sizeof(raw),"POST %s HTTP/1.1\r\nhost: aurekai.work\r\norigin: https://aurekai.work\r\ncontent-type: application/json\r\naccept: application/json\r\ncontent-length: %zu",action,strlen(json));
    CHECK(raw[strlen(raw)-1]>='0'&&raw[strlen(raw)-1]<='9');
    call("POST","repr=API",json,raw,host,back);status(200,"application/json");
    CHECK(strstr(response_body,"x=1&utm_source=news&utm_medium=email&utm_campaign=fall#keep"));
    call("POST","",json,raw,host,back);status(200,"application/json");
    const char *bad_queries[]={"repr=API&repr=API","repr=API&utm_source=forged","utm_source=forged&repr=API","repr=API&","repr=api","%72epr=API","repr=API;actor=forged"};
    for(size_t i=0;i<sizeof(bad_queries)/sizeof(*bad_queries);i++){call("POST",bad_queries[i],json,raw,host,back);status(400,"text/plain");}
    call("GET","repr=API",NULL,raw,host,back);status(400,"text/plain");
    snprintf(raw,sizeof(raw),"POST %s HTTP/1.1\r\nOrigin: https://aurekai.work\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: %zu",action,strlen(good));
    call("POST","repr=API",good,raw,host,back);status(400,"text/plain");
    /* The same isolated contract also exists for the reader's LF path. */
    snprintf(raw,sizeof(raw),"POST %s HTTP/1.1\nOrigin: https://aurekai.work\nContent-Type: application/json\nContent-Length: %zu",action,strlen(json));
    call("POST","repr=API",json,raw,host,back);status(200,"application/json");
    snprintf(raw,sizeof(raw),"POST / HTTP/1.1\r\nOrigin: https://aurekai.work\r\nContent-Type: application/json\r\nContent-Length: %zu\r\nOrigin: https://aurekai.work",strlen(json));
    call("POST","repr=API",json,raw,host,back);status(403,"text/plain");
    snprintf(raw,sizeof(raw),"POST / HTTP/1.1\r\nOrigin: https://aurekai.work\r\nContent-Type: application/json\r\nContent-Length: %zu\r\nContent-Length: %zu",strlen(json),strlen(json));
    call("POST","repr=API",json,raw,host,back);status(400,"text/plain");
    snprintf(raw,sizeof(raw),"POST / HTTP/1.1\r\nOrigin: https://aurekai.work\r\nContent-Type: application/json\r\nContent-Length: %zu\r\n broken: line",strlen(json));
    call("POST","repr=API",json,raw,host,back);status(400,"text/plain");
    /* Full framed callers are still bounded before body-like headers. */
    snprintf(raw,sizeof(raw),"POST / HTTP/1.1\r\nOrigin: https://aurekai.work\r\nContent-Type: application/json\r\nContent-Length: %zu\r\n\r\nOrigin: https://attacker.example",strlen(json));
    call("POST","repr=API",json,raw,host,back);status(200,"application/json");
    snprintf(raw,sizeof(raw),"POST / HTTP/1.1\r\nOrigin: https://aurekai.work\r\nContent-Type: application/json\r\nContent-Length: %zu\r",strlen(json));
    call("POST","repr=API",json,raw,host,back);status(400,"text/plain");
    char oversized[CWL_HEADER_MAX+2];memset(oversized,'x',sizeof(oversized)-1);oversized[sizeof(oversized)-1]=0;
    call("POST","repr=API",json,oversized,host,back);status(400,"text/plain");
}
int main(void) {
    get_and_build();form_refusal();json_api();transport();reader_contract();free(response_body);
    printf("campaign workspace helper: %u checks passed (outer Auth not exercised)\n",checks);return 0;
}
