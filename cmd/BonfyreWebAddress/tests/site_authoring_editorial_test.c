#define _POSIX_C_SOURCE 200809L
#include "../src/site_authoring_editorial.h"
#include <assert.h>
static char *render(sqlite3 *db,const char *tags,int wanted){
    char *bytes=NULL;size_t len=0;FILE *out=open_memstream(&bytes,&len);
    assert(out);
    int status=bf_site_editorial_blocks(out,db,tags);
    assert(status==wanted);
    assert(fclose(out)==0);
    assert(bytes);
    return bytes;
}
int main(void){
    sqlite3 *db=NULL;assert(sqlite3_open(":memory:",&db)==SQLITE_OK);
    char *empty=render(db,"{}",0);assert(!*empty);free(empty);
    const char *sample="{\"authoring_v1\":{\"editorial\":["
      "{\"type\":\"header\",\"data\":{\"text\":\"Article heading\",\"level\":3}},"
      "{\"type\":\"paragraph\",\"data\":{\"text\":\"Visible <script>alert(1)</script> story\"}},"
      "{\"type\":\"quote\",\"data\":{\"text\":\"Quoted source\",\"caption\":\"Author <b>name</b>\"}},"
      "{\"type\":\"list\",\"data\":{\"style\":\"ordered\",\"items\":[\"First\",{\"text\":\"Second\"}]}},"
      "{\"type\":\"table\",\"data\":{\"content\":[[\"Name\",\"Value\"],[\"Alpha\",\"<unsafe>\"]]}},"
      "{\"type\":\"code\",\"data\":{\"code\":\"return '<html>';\"}},"
      "{\"type\":\"delimiter\",\"data\":{}}"
    "]}}";
    char *html=render(db,sample,1);
    assert(strstr(html,"<h3>Article heading</h3>"));
    assert(strstr(html,"<p>Visible &lt;script&gt;alert(1)&lt;/script&gt; story</p>"));
    assert(!strstr(html,"<script>"));
    assert(strstr(html,"<blockquote><p>Quoted source</p><cite>Author &lt;b&gt;name&lt;/b&gt;</cite></blockquote>"));
    assert(strstr(html,"<ol><li>First</li><li>Second</li></ol>"));
    assert(strstr(html,"<tr><td>Alpha</td><td>&lt;unsafe&gt;</td></tr>"));
    assert(strstr(html,"<pre><code>return &#39;&lt;html&gt;&#39;;</code></pre>"));
    assert(strstr(html,"<hr>"));
    free(html);
    char *invalid=render(db,"{\"authoring_v1\":{\"editorial\":[{\"type\":\"iframe\",\"data\":{\"text\":\"untrusted\"}}]}}",-1);
    assert(!*invalid);free(invalid);
    char *broken=render(db,"{this is invalid}",0);assert(!*broken);free(broken);
    assert(sqlite3_close(db)==SQLITE_OK);
    puts("site editorial: real Editor.js block HTML, escaping, bounded grammar, invalid fallback PASS");
    return 0;
}
