#define _POSIX_C_SOURCE 200809L
#include "atom_feed.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *render(sqlite3 *cms, sqlite3 *fabric, const char *subject,
                    const char *tag, int head, const char *hub) {
    FILE *out = tmpfile();
    char *result;
    long size;
    assert(out);
    assert(bf_atom_feed_serve(out, cms, fabric, subject, "aurekai.live",
                              "https://aurekai.example", tag, head, hub));
    assert(fseek(out, 0, SEEK_END) == 0);
    size = ftell(out);
    assert(size >= 0);
    assert(fseek(out, 0, SEEK_SET) == 0);
    result = malloc((size_t)size + 1);
    assert(result);
    assert(fread(result, 1, (size_t)size, out) == (size_t)size);
    result[size] = '\0';
    fclose(out);
    return result;
}

int main(void) {
    sqlite3 *fabric = NULL, *cms = NULL;
    char *body, *other, *head, *conditional, *fallback, *utf8, *not_a_match;
    char tag[64] = {0};
    const char *start;
    assert(sqlite3_open(":memory:", &fabric) == SQLITE_OK);
    assert(sqlite3_open(":memory:", &cms) == SQLITE_OK);
    assert(sqlite3_exec(fabric,
        "CREATE TABLE subject_projection_build(build_id INTEGER,compiled_at TEXT);"
        "INSERT INTO subject_projection_build VALUES(1,'2026-09-29T00:00:00Z');"
        "CREATE TABLE subject_route(subject TEXT,scope TEXT);"
        "INSERT INTO subject_route VALUES('subject:one','public');"
        "INSERT INTO subject_route VALUES('subject:two','public');"
        "CREATE TABLE subject_material(subject TEXT,scope TEXT,material TEXT,"
        "source TEXT,item_key TEXT,title TEXT,body TEXT,at INTEGER);"
        "INSERT INTO subject_material VALUES('subject:one','public','publication',"
        "'included','0123456789abcdef01234567','A & <B>','Read & learn',1780000000);"
        "CREATE TABLE subject_route_item(subject TEXT,scope TEXT,source TEXT,"
        "item_key TEXT,material TEXT);"
        "INSERT INTO subject_route_item VALUES('subject:one','public',"
        "'included','0123456789abcdef01234567','publication');", NULL, NULL, NULL) == SQLITE_OK);
    assert(sqlite3_exec(cms,
        "CREATE TABLE wiki_page(title TEXT,route TEXT,published_at TEXT,"
        "meta_description TEXT,status TEXT,allow_guest INTEGER,id INTEGER);"
        "INSERT INTO wiki_page VALUES('Legacy','notes/legacy',"
        "'2026-09-29T00:00:00Z','Old note','published',1,1);",
        NULL, NULL, NULL) == SQLITE_OK);

    body = render(cms, fabric, "subject:one", NULL, 0, "https://hub.example/path");
    assert(strstr(body, "HTTP/1.1 200 OK\r\n"));
    assert(strstr(body, "<title>A &amp; &lt;B&gt;</title>"));
    assert(strstr(body, "https://aurekai.live/o/included/0123456789abcdef01234567"));
    assert(strstr(body, "<https://hub.example/path>; rel=\"hub\""));
    assert(!strstr(body, "notes/legacy"));
    start = strstr(body, "ETag: ");
    assert(start && strlen(start) >= 25);
    memcpy(tag, start + 6, 20);
    tag[20] = '\0';
    conditional = render(cms, fabric, "subject:one", tag, 0, NULL);
    assert(strstr(conditional, "HTTP/1.1 304 Not Modified\r\n"));
    assert(!strstr(conditional, "<entry>"));
    {
        char false_tag[80];
        snprintf(false_tag, sizeof(false_tag), "junk%sjunk", tag);
        not_a_match = render(cms, fabric, "subject:one", false_tag, 0, NULL);
        assert(strstr(not_a_match, "HTTP/1.1 200 OK\r\n"));
    }
    head = render(cms, fabric, "subject:one", NULL, 1, NULL);
    assert(strstr(head, "HTTP/1.1 200 OK\r\n"));
    assert(!strstr(head, "<feed"));
    {
        static const char malformed[] = {'A',' ',(char)0xc3,(char)0xa9,' ',(char)0xff,0};
        sqlite3_stmt *update = NULL;
        assert(sqlite3_prepare_v2(fabric, "UPDATE subject_material SET title=?1", -1,
                                  &update, NULL) == SQLITE_OK);
        sqlite3_bind_text(update, 1, malformed, 6, SQLITE_STATIC);
        assert(sqlite3_step(update) == SQLITE_DONE);
        sqlite3_finalize(update);
    }
    utf8 = render(cms, fabric, "subject:one", NULL, 0, NULL);
    assert(strstr(utf8, "<title>A \xc3\xa9 </title>"));
    assert(!strchr(utf8, (char)0xff));
    other = render(cms, fabric, "subject:two", NULL, 0,
                   "https://hub.example/\r\nX-Injected: yes");
    assert(!strstr(other, "included"));
    assert(!strstr(other, "notes/legacy"));
    assert(!strstr(other, "X-Injected"));
    fallback = render(cms, NULL, "", NULL, 0, NULL);
    assert(strstr(fallback, "notes/legacy"));
    {
        FILE *out = tmpfile();
        assert(out);
        assert(!bf_atom_feed_serve(out, cms, fabric, "subject:one", "aurekai.live",
                                   "https://bad.example\r\nX-Injected: yes", NULL, 0, NULL));
        assert(ftell(out) == 0);
        fclose(out);
    }
    free(body); free(other); free(head); free(conditional); free(fallback); free(utf8);
    free(not_a_match);
    sqlite3_close(fabric); sqlite3_close(cms);
    puts("atom feed: PASS");
    return 0;
}
