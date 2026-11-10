#include "subject_address.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(void) {
    sqlite3 *cms = NULL;
    char subject[384], host[256];
    assert(lower_host("WWW.AUREKAI.LIVE:443", host));
    assert(!strcmp(host, "www.aurekai.live"));
    assert(!lower_host("aurekai.live:bad", host));
    assert(!lower_host("aurekai.live:999999999999999999", host));
    assert(!lower_host("aurekai.live:0", host));
    assert(!lower_host("aurekai.live:\r\nX-Host: evil", host));
    assert(page_subject_for_host(NULL, "WWW.AUREKAI.LIVE:443", subject, sizeof(subject)));
    assert(!strcmp(subject, "institution:aurekai:live"));
    assert(!page_subject_for_host(NULL, "other.aurekai.live", subject, sizeof(subject)));
    assert(!page_subject_for_host(NULL, "aurekai.live:bad", subject, sizeof(subject)));
    assert(sqlite3_open(":memory:", &cms) == SQLITE_OK);
    assert(sqlite3_exec(cms,
        "CREATE TABLE site_presentation(id INTEGER PRIMARY KEY,host TEXT,"
        "subject_address TEXT,status TEXT);"
        "INSERT INTO site_presentation VALUES(1,'school.example','institution:school-a','draft');"
        "INSERT INTO site_presentation VALUES(2,'school.example','institution:school-b','published');"
        "INSERT INTO site_presentation VALUES(3,'school.example','institution:school-c','published');"
        "INSERT INTO site_presentation VALUES(4,'other.aurekai.live','institution:claimed','published');",
        NULL, NULL, NULL) == SQLITE_OK);
    assert(page_subject_for_host(cms, "school.example", subject, sizeof(subject)));
    assert(!strcmp(subject, "institution:school-c"));
    assert(!page_subject_for_host(cms, "school.example", subject, 5));
    assert(!subject[0]);
    assert(page_subject_for_host(cms, "other.aurekai.live", subject, sizeof(subject)));
    assert(!strcmp(subject, "institution:claimed"));
    assert(!page_subject_for_host(cms, "unclaimed.aurekai.live", subject, sizeof(subject)));
    sqlite3_close(cms);
    puts("subject address: PASS");
    return 0;
}
