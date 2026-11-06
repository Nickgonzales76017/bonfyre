#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../src/stitch_work_presentation.h"

static int contains(const char *haystack, const char *needle) {
    return strstr(haystack,needle) != NULL;
}
int main(void) {
    assert(!strcmp(bf_stitch_work_pattern("aurekai.work","/people",NULL),"directory"));
    assert(!strcmp(bf_stitch_work_pattern("aurekai.work","/people","who=Jane%20Doe&source=one"),"detail"));
    assert(!strcmp(bf_stitch_work_pattern("aurekai.work","/people","source=one&subject=person:1"),"detail"));
    assert(!strcmp(bf_stitch_work_pattern("aurekai.work","/people","somewho=x"),"directory"));
    assert(!strcmp(bf_stitch_work_pattern("aurekai.work","/packs",NULL),"market"));
    assert(!strcmp(bf_stitch_work_pattern("aurekai.work","/studio",NULL),"workbench"));
    assert(!strcmp(bf_stitch_work_pattern("aurekai.work","/link",NULL),"connections"));
    assert(!strcmp(bf_stitch_work_pattern("aurekai.work","/jobs",NULL),"directory"));
    assert(!strcmp(bf_stitch_work_native_pattern("work","/inbox"),"feed"));
    assert(bf_stitch_work_native_pattern("live","/inbox") == NULL);
    assert(bf_stitch_work_pattern("example.com","/people",NULL) == NULL);
    assert(!strcmp(bf_stitch_work_pattern("aurekai.life","/people",NULL),"directory"));
    assert(!strcmp(bf_stitch_work_pattern("aurekai.life","/food",NULL),"discover"));
    assert(!strcmp(bf_stitch_work_pattern("aurekai.life","/whats-on",NULL),"discover"));
    assert(!strcmp(bf_stitch_work_pattern("aurekai.life","/people","subject=person%3A1"),"detail"));
    assert(!strcmp(bf_stitch_work_pattern("aurekai.live","/film",NULL),"media"));
    assert(!strcmp(bf_stitch_work_pattern("aurekai.live","/watch",NULL),"media"));
    assert(!strcmp(bf_stitch_work_pattern("aurekai.live","/whats-on",NULL),"discover"));
    assert(!strcmp(bf_stitch_work_pattern("aurekai.shop","/style",NULL),"discover"));
    assert(!strcmp(bf_stitch_work_pattern("aurekai.shop","/recalls",NULL),"reader"));
    assert(!strcmp(bf_stitch_work_pattern("aurekai.shop","/deals",NULL),"market"));
    assert(!strcmp(bf_stitch_work_native_pattern("life","/health"),"reader"));
    assert(!strcmp(bf_stitch_work_native_pattern("live","/listen"),"media"));
    assert(!strcmp(bf_stitch_work_native_pattern("shop","/retail"),"market"));
    assert(bf_stitch_work_pattern("aurekai.live.evil.test","/watch",NULL)==NULL);
    assert(bf_stitch_work_pattern("nick.aurekai.work","/people",NULL)==NULL);
    assert(bf_stitch_work_pattern("aurekai.work","/not-a-real-page",NULL)==NULL);
    assert(bf_stitch_work_pattern("aurekai.work","/unknown",NULL) == NULL);
    FILE *out=tmpfile(); assert(out);
    bf_stitch_work_emit_head(out, "directory");
    bf_stitch_work_emit_body_attribute(out, "directory");
    fflush(out); assert(fseek(out,0,SEEK_END)==0);
    long n=ftell(out); assert(n>256 && n<8192 && fseek(out,0,SEEK_SET)==0);
    char *buffer=calloc((size_t)n+1,1); assert(buffer);
    assert(fread(buffer,1,(size_t)n,out)==(size_t)n);
    assert(contains(buffer,"<meta name=\"aurekai:composition\" content=\"directory\">"));
    assert(contains(buffer,"body[data-aurekai-pattern]"));
    assert(contains(buffer," data-aurekai-pattern=\"directory\""));
    assert(!contains(buffer,"Ferrari") && !contains(buffer,"fake"));
    free(buffer);fclose(out);
    printf("stitch_work_presentation_test: PASS exact .work routes, native aliases, safe responsive presentation\n");
    return 0;
}
