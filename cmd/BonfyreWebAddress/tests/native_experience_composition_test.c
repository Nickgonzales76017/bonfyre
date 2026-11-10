#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../generated/native_page_composition.h"
#include "../src/native_experience_composition.h"

static size_t role_index(const char *role) {
    for(size_t i=0;i<BF_PAGE_REGION_COUNT;i++)
        if(!strcmp(bf_page_regions[i].role,role))return i;
    assert(!"missing compiled native region");return 0;
}
int main(int argc,char **argv) {
    if(argc==2 && !strcmp(argv[1],"--html")){
        const BfNativeExperienceProgram *program=bf_native_experience_find("media");
        fputs("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">",stdout);
        bf_native_experience_emit_head(stdout,program);
        fputs("</head><body><main style=\"width:min(100% - 20px,1200px);margin:auto\">",stdout);
        bf_native_experience_begin(stdout,program);
        const char *roles[]={"watch","channels","grid"};
        const char *titles[]={"Retained film","Published channel","Screening catalogue"};
        for(size_t i=0;i<3;i++){
            fputs("<section data-verified-native=\"true\"",stdout);
            bf_native_experience_region(stdout,program,role_index(roles[i]),i);
            fputs("><h2>",stdout);fputs(titles[i],stdout);fputs("</h2>",stdout);
            bf_native_experience_focus(stdout,program);
            fputs("<p>Exact native region, read-only fixture.</p></section>",stdout);
        }
        bf_native_experience_end(stdout,program);
        fputs("</main></body></html>",stdout);
        return 0;
    }
    assert(BF_NATIVE_EXPERIENCE_COUNT==14);
    assert(BF_NATIVE_EXPERIENCE_REGION_COUNT==BF_PAGE_REGION_COUNT);
    const char *patterns[]={"profile","organization","directory","detail","discover",
        "reader","connections","workbench","review","feed","market","media","spatial","device"};
    for(size_t i=0;i<sizeof(patterns)/sizeof(patterns[0]);i++){
        const BfNativeExperienceProgram *program=bf_native_experience_find(patterns[i]);
        assert(program&&program->shell&&program->views&BF_EXP_VIEW_FOCUS);
        for(size_t j=i+1;j<sizeof(patterns)/sizeof(patterns[0]);j++)
            assert(strcmp(program->pattern,patterns[j]));
        for(size_t j=0;j<BF_PAGE_REGION_COUNT;j++)
            assert(program->lane[j]<=BF_EXP_WIDE);
    }
    assert(!bf_native_experience_find(NULL));
    assert(!bf_native_experience_find("other"));
    const BfNativeExperienceProgram *media=bf_native_experience_find("media");
    assert(media->lane[role_index("watch")]==BF_EXP_HERO);
    assert(media->lane[role_index("listen")]==BF_EXP_HERO);
    assert(media->lane[role_index("channels")]==BF_EXP_RAIL);
    assert(media->lane[role_index("grid")]==BF_EXP_WIDE);
    const BfNativeExperienceProgram *reader=bf_native_experience_find("reader");
    assert(reader->views==BF_EXP_VIEW_FOCUS);
    assert(reader->lane[role_index("parts")]==BF_EXP_RAIL);
    FILE *out=tmpfile();assert(out);
    bf_native_experience_emit_head(out,media);
    bf_native_experience_begin(out,media);
    bf_native_experience_region(out,media,role_index("watch"),0);
    bf_native_experience_focus(out,media);
    bf_native_experience_end(out,media);
    fflush(out);assert(fseek(out,0,SEEK_END)==0);
    long n=ftell(out);assert(n>3500&&n<30000);rewind(out);
    char *text=calloc((size_t)n+1,1);assert(text);
    assert(fread(text,1,(size_t)n,out)==(size_t)n);
    assert(strstr(text,"data-ak-experience=\"media\""));
    assert(strstr(text,"data-exp-shell=\"player\""));
    assert(strstr(text,"data-exp-lane=\"hero\""));
    assert(strstr(text,"data-exp-mode=\"compare\""));
    assert(strstr(text,"data-exp-focus hidden"));
    assert(strstr(text,"data-ak-region"));
    assert(!strstr(text,"sample offer")&&!strstr(text,"authorized action"));
    free(text);fclose(out);
    printf("Native experience composition: PASS 14 owner-backed zone recipes and read-only states\n");
    return 0;
}
