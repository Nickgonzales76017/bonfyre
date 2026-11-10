#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "../generated/native_page_composition.h"
/* Only count/cursor are consumed; the real enums and region contract are used. */
typedef struct { int count,cursor; } PageStream;
typedef struct { PageStream stream[BF_PAGE_MATERIAL_COUNT]; } PageMaterial;
#include "../src/page_preview_region.h"
static unsigned checks;
#define CHECK(x) do { assert(x); checks++; } while(0)
static unsigned char id(const char *role) { for(size_t i=0;i<BF_PAGE_REGION_COUNT;i++)if(!strcmp(bf_page_regions[i].role,role))return (unsigned char)i;assert(0);return 0; }
int main(void) {
    PageMaterial m={0};unsigned char sequence[]={id("lead"),id("photos"),id("latest"),id("listen")};
    m.stream[BF_PAGE_MATERIAL_PUBLICATION].count=3;
    CHECK(page_preview_region(&m,sequence,4)==&bf_page_regions[id("latest")]);
    m.stream[BF_PAGE_MATERIAL_PHOTO].count=1;
    CHECK(page_preview_region(&m,sequence,4)==&bf_page_regions[id("latest")]);
    m.stream[BF_PAGE_MATERIAL_PUBLICATION].cursor=3;
    CHECK(page_preview_region(&m,sequence,4)==NULL);
    m.stream[BF_PAGE_MATERIAL_AUDIO].count=2;PageMaterial original=m;
    CHECK(page_preview_region(&m,sequence,4)==&bf_page_regions[id("listen")]);CHECK(!memcmp(&original,&m,sizeof(m)));
    unsigned char lead[]={id("lead")};m.stream[BF_PAGE_MATERIAL_PUBLICATION].cursor=2;
    CHECK(page_preview_region(&m,lead,1)==&bf_page_regions[id("lead")]);
    unsigned char invalid[]={255,id("listen")};CHECK(page_preview_region(&m,invalid,2)==&bf_page_regions[id("listen")]);
    CHECK(page_preview_region(&m,sequence,0)==NULL);CHECK(page_preview_region(NULL,sequence,4)==NULL);
    m.stream[BF_PAGE_MATERIAL_AUDIO].cursor=-1;CHECK(page_preview_region(&m,invalid,2)==NULL);
    printf("page preview regions: PASS (%u checks)\n",checks);return 0;
}
