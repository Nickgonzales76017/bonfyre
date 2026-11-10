#ifndef BONFYRE_PAGE_PREVIEW_REGION_H
#define BONFYRE_PAGE_PREVIEW_REGION_H
/* Choose only a declared rhythm with material remaining in this exact cut. */
static const BfPageRegion *page_preview_region(const PageMaterial *material,const unsigned char *sequence,size_t count) {
    if(!material||!sequence)return NULL;
    for(int leads=0;leads<2;leads++) {
        for(size_t i=0;i<count;i++) {
            if(sequence[i]>=BF_PAGE_REGION_COUNT)continue;
            const BfPageRegion *region=&bf_page_regions[sequence[i]];
            const PageStream *stream=&material->stream[region->material];
            if((region->topology==BF_PAGE_TOPOLOGY_LEAD)!=leads)continue;
            if(stream->cursor<0||stream->count<stream->cursor||stream->count-stream->cursor<(int)region->min_items)continue;
            return region;
        }
    }
    return NULL;
}
#endif
