/* Generated from authoring-prototype.mjs; do not hand-edit. */
#ifndef BONFYRE_NATIVE_AUTHORING_CONTRACT_H
#define BONFYRE_NATIVE_AUTHORING_CONTRACT_H
#include <stddef.h>
#include <string.h>
typedef struct {const char *kind;const char *role;size_t max_heading;size_t max_body;} BfAuthoringBlock;
static const BfAuthoringBlock bf_authoring_blocks[]={
    {"profile-hero","lead",220,5000},
    {"profile-section","shelf",220,5000},
    {"profile-card","grid",220,5000},
    {"profile-story","thread",220,5000},
    {"profile-quote","thread",220,5000},
    {"profile-metric","grid",220,5000},
    {"profile-action","field",220,5000},
};
#define BF_AUTHORING_BLOCK_COUNT (sizeof(bf_authoring_blocks)/sizeof(bf_authoring_blocks[0]))
static inline const BfAuthoringBlock *bf_authoring_block(const char *kind){
 if(!kind)return NULL;
 for(size_t i=0;i<BF_AUTHORING_BLOCK_COUNT;i++)if(strcmp(kind,bf_authoring_blocks[i].kind)==0)return &bf_authoring_blocks[i];
 return NULL;
}
static inline int bf_authoring_block_accepts(const char *kind,size_t heading,size_t body){
 const BfAuthoringBlock *s=bf_authoring_block(kind);return s&&heading<=s->max_heading&&body<=s->max_body;
}
/* Shape validity never grants source, audience, account or publication authority. */
#endif
