#include <assert.h>
#include <stdio.h>
#include "../../BonfyreWebAddress/generated/native_authoring_contract.h"
int main(void){
 assert(BF_AUTHORING_BLOCK_COUNT==7);
 assert(bf_authoring_block_accepts("profile-hero",220,5000));
 assert(!bf_authoring_block_accepts("profile-hero",221,5000));
 assert(!bf_authoring_block_accepts("profile-card",220,5001));
 assert(!bf_authoring_block_accepts("unknown",1,1));
 assert(!bf_authoring_block_accepts(NULL,0,0));
 assert(strcmp(bf_authoring_block("profile-story")->role,"thread")==0);
 puts("native authoring contract PASS");return 0;
}
