#include "recipe_material.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
int main(int argc,char **argv) {
    static char html[131072];
    if(argc>1&&!strcmp(argv[1],"corpus"))return bf_recipe_material_command(argc,argv);
    if(argc>2&&!strcmp(argv[1],"view")) {
        int rc=bf_recipe_material_view(argv[2],argc>3?argv[3]:"",argc>5?argv[5]:"",argc>4?atoi(argv[4]):1,html,sizeof(html));
        puts(html);return rc;
    }
    if(argc==4&&!strcmp(argv[1],"card")) {
        int rc=bf_recipe_material_card(argv[2],atoi(argv[3]),html,sizeof(html));
        puts(html);return rc<0?1:rc==0?2:0;
    }
    if(argc==4&&!strcmp(argv[1],"tiny")) {
        return bf_recipe_material_view(argv[2],argv[3],"",1,html,16)==1?0:1;
    }
    return 2;
}
