#include <stdio.h>
#include <stdlib.h>
#include "../native/authoring_carrier.h"
int main(int argc,char **argv){
 if(argc!=3){fprintf(stderr,"Usage: native-authoring-preview <source.bfau> <preview.html>\n");return 2;}
 FILE *f=fopen(argv[1],"rb");if(!f){perror("wire input");return 2;}
 BfWireScene scene={0};
 int good=bf_wire_read(f,&scene);fclose(f);
 if(!good){fprintf(stderr,"Rejected malformed, unauthorized-shape or oversized authoring wire.\n");return 3;}
 FILE *out=fopen(argv[2],"wb");if(!out){perror("preview output");bf_wire_clear(&scene);return 4;}
 good=bf_wire_render(out,&scene);int closed=fclose(out);
 if(!good||closed){remove(argv[2]);fprintf(stderr,"Preview blocked by unsupported or unsafe content.\n");bf_wire_clear(&scene);return 5;}
 fprintf(stderr,"AUTHORING_NATIVE_PREVIEW_PASS site=%s route=%s nodes=%u\n",scene.site,scene.route,scene.count);
 bf_wire_clear(&scene);return 0;
}
