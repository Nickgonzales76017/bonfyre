#include <stdio.h>
#include <stdlib.h>
#include "../native/authoring_carrier.h"
int main(int argc,char **argv){
 int roundtrip=argc==4&&!strcmp(argv[1],"--roundtrip");
 if((!roundtrip&&argc!=3)||(roundtrip&&argc!=4)){
  fprintf(stderr,"Usage: native-authoring-preview [--roundtrip] <source.bfau> <output>\n");
  return 2;
 }
 FILE *f=fopen(argv[roundtrip?2:1],"rb");if(!f){perror("wire input");return 2;}
 BfWireScene scene={0};
 int good=bf_wire_read(f,&scene);fclose(f);
 if(!good){fprintf(stderr,"Rejected malformed, unauthorized-shape or oversized authoring wire.\n");return 3;}
 const char *destination=argv[roundtrip?3:2];
 FILE *out=fopen(destination,"wb");if(!out){perror("native output");bf_wire_clear(&scene);return 4;}
 good=roundtrip?bf_wire_write(out,&scene):bf_wire_render(out,&scene);
 int closed=fclose(out);
 if(!good||closed){remove(destination);fprintf(stderr,"Native projection refused unsupported or unsafe data.\n");bf_wire_clear(&scene);return 5;}
 fprintf(stderr,"AUTHORING_NATIVE_PREVIEW_PASS site=%s route=%s nodes=%u\n",scene.site,scene.route,scene.count);
 bf_wire_clear(&scene);return 0;
}
